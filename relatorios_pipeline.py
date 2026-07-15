"""
Pesquisas eleitorais: alerta diário + extração de voto por segmento e rejeição.

Uso:
  python relatorios_pipeline.py alerta    # email com as pesquisas que divulgam hoje (PesqEle)
  python relatorios_pipeline.py extrair   # extrai voto por segmento e rejeição dos relatórios
  python relatorios_pipeline.py rebuild_bi # reconstrói resultados_bi nas planilhas PollingData

Secrets: GOOGLE_CREDENTIALS_JSON, GEMINI_API_KEY, BREVO_API_KEY, EMAIL,
DESTINATARIOS, SPREADSHEET_ID (PesqEle), SPREADSHEET_ID_RELATORIOS.
"""

import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone

import gspread
import requests
from google.oauth2.service_account import Credentials

BRT = timezone(timedelta(hours=-3))
HEADERS = {"User-Agent": "Mozilla/5.0"}

RELATORIOS_COLUNAS = [
    ("registro", "Registro TSE"),
    ("cargo", "Cargo"),
    ("uf", "UF"),
    ("instituto", "Instituto"),
    ("data_divulgacao", "Data de divulgação"),
    ("link", "Link do relatório"),
    ("origem_link", "Origem do link"),
    ("nivel_conferencia", "Nível de conferência"),
    ("tipo_fonte", "Tipo"),
    ("conferido", "Conferido?"),
    ("segmentos_extraido", "Segmentos extraídos?"),
    ("segmentos_data_extracao", "Data da extração de segmentos"),
    ("segmentos_erro", "Erro na extração de segmentos"),
    ("segmentos_tentativas", "Tentativas de segmentos"),
    ("topline_extraido", "Topline extraída?"),
    ("topline_data_extracao", "Data da extração de topline"),
    ("topline_erro", "Erro na extração de topline"),
    ("topline_tentativas", "Tentativas de topline"),
]
REL_COL = dict(RELATORIOS_COLUNAS)
CABECALHO_RELATORIOS = [rotulo for _, rotulo in RELATORIOS_COLUNAS]
ALIASES_RELATORIOS = {
    "Registro TSE": ["registro", "registro_tse"],
    "Cargo": ["cargo"],
    "UF": ["uf"],
    "Instituto": ["instituto"],
    "Data de divulgação": ["data_divulgacao", "data_divulgacao_pesqele"],
    "Link do relatório": ["link"],
    "Origem do link": ["origem_link", "origem_busca"],
    "Nível de conferência": ["nivel_conferencia"],
    "Tipo": ["tipo_fonte", "tipo", "tipo_de_fonte"],
    "Conferido?": ["conferido"],
    "Segmentos extraídos?": ["segmentos_extraido", "extraido"],
    "Data da extração de segmentos": ["segmentos_data_extracao", "data_extracao"],
    "Erro na extração de segmentos": ["segmentos_erro", "extracao_erro"],
    "Tentativas de segmentos": ["segmentos_tentativas", "extracao_tentativas"],
    "Topline extraída?": ["topline_extraido"],
    "Data da extração de topline": ["topline_data_extracao"],
    "Erro na extração de topline": ["topline_erro"],
    "Tentativas de topline": ["topline_tentativas"],
}
REL_KEY = {rotulo: chave for chave, rotulo in RELATORIOS_COLUNAS}
for chave, rotulo in RELATORIOS_COLUNAS:
    REL_KEY[chave] = chave
    for alias in ALIASES_RELATORIOS.get(rotulo, []):
        REL_KEY[alias] = chave

# Não é uma extração concluída: é uma sinalização operacional visível de que o
# relatório exige lançamento pelo Polling Manual por não haver ficha cadastrada.
STATUS_TOPLINE_MANUAL = "⚠️ REGISTRE NO POLLING MANUAL"
POLLING_MANUAL_URL = "https://eixoestrategiapolitica.streamlit.app/Polling_Manual"


def _link_status_topline_manual() -> str:
    """Fórmula pt-BR: o status visível também leva direto ao lançamento manual."""
    return f'=HYPERLINK("{POLLING_MANUAL_URL}";"{STATUS_TOPLINE_MANUAL}")'

CABECALHOS = {
    "relatorios": CABECALHO_RELATORIOS,
    "voto_segmento": ["registro", "cargo", "turno", "uf", "instituto", "data_divulgacao",
                      "cenario", "candidato", "tipo_segmento", "segmento", "valor"],
    "rejeicao": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                 "candidato", "tipo_segmento", "segmento", "valor"],
    "aprovacao": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                  "alvo", "tipo_avaliacao", "resposta", "tipo_segmento", "segmento", "valor"],
    "topline_pesquisas": ["registro_tse", "ano", "cargo", "uf", "turno", "disputa",
                          "instituto", "classificacao_instituto", "data_campo",
                          "scenario_label", "descricao", "votos_por_entrevistado",
                          "modo", "amostra", "margem_erro",
                          "confianca", "metodologia", "poll_id", "scenario_id", "fonte_url",
                          "fonte_url_original", "horario_raspagem",
                          "validacao", "origem", "publicado", "liberado"],
    "topline_resultados": ["registro_tse", "ano", "cargo", "uf", "turno", "disputa",
                           "instituto", "classificacao_instituto", "data_campo",
                           "scenario_label", "candidato", "partido", "candidato_partido",
                           "tipo", "percentual", "poll_id", "scenario_id", "fonte_url",
                           "horario_raspagem", "origem"],
}

CARGOS_MONITORADOS = ("presidente", "governador", "senador")
CARGO_ROTULO = {
    "presidente": "Presidente",
    "governador": "Governador",
    "senador": "Senador",
}
POLLING_PESQUISAS_COLS = [
    "scenario_id", "poll_id", "ano", "uf", "cargo", "turno", "disputa",
    "instituto", "classificacao_instituto", "registro_tse", "data_campo",
    "modo", "amostra", "margem_erro", "confianca", "scenario_label",
    "fonte_url", "fonte_url_original", "horario_raspagem", "metodologia", "origem",
]
POLLING_RESULTADOS_COLS = [
    "scenario_id", "poll_id", "ano", "uf", "cargo", "turno", "disputa",
    "data_campo", "instituto", "classificacao_instituto", "registro_tse",
    "scenario_label", "candidato", "partido", "candidato_partido", "tipo",
    "percentual", "fonte_url", "horario_raspagem", "origem",
]


def _normalizar_booleanos_coluna(ws, col_i, ate_linha):
    """Converte texto literal 'TRUE'/'FALSE' (sobra de reescrita com value_input_option
    RAW, que nunca interpreta string como booleano) em booleano de verdade. Com
    validação BOOLEAN estrita, uma célula com o TEXTO 'TRUE' não conta como valor
    válido de checkbox e a caixinha não marca, mesmo com a validação certa aplicada."""
    try:
        valores = ws.col_values(col_i)
    except Exception as e:
        print(f"[AVISO] não deu pra ler a coluna pra normalizar booleanos: {e}")
        return
    requests = []
    for i in range(2, ate_linha + 1):
        v = valores[i - 1].strip().upper() if i - 1 < len(valores) else ""
        if v in ("TRUE", "FALSE"):
            requests.append({
                "updateCells": {
                    "range": {"sheetId": ws.id, "startRowIndex": i - 1, "endRowIndex": i,
                               "startColumnIndex": col_i - 1, "endColumnIndex": col_i},
                    "rows": [{"values": [{"userEnteredValue": {"boolValue": v == "TRUE"}}]}],
                    "fields": "userEnteredValue",
                }
            })
    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"[AVISO] não deu pra normalizar booleanos da coluna: {e}")


def _ativar_checkbox(ws, coluna, header, ate_linha):
    """Transforma a coluna em checkbox real do Sheets (TRUE/FALSE), da linha 2 até
    ate_linha (última linha COM pesquisa). Sem linha de dados, não faz nada, pra não
    espalhar checkbox vazio nas linhas de baixo."""
    if coluna not in header or ate_linha < 2:
        return
    col_i = header.index(coluna) + 1
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": ate_linha,
                        "startColumnIndex": col_i - 1,
                        "endColumnIndex": col_i,
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra criar o checkbox de '{coluna}': {e}")
    _normalizar_booleanos_coluna(ws, col_i, ate_linha)


def _ativar_dropdown(ws, coluna, header, ate_linha, opcoes):
    """Transforma a coluna em lista suspensa (ONE_OF_LIST) com as 'opcoes', da linha 2
    até ate_linha. strict=True + showCustomUi=True: mostra a setinha e só aceita um
    valor da lista (blank continua permitido)."""
    if coluna not in header or ate_linha < 2:
        return
    col_i = header.index(coluna) + 1
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": ate_linha,
                        "startColumnIndex": col_i - 1,
                        "endColumnIndex": col_i,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": str(o)} for o in opcoes],
                        },
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra criar a lista suspensa de '{coluna}': {e}")


def _colorir_por_valor(ws, coluna, header, ate_linha, cores):
    """Formatação condicional: pinta o fundo da coluna conforme o valor (ex.: relatório
    verde, notícia laranja, N/A cinza). 'cores' = {valor: (r,g,b)} com r,g,b em 0-1.
    Idempotente: remove as regras que já cobrem essa coluna antes de recriar, pra não
    acumular a cada rodada de manutenção. A cor persiste e vale pras linhas novas também."""
    if coluna not in header or ate_linha < 2:
        return
    col = header.index(coluna)
    try:
        meta = ws.spreadsheet.fetch_sheet_metadata(
            params={"fields": "sheets(properties(sheetId),conditionalFormats)"})
        reqs = []
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") != ws.id:
                continue
            cfs = s.get("conditionalFormats", [])
            for idx in range(len(cfs) - 1, -1, -1):   # de trás pra frente: índice não desloca
                rngs = cfs[idx].get("ranges", [])
                if any(r.get("startColumnIndex") == col and r.get("endColumnIndex") == col + 1 for r in rngs):
                    reqs.append({"deleteConditionalFormatRule": {"sheetId": ws.id, "index": idx}})
        faixa = {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": max(ate_linha, ws.row_count),
                 "startColumnIndex": col, "endColumnIndex": col + 1}
        for valor, (r, g, b) in cores.items():
            reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {
                "ranges": [faixa],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": valor}]},
                    "format": {"backgroundColor": {"red": r, "green": g, "blue": b}},
                }}}})
        if reqs:
            ws.spreadsheet.batch_update({"requests": reqs})
    except Exception as e:
        print(f"[AVISO] não deu pra colorir a coluna '{coluna}': {e}")


def _colorir_cabecalhos_relatorios(ws, header):
    """Dá leitura visual imediata aos três blocos operacionais da fila."""
    grupos = [
        # Identificação da pesquisa: cinza azulado.
        (["registro", "cargo", "uf", "instituto", "data_divulgacao"],
         (0.85, 0.90, 0.95)),
        # Fonte e revisão humana: verde muito claro.
        (["link", "origem_link", "nivel_conferencia", "tipo_fonte", "conferido"],
         (0.88, 0.94, 0.86)),
        # Resultado da extração demográfica: azul claro.
        (["segmentos_extraido", "segmentos_data_extracao", "segmentos_erro", "segmentos_tentativas"],
         (0.82, 0.91, 0.97)),
        # Resultado da extração de topline: roxo claro.
        (["topline_extraido", "topline_data_extracao", "topline_erro", "topline_tentativas"],
         (0.89, 0.84, 0.95)),
    ]
    requests = []
    for chaves, (r, g, b) in grupos:
        indices = [header.index(_rel_display(chave)) for chave in chaves if _rel_display(chave) in header]
        if not indices:
            continue
        # Os campos de cada bloco são contíguos no cabeçalho canônico.
        requests.append({"repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": min(indices),
                "endColumnIndex": max(indices) + 1,
            },
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": r, "green": g, "blue": b}}},
            "fields": "userEnteredFormat.backgroundColor",
        }})
    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"[AVISO] não deu pra colorir cabeçalhos da aba relatorios: {e}")


def _ultima_linha_com_registro(ws):
    col_a = ws.col_values(1)
    ultima = 1
    for idx, val in enumerate(col_a, start=1):
        if idx > 1 and str(val).strip():
            ultima = idx
    return ultima


def _ultima_linha_com_dados(ws):
    """Última linha com algum valor, incluindo o cabeçalho.

    A grade do Google Sheets pode ter centenas de linhas vazias pré-criadas;
    uma validação de checkbox também pode fazer ``get_all_values`` devolver linhas
    vazias. Por isso, contamos apenas linhas com conteúdo de verdade.
    """
    ultima = 1
    for linha, valores in enumerate(ws.get_all_values(), start=1):
        # O Sheets materializa checkbox vazio como FALSE. Uma linha que só tem
        # esse FALSE é sobra da validação, não é uma linha de dado.
        if any(str(valor).strip() and str(valor).strip().upper() != "FALSE"
               for valor in valores):
            ultima = linha
    return ultima


def _encolher_linhas_vazias(ws):
    """Remove somente as linhas vazias que sobram abaixo dos dados."""
    ultima = _ultima_linha_com_dados(ws)
    if ws.row_count <= ultima:
        return
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": ultima,
                        "endIndex": ws.row_count,
                    }
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra remover linhas vazias da aba '{ws.title}': {e}")


def _append_rows_compacto(ws, linhas, value_input_option="RAW"):
    """Acrescenta linhas e deixa a grade exatamente no tamanho dos dados.

    Antes de gravar, expande apenas o necessário; depois remove qualquer sobra.
    Assim as abas crescem junto com a entrada de dados, sem um bloco permanente
    de linhas vazias.
    """
    if not linhas:
        return
    ultima = _ultima_linha_com_dados(ws)
    necessario = ultima + len(linhas)
    if ws.row_count < necessario:
        ws.add_rows(necessario - ws.row_count)
    ws.append_rows(linhas, value_input_option=value_input_option)
    _encolher_linhas_vazias(ws)


def _normalizar_cabecalho(ws, cabecalho, remover_sobras=False):
    """Garante a ordem canônica sem perder dados de colunas já existentes."""
    valores = ws.get_all_values()
    if not valores:
        ws.update(range_name="A1", values=[cabecalho])
        # sem dados ainda: o checkbox nasce junto com as pesquisas (no atualiza_relatorios)
        return cabecalho[:]

    atual = valores[0]
    aliases_antigos = {a for aliases in ALIASES_RELATORIOS.values() for a in aliases}
    aliases_antigos.update(REL_COL.keys())
    extras = [c for c in atual if c and c not in cabecalho and c not in aliases_antigos]
    alvo = cabecalho + extras
    if atual == alvo:
        return atual

    idx = {nome: pos for pos, nome in enumerate(atual) if nome}

    def _valor(row, coluna):
        candidatos = [coluna] + ALIASES_RELATORIOS.get(coluna, [])
        for c in candidatos:
            pos = idx.get(c)
            if pos is not None and pos < len(row) and row[pos] != "":
                return row[pos]
        return ""

    novos = [alvo]
    for row in valores[1:]:
        novos.append([_valor(row, c) for c in alvo])

    if ws.col_count < len(alvo):
        ws.add_cols(len(alvo) - ws.col_count)
    ws.update(range_name="A1", values=novos, value_input_option="RAW")
    # coluna conferido recém-criada: aplica o checkbox só nas linhas que já têm pesquisa
    if _rel_display("conferido") in alvo and _rel_display("conferido") not in idx:
        _ativar_checkbox(ws, _rel_display("conferido"), alvo, ate_linha=len(valores))
    if remover_sobras:
        _remover_colunas_sobrando(ws, len(alvo))
    return alvo


def _migrar_topline_sem_conferida(ws):
    """Transfere a marca legada para `origem` e remove `conferida` do staging.

    A exclusão usa a API estrutural do Sheets, preservando as validações e a
    formatação das demais colunas, inclusive o checkbox `liberado`.
    """
    valores = ws.get_all_values()
    if not valores or "conferida" not in valores[0]:
        return

    header = valores[0]
    if "origem" not in header:
        _garantir_coluna(ws, header, "origem")
        valores = ws.get_all_values()
        header = valores[0]

    i_conferida = header.index("conferida")
    i_origem = header.index("origem")
    atualizacoes = []
    for linha, row in enumerate(valores[1:], start=2):
        origem = row[i_origem].strip() if len(row) > i_origem else ""
        legado = row[i_conferida].strip().lower() if len(row) > i_conferida else ""
        if origem:
            continue
        if "manual" in legado:
            origem = "polling_manual"
        elif legado:
            origem = "PDF (relatório do instituto)"
        else:
            # Esta aba recebe exclusivamente a extração de relatórios.
            origem = "PDF (relatório do instituto)"
        atualizacoes.append(gspread.Cell(linha, i_origem + 1, origem))

    if atualizacoes:
        ws.update_cells(atualizacoes, value_input_option="RAW")

    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": i_conferida,
                        "endIndex": i_conferida + 1,
                    }
                }
            }]
        })
        print(f"[migracao] {ws.title}: coluna 'conferida' removida")
    except Exception as e:
        print(f"[AVISO] não foi possível remover 'conferida' de {ws.title}: {e}")


def _remover_colunas_sobrando(ws, total_colunas):
    """Remove colunas antigas/duplicadas que ficaram à direita após migrar cabeçalho."""
    if ws.col_count <= total_colunas:
        return
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": total_colunas,
                        "endIndex": ws.col_count,
                    }
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra remover colunas antigas à direita: {e}")


def _creds_info():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    return json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))


def _sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return gspread.authorize(Credentials.from_service_account_info(_creds_info(), scopes=scopes))


def _rel_display(nome):
    return REL_COL.get(nome, nome)


def _rel_key(nome):
    return REL_KEY.get(nome, nome)


def _rel_record(row):
    return {_rel_key(k): v for k, v in row.items()}


def _rel_records(ws):
    return [_rel_record(r) for r in ws.get_all_records()]


def _rel_row(valores, header):
    return [valores.get(_rel_key(c), valores.get(c, "")) for c in header]


def _garantir_coluna_relatorios(ws, header, nome):
    display = _rel_display(nome)
    for candidato in [display, nome] + ALIASES_RELATORIOS.get(display, []):
        if candidato in header:
            return header.index(candidato) + 1
    return _garantir_coluna(ws, header, display)


def _resetar_validacoes_relatorios(ws, header, ate_linha):
    """Remove checkboxes/validações acidentais nas OUTRAS colunas e garante a de
    Conferido?. NUNCA limpa a validação de Conferido? antes de recriar: isso roda em
    TODA chamada de _aba() pra 'relatorios' (extrair, topline...), e limpar+recriar em
    duas chamadas separadas deixa uma janela em que, se a segunda falhar (rede, limite
    de taxa), o checkbox de Conferido? some da planilha até a próxima rodada consertar.
    setDataValidation é idempotente, então só (re)aplicar a de Conferido? já resolve
    sem esse risco."""
    if ate_linha < 2:
        return
    col_conferido = _rel_display("conferido")
    # colunas com validação PRÓPRIA que não podem ser limpas junto (senão o checkbox do
    # Conferido? e a lista suspensa do Tipo somem a cada rodada de manutenção).
    protegidas = sorted({header.index(_rel_display(n)) for n in ("conferido", "tipo_fonte")
                         if _rel_display(n) in header})
    faixas, ini = [], 0
    for pc in protegidas:
        if pc > ini:
            faixas.append((ini, pc))
        ini = pc + 1
    if ini < len(header):
        faixas.append((ini, len(header)))
    if faixas:
        try:
            ws.spreadsheet.batch_update({
                "requests": [{
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,
                            "endRowIndex": ate_linha,
                            "startColumnIndex": ini,
                            "endColumnIndex": fim,
                        },
                        "cell": {"dataValidation": None},
                        "fields": "dataValidation",
                    }
                } for ini, fim in faixas]
            })
        except Exception as e:
            print(f"[AVISO] não deu pra limpar validações antigas da aba relatorios: {e}")
    _ativar_checkbox(ws, col_conferido, header, ate_linha)
    _ativar_dropdown(ws, _rel_display("tipo_fonte"), header, ate_linha, ["relatório", "notícia", "N/A"])
    _colorir_por_valor(ws, _rel_display("tipo_fonte"), header, ate_linha, {
        "relatório": (0.82, 0.93, 0.82),   # verde claro
        "notícia": (1.0, 0.90, 0.80),      # laranja claro
        "N/A": (0.90, 0.90, 0.90),         # cinza claro
    })
    _colorir_por_valor(ws, _rel_display("topline_extraido"), header, ate_linha, {
        "sim": (0.82, 0.93, 0.82),
        STATUS_TOPLINE_MANUAL: (1.0, 0.82, 0.68),  # laranja: ação manual necessária
    })
    _colorir_por_valor(ws, _rel_display("segmentos_extraido"), header, ate_linha, {
        "sim": (0.82, 0.93, 0.82),
        "não": (0.96, 0.80, 0.80),  # vermelho pastel: relatório sem quebra de segmento
    })
    _colorir_cabecalhos_relatorios(ws, header)


def _aba(sh, nome, manutencao=True):
    """Garante a aba e o cabeçalho. Cria a aba se não existir; escreve o
    cabeçalho se a primeira linha estiver vazia. Não mexe em dados existentes.

    manutencao=False pula a manutenção pesada da aba 'relatorios' (split de
    linhas multicargo + reset das validações + remoção de colunas sobrando).
    Comandos que só LEEM a fila (extrair, topline) passam False pra não refazer,
    a cada invocação, um trabalho de planilha que só precisa rodar quando linhas
    novas entram (isso acontece no passo que adiciona pesquisas e no busca_fontes)."""
    header = CABECALHOS[nome]
    try:
        ws = sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=nome, rows=1, cols=len(header))
    if not ws.row_values(1):
        ws.update(range_name="A1", values=[header])
    elif nome == "relatorios":
        header = _normalizar_cabecalho(ws, header, remover_sobras=manutencao)
        if manutencao:
            _separar_linhas_multicargo(ws, header)
            _resetar_validacoes_relatorios(ws, header, _ultima_linha_com_registro(ws))
    elif nome in ("topline_pesquisas", "topline_resultados"):
        _normalizar_cabecalho(ws, header)
        _migrar_topline_sem_conferida(ws)
    return ws


def _verdadeiro(v):
    return str(v).strip().lower() in ("sim", "true", "verdadeiro", "1", "x")


def _int0(v):
    """Inteiro tolerante: '', texto ou lixo viram 0 (célula editada à mão não derruba o run)."""
    try:
        return int(float(str(v).strip() or 0))
    except Exception:
        return 0


def _data_iso(valor):
    """'01/07/2026' (padrão BR, dia primeiro) ou '2026-07-01' -> '2026-07-01'."""
    s = str(valor or "").strip()
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return s[:10]


def _sem_acento(valor):
    return unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode()


def _cargo_norm(valor):
    s = _sem_acento(valor).strip().lower()
    for cargo in CARGOS_MONITORADOS:
        if cargo in s:
            return cargo
    return s


def _cargos_monitorados(valor):
    s = _sem_acento(valor).lower()
    cargos = []
    for cargo in CARGOS_MONITORADOS:
        if cargo in s and cargo not in cargos:
            cargos.append(cargo)
    return [CARGO_ROTULO[c] for c in cargos]


def _chave_fila(registro, cargo, uf):
    return (
        re.sub(r"[^A-Z0-9]", "", str(registro or "").upper()),
        _cargo_norm(cargo),
        _sem_acento(uf).strip().upper(),
    )


def _limpar_status_extracao(row):
    for coluna in (
        "link", "nivel_conferencia", "conferido",
        "segmentos_extraido", "segmentos_data_extracao", "segmentos_erro", "segmentos_tentativas",
        "topline_extraido", "topline_data_extracao", "topline_erro", "topline_tentativas",
    ):
        row[_rel_display(coluna)] = ""
        row[coluna] = ""
    row[_rel_display("origem_link")] = "separado de linha multicargo; buscar fonte específica"
    row["origem_link"] = "separado de linha multicargo; buscar fonte específica"
    return row


def _separar_linhas_multicargo(ws, header):
    """Migra a fila para uma linha por registro+cargo+UF.

    Quando uma linha antiga tinha "Governador, Senador", a original fica com o
    primeiro cargo monitorado e as demais viram novas linhas sem link. Isso evita
    que uma matéria parcial de governador cubra senador por engano.
    """
    if _rel_display("cargo") not in header or _rel_display("registro") not in header:
        return

    registros = _rel_records(ws)
    existentes = {
        _chave_fila(r.get("registro"), r.get("cargo"), r.get("uf"))
        for r in registros
        if str(r.get("registro", "")).strip()
    }
    col_cargo = header.index(_rel_display("cargo")) + 1
    updates, novas = [], []

    for row_i, r in enumerate(registros, start=2):
        cargos = _cargos_monitorados(r.get("cargo"))
        if not cargos:
            continue

        primeiro = cargos[0]
        if str(r.get("cargo", "")).strip() != primeiro:
            updates.append(gspread.Cell(row_i, col_cargo, primeiro))
        if len(cargos) <= 1:
            continue
        existentes.add(_chave_fila(r.get("registro"), primeiro, r.get("uf")))

        for cargo in cargos[1:]:
            chave = _chave_fila(r.get("registro"), cargo, r.get("uf"))
            if chave in existentes:
                continue
            novo = {c: r.get(_rel_key(c), "") for c in header}
            novo[_rel_display("cargo")] = cargo
            _limpar_status_extracao(novo)
            novas.append([novo.get(c, "") for c in header])
            existentes.add(chave)

    if updates:
        ws.update_cells(updates, value_input_option="RAW")
    if novas:
        _append_rows_compacto(ws, novas)
        print(f"{len(novas)} linha(s) multicargo separada(s) na fila de relatórios.", flush=True)


def _garantir_coluna(ws, header, nome):
    """Retorna o índice (1-based) da coluna 'nome'. Se não existir, cria (expandindo
    a grade se preciso, pra não estourar o limite de colunas)."""
    if nome in header:
        return header.index(nome) + 1
    novo = len(header) + 1
    if ws.col_count < novo:
        ws.add_cols(novo - ws.col_count)
    ws.update_cell(1, novo, nome)
    header.append(nome)
    return novo


def _scenario_ids_na_aba(ws):
    """Retorna os scenario_id já existentes sem depender de ordem ou formato da aba."""
    header = ws.row_values(1)
    if "scenario_id" not in header:
        return set()
    i_scenario = header.index("scenario_id")
    vals = ws.get_all_values()
    return {
        row[i_scenario].strip()
        for row in vals[1:]
        if len(row) > i_scenario and row[i_scenario].strip()
    }


def _texto_chave_publicacao(valor):
    """Chave estável para comparar staging e legado sem mudar seus valores."""
    return re.sub(r"\s+", " ", str(valor or "").strip()).casefold()


def _chave_legado_publicacao(linha):
    """Chave semântica de cenário usada apenas como rede de segurança do legado.

    ``scenario_id`` e ``poll_id`` são as travas primárias. O histórico contém
    alguns ids produzidos por versões antigas; por isso, para t1 com mesmo
    Registro TSE + cargo + turno + cenário também tratamos como já publicado.
    Em t2 a disputa é obrigatória nessa chave, para nunca confundir confrontos
    hipotéticos diferentes do mesmo relatório.
    """
    registro = _texto_chave_publicacao(linha.get("registro_tse", ""))
    cargo = _texto_chave_publicacao(linha.get("cargo", ""))
    turno = _texto_chave_publicacao(linha.get("turno", ""))
    cenario = _texto_chave_publicacao(linha.get("scenario_label", ""))
    disputa = _texto_chave_publicacao(linha.get("disputa", ""))
    if not all((registro, cargo, turno, cenario)):
        return None
    if turno == "t2":
        return (registro, cargo, turno, cenario, disputa) if disputa else None
    return (registro, cargo, turno, cenario, "")


def _indice_publicacao_destino(ws):
    """Indexa as chaves já existentes de uma aba ``pesquisas`` de destino."""
    header = ws.row_values(1)
    valores = ws.get_all_values()
    registros = []
    for row in valores[1:]:
        registros.append({c: row[i] if i < len(row) else "" for i, c in enumerate(header)})
    return {
        "scenario_id": {
            _texto_chave_publicacao(r.get("scenario_id")) for r in registros
            if _texto_chave_publicacao(r.get("scenario_id"))
        },
        "poll_id": {
            _texto_chave_publicacao(r.get("poll_id")) for r in registros
            if _texto_chave_publicacao(r.get("poll_id"))
        },
        "legado": {k for r in registros if (k := _chave_legado_publicacao(r))},
    }


def _motivo_colisao_publicacao(linha, indice_destino):
    """Retorna a trava que bloqueia um cenário já existente, ou vazio."""
    scenario_id = _texto_chave_publicacao(linha.get("scenario_id", ""))
    if scenario_id and scenario_id in indice_destino["scenario_id"]:
        return "scenario_id já existe no destino"
    poll_id = _texto_chave_publicacao(linha.get("poll_id", ""))
    if poll_id and poll_id in indice_destino["poll_id"]:
        return "poll_id já existe no destino"
    chave_legado = _chave_legado_publicacao(linha)
    if chave_legado and chave_legado in indice_destino["legado"]:
        return "cenário legado equivalente já existe no destino"
    return ""


def _marcar_origem(ws, label, scenario_ids):
    """Marca somente as linhas recém-inseridas nesta publicação.

    O recorte por ``scenario_id`` impede que uma publicação de PDF reescreva a
    origem de linhas já existentes, em especial as salvas pelo Polling Manual.
    """
    ids = {str(s).strip() for s in scenario_ids if str(s).strip()}
    if not ids:
        return
    header = ws.row_values(1)
    if "scenario_id" not in header:
        raise ValueError("Aba de destino sem coluna scenario_id.")
    col_o = _garantir_coluna(ws, header, "origem")
    i_scenario, i_o = header.index("scenario_id"), col_o - 1
    vals = ws.get_all_values()
    updates = []
    for linha, row in enumerate(vals[1:], start=2):
        scenario_id = row[i_scenario].strip() if len(row) > i_scenario else ""
        atual = row[i_o] if len(row) > i_o else ""
        if scenario_id in ids and atual != label:
            updates.append(gspread.Cell(linha, col_o, label))
    if updates:
        ws.update_cells(updates, value_input_option="RAW")


# ─────────────────────────────── ALERTA ───────────────────────────────

PESQELE_ID = os.getenv("SPREADSHEET_ID", "")
PESQELE_ABA = os.getenv("PESQELE_ABA", "Consolidado")
PASTA_URL = os.getenv("PASTA_RELATORIOS_URL",
                      "https://drive.google.com/drive/folders/0AH-94UFLKIFPUk9PVA")
GRUPOS = ["Presidente (Nacional)", "Presidente (por UF)", "Governador / Senador"]


def _grupo(p):
    cargos = str(p.get("cargos", "")).lower()
    abr = str(p.get("abrangencia", "")).strip().upper()
    if "presidente" in cargos:
        return GRUPOS[0] if abr == "BRASIL" else GRUPOS[1]
    return GRUPOS[2]


def _tabela(pesquisas):
    linhas = "".join(
        "<tr>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('numero_identificacao','')}</td>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('empresa_contratada','')}</td>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('abrangencia','')}</td>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('cargos','')}</td>"
        "</tr>"
        for p in pesquisas
    )
    return (
        "<table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:6px'>"
        "<tr style='text-align:left;background:#f3f4f6'>"
        "<th style='padding:6px 10px'>Registro</th><th style='padding:6px 10px'>Instituto</th>"
        "<th style='padding:6px 10px'>Abrangência</th><th style='padding:6px 10px'>Cargos</th></tr>"
        f"{linhas}</table>"
    )


def _html(pesquisas, hoje):
    por_grupo = {g: [] for g in GRUPOS}
    for p in pesquisas:
        por_grupo[_grupo(p)].append(p)
    secoes = "".join(
        f"<h3 style='margin:18px 0 6px 0'>{g} ({len(por_grupo[g])})</h3>{_tabela(por_grupo[g])}"
        for g in GRUPOS if por_grupo[g]
    )
    planilha_url = (f"https://docs.google.com/spreadsheets/d/{RELATORIOS_ID}/edit"
                    if RELATORIOS_ID else "#")
    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#111">
      <h2 style="margin:0 0 6px 0">Pesquisas com divulgação prevista para hoje</h2>
      <div style="color:#374151;margin:0 0 14px 0">{hoje} · {len(pesquisas)} pesquisa(s)</div>
      <div style="background:#eef0f6;border-left:3px solid #192D4E;padding:10px 12px;margin:0 0 16px 0;font-size:13px">
        <strong style="color:#192D4E">Ação do dia</strong>, para cada pesquisa abaixo:
        <ol style="margin:8px 0 0 0;padding-left:18px">
          <li>Baixe o relatório completo e salve na pasta
            <a href="{PASTA_URL}" style="color:#192D4E">Pesquisas Eleitorais</a> do Drive,
            na subpasta do cargo: Presidente Nacional em <b>Presidenciáveis/Nacional</b>;
            Presidente por UF em <b>Presidenciáveis/Por UF</b>;
            Governador e Senador em <b>Governadores+Senadores</b>.</li>
          <li>Na planilha
            <a href="{planilha_url}" style="color:#192D4E">Voto por Segmento</a>,
            aba <b>relatorios</b>, ache a linha do registro (já criada hoje) e cole o link
            do relatório na coluna <b>link</b>. A extração roda em cima dessas linhas.</li>
        </ol>
      </div>
      {secoes}
    </body></html>
    """


def _enviar(subject, html_body):
    import re
    api_key, sender = os.getenv("BREVO_API_KEY"), os.getenv("EMAIL")
    bruto = re.split(r"[,;\s]+", os.getenv("DESTINATARIOS", ""))
    dests = [e.strip(" <>") for e in bruto if "@" in e]
    if not (api_key and sender and dests):
        print("Config de email incompleta ou sem destinatário válido; pulando envio.")
        return
    from brevo_python import ApiClient, Configuration
    from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
    from brevo_python.models.send_smtp_email import SendSmtpEmail
    cfg = Configuration()
    cfg.api_key["api-key"] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=cfg))
    for dest in dests:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}], sender={"email": sender},
                subject=subject, html_content=html_body))
            print(f"enviado para {dest}")
        except Exception as e:
            print(f"falha para {dest}: {e}")


def _preencher_fila(pesquisas):
    """Cria na aba 'relatorios' uma linha por registro+cargo monitorado."""
    if not RELATORIOS_ID:
        print("SPREADSHEET_ID_RELATORIOS não definido; pulando preenchimento da fila.")
        return
    fila = _aba(_sheets().open_by_key(RELATORIOS_ID), "relatorios")
    header = fila.row_values(1)
    existentes = {
        _chave_fila(r.get("registro"), r.get("cargo"), r.get("uf"))
        for r in _rel_records(fila)
    }
    novas = []
    for p in pesquisas:
        reg = str(p.get("numero_identificacao", "")).strip()
        if not reg:
            continue
        for cargo in _cargos_monitorados(p.get("cargos", "")):
            chave = _chave_fila(reg, cargo, p.get("abrangencia", ""))
            if chave in existentes:
                continue
            valores = {
                "registro": reg,
                "cargo": cargo,
                "uf": p.get("abrangencia", ""),
                "instituto": p.get("empresa_contratada", ""),
                "data_divulgacao": str(p.get("data_divulgacao", ""))[:10],
            }
            novas.append(_rel_row(valores, header))
            existentes.add(chave)
    if novas:
        _append_rows_compacto(fila, novas)
        _resetar_validacoes_relatorios(fila, header, _ultima_linha_com_registro(fila))
    print(f"{len(novas)} linha(s) adicionada(s) à fila de relatórios.")


def cmd_alerta():
    if not PESQELE_ID:
        raise RuntimeError("Defina SPREADSHEET_ID (planilha do PesqEle).")
    ws = _sheets().open_by_key(PESQELE_ID).worksheet(PESQELE_ABA)
    hoje = datetime.now(BRT).strftime("%Y-%m-%d")
    pesquisas = [r for r in ws.get_all_records()
                 if str(r.get("data_divulgacao", ""))[:10] == hoje]
    print(f"{len(pesquisas)} pesquisa(s) com divulgação hoje ({hoje})")
    if pesquisas:
        # fila primeiro: mesmo que o e-mail falhe, as linhas do dia ficam criadas
        _preencher_fila(pesquisas)
        _enviar(f"Pesquisas eleitorais previstas para hoje ({len(pesquisas)})",
                _html(pesquisas, hoje))


# ────────────────────────────── EXTRAÇÃO ──────────────────────────────

RELATORIOS_ID = os.getenv("SPREADSHEET_ID_RELATORIOS", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

PROMPT = (
    "Você é um analista de dados de pesquisas eleitorais da Eixo. Você recebe o PDF do "
    "relatório completo de uma pesquisa e extrai os cruzamentos por segmento.\n\n"
    "O relatório PODE conter mais de um cargo (presidente, governador, senador) no mesmo "
    "documento, mesmo que só um esteja no título. Quando houver FOCO DE CARGO abaixo, "
    "extraia APENAS esse cargo. Sem FOCO DE CARGO, extraia todos os cargos que aparecerem, "
    "cada linha com o cargo correto.\n\n"
    "Extraia TRÊS listas, em JSON:\n\n"
    "1) voto_segmento: para CADA cenário de voto estimulado e CADA candidato, o percentual "
    "de voto quebrado por segmento demográfico. "
    'Cada item: {"cargo": "presidente|governador|senador", "turno": "t1|t2", "cenario": "...", '
    '"candidato": "Nome (PARTIDO)", "tipo_segmento": "...", "segmento": "...", "valor": número}. '
    "'cargo' é a disputa daquele cenário específico; 'turno' é t2 quando for simulação de "
    "segundo turno (confronto direto), senão t1.\n\n"
    "2) rejeicao: para CADA candidato, o percentual de rejeição quebrado por segmento. "
    'Cada item: {"cargo": "presidente|governador|senador", "candidato": "Nome (PARTIDO)", '
    '"tipo_segmento": "...", "segmento": "...", "valor": número}. '
    "'cargo' é a disputa a que a rejeição se refere.\n\n"
    "3) aprovacao: APENAS aprovação/desaprovação ou avaliação do DESEMPENHO de quem está no "
    "cargo (presidente ou governador em exercício), quebrada por segmento. "
    'Cada item: {"alvo": "...", "tipo_avaliacao": "aprova_desaprova|nota_gestao", "resposta": "...", "tipo_segmento": "...", "segmento": "...", "valor": número}.\n\n'
    "Regras:\n"
    "1) Preserve os números EXATAMENTE como no relatório. Não arredonde nem recalcule.\n"
    "1b) Percentuais com vírgula decimal devem virar número decimal com ponto: 26,1% -> 26.1; "
    "50,3% -> 50.3; 64,01% -> 64.01. NUNCA remova a vírgula transformando 26,1 em 261 "
    "ou 50,3 em 503. Todo 'valor' individual deve ficar entre 0 e 100.\n"
    "2) Não invente. Se um cruzamento não existir no relatório, omita.\n"
    "3) Use os rótulos de segmento como aparecem (ex: Masculino, Feminino, 16 a 24 anos, "
    "25 a 34 anos, Fundamental, Médio, Superior, Até 2 SM, Mais de 5 a 10 SM).\n"
    "4) 'tipo_segmento' classifica o segmento em uma destas categorias: genero, idade, "
    "escolaridade, renda, regiao, religiao, raca. Use exatamente esses rótulos minúsculos. "
    "Se não encaixar em nenhuma (ex.: ocupação, classe social, PEA/não PEA), use 'outro'. "
    "Para o total geral (sem recorte), use tipo_segmento='geral' e segmento='Total'.\n"
    "4b) REGRA DO TOTAL GERAL, diferente em cada lista: em voto_segmento, NÃO inclua o total "
    "geral/sem recorte (a intenção de voto geral já vai pro topline por outro fluxo; repetir "
    "aqui duplica o dado) - se o item de voto não tiver recorte demográfico, OMITA-o. Já em "
    "rejeicao e aprovacao é o CONTRÁRIO: o total geral (tipo_segmento='geral', segmento='Total') "
    "é o número principal e DEVE SEMPRE ser incluído quando o relatório o trouxer, além das "
    "quebras demográficas - rejeição e aprovação não têm outro fluxo, se faltar o geral aqui "
    "o dado se perde.\n"
    "5) 'valor' é número, sem o símbolo de %.\n"
    "6) Em 'cenario', use 'Cenário N' para votação estimulada de 1º turno. A única votação "
    "estimulada de um cargo/turno é 'Cenário 1' (NUNCA apenas 'Estimulada'). 'Estimulada 1', "
    "'Estimulada - Cenário 1' e 'Cenário 01' também viram 'Cenário 1'. Em confronto direto, "
    "use os dois nomes completos com partido (ex.: 'Lula (PT) x Flávio Bolsonaro (PL)'). "
    "NUNCA repita o mesmo rótulo para conjuntos de candidatos diferentes.\n"
    "7) Em 'candidato', use SEMPRE o formato 'Nome (SIGLA)'. Se o candidato constar na lista "
    "canônica fornecida abaixo, use EXATAMENTE o nome e a sigla de lá. A sigla do partido é "
    "sempre curta e em caixa alta (PT, PL, MDB, REP, UNIAO...), nunca por extenso.\n"
    "7b) Em voto_segmento, consolide as respostas inválidas (branco, nulo, não sabe, não "
    "respondeu, indeciso, nenhum) em um único candidato='Não válido' por cenário e segmento, "
    "somando os valores. Em rejeicao, mantenha as categorias de resposta como no relatório.\n"
    "7c) Quando uma tabela tiver colunas 'Porcentual' e 'Porcentagem válida', escolha UMA "
    "base só. Se você incluir 'Não válido' no cenário, use a coluna 'Porcentual' para "
    "candidatos e inválidos. Não misture 'Porcentagem válida' dos candidatos com "
    "branco/nulo/NS/NR da coluna 'Porcentual'.\n"
    "7d) Para senador, '1º voto', '2º voto' e 'média do 1º e 2º votos' continuam sendo "
    "turno='t1'. Use turno='t2' somente para confronto direto de segundo turno entre "
    "dois nomes, não para segundo voto de senador.\n"
    "8) Em aprovacao, PADRONIZE o 'alvo' assim: se for avaliação do presidente/governo "
    "federal, use SEMPRE 'Presidente <Nome>' (ex: 'Presidente Lula'), mesmo que o relatório "
    "escreva 'Governo Lula', 'Governo Federal' ou 'gestão do presidente'. Se for governador/"
    "governo estadual, use SEMPRE 'Governador <Nome>'. 'resposta' é a categoria como no "
    "relatório (ex: 'Aprova', 'Desaprova', 'Não sabe', 'Ótimo/Bom', 'Regular', 'Ruim/Péssimo').\n"
    "8b) 'tipo_avaliacao' separa as DUAS perguntas de avaliação, que são diferentes e cada uma "
    "soma ~100% sozinha, NÃO as misture: use 'aprova_desaprova' para a pergunta binária "
    "(respostas Aprova / Desaprova / Não sabe) e 'nota_gestao' para a pergunta de nota/escala "
    "(respostas Ótimo / Bom / Regular / Ruim / Péssimo, ou Ótimo-Bom / Regular / Ruim-Péssimo). "
    "Cada linha de aprovacao deve trazer o 'tipo_avaliacao' correto. Em UMA mesma pergunta, "
    "use uma base só: se Ótimo e Bom (ou Ótima e Boa) aparecem separados, NÃO inclua o subtotal "
    "Ótimo/Bom (ou Ótima/Boa); aplique a mesma regra a Ruim/Péssimo. Só use a categoria "
    "combinada quando as categorias individuais não estiverem no gráfico.\n"
    "9) NÃO inclua em aprovacao perguntas hipotéticas ou de intenção (ex: 'gostaria que se "
    "reelegesse', 'a reeleição de X', 'a eleição de Y', desejo de candidatura). aprovacao é só "
    "avaliação do trabalho de quem já governa.\n"
    "10) voto_segmento é a votação por segmento DEMOGRÁFICO. O campo 'segmento' deve ser uma "
    "categoria curta de gênero (Masculino/Feminino), idade (16 a 24 anos...), raça/cor "
    "(Branca, Preta, Parda, Indígena), religião (Católicos, Evangélicos...), região, "
    "escolaridade, renda, ou atividade/PEA. NUNCA use o TEXTO de uma pergunta como 'segmento' "
    "(ex: 'Nos últimos 10 dias participou de celebração religiosa? Sim' NÃO é segmento). "
    "Se o recorte não for demográfico, não inclua em voto_segmento.\n"
    "10b) Em voto_segmento, quebre por segmento APENAS a votação principal estimulada de 1º "
    "turno. NÃO quebre por segmento as simulações de 2º turno nem perguntas filtro/arrasto.\n"
    "10c) PERGUNTA FILTRO NUNCA entra em voto_segmento: a pergunta 'O candidato que você "
    "votaria é um desses nomes ou outro candidato?' (assinatura: respostas com 'Outro "
    "candidato' e alto percentual de 'Ausente'/sem resposta) NÃO é intenção de voto - é só um "
    "filtro que antecede a pergunta formal. CUIDADO: o cruzamento demográfico dela vem em "
    "tabela IGUAL à da pergunta formal e costuma aparecer ANTES no relatório; não a confunda "
    "com a estimulada real nem a rotule de 'Estimulada'. Quebre por segmento a pergunta formal "
    "('Se a eleição fosse hoje e esses fossem os candidatos...'), que vem logo depois.\n"
    "11) DADO AUSENTE: se um valor não estiver no material (cruzamento que não cabe no PDF, "
    "célula em branco, cargo sem aquela quebra), OMITA o item. NUNCA grave 0 para dado ausente "
    "- 0 é um número real (candidato com zero voto), não use 0 como 'não encontrei'.\n\n"
    "Responda SOMENTE o JSON, sem texto extra e sem markdown:\n"
    '{"voto_segmento": [...], "rejeicao": [...], "aprovacao": [...]}'
)


def _drive_id(link):
    import re
    m = re.search(r"/d/([A-Za-z0-9_-]+)", link) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", link)
    return m.group(1) if m else None


def _pdf_cache_path(link):
    """Caminho de cache pro PDF do link, num diretório temporário estável do runner.
    Serve pra baixar UMA vez por link e reusar entre cmd_extrair e cmd_topline (mesmo
    job) e entre tentativas, em vez de rebaixar o mesmo PDF várias vezes."""
    import hashlib, tempfile
    if not link:
        return None
    d = os.path.join(tempfile.gettempdir(), "eixo_pdf_cache")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        return None
    h = hashlib.sha1(str(link).encode("utf-8", "ignore")).hexdigest()[:20]
    return os.path.join(d, f"{h}.pdf")


def _baixar_pdf(link):
    """Baixa o PDF do link (com cache em disco pra não rebaixar o mesmo arquivo).
    Se for link do Google Drive, usa a API do Drive com a conta de serviço (a pasta
    precisa estar compartilhada com ela). Senão, download direto."""
    cache = _pdf_cache_path(link)
    if cache and os.path.exists(cache) and os.path.getsize(cache) > 1000:
        with open(cache, "rb") as f:
            return f.read()
    conteudo = _baixar_pdf_raw(link)
    if cache and conteudo:
        try:
            with open(cache, "wb") as f:
                f.write(conteudo)
        except OSError:
            pass
    return conteudo


def _baixar_pdf_raw(link):
    fid = _drive_id(link)
    if not fid:
        return requests.get(link, headers=HEADERS, timeout=60).content
    from google.auth.transport.requests import Request
    creds = Credentials.from_service_account_info(
        _creds_info(), scopes=["https://www.googleapis.com/auth/drive.readonly"])
    creds.refresh(Request())
    r = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{fid}",
        params={"alt": "media", "supportsAllDrives": "true"},
        headers={"Authorization": f"Bearer {creds.token}"}, timeout=120)
    r.raise_for_status()
    if r.content[:4] != b"%PDF":
        raise RuntimeError("conteúdo não é PDF (a conta de serviço tem acesso ao arquivo?)")
    return r.content


PAGINAS_POR_BLOCO = 5

REGISTRO_TSE_RE = re.compile(
    r"\b(?:BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)"
    r"[\s-]*\d{5}/2026\b",
    flags=re.I,
)


def _norm_registro(valor):
    return re.sub(r"[^A-Z0-9]", "", str(valor or "").upper())


def _registros_tse_texto(texto):
    return {_norm_registro(m.group(0)) for m in REGISTRO_TSE_RE.finditer(texto or "")}


def _texto_pdf_bytes(pdf_bytes, max_paginas=None):
    """Texto do PDF para validação e fallback. Vazio em PDF só imagem."""
    try:
        import fitz  # PyMuPDF
        partes = []
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            total = doc.page_count if max_paginas is None else min(doc.page_count, max_paginas)
            for i in range(total):
                raw = doc.load_page(i).get_text("text") or ""
                raw = raw.replace("-\n", "").replace("\n", " ")
                raw = re.sub(r"\s{2,}", " ", raw).strip()
                if raw:
                    partes.append(raw)
        return "\n".join(partes)
    except Exception:
        return ""


def _validar_registro_pdf(pdf_bytes, registro):
    """Retorna mensagem de erro se o PDF cita registros TSE e nenhum é o da fila."""
    registro_norm = _norm_registro(registro)
    if not registro_norm:
        return ""
    texto = _texto_pdf_bytes(pdf_bytes, max_paginas=30)
    encontrados = _registros_tse_texto(texto)
    if encontrados and registro_norm not in encontrados:
        regs = ", ".join(sorted(encontrados))
        return f"registro da fila não aparece no PDF; registros encontrados: {regs}"
    return ""


def _extrair_json_objeto(texto):
    bruto = (texto or "").strip()
    bruto = re.sub(r"^```json\s*", "", bruto, flags=re.I)
    bruto = re.sub(r"^```\s*", "", bruto)
    bruto = re.sub(r"\s*```$", "", bruto)
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", bruto):
        try:
            obj, _ = decoder.raw_decode(bruto[match.start():])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError("JSON não encontrado na resposta do Gemini")


def _normalizar_percentual_extraido(valor):
    """Percentual individual do Gemini, tolerando vírgula decimal perdida.

    Exemplos comuns do erro: 26,1 -> 261; 50,3 -> 503; 64,01 -> 6401.
    Só corrige quando o valor veio como inteiro acima de 100; número decimal
    acima de 100 continua inválido para não esconder mistura de bases.
    """
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    limpo = texto.replace("%", "").replace(" ", "").replace(",", ".")
    try:
        numero = float(limpo)
    except Exception:
        return None

    if numero > 100:
        inteiro_sem_decimal = re.fullmatch(r"\d+", texto.replace("%", "").replace(" ", "")) is not None
        if not inteiro_sem_decimal:
            return None
        digitos = re.sub(r"\D", "", texto)
        if not digitos:
            return None
        if int(digitos) == 1000:
            numero = 100.0
        elif len(digitos) >= 4:
            numero = float(digitos) / 100
        else:
            numero = float(digitos) / 10

    if numero < 0 or numero > 100:
        return None
    return round(numero, 2)


def _normalizar_percentuais_lista(itens, contexto):
    erros = []
    for idx, item in enumerate(itens, start=1):
        valor = _normalizar_percentual_extraido(item.get("valor"))
        if valor is None:
            bruto = item.get("valor", "")
            alvo = item.get("candidato") or item.get("alvo") or item.get("resposta") or "item"
            erros.append(f"{contexto} #{idx} {alvo}: valor inválido ({bruto})")
            continue
        item["valor"] = valor
    return erros


def _avisos_soma_voto(itens, cargo):
    """Aviso (não bloqueia) quando os candidatos de um mesmo cenário+segmento somam
    longe de 100% (ou ~200% no senador de 2 vagas). Pega candidato faltando, zero
    indevido, base errada. Valores já normalizados (float)."""
    from collections import defaultdict
    grupos = defaultdict(list)
    for v in itens:
        try:
            val = float(v.get("valor"))
        except (TypeError, ValueError):
            continue
        chave = (str(v.get("cenario", "")), str(v.get("tipo_segmento", "")), str(v.get("segmento", "")))
        grupos[chave].append(val)
    eh_senador = "senador" in str(cargo).lower()
    avisos = []
    for (cen, _tseg, seg), vals in grupos.items():
        if len(vals) < 2:   # 1 valor só não dá pra checar soma
            continue
        s = sum(vals)
        if 90 <= s <= 110:
            continue
        if eh_senador and 150 <= s <= 210:   # 2 vagas: soma ~200 é legítima
            continue
        rotulo = seg or cen or "Total"
        avisos.append(f"soma {s:.0f}% em '{rotulo[:24]}'")
    return avisos[:4]


def _avisos_soma_aprovacao(itens):
    """Aviso (não bloqueia) quando as respostas de uma MESMA pergunta de avaliação
    (mesmo alvo + tipo_avaliacao + segmento) somam longe de 100%. Pega pergunta
    incompleta (faltou uma resposta). Valores já normalizados (float)."""
    from collections import defaultdict
    grupos = defaultdict(list)
    for v in itens:
        try:
            val = float(v.get("valor"))
        except (TypeError, ValueError):
            continue
        chave = (str(v.get("alvo", "")), str(v.get("tipo_avaliacao", "")),
                 str(v.get("tipo_segmento", "")), str(v.get("segmento", "")))
        grupos[chave].append(val)
    avisos = []
    for (alvo, _tipo, _tseg, seg), vals in grupos.items():
        if len(vals) < 2:
            continue
        s = sum(vals)
        if 90 <= s <= 110:
            continue
        avisos.append(f"aprovação {alvo[:14]} soma {s:.0f}% em '{(seg or 'Total')[:16]}'")
    return avisos[:3]


def _turno_segmento(item):
    turno = str(item.get("turno", "t1") or "t1").strip().lower()
    cargo = _cargo_norm(item.get("cargo", ""))
    cenario = _sem_acento(item.get("cenario", "")).lower()
    if cargo == "senador" and turno == "t2":
        fala_de_voto_senado = any(t in cenario for t in (
            "1o voto", "1 voto", "primeiro voto", "2o voto", "2 voto",
            "segundo voto", "media do 1", "media do primeiro", "media do 2",
        ))
        confronto = re.search(r"\b(x|versus|contra)\b", cenario) is not None
        if fala_de_voto_senado and not confronto:
            return "t1"
    return "t2" if turno == "t2" else "t1"


def _padronizar_cenario(cenario):
    """Converte rótulos de voto estimulado para o único formato da planilha.

    O Gemini pode variar entre 'Cenário 01', 'Estimulada 1' e 'Estimulada -
    Cenário 1'. Uma estimulada sem número é, por definição, a única/primeira
    medição do cargo e turno, então também deve ser Cenário 1. Confrontos
    conhecidos também ganham nomes completos e partido.
    """
    texto = str(cenario or "").strip()
    normalizado = _sem_acento(texto).lower()
    # Com ou sem os partidos no PDF, os confrontos conhecidos usam exatamente o
    # mesmo rótulo. Isso evita que uma mesma disputa apareça misturada entre
    # "Lula x Michelle" e "Lula (PT) x Michelle Bolsonaro (PL)".
    confronto = re.sub(r"\([^)]*\)", "", normalizado)
    confronto = " ".join(confronto.split())
    confrontos = {
        "lula x flavio": "Lula (PT) x Flávio Bolsonaro (PL)",
        "lula x flavio bolsonaro": "Lula (PT) x Flávio Bolsonaro (PL)",
        "lula x michelle": "Lula (PT) x Michelle Bolsonaro (PL)",
        "lula x michelle bolsonaro": "Lula (PT) x Michelle Bolsonaro (PL)",
        "sergio moro x sandro alex": "Sergio Moro (UNIAO) x Sandro Alex (PSD)",
    }
    if confronto in confrontos:
        return confrontos[confronto]
    numero = re.search(r"\bcenario\s*0*(\d+)\b", normalizado)
    if not numero:
        numero = re.search(r"\bestimulad[ao]\s*[-:]?\s*0*(\d+)\b", normalizado)
    if numero:
        return f"Cenário {int(numero.group(1))}"
    if normalizado in {"estimulada", "estimulado", "voto estimulado", "votacao estimulada"}:
        return "Cenário 1"
    return texto


def _texto_limpo(valor):
    """Remove somente variações tipográficas, sem alterar o significado."""
    return " ".join(str(valor or "").strip().split())


def _chave_padronizacao(valor):
    texto = _sem_acento(_texto_limpo(valor)).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", texto).split())


def _padronizar_candidato(candidato):
    """Uniformiza grafias já conhecidas de candidatos e rótulos de rejeição."""
    texto = _texto_limpo(candidato)
    rotulos = {
        "cintia dias psol": "Cíntia Dias (PSOL)",
        "nao sabe nao opinou": "Não sabe/Não opinou",
        "nao sabe nao respondeu": "Não sabe/Não respondeu",
        "poderia votar em todos": "Poderia votar em todos",
        "branco nulo": "Branco/Nulo",
        "indeciso n nao resp": "Indeciso/N/Não resp.",
    }
    return rotulos.get(_chave_padronizacao(texto), texto)


def _padronizar_segmento(segmento):
    """Aplica os rótulos demográficos canônicos usados nas abas de saída."""
    texto = _texto_limpo(segmento)
    chave = _chave_padronizacao(texto)
    rotulos = {
        "feminino": "Feminino",
        "masculino": "Masculino",
        "catolico": "Católico",
        "evangelico": "Evangélico",
        "superior completo": "Superior completo",
        "nordeste": "Nordeste",
        "norte": "Norte",
        "sudeste": "Sudeste",
        "sul": "Sul",
        "centro oeste": "Centro-Oeste",
        "ate 02 salarios minimos": "Até 02 salários mínimos",
    }
    if chave in rotulos:
        return rotulos[chave]

    # A mesma faixa etária já apareceu como "16-24", "16 - 24" e
    # "De 16 a 24 anos". Sempre grava na última forma, explícita e legível.
    faixa = re.fullmatch(r"(?:de )?(\d{1,2})(?: a | )(\d{1,2})(?: anos?)?", chave)
    if faixa:
        return f"De {int(faixa.group(1))} a {int(faixa.group(2))} anos"

    idade_unica = re.fullmatch(r"(\d{1,2}) anos?", chave)
    if idade_unica:
        return f"{int(idade_unica.group(1))} anos"
    return texto


def _padronizar_resposta(resposta):
    """Normaliza apenas rótulos equivalentes de aprovação."""
    texto = _texto_limpo(resposta)
    rotulos = {
        "aprova": "Aprova",
        "desaprova": "Desaprova",
        "ns nr": "NS/NR",
        "nao sabe nao opinou": "Não sabe/Não opinou",
    }
    return rotulos.get(_chave_padronizacao(texto), texto)


def _remover_subtotais_avaliacao(itens):
    """Remove totais derivados quando as categorias componentes já foram extraídas.

    Alguns gráficos exibem, por exemplo, Ótima + Boa e também o número em
    destaque Ótima/Boa. O destaque não é uma nova resposta; gravá-lo junto às
    componentes duplica a pergunta e faz a soma passar de 100%. Quando o
    gráfico publica SOMENTE a categoria combinada, ela é mantida.
    """
    componentes_por_subtotal = {
        "otima boa": {"otima", "boa"},
        "otimo bom": {"otimo", "bom"},
        "ruim pessima": {"ruim", "pessima"},
        "ruim pessimo": {"ruim", "pessimo"},
    }
    grupos = {}
    for idx, item in enumerate(itens):
        chave = (
            _texto_limpo(item.get("alvo", "")),
            _texto_limpo(item.get("tipo_avaliacao", "")).lower(),
            _texto_limpo(item.get("tipo_segmento", "")).lower(),
            _texto_limpo(item.get("segmento", "")),
        )
        grupos.setdefault(chave, []).append((idx, _chave_padronizacao(item.get("resposta", ""))))

    descartar = set()
    for respostas in grupos.values():
        presentes = {resposta for _, resposta in respostas}
        for idx, resposta in respostas:
            componentes = componentes_por_subtotal.get(resposta)
            if componentes and componentes.issubset(presentes):
                descartar.add(idx)
    return [item for idx, item in enumerate(itens) if idx not in descartar]


def _padronizar_dados_extraidos(dados):
    """Normaliza antes da deduplicação e de qualquer escrita nas abas."""
    for item in dados.get("voto_segmento", []):
        item["cargo"] = _cargo_norm(item.get("cargo", ""))
        item["turno"] = _turno_segmento(item)
        item["cenario"] = _padronizar_cenario(item.get("cenario", ""))
        item["candidato"] = _padronizar_candidato(item.get("candidato", ""))
        item["tipo_segmento"] = _sem_acento(_texto_limpo(item.get("tipo_segmento", ""))).lower()
        item["segmento"] = _padronizar_segmento(item.get("segmento", ""))
    for item in dados.get("rejeicao", []):
        item["cargo"] = _cargo_norm(item.get("cargo", ""))
        item["candidato"] = _padronizar_candidato(item.get("candidato", ""))
        item["tipo_segmento"] = _sem_acento(_texto_limpo(item.get("tipo_segmento", ""))).lower()
        item["segmento"] = _padronizar_segmento(item.get("segmento", ""))
    for item in dados.get("aprovacao", []):
        item["tipo_avaliacao"] = _texto_limpo(item.get("tipo_avaliacao", "")).lower()
        item["resposta"] = _padronizar_resposta(item.get("resposta", ""))
        item["tipo_segmento"] = _sem_acento(_texto_limpo(item.get("tipo_segmento", ""))).lower()
        item["segmento"] = _padronizar_segmento(item.get("segmento", ""))
    return dados


LIMITE_BYTES_BLOCO = 15_000_000   # a API do Gemini rejeita requisição inline grande demais


def _blocos_pdf(pdf_bytes, tamanho=PAGINAS_POR_BLOCO):
    """Fatia o PDF em blocos de páginas. Cada página é um slide autocontido,
    então a tabela nunca se parte entre blocos. Reduz o tamanho do bloco quando o PDF
    tem poucas páginas mas é gigante em bytes (scan em resolução muito alta, ex.: 17
    páginas / 85MB): um bloco de 5 páginas nesse caso ainda estoura o limite de
    requisição inline do Gemini (400 INVALID_ARGUMENT), mesmo já sendo "só um bloco"."""
    import io
    from pypdf import PdfReader, PdfWriter
    reader = PdfReader(io.BytesIO(pdf_bytes))
    n = len(reader.pages)
    if n:
        bytes_por_pagina = len(pdf_bytes) / n
        if bytes_por_pagina > 0:
            tamanho = max(1, min(tamanho, int(LIMITE_BYTES_BLOCO // bytes_por_pagina)))
    for ini in range(0, n, tamanho):
        writer = PdfWriter()
        for i in range(ini, min(ini + tamanho, n)):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        yield buf.getvalue()


# Uso acumulado de tokens do Gemini nesta execução (processo novo a cada rodada do
# workflow, então não precisa resetar entre chamadas de cmd_extrair).
USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}


def _registrar_uso(resp):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    USO_TOKENS["chamadas"] += 1
    USO_TOKENS["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    USO_TOKENS["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    USO_TOKENS["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _custo_estimado(entrada, saida, pensamento):
    # preço aproximado da faixa "flash" (~$0,30/1M tokens de entrada, ~$2,50/1M de
    # saída, saída e pensamento cobram na mesma tabela). Ajuste se trocar de modelo
    # (GEMINI_MODEL) ou se o preço mudar. Estimativa, não fatura oficial; confira o
    # console de billing do Google pro valor exato.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _resumo_uso_tokens(rotulo, uso):
    if not uso["chamadas"]:
        return
    custo = _custo_estimado(uso["entrada"], uso["saida"], uso["pensamento"])
    print(f"\nGemini ({rotulo}): {uso['chamadas']} chamada(s) · "
          f"{uso['entrada']:,} tokens entrada · {uso['saida']:,} saída · "
          f"{uso['pensamento']:,} pensamento · custo estimado ${custo:.4f}")


def _gemini_json(pdf_bytes, extra="", texto_bloco=""):
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0,
        max_output_tokens=65536,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )
    prompt = PROMPT + (f"\n\n{extra}" if extra else "")
    texto_bloco = (texto_bloco or "").strip()
    # Sempre manda o PDF junto, não só quando o texto for "insuficiente" por tamanho:
    # relatório com dado só em gráfico tem texto extraído comprido (rodapé legal
    # repetido em toda página) mas sem nenhum candidato/percentual nele, e tabela
    # cruzada (região x segmento) vira uma sequência linear de números no texto, sem
    # estrutura de linha/coluna, que o modelo pode desalinhar. O visual resolve os dois.
    if texto_bloco:
        contents = [
            prompt + "\n\nTEXTO EXTRAÍDO DO PDF/PÁGINA:\n" + texto_bloco +
            "\n\nPDF ANEXO: confira o visual (tabelas e gráficos). O texto extraído pode não "
            "conter os números (dado só em gráfico) ou desalinhar tabela cruzada (linha x "
            "coluna); nesses casos, confie no PDF, não no texto.",
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        ]
    else:
        contents = [prompt, types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")]
    resp = client.models.generate_content(model=GEMINI_MODEL, contents=contents, config=config)
    _registrar_uso(resp)
    raw = (getattr(resp, "text", "") or "").strip()
    if not raw:
        fr = "?"
        cands = getattr(resp, "candidates", None)
        if cands:
            fr = getattr(cands[0], "finish_reason", "?")
        raise RuntimeError(f"resposta vazia ou truncada do Gemini (finish_reason={fr})")
    return _extrair_json_objeto(raw)


def _dedup(itens, chaves):
    vistos, saida = set(), []
    for it in itens:
        k = tuple(str(it.get(c, "")).strip() for c in chaves)
        if k not in vistos:
            vistos.add(k)
            saida.append(it)
    return saida


def extrair_do_pdf(pdf_bytes, extra=""):
    """Extrai bloco a bloco (para caber no limite de tokens e melhorar a
    precisão) e junta os resultados, removendo duplicatas entre blocos."""
    voto, rej, aprov = [], [], []
    for bloco in _blocos_pdf(pdf_bytes):
        texto_bloco = _texto_pdf_bytes(bloco)
        dados = _padronizar_dados_extraidos(_gemini_json(bloco, extra, texto_bloco=texto_bloco))
        voto += dados.get("voto_segmento", [])
        rej += dados.get("rejeicao", [])
        aprov += dados.get("aprovacao", [])
    aprov = _remover_subtotais_avaliacao(aprov)
    voto = _dedup(voto, ["cargo", "turno", "cenario", "candidato", "tipo_segmento", "segmento"])
    rej = _dedup(rej, ["cargo", "candidato", "tipo_segmento", "segmento"])
    aprov = _dedup(aprov, ["alvo", "tipo_avaliacao", "resposta", "tipo_segmento", "segmento"])
    return {"voto_segmento": voto, "rejeicao": rej, "aprovacao": aprov}


def cmd_extrair():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    from relatorios_topline_core import _referencia, ficha_instituto, instituto_canonico, sigla_uf

    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = _aba(sh, "relatorios", manutencao=False)
    ws_voto = _aba(sh, "voto_segmento")
    ws_rej = _aba(sh, "rejeicao")
    ws_aprov = _aba(sh, "aprovacao")

    header = fila.row_values(1)
    col_err = _garantir_coluna_relatorios(fila, header, "segmentos_erro")
    col_ten = _garantir_coluna_relatorios(fila, header, "segmentos_tentativas")

    ci_ext = _garantir_coluna_relatorios(fila, header, "segmentos_extraido")
    ci_data = _garantir_coluna_relatorios(fila, header, "segmentos_data_extracao")
    _colorir_por_valor(fila, _rel_display("segmentos_extraido"), header, _ultima_linha_com_registro(fila), {
        "sim": (0.82, 0.93, 0.82),
        "não": (0.96, 0.80, 0.80),
    })

    linhas = _rel_records(fila)
    ok_regs, err_regs = [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")

    print(f"Extrair segmentos: {len(linhas)} linha(s) na fila.", flush=True)

    def _bloco_canonico(uf, cargo_fila):
        """Lista canônica do cargo da linha; a fila agora é registro+cargo."""
        partes = []
        cargos = _cargos_monitorados(cargo_fila) or [cargo_fila]
        partes.append(f"FOCO DE CARGO: extraia APENAS {', '.join(cargos)}.")
        for cargo in cargos:
            cands, _ = _referencia(cargo, uf)
            if cands:
                partes.append(f"CANDIDATOS CANÔNICOS ({cargo} {sigla_uf(uf)}):\n" + "\n".join(cands))
        return "\n\n".join(partes)

    def _item_casa_cargo(item, cargo_fila):
        cargo_item = _cargo_norm(item.get("cargo", ""))
        return not cargo_item or cargo_item == _cargo_norm(cargo_fila)

    # voto_segmento é só quebra DEMOGRÁFICA. Cenário geral/total, turno, pergunta filtro
    # etc. já vão pro topline; se entrarem aqui também, duplicam o dado e inflam a aba.
    # Filtro no código, não só no prompt, porque o Gemini às vezes desobedece a regra.
    TIPO_SEGMENTO_FORA = {"geral", "total", "cenario", "cenário", "turno", "pergunta", "voto"}
    SEGMENTO_FORA = {"total", "geral"}

    def _e_segmento_demografico(item):
        tipo = _sem_acento(item.get("tipo_segmento", "")).strip().lower()
        seg = _sem_acento(item.get("segmento", "")).strip().lower()
        if tipo in TIPO_SEGMENTO_FORA or seg in SEGMENTO_FORA:
            return False
        return True

    # Chaves já gravadas em cada aba, carregadas UMA vez pra dedup em memória (sem
    # reler a aba a cada gravação). Protege contra duplicata quando um lote foi
    # gravado mas a linha não chegou a ser marcada (ex.: timeout no meio).
    CH_VOTO = ["registro", "cargo", "turno", "cenario", "candidato", "tipo_segmento", "segmento"]
    CH_REJ = ["registro", "cargo", "candidato", "tipo_segmento", "segmento"]
    CH_APROV = ["registro", "alvo", "tipo_avaliacao", "resposta", "tipo_segmento", "segmento"]

    def _carregar_chaves(ws, aba_nome, chaves):
        return {tuple(str(e.get(c, "")).strip() for c in chaves) for e in ws.get_all_records()}

    voto_keys = _carregar_chaves(ws_voto, "voto_segmento", CH_VOTO)
    rej_keys = _carregar_chaves(ws_rej, "rejeicao", CH_REJ)
    aprov_keys = _carregar_chaves(ws_aprov, "aprovacao", CH_APROV)

    # Grava em lotes ao longo da rodada: se o passo estourar o tempo (timeout do
    # Actions), o que já foi extraído fica salvo e a marcação segue junto, então a
    # próxima rodada continua de onde parou em vez de perder tudo e reprocessar o
    # mesmo backlog eternamente.
    LOTE = 5
    voto_buf, rej_buf, aprov_buf, updates = [], [], [], []
    _contador = {"n": 0}

    def _flush(ctx=""):
        if voto_buf:
            _append_rows_compacto(ws_voto, voto_buf)
        if rej_buf:
            _append_rows_compacto(ws_rej, rej_buf)
        if aprov_buf:
            _append_rows_compacto(ws_aprov, aprov_buf)
        if updates:   # marca as linhas extraídas DEPOIS de gravar os dados delas
            fila.update_cells(updates, value_input_option="RAW")
        if voto_buf or rej_buf or aprov_buf or updates:
            print(f"  [gravado{(' ' + ctx) if ctx else ''}] "
                  f"{len(voto_buf)} voto, {len(rej_buf)} rejeição, {len(aprov_buf)} aprovação", flush=True)
        voto_buf.clear(); rej_buf.clear(); aprov_buf.clear(); updates.clear()
        _contador["n"] = 0

    for i, r in enumerate(linhas, start=2):   # linha 1 = cabeçalho
        link = str(r.get("link", "")).strip()
        if not link or not _verdadeiro(r.get("conferido")) or _verdadeiro(r.get("segmentos_extraido")):
            continue
        tentativas = _int0(r.get("segmentos_tentativas"))
        if tentativas >= 3:   # desiste após 3 falhas; limpe a coluna pra tentar de novo
            continue
        try:
            print(f"linha {i} ({r.get('registro')} / {r.get('cargo')}): baixando PDF para segmentos...", flush=True)
            pdf = _baixar_pdf(link)
            erro_registro = _validar_registro_pdf(pdf, r.get("registro", ""))
            if erro_registro:
                raise RuntimeError(erro_registro)
            print(f"linha {i} ({r.get('registro')} / {r.get('cargo')}): PDF baixado ({len(pdf)} bytes), enviando ao Gemini...", flush=True)
            extra = ficha_instituto(r.get("instituto", "")) + _bloco_canonico(r.get("uf"), r.get("cargo"))
            dados = extrair_do_pdf(pdf, extra=extra)
        except Exception as e:
            msg = str(e)
            updates.extend([gspread.Cell(i, col_err, msg[:300]),
                            gspread.Cell(i, col_ten, tentativas + 1)])
            err_regs.append(f"{r.get('registro')} [{msg[:80]}]")
            print(f"linha {i} ({r.get('registro')}): erro {msg}")
            _contador["n"] += 1
            if _contador["n"] >= LOTE:
                _flush("parcial")
            continue
        registro = r.get("registro", "")
        cargo_fila = r.get("cargo", "")
        uf = sigla_uf(r.get("uf", ""))
        inst = instituto_canonico(r.get("instituto", ""))
        data_div = _data_iso(r.get("data_divulgacao", ""))
        voto_filtrado = [v for v in dados.get("voto_segmento", [])
                         if _item_casa_cargo(v, cargo_fila) and _e_segmento_demografico(v)]
        rej_filtrada = [v for v in dados.get("rejeicao", []) if _item_casa_cargo(v, cargo_fila)]
        aprov_filtrada = dados.get("aprovacao", []) if _cargo_norm(cargo_fila) in ("presidente", "governador") else []
        erros_valor = []
        erros_valor += _normalizar_percentuais_lista(voto_filtrado, "voto_segmento")
        erros_valor += _normalizar_percentuais_lista(rej_filtrada, "rejeicao")
        erros_valor += _normalizar_percentuais_lista(aprov_filtrada, "aprovacao")
        if erros_valor:
            msg = "; ".join(erros_valor[:4])
            updates.extend([gspread.Cell(i, col_err, msg[:300]),
                            gspread.Cell(i, col_ten, tentativas + 1)])
            err_regs.append(f"{registro} {cargo_fila} [{msg[:80]}]")
            print(f"linha {i} ({registro} / {cargo_fila}): valores inválidos: {msg}", flush=True)
            _contador["n"] += 1
            if _contador["n"] >= LOTE:
                _flush("parcial")
            continue
        if not (voto_filtrado or rej_filtrada or aprov_filtrada):
            # Sem dado de segmento pode ser NORMAL: relatório que só traz "RESULTADO GERAL"
            # (sem quebra demográfica) não tem o que extrair aqui, o número geral vai no
            # topline. Só é erro de verdade se o PDF TEM quebra demográfica PRO CARGO DA
            # LINHA e mesmo assim não veio nada. Distingue procurando termo demográfico
            # no texto do PDF, mas só na seção do cargo pedido: um relatório multi-cargo
            # pode ter quebra pra Governador e não pra Senador, e olhar o PDF inteiro
            # faria a checagem "ver" a quebra do Governador e gerar erro falso pro
            # Senador (aconteceu com RO-07927: IHPEC tem quebra só pra Governador).
            from relatorios_topline_core import extrair_texto_pdf_bytes
            if _n_paginas_pdf(pdf) <= 10:
                try:
                    txt_pdf = extrair_texto_pdf_bytes(pdf).lower()
                except Exception:
                    txt_pdf = ""
            else:
                txt_pdf = " ".join(t for _, t in _blocos_ativos_cargo(pdf, _cargo_norm(cargo_fila))).lower()
            # Exige 2+ termos demográticos DIFERENTES: um único hit isolado costuma ser
            # falso positivo (ex.: matéria de portal de notícia com link/manchete não
            # relacionada mencionando "feminino" de passagem, sem tabela de quebra
            # nenhuma no relatório em si).
            termos_batidos = sum(1 for w in (
                "masculino", "feminino", "evangélic", "evangelic", "católic", "catolic",
                "renda familiar", "faixa etária", "faixa etaria", "escolaridade",
                "por região", "por regiao") if w in txt_pdf)
            tem_quebra = termos_batidos >= 2
            if not tem_quebra:
                updates.extend([gspread.Cell(i, col_err, "sem quebra por segmento (só resultado geral; topline cobre)"),
                                gspread.Cell(i, ci_ext, "não"), gspread.Cell(i, ci_data, agora)])
                print(f"linha {i} ({registro} / {cargo_fila}): sem quebra por segmento, marcado como não", flush=True)
            else:
                msg = f"nenhum dado encontrado para o cargo da linha ({cargo_fila})"
                updates.extend([gspread.Cell(i, col_err, msg), gspread.Cell(i, col_ten, tentativas + 1)])
                err_regs.append(f"{registro} {cargo_fila} [{msg}]")
                print(f"linha {i} ({registro} / {cargo_fila}): {msg}", flush=True)
            _contador["n"] += 1
            if _contador["n"] >= LOTE:
                _flush("parcial")
            continue
        for v in voto_filtrado:
            # cargo/turno da disputa daquele cenário (o Gemini identifica); se faltar,
            # cai no texto da fila, pra linha nunca ficar sem referência
            cargo_item = _cargo_norm(v.get("cargo") or cargo_fila)
            turno = _turno_segmento(v)
            cenario = _padronizar_cenario(v.get("cenario", ""))
            chave = (str(registro).strip(), cargo_item,
                     turno, cenario,
                     str(v.get("candidato", "")).strip(), str(v.get("tipo_segmento", "")).strip(),
                     str(v.get("segmento", "")).strip())
            if chave in voto_keys:
                continue
            voto_keys.add(chave)
            voto_buf.append([registro, cargo_item, turno,
                             uf, inst, data_div,
                             cenario, v.get("candidato", ""),
                             v.get("tipo_segmento", ""), v.get("segmento", ""),
                             v.get("valor", "")])
        for v in rej_filtrada:
            cargo_item = _cargo_norm(v.get("cargo") or cargo_fila)
            chave = (str(registro).strip(), cargo_item,
                     str(v.get("candidato", "")).strip(), str(v.get("tipo_segmento", "")).strip(),
                     str(v.get("segmento", "")).strip())
            if chave in rej_keys:
                continue
            rej_keys.add(chave)
            rej_buf.append([registro, cargo_item, uf, inst, data_div,
                            v.get("candidato", ""), v.get("tipo_segmento", ""),
                            v.get("segmento", ""), v.get("valor", "")])
        for v in aprov_filtrada:
            tipo_aval = str(v.get("tipo_avaliacao", "")).strip()
            chave = (str(registro).strip(), str(v.get("alvo", "")).strip(), tipo_aval,
                     str(v.get("resposta", "")).strip(), str(v.get("tipo_segmento", "")).strip(),
                     str(v.get("segmento", "")).strip())
            if chave in aprov_keys:
                continue
            aprov_keys.add(chave)
            aprov_buf.append([registro, _cargo_norm(cargo_fila), uf, inst, data_div,
                              v.get("alvo", ""), tipo_aval, v.get("resposta", ""),
                              v.get("tipo_segmento", ""), v.get("segmento", ""),
                              v.get("valor", "")])
        # aviso de soma (não bloqueia): fica na coluna de erro como alerta pra conferência
        avisos_soma = _avisos_soma_voto(voto_filtrado, cargo_fila) + _avisos_soma_aprovacao(aprov_filtrada)
        nota_soma = ("conferir: " + "; ".join(avisos_soma[:4])) if avisos_soma else ""
        # marca a linha como extraída no MESMO lote em que os dados dela vão (progresso durável)
        updates.extend([gspread.Cell(i, col_err, nota_soma[:300]),
                        gspread.Cell(i, ci_ext, "sim"),
                        gspread.Cell(i, ci_data, agora)])
        ok_regs.append(registro)
        aviso_txt = f"  [!] {nota_soma}" if nota_soma else ""
        print(f"linha {i} ({registro} / {cargo_fila}): "
              f"{len(voto_filtrado)} voto, "
              f"{len(rej_filtrada)} rejeição, "
              f"{len(aprov_filtrada)} aprovação{aviso_txt}", flush=True)
        _contador["n"] += 1
        if _contador["n"] >= LOTE:
            _flush("parcial")

    _flush("fim")

    print("\n───────── resumo ─────────")
    print(f"extraídos: {len(ok_regs)}  {ok_regs}")
    print(f"com erro:  {len(err_regs)}  {err_regs}")
    _resumo_uso_tokens("segmentos", USO_TOKENS)


# ────────────────────────────── TOPLINE ──────────────────────────────
# Versão em lote do painel Polling Manual: lê os PDFs da fila e grava o voto
# estimulado (topline) na planilha do PollingData, no mesmo formato do
# `resultados`, tagueado manual (reconcilia com o oficial quando a assinatura sair).

POLLING_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA", "")
FLAG_TOPLINE = "topline_extraido"
CARGOS_POLLING = {"presidente", "governador", "senador"}

# palavras que indicam o cargo no corpo do texto (matérias de site costumam falar
# "vaga ao Senado"/"disputa pelo Senado" em vez de "senador" literalmente)
PALAVRAS_CARGO = {
    "presidente": ["presiden"],
    "governador": ["governad", "governo do estado"],
    "senador": ["senador", "senado"],
}


def _blocos_ativos_cargo(pdf, cargo):
    """Gera (bloco_bytes, texto_bloco) só dos blocos de 5 páginas que pertencem à
    seção do cargo pedido. Um PDF grande organiza os cargos em seções contínuas, mas
    nem toda página de uma seção repete a palavra do cargo (tabela/gráfico sem
    legenda, rodapé de página sem relação, bloco intermediário sem título). Fica ativo
    a partir do bloco onde o cargo pedido aparece, e continua ativo enquanto os blocos
    seguintes não mencionarem OUTRO cargo monitorado; só desativa quando um outro
    cargo assume claramente a seção. Bloco inicial começa ativo (cobre PDF que já abre
    no cargo certo)."""
    from relatorios_topline_core import extrair_texto_pdf_bytes
    palavras_alvo = PALAVRAS_CARGO.get(str(cargo or "").lower(), [str(cargo or "").lower()])
    outros_cargos = [p for c, ps in PALAVRAS_CARGO.items() if c != cargo for p in ps]
    ativo = True
    for bloco in _blocos_pdf(pdf, tamanho=5):
        try:
            txt_bloco = extrair_texto_pdf_bytes(bloco)
        except Exception:
            txt_bloco = ""
        low_bloco = txt_bloco.lower()
        if any(p in low_bloco for p in palavras_alvo):
            ativo = True
        elif any(p in low_bloco for p in outros_cargos):
            ativo = False
        # bloco sem nenhuma palavra de cargo (imagem, rodapé, texto neutro): mantém o
        # estado do bloco anterior, não desativa nem ativa.
        if ativo:
            yield bloco, txt_bloco


def _cargos_da_linha(valor):
    return [_cargo_norm(c) for c in _cargos_monitorados(valor)]


def _cargos_presentes(texto, cargo_fila):
    """Cargos a extrair.

    Com a fila separada por registro+cargo, respeita o cargo da linha. Só detecta
    pelo texto quando a célula de cargo estiver vazia.
    """
    cargos_linha = _cargos_da_linha(cargo_fila)
    if cargos_linha:
        return cargos_linha
    cargos = set()
    low = (texto or "").lower()
    if not low:
        return ["presidente", "governador", "senador"]
    if "presiden" in low:
        cargos.add("presidente")
    if "governador" in low or "governo do estado" in low:
        cargos.add("governador")
    if "senador" in low:
        cargos.add("senador")
    return [c for c in ("presidente", "governador", "senador") if c in cargos]


def _n_paginas_pdf(pdf_bytes):
    try:
        import io
        from pypdf import PdfReader
        return len(PdfReader(io.BytesIO(pdf_bytes)).pages)
    except Exception:
        return 0


def _extrair_topline_pdf(pdf, texto, link, escopo, cargo):
    """Extrai o topline de um cargo/turno. PDF pequeno: manda inteiro (comportamento
    de sempre). PDF grande: lê em blocos de 5 páginas e junta os cenários, senão o
    modelo perde o cargo que está no fim do PDF (ex.: VOX senador na pág 49, Meio/Ideia
    92 páginas). Pré-filtra blocos pelo texto do cabeçalho pra economizar chamada."""
    from relatorios_topline_core import classificar_tipo_resultado, extrair_dados_polling_gemini
    if _n_paginas_pdf(pdf) <= 10:
        return extrair_dados_polling_gemini(texto, url_original=link, escopo=escopo, pdf_bytes=pdf)

    def _mapa_candidatos(c):
        """dict candidato_normalizado -> percentual, só candidatos de verdade com número.
        'Não válido' fica fora: é o item mais instável entre duas leituras do mesmo
        cenário (some, muda de posição), e incluí-lo faria comparações de conteúdo
        falharem em cenários claramente idênticos nos candidatos."""
        mapa = {}
        for it in (c.get("itens") or []):
            if classificar_tipo_resultado(str(it.get("candidato", "")), it.get("tipo", "")) == "nao_valido":
                continue
            if it.get("percentual") is None:
                continue
            mapa[str(it.get("candidato", "")).strip().lower()] = it.get("percentual")
        return mapa

    payload_final, cenarios, vistos = None, [], set()
    mapas_aceitos = []   # paralelo a 'cenarios': mapa candidato->pct de cada cenário aceito
    blocos_falhos = 0
    for bloco, txt_bloco in _blocos_ativos_cargo(pdf, cargo):
        try:
            p = extrair_dados_polling_gemini(txt_bloco, url_original=link, escopo=escopo, pdf_bytes=bloco)
        except Exception as e:
            # falha de UM bloco não pode ser invisível: os cenários daquelas páginas
            # somem e a linha ainda seria marcada como sucesso (ex.: BR-05628 perdeu
            # os confrontos Lula x Renan e Lula x Joaquim das págs. 42/44 sem nenhum
            # rastro no log). Conta e propaga pro aviso da linha.
            blocos_falhos += 1
            print(f"    [bloco falhou] {e}", flush=True)
            continue
        for c in (p.get("cenarios") or []):
            mapa = _mapa_candidatos(c)
            # cenário sem nenhum candidato com percentual numérico é lixo: ou é o
            # placeholder vazio que normalizar_payload_polling injeta quando um bloco
            # não tem dado, ou é gráfico de que o modelo só leu os nomes (sem números).
            # Se passasse, viraria linha órfã em topline_pesquisas sem resultado nenhum.
            if not mapa:
                continue
            chave = f"{c.get('scenario_label','')}|{c.get('descricao','')}|{c.get('disputa','')}"
            # blocos diferentes às vezes capturam o MESMO cenário do PDF com rótulo
            # diferente ("1º CENÁRIO - COM FULANO" num bloco, "Cenário 1 (com Fulano)"
            # no seguinte), e páginas de síntese/destaque repetem só os líderes do
            # cenário (ex.: capa de capítulo "36% × 36%"). Dedup em dois níveis:
            # conteúdo idêntico (fingerprint) e SUBCONJUNTO de um cenário já aceito
            # (mesmos candidatos com os mesmos números, só que menos candidatos =
            # fragmento/resumo do mesmo cenário, não um cenário novo).
            fingerprint = tuple(sorted(mapa.items())) if len(mapa) >= 2 else ()
            if chave in vistos or (fingerprint and fingerprint in vistos):
                continue
            eh_subconjunto = any(
                mapa.keys() <= m.keys() and all(m[k] == v for k, v in mapa.items())
                for m in mapas_aceitos)
            if eh_subconjunto:
                continue
            # caso inverso: o cenário NOVO é a versão completa de um fragmento aceito
            # antes (o resumo veio num bloco anterior à tabela cheia). Substitui.
            for idx, m in enumerate(mapas_aceitos):
                if m.keys() <= mapa.keys() and all(mapa[k] == v for k, v in m.items()):
                    cenarios[idx] = c
                    mapas_aceitos[idx] = mapa
                    break
            else:
                cenarios.append(c)
                mapas_aceitos.append(mapa)
            vistos.add(chave)
            if fingerprint:
                vistos.add(fingerprint)
            if payload_final is None:
                payload_final = p   # metadados (uf, instituto, data) do 1º bloco com dado
    if payload_final is None:
        return {"cenarios": [], "blocos_falhos": blocos_falhos}
    payload_final["cenarios"] = cenarios
    payload_final["blocos_falhos"] = blocos_falhos
    return payload_final


def cmd_topline():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    import pandas as pd
    from relatorios_topline_core import (
        USO_TOKENS as USO_TOKENS_TOPLINE, extrair_dados_polling_gemini,
        extrair_texto_pdf_bytes, montar_dataframes_polling, ficha_instituto,
        resolver_data_campo_deterministica,
    )

    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = _aba(sh, "relatorios", manutencao=False)

    header = fila.row_values(1)
    col_flag = _garantir_coluna_relatorios(fila, header, FLAG_TOPLINE)
    col_data = _garantir_coluna_relatorios(fila, header, "topline_data_extracao")
    col_erro = _garantir_coluna_relatorios(fila, header, "topline_erro")
    col_tent = _garantir_coluna_relatorios(fila, header, "topline_tentativas")
    _colorir_por_valor(fila, _rel_display("topline_extraido"), header, _ultima_linha_com_registro(fila), {
        "sim": (0.82, 0.93, 0.82),
        STATUS_TOPLINE_MANUAL: (1.0, 0.82, 0.68),
    })

    linhas = _rel_records(fila)
    todos_p, todos_r = [], []
    updates = []
    ok_regs, err_regs = [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")
    print(f"Topline: {len(linhas)} linha(s) na fila.", flush=True)

    LOTE = 5
    _contador = {"n": 0}

    def _gravar(dfs, aba_nome, chaves):
        if not dfs:
            return
        cols = CABECALHOS[aba_nome]
        df = pd.concat(dfs, ignore_index=True).reindex(columns=cols).fillna("")
        # dedup DENTRO do próprio lote: um PDF grande, lido em blocos, pode devolver o
        # mesmo cenário (mesmo scenario_id) mais de uma vez vindo de blocos diferentes
        # (ex.: t2 do mesmo confronto aparece em duas seções do relatório). Sem isso, os
        # dois vão pra planilha e a soma do cenário dobra (ex.: ~200% em vez de ~100%).
        antes = len(df)
        df = df.drop_duplicates(subset=chaves, keep="first")
        if len(df) < antes:
            print(f"  [dedup {aba_nome} intra-lote] {antes - len(df)} linha(s) duplicada(s) no mesmo lote")
        ws = _aba(sh, aba_nome)
        # dedup contra o que já está na aba (protege reprocessamento com staging não apagado)
        existentes = {tuple(str(e.get(c, "")).strip() for c in chaves)
                      for e in ws.get_all_records()}
        if existentes:
            mask = df.apply(lambda rw: tuple(str(rw[c]).strip() for c in chaves) not in existentes,
                            axis=1)
            if (~mask).any():
                print(f"  [dedup {aba_nome}] {int((~mask).sum())} linha(s) já existiam")
            df = df[mask]
        if df.empty:
            return
        _append_rows_compacto(ws, df.astype(str).values.tolist())
        if aba_nome == "topline_pesquisas":
            _ativar_checkbox(ws, "liberado", cols, _ultima_linha_com_dados(ws))
        print(f"{len(df)} linha(s) gravadas na aba '{aba_nome}'.")

    # Grava em lotes: se o passo estourar o tempo (timeout do Actions), o que já foi
    # extraído fica salvo e a marcação segue junto, então a próxima rodada continua de
    # onde parou em vez de perder tudo. Marca a linha DEPOIS de gravar os cenários dela.
    def _flush_topline(ctx=""):
        _gravar(todos_p, "topline_pesquisas", ["scenario_id"])
        _gravar(todos_r, "topline_resultados", ["scenario_id", "candidato_partido", "tipo"])
        if updates:
            # O aviso de lançamento manual é uma fórmula HYPERLINK; USER_ENTERED
            # preserva o link sem mudar os demais textos e datas do lote.
            fila.update_cells(updates, value_input_option="USER_ENTERED")
        todos_p.clear(); todos_r.clear(); updates.clear()
        _contador["n"] = 0

    def _falha(row_i, registro, tentativas, msg):
        updates.extend([gspread.Cell(row_i, col_erro, msg[:300]),
                        gspread.Cell(row_i, col_tent, tentativas + 1)])
        err_regs.append(f"{registro} [{msg[:80]}]")
        print(f"linha {row_i} ({registro}): erro: {msg}")
        _contador["n"] += 1

    for i, r in enumerate(linhas, start=2):
        if _contador["n"] >= LOTE:
            _flush_topline("parcial")
        link = str(r.get("link", "")).strip()
        registro_fila = str(r.get("registro", "")).strip()
        if (not link or not _verdadeiro(r.get("conferido"))
                or _verdadeiro(r.get(FLAG_TOPLINE))
                or str(r.get(FLAG_TOPLINE, "")).strip() == STATUS_TOPLINE_MANUAL):
            continue
        # Topline automático só p/ RELATÓRIO de instituto COM FICHA. Notícia vai
        # para o Polling Manual; N/A/em branco significa que não foi localizado
        # relatório e não entra em nenhuma fila. Segmento/rejeição/aprovação NÃO
        # têm gate (rodam em qualquer PDF conferido, no cmd_extrair).
        tipo = _sem_acento(r.get("tipo_fonte")).strip().lower()
        if not tipo.startswith("relat"):
            if tipo.startswith("not"):
                # Notícia conferida: há números disponíveis, mas sem a ficha de
                # relatório necessária para extração automática confiável.
                updates.extend([
                    gspread.Cell(i, col_flag, _link_status_topline_manual()),
                    gspread.Cell(i, col_erro, ""),
                ])
            continue
        if not ficha_instituto(r.get("instituto", "")).strip():
            # RELATÓRIO de instituto SEM ficha: topline não automatiza. Sinaliza UMA vez
            # diretamente em 'Topline extraída?', para a ação manual ficar visível.
            updates.extend([
                gspread.Cell(i, col_flag, _link_status_topline_manual()),
                gspread.Cell(i, col_erro, ""),
            ])
            continue
        tentativas = _int0(r.get("topline_tentativas"))
        if tentativas >= 3:   # desiste após 3 falhas; limpe a coluna pra tentar de novo
            continue
        try:
            print(f"linha {i} ({registro_fila} / {r.get('cargo')}): baixando PDF para topline...", flush=True)
            pdf = _baixar_pdf(link)
            texto = extrair_texto_pdf_bytes(pdf)
        except Exception as e:
            _falha(i, registro_fila, tentativas, f"baixar/ler PDF: {e}")
            continue
        registros_pdf = _registros_tse_texto(texto)
        if registros_pdf and _norm_registro(registro_fila) not in registros_pdf:
            regs = ", ".join(sorted(registros_pdf))
            _falha(i, registro_fila, tentativas,
                   f"registro da fila não aparece no PDF; registros encontrados: {regs}")
            continue
        if len(texto) < 200:   # scan/sem camada de texto: manda o PDF pro Gemini (visão)
            print(f"linha {i} ({registro_fila}): PDF sem texto útil, usando visão")
            texto = ""
        cargos = _cargos_presentes(texto, r.get("cargo"))
        if not cargos:
            _falha(i, registro_fila, tentativas, "cargo da linha vazio ou não monitorado")
            continue
        print(f"linha {i} ({registro_fila} / {r.get('cargo')}): texto={len(texto)} caracteres; cargos={', '.join(cargos)}", flush=True)

        n_cen, avisos, houve_erro = 0, [], False
        linha_p, linha_r = [], []

        def _aviso(msg):
            if msg not in avisos:
                avisos.append(msg)

        def _norm_reg(s):
            import re as _re
            return _re.sub(r"[^A-Z0-9]", "", str(s).upper())

        # um mesmo PDF costuma ter 1º e 2º turno; extrai cada turno separado,
        # senão o Gemini fixa um turno só no payload e descarta o outro. Senador NUNCA
        # tem 2º turno no Brasil: nem pergunta pelo t2 (mesmo que o relatório traga uma
        # "simulação de 2º turno para Senador", isso não é um resultado t2 publicável).
        for cargo in cargos:
            turnos_cargo = ("t1",) if cargo == "senador" else ("t1", "t2")
            for turno in turnos_cargo:
                try:
                    escopo = {"cargo": cargo, "turno": turno, "instituto": r.get("instituto", ""),
                              "registro_tse": registro_fila}
                    if cargo != "presidente":   # governador/senador são estaduais: restrição obrigatória
                        escopo["uf"] = r.get("uf", "")
                    else:   # presidente pode ser nacional ou lido dentro do estado da fila: só referência
                        escopo["uf_referencia"] = r.get("uf", "")
                    print(f"linha {i} ({registro_fila} / {r.get('cargo')}): extraindo topline {cargo}/{turno}...", flush=True)
                    payload = _extrair_topline_pdf(pdf, texto, link, escopo, cargo)
                    n_falhos = payload.get("blocos_falhos") or 0
                    if n_falhos:
                        houve_erro = True
                        _aviso(f"{cargo}/{turno}: {n_falhos} bloco(s) do PDF falharam na "
                               "leitura; cenários dessas páginas podem estar faltando")
                    payload["turno"] = turno   # garante o turno pedido no rótulo/poll_id
                    # registro da fila é a fonte da verdade; só avisa se o registro da
                    # fila NÃO estiver entre os do PDF (compara sem hífen/pontuação;
                    # relatórios grafam BA04848 e podem trazer mais de um registro)
                    reg_pdf = str(payload.get("registro_tse", "")).strip()
                    if (not registros_pdf and reg_pdf and registro_fila and
                            _norm_reg(registro_fila) not in _norm_reg(reg_pdf)):
                        _aviso(f"registro no PDF ({reg_pdf}) difere da fila")
                    payload["registro_tse"] = registro_fila or reg_pdf
                    # A data do Gemini nunca é gravada sem validação. Quando o PDF
                    # declara o período de campo, o último dia desse período prevalece;
                    # sem período, a divulgação vira fallback. Datas futuras ou
                    # posteriores à divulgação retêm o cenário em vez de deslocar a série.
                    data_campo, aviso_data = resolver_data_campo_deterministica(
                        payload.get("data_campo", ""),
                        texto,
                        r.get("data_divulgacao", ""),
                        data_referencia=datetime.now(BRT).date(),
                    )
                    if not data_campo:
                        raise ValueError(aviso_data or "data_campo inválida")
                    payload["data_campo"] = data_campo
                    if aviso_data:
                        _aviso(f"{cargo}/{turno}: {aviso_data}")
                    df_p, df_r = montar_dataframes_polling(
                        payload, fonte_url=link, instituto_fonte=r.get("instituto", ""))
                except Exception as e:
                    print(f"linha {i} ({registro_fila}) [{cargo}/{turno}]: erro na extração: {e}")
                    houve_erro = True
                    # falha parcial não pode ficar invisível: registra no aviso da linha
                    _aviso(f"extração falhou em {cargo}/{turno}")
                    continue
                # o cargo pedido (escopo) é a fonte da verdade; o Gemini às vezes devolve
                # um cargo EXPLICITAMENTE diferente do pedido no payload, e esse cenário
                # NÃO pertence à linha da fila que estamos processando (ex.: um relatório
                # de presidente devolvendo um cenário rotulado "governador" por engano).
                # Descarta antes de gravar, senão o dado vai pra aba errada.
                # Cargo VAZIO é diferente: o prompt já restringe explicitamente qual cargo
                # extrair (FOCO DA EXTRAÇÃO), então "não rotulou" não é sinal de vazamento,
                # é só um campo que o modelo deixou em branco numa resposta mais fraca
                # (comum em PDF sem texto, extração por visão). Tratar vazio como
                # "diferente" descartaria dado real (MS-06247 Governador, texto=0).
                if not df_p.empty:
                    mask_cargo = (df_p["cargo"] == cargo) | (df_p["cargo"] == "")
                    if (~mask_cargo).any():
                        ids_ruins = set(df_p.loc[~mask_cargo, "scenario_id"])
                        _aviso(f"{cargo}/{turno}: descartado(s) {len(ids_ruins)} cenário(s) "
                               f"com cargo diferente do esperado ({cargo})")
                        df_p = df_p[mask_cargo].copy()
                        # df_r pode vir sem NENHUMA coluna (pd.DataFrame([]) quando o
                        # cenário só tinha o placeholder vazio, sem nenhum item de
                        # resultado) - "scenario_id" nem existe nesse caso, e indexar por
                        # ele estoura KeyError em vez de simplesmente não ter nada pra
                        # filtrar (MS-06247 Governador, PDF sem texto).
                        if "scenario_id" in df_r.columns:
                            df_r = df_r[~df_r["scenario_id"].isin(ids_ruins)].copy()
                    # preenche o cargo assumido (era vazio, aceito pela restrição do
                    # prompt) pra não sobrar célula em branco em topline_pesquisas/resultados
                    df_p.loc[df_p["cargo"] == "", "cargo"] = cargo
                    if "cargo" in df_r.columns:
                        df_r.loc[df_r["cargo"] == "", "cargo"] = cargo
                if df_r.empty:
                    continue
                # linha de cenário sem NENHUM resultado é órfã (placeholder de bloco
                # vazio ou gráfico lido sem números): não grava, senão vira linha morta
                # em topline_pesquisas.
                orfaos = ~df_p["scenario_id"].isin(df_r["scenario_id"])
                if orfaos.any():
                    df_p = df_p[~orfaos]
                # pergunta filtro ("o candidato que você votaria é um desses nomes ou
                # outro candidato?") vazando como cenário: a assinatura é um item de
                # resposta "Outro candidato" (não confundir com "Outros", legítimo).
                # A regra do prompt (filtro só entra se for a única medição do cargo/
                # turno) falha no chunking, porque cada bloco é avaliado isolado e o
                # bloco da pergunta filtro não vê as outras medições; avalia aqui no
                # consolidado (AM-03497 senador, Verità).
                cands_norm = df_r["candidato"].map(lambda s: _sem_acento(str(s)).strip().lower())
                filtro_ids = set(df_r.loc[cands_norm.str.match(r"outros? candidatos?$"), "scenario_id"])
                if filtro_ids and df_p["scenario_id"].nunique() > len(filtro_ids):
                    _aviso(f"{cargo}/{turno}: descartado(s) {len(filtro_ids)} cenário(s) de "
                           "pergunta filtro (resposta 'outro candidato'; há medição formal)")
                    df_p = df_p[~df_p["scenario_id"].isin(filtro_ids)]
                    df_r = df_r[~df_r["scenario_id"].isin(filtro_ids)]
                if df_r.empty:
                    continue
                # cenário de t1 somando muito pouco é fragmento, não cenário: rabo de
                # tabela cortada na fronteira de blocos (ex.: metade de baixo de uma
                # tabela de REJEIÇÃO sem o título, que o modelo interpreta como intenção
                # de voto - RO-07927 governador, soma 13.6). Nenhum cenário legítimo soma
                # menos de 50 nem sem o "Não válido".
                somas_sid = df_r.groupby("scenario_id")["percentual"].sum()
                fragmentos = set(somas_sid[somas_sid < 50].index)
                if fragmentos:
                    _aviso(f"{cargo}/{turno}: descartado(s) {len(fragmentos)} fragmento(s) "
                           f"com soma abaixo de 50% (lista parcial/sem contexto)")
                    df_p = df_p[~df_p["scenario_id"].isin(fragmentos)]
                    df_r = df_r[~df_r["scenario_id"].isin(fragmentos)]
                if df_r.empty:
                    continue
                # votos_por_entrevistado=2 com soma ~100 é marcação errada: o modelo viu
                # "senador de 2 vagas" e marcou voto duplo, mas os números são de uma
                # pergunta de voto único (soma ~100). Corrige pra 1, senão o downstream
                # divide/normaliza errado (SP-01703 Datafolha senador).
                somas_sid = df_r.groupby("scenario_id")["percentual"].sum()
                for sid_v in df_p.loc[df_p["votos_por_entrevistado"].astype(str) == "2", "scenario_id"]:
                    if somas_sid.get(sid_v, 0) < 140:
                        df_p.loc[df_p["scenario_id"] == sid_v, "votos_por_entrevistado"] = 1
                        _aviso(f"{cargo}/{turno}: votos_por_entrevistado corrigido 2->1 "
                               f"(soma ~100, voto único)")
                # sanidade: percentual fora de 0-100 e soma do cenário fora da faixa.
                # Cenário simples deve somar ~100; senador com voto duplo declarado
                # pode somar bem menos que 200. Isso pega mistura de "Porcentagem válida"
                # com "Porcentual" dos inválidos, como 50.3+49.7+23.9.
                if (df_r["percentual"] < 0).any() or (df_r["percentual"] > 100).any():
                    _aviso(f"{cargo}/{turno}: percentual fora de 0-100")
                votos_map = dict(zip(df_p["scenario_id"], df_p["votos_por_entrevistado"]))
                for sid, grupo in df_r.groupby("scenario_id"):
                    soma = grupo["percentual"].sum()
                    votos = int(votos_map.get(sid) or 1)
                    # "poderia citar ATÉ 2 candidatos" não é "sempre cita 2": quem citou só
                    # 1 (ou nenhum) puxa a soma pra baixo de 200% legitimamente. Faixa
                    # 185-215 rejeitava dado real da Paraná Pesquisas que soma 150-180%
                    # (RJ-04259, PR-01166, BA-04848, AL-04491 — confirmado no PDF original,
                    # nota "*Cada entrevistado poderia citar até 2 candidatos"). Mesma faixa
                    # 150-210 já usada em _avisos_soma_voto (segmentos) pro mesmo padrão.
                    minimo, teto = (150, 215) if votos >= 2 else (97, 103)
                    if minimo <= soma <= teto:
                        continue
                    lbl = grupo["scenario_label"].iloc[0]
                    if cargo == "senador" and votos == 1 and 115 < soma <= 215:
                        # soma de voto duplo, mas o relatório não declarou (ou o Gemini
                        # não achou a nota): não silencia, pede conferência
                        _aviso(f"{cargo}/{turno} cenário {lbl}: soma {soma:.1f} "
                               "(possível voto duplo não sinalizado; conferir)")
                    else:
                        _aviso(f"{cargo}/{turno} cenário {lbl}: soma {soma:.1f}")
                df_p["validacao"] = "; ".join(avisos[-3:]) if avisos else ""
                linha_p.append(df_p)
                linha_r.append(df_r)
                n_cen += len(df_p)
                print(f"linha {i} ({registro_fila} / {r.get('cargo')}) [{cargo}/{turno}]: {len(df_p)} cenário(s)", flush=True)

        if n_cen == 0:
            msg = "todas as extrações falharam" if houve_erro else f"nenhum cenário de topline encontrado para {r.get('cargo')}"
            _falha(i, registro_fila, tentativas, msg)
            continue
        nota_validacao = "; ".join(avisos[:3]) if avisos else ""
        todos_p.extend(linha_p)
        todos_r.extend(linha_r)
        updates.extend([gspread.Cell(i, col_flag, "sim"),
                        gspread.Cell(i, col_data, agora),
                        gspread.Cell(i, col_erro, nota_validacao[:300])])
        ok_regs.append(registro_fila)
        aviso_txt = f" | avisos: {'; '.join(avisos[:2])}" if avisos else ""
        print(f"linha {i} ({registro_fila}): {n_cen} cenário(s) de topline{aviso_txt}")
        _contador["n"] += 1

    _flush_topline("fim")

    print("\n───────── resumo ─────────")
    print(f"processados: {len(ok_regs)}  {ok_regs}")
    print(f"com erro:    {len(err_regs)}  {err_regs}")
    _resumo_uso_tokens("topline", USO_TOKENS_TOPLINE)


# ────────────────────────────── PUBLICAR ──────────────────────────────
# Parte 2: lê o staging (topline_pesquisas/topline_resultados), separa por turno
# e envia pras planilhas do PollingData (t1 e t2) via salvar_tudo, que gera
# pesquisas + resultados + resultados_bi (média móvel). Reusa o scraper_polling
# chamando-o, sem alterá-lo.

T1_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA", "")
T2_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA_T2", "")


def cmd_publicar():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    import pandas as pd
    from pollingdata_scraper import (
        classificar_instituto, gs_client_from_env, normalizar_instituto, salvar_tudo,
    )
    from relatorios_topline_core import ORIGEM

    gc = gs_client_from_env()
    sh = gc.open_by_key(RELATORIOS_ID)
    ws_p = _aba(sh, "topline_pesquisas")
    ws_r = _aba(sh, "topline_resultados")

    df_p = pd.DataFrame(ws_p.get_all_records())
    df_r = pd.DataFrame(ws_r.get_all_records())
    if df_p.empty:
        print("topline_pesquisas vazia; nada a publicar.")
        return

    header_p = ws_p.row_values(1)
    col_pub = _garantir_coluna(ws_p, header_p, "publicado")
    col_validacao = _garantir_coluna(ws_p, header_p, "validacao")
    if "publicado" not in df_p.columns:
        df_p["publicado"] = ""
    # Gate de liberação humana OBRIGATÓRIO: nenhum cenário extraído de PDF segue para
    # as matrizes sem 'liberado=sim' na aba topline_pesquisas, inclusive os cenários
    # sem aviso em 'validacao'. Isso deixa a revisão humana como etapa explícita.
    _garantir_coluna(ws_p, header_p, "liberado")
    if "liberado" not in df_p.columns:
        df_p["liberado"] = ""

    feito = df_p["publicado"].astype(str).str.strip().str.lower().isin(["sim", "true", "1", "x"])
    pendentes = df_p[~feito]
    if pendentes.empty:
        print("Tudo já publicado.")
        return

    def _preparar(df, colunas_destino):
        # Tira somente controles do staging. A origem já vem da extração e segue
        # junto para a matriz; `_marcar_origem` permanece como salvaguarda.
        df = df.drop(columns=[
            "publicado", "liberado", "validacao", "descricao", "votos_por_entrevistado"
        ], errors="ignore").copy()
        if "instituto" in df.columns:
            df["instituto"] = df["instituto"].apply(normalizar_instituto)
            df["classificacao_instituto"] = df["instituto"].apply(classificar_instituto)
        # Envia métricas como números reais para as matrizes. As abas de origem
        # podem exibir vírgula ou ponto decimal; transformar explicitamente evita
        # que 45,9/45.9 seja gravado como 459 em uma publicação futura.
        for coluna in ("percentual", "margem_erro", "confianca", "amostra", "ano"):
            if coluna in df.columns:
                bruto = df[coluna].astype(str).str.strip().str.replace("%", "", regex=False)
                bruto = bruto.str.replace(",", ".", regex=False)
                df[coluna] = pd.to_numeric(bruto, errors="coerce")
        for coluna in colunas_destino:
            if coluna not in df.columns:
                df[coluna] = ""
        return df.reindex(columns=colunas_destino)

    publicados = []
    retidos_por_colisao = []
    novos_por_destino = {}
    for turno, sheet_id in (("t1", T1_ID), ("t2", T2_ID)):
        if not sheet_id:
            continue
        pt_all = pendentes[pendentes["turno"].astype(str).str.lower() == turno]
        if pt_all.empty:
            continue
        liberado = pt_all["liberado"].astype(str).str.strip().str.lower().isin(["sim", "true", "1", "x"])
        retidos = ~liberado
        if retidos.any():
            print(f"{turno}: {int(retidos.sum())} cenário(s) RETIDOS aguardando "
                  "'liberado=sim' na topline_pesquisas")
        pt = pt_all[liberado]
        if pt.empty:
            continue

        sh_destino = gc.open_by_key(sheet_id)
        indice_destino = _indice_publicacao_destino(sh_destino.worksheet("pesquisas"))
        motivos = pt.apply(lambda linha: _motivo_colisao_publicacao(linha, indice_destino), axis=1)
        tem_colisao = motivos.ne("")
        if tem_colisao.any():
            for indice, motivo in motivos[tem_colisao].items():
                retidos_por_colisao.append((indice, motivo))
            print(f"{turno}: {int(tem_colisao.sum())} cenário(s) RETIDO(s): já existe(m) "
                  "no destino (scenario_id, poll_id ou chave legado).")
            pt = pt[~tem_colisao].copy()
        if pt.empty:
            continue

        ids = set(pt["scenario_id"].astype(str))
        rt = df_r[df_r["scenario_id"].astype(str).isin(ids)]
        # Após a trava acima, todos estes ids são novos no destino; manter os
        # valores originais aqui é importante para _marcar_origem encontrá-los.
        ids_novos = ids
        salvar_tudo(gc, sheet_id, _preparar(pt, POLLING_PESQUISAS_COLS),
                    _preparar(rt, POLLING_RESULTADOS_COLS))
        if ids_novos:
            novos_por_destino.setdefault(sheet_id, set()).update(ids_novos)
        publicados += list(pt.index)
        print(f"{turno}: {len(pt)} cenário(s), {len(rt)} resultado(s) -> planilha de {turno}")

    # Origem como última coluna (após metodologia), restrita às linhas inseridas
    # nesta execução; nunca reclassifica registros manuais já existentes.
    for sheet_id, scenario_ids in novos_por_destino.items():
        sh_t = gc.open_by_key(sheet_id)
        for tab in ("pesquisas", "resultados"):
            try:
                _marcar_origem(sh_t.worksheet(tab), ORIGEM, scenario_ids)
            except Exception as e:
                print(f"  aviso: origem em {tab} ({sheet_id[:6]}...): {e}")

    if publicados:   # índice 0-based do df = linha (i+2) na planilha
        ws_p.update_cells([gspread.Cell(i + 2, col_pub, "sim") for i in publicados],
                          value_input_option="RAW")
    if retidos_por_colisao:
        updates_validacao = []
        for indice, motivo in retidos_por_colisao:
            anterior = str(df_p.at[indice, "validacao"]).strip() if "validacao" in df_p.columns else ""
            nota = f"não publicado: {motivo}"
            if nota not in anterior:
                nota = f"{anterior}; {nota}".strip("; ")
            updates_validacao.append(gspread.Cell(indice + 2, col_validacao, nota[:300]))
        ws_p.update_cells(updates_validacao, value_input_option="RAW")
    print(f"\n{len(publicados)} cenário(s) publicado(s); "
          f"{len(retidos_por_colisao)} retido(s) por colisão.")


def cmd_rebuild_bi():
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    from pollingdata_scraper import gs_client_from_env, reconstruir_resultados_bi

    gc = gs_client_from_env()
    for turno, sheet_id in (("t1", T1_ID), ("t2", T2_ID)):
        if not sheet_id:
            continue
        print(f"{turno}: reconstruindo resultados_bi...")
        reconstruir_resultados_bi(gc, sheet_id)


def cmd_canonico():
    """Regenera o canonico.json a partir das planilhas T1/T2 do PollingData.
    Rodar localmente quando surgirem candidatos/institutos novos; commitar o arquivo."""
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    from pollingdata_scraper import gs_client_from_env

    gc = gs_client_from_env()
    institutos, pres = set(), set()
    gov, sen = {}, {}
    for sid in (T1_ID, T2_ID):
        if not sid:
            continue
        for r in gc.open_by_key(sid).worksheet("resultados").get_all_records():
            inst = str(r.get("instituto", "")).strip()
            if inst:
                institutos.add(inst)
            if str(r.get("tipo", "")).strip().lower() == "nao_valido":
                continue
            cp = str(r.get("candidato_partido", "")).strip()
            cargo = str(r.get("cargo", "")).strip().lower()
            uf = str(r.get("uf", "")).strip().upper()
            if not cp:
                continue
            if cargo == "presidente":
                pres.add(cp)
            elif cargo == "governador":
                gov.setdefault(uf, set()).add(cp)
            elif cargo == "senador":
                sen.setdefault(uf, set()).add(cp)

    data = {
        "institutos": sorted(institutos),
        "presidente": sorted(pres),
        "governador": {uf: sorted(v) for uf, v in sorted(gov.items())},
        "senador": {uf: sorted(v) for uf, v in sorted(sen.items())},
    }
    caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), "canonico.json")
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print(f"canonico.json atualizado: {len(data['institutos'])} institutos, "
          f"{len(data['presidente'])} presidenciáveis, "
          f"{sum(len(v) for v in data['governador'].values())} gov, "
          f"{sum(len(v) for v in data['senador'].values())} sen. Commite o arquivo.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "alerta":
        cmd_alerta()
    elif cmd == "extrair":
        cmd_extrair()
    elif cmd == "topline":
        cmd_topline()
    elif cmd == "publicar":
        cmd_publicar()
    elif cmd == "rebuild_bi":
        cmd_rebuild_bi()
    elif cmd == "canonico":
        cmd_canonico()
    else:
        print("uso: python relatorios_pipeline.py [alerta|extrair|topline|publicar|rebuild_bi|canonico]")
