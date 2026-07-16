"""
Helpers de manutenção de planilha compartilhados entre relatorios_pipeline.py
(workflow 04, extrai segmentos/topline dos PDFs) e relatorios_busca_fontes.py
(workflow 03, busca o link do relatório faltante).

Os dois arquivos leem e escrevem na MESMA aba 'relatorios', e por muito tempo
cada um tinha sua própria cópia colada destes helpers. Isso já causou bug real
duas vezes (14-16/07/2026): um fix de _row_count_atual foi aplicado só numa
cópia, e _resetar_validacoes_relatorios ficou sem 2 chamadas de coloração na
outra. Consolidado aqui pra qualquer correção valer pros dois workflows de
uma vez.

Não colocar aqui nada que dependa de estado GLOBAL mutável específico de cada
arquivo (ex.: USO_TOKENS, GEMINI_MODEL) - cada workflow rastreia seu próprio
custo de Gemini separadamente. _registrar_uso por isso recebe o dicionário de
uso como parâmetro explícito em vez de mexer numa variável global do módulo.
"""

import json
import re
import time
import unicodedata
from datetime import timedelta, timezone

import gspread

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

STATUS_TOPLINE_MANUAL = "⚠️ REGISTRE NO POLLING MANUAL"

CARGOS_MONITORADOS = ("presidente", "governador", "senador")

CARGO_ROTULO = {
    "presidente": "Presidente",
    "governador": "Governador",
    "senador": "Senador",
}

BRT = timezone(timedelta(hours=-3))

REGISTRO_TSE_RE = re.compile(
    r"\b(?:BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)"
    r"[\s-]*\d{5}/2026\b",
    flags=re.I,
)


def _sem_acento(valor):
    return unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode()


def _rel_display(nome):
    return REL_COL.get(nome, nome)


def _rel_key(nome):
    return REL_KEY.get(nome, nome)


def _rel_record(row):
    return {_rel_key(k): v for k, v in row.items()}


def _rel_records(ws):
    return [_rel_record(r) for r in ws.get_all_records()]


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


def _ultima_linha_com_registro(ws):
    col_a = ws.col_values(1)
    ultima = 1
    for idx, val in enumerate(col_a, start=1):
        if idx > 1 and str(val).strip():
            ultima = idx
    return ultima


def _row_count_atual(ws):
    """Linhas da grade direto da API, sem confiar no cache local do gspread.

    ws.row_count é lido de ws._properties, setado na hora que o Worksheet foi
    aberto (ou na última chamada de resize()/add_rows() NESSE MESMO objeto).
    Se outra chamada de API mudou a grade nesse meio-tempo (ex.: append_rows
    expande a grade sozinho além do que add_rows pediu), o cache local fica
    errado e deleteDimension recebe um endIndex que não bate mais com a
    grade real — foi o que gerou 'Cannot delete a row that doesn't exist'."""
    meta = ws.spreadsheet.fetch_sheet_metadata()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == ws.id:
            return props.get("gridProperties", {}).get("rowCount", ws.row_count)
    return ws.row_count


def _encolher_linhas_vazias(ws):
    """Remove somente as linhas vazias que sobram abaixo dos dados.

    Roda logo depois de add_rows()+append_rows(). add_rows(N) às vezes
    expande a grade em blocos maiores que N por conta própria, e um fetch de
    metadata (_row_count_atual) imediatamente depois pode devolver um
    total_atual desatualizado (ainda não propagado), igual a 'ultima' - o
    early-return antigo confiava nessa ÚNICA leitura e pulava a limpeza
    achando que não sobrava nada. A linha extra fica com a validação de
    checkbox aplicada (herdada da coluna 'liberado'), então o Sheets ainda
    conta ela como "usada" pro auto-append da PRÓXIMA rodada - o resultado é
    um buraco permanente entre o fim de uma execução do workflow e o começo
    da seguinte (achado em topline_pesquisas: linhas 83-107 e 133-158, cada
    uma bem na fronteira entre duas rodadas em dias diferentes). Por isso
    confere de novo, com uma pequena pausa, antes de desistir.
    """
    ultima = _ultima_linha_com_dados(ws)
    total_atual = _row_count_atual(ws)
    if total_atual <= ultima:
        time.sleep(1.5)
        total_atual = _row_count_atual(ws)
        if total_atual <= ultima:
            return
    for tentativa in range(2):
        try:
            ws.spreadsheet.batch_update({
                "requests": [{
                    "deleteDimension": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "ROWS",
                            "startIndex": ultima,
                            "endIndex": total_atual,
                        }
                    }
                }]
            })
            return
        except Exception as e:
            if tentativa == 1:
                print(f"[AVISO] não deu pra remover linhas vazias da aba '{ws.title}': {e}")
                return
            # endIndex pode ter ficado desatualizado NO SENTIDO CONTRÁRIO
            # (grade real menor que o total_atual lido) - relê e tenta de novo.
            time.sleep(1.5)
            total_atual = _row_count_atual(ws)
            if total_atual <= ultima:
                return


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


def _garantir_coluna_relatorios(ws, header, nome):
    display = _rel_display(nome)
    for candidato in [display, nome] + ALIASES_RELATORIOS.get(display, []):
        if candidato in header:
            return header.index(candidato) + 1
    return _garantir_coluna(ws, header, display)


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


def _custo_estimado(entrada, saida, pensamento):
    # preço aproximado da faixa "flash" (~$0,30/1M tokens de entrada, ~$2,50/1M de
    # saída, saída e pensamento cobram na mesma tabela). Ajuste se trocar de modelo
    # (GEMINI_MODEL) ou se o preço mudar. Estimativa, não fatura oficial; confira o
    # console de billing do Google pro valor exato.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _registrar_uso(resp, uso):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    uso["chamadas"] += 1
    uso["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    uso["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    uso["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _resumo_uso_tokens(rotulo, uso):
    if not uso["chamadas"]:
        return
    custo = _custo_estimado(uso["entrada"], uso["saida"], uso["pensamento"])
    print(f"\nGemini ({rotulo}): {uso['chamadas']} chamada(s) · "
          f"{uso['entrada']:,} tokens entrada · {uso['saida']:,} saída · "
          f"{uso['pensamento']:,} pensamento · custo estimado ${custo:.4f}")
