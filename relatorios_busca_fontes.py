import base64
import html as html_lib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import gspread
import requests
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from google import genai
from google.genai import types

BRT = timezone(timedelta(hours=-3))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
# Só busca fonte de pesquisa divulgada nos últimos N dias. Mais velha que isso não
# vale mais buscar/divulgar. Ajustável por env (MAX_DIAS_BUSCA).
MAX_DIAS_BUSCA = int(os.getenv("MAX_DIAS_BUSCA", "5"))

# Cabeçalhos para fingir ser um usuário real do Windows/Chrome + IP do Googlebot (Fallback)
STEALTH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Forwarded-For": "66.249.66.1"  # IP do Googlebot para furar alguns paywalls híbridos
}

MAPA_UF = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPÁ": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARÁ": "CE", "DISTRITO FEDERAL": "DF", "ESPÍRITO SANTO": "ES", "GOIÁS": "GO",
    "MARANHÃO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARÁ": "PA", "PARAÍBA": "PB", "PARANÁ": "PR", "PERNAMBUCO": "PE", "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO", "RORAIMA": "RR", "SANTA CATARINA": "SC", "SÃO PAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO", "BRASIL": "BR"
}

DRIVE_ID = '0AH-94UFLKIFPUk9PVA'
PASTA_PRESIDENCIAVEIS = '1apgniY4undEtkqjDYOEdf1aoMU7HDfOh'
PASTA_GOV_SEN = '1MmeVz63PG9imU_oDqk7thw5gHha0xAWa'

# colunas da fila compartilhada com relatorios_pipeline.py
COL_ORIGEM_LINK = "origem_link"
COL_NIVEL_CONFERENCIA = "nivel_conferencia"
COL_CONFERIDO = "conferido"
RELATORIOS_COLUNAS = [
    ("registro", "Registro TSE"),
    ("cargo", "Cargo"),
    ("uf", "UF"),
    ("instituto", "Instituto"),
    ("data_divulgacao", "Data de divulgação"),
    ("link", "Link do relatório"),
    (COL_ORIGEM_LINK, "Origem do link"),
    (COL_NIVEL_CONFERENCIA, "Nível de conferência"),
    ("tipo_fonte", "Tipo"),
    (COL_CONFERIDO, "Conferido?"),
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
    "Origem do link": [COL_ORIGEM_LINK, "origem_busca"],
    "Nível de conferência": [COL_NIVEL_CONFERENCIA],
    "Tipo": ["tipo_fonte", "tipo", "tipo_de_fonte"],
    "Conferido?": [COL_CONFERIDO],
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
CARGOS_MONITORADOS = ("presidente", "governador", "senador")
CARGO_ROTULO = {
    "presidente": "Presidente",
    "governador": "Governador",
    "senador": "Senador",
}

IMAGE_MIME_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif")
PDF_LINK_HINTS = (
    "pdf", "download", "baixar", "anexo", "arquivo", "file", "documento",
    "relatorio", "relatório", "resultado", "pesquisa"
)
IMAGE_LINK_HINTS = ("image", "img", "foto", "photo", "thumb", "media", "wp-content/uploads")
# Fontes de vídeo/rede social não trazem o relatório em texto ou PDF; descartar
# como fonte (o Gemini às vezes devolve um vídeo do YouTube sobre a pesquisa).
FONTES_VIDEO = ("youtube.com", "youtu.be", "tiktok.com", "vimeo.com", "dailymotion.com")
MAX_PDF_CANDIDATOS = 80
MAX_IMAGENS_OCR = int(os.getenv("MAX_IMAGENS_OCR", "40"))
MAX_IMAGE_BYTES = int(os.getenv("MAX_IMAGE_BYTES", str(12 * 1024 * 1024)))
CHROME_VIRTUAL_TIME_MS = int(os.getenv("CHROME_VIRTUAL_TIME_MS", "12000"))
SITUACOES_PENDENTES = {"nao", "provavel", "teaser", "paywall", "bloqueado", "erro_chrome"}
PAYWALL_MARKERS = (
    "assine para continuar", "assine para ler", "conteúdo exclusivo para assinantes",
    "conteudo exclusivo para assinantes", "exclusivo para assinantes",
    "faça login para continuar", "faca login para continuar", "entre para continuar",
    "login para ler", "subscribe to continue", "sign in to continue",
    "already a subscriber", "paywall"
)
COOKIE_ACCEPT_RE = re.compile(
    r"\b(aceitar|aceito|concordo|permitir|continuar|prosseguir|ok|entendi|accept|agree|allow)\b",
    flags=re.I,
)


def obter_credenciais():
    """Lê as credenciais exclusivamente do ambiente (GitHub Secrets)"""
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não encontrado nas variáveis de ambiente.")
    info = json.loads(creds_json)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    creds.refresh(Request())
    return creds


def obter_cliente_gemini():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não encontrada nas variáveis de ambiente.")
    return genai.Client(api_key=api_key)


def _rel_display(nome):
    return REL_COL.get(nome, nome)


def _rel_key(nome):
    return REL_KEY.get(nome, nome)


def _rel_record(row):
    return {_rel_key(k): v for k, v in row.items()}


def _rel_records(ws):
    return [_rel_record(r) for r in ws.get_all_records()]


def _garantir_coluna_relatorios(ws, header, nome):
    display = _rel_display(nome)
    for candidato in [display, nome] + ALIASES_RELATORIOS.get(display, []):
        if candidato in header:
            return header.index(candidato) + 1
    return _garantir_coluna(ws, header, display)


def _resetar_validacoes_relatorios(ws, header, ate_linha):
    """Remove checkboxes/validações acidentais nas OUTRAS colunas e garante a de
    Conferido?. NUNCA limpa a validação de Conferido? antes de recriar: isso roda em
    toda execução, e limpar+recriar em duas chamadas separadas deixa uma janela em
    que, se a segunda falhar (rede, limite de taxa), o checkbox de Conferido? some da
    planilha até a próxima rodada consertar. setDataValidation é idempotente, então
    só (re)aplicar a de Conferido? já resolve sem esse risco."""
    if ate_linha < 2:
        return
    col_conferido = _rel_display(COL_CONFERIDO)
    # protege as colunas com validação própria (checkbox do Conferido? e lista suspensa
    # do Tipo) pra não limpá-las junto com as validações acidentais das outras.
    protegidas = sorted({header.index(_rel_display(n)) for n in (COL_CONFERIDO, "tipo_fonte")
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
        "relatório": (0.82, 0.93, 0.82),
        "notícia": (1.0, 0.90, 0.80),
        "N/A": (0.90, 0.90, 0.90),
    })


def _garantir_coluna(ws, header, nome):
    """Índice (1-based) da coluna 'nome'; cria no fim se não existir (sem estourar a grade)."""
    if nome in header:
        return header.index(nome) + 1
    novo = len(header) + 1
    if ws.col_count < novo:
        ws.add_cols(novo - ws.col_count)
    ws.update_cell(1, novo, nome)
    header.append(nome)
    return novo


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
    """Lista suspensa (ONE_OF_LIST) na coluna, da linha 2 até ate_linha. strict=True +
    showCustomUi=True: mostra a setinha e só aceita valor da lista (blank permitido)."""
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
    """Formatação condicional: pinta o fundo da coluna conforme o valor. Idempotente
    (remove as regras que já cobrem a coluna antes de recriar). A cor vale pras linhas
    novas também."""
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
            for idx in range(len(cfs) - 1, -1, -1):
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


def _ultima_linha_com_registro(ws):
    """Índice (1-based) da última linha que tem registro preenchido. Serve pra
    limitar o checkbox ao range de dados real, sem sobrar checkbox vazio embaixo."""
    col_a = ws.col_values(1)
    ultima = 1
    for idx, val in enumerate(col_a, start=1):
        if idx > 1 and str(val).strip():
            ultima = idx
    return ultima


def _ultima_linha_com_dados(ws):
    """Última linha com algum valor, incluindo o cabeçalho."""
    ultima = 1
    for linha, valores in enumerate(ws.get_all_values(), start=1):
        # Checkbox vazio aparece como FALSE na API, mas não representa dado.
        if any(str(valor).strip() and str(valor).strip().upper() != "FALSE"
               for valor in valores):
            ultima = linha
    return ultima


def _encolher_linhas_vazias(ws):
    """Apaga apenas o bloco vazio abaixo da última linha usada."""
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
    """Expande a aba somente para as linhas novas e remove sobras ao final."""
    if not linhas:
        return
    ultima = _ultima_linha_com_dados(ws)
    necessario = ultima + len(linhas)
    if ws.row_count < necessario:
        ws.add_rows(necessario - ws.row_count)
    ws.append_rows(linhas, value_input_option=value_input_option)
    _encolher_linhas_vazias(ws)


def _normalizar_cabecalho_relatorios(ws):
    """Garante a ordem canônica da fila sem perder dados de colunas antigas."""
    valores = ws.get_all_values()
    if not valores:
        ws.update(range_name="A1", values=[CABECALHO_RELATORIOS])
        # sem dados ainda: o checkbox nasce junto com as pesquisas, no fim do run
        return CABECALHO_RELATORIOS[:]

    atual = valores[0]
    aliases_antigos = {a for aliases in ALIASES_RELATORIOS.values() for a in aliases}
    aliases_antigos.update(REL_COL.keys())
    extras = [c for c in atual if c and c not in CABECALHO_RELATORIOS and c not in aliases_antigos]
    alvo = CABECALHO_RELATORIOS + extras
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
    if _rel_display(COL_CONFERIDO) in alvo and _rel_display(COL_CONFERIDO) not in idx:
        _ativar_checkbox(ws, _rel_display(COL_CONFERIDO), alvo, ate_linha=len(valores))
    _remover_colunas_sobrando(ws, len(alvo))
    return alvo


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
        "link", COL_NIVEL_CONFERENCIA, COL_CONFERIDO,
        "segmentos_extraido", "segmentos_data_extracao", "segmentos_erro", "segmentos_tentativas",
        "topline_extraido", "topline_data_extracao", "topline_erro", "topline_tentativas",
    ):
        row[_rel_display(coluna)] = ""
        row[coluna] = ""
    row[_rel_display(COL_ORIGEM_LINK)] = "separado de linha multicargo; buscar fonte específica"
    row[COL_ORIGEM_LINK] = "separado de linha multicargo; buscar fonte específica"
    return row


def _separar_linhas_multicargo(ws, header):
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
        print(f"{len(novas)} linha(s) multicargo separada(s) na fila de relatórios.")


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
    raise ValueError("JSON não encontrado na resposta do Gemini")


def agente_buscar_link_faltante(gemini_client, registro, instituto, cargo, uf, data):
    prompt = f"""
    Você é um pesquisador sênior de dados eleitorais da Eixo. Localize a publicação do relatório completo de resultados da seguinte pesquisa eleitoral de 2026:
    Registro TSE: {registro} | Instituto: {instituto} | Cargo: {cargo} - UF: {uf} | Data prevista: ~{data}

    ESTRATÉGIA DE BUSCA E REGRAS CRÍTICAS:
    1. FONTES: Priorize o site oficial do instituto (ex: paranapesquisas.com.br, realtimebigdata.com.br, eleicoes26.institutoverita.com.br). Se não houver, busque cobertura jornalística aberta (Poder360, G1, CNN, Gazeta do Povo, Metrópoles, blogs locais).
    2. PROIBIDO TSE: NÃO retorne links do sistema PesqEle do TSE ou PDFs que sejam apenas o "Recibo de Registro/Questionário". O alvo é o relatório com os RESULTADOS (gráficos, intenção de voto).
    2b. PROIBIDO VÍDEO: NÃO retorne links de YouTube, TikTok, Vimeo ou qualquer vídeo. Precisamos de PDF, matéria de site ou imagem do relatório, algo com texto/números; vídeo não serve.
    3. FALSO PAYWALL: Se a primeira fonte encontrada for paga (Estadão, Folha, O Globo), ESCARAFUNCHE a internet buscando fontes abertas secundárias sobre a mesma pesquisa. Retorne o paywall apenas como último recurso.
    4. ALUCINAÇÃO DE DATA: Só aceite matérias ou relatórios que citem dados de 2026. Ignore notícias velhas de 2022 ou 2024.
    5. CARGO CERTO: A fonte precisa trazer números do cargo pedido ({cargo}). Se o mesmo registro tiver matérias separadas para Governador e Senador, retorne a matéria do cargo pedido. Não aceite uma matéria apenas de Governador para uma linha de Senador, nem o contrário.
    6. IGNORE TEASERS: Descarte matérias que dizem "vai ser divulgada", "será marcada pela divulgação", "está prevista para", "ainda vai sair" ou "foi registrada". Mesmo que citem o registro {registro}, elas não valem se não trouxerem os números da pesquisa. Só aceite fonte com resultado JÁ publicado.

    Retorne APENAS um JSON válido (sem tags markdown de bloco de código):
    {{
      "link": "URL_COMPLETA_AQUI ou deixe vazio se não achar",
      "tipo": "pdf", "imagem", "materia", "paywall" ou "nao_encontrado",
      "origem_texto": "Descrição breve (ex: 'Relatório no site do instituto' ou 'Matéria G1')"
    }}
    """
    config = types.GenerateContentConfig(
        temperature=0.1,
        tools=[types.Tool(google_search=types.GoogleSearch())],
        # Sem timeout essa é a única chamada de rede do arquivo que pode travar
        # indefinidamente (grounding com Google Search é mais pesado/sujeito a blip).
        # Runs reais já ficaram 3h+ parados numa linha até o timeout de 200min do
        # job matar o processo, porque sem timeout aqui a exceção nunca dispara
        # e o retry abaixo nunca roda.
        http_options=types.HttpOptions(timeout=120_000),
    )
    ultimo = None
    # "Server disconnected without sending a response" é falha de conexão transitória
    # (não erro da API), comum em chamada com grounding (mais pesada/demorada). Sem
    # retry, um blip de rede zera a busca do registro até a próxima rodada do dia
    # seguinte; com 3 tentativas e backoff exponencial (mesmo padrão de
    # gerar_conteudo_gemini em relatorios_topline_core.py) a maioria se resolve na hora.
    for tentativa in range(1, 4):
        try:
            res = gemini_client.models.generate_content(model=GEMINI_MODEL, contents=prompt, config=config)
            return _extrair_json_objeto(getattr(res, "text", "") or "")
        except Exception as e:
            ultimo = e
            if tentativa < 3:
                time.sleep(1.5 * (2 ** (tentativa - 1)))
    print(f"  [AVISO] Gemini/search falhou para {registro} após 3 tentativas: {str(ultimo)[:200]}")
    return {"tipo": "nao_encontrado"}


def _norm(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


REGISTRO_TSE_RE = re.compile(
    r"\b(?:BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)"
    r"[\s-]*\d{5}/2026\b",
    flags=re.I,
)


def _registros_tse_norm(texto):
    """Registros TSE citados no texto, normalizados para comparar com a fila."""
    registros = set()
    for m in REGISTRO_TSE_RE.finditer(texto or ""):
        registros.add(_norm(m.group(0)))
    return registros


def _texto_pdf(pdf_bytes):
    """Extrai texto do PDF (até 30 páginas). Vazio se for PDF só imagem."""
    try:
        import fitz  # PyMuPDF
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return " ".join((doc.load_page(i).get_text("text") or "")
                             for i in range(min(doc.page_count, 30)))
    except Exception:
        return ""


def _url_tem_extensao(url, exts):
    return urlparse(str(url or "")).path.lower().endswith(exts)


def _fonte_video(url):
    """True se o link é de plataforma de vídeo (YouTube, TikTok etc.): não tem
    relatório em texto/PDF pra conferir, não adianta baixar."""
    host = urlparse(str(url or "")).netloc.lower()
    return any(host == d or host.endswith("." + d) for d in FONTES_VIDEO)


def _headers_com_referer(referer=""):
    headers = dict(STEALTH_HEADERS)
    if referer:
        headers["Referer"] = referer
    return headers


def _normalizar_url_extraida(url, url_base):
    url = html_lib.unescape(str(url or ""))
    url = url.replace("\\/", "/").replace("\\u002F", "/").replace("\\u0026", "&")
    url = url.strip().strip("'\"()[]{}<>")
    if not url or url.startswith(("mailto:", "tel:", "javascript:", "#", "data:")):
        return ""
    if url.startswith("//"):
        url = "https:" + url
    return urljoin(url_base, url)


def _parece_pdf_candidato(url, texto_link=""):
    bruto = html_lib.unescape(f"{url} {texto_link}").lower()
    bruto_norm = _norm(bruto)
    if _url_tem_extensao(url, (".pdf",)):
        return True
    return any(_norm(h) in bruto_norm for h in PDF_LINK_HINTS)


def _parece_imagem_candidata(url):
    bruto = html_lib.unescape(str(url or "")).lower()
    bruto_norm = _norm(bruto)
    if _url_tem_extensao(url, IMAGE_EXTS):
        return True
    return any(_norm(h) in bruto_norm for h in IMAGE_LINK_HINTS)


def _mime_imagem(resposta, url=""):
    content_type = (resposta.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if content_type in IMAGE_MIME_TYPES:
        return content_type
    if content_type.startswith("text/") or content_type in ("application/json", "application/xml"):
        return ""
    if _url_tem_extensao(url, (".jpg", ".jpeg")):
        return "image/jpeg"
    if _url_tem_extensao(url, (".png",)):
        return "image/png"
    if _url_tem_extensao(url, (".webp",)):
        return "image/webp"
    if _url_tem_extensao(url, (".gif",)):
        return "image/gif"
    return ""


def _imagem_data_uri(url, referer=""):
    try:
        r = requests.get(url, headers=_headers_com_referer(referer), timeout=30)
        if r.status_code != 200 or len(r.content) > MAX_IMAGE_BYTES:
            return ""
        mime_type = _mime_imagem(r, url)
        if not mime_type:
            return ""
        return f"data:{mime_type};base64,{base64.b64encode(r.content).decode('ascii')}"
    except requests.exceptions.RequestException:
        return ""


def _imagem_para_pdf(image_bytes):
    """Encapsula uma imagem em PDF para manter o padrão de upload no Drive."""
    import fitz  # PyMuPDF
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)  # A4 em pontos
    page.insert_image(fitz.Rect(36, 36, 559, 806), stream=image_bytes, keep_proportion=True)
    return doc.tobytes(garbage=4, deflate=True)


def _texto_imagem_gemini(gemini_client, image_bytes, mime_type):
    if not gemini_client:
        return ""
    prompt = (
        "Transcreva o texto legível desta imagem de relatório ou matéria de pesquisa eleitoral. "
        "Inclua registros TSE, instituto, UF, datas e números relevantes quando aparecerem. "
        "Retorne apenas texto corrido, sem comentários."
    )
    try:
        config = types.GenerateContentConfig(temperature=0)
        res = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt, types.Part.from_bytes(data=image_bytes, mime_type=mime_type)],
            config=config,
        )
        return getattr(res, "text", "") or ""
    except Exception:
        return ""


def _texto_imagens_da_pagina(gemini_client, urls, registro, uf, instituto, referer="", limite=MAX_IMAGENS_OCR):
    partes = []
    for lp in urls[:limite]:
        try:
            ir = requests.get(lp, headers=_headers_com_referer(referer), timeout=30)
        except requests.exceptions.RequestException:
            continue
        if ir.status_code != 200:
            continue
        mime_type = _mime_imagem(ir, lp)
        if not mime_type:
            continue
        texto = _texto_imagem_gemini(gemini_client, ir.content, mime_type)
        if texto:
            partes.append(texto)
            if _confere_texto(texto, registro, uf, instituto) == "ok":
                break
    return "\n".join(partes)


def _norm_espacos(s):
    """Minúsculo e sem acento, mas preserva espaço (pra casar frase com \\s+)."""
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s)


TEASER_RE = re.compile(
    r"ser[ao]o?\s+divulgad"                       # será divulgada / serão divulgadas
    r"|ser[ao]\s+marcad\w*\s+pela\s+divulga"       # será marcada pela divulgação
    r"|va[oi]\s+divulgar"                          # vai/vão divulgar
    r"|ainda\s+vai\s+sair|vai\s+sair\s+ainda"      # ainda vai sair
    r"|est[a]o?\s+previst\w*\s+para"               # está/estão previstas para
    r"|deve[m]?\s+divulgar"                        # deve(m) divulgar
    r"|ainda\s+nao\s+saiu"                         # ainda não saiu
    r"|aguard\w*\s+(o\s+)?resultado"               # aguarde o resultado
    r"|ser[ao]o?\s+revelad"                        # será/serão revelados
)


def _eh_teaser(texto):
    """Matéria que só anuncia pesquisa futura ('será marcada pela divulgação',
    'ainda vai sair', 'está prevista para'), sem trazer os números de fato. O
    registro pode até aparecer numa lista de pesquisas previstas pra semana,
    mas o conteúdo não é o resultado."""
    if not TEASER_RE.search(_norm_espacos(texto)):
        return False
    qtd_percentuais = len(re.findall(r"\d{1,3}[.,]?\d*\s?%", texto or ""))
    return qtd_percentuais < 3


def _confere_texto(texto, registro, uf, instituto):
    if _eh_teaser(texto):
        return "teaser"
    tn = _norm(texto)
    reg_esperado = _norm(registro)
    registros_citados = _registros_tse_norm(texto)
    if reg_esperado and reg_esperado in registros_citados:   # registro é a prova forte
        return "ok"
    if reg_esperado and registros_citados:
        # Se a fonte cita outro(s) registro(s) TSE e não cita o registro da fila,
        # ela é perigosa demais para entrar por prova fraca UF+instituto+2026.
        return "nao"
    if len((texto or "").strip()) < 100:
        return "imagem"
    # prova fraca: UF (por extenso) + algum token do instituto + o ano 2026
    uf_ok = _norm(uf) and _norm(uf) in tn
    toks_inst = [t for t in re.sub(r"[^a-zà-ú ]", " ", str(instituto or "").lower()).split()
                 if len(t) > 3 and t not in ("instituto", "pesquisas", "pesquisa", "consultoria", "dados")]
    inst_ok = any(_norm(t) in tn for t in toks_inst)
    if uf_ok and inst_ok and "2026" in tn:
        return "provavel"
    return "nao"


def confere_pesquisa(pdf_bytes, registro, uf, instituto):
    """'ok' se achou o registro; 'provavel' se bate UF+instituto+2026;
    'teaser' se a matéria só anuncia divulgação futura sem números;
    'nao' se claramente não é; 'imagem' se não dá pra conferir."""
    return _confere_texto(_texto_pdf(pdf_bytes), registro, uf, instituto)


def confere_pesquisa_com_texto_extra(pdf_bytes, registro, uf, instituto, texto_extra=""):
    texto = "\n".join([_texto_pdf(pdf_bytes), texto_extra or ""])
    return _confere_texto(texto, registro, uf, instituto)


def confere_pesquisa_imagem(image_bytes, mime_type, registro, uf, instituto, gemini_client=None):
    """Confere imagem com visão/OCR do Gemini; se não conseguir ler, salva como não conferida."""
    texto = _texto_imagem_gemini(gemini_client, image_bytes, mime_type)
    return _confere_texto(texto, registro, uf, instituto)


def _nota_conferencia(situacao):
    if situacao == "imagem":
        return " (imagem, não conferido)"
    if situacao == "provavel":
        return " (conferência fraca: sem registro TSE)"
    if situacao == "teaser":
        return " (matéria só anuncia divulgação futura, sem números ainda)"
    if situacao == "paywall":
        return " (paywall/conteúdo não entregue)"
    if situacao == "erro_chrome":
        return " (Chrome indisponível no runner)"
    if situacao == "bloqueado":
        return " (conteúdo bloqueado/incompleto)"
    return ""


def _mensagem_pendente(registro, situacao):
    if situacao == "paywall":
        return f"{registro} - paywall ou conteúdo não entregue; deixado pendente"
    if situacao == "erro_chrome":
        return f"{registro} - Chrome não disponível no runner; instalar Chrome ou definir CHROME_BIN"
    if situacao == "bloqueado":
        return f"{registro} - conteúdo bloqueado/incompleto; deixado pendente"
    if situacao == "provavel":
        return f"{registro} - registro TSE não apareceu no texto (só bateu UF+instituto+2026); deixado pendente"
    if situacao == "teaser":
        return f"{registro} - matéria encontrada só anuncia que a pesquisa vai sair, sem números ainda; deixado pendente"
    return f"{registro} - conteúdo não confere com a pesquisa, deixado pendente"


def _adicionar_url(lista, url, url_base, texto_link=""):
    url_abs = _normalizar_url_extraida(url, url_base)
    if not url_abs:
        return
    lista.append((url_abs, texto_link or ""))


def _parse_srcset(srcset):
    for parte in str(srcset or "").split(","):
        pedaços = parte.strip().split()
        if pedaços:
            yield pedaços[0]


def _extrair_links_documentos(html, url_base):
    html = html or ""
    html_scan = html.replace("\\/", "/").replace("\\u002F", "/").replace("\\u0026", "&")
    encontrados = []

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        attrs_url = (
            "href", "src", "data-src", "data-original", "data-lazy-src",
            "data-url", "data-href", "content", "poster"
        )
        attrs_srcset = ("srcset", "data-srcset")
        for tag in soup.find_all(True):
            texto_link = tag.get_text(" ", strip=True) if tag.name == "a" else ""
            for attr in attrs_url:
                _adicionar_url(encontrados, tag.get(attr), url_base, texto_link)
            for attr in attrs_srcset:
                for src in _parse_srcset(tag.get(attr)):
                    _adicionar_url(encontrados, src, url_base, texto_link)
            style = tag.get("style", "")
            for css_url in re.findall(r"url\(([^)]+)\)", style):
                _adicionar_url(encontrados, css_url, url_base, texto_link)
    except Exception:
        pass

    for attr_url in re.findall(r'(?:href|src|data-src|data-original|data-lazy-src|data-url|content)=[\'"]?([^\'" >]+)', html_scan):
        _adicionar_url(encontrados, attr_url, url_base)
    for srcset in re.findall(r'(?:srcset|data-srcset)=[\'"]([^\'"]+)', html_scan):
        for src in _parse_srcset(srcset):
            _adicionar_url(encontrados, src, url_base)
    for css_url in re.findall(r"url\(([^)]+)\)", html_scan):
        _adicionar_url(encontrados, css_url, url_base)
    for raw_url in re.findall(r"https?://[^\s'\"<>\\)]+", html_scan):
        _adicionar_url(encontrados, raw_url, url_base)

    cands, imgs, vistos = [], [], set()
    for url_abs, texto_link in encontrados:
        if url_abs in vistos:
            continue
        vistos.add(url_abs)
        low = url_abs.lower()
        if "registro" in low and "tse" in low:
            continue
        if any(x in low for x in ("facebook.com", "twitter.com", "x.com/share", "whatsapp://")):
            continue
        if _parece_imagem_candidata(url_abs) and "logo" not in low and "avatar" not in low:
            imgs.append(url_abs)
        elif _parece_pdf_candidato(url_abs, texto_link):
            cands.append(url_abs)
    return cands[:MAX_PDF_CANDIDATOS], imgs


def _baixar_pdf_candidato(url, referer=""):
    try:
        r = requests.get(url, headers=_headers_com_referer(referer), timeout=45, allow_redirects=True)
        if r.status_code != 200:
            return None
        content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        content_disp = (r.headers.get("Content-Disposition") or "").lower()
        if (content_type == "application/pdf" or r.content[:4] == b"%PDF" or
                ("pdf" in content_disp and r.content[:4] == b"%PDF")):
            return r.content
    except requests.exceptions.RequestException:
        return None
    return None


def _chrome_cmd():
    env = os.getenv("CHROME_BIN")
    if env:
        return env
    for candidato in ("google-chrome-stable", "google-chrome", "chromium-browser", "chromium"):
        caminho = shutil.which(candidato)
        if caminho:
            return caminho
    mac = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.exists(mac):
        return mac
    return "google-chrome-stable"


def _chrome_args_base():
    chrome = _chrome_cmd()
    if not os.path.exists(chrome) and not shutil.which(chrome):
        raise RuntimeError(
            "Chrome não encontrado. Instale google-chrome-stable no GitHub Actions "
            "ou defina CHROME_BIN com o caminho do binário."
        )
    return [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-extensions",
        "--no-first-run",
        "--hide-scrollbars",
        "--lang=pt-BR",
        "--window-size=1365,1800",
        "--run-all-compositor-stages-before-draw",
        f"--virtual-time-budget={CHROME_VIRTUAL_TIME_MS}",
        f"--user-agent={STEALTH_HEADERS['User-Agent']}",
    ]


def _renderizar_dom_headless(url):
    try:
        r = subprocess.run(
            _chrome_args_base() + ["--dump-dom", url],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=90,
        )
        html = r.stdout.decode("utf-8", errors="ignore").strip()
        return html if len(html) > 200 else ""
    except Exception:
        return ""


def _meta_content(soup, seletores):
    for seletor in seletores:
        tag = soup.select_one(seletor)
        if not tag:
            continue
        val = tag.get("content") or tag.get("datetime") or tag.get_text(" ", strip=True)
        if val:
            return val.strip()
    return ""


def _limpar_sopa_noticia(soup):
    remover_tags = "script style iframe svg canvas form button input select textarea".split()
    for tag in list(soup.find_all(remover_tags)):
        tag.decompose()

    seletores = [
        "nav", "header", "footer", "aside", "[role='navigation']", "[role='banner']",
        "[role='contentinfo']", "[role='search']", "[aria-modal='true']",
        ".cookie", "#cookie", ".cookies", "#cookies", ".lgpd", "#lgpd",
        ".advertisement", ".ads", ".ad", ".publicidade", "#publicidade",
        ".newsletter", ".modal", ".popup", ".share", ".social", ".related",
        ".relacionadas", ".relacionados", ".recomendadas", ".recomendados",
        ".sidebar", ".menu", ".breadcrumb", ".comments", "#comments",
    ]
    for seletor in seletores:
        for tag in list(soup.select(seletor)):
            tag.decompose()

    ruim = re.compile(
        r"\b(cookie|cookies|lgpd|banner|popup|modal|newsletter|share|social|"
        r"publicidade|advertisement|ads|related|relacionad[oa]s?|recomendad[oa]s?|"
        r"sidebar|menu|breadcrumb|comment|comentario)\b",
        flags=re.I,
    )
    for tag in list(soup.find_all(True)):
        if not getattr(tag, "attrs", None):
            continue
        ident = " ".join([
            str(tag.get("id") or ""),
            " ".join(tag.get("class") or []),
            str(tag.get("role") or ""),
            str(tag.get("aria-label") or ""),
        ])
        style = str(tag.get("style") or "").lower().replace(" ", "")
        if ruim.search(ident) or "position:fixed" in style or "position:sticky" in style:
            tag.decompose()


def _absolutizar_midia(no, url_base):
    for tag in no.find_all(True):
        if tag.name == "img":
            src = tag.get("src") or tag.get("data-src") or tag.get("data-original") or tag.get("data-lazy-src")
            if not src:
                srcset = tag.get("srcset") or tag.get("data-srcset")
                src = list(_parse_srcset(srcset))[-1] if srcset else ""
            if src:
                src_abs = _normalizar_url_extraida(src, url_base)
                tag["src"] = _imagem_data_uri(src_abs, referer=url_base) or src_abs
            for attr in list(tag.attrs):
                if attr != "src" and (attr.startswith("data-") or attr in ("srcset", "loading", "sizes")):
                    del tag[attr]
        elif tag.name == "a" and tag.get("href"):
            tag["href"] = _normalizar_url_extraida(tag.get("href"), url_base)
        if tag.get("style"):
            del tag["style"]


def _score_conteudo(no):
    texto = no.get_text(" ", strip=True)
    return len(texto) + 120 * len(no.find_all("p")) + 80 * len(no.find_all("img"))


def _texto_html_simples(html_fonte):
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_fonte or "", "html.parser")
        for tag in soup.find_all("script style noscript".split()):
            tag.decompose()
        return soup.get_text(" ", strip=True)
    except Exception:
        return re.sub(r"<[^>]+>", " ", html_fonte or "")


def _tem_bloqueio_conteudo(html_fonte, texto_extra=""):
    texto = " ".join([_texto_html_simples(html_fonte), texto_extra or ""])
    texto_norm = _norm(texto)
    if not texto_norm:
        return False
    achou_marker = any(_norm(m) in texto_norm for m in PAYWALL_MARKERS)
    texto_curto = len(texto.strip()) < 1200
    poucos_paragrafos = len(re.findall(r"<p\b", html_fonte or "", flags=re.I)) < 3
    return achou_marker and (texto_curto or poucos_paragrafos)


def _tamanho_texto_pdf(pdf_bytes):
    return len((_texto_pdf(pdf_bytes) or "").strip())


def _juntar_pdfs(*pdfs):
    pdfs = [p for p in pdfs if p]
    if len(pdfs) <= 1:
        return pdfs[0] if pdfs else b""
    try:
        import fitz  # PyMuPDF
        out = fitz.open()
        for pdf in pdfs:
            with fitz.open(stream=pdf, filetype="pdf") as doc:
                out.insert_pdf(doc)
        return out.tobytes(garbage=4, deflate=True)
    except Exception:
        return pdfs[0]


def _html_leitura(html_fonte, url, imagens_extras=None):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_fonte or "", "html.parser")
    titulo = (
        _meta_content(soup, ["meta[property='og:title']", "meta[name='twitter:title']"]) or
        (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "") or
        (soup.title.get_text(" ", strip=True) if soup.title else "") or
        "Notícia"
    )
    data_pub = _meta_content(soup, [
        "meta[property='article:published_time']", "meta[name='date']",
        "meta[itemprop='datePublished']", "time[datetime]", "time"
    ])

    _limpar_sopa_noticia(soup)
    candidatos = soup.select(
        "article, main, [role='main'], [itemprop='articleBody'], "
        ".article, .article-body, .article-content, .entry-content, .post-content, "
        ".materia, .noticia, .content, #content"
    )
    if not candidatos:
        candidatos = [soup.body or soup]
    conteudo = max(candidatos, key=_score_conteudo)
    if _score_conteudo(conteudo) < 500 and soup.body:
        conteudo = soup.body
    _absolutizar_midia(conteudo, url)
    imagens_no_corpo = {img.get("src") for img in conteudo.find_all("img") if img.get("src")}
    extras = []
    for img_url in imagens_extras or []:
        img_abs = _normalizar_url_extraida(img_url, url)
        if img_abs and img_abs not in imagens_no_corpo:
            extras.append(img_abs)
            imagens_no_corpo.add(img_abs)
    corpo = str(conteudo)
    extras_html = ""
    if extras:
        imagens = "\n".join(
            f'<img src="{html_lib.escape(_imagem_data_uri(img, referer=url) or img)}" alt="Imagem extraída da página">'
            for img in extras[:MAX_IMAGENS_OCR]
        )
        extras_html = f"<section><h2>Imagens da página</h2>{imagens}</section>"

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <base href="{html_lib.escape(url)}">
  <title>{html_lib.escape(titulo)}</title>
  <style>
    @page {{ margin: 18mm 16mm; }}
    body {{ font-family: Arial, Helvetica, sans-serif; color: #111; line-height: 1.48; }}
    main {{ max-width: 760px; margin: 0 auto; }}
    h1 {{ font-size: 25px; line-height: 1.18; margin: 0 0 8px; }}
    .meta {{ color: #555; font-size: 12px; margin: 0 0 18px; }}
    p, li {{ font-size: 14px; }}
    img {{ max-width: 100%; height: auto; display: block; margin: 14px auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 5px; }}
    a {{ color: #111; text-decoration: none; }}
  </style>
</head>
<body>
  <main>
    <h1>{html_lib.escape(titulo)}</h1>
    <div class="meta">Fonte: {html_lib.escape(url)}{(" · " + html_lib.escape(data_pub)) if data_pub else ""}</div>
    {corpo}
    {extras_html}
  </main>
</body>
</html>"""


def _imprimir_url_pdf(url):
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        pdf_temp_path = tmp.name
    try:
        subprocess.run(
            _chrome_args_base() + ["--no-pdf-header-footer", f"--print-to-pdf={pdf_temp_path}", url],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
        )
        with open(pdf_temp_path, "rb") as f:
            return f.read()
    finally:
        if os.path.exists(pdf_temp_path):
            os.remove(pdf_temp_path)


def _html_para_pdf(html_doc):
    with tempfile.NamedTemporaryFile(suffix=".html", mode="w", encoding="utf-8", delete=False) as html_tmp:
        html_tmp.write(html_doc)
        html_path = html_tmp.name
    try:
        return _imprimir_url_pdf(Path(html_path).as_uri())
    finally:
        if os.path.exists(html_path):
            os.remove(html_path)


def _selenium_options(download_dir):
    from selenium import webdriver
    opts = webdriver.ChromeOptions()
    chrome = _chrome_cmd()
    if os.path.exists(chrome):
        opts.binary_location = chrome
    for arg in _chrome_args_base()[1:]:
        if arg.startswith("--virtual-time-budget"):
            continue
        opts.add_argument(arg)
    opts.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
    })
    return opts


def _selenium_aceitar_cookies(driver):
    try:
        elems = driver.find_elements("css selector", "button, a, [role='button'], input[type='button'], input[type='submit']")
    except Exception:
        return
    clicados = 0
    for el in elems:
        try:
            if not el.is_displayed():
                continue
            texto = " ".join([
                el.text or "",
                el.get_attribute("aria-label") or "",
                el.get_attribute("value") or "",
                el.get_attribute("title") or "",
            ]).strip()
            if 2 <= len(texto) <= 90 and COOKIE_ACCEPT_RE.search(texto):
                driver.execute_script("arguments[0].click();", el)
                clicados += 1
                time.sleep(0.6)
                if clicados >= 3:
                    break
        except Exception:
            continue


def _selenium_scroll(driver):
    try:
        altura = driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);") or 0
        passos = min(10, max(3, int(altura // 900) + 1))
        for i in range(passos):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight * arguments[0] / arguments[1]);", i + 1, passos)
            time.sleep(0.7)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)
    except Exception:
        pass


def _selenium_pdf_da_pagina(driver):
    try:
        data = driver.execute_cdp_cmd("Page.printToPDF", {
            "printBackground": True,
            "preferCSSPageSize": True,
            "marginTop": 0.45,
            "marginBottom": 0.45,
            "marginLeft": 0.4,
            "marginRight": 0.4,
        }).get("data")
        return base64.b64decode(data) if data else b""
    except Exception:
        return b""


def _selenium_blob_pdfs(driver):
    script = """
    const done = arguments[0];
    (async () => {
      const out = [];
      const links = Array.from(document.querySelectorAll('a[href^="blob:"]'));
      for (const a of links.slice(0, 20)) {
        try {
          const resp = await fetch(a.href);
          const blob = await resp.blob();
          const data = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(String(reader.result).split(',')[1] || '');
            reader.onerror = reject;
            reader.readAsDataURL(blob);
          });
          out.push({mime: blob.type || '', data});
        } catch (e) {}
      }
      done(out);
    })();
    """
    pdfs = []
    try:
        blobs = driver.execute_async_script(script)
        for item in blobs or []:
            data = item.get("data") if isinstance(item, dict) else ""
            if not data:
                continue
            b = base64.b64decode(data)
            if b[:4] == b"%PDF" or "pdf" in str(item.get("mime", "")).lower():
                pdfs.append(b)
    except Exception:
        pass
    return pdfs


def _selenium_clicar_downloads(driver, download_dir, referer=""):
    pdfs = []
    try:
        elems = driver.find_elements("css selector", "a, button, [role='button'], input[type='button'], input[type='submit']")
    except Exception:
        return pdfs
    clicados = 0
    janela_base = driver.current_window_handle
    for el in elems:
        if clicados >= 12:
            break
        try:
            texto = " ".join([
                el.text or "",
                el.get_attribute("aria-label") or "",
                el.get_attribute("title") or "",
                el.get_attribute("download") or "",
                el.get_attribute("href") or "",
                el.get_attribute("value") or "",
            ])
            texto_norm = _norm(texto)
            if not any(_norm(h) in texto_norm for h in PDF_LINK_HINTS):
                continue
            if el.is_displayed():
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", el)
                clicados += 1
                time.sleep(1.2)
                for handle in list(driver.window_handles):
                    if handle != janela_base:
                        driver.switch_to.window(handle)
                        time.sleep(0.8)
                        try:
                            pdfs.extend(_selenium_blob_pdfs(driver))
                            if str(driver.current_url or "").startswith("http"):
                                b = _baixar_pdf_candidato(driver.current_url, referer=referer)
                                if b:
                                    pdfs.append(b)
                        except Exception:
                            pass
                        driver.close()
                        driver.switch_to.window(janela_base)
        except Exception:
            try:
                driver.switch_to.window(janela_base)
            except Exception:
                pass
            continue

    # Aguarda downloads terminarem.
    fim = time.time() + 20
    while time.time() < fim:
        try:
            pendentes = list(Path(download_dir).glob("*.crdownload"))
            if not pendentes:
                return pdfs
        except Exception:
            return pdfs
        time.sleep(0.8)
    return pdfs


def _pdfs_baixados(download_dir):
    pdfs = []
    try:
        for caminho in Path(download_dir).glob("*"):
            if not caminho.is_file() or caminho.suffix == ".crdownload":
                continue
            try:
                b = caminho.read_bytes()
            except Exception:
                continue
            if b[:4] == b"%PDF" or caminho.suffix.lower() == ".pdf":
                pdfs.append(b)
    except Exception:
        pass
    return pdfs


def _capturar_com_selenium(url):
    if str(os.getenv("USAR_SELENIUM", "1")).strip().lower() in ("0", "false", "nao", "não"):
        return {"html": "", "pdfs": [], "page_pdf": b"", "erro": ""}
    download_dir = tempfile.mkdtemp(prefix="relatorios_downloads_")
    driver = None
    try:
        from selenium import webdriver
        opts = _selenium_options(download_dir)
        driver = webdriver.Chrome(options=opts)
        try:
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
        except Exception:
            pass
        driver.set_page_load_timeout(60)
        driver.get(url)
        time.sleep(2)
        _selenium_aceitar_cookies(driver)
        _selenium_scroll(driver)
        html = driver.page_source or ""
        page_pdf = _selenium_pdf_da_pagina(driver)
        pdfs = _selenium_blob_pdfs(driver)
        pdfs.extend(_selenium_clicar_downloads(driver, download_dir, referer=url))
        pdfs.extend(_selenium_blob_pdfs(driver))
        pdfs.extend(_pdfs_baixados(download_dir))
        return {"html": html, "pdfs": pdfs, "page_pdf": page_pdf, "erro": ""}
    except Exception as e:
        return {"html": "", "pdfs": [], "page_pdf": b"", "erro": str(e)}
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        shutil.rmtree(download_dir, ignore_errors=True)


def baixar_pdf_ou_gerar_headless(url, registro="", uf="", instituto="", gemini_client=None):
    """
    Tenta achar o PDF do relatório da pesquisa certa na página. Se a página tem
    vários PDFs, escolhe o que confere com a pesquisa e, entre os que conferem,
    o maior (mais completo). Se nada servir, renderiza a notícia com Chrome,
    limpa a página e imprime um PDF de leitura. Imagens internas ajudam na
    conferência por OCR/visão. Retorna (pdf_bytes, situacao), onde situacao é
    'ok'/'provavel'/'teaser'/'imagem'/'nao'/'paywall'/'bloqueado'/'erro_chrome'.
    """
    htmls = []
    pdfs_selenium = []
    pdf_pagina_selenium = b""
    erro_selenium = ""
    try:
        req = requests.get(url, headers=STEALTH_HEADERS, timeout=30)
        req.raise_for_status()

        # PDF direto
        content_type = (req.headers.get('Content-Type') or "").split(";")[0].strip().lower()
        if content_type == 'application/pdf' or _url_tem_extensao(url, (".pdf",)):
            b = req.content
            if content_type == 'application/pdf' or b[:4] == b"%PDF":
                return b, confere_pesquisa(b, registro, uf, instituto)

        # Imagem direta: confere por visão/OCR e encapsula em PDF para salvar no Drive.
        mime_type = _mime_imagem(req, url)
        if mime_type:
            b = req.content
            sit = confere_pesquisa_imagem(b, mime_type, registro, uf, instituto, gemini_client)
            return _imagem_para_pdf(b), sit

        htmls.append(req.text or "")

    except requests.exceptions.RequestException:
        pass  # 403 etc.: ainda tenta renderizar com Chrome

    captura_selenium = _capturar_com_selenium(url)
    if captura_selenium.get("html"):
        htmls.append(captura_selenium["html"])
    pdfs_selenium = captura_selenium.get("pdfs") or []
    pdf_pagina_selenium = captura_selenium.get("page_pdf") or b""
    erro_selenium = captura_selenium.get("erro") or ""

    html_renderizado = _renderizar_dom_headless(url)
    if html_renderizado:
        htmls.append(html_renderizado)

    html_total = "\n".join(h for h in htmls if h)
    texto_imagens_pagina = ""
    if html_total:
        # PDFs podem vir sem .pdf no link, por botão JS ou blob. Aceita só bytes PDF.
        cands, imgs = _extrair_links_documentos(html_total, url)
        # Prioridade por confiança primeiro (registro > OCR/leitura > UF+instituto+ano),
        # tamanho só desempata dentro do mesmo nível. Sem isso um PDF "provavel" maior
        # podia vencer um PDF "ok" menor, escondendo o match certo.
        prioridade = {"ok": 3, "imagem": 2, "provavel": 1}
        melhor, melhor_sit, melhor_rank = None, None, (-1, -1)
        for b in pdfs_selenium:
            if not b or b[:4] != b"%PDF":
                continue
            sit = confere_pesquisa(b, registro, uf, instituto)
            if registro and sit == "nao":
                continue
            rank = (prioridade.get(sit, 0), len(b))
            if rank > melhor_rank:
                melhor, melhor_sit, melhor_rank = b, sit, rank
        for lp in cands:
            b = _baixar_pdf_candidato(lp, referer=url)
            if not b:
                continue
            sit = confere_pesquisa(b, registro, uf, instituto)
            if registro and sit == "nao":
                continue  # esse PDF não é desta pesquisa
            rank = (prioridade.get(sit, 0), len(b))
            if rank > melhor_rank:
                melhor, melhor_sit, melhor_rank = b, sit, rank
        if melhor is not None:
            return melhor, melhor_sit

        texto_imagens_pagina = _texto_imagens_da_pagina(gemini_client, imgs, registro, uf, instituto, referer=url)
        if _tem_bloqueio_conteudo(html_total, texto_imagens_pagina):
            return b"", "paywall"

        try:
            html_leitura = _html_leitura(htmls[-1], url, imgs)
            pdf_limpo = _html_para_pdf(html_leitura)
            situacao_limpo = confere_pesquisa_com_texto_extra(
                pdf_limpo, registro, uf, instituto, texto_imagens_pagina)
            pdf_original = pdf_pagina_selenium or b""
            if not pdf_original:
                try:
                    pdf_original = _imprimir_url_pdf(url)
                except Exception:
                    pdf_original = b""
            situacao_original = (
                confere_pesquisa_com_texto_extra(pdf_original, registro, uf, instituto, texto_imagens_pagina)
                if pdf_original else "nao"
            )
            if situacao_limpo in ("ok", "provavel", "imagem"):
                if (pdf_original and situacao_original != "nao" and
                        _tamanho_texto_pdf(pdf_original) > max(1200, int(_tamanho_texto_pdf(pdf_limpo) * 1.35))):
                    return _juntar_pdfs(pdf_limpo, pdf_original), situacao_limpo
                return pdf_limpo, situacao_limpo
            if situacao_original != "nao" and pdf_original:
                return pdf_original, situacao_original
        except Exception:
            pass

    # Último fallback: imprime a página original, mas com espera para JS/lazy load.
    try:
        pdf_bytes = pdf_pagina_selenium or _imprimir_url_pdf(url)
    except Exception as e:
        if "chrome" in str(e).lower() or erro_selenium:
            return b"", "erro_chrome"
        raise
    return pdf_bytes, confere_pesquisa_com_texto_extra(
        pdf_bytes, registro, uf, instituto, texto_imagens_pagina)


def resolver_pasta_drive(creds, cargo, uf_extenso):
    """Retorna o ID da subpasta correta, criando-a se necessário"""
    headers = {'Authorization': f'Bearer {creds.token}'}
    pasta_mae = PASTA_PRESIDENCIAVEIS if "presidente" in cargo.lower() else PASTA_GOV_SEN

    uf_extenso_limpo = uf_extenso.strip().upper()
    if "presidente" in cargo.lower() and uf_extenso_limpo == "BRASIL":
        nome_subpasta = "Nacional"
    else:
        nome_subpasta = MAPA_UF.get(uf_extenso_limpo, uf_extenso_limpo)

    query = f"name='{nome_subpasta}' and '{pasta_mae}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    r = requests.get(
        "https://www.googleapis.com/drive/v3/files",
        params={"q": query, "corpora": "drive", "driveId": DRIVE_ID, "includeItemsFromAllDrives": "true", "supportsAllDrives": "true"},
        headers=headers
    ).json()

    if r.get("files"):
        return r["files"][0]["id"]

    meta = {'name': nome_subpasta, 'parents': [pasta_mae], 'mimeType': 'application/vnd.google-apps.folder'}
    r_create = requests.post(
        "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true",
        headers=headers, json=meta
    ).json()
    return r_create["id"]


def fazer_upload_drive(creds, pasta_id, nome_arquivo, pdf_bytes, nomes_existentes=None):
    headers = {'Authorization': f'Bearer {creds.token}'}
    nomes_busca = [nome_arquivo] + [n for n in (nomes_existentes or []) if n and n != nome_arquivo]

    for nome_busca in nomes_busca:
        query = f"name='{nome_busca}' and '{pasta_id}' in parents and trashed=false"
        r = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            params={"q": query, "corpora": "drive", "driveId": DRIVE_ID, "includeItemsFromAllDrives": "true", "supportsAllDrives": "true", "fields": "files(id, name, webViewLink)"},
            headers=headers
        ).json()

        if r.get("files"):
            arquivo = r["files"][0]
            if arquivo.get("name") != nome_arquivo:
                r_rename = requests.patch(
                    f"https://www.googleapis.com/drive/v3/files/{arquivo['id']}?supportsAllDrives=true&fields=id,name,webViewLink",
                    headers=headers,
                    json={"name": nome_arquivo},
                )
                if r_rename.ok:
                    return r_rename.json().get("webViewLink", arquivo.get("webViewLink", ""))
            return arquivo["webViewLink"]  # Já existe

    meta = {'name': nome_arquivo, 'parents': [pasta_id]}
    files = {
        'metadata': ('m', json.dumps(meta), 'application/json'),
        'file': (nome_arquivo, pdf_bytes, 'application/pdf')
    }
    r_upload = requests.post(
        'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true&fields=id,name,webViewLink',
        headers=headers, files=files
    )
    return r_upload.json().get('webViewLink', '')


def _eh_link_drive(link):
    """Confere se o link É do Google Drive antes de tratar como tal - checagem só de
    domínio, sem regex frouxo. Necessária porque _drive_file_id tem um fallback
    ([-\\w]{25,}) que casa qualquer slug longo de URL comum (site de notícia com
    título extenso na URL), dando falso positivo pra link que não é do Drive."""
    if not link:
        return False
    host = urlparse(str(link)).netloc.lower()
    return host in ("drive.google.com", "docs.google.com") or host.endswith(".drive.google.com")


def _drive_file_id(link):
    if not link:
        return ""
    parsed = urlparse(str(link))
    match = re.search(r"/file/d/([^/]+)", parsed.path)
    if match:
        return match.group(1)
    query_id = parse_qs(parsed.query).get("id", [""])[0]
    if query_id:
        return query_id
    match = re.search(r"[-\w]{25,}", str(link))
    return match.group(0) if match else ""


def renomear_arquivo_drive_por_link(creds, link, nome_arquivo):
    file_id = _drive_file_id(link)
    if not file_id:
        return False

    headers = {'Authorization': f'Bearer {creds.token}'}
    try:
        r_atual = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            params={"supportsAllDrives": "true", "fields": "id,name,webViewLink"},
            headers=headers,
            timeout=30,
        )
        if not r_atual.ok:
            return False
        atual = r_atual.json()
        if atual.get("name") == nome_arquivo:
            return False

        r_rename = requests.patch(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            params={"supportsAllDrives": "true", "fields": "id,name,webViewLink"},
            headers=headers,
            json={"name": nome_arquivo},
            timeout=30,
        )
        return r_rename.ok
    except requests.RequestException:
        return False


def normalizar_nome_arquivo(registro, data_div, cargo=""):
    reg_limpo = registro.replace("/", "-")
    cargo_limpo = re.sub(r"[^A-Za-z0-9]+", "-", _sem_acento(cargo).strip()).strip("-")
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", str(data_div))
    data_iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else data_div
    if cargo_limpo:
        return f"{reg_limpo}_{cargo_limpo}_{data_iso}.pdf"
    return f"{reg_limpo}_{data_iso}.pdf"


def _data_iso_origem(valor):
    s = str(valor or "").strip()
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return ""


def _dias_desde_divulgacao(data_divulgacao):
    """Dias entre a data de divulgação e hoje (BRT). None se a data não for legível
    (nesse caso o chamador NÃO deve pular a linha, pra não descartar por falha de parse)."""
    iso = _data_iso_origem(data_divulgacao)
    if not iso:
        return None
    try:
        d = datetime.strptime(iso, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (datetime.now(BRT).date() - d).days


def _origem_com_carimbo(origem, data_divulgacao=""):
    origem = str(origem or "").strip() or "Capturado na Web"
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")
    partes = [origem, f"puxado em {agora}"]
    data_prevista = _data_iso_origem(data_divulgacao)
    hoje = datetime.now(BRT).strftime("%Y-%m-%d")
    if data_prevista and data_prevista != hoje:
        partes.append(f"divulgação prevista {data_prevista}")
    return " | ".join(partes)


def atualizar_planilha():
    print("Iniciando automação de busca de relatórios...")
    creds = obter_credenciais()
    gemini_client = obter_cliente_gemini()

    sheet_id = os.getenv("SPREADSHEET_ID_RELATORIOS")
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID_RELATORIOS ausente nos Secrets.")

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("relatorios")

    header = _normalizar_cabecalho_relatorios(ws)
    col_link = _garantir_coluna_relatorios(ws, header, "link")
    col_status = _garantir_coluna_relatorios(ws, header, COL_ORIGEM_LINK)
    col_nivel = _garantir_coluna_relatorios(ws, header, COL_NIVEL_CONFERENCIA)
    _separar_linhas_multicargo(ws, header)
    _resetar_validacoes_relatorios(ws, header, _ultima_linha_com_registro(ws))

    dados = _rel_records(ws)
    primeiro_cargo_por_registro = {}
    for r in dados:
        reg_norm = re.sub(r"[^A-Z0-9]", "", str(r.get("registro", "")).upper())
        if reg_norm and reg_norm not in primeiro_cargo_por_registro:
            primeiro_cargo_por_registro[reg_norm] = _cargo_norm(r.get("cargo", ""))

    pdfs_salvos = 0
    links_preenchidos = 0
    pendentes_finais = []
    celulas_para_atualizar = []

    # Grava em lotes ao longo da rodada, não só no fim: se o run cair no meio
    # (timeout, Gemini travar), o que já foi baixado e escrito fica salvo, em vez
    # de perder tudo e ter que refazer download/Gemini na próxima rodada.
    LOTE_CELULAS = 60      # ~20 linhas (cada linha escreve link/status/nivel)

    def _gravar_celulas_pendentes(contexto=""):
        if not celulas_para_atualizar:
            return
        detalhe = f" ({contexto})" if contexto else ""
        print(f"Gravando {len(celulas_para_atualizar)} atualização(ões) na planilha{detalhe}...")
        ws.update_cells(celulas_para_atualizar, value_input_option="USER_ENTERED")
        celulas_para_atualizar.clear()

    # PROCESSAR LINHAS SEM LINK: busca e salva o PDF de cada pesquisa que já está
    # na fila (adicionada pelo relatorios_pipeline.py). Não adiciona linha nenhuma.
    print(f"\n--- Buscando PDFs das pesquisas na fila ({len(dados)} linhas analisadas) ---")
    for i, linha in enumerate(dados, start=2):  # +2: sheets começa em 1 e tem cabeçalho
        if len(celulas_para_atualizar) >= LOTE_CELULAS:
            _gravar_celulas_pendentes("lote parcial")
        registro = str(linha.get("registro", "")).strip()
        link_atual = str(linha.get("link", "")).strip()
        if not registro:
            continue
        if _cargo_norm(linha.get("cargo", "")) not in CARGOS_MONITORADOS:
            continue
        if link_atual and _eh_link_drive(link_atual):
            # Já é link do Drive: só normaliza o nome (não precisa baixar/converter,
            # o arquivo já está lá). Só na primeira vez que a linha aparece com link
            # (origem_link ainda vazio); depois de carimbada, pula nas próximas
            # rodadas, senão seria 1 GET no Drive por linha toda rodada só pra
            # reconferir um nome que já está certo.
            if not str(linha.get(COL_ORIGEM_LINK, "")).strip():
                nome_pdf = normalizar_nome_arquivo(registro, str(linha.get("data_divulgacao", "")), linha.get("cargo", ""))
                if renomear_arquivo_drive_por_link(creds, link_atual, nome_pdf):
                    print(f"  [OK] Arquivo renomeado no Drive: {nome_pdf}")
                celulas_para_atualizar.append(gspread.Cell(
                    i, col_status,
                    _origem_com_carimbo("link já existente na fila", linha.get("data_divulgacao", "")),
                ))
                if not str(linha.get(COL_NIVEL_CONFERENCIA, "")).strip():
                    celulas_para_atualizar.append(gspread.Cell(i, col_nivel, "link_existente"))
            continue

        if link_atual:
            # Link colado manualmente na fila (site, matéria) que NÃO é do Drive:
            # antes disso caía no ramo acima, que só tenta renomear um arquivo do
            # Drive - _drive_file_id vinha vazio, renomear_arquivo_drive_por_link
            # falhava calado, e a linha ainda assim era carimbada como "resolvida"
            # sem nada ter sido baixado/convertido/salvo. Agora cai no MESMO fluxo
            # de baixar/converter/subir pro Drive usado pro link que a IA acha
            # sozinha (baixar_pdf_ou_gerar_headless mais abaixo), só sem precisar
            # buscar - o link já está na célula.
            if str(linha.get(COL_ORIGEM_LINK, "")).strip():
                continue   # já processado numa rodada anterior
            link_fonte = link_atual
            origem_texto = "Link inserido manualmente"
        else:
            dias = _dias_desde_divulgacao(linha.get("data_divulgacao", ""))
            if dias is not None and dias > MAX_DIAS_BUSCA:
                # divulgada há mais de MAX_DIAS_BUSCA: não vale mais buscar (nem seria
                # divulgada). Carimba uma vez (só se origem ainda vazia) pra a equipe
                # saber o motivo de a linha não ter link, e pula em silêncio nas
                # próximas rodadas.
                if not str(linha.get(COL_ORIGEM_LINK, "")).strip():
                    celulas_para_atualizar.append(gspread.Cell(
                        i, col_status,
                        _origem_com_carimbo(f"não buscado: divulgada há mais de {MAX_DIAS_BUSCA} dias",
                                            linha.get("data_divulgacao", "")),
                    ))
                    if not str(linha.get(COL_NIVEL_CONFERENCIA, "")).strip():
                        celulas_para_atualizar.append(gspread.Cell(i, col_nivel, "fora_da_janela"))
                continue

            print(f"Buscando: {registro} / {linha.get('cargo', '')}...")
            resultado = agente_buscar_link_faltante(
                gemini_client, registro, linha.get("instituto", ""),
                linha.get("cargo", ""), linha.get("uf", ""), linha.get("data_divulgacao", "")
            )

            if resultado.get("tipo") == "paywall" and not resultado.get("link"):
                pendentes_finais.append(f"{registro} - Paywall, sem matéria aberta localizada.")
                celulas_para_atualizar.append(gspread.Cell(
                    i, col_status,
                    _origem_com_carimbo("paywall, aguardando fonte aberta", linha.get("data_divulgacao", "")),
                ))
                celulas_para_atualizar.append(gspread.Cell(i, col_nivel, "paywall"))
                continue

            link_fonte = resultado.get("link")
            if not link_fonte:
                pendentes_finais.append(f"{registro} - Nenhum relatório ou matéria encontrada na web hoje.")
                continue
            origem_texto = resultado.get("origem_texto", "Capturado na Web")

        if _fonte_video(link_fonte):
            pendentes_finais.append(f"{registro} - Fonte é vídeo ({link_fonte}); sem relatório legível, deixado pendente.")
            celulas_para_atualizar.append(gspread.Cell(i, col_link, link_fonte))
            celulas_para_atualizar.append(gspread.Cell(
                i, col_status,
                _origem_com_carimbo("fonte é vídeo, aguardando relatório/matéria", linha.get("data_divulgacao", "")),
            ))
            celulas_para_atualizar.append(gspread.Cell(i, col_nivel, "nao"))
            continue

        try:
            pdf_bytes, situacao = baixar_pdf_ou_gerar_headless(
                link_fonte, registro, linha.get("uf", ""), linha.get("instituto", ""), gemini_client)
            if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
                situacao = "bloqueado"
            celulas_para_atualizar.append(gspread.Cell(i, col_nivel, situacao))
            if situacao in SITUACOES_PENDENTES:
                pendentes_finais.append(_mensagem_pendente(registro, situacao))
                celulas_para_atualizar.append(gspread.Cell(i, col_link, link_fonte))
                celulas_para_atualizar.append(gspread.Cell(
                    i, col_status,
                    _origem_com_carimbo(
                        "verificar fonte" + _nota_conferencia(situacao),
                        linha.get("data_divulgacao", ""),
                    ),
                ))
                continue
            pasta_id = resolver_pasta_drive(creds, linha.get("cargo", ""), linha.get("uf", ""))
            nome_pdf = normalizar_nome_arquivo(registro, str(linha.get("data_divulgacao", "")), linha.get("cargo", ""))
            nomes_legado = []
            reg_norm = re.sub(r"[^A-Z0-9]", "", registro.upper())
            if primeiro_cargo_por_registro.get(reg_norm) == _cargo_norm(linha.get("cargo", "")):
                nomes_legado.append(normalizar_nome_arquivo(registro, str(linha.get("data_divulgacao", ""))))
            link_drive = fazer_upload_drive(creds, pasta_id, nome_pdf, pdf_bytes, nomes_existentes=nomes_legado)

            nota = _nota_conferencia(situacao)
            celulas_para_atualizar.append(gspread.Cell(i, col_link, link_drive))
            celulas_para_atualizar.append(gspread.Cell(
                i, col_status,
                _origem_com_carimbo(
                    origem_texto + nota,
                    linha.get("data_divulgacao", ""),
                ),
            ))
            pdfs_salvos += 1
            links_preenchidos += 1
            print(f"  [OK] Salvo no Drive: {nome_pdf}{nota}")
        except Exception as e:
            celulas_para_atualizar.append(gspread.Cell(i, col_link, link_fonte))
            celulas_para_atualizar.append(gspread.Cell(i, col_nivel, "erro_tecnico"))
            pendentes_finais.append(f"{registro} - Erro técnico ao baixar/salvar PDF: {str(e)}")

    _gravar_celulas_pendentes("fim da execução")

    # A fila de relatorios é populada só pelo relatorios_pipeline.py (a partir do PesqEle do
    # TSE, fonte oficial). O atualiza_relatorios NÃO adiciona linhas: só preenche o
    # link das que já existem. Antes havia uma "Fase 2" que buscava pesquisas novas
    # na web e anexava linhas, mas isso duplicava (UF em sigla vs por extenso) e
    # trazia lixo sem registro; foi removida de propósito.

    # Aplica/estende o checkbox 'conferido' até a última linha com pesquisa (sem
    # teto fixo e sem checkbox vazio sobrando embaixo).
    _resetar_validacoes_relatorios(ws, header, _ultima_linha_com_registro(ws))

    hoje_fmt = datetime.now(BRT).strftime("%d/%m/%Y")
    print(f"\n--- Relatório de Automação – {hoje_fmt} ---")
    print(f"* PDFs salvos no Drive: {pdfs_salvos}")
    print(f"* Links preenchidos em linhas existentes: {links_preenchidos}")
    if pendentes_finais:
        print("* Registros que seguem pendentes:")
        for pend in pendentes_finais:
            print(f"  * {pend}")
    else:
        print("* Registros que seguem pendentes: 0")


if __name__ == "__main__":
    atualizar_planilha()
