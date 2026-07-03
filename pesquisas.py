"""
Pesquisas eleitorais: alerta diário + extração de voto por segmento e rejeição.

Uso:
  python pesquisas.py alerta    # email com as pesquisas que divulgam hoje (PesqEle)
  python pesquisas.py extrair   # extrai voto por segmento e rejeição dos relatórios

Secrets: GOOGLE_CREDENTIALS_JSON, GEMINI_API_KEY, BREVO_API_KEY, EMAIL,
DESTINATARIOS, SPREADSHEET_ID (PesqEle), SPREADSHEET_ID_RELATORIOS.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import gspread
import requests
from google.oauth2.service_account import Credentials

BRT = timezone(timedelta(hours=-3))
HEADERS = {"User-Agent": "Mozilla/5.0"}

CABECALHOS = {
    "relatorios": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                   "link", "salvo_drive", "extraido", "data_extracao"],
    "voto_segmento": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                      "cenario", "candidato", "tipo_segmento", "segmento", "valor"],
    "rejeicao": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                 "candidato", "tipo_segmento", "segmento", "valor"],
    "aprovacao": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                  "alvo", "resposta", "tipo_segmento", "segmento", "valor"],
    "topline_pesquisas": ["registro_tse", "ano", "cargo", "uf", "turno",
                          "instituto", "classificacao_instituto", "data_campo",
                          "scenario_label", "descricao", "modo", "amostra", "margem_erro",
                          "confianca", "metodologia", "poll_id", "scenario_id", "fonte_url",
                          "fonte_url_original", "conferida", "horario_raspagem",
                          "validacao", "origem"],
    "topline_resultados": ["registro_tse", "ano", "cargo", "uf", "turno",
                           "instituto", "classificacao_instituto", "data_campo",
                           "scenario_label", "candidato", "partido", "candidato_partido",
                           "tipo", "percentual", "poll_id", "scenario_id", "fonte_url",
                           "horario_raspagem", "origem"],
}


def _creds_info():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    return json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))


def _sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return gspread.authorize(Credentials.from_service_account_info(_creds_info(), scopes=scopes))


def _aba(sh, nome):
    """Garante a aba e o cabeçalho. Cria a aba se não existir; escreve o
    cabeçalho se a primeira linha estiver vazia. Não mexe em dados existentes."""
    header = CABECALHOS[nome]
    try:
        ws = sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=nome, rows=1000, cols=len(header))
    if not ws.row_values(1):
        ws.update(range_name="A1", values=[header])
    return ws


def _verdadeiro(v):
    return str(v).strip().lower() in ("sim", "true", "verdadeiro", "1", "x")


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


def _marcar_origem(ws, label):
    """Adiciona 'origem' como ÚLTIMA coluna (depois de metodologia) e marca as
    linhas nossas (conferida=manual_streamlit). Não desloca coluna existente."""
    from gspread.utils import rowcol_to_a1
    header = ws.row_values(1)
    if "conferida" not in header:
        return
    col_o = _garantir_coluna(ws, header, "origem")
    i_conf, i_o = header.index("conferida"), col_o - 1
    vals = ws.get_all_values()
    if len(vals) < 2:
        return
    coluna = []
    for row in vals[1:]:
        conf = (row[i_conf] if i_conf < len(row) else "").strip().lower()
        atual = row[i_o] if i_o < len(row) else ""
        coluna.append([label if conf == "manual_streamlit" else atual])
    rng = f"{rowcol_to_a1(2, col_o)}:{rowcol_to_a1(len(vals), col_o)}"
    ws.update(range_name=rng, values=coluna)


# ─────────────────────────────── ALERTA ───────────────────────────────

PESQELE_ID = os.getenv("SPREADSHEET_ID", "")
PESQELE_ABA = os.getenv("PESQELE_ABA", "Consolidado")
PASTA_URL = os.getenv("PASTA_RELATORIOS_URL",
                      "https://drive.google.com/drive/folders/1DD3qewc6nhhdFw8x85i7qtuNRXghd9C6")
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
    """Cria na aba 'relatorios' uma linha por pesquisa de hoje (link fica manual)."""
    if not RELATORIOS_ID:
        print("SPREADSHEET_ID_RELATORIOS não definido; pulando preenchimento da fila.")
        return
    fila = _aba(_sheets().open_by_key(RELATORIOS_ID), "relatorios")
    header = fila.row_values(1)
    existentes = {str(r.get("registro", "")).strip() for r in fila.get_all_records()}
    novas = []
    for p in pesquisas:
        reg = str(p.get("numero_identificacao", "")).strip()
        if not reg or reg in existentes:
            continue
        valores = {
            "registro": reg,
            "cargo": p.get("cargos", ""),
            "uf": p.get("abrangencia", ""),
            "instituto": p.get("empresa_contratada", ""),
            "data_divulgacao": str(p.get("data_divulgacao", ""))[:10],
        }
        novas.append([valores.get(c, "") for c in header])
        existentes.add(reg)
    if novas:
        fila.append_rows(novas, value_input_option="RAW")
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
        _enviar(f"Pesquisas eleitorais previstas para hoje ({len(pesquisas)})",
                _html(pesquisas, hoje))
        _preencher_fila(pesquisas)


# ────────────────────────────── EXTRAÇÃO ──────────────────────────────

RELATORIOS_ID = os.getenv("SPREADSHEET_ID_RELATORIOS", "")
GEMINI_MODEL = "gemini-2.5-flash"

PROMPT = (
    "Você é um analista de dados de pesquisas eleitorais da Eixo. Você recebe o PDF do "
    "relatório completo de uma pesquisa e extrai os cruzamentos por segmento.\n\n"
    "Extraia TRÊS listas, em JSON:\n\n"
    "1) voto_segmento: para CADA cenário de voto estimulado e CADA candidato, o percentual "
    "de voto quebrado por segmento demográfico. "
    'Cada item: {"cenario": "...", "candidato": "Nome (PARTIDO)", "tipo_segmento": "...", "segmento": "...", "valor": número}.\n\n'
    "2) rejeicao: para CADA candidato, o percentual de rejeição quebrado por segmento. "
    'Cada item: {"candidato": "Nome (PARTIDO)", "tipo_segmento": "...", "segmento": "...", "valor": número}.\n\n'
    "3) aprovacao: APENAS aprovação/desaprovação ou avaliação do DESEMPENHO de quem está no "
    "cargo (presidente ou governador em exercício), quebrada por segmento. "
    'Cada item: {"alvo": "...", "resposta": "...", "tipo_segmento": "...", "segmento": "...", "valor": número}.\n\n'
    "Regras:\n"
    "1) Preserve os números EXATAMENTE como no relatório. Não arredonde nem recalcule.\n"
    "2) Não invente. Se um cruzamento não existir no relatório, omita.\n"
    "3) Use os rótulos de segmento como aparecem (ex: Masculino, Feminino, 16 a 24 anos, "
    "25 a 34 anos, Fundamental, Médio, Superior, Até 2 SM, Mais de 5 a 10 SM).\n"
    "4) 'tipo_segmento' classifica o segmento em uma destas categorias: genero, idade, "
    "escolaridade, renda, regiao, religiao, raca. Use exatamente esses rótulos minúsculos. "
    "Se não encaixar em nenhuma, use 'outro'. Para o total geral (sem recorte), use "
    "tipo_segmento='geral' e segmento='Total'.\n"
    "5) 'valor' é número, sem o símbolo de %.\n"
    "6) Identifique o cenário pelo nome ou título que o relatório usa (ex: 'Estimulada 1', "
    "'Lula x Flávio'). Se houver só um, use 'Estimulada'.\n"
    "7) Em 'candidato', use SEMPRE o formato 'Nome (SIGLA)'. Se o candidato constar na lista "
    "canônica fornecida abaixo, use EXATAMENTE o nome e a sigla de lá. A sigla do partido é "
    "sempre curta e em caixa alta (PT, PL, MDB, REP, UNIAO...), nunca por extenso.\n"
    "7b) Em voto_segmento, consolide as respostas inválidas (branco, nulo, não sabe, não "
    "respondeu, indeciso, nenhum) em um único candidato='Não válido' por cenário e segmento, "
    "somando os valores. Em rejeicao, mantenha as categorias de resposta como no relatório.\n"
    "8) Em aprovacao, PADRONIZE o 'alvo' assim: se for avaliação do presidente/governo "
    "federal, use SEMPRE 'Presidente <Nome>' (ex: 'Presidente Lula'), mesmo que o relatório "
    "escreva 'Governo Lula', 'Governo Federal' ou 'gestão do presidente'. Se for governador/"
    "governo estadual, use SEMPRE 'Governador <Nome>'. 'resposta' é a categoria como no "
    "relatório (ex: 'Aprova', 'Desaprova', 'Não sabe', 'Ótimo/Bom', 'Regular', 'Ruim/Péssimo').\n"
    "9) NÃO inclua em aprovacao perguntas hipotéticas ou de intenção (ex: 'gostaria que se "
    "reelegesse', 'a reeleição de X', 'a eleição de Y', desejo de candidatura). aprovacao é só "
    "avaliação do trabalho de quem já governa.\n\n"
    "Responda SOMENTE o JSON, sem texto extra e sem markdown:\n"
    '{"voto_segmento": [...], "rejeicao": [...], "aprovacao": [...]}'
)


def _drive_id(link):
    import re
    m = re.search(r"/d/([A-Za-z0-9_-]+)", link) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", link)
    return m.group(1) if m else None


def _baixar_pdf(link):
    """Baixa o PDF do link. Se for link do Google Drive, usa a API do Drive com a
    conta de serviço (a pasta precisa estar compartilhada com ela). Senão, download direto."""
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


def _blocos_pdf(pdf_bytes, tamanho=PAGINAS_POR_BLOCO):
    """Fatia o PDF em blocos de páginas. Cada página é um slide autocontido,
    então a tabela nunca se parte entre blocos."""
    import io
    from pypdf import PdfReader, PdfWriter
    reader = PdfReader(io.BytesIO(pdf_bytes))
    n = len(reader.pages)
    for ini in range(0, n, tamanho):
        writer = PdfWriter()
        for i in range(ini, min(ini + tamanho, n)):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        yield buf.getvalue()


def _gemini_json(pdf_bytes, extra=""):
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
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[prompt, types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")],
        config=config,
    )
    raw = (getattr(resp, "text", "") or "").strip()
    if not raw:
        fr = "?"
        cands = getattr(resp, "candidates", None)
        if cands:
            fr = getattr(cands[0], "finish_reason", "?")
        raise RuntimeError(f"resposta vazia ou truncada do Gemini (finish_reason={fr})")
    raw = raw.replace("```json", "").replace("```", "").strip()
    return json.loads(raw)


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
        dados = _gemini_json(bloco, extra)
        voto += dados.get("voto_segmento", [])
        rej += dados.get("rejeicao", [])
        aprov += dados.get("aprovacao", [])
    voto = _dedup(voto, ["cenario", "candidato", "tipo_segmento", "segmento"])
    rej = _dedup(rej, ["candidato", "tipo_segmento", "segmento"])
    aprov = _dedup(aprov, ["alvo", "resposta", "tipo_segmento", "segmento"])
    return {"voto_segmento": voto, "rejeicao": rej, "aprovacao": aprov}


def cmd_extrair():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    from polling_manual_core import _referencia, instituto_canonico, sigla_uf

    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = _aba(sh, "relatorios")
    ws_voto = _aba(sh, "voto_segmento")
    ws_rej = _aba(sh, "rejeicao")
    ws_aprov = _aba(sh, "aprovacao")

    linhas = fila.get_all_records()
    voto_novos, rej_novos, aprov_novos, marcar = [], [], [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")

    def _bloco_canonico(cargo_txt, uf):
        """Listas canônicas de candidatos dos cargos/UF desta pesquisa, pro prompt."""
        partes = []
        for cargo in _cargos_da_linha(cargo_txt):
            cands, _ = _referencia(cargo, uf)
            if cands:
                partes.append(f"CANDIDATOS CANÔNICOS ({cargo} {sigla_uf(uf)}):\n" + "\n".join(cands))
        return "\n\n".join(partes)

    for i, r in enumerate(linhas, start=2):   # linha 1 = cabeçalho
        link = str(r.get("link", "")).strip()
        if not link or _verdadeiro(r.get("extraido")):
            continue
        try:
            pdf = _baixar_pdf(link)
            dados = extrair_do_pdf(pdf, extra=_bloco_canonico(r.get("cargo"), r.get("uf")))
        except Exception as e:
            print(f"linha {i} ({r.get('registro')}): erro {e}")
            continue
        meta = [r.get("registro", ""), r.get("cargo", ""), sigla_uf(r.get("uf", "")),
                instituto_canonico(r.get("instituto", "")), r.get("data_divulgacao", "")]
        for v in dados.get("voto_segmento", []):
            voto_novos.append(meta + [v.get("cenario", ""), v.get("candidato", ""),
                                      v.get("tipo_segmento", ""), v.get("segmento", ""),
                                      v.get("valor", "")])
        for v in dados.get("rejeicao", []):
            rej_novos.append(meta + [v.get("candidato", ""), v.get("tipo_segmento", ""),
                                     v.get("segmento", ""), v.get("valor", "")])
        for v in dados.get("aprovacao", []):
            aprov_novos.append(meta + [v.get("alvo", ""), v.get("resposta", ""),
                                       v.get("tipo_segmento", ""), v.get("segmento", ""),
                                       v.get("valor", "")])
        marcar.append(i)
        print(f"linha {i} ({r.get('registro')}): "
              f"{len(dados.get('voto_segmento', []))} voto, "
              f"{len(dados.get('rejeicao', []))} rejeição, "
              f"{len(dados.get('aprovacao', []))} aprovação")

    if voto_novos:
        ws_voto.append_rows(voto_novos, value_input_option="RAW")
    if rej_novos:
        ws_rej.append_rows(rej_novos, value_input_option="RAW")
    if aprov_novos:
        ws_aprov.append_rows(aprov_novos, value_input_option="RAW")

    headers = fila.row_values(1)
    if marcar and "extraido" in headers:
        ci_ext = headers.index("extraido") + 1
        ci_data = headers.index("data_extracao") + 1 if "data_extracao" in headers else None
        celulas = []
        for row_i in marcar:
            celulas.append(gspread.Cell(row_i, ci_ext, "sim"))
            if ci_data:
                celulas.append(gspread.Cell(row_i, ci_data, agora))
        fila.update_cells(celulas, value_input_option="RAW")

    print(f"\n{len(marcar)} relatório(s) extraído(s).")


# ────────────────────────────── TOPLINE ──────────────────────────────
# Versão em lote do painel Polling Manual: lê os PDFs da fila e grava o voto
# estimulado (topline) na planilha do PollingData, no mesmo formato do
# `resultados`, tagueado manual (reconcilia com o oficial quando a assinatura sair).

POLLING_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA", "")
FLAG_TOPLINE = "topline_extraido"
CARGOS_POLLING = {"presidente", "governador", "senador"}


def _cargos_da_linha(valor):
    cargos = []
    for c in str(valor or "").split(","):
        c = c.strip().lower()
        if c in CARGOS_POLLING and c not in cargos:
            cargos.append(c)
    return cargos


def cmd_topline():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    import pandas as pd
    from polling_manual_core import (
        extrair_dados_polling_gemini, extrair_texto_pdf_bytes, montar_dataframes_polling,
    )

    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = sh.worksheet("relatorios")

    header = fila.row_values(1)
    col_flag = _garantir_coluna(fila, header, FLAG_TOPLINE)
    col_data = _garantir_coluna(fila, header, "topline_data_extracao")
    col_erro = _garantir_coluna(fila, header, "topline_erro")
    col_tent = _garantir_coluna(fila, header, "topline_tentativas")

    linhas = fila.get_all_records()
    todos_p, todos_r = [], []
    updates = []          # células a marcar (batch no fim, evita cota do Sheets)
    ok_regs, err_regs = [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")

    def _falha(row_i, registro, tentativas, msg):
        updates.extend([gspread.Cell(row_i, col_erro, msg[:300]),
                        gspread.Cell(row_i, col_tent, tentativas + 1)])
        err_regs.append(f"{registro} [{msg[:80]}]")
        print(f"linha {row_i} ({registro}): erro: {msg}")

    for i, r in enumerate(linhas, start=2):
        link = str(r.get("link", "")).strip()
        registro_fila = str(r.get("registro", "")).strip()
        if not link or _verdadeiro(r.get(FLAG_TOPLINE)):
            continue
        tentativas = int(r.get("topline_tentativas") or 0)
        if tentativas >= 3:   # desiste após 3 falhas; erro fica anotado na planilha
            continue
        cargos = _cargos_da_linha(r.get("cargo"))
        if not cargos:
            continue
        try:
            pdf = _baixar_pdf(link)
            texto = extrair_texto_pdf_bytes(pdf)
        except Exception as e:
            _falha(i, registro_fila, tentativas, f"baixar/ler PDF: {e}")
            continue
        if len(texto) < 200:   # scan/sem camada de texto: manda o PDF pro Gemini (visão)
            print(f"linha {i} ({registro_fila}): PDF sem texto útil, usando visão")
            texto = ""

        n_cen, avisos, houve_erro = 0, [], False
        # um mesmo PDF costuma ter 1º e 2º turno; extrai cada turno separado,
        # senão o Gemini fixa um turno só no payload e descarta o outro.
        for cargo in cargos:
            for turno in ("t1", "t2"):
                try:
                    payload = extrair_dados_polling_gemini(
                        texto, url_original=link,
                        escopo={"cargo": cargo, "turno": turno, "uf": r.get("uf", "")},
                        pdf_bytes=None if texto else pdf)
                    payload["turno"] = turno   # garante o turno pedido no rótulo/poll_id
                    # registro da fila é a fonte da verdade; avisa se o PDF divergir
                    reg_pdf = str(payload.get("registro_tse", "")).strip()
                    if reg_pdf and registro_fila and reg_pdf != registro_fila:
                        avisos.append(f"registro no PDF ({reg_pdf}) difere da fila")
                    payload["registro_tse"] = registro_fila or reg_pdf
                    # sem data de campo no PDF: usa a data de divulgação da fila
                    if not str(payload.get("data_campo", "")).strip():
                        payload["data_campo"] = str(r.get("data_divulgacao", "")).strip()
                    df_p, df_r = montar_dataframes_polling(
                        payload, fonte_url=link, instituto_fonte=r.get("instituto", ""))
                except Exception as e:
                    print(f"linha {i} ({registro_fila}) [{cargo}/{turno}]: erro na extração: {e}")
                    houve_erro = True
                    continue
                if df_r.empty:
                    continue
                # sanidade: percentual fora de 0-100 e soma do cenário fora de 85-115
                if (df_r["percentual"] < 0).any() or (df_r["percentual"] > 100).any():
                    avisos.append(f"{cargo}/{turno}: percentual fora de 0-100")
                for sid, grupo in df_r.groupby("scenario_id"):
                    soma = grupo["percentual"].sum()
                    if not 85 <= soma <= 115:
                        lbl = grupo["scenario_label"].iloc[0]
                        avisos.append(f"{cargo}/{turno} cenário {lbl}: soma {soma:.1f}")
                df_p["validacao"] = "; ".join(avisos[-3:]) if avisos else ""
                todos_p.append(df_p)
                todos_r.append(df_r)
                n_cen += len(df_p)

        if n_cen == 0 and houve_erro:
            _falha(i, registro_fila, tentativas, "todas as extrações falharam")
            continue
        updates.extend([gspread.Cell(i, col_flag, "sim"),
                        gspread.Cell(i, col_data, agora),
                        gspread.Cell(i, col_erro, "; ".join(avisos[:3]))])
        ok_regs.append(registro_fila)
        aviso_txt = f" | avisos: {'; '.join(avisos[:2])}" if avisos else ""
        print(f"linha {i} ({registro_fila}): {n_cen} cenário(s) de topline{aviso_txt}")

    def _gravar(dfs, aba_nome):
        if not dfs:
            return
        cols = CABECALHOS[aba_nome]
        df = pd.concat(dfs, ignore_index=True).reindex(columns=cols).fillna("")
        ws = _aba(sh, aba_nome)
        ws.append_rows(df.astype(str).values.tolist(), value_input_option="RAW")
        print(f"{len(df)} linha(s) gravadas na aba '{aba_nome}'.")

    _gravar(todos_p, "topline_pesquisas")
    _gravar(todos_r, "topline_resultados")

    if updates:
        fila.update_cells(updates, value_input_option="RAW")

    print("\n───────── resumo ─────────")
    print(f"processados: {len(ok_regs)}  {ok_regs}")
    print(f"com erro:    {len(err_regs)}  {err_regs}")


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
    from scraper_polling import classificar_instituto, gs_client_from_env, salvar_tudo
    from polling_manual_core import ORIGEM

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
    if "publicado" not in df_p.columns:
        df_p["publicado"] = ""

    feito = df_p["publicado"].astype(str).str.strip().str.lower().isin(["sim", "true", "1", "x"])
    pendentes = df_p[~feito]
    if pendentes.empty:
        print("Tudo já publicado.")
        return

    def _preparar(df):
        # tira colunas de controle do staging: o salvar_tudo forçaria metodologia por
        # último e jogaria colunas novas antes dela (origem entra depois, => _marcar_origem).
        df = df.drop(columns=["publicado", "origem", "validacao"], errors="ignore").copy()
        if "instituto" in df.columns:
            df["classificacao_instituto"] = df["instituto"].apply(classificar_instituto)
        if "percentual" in df.columns:
            df["percentual"] = pd.to_numeric(df["percentual"], errors="coerce")
        return df

    def _registros_no_polling(sheet_id):
        """registros que já existem na planilha de turno (do PollingData ou de rodada
        anterior). Evita republicar e duplicar."""
        try:
            aba = gc.open_by_key(sheet_id).worksheet("resultados")
        except Exception:
            return set()
        cab = aba.row_values(1)
        if "registro_tse" not in cab:
            return set()
        col = aba.col_values(cab.index("registro_tse") + 1)[1:]
        return {v.strip() for v in col if v.strip()}

    publicados = []
    for turno, sheet_id in (("t1", T1_ID), ("t2", T2_ID)):
        if not sheet_id:
            continue
        pt_all = pendentes[pendentes["turno"].astype(str).str.lower() == turno]
        if pt_all.empty:
            continue
        publicados += list(pt_all.index)   # tratados nesta rodada (enviados ou pulados)

        existentes = _registros_no_polling(sheet_id)
        ja = pt_all["registro_tse"].astype(str).str.strip().isin(existentes)
        if ja.any():
            print(f"{turno}: {int(ja.sum())} cenário(s) já na planilha (registro existente), pulando")
        pt = pt_all[~ja]
        if pt.empty:
            continue

        ids = set(pt["scenario_id"].astype(str))
        rt = df_r[df_r["scenario_id"].astype(str).isin(ids)]
        salvar_tudo(gc, sheet_id, _preparar(pt), _preparar(rt))
        print(f"{turno}: {len(pt)} cenário(s), {len(rt)} resultado(s) -> planilha de {turno}")

    # origem como última coluna (após metodologia) e marca nossas linhas, sempre
    for sheet_id in (T1_ID, T2_ID):
        if not sheet_id:
            continue
        sh_t = gc.open_by_key(sheet_id)
        for tab in ("pesquisas", "resultados"):
            try:
                _marcar_origem(sh_t.worksheet(tab), ORIGEM)
            except Exception as e:
                print(f"  aviso: origem em {tab} ({sheet_id[:6]}...): {e}")

    if publicados:   # índice 0-based do df = linha (i+2) na planilha
        ws_p.update_cells([gspread.Cell(i + 2, col_pub, "sim") for i in publicados],
                          value_input_option="RAW")
    print(f"\n{len(publicados)} cenário(s) publicado(s).")


def cmd_canonico():
    """Regenera o canonico.json a partir das planilhas T1/T2 do PollingData.
    Rodar localmente quando surgirem candidatos/institutos novos; commitar o arquivo."""
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    from scraper_polling import gs_client_from_env

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
    elif cmd == "canonico":
        cmd_canonico()
    else:
        print("uso: python pesquisas.py [alerta|extrair|topline|publicar|canonico]")
