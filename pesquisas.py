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


def _sheets():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    info = json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=scopes))


def _verdadeiro(v):
    return str(v).strip().lower() in ("sim", "true", "verdadeiro", "1", "x")


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
    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#111">
      <h2 style="margin:0 0 6px 0">Pesquisas com divulgação prevista para hoje</h2>
      <div style="color:#374151;margin:0 0 14px 0">{hoje} · {len(pesquisas)} pesquisa(s)</div>
      <div style="background:#eef0f6;border-left:3px solid #192D4E;padding:10px 12px;margin:0 0 16px 0;font-size:13px">
        <strong style="color:#192D4E">Ação do dia:</strong> baixe o relatório completo de cada pesquisa e salve na pasta
        <a href="{PASTA_URL}" style="color:#192D4E">Pesquisas Eleitorais</a> do Drive, na subpasta do cargo:
        Presidente Nacional vai em <b>Presidenciáveis/Nacional</b>;
        Presidente por UF em <b>Presidenciáveis/Por UF</b>;
        Governador e Senador em <b>Governadores+Senadores</b>.
      </div>
      {secoes}
    </body></html>
    """


def _enviar(subject, html_body):
    api_key, sender = os.getenv("BREVO_API_KEY"), os.getenv("EMAIL")
    dests = [e.strip() for e in os.getenv("DESTINATARIOS", "").split(",") if e.strip()]
    if not (api_key and sender and dests):
        print("Config de email incompleta; pulando envio.")
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


# ────────────────────────────── EXTRAÇÃO ──────────────────────────────

RELATORIOS_ID = os.getenv("SPREADSHEET_ID_RELATORIOS", "")
GEMINI_MODEL = "gemini-2.5-flash"

PROMPT = (
    "Você é um analista de dados de pesquisas eleitorais da Eixo. Você recebe o PDF do "
    "relatório completo de uma pesquisa e extrai os cruzamentos por segmento.\n\n"
    "Extraia DUAS listas, em JSON:\n\n"
    "1) voto_segmento: para CADA cenário de voto estimulado e CADA candidato, o percentual "
    "de voto quebrado por segmento demográfico (gênero, idade, escolaridade e renda). "
    'Cada item: {"cenario": "...", "candidato": "Nome (PARTIDO)", "segmento": "...", "valor": número}.\n\n'
    "2) rejeicao: para CADA candidato, o percentual de rejeição quebrado por segmento. "
    'Cada item: {"candidato": "Nome (PARTIDO)", "segmento": "...", "valor": número}.\n\n'
    "Regras:\n"
    "1) Preserve os números EXATAMENTE como no relatório. Não arredonde nem recalcule.\n"
    "2) Não invente. Se um cruzamento não existir no relatório, omita.\n"
    "3) Use os rótulos de segmento como aparecem (ex: Masculino, Feminino, 16 a 24 anos, "
    "25 a 34 anos, Fundamental, Médio, Superior, Até 2 SM, Mais de 5 a 10 SM).\n"
    "4) 'valor' é número, sem o símbolo de %.\n"
    "5) Identifique o cenário pelo nome ou título que o relatório usa (ex: 'Estimulada 1', "
    "'Lula x Flávio'). Se houver só um, use 'Estimulada'.\n"
    "6) Em 'candidato', use o nome como aparece, com o partido entre parênteses se houver.\n\n"
    "Responda SOMENTE o JSON, sem texto extra e sem markdown:\n"
    '{"voto_segmento": [...], "rejeicao": [...]}'
)


def extrair_do_pdf(pdf_bytes):
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[PROMPT, types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")],
    )
    raw = (getattr(resp, "text", "") or "").strip().replace("```json", "").replace("```", "").strip()
    return json.loads(raw)


def cmd_extrair():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = sh.worksheet("relatorios")
    ws_voto = sh.worksheet("voto_segmento")
    ws_rej = sh.worksheet("rejeicao")

    linhas = fila.get_all_records()
    voto_novos, rej_novos, marcar = [], [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")

    for i, r in enumerate(linhas, start=2):   # linha 1 = cabeçalho
        link = str(r.get("link", "")).strip()
        if not link or _verdadeiro(r.get("extraido")):
            continue
        try:
            pdf = requests.get(link, headers=HEADERS, timeout=60).content
            dados = extrair_do_pdf(pdf)
        except Exception as e:
            print(f"linha {i} ({r.get('registro')}): erro {e}")
            continue
        meta = [r.get("registro", ""), r.get("cargo", ""), r.get("uf", ""), r.get("instituto", "")]
        for v in dados.get("voto_segmento", []):
            voto_novos.append(meta + [v.get("cenario", ""), v.get("candidato", ""),
                                      v.get("segmento", ""), v.get("valor", "")])
        for v in dados.get("rejeicao", []):
            rej_novos.append(meta + [v.get("candidato", ""), v.get("segmento", ""), v.get("valor", "")])
        marcar.append(i)
        print(f"linha {i} ({r.get('registro')}): "
              f"{len(dados.get('voto_segmento', []))} voto, {len(dados.get('rejeicao', []))} rejeição")

    if voto_novos:
        ws_voto.append_rows(voto_novos, value_input_option="RAW")
    if rej_novos:
        ws_rej.append_rows(rej_novos, value_input_option="RAW")

    headers = fila.row_values(1)
    if marcar and "extraido" in headers:
        ci_ext = headers.index("extraido") + 1
        ci_data = headers.index("data_extracao") + 1 if "data_extracao" in headers else None
        for row_i in marcar:
            fila.update_cell(row_i, ci_ext, "sim")
            if ci_data:
                fila.update_cell(row_i, ci_data, agora)

    print(f"\n{len(marcar)} relatório(s) extraído(s).")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "alerta":
        cmd_alerta()
    elif cmd == "extrair":
        cmd_extrair()
    else:
        print("uso: python pesquisas.py [alerta|extrair]")
