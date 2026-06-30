"""
Alerta diário: pesquisas eleitorais com divulgação prevista para hoje (PesqEle).

Lê a aba do PesqEle (que o scraper_pesqele popula), filtra data_divulgacao == hoje
e manda um email pelo Brevo, no mesmo padrão do DOU. Roda 1x/dia de manhã.

Secrets (mesmos do DOU): BREVO_API_KEY, EMAIL (remetente), DESTINATARIOS.
Credenciais do Sheets: GOOGLE_CREDENTIALS_JSON (env) ou credentials.json.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.getenv("SPREADSHEET_ID", "1OEmfn_RyTgrkPenzXlc6qvySs8rbVV39qmuHoULwtjQ")
ABA = os.getenv("PESQELE_ABA", "Consolidado")
PASTA_URL = os.getenv("PASTA_RELATORIOS_URL",
                      "https://drive.google.com/drive/folders/1DD3qewc6nhhdFw8x85i7qtuNRXghd9C6")
BRT = timezone(timedelta(hours=-3))


def _sheets():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    info = json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=scopes))


def pesquisas_de_hoje(hoje):
    ws = _sheets().open_by_key(SHEET_ID).worksheet(ABA)
    registros = ws.get_all_records()
    return [r for r in registros
            if str(r.get("data_divulgacao", ""))[:10] == hoje]


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


def enviar(subject, html_body):
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


if __name__ == "__main__":
    hoje = datetime.now(BRT).strftime("%Y-%m-%d")
    pesquisas = pesquisas_de_hoje(hoje)
    print(f"{len(pesquisas)} pesquisa(s) com divulgação hoje ({hoje})")
    if pesquisas:
        enviar(f"Pesquisas eleitorais previstas para hoje ({len(pesquisas)})",
               _html(pesquisas, hoje))
