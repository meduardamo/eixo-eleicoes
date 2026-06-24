"""
Alertas de notícias sobre candidaturas.

Usado na fase das convenções (julho), quando o TSE ainda não tem dado. A gente
monitora notícia por palavra-chave e usa pra montar a matriz de quem foi anunciado.
Fonte: Google Notícias (RSS). A classificação fica por conta do Gemini (ver TODO).
"""

import json
import os
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

HEADERS = {"User-Agent": "Mozilla/5.0"}

GEMINI_MODEL = "gemini-2.5-flash"
SHEET_ID  = os.getenv("SPREADSHEET_ID_TSE", "1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA")
SHEET_ABA = "noticias"

_SA_FIELDS = {
    "type", "project_id", "private_key_id", "private_key", "client_email",
    "client_id", "auth_uri", "token_uri", "auth_provider_x509_cert_url",
    "client_x509_cert_url", "universe_domain",
}


_CLIENT = None

def _gemini_client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    from google import genai
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass
    if not key:
        try:
            _dir = os.path.dirname(os.path.abspath(__file__))
            cred_path = os.path.join(_dir, "credentials.json")
            with open(cred_path, encoding="utf-8") as f:
                key = json.load(f).get("genai_api_key", "")
        except Exception:
            pass
    if not key:
        raise RuntimeError("Faltou a GEMINI_API_KEY (env var, secrets ou credentials.json).")
    _CLIENT = genai.Client(api_key=key)
    return _CLIENT

UFS = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB',
       'PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']


BRT = timezone(timedelta(hours=-3))


def _formatar_data(pubdate):
    """Converte a data do RSS (GMT) para 'dd/mm/aaaa HH:MM' no horário de Brasília."""
    if not pubdate:
        return ""
    try:
        return parsedate_to_datetime(pubdate).astimezone(BRT).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return pubdate


def google_news_rss(busca, max_itens=20):
    """Retorna as notícias de uma busca (título, fonte, data, link)."""
    q = urllib.parse.quote(busca)
    url = f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    itens = []
    for item in root.findall(".//item")[:max_itens]:
        fonte = item.find("{*}source")
        itens.append({
            "titulo": item.findtext("title", ""),
            "fonte": fonte.text if fonte is not None else "",
            "data": _formatar_data(item.findtext("pubDate", "")),
            "link": item.findtext("link", ""),
        })
    return itens


def gerar_buscas(cargos=('presidente', 'governador', 'senador')):
    buscas = []
    for cargo in cargos:
        if cargo == 'presidente':            # presidente é nacional, sem UF
            buscas.append("eleições 2026 presidente (convenção OR pré-candidato)")
        else:
            buscas += [f"eleições 2026 {cargo} {uf} (convenção OR pré-candidato)"
                       for uf in UFS]
    return buscas


def coletar(cargos=('presidente', 'governador', 'senador'), pausa=1.0):
    """Roda todas as buscas e junta as notícias, sem repetir título."""
    vistos, resultado = set(), []
    for busca in gerar_buscas(cargos):
        try:
            for it in google_news_rss(busca):
                chave = it['titulo'].strip().lower()
                if chave and chave not in vistos:
                    vistos.add(chave)
                    it['busca'] = busca
                    resultado.append(it)
        except Exception as e:
            print(f"erro em '{busca}': {e}")
        time.sleep(pausa)
    return resultado


def classificar_com_gemini(titulo, trecho=""):
    """Lê a manchete (e trecho do artigo) e extrai os campos estruturados (JSON) via Gemini."""
    contexto = f"Manchete: {titulo}"
    if trecho:
        contexto += f"\n\nTrecho do artigo: {trecho}"

    prompt = (
        "Você é um analista eleitoral especializado nas eleições brasileiras de 2026.\n"
        "Analise o conteúdo abaixo e extraia as informações em JSON com exatamente estes campos:\n\n"
        "{\n"
        '  "candidato": "Nome, apelido ou sobrenome da pessoa como aparece no texto (ex: \'Tarcísio\', \'Lula\', \'Romeu Zema\'). Use null SOMENTE se nenhum nome próprio de pessoa aparecer",\n'
        '  "cargo": "governador | senador | presidente | vice-governador | outro | null",\n'
        '  "uf": "Sigla do estado (ex: SP, RJ, MG) ou null se cargo federal ou não identificado",\n'
        '  "partido": "Sigla do partido ou federação (ex: PT, PL, MDB, FE BRASIL) ou null se não mencionado",\n'
        '  "status": "confirmado | pré-candidato | em disputa | renúncia | desistência | indefinido",\n'
        '  "confianca": "alto | médio | baixo"\n'
        "}\n\n"
        "Regras de preenchimento:\n"
        "- status='confirmado': candidatura oficializada em convenção partidária ou registro formal\n"
        "- status='pré-candidato': intenção declarada publicamente, sem oficialização ainda\n"
        "- status='em disputa': partido ou coligação ainda decide entre dois ou mais nomes\n"
        "- status='renúncia' ou 'desistência': candidato que retirou ou perdeu a candidatura\n"
        "- status='indefinido': manchete ambígua ou sem informação suficiente para classificar\n"
        "- confianca='alto': candidato, cargo e UF estão todos explícitos no texto\n"
        "- confianca='médio': algum campo foi inferido com boa certeza pelo contexto\n"
        "- confianca='baixo': muita ambiguidade ou faltam dois ou mais campos principais\n"
        "- Use null (não a string 'null') para campos ausentes\n"
        "- Responda SOMENTE o objeto JSON, sem texto extra, sem markdown, sem bloco de código\n\n"
        f"{contexto}"
    )
    resp = _gemini_client().models.generate_content(model=GEMINI_MODEL, contents=prompt)
    texto = (getattr(resp, "text", "") or "").strip()
    texto = texto.replace("```json", "").replace("```", "").strip()
    return json.loads(texto)


def classificar_noticias(noticias):
    """Aplica o Gemini em cada notícia coletada, acrescentando os campos."""
    total = len(noticias)
    for i, n in enumerate(noticias, 1):
        try:
            n.update(classificar_com_gemini(n["titulo"]))
        except Exception as e:
            print(f"[{i}/{total}] erro ao classificar: {e}")
        if i % 10 == 0 or i == total:
            print(f"[{i}/{total}] classificadas...")
    return noticias


COLUNAS_PLANILHA = [
    "candidato", "cargo", "uf", "partido", "status", "confianca",
    "titulo", "fonte", "data", "link", "busca",
]


def _sheets_aba():
    import gspread
    from google.oauth2.service_account import Credentials
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        info = {k: v for k, v in json.loads(creds_json).items() if k in _SA_FIELDS}
    else:
        _dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(_dir, "credentials.json"), encoding="utf-8") as f:
            info = {k: v for k, v in json.load(f).items() if k in _SA_FIELDS}
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        return sh.worksheet(SHEET_ABA)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Aba '{SHEET_ABA}' não encontrada — criando com cabeçalho...")
        aba = sh.add_worksheet(title=SHEET_ABA, rows=5000, cols=len(COLUNAS_PLANILHA))
        aba.append_row(COLUNAS_PLANILHA)
        return aba


def carregar_titulos_existentes():
    """Retorna o conjunto de títulos já salvos na aba do Sheets."""
    try:
        aba = _sheets_aba()
        headers = aba.row_values(1)
        if "titulo" not in headers:
            return set()
        col = headers.index("titulo") + 1
        valores = aba.col_values(col)[1:]
        return {v.strip().lower() for v in valores if v.strip()}
    except Exception as e:
        print(f"Aviso: não foi possível carregar títulos existentes: {e}")
        return set()


def salvar_no_sheets(noticias):
    """Adiciona as notícias novas na aba do Google Sheets."""
    if not noticias:
        print("Nenhuma notícia nova para salvar.")
        return
    aba = _sheets_aba()
    headers = aba.row_values(1)
    if not headers:
        aba.append_row(COLUNAS_PLANILHA)
        headers = COLUNAS_PLANILHA
    def _safe(v):
        # neutraliza injeção de fórmula em texto que começa com = + - @
        s = str(v or "")
        return ("'" + s) if s[:1] in ("=", "+", "-", "@") else s

    linhas = [[_safe(n.get(col)) for col in headers] for n in noticias]
    # USER_ENTERED deixa a URL clicável; o _safe protege o título
    aba.append_rows(linhas, value_input_option="USER_ENTERED")
    print(f"{len(noticias)} notícias salvas no Google Sheets.")


if __name__ == '__main__':
    print("Carregando títulos já salvos na planilha...")
    titulos_existentes = carregar_titulos_existentes()
    print(f"{len(titulos_existentes)} títulos já na planilha.")

    noticias = coletar()   # presidente + governador + senador
    novas = [n for n in noticias if n["titulo"].strip().lower() not in titulos_existentes]
    print(f"{len(novas)} notícias novas (de {len(noticias)} coletadas)")

    if novas:
        novas = classificar_noticias(novas)
        salvar_no_sheets(novas)
