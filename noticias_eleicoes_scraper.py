"""
Alertas de notícias sobre candidaturas.

Usado na fase das convenções (julho), quando o TSE ainda não tem dado. A gente
monitora notícia por palavra-chave e usa pra montar a matriz de quem foi anunciado.
Fonte: Google Notícias (RSS). A classificação fica por conta do Gemini (ver TODO).
"""

import json
import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from googlenewsdecoder import gnewsdecoder
from newspaper import Article

HEADERS = {"User-Agent": "Mozilla/5.0"}

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
SHEET_ID  = os.getenv("SPREADSHEET_ID_TSE", "1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA")
SHEET_ABA = "noticias"

# Planilha com os sites/blogs regionais (colunas Link, Estado).
SITES_ID  = os.getenv("SPREADSHEET_ID_SITES", "")
SITES_ABA = os.getenv("SITES_ABA", "deduplicado")

NOME_UF = {
    'acre': 'AC', 'alagoas': 'AL', 'amapá': 'AP', 'amapa': 'AP', 'amazonas': 'AM',
    'bahia': 'BA', 'ceará': 'CE', 'ceara': 'CE', 'distrito federal': 'DF',
    'espírito santo': 'ES', 'espirito santo': 'ES', 'goiás': 'GO', 'goias': 'GO',
    'maranhão': 'MA', 'maranhao': 'MA', 'mato grosso': 'MT',
    'mato grosso do sul': 'MS', 'minas gerais': 'MG', 'pará': 'PA', 'para': 'PA',
    'paraíba': 'PB', 'paraiba': 'PB', 'paraná': 'PR', 'parana': 'PR',
    'pernambuco': 'PE', 'piauí': 'PI', 'piaui': 'PI', 'rio de janeiro': 'RJ',
    'rio grande do norte': 'RN', 'rio grande do sul': 'RS', 'rondônia': 'RO',
    'rondonia': 'RO', 'roraima': 'RR', 'santa catarina': 'SC', 'são paulo': 'SP',
    'sao paulo': 'SP', 'sergipe': 'SE', 'tocantins': 'TO',
}

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

# Só entram notícias publicadas nos últimos N dias (cobre as duas rodadas diárias
# e o fuso). Ajustável pelo secret NOTICIAS_JANELA_DIAS.
JANELA_DIAS = int(os.getenv("NOTICIAS_JANELA_DIAS", "2"))


def _parse_data(pubdate):
    """Data do RSS -> datetime em BRT, ou None se não der pra ler."""
    if not pubdate:
        return None
    try:
        return parsedate_to_datetime(pubdate).astimezone(BRT)
    except Exception:
        return None


def _formatar_data(pubdate):
    """Converte a data do RSS (GMT) para 'dd/mm/aaaa HH:MM' no horário de Brasília."""
    dt = _parse_data(pubdate)
    return dt.strftime("%d/%m/%Y %H:%M") if dt else (pubdate or "")


def google_news_rss(busca, max_itens=20):
    """Retorna as notícias recentes de uma busca (título, fonte, data, link).
    Descarta o que for mais antigo que JANELA_DIAS."""
    q = urllib.parse.quote(busca)
    url = f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    corte = datetime.now(BRT) - timedelta(days=JANELA_DIAS)
    itens = []
    for item in root.findall(".//item")[:max_itens]:
        pub = item.findtext("pubDate", "")
        dt = _parse_data(pub)
        if dt and dt < corte:      # notícia velha, ignora
            continue
        fonte = item.find("{*}source")
        itens.append({
            "titulo": item.findtext("title", ""),
            "fonte": fonte.text if fonte is not None else "",
            "data": _formatar_data(pub),
            "link": item.findtext("link", ""),
        })
    return itens


TERMOS = ('(convenção OR pré-candidato OR "coordenador de campanha" '
          'OR "lançamento de candidatura")')


def gerar_buscas(cargos=('presidente', 'governador', 'senador')):
    buscas = []
    for cargo in cargos:
        if cargo == 'presidente':            # presidente é nacional, sem UF
            buscas.append(f"eleições 2026 presidente {TERMOS}")
        else:
            buscas += [f"eleições 2026 {cargo} {uf} {TERMOS}" for uf in UFS]
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


def _extrair_texto_pagina(url: str, limite: int = 6000) -> str:
    """Devolve o texto completo do artigo por trás do link do Google Notícias.

    O <link> do RSS (news.google.com/rss/articles/...) não é o artigo: é uma
    página de redirecionamento via JavaScript. Um navegador executa o JS e
    cai no site real; um requests.get() direto só pega a casca do Google,
    sem o texto da notícia. Por isso decodifica pra URL real (gnewsdecoder,
    mesmo truque usado pra esse tipo de link) antes de extrair — mesmo padrão
    do repo googlenews, usando newspaper3k pra extração (mais robusto que
    tirar tag na unha, lida melhor com a variedade de sites de notícia).

    Falha em qualquer etapa (decodificação, paywall, bloqueio, timeout) só
    volta string vazia; quem chama cai de volta pra classificar só pela
    manchete."""
    if not url or not url.startswith("http"):
        return ""
    try:
        decoded = gnewsdecoder(url, interval=1)
        url_real = decoded.get("decoded_url") if decoded.get("status") else url
    except Exception:
        url_real = url
    try:
        art = Article(url_real, language="pt")
        art.download()
        art.parse()
        return (art.text or "").strip()[:limite]
    except Exception:
        return ""


# Variações de texto livre que o Gemini gera pra federação/partido, canonicalizadas
# pra sigla oficial em maiúsculas com "/" entre siglas (ex: PSOL/REDE, UNIÃO/PP).
# Ver normalize_partido() no painel (pages/5_Notícias.py) pro mesmo mapeamento,
# aplicado também nos dados antigos que já estão na planilha.
_PARTIDO_ALIAS = {
    "UNIAO": "UNIÃO",
    "UNIÃO BRASIL": "UNIÃO", "UNIAO BRASIL": "UNIÃO",
    "UNIÃO PROGRESSISTA": "UNIÃO/PP", "UNIAO PROGRESSISTA": "UNIÃO/PP",
    "UNIÃO/PROGRESSISTA": "UNIÃO/PP", "UNIAO/PROGRESSISTA": "UNIÃO/PP",
    "FEDERAÇÃO": "", "FEDERACAO": "",
}
_PARTIDO_VAZIO = {"", "NAN", "NONE", "NULL"}


def normalize_partido(raw) -> str:
    v = str(raw or "").strip()
    if not v:
        return ""
    key = re.sub(r"\s*[-/]\s*", "/", v.upper())
    if key in _PARTIDO_VAZIO:
        return ""
    return _PARTIDO_ALIAS.get(key, key)


def classificar_com_gemini(titulo, trecho=""):
    """Lê a manchete (e trecho do artigo) e extrai os campos estruturados (JSON) via Gemini."""
    contexto = f"Manchete: {titulo}"
    if trecho:
        contexto += f"\n\nTrecho do artigo: {trecho}"

    prompt = (
        "Você é um analista eleitoral especializado nas eleições brasileiras de 2026.\n"
        "Analise o conteúdo abaixo e extraia as informações em JSON com exatamente estes campos:\n\n"
        "{\n"
        '  "candidato": "Nome, apelido ou sobrenome de UMA ÚNICA pessoa, como aparece no texto (ex: \'Tarcísio\', \'Lula\', \'Romeu Zema\'). Se a notícia citar vários políticos, escolha só o mais central ao tema da manchete — nunca liste mais de um nome no campo. Use null se nenhum nome próprio de político aparecer",\n'
        '  "cargo": "governador | senador | presidente | vice-governador | outro | null",\n'
        '  "uf": "Sigla do estado (ex: SP, RJ, MG) ou null se cargo federal ou não identificado",\n'
        '  "partido": "Sigla oficial do partido em maiúsculas (ex: PT, PL, MDB). Se for federação, siglas separadas por \'/\' (ex: PSOL/REDE, UNIÃO/PP). null se não mencionado",\n'
        '  "status": "confirmado | pré-candidato | em disputa | renúncia | desistência | cobertura geral | não relacionado | indefinido",\n'
        '  "convencao": true ou false — true SOMENTE se a notícia trata diretamente de uma convenção partidária '
        "(data, realização, resultado ou decisão tomada em convenção). Independente do status da candidatura.\n"
        '  "confianca": "alto | médio | baixo"\n'
        "}\n\n"
        "Regras de preenchimento:\n"
        "- status='confirmado': candidatura oficializada em convenção partidária ou registro formal\n"
        "- status='pré-candidato': intenção declarada publicamente, sem oficialização ainda\n"
        "- status='em disputa': partido ou coligação ainda decide entre dois ou mais nomes\n"
        "- status='renúncia' ou 'desistência': candidato que retirou ou perdeu a candidatura\n"
        "- status='cobertura geral': cita um político, partido ou estado brasileiro e tem a ver com política/eleições "
        "do Brasil, mas não trata do status da candidatura em si (ex.: declaração, agenda de campanha, resultado de "
        "pesquisa, repercussão de um fato político)\n"
        "- status='não relacionado': NÃO cita nenhum político, partido ou estado brasileiro, ou trata de assunto sem "
        "ligação com política brasileira (ex.: notícia de política internacional — Trump, eleições de outro país — "
        "que não menciona nenhum político/partido/estado do Brasil)\n"
        "- status='indefinido': é sobre candidatura, mas a manchete é ambígua ou falta informação\n"
        "- Notícia de política internacional só NÃO é 'não relacionado' se citar explicitamente um político, "
        "partido ou estado brasileiro (nesse caso, classifique normalmente pelos outros campos)\n"
        "- convencao é independente de status: uma notícia pode ser status='confirmado' e convencao=true "
        "(candidatura saiu de uma convenção) ou status='cobertura geral' e convencao=true (cobertura do evento "
        "em si, sem falar do status de ninguém)\n"
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
    dados = json.loads(texto)
    # o Gemini às vezes desobedece a instrução e devolve a string "null" em vez
    # do null de JSON de verdade — isso vazava pra planilha como texto "null"
    for campo, valor in list(dados.items()):
        if isinstance(valor, str) and valor.strip().lower() in ("null", "none", "n/a"):
            dados[campo] = None
    dados["partido"] = normalize_partido(dados.get("partido"))
    # normaliza pra string: bool False vira "" com o _safe() de salvar_no_sheets
    # (False é falsy em Python), o que confundiria "não" com "não preenchido"
    dados["convencao"] = "sim" if dados.get("convencao") is True else "não"
    return dados


def classificar_noticias(noticias):
    """Aplica o Gemini em cada notícia coletada, acrescentando os campos."""
    total = len(noticias)
    for i, n in enumerate(noticias, 1):
        try:
            trecho = _extrair_texto_pagina(n.get("link", ""))
            n["texto_completo"] = trecho
            n.update(classificar_com_gemini(n["titulo"], trecho))
            # site regional: a UF é conhecida, preenche se o Gemini não achou
            if not n.get("uf") and n.get("uf_regional"):
                n["uf"] = n["uf_regional"]
        except Exception as e:
            print(f"[{i}/{total}] erro ao classificar: {e}")
        if i % 10 == 0 or i == total:
            print(f"[{i}/{total}] classificadas...")
    return noticias


COLUNAS_PLANILHA = [
    "candidato", "cargo", "uf", "partido", "status", "convencao", "resumo", "confianca",
    "titulo", "fonte", "data", "link", "busca", "texto_completo",
]


def _gc():
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
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=scopes))


def _sheets_aba():
    import gspread
    sh = _gc().open_by_key(SHEET_ID)
    try:
        return sh.worksheet(SHEET_ABA)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Aba '{SHEET_ABA}' não encontrada — criando com cabeçalho...")
        aba = sh.add_worksheet(title=SHEET_ABA, rows=5000, cols=len(COLUNAS_PLANILHA))
        aba.append_row(COLUNAS_PLANILHA)
        return aba


def carregar_sites_regionais():
    """Lê a planilha de sites regionais e retorna [(dominio, uf)]."""
    if not SITES_ID:
        print("SPREADSHEET_ID_SITES não definido; pulando sites regionais.")
        return []
    try:
        aba = _gc().open_by_key(SITES_ID).worksheet(SITES_ABA)
        registros = aba.get_all_records()
    except Exception as e:
        print(f"Aviso: não foi possível ler os sites regionais: {e}")
        return []
    sites, vistos = [], set()
    for r in registros:
        link = str(r.get("Link", "") or r.get("link", "")).strip()
        estado = str(r.get("Estado", "") or r.get("estado", "")).strip()
        if not link:
            continue
        alvo = link if "//" in link else "http://" + link
        dom = urllib.parse.urlparse(alvo).netloc.lower()
        dom = dom[4:] if dom.startswith("www.") else dom
        if not dom or dom in vistos:
            continue
        vistos.add(dom)
        sites.append((dom, NOME_UF.get(estado.lower(), "")))
    return sites


def coletar_regionais(sites, pausa=1.0):
    """Busca no Google Notícias restrito a cada domínio (site:), tagueando a UF."""
    vistos, resultado = set(), []
    for dom, uf in sites:
        busca = f"site:{dom} {TERMOS}"
        try:
            for it in google_news_rss(busca):
                chave = it['titulo'].strip().lower()
                if chave and chave not in vistos:
                    vistos.add(chave)
                    it['busca'] = f"regional:{dom}"
                    it['uf_regional'] = uf
                    resultado.append(it)
        except Exception as e:
            print(f"erro em '{dom}': {e}")
        time.sleep(pausa)
    return resultado


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


def _safe(v):
    # neutraliza injeção de fórmula em texto que começa com = + - @
    s = str(v or "")
    return ("'" + s) if s[:1] in ("=", "+", "-", "@") else s


LOTE_RECLASSIFICACAO = 25  # linhas por batch_update


def reclassificar_pendentes(aba):
    """Reclassifica no Gemini as linhas com a coluna 'status' vazia (ex.: você
    apagou a classificação à mão pra refazer). Atualiza só as colunas de
    classificação, mantendo a notícia.

    Salva em lotes de LOTE_RECLASSIFICACAO linhas em vez de um único
    batch_update no final: com milhares de linhas pendentes (cada uma custa
    um download de artigo + uma chamada ao Gemini), o job pode ser cancelado
    pelo timeout do runner antes de terminar. Sem salvar incrementalmente,
    isso jogava fora todo o progresso já feito, porque a lista de updates só
    ia pro Sheets no final do loop inteiro.
    """
    vals = aba.get_all_values()
    if len(vals) < 2:
        return
    headers = vals[0]
    if "titulo" not in headers or "status" not in headers:
        return
    i_titulo, i_status = headers.index("titulo"), headers.index("status")
    i_link = headers.index("link") if "link" in headers else None
    # "resumo" fica de fora: não é gerado aqui, é o alerta que alguém gerou e
    # decidiu salvar no painel. Reclassificar não pode sobrescrever isso.
    cols = [c for c in ("candidato", "cargo", "uf", "partido", "status", "convencao",
                        "confianca", "texto_completo")
            if c in headers]
    # célula por célula, não um range candidato→confiança: se as colunas novas
    # forem adicionadas fora da ordem (ex.: no fim da planilha, depois de titulo/
    # fonte/data/link/busca), um range contíguo escreveria por cima dessas colunas.
    col_letra = {c: chr(65 + headers.index(c)) for c in cols}

    pendentes = [
        (r, row[i_titulo], row[i_link] if (i_link is not None and i_link < len(row)) else "")
        for r, row in enumerate(vals[1:], start=2)
        if (row[i_titulo] if i_titulo < len(row) else "")
        and not (row[i_status] if i_status < len(row) else "").strip()
    ]
    if not pendentes:
        return
    print(f"{len(pendentes)} linhas pendentes de reclassificação.")

    updates, linhas_no_lote, total = [], 0, 0
    for i, (r, titulo, link) in enumerate(pendentes, 1):
        try:
            trecho = _extrair_texto_pagina(link)
            cl = classificar_com_gemini(titulo, trecho)
            cl["texto_completo"] = trecho
        except Exception as e:
            print(f"  erro ao reclassificar linha {r}: {e}")
            continue
        for c in cols:
            updates.append({"range": f"{col_letra[c]}{r}",
                            "values": [[_safe(cl.get(c))]]})
        linhas_no_lote += 1
        if linhas_no_lote >= LOTE_RECLASSIFICACAO:
            aba.batch_update(updates, value_input_option="USER_ENTERED")
            total += linhas_no_lote
            updates, linhas_no_lote = [], 0
            print(f"[{i}/{len(pendentes)}] reclassificadas... ({total} salvas)")
    if updates:
        aba.batch_update(updates, value_input_option="USER_ENTERED")
        total += linhas_no_lote
    print(f"{total} linhas reclassificadas.")


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
    # mais recentes primeiro, pra entrarem no topo
    def _chave(n):
        try:
            return datetime.strptime(n.get("data", ""), "%d/%m/%Y %H:%M")
        except Exception:
            return datetime.min
    noticias = sorted(noticias, key=_chave, reverse=True)

    linhas = [[_safe(n.get(col)) for col in headers] for n in noticias]
    # insere no topo (logo abaixo do cabeçalho); USER_ENTERED deixa a URL clicável
    aba.insert_rows(linhas, row=2, value_input_option="USER_ENTERED")
    print(f"{len(noticias)} notícias inseridas no topo do Google Sheets.")


if __name__ == '__main__':
    print("Carregando títulos já salvos na planilha...")
    titulos_existentes = carregar_titulos_existentes()
    print(f"{len(titulos_existentes)} títulos já na planilha.")

    noticias = coletar()   # presidente + governador + senador (palavra-chave)

    sites = carregar_sites_regionais()
    if sites:
        print(f"{len(sites)} sites regionais; buscando via site:...")
        noticias += coletar_regionais(sites)

    # dedup por título entre as duas fontes (nacional + regional)
    vistos, unicas = set(), []
    for n in noticias:
        chave = n["titulo"].strip().lower()
        if chave and chave not in vistos:
            vistos.add(chave)
            unicas.append(n)
    noticias = unicas

    novas = [n for n in noticias if n["titulo"].strip().lower() not in titulos_existentes]
    print(f"{len(novas)} notícias novas (de {len(noticias)} coletadas)")

    if novas:
        novas = classificar_noticias(novas)
        salvar_no_sheets(novas)

    # reclassifica linhas cuja classificação foi apagada à mão (status vazio)
    reclassificar_pendentes(_sheets_aba())
