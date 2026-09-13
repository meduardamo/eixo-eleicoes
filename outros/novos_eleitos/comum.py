"""Peças que todas as etapas do mapeamento de novos eleitos usam.

Nada aqui decide classificação. Aqui ficam: onde os dados moram, como ler e
gravar planilha, como baixar do TSE e das Casas, e como normalizar nome, CPF e
título de eleitor para cruzar bases diferentes.
"""
import io
import json
import os
import re
import time
import unicodedata
import zipfile
from datetime import datetime
from pathlib import Path

import gspread
import pandas as pd
import requests
from curl_cffi import requests as navegador
from google.oauth2.service_account import Credentials

DADOS = Path(os.getenv("NOVOS_ELEITOS_DADOS", "dados_novos_eleitos"))
TSE_CACHE = Path(os.getenv("TSE_CACHE", str(DADOS / "tse")))
CRED = Path(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"))
ESCOPO = ["https://www.googleapis.com/auth/spreadsheets"]

UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
       "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]

# Cadeiras na Câmara por UF. Soma 513.
VAGAS_CAMARA = {"SP": 70, "MG": 53, "RJ": 46, "BA": 39, "RS": 31, "PR": 30, "PE": 25, "CE": 22,
                "MA": 18, "PA": 17, "GO": 17, "SC": 16, "PB": 12, "ES": 10, "PI": 10, "AL": 9,
                "AC": 8, "AM": 8, "AP": 8, "DF": 8, "MS": 8, "MT": 8, "RN": 8, "RO": 8, "RR": 8,
                "SE": 8, "TO": 8}


def vagas_assembleia(uf):
    """CF art. 27: o triplo da bancada federal até 36; acima disso, soma o que passar de 12.
    O DF tem 24 distritais. Soma 1.059."""
    federal = VAGAS_CAMARA[uf]
    return 3 * federal if federal <= 12 else 36 + federal - 12


# O TSE escreve "MÉDIA" até 2014 e "ELEITO POR MÉDIA" depois. As duas são eleito.
ELEITO = {"ELEITO", "ELEITO POR QP", "ELEITO POR MÉDIA", "MÉDIA"}

# Situação do DivulgaCand que tira a candidatura da disputa. Indeferido com recurso
# continua na urna e por isso fica.
FORA_DA_DISPUTA = {"Renúncia", "Cancelado", "Falecido", "Cassado", "Pedido não conhecido"}

CASA_DO_CARGO = {"SENADOR": "Senado", "DEPUTADO FEDERAL": "Câmara",
                 "DEPUTADO ESTADUAL": "Assembleia", "DEPUTADO DISTRITAL": "Assembleia"}

ROTULO_CARGO = {
    "PRESIDENTE": "Presidente", "VICE-PRESIDENTE": "Vice-presidente",
    "GOVERNADOR": "Governador", "VICE-GOVERNADOR": "Vice-governador",
    "SENADOR": "Senador", "1º SUPLENTE": "1º suplente de senador",
    "2º SUPLENTE": "2º suplente de senador", "DEPUTADO FEDERAL": "Deputado federal",
    "DEPUTADO ESTADUAL": "Deputado estadual", "DEPUTADO DISTRITAL": "Deputado distrital",
    "PREFEITO": "Prefeito", "VICE-PREFEITO": "Vice-prefeito", "VEREADOR": "Vereador",
}
MUNICIPAIS = {"PREFEITO", "VICE-PREFEITO", "VEREADOR"}


def duracao(cargo):
    return 8 if cargo in ("SENADOR", "1º SUPLENTE", "2º SUPLENTE") else 4


# ---------------------------------------------------------------- identificadores

def id_planilha(nome):
    """O repositório é público: id de planilha vem de variável de ambiente."""
    valor = os.getenv(nome, "").strip()
    if not valor:
        raise RuntimeError(f"Variável {nome} não configurada.")
    return valor


def planilha_destino():
    """Toda aba desta pasta é gravada na planilha "Pré mapeamento", separada do Radar do
    Congresso para não lotar aquela. O Radar só é lido (composição, competitividade)."""
    return id_planilha("SPREADSHEET_ID_NOVOS_ELEITOS")


def digitos(valor):
    return re.sub(r"\D", "", str(valor or ""))


def cpf(valor):
    """11 dígitos. O Sheets e alguns anos do TSE comem o zero da frente. "-4" (2024) e
    00000000004 (máscara usada por centenas de candidatos em 2018 e 2022) não são CPF."""
    d = digitos(valor)
    if len(d) < 7:
        return ""
    d = d.zfill(11)
    return "" if d == "00000000004" else d


def titulo(valor):
    """12 dígitos. É a chave que existe em todos os anos, inclusive 2024, que veio sem CPF."""
    d = digitos(valor)
    return d.zfill(12) if len(d) >= 7 else ""


def data_iso(valor):
    """dd/mm/aaaa (TSE) ou aaaa-mm-dd (Casas) para aaaa-mm-dd."""
    texto = str(valor or "").strip()[:10]
    for formato in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, formato).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


PARTICULAS = {"DE", "DA", "DO", "DAS", "DOS", "E"}
TITULOS = {"DR", "DRA", "DOUTOR", "DOUTORA", "DEL", "DELEGADO", "DELEGADA", "PASTOR", "PASTORA",
           "PROF", "PROFA", "PROFESSOR", "PROFESSORA", "CAP", "CAPITAO", "CEL", "CORONEL", "SGT",
           "SARGENTO", "MAJOR", "GENERAL", "TENENTE", "CABO", "SOLDADO", "IRMAO", "IRMA",
           "MISSIONARIO", "MISSIONARIA", "BISPO", "PADRE", "DEPUTADO", "DEPUTADA", "SENADOR",
           "SENADORA", "VEREADOR", "VEREADORA", "PREFEITO", "PREFEITA", "ENFERMEIRA", "ENFERMEIRO"}


def sem_acento(texto):
    texto = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(c for c in texto if not unicodedata.combining(c))


def chave_nome(texto, tirar_titulo=False):
    """Maiúscula, sem acento, sem pontuação e sem partícula. Com tirar_titulo, tira
    também Dr., Delegado, Pastor e afins, que o nome de urna tem e o nome civil não."""
    texto = sem_acento(texto).upper()
    texto = re.sub(r"['’`]", "", texto)  # "D'Angelo" tem que virar DANGELO, como na urna
    texto = re.sub(r"\(.*?\)", " ", texto)
    tokens = re.sub(r"[^A-Z ]", " ", texto).split()
    tokens = [t for t in tokens if t not in PARTICULAS]
    if tirar_titulo:
        tokens = [t for t in tokens if t not in TITULOS]
    return " ".join(tokens)


def semelhanca(a, b):
    """Proporção de palavras em comum sobre a menor das duas. "Jorge Caruso" contra
    "CARUSO" dá 1,0; por isso só usar junto com outra chave (UF, partido, nascimento)."""
    ta, tb = set(a.split()), set(b.split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / min(len(ta), len(tb))


# ---------------------------------------------------------------- arquivos locais

def caminho(nome):
    DADOS.mkdir(parents=True, exist_ok=True)
    return DADOS / nome


def canonizar(serie):
    """Troca título antigo pelo título mais recente da mesma pessoa (etapa 2). Quem
    transfere o título ganha número novo: André Amaral votava em RR em 2006 e na PB em 2022."""
    p = caminho("titulos_canonicos.csv")
    if not p.exists():
        return serie
    mapa = dict(pd.read_csv(p, dtype=str, keep_default_na=False).values)
    return serie.map(lambda t: mapa.get(t, t))


def ler_csv(nome):
    p = caminho(nome)
    if not p.exists():
        raise FileNotFoundError(f"{p} não existe. Rode a etapa que gera esse arquivo antes.")
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def salvar_csv(df, nome):
    df.to_csv(caminho(nome), index=False)
    print(f"gravado {caminho(nome)} ({len(df)} linhas)")


def em_cache(pasta, chave, buscar):
    """Guarda a resposta de API em disco. Quem chama decide quando apagar o cache."""
    p = caminho("cache") / pasta / f"{chave}.json"
    if p.exists():
        return json.loads(p.read_text())
    valor = buscar()
    if valor is not None:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(valor, ensure_ascii=False))
    return valor


# ---------------------------------------------------------------- HTTP

CABECALHOS_DIVULGACAND = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://divulgacandcontas.tse.jus.br/divulga/",
    "Origin": "https://divulgacandcontas.tse.jus.br",
    "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
}


def buscar_json(url, params=None, headers=None, como_navegador=False, pausa=0.0, tentativas=5):
    """GET com retentativa. 404 e corpo vazio devolvem None. O TSE só responde a
    requisição com perfil TLS de navegador, daí o curl_cffi."""
    ultimo = None
    for n in range(1, tentativas + 1):
        try:
            if como_navegador:
                r = navegador.get(url, params=params, headers=headers, timeout=60, impersonate="chrome")
            else:
                r = requests.get(url, params=params, headers=headers or {"Accept": "application/json"},
                                 timeout=60)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            if pausa:
                time.sleep(pausa)
            return r.json() if r.text.strip() else None
        except Exception as erro:  # noqa: BLE001 - blip de API pública, tenta de novo
            ultimo = erro
            time.sleep(2 * n)
    raise RuntimeError(f"Falhou {tentativas} vezes: {url} ({ultimo})")


# ---------------------------------------------------------------- TSE

CDN_CONSULTA = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"


def consulta_cand(ano, atualizar=False):
    """consulta_cand do ano, só eleição ordinária, só o arquivo _BRASIL.csv.
    Os arquivos por UF duplicam o BRASIL; somar os dois dobra tudo em silêncio."""
    TSE_CACHE.mkdir(parents=True, exist_ok=True)
    arquivo = TSE_CACHE / f"consulta_cand_{ano}.zip"
    if atualizar or not arquivo.exists():
        r = navegador.get(f"{CDN_CONSULTA}/consulta_cand_{ano}.zip", impersonate="chrome", timeout=1800)
        r.raise_for_status()
        arquivo.write_bytes(r.content)
    with zipfile.ZipFile(arquivo) as z:
        nome = [x for x in z.namelist() if x.endswith("BRASIL.csv")][0]
        df = pd.read_csv(io.BytesIO(z.read(nome)), sep=";", encoding="latin-1", dtype=str,
                         keep_default_na=False)
    # 2006 codifica a ordinária como "0"/"ORDINÁRIA"; depois vira "2"/"ELEIÇÃO ORDINÁRIA".
    df = df[df.NM_TIPO_ELEICAO.str.contains("ORDIN", na=False)].copy()
    df["titulo"] = df.NR_TITULO_ELEITORAL_CANDIDATO.map(titulo)
    df["cpf"] = df.NR_CPF_CANDIDATO.map(cpf)
    df["nascimento"] = df.DT_NASCIMENTO.map(data_iso)
    return df


def codigo_eleicao_divulgacand(ano, abrangencia="F"):
    """Id da eleição no DivulgaCand (F geral, M municipal). Muda a cada pleito."""
    eleicoes = buscar_json("https://divulgacandcontas.tse.jus.br/divulga/rest/v1/eleicao/ordinarias",
                           headers=CABECALHOS_DIVULGACAND, como_navegador=True) or []
    for e in eleicoes:
        if int(e.get("ano", 0)) == int(ano) and e.get("tipoAbrangencia") == abrangencia:
            return str(e["id"])
    raise RuntimeError(f"Eleição {ano}/{abrangencia} não está no catálogo do DivulgaCand.")


def detalhe_divulgacand(ano, ue, eleicao, sq):
    """Ficha da candidatura: redes sociais declaradas, eleições anteriores e arquivos
    (o plano de governo é o arquivo de codTipo 5)."""
    url = (f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/buscar/"
           f"{ano}/{ue}/{eleicao}/candidato/{sq}")
    return em_cache(f"divulgacand_{ano}", sq, lambda: buscar_json(
        url, headers=CABECALHOS_DIVULGACAND, como_navegador=True, pausa=0.3))


def link_plano(detalhe):
    for arquivo in (detalhe or {}).get("arquivos") or []:
        if str(arquivo.get("codTipo")) == "5" and arquivo.get("idArquivo"):
            return f"https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/doc/{arquivo['idArquivo']}"
    return ""


REDES = {"instagram": "Instagram", "facebook": "Facebook", "tiktok": "TikTok", "youtube": "YouTube",
         "x.com": "X", "twitter": "X", "threads": "Threads", "kwai": "Kwai", "linkedin": "LinkedIn"}


def perfis_declarados(sites):
    """O campo sites vem digitado pelo candidato: "INSTAGRAN: @PERFIL",
    "https://INSTAGRAM: HTTPS://WWW.INSTAGRAM.COM/PERFIL". Devolve {rede: url}.
    Só o primeiro perfil de cada rede; perfil que não dá para ler fica de fora."""
    perfis = {}
    for bruto in sites or []:
        texto = str(bruto).strip().lower().replace("instagran", "instagram")
        # "https://instagram: https://www.instagram.com/x" tem duas URLs; vale a que tem domínio
        # o candidato digita "https://HTTPS://www.tiktok.com/...": o prefixo repetido é pulado
        urls = [u for u in re.findall(r"https?://(?!https?:)[^\s,;]+", texto)
                if re.match(r"https?://[a-z0-9.-]+\.[a-z]{2,}(/|$)", u)]
        if urls:
            # a mesma linha pode ter dois links ("tiktok.com/@x E youtube.com/@x"): lê todos.
            # A rede é a do domínio, não a palavra escrita antes: "TikTok: https://youtube.com/@x"
            for url in urls:
                url = url.split("?")[0].rstrip("/").replace("instagram.com.br", "instagram.com")
                dominio = url.split("//", 1)[1].split("/")[0]
                rede = next((nome for chave, nome in REDES.items() if chave in dominio), None)
                if rede and rede not in perfis:
                    perfis[rede] = url
            continue
        rede = next((nome for chave, nome in REDES.items() if chave in texto), None)
        # @ colado em letra é e-mail ("fulano@gmail.com"), não perfil
        arroba = re.search(r"(?<![a-z0-9._])@([a-z0-9_.]+)", texto)
        if rede == "Instagram" and rede not in perfis and arroba and not re.search(r"\.(com|br|net|org)$", arroba.group(1)):
            perfis[rede] = f"https://www.instagram.com/{arroba.group(1).strip('.')}"
    return perfis


# ---------------------------------------------------------------- Câmara e Senado

API_CAMARA = "https://dadosabertos.camara.leg.br/api/v2"
API_SENADO = "https://legis.senado.leg.br/dadosabertos"


def paginar_camara(recurso, params):
    params = dict(params, itens=100, pagina=1)
    while True:
        j = buscar_json(f"{API_CAMARA}/{recurso}", params=params)
        dados = (j or {}).get("dados") or []
        yield from dados
        if not any(l.get("rel") == "next" for l in (j or {}).get("links", [])):
            return
        params["pagina"] += 1


def deputado(id_camara):
    return em_cache("camara_deputado", id_camara,
                    lambda: (buscar_json(f"{API_CAMARA}/deputados/{id_camara}") or {}).get("dados"))


def senado(recurso, chave_cache=None):
    """Endpoints do Senado em JSON. Lista com um item só vem como objeto, não lista."""
    buscar = lambda: buscar_json(f"{API_SENADO}/{recurso}.json")
    return em_cache("senado", chave_cache, buscar) if chave_cache else buscar()


def como_lista(valor):
    if valor is None:
        return []
    return valor if isinstance(valor, list) else [valor]


# ---------------------------------------------------------------- planilhas

def cliente():
    return gspread.authorize(Credentials.from_service_account_file(str(CRED), scopes=ESCOPO))


def ler_aba(planilha_id, aba):
    valores = cliente().open_by_key(planilha_id).worksheet(aba).get_all_values()
    if not valores:
        return pd.DataFrame()
    return pd.DataFrame(valores[1:], columns=valores[0])


def campo(linha, *nomes):
    """A planilha é editada à mão e renomeia coluna com o uso: ler por lista de nomes."""
    for nome in nomes:
        if nome in linha and str(linha[nome]).strip():
            return str(linha[nome]).strip()
    return ""


def gravar_aba(planilha_id, aba, df, preservar=None, chave=None, congelar_colunas=1, largura_minima=None):
    """Apaga e reescreve a aba a partir da origem, em RAW (USER_ENTERED transforma
    "19,6%" e datas). Colunas de `preservar` são preenchidas à mão pela equipe: o valor
    que já estava na aba volta, casado por `chave`."""
    sh = cliente().open_by_key(planilha_id)
    df = df.fillna("").astype(str)
    try:
        ws = sh.worksheet(aba)
        if preservar and chave:
            antigo = ws.get_all_values()
            if antigo:
                antigo = pd.DataFrame(antigo[1:], columns=antigo[0])
                for col in preservar:
                    if col in antigo.columns and chave in antigo.columns:
                        mapa = dict(zip(antigo[chave], antigo[col]))
                        df[col] = [mapa.get(k, "") or v for k, v in zip(df[chave], df[col])]
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba, rows=len(df) + 10, cols=max(len(df.columns), 1))
    ws.resize(rows=len(df) + 1, cols=len(df.columns))
    valores = [list(df.columns)] + df.values.tolist()
    for inicio in range(0, len(valores), 3000):
        ws.update(values=valores[inicio:inicio + 3000], range_name=f"A{inicio + 1}",
                  value_input_option="RAW")
    formatar(sh, ws, df, congelar_colunas, largura_minima)
    print(f"aba '{aba}' gravada: {len(df)} linhas")


def cor(hexa):
    hexa = hexa.lstrip("#")
    return {"red": int(hexa[0:2], 16) / 255, "green": int(hexa[2:4], 16) / 255, "blue": int(hexa[4:6], 16) / 255}


def largura(coluna, valores):
    """Largura em pixels entre 70 e 340. O conteúdo manda: percentil 80 das células
    preenchidas, porque coluna quase vazia (Instagram só em 191 de 19 mil linhas) daria zero.
    O cabeçalho quebra em até três linhas nos 54 px, então pesa só um terço do tamanho.
    Célula longa não estica a linha: o corpo é cortado na largura."""
    tamanhos = valores.astype(str).str.len()
    tamanhos = tamanhos[tamanhos > 0]
    conteudo = float(tamanhos.quantile(0.8)) if len(tamanhos) else 0
    cabecalho = len(coluna) / 2.6
    return int(min(340, max(70, 7 * max(conteudo, cabecalho) + 16)))


def formatar(sh, ws, df, congelar_colunas=1, largura_minima=None):
    """Identidade das abas de dado do Radar: cabeçalho de 54 px em Montserrat 10 negrito sobre
    #E1E1E1, corpo de 21 px em Montserrat 9 sobre #F4F3EF, borda #DADAD4, primeira linha e
    primeiras colunas congeladas. Tudo numa chamada só, para não estourar a cota de escrita."""
    linhas, colunas = len(df) + 1, len(df.columns)
    borda = {"style": "SOLID", "color": cor("#DADAD4")}
    pedidos = [
        {"updateSheetProperties": {"properties": {"sheetId": ws.id, "gridProperties": {
            "frozenRowCount": 1, "frozenColumnCount": min(congelar_colunas, colunas)}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": cor("#E1E1E1"), "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE",
                            "textFormat": {"fontFamily": "Montserrat", "fontSize": 10, "bold": True}}},
                        "fields": "userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)"}},
        {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": linhas},
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": cor("#F4F3EF"), "wrapStrategy": "CLIP", "verticalAlignment": "MIDDLE",
                            "textFormat": {"fontFamily": "Montserrat", "fontSize": 9, "bold": False}}},
                        "fields": "userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)"}},
        {"updateBorders": {"range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": linhas,
                                     "startColumnIndex": 0, "endColumnIndex": colunas},
                           "innerHorizontal": borda, "innerVertical": borda, "bottom": borda, "right": borda}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                                       "properties": {"pixelSize": 54}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1,
                                                 "endIndex": linhas},
                                       "properties": {"pixelSize": 21}, "fields": "pixelSize"}},
    ]
    for i, col in enumerate(df.columns):
        pedidos.append({"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            # coluna de preenchimento manual nasce vazia e precisa de espaço para digitar
            "properties": {"pixelSize": max(largura(col, df[col]), (largura_minima or {}).get(col, 0))},
            "fields": "pixelSize"}})
    sh.batch_update({"requests": pedidos})


def falhar_se(condicao, mensagem):
    """Conferência que para a etapa em vez de gravar dado errado."""
    if condicao:
        raise SystemExit(f"CONFERÊNCIA FALHOU: {mensagem}")
