import os
import re
import time
import json
import hashlib
from datetime import datetime
import zoneinfo
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False


# Config geral

UFS = [
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go",
    "ma", "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr",
    "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to"
]

PRESIDENTE_URLS_DEFAULT = [
    "https://www.pollingdata.com.br/2026/presidente/br/2026_presidente_br_t1_lula-flavio-sem-bolsonaros.html"
]

WAIT_CSS = "div#dados-das-pesquisas"


# Classificação de institutos

CLASSIFICACAO_INSTITUTOS = {
    "Datafolha": "A+",
    "AtlasIntel": "A+",
    "Jornal Girassol": "A+",
    "Gazeta Dados": "A+",
    "Jornal Stylo": "A+",
    "MDA": "A+",
    "MAS Opinião": "A",
    "Badra Comunicação": "A",
    "Ibope": "A",
    "DMP": "A",
    "Paraná Pesquisas": "A",
    "Real Time Big Data": "A",
    "Grupo M": "A",
    "Futura": "A-",
    "Instituto Amostragem": "A-",
    "Instituto Gasparetto de Pesquisas": "A-",
    "Serpes": "A-",
    "Perfil Pesquisas Técnicas (RN)": "A-",
    "Voice Pesquisas (MT)": "A-",
    "Govnet/Instituto Opinião (SP)": "A-",
    "Instituto Econométrica": "B+",
    "MT Dados": "B+",
    "Instituto de Pesquisa Resultado (MS)": "B+",
    "Data M": "B+",
    "6 Sigma": "B+",
    "Prever Pesquisas": "B+",
    "Solução Treinamento": "B+",
    "Ágora Pesquisa (RJ)": "B",
    "EPP": "B",
    "Exata Pesquisa (MA)": "B",
    "Instituto Jales": "B",
    "Dataqualy": "B",
    "Painel Brasil": "B",
    "Brand Consultoria": "B",
    "Instituto Opinião (PR)": "B",
    "INOPE": "B",
    "Vox Populi": "B",
    "IPEMS": "B",
    "Dataform": "B",
    "Real Dados": "B",
    "Incope": "B",
    "Instituto Datailha": "B-",
    "Tendência Pesquisa (SC)": "B-",
    "Certifica Consultoria": "B-",
    "Opinar Pesquisas": "B-",
    "FLS Pesquisa": "B-",
    "IPAT": "B-",
    "Dados Pesquisa (GO)": "B-",
    "Agora Pesquisa (BA)": "B-",
    "Pontual Pesquisas (AM)": "B-",
    "ABC Dados": "B-",
    "Mapa Marketing": "B-",
    "Múltipla Pesquisa (PE)": "B-",
    "Instituto Múltipla": "B-",
    "Instituto Haverroth": "B-",
    "IPPEC (PR)": "B-",
    "IABR": "B-",
    "CP2 Pesquisa": "B-",
    "RF Consultoria": "B-",
    "IRG Consultoria": "B-",
    "Fortiori": "B-",
    "Colectta Consultoria": "B-",
    "Consult Pesquisa (RN)": "B-",
    "Agorasei Pesquisa": "B-",
    "BMO": "B-",
    "Opinião Pesquisas (PB)": "B-",
    "Ipespe": "B-",
    "IPESPE": "B-",
    "Quaest": "B-",
    "W J Mendes": "B-",
    "Instituto Qualitativa": "B-",
    "Folha Capital": "C+",
    "AR7 Pesquisa": "C+",
    "Veritá": "C+",
    "Instituto Methodus": "C+",
    "Methodus": "C+",
    "Estimativa": "C+",
    "Camminus Marketing": "C+",
    "Doxa": "C+",
    "R M Mariath": "C+",
    "Equação Pesquisas": "C+",
    "Instituto Seta": "C+",
    "Polo Pesquisas": "C+",
    "Ranking Pesquisa": "C+",
    "Ranking Brasil Inteligência": "C+",
    "Index Pesquisas": "C+",
    "Potencial": "C+",
    "F5 Atualiza Dados": "C",
    "Jornal Correio Continental": "C",
    "MBO": "C",
    "Comunicare": "C",
    "Naipes Marketing": "C",
    "Instituto Exatta (PE)": "C",
    "Escutec": "C",
    "Instituto França": "C",
    "Ibrape": "C",
    "Instituto Datasensus": "C",
    "INOP": "C",
    "Instituto Vope": "C",
    "Vox Opinião Pública (SP)": "C",
    "Surgiu Pesquisas": "C",
    "Alternativa Dados": "C-",
    "Seculus Consultoria": "C-",
    "Seculus": "C-",
    "Datavox (PB)": "C-",
    "Instituto Credibilidade": "C-",
    "Multidados": "C-",
    "Voga Pesquisas": "C-",
    "Jornal O+Positivo": "C-",
    "Studio Pesquisas": "C-",
    "Brasil Dados": "C-",
    "Census Pesquisas": "C-",
    "Exatus Consultoria (RN)": "C-",
    "Infornews": "C-",
    "Zaytec Brasil": "C-",
    "Access": "C-",
    "Brasmarket": "N",
    "DataPoder360": "N",
    "FSB Pesquisa": "N",
    "Gerp": "N",
    "Ideia Big Data": "N",
    "Ipec (antigo Ibope)": "N",
    "Ipsos Ipec": "N",
    "Sensus": "N",
    "121 Labs": "N",
    "A.R. Akakia": "N",
    "A.S. Instituto de Planejamento": "N",
    "Acertar": "N",
    "Action Marketing": "N",
    "Adans Estatística": "N",
    "Agência Mind": "N",
    "AGESP": "N",
    "Agili Pesquisas": "N",
    "Agilize Marketing": "N",
    "AGRJPEL": "N",
    "Alencar Consultoria": "N",
    "Alexandre Sócrates Naviskas": "N",
    "Aliança Imóveis": "N",
    "All4 Innovation": "N",
    "Alô Brasil": "N",
    "Alô Sergipe": "N",
    "Alvo Marketing (PA)": "N",
    "Amaral dos Santos & Pimentel": "N",
    "American Analytics": "N",
    "Ampla Consultoria (BA)": "N",
    "Ampla Pesquisa (CE)": "N",
    "Ampla Serviços (MG)": "N",
    "Âncora Pesquisa (Irecê/BA)": "N",
    "Andreia Rafael Publicidade": "N",
    "Ângulo Pesquisas": "N",
    "Apraico Pesquisas": "N",
    "Arbeit Pesquisas": "N",
    "Arpel IPEC (PE)": "N",
    "Ascensão Tecnologia": "N",
    "Associação do Comércio e Indústria de Franca": "N",
    "Atlas Assessoria (MS)": "N",
    "Attitude Consultoria": "N",
    "Atual Pesquisas": "N",
    "Avaliar Pesquisa": "N",
    "Axio Inteligência": "N",
    "Axioma": "N",
    "BABESP": "N",
    "Bis Comunicação": "N",
    "Boas e Boas": "N",
    "Brasília Dados": "N",
    "Braslopes Pesquisa": "N",
    "Bureau Pesquisas": "N",
    "BZN Pesquisa": "N",
    "C. R. Carraro": "N",
    "CBP Pesquisa e Hotelaria": "N",
    "Centro Oeste Pesquisa": "N",
    "Cerrado Pesquisa": "N",
    "Certus": "N",
    "Cipec": "N",
    "Civita Inteligência": "N",
    "Compasso Consultoria": "N",
    "Compope": "N",
    "Conpar": "N",
    "Conpeom": "N",
    "Consulte Inteligência (PI)": "N",
    "Contatus Comunicações": "N",
    "Correio do Povo Tocantinense": "N",
    "Criattiva Pesquisa": "N",
    "Cruz Consulting": "N",
    "CTAS Tecnologia": "N",
    "D M Duarte": "N",
    "Dado Pesquisa": "N",
    "Dados Folha (GO)": "N",
    "Data AZ": "N",
    "Data Center Pesquisas": "N",
    "Data Control": "N",
    "Data Dados": "N",
    "Data Fato": "N",
    "Data Índice": "N",
    "Data Populi": "N",
    "Data Qualyt": "N",
    "Data Verus Pesquisa": "N",
    "Data Vox": "N",
    "Data X": "N",
    "Datacerto": "N",
    "Datacidad Consultoria": "N",
    "Dataconsulte": "N",
    "Dataeco": "N",
    "Datamétrica": "N",
    "Datamob": "N",
    "Dataplan": "N",
    "Datasonda": "N",
    "Dattamarketing": "N",
    "Decisão Pesquisa": "N",
    "Delta Agência de Pesquisa": "N",
    "Destak Publicidade": "N",
    "Destake Pesquisas": "N",
    "Desttaq": "N",
    "Diagnóstico Pesquisas": "N",
    "Dimensão Pesquisa": "N",
    "Disan Empreendimentos": "N",
    "Dominiun Filmes": "N",
    "E. M. Produções": "N",
    "E. Tormes & Cia.": "N",
    "Ebrap": "N",
    "Ecodatta": "N",
    "Economic Cat": "N",
    "Ecos Pesquisa": "N",
    "Editora Ana Cássia": "N",
    "Editora Azul": "N",
    "Editora Cidades": "N",
    "Editora Zen": "N",
    "Efetiva Pesquisas": "N",
    "Eficaz Pesquisas": "N",
    "EGS Comunicação": "N",
    "Eleva": "N",
    "Empresa de Pesquisas Técnicas": "N",
    "Enfoque Assessoria": "N",
    "Enquet Pesquisas": "N",
    "EPB Consultoria": "N",
    "Equatorial Pesquisa": "N",
    "Erica Regina Análise": "N",
    "Ética Pesquisa": "N",
    "Exata Op (DF)": "N",
    "Excelência Pesquisa": "N",
    "Exitus Comunicação": "N",
    "Farias Imóveis e Pesquisas": "N",
    "FC Pesquisas": "N",
    "Fernandes Consultoria": "N",
    "Flemming Pesquisas": "N",
    "Flex Consultoria e Pesquisas": "N",
    "Foca Comunicação (RS)": "N",
    "Foccus": "N",
    "Foco Consultoria (PI)": "N",
    "Foco Pesquisa (BA)": "N",
    "Folha de Caiaponia": "N",
    "Folha Regional (Livramento)": "N",
    "G3 Soluções": "N",
    "Gazeta de Alagoas": "N",
    "Giga Consultoria": "N",
    "Global Consulting (GO)": "N",
    "Global Consultoria (PR)": "N",
    "Global Service Pesquisas": "N",
    "Globo Pesquisa (PR)": "N",
    "Goiás Pesquisas": "N",
    "Gold Solution": "N",
    "Gomes Comunicação": "N",
    "GPP": "N",
    "Gradux": "N",
    "Grupo Carlos Duarte": "N",
    "Grupo Jet": "N",
}


# Utilitários

def classificar_instituto(nome: str) -> str:
    return CLASSIFICACAO_INSTITUTOS.get(_norm_ws(nome), "Ainda não foi avaliado")


def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "t", "yes", "y", "sim", "on")


def _norm_ws(s) -> str:
    try:
        if pd.isna(s):
            s = ""
    except (TypeError, ValueError):
        pass
    return re.sub(r"\s+", " ", str(s)).strip()


def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", str(s or "")).strip()


def _slug(s: str) -> str:
    s = _norm_ws(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _sha1_short(s: str, n=10) -> str:
    return hashlib.sha1(str(s).encode("utf-8", errors="ignore")).hexdigest()[:n]


def extrair_ultima_data(s: str) -> str:
    datas = re.findall(r"\d{4}-\d{2}-\d{2}", str(s))
    return datas[-1] if datas else _norm_ws(s)


def parse_url_meta(url: str):
    u = url.strip()

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>governador)/(?P<uf>[a-z]{2})/.*?_t(?P<turno>\d)\.html",
        u, re.I
    )
    if m:
        return {
            "ano": int(m.group("ano")),
            "cargo": "governador",
            "uf": m.group("uf").upper(),
            "turno": f"t{m.group('turno')}"
        }

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>presidente)/(?P<uf>br)/\d{4}_presidente_br_(?P<turno>t\d)",
        u, re.I
    )
    if m:
        return {
            "ano": int(m.group("ano")),
            "cargo": "presidente",
            "uf": "BR",
            "turno": m.group("turno").lower()
        }

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>senador)/(?P<uf>[a-z]{2})/(?:.*?_(?P<turno>t\d)\.html|(?P<turno2>t\d)/?$)",
        u, re.I
    )
    if m:
        turno = m.group("turno") or m.group("turno2")
        return {
            "ano": int(m.group("ano")),
            "cargo": "senador",
            "uf": m.group("uf").upper(),
            "turno": turno.lower()
        }

    return {"ano": None, "cargo": None, "uf": None, "turno": None}


def parsear_pesquisa(texto):
    nome, id_pesquisa, data = "", "", ""

    for linha in [l.strip() for l in str(texto).strip().split("\n") if l.strip()]:
        if re.match(r"^\(\d+\)$", linha):
            continue

        m = re.search(r"(\d{4}-\d{2}-\d{2})", linha)
        if m:
            data = m.group(1)
            antes = linha[:linha.index(data)].strip()
            if antes:
                id_pesquisa = antes
        else:
            nome = re.sub(r"\s*\(\d+\)\s*$", "", linha).strip()

    return _norm_ws(nome), _norm_ws(id_pesquisa), _norm_ws(data)


def parsear_pct(valor):
    v = str(valor).strip()
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", "NA", ""):
        return None

    try:
        return float(v.replace("%", "").replace(",", ".").strip())
    except Exception:
        return None


def parsear_candidato_partido(col_header: str):
    col_clean = _strip_html(str(col_header).replace("<br>", " "))
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", col_clean.strip())
    if m:
        return _norm_ws(m.group(1)), _norm_ws(m.group(2))
    return _norm_ws(col_clean), ""


def inferir_confianca(s):
    m = re.search(r"(\d{2,3})\s*%\s*\)", str(s or ""))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def inferir_margem_erro(s):
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", str(s or "").replace(",", "."))
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash):
    uf = uf.upper()
    data_campo = _norm_ws(data_campo)
    instituto_slug = _slug(instituto)

    if id_pesquisa and id_pesquisa.lower() not in ("sem registro", "sem_registro", "semregistro", "nan", ""):
        return f"{uf}|{cargo}|{turno}|{id_pesquisa}|{data_campo}"

    return f"{uf}|{cargo}|{turno}|{instituto_slug}|{data_campo}|{raw_block_hash}"


def gerar_scenario_id(poll_id, scenario_label):
    return f"{poll_id}|{_norm_ws(scenario_label)}"


def obter_spreadsheet_id():
    spreadsheet_id_pollingdata = (os.getenv("SPREADSHEET_ID_POLLINGDATA", "") or "").strip()
    spreadsheet_id_padrao = (os.getenv("SPREADSHEET_ID", "") or "").strip()

    if spreadsheet_id_pollingdata:
        return spreadsheet_id_pollingdata

    if spreadsheet_id_padrao:
        return spreadsheet_id_padrao

    raise RuntimeError("Nenhum spreadsheet id foi definido. Use SPREADSHEET_ID_POLLINGDATA ou SPREADSHEET_ID.")


# Builders de URL

def urls_governador_2026_t1(ufs):
    return [
        f"https://www.pollingdata.com.br/2026/governador/{uf}/2026_governador_{uf}_t1.html"
        for uf in ufs
    ]


def urls_senado_2026_t1(ufs):
    return [
        f"https://www.pollingdata.com.br/2026/senador/{uf}/2026_senador_{uf}_t1.html"
        for uf in ufs
    ]


def montar_urls(incluir_governador: bool, incluir_senado: bool, incluir_presidente: bool):
    urls = []

    if incluir_governador:
        urls += urls_governador_2026_t1(UFS)

    if incluir_senado:
        urls += urls_senado_2026_t1(UFS)

    if incluir_presidente:
        urls += list(PRESIDENTE_URLS_DEFAULT)

    return urls


# Selenium

def criar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if _HAS_WDM:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service()

    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def expandir_todos(driver, secao, max_clicks=120):
    i = 0

    while True:
        btns = secao.find_elements(By.CSS_SELECTOR, "button.rt-expander-button")
        fechados = [b for b in btns if b.get_attribute("aria-expanded") == "false"]

        if not fechados:
            break

        driver.execute_script("arguments[0].click();", fechados[0])
        time.sleep(0.8)

        i += 1
        if i >= max_clicks:
            break


# Detecção de layout

def detectar_layout_novo(driver) -> bool:
    try:
        secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

        headers = secao.find_elements(By.CSS_SELECTOR, "thead th")
        textos = [h.text.strip() for h in headers]
        if any("CNPJ" in t for t in textos):
            return True

        rt_headers = secao.find_elements(By.CSS_SELECTOR, "div.rt-th")
        textos_rt = [h.text.strip() for h in rt_headers]
        if any("CNPJ" in t for t in textos_rt):
            return True

        return False
    except Exception:
        return False


# Layout novo

def _extrair_blocos_novo_layout(secao):
    blocos = []
    linhas = secao.find_elements(By.CSS_SELECTOR, "tbody tr")
    i = 0

    while i < len(linhas):
        linha = linhas[i]
        tds = linha.find_elements(By.CSS_SELECTOR, "td")

        if not tds:
            i += 1
            continue

        tem_botao = len(linha.find_elements(By.CSS_SELECTOR, "button")) > 0
        tem_colunas = len(tds) >= 4

        if not (tem_botao or tem_colunas):
            i += 1
            continue

        instituto_cel = tds[1] if len(tds) > 1 else None
        modo_raw = tds[2].text.strip() if len(tds) > 2 else ""
        entrev_erro_raw = tds[3].text.strip() if len(tds) > 3 else ""

        instituto_nome = ""
        registro = ""
        data_campo_raw = ""
        link_fonte = ""

        if instituto_cel is not None:
            texto_inst = instituto_cel.text.strip()
            linhas_inst = [l.strip() for l in texto_inst.split("\n") if l.strip()]

            if linhas_inst:
                instituto_nome = linhas_inst[0]

            for linha_inst in linhas_inst[1:]:
                reg_m = re.match(r"([A-Z]{2}-[\d]+/\d+)", linha_inst)
                if reg_m:
                    registro = reg_m.group(1)

                if re.search(r"\d{4}-\d{2}-\d{2}", linha_inst):
                    data_campo_raw = linha_inst

            try:
                a = instituto_cel.find_element(By.CSS_SELECTOR, "a[href]")
                link_fonte = (a.get_attribute("href") or "").strip()
            except Exception:
                pass

        bloco = {
            "instituto": _norm_ws(instituto_nome),
            "registro_tse": _norm_ws(registro),
            "data_campo": extrair_ultima_data(data_campo_raw),
            "modo": _norm_ws(_strip_html(modo_raw)),
            "entrev_erro_raw": entrev_erro_raw,
            "link_fonte": link_fonte,
            "cenarios": [],
        }

        j = i + 1
        while j < len(linhas):
            sub = linhas[j]
            sub_tds = sub.find_elements(By.CSS_SELECTOR, "td")
            tem_bt_s = len(sub.find_elements(By.CSS_SELECTOR, "button")) > 0

            if tem_bt_s:
                break

            if not sub_tds:
                j += 1
                continue

            try:
                sub_table = sub.find_element(By.CSS_SELECTOR, "table")
                cand_headers = [_norm_ws(th.text) for th in sub_table.find_elements(By.CSS_SELECTOR, "thead th")]

                for tr_cen in sub_table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                    cels = tr_cen.find_elements(By.CSS_SELECTOR, "td")
                    vals = [c.text.strip() for c in cels]
                    if vals and cand_headers:
                        bloco["cenarios"].append(dict(zip(cand_headers, vals)))
            except Exception:
                pass

            j += 1

        blocos.append(bloco)
        i = j

    return blocos


def scrape_novo_layout(driver, url, horario_raspagem, meta):
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]

    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    blocos = _extrair_blocos_novo_layout(secao)
    if not blocos:
        print("  [-] novo layout: nenhum bloco extraído")
        return None, None

    pesquisas_rows = []
    resultados_rows = []

    for bloco in blocos:
        instituto = bloco["instituto"]
        registro_tse = bloco["registro_tse"]
        data_campo = bloco["data_campo"]
        modo = bloco["modo"]
        link_fonte = bloco["link_fonte"]
        entrev_erro = bloco["entrev_erro_raw"]

        amostra = None
        erro_conf = ""

        for le in [l.strip() for l in entrev_erro.split("\n") if l.strip()]:
            if re.match(r"^\d+$", le):
                try:
                    amostra = int(le)
                except Exception:
                    pass
            elif "%" in le:
                erro_conf = le

        margem = inferir_margem_erro(erro_conf)
        confianca = inferir_confianca(erro_conf)
        classificacao = classificar_instituto(instituto)
        block_hash = _sha1_short(f"{instituto}|{registro_tse}|{data_campo}", 10)
        poll_id = gerar_poll_id(uf, instituto, registro_tse, data_campo, cargo, turno, block_hash)

        cenarios = bloco["cenarios"] or [{}]

        for c_idx, cenario_dict in enumerate(cenarios):
            scenario_label = _norm_ws(cenario_dict.get("cenario", str(c_idx + 1))) or str(c_idx + 1)
            scenario_id = gerar_scenario_id(poll_id, scenario_label)

            pesquisas_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "instituto": instituto,
                "classificacao_instituto": classificacao,
                "registro_tse": registro_tse,
                "data_campo": data_campo,
                "modo": modo,
                "amostra": amostra,
                "margem_erro": margem,
                "confianca": confianca,
                "scenario_label": scenario_label,
                "fonte_url": url,
                "fonte_url_original": link_fonte,
                "horario_raspagem": horario_raspagem,
                "conferida": "",
            })

            for col_key, val in cenario_dict.items():
                if col_key.lower() == "cenario":
                    continue

                pct = parsear_pct(val)
                if pct is None:
                    continue

                col_clean = _norm_ws(_strip_html(col_key.replace("<br>", " ")))

                if col_clean.lower() in ("não válido", "nao valido", "não valido"):
                    candidato, partido, tipo = "Não válido", "", "nao_valido"
                    candidato_partido = "Não válido"
                else:
                    candidato, partido = parsear_candidato_partido(col_key)
                    tipo = "candidato"
                    candidato_partido = f"{candidato} ({partido})" if partido else candidato

                resultados_rows.append({
                    "scenario_id": scenario_id,
                    "poll_id": poll_id,
                    "ano": ano,
                    "uf": uf,
                    "cargo": cargo,
                    "turno": turno,
                    "data_campo": data_campo,
                    "instituto": instituto,
                    "classificacao_instituto": classificacao,
                    "registro_tse": registro_tse,
                    "scenario_label": scenario_label,
                    "candidato": candidato,
                    "partido": partido,
                    "candidato_partido": candidato_partido,
                    "tipo": tipo,
                    "percentual": pct,
                    "fonte_url": url,
                    "horario_raspagem": horario_raspagem,
                })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    print(f"  [novo] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


# Layout antigo

def extrair_link_fonte_do_grupo(group) -> str:
    for sel in [
        "table#tab_instituto a[href]",
        "div.rt-td-inner table a[href]",
        "div[id^='tab_'] a[href]",
        ".rt-expandable-content a[href]",
    ]:
        try:
            el = group.find_element(By.CSS_SELECTOR, sel)
            href = (el.get_attribute("href") or "").strip()
            if href and href.startswith("http"):
                return href
        except Exception:
            continue

    return ""


def extrair_tabela_react(secao):
    headers = []

    for el in secao.find_elements(By.CSS_SELECTOR, "div.rt-thead .rt-th"):
        inner = el.find_elements(By.CSS_SELECTOR, ".rt-text-content, .rt-sort-header")
        text = (inner[0].text.strip() if inner else el.text.strip()).replace("\n", " ").strip()
        if text:
            headers.append(text)

    rows_data = []
    row_group_idx = []
    links_por_grupo = []

    for g_idx, group in enumerate(secao.find_elements(By.CSS_SELECTOR, "div.rt-tbody div.rt-tr-group")):
        links_por_grupo.append(extrair_link_fonte_do_grupo(group))

        for row in group.find_elements(By.CSS_SELECTOR, "div.rt-tr"):
            cells = row.find_elements(By.CSS_SELECTOR, "div.rt-td")
            if not cells:
                continue

            vals = [c.text.strip() for c in cells]
            if any(vals):
                rows_data.append(vals)
                row_group_idx.append(g_idx)

    if not rows_data:
        return None, []

    n_cols = max(len(r) for r in rows_data)
    if len(headers) < n_cols:
        headers += [f"Col_{i}" for i in range(len(headers), n_cols)]

    headers = headers[:n_cols]

    for r in rows_data:
        while len(r) < n_cols:
            r.append("")

    df = pd.DataFrame(rows_data, columns=headers)
    df["_link_fonte"] = [links_por_grupo[g] for g in row_group_idx]

    return df, links_por_grupo


def scrape_antigo_layout(driver, url, horario_raspagem, meta):
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]

    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    df_raw, _ = extrair_tabela_react(secao)
    if df_raw is None or df_raw.empty:
        print("  [-] sem tabela")
        return None, None

    col_pesquisa = df_raw.columns.tolist()[0]
    df_raw[col_pesquisa] = df_raw[col_pesquisa].replace("", pd.NA).ffill()
    df_raw["_link_fonte"] = df_raw["_link_fonte"].replace("", pd.NA).ffill().fillna("")

    parsed = df_raw[col_pesquisa].apply(parsear_pesquisa)
    df_raw["instituto"] = parsed.apply(lambda x: x[0])
    df_raw["registro_tse"] = parsed.apply(lambda x: x[1])
    df_raw["data_campo"] = parsed.apply(lambda x: extrair_ultima_data(x[2]))
    df_raw["_block_hash"] = df_raw[col_pesquisa].apply(lambda x: _sha1_short(_norm_ws(x), 10))
    df_raw = df_raw.drop(columns=[col_pesquisa])

    if "Cenários" not in df_raw.columns:
        df_raw["Cenários"] = ""

    cols_meta = [c for c in df_raw.columns if c in {"Modo Pesquisa", "Entrevistas", "Erro (Confiança)", "Cenários"}]
    cols_meta += ["instituto", "registro_tse", "data_campo", "_block_hash", "_link_fonte"]
    cols_meta = [c for c in cols_meta if c in df_raw.columns]

    cols_cand = [c for c in df_raw.columns if c not in cols_meta]
    cols_cand = [
        c for c in cols_cand
        if re.search(r"\([A-Za-z]{2,}\)", str(c)) or str(c).lower().strip() in ("não válido", "nao valido")
    ]

    pesquisas_rows = []
    resultados_rows = []

    for _, row in df_raw.iterrows():
        instituto = _norm_ws(row.get("instituto", ""))
        registro_tse = _norm_ws(row.get("registro_tse", ""))
        data_campo = _norm_ws(row.get("data_campo", ""))
        modo = _norm_ws(row.get("Modo Pesquisa", ""))
        entrevistas_raw = _norm_ws(row.get("Entrevistas", ""))
        erro_conf = _norm_ws(row.get("Erro (Confiança)", ""))
        scenario_label = _norm_ws(row.get("Cenários", "")) or "NA"
        block_hash = _norm_ws(row.get("_block_hash", ""))
        link_fonte_original = _norm_ws(row.get("_link_fonte", ""))

        poll_id = gerar_poll_id(uf, instituto, registro_tse, data_campo, cargo, turno, block_hash)
        scenario_id = gerar_scenario_id(poll_id, scenario_label)

        amostra = None
        try:
            if entrevistas_raw and str(entrevistas_raw).strip().isdigit():
                amostra = int(entrevistas_raw)
        except Exception:
            pass

        pesquisas_rows.append({
            "scenario_id": scenario_id,
            "poll_id": poll_id,
            "ano": ano,
            "uf": uf,
            "cargo": cargo,
            "turno": turno,
            "instituto": instituto,
            "classificacao_instituto": classificar_instituto(instituto),
            "registro_tse": registro_tse,
            "data_campo": data_campo,
            "modo": modo,
            "amostra": amostra,
            "margem_erro": inferir_margem_erro(erro_conf),
            "confianca": inferir_confianca(erro_conf),
            "scenario_label": scenario_label,
            "fonte_url": url,
            "fonte_url_original": link_fonte_original,
            "horario_raspagem": horario_raspagem,
            "conferida": "",
        })

        for col in cols_cand:
            pct = parsear_pct(row.get(col, ""))
            if pct is None:
                continue

            colname = _norm_ws(col)
            if colname.lower() in ("não válido", "nao valido"):
                candidato, partido, tipo = "Não válido", "", "nao_valido"
                candidato_partido = "Não válido"
            else:
                candidato, partido = parsear_candidato_partido(colname)
                tipo = "candidato"
                candidato_partido = f"{candidato} ({partido})" if partido else candidato

            resultados_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "data_campo": data_campo,
                "instituto": instituto,
                "classificacao_instituto": classificar_instituto(instituto),
                "registro_tse": registro_tse,
                "scenario_label": scenario_label,
                "candidato": candidato,
                "partido": partido,
                "candidato_partido": candidato_partido,
                "tipo": tipo,
                "percentual": pct,
                "fonte_url": url,
                "horario_raspagem": horario_raspagem,
            })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    print(f"  [antigo] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


# Dispatcher

def scrape_url(driver, url: str, horario_raspagem: str):
    meta = parse_url_meta(url)

    if not meta["cargo"]:
        print(f"[-] URL não reconhecida: {url}")
        return None, None

    print(f"[+] {meta['cargo'].upper()} {meta['uf']} {meta['turno']} -> {url}")
    driver.get(url)
    time.sleep(10)

    try:
        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
        )
    except Exception:
        print("  [-] timeout (sem container)")
        return None, None

    time.sleep(2)

    if detectar_layout_novo(driver):
        print("  [layout] NOVO (CNPJ detectado)")
        return scrape_novo_layout(driver, url, horario_raspagem, meta)

    print("  [layout] ANTIGO (ReactTable)")
    return scrape_antigo_layout(driver, url, horario_raspagem, meta)


# Google Sheets

def gs_client_from_env():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não definido.")

    creds_dict = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def garantir_aba(sh, nome, rows=50000, cols=25):
    try:
        return sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=nome, rows=rows, cols=cols)


def _aba_vazia(values):
    if not values:
        return True

    if len(values) == 1 and all(str(x).strip() == "" for x in values[0]):
        return True

    return False


def dedup_e_salvar(aba, df: pd.DataFrame, key_col: str):
    if key_col not in df.columns:
        raise RuntimeError(f"df não tem coluna de chave: '{key_col}'")

    antes = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first").reset_index(drop=True)

    if len(df) < antes:
        print(f"  [dedup interno] removidas {antes - len(df)} duplicatas na coleta")

    values = aba.get_all_values()

    if _aba_vazia(values):
        aba.clear()
        aba.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
        print(f"  [aba vazia] {len(df)} linhas gravadas")
        return len(df), 0

    header = values[0]
    if key_col not in header:
        print(f"  [aviso] chave '{key_col}' ausente no header. Reescrevendo aba.")
        aba.clear()
        aba.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
        return len(df), 0

    idx_key = header.index(key_col)
    existing = {row[idx_key] for row in values[1:] if len(row) > idx_key and row[idx_key].strip()}
    df_add = df[~df[key_col].astype(str).isin(existing)].reset_index(drop=True)

    if df_add.empty:
        print(f"  [sem novidades] {len(existing)} já existiam")
        return 0, len(existing)

    colunas_novas = [c for c in df_add.columns if c not in header]
    if colunas_novas:
        header_final = header + colunas_novas
        aba.update([header_final], range_name="A1")
        print(f"  [schema] {len(colunas_novas)} coluna(s) nova(s): {colunas_novas}")
    else:
        header_final = header

    df_add = df_add.reindex(columns=header_final, fill_value="")
    aba.insert_rows(df_add.fillna("").astype(str).values.tolist(), row=2)

    print(f"  [insert] {len(df_add)} nova(s) | {len(existing)} já existiam")
    return len(df_add), len(existing)


def salvar_tudo(gc, spreadsheet_id: str, df_p: pd.DataFrame, df_r: pd.DataFrame):
    if (df_p is None or df_p.empty) and (df_r is None or df_r.empty):
        print("[-] nada para salvar")
        return

    sh = gc.open_by_key(spreadsheet_id)
    aba_pesquisas = garantir_aba(sh, "pesquisas", rows=50000, cols=25)
    aba_resultados = garantir_aba(sh, "resultados", rows=200000, cols=25)

    if df_p is not None and not df_p.empty:
        novos, exist = dedup_e_salvar(aba_pesquisas, df_p, key_col="scenario_id")
        print(f"[+] pesquisas: {novos} novas | {exist} já existiam")

    if df_r is not None and not df_r.empty:
        df_r = df_r.copy()
        df_r["_dedup_key"] = (
            df_r["scenario_id"].astype(str)
            + "|" + df_r["tipo"].astype(str)
            + "|" + df_r["candidato"].astype(str)
        )
        novos, exist = dedup_e_salvar(aba_resultados, df_r, key_col="_dedup_key")
        print(f"[+] resultados: {novos} novas | {exist} já existiam")


# Main

def main():
    incluir_governador = env_bool("INCLUIR_GOVERNADOR", True)
    incluir_senado = env_bool("INCLUIR_SENADO", True)
    incluir_presidente = env_bool("INCLUIR_PRESIDENTE", True)

    spreadsheet_id = obter_spreadsheet_id()

    urls = montar_urls(incluir_governador, incluir_senado, incluir_presidente)
    if not urls:
        print("[-] Nenhuma URL selecionada. Ajuste INCLUIR_*.")
        return

    horario_raspagem = datetime.now(
        zoneinfo.ZoneInfo("America/Recife")
    ).strftime("%Y-%m-%d %H:%M:%S")

    print("[+] Conectando ao Google Sheets...")
    gc = gs_client_from_env()

    print("[+] Iniciando Chrome...")
    driver = criar_driver()

    all_p = []
    all_r = []

    try:
        for url in urls:
            df_p, df_r = scrape_url(driver, url, horario_raspagem)

            if df_p is not None and not df_p.empty:
                all_p.append(df_p)

            if df_r is not None and not df_r.empty:
                all_r.append(df_r)
    finally:
        driver.quit()

    df_p_all = pd.concat(all_p, ignore_index=True) if all_p else pd.DataFrame()
    df_r_all = pd.concat(all_r, ignore_index=True) if all_r else pd.DataFrame()

    salvar_tudo(gc, spreadsheet_id, df_p_all, df_r_all)
    print("[+] OK")


if __name__ == "__main__":
    main()
