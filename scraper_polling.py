import os
import re
import time
import json
import hashlib
from datetime import datetime
import zoneinfo
import pandas as pd
import gspread
from gspread import Cell
from gspread.utils import rowcol_to_a1
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


UFS = [
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go",
    "ma", "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr",
    "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to"
]

PRESIDENTE_URLS_DEFAULT = [
    "https://www.pollingdata.com.br/2026/presidente/br/2026_presidente_br_t1_lula-flavio-sem-bolsonaros.html"
]

WAIT_CSS = "div#dados-das-pesquisas"
FORCAR_LOCALE_PLANILHA = True
LOCALE_PLANILHA = "en_US"

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
}

METODOLOGIA_INSTITUTOS = {
    "Datafolha": "Pesquisa quantitativa, por amostragem, com aplicação de questionário estruturado e abordagem presencial em pontos de fluxo. Universo: população brasileira com 16 anos ou mais.",
    "AtlasIntel": "Pesquisa quantitativa, com coleta online via questionário estruturado e pós-estratificação da amostra conforme o perfil do eleitorado nacional.",
    "Jornal Girassol": "Pesquisa quantitativa, com aplicação de questionário estruturado em entrevistas presenciais junto ao eleitorado, realizadas em domicílios e estabelecimentos comerciais na zona urbana. Uma entrevista por domicílio ou estabelecimento.",
    "Gazeta Dados": "Pesquisa quantitativa, com aplicação de questionário estruturado para aferição de intenção de voto e avaliação de governo.",
    "MDA": "Pesquisa quantitativa, com entrevistas presenciais realizadas em domicílios e, complementarmente, em pontos de fluxo.",
    "Badra Comunicação": "Pesquisa quantitativa, por amostragem por cotas, com seleção de entrevistados proporcional ao perfil do universo pesquisado.",
    "Ibope": "Pesquisa quantitativa, com entrevistas telefônicas e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "DMP": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado, por amostragem probabilística simples. Universo: eleitorado das zonas eleitorais do município.",
    "Paraná Pesquisas": "Pesquisa quantitativa, com aplicação de questionário em formato espontâneo (sem apresentação de nomes) e estimulado (com apresentação de nomes).",
    "Real Time Big Data": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Grupo M": "Pesquisa científica estruturada, com foco em modelagem de sistemas de informação.",
    "Futura": "Pesquisa quantitativa, com foco em intenção de voto e avaliação de governo.",
    "Instituto Amostragem": "Pesquisa qualitativa, realizada por meio de grupos de discussão (focus group), entrevistas semiestruturadas e entrevistas em profundidade.",
    "Instituto Gasparetto de Pesquisas": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado, por amostragem aleatória estratificada, com base em dados do IBGE/TSE.",
    "Serpes": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Perfil Pesquisas Técnicas (RN)": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Voice Pesquisas (MT)": "Pesquisa quantitativa, com entrevistas telefônicas (fixo e celular) e aplicação de questionário estruturado, por amostragem probabilística do eleitorado.",
    "Govnet/Instituto Opinião (SP)": "Pesquisa quantitativa, com foco em intenção de voto e opinião pública junto a uma amostra representativa da população.",
    "Instituto Econométrica": "Pesquisa quantitativa, com entrevistas presenciais face a face e aplicação de questionário estruturado com questões abertas e fechadas, conduzida por entrevistadores treinados e identificados.",
    "Instituto de Pesquisa Resultado (MS)": "Pesquisa mista, com abordagens qualitativa e quantitativa, utilizando entrevistas telefônicas (CATI) e coleta online. Foco em intenção de voto, imagem política, tendências de mercado e satisfação de clientes.",
    "6 Sigma": "Pesquisa quantitativa, com entrevistas presenciais por abordagem aleatória e aplicação de questionário estruturado junto à amostra definida.",
    "Prever Pesquisas": "Pesquisa quantitativa de campo, com foco em intenção de voto e opinião pública.",
    "Ágora Pesquisa (RJ)": "Pesquisa quantitativa, de natureza exploratório-descritiva, operacionalizada por meio da técnica survey com aplicação de questionário estruturado. A coleta de dados será realizada exclusivamente por meio de entrevistas pessoais domiciliares, utilizando dispositivos eletrônicos (tablets). O universo é constituído pelos eleitores com 16 anos ou mais, regularmente inscritos no cadastro eleitoral e residentes no estado do Rio de Janeiro.",
    "EPP": "Pesquisa quantitativa, por amostragem por cotas de gênero, faixa etária, grau de instrução, renda familiar, zona (urbana e rural), microrregiões e municípios.",
    "Exata Pesquisa (MA)": "Pesquisa quantitativa, com entrevistas domiciliares presenciais e aplicação de questionário estruturado, conduzida por profissionais treinados. Universo: população eleitora com 16 anos ou mais.",
    "Dataqualy": "Pesquisa mista, com abordagens qualitativa (grupos focais, entrevistas em profundidade, etnografia) e quantitativa (amostragem).",
    "Painel Brasil": "Pesquisa mista, com abordagens qualitativa e quantitativa, utilizando diversas técnicas de coleta.",
    "Brand Consultoria": "Pesquisa quantitativa, com entrevistas telefônicas (CATI) e aplicação de questionário estruturado com alternativas randomizadas, junto a uma amostra representativa do eleitorado.",
    "Vox Populi": "Pesquisa mista, com abordagem qualitativa para análise de motivações e comportamentos e abordagem quantitativa com aplicação de questionário estruturado junto a uma amostra representativa do público-alvo.",
    "IPEMS": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Real Dados": "Pesquisa quantitativa, com foco em coleta e análise de dados sobre comportamento do consumidor.",
    "Instituto Datailha": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado para aferição da opinião do eleitorado.",
    "Tendência Pesquisa (SC)": "Pesquisa quantitativa, com entrevistas presenciais do tipo intercept em pontos de fluxo, por amostragem aleatória simples com controle de cotas (sexo, faixa etária, grau de instrução e nível econômico).",
    "Certifica Consultoria": "Pesquisa mista, com abordagens qualitativa e quantitativa combinadas.",
    "IPAT": "Pesquisa quantitativa, com foco em opinião pública, hábitos e mercado.",
    "Dados Pesquisa (GO)": "Pesquisa quantitativa, com coleta de dados primários, aplicada a estudos eleitorais, de opinião pública e de mercado.",
    "Agora Pesquisa (BA)": "Pesquisa quantitativa, com foco em intenção de voto e avaliação de governo.",
    "Pontual Pesquisas (AM)": "Pesquisa quantitativa, com entrevistas presenciais.",
    "Instituto Haverroth": "Pesquisa mista, com abordagens qualitativa e quantitativa, voltada à identificação do perfil, expectativas e tendências de eleitores e consumidores.",
    "IRG Consultoria": "Pesquisa quantitativa, com entrevistas telefônicas individuais e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "Fortiori": "Pesquisa mista, com abordagem quantitativa para análise de dados e qualitativa para compreensão de comportamentos e atitudes.",
    "Colectta Consultoria": "Pesquisa quantitativa, com entrevistas presenciais por abordagem aleatória e aplicação de questionário estruturado.",
    "Consult Pesquisa (RN)": "Pesquisa quantitativa, por amostragem probabilística com sorteio em múltiplos estágios.",
    "Agorasei Pesquisa": "Pesquisa quantitativa de campo, com foco em intenção de voto e avaliação de governo.",
    "Opinião Pesquisas (PB)": "Pesquisa quantitativa, por amostragem probabilística ou por cotas, visando a representatividade da população estudada.",
    "Ipespe": "Pesquisa baseada em análise de dados secundários, com foco em histórico eleitoral e comportamento do eleitor.",
    "Quaest": "Pesquisa quantitativa, com aplicação de questionário estruturado junto a uma amostra representativa, com análise estatística dos dados.",
    "W J Mendes": "Pesquisa quantitativa, com uso de métodos estatísticos e análise de dados numéricos para identificação de padrões.",
    "Instituto Qualitativa": "Pesquisa qualitativa, com foco na compreensão de motivações, sentimentos e opiniões sobre fenômenos sociais, políticos ou de mercado.",
    "AR7 Pesquisa": "Pesquisa quantitativa, com foco em opinião pública.",
    "Veritá": "Pesquisa quantitativa, com entrevistas e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado estadual.",
    "Instituto Methodus": "Pesquisa mista, com abordagens qualitativa e quantitativa, presenciais e digitais, com foco em inteligência estratégica.",
    "Doxa": "Pesquisa quantitativa, por amostragem, com aplicação de questionário estruturado e abordagem presencial junto a uma amostra representativa do eleitorado, com base em dados do TSE, IBGE e FAPESPA.",
    "Instituto Seta": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "Ranking Pesquisa": "Pesquisa quantitativa, com entrevistas domiciliares presenciais e aplicação de questionário estruturado junto a uma amostra representativa da população. Universo: eleitorado do estado do Rio Grande do Norte. Foco em avaliação do cenário político e administrativo.",
    "Potencial": "Pesquisa quantitativa, com coleta de dados e uso de tecnologia aplicados à inteligência de mercado.",
    "F5 Atualiza Dados": "Pesquisa quantitativa, com entrevistas telefônicas (CATI) e aplicação de questionário estruturado.",
    "MBO": "Pesquisa quantitativa, com aplicação de questionário em formato espontâneo (sem apresentação de nomes) e estimulado (com apresentação de nomes dos pré-candidatos).",
    "Comunicare": "Pesquisa quantitativa, com foco em opinião pública e preferências do eleitorado e da sociedade civil.",
    "Naipes Marketing": "Pesquisa quantitativa de mercado, com uso de tecnologia e inteligência de dados.",
    "Instituto Exatta (PE)": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Escutec": "Pesquisa quantitativa, com aplicação de questionário estruturado.",
    "Instituto França": "Pesquisa quantitativa, com entrevistas presenciais individuais e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "Ibrape": "Pesquisa quantitativa, com metodologia científica validada.",
    "Instituto Datasensus": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "INOP": "Pesquisa qualitativa, com metodologia estruturada e em conformidade legal.",
    "Instituto Vope": "Pesquisa quantitativa, com entrevistas presenciais diretas junto ao eleitorado.",
    "Vox Opinião Pública (SP)": "Pesquisa quantitativa, com entrevistas presenciais domiciliares.",
    "Seculus Consultoria": "Pesquisa quantitativa de campo, com aplicação de questionário estruturado, por amostragem probabilística ou por cotas, visando representatividade geográfica e demográfica.",
    "Datavox (PB)": "Pesquisa quantitativa, com foco em opinião pública e intenção de voto.",
    "Multidados": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Voga Pesquisas": "Pesquisa mista, com abordagens qualitativa, quantitativa e de monitoramento.",
    "Studio Pesquisas": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Census Pesquisas": "Pesquisa quantitativa, com entrevistas domiciliares presenciais e aplicação de questionário estruturado com perguntas espontâneas e estimuladas (alternativas em ordem alfabética ou randomizadas), por amostragem estratificada em 2 estágios com abordagem aleatória simples (gênero, faixa etária, nível de instrução e renda).",
    "Zaytec Brasil": "Pesquisa mista, com abordagens qualitativa, quantitativa, análise de dados secundários e combinação quali-quanti.",
    "Access": "Pesquisa baseada em análise de dados secundários e revisão bibliográfica.",
    "Brasmarket": "Pesquisa quantitativa, com foco em opinião pública e intenção de voto.",
    "FSB Pesquisa": "Pesquisa quantitativa, com entrevistas telefônicas e aplicação de questionário estruturado para aferição de intenção de voto.",
    "Gerp": "Pesquisa quantitativa, com aplicação de questionário em formato espontâneo (sem apresentação de lista de candidatos).",
    "Ideia Big Data": "Pesquisa mista, com abordagens qualitativa, quantitativa e análise combinada quali-quanti.",
    "Ipec (antigo Ibope)": "Pesquisa quantitativa, com entrevistas presenciais face a face e aplicação de questionário estruturado, por amostragem com base em dados do IBGE para representatividade geográfica e socioeconômica. Amostra típica: 2.000 entrevistas em levantamentos nacionais.",
    "Sensus": "Pesquisa quantitativa, com entrevistas presenciais face a face.",
    "121 Labs": "Pesquisa quantitativa, com foco em opinião pública e mercado, com análise de dados.",
    "Agili Pesquisas": "Pesquisa quantitativa, com foco na captação de percepções e opiniões do público-alvo.",
    "Alexandre Sócrates Naviskas": "Pesquisa quantitativa, com foco em pesquisas eleitorais e de opinião pública.",
    "American Analytics": "Pesquisa quantitativa, com coleta, organização e análise de dados para suporte à tomada de decisão estratégica.",
    "Datasonda": "Pesquisa quantitativa, por amostragem por cotas representativas do eleitorado (gênero, faixa etária, escolaridade, renda e bairros).",
    "Delta Agência de Pesquisa": "Pesquisa quantitativa, por amostragem por cotas, com entrevistas presenciais face a face e aplicação de questionário estruturado por entrevistadores treinados.",
}


def classificar_instituto(nome):
    return CLASSIFICACAO_INSTITUTOS.get(_norm_ws(nome), "Ainda não foi avaliado")


def obter_metodologia(nome):
    return METODOLOGIA_INSTITUTOS.get(_norm_ws(nome), "")


def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "t", "yes", "y", "sim", "on")


def _norm_ws(s) -> str:
    try:
        if pd.isna(s):
            s = ""
    except Exception:
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


def normalizar_percentual_planilha(valor):
    v = str(valor).strip().lstrip("'")
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", "NA", ""):
        return None
    try:
        n = float(v.replace("%", "").replace(",", ".").strip())
        if abs(n) >= 1000:
            n = n / 1000.0
        return round(n, 1)
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


def eh_cenario_media(scenario_label) -> bool:
    s = _norm_ws(scenario_label).lower()
    return bool(re.match(r"^m[eé]dia\b", s))


def obter_spreadsheet_id():
    spreadsheet_id = (os.getenv("SPREADSHEET_ID_POLLINGDATA", "") or "").strip()
    if spreadsheet_id:
        return spreadsheet_id
    raise RuntimeError("SPREADSHEET_ID_POLLINGDATA não definido.")


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


def esperar_tabela(driver, timeout=40):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
    )
    time.sleep(2)


def coletar_headers_visiveis(driver):
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    headers = []

    for sel in ["thead th", "table th", ".rt-thead .rt-th"]:
        try:
            els = secao.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                txt = _norm_ws(el.text)
                if txt and txt.lower() not in [h.lower() for h in headers]:
                    headers.append(txt)
        except Exception:
            pass

    return headers


def decidir_layout(driver):
    headers = coletar_headers_visiveis(driver)
    headers_txt = " | ".join([h.lower() for h in headers])

    tem_cnpj_instituto = "cnpj instituto" in headers_txt
    tem_cnpj_contratante = "cnpj contratante" in headers_txt

    if tem_cnpj_instituto and tem_cnpj_contratante:
        return "novo"

    return "antigo"


def detectar_layout_novo_json(driver) -> bool:
    try:
        scripts = driver.find_elements(
            By.CSS_SELECTOR,
            "script[type='application/json'][data-for='tab_pesquisas']"
        )
        return len(scripts) > 0
    except Exception:
        return False


def scrape_novo_layout(driver, url, horario_raspagem, meta):
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]

    try:
        el = driver.find_element(
            By.CSS_SELECTOR,
            "script[type='application/json'][data-for='tab_pesquisas']"
        )
        raw = el.get_attribute("innerHTML") or el.text
        data_json = json.loads(raw)
        d = data_json["x"]["tag"]["attribs"]["data"]
    except Exception as e:
        print(f"  [-] erro ao extrair JSON do novo: {e}")
        return None, None

    institutos = d.get("Instituto", [])
    urls_fonte = d.get("url", [])
    modos = d.get("Modo", [])
    entrevistas = d.get("Entrevistas", [])
    registros = d.get("registro", [])
    erros = d.get("erro", [])
    ranges = d.get("range", [])
    cenarios_lst = d.get("cenarios", [])

    pesquisas_rows = []
    resultados_rows = []

    for i in range(len(institutos)):
        instituto = _norm_ws(institutos[i])
        link_fonte = _norm_ws(urls_fonte[i]) if i < len(urls_fonte) else ""
        modo = _norm_ws(_strip_html(modos[i])) if i < len(modos) else ""
        amostra_raw = entrevistas[i] if i < len(entrevistas) else None
        registro = _norm_ws(registros[i]) if i < len(registros) else ""
        erro_conf = _strip_html(erros[i]) if i < len(erros) else ""
        data_campo = extrair_ultima_data(ranges[i]) if i < len(ranges) else ""

        registro_norm = "" if registro.lower().startswith("sem") else registro

        amostra = None
        try:
            amostra = int(float(str(amostra_raw)))
        except Exception:
            pass

        margem = inferir_margem_erro(erro_conf)
        confianca = inferir_confianca(erro_conf)
        classificacao = classificar_instituto(instituto)
        metodologia = obter_metodologia(instituto)

        block_hash = _sha1_short(f"{instituto}|{registro}|{data_campo}", 10)
        poll_id = gerar_poll_id(uf, instituto, registro_norm, data_campo, cargo, turno, block_hash)

        cenarios_dict = cenarios_lst[i] if i < len(cenarios_lst) else {}
        cenario_nums = cenarios_dict.get("cenario", [1])

        for c_idx, c_num in enumerate(cenario_nums):
            scenario_label = str(c_num)
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
                "registro_tse": registro,
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
                "metodologia": metodologia,
            })

            for col_key, col_vals in cenarios_dict.items():
                if col_key == "cenario":
                    continue

                val = col_vals[c_idx] if c_idx < len(col_vals) else None
                pct = parsear_pct(val)
                if pct is None:
                    continue

                col_clean = _strip_html(col_key.replace("<br>", " "))
                if col_clean.lower() in ("não válido", "nao valido", "não valido"):
                    candidato = "Não válido"
                    partido = ""
                    tipo = "nao_valido"
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
                    "registro_tse": registro,
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


def expandir_todos_antigo(driver, secao, max_clicks=120):
    i = 0

    while True:
        btns = secao.find_elements(By.CSS_SELECTOR, "button.rt-expander-button")
        fechados = [b for b in btns if (b.get_attribute("aria-expanded") or "").lower() == "false"]

        if not fechados:
            break

        driver.execute_script("arguments[0].click();", fechados[0])
        time.sleep(0.8)

        i += 1
        if i >= max_clicks:
            break


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
    expandir_todos_antigo(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    df_raw, _ = extrair_tabela_react(secao)
    if df_raw is None or df_raw.empty:
        print("  [-] sem tabela no antigo")
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
        classificacao = classificar_instituto(instituto)
        metodologia = obter_metodologia(instituto)

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
            "classificacao_instituto": classificacao,
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
            "metodologia": metodologia,
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

    print(f"  [antigo] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def scrape_url(driver, url, horario_raspagem):
    meta = parse_url_meta(url)

    if not meta["cargo"]:
        print(f"[-] URL não reconhecida: {url}")
        return None, None

    print(f"[+] {meta['cargo'].upper()} {meta['uf']} {meta['turno']} -> {url}")

    driver.get(url)
    esperar_tabela(driver)

    layout = decidir_layout(driver)

    if layout == "novo":
        if not detectar_layout_novo_json(driver):
            print("  [-] headers indicaram novo, mas o JSON do novo não apareceu")
            return None, None
        return scrape_novo_layout(driver, url, horario_raspagem, meta)

    return scrape_antigo_layout(driver, url, horario_raspagem, meta)


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


def reordenar_metodologia_para_ultima_coluna(df: pd.DataFrame) -> pd.DataFrame:
    cols_final = [
        "percentual_media_cenarios",
        "origem_percentual_media",
        "metodologia",
    ]
    cols = [c for c in df.columns if c not in cols_final]
    cols += [c for c in cols_final if c in df.columns]
    return df[cols]


def adicionar_posicao_pesquisa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém compatibilidade com o fluxo atual, mas calcula a posição usando
    a média dos cenários por pesquisa.
    """
    if df.empty:
        return df

    return adicionar_metricas_media_cenarios(df)


def adicionar_metricas_media_cenarios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula um percentual de referência por candidato dentro de cada poll_id.

    Regra:
    - se já existir cenário de média para o candidato, usa esse valor;
    - caso contrário, calcula a média dos cenários numéricos no código.

    A posição passa a ser calculada por poll_id com base nesse percentual
    de referência, e não mais por scenario_id.
    """
    if df.empty:
        return df

    df = df.copy()
    cols_auxiliares = [
        "percentual_media_cenarios",
        "origem_percentual_media",
        "posicao_pesquisa",
        "percentual_media_existente",
        "percentual_media_calculada",
        "eh_cenario_media",
    ]
    cols_para_remover = [c for c in cols_auxiliares if c in df.columns]
    if cols_para_remover:
        df = df.drop(columns=cols_para_remover)

    df["scenario_label"] = df["scenario_label"].fillna("").astype(str)
    df["eh_cenario_media"] = df["scenario_label"].apply(eh_cenario_media)

    chaves = ["poll_id", "tipo", "candidato"]
    df_media = (
        df[df["eh_cenario_media"]]
        .groupby(chaves, dropna=False)["percentual"]
        .mean()
        .reset_index(name="percentual_media_existente")
    )
    df_cenarios = (
        df[~df["eh_cenario_media"]]
        .groupby(chaves, dropna=False)["percentual"]
        .mean()
        .reset_index(name="percentual_media_calculada")
    )

    df_ref = (
        df[chaves]
        .drop_duplicates()
        .merge(df_media, on=chaves, how="left")
        .merge(df_cenarios, on=chaves, how="left")
    )
    df_ref["percentual_media_cenarios"] = df_ref["percentual_media_existente"].combine_first(
        df_ref["percentual_media_calculada"]
    )
    df_ref["origem_percentual_media"] = df_ref["percentual_media_existente"].apply(
        lambda x: "cenario_media_existente" if pd.notna(x) else ""
    )
    df_ref.loc[
        df_ref["origem_percentual_media"].eq("") & df_ref["percentual_media_calculada"].notna(),
        "origem_percentual_media"
    ] = "media_calculada_no_codigo"
    df_ref["posicao_pesquisa"] = (
        df_ref.groupby("poll_id")["percentual_media_cenarios"]
        .rank(method="min", ascending=False)
        .astype("Int64")
    )

    df = df.merge(
        df_ref[chaves + ["percentual_media_cenarios", "origem_percentual_media", "posicao_pesquisa"]],
        on=chaves,
        how="left",
    )

    return df.drop(columns=["eh_cenario_media"])


def preencher_posicao_pesquisa_na_aba(aba):
    """
    Lê a aba 'resultados', recalcula a média dos cenários e a posição
    consolidada por pesquisa, e atualiza apenas as células necessárias.
    """
    print("  [posicao] recalculando média dos cenários e posição consolidada...")
    values = aba.get_all_values()

    if _aba_vazia(values) or len(values) < 2:
        print("  [posicao] aba vazia ou sem dados, nada a preencher")
        return

    header = values[0]

    if "posicao_pesquisa" not in header:
        print("  [posicao] coluna posicao_pesquisa ainda não existe na aba, será criada no próximo insert")
        return

    col_idx_pos = header.index("posicao_pesquisa")

    if "percentual_media_cenarios" not in header:
        print("  [posicao] coluna percentual_media_cenarios ainda não existe na aba, será criada no próximo insert")
        return

    col_idx_pct_media = header.index("percentual_media_cenarios")

    if "poll_id" not in header or "percentual" not in header:
        print("  [posicao] colunas poll_id ou percentual ausentes, impossível recalcular")
        return

    if "tipo" not in header or "candidato" not in header:
        print("  [posicao] colunas tipo ou candidato ausentes, impossível recalcular")
        return

    col_idx_poll = header.index("poll_id")
    col_idx_pct = header.index("percentual")
    col_idx_tipo = header.index("tipo")
    col_idx_candidato = header.index("candidato")
    col_idx_scenario_label = header.index("scenario_label") if "scenario_label" in header else None
    col_idx_origem = header.index("origem_percentual_media") if "origem_percentual_media" in header else None

    rows = values[1:]
    registros = []
    for i, row in enumerate(rows):
        def safe_get(r, idx):
            return r[idx] if idx < len(r) else ""

        poll_id = safe_get(row, col_idx_poll).strip()
        pct_raw = safe_get(row, col_idx_pct).strip()
        pos_atual = safe_get(row, col_idx_pos).strip()
        pct_media_atual = safe_get(row, col_idx_pct_media).strip()
        origem_atual = safe_get(row, col_idx_origem).strip() if col_idx_origem is not None else ""

        pct = parsear_pct(pct_raw)

        registros.append({
            "row_num": i + 2,
            "poll_id": poll_id,
            "percentual": pct,
            "tipo": safe_get(row, col_idx_tipo).strip(),
            "candidato": safe_get(row, col_idx_candidato).strip(),
            "scenario_label": safe_get(row, col_idx_scenario_label).strip() if col_idx_scenario_label is not None else "",
            "posicao_atual": pos_atual,
            "percentual_media_atual": parsear_pct(pct_media_atual),
            "origem_percentual_media_atual": origem_atual,
        })

    df = pd.DataFrame(registros)

    df_valido = df[df["poll_id"].ne("") & df["percentual"].notna()].copy()

    if df_valido.empty:
        print("  [posicao] nenhuma linha com dados válidos para recalcular")
        return

    df_valido = adicionar_metricas_media_cenarios(df_valido)

    def col_idx_to_letter(idx):
        result = ""
        while idx >= 0:
            result = chr(idx % 26 + ord("A")) + result
            idx = idx // 26 - 1
        return result

    col_letter_pos = col_idx_to_letter(col_idx_pos)
    col_letter_pct_media = col_idx_to_letter(col_idx_pct_media)
    col_letter_origem = col_idx_to_letter(col_idx_origem) if col_idx_origem is not None else None

    cell_updates = []
    for _, row in df_valido.iterrows():
        posicao_nova = row["posicao_pesquisa"]
        pct_media_novo = row["percentual_media_cenarios"]

        if pd.notna(posicao_nova):
            posicao_nova_str = str(int(posicao_nova))
            if row["posicao_atual"] != posicao_nova_str:
                cell_updates.append({
                    "range": f"{col_letter_pos}{row['row_num']}",
                    "values": [[posicao_nova_str]]
                })

        if pd.notna(pct_media_novo):
            pct_media_atual = row["percentual_media_atual"]
            if pd.isna(pct_media_atual) or abs(float(pct_media_atual) - float(pct_media_novo)) > 1e-9:
                cell_updates.append({
                    "range": f"{col_letter_pct_media}{row['row_num']}",
                    "values": [[str(pct_media_novo)]]
                })

        if col_letter_origem is not None:
            origem = row.get("origem_percentual_media", "") or ""
            if row.get("origem_percentual_media_atual", "") != origem:
                cell_updates.append({
                    "range": f"{col_letter_origem}{row['row_num']}",
                    "values": [[origem]]
                })

    if cell_updates:
        aba.batch_update(cell_updates)
        print(f"  [posicao] {len(cell_updates)} células atualizadas com média dos cenários e posição")
    else:
        print("  [posicao] planilha já estava consistente com a média dos cenários")


def carregar_df_da_aba(aba) -> pd.DataFrame:
    values = aba.get_all_values()
    if _aba_vazia(values) or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    rows_norm = []
    for row in rows:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        rows_norm.append(row[:len(header)])

    return pd.DataFrame(rows_norm, columns=header)


def sobrescrever_aba(aba, df: pd.DataFrame):
    df = reordenar_metodologia_para_ultima_coluna(df)
    aba.clear()
    if df.empty:
        aba.update([df.columns.tolist()])
        print(f"  [rewrite] aba '{aba.title}' limpa e mantido apenas header")
        return

    aba.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
    print(f"  [rewrite] aba '{aba.title}' regravada com {len(df)} linhas")


def corrigir_coluna_numerica_na_aba(aba, nome_coluna: str, padrao: str = "0.0"):
    values_raw = aba.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if _aba_vazia(values_raw) or len(values_raw) < 2:
        return

    header = values_raw[0]
    if nome_coluna not in header:
        return

    col_idx = header.index(nome_coluna) + 1
    updates = []

    for row_idx, row in enumerate(values_raw[1:], start=2):
        atual_raw = row[col_idx - 1] if len(row) >= col_idx else ""
        novo = normalizar_percentual_planilha(atual_raw)
        if novo is None:
            continue

        atual_raw_txt = str(atual_raw).strip()
        veio_como_texto = isinstance(atual_raw, str)
        exige_forcar_numero = veio_como_texto and (
            atual_raw_txt.startswith("'")
            or "%" in atual_raw_txt
            or "," in atual_raw_txt
            or "." in atual_raw_txt
        )

        atual_f = None
        if isinstance(atual_raw, (int, float)):
            atual_f = float(atual_raw)
        else:
            atual_txt = str(atual_raw).strip().lstrip("'")
            try:
                atual_f = float(atual_txt.replace("%", "").replace(",", "."))
            except Exception:
                atual_f = None

        if veio_como_texto and atual_f is not None:
            exige_forcar_numero = True

        if (
            atual_f is not None
            and round(atual_f, 1) == novo
            and abs(atual_f) < 1000
            and not exige_forcar_numero
        ):
            continue

        updates.append(Cell(row=row_idx, col=col_idx, value=float(novo)))

    if updates:
        aba.update_cells(updates, value_input_option="RAW")
        print(f"  [fmt] aba '{aba.title}': {len(updates)} célula(s) corrigidas na coluna '{nome_coluna}'")

    n_linhas = len(values_raw)
    col_letra = rowcol_to_a1(1, col_idx)[:-1]
    intervalo = f"{col_letra}2:{col_letra}{n_linhas}"
    aba.format(intervalo, {"numberFormat": {"type": "NUMBER", "pattern": padrao}})


def adicionar_media_movel_13d_resultados_bi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula uma média móvel de 13 dias por candidato, usando a média diária do
    percentual_base em cada combinação de ano/cargo/uf/turno/tipo/candidato.
    """
    if df.empty:
        df = df.copy()
        df["media_movel_13d"] = None
        return df

    df = df.copy()
    df["media_movel_13d"] = None

    if "data_campo" not in df.columns or "percentual_base" not in df.columns:
        return df

    df["_data_campo_dt"] = pd.to_datetime(df["data_campo"], errors="coerce")
    df["_percentual_base_num"] = pd.to_numeric(df["percentual_base"], errors="coerce")

    chaves_serie = ["ano", "cargo", "uf", "turno", "tipo", "candidato"]
    df_valid = df[
        df["_data_campo_dt"].notna()
        & df["_percentual_base_num"].notna()
        & df["candidato"].astype(str).str.strip().ne("")
    ].copy()

    if df_valid.empty:
        return df.drop(columns=["_data_campo_dt", "_percentual_base_num"], errors="ignore")

    df_diario = (
        df_valid.groupby(chaves_serie + ["_data_campo_dt"], dropna=False)["_percentual_base_num"]
        .mean()
        .reset_index()
        .sort_values(chaves_serie + ["_data_campo_dt"])
    )

    def calcular_mm_13d(grupo: pd.DataFrame) -> pd.Series:
        serie = (
            grupo.set_index("_data_campo_dt")["_percentual_base_num"]
            .rolling(window="13D")
            .mean()
        )
        return pd.Series(serie.to_numpy(), index=grupo.index)

    df_diario["media_movel_13d"] = (
        df_diario.groupby(chaves_serie, dropna=False, group_keys=False)
        .apply(calcular_mm_13d)
    )

    df = df.merge(
        df_diario[chaves_serie + ["_data_campo_dt", "media_movel_13d"]],
        on=chaves_serie + ["_data_campo_dt"],
        how="left",
    )

    return df.drop(columns=["_data_campo_dt", "_percentual_base_num"], errors="ignore")


def construir_resultados_bi(df_resultados: pd.DataFrame) -> pd.DataFrame:
    """
    Gera uma base consolidada para BI com 1 linha por poll_id + candidato.
    Usa a média dos cenários como percentual base e calcula a posição entre
    candidatos da mesma pesquisa.
    """
    cols = [
        "poll_id", "ano", "uf", "cargo", "turno", "data_campo", "instituto",
        "classificacao_instituto", "registro_tse", "candidato", "partido",
        "candidato_partido", "tipo", "percentual_base",
        "origem_percentual_base", "cenario_usado_no_calculo",
        "qtd_cenarios_considerados", "media_movel_13d", "posicao_candidato", "eh_lider",
        "eh_segundo", "fonte_url", "horario_raspagem"
    ]

    if df_resultados is None or df_resultados.empty:
        return pd.DataFrame(columns=cols)

    df = df_resultados.copy()
    if "percentual" in df.columns:
        df["percentual"] = df["percentual"].apply(parsear_pct)
    else:
        df["percentual"] = None

    for col in ["poll_id", "tipo", "candidato", "scenario_label"]:
        if col not in df.columns:
            df[col] = ""

    df = df[df["poll_id"].astype(str).str.strip().ne("") & df["percentual"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = adicionar_metricas_media_cenarios(df)
    df["eh_cenario_media"] = df["scenario_label"].apply(eh_cenario_media)

    chaves = ["poll_id", "tipo", "candidato"]
    df_qtd = (
        df[~df["eh_cenario_media"]]
        .groupby(chaves, dropna=False)["scenario_label"]
        .nunique()
        .reset_index(name="qtd_cenarios_considerados")
    )

    df_media_lbl = (
        df[df["eh_cenario_media"]]
        .sort_values(chaves + ["scenario_label"])
        .drop_duplicates(subset=chaves, keep="first")[chaves + ["scenario_label"]]
        .rename(columns={"scenario_label": "cenario_usado_no_calculo"})
    )

    df_pref = df.copy()
    df_pref["_ordem_escolha"] = df_pref["eh_cenario_media"].astype(int) * -1
    df_base = (
        df_pref
        .sort_values(chaves + ["_ordem_escolha", "scenario_label"])
        .drop_duplicates(subset=chaves, keep="first")
        .copy()
    )

    df_base = df_base.merge(df_qtd, on=chaves, how="left")
    df_base = df_base.merge(df_media_lbl, on=chaves, how="left")

    df_base["origem_percentual_base"] = df_base["origem_percentual_media"]
    df_base["percentual_base"] = df_base["percentual_media_cenarios"]
    df_base["qtd_cenarios_considerados"] = df_base["qtd_cenarios_considerados"].fillna(0).astype(int)
    df_base["cenario_usado_no_calculo"] = df_base["cenario_usado_no_calculo"].fillna("")
    df_base.loc[
        df_base["origem_percentual_base"].eq("media_calculada_no_codigo"),
        "cenario_usado_no_calculo"
    ] = "media_calculada_no_codigo"

    df_candidatos = df_base[df_base["tipo"].astype(str).str.lower().eq("candidato")].copy()
    df_candidatos["posicao_candidato"] = (
        df_candidatos.groupby("poll_id")["percentual_base"]
        .rank(method="min", ascending=False)
        .astype("Int64")
    )
    df_candidatos["eh_lider"] = df_candidatos["posicao_candidato"].eq(1)
    df_candidatos["eh_segundo"] = df_candidatos["posicao_candidato"].eq(2)
    df_candidatos = adicionar_media_movel_13d_resultados_bi(df_candidatos)

    for col in cols:
        if col not in df_candidatos.columns:
            df_candidatos[col] = ""

    df_candidatos = df_candidatos[cols].copy()
    df_candidatos = df_candidatos.sort_values(
        by=["cargo", "uf", "turno", "data_campo", "poll_id", "posicao_candidato", "candidato"],
        ascending=[True, True, True, False, True, True, True],
        na_position="last",
    ).reset_index(drop=True)

    df_candidatos["eh_lider"] = df_candidatos["eh_lider"].map(lambda x: "TRUE" if x is True else "FALSE")
    df_candidatos["eh_segundo"] = df_candidatos["eh_segundo"].map(lambda x: "TRUE" if x is True else "FALSE")

    return df_candidatos


def dedup_e_salvar(aba, df: pd.DataFrame, key_col: str):
    if key_col not in df.columns:
        raise RuntimeError(f"df não tem coluna de chave: '{key_col}'")

    df = reordenar_metodologia_para_ultima_coluna(df)

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

    colunas_novas = [c for c in df.columns if c not in header]
    cols_final = ["percentual_media_cenarios", "origem_percentual_media", "metodologia"]
    header_base = [c for c in header if c not in cols_final]
    novas_base = [c for c in colunas_novas if c not in cols_final]
    header_final = header_base + novas_base
    header_final += [c for c in cols_final if c in df.columns or c in header]

    if header_final != header:
        aba.update([header_final], range_name="A1")
        if colunas_novas:
            print(f"  [schema] {len(colunas_novas)} coluna(s) nova(s): {colunas_novas}")
        else:
            print(f"  [schema] colunas finais reposicionadas: {[c for c in cols_final if c in header_final]}")

    idx_key = header_final.index(key_col)
    values = aba.get_all_values()
    existing = {row[idx_key] for row in values[1:] if len(row) > idx_key and row[idx_key].strip()}

    df_add = df[~df[key_col].astype(str).isin(existing)].reset_index(drop=True)

    if df_add.empty:
        print(f"  [sem novidades] {len(existing)} já existiam")
        return 0, len(existing)

    df_add = df_add.reindex(columns=header_final, fill_value="")
    aba.insert_rows(df_add.fillna("").astype(str).values.tolist(), row=2)

    print(f"  [insert] {len(df_add)} nova(s) | {len(existing)} já existiam")
    return len(df_add), len(existing)


def salvar_tudo(gc, spreadsheet_id: str, df_p: pd.DataFrame, df_r: pd.DataFrame):
    if (df_p is None or df_p.empty) and (df_r is None or df_r.empty):
        print("[-] nada para salvar")
        return

    sh = gc.open_by_key(spreadsheet_id)
    if FORCAR_LOCALE_PLANILHA:
        try:
            sh.update_locale(LOCALE_PLANILHA)
        except Exception as e:
            print(f"  [fmt] não foi possível definir locale da planilha para {LOCALE_PLANILHA}: {e}")

    aba_pesquisas = garantir_aba(sh, "pesquisas", rows=50000, cols=35)
    aba_resultados = garantir_aba(sh, "resultados", rows=200000, cols=35)
    aba_resultados_bi = garantir_aba(sh, "resultados_bi", rows=200000, cols=40)

    if df_p is not None and not df_p.empty:
        novos, exist = dedup_e_salvar(aba_pesquisas, df_p, key_col="scenario_id")
        print(f"[+] pesquisas: {novos} novas | {exist} já existiam")

    if df_r is not None and not df_r.empty:
        df_r = df_r.copy()

        df_r = adicionar_posicao_pesquisa(df_r)

        df_r["_dedup_key"] = (
            df_r["scenario_id"].astype(str)
            + "|" + df_r["tipo"].astype(str)
            + "|" + df_r["candidato"].astype(str)
        )
        novos, exist = dedup_e_salvar(aba_resultados, df_r, key_col="_dedup_key")
        print(f"[+] resultados: {novos} novas | {exist} já existiam")

    preencher_posicao_pesquisa_na_aba(aba_resultados)

    df_resultados_all = carregar_df_da_aba(aba_resultados)
    df_resultados_bi = construir_resultados_bi(df_resultados_all)
    sobrescrever_aba(aba_resultados_bi, df_resultados_bi)
    print(f"[+] resultados_bi: {len(df_resultados_bi)} linhas consolidadas para Looker")

    corrigir_coluna_numerica_na_aba(aba_resultados, "percentual")
    corrigir_coluna_numerica_na_aba(aba_resultados, "percentual_media_cenarios")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, "percentual_base")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, "media_movel_13d")


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
            try:
                df_p, df_r = scrape_url(driver, url, horario_raspagem)

                if df_p is not None and not df_p.empty:
                    all_p.append(df_p)

                if df_r is not None and not df_r.empty:
                    all_r.append(df_r)
            except Exception as e:
                print(f"  [-] erro ao processar URL {url}: {e}")
    finally:
        driver.quit()

    df_p_all = pd.concat(all_p, ignore_index=True) if all_p else pd.DataFrame()
    df_r_all = pd.concat(all_r, ignore_index=True) if all_r else pd.DataFrame()

    salvar_tudo(gc, spreadsheet_id, df_p_all, df_r_all)
    print("[+] OK")


if __name__ == "__main__":
    main()
