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


TEST_URLS = [
    "https://www.pollingdata.com.br/2026/governador/ce/2026_governador_ce_t1.html",
    "https://www.pollingdata.com.br/2026/presidente/br/2026_presidente_br_t1_lula-flavio-sem-bolsonaros.html",
]

WAIT_CSS = "div#dados-das-pesquisas"


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


def detectar_layout_novo(driver) -> bool:
    try:
        scripts = driver.find_elements(
            By.CSS_SELECTOR,
            "script[type='application/json'][data-for='tab_pesquisas']"
        )
        if scripts:
            return True
    except Exception:
        pass

    try:
        secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
        texto = secao.text.lower()

        if "cnpj instituto" in texto or "cnpj contratante" in texto:
            return True

        headers = secao.find_elements(By.CSS_SELECTOR, "thead th")
        textos = [h.text.strip().lower() for h in headers]
        if any("cnpj" in t for t in textos):
            return True
    except Exception:
        pass

    return False


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


def _linha_principal_novo(tr):
    try:
        return len(tr.find_elements(By.CSS_SELECTOR, "td")) >= 6
    except Exception:
        return False


def _linha_tem_subtabela(tr):
    try:
        return len(tr.find_elements(By.CSS_SELECTOR, "table")) > 0
    except Exception:
        return False


def _clicar_expansor_novo(driver, linha):
    tds = linha.find_elements(By.CSS_SELECTOR, "td")
    if not tds:
        return False

    cel = tds[0]

    for sel in ["button[aria-expanded='false']", "button", "[role='button']", "svg", "span", "div"]:
        try:
            elementos = cel.find_elements(By.CSS_SELECTOR, sel)
            for el in elementos:
                try:
                    if el.is_displayed() and el.is_enabled():
                        driver.execute_script("arguments[0].click();", el)
                        return True
                except Exception:
                    pass
        except Exception:
            pass

    try:
        driver.execute_script("arguments[0].click();", cel)
        return True
    except Exception:
        return False


def expandir_todos_novo(driver, secao, max_clicks=200):
    clicks = 0

    while clicks < max_clicks:
        linhas = secao.find_elements(By.CSS_SELECTOR, "tbody > tr")
        mudou = False

        i = 0
        while i < len(linhas):
            linha = linhas[i]

            if not _linha_principal_novo(linha):
                i += 1
                continue

            proxima = linhas[i + 1] if i + 1 < len(linhas) else None
            ja_expandida = proxima is not None and _linha_tem_subtabela(proxima)

            if not ja_expandida:
                clicou = _clicar_expansor_novo(driver, linha)
                if clicou:
                    time.sleep(1.0)
                    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
                    clicks += 1
                    mudou = True
                    break

            i += 1

        if not mudou:
            break


def _extrair_blocos_novo_layout(secao):
    blocos = []
    linhas = secao.find_elements(By.CSS_SELECTOR, "tbody > tr")
    i = 0

    while i < len(linhas):
        linha = linhas[i]
        tds = linha.find_elements(By.CSS_SELECTOR, "td")

        if len(tds) < 6:
            i += 1
            continue

        instituto_cel = tds[1]
        modo_raw = tds[2].text.strip() if len(tds) > 2 else ""
        entrev_erro_raw = tds[3].text.strip() if len(tds) > 3 else ""

        instituto_nome = ""
        registro = ""
        data_campo_raw = ""
        link_fonte = ""

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

            if len(sub_tds) >= 6:
                break

            try:
                tabelas = sub.find_elements(By.CSS_SELECTOR, "table")
                for sub_table in tabelas:
                    cand_headers = [
                        _norm_ws(th.text)
                        for th in sub_table.find_elements(By.CSS_SELECTOR, "thead th")
                    ]

                    if not cand_headers:
                        continue

                    for tr_cen in sub_table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                        cels = tr_cen.find_elements(By.CSS_SELECTOR, "td")
                        vals = [c.text.strip() for c in cels]

                        if vals and len(vals) == len(cand_headers):
                            bloco["cenarios"].append(dict(zip(cand_headers, vals)))
            except Exception:
                pass

            j += 1

        blocos.append(bloco)
        i = j

    return blocos


def scrape_novo_layout_json(driver, url, horario_raspagem, meta):
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
    except Exception:
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
                else:
                    candidato, partido = parsear_candidato_partido(col_key)
                    tipo = "candidato"

                resultados_rows.append({
                    "scenario_id": scenario_id,
                    "poll_id": poll_id,
                    "ano": ano,
                    "uf": uf,
                    "cargo": cargo,
                    "turno": turno,
                    "data_campo": data_campo,
                    "instituto": instituto,
                    "registro_tse": registro,
                    "scenario_label": scenario_label,
                    "candidato": candidato,
                    "partido": partido,
                    "tipo": tipo,
                    "percentual": pct,
                    "fonte_url": url,
                    "horario_raspagem": horario_raspagem,
                })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    if df_p.empty and df_r.empty:
        return None, None

    print(f"  [novo/json] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def scrape_novo_layout_dom(driver, url, horario_raspagem, meta):
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]

    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos_novo(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    blocos = _extrair_blocos_novo_layout(secao)
    if not blocos:
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
        block_hash = _sha1_short(f"{instituto}|{registro_tse}|{data_campo}", 10)
        poll_id = gerar_poll_id(uf, instituto, registro_tse, data_campo, cargo, turno, block_hash)

        cenarios = bloco["cenarios"] or [{}]

        for c_idx, cenario_dict in enumerate(cenarios):
            scenario_label = (
                _norm_ws(cenario_dict.get("Cenário", ""))
                or _norm_ws(cenario_dict.get("cenario", ""))
                or str(c_idx + 1)
            )

            scenario_id = gerar_scenario_id(poll_id, scenario_label)

            pesquisas_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "instituto": instituto,
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
            })

            for col_key, val in cenario_dict.items():
                if col_key.lower() in ("cenário", "cenario"):
                    continue

                pct = parsear_pct(val)
                if pct is None:
                    continue

                col_clean = _norm_ws(_strip_html(col_key.replace("<br>", " ")))

                if col_clean.lower() in ("não válido", "nao valido", "não valido"):
                    candidato = "Não válido"
                    partido = ""
                    tipo = "nao_valido"
                else:
                    candidato, partido = parsear_candidato_partido(col_clean)
                    tipo = "candidato"

                resultados_rows.append({
                    "scenario_id": scenario_id,
                    "poll_id": poll_id,
                    "ano": ano,
                    "uf": uf,
                    "cargo": cargo,
                    "turno": turno,
                    "data_campo": data_campo,
                    "instituto": instituto,
                    "registro_tse": registro_tse,
                    "scenario_label": scenario_label,
                    "candidato": candidato,
                    "partido": partido,
                    "tipo": tipo,
                    "percentual": pct,
                    "fonte_url": url,
                    "horario_raspagem": horario_raspagem,
                })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    if df_p.empty and df_r.empty:
        return None, None

    print(f"  [novo/dom] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def scrape_novo_layout(driver, url, horario_raspagem, meta):
    df_p, df_r = scrape_novo_layout_json(driver, url, horario_raspagem, meta)
    if df_p is not None and not df_p.empty:
        return df_p, df_r

    print("  [novo] fallback para DOM")
    return scrape_novo_layout_dom(driver, url, horario_raspagem, meta)


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
        })

        for col in cols_cand:
            pct = parsear_pct(row.get(col, ""))
            if pct is None:
                continue

            colname = _norm_ws(col)
            if colname.lower() in ("não válido", "nao valido"):
                candidato = "Não válido"
                partido = ""
                tipo = "nao_valido"
            else:
                candidato, partido = parsear_candidato_partido(colname)
                tipo = "candidato"

            resultados_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "data_campo": data_campo,
                "instituto": instituto,
                "registro_tse": registro_tse,
                "scenario_label": scenario_label,
                "candidato": candidato,
                "partido": partido,
                "tipo": tipo,
                "percentual": pct,
                "fonte_url": url,
                "horario_raspagem": horario_raspagem,
            })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    print(f"  [antigo] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def scrape_url(driver, url: str, horario_raspagem: str):
    meta = parse_url_meta(url)

    print(f"[+] {meta['cargo'].upper()} {meta['uf']} {meta['turno']} -> {url}")
    driver.get(url)
    time.sleep(8)

    WebDriverWait(driver, 40).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
    )
    time.sleep(2)

    if detectar_layout_novo(driver):
        print("  [layout] NOVO")
        return scrape_novo_layout(driver, url, horario_raspagem, meta)

    print("  [layout] ANTIGO")
    return scrape_antigo_layout(driver, url, horario_raspagem, meta)


def main():
    horario_raspagem = datetime.now(
        zoneinfo.ZoneInfo("America/Recife")
    ).strftime("%Y-%m-%d %H:%M:%S")

    driver = criar_driver()

    try:
        all_p = []
        all_r = []

        for url in TEST_URLS:
            df_p, df_r = scrape_url(driver, url, horario_raspagem)

            if df_p is not None and not df_p.empty:
                all_p.append(df_p)

            if df_r is not None and not df_r.empty:
                all_r.append(df_r)

        df_p_all = pd.concat(all_p, ignore_index=True) if all_p else pd.DataFrame()
        df_r_all = pd.concat(all_r, ignore_index=True) if all_r else pd.DataFrame()

        print("\n=== PESQUISAS ===")
        if not df_p_all.empty:
            print(df_p_all[[
                "uf", "cargo", "instituto", "registro_tse", "data_campo",
                "scenario_label", "amostra", "margem_erro", "confianca"
            ]].to_string(index=False))
        else:
            print("vazio")

        print("\n=== RESULTADOS ===")
        if not df_r_all.empty:
            print(df_r_all[[
                "uf", "cargo", "instituto", "data_campo",
                "scenario_label", "candidato", "partido", "percentual"
            ]].to_string(index=False))
        else:
            print("vazio")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
