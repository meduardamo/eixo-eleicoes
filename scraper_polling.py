import re
import time
import hashlib
from datetime import datetime
import zoneinfo
import pandas as pd

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


def _norm_ws(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _strip_html(s):
    return re.sub(r"<[^>]+>", "", str(s or "")).strip()


def _sha1_short(s, n=10):
    return hashlib.sha1(str(s).encode("utf-8", errors="ignore")).hexdigest()[:n]


def extrair_ultima_data(s):
    datas = re.findall(r"\d{4}-\d{2}-\d{2}", str(s))
    return datas[-1] if datas else _norm_ws(s)


def parse_url_meta(url):
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
    if not v or v.lower() in ("-", "nan%", "nan", "na", ""):
        return None

    try:
        return float(v.replace("%", "").replace(",", ".").strip())
    except Exception:
        return None


def parsear_candidato_partido(col_header):
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


def esperar_tabela_renderizar(driver, timeout=40):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
    )

    def _pronto(_driver):
        try:
            secao = _driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

            candidatos = [
                "thead th",
                "table th",
                ".rt-thead .rt-th",
                "tbody tr",
                ".rt-tbody .rt-tr-group",
            ]

            for sel in candidatos:
                els = secao.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    return True

            return False
        except Exception:
            return False

    WebDriverWait(driver, timeout).until(_pronto)
    time.sleep(2)


def coletar_headers_visiveis(driver):
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    headers = []

    seletores = [
        "thead th",
        "table th",
        ".rt-thead .rt-th",
    ]

    for sel in seletores:
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

    layout = "novo" if (tem_cnpj_instituto and tem_cnpj_contratante) else "antigo"

    print("  headers encontrados:")
    if headers:
        for h in headers:
            print(f"    - {h}")
    else:
        print("    - nenhum header encontrado")

    print(f"  layout decidido: {layout.upper()}")
    return layout


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


def extrair_tabela_react(secao):
    headers = []

    for el in secao.find_elements(By.CSS_SELECTOR, "div.rt-thead .rt-th"):
        inner = el.find_elements(By.CSS_SELECTOR, ".rt-text-content, .rt-sort-header")
        text = (inner[0].text.strip() if inner else el.text.strip()).replace("\n", " ").strip()
        if text:
            headers.append(text)

    rows_data = []

    for group in secao.find_elements(By.CSS_SELECTOR, "div.rt-tbody div.rt-tr-group"):
        for row in group.find_elements(By.CSS_SELECTOR, "div.rt-tr"):
            cells = row.find_elements(By.CSS_SELECTOR, "div.rt-td")
            if not cells:
                continue

            vals = [c.text.strip() for c in cells]
            if any(vals):
                rows_data.append(vals)

    if not rows_data:
        return None

    n_cols = max(len(r) for r in rows_data)
    if len(headers) < n_cols:
        headers += [f"Col_{i}" for i in range(len(headers), n_cols)]

    headers = headers[:n_cols]

    for r in rows_data:
        while len(r) < n_cols:
            r.append("")

    return pd.DataFrame(rows_data, columns=headers)


def scrape_antigo(driver, url, meta, horario_raspagem):
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos_antigo(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    df = extrair_tabela_react(secao)
    if df is None or df.empty:
        print("  sem dados no antigo")
        return pd.DataFrame(), pd.DataFrame()

    col_pesquisa = df.columns.tolist()[0]
    df[col_pesquisa] = df[col_pesquisa].replace("", pd.NA).ffill()

    parsed = df[col_pesquisa].apply(parsear_pesquisa)
    df["instituto"] = parsed.apply(lambda x: x[0])
    df["registro_tse"] = parsed.apply(lambda x: x[1])
    df["data_campo"] = parsed.apply(lambda x: extrair_ultima_data(x[2]))
    df["_block_hash"] = df[col_pesquisa].apply(lambda x: _sha1_short(_norm_ws(x), 10))

    if "Cenários" not in df.columns:
        df["Cenários"] = ""

    cols_meta = {
        col_pesquisa, "Modo Pesquisa", "Entrevistas", "Erro (Confiança)",
        "Cenários", "instituto", "registro_tse", "data_campo", "_block_hash"
    }

    cols_cand = [c for c in df.columns if c not in cols_meta]
    cols_cand = [
        c for c in cols_cand
        if re.search(r"\([A-Za-z]{2,}\)", str(c)) or str(c).lower().strip() in ("não válido", "nao valido")
    ]

    pesquisas = []
    resultados = []

    for _, row in df.iterrows():
        instituto = row.get("instituto", "")
        registro_tse = row.get("registro_tse", "")
        data_campo = row.get("data_campo", "")
        cenario = row.get("Cenários", "") or "NA"
        block_hash = row.get("_block_hash", "")

        poll_id = f"{meta['uf']}|{meta['cargo']}|{meta['turno']}|{registro_tse or instituto}|{data_campo}|{block_hash}"
        scenario_id = f"{poll_id}|{cenario}"

        pesquisas.append({
            "scenario_id": scenario_id,
            "poll_id": poll_id,
            "cargo": meta["cargo"],
            "uf": meta["uf"],
            "turno": meta["turno"],
            "instituto": instituto,
            "registro_tse": registro_tse,
            "data_campo": data_campo,
            "cenario": cenario,
            "modo": row.get("Modo Pesquisa", ""),
            "amostra": row.get("Entrevistas", ""),
            "erro": row.get("Erro (Confiança)", ""),
            "margem_erro": inferir_margem_erro(row.get("Erro (Confiança)", "")),
            "confianca": inferir_confianca(row.get("Erro (Confiança)", "")),
            "horario_raspagem": horario_raspagem,
        })

        for col in cols_cand:
            pct = parsear_pct(row.get(col, ""))
            if pct is None:
                continue

            if str(col).lower().strip() in ("não válido", "nao valido"):
                cand = "Não válido"
                partido = ""
                tipo = "nao_valido"
            else:
                cand, partido = parsear_candidato_partido(col)
                tipo = "candidato"

            resultados.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "cargo": meta["cargo"],
                "uf": meta["uf"],
                "turno": meta["turno"],
                "instituto": instituto,
                "data_campo": data_campo,
                "cenario": cenario,
                "candidato": cand,
                "partido": partido,
                "tipo": tipo,
                "percentual": pct,
                "horario_raspagem": horario_raspagem,
            })

    print(f"  antigo -> {len(pesquisas)} cenários | {len(resultados)} resultados")
    return pd.DataFrame(pesquisas), pd.DataFrame(resultados)


def _extrair_blocos_novo(secao):
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

        bloco = {
            "instituto": _norm_ws(instituto_nome),
            "registro_tse": _norm_ws(registro),
            "data_campo": extrair_ultima_data(data_campo_raw),
            "modo": _norm_ws(_strip_html(modo_raw)),
            "entrev_erro_raw": entrev_erro_raw,
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
                    headers = [_norm_ws(th.text) for th in sub_table.find_elements(By.CSS_SELECTOR, "thead th")]

                    for tr_cen in sub_table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                        cels = tr_cen.find_elements(By.CSS_SELECTOR, "td")
                        vals = [c.text.strip() for c in cels]

                        if vals and len(vals) == len(headers):
                            bloco["cenarios"].append(dict(zip(headers, vals)))
            except Exception:
                pass

            j += 1

        blocos.append(bloco)
        i = j

    return blocos


def scrape_novo(driver, url, meta, horario_raspagem):
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos_novo(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    blocos = _extrair_blocos_novo(secao)
    if not blocos:
        print("  sem dados no novo")
        return pd.DataFrame(), pd.DataFrame()

    pesquisas = []
    resultados = []

    for bloco in blocos:
        amostra = None
        erro_conf = ""

        for le in [l.strip() for l in bloco["entrev_erro_raw"].split("\n") if l.strip()]:
            if re.match(r"^\d+$", le):
                amostra = le
            elif "%" in le:
                erro_conf = le

        cenarios = bloco["cenarios"] or [{}]

        for i, cenario_dict in enumerate(cenarios, start=1):
            cenario = (
                _norm_ws(cenario_dict.get("Cenário", ""))
                or _norm_ws(cenario_dict.get("cenario", ""))
                or str(i)
            )

            block_hash = _sha1_short(f"{bloco['instituto']}|{bloco['registro_tse']}|{bloco['data_campo']}", 10)
            poll_id = f"{meta['uf']}|{meta['cargo']}|{meta['turno']}|{bloco['registro_tse'] or bloco['instituto']}|{bloco['data_campo']}|{block_hash}"
            scenario_id = f"{poll_id}|{cenario}"

            pesquisas.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "cargo": meta["cargo"],
                "uf": meta["uf"],
                "turno": meta["turno"],
                "instituto": bloco["instituto"],
                "registro_tse": bloco["registro_tse"],
                "data_campo": bloco["data_campo"],
                "cenario": cenario,
                "modo": bloco["modo"],
                "amostra": amostra,
                "erro": erro_conf,
                "margem_erro": inferir_margem_erro(erro_conf),
                "confianca": inferir_confianca(erro_conf),
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
                    cand = "Não válido"
                    partido = ""
                    tipo = "nao_valido"
                else:
                    cand, partido = parsear_candidato_partido(col_key)
                    tipo = "candidato"

                resultados.append({
                    "scenario_id": scenario_id,
                    "poll_id": poll_id,
                    "cargo": meta["cargo"],
                    "uf": meta["uf"],
                    "turno": meta["turno"],
                    "instituto": bloco["instituto"],
                    "data_campo": bloco["data_campo"],
                    "cenario": cenario,
                    "candidato": cand,
                    "partido": partido,
                    "tipo": tipo,
                    "percentual": pct,
                    "horario_raspagem": horario_raspagem,
                })

    print(f"  novo -> {len(pesquisas)} cenários | {len(resultados)} resultados")
    return pd.DataFrame(pesquisas), pd.DataFrame(resultados)


def scrape_url(driver, url, horario_raspagem):
    meta = parse_url_meta(url)
    print(f"\n[+] {meta['cargo'].upper()} {meta['uf']} {meta['turno']}")
    print(url)

    driver.get(url)
    esperar_tabela_renderizar(driver)

    layout = decidir_layout(driver)

    if layout == "novo":
        return scrape_novo(driver, url, meta, horario_raspagem)

    return scrape_antigo(driver, url, meta, horario_raspagem)


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

            if not df_p.empty:
                all_p.append(df_p)

            if not df_r.empty:
                all_r.append(df_r)

        df_p_all = pd.concat(all_p, ignore_index=True) if all_p else pd.DataFrame()
        df_r_all = pd.concat(all_r, ignore_index=True) if all_r else pd.DataFrame()

        print("\n=== PESQUISAS ===")
        if not df_p_all.empty:
            print(df_p_all[[
                "uf", "cargo", "instituto", "registro_tse", "data_campo",
                "cenario", "modo", "amostra", "erro"
            ]].to_string(index=False))
        else:
            print("vazio")

        print("\n=== RESULTADOS ===")
        if not df_r_all.empty:
            print(df_r_all[[
                "uf", "cargo", "instituto", "data_campo",
                "cenario", "candidato", "partido", "percentual"
            ]].to_string(index=False))
        else:
            print("vazio")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
