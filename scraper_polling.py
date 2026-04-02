"""
backfill_colunas.py
-------------------
Roda UMA VEZ para preencher retroativamente:
  - candidato_partido  → aba RESULTADOS
  - fonte_url_original → aba PESQUISAS

Variáveis de ambiente necessárias:
  GOOGLE_CREDENTIALS_JSON
  SPREADSHEET_ID_POLLINGDATA
"""

import os
import re
import time
import json
import hashlib

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

WAIT_CSS = "div#dados-das-pesquisas"


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _norm_ws(s) -> str:
    try:
        if pd.isna(s):
            s = ""
    except (TypeError, ValueError):
        pass
    return re.sub(r"\s+", " ", str(s)).strip()


def _slug(s: str) -> str:
    s = _norm_ws(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _sha1_short(s: str, n=10) -> str:
    return hashlib.sha1(str(s).encode("utf-8", errors="ignore")).hexdigest()[:n]


def parsear_pesquisa(texto):
    nome, id_pesquisa, data = "", "", ""
    linhas = [l.strip() for l in str(texto).strip().split("\n") if l.strip()]
    for linha in linhas:
        if re.match(r"^\(\d+\)$", linha):
            continue
        match_data = re.search(r"(\d{4}-\d{2}-\d{2})", linha)
        if match_data:
            data = match_data.group(1)
            antes = linha[:linha.index(data)].strip()
            if antes:
                id_pesquisa = antes
        else:
            nome = re.sub(r"\s*\(\d+\)\s*$", "", linha).strip()
    return _norm_ws(nome), _norm_ws(id_pesquisa), _norm_ws(data)


def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash):
    uf = uf.upper()
    data_campo = _norm_ws(data_campo)
    instituto_slug = _slug(instituto)
    if id_pesquisa and id_pesquisa.lower() not in ("sem registro", "sem_registro", "semregistro", "nan"):
        return f"{uf}|{cargo}|{turno}|{id_pesquisa}|{data_campo}"
    return f"{uf}|{cargo}|{turno}|{instituto_slug}|{data_campo}|{raw_block_hash}"


def parse_url_meta(url: str):
    u = url.strip()
    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>governador)/(?P<uf>[a-z]{2})/.*?_t(?P<turno>\d)\.html", u, re.I)
    if m:
        return {"cargo": "governador", "uf": m.group("uf").upper(), "turno": f"t{m.group('turno')}"}
    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>presidente)/(?P<uf>br)/\d{4}_presidente_br_(?P<turno>t\d)", u, re.I)
    if m:
        return {"cargo": "presidente", "uf": "BR", "turno": m.group("turno").lower()}
    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>senador)/(?P<uf>[a-z]{2})/(?:.*?_(?P<turno>t\d)\.html|(?P<turno2>t\d)/?$)",
        u, re.I)
    if m:
        turno = m.group("turno") or m.group("turno2")
        return {"cargo": "senador", "uf": m.group("uf").upper(), "turno": turno.lower()}
    return {"cargo": None, "uf": None, "turno": None}


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

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


def col_index(headers: list, name: str) -> int:
    try:
        return headers.index(name)
    except ValueError:
        return -1


def garantir_coluna(aba, headers: list, nome_col: str) -> int:
    """Garante que a coluna existe no header; adiciona ao final se não existir."""
    idx = col_index(headers, nome_col)
    if idx >= 0:
        return idx
    novo_idx = len(headers)
    col_letra = gspread.utils.rowcol_to_a1(1, novo_idx + 1)[:-1]
    aba.update([[nome_col]], range_name=f"{col_letra}1")
    headers.append(nome_col)
    print(f"  [schema] coluna '{nome_col}' adicionada na posição {novo_idx + 1}")
    return novo_idx


def expandir_aba_se_necessario(aba, n_linhas_dados: int):
    """
    Garante que a aba tem linhas suficientes para receber os dados.
    n_linhas_dados = número de linhas de dados (sem contar o header).
    """
    n_necessario = n_linhas_dados + 1  # +1 pelo header
    props = aba.spreadsheet.get_worksheet_by_id(aba.id)
    atual = aba.row_count
    if n_necessario > atual:
        extra = n_necessario - atual + 1000  # margem de 1000
        aba.add_rows(extra)
        print(f"  [resize] aba '{aba.title}' expandida: {atual} → {atual + extra} linhas")


# ---------------------------------------------------------------------------
# Selenium
# ---------------------------------------------------------------------------

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


def extrair_link_fonte_do_grupo(group) -> str:
    seletores = [
        "table#tab_instituto a[href]",
        "div.rt-td-inner table a[href]",
        "div[id^='tab_'] a[href]",
        ".rt-expandable-content a[href]",
    ]
    for sel in seletores:
        try:
            el = group.find_element(By.CSS_SELECTOR, sel)
            href = (el.get_attribute("href") or "").strip()
            if href and href.startswith("http"):
                return href
        except Exception:
            continue
    return ""


def raspar_links_por_poll_id(driver, url: str) -> dict:
    meta = parse_url_meta(url)
    cargo = meta["cargo"]
    uf    = meta["uf"]
    turno = meta["turno"]

    if not cargo:
        print(f"  [-] URL não reconhecida: {url}")
        return {}

    print(f"  [backfill] {cargo.upper()} {uf} {turno} -> {url}")
    driver.get(url)
    time.sleep(10)

    try:
        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
        )
    except Exception:
        print("    [-] timeout")
        return {}

    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    resultado = {}
    for group in secao.find_elements(By.CSS_SELECTOR, "div.rt-tbody div.rt-tr-group"):
        link_fonte = extrair_link_fonte_do_grupo(group)

        texto_pesquisa = ""
        for row in group.find_elements(By.CSS_SELECTOR, "div.rt-tr"):
            cells = row.find_elements(By.CSS_SELECTOR, "div.rt-td")
            if cells and cells[0].text.strip():
                texto_pesquisa = cells[0].text.strip()
                break

        if not texto_pesquisa:
            continue

        nome, id_pesquisa, data_campo = parsear_pesquisa(texto_pesquisa)
        block_hash = _sha1_short(_norm_ws(texto_pesquisa), 10)
        poll_id = gerar_poll_id(uf, nome, id_pesquisa, data_campo, cargo, turno, block_hash)

        if poll_id and link_fonte:
            resultado[poll_id] = link_fonte

    print(f"    [+] {len(resultado)} poll_ids com link capturado")
    return resultado


# ---------------------------------------------------------------------------
# Envio em lotes — range qualificado com nome da aba
# ---------------------------------------------------------------------------

def _aplicar_updates(aba, updates: list, batch_size: int = 500):
    """
    updates: lista de (row_1based, col_1based, valor)
    O range é qualificado com o nome da aba ('NomeAba'!A1) para que a API
    do Sheets nunca confunda com outra aba.
    """
    if not updates:
        print("  [skip] nenhum update necessário")
        return

    # Garante que a aba tem linhas suficientes para o maior índice de linha
    max_row = max(r for r, c, v in updates)
    if max_row > aba.row_count:
        extra = max_row - aba.row_count + 1000
        aba.add_rows(extra)
        print(f"  [resize] aba '{aba.title}' expandida para {aba.row_count + extra} linhas")

    nome_aba = aba.title  # ex: "resultados" ou "pesquisas"
    total = 0

    for i in range(0, len(updates), batch_size):
        lote = updates[i: i + batch_size]
        data = []
        for r, c, v in lote:
            celula = gspread.utils.rowcol_to_a1(r, c)
            # Qualifica com o nome da aba: 'resultados'!T6993
            range_qualificado = f"'{nome_aba}'!{celula}"
            data.append({"range": range_qualificado, "values": [[v]]})

        aba.spreadsheet.values_batch_update({
            "valueInputOption": "RAW",
            "data": data,
        })
        total += len(lote)
        print(f"  [update] {total}/{len(updates)} células enviadas")
        time.sleep(1)

    print(f"  [ok] {total} células atualizadas em '{nome_aba}'")


# ---------------------------------------------------------------------------
# Backfill: candidato_partido → aba RESULTADOS
# ---------------------------------------------------------------------------

def backfill_candidato_partido(aba):
    print("[resultados] lendo planilha...")
    values = aba.get_all_values()
    if not values or len(values) < 2:
        print("  [-] aba vazia")
        return

    headers = values[0]
    idx_cp = garantir_coluna(aba, headers, "candidato_partido")

    idx_candidato = col_index(headers, "candidato")
    idx_partido   = col_index(headers, "partido")
    idx_cp_exist  = col_index(headers, "candidato_partido")

    if idx_candidato < 0 or idx_partido < 0:
        print("  [-] colunas candidato/partido não encontradas — abortando")
        return

    updates = []
    for i, row in enumerate(values[1:], start=2):
        def get(idx):
            return row[idx].strip() if 0 <= idx < len(row) else ""

        candidato = get(idx_candidato)
        partido   = get(idx_partido)
        cp_atual  = get(idx_cp_exist)

        if cp_atual or not candidato:
            continue

        if candidato.lower() in ("não válido", "nao valido"):
            novo_cp = "Não válido"
        else:
            novo_cp = f"{candidato} ({partido})" if partido else candidato

        updates.append((i, idx_cp + 1, novo_cp))

    print(f"  [resultados] {len(updates)} células candidato_partido para preencher")
    _aplicar_updates(aba, updates)


# ---------------------------------------------------------------------------
# Backfill: fonte_url_original → aba PESQUISAS
# ---------------------------------------------------------------------------

def backfill_fonte_url_original(aba, poll_id_para_link: dict):
    print("[pesquisas] lendo planilha...")
    values = aba.get_all_values()
    if not values or len(values) < 2:
        print("  [-] aba vazia")
        return

    headers = values[0]
    idx_fo = garantir_coluna(aba, headers, "fonte_url_original")

    idx_poll_id  = col_index(headers, "poll_id")
    idx_fo_exist = col_index(headers, "fonte_url_original")

    if idx_poll_id < 0:
        print("  [-] coluna poll_id não encontrada — abortando")
        return

    updates = []
    for i, row in enumerate(values[1:], start=2):
        def get(idx):
            return row[idx].strip() if 0 <= idx < len(row) else ""

        poll_id  = get(idx_poll_id)
        fo_atual = get(idx_fo_exist)

        if fo_atual or not poll_id:
            continue

        link = poll_id_para_link.get(poll_id, "")
        if link:
            updates.append((i, idx_fo + 1, link))

    print(f"  [pesquisas] {len(updates)} células fonte_url_original para preencher")
    _aplicar_updates(aba, updates)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    sheet_id = (os.getenv("SPREADSHEET_ID_POLLINGDATA", "") or "").strip()
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID_POLLINGDATA não definido.")

    print("[+] Conectando ao Google Sheets...")
    gc = gs_client_from_env()
    sh = gc.open_by_key(sheet_id)

    # --- candidato_partido na aba resultados (sem Chrome) ---
    try:
        aba_res = sh.worksheet("resultados")
        backfill_candidato_partido(aba_res)
    except gspread.exceptions.WorksheetNotFound:
        print("[-] aba 'resultados' não encontrada, pulando.")

    # --- fonte_url_original na aba pesquisas (precisa do Chrome) ---
    try:
        aba_pes = sh.worksheet("pesquisas")
    except gspread.exceptions.WorksheetNotFound:
        print("[-] aba 'pesquisas' não encontrada, encerrando.")
        return

    print("[+] Identificando URLs para raspar (aba pesquisas)...")
    vals_pes = aba_pes.get_all_values()
    headers_pes = vals_pes[0] if vals_pes else []
    idx_fu = col_index(headers_pes, "fonte_url")
    idx_fo = col_index(headers_pes, "fonte_url_original")

    if idx_fu < 0:
        print("[-] Coluna 'fonte_url' não encontrada na aba pesquisas. Encerrando.")
        return

    urls_sem_link = set()
    for row in vals_pes[1:]:
        fu = row[idx_fu].strip() if idx_fu < len(row) else ""
        fo = row[idx_fo].strip() if 0 <= idx_fo < len(row) else ""
        if fu and not fo and "pollingdata.com.br" in fu:
            urls_sem_link.add(fu)

    print(f"  [+] {len(urls_sem_link)} URLs do pollingdata com linhas sem fonte_url_original")

    if not urls_sem_link:
        print("[+] Nada para raspar — todas as linhas já têm fonte_url_original.")
        return

    print("[+] Iniciando Chrome...")
    driver = criar_driver()
    poll_id_para_link = {}

    try:
        for url in sorted(urls_sem_link):
            mapa = raspar_links_por_poll_id(driver, url)
            poll_id_para_link.update(mapa)
    finally:
        driver.quit()

    print(f"[+] Total de poll_ids mapeados: {len(poll_id_para_link)}")

    if not poll_id_para_link:
        print("[-] Nenhum link capturado. Verifique os seletores CSS.")
        return

    backfill_fonte_url_original(aba_pes, poll_id_para_link)

    print("[+] Backfill concluído.")


if __name__ == "__main__":
    main()
