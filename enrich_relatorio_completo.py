# enrich_relatorio_completo.py
import os
import re
import time
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException


URL_LISTAR = "https://pesqele-divulgacao.tse.jus.br/app/pesquisa/listar.xhtml"

ID_ELEICAO_LABEL = "formPesquisa:eleicoes_label"
ID_ELEICAO_PANEL = "formPesquisa:eleicoes_panel"

ID_UF_LABEL = "formPesquisa:filtroUF_label"
ID_UF_PANEL = "formPesquisa:filtroUF_panel"

ID_BTN_PESQUISAR = "formPesquisa:idBtnPesquisar"

ID_TBODY = "formPesquisa:tabelaPesquisas_data"
ID_PAGINATOR = "formPesquisa:tabelaPesquisas_paginator_bottom"

CREDS_PATH = "credentials.json"

HEADER_ROW = 3
DATA_START_ROW = 4

SKIP_SHEETS = {"Dashboard"}

ELEICAO_TEXT = os.getenv("ELEICAO_TEXT", "Eleições Gerais 2026")

PDF_LABEL = "Visualizar arquivo relatório completo com o resultado da pesquisa"

RECHECK_DAYS = int(os.getenv("RECHECK_DAYS", "3"))
PER_SHEET_LIMIT = int(os.getenv("PER_SHEET_LIMIT", "0") or "0") or None

DOWNLOAD_PDF = os.getenv("DOWNLOAD_PDF", "0").strip() == "1"
PDF_DIR = os.getenv("PDF_DIR", "pdfs_relatorios")


NEEDED_COLS = [
    "numero_identificacao",
    "data_divulgacao",
    "uf_filtro",
    "pdf_relatorio_completo_url",
    "pdf_relatorio_completo_local",
    "pdf_relatorio_completo_checado_em",
]


def make_driver(profile_dir: str = "./chrome-profile-pesqele", headless: bool = False) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")

    if headless or os.getenv("CI") or os.getenv("HEADLESS") == "1":
        opts.add_argument("--headless=new")

    if not os.getenv("CI"):
        opts.add_argument(f"--user-data-dir={os.path.abspath(profile_dir)}")

    caps = webdriver.DesiredCapabilities.CHROME.copy()
    caps["goog:loggingPrefs"] = {"performance": "ALL"}

    if os.getenv("CI"):
        opts.binary_location = "/usr/bin/chromium-browser"
        return webdriver.Chrome(options=opts, desired_capabilities=caps)

    return webdriver.Chrome(options=opts, desired_capabilities=caps)


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = 30) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def safe_click(driver: webdriver.Chrome, wait: WebDriverWait, by: By, value: str, timeout: int = 30):
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
    try:
        el.click()
    except Exception:
        driver.execute_script("arguments[0].click();", el)
    return el


def force_close_any_menu(driver: webdriver.Chrome):
    try:
        driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    except Exception:
        pass
    try:
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass


def open_menu(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str) -> None:
    safe_click(driver, wait, By.ID, label_id)
    wait.until(EC.presence_of_element_located((By.ID, panel_id)))
    wait.until(EC.visibility_of_element_located((By.ID, panel_id)))


def select_one_menu_by_text(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str, text: str) -> None:
    open_menu(driver, wait, label_id, panel_id)

    panel = driver.find_element(By.ID, panel_id)
    item = panel.find_element(By.XPATH, f".//li[normalize-space()='{text}']")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
    try:
        item.click()
    except Exception:
        driver.execute_script("arguments[0].click();", item)

    force_close_any_menu(driver)


def click_and_wait_table_refresh(driver: webdriver.Chrome, wait: WebDriverWait, btn_id: str, tbody_id: str) -> None:
    old_tbody = None
    try:
        old_tbody = driver.find_element(By.ID, tbody_id)
    except Exception:
        pass

    btn = safe_click(driver, wait, By.ID, btn_id)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    try:
        btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", btn)

    if old_tbody is not None:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.ID, tbody_id)))


def wait_list_page_ready(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.ID, ID_TBODY)))
    wait.until(EC.presence_of_element_located((By.ID, ID_PAGINATOR)))


def wait_detail_page_ready(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.ID, "print")))


def get_page_numbers(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str) -> List[int]:
    pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
    links = pag.find_elements(By.CSS_SELECTOR, "a.ui-paginator-page")

    nums = []
    for a in links:
        txt = (a.text or "").strip()
        if txt.isdigit():
            nums.append(int(txt))
    return sorted(set(nums))


def go_to_page(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str, tbody_id: str, page_num: int, max_tries: int = 6) -> None:
    last_err = None
    for _ in range(max_tries):
        try:
            pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
            a = pag.find_element(By.CSS_SELECTOR, f"a.ui-paginator-page[aria-label='Page {page_num}']")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)

            tbody_before = driver.find_element(By.ID, tbody_id)
            driver.execute_script("arguments[0].click();", a)

            try:
                wait.until(EC.staleness_of(tbody_before))
            except TimeoutException:
                pass

            wait.until(EC.presence_of_element_located((By.ID, tbody_id)))
            return

        except (StaleElementReferenceException, TimeoutException) as e:
            last_err = e
            time.sleep(0.5)

    raise last_err


def extract_pdf_url_from_performance_logs(driver: webdriver.Chrome, timeout: int = 18) -> str:
    deadline = time.time() + timeout
    best_url = ""

    while time.time() < deadline:
        try:
            logs = driver.get_log("performance")
        except Exception:
            logs = []

        for entry in logs:
            try:
                payload = json.loads(entry.get("message", "{}"))
                msg = payload.get("message", {})
                method = msg.get("method", "")
                params = msg.get("params", {})
            except Exception:
                continue

            if method != "Network.responseReceived":
                continue

            response = params.get("response", {}) or {}
            url = (response.get("url") or "").strip()
            if not url:
                continue

            mime = (response.get("mimeType") or "").lower()
            headers = response.get("headers") or {}
            ct = ""
            if isinstance(headers, dict):
                ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()

            if "application/pdf" in mime or "application/pdf" in ct:
                return url

            if url.lower().endswith(".pdf") or "pdf" in url.lower():
                best_url = url

        time.sleep(0.3)

    return best_url


def enable_network_capture(driver: webdriver.Chrome) -> None:
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.enable", {})
        driver.execute_cdp_cmd("Runtime.enable", {})
    except Exception:
        pass


def clear_performance_logs(driver: webdriver.Chrome) -> None:
    try:
        driver.get_log("performance")
    except Exception:
        pass


def is_disabled(el) -> bool:
    disabled_attr = (el.get_attribute("disabled") or "").strip().lower()
    aria_disabled = (el.get_attribute("aria-disabled") or "").strip().lower()
    return disabled_attr in {"true", "disabled"} or aria_disabled == "true"


def find_relatorio_button(driver: webdriver.Chrome):
    xps = [
        f"//span[contains(normalize-space(), '{PDF_LABEL}')]/ancestor::button[1]",
        f"//button[contains(normalize-space(), '{PDF_LABEL}')]",
    ]
    for xp in xps:
        try:
            return driver.find_element(By.XPATH, xp)
        except Exception:
            pass
    return None


def download_with_session_cookies(driver: webdriver.Chrome, url: str, out_path: str) -> bool:
    if not url:
        return False

    sess = requests.Session()

    for c in driver.get_cookies():
        try:
            sess.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        except Exception:
            sess.cookies.set(c["name"], c["value"])

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Referer": driver.current_url,
    }

    try:
        resp = sess.get(url, headers=headers, timeout=60, allow_redirects=True)
        if resp.status_code != 200:
            return False

        ct = (resp.headers.get("Content-Type") or "").lower()
        if "pdf" not in ct and resp.content[:4] != b"%PDF":
            return False

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception:
        return False


def try_get_relatorio_completo(driver: webdriver.Chrome, wait: WebDriverWait, numero: str) -> Tuple[str, str]:
    btn = find_relatorio_button(driver)
    if not btn:
        return "", ""

    if is_disabled(btn):
        return "", ""

    enable_network_capture(driver)
    clear_performance_logs(driver)

    before_handles = set(driver.window_handles)

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    try:
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        try:
            btn.click()
        except Exception:
            return "", ""

    pdf_url = extract_pdf_url_from_performance_logs(driver, timeout=18)

    # se abriu nova aba, tenta capturar alguma URL útil (headless geralmente não tem viewer)
    try:
        WebDriverWait(driver, 6).until(lambda d: len(d.window_handles) > len(before_handles))
        new_handles = [h for h in driver.window_handles if h not in before_handles]
        if new_handles:
            driver.switch_to.window(new_handles[0])
            time.sleep(0.5)
            cur = (driver.current_url or "").strip()
            if cur and cur.startswith("http") and ("pdf" in cur.lower() or cur.lower().endswith(".pdf")):
                if not pdf_url:
                    pdf_url = cur
            driver.close()
            driver.switch_to.window(list(before_handles)[0])
    except Exception:
        pass

    local_path = ""
    if DOWNLOAD_PDF and pdf_url:
        safe = re.sub(r"[^A-Za-z0-9\-_\.]", "_", (numero or "relatorio"))
        out_path = os.path.abspath(os.path.join(PDF_DIR, f"{safe}.pdf"))
        ok = download_with_session_cookies(driver, pdf_url, out_path)
        local_path = out_path if ok else ""

    return pdf_url, local_path


def click_lupa_for_row(driver: webdriver.Chrome, wait: WebDriverWait, row_el) -> bool:
    try:
        lupa = row_el.find_element(By.CSS_SELECTOR, "a[id$=':detalhar']")
    except NoSuchElementException:
        try:
            lupa = row_el.find_element(By.CSS_SELECTOR, "a[title='Visualizar dados da pesquisa']")
        except NoSuchElementException:
            return False

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", lupa)
    try:
        driver.execute_script("arguments[0].click();", lupa)
    except Exception:
        try:
            lupa.click()
        except Exception:
            return False

    wait_detail_page_ready(driver, wait)
    return True


def open_detail_by_numero(driver: webdriver.Chrome, wait: WebDriverWait, numero: str) -> bool:
    numero = (numero or "").strip()
    if not numero:
        return False

    pages = get_page_numbers(driver, wait, ID_PAGINATOR)
    if not pages:
        pages = [1]

    for p in pages:
        if p != pages[0]:
            go_to_page(driver, wait, ID_PAGINATOR, ID_TBODY, p)

        tbody = driver.find_element(By.ID, ID_TBODY)
        trs = tbody.find_elements(By.XPATH, ".//tr")

        for tr in trs:
            tds = tr.find_elements(By.XPATH, "./td")
            if not tds:
                continue
            num_cell = (tds[0].text or "").strip()
            if num_cell == numero:
                return click_lupa_for_row(driver, wait, tr)

    return False


def parse_iso_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return None
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


def parse_iso_dt(s: str) -> Optional[date]:
    s = (s or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    y, mo, da = m.groups()
    return date(int(y), int(mo), int(da))


def should_check(row: Dict[str, str]) -> bool:
    today = date.today()

    if (row.get("pdf_relatorio_completo_url") or "").strip():
        return False

    checked = parse_iso_dt(row.get("pdf_relatorio_completo_checado_em") or "")
    if checked and (today - checked).days < RECHECK_DAYS:
        return False

    data_div = parse_iso_date(row.get("data_divulgacao") or "")
    if data_div:
        return data_div <= today

    return True


def sheet_safe(name: str) -> str:
    s = re.sub(r"[\[\]\:\*\?\/\\]", "-", name.strip())
    return s[:31] if len(s) > 31 else s


def gspread_client(creds_path: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("SPREADSHEET_ID não definido (use Secrets/env).")
    return gc.open_by_key(sid)


def ensure_header_has(ws: gspread.Worksheet, needed: List[str]) -> List[str]:
    header = ws.row_values(HEADER_ROW)
    header = [h.strip() for h in header if h is not None]

    if not header:
        ws.update(f"A{HEADER_ROW}", [needed])
        return needed

    missing = [c for c in needed if c not in header]
    if missing:
        ws.update(f"A{HEADER_ROW}", [header + missing])
        return header + missing

    return header


def col_idx(header: List[str], name: str) -> Optional[int]:
    try:
        return header.index(name) + 1
    except ValueError:
        return None


def build_rows(ws: gspread.Worksheet) -> Tuple[List[str], List[Dict[str, str]]]:
    values = ws.get_all_values()
    if len(values) < HEADER_ROW:
        return [], []

    header = [h.strip() for h in values[HEADER_ROW - 1]]
    rows = []
    for i in range(DATA_START_ROW - 1, len(values)):
        row_vals = values[i]
        d = {}
        for j, col in enumerate(header):
            d[col] = row_vals[j].strip() if j < len(row_vals) else ""
        d["__row_number"] = str(i + 1)
        rows.append(d)

    return header, rows


def update_cells_batch(ws: gspread.Worksheet, header: List[str], updates: List[Tuple[int, str, str]]) -> None:
    cells = []
    for row_number, col_name, value in updates:
        idx = col_idx(header, col_name)
        if not idx:
            continue
        cells.append(gspread.Cell(row_number, idx, value))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


def enrich_worksheet(ws: gspread.Worksheet) -> None:
    title = ws.title
    if title in SKIP_SHEETS:
        return

    header = ensure_header_has(ws, NEEDED_COLS)
    header, rows = build_rows(ws)

    candidates = []
    for r in rows:
        if not should_check(r):
            continue
        if not (r.get("numero_identificacao") or "").strip():
            continue
        candidates.append(r)

    if not candidates:
        print(f"{title}: nada pra checar")
        return

    def sort_key(r: Dict[str, str]) -> Tuple[int, str]:
        d = (r.get("data_divulgacao") or "").strip()
        return (0, d) if d else (1, "9999-99-99")

    candidates = sorted(candidates, key=sort_key)
    if PER_SHEET_LIMIT:
        candidates = candidates[:PER_SHEET_LIMIT]

    driver = make_driver(headless=(os.getenv("CI") or os.getenv("HEADLESS") == "1"))
    wait = WebDriverWait(driver, 30)

    updates: List[Tuple[int, str, str]] = []

    try:
        driver.get(URL_LISTAR)
        wait_dom_ready(driver)

        uf_text = (title or "").strip()
        if not uf_text:
            return

        select_one_menu_by_text(driver, wait, ID_ELEICAO_LABEL, ID_ELEICAO_PANEL, ELEICAO_TEXT)
        time.sleep(0.3)
        select_one_menu_by_text(driver, wait, ID_UF_LABEL, ID_UF_PANEL, uf_text)
        time.sleep(0.3)

        click_and_wait_table_refresh(driver, wait, ID_BTN_PESQUISAR, ID_TBODY)
        wait_list_page_ready(driver, wait)

        for i, r in enumerate(candidates, 1):
            numero = (r.get("numero_identificacao") or "").strip()
            row_number = int(r["__row_number"])

            print(f"{title}: {numero} ({i}/{len(candidates)})")

            pdf_url = ""
            local_path = ""

            ok = False
            try:
                ok = open_detail_by_numero(driver, wait, numero)
            except Exception:
                ok = False

            if ok:
                try:
                    pdf_url, local_path = try_get_relatorio_completo(driver, wait, numero)
                except Exception:
                    pdf_url, local_path = "", ""

                driver.back()
                wait_dom_ready(driver)
                wait_list_page_ready(driver, wait)

            if pdf_url:
                updates.append((row_number, "pdf_relatorio_completo_url", pdf_url))
            if local_path:
                updates.append((row_number, "pdf_relatorio_completo_local", local_path))

            updates.append((row_number, "pdf_relatorio_completo_checado_em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

            if len(updates) >= 150:
                update_cells_batch(ws, header, updates)
                updates = []

            time.sleep(0.3)

        if updates:
            update_cells_batch(ws, header, updates)

    finally:
        driver.quit()


def run_all() -> None:
    gc = gspread_client(CREDS_PATH)
    ss = get_spreadsheet(gc)

    for ws in ss.worksheets():
        if ws.title in SKIP_SHEETS:
            continue
        try:
            enrich_worksheet(ws)
        except Exception as e:
            print(f"Erro em {ws.title}: {str(e)[:200]}")
            continue


if __name__ == "__main__":
    run_all()
    print("Enrichment de relatório completo finalizado.")
