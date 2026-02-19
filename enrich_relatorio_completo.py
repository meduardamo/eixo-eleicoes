import os
import re
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

CREDS_PATH = "credentials.json"

HEADER_ROW = 3
DATA_START_ROW = 4

SKIP_SHEETS = {"Dashboard"}

# só o que a gente precisa p/ essa rotina
NEEDED_COLS = [
    "detail_url",
    "pdf_relatorio_completo_url",
    "pdf_relatorio_completo_checado_em",
]

# priorização
FALLBACK_DAYS_AFTER_REGISTRO = 7
RECHECK_DAYS = 3


def make_driver(profile_dir: str = "./chrome-profile-pesqele", headless: bool = False) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")

    if headless or os.getenv("CI"):
        opts.add_argument("--headless=new")

    if not os.getenv("CI"):
        opts.add_argument(f"--user-data-dir={os.path.abspath(profile_dir)}")

    if os.getenv("CI"):
        opts.binary_location = "/usr/bin/chromium-browser"
        return webdriver.Chrome(options=opts)

    return webdriver.Chrome(options=opts)


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = 30) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def gspread_client(creds_path: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Defina SPREADSHEET_ID (env/secrets).")
    return gc.open_by_key(spreadsheet_id)


def iso_to_date(x: str) -> Optional[date]:
    x = (x or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", x):
        return None
    y, m, d = x.split("-")
    return date(int(y), int(m), int(d))


def iso_dt_to_date(x: str) -> Optional[date]:
    x = (x or "").strip()
    if not x:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", x)
    if not m:
        return None
    y, mo, da = m.groups()
    return date(int(y), int(mo), int(da))


def ensure_header_has(ws: gspread.Worksheet, needed_cols: List[str]) -> List[str]:
    header = ws.row_values(HEADER_ROW)
    header = [h.strip() for h in header if h is not None]

    if not header:
        ws.update(f"A{HEADER_ROW}", [["numero_identificacao", "data_divulgacao", "data_registro"] + needed_cols])
        return ws.row_values(HEADER_ROW)

    missing = [c for c in needed_cols if c not in header]
    if missing:
        new_header = header + missing
        ws.update(f"A{HEADER_ROW}", [new_header])
        return new_header

    return header


def col_idx(header: List[str], name: str) -> Optional[int]:
    try:
        return header.index(name) + 1
    except ValueError:
        return None


def should_check_row(row: Dict[str, str]) -> bool:
    today = date.today()

    detail_url = (row.get("detail_url") or "").strip()
    if not detail_url:
        return False

    # se já tem link do relatório completo, não precisa checar
    if (row.get("pdf_relatorio_completo_url") or "").strip():
        return False

    checked_at = iso_dt_to_date(row.get("pdf_relatorio_completo_checado_em") or "")
    if checked_at and (today - checked_at).days < RECHECK_DAYS:
        return False

    data_div = iso_to_date(row.get("data_divulgacao") or "")
    if data_div:
        return data_div <= today

    reg = iso_to_date(row.get("data_registro") or "")
    if reg:
        return (today - reg).days >= FALLBACK_DAYS_AFTER_REGISTRO

    return True


def click_and_capture_new_url(driver: webdriver.Chrome, wait: WebDriverWait, el) -> Optional[str]:
    handles_before = driver.window_handles[:]
    current_before = driver.current_url

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    try:
        driver.execute_script("arguments[0].click();", el)
    except Exception:
        try:
            el.click()
        except Exception:
            return None

    time.sleep(0.6)

    handles_after = driver.window_handles[:]
    if len(handles_after) > len(handles_before):
        new_handle = [h for h in handles_after if h not in handles_before][0]
        driver.switch_to.window(new_handle)
        wait_dom_ready(driver, timeout=20)
        url = driver.current_url
        driver.close()
        driver.switch_to.window(handles_before[0])
        wait_dom_ready(driver, timeout=20)
        return url

    try:
        wait.until(lambda d: d.current_url != current_before)
        url = driver.current_url
        driver.back()
        wait_dom_ready(driver, timeout=20)
        return url
    except Exception:
        return None


def is_disabled(el) -> bool:
    disabled_attr = (el.get_attribute("disabled") or "").strip().lower()
    aria_disabled = (el.get_attribute("aria-disabled") or "").strip().lower()
    if disabled_attr in {"true", "disabled"}:
        return True
    if aria_disabled == "true":
        return True
    return False


def extract_relatorio_completo_url(driver: webdriver.Chrome, wait: WebDriverWait, detail_url: str) -> str:
    driver.get(detail_url)
    wait_dom_ready(driver)
    try:
        wait.until(EC.presence_of_element_located((By.ID, "print")))
    except TimeoutException:
        pass

    label = "Visualizar arquivo relatório completo com o resultado da pesquisa"

    # tenta achar pelo span (como no seu print do devtools)
    xps = [
        f"//span[contains(normalize-space(), '{label}')]/ancestor::button[1]",
        f"//button[contains(normalize-space(), '{label}')]",
    ]

    btn = None
    for xp in xps:
        try:
            btn = driver.find_element(By.XPATH, xp)
            break
        except Exception:
            btn = None

    if not btn:
        return ""

    if is_disabled(btn):
        return ""

    url = click_and_capture_new_url(driver, wait, btn)
    if url:
        return url

    # fallback: às vezes abre popup bloqueado; tenta enter no botão focado
    try:
        btn.send_keys(Keys.ENTER)
        time.sleep(0.6)
        handles = driver.window_handles[:]
        if len(handles) > 1:
            driver.switch_to.window(handles[-1])
            wait_dom_ready(driver, timeout=20)
            url2 = driver.current_url
            driver.close()
            driver.switch_to.window(handles[0])
            wait_dom_ready(driver, timeout=20)
            return url2
    except Exception:
        pass

    return ""


def build_rows_from_sheet(ws: gspread.Worksheet) -> Tuple[List[str], List[Dict[str, str]]]:
    values = ws.get_all_values()
    if len(values) < HEADER_ROW:
        return [], []

    header_row = values[HEADER_ROW - 1]
    header_row = [h.strip() for h in header_row]

    rows = []
    for i in range(DATA_START_ROW - 1, len(values)):
        row_vals = values[i]
        row_dict = {}
        for j, col in enumerate(header_row):
            row_dict[col] = row_vals[j].strip() if j < len(row_vals) else ""
        row_dict["__row_number"] = str(i + 1)
        rows.append(row_dict)

    return header_row, rows


def update_cells_batch(ws: gspread.Worksheet, header: List[str], updates: List[Tuple[int, str, str]]) -> None:
    cell_updates = []
    for row_number, col_name, value in updates:
        idx = col_idx(header, col_name)
        if not idx:
            continue
        cell_updates.append(gspread.Cell(row_number, idx, value))

    if cell_updates:
        ws.update_cells(cell_updates, value_input_option="USER_ENTERED")


def enrich_one_worksheet(ws: gspread.Worksheet, headless: bool = False, limit: Optional[int] = None) -> None:
    header = ensure_header_has(ws, NEEDED_COLS)
    header, rows = build_rows_from_sheet(ws)

    candidates = [r for r in rows if should_check_row(r)]

    # prioriza por data_divulgacao (as mais antigas primeiro)
    def sort_key(r: Dict[str, str]) -> Tuple[int, str]:
        d = r.get("data_divulgacao") or ""
        return (0, d) if d else (1, "9999-99-99")

    candidates = sorted(candidates, key=sort_key)
    if limit:
        candidates = candidates[:limit]

    if not candidates:
        print(f"{ws.title}: nada pra checar")
        return

    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, 30)

    updates: List[Tuple[int, str, str]] = []

    try:
        for i, r in enumerate(candidates, 1):
            numero = (r.get("numero_identificacao") or "").strip()
            detail_url = (r.get("detail_url") or "").strip()

            print(f"{ws.title}: checando relatorio completo {numero} ({i}/{len(candidates)})")

            pdf_url = ""
            try:
                pdf_url = extract_relatorio_completo_url(driver, wait, detail_url)
            except Exception:
                pdf_url = ""

            row_number = int(r["__row_number"])

            if pdf_url:
                updates.append((row_number, "pdf_relatorio_completo_url", pdf_url))

            updates.append((row_number, "pdf_relatorio_completo_checado_em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

            if len(updates) >= 150:
                update_cells_batch(ws, header, updates)
                updates = []

            time.sleep(0.4)

        if updates:
            update_cells_batch(ws, header, updates)

    finally:
        driver.quit()


def run_enrichment_all_sheets(headless: bool = False, per_sheet_limit: Optional[int] = None) -> None:
    gc = gspread_client(CREDS_PATH)
    ss = get_spreadsheet(gc)

    for ws in ss.worksheets():
        if ws.title in SKIP_SHEETS:
            continue

        try:
            enrich_one_worksheet(ws, headless=headless, limit=per_sheet_limit)
        except Exception as e:
            print(f"Erro em {ws.title}: {str(e)[:200]}")
            continue


if __name__ == "__main__":
    headless = bool(os.getenv("CI", False))
    per_sheet_limit = os.getenv("PER_SHEET_LIMIT", "")
    per_sheet_limit = int(per_sheet_limit) if per_sheet_limit.strip().isdigit() else None

    print(f"Enriquecendo só PDF do relatório completo (headless={headless}) SPREADSHEET_ID={os.getenv('SPREADSHEET_ID')}")
    run_enrichment_all_sheets(headless=headless, per_sheet_limit=per_sheet_limit)
    print("Enriquecimento concluído.")
