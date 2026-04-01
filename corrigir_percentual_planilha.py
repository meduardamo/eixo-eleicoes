import os
import re

import gspread
from gspread import Cell
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials


CREDENTIALS_FILE = "credentials.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1snoT-Zzf9UBaU9q3Y08_0l8vC7HlJn2hYOkou1qvZPg/edit?gid=1555663512#gid=1555663512"
SPREADSHEET_ID = ""
NOME_ABA = "resultados"
NOME_COLUNA = "percentual"
FORCAR_LOCALE = True
LOCALE_PLANILHA = "en_US"


def gs_client_from_file():
    if not os.path.exists(CREDENTIALS_FILE):
        raise RuntimeError(f"Arquivo de credenciais não encontrado: {CREDENTIALS_FILE}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(credentials)


def normalizar_percentual(valor):
    v = str(valor).strip().lstrip("'")
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", ""):
        return None
    try:
        n = float(v.replace('%', '').replace(',', '.').strip())
        # alguns registros entram inflados (ex.: 46041.0). Converte para 46.0
        if abs(n) >= 1000:
            n = n / 1000.0
        return f"{round(n, 1):.1f}"
    except Exception:
        return None


def obter_sheet_id():
    sheet_url = SPREADSHEET_URL.strip()
    if sheet_url:
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
        if not match:
            raise RuntimeError("SPREADSHEET_URL inválida.")
        return match.group(1)

    sheet_id = SPREADSHEET_ID.strip()
    if not sheet_id:
        raise RuntimeError("Preencha SPREADSHEET_URL ou SPREADSHEET_ID no arquivo.")
    return sheet_id


def obter_aba(spreadsheet, nome_aba):
    try:
        return spreadsheet.worksheet(nome_aba)
    except gspread.exceptions.WorksheetNotFound:
        abas = spreadsheet.worksheets()
        titulos = [w.title for w in abas]

        # fallback simples: tenta singular/plural e comparação sem case-sensitive
        candidatos = {
            nome_aba,
            nome_aba.lower(),
            nome_aba.upper(),
            nome_aba.rstrip("s"),
            f"{nome_aba}s",
        }
        for w in abas:
            titulo = w.title.strip()
            if titulo in candidatos or titulo.lower() in {c.lower() for c in candidatos}:
                print(f"[!] Aba '{nome_aba}' não encontrada. Usando '{w.title}'.")
                return w

        raise RuntimeError(
            f"Aba '{nome_aba}' não encontrada. Abas disponíveis: {', '.join(titulos)}"
        )


def main():
    sheet_id = obter_sheet_id()
    nome_aba = NOME_ABA.strip() or "resultados"
    nome_coluna = NOME_COLUNA.strip() or "percentual"

    print("[+] Conectando ao Google Sheets...")
    gc = gs_client_from_file()
    sh = gc.open_by_key(sheet_id)

    if FORCAR_LOCALE:
        print(f"[+] Definindo locale da planilha para {LOCALE_PLANILHA}...")
        sh.update_locale(LOCALE_PLANILHA)

    aba = obter_aba(sh, nome_aba)

    print(f"[+] Lendo aba '{nome_aba}'...")
    values_raw = aba.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if not values_raw:
        print("[-] Aba vazia")
        return

    header = values_raw[0]
    if nome_coluna not in header:
        raise RuntimeError(f"Coluna '{nome_coluna}' não encontrada na aba '{nome_aba}'.")

    col_idx = header.index(nome_coluna) + 1
    updates = []
    alteradas = 0

    for row_idx, row in enumerate(values_raw[1:], start=2):
        atual_raw = row[col_idx - 1] if len(row) >= col_idx else ""

        novo = normalizar_percentual(atual_raw)
        if novo is None:
            continue
        novo_f = float(novo)

        # Se veio como texto (ex.: "'32.0" ou "32.0"), precisamos regravar
        # como número mesmo quando o valor numérico já é igual.
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
            and round(atual_f, 1) == novo_f
            and abs(atual_f) < 1000
            and not exige_forcar_numero
        ):
            continue

        updates.append(Cell(row=row_idx, col=col_idx, value=novo_f))

        alteradas += 1

    if updates:
        print(f"[+] Atualizando {alteradas} valor(es)...")
        aba.update_cells(updates, value_input_option="RAW")

    # aplica formato 0.0 em toda a coluna de dados (garante exibição uniforme)
    n_linhas = len(values_raw)
    col_letra = rowcol_to_a1(1, col_idx)[:-1]
    intervalo = f"{col_letra}2:{col_letra}{n_linhas}"
    print(f"[+] Aplicando formato numérico na coluna '{nome_coluna}' ({intervalo})...")
    aba.format(intervalo, {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}})
    print("[+] OK")


if __name__ == "__main__":
    main()