"""Grava o Senado fechado na planilha do mapeamento, no padrão Eixo."""
from ids import valor
import time
import gspread
import pandas as pd

D = 'dados_novos_eleitos/'
ID = valor('SPREADSHEET_ID_58_SENADO_BASES')
GRUPOS = ['curso', 'reeleitos', 'novos']
ORDEM_SIT = {'Mandato em curso': 0, 'Reeleito': 1, 'Reeleita': 1, 'Novo': 2, 'Nova': 2}
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
ler = lambda p: pd.read_csv(D + p, dtype=str).fillna('')
sen = lambda t: pd.concat([ler(f'perfil_senado_{g}/{t}.csv') for g in GRUPOS], ignore_index=True)

ABAS = [
    ('Notícias: o que defendem', ler('noticias_senado/noticias.csv'), 3),
    ('Notícias: cobertura por área', ler('noticias_senado/cobertura.csv'), 1),
]

gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key(ID)

def largura(col, serie):
    t = serie.astype(str).str.len(); t = t[t > 0]
    q = float(t.quantile(0.8)) if len(t) else 0
    minimo = {'Parlamentar': 170, 'Partido': 115, 'UF': 55, 'Situação': 130}.get(col, 80)
    return int(min(340, max(minimo, 6.5 * max(q, len(col) / 2.2) + 16)))

def tentar(f, *a, **k):
    for i in range(6):
        try:
            return f(*a, **k)
        except gspread.exceptions.APIError as e:
            if '429' not in str(e) and '50' not in str(e)[:12]:
                raise
            time.sleep(20 * (i + 1))
    raise RuntimeError('cota do Sheets')

for titulo, df, congelar in ABAS:
    df = df.fillna('').astype(str)
    try:
        ws = sh.worksheet(titulo)
        tentar(sh.batch_update, {'requests': [{'clearBasicFilter': {'sheetId': ws.id}}]})
        tentar(ws.clear)
    except gspread.WorksheetNotFound:
        ws = tentar(sh.add_worksheet, title=titulo, rows=len(df) + 1, cols=len(df.columns))
    tentar(ws.resize, rows=len(df) + 1, cols=len(df.columns))
    valores = [list(df.columns)] + df.values.tolist()
    for i in range(0, len(valores), 2500):
        tentar(ws.update, values=valores[i:i + 2500], range_name=f'A{i + 1}', value_input_option='RAW')
        time.sleep(1.2)
    n, m = len(df) + 1, len(df.columns)
    borda = {'style': 'SOLID', 'color': cor('#DADAD4')}
    fmt = lambda a, b, fundo, texto, negrito, wrap: {'repeatCell': {
        'range': {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b},
        'cell': {'userEnteredFormat': {'backgroundColor': cor(fundo), 'wrapStrategy': wrap, 'verticalAlignment': 'MIDDLE',
                 'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 9, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)'}}
    dim = lambda d, a, b, px: {'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': d, 'startIndex': a, 'endIndex': b},
        'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
    reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {
                'frozenRowCount': 1, 'frozenColumnCount': min(congelar, m)}},
                'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
            fmt(0, 1, '#192D4E', '#FFFFFF', True, 'WRAP'), fmt(1, n, '#F4F3EF', '#000000', False, 'CLIP'),
            {'updateBorders': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': n, 'startColumnIndex': 0,
                               'endColumnIndex': m}, 'innerHorizontal': borda, 'innerVertical': borda, 'bottom': borda, 'right': borda}},
            dim('ROWS', 0, 1, 46), dim('ROWS', 1, n, 21),
            {'setBasicFilter': {'filter': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': n,
                                                     'startColumnIndex': 0, 'endColumnIndex': m}}}}]
    reqs += [dim('COLUMNS', i, i + 1, largura(c, df[c])) for i, c in enumerate(df.columns)]
    tentar(sh.batch_update, {'requests': reqs})
    print(titulo, len(df), 'x', m, flush=True)
    time.sleep(2)

print('ok')
