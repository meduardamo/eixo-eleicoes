"""Aba Definição dos subtemas da planilha da 58ª Legislatura: o que entra e o que fica fora de cada subtema,
a régua usada na leitura das ementas (temas_clientes/subtemas_definicao.csv, gerado pela etapa 25)."""
from ids import valor
import sys

import gspread
import pandas as pd

sys.path.insert(0, 'dados_novos_eleitos/publicacao')
from ordem_abas import ordenar

ID = valor('SPREADSHEET_ID_58_SENADO')
TITULO = 'Definição dos subtemas'
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, BORDA = '#192D4E', '#F4F3EF', '#DADAD4'

d = pd.read_csv('dados_novos_eleitos/temas_clientes/subtemas_definicao.csv', dtype=str, keep_default_na=False)
valores = [list(d.columns)] + d.values.tolist()
n, m = len(valores), len(valores[0])

sh = gspread.service_account(filename='credentials.json').open_by_key(ID)
try:
    ws = sh.worksheet(TITULO)
    ws.clear()
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TITULO, rows=n, cols=m)
ws.resize(rows=n, cols=m)
ws.update(values=valores, range_name='A1', value_input_option='RAW')

R = lambda a, b, c0=0, c1=m: {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}
fina = {'style': 'SOLID', 'color': cor(BORDA)}


def fmt(r, fundo, texto, negrito):
    return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': negrito, 'foregroundColor': cor(texto)},
        'borders': {'top': fina, 'bottom': fina, 'left': fina, 'right': fina}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat,borders)'}}


largura = lambda i, px: {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                                       'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
reqs = [{'updateCells': {'range': R(0, n), 'fields': 'userEnteredFormat'}},
        fmt(R(0, 1), MARINHO, '#FFFFFF', True), fmt(R(1, n), GELO, '#000000', False),
        fmt(R(1, n, 0, 2), GELO, MARINHO, True),
        {'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'hideGridlines': True}},
                                   'fields': 'gridProperties(frozenRowCount,hideGridlines)'}},
        {'setBasicFilter': {'filter': {'range': R(0, n)}}},
        largura(0, 140), largura(1, 300), largura(2, 620), largura(3, 460),
        {'autoResizeDimensions': {'dimensions': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': n}}}]
sh.batch_update({'requests': reqs})
print(TITULO, n - 1, 'subtemas', ordenar(sh))
