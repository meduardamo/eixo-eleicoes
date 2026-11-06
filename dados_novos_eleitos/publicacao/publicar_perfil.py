from ids import valor
import sys, time
import gspread
import pandas as pd

PASTA = 'dados_novos_eleitos/perfil_senado_curso/'
NOME = 'Mapeamento de parlamentares | Senado, eleitos em 2022 (perfil geral)'
DONA = valor('EMAIL_DONA_PLANILHAS')
ABAS = [('resumo', 'Resumo', 1), ('temas', 'Temas', 1), ('colegiados', 'Colegiados', 1), ('autoria', 'Autoria', 2),
        ('relatorias', 'Relatorias', 2), ('emendas', 'Emendas', 2), ('votos', 'Votações', 3), ('discursos', 'Discursos', 2)]
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}

gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key(sys.argv[1]) if len(sys.argv) > 1 else gc.create(NOME)
print(sh.id)

def largura(col, s):
    t = s.astype(str).str.len(); t = t[t > 0]
    q = float(t.quantile(0.8)) if len(t) else 0
    return int(min(340, max(70, 6.5 * max(q, len(col) / 2.2) + 16)))

for arq, titulo, congelar in ABAS:
    df = pd.read_csv(PASTA + arq + '.csv', dtype=str).fillna('')
    if arq == 'votos':
        df['Ementa'] = df['Ementa'].str.slice(0, 300)
    try:
        ws = sh.worksheet(titulo); ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=titulo, rows=len(df) + 1, cols=len(df.columns))
    ws.resize(rows=len(df) + 1, cols=len(df.columns))
    valores = [list(df.columns)] + df.values.tolist()
    for i in range(0, len(valores), 2500):
        ws.update(values=valores[i:i + 2500], range_name=f'A{i + 1}', value_input_option='RAW')
        time.sleep(1.5)
    n, k = len(df) + 1, len(df.columns)
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
                'frozenRowCount': 1, 'frozenColumnCount': min(congelar, k)}},
                'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
            fmt(0, 1, '#192D4E', '#FFFFFF', True, 'WRAP'), fmt(1, n, '#F4F3EF', '#000000', False, 'CLIP'),
            {'updateBorders': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': n, 'startColumnIndex': 0,
                               'endColumnIndex': k}, 'innerHorizontal': borda, 'innerVertical': borda, 'bottom': borda, 'right': borda}},
            dim('ROWS', 0, 1, 46), dim('ROWS', 1, n, 21),
            {'setBasicFilter': {'filter': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': n,
                                                     'startColumnIndex': 0, 'endColumnIndex': k}}}}]
    reqs += [dim('COLUMNS', i, i + 1, largura(c, df[c])) for i, c in enumerate(df.columns)]
    sh.batch_update({'requests': reqs})
    print(titulo, len(df), 'x', k)
    time.sleep(2)

for ws in sh.worksheets():
    if ws.title not in [t for _, t, _ in ABAS]:
        sh.del_worksheet(ws)
if len(sys.argv) == 1:
    sh.share(DONA, perm_type='user', role='writer', notify=False)
print('https://docs.google.com/spreadsheets/d/' + sh.id)
