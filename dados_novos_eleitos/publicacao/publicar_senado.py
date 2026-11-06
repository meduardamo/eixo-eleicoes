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

origem = ler('perfil_senado_origem/origem.csv')
rs = sen('resumo').drop_duplicates('Parlamentar')
rc = ler('perfil_camara_senado/resumo.csv').drop_duplicates('Parlamentar')
s, k = rs.set_index('Parlamentar'), rc.set_index('Parlamentar')
p = origem.copy()
pega = lambda base, col: [base[col].get(n, '') for n in p.Parlamentar]
p['Senado: matérias como primeiro autor'] = pega(s, 'Matérias legislativas como primeiro autor')
p['Senado: temas da autoria principal'] = pega(s, 'Temas da autoria principal (5 maiores)')
p['Senado: relatorias'] = pega(s, 'Relatorias')
p['Senado: temas das relatorias'] = pega(s, 'Temas das relatorias (5 maiores)')
p['Senado: votações nominais abertas'] = pega(s, 'Votações nominais abertas')
p['Senado: discursos'] = pega(s, 'Discursos e apartes registrados')
p['Senado: comissões hoje (titular)'] = pega(s, 'Comissões hoje (titular)')
p['Câmara: proposições como primeiro autor'] = pega(k, 'Proposições legislativas como primeiro autor')
p['Câmara: temas da autoria principal'] = pega(k, 'Temas da autoria principal (5 maiores)')
p['Câmara: relator no último andamento'] = pega(k, 'Relator no último andamento')
p['Câmara: votações nominais em plenário'] = pega(k, 'Votações nominais em plenário')
p['Câmara: discursos'] = pega(k, 'Discursos registrados')
fim = ['Plano de governo (eleições de governador ou prefeito que venceu)', 'Instagram', 'Outras redes declaradas',
       'Ficha do TSE usada (redes e planos)', 'Fonte dos cargos', 'Observação']
p = p[[c for c in p.columns if c not in fim] + fim]
p['_o'] = p['Situação'].map(ORDEM_SIT)
p = p.sort_values(['_o', 'UF', 'Parlamentar']).drop(columns='_o')

temas = pd.concat([sen('temas').assign(Casa='Senado'), ler('perfil_camara_senado/temas.csv').assign(Casa='Câmara')])
temas = temas[['Parlamentar', 'Casa', 'Tema', 'Primeiro autor', 'Coautor', 'Relatorias', 'Votações nominais abertas']]
coleg = pd.concat([sen('colegiados'), ler('perfil_camara_senado/colegiados.csv')], ignore_index=True)
corta = lambda df, cols, n=300: df.assign(**{c: df[c].str.slice(0, n) for c in cols if c in df})
tira = lambda df, cols: df.drop(columns=[c for c in cols if c in df])

ABAS = [
    ('Senadores', p, 4),
    ('Cargos anteriores', ler('perfil_senado_origem/cargos.csv'), 1),
    ('Senado: ficha e números', rs, 1),
    ('Câmara: ficha e números', rc, 1),
    ('Temas', temas, 2),
    ('Senado: assuntos (classificação do Senado)', sen('classificacao'), 3),
    ('Colegiados', coleg, 1),
    ('Senado: autoria', corta(sen('autoria'), ['Ementa']), 2),
    ('Senado: relatorias', corta(sen('relatorias'), ['Ementa']), 2),
    ('Senado: emendas', sen('emendas'), 2),
    ('Senado: votações', corta(sen('votos'), ['Ementa']), 3),
    ('Senado: discursos', tira(sen('discursos'), ['Texto', 'Código']), 2),
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

nomes = [t for t, _, _ in ABAS]
# aba que este script não grava (planos de governo) fica onde está, depois das dele
ordem = {t: i for i, t in enumerate(nomes)}
sh.reorder_worksheets(sorted(sh.worksheets(), key=lambda w: ordem.get(w.title, 99)))
print('células:', sum(w.row_count * w.col_count for w in sh.worksheets()))
