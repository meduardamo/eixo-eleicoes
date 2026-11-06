"""Publica a aba Subtemas por cliente na planilha da 58ª Legislatura, para revisão dos coordenadores: subtemas nas
linhas, clientes nas colunas (agrupados por área) e caixa de seleção marcada onde o subtema vale para o cliente
(temas_clientes/subtemas_clientes.csv). Embaixo, uma linha para assunto do cliente que ficou sem subtema e uma para
comentário. A aba recebe edição dos coordenadores: o script não regrava se ela já existir (use --forca)."""
from ids import valor
import sys

import gspread
import pandas as pd

sys.path.insert(0, 'dados_novos_eleitos/publicacao')
from subtemas_texto import SUB, TEMAS, SUB_CLIENTE

ID = valor('SPREADSHEET_ID_58_SENADO')
TITULO = 'Subtemas por cliente'
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, BORDA = '#192D4E', '#F4F3EF', '#DADAD4'
NOME = {'Coletivo Feminista': 'Coletivo'}
NOTA_CLI = {'Coletivo': 'Nem Presa Nem Morta, Católicas pelo Direito de Decidir e Coletivo Feminista Sexualidade e Saúde'}
SEM_SUBTEMA = {'Vital Strategies': 'Trânsito (formação de condutores)',
               'IDEC': 'Consumidor, crédito, dados pessoais, telecomunicações e energia',
               'Infinis': 'Assistência social',
               'UNFPA': 'Maioridade penal e sistema socioeducativo'}

sh = gspread.service_account(filename='credentials.json').open_by_key(ID)
if TITULO in [w.title for w in sh.worksheets()] and '--forca' not in sys.argv:
    sys.exit(f'A aba {TITULO} já existe e pode ter revisão dos coordenadores; rode com --forca para regravar.')

res = pd.read_csv('dados_novos_eleitos/temas_clientes/clientes_resumo.csv', dtype=str)
AREAS = ['Educação', 'Saúde', 'Esporte']
clientes = [(a, c) for a in AREAS for c in sorted(res[res.Área == a].Cliente, key=lambda s: s.lower())]
nomes = [NOME.get(c, c) for _, c in clientes]
linhas_sub = [(t, s) for t in TEMAS for s in SUB[t]]

valores = [['', ''] + [a if i == 0 or clientes[i - 1][0] != a else '' for i, (a, _) in enumerate(clientes)],
           ['Tema', 'Subtema'] + nomes]
for t, s in linhas_sub:
    valores.append([t, s] + [(t, s) in SUB_CLIENTE.get(c, []) for _, c in clientes])
valores.append(['', 'Assunto do cliente sem subtema'] + [SEM_SUBTEMA.get(c, '') for _, c in clientes])
valores.append(['', 'Comentário'] + [''] * len(clientes))
n, m = len(valores), len(valores[0])

try:
    ws = sh.worksheet(TITULO)
    sh.batch_update({'requests': [{'unmergeCells': {'range': {'sheetId': ws.id}}}]})
    ws.clear()
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TITULO, rows=n, cols=m)
ws.resize(rows=n, cols=m)
ws.update(values=valores, range_name='A1', value_input_option='RAW')

R = lambda a, b, c0=0, c1=m: {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}


def fmt(r, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE', tam=10):
    return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': tam, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


fina = {'style': 'SOLID', 'color': cor(BORDA)}
grossa = {'style': 'SOLID_MEDIUM', 'color': cor(MARINHO)}
k = len(linhas_sub)
reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 2, 'frozenColumnCount': 2}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(R(0, 1), '#0F1E36', '#FFFFFF', True, 'CENTER', 'MIDDLE', 10),
        fmt(R(1, 2), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'),
        fmt(R(2, n), GELO), fmt(R(2, n, 0, 1), GELO, MARINHO, True), fmt(R(2, n, 1, 2), GELO, '#000000', True),
        fmt(R(2 + k, n, 2, m), '#FFFFFF', '#000000', False, 'LEFT', 'MIDDLE'),
        {'setDataValidation': {'range': R(2, 2 + k, 2, m), 'rule': {'condition': {'type': 'BOOLEAN'}}}},
        {'repeatCell': {'range': R(2, 2 + k, 2, m), 'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER'}},
                        'fields': 'userEnteredFormat.horizontalAlignment'}},
        {'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [R(2, 2 + k, 2, m)], 'booleanRule': {
            'condition': {'type': 'CUSTOM_FORMULA', 'values': [{'userEnteredValue': '=C3=TRUE'}]},
            'format': {'backgroundColor': cor('#9BD0B7')}}}}},
        {'updateBorders': {'range': R(1, n), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1},
                                       'properties': {'pixelSize': 115}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 1, 'endIndex': 2},
                                       'properties': {'pixelSize': 330}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 2, 'endIndex': m},
                                       'properties': {'pixelSize': 92}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                       'properties': {'pixelSize': 28}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 1, 'endIndex': 2},
                                       'properties': {'pixelSize': 44}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 2, 'endIndex': 2 + k},
                                       'properties': {'pixelSize': 34}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 2 + k, 'endIndex': n},
                                       'properties': {'pixelSize': 90}, 'fields': 'pixelSize'}}]
# faixa de área sobre os clientes e linha grossa entre áreas
ini = 2
for a in AREAS:
    q = sum(1 for x, _ in clientes if x == a)
    if q > 1:
        reqs.append({'mergeCells': {'range': R(0, 1, ini, ini + q), 'mergeType': 'MERGE_ALL'}})
    reqs.append({'updateBorders': {'range': R(0, n, ini, ini + q), 'left': grossa}})
    ini += q
# tema: célula mesclada na coluna A e linha grossa entre temas
ini = 2
for t in TEMAS:
    q = len(SUB[t])
    reqs.append({'mergeCells': {'range': R(ini, ini + q, 0, 1), 'mergeType': 'MERGE_ALL'}})
    reqs.append({'updateBorders': {'range': R(ini, ini + q), 'bottom': grossa}})
    ini += q
for j, nm in enumerate(nomes, start=2):
    if nm in NOTA_CLI:
        reqs.append({'updateCells': {'range': R(1, 2, j, j + 1), 'rows': [{'values': [{'note': NOTA_CLI[nm]}]}], 'fields': 'note'}})
sh.batch_update({'requests': reqs})

from ordem_abas import ordenar
print(TITULO, n, 'x', m, ordenar(sh))
