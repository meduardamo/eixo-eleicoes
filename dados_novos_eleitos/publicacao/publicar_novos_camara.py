"""Abas de trabalho da estagiária na planilha Câmara dos Deputados | Histórico e bases: imprensa e trajetória
dos deputados novos (sem passagem pela Câmara). Uma linha por deputado para começar; ela insere linhas abaixo
para cada matéria ou cargo a mais. Não regrava aba que já existe (o trabalho dela fica), salvo com --forca."""
from ids import valor
import sys

import gspread

ID = valor('SPREADSHEET_ID_58_CAMARA_BASES')
CONGRESSO = valor('SPREADSHEET_ID_CONGRESSO_ELEITO')
MARINHO, GELO = {'red': 25/255, 'green': 45/255, 'blue': 78/255}, {'red': 244/255, 'green': 243/255, 'blue': 239/255}
BRANCO = {'red': 1, 'green': 1, 'blue': 1}
ORDEM_ORIGEM = ['Sem mandato eletivo', 'Prefeitura', 'Assembleia', 'Câmara Municipal', 'Governo estadual', 'Senado', 'Câmara dos Deputados']
TEMAS = ['Saúde', 'Educação', 'Esporte', 'Primeira infância', 'Nenhum dos quatro']
SITUACAO = ['Pré-preenchido', 'Conferido', 'A conferir']

ABAS = {
    'Novos: imprensa': {
        'origens': ['Sem mandato eletivo', 'Prefeitura'],
        'colunas': ['Deputado', 'Partido/UF', 'De onde vem', 'Tema', 'O que a matéria registra', 'Veículo', 'Data', 'Link', 'Observação'],
        'larguras': [200, 110, 160, 140, 420, 160, 100, 220, 220],
        'listas': {'Tema': TEMAS},
    },
    'Novos: trajetória': {
        'origens': ORDEM_ORIGEM,
        'colunas': ['Deputado', 'Partido/UF', 'De onde vem', 'Mandatos eletivos (TSE)', 'Cargo', 'Órgão ou entidade',
                    'Início', 'Fim', 'Fonte', 'Situação', 'Observação'],
        'larguras': [200, 110, 160, 260, 220, 220, 80, 80, 220, 130, 220],
        'listas': {'Situação': SITUACAO},
    },
}

forca = '--forca' in sys.argv
gc = gspread.service_account(filename='credentials.json')
eleitos = gc.open_by_key(CONGRESSO).worksheet('Câmara eleita 2026').get_all_records()
novos = [r for r in eleitos if r['Situação'] in ('Novo', 'Nova')]
sh = gc.open_by_key(ID)
existentes = {w.title for w in sh.worksheets()}

for titulo, cfg in ABAS.items():
    if titulo in existentes and not forca:
        print(f'{titulo}: já existe, não regravei (use --forca)')
        continue
    if titulo in existentes:
        sh.del_worksheet(sh.worksheet(titulo))
    linhas = sorted((r for r in novos if r['Tipo de origem'] in cfg['origens']),
                    key=lambda r: (ORDEM_ORIGEM.index(r['Tipo de origem']), r['UF'], r['Parlamentar']))
    cols = cfg['colunas']
    dados = [cols]
    for r in linhas:
        base = {'Deputado': r['Parlamentar'], 'Partido/UF': f"{r['Partido']}/{r['UF']}", 'De onde vem': r['De onde vem'],
                'Mandatos eletivos (TSE)': r['Mandatos anteriores (histórico TSE, desde 2004)']}
        dados.append([base.get(c, '') for c in cols])
    ws = sh.add_worksheet(titulo, rows=len(dados), cols=len(cols))
    ws.update(dados, 'A1', value_input_option='RAW')
    gid, n, m = ws.id, len(dados), len(cols)
    reqs = [
        {'repeatCell': {'range': {'sheetId': gid}, 'cell': {'userEnteredFormat': {
            'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10}, 'verticalAlignment': 'MIDDLE',
            'wrapStrategy': 'WRAP', 'backgroundColor': GELO}},
            'fields': 'userEnteredFormat(textFormat,verticalAlignment,wrapStrategy,backgroundColor)'}},
        {'repeatCell': {'range': {'sheetId': gid, 'endRowIndex': 1}, 'cell': {'userEnteredFormat': {
            'backgroundColor': MARINHO, 'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': True,
                                                       'foregroundColor': BRANCO}}},
            'fields': 'userEnteredFormat(backgroundColor,textFormat)'}},
        {'updateSheetProperties': {'properties': {'sheetId': gid, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': 1}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        {'setBasicFilter': {'filter': {'range': {'sheetId': gid, 'startRowIndex': 0, 'endRowIndex': n,
                                                 'startColumnIndex': 0, 'endColumnIndex': m}}}},
    ]
    reqs += [{'updateDimensionProperties': {'range': {'sheetId': gid, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                            'properties': {'pixelSize': w}, 'fields': 'pixelSize'}}
             for i, w in enumerate(cfg['larguras'])]
    for col, valores in cfg['listas'].items():
        i = cols.index(col)
        reqs.append({'setDataValidation': {'range': {'sheetId': gid, 'startRowIndex': 1, 'startColumnIndex': i, 'endColumnIndex': i + 1},
                                           'rule': {'condition': {'type': 'ONE_OF_LIST', 'values': [{'userEnteredValue': v} for v in valores]},
                                                    'showCustomUi': True, 'strict': True}}})
    sh.batch_update({'requests': reqs})
    print(f'{titulo}: {n - 1} deputados')
