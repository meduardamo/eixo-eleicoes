"""Cabeçalhos das abas de base da consulta da Câmara, iguais aos do Senado, com os 513 nomes para preencher.
Não regrava aba que já tem conteúdo além do cabeçalho (o preenchimento fica), salvo com --forca."""
from ids import valor
import sys

import gspread

sys.path.insert(0, '.')
from outros.novos_eleitos.e22_perfil_temas_senado import VOTACOES_CAMARA

CAMARA = valor('SPREADSHEET_ID_58_CAMARA')
SENADO = valor('SPREADSHEET_ID_58_SENADO')
CONGRESSO = valor('SPREADSHEET_ID_CONGRESSO_ELEITO')
TEMAS = ['Saúde', 'Educação', 'Esporte', 'Primeira infância']
CADEIRA = {'Reeleito': 'Reeleito em 2026', 'Reeleita': 'Reeleita em 2026', 'Voltando': 'Volta à Câmara',
           'Novo': 'Eleito em 2026', 'Nova': 'Eleita em 2026'}
ORDEM = {'Reeleito': 0, 'Reeleita': 0, 'Voltando': 1, 'Novo': 2, 'Nova': 2}
TROCA = {'Senador': 'Deputado', 'Posição no Senado': 'Posição na Câmara', 'Frentes do tema no Senado': 'Frentes do tema na Câmara'}
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}

gc = gspread.service_account(filename='credentials.json')
sen, cam = gc.open_by_key(SENADO), gc.open_by_key(CAMARA)
eleitos = sorted(gc.open_by_key(CONGRESSO).worksheet('Câmara eleita 2026').get_all_records(),
                 key=lambda r: (ORDEM[r['Situação']], r['UF'], r['Parlamentar']))
ident = [[r['Parlamentar'], f"{r['Partido']}/{r['UF']}", CADEIRA[r['Situação']]] for r in eleitos]
troca = lambda linha: [TROCA.get(c, c) for c in linha]

rotulos = list(dict.fromkeys(rot for *_, rot in VOTACOES_CAMARA))
abas = {}
p = sen.worksheet('Parlamentares').get('A1:ZZ2')
cab = troca(p[1]); i_onde = cab.index('De onde vem')
abas['Parlamentares'] = ([p[0] + [''] * (len(cab) - len(p[0])), cab],
                         [l + [''] * (i_onde - 3) + [r['De onde vem']] + [''] * (len(cab) - i_onde - 1) for l, r in zip(ident, eleitos)])
cab = troca(sen.worksheet('Perfil por tema').row_values(1))
abas['Perfil por tema'] = ([cab], [[l[0], t, '', '', l[1], l[2]] + [''] * (len(cab) - 6) for l in ident for t in TEMAS])
abas['Votações-chave'] = ([['', '', '', 'Câmara: plenário'] + [''] * (len(rotulos) - 1), ['Deputado', 'Partido/UF', 'Cadeira'] + rotulos],
                          [l + [''] * len(rotulos) for l in ident])
abas['Clientes'] = ([troca(sen.worksheet('Clientes').row_values(1))], [])

forca = '--forca' in sys.argv
for titulo, (cabecalho, linhas) in abas.items():
    ws = cam.worksheet(titulo)
    if any(any(c.strip() for c in l) for l in ws.get_all_values()) and not forca:
        print(f'{titulo}: já tem conteúdo, não regravei (use --forca)')
        continue
    valores = cabecalho + linhas
    n, m, k = len(valores), max(len(v) for v in valores), len(cabecalho)
    ws.clear()
    ws.resize(rows=max(n, 2), cols=m)
    ws.update(values=valores, range_name='A1', value_input_option='RAW')
    fmt = lambda a, b, fundo, texto, negrito: {'repeatCell': {
        'range': {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b},
        'cell': {'userEnteredFormat': {'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': 'MIDDLE',
                 'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)'}}
    cam.batch_update({'requests': [
        fmt(0, k, '#192D4E', '#FFFFFF', True), fmt(k, max(n, 2), '#F4F3EF', '#000000', False),
        {'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': k, 'frozenColumnCount': 1}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': m},
                                       'properties': {'pixelSize': 180}, 'fields': 'pixelSize'}},
    ]})
    print(f'{titulo}: {n - k} linhas, {m} colunas')
