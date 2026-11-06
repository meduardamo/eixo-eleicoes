"""Publica a aba Por parlamentar na planilha da 58ª Legislatura: lista suspensa com o nome no alto e, abaixo, a ficha
do senador montada por fórmula a partir das abas Parlamentares, Perfil por tema e Clientes (nada é copiado; muda a
base, muda a ficha). As fórmulas procuram a coluna pelo nome do cabeçalho, então mudar a ordem das colunas nas
bases não quebra a ficha. Rodar depois de publicar_parlamentares, publicar_temas_clientes e publicar_clientes."""
from ids import valor
import gspread
import pandas as pd

D = 'dados_novos_eleitos/temas_clientes/'
ID = valor('SPREADSHEET_ID_58_SENADO')
TITULO = 'Por parlamentar'
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, BORDA, CINZA = '#192D4E', '#F4F3EF', '#DADAD4', '#8A8A84'
ATUACAO = {'Não atua': '#F4F3EF', 'Pouco': '#DCEFE4', 'Atua': '#9BD0B7', 'Referência': '#2A412D'}
CLASSE = {'Promotor': '#B7E1CD', 'Neutro': '#FCE8B2', 'Detrator': '#F4C7C3', 'Um sinal a favor': '#E6F4EC', 'Só coautorias a favor': '#E6F4EC',
          'Um sinal contra': '#FBEAE8', 'Só coautorias contra': '#FBEAE8', 'Sinais opostos': '#FDF6E3', 'Registro sem sinal': '#F4F3EF'}
FORTES = ('Promotor', 'Neutro', 'Detrator')

# ficha: (rótulo, colunas da aba Parlamentares juntas na mesma célula)
FICHA = [('Partido/UF', ['Partido/UF']), ('Cadeira', ['Cadeira']),
         ('Posição no Senado', ['Posição no Senado', 'Governismo']),
         ('Relevância política (1 a 4)', ['Relevância política (1 a 4)', 'O que define a relevância']),
         ('De onde vem', ['De onde vem']), ('Profissão declarada (TSE)', ['Profissão declarada (TSE)']),
         ('Comissões permanentes (titular)', ['Comissões permanentes (titular)']), ('Preside hoje', ['Preside hoje']),
         ('Destaques nos subtemas', ['Destaques nos subtemas'])]
TEMA = ['Tema', 'Atuação no tema', 'Subtemas', 'O que fez', 'O que defende (desde 2023)', 'Como votou (votações-chave)',
        'Comissão do tema hoje']
CLI = ['Cliente', 'Classificação', 'Sinais', 'Atuação nos subtemas do cliente', 'Evidências', 'Subtemas do cliente']
LARG = [170, 125, 300, 420, 420, 380, 300]
m = len(LARG)

sh = gspread.service_account(filename='credentials.json').open_by_key(ID)
senadores = [r[0] for r in sh.worksheet('Parlamentares').get_values('A3:A') if r and r[0]]
max_cli = int(pd.read_csv(D + 'clientes_classificacao.csv', dtype=str).Senador.value_counts().max())

L1 = 2 + len(FICHA)          # linha (base 1) do título do bloco de temas
T0 = L1 + 1                  # cabeçalho da tabela de temas
C1 = T0 + 1 + 4 + 1          # título do bloco de clientes (4 temas + linha em branco)
C0 = C1 + 1
NL = C0 + max_cli + 3

try:
    ws = sh.worksheet(TITULO)
    meta = sh.fetch_sheet_metadata({'includeGridData': False})
    folha = next(s for s in meta['sheets'] if s['properties']['sheetId'] == ws.id)
    limpa = [{'unmergeCells': {'range': {'sheetId': ws.id}}}]
    limpa += [{'deleteConditionalFormatRule': {'sheetId': ws.id, 'index': 0}} for _ in folha.get('conditionalFormats', [])]
    limpa += [{'deleteProtectedRange': {'protectedRangeId': p['protectedRangeId']}} for p in folha.get('protectedRanges', [])]
    sh.batch_update({'requests': limpa})
    ws.clear()
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TITULO, rows=NL, cols=m)
ws.resize(rows=NL, cols=m)

PA = "'Parlamentares'"
valor = lambda col: (f'IFERROR(INDEX({PA}!$A$3:$CZ;MATCH($B$1;{PA}!$A$3:$A;0);'
                     f'MATCH("{col}";{PA}!$A$2:$CZ$2;0));"")')
cols_de = lambda aba, nomes: ';'.join(f'MATCH("{c}";\'{aba}\'!$1:$1;0)' for c in nomes)

linhas = [['Senador', senadores[0]] + [''] * (m - 2)]
for rot, cols in FICHA:
    if len(cols) == 1:
        f = '=' + valor(cols[0])
    else:
        f = f'={valor(cols[0])}&IF({valor(cols[1])}="";"";"   ·   "&{valor(cols[1])})'
    linhas.append([rot, f] + [''] * (m - 2))
linhas.append(['Atuação e posições por tema'] + [''] * (m - 1))
linhas.append(TEMA)
linhas.append([f"=IFERROR(CHOOSECOLS(FILTER('Perfil por tema'!$A$2:$Z;'Perfil por tema'!$A$2:$A=$B$1);"
               f"{cols_de('Perfil por tema', TEMA)});\"\")"] + [''] * (m - 1))
linhas += [[''] * m] * 4
linhas.append(['Classificação por cliente'] + [''] * (m - 1))
linhas.append(['Cliente', 'Classificação', 'Sinais', 'Atuação nos subtemas do cliente', 'Evidências', 'Subtemas do cliente', ''])
linhas.append([f"=IFERROR(CHOOSECOLS(FILTER('Clientes'!$A$2:$Z;'Clientes'!$C$2:$C=$B$1);"
               f"{cols_de('Clientes', CLI)});\"Nenhum cliente com registro para este senador.\")"] + [''] * (m - 1))
assert len(linhas) == C0 + 1
ws.update(values=linhas, range_name='A1', value_input_option='USER_ENTERED')

P = lambda a, b, c0=0, c1=m: {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}


def fmt(r, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE', tam=10):
    return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': tam, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


def cores(r, mapa):
    return [{'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [r], 'booleanRule': {
        'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': k}]},
        'format': {'backgroundColor': cor(c), 'textFormat': {'bold': True, 'foregroundColor': cor('#FFFFFF' if k == 'Referência' else '#000000')}}}}}}
        for k, c in mapa.items()]


fina = {'style': 'SOLID', 'color': cor(BORDA)}
t0, c0 = T0 - 1, C0 - 1  # base 0
reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': 0}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(P(0, NL), '#FFFFFF'),
        fmt(P(0, 1, 0, 1), MARINHO, '#FFFFFF', True, 'LEFT', 'MIDDLE', 10),
        fmt(P(0, 1, 1, 4), '#D9DEE7', MARINHO, True, 'LEFT', 'MIDDLE', 12),
        {'mergeCells': {'range': P(0, 1, 1, 4), 'mergeType': 'MERGE_ALL'}},
        {'setDataValidation': {'range': P(0, 1, 1, 2), 'rule': {'condition': {'type': 'ONE_OF_LIST', 'values': [{'userEnteredValue': s} for s in senadores]},
                                                                 'strict': True, 'showCustomUi': True}}},
        # ficha
        fmt(P(1, 1 + len(FICHA), 0, 1), GELO, MARINHO, True), fmt(P(1, 1 + len(FICHA), 1, m), GELO),
        {'updateBorders': {'range': P(1, 1 + len(FICHA)), 'innerHorizontal': fina, 'bottom': fina}},
        # títulos de bloco e cabeçalhos
        fmt(P(L1 - 1, L1), '#FFFFFF', MARINHO, True, 'LEFT', 'MIDDLE', 11),
        fmt(P(C1 - 1, C1), '#FFFFFF', MARINHO, True, 'LEFT', 'MIDDLE', 11),
        fmt(P(t0, t0 + 1), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'),
        fmt(P(c0, c0 + 1, 0, 6), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'),
        fmt(P(t0 + 1, t0 + 5, 0, 1), '#FFFFFF', MARINHO, True), fmt(P(c0 + 1, NL, 0, 1), '#FFFFFF', MARINHO, True),
        fmt(P(t0 + 1, t0 + 5, 1, 2), '#FFFFFF', h='CENTER'), fmt(P(c0 + 1, NL, 1, 2), '#FFFFFF', h='CENTER'),
        {'updateBorders': {'range': P(t0, t0 + 5), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'updateBorders': {'range': P(c0, NL, 0, 6), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                       'properties': {'pixelSize': 40}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': L1 - 1, 'endIndex': L1},
                                       'properties': {'pixelSize': 40}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': C1 - 1, 'endIndex': C1},
                                       'properties': {'pixelSize': 40}, 'fields': 'pixelSize'}}]
for a in range(1, 1 + len(FICHA)):
    reqs.append({'mergeCells': {'range': P(a, a + 1, 1, m), 'mergeType': 'MERGE_ALL'}})
for a in (L1 - 1, C1 - 1):
    reqs.append({'mergeCells': {'range': P(a, a + 1, 0, m), 'mergeType': 'MERGE_ALL'}})
reqs += cores(P(t0 + 1, t0 + 5, 1, 2), ATUACAO) + cores(P(c0 + 1, NL, 1, 2), CLASSE)
# as tabelas vêm de fórmula: valor colado nelas trava o resultado; a proteção só avisa
reqs += [{'addProtectedRange': {'protectedRange': {'range': P(1, NL), 'warningOnly': True,
                                                   'description': 'Ficha gerada por fórmula; escolha o senador na célula B1'}}}]
reqs += [{'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                        'properties': {'pixelSize': w}, 'fields': 'pixelSize'}} for i, w in enumerate(LARG)]
sh.batch_update({'requests': reqs})

from ordem_abas import ordenar
print(TITULO, NL, ordenar(sh))
