"""Publica a classificação dos senadores por cliente (etapa 24) na planilha da 58ª Legislatura, em duas abas:
- Por cliente: lista suspensa no alto; escolhido o cliente, aparecem as pautas, a contagem por classificação e os
  senadores com registro (fórmula FILTER sobre a aba Clientes), com a classificação colorida;
- Clientes: uma linha por cliente e senador, sem célula mesclada, com filtro em todas as colunas.
Substitui as abas antigas Clientes Educação, Clientes Saúde e Clientes Esporte."""
from ids import valor
import gspread
import pandas as pd

D = 'dados_novos_eleitos/temas_clientes/'
ID = valor('SPREADSHEET_ID_58_SENADO')
ANTIGAS = ['Clientes Educação', 'Clientes Saúde', 'Clientes Esporte']
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, BORDA, CINZA = '#192D4E', '#F4F3EF', '#DADAD4', '#8A8A84'
CLASSE = {'Promotor': '#B7E1CD', 'Neutro': '#FCE8B2', 'Detrator': '#F4C7C3', 'Um sinal a favor': '#E6F4EC', 'Só coautorias a favor': '#E6F4EC',
          'Um sinal contra': '#FBEAE8', 'Só coautorias contra': '#FBEAE8', 'Sinais opostos': '#FDF6E3', 'Registro sem sinal': '#F4F3EF'}
FORTES = ('Promotor', 'Neutro', 'Detrator')
ORDEM = {k: i for i, k in enumerate(CLASSE)}
COLS = ['Cliente', 'Área', 'Senador', 'Partido/UF', 'Cadeira', 'Classificação', 'Sinais',
        'Atuação nos subtemas do cliente', 'Evidências', 'Subtemas do cliente']
LARG = {'Cliente': 140, 'Área': 80, 'Senador': 165, 'Partido/UF': 125, 'Cadeira': 112, 'Classificação': 150,
        'Sinais': 190, 'Atuação nos subtemas do cliente': 300, 'Evidências': 600, 'Subtemas do cliente': 300}

cla = pd.read_csv(D + 'clientes_classificacao.csv', dtype=str).fillna('')
N_SEN = len(pd.read_csv(D + 'parlamentares.csv'))  # os 81 da 58ª no Senado; quem não aparece na Clientes não tem registro
res = pd.read_csv(D + 'clientes_resumo.csv', dtype=str).fillna('')
# subtemas do cliente e quanto o senador atua neles (etapa 25), antes de trocar o nome do Coletivo
from subtemas_texto import por_cliente, lista_cliente
cla['Partido/UF'] = cla.Partido + '/' + cla.UF
cla['Atuação nos subtemas do cliente'] = [por_cliente(s, c) for s, c in zip(cla.Senador, cla.Cliente)]
cla['Subtemas do cliente'] = cla.Cliente.map(lista_cliente)
res['Subtemas'] = res.Cliente.map(lista_cliente)
# o cliente "Coletivo" são três organizações (lista da coordenação de saúde, 15/10/2026)
NOME = {'Coletivo Feminista': 'Coletivo (Nem Presa Nem Morta, Católicas pelo Direito de Decidir, Coletivo Feminista Sexualidade e Saúde)'}
cla['Cliente'] = cla.Cliente.replace(NOME)
res['Cliente'] = res.Cliente.replace(NOME)
ordem_cli = list(res.sort_values(['Área', 'Cliente'], key=lambda s: s.str.normalize('NFKD')).Cliente)
cla['o'] = cla['Classificação'].map(ORDEM)
cla['c'] = cla.Cliente.map(ordem_cli.index)
cla = cla.sort_values(['c', 'o', 'Senador'])[COLS]

sh = gspread.service_account(filename='credentials.json').open_by_key(ID)


def aba(titulo, linhas, colunas):
    try:
        ws = sh.worksheet(titulo)
        sh.batch_update({'requests': [{'clearBasicFilter': {'sheetId': ws.id}}, {'unmergeCells': {'range': {'sheetId': ws.id}}}]})
        meta = sh.fetch_sheet_metadata({'includeGridData': False})
        regras = next(s for s in meta['sheets'] if s['properties']['sheetId'] == ws.id).get('conditionalFormats', [])
        if regras:
            sh.batch_update({'requests': [{'deleteConditionalFormatRule': {'sheetId': ws.id, 'index': 0}} for _ in regras]})
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=titulo, rows=linhas, cols=colunas)
    ws.resize(rows=linhas, cols=colunas)
    return ws


def fmt(R, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE', tam=10):
    return {'repeatCell': {'range': R, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': tam, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


def cores_classe(ws, R):
    """Classificação colorida por formatação condicional (vale para o resultado da fórmula também)."""
    return [{'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [R], 'booleanRule': {
        'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': k}]},
        'format': {'backgroundColor': cor(c), 'textFormat': {'bold': k in FORTES,
                                                              'foregroundColor': cor(CINZA if k == 'Registro sem sinal' else '#000000')}}}}}}
        for k, c in CLASSE.items()]


def larguras(ws, cols):
    return [{'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                           'properties': {'pixelSize': LARG.get(c, 110)}, 'fields': 'pixelSize'}} for i, c in enumerate(cols)]


# ------------------------------------------------------------------ Clientes (base plana)
n = len(cla) + 1
ws = aba('Clientes', n, len(COLS))
ws.update(values=[COLS] + cla.values.tolist(), range_name='A1', value_input_option='RAW')
R = lambda a, b, c0=0, c1=len(COLS): {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}
fina = {'style': 'SOLID', 'color': cor(BORDA)}
j = COLS.index('Classificação')
reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': 3}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(R(0, 1), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'), fmt(R(1, n), '#FFFFFF'),
        fmt(R(1, n, 0, 1), '#FFFFFF', MARINHO, True), fmt(R(1, n, 2, 3), '#FFFFFF', MARINHO, True),
        fmt(R(1, n, j, j + 1), '#FFFFFF', h='CENTER'),
        {'updateBorders': {'range': R(0, n), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'setBasicFilter': {'filter': {'range': R(0, n)}}}]
# linha mais grossa entre clientes
ini = 1
for cl, k in cla.groupby('Cliente', sort=False).size().items():
    reqs.append({'updateBorders': {'range': R(ini, ini + k), 'bottom': {'style': 'SOLID_MEDIUM', 'color': cor(MARINHO)}}})
    ini += k
reqs += cores_classe(ws, R(1, n, j, j + 1)) + larguras(ws, COLS)
reqs.append({'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                           'properties': {'pixelSize': 36}, 'fields': 'pixelSize'}})
reqs.append({'autoResizeDimensions': {'dimensions': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 1, 'endIndex': n}}})
for lote in range(0, len(reqs), 300):
    sh.batch_update({'requests': reqs[lote:lote + 300]})
print('Clientes', n)

# ------------------------------------------------------------------ Por cliente (seletor)
# Fórmulas com ponto e vírgula: a planilha está em pt-BR.
# Linhas: 1 seletor | 2 área | 3 subtemas | 4 pautas | 5 projetos marcados | 6 contagem | 7 em branco | 8 cabeçalho | 9+ senadores
VIS = ['Senador', 'Partido/UF', 'Cadeira', 'Classificação', 'Sinais', 'Atuação nos subtemas do cliente', 'Evidências']
m = len(VIS)
NL = 8 + max(cla.Cliente.value_counts()) + 5
wp = aba('Por cliente', NL, m)
ref = lambda r: res.set_index('Cliente').loc[:, r]
# tabela de apoio com o resumo, à direita e oculta
apoio = [['Cliente', 'Área', 'Pautas', 'Projetos', 'Posição fora', 'Subtemas']]
for r in res.itertuples():
    pl = lambda q, um, varios: f"{q} {um if q == '1' else varios}"
    proj = (f"Projetos na base de pautas: {pl(r.Promotores, 'promotor', 'promotores')} e {pl(r.Detratores, 'detrator', 'detratores')}"
            + (f"; {pl(r.Aguardando, 'precisa', 'precisam')} de checagem dos coordenadores" if r.Aguardando != '0' else ''))
    apoio.append([r.Cliente, r.Área, r.Pautas, proj, r._8, r.Subtemas])
wp.resize(rows=max(NL, len(apoio)), cols=m + 7)
L = "'Clientes'"
F = lambda col: f"{L}!${chr(65 + COLS.index(col))}$2:${chr(65 + COLS.index(col))}"
valores = [
    ['Cliente', ordem_cli[0]] + [''] * (m - 2),
    ['Área', f'=IFERROR(VLOOKUP($B$1;$I:$N;2;0);"")'] + [''] * (m - 2),
    ['Subtemas', f'=IFERROR(VLOOKUP($B$1;$I:$N;6;0);"")'] + [''] * (m - 2),
    ['Pautas', f'=IFERROR(VLOOKUP($B$1;$I:$N;3;0);"")'] + [''] * (m - 2),
    ['Projetos', f'=IFERROR(VLOOKUP($B$1;$I:$N;4;0);"")'] + [''] * (m - 2),
    ['Senadores', f'="Promotores: "&COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Promotor")&"   ·   Neutros: "&COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Neutro")&"   ·   Detratores: "&COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Detrator")&"   ·   Um sinal a favor: "&(COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Um sinal a favor")+COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Só coautorias a favor"))&"   ·   Um sinal contra: "&(COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Um sinal contra")+COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Só coautorias contra"))&IF(COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Sinais opostos")>0;"   ·   Sinais opostos: "&COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Sinais opostos");"")&"   ·   Registro sem sinal: "&COUNTIFS({F("Cliente")};$B$1;{F("Classificação")};"Registro sem sinal")&"   ·   Sem registro: "&({N_SEN}-COUNTIF({F("Cliente")};$B$1))'] + [''] * (m - 2),
    [''] * m,
    VIS,
    [f'=IFERROR(CHOOSECOLS(FILTER({L}!$A$2:${chr(64 + len(COLS))};{F("Cliente")}=$B$1);{";".join(str(COLS.index(c) + 1) for c in VIS)});"Nenhum senador com registro para este cliente.")'] + [''] * (m - 1),
]
wp.update(values=valores, range_name='A1', value_input_option='USER_ENTERED')
wp.update(values=apoio, range_name='I1', value_input_option='RAW')
P = lambda a, b, c0=0, c1=m: {'sheetId': wp.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}
reqs = [{'updateSheetProperties': {'properties': {'sheetId': wp.id, 'gridProperties': {'frozenRowCount': 8, 'frozenColumnCount': 1}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(P(0, NL), '#FFFFFF'),
        # seletor
        fmt(P(0, 1, 0, 1), MARINHO, '#FFFFFF', True, 'LEFT', 'MIDDLE', 10),
        fmt(P(0, 1, 1, 2), '#D9DEE7', MARINHO, True, 'LEFT', 'MIDDLE', 12),
        {'setDataValidation': {'range': P(0, 1, 1, 2), 'rule': {'condition': {'type': 'ONE_OF_LIST', 'values': [{'userEnteredValue': c} for c in ordem_cli]},
                                                                  'strict': True, 'showCustomUi': True}}},
        # rótulos do resumo
        fmt(P(1, 6, 0, 1), GELO, MARINHO, True, 'LEFT', 'MIDDLE'), fmt(P(1, 6, 1, m), GELO),
        fmt(P(7, 8), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'),
        fmt(P(8, NL, 0, 1), '#FFFFFF', MARINHO, True), fmt(P(8, NL, 3, 4), '#FFFFFF', h='CENTER'),
        {'updateBorders': {'range': P(7, NL), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'updateDimensionProperties': {'range': {'sheetId': wp.id, 'dimension': 'COLUMNS', 'startIndex': m, 'endIndex': m + 7},
                                       'properties': {'hiddenByUser': True}, 'fields': 'hiddenByUser'}},
        {'updateDimensionProperties': {'range': {'sheetId': wp.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                       'properties': {'pixelSize': 40}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': wp.id, 'dimension': 'ROWS', 'startIndex': 7, 'endIndex': 8},
                                       'properties': {'pixelSize': 32}, 'fields': 'pixelSize'}}]
for a in range(1, 6):
    reqs.append({'mergeCells': {'range': P(a, a + 1, 1, m), 'mergeType': 'MERGE_ALL'}})
reqs.append({'mergeCells': {'range': P(0, 1, 1, 3), 'mergeType': 'MERGE_ALL'}})
# a lista vem de fórmula: qualquer valor digitado ou colado nela trava o resultado (#REF!); a proteção só avisa
meta = sh.fetch_sheet_metadata({'includeGridData': False})
for pr in next(x for x in meta['sheets'] if x['properties']['sheetId'] == wp.id).get('protectedRanges', []):
    reqs.append({'deleteProtectedRange': {'protectedRangeId': pr['protectedRangeId']}})
reqs.append({'addProtectedRange': {'protectedRange': {'range': P(8, NL), 'warningOnly': True,
                                                      'description': 'Lista gerada pela fórmula da célula A9'}}})
reqs += cores_classe(wp, P(8, NL, 3, 4)) + larguras(wp, VIS)
reqs.append({'updateDimensionProperties': {'range': {'sheetId': wp.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1},
                                           'properties': {'pixelSize': 170}, 'fields': 'pixelSize'}})
for lote in range(0, len(reqs), 300):
    sh.batch_update({'requests': reqs[lote:lote + 300]})
print('Por cliente', NL)

# ------------------------------------------------------------------ abas antigas e ordem
for t in ANTIGAS:
    try:
        sh.del_worksheet(sh.worksheet(t))
    except gspread.WorksheetNotFound:
        pass
from ordem_abas import ordenar
print(ordenar(sh))
