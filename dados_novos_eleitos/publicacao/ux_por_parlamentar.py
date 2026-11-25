"""Acabamento da aba Por parlamentar das planilhas da 58ª (Câmara e Senado) no padrão aprovado nas planilhas da UNFPA e da ACT
(25/11/2026, ela: "eu curti" e "e essas?"). Só formatação: não mexe em fórmula, valor, lista nem proteção, então pode rodar com a
revisão em andamento. publicar_por_parlamentar.py chama aplica(sh) no fim; rodar sozinho: python3 ux_por_parlamentar.py camara|senado
(faz as duas abas: Por parlamentar e Por cliente; publicar_clientes.py chama aplica_cliente(sh) no fim)
- títulos de bloco em faixa marinho e cabeçalho das tabelas em cinza claro (hierarquia: faixa > cabeçalho > rótulo > texto);
- alinhamento vertical no meio em tudo (regra dela de 20/10/2026; a versão de 25/11 com o alto foi desfeita no mesmo dia);
- cor por tema (Saúde, Educação, Esporte, Primeira infância) e o cliente na cor da área dele (aba Descrição dos clientes);
- nome em branco com fonte 14. As cores de Atuação no tema e de Classificação continuam as do publicar_por_parlamentar.
"""
import sys
import gspread

from ids import valor
IDS = {'camara': 'SPREADSHEET_ID_58_CAMARA', 'senado': 'SPREADSHEET_ID_58_SENADO'}  # nome da variável (repo público)
def hx(h): h = h.lstrip('#'); return {'red': int(h[0:2], 16) / 255, 'green': int(h[2:4], 16) / 255, 'blue': int(h[4:6], 16) / 255}
MAR, BRANCO, CINZA_CL = hx('192D4E'), hx('FFFFFF'), hx('E1E1E1')
TEMA_COR = {'Saúde': hx('F6AA90'), 'Educação': hx('BAC8DD'), 'Esporte': hx('D8BE6A'), 'Primeira infância': hx('D5B6D5')}
F = lambda **k: {'fontFamily': 'Montserrat', 'fontSize': 10, **k}


def aplica(sh):
    ws = sh.worksheet('Por parlamentar'); g = ws.id
    a = ws.col_values(1)
    L1, C1 = a.index('Atuação e posições por tema'), a.index('Classificação por cliente')
    t0, c0 = L1 + 1, C1 + 1            # cabeçalhos (base 0)
    nl, m = ws.row_count, ws.col_count
    area = {r[1]: r[0] for r in sh.worksheet('Descrição dos clientes').get_all_values()[1:] if len(r) > 1 and r[1]}

    def rng(r0, r1, c_0=0, c_1=m): return {'sheetId': g, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c_0, 'endColumnIndex': c_1}
    def fmt(r, fields, **uf): return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': uf}, 'fields': fields}}
    def regra(r, valor, cor): return {'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [r], 'booleanRule': {
        'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': valor}]}, 'format': {'backgroundColor': cor}}}}}

    # tira só as regras desta rotina (coluna A das duas tabelas), as de Atuação/Classificação (coluna B) ficam
    meta = sh.fetch_sheet_metadata({'fields': 'sheets(properties.sheetId,conditionalFormats)'})
    cfs = next(s.get('conditionalFormats', []) for s in meta['sheets'] if s['properties']['sheetId'] == g)
    minhas = [i for i, c in enumerate(cfs) if all(r.get('startColumnIndex', 0) == 0 and r.get('endColumnIndex') == 1 for r in c['ranges'])]
    R = [{'deleteConditionalFormatRule': {'sheetId': g, 'index': i}} for i in sorted(minhas, reverse=True)]

    R += [fmt(rng(0, 1, 1, 4), 'userEnteredFormat(backgroundColor,textFormat)', backgroundColor=BRANCO, textFormat=F(bold=True, fontSize=14, foregroundColor=MAR))]
    for r in (L1, C1):
        R.append(fmt(rng(r, r + 1), 'userEnteredFormat(backgroundColor,textFormat,verticalAlignment)', backgroundColor=MAR,
                     textFormat=F(bold=True, fontSize=11, foregroundColor=BRANCO), verticalAlignment='MIDDLE'))
        R.append({'updateDimensionProperties': {'range': {'sheetId': g, 'dimension': 'ROWS', 'startIndex': r, 'endIndex': r + 1}, 'properties': {'pixelSize': 32}, 'fields': 'pixelSize'}})
    for r, c1 in ((t0, m), (c0, 6)):
        R.append(fmt(rng(r, r + 1, 0, c1), 'userEnteredFormat(backgroundColor,textFormat)', backgroundColor=CINZA_CL, textFormat=F(bold=True, foregroundColor=MAR)))
    R.append(fmt(rng(t0 + 1, C1), 'userEnteredFormat.verticalAlignment', verticalAlignment='MIDDLE'))
    R.append(fmt(rng(c0 + 1, nl), 'userEnteredFormat.verticalAlignment', verticalAlignment='MIDDLE'))
    R += [regra(rng(t0 + 1, C1, 0, 1), t, c) for t, c in TEMA_COR.items()]
    R += [regra(rng(c0 + 1, nl, 0, 1), cli, TEMA_COR[ar]) for cli, ar in area.items() if ar in TEMA_COR]
    sh.batch_update({'requests': R})
    return L1 + 1, C1 + 1, len(area)


def aplica_cliente(sh):
    """Por cliente (25/11/2026, ela: "pode ser por cliente tbm"): nome em branco com fonte 14, a rótulo Área (A2) na cor da área,
    linhas de parlamentar com alinhamento no meio. Classificação, cabeçalho e coluna de apoio I:N ficam como o publicar_clientes deixa."""
    ws = sh.worksheet('Por cliente'); g = ws.id
    a = ws.col_values(1)[:12]
    cab = next(i for i, x in enumerate(a) if x in ('Senador', 'Deputado'))
    area_lin = a.index('Área')
    def rng(r0, r1, c_0, c_1): return {'sheetId': g, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c_0, 'endColumnIndex': c_1}
    meta = sh.fetch_sheet_metadata({'fields': 'sheets(properties.sheetId,conditionalFormats)'})
    cfs = next(s.get('conditionalFormats', []) for s in meta['sheets'] if s['properties']['sheetId'] == g)
    minhas = [i for i, c in enumerate(cfs) if any(r.get('startRowIndex') == area_lin and r.get('endRowIndex') == area_lin + 1 for r in c['ranges'])]
    R = [{'deleteConditionalFormatRule': {'sheetId': g, 'index': i}} for i in sorted(minhas, reverse=True)]
    R.append({'repeatCell': {'range': rng(0, 1, 1, 3), 'cell': {'userEnteredFormat': {'backgroundColor': BRANCO, 'textFormat': F(bold=True, fontSize=14, foregroundColor=MAR)}},
                             'fields': 'userEnteredFormat(backgroundColor,textFormat)'}})
    R.append({'repeatCell': {'range': rng(area_lin, area_lin + 1, 1, 2), 'cell': {'userEnteredFormat': {'textFormat': F(bold=True, foregroundColor=hx('333333'))}},
                             'fields': 'userEnteredFormat.textFormat'}})
    a2 = rng(area_lin, area_lin + 1, 0, 1)  # só o rótulo Área ganha a cor (a linha inteira pintada disputava com o cabeçalho)
    R += [{'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [a2], 'booleanRule': {
        'condition': {'type': 'CUSTOM_FORMULA', 'values': [{'userEnteredValue': f'=$B${area_lin + 1}="{ar}"'}]}, 'format': {'backgroundColor': TEMA_COR[ar]}}}}} for ar in ('Saúde', 'Educação', 'Esporte')]
    R.append({'repeatCell': {'range': rng(cab + 1, ws.row_count, 0, 7), 'cell': {'userEnteredFormat': {'verticalAlignment': 'MIDDLE'}},
                             'fields': 'userEnteredFormat.verticalAlignment'}})
    sh.batch_update({'requests': R})
    return cab + 1


if __name__ == '__main__':
    casa = sys.argv[1] if sys.argv[1:] else 'senado'
    gc = gspread.service_account(filename='credentials.json')  # rodar da raiz do repositório, como os publicar_*
    sh = gc.open_by_key(valor(IDS[casa]))
    print(casa, aplica(sh), aplica_cliente(sh))
