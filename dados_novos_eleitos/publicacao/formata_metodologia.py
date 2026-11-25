"""Acabamento da aba Metodologia das planilhas da UNFPA (Bahia e Congresso), 25/11/2026, pedido dela: "melhora a ux ui da metodologia
das sheets da unfpa, so p ser mais legivel". Padrão do Leia-me do manual (Design da planilha): título no alto; seção em linha mesclada,
caixa alta, negrito azul-marinho sobre #DCE3EE; rótulo em negrito azul-marinho na coluna A e texto em tópicos "·" na B; fio #DADAD4
entre os itens; alinhamento no meio; sem grade. Só formatação: lê a aba como está (título = A vazia e B "Metodologia"; seção = B vazia).
Chamado no fim de arruma_planilha_unfpa.py (Bahia) e de produto3/planilha_congresso.py (Congresso).
Cópias idênticas: ~/Downloads/act/scripts (ACT, planilha_act_metodologia.py) e eixo-eleicoes/dados_novos_eleitos/publicacao (58ª, publicar_metodologia.py).
Se A1 tem fundo escuro (faixa do Leia-me com logo), a linha 1 e a coluna A não mudam. Com logo_de (ID da planilha do IEPS PL 7419, variável
SPREADSHEET_ID_IEPS_PL7419 do .env do eixo-eleicoes), a linha 1 vira essa faixa antes (25/11/2026, ela: "cade a logo?")."""
import gspread

def hx(h): return {'red': int(h[0:2], 16) / 255, 'green': int(h[2:4], 16) / 255, 'blue': int(h[4:6], 16) / 255}
MAR, GELO, TXT, AZUL_CL, FIO, BRANCO = hx('192D4E'), hx('F4F3EF'), hx('333333'), hx('DCE3EE'), hx('DADAD4'), hx('FFFFFF')
F = lambda **k: {'fontFamily': 'Montserrat', 'fontSize': 10, 'foregroundColor': TXT, **k}


def faixa_com_logo(sh, ws, logo_de):
    """Faixa do Leia-me do manual na linha 1: logo eixo gov branco embutido em A1 (copiado da célula A1 da aba Leia-me da planilha
    logo_de, como faz o publicar_metodologia da 58ª; sem =IMAGE, que pede "Permitir acesso") e "Metodologia" em B1, branco 26 sobre marinho."""
    g = ws.id
    sh.batch_update({'requests': [
        {'unmergeCells': {'range': {'sheetId': g, 'startRowIndex': 0, 'endRowIndex': 1}}},
        {'repeatCell': {'range': {'sheetId': g, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 2},
                        'cell': {'userEnteredFormat': {'backgroundColor': MAR, 'horizontalAlignment': 'CENTER', 'verticalAlignment': 'MIDDLE',
                                                       'textFormat': F(bold=True, fontSize=26, foregroundColor=BRANCO)}},
                        'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)'}},
        {'updateDimensionProperties': {'range': {'sheetId': g, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1}, 'properties': {'pixelSize': 96}, 'fields': 'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId': g, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1}, 'properties': {'pixelSize': 255}, 'fields': 'pixelSize'}}]})
    tmp = gspread.Spreadsheet(sh.client, {'id': logo_de}).worksheet('Leia-me').copy_to(sh.id)['sheetId']
    cel = lambda s_: {'sheetId': s_, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 1}
    sh.batch_update({'requests': [{'copyPaste': {'source': cel(tmp), 'destination': cel(g), 'pasteType': 'PASTE_VALUES'}},
                                  {'deleteSheet': {'sheetId': tmp}}]})


def aplica(sh, titulo='Metodologia', logo_de=None):
    ws = sh.worksheet(titulo); g = ws.id
    v = ws.get_all_values()
    if logo_de and v and v[0][1:2] == ['Metodologia']:
        faixa_com_logo(sh, ws, logo_de)
    n = len(v)
    # faixa do Leia-me com o logo (58ª): A1 com fundo escuro. Nesse caso a linha 1 e a largura da coluna A ficam como estão
    a1 = sh.fetch_sheet_metadata({'ranges': f"'{titulo}'!A1", 'fields': 'sheets(data(rowData(values(userEnteredFormat(backgroundColor)))))'})
    try: fundo = a1['sheets'][0]['data'][0]['rowData'][0]['values'][0]['userEnteredFormat']['backgroundColor']
    except (KeyError, IndexError): fundo = {}
    faixa = fundo.get('red', 1) < 0.3 and fundo.get('blue', 1) < 0.5
    r0 = 1 if faixa else 0
    def rng(r0, r1, c0=0, c1=2): return {'sheetId': g, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c0, 'endColumnIndex': c1}
    def fmt(r, campos, **uf): return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': uf}, 'fields': campos}}
    def altura(i, px): return {'updateDimensionProperties': {'range': {'sheetId': g, 'dimension': 'ROWS', 'startIndex': i, 'endIndex': i + 1}, 'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
    fio = {'style': 'SOLID', 'color': FIO}
    R = [{'unmergeCells': {'range': rng(r0, n)}},
         {'updateBorders': {'range': rng(r0, n), 'top': {'style': 'NONE'}, 'bottom': {'style': 'NONE'}, 'left': {'style': 'NONE'}, 'right': {'style': 'NONE'},
                            'innerHorizontal': {'style': 'NONE'}, 'innerVertical': {'style': 'NONE'}}},
         fmt(rng(r0, n), 'userEnteredFormat', backgroundColor=GELO, textFormat=F(), verticalAlignment='MIDDLE', wrapStrategy='WRAP',
             horizontalAlignment='LEFT', padding={'top': 8, 'bottom': 8, 'left': 10, 'right': 10}),
         {'autoResizeDimensions': {'dimensions': {'sheetId': g, 'dimension': 'ROWS', 'startIndex': r0, 'endIndex': n}}}]
    for i, r in enumerate(v):
        if i < r0: continue
        a, b = (r + ['', ''])[:2]
        if not a and b == 'Metodologia' and not faixa:      # título sem faixa
            R += [fmt(rng(i, i + 1), 'userEnteredFormat(backgroundColor,textFormat)', backgroundColor=BRANCO, textFormat=F(bold=True, fontSize=14, foregroundColor=MAR)),
                  altura(i, 48)]
        elif a and not b:                                   # seção
            R += [{'updateCells': {'range': rng(i, i + 1, 0, 1), 'rows': [{'values': [{'userEnteredValue': {'stringValue': a.upper()}}]}], 'fields': 'userEnteredValue'}},
                  {'mergeCells': {'range': rng(i, i + 1), 'mergeType': 'MERGE_ALL'}},
                  fmt(rng(i, i + 1), 'userEnteredFormat(backgroundColor,textFormat)', backgroundColor=AZUL_CL, textFormat=F(bold=True, foregroundColor=MAR)),
                  altura(i, 34)]
        elif a:                                             # item
            R += [fmt(rng(i, i + 1, 0, 1), 'userEnteredFormat.textFormat', textFormat=F(bold=True, foregroundColor=MAR)),
                  {'updateBorders': {'range': rng(i, i + 1), 'bottom': fio}}]
    R += [{'updateDimensionProperties': {'range': {'sheetId': g, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1}, 'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
          for i, px in enumerate([240, 780]) if not (faixa and i == 0)]
    R.append({'updateSheetProperties': {'properties': {'sheetId': g, 'gridProperties': {'hideGridlines': True, 'frozenRowCount': 1 if faixa or (v and v[0][1:2] == ['Metodologia']) else 0}},
              'fields': 'gridProperties(hideGridlines,frozenRowCount)'}})
    sh.batch_update({'requests': R})
    return n
