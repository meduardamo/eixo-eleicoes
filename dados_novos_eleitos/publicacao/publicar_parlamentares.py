"""Publica as abas Parlamentares e Votações-chave (etapa 23) na planilha da 58ª Legislatura, no padrão Eixo.
Cabeçalho em duas linhas: faixa de grupo em cima, nome da coluna embaixo. Notas de 1 a 4 em escala de marinho,
posição no Senado com cor de fundo, voto Sim e Não com cor e a orientação do partido como nota da célula."""
from ids import valor
import json
import re

import gspread
import pandas as pd

D = 'dados_novos_eleitos/temas_clientes/'
ID = valor('SPREADSHEET_ID_58_SENADO')
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, BORDA = '#192D4E', '#F4F3EF', '#DADAD4'
# relevância em marinho e atuação em verde (paleta de gráficos da Eixo), para não parecerem a mesma medida
ESCALA = {'1': '#F4F3EF', '2': '#D9DEE7', '3': '#9AA8C0', '4': '#192D4E',
          'Não atua': '#F4F3EF', 'Pouco': '#DCEFE4', 'Atua': '#9BD0B7', 'Referência': '#2A412D'}
TEXTO_BRANCO = ('3', '4', 'Referência')
POSICAO = {'Base': '#D6E4F0', 'Oposição': '#F6D5C8', 'Independente': '#EDE6C8'}
VOTO = {'Sim': '#D6E4F0', 'Não': '#F6D5C8'}

gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key(ID)


def aba(titulo, linhas, colunas):
    try:
        ws = sh.worksheet(titulo)
        meta = next(x for x in sh.fetch_sheet_metadata()['sheets'] if x['properties']['sheetId'] == ws.id)
        # grupos de colunas sobrevivem ao clear; apaga do mais fundo para o mais raso
        grupos_velhos = sorted(meta.get('columnGroups', []), key=lambda g: -g['depth'])
        sh.batch_update({'requests': [{'clearBasicFilter': {'sheetId': ws.id}},
                                      {'unmergeCells': {'range': {'sheetId': ws.id}}}]
                         + [{'deleteDimensionGroup': {'range': {**g['range'], 'sheetId': ws.id}}} for g in grupos_velhos]})
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=titulo, rows=linhas, cols=colunas)
    ws.resize(rows=linhas, cols=colunas)
    # coluna escondida por grupo recolhido antigo continua escondida depois de apagar o grupo
    sh.batch_update({'requests': [{'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': colunas},
        'properties': {'hiddenByUser': False}, 'fields': 'hiddenByUser'}}]})
    return ws


def publicar(titulo, df, grupos, largura, congela_col, pinta, notas=None, recolher=(), altura=None, alt_cab=None, corta=()):
    """grupos: lista de (rótulo da faixa, número de colunas). recolher: (coluna inicial, final) de grupos de
    colunas que abrem e fecham pelo botão de - no alto, começando abertos. altura: linhas de altura fixa, para a
    tabela ler de cima a baixo sem degrau; as colunas em corta têm o texto longo cortado na célula (inteiro ao clicar)."""
    n, m = len(df) + 2, len(df.columns)
    ws = aba(titulo, n, m)
    faixa = []
    for rot, k in grupos:
        faixa += [rot] + [''] * (k - 1)
    ws.update(values=[faixa, list(df.columns)] + df.values.tolist(), range_name='A1', value_input_option='RAW')
    R = lambda a, b, c0=0, c1=m: {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}

    def fmt(r, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE', tam=10):
        return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
            'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
            'textFormat': {'fontFamily': 'Montserrat', 'fontSize': tam, 'bold': negrito, 'foregroundColor': cor(texto)}}},
            'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}
    fina = {'style': 'SOLID', 'color': cor(BORDA)}
    reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 2, 'frozenColumnCount': congela_col}},
                                       'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
            fmt(R(0, 1), '#0F1E36', '#FFFFFF', True, 'CENTER', 'MIDDLE', 10),
            fmt(R(1, 2), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'),
            fmt(R(2, n), GELO), fmt(R(2, n, 0, 1), GELO, MARINHO, True),
            {'updateBorders': {'range': R(1, n), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
            {'setBasicFilter': {'filter': {'range': R(1, n)}}}]
    ini = 0
    for rot, k in grupos:
        if k > 1:
            reqs.append({'mergeCells': {'range': R(0, 1, ini, ini + k), 'mergeType': 'MERGE_ALL'}})
        reqs.append({'updateBorders': {'range': R(0, n, ini, ini + k), 'left': {'style': 'SOLID_MEDIUM', 'color': cor(MARINHO)}}})
        ini += k
    for i, c in enumerate(df.columns):
        reqs.append({'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                                   'properties': {'pixelSize': largura.get(c, 110)}, 'fields': 'pixelSize'}})
    reqs.append({'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                               'properties': {'pixelSize': 28}, 'fields': 'pixelSize'}})
    reqs.append({'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 1, 'endIndex': 2},
                                               'properties': {'pixelSize': alt_cab or (120 if any('\n' in c for c in df.columns) else 58)}, 'fields': 'pixelSize'}})
    reqs += pinta(df, R, fmt)
    if notas:
        reqs += notas(df, ws)
    if altura:
        for c in corta:
            j = list(df.columns).index(c)
            reqs.append({'repeatCell': {'range': R(2, n, j, j + 1), 'cell': {'userEnteredFormat': {'wrapStrategy': 'CLIP'}},
                                        'fields': 'userEnteredFormat.wrapStrategy'}})
        reqs.append({'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 2, 'endIndex': n},
                                                   'properties': {'pixelSize': altura}, 'fields': 'pixelSize'}})
    else:
        reqs.append({'autoResizeDimensions': {'dimensions': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 2, 'endIndex': n}}})
    for a, b in recolher:
        reqs.append({'addDimensionGroup': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': a, 'endIndex': b}}})
    for lote in range(0, len(reqs), 300):
        sh.batch_update({'requests': reqs[lote:lote + 300]})
    return ws


# ---------------------------------------------------------------- Parlamentares
f = pd.read_csv(D + 'parlamentares.csv', dtype=str).fillna('')
f = f.rename(columns={c: c.replace('Atuação: ', '') for c in f.columns if c.startswith('Atuação: ')})
f = f.rename(columns={'Relevância política (1 a 4)': 'Relevância política (1 a 4)', 'Por quê': 'O que define a relevância'})
# subtemas (etapa 25), depois dos quatro grandes temas; textos longos num bloco de ficha no fim
from subtemas_texto import SUB, TEMAS, destaques
st = pd.read_csv(D + 'subtemas.csv', dtype=str).fillna('')
st = st.rename(columns={c: c.split('|')[1] for c in st.columns if '|' in c})
f = f.merge(st, on='Senador', how='left')
f['Partido/UF'] = f.Partido + '/' + f.UF
f['Destaques nos subtemas'] = f.Senador.map(destaques)
# Cabeças do Congresso do DIAP (etapa 26), na nota de "O que define a relevância"
dp = pd.read_csv(D + 'diap_cabecas.csv', dtype=str).fillna('').set_index('Senador')
NOTA_DIAP_CEL = {}
for n, r in dp.iterrows():
    partes = []
    if r['2026']:
        partes.append(f"DIAP 2026: {r['2026']}, avaliado como {r['Cargo na lista 2026'].lower()}.")
    else:
        partes.append("DIAP 2026: fora da lista.")
    partes.append(f"DIAP 2025: {r['2025'].lower() + ', avaliado como ' + r['Cargo na lista 2025'].lower() if r['2025'] else 'fora da lista'}.")
    NOTA_DIAP_CEL[n] = "\n".join(partes)
ID_COLS = ['Partido/UF', 'Cadeira', 'Posição no Senado', 'Relevância política (1 a 4)']
TEMA_COLS = TEMAS + ['Destaques nos subtemas']
FICHA = ['De onde vem', 'Profissão declarada (TSE)', 'Governismo', 'O que define a relevância',
         'Comissões permanentes (titular)', 'Preside hoje']
ROT = {'Saúde': 'saúde', 'Educação': 'educação', 'Esporte': 'esporte', 'Primeira infância': 'primeira infância'}
ordem = ['Senador'] + ID_COLS + TEMA_COLS
GR_P = [('', 1), ('Identidade', len(ID_COLS)), ('Atuação nos temas dos clientes', len(TEMA_COLS))]
RECOLHER = []
for t in TEMAS:
    RECOLHER.append((len(ordem), len(ordem) + len(SUB[t])))
    ordem += SUB[t]
    GR_P.append((f'Subtemas de {ROT[t]}', len(SUB[t])))
ordem += FICHA
GR_P.append(('Ficha', len(FICHA)))
f = f[ordem]
ATU = TEMAS + [s for t in TEMAS for s in SUB[t]]
LARG_P = {'Senador': 165, 'Partido/UF': 125, 'Cadeira': 112, 'Posição no Senado': 110, 'Relevância política (1 a 4)': 96,
          'Saúde': 96, 'Educação': 96, 'Esporte': 96, 'Primeira infância': 96, 'Destaques nos subtemas': 380,
          'De onde vem': 230, 'Profissão declarada (TSE)': 170, 'Governismo': 120, 'O que define a relevância': 300,
          'Comissões permanentes (titular)': 170, 'Preside hoje': 80, **{s: 128 for t in TEMAS for s in SUB[t]}}


def pinta_p(df, R, fmt):
    reqs = []
    cols = list(df.columns)
    for c in ['Cadeira', 'Posição no Senado', 'Governismo', 'Relevância política (1 a 4)', 'Preside hoje'] + ATU:
        j = cols.index(c)
        reqs.append({'repeatCell': {'range': R(2, len(df) + 2, j, j + 1), 'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER'}},
                                    'fields': 'userEnteredFormat.horizontalAlignment'}})
    for i, r in enumerate(df.itertuples(index=False), start=2):
        for c in ['Relevância política (1 a 4)'] + ATU:
            v = str(getattr(r, '_' + str(cols.index(c))) if False else r[cols.index(c)])
            if v in ESCALA:
                j = cols.index(c)
                reqs.append(fmt(R(i, i + 1, j, j + 1), ESCALA[v], '#FFFFFF' if v in TEXTO_BRANCO else '#000000', True, 'CENTER', 'MIDDLE'))
        p = r[cols.index('Posição no Senado')]
        if p in POSICAO:
            j = cols.index('Posição no Senado')
            reqs.append(fmt(R(i, i + 1, j, j + 1), POSICAO[p], '#000000', True, 'CENTER', 'MIDDLE'))
    # faixa dos subtemas em verde-escuro para separar dos grandes temas; ficha em cinza
    ini = 0
    for rot, k in GR_P:
        if rot.startswith('Subtemas'):
            reqs.append(fmt(R(0, 1, ini, ini + k), '#2A412D', '#FFFFFF', True, 'LEFT', 'MIDDLE', 10))
        elif rot == 'Ficha':
            reqs.append(fmt(R(0, 1, ini, ini + k), '#5A5A55', '#FFFFFF', True, 'LEFT', 'MIDDLE', 10))
        ini += k
    return reqs


NOTA_REL = ("Relevância política, de 1 a 4, pelo cargo mais alto que já ocupou ou ocupa hoje:\n"
            "4 = governador, ministro, presidente do Senado ou da Câmara, presidente ou vice da República, ou líder hoje "
            "do governo, da oposição, da maioria ou da minoria;\n"
            "3 = prefeito, vice-governador, secretário de estado, presidente de Assembleia, Mesa do Senado, líder hoje de "
            "partido ou bloco, ou presidente hoje de comissão permanente;\n"
            "2 = outro mandato eletivo ou cargo anterior (deputado, vereador, vice-prefeito, senador);\n"
            "1 = primeiro mandato eletivo.\n"
            "Quem está entre os Cabeças do Congresso do DIAP 2026 (os 100 mais influentes) tem no mínimo 3.\n"
            "A coluna ao lado diz o cargo que define a nota. Lideranças vigentes na data da atualização (API do Senado).")
NOTA_PROX = ("Quanto o senador trabalha neste tema no Congresso, olhando quatro coisas: se está hoje na comissão do tema, "
             "quantos projetos do tema escreveu, quantos relatou com parecer e quantos viraram lei.\n"
             "Referência: faz quase tudo isso, e muito. Atua: faz parte. Pouco: um sinal só. Não atua: nenhum.\n"
             "A nota de cada célula mostra os números do senador. Não diz se é a favor ou contra o cliente; "
             "isso está nas abas de cliente.")
SIGLAS_COM = ("Comissões do Senado: CAE Assuntos Econômicos; CAS Assuntos Sociais (saúde); CCDD Comunicação e Direito Digital; "
              "CCJ Constituição e Justiça; CCT Ciência e Tecnologia; CDH Direitos Humanos; CDIR Mesa Diretora; "
              "CDR Desenvolvimento Regional; CE Educação; CEsp Esporte; CI Infraestrutura; CMA Meio Ambiente; "
              "CRA Agricultura; CRE Relações Exteriores; CSP Segurança Pública; CTFC Transparência e Fiscalização.")
NOTA_SUB = ("Quanto o senador trabalha neste subtema: número de projetos do subtema em que é autor ou relator "
            "com parecer, no Senado e na Câmara. Não atua: nenhum. Pouco: 1. Atua: 2 a 4. Referência: 5 ou mais.\n"
            "Cada projeto foi lido pela ementa e encaixado em um ou dois subtemas. A nota de cada célula lista os projetos.\n"
            "Não diz se é a favor ou contra o cliente.")
NOTA_DEST = ("Subtemas em que o senador é Referência (5 ou mais projetos como autor ou relator com parecer) ou Atua "
             "(2 a 4). A grade completa, com Pouco e Não atua, está nas colunas de subtemas à direita; "
             "o botão de - acima delas recolhe cada grupo.")
NOTA_DIAP = ("Cabeças do Congresso: os 100 parlamentares mais influentes na avaliação anual do DIAP, "
             "edição de junho de 2026, e os 50 em ascensão (entre os 150 mais influentes). Para os eleitos em 2026 que eram "
             "deputados, vale a avaliação como deputado. Quem estava licenciado (ministro) no período não é avaliado.\n"
             "Perfil principal dado pelo DIAP: debatedor (repercute e domina o debate em plenário e na imprensa); "
             "articulador ou organizador (organiza bancadas e votações); formulador (elabora propostas e pareceres); "
             "formador de opinião (influencia a decisão dos demais); negociador (conduz acordos e negociações).\n"
             "A nota de cada célula traz também a edição de 2025.\n"
             "Fontes: diap.org.br/images/stories/novo_doc_compressed.pdf (2026) e "
             "diap.org.br/images/stories/cabecas_2025_resumo_imprensa.pdf (2025).")
notas_p = json.load(open(D + 'parlamentares_notas.json'))
notas_s = json.load(open(D + 'subtemas_notas.json'))


def notas_parl(df, ws):
    cols = list(df.columns)
    nota = lambda i, j, t: {'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': i, 'endRowIndex': i + 1,
                                                      'startColumnIndex': j, 'endColumnIndex': j + 1},
                                            'rows': [{'values': [{'note': t}]}], 'fields': 'note'}}
    reqs = [nota(1, cols.index('Relevância política (1 a 4)'), NOTA_REL),
            nota(1, cols.index('Destaques nos subtemas'), NOTA_DEST),
            nota(1, cols.index('Comissões permanentes (titular)'), SIGLAS_COM),
            nota(1, cols.index('Preside hoje'), SIGLAS_COM),
            nota(1, cols.index('O que define a relevância'), NOTA_DIAP)]
    for i, r in enumerate(df.itertuples(index=False), start=2):
        if r[0] in NOTA_DIAP_CEL:
            reqs.append(nota(i, cols.index('O que define a relevância'), NOTA_DIAP_CEL[r[0]]))
    for t in TEMAS:
        j = cols.index(t)
        reqs.append(nota(1, j, NOTA_PROX))
        for i, r in enumerate(df.itertuples(index=False), start=2):
            if f"{r[0]}|{t}" in notas_p:
                reqs.append(nota(i, j, notas_p[f"{r[0]}|{t}"]))
        for s in SUB[t]:
            j = cols.index(s)
            reqs.append(nota(1, j, NOTA_SUB))
            for i, r in enumerate(df.itertuples(index=False), start=2):
                if f"{r[0]}|{t}|{s}" in notas_s:
                    reqs.append(nota(i, j, notas_s[f"{r[0]}|{t}|{s}"]))
    return reqs


publicar('Parlamentares', f, GR_P, LARG_P, 1, pinta_p, notas_parl, RECOLHER, altura=46, alt_cab=96,
         corta=['Destaques nos subtemas'] + FICHA)

# ---------------------------------------------------------------- Votações-chave
v = pd.read_csv(D + 'votacoes_chave.csv', dtype=str).fillna('')
notas = json.load(open(D + 'votacoes_chave_notas.json'))
# o cabeçalho já diz o que o Sim faz; a nota do cabeçalho guarda o placar
SIGLA = {'EDI 0-5': 'Primeira infância desde a gestação (PL 1924)',
         'PLP 163/2025': 'Saúde e educação fora do arcabouço, PLP 163/2025'}


def titulo(c):
    base = c
    for k, nv in SIGLA.items():
        base = base.replace(k, nv)
    sim = re.split(r"[.;] (?:Placar|aprovad|rejeitad)", notas['cabecalho'].get(c, ''))[0].rstrip('.')
    return f"{base}\n{sim}" if sim else base


# quem não estava na Casa vira traço: o que importa na linha são os votos
FORA_DA_CASA = {'Não era senador', 'Não era deputado'}
v = v.replace({k: '–' for k in FORA_DA_CASA})
v.insert(1, 'Partido/UF', v.Partido + '/' + v.UF)
v = v.drop(columns=['Partido', 'UF'])
ORIG = {titulo(c): c for c in v.columns}
v = v.rename(columns={c: titulo(c) for c in v.columns})
cols = list(v.columns)
n_sen = sum(1 for c in cols[3:] if not c.startswith('Câmara') and 'CE (' not in c and 'CAS (' not in c)
n_com = sum(1 for c in cols[3:] if 'CE (' in c or 'CAS (' in c)
n_cam = sum(1 for c in cols[3:] if c.startswith('Câmara'))
GR_V = [('', 1), ('', 2), ('Senado: plenário', n_sen), ('Senado: comissões', n_com), ('Câmara: plenário (para quem era deputado)', n_cam)]
LARG_V = {'Senador': 165, 'Partido/UF': 125, 'Cadeira': 112, **{c: 175 for c in cols[3:]}}


def pinta_v(df, R, fmt):
    reqs = []
    for j in range(0, len(df.columns)):
        reqs.append({'repeatCell': {'range': R(2, len(df) + 2, j, j + 1), 'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT' if j < 2 else 'CENTER', 'verticalAlignment': 'MIDDLE'}},
                                    'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'}})
    for i, r in enumerate(df.itertuples(index=False), start=2):
        for j in range(3, len(df.columns)):
            x = r[j]
            if x == '–':
                reqs.append(fmt(R(i, i + 1, j, j + 1), '#FFFFFF', '#C8C8C2', False, 'CENTER', 'MIDDLE'))
            elif x in VOTO:
                reqs.append(fmt(R(i, i + 1, j, j + 1), VOTO[x], '#000000', True, 'CENTER', 'MIDDLE'))
            elif x:
                reqs.append(fmt(R(i, i + 1, j, j + 1), GELO, '#8A8A84', False, 'CENTER', 'MIDDLE', 10))
    return reqs


def notas_v(df, ws):
    reqs = [{'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': 1},
                             'rows': [{'values': [{'note': "Traço (–): o senador não estava na Casa na data da votação "
                                                            "(não era senador, ou não era deputado nas colunas da Câmara)."}]}],
                             'fields': 'note'}}]
    for j, c in enumerate(df.columns):
        c0 = ORIG.get(c, c)
        if c0 in notas['cabecalho']:
            reqs.append({'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': j, 'endColumnIndex': j + 1},
                                         'rows': [{'values': [{'note': notas['cabecalho'][c0]}]}], 'fields': 'note'}})
    for i, r in enumerate(df.itertuples(index=False), start=2):
        for j, c in enumerate(df.columns):
            t = notas['celulas'].get(f'{r[0]}|{ORIG.get(c, c)}')
            if t:
                reqs.append({'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': i, 'endRowIndex': i + 1, 'startColumnIndex': j, 'endColumnIndex': j + 1},
                                             'rows': [{'values': [{'note': t}]}], 'fields': 'note'}})
    return reqs


publicar('Votações-chave', v, GR_V, LARG_V, 1, pinta_v, notas_v)

# ---------------------------------------------------------------- ordem das abas
try:
    sh.worksheet('Senado: temas dos clientes').update_title('Perfil por tema')
except gspread.WorksheetNotFound:
    pass
from ordem_abas import ordenar
print(ordenar(sh))
