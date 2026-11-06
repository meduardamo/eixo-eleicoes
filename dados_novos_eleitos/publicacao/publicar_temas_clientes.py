"""Grava o perfil por tema dos clientes (etapa 22) dos 81 senadores numa aba só, no padrão Eixo.
Uma linha por senador e tema; a coluna Cadeira separa mandato em curso, reeleitos e novos.
Leitura: cabeçalho e as duas primeiras colunas (senador e tema) congelados, filtro em todas as colunas,
blocos de quatro linhas por senador com fundo alternado e linha divisória entre senadores."""
from ids import valor
import re

import gspread
import pandas as pd

D = 'dados_novos_eleitos/'
ID = valor('SPREADSHEET_ID_58_SENADO')
TITULO = 'Perfil por tema'
ANTIGAS = ['Senado: temas dos clientes', 'Senado 2022: temas dos clientes', 'Senado reeleitos: temas dos clientes', 'Senado novos: temas dos clientes']
CADEIRA = {'curso': 'Mandato até 2031', 'reeleitos': 'Reeleito em 2026', 'novos': 'Eleito em 2026'}
TEMAS = ['Saúde', 'Educação', 'Esporte', 'Primeira infância']
RENOMEIA = {'Parlamentar': 'Senador',
            'O que defende (plenário e comissões, desde 2023)': 'O que defende (desde 2023)',
            'O que defende (plenário da Câmara desde 2023 ou imprensa)': 'O que defende (desde 2023)',
            'O que defende (plenário e comissões da Câmara desde 2023, ou imprensa)': 'O que defende (desde 2023)',
            'Como votou (votações que separam os senadores)': 'Como votou (votações-chave)',
            'Como votou (Câmara, para quem era deputado)': 'Como votou (votações-chave)',
            'Requerimentos no tema (audiência e urgência, desde 2023)': 'Pedidos de audiência e urgência (desde 2023)',
            'Primeiro autor': 'Projetos como primeiro autor', 'Relatorias com parecer': 'Relatorias com parecer',
            'Relatorias em curso': 'Relatorias em curso', 'Viraram lei': 'Viraram lei'}
COLUNAS = ['Senador', 'Tema', 'Atuação no tema', 'Subtemas', 'Partido/UF', 'Cadeira', 'O que fez', 'O que defende (desde 2023)',
           'Como votou (votações-chave)', 'Pedidos de audiência e urgência (desde 2023)', 'Comissão do tema hoje',
           'Frentes do tema no Senado', 'Projetos como primeiro autor', 'Relatorias com parecer', 'Relatorias em curso',
           'Viraram lei', 'Leis', 'Observação sobre a cadeira', 'Fontes (matérias)']
LARG = {'Senador': 165, 'Tema': 110, 'Atuação no tema': 92, 'Subtemas': 330, 'Partido/UF': 125, 'Cadeira': 112, 'O que fez': 480,
        'O que defende (desde 2023)': 520, 'Como votou (votações-chave)': 400,
        'Pedidos de audiência e urgência (desde 2023)': 360, 'Comissão do tema hoje': 150,
        'Frentes do tema no Senado': 200, 'Projetos como primeiro autor': 85, 'Relatorias com parecer': 85,
        'Relatorias em curso': 85, 'Viraram lei': 70, 'Leis': 260, 'Observação sobre a cadeira': 220,
        'Fontes (matérias)': 200}
NUMEROS = ['Projetos como primeiro autor', 'Relatorias com parecer', 'Relatorias em curso', 'Viraram lei', 'Atuação no tema']
# mesma escala verde da aba Parlamentares
ATUACAO = {'Não atua': '#F4F3EF', 'Pouco': '#DCEFE4', 'Atua': '#9BD0B7', 'Referência': '#2A412D'}
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}

partes = []
for g, rot in CADEIRA.items():
    p = pd.read_csv(D + f'temas_clientes/perfil_{g}.csv', dtype=str).fillna('').rename(columns=RENOMEIA)
    p['Cadeira'] = rot
    partes.append(p)
df = pd.concat(partes)
df['ordem_tema'] = df.Tema.map(TEMAS.index)
df = df.sort_values(['Senador', 'ordem_tema'], key=lambda s: s.str.normalize('NFKD') if s.dtype == object else s)
# atuação no grande tema (etapa 23) e nos subtemas (etapa 25), ao lado do tema
from subtemas_texto import por_tema
atu = pd.read_csv(D + 'temas_clientes/parlamentares.csv', dtype=str).fillna('').set_index('Senador')
df['Atuação no tema'] = [atu.at[s, f'Atuação: {t}'] for s, t in zip(df.Senador, df.Tema)]
df['Subtemas'] = [por_tema(s, t) for s, t in zip(df.Senador, df.Tema)]
df['Partido/UF'] = df.Partido + '/' + df.UF
# textos corridos viram tópicos (só apresentação)
import topicos
for col, f in [('O que fez', topicos.o_que_fez), ('O que defende (desde 2023)', topicos.o_que_defende),
               ('Como votou (votações-chave)', topicos.como_votou),
               ('Pedidos de audiência e urgência (desde 2023)', topicos.requerimentos)]:
    df[col] = df[col].map(f)
df = df[COLUNAS]


def leis_legiveis(t):
    """'PL 2538/2019: Transformado em Norma Jurídica' e 'PL x: Lei nº 14.819 de 16/01/2024' viram frases curtas."""
    out = []
    for l in str(t).split("\n"):
        if not l.strip():
            continue
        mat, _, st = l.partition(":")
        st = st.strip()
        m = re.match(r"(Lei(?: Complementar)?|Resolução do Senado Federal|Decreto Legislativo) nº ([\d.]+) de \d{2}/\d{2}/(\d{4})", st)
        if m:
            out.append(f"{mat.strip()} virou a {m.group(1)} {m.group(2)}/{m.group(3)}")
        elif "Norma Jurídica" in st:
            out.append(f"{mat.strip()} virou lei")
        else:
            out.append(l.strip())
    return "\n".join(out)


df['Leis'] = df['Leis'].map(leis_legiveis)

gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key(ID)
try:
    ws = sh.worksheet(TITULO)
    sh.batch_update({'requests': [{'clearBasicFilter': {'sheetId': ws.id}}]})
    ws.clear()
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TITULO, rows=len(df) + 1, cols=len(df.columns))
n, m = len(df) + 1, len(df.columns)
ws.resize(rows=n, cols=m)
ws.update(values=[COLUNAS] + df.values.tolist(), range_name='A1', value_input_option='RAW')


def rng(a, b, c0=0, c1=m):
    return {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}


def fmt(r, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE'):
    return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


fina = {'style': 'SOLID', 'color': cor('#DADAD4')}
divisoria = {'style': 'SOLID_MEDIUM', 'color': cor('#192D4E')}
reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': 2}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(rng(0, 1), '#192D4E', '#FFFFFF', True, 'CENTER', 'MIDDLE')]
# blocos de quatro linhas por senador, fundo alternado (gelo e branco)
senadores = df.Senador.tolist()
inicio = 1
for i, s in enumerate(dict.fromkeys(senadores)):
    k = senadores.count(s)
    reqs.append(fmt(rng(inicio, inicio + k), '#F4F3EF' if i % 2 == 0 else '#FFFFFF'))
    reqs.append(fmt(rng(inicio, inicio + k, 0, 1), '#F4F3EF' if i % 2 == 0 else '#FFFFFF', '#192D4E', True))
    reqs.append({'updateBorders': {'range': rng(inicio, inicio + k), 'bottom': divisoria}})
    inicio += k
for c in NUMEROS:
    j = COLUNAS.index(c)
    reqs.append({'repeatCell': {'range': rng(1, n, j, j + 1), 'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER'}},
                                'fields': 'userEnteredFormat.horizontalAlignment'}})
j = COLUNAS.index('Tema')
reqs.append({'repeatCell': {'range': rng(1, n, j, j + 1), 'cell': {'userEnteredFormat': {'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': True}}},
                            'fields': 'userEnteredFormat.textFormat.bold'}})
j = COLUNAS.index('Atuação no tema')
for i, v in enumerate(df['Atuação no tema'], start=1):
    if v in ATUACAO:
        reqs.append(fmt(rng(i, i + 1, j, j + 1), ATUACAO[v], '#FFFFFF' if v == 'Referência' else '#000000', True, 'CENTER', 'MIDDLE'))
reqs += [{'updateBorders': {'range': rng(0, n), 'innerVertical': fina, 'right': fina}},
         {'setBasicFilter': {'filter': {'range': rng(0, n)}}}]
reqs += [{'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                        'properties': {'pixelSize': LARG[c]}, 'fields': 'pixelSize'}} for i, c in enumerate(COLUNAS)]
reqs.append({'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                           'properties': {'pixelSize': 42}, 'fields': 'pixelSize'}})
reqs.append({'autoResizeDimensions': {'dimensions': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 1, 'endIndex': n}}})
for lote in range(0, len(reqs), 200):
    sh.batch_update({'requests': reqs[lote:lote + 200]})

for t in ANTIGAS:
    try:
        sh.del_worksheet(sh.worksheet(t))
    except gspread.WorksheetNotFound:
        pass
abas = sh.worksheets(); por = {w.title: w for w in abas}
from ordem_abas import ordenar
ordenar(sh)
print(TITULO, len(df), 'x', m, df.Senador.nunique(), [w.title for w in sh.worksheets()])
