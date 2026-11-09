"""Publica a aba Lado dos projetos na planilha da 58ª Legislatura, para revisão dos coordenadores: uma linha por
cliente e projeto cujo lado (a favor ou contra o cliente) ainda não foi marcado por eles e que aparece nas evidências
de algum senador. Vem de três lugares do e24: a descrição do cliente (DESCRICAO, DESCRICAO_VOTOS, SINAIS_DIRETOS),
os projetos da base de pautas em Dúvida ou em branco (contam pela posição do cliente na pauta) e a posição informada
fora da base (posicoes_clientes_complemento.csv). Coluna Revisão em lista suspensa, Comentário livre.
A aba recebe edição dos coordenadores: o script não regrava se ela já existir (use --forca)."""
from ids import valor
import re
import sys

import gspread
import pandas as pd

sys.path.insert(0, '.')
sys.path.insert(0, 'dados_novos_eleitos/publicacao')
from outros.novos_eleitos import e24_clientes_senado as e24

ID = valor('SPREADSHEET_ID_58_SENADO')
TITULO = 'Lado dos projetos'
D = 'dados_novos_eleitos/temas_clientes/'
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, BORDA = '#192D4E', '#F4F3EF', '#DADAD4'
REVISAO = ['Confirma', 'Inverte o lado', 'Não conta para o cliente']
LADO = {1: 'A favor do cliente', -1: 'Contra o cliente'}
NOME = {'Coletivo Feminista': 'Coletivo (Nem Presa Nem Morta, Católicas pelo Direito de Decidir, Coletivo Feminista Sexualidade e Saúde)'}
AREA_ORDEM = ['Educação', 'Saúde', 'Esporte']

sh = gspread.service_account(filename='credentials.json').open_by_key(ID)
if TITULO in [w.title for w in sh.worksheets()] and '--forca' not in sys.argv:
    sys.exit(f'A aba {TITULO} já existe e pode ter revisão dos coordenadores; rode com --forca para regravar.')

cla = pd.read_csv(D + 'clientes_classificacao.csv', dtype=str).fillna('')
cla['Cliente'] = cla.Cliente.replace({v: k for k, v in NOME.items()})
area = dict(zip(cla.Cliente, cla.Área))
base = pd.read_csv(D + 'base_pautas.csv', dtype=str).fillna('')
area.update(dict(zip(base.Cliente, base.Área)))
area.update({k: a for k, (a, _) in e24.CLIENTES_FORA_DA_BASE.items()})

# ementa e Casa de cada matéria, para a nota da célula
GR = ('curso', 'reeleitos', 'novos')
aut = pd.concat([pd.read_csv(f'dados_novos_eleitos/perfil_senado_{g}/autoria.csv', dtype=str).assign(Casa='Senado') for g in GR]
                + [pd.read_csv('dados_novos_eleitos/perfil_camara_senado/autoria.csv', dtype=str).assign(Casa='Câmara')]).fillna('')
aut['k'] = aut['Matéria'].str.extract(r'^(\S+ \d+/\d{4})')[0]
ementa = aut.drop_duplicates(['k', 'Casa']).set_index(['k', 'Casa']).Ementa.to_dict()


def senadores_com(cliente, termo):
    """Senadores do cliente cuja evidência cita o termo num item que ainda precisa de checagem."""
    pad = re.compile(r'(?<![\w/])' + re.escape(termo) + r'(?!\d)')
    g = cla[cla.Cliente == cliente]
    return [r.Senador for r in g.itertuples()
            if any(pad.search(l) and 'checagem' in l for l in r.Evidências.split('\n'))]


linhas = []
for (k, casa), (sentidos, rot) in e24.DESCRICAO.items():
    for cli, s in sentidos.items():
        sen = senadores_com(cli, k)
        if sen:
            linhas.append([cli, f'{k} ({casa})', rot, LADO[s], 'Descrição do cliente', sen, ementa.get((k, casa), '')])
for k, casa, trecho, sentidos, rot, *fonte in e24.DESCRICAO_VOTOS:
    for cli, s in sentidos.items():
        # o mesmo projeto votado nas duas Casas sai em duas linhas, cada uma com quem votou naquela Casa
        sen = [n for n in senadores_com(cli, k) if any(rot.split(' (')[0] in l and ('plenário do Senado' in l) == (casa == 'Senado')
                                                       for l in cla[(cla.Cliente == cli) & (cla.Senador == n)].Evidências.iloc[0].split('\n'))]
        if sen:
            linhas.append([cli, f"{k} ({casa}), voto {'na comissão' if 'Parecer' in trecho else 'no plenário'}", re.sub(r'^(no|na) ', '', rot)[0].upper() + re.sub(r'^(no|na) ', '', rot)[1:], f'Votar Sim é {LADO[s].lower()}', 'Descrição do cliente', sen, ''])
for n, sentidos, forca, texto in e24.SINAIS_DIRETOS:
    for cli, s in sentidos.items():
        trecho = texto.split(' (')[0]  # o parêntese do texto vira '; ' nas evidências
        if any(trecho in l and 'checagem' in l for l in cla[(cla.Cliente == cli) & (cla.Senador == n)].Evidências):
            m = re.search(r'(Emenda \d+ ao \S+ \d+/\d{4}|\S+ \d+/\d{4})', texto)
            linhas.append([cli, m.group(1) if m else 'Declaração', texto[0].upper() + texto[1:], LADO[s], 'Descrição do cliente', [n], ''])
# projetos de Assembleia e câmara municipal (Inteligov, SAPL) lidos pela descrição do cliente
loc = pd.read_csv('dados_novos_eleitos/' + e24.LOCAIS, dtype=str).fillna('')
casa_loc = {(r.Parlamentar, r['Matéria'].strip()): r.Casa for _, r in loc.iterrows()}
for (n, mat), (sentidos, rot) in e24.DESCRICAO_LOCAL.items():
    for cli, s in sentidos.items():
        if n in senadores_com(cli, mat):
            linhas.append([cli, f"{mat} ({casa_loc.get((n, mat), 'Assembleia ou câmara municipal')})", rot, LADO[s], 'Descrição do cliente', [n], ''])
pend = base[base['Para o cliente'].isin(['', 'Dúvida']) & base['Cliente é'].isin(['A favor', 'Contra'])]
for r in pend.itertuples():
    lado = 'A favor do cliente' if r._4 == 'A favor' else 'Contra o cliente'
    for k, _ in e24.chaves(r.Projeto):
        sen = senadores_com(r.Cliente, k)
        if sen:
            marc = r._6 or 'em branco'
            linhas.append([r.Cliente, r.Projeto, e24.ROTULO_BASE.get(k, r.Pauta),
                           lado, f'Pauta "{r.Pauta}" na base de pautas (cliente {r._4.lower()}); projeto marcado {marc}', sen, ''])
            break
for r in pd.read_csv(D + 'posicoes_clientes_complemento.csv', dtype=str).itertuples():
    k = e24.chaves(r.Projeto)[0][0]
    sen = senadores_com(r.Cliente, k)
    if sen:
        linhas.append([r.Cliente, r.Projeto, r.Pauta, LADO[1 if r._5 == 'Promotor' else -1], 'Posição informada fora da base de pautas', sen, ''])

df = pd.DataFrame(linhas, columns=['Cliente', 'Projeto', 'O que o projeto faz', 'Lado assumido', 'De onde veio o lado', 'sen', 'Ementa'])
# só os itens decisivos (27/10/2026): o que muda Promotor, Neutro ou Detrator se o coordenador disser que não conta ou
# que o lado é o contrário (regra em decisivos_lado.py, que tem de rodar antes). A coluna mostra só os senadores afetados.
dec = pd.read_csv('dados_novos_eleitos/publicacao/decisivos_lado.csv', dtype=str).fillna('')
dec = dec[dec.decisivo == 'True']
dec['Cliente'] = dec.Cliente.replace({v: k for k, v in NOME.items()})
afetados = dec.groupby(['Cliente', 'Projeto']).Senador.agg(set).to_dict()
ID_PROJ = re.compile(r'(Emenda (?:de Plenário )?\d+ ao \S+ \d+/\d{4}|(?:PLS|PLP|PEC|PDL|PDC|PL|RQS|REQ) \d+/\d{4})')
def _afet(r):
    m = ID_PROJ.search(r.Projeto) or ID_PROJ.search(r['O que o projeto faz'])
    return sorted(set(r.sen) & afetados.get((r.Cliente, m.group(1) if m else ''), set()))
df['sen'] = df.apply(_afet, axis=1)
df = df[df.sen.map(len) > 0].copy()
# 09/11/2026: corte pedido por ela (os coordenadores não revisam milhares de linhas): fica só o item que pesa na
# classificação e cujo lado ficou ambíguo na leitura do texto (temas_clientes/lado_corte_senado.csv), com a pergunta.
corte = pd.read_csv(D + 'lado_corte_senado.csv', dtype=str).fillna('')
corte = corte[corte.ambiguo == 'True']
perg = {(r.Cliente, r.Projeto): r.pergunta for r in corte.itertuples()}
df['pergunta'] = [perg.get((c, p), '') for c, p in zip(df.Cliente, df.Projeto)]
df = df[df.pergunta != ''].copy()
df['Área'] = df.Cliente.map(area)
df['Senadores'] = df.sen.map(lambda x: '; '.join(sorted(x)))
df['Qtd'] = df.sen.map(len)
df['O que o projeto faz'] = df['O que o projeto faz'].map(lambda t: t[0].upper() + t[1:])
df['ordem'] = df['Área'].map(AREA_ORDEM.index)
df = df.sort_values(['ordem', 'Cliente', 'Lado assumido', 'Qtd'], ascending=[True, True, True, False],
                    key=lambda s: s.str.lower() if s.dtype == object else s).drop_duplicates(['Cliente', 'Projeto'])
df['Cliente'] = df.Cliente.replace(NOME)

CAB = ['Área', 'Cliente', 'Projeto', 'O que o projeto faz', 'Lado assumido', 'De onde veio o lado',
       'Senadores cuja classificação depende disso', 'Revisão', 'Comentário']
valores = [CAB] + [[r['Área'], r.Cliente, r.Projeto, r['O que o projeto faz'], r['Lado assumido'], r['De onde veio o lado'],
                    f"{r.Qtd}: {r.Senadores}", '', f"precisa de checagem dos coordenadores: {r.pergunta}"] for _, r in df.iterrows()]
n, m = len(valores), len(CAB)

try:
    ws = sh.worksheet(TITULO)
    ws.clear()
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TITULO, rows=n, cols=m)
ws.resize(rows=n, cols=m)
ws.update(values=valores, range_name='A1', value_input_option='RAW')

R = lambda a, b, c0=0, c1=m: {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}


def fmt(r, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE'):
    return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


fina = {'style': 'SOLID', 'color': cor(BORDA)}
grossa = {'style': 'SOLID_MEDIUM', 'color': cor(MARINHO)}
LARG = [80, 150, 150, 300, 130, 230, 260, 150, 260]
reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': 3}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(R(0, 1), MARINHO, '#FFFFFF', True, 'CENTER', 'MIDDLE'),
        fmt(R(1, n), GELO), fmt(R(1, n, 1, 2), GELO, MARINHO, True),
        fmt(R(1, n, 7, 9), '#FFFFFF'),
        {'setDataValidation': {'range': R(1, n, 7, 8), 'rule': {'condition': {'type': 'ONE_OF_LIST', 'values': [{'userEnteredValue': v} for v in REVISAO]},
                                                                'strict': True, 'showCustomUi': True}}},
        {'setBasicFilter': {'filter': {'range': R(0, n)}}},
        {'updateBorders': {'range': R(0, n), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                       'properties': {'pixelSize': 36}, 'fields': 'pixelSize'}}]
reqs += [{'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                        'properties': {'pixelSize': w}, 'fields': 'pixelSize'}} for i, w in enumerate(LARG)]
cores = {'Confirma': '#9BD0B7', 'Inverte o lado': '#F4C7C3', 'Não conta para o cliente': '#E1E1E1'}
reqs += [{'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [R(1, n, 7, 8)], 'booleanRule': {
    'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': k}]}, 'format': {'backgroundColor': cor(c)}}}}} for k, c in cores.items()]
# linha grossa entre clientes
cl = list(df.Cliente)
reqs += [{'updateBorders': {'range': R(i + 1, i + 2), 'bottom': grossa}} for i in range(len(cl)) if i + 1 == len(cl) or cl[i + 1] != cl[i]]
notas = [(0, 7, 'Confirma: o lado está certo. Inverte o lado: o projeto é do lado contrário. Não conta para o cliente: o projeto não tem a ver com a agenda do cliente.')]
notas += [(i + 1, 2, e) for i, e in enumerate(df.Ementa) if e]
reqs += [{'updateCells': {'range': R(a, a + 1, b, b + 1), 'rows': [{'values': [{'note': t}]}], 'fields': 'note'}} for a, b, t in notas]
sh.batch_update({'requests': reqs})

from ordem_abas import ordenar
print(TITULO, n - 1, 'linhas', df.groupby('Cliente').size().to_dict(), ordenar(sh))
