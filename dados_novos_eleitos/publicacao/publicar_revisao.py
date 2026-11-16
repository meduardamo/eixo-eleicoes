"""Publica as duas abas da revisão manual (segunda etapa do mapeamento, 16/11/2026) na planilha da Câmara ou do Senado:
'Revisão: lista prioritária' (por cliente, até LIMITE nomes: promotores, detratores e neutros de maior relevância e
classificações que dependem de um sinal só, revisão por exceção, evidências em nota na classificação), 'Revisão: andamento'
(conferidos, faltam, erros e situação por cliente, por fórmula a partir da caixa Conferido de cada linha). Lê as abas Clientes e Parlamentares da própria planilha.
As abas recebem resposta dos analistas e coordenadores: o script não regrava se existirem (use --forca).
Uso: python dados_novos_eleitos/publicacao/publicar_revisao.py camara|senado [--previa] [--forca]"""
import collections
import re
import sys

import gspread

sys.path.insert(0, 'dados_novos_eleitos/publicacao')
sys.path.insert(0, 'dados_novos_eleitos/buscas_locais_2026-11-04')
sys.path.insert(0, 'dados_novos_eleitos/conferir_camara_2026-11-03/apl')
from camfun import classifica, parse_sinais
from ids import valor

CASA = sys.argv[1]
ID = valor({'camara': 'SPREADSHEET_ID_58_CAMARA', 'senado': 'SPREADSHEET_ID_58_SENADO'}[CASA])
QUEM = {'camara': 'Deputado', 'senado': 'Senador'}[CASA]
LISTA, ANDAMENTO = 'Revisão: lista prioritária', 'Revisão: andamento'
OUTROS = 'Revisão: outros nomes'  # 16/11: tirada a pedido dela; se existir, o --forca apaga
LIMITE = 25
COTA = [('Promotor', 12), ('Detrator', 8), ('Neutro', 3), ('Sinais opostos', 2)]
RESTO = ['Promotor', 'Detrator', 'Neutro', 'Sinais opostos', 'Um sinal contra', 'Só coautorias contra', 'Um sinal a favor', 'Só coautorias a favor']
ORDEM_CLASSE = ['Promotor', 'Detrator', 'Neutro', 'Sinais opostos', 'Um sinal a favor', 'Só coautorias a favor', 'Um sinal contra', 'Só coautorias contra']
AREA_ORDEM = ['Educação', 'Saúde', 'Esporte']
# revisão por exceção: só se marca o que está errado; linha em branco = de acordo, e o cliente é dado por conferido na aba de andamento
REVISAO = ['Classificação errada', 'Evidência errada ou faltando']
MUDA = ['Classificação errada', 'Evidência errada ou faltando', 'Atua na pauta e não aparece']
FICA = ['Promotor', 'Neutro', 'Detrator', 'Sem posição clara']
# retorno da aplicação, preenchido por Dados no lote de 19/11 e no de 24/11; o motivo vai em nota na célula
SITUACAO = ['Aplicado', 'Precisa de fonte', 'Não aplicado']
MARINHO, GELO, BORDA = '#192D4E', '#F4F3EF', '#DADAD4'
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}

sh = gspread.service_account(filename='credentials.json').open_by_key(ID)
existentes = {w.title: w for w in sh.worksheets()}
if any(t in existentes for t in (LISTA, ANDAMENTO)) and '--forca' not in sys.argv and '--previa' not in sys.argv:
    sys.exit('As abas de revisão já existem e podem ter resposta da equipe; rode com --forca para regravar.')

cli = sh.worksheet('Clientes').get_all_values()
par = sh.worksheet('Parlamentares').get_all_values()
rel = {r[0]: int(r[4]) if r[4].strip().isdigit() else 0 for r in par[2:] if r[0]}
# 16/11: o número sozinho não diz nada; o motivo vem da coluna "O que define a relevância" da Parlamentares
_i_motivo = par[1].index('O que define a relevância')
motivo_rel = {r[0]: r[_i_motivo].strip() for r in par[2:] if r[0] and len(r) > _i_motivo}
_MANDATO = re.compile(r'^(deputad[oa] (federal|estadual|distrital)|vereador[a]?|senador[a]?)\b', re.I)


def motivo_curto(nome, nivel):
    """Só os cargos que dão o nível (tira 'deputado federal desde...' e parecidos); até dois itens. O texto inteiro vai em nota."""
    partes = [t.strip() for t in motivo_rel.get(nome, '').split(';') if t.strip()]
    if nivel >= 3:
        fortes = [t for t in partes if not _MANDATO.match(t)]
        partes = fortes or partes
    curto = '; '.join(partes[:2])
    return curto[0].upper() + curto[1:] if curto else ''


def depende_de_um_sinal(sinais, classe):
    """Promotor ou Detrator que deixa de ser se o sinal mais forte do lado dele sair da conta."""
    d = parse_sinais(sinais)
    if classe not in ('Promotor', 'Detrator'): return False
    lado = 'p' if classe == 'Promotor' else 'n'
    k = 'f' + lado if d['f' + lado] else 'm' + lado
    e = collections.Counter(d); e[k] -= 1
    return classifica(e['fp'], e['fn'], e['mp'], e['mn']) != classe


por_cliente = collections.defaultdict(list)
for r in cli[1:]:
    if r[0]:
        por_cliente[(r[1], r[0])].append({'nome': r[2], 'partido': r[3], 'classe': r[5], 'sinais': r[6], 'ev': r[8],
                                          'rel': rel.get(r[2], 0), 'fragil': depende_de_um_sinal(r[6], r[5]),
                                          'n': sum(parse_sinais(r[6]).values())})
chave = lambda x: (-x['rel'], not x['fragil'], -x['n'], x['nome'])
linhas, resumo, evid, motivos_inteiros = [], [], [], []
for (area, cliente) in sorted(por_cliente, key=lambda k: (AREA_ORDEM.index(k[0]) if k[0] in AREA_ORDEM else 9, k[1].lower())):
    g = por_cliente[(area, cliente)]
    esc = []
    for classe, q in COTA:
        esc += sorted([x for x in g if x['classe'] == classe], key=chave)[:q]
    for classe in RESTO:  # sobra de cota vai para quem ficou de fora, na ordem das classes
        if len(esc) >= LIMITE: break
        esc += sorted([x for x in g if x['classe'] == classe and x not in esc], key=chave)[:LIMITE - len(esc)]
    esc.sort(key=lambda x: (ORDEM_CLASSE.index(x['classe']), chave(x)))
    for x in esc:
        motivo = ['Peso político'] if x['rel'] >= 3 else []
        if x['fragil']: motivo.append('Classificação depende de um sinal')
        if x['classe'] in ('Neutro', 'Sinais opostos'): motivo.append('Sinais dos dois lados')
        if x['classe'].startswith(('Um sinal', 'Só coautorias')): motivo.append('Um sinal só')
        linhas.append([area, cliente, x['nome'], x['partido'], x['rel'] or '', motivo_curto(x['nome'], x['rel']), x['classe'], x['sinais'],
                       '; '.join(motivo), False, '', '', '', '', ''])
        evid.append(x['ev'])
        motivos_inteiros.append(motivo_rel.get(x['nome'], ''))
    resumo.append((area, cliente, len(esc), collections.Counter(x['classe'] for x in esc)))

if '--previa' in sys.argv:
    for a, c, n, cnt in resumo: print(f'{a:9} {c[:28]:28} {n:3}', dict(cnt))
    print('total', len(linhas)); sys.exit()

CAB = ['Área', 'Cliente', QUEM, 'Partido/UF', 'Relevância política (1 a 4)', 'O que define a relevância', 'Classificação', 'Sinais', 'Por que está na lista', 'Conferido',
       'O que está errado', 'Como deveria ficar', 'O que você sabe e de onde vem', 'Quem revisou', 'Situação']
fina = {'style': 'SOLID', 'color': cor(BORDA)}
grossa = {'style': 'SOLID_MEDIUM', 'color': cor(MARINHO)}


def aba(titulo, n, m):
    if titulo in existentes:  # --forca: apaga e recria, para não sobrar nota, lista ou regra de cor da versão anterior
        sh.del_worksheet(existentes[titulo])
    return sh.add_worksheet(title=titulo, rows=n, cols=m)


def fmt(ws, a, b, c0, c1, fundo, texto='#000000', negrito=False, h='LEFT', quebra='WRAP'):
    return {'repeatCell': {'range': {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1},
                           'cell': {'userEnteredFormat': {'backgroundColor': cor(fundo), 'wrapStrategy': quebra, 'verticalAlignment': 'MIDDLE',
                                                          'horizontalAlignment': h, 'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': negrito,
                                                                                                   'foregroundColor': cor(texto)}}},
                           'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


def lista(ws, a, b, c, valores=None, intervalo=None, estrita=True):
    cond = ({'type': 'ONE_OF_LIST', 'values': [{'userEnteredValue': v} for v in valores]} if valores
            else {'type': 'ONE_OF_RANGE', 'values': [{'userEnteredValue': intervalo}]})
    return {'setDataValidation': {'range': {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c, 'endColumnIndex': c + 1},
                                  'rule': {'condition': cond, 'strict': estrita, 'showCustomUi': True}}}


def comum(ws, n, m, larguras, congela):
    return ([{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': congela}},
                                        'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
             fmt(ws, 0, 1, 0, m, MARINHO, '#FFFFFF', True, 'CENTER'),
             {'updateBorders': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': n, 'startColumnIndex': 0, 'endColumnIndex': m},
                                'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
             {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                            'properties': {"pixelSize": 66}, 'fields': 'pixelSize'}}]
            + [{'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                              'properties': {'pixelSize': w}, 'fields': 'pixelSize'}} for i, w in enumerate(larguras)])


def nota(ws, c, t):
    return {'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': c, 'endColumnIndex': c + 1},
                            'rows': [{'values': [{'note': t}]}], 'fields': 'note'}}


def cores_resposta(ws, n, c, mapa):
    return [{'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [{'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': n, 'startColumnIndex': c, 'endColumnIndex': c + 1}],
                                                               'booleanRule': {'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': k}]},
                                                                               'format': {'backgroundColor': cor(v)}}}}} for k, v in mapa.items()]


# 1. lista prioritária: linha de uma altura só (o cliente inteiro cabe na tela); as evidências ficam em nota na célula da
# classificação e a trajetória inteira em nota na célula do motivo da relevância
C_REL, C_MOT, C_CLA, C_POR, C_CONF, C_ERR, C_FICA, C_SABE = 4, 5, 6, 8, 9, 10, 11, 12
LCONF = 'J'  # letra da coluna Conferido (andamento e cor da linha)
n, m = len(linhas) + 1, len(CAB)
ws = aba(LISTA, n, m)
ws.update(values=[CAB] + linhas, range_name='A1', value_input_option='RAW')
reqs = comum(ws, n, m, [80, 120, 190, 110, 95, 300, 130, 190, 235, 95, 190, 150, 380, 130, 140], 3)
reqs += [fmt(ws, 1, n, 0, C_CONF, GELO, quebra='CLIP'), fmt(ws, 1, n, 2, 3, GELO, MARINHO, True, quebra='CLIP'), fmt(ws, 1, n, C_REL, C_REL + 1, GELO, h='CENTER'),
         fmt(ws, 1, n, C_CONF, m - 1, '#FFFFFF'), fmt(ws, 1, n, C_CONF, C_CONF + 1, '#FFFFFF', h='CENTER'),
         {'setDataValidation': {'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': n, 'startColumnIndex': C_CONF, 'endColumnIndex': C_CONF + 1},
                                'rule': {'condition': {'type': 'BOOLEAN'}, 'strict': True}}}, fmt(ws, 1, n, m - 1, m, GELO), lista(ws, 1, n, m - 1, SITUACAO),
         lista(ws, 1, n, C_ERR, REVISAO), lista(ws, 1, n, C_FICA, FICA, estrita=False),
         {'setBasicFilter': {'filter': {'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': n, 'startColumnIndex': 0, 'endColumnIndex': m}}}},
         nota(ws, C_MOT, 'Os cargos que dão o nível de relevância. Passe o mouse para ver a trajetória inteira.'),
         nota(ws, C_CLA, 'Passe o mouse na classificação de cada nome para ler as evidências.'),
         nota(ws, C_CONF, 'Marque cada nome que você já olhou, esteja certo ou errado.'),
         nota(ws, C_ERR, 'Preencha só quando algo estiver errado. Classificação errada: deveria ser outra. '
                         'Evidência errada ou faltando: a classificação pode estar certa, mas alguma evidência está errada ou falta alguma. '
                         'Linha em branco quer dizer que está de acordo.'),
         nota(ws, C_SABE, 'Posição ou ação concreta, com a fonte: projeto, fala, notícia, reunião, conversa com o cliente. Partido e perfil não contam.'),
         {'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': n, 'startColumnIndex': C_CLA, 'endColumnIndex': C_CLA + 1},
                          'rows': [{'values': [{'note': e}]} for e in evid], 'fields': 'note'}},
         {'updateCells': {'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': n, 'startColumnIndex': C_MOT, 'endColumnIndex': C_MOT + 1},
                          'rows': [{'values': [{'note': t}]} for t in motivos_inteiros], 'fields': 'note'}}]
# linha conferida fica verde de A até a caixa, para ver de longe o que já foi olhado
reqs.append({'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [{'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': n, 'startColumnIndex': 0, 'endColumnIndex': C_CONF + 1}],
                                                               'booleanRule': {'condition': {'type': 'CUSTOM_FORMULA', 'values': [{'userEnteredValue': f'=${LCONF}2=TRUE'}]},
                                                                               'format': {'backgroundColor': cor('#C4E4D3')}}}}})
reqs += cores_resposta(ws, n, C_ERR, {'Classificação errada': '#F4C7C3', 'Evidência errada ou faltando': '#F6DFA4'})
reqs += cores_resposta(ws, n, m - 1, {'Aplicado': '#9BD0B7', 'Precisa de fonte': '#F6DFA4', 'Não aplicado': '#E1E1E1'})
reqs.append(nota(ws, m - 1, 'Retorno de cada marcação. Aplicado: entrou na base. Precisa de fonte: falta dizer de onde vem a informação para aplicar. '
                          'Não aplicado: o motivo está na nota da célula. Aplicação em dois lotes: 19/11 e 25/11.'))
cl = [l[1] for l in linhas]
reqs += [{'updateBorders': {'range': {'sheetId': ws.id, 'startRowIndex': i + 1, 'endRowIndex': i + 2, 'startColumnIndex': 0, 'endColumnIndex': m}, 'bottom': grossa}}
         for i in range(len(cl)) if i + 1 == len(cl) or cl[i + 1] != cl[i]]
sh.batch_update({'requests': reqs})

if OUTROS in existentes: sh.del_worksheet(existentes[OUTROS])

# 3. andamento: tudo por fórmula a partir da caixa Conferido de cada linha (16/11: ela pediu marcar também o que está certo,
# para ninguém reler o mesmo nome)
CAB3 = ['Área', 'Cliente', 'Nomes na lista', 'Conferidos', 'Faltam', 'Erros apontados', 'Situação']
L = f"'{LISTA}'"
v3 = [CAB3] + [[a, c, f'=COUNTIF({L}!$B$2:$B;B{i})', f'=COUNTIFS({L}!$B$2:$B;B{i};{L}!${LCONF}$2:${LCONF};TRUE)', f'=C{i}-D{i}',
                f'=COUNTIFS({L}!$B$2:$B;B{i};{L}!$K$2:$K;"?*")',
                f'=IF(E{i}=0;"Concluído";IF(D{i}=0;"Não começou";"Em andamento"))'] for i, (a, c, _, _) in enumerate(resumo, 2)]
ws3 = aba(ANDAMENTO, len(v3), len(CAB3))
ws3.update(values=v3, range_name='A1', value_input_option='USER_ENTERED')
n3 = len(v3)
reqs = comum(ws3, n3, len(CAB3), [90, 260, 110, 110, 90, 130, 130], 2)
reqs += [fmt(ws3, 1, n3, 0, len(CAB3), GELO), fmt(ws3, 1, n3, 1, 2, GELO, MARINHO, True), fmt(ws3, 1, n3, 2, len(CAB3), GELO, h='CENTER')]
reqs += cores_resposta(ws3, n3, 6, {'Concluído': '#9BD0B7', 'Em andamento': '#F6DFA4'})
sh.batch_update({'requests': reqs})

from ordem_abas import ordenar
print(CASA, len(linhas), 'linhas na lista;', ordenar(sh))
