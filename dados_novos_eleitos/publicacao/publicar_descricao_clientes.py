"""Publica a aba 'Descrição dos clientes' (16/11/2026, para a revisão manual da equipe) na planilha da Câmara ou do
Senado: uma linha por cliente com a descrição usada no mapeamento (texto do documento de clientes dos coordenadores,
PDF "Monitor Legislativo e DOU - Segunda Etapa", sem as listas de cargos, que são do DOU), as pautas acompanhadas com o
lado do cliente e os subtemas. Pautas e subtemas vêm da tabela de apoio da aba Por cliente (I:N) da própria planilha,
para a aba mostrar exatamente o que a classificação usou.
Uso: python dados_novos_eleitos/publicacao/publicar_descricao_clientes.py camara|senado"""
import re
import subprocess
import sys

import gspread

sys.path.insert(0, 'dados_novos_eleitos/publicacao')
from ids import valor

CASA = sys.argv[1]
ID = valor({'camara': 'SPREADSHEET_ID_58_CAMARA', 'senado': 'SPREADSHEET_ID_58_SENADO'}[CASA])
TITULO = 'Descrição dos clientes'
PDF = '/Users/eduardamarques/Downloads/clientes/iu/Monitor Legislativo e DOU - Segunda Etapa.pdf'
MARINHO, GELO, BORDA = '#192D4E', '#F4F3EF', '#DADAD4'
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
AREA_ORDEM = ['Educação', 'Saúde', 'Esporte']

# início de cada bloco no PDF; o bloco vai até a lista de cargos, um separador ou o início do próximo cliente
INICIO = {
    'IU': 'O Instituto Unibanco (IU)', 'FMCSV': 'A Fundação Maria Cecilia Souto Vidigal', 'IEPS': 'O Instituto de Estudos para Políticas de Saúde',
    'ISG': 'O Instituto Sonho Grande', 'REMS': 'A REMS', 'Cactus': 'O Instituto Cactus', 'Vital Strategies': 'A Vital Strategies',
    'Nem Presa Nem Morta': 'O Nem Presa Nem Morta', 'IDEC': 'O Instituto Brasileiro de Defesa do Consumidor', 'Umane': 'A Umane',
    'ASBAI': 'A Associação Brasileira de Alergia', 'Infinis': 'O Instituto Futuro é Infância Saudável', 'Fundação Itaú': 'A Fundação Itaú é',
    'UNFPA': 'O Fundo de População das Nações Unidas', '_Manual': 'A Manual (saúde)', '_Mevo': 'A Mevo é',
}
FIM = re.compile(r'^\s*(Cargos|STAKEHOLDERS|–|—)')
# clientes sem texto no documento de clientes: descrição escrita a partir do site da organização (lido em 16/11/2026),
# mais a pauta que o mapeamento usou (CLIENTES_FORA_DA_BASE do e24), porque a classificação não mudou com o texto novo
SITE = {
    'Fundação Lemann': (
        'A Fundação Lemann é uma organização sem fins lucrativos que atua em educação e na formação de lideranças. Em educação, '
        'apoia a formulação de políticas públicas pela aprendizagem na idade adequada, com foco no fim do analfabetismo escolar e '
        'em escolas capazes de engajar os adolescentes, reduzindo desigualdades. Suas frentes são alfabetização (todas as crianças '
        'alfabetizadas ao fim do 2º ano), aprendizagem adequada e inovação e conectividade, com investimento em tecnologia na '
        'educação. Em lideranças, mantém redes, programas de formação e bolsas de estudo. Trabalha com organizações parceiras, '
        'entre elas a MegaEdu, que leva internet às escolas públicas, e iniciativas de apoio a estados e municípios na alfabetização.',
        'fundacaolemann.org.br',
        'alfabetização, aprendizagem, anos finais, professores, gestão escolar e conectividade.'),
    'MegaEdu': (
        'A MegaEdu é uma organização sem fins lucrativos que trabalha para levar internet de alta velocidade a todas as escolas '
        'públicas do Brasil, tratando a conexão como condição básica da educação. Dá apoio técnico a diretores para resolver '
        'problemas de conexão nas escolas, assessora secretarias de educação na elaboração de planos de universalização da internet '
        'para uso pedagógico e apoia gestores federais na criação e no aperfeiçoamento de políticas de conectividade nas escolas. '
        'Defende que a conexão de qualidade chegue a todas as escolas, nas capitais e no interior, com velocidade mínima por aluno '
        'para uso pedagógico, e acompanha também a infraestrutura de wi-fi e de computadores em sala de aula.',
        'megaedu.org.br',
        'conectividade das escolas e FUST.'),
    'Desiderata': (
        'O Instituto Desiderata, fundado em 2003 no Rio de Janeiro, trabalha pelo fortalecimento de políticas públicas de saúde para '
        'crianças e adolescentes. Começou no câncer infantojuvenil e, em 2019, incluiu a obesidade infantojuvenil. A missão é garantir '
        'vida saudável a crianças e adolescentes, articulando atores sociais para fortalecer políticas públicas nessas duas agendas. '
        'Atua com mobilização e articulação entre poder público e sociedade civil, formação de profissionais de saúde, produção de '
        'dados (Panorama da Obesidade em Crianças e Adolescentes) e, desde 2021, incidência no Legislativo. Apoiou a lei municipal do '
        'Rio de Janeiro que proíbe a oferta e a venda de ultraprocessados nas escolas (Lei 7.987/2023) e leis de infância saudável em '
        'Niterói, e promove ambientes alimentares escolares mais saudáveis.',
        'desiderata.org.br',
        'obesidade infantil, ultraprocessados e alimentação escolar.'),
}
COLETIVO_OUTRAS = (
    'Católicas pelo Direito de Decidir é uma ONG feminista formada por mulheres católicas, criada em 1993 e parte de uma rede '
    'presente em 12 países. Defende a autonomia e a liberdade das mulheres e das pessoas sobre sexualidade e reprodução, com base '
    'no direito de decidir e na ideia de que a religião não deve limitar essa autonomia. Atua pela laicidade do Estado nas políticas '
    'públicas, pela igualdade de gênero, inclusive dentro da Igreja, e pelo reconhecimento dos direitos sexuais e reprodutivos como '
    'direitos humanos, com foco em mulheres, jovens, pessoas LGBT e mulheres negras. Tem campanhas críticas à posição da Igreja '
    'Católica sobre o aborto.\n\n'
    'O Coletivo Feminista Sexualidade e Saúde atua em São Paulo desde 1981 e é referência em saúde sexual e reprodutiva na atenção '
    'primária. Trabalha para garantir direitos sexuais e reprodutivos a mulheres, meninas e pessoas que podem gestar, com cuidado em '
    'perspectiva feminista, equidade de gênero, autonomia e educação em saúde. Entre seus projetos está a formação de profissionais '
    'de unidades básicas de saúde para acolher a gravidez não planejada e orientar sobre o acesso ao aborto nos casos previstos em lei.\n\n'
    'Fontes das duas últimas: catolicas.org.br e perfil do Coletivo Feminista na BrazilFoundation (o site mulheres.org.br não abriu em 16/11/2026).')
# defeito de extração do PDF (texto de outro campo caído no meio da frase)
CONSERTOS = [('acompanha de perto a r Serviço de Radiodifusão Comunitária egulação', 'acompanha de perto a regulação')]


def blocos():
    txt = subprocess.run(['pdftotext', '-layout', PDF, '-'], capture_output=True, text=True, check=True).stdout.split('\n')
    pos = {}
    for k, ini in INICIO.items():
        pos[k] = next(i for i, l in enumerate(txt) if l.strip().startswith(ini))
    inicios = sorted(pos.values())
    out = {}
    for k, i in pos.items():
        if k.startswith('_'): continue
        prox = min([j for j in inicios if j > i] + [len(txt)])
        linhas = []
        for l in txt[i:prox]:
            if FIM.match(l): break
            linhas.append(l.strip())
        pars = [re.sub(r'\s+', ' ', p).strip() for p in '\n'.join(linhas).split('\n\n')]
        t = '\n\n'.join(p for p in pars if p)
        for a, b in CONSERTOS: t = t.replace(a, b)
        out[k] = t
    return out


B = blocos()
sh = gspread.service_account(filename='credentials.json').open_by_key(ID)
tab = sh.worksheet('Por cliente').get('I2:N40')
linhas = []
for r in tab:
    r = r + [''] * (6 - len(r))
    cliente, area, pautas, _, fora, subt = r
    if not cliente: continue
    if cliente in B:
        desc = B[cliente]
    elif cliente.startswith('Coletivo'):
        desc = B['Nem Presa Nem Morta'] + '\n\n' + COLETIVO_OUTRAS
    elif cliente in SITE:
        texto, site, pauta = SITE[cliente]
        desc = f'{texto}\n\nFonte: {site} (o cliente não tem descrição no documento de clientes). O mapeamento usou a pauta: {pauta}'
    else:
        sys.exit(f'Cliente sem descrição: {cliente}')
    if '(fora da base de pautas' in pautas:
        pautas = 'Nenhum projeto na base de pautas.'
    else:
        pautas = '\n'.join('• ' + p.strip() for p in pautas.split(';') if p.strip())
    if fora:
        fora_txt = re.sub(r':.*', '', fora)
        pautas += f'\n• {fora_txt} (posição informada fora da base de pautas)'
    subt = '\n'.join('• ' + s.strip() for s in subt.split(';') if s.strip())
    desc = desc.replace(' — ', '; ').replace('—', ', ')  # 24/11: padrão de texto sem travessão
    linhas.append([area, cliente, desc, pautas, subt])
linhas.sort(key=lambda l: (AREA_ORDEM.index(l[0]) if l[0] in AREA_ORDEM else 9, l[1].lower()))

CAB = ['Área', 'Cliente', 'Descrição do cliente', 'Pautas acompanhadas (lado do cliente)', 'Subtemas']
n, m = len(linhas) + 1, len(CAB)
if TITULO in [w.title for w in sh.worksheets()]:
    sh.del_worksheet(sh.worksheet(TITULO))
ws = sh.add_worksheet(title=TITULO, rows=n, cols=m)
ws.update(values=[CAB] + linhas, range_name='A1', value_input_option='RAW')
R = lambda a, b, c0=0, c1=m: {'sheetId': ws.id, 'startRowIndex': a, 'endRowIndex': b, 'startColumnIndex': c0, 'endColumnIndex': c1}


def fmt(r, fundo, texto='#000000', negrito=False, h='LEFT', v='MIDDLE'):
    return {'repeatCell': {'range': r, 'cell': {'userEnteredFormat': {
        'backgroundColor': cor(fundo), 'wrapStrategy': 'WRAP', 'verticalAlignment': v, 'horizontalAlignment': h,
        'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 10, 'bold': negrito, 'foregroundColor': cor(texto)}}},
        'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,horizontalAlignment,textFormat)'}}


fina = {'style': 'SOLID', 'color': cor(BORDA)}
LARG = [80, 150, 620, 330, 330]
reqs = [{'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1, 'frozenColumnCount': 2}},
                                   'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}},
        fmt(R(0, 1), MARINHO, '#FFFFFF', True, 'CENTER'), fmt(R(1, n), GELO), fmt(R(1, n, 1, 2), GELO, MARINHO, True),
        {'updateBorders': {'range': R(0, n), 'innerHorizontal': fina, 'innerVertical': fina, 'bottom': fina, 'right': fina}},
        {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                       'properties': {'pixelSize': 36}, 'fields': 'pixelSize'}},
        {'setBasicFilter': {'filter': {'range': R(0, n)}}}]
reqs += [{'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                        'properties': {'pixelSize': w}, 'fields': 'pixelSize'}} for i, w in enumerate(LARG)]
sh.batch_update({'requests': reqs})

from ordem_abas import ordenar
print(CASA, len(linhas), 'clientes;', ordenar(sh))
