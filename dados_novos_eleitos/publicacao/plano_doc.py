from ids import valor
import sys, time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

T = valor('DOC_ID_PLANO_MAPEAMENTO')
cr = Credentials.from_service_account_file(sys.argv[1], scopes=['https://www.googleapis.com/auth/documents'])
docs = build('docs', 'v1', credentials=cr)
ESCURO = {'red': 0.125, 'green': 0.129, 'blue': 0.141}
TEXTO = {'red': 0.235, 'green': 0.251, 'blue': 0.263}
MARINHO = {'red': 0x19 / 255, 'green': 0x2D / 255, 'blue': 0x4E / 255}
GELO = {'red': 0xF4 / 255, 'green': 0xF3 / 255, 'blue': 0xEF / 255}
BRANCO = {'red': 1, 'green': 1, 'blue': 1}

B = [
 ('title', 'Plano de ação: mapeamento dos parlamentares da 58ª Legislatura'),
 ('p', 'Versão de 13/10/2026. Prazo da base: 30/11/2026.'),

 ('h', '1. Objetivo'),
 ('p', 'Montar uma base única, parlamentar por parlamentar, com a trajetória, a atuação e as pautas principais de cada eleito para a legislatura que começa em fevereiro de 2027. A base parte do geral (tudo o que há de registro sobre cada nome) e depois afunila: primeiro por tema, como saúde, educação e esporte, e em seguida por subtema, como imposto seletivo, tabaco e álcool dentro de saúde.'),
 ('p', 'A mesma base atende a mais de um contrato. Cada cliente recebe o recorte dos seus temas, sem refazer a coleta.'),

 ('h', '2. Contratos que dependem da base'),
 ('item', 'ACT Promoção da Saúde:', ' 513 deputados federais e 81 senadores, classificados em cinco temáticas (tabaco, álcool, alimentação, inatividade física e poluição do ar) como Promotor / Aliado, Neutro / Dialogável ou Detrator / Opositor, com índice de proximidade geral e nível de influência de 1 a 4. Quem não tiver conexão com nenhuma das cinco sai como "sem conexão temática". A entrega é um dashboard, com cronograma próprio.'),
 ('item', 'Mapeamento de São Paulo (justiça reprodutiva e campanha Criança Não É Mãe):', ' eleitos por SP para a ALESP, a Câmara dos Deputados e o Senado, com ficha, votações correlatas, histórico de atuação e classificação entre Promotores, Neutros e Detratores. Entrega em planilha até 18/12/2026.'),
 ('item', 'Demais clientes:', ' os temas gerais (educação, saúde, esporte e os outros) ficam prontos na base para qualquer recorte novo.'),

 ('h', '3. Universo'),
 ('table', [['Casa', 'Grupo', 'Nomes', 'O que existe de registro'],
            ['Senado', 'Eleitos em 2022, mandato até 2031', '27', 'Mandato em curso no Senado'],
            ['Senado', 'Reeleitos em 2026', '14', 'Mandato em curso no Senado'],
            ['Senado', 'Novos', '40', 'Depende da origem: Câmara, assembleia, Executivo ou nenhum mandato'],
            ['Câmara', 'Reeleitos', '301', 'Mandato em curso na Câmara'],
            ['Câmara', 'Voltando', '29', 'Mandato anterior na Câmara'],
            ['Câmara', 'Novos', '183', 'Depende da origem'],
            ['ALESP', 'Eleitos em 2026', '94', 'Só para o contrato de São Paulo']], [75, 155, 55, 201]),
 ('p', 'A lista de nomes é a da planilha Congresso Eleito (2026), conferida com o resultado oficial do TSE em 08/10/2026. Ela muda em três momentos: depois do 2º turno de 25/10, na posse dos suplentes de quem foi eleito governador e quando parlamentares eleitos forem nomeados para ministérios e secretarias em 2027.'),

 ('h', '4. Método: do geral ao subtema'),
 ('item', 'Camada 1, perfil geral.', ' Tudo o que as fontes oficiais registram sobre o parlamentar, sem recorte: ficha (nascimento, profissão, formação, mandatos, suplentes), comissões e frentes, autoria, relatoria, emendas, votações nominais e discursos. As pautas principais saem por contagem, pelo tema que a própria Casa atribui a cada matéria.'),
 ('item', 'Camada 2, tema.', ' As tabelas da camada 1 filtradas por tema (saúde, educação, esporte e os demais). Para cada parlamentar: quantas matérias propôs e relatou no tema, em que comissões do tema atua e como votou nas votações nominais do tema.'),
 ('item', 'Camada 3, subtema.', ' Cada subtema ganha uma ficha fechada antes de olhar os nomes: termos de busca, votações de referência e proposições centrais. A busca localiza os registros de cada parlamentar e a equipe lê um a um antes de classificar.'),
 ('p', 'Cada registro que sustenta uma classificação fica numa tabela de evidências, uma linha por parlamentar, subtema e registro, com o link da fonte. A classificação do parlamentar é derivada dessa tabela.'),

 ('h', '5. Fontes por trajetória'),
 ('table', [['Trajetória', 'Fontes'],
            ['Senador em exercício ou ex-senador', 'Dados abertos do Senado: autoria, relatoria, emendas, votações nominais, discursos, comissões e frentes'],
            ['Deputado federal em exercício ou ex-deputado', 'Dados abertos da Câmara: autoria e tema oficial, relatoria, votações nominais, discursos, comissões e frentes'],
            ['Deputado estadual ou vereador', 'Dados abertos da assembleia ou da câmara municipal, Inteligov e o levantamento de deputados estaduais já existente'],
            ['Governador, prefeito, ministro ou secretário', 'Plano de governo registrado no TSE, atos e programas da gestão, entrevistas e imprensa'],
            ['Sem mandato nem cargo anterior', 'Biografia, redes sociais declaradas no registro da candidatura, imprensa, vínculos institucionais e produção técnica'],
            ['Todos', 'Redes sociais e imprensa, com link']], [160, 326]),

 ('h', '6. Ordem de execução'),
 ('p', 'Primeiro o Senado, nesta ordem: eleitos em 2022, reeleitos e novos. Depois a Câmara, na mesma lógica: reeleitos, voltando e novos. A ALESP entra por último e só para o contrato de São Paulo. Em cada grupo, a camada geral vem antes de qualquer tema.'),

 ('h', '7. Cronograma'),
 ('table', [['Período', 'Etapa', 'Resultado'],
            ['13 a 16/10', 'Senado, eleitos em 2022 (27): perfil geral', 'Resumo e tabelas dos 27; formato da ficha validado'],
            ['19 a 23/10', 'Senado, reeleitos (14) e novos (40): perfil geral. Mandato anterior na Câmara e cargos não eletivos dos 81', 'Senado completo na camada geral'],
            ['26 a 30/10', 'Câmara, reeleitos (301) e voltando (29): perfil geral. Atualização da lista com o 2º turno de 25/10', '330 deputados na camada geral'],
            ['03 a 06/11', 'Câmara, novos (183): coleta por trajetória. Validação da régua de classificação e das fichas de subtema', '594 nomes na camada geral; régua fechada'],
            ['09 a 13/11', 'Tema e subtema: Senado (81)', 'Senado classificado'],
            ['16 a 19/11', 'Tema e subtema: Câmara, reeleitos e voltando (330)', '411 nomes classificados'],
            ['23 a 27/11', 'Tema e subtema: Câmara, novos (183). Nível de influência e índice de proximidade', '594 nomes classificados'],
            ['30/11', 'Revisão final e fechamento', 'Base pronta'],
            ['01 a 18/12', 'ALESP (94) e recorte de São Paulo', 'Planilha do contrato de São Paulo']], [75, 250, 161]),
 ('p', 'Os dias 02/11 e 20/11 são feriados nacionais e já estão fora das semanas acima.'),

 ('h', '8. Régua de classificação (a validar)'),
 ('item', 'Categorias:', ' Promotor / Aliado, Neutro / Dialogável, Detrator / Opositor e "sem conexão temática", por subtema.'),
 ('item', 'O que classifica:', ' voto nominal de mérito, autoria principal, parecer de relatoria, emenda e declaração pública com link.'),
 ('item', 'O que entra só como observação:', ' assinatura de apoiamento em PEC ou requerimento, coautoria coletiva e participação em frente parlamentar.'),
 ('item', 'Registros nos dois sentidos:', ' o parlamentar fica como Neutro / Dialogável e os dois registros aparecem na ficha.'),
 ('item', 'Sem registro:', ' "sem conexão temática". Informação sem fonte fica marcada como "a conferir" e não classifica.'),
 ('item', 'Nível de influência (1 a 4):', ' Mesa Diretora, liderança de partido, bloco ou governo, presidência de comissão e relatorias. Critérios e pesos a fechar na validação.'),
 ('item', 'Índice de proximidade geral:', ' fórmula a fechar na validação, a partir das classificações por subtema.'),

 ('h', '9. Entregas'),
 ('item', 'Planilha da base (até 30/11):', ' uma aba de resumo por Casa, com as pautas principais e a classificação por subtema de cada parlamentar; as tabelas completas de autoria, relatoria, emendas, votações, discursos e colegiados; a tabela de evidências; as fichas de subtema.'),
 ('item', 'Recorte da ACT:', ' as cinco temáticas, o índice de proximidade e o nível de influência, no formato que alimenta o dashboard.'),
 ('item', 'Recorte de São Paulo (até 18/12):', ' ALESP, Câmara e Senado por SP, com justiça reprodutiva e Criança Não É Mãe.'),

 ('h', '10. Situação em 13/10/2026'),
 ('item', 'Lista de eleitos:', ' pronta e conferida com o TSE (513 deputados, 54 senadores eleitos e 27 com mandato em curso).'),
 ('item', 'Origem e trajetória eleitoral:', ' prontas para Senado e Câmara desde o pré-mapeamento de setembro (mandatos desde 2006 e de onde cada nome vem).'),
 ('item', 'Coleta do Senado:', ' coletor escrito e rodando para os 27 eleitos em 2022. Fichas, colegiados e autoria já baixados; relatorias, emendas, votações e discursos em andamento.'),

 ('h', '11. Pontos de atenção'),
 ('item', 'Novos sem histórico legislativo:', ' parte dos 183 novos da Câmara e dos 40 novos do Senado não tem voto nem proposição. Para eles a classificação depende de imprensa, redes e plano de governo, e muitos devem ficar em "sem conexão temática".'),
 ('item', 'Votação secreta:', ' o Senado informa só que o parlamentar votou. Indicações de autoridades não entram na leitura de posição.'),
 ('item', 'Origem estadual e municipal:', ' não há base única. Depende do Inteligov e dos dados abertos de cada assembleia.'),
 ('item', 'Volume de leitura:', ' a busca é automática, a classificação é lida registro a registro. As três semanas de novembro dependem de quantas pessoas leem.'),
 ('item', 'Cadeiras do MA e de SC no Senado:', ' a lista de senadores em exercício do Senado traz Ana Paula Lobato e Jorge Seif; a planilha Congresso Eleito traz Lourdinha Pereira e Hermes Klann. A conferir.'),

 ('h', '12. Decisões pendentes'),
 ('item', '1.', ' Lista fechada de subtemas por cliente, além das cinco temáticas da ACT e das duas pautas de São Paulo.'),
 ('item', '2.', ' Quem valida a régua de classificação e em que data (previsto para a semana de 03 a 06/11).'),
 ('item', '3.', ' Quem lê e classifica em novembro, além da coleta automática.'),
 ('item', '4.', ' Se a ALESP começa só em dezembro ou entra em paralelo em novembro.'),
 ('item', '5.', ' Onde a base fica: abas novas na planilha Congresso Eleito ou planilha própria.'),
]

def end():
    d = docs.documents().get(documentId=T).execute()
    return d, d['body']['content'][-1]['endIndex']

def ts(a, b, **k):
    f = {'weightedFontFamily': {'fontFamily': 'Montserrat', 'weight': k.get('w', 300)},
         'fontSize': {'magnitude': k.get('size', 10.5), 'unit': 'PT'},
         'foregroundColor': {'color': {'rgbColor': k.get('cor', TEXTO)}}, 'bold': k.get('bold', False)}
    return {'updateTextStyle': {'range': {'startIndex': a, 'endIndex': b}, 'textStyle': f,
                                'fields': 'weightedFontFamily,fontSize,foregroundColor,bold'}}

def ps(a, b, **st):
    return {'updateParagraphStyle': {'range': {'startIndex': a, 'endIndex': b}, 'paragraphStyle': st, 'fields': ','.join(st)}}

pt = lambda v: {'magnitude': v, 'unit': 'PT'}

def run(reqs):
    for i in range(0, len(reqs), 150):
        docs.documents().batchUpdate(documentId=T, body={'requests': reqs[i:i + 150]}).execute()
        time.sleep(1.2)

REC = -35.43307086614173  # recuo do texto no modelo Eixo

def textos(blocos, apos_tabela=False):
    d, fim = end()
    pos = fim - 1
    texto = ''.join((b[1] + (b[2] if b[0] == 'item' else '')) + '\n' for b in blocos)
    reqs = [{'insertText': {'location': {'index': pos}, 'text': texto}}]
    for b in blocos:
        t = b[1] + (b[2] if b[0] == 'item' else '')
        a, z = pos, pos + len(t) + 1
        if b[0] == 'title':
            reqs += [ps(a, z, namedStyleType='TITLE', alignment='START', spaceBelow=pt(8), indentStart=pt(REC), indentFirstLine=pt(REC)),
                     ts(a, z - 1, w=400, size=14, cor=ESCURO, bold=True)]
        elif b[0] == 'h':
            reqs += [ps(a, z, namedStyleType='NORMAL_TEXT', alignment='START', spaceAbove=pt(12), spaceBelow=pt(6),
                        lineSpacing=133.33, indentStart=pt(REC), indentFirstLine=pt(REC), keepWithNext=True),
                     ts(a, z - 1, w=400, size=11, cor=ESCURO, bold=True)]
        elif b[0] == 'p':
            reqs += [ps(a, z, namedStyleType='NORMAL_TEXT', alignment='JUSTIFIED', spaceAbove=pt(8 if b is blocos[0] and apos_tabela else 0), spaceBelow=pt(7),
                        lineSpacing=133.33, indentStart=pt(REC), indentFirstLine=pt(REC), keepWithNext=False),
                     ts(a, z - 1)]
        else:
            reqs += [ps(a, z, namedStyleType='NORMAL_TEXT', alignment='JUSTIFIED', spaceAbove=pt(2), spaceBelow=pt(4),
                        lineSpacing=133.33, indentStart=pt(REC + 24), indentFirstLine=pt(REC + 6), keepWithNext=False),
                     ts(a, z - 1), ts(a, a + len(b[1]), w=400, bold=True)]
        pos = z
    run(reqs)

def tabela(linhas, larguras):
    run([{'insertTable': {'rows': len(linhas), 'columns': len(linhas[0]), 'endOfSegmentLocation': {}}}])
    d, _ = end()
    el = [e for e in d['body']['content'] if 'table' in e][-1]
    ini = el['startIndex']
    cels = [(i, j, c['content'][0]['startIndex']) for i, r in enumerate(el['table']['tableRows'])
            for j, c in enumerate(r['tableCells'])]
    run([{'insertText': {'location': {'index': s}, 'text': linhas[i][j]}} for i, j, s in reversed(cels)])
    d, _ = end()
    el = [e for e in d['body']['content'] if 'table' in e][-1]
    reqs = []
    for i, r in enumerate(el['table']['tableRows']):
        for c in r['tableCells']:
            a, z = c['content'][0]['startIndex'], c['content'][-1]['endIndex']
            reqs.append(ps(a, z, namedStyleType='NORMAL_TEXT', alignment='START', spaceAbove=pt(0), spaceBelow=pt(0),
                           lineSpacing=115, indentStart=pt(0), indentFirstLine=pt(0)))
            if z - 1 > a:
                reqs.append(ts(a, z - 1, size=9, w=400 if i == 0 else 300, bold=i == 0, cor=BRANCO if i == 0 else TEXTO))
    loc = {'tableStartLocation': {'index': ini}}
    borda = {'color': {'color': {'rgbColor': BRANCO}}, 'width': pt(1), 'dashStyle': 'SOLID'}
    reqs.append({'updateTableCellStyle': {'tableRange': {'tableCellLocation': {**loc, 'rowIndex': 0, 'columnIndex': 0},
                 'rowSpan': len(linhas), 'columnSpan': len(linhas[0])},
                 'tableCellStyle': {'backgroundColor': {'color': {'rgbColor': GELO}}, 'borderTop': borda, 'borderBottom': borda,
                                    'borderLeft': borda, 'borderRight': borda, 'paddingTop': pt(4), 'paddingBottom': pt(4),
                                    'paddingLeft': pt(6), 'paddingRight': pt(6)},
                 'fields': 'backgroundColor,borderTop,borderBottom,borderLeft,borderRight,paddingTop,paddingBottom,paddingLeft,paddingRight'}})
    reqs.append({'updateTableCellStyle': {'tableRange': {'tableCellLocation': {**loc, 'rowIndex': 0, 'columnIndex': 0},
                 'rowSpan': 1, 'columnSpan': len(linhas[0])},
                 'tableCellStyle': {'backgroundColor': {'color': {'rgbColor': MARINHO}}}, 'fields': 'backgroundColor'}})
    for j, w in enumerate(larguras):
        reqs.append({'updateTableColumnProperties': {'tableStartLocation': {'index': ini}, 'columnIndices': [j],
                     'tableColumnProperties': {'widthType': 'FIXED_WIDTH', 'width': pt(w)}, 'fields': 'widthType,width'}})
    # o parágrafo vazio que o Docs cria antes da tabela
    reqs.append(ts(ini - 1, ini, size=4))
    reqs.append(ps(ini - 1, ini, spaceAbove=pt(0), spaceBelow=pt(0), lineSpacing=100, indentStart=pt(0), indentFirstLine=pt(0)))
    run(reqs)

d, fim = end()
if fim > 2:  # sobra da tentativa anterior deste mesmo script
    assert 'Plano de ação: mapeamento' in str(d['body']['content'][1]), 'conteúdo que não é deste script'
    run([{'deleteContentRange': {'range': {'startIndex': 1, 'endIndex': fim - 1}}}])
lote, depois = [], False
for b in B:
    if b[0] == 'table':
        if lote:
            textos(lote, depois); lote = []
        tabela(b[1], b[2]); depois = True
    else:
        lote.append(b)
if lote:
    textos(lote, depois)
d, fim = end()
# parágrafo vazio que sobra no fim e o que fica depois de cada tabela
print('fim', fim)
