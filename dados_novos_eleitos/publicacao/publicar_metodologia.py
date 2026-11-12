"""Aba Metodologia da planilha do mapeamento da 58ª Legislatura, no padrão Eixo."""
from ids import valor
import gspread

ID = valor('SPREADSHEET_ID_58_SENADO')
TITULO = 'Metodologia'
# linha com um só texto é título de bloco (mesclada em A:C)
LINHAS = [
    ['', 'Metodologia'],
    ['COMO USAR A PLANILHA'],
    ['Abas', ['Para consultar: Por cliente e Por parlamentar', 'Bases: Parlamentares e Votações-chave, para comparar os senadores lado a lado', 'Ocultas: Perfil por tema e Clientes, que alimentam as abas de consulta (Exibir > Abas ocultas, se precisar filtrar todos de uma vez)', 'Em revisão pelos coordenadores: Subtemas por cliente e Lado dos projetos (oculta quando não há projeto em revisão)'], ''],
    ['Por cliente', ['Escolha o cliente na lista do alto', 'Aparecem os subtemas do cliente, as pautas, a contagem por classificação e os senadores com registro'], ''],
    ['Por parlamentar', ['Escolha o senador na lista do alto', 'Aparecem a ficha, a atuação em cada tema e subtema e a classificação em cada cliente'], ''],
    ['Parlamentares', ['Uma linha por senador', 'Identidade, posição no Senado, relevância, atuação nos quatro temas e destaques', 'Subtemas em grupos de colunas que abrem', 'Ficha à direita'], ''],
    ['Perfil por tema', ['Uma linha por senador e tema', 'Atuação, subtemas, o que fez, o que defende e como votou', 'Pedidos de audiência e urgência, comissão e frentes do tema'], ''],
    ['Clientes', ['Uma linha por cliente e senador, com filtro em todas as colunas'], ''],
    ['Votações-chave', ['O voto de cada senador nas votações escolhidas', 'O cabeçalho diz o que o Sim fazia', 'Traço quando o senador não estava na Casa'], ''],
    ['Cadeira', ['Mandato até 2031 (eleitos em 2022)', 'Reeleito em 2026', 'Eleito em 2026', 'A Câmara entra depois'], 'Congresso Eleito (2026), conferido com o TSE em 08/10/2026'],
    ['O QUE FOI PUXADO'],
    ['Projetos de autoria', ['Projetos, PECs, requerimentos e demais proposições de que o senador é primeiro autor ou coautor', 'Senado: cerca de 27 mil registros', 'Câmara, para quem foi deputado: cerca de 50 mil', 'Na classificação valem os de primeiro autor'], 'APIs do Senado e da Câmara'],
    ['Relatorias', ['Senado: cerca de 11.500; Câmara: cerca de 2.200', 'Conta só com parecer dado ou matéria ainda com o senador', 'Não contam redistribuição, troca na comissão, relator ad hoc e fim de legislatura', 'Na Câmara, a fonte só informa o relator do último andamento'], 'APIs do Senado e da Câmara'],
    ['Inteiro teor', ['215 projetos lidos no texto e na justificação', 'Usado quando a ementa não mostrava o lado em relação ao cliente'], 'Senado e Câmara (texto dos projetos)'],
    ['Emendas', ['Senado: cerca de 17.500; Câmara: cerca de 16.600', 'Lidas no texto: Imposto Seletivo (PLP 68/2024) e Código Civil'], 'APIs do Senado e da Câmara'],
    ['Discursos', ['Pronunciamentos em plenário, com resumo oficial e íntegra', 'Senado: cerca de 11.500; Câmara: cerca de 12.500', 'Entram os feitos desde fevereiro de 2023'], 'Pronunciamentos do Senado; discursos da Câmara'],
    ['Notas taquigráficas', ['Transcrição de tudo o que foi dito nas reuniões de comissão, que não aparece nos discursos', 'Senado: 5.127 reuniões desde 2019 (115 mil falas); entram CAS, CE, CEsp, CDH e CPI das Bets', 'Câmara: 1.671 reuniões desde 2023 das comissões de Saúde, Educação, Previdência e Família e Direitos Humanos', 'Falas com o tema lidas uma a uma'], 'Notas taquigráficas do Senado e da Câmara'],
    ['Votações nominais', ['Senado: cerca de 35 mil votos em plenário e 6.600 em comissões', 'Câmara, para quem foi deputado: cerca de 67 mil votos em plenário', 'Junto vai a orientação da liderança do partido, que mede o partido e não o senador'], 'Votações do Senado e da Câmara; orientação de bancada do Senado'],
    ['Requerimentos', ['Pedidos de audiência pública (autoria ou coautoria, em qualquer comissão)', 'Requerimentos de urgência assinados', 'Desde 2023, quando o objeto é do tema'], 'API do Senado'],
    ['Comissões e frentes', ['Comissões de hoje, com o papel (presidente, titular, suplente)', 'Frentes parlamentares do tema'], 'API do Senado'],
    ['Assembleias e câmaras municipais', ['4.383 proposições de André do Prado, Capitão Contar, Carlos Bolsonaro, Chicão e Samanda de Lula, com a área de cada uma; os 647 projetos lidos um a um nos subtemas', 'Para os outros 13 senadores com mandato de deputado estadual, vereador ou prefeito no TSE, busca nos portais de cada Casa em todos os mandatos', 'Sem registro quando a Casa não tem consulta do período ou exige captcha: Assembleias de AL, MA, MS e SE (antes de 2010), câmaras de Macapá, Dois Irmãos e Londrina', 'Prefeitura: leis e programas da gestão aparecem como registro, sem contar para classificar', 'Doença, hospital, medicamento, urgência e vigilância contam nos subtemas, não para cliente'], 'Inteligov; SAPL e portais das Casas'],
    ['Imprensa', ['365 matérias de 19 veículos', 'Para quem não tinha mandato no Congresso: o que a cobertura da eleição registra, com veículo e data'], 'Agência Senado, Rádio Senado e imprensa'],
    ['Trajetória e profissão', ['Cargos anteriores com o período', 'Ocupação declarada na candidatura mais recente'], 'Wikipédia e TSE (candidaturas desde 2006)'],
    ['Influência', ['Os 100 mais influentes do Congresso em 2026'], 'DIAP, Cabeças do Congresso 2026'],
    ['Pautas dos clientes', ['Projetos marcados pelos coordenadores como Promotor ou Detrator para cada cliente', 'Descrição de cada cliente', 'Posições do relatório de apoiadores e detratores do Infinis (05/10/2026)'], 'Base de pautas por cliente; Monitor Legislativo e DOU; relatório Infinis'],
    ['COMO SE CLASSIFICA'],
    ['Posição no Senado', ['Pelo governismo: votações nominais do plenário desde fevereiro de 2023 com orientação Sim ou Não do Governo (60 votações)', 'Ficam fora as votações consensuais, em que mais de 90% dos votos foram iguais (67 votações)', 'Governismo: votos iguais à orientação sobre votos dados (Sim, Não, abstenção e obstrução); ausência não conta', 'Base: 85% ou mais de votos iguais à orientação', 'Independente: entre 65% e 85%', 'Oposição: 65% ou menos', 'Menos de 20 votações: sem posição', 'Cortes de 85% e 65% definidos pela Eixo; ao lado da posição vão o percentual e o número de votações'], 'Votações nominais do Senado'],
    ['Relevância política (1 a 4)', ['4: foi ou é governador, ministro, presidente do Senado ou da Câmara, presidente ou vice da República, ou é hoje líder do governo, da oposição, da maioria ou da minoria', '3: prefeito, vice-governador, secretário de estado, presidente de Assembleia, Mesa do Senado, líder de partido ou bloco hoje ou presidente de comissão permanente hoje', '2: outro mandato eletivo ou cargo anterior', '1: primeiro mandato', 'Cabeça do Congresso no DIAP 2026 tem no mínimo 3', 'Vale o cargo mais alto, indicado na coluna ao lado'], 'Wikipédia e TSE; API do Senado; DIAP'],
    ['Temas', ['Saúde, educação, esporte e primeira infância', 'Os três primeiros pela classificação de assunto de cada Casa', 'Primeira infância lida projeto a projeto entre os que tratam de crianças: gestação, parto, recém-nascido, creche, educação infantil, licenças parentais, vacinação de bebês e orçamento'], ''],
    ['Atuação no tema', ['Olha quatro coisas: comissão do tema hoje (presidir conta mais), projetos do tema como primeiro autor (5 ou mais; 15 ou mais conta mais), 2 ou mais relatorias com parecer e projeto que virou lei', 'Não atua: nenhuma', 'Pouco: uma ou duas', 'Atua: três ou quatro', 'Referência: todas, com volume alto', 'Os números estão na nota da célula', 'Não diz se é a favor ou contra o cliente'], ''],
    ['Subtemas', ['Saúde (21): atenção primária à saúde; hospitais e alta complexidade no SUS; urgência e emergência em saúde; saúde mental, drogas e apostas; tabaco e álcool; alimentação saudável, ultraprocessados e agrotóxicos; violência contra crianças e adolescentes; violência contra a mulher; casamento infantil e uniões precoces; planos de saúde; medicamentos e vacinas; doenças e condições de saúde específicas; aborto legal: acesso e garantia; restrições ao aborto legal e criminalização; vida desde a concepção e direitos do nascituro; gravidez infantil e aborto em meninas; planejamento reprodutivo e contracepção; saúde da mulher, gestação e parto; profissionais e formação em saúde; financiamento e gestão do SUS; vigilância e emergências sanitárias', 'Educação (14): educação em tempo integral; alfabetização; recomposição das aprendizagens e avaliação educacional; ensino médio e educação profissional e tecnológica; profissionais da educação; financiamento da educação; equidade na educação e educação inclusiva; conectividade e tecnologia nas escolas; gestão e governança da educação; alimentação, transporte e segurança escolar; infraestrutura escolar; adaptação e emergência climática nas escolas; currículo e conteúdos escolares; educação domiciliar, escola sem partido e escolas cívico-militares', 'Esporte (5): financiamento e incentivo ao esporte; esporte educacional, comunitário e inclusão pelo esporte; atletas e esporte de alto rendimento; gestão, regulação e integridade do esporte; apostas esportivas', 'Primeira infância (5): educação infantil e creche; saúde materno-infantil; parentalidade e apoio às famílias; proteção e segurança da criança na primeira infância; governança e orçamento da primeira infância'], ''],
    ['Leitura dos subtemas', ['Cada projeto de primeira autoria ou relatoria com parecer foi lido no texto integral (Câmara e Senado) e encaixado em um ou dois subtemas: 1.968 projetos', 'Projeto cujo texto integral não abriu não conta: 5 projetos', 'Violência, que a Casa classifica em outros temas, e projetos que só batiam com palavra-chave de cliente: 855 projetos lidos no texto integral, 481 entraram', 'Data comemorativa, semana ou mês temático, honraria, nome de lei, porte de arma ou defesa pessoal para a mulher e prisão perpétua ficam fora', 'Projeto com castração química fica fora, mesmo quando também aumenta pena', 'Pena maior para falsa acusação de crime sexual conta em violência contra a mulher e, no lado do cliente, contra a pauta de enfrentamento da violência', 'Crime contra a dignidade sexual sem vítima especificada conta em violência contra a mulher; com vítima vulnerável, também em violência contra crianças e adolescentes', 'Violência sem recorte de mulher ou criança, maus-tratos a animais, pessoa idosa, tráfico de pessoas e pessoas desaparecidas não têm coluna', 'Palavras dos clientes que não caíam em nenhuma leitura: 453 projetos lidos (394 do IDEC), 38 entraram (agrotóxicos, segurança alimentar, patentes de medicamento, tabaco, saúde mental online, esporte)', 'Licença-maternidade, licença-paternidade e salário-maternidade contam em parentalidade e apoio às famílias; amamentação e exames do recém-nascido, em saúde materno-infantil', 'Agressão a profissional de saúde ou de educação no exercício da função conta em profissionais do tema', 'O que entra e o que fica fora de cada subtema está na aba Definição dos subtemas', 'Conferência: 1 em cada 10 projetos lidos (298) foi relido sem ver a classificação, só pela definição; 84% bateram em pelo menos um subtema e os 14 erros achados foram corrigidos, junto com os casos iguais na base', 'Educação classificada em outros temas: 170 projetos lidos, 42 entraram (royalties do petróleo, merenda, transporte e segurança escolar, agressão a professor, entre outros)', 'Direitos reprodutivos classificados em outros temas (Constituição, Código Penal, Conanda, FGTS): 128 projetos lidos, 19 entraram', 'Portarias e notas técnicas do Ministério da Saúde sustadas por decreto legislativo: lidas pelo texto, não só pela ementa', 'Ensino superior e assuntos fora das pautas dos clientes não têm coluna'], 'Texto integral dos projetos (Câmara e Senado)'],
    ['Nível no subtema', ['Não atua: nenhum projeto', 'Pouco: 1', 'Atua: 2 a 4', 'Referência: 5 ou mais', 'Emendas lidas pelo texto também contam; as do senador a um mesmo projeto contam como um', 'A nota da célula lista os projetos', 'Não diz se é a favor ou contra o cliente'], ''],
    ['Subtemas dos clientes', ['Cada cliente tem os subtemas que acompanha, tirados da base de pautas e das palavras monitoradas no DOU', 'Atuação nos subtemas do cliente: quanto o senador atua só neles', 'Sem subtema próprio: trânsito sem álcool (Vital; álcool ao volante conta em Tabaco e álcool), consumidor, crédito, telecomunicações, dados e energia (IDEC), assistência social (Infinis) e maioridade penal (UNFPA)'], 'Base de pautas; Monitor do DOU'],
    ['Peso dos sinais', ['Fortes: relatoria com parecer, primeira autoria (também em Assembleia ou câmara municipal) e voto nominal, inclusive em destaque', 'Médios: coautoria e pedido de audiência pública cujo objetivo mostra o sentido', 'Fracos: requerimento de urgência (pedido ou voto), discurso, fala em comissão, frente parlamentar, imprensa, campanha e plano de governo', 'Cada linha de evidência é um sinal; projeto reapresentado ou com o mesmo objeto conta uma vez', 'Voto contra a orientação do próprio partido é marcado na evidência', 'Assinatura de PEC aparece só como evidência', 'Autoria cujo sentido nem a ementa nem o inteiro teor mostram não conta'], ''],
    ['Classificação', ['Promotor, Neutro ou Detrator, só por cliente e subtema', 'Promotor ou Detrator: dois sinais fortes no mesmo sentido, ou um forte confirmado por um médio', 'Fortes nos dois sentidos: diferença de dois ou mais decide; diferença de um decide só com sinal médio no mesmo sentido; senão, Neutro', 'Sinal fraco sozinho nunca classifica', 'Abaixo disso: Um sinal a favor, Um sinal contra ou Sinais opostos', 'Registro sem sinal: só registro que não conta, como assinatura de PEC ou relatoria com parecer não lido', 'Sem registro: senador sem nenhum registro no tema do cliente; não aparece na lista', 'Nos grandes temas não há classificação'], ''],
    ['CLIENTES'],
    ['O que conta', ['Projetos marcados como Promotor ou Detrator na base de pautas', 'Marcação em Dúvida ou em branco: conta no sentido da posição do cliente na pauta', 'Pauta "Depende do texto" e projeto marcado Neutro: ficam fora', 'Contam autoria, coautoria e voto no próprio projeto ou no parecer, quando a votação dividiu o plenário', 'Voto em emenda ou requerimento: só quando o que o Sim fazia foi lido no texto', 'Relatoria: aparece como evidência até o parecer ser lido'], 'Base de pautas por cliente'],
    ['Projetos fora da base de pautas', ['Entram pela descrição de cada cliente, só quando o sentido aparece no texto (ementa conferida com o inteiro teor)', 'Também conta o voto no projeto inteiro ou no parecer', 'Projeto de primeira autoria ou relatoria com parecer lido no texto integral, em subtema do cliente, também conta, com o lado tirado da descrição do cliente', 'Quando o cliente tem posição pública sobre o projeto (carta, nota ou fala de dirigente), o lado vem dela e o item não precisa de checagem', 'Projeto que muda a classificação de algum parlamentar foi lido no texto, com um lado por cliente; a evidência traz "lado lido no texto", e o que não dá lado para o cliente vai para Não conta', 'Na aba Lado dos projetos ficam só os projetos que mudam a classificação e cujo lado ficou ambíguo no texto, com a pergunta no Comentário; só essas evidências trazem "precisa de checagem dos coordenadores"', 'MegaEdu, Fundação Itaú e Desiderata, que não estão na base de pautas, entram só assim', 'Violência contra crianças e adolescentes, contra a mulher e casamento infantil: os projetos lidos nesses subtemas contam para os clientes que os acompanham (Cactus, FMCSV, Infinis, UNFPA e Vital Strategies), no sentido da ementa', 'FMCSV: violência contra crianças e adolescentes conta; aborto e direitos do nascituro aparecem na atuação, sem lado, porque a FMCSV não tem posição pública no tema; resoluções do Conanda sobre aborto legal de meninas e apostas por menores não contam para ela', 'Ficam fora porte de arma ou de defesa pessoal para a mulher, castração química, prisão perpétua, datas comemorativas e nome de lei', 'UNFPA: projetos de aborto e de direitos do nascituro não contam, porque o programa do UNFPA no Brasil (2024-2028) não atua no tema; contam para o Coletivo', 'Coletivo: contam aborto, contracepção e autonomia reprodutiva; violência contra a mulher conta para o UNFPA, não para o Coletivo', 'Fundação Lemann: benefício a professor que não mexe em carreira, piso nem formação (merenda, passe livre, vacina, prioridade na saúde, meia-entrada, isenção de imposto, adicional, gratificação, carteira de docente) conta a favor','Declaração em comissão aparece como evidência, sem contar'], 'Descrição dos clientes (Monitor Legislativo e DOU); relatório Infinis (05/10/2026)'],
]
# lista vira uma linha por item, com marcador
# duas colunas, como o Leia-me: rótulo em A e tópicos em B, um "·" por linha; a fonte é o último tópico
LINHAS = [l if len(l) < 3 else [l[0], '\n'.join('· ' + x for x in (l[1] if isinstance(l[1], list) else [l[1]]) + ([f'Fonte: {l[2]}'] if l[2] else []))]
          for l in LINHAS]
BLOCOS = [i for i, l in enumerate(LINHAS) if len(l) == 1]
LINHAS = [l + [''] * (2 - len(l)) for l in LINHAS]
# logo eixo gov branco já embutido na célula A1 do Leia-me do IEPS (sem =IMAGE, que pede "Permitir acesso")
LOGO_DE = (valor('SPREADSHEET_ID_IEPS_PL7419'), 'Leia-me')

gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key(ID)
try:
    ws = sh.worksheet(TITULO); ws.clear()
    sh.batch_update({'requests': [{'unmergeCells': {'range': {'sheetId': ws.id}}}]})
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TITULO, rows=len(LINHAS), cols=2)
ws.resize(rows=len(LINHAS), cols=2)
ws.update(values=LINHAS, range_name='A1', value_input_option='RAW')
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}
MARINHO, GELO, AZC, TEXTO = cor('#192D4E'), cor('#F4F3EF'), cor('#DCE3EE'), cor('#3A3A3A')
FIO = {'style': 'SOLID', 'color': cor('#DADAD4')}
rg = lambda r0, r1, c0=0, c1=2: {'sheetId': ws.id, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c0, 'endColumnIndex': c1}
texto = lambda tam, negrito, c: {'fontFamily': 'Montserrat', 'fontSize': tam, 'bold': negrito, 'foregroundColor': c}
altura = lambda r, px: {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': r, 'endIndex': r + 1},
                                                      'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
largura = lambda i, px: {'updateDimensionProperties': {'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
                                                       'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
n = len(LINHAS)
itens = [i for i in range(1, n) if i not in BLOCOS]
reqs = [
    {'updateCells': {'range': rg(0, n), 'fields': 'userEnteredFormat'}},
    {'updateSheetProperties': {'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 0, 'hideGridlines': True}},
                               'fields': 'gridProperties(frozenRowCount,hideGridlines)'}},
    {'repeatCell': {'range': rg(0, n), 'cell': {'userEnteredFormat': {'backgroundColor': GELO, 'wrapStrategy': 'WRAP', 'verticalAlignment': 'MIDDLE',
                                                                     'textFormat': texto(10, False, TEXTO)}},
                    'fields': 'userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)'}},
    # faixa: logo em A1 e título em B1
        {'repeatCell': {'range': rg(0, 1), 'cell': {'userEnteredFormat': {'backgroundColor': MARINHO, 'horizontalAlignment': 'CENTER',
                                                                     'textFormat': texto(26, True, cor('#FFFFFF'))}},
                    'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)'}},
    altura(0, 96),
]
for i in BLOCOS:
    reqs += [{'mergeCells': {'range': rg(i, i + 1), 'mergeType': 'MERGE_ALL'}},
             {'repeatCell': {'range': rg(i, i + 1), 'cell': {'userEnteredFormat': {'backgroundColor': AZC, 'textFormat': texto(11, True, MARINHO),
                                                                                  'borders': {'top': FIO, 'bottom': FIO}}},
                             'fields': 'userEnteredFormat(backgroundColor,textFormat,borders)'}},
             altura(i, 30)]
for i in itens:
    reqs += [{'repeatCell': {'range': rg(i, i + 1, 0, 1), 'cell': {'userEnteredFormat': {'textFormat': texto(10, True, MARINHO)}},
                             'fields': 'userEnteredFormat.textFormat'}},
             {'repeatCell': {'range': rg(i, i + 1), 'cell': {'userEnteredFormat': {'borders': {'top': FIO, 'bottom': FIO, 'left': FIO, 'right': FIO}}},
                             'fields': 'userEnteredFormat.borders'}}]
reqs += [largura(0, 255), largura(1, 1000),
         {'autoResizeDimensions': {'dimensions': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 1, 'endIndex': n}}}]
sh.batch_update({'requests': reqs})
for i in BLOCOS:
    sh.batch_update({'requests': [altura(i, 30)]})
# logo: copia a aba de origem para cá, cola a célula do logo e apaga a cópia
tmp = gc.open_by_key(LOGO_DE[0]).worksheet(LOGO_DE[1]).copy_to(ID)['sheetId']
sh.batch_update({'requests': [{'copyPaste': {'source': {**rg(0, 1, 0, 1), 'sheetId': tmp}, 'destination': rg(0, 1, 0, 1), 'pasteType': 'PASTE_VALUES'}},
                              {'deleteSheet': {'sheetId': tmp}}]})
from ordem_abas import ordenar
ordenar(sh)
print([w.title for w in sh.worksheets()][:4])
