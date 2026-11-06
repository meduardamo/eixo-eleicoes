"""Etapa 25. Atuação de cada senador nos subtemas dos clientes, dentro dos quatro grandes temas.

Subtemas tirados da taxonomia dos planos de governo (outros/analise_planos.py) e das pautas dos clientes:
tempo integral e recomposição das aprendizagens (ISG, IU), alfabetização (Lemann), conectividade (MegaEdu),
tabaco e álcool (Vital), alimentação saudável e ultraprocessados (Desiderata, IDEC, Umane), violência contra
crianças e adolescentes, violência contra a mulher e casamento infantil (Vital, Cactus, Infinis, Coletivo, UNFPA), planos de saúde (IDEC, IEPS),
saúde mental, drogas e apostas (Cactus, IEPS), incentivo ao esporte (REMS), primeira infância (FMCSV) etc.

Cada matéria da etapa 21 (primeira autoria e relatorias com parecer, Senado e Câmara) foi lida pela ementa e
recebeu um ou dois subtemas: dados_novos_eleitos/temas_clientes/subtemas_lidos.csv (2.060 matérias, leitura
de 16/10/2026; em 19/10 o antigo FR foi relido e dividido em TA e AS). Violência (VI) vem de outra leitura,
temas_clientes/violencia_lidos.csv (19/10/2026): as 525 matérias da base completa do levantamento (etapa 18) com
público mulheres ou crianças em segurança, direitos humanos, direito civil ou assistência, ou com termo de
violência, lidas uma a uma; 330 entraram. Em 20/10 as 865 ementas de Educação foram relidas para dois subtemas novos,
pedidos por ISG e IU: infraestrutura escolar (IN: obras, reforma, condições mínimas, água, mobiliário, normas de
segurança do prédio; 12 matérias, uma delas lida em Primeira infância) e adaptação e emergência climática nas escolas
(CL: 2 matérias). Defesa civil no currículo e o programa do RS para o ensino superior ficaram fora.
Também em 20/10: as 170 matérias da base completa com termo escolar que a Casa classificou fora de Educação foram lidas
uma a uma; 40 são de educação básica e entraram com o código do subtema (temas_clientes/educacao_outros_temas_lidos.csv;
o PL 4501/2020, ultraprocessados nas escolas, conta também em Saúde).
Em 04/11/2026 "Saúde da mulher e direitos reprodutivos" (MU) virou seis subtemas, a pedido do Coletivo Feminista:
aborto legal: acesso e garantia (AB); restrições ao aborto legal e criminalização (RE); vida desde a concepção e
direitos do nascituro (NC); gravidez infantil e aborto em meninas (GI); planejamento reprodutivo e contracepção (PP);
saúde da mulher, gestação e parto (MU, o que sobrou). As 65 matérias de MU foram relidas (11 mudaram de código; os
PDLs das portarias do Ministério da Saúde lidos pelo texto: 2.561/2020 e a nota técnica de 2024 sobre idade gestacional
são RE, a Rede de Atenção Materna e Infantil é MU) e as 128 matérias da base completa com termo reprodutivo foram lidas
uma a uma; 20 entraram (temas_clientes/reprodutivos_lidos.csv). Estupro de vulnerável sem gravidez fica em VC;
direito trabalhista e previdenciário da gestante fica fora. Ensino superior e o que não cabe em subtema de cliente ficam fora das colunas.

Também em 04/11/2026 a violência (VI) virou três subtemas: violência contra crianças e adolescentes (VC), violência
contra a mulher (VM) e casamento infantil e uniões precoces (CI). Foram lidas uma a uma as 330 matérias de VI, as 150
da leitura de 19/10 que tinham ficado fora, as 139 que só batiam com palavra-chave de cliente e nunca tinham sido lidas
e as 236 da base completa com termo de violência (estupro, assédio, dignidade sexual, feminicídio etc.) que nenhuma
leitura tinha pego. Regras: crime contra a dignidade sexual sem vítima especificada é VM, e VC quando a vítima é
vulnerável; medida de segurança para mulheres em transporte, bar ou evento é VM; violência sem recorte de mulher ou
criança, maus-tratos a animais, idosos, tráfico de pessoas sem recorte, LGBT, desaparecidos, assistência, adoção,
socioeducativo e direito trabalhista ficam fora. 415 entraram (temas_clientes/violencia_lidos.csv, agora com o código;
algumas levam também código de outro tema, como TA para propaganda de bebida). O que ficou fora em cada leitura, com o
motivo, está em temas_clientes/leituras_fora.csv.
Ainda em 04/11, entram também em violencia_lidos.csv (com o tema e o código de cada linha): 18 das 394 matérias que só
batiam com palavra do IDEC (agrotóxicos, segurança alimentar, patentes de medicamento, obesidade; o resto é consumidor,
crédito, telecomunicações, dados, energia e clima, fora dos quatro temas) e 20 das 59 achadas com termos novos por
subtema de cliente (tabaco, suicídio e automutilação online, psicólogos, esporte). As 130 rejeitadas de Educação de 20/10
ganharam motivo; PL 6277/2019 (agressão a professor) e PL 1097/2015 (centro de formação técnica) entraram.
Conferência: 1 em cada 10 matérias lidas (250 classificadas + 48 rejeitadas) foi relida às cegas por dois agentes, só
pela DEFINICAO; 84% bateram em pelo menos um código. Erros achados e corrigidos com os casos iguais na base: licença-
maternidade, licença-paternidade e salário-maternidade estavam fora e passam a PF (23 matérias, com amamentação em SM);
exames do recém-nascido que só contavam em MU ganham SM; proteção ao apostador ganha SME; residência médica é PR;
vaquejada e pipa desportiva são GR; agressão a profissional de saúde ou de educação conta em PR; EE (esporte escolar,
código que não existia) vira EC.

Emendas lidas pelo texto (EMENDAS_LIDAS, 04/11/2026) contam como item; as do senador a um mesmo projeto contam uma vez.
Nível por subtema, pelo número de matérias distintas do senador no subtema (autor ou relator):
0 = Não atua; 1 = Pouco; 2 a 4 = Atua; 5 ou mais = Referência. A nota da célula lista as matérias.

Saída: temas_clientes/subtemas.csv (uma linha por senador) e temas_clientes/subtemas_notas.json
Rodar: python -m outros.novos_eleitos.e25_subtemas_senado   (depois da 21)
"""
import json

import pandas as pd

from outros.novos_eleitos import comum as c

GRUPOS = ["curso", "reeleitos", "novos"]
# (grande tema, código, rótulo da coluna). Educação infantil fica em Primeira infância e junta as duas leituras.
SUBTEMAS = [
    ("Saúde", "AP", "Atenção primária à saúde"),
    ("Saúde", "MA", "Hospitais e alta complexidade no SUS"),
    ("Saúde", "UE", "Urgência e emergência em saúde"),
    ("Saúde", "SME", "Saúde mental, drogas e apostas"),
    ("Saúde", "TA", "Tabaco e álcool"),
    ("Saúde", "AS", "Alimentação saudável, ultraprocessados e agrotóxicos"),
    ("Saúde", "VC", "Violência contra crianças e adolescentes"),
    ("Saúde", "VM", "Violência contra a mulher"),
    ("Saúde", "CI", "Casamento infantil e uniões precoces"),
    ("Saúde", "PS", "Planos de saúde"),
    ("Saúde", "MV", "Medicamentos e vacinas"),
    ("Saúde", "DC", "Doenças e condições de saúde específicas"),
    ("Saúde", "AB", "Aborto legal: acesso e garantia"),
    ("Saúde", "RE", "Restrições ao aborto legal e criminalização"),
    ("Saúde", "NC", "Vida desde a concepção e direitos do nascituro"),
    ("Saúde", "GI", "Gravidez infantil e aborto em meninas"),
    ("Saúde", "PP", "Planejamento reprodutivo e contracepção"),
    ("Saúde", "MU", "Saúde da mulher, gestação e parto"),
    ("Saúde", "PR", "Profissionais e formação em saúde"),
    ("Saúde", "FG", "Financiamento e gestão do SUS"),
    ("Saúde", "VS", "Vigilância e emergências sanitárias"),
    ("Educação", "TI", "Educação em tempo integral"),
    ("Educação", "AL", "Alfabetização"),
    ("Educação", "RA", "Recomposição das aprendizagens e avaliação educacional"),
    ("Educação", "EM", "Ensino médio e educação profissional e tecnológica"),
    ("Educação", "PR", "Profissionais da educação"),
    ("Educação", "FI", "Financiamento da educação"),
    ("Educação", "EQ", "Equidade na educação e educação inclusiva"),
    ("Educação", "TC", "Conectividade e tecnologia nas escolas"),
    ("Educação", "GG", "Gestão e governança da educação (PNE, SNE, CNE)"),
    ("Educação", "IS", "Alimentação, transporte e segurança escolar"),
    ("Educação", "IN", "Infraestrutura escolar"),
    ("Educação", "CL", "Adaptação e emergência climática nas escolas"),
    ("Educação", "CU", "Currículo e conteúdos escolares"),
    ("Educação", "ED", "Educação domiciliar, escola sem partido e escolas cívico-militares"),
    ("Esporte", "FN", "Financiamento e incentivo ao esporte"),
    ("Esporte", "EC", "Esporte educacional, comunitário e inclusão pelo esporte"),
    ("Esporte", "AR", "Atletas e esporte de alto rendimento"),
    ("Esporte", "GR", "Gestão, regulação e integridade do esporte"),
    ("Esporte", "AP", "Apostas esportivas"),
    ("Primeira infância", "EI", "Educação infantil e creche"),
    ("Primeira infância", "SM", "Saúde materno-infantil"),
    ("Primeira infância", "PF", "Parentalidade e apoio às famílias"),
    ("Primeira infância", "PC", "Proteção e segurança da criança na primeira infância"),
    ("Primeira infância", "GV", "Governança e orçamento da primeira infância"),
]
# códigos lidos em outro tema que contam no subtema: (tema da leitura, código) -> (tema da coluna, código)
JUNTA = {("Educação", "EI"): ("Primeira infância", "EI"), ("Esporte", "EE"): ("Esporte", "EC"), ("Saúde", "BT"): ("Saúde", "SME"),
         ("Primeira infância", "IN"): ("Educação", "IN")}
# Emendas lidas pelo texto (as mesmas dos sinais diretos da etapa 24). Todas as emendas do senador a um mesmo projeto
# contam como um item no subtema, para emenda não pesar mais que projeto. A 697 (Teresa Leitão, autonomia progressiva) e a
# 769 (Magno Malta, recusa de tratamento) ao Código Civil não têm subtema.
EMENDAS_LIDAS = [
    # (senador, Casa, projeto emendado, números, ano, (tema, código), o que fazem)
    ("Efraim Filho", "Senado", "PLP 68/2024", "1490", 2024, ("Saúde", "AS"), "tira as bebidas açucaradas do Imposto Seletivo"),
    ("Laércio Oliveira", "Senado", "PLP 68/2024", "642", 2024, ("Saúde", "AS"), "limita a 1% o Imposto Seletivo das bebidas açucaradas"),
    ("Veneziano Vital do Rêgo", "Senado", "PLP 68/2024", "1730", 2024, ("Saúde", "AS"), "tira as bebidas açucaradas do Imposto Seletivo"),
    ("Magno Malta", "Senado", "PLP 68/2024", "1928", 2024, ("Saúde", "AS"), "tira as bebidas açucaradas do Imposto Seletivo"),
    ("Beto Faro", "Senado", "PLP 68/2024", "2197", 2024, ("Saúde", "AS"), "estende o Imposto Seletivo às bebidas ultraprocessadas"),
    ("Tereza Cristina", "Senado", "PLP 68/2024", "1873", 2024, ("Saúde", "TA"), "escalona o Imposto Seletivo das bebidas alcoólicas"),
    ("Carlos Portinho", "Senado", "PL 4/2025", "741", 2025, ("Saúde", "CI"), "fixa 18 anos como idade mínima para o casamento"),
    ("Damares Alves", "Senado", "PL 4/2025", "15, 26, 27, 38 e 40", 2025, ("Saúde", "NC"),
     "vida desde a concepção, personalidade civil e direitos do nascituro, vedação ao descarte de embriões"),
    ("Jorge Seif", "Senado", "PL 4/2025", "56, 61 e 67", 2025, ("Saúde", "NC"), "nascituro titular de direitos da personalidade, vida desde a concepção"),
    ("Hamilton Mourão", "Senado", "PL 4/2025", "139", 2025, ("Saúde", "NC"), "repete a regra vigente sobre os direitos do nascituro desde a concepção"),
    ("Magno Malta", "Senado", "PL 4/2025", "781", 2025, ("Saúde", "NC"), "suprime a vida pré-uterina como potencialidade"),
]
# Definição de cada subtema (04/11/2026): o que entra e o que fica fora, a régua da leitura das ementas.
# Sai em temas_clientes/subtemas_definicao.csv e na aba Definição dos subtemas.
DEFINICAO = {
    ("Saúde", "AP"): ("Unidade básica, Estratégia Saúde da Família, agentes comunitários, Mais Médicos como provimento na atenção básica, porta de entrada do SUS",
                      "Hospital e especialidade (Hospitais e alta complexidade); carreira e formação sem recorte de atenção básica (Profissionais e formação)"),
    ("Saúde", "MA"): ("Hospitais, leitos, cirurgias, transplantes, tratamento oncológico e de alta complexidade, fila de procedimento especializado, acompanhante e estrutura hospitalar",
                      "Pronto-socorro e SAMU (Urgência e emergência); doença sem ação hospitalar (Doenças e condições específicas)"),
    ("Saúde", "UE"): ("SAMU, UPA, pronto-socorro, ambulância, desfibrilador, primeiros socorros, protocolos de atendimento de urgência",
                      "Epidemia e emergência sanitária (Vigilância e emergências sanitárias)"),
    ("Saúde", "SME"): ("Saúde mental, Rede de Atenção Psicossocial e CAPS, suicídio e automutilação, dependência química, comunidades terapêuticas, ludopatia e proteção ao apostador",
                       "Regulação econômica das apostas (Esporte, Apostas esportivas); tabaco e álcool (Tabaco e álcool)"),
    ("Saúde", "TA"): ("Produtos de tabaco e cigarro eletrônico; bebida alcoólica: propaganda, rotulagem, advertência, venda a menor, venda em estrada, posto e estádio, tributação e Imposto Seletivo; álcool ao volante (embriaguez, lei seca, teste de alcoolemia, pena para quem dirige sob efeito de álcool)",
                      "Segurança viária sem álcool (velocidade, capacete, cinto, radar)"),
    ("Saúde", "AS"): ("Rotulagem e publicidade de alimentos, ultraprocessados, bebidas açucaradas, educação alimentar, segurança alimentar e nutricional, agrotóxicos",
                      "Merenda escolar (Educação, Alimentação, transporte e segurança escolar); fiscalização sanitária de alimentos (Vigilância e emergências sanitárias)"),
    ("Saúde", "VC"): ("Violência física, sexual e psicológica contra criança e adolescente: estupro de vulnerável, exploração sexual, abuso online, maus-tratos, bullying, indução à autolesão, notificação, escuta protegida, cadastro de condenados, atendimento à vítima",
                      "Ato infracional e socioeducativo; adoção e acolhimento sem violência; conteúdo impróprio sem violência; trabalho infantil"),
    ("Saúde", "VM"): ("Lei Maria da Penha, feminicídio, medida protetiva, violência doméstica, crime contra a dignidade sexual sem vítima especificada, assédio e importunação sexual, divulgação de nudez, violência obstétrica, violência política de gênero, segurança da mulher em transporte, bar e evento",
                      "Igualdade de gênero sem violência (cotas, salário, crédito, representação); violência sem recorte de mulher"),
    ("Saúde", "CI"): ("Idade mínima para casar, casamento com autorização dos pais abaixo de 18 anos, uniões informais com criança ou adolescente",
                      "Casamento entre adultos, regime de bens, licença por casamento"),
    ("Saúde", "PS"): ("Planos e seguros de saúde: Lei 9.656, ANS, reajuste, cobertura, rol, carência, ressarcimento ao SUS", ""),
    ("Saúde", "MV"): ("Medicamentos (preço, registro, acesso, distribuição, cannabis medicinal) e vacinação",
                      "Fiscalização sanitária de produtos que não são medicamento (Vigilância e emergências sanitárias)"),
    ("Saúde", "DC"): ("Programas, campanhas de prevenção, datas e direitos por doença ou condição: câncer, doenças raras, autismo, fibromialgia, diabetes, obesidade, doenças crônicas",
                      "Saúde mental (Saúde mental, drogas e apostas); câncer de mama e colo como saúde da mulher (Saúde da mulher, gestação e parto)"),
    ("Saúde", "AB"): ("Garantia e ampliação do aborto nos casos previstos em lei: serviços de referência, acesso, proteção a profissionais",
                      "Plebiscito sobre legalização"),
    ("Saúde", "RE"): ("Restrição ao aborto legal: limite de idade gestacional, assistolia, exigências que dificultam o acesso, aumento de pena, campanha contra o aborto, sustação de normas de acesso",
                      "Aborto sem consentimento da gestante (Violência contra a mulher)"),
    ("Saúde", "NC"): ("Estatuto do Nascituro, vida desde a concepção na Constituição ou no Código Civil, personalidade e direitos do nascituro, descarte de embriões", ""),
    ("Saúde", "GI"): ("Gravidez de menina vítima de estupro, aborto legal em meninas, Resolução 258 do Conanda",
                      "Estupro de vulnerável sem gravidez (Violência contra crianças e adolescentes)"),
    ("Saúde", "PP"): ("Planejamento familiar (Lei 9.263), laqueadura e vasectomia, métodos contraceptivos, reprodução assistida", ""),
    ("Saúde", "MU"): ("Pré-natal, parto, acompanhante, violência obstétrica, saúde menstrual, câncer de mama e colo, menopausa",
                      "Direito trabalhista e previdenciário da gestante"),
    ("Saúde", "PR"): ("Carreira, piso, jornada, formação, residência e exercício das profissões de saúde, provimento de médicos, segurança do profissional (inclusive pena maior para agressão no exercício da função)", ""),
    ("Saúde", "FG"): ("Orçamento e repasses do SUS, piso da saúde, emendas, gestão e organização do SUS, regionalização, filas e transparência, entidades filantrópicas", ""),
    ("Saúde", "VS"): ("Epidemias e Covid-19 como emergência, vigilância epidemiológica e sanitária, Anvisa na fiscalização, segurança sanitária de alimentos e produtos, notificação de doença", ""),
    ("Educação", "TI"): ("Jornada ampliada, Programa Escola em Tempo Integral, educação integral", ""),
    ("Educação", "AL"): ("Alfabetização de crianças, jovens e adultos, Política Nacional de Alfabetização, idade certa", ""),
    ("Educação", "RA"): ("Avaliação externa (Saeb, Sinaeb), recuperação, reforço e estudos complementares", ""),
    ("Educação", "EM"): ("Ensino médio, educação profissional e tecnológica, aprendizagem profissional, Pé-de-Meia", ""),
    ("Educação", "PR"): ("Piso, carreira, formação, valorização, saúde e segurança do professor (inclusive pena maior para agressão no exercício da função), profissional de apoio escolar", ""),
    ("Educação", "FI"): ("Fundeb, VAAT, VAAR, vinculação constitucional, FNDE, royalties para a educação, gasto mínimo", ""),
    ("Educação", "EQ"): ("Educação especial e inclusiva, educação do campo, indígena e quilombola, prioridade de matrícula para grupos vulneráveis, estudante gestante ou doente", ""),
    ("Educação", "TC"): ("Internet nas escolas, equipamentos, educação digital, inteligência artificial, celular nas escolas", ""),
    ("Educação", "GG"): ("PNE, Sistema Nacional de Educação, CNE e conselhos, regime de colaboração, gestão democrática, diretores, regras gerais de calendário", ""),
    ("Educação", "IS"): ("Alimentação escolar (PNAE), transporte escolar, segurança e violência na escola", ""),
    ("Educação", "IN"): ("Obras, reforma, condições mínimas do prédio, água e saneamento, mobiliário, prevenção de incêndio", ""),
    ("Educação", "CL"): ("Preparação da escola para eventos climáticos e educação sobre mudança do clima", "Defesa civil no currículo"),
    ("Educação", "CU"): ("Conteúdos obrigatórios e temas transversais, bibliotecas, línguas, educação financeira", ""),
    ("Educação", "ED"): ("Educação domiciliar, escola sem partido, vedação de conteúdo de gênero, escolas cívico-militares", ""),
    ("Esporte", "FN"): ("Lei de Incentivo ao Esporte, fundos e loterias para o esporte, patrocínio, benefício fiscal", ""),
    ("Esporte", "EC"): ("Esporte escolar e comunitário, atividade física, paradesporto, inclusão pelo esporte, igualdade de premiação", ""),
    ("Esporte", "AR"): ("Atletas, Bolsa Atleta, dopagem, carreira e ex-atletas, competições", ""),
    ("Esporte", "GR"): ("Lei Geral do Esporte, Estatuto do Torcedor, violência em estádios, manipulação de resultados, reconhecimento de modalidade, clubes e SAF", ""),
    ("Esporte", "AP"): ("Apostas de quota fixa: regulação, publicidade, bônus, tributação", "Ludopatia e proteção ao apostador (Saúde mental, drogas e apostas)"),
    ("Primeira infância", "EI"): ("Creche e pré-escola: vagas, matrícula, financiamento, funcionamento", ""),
    ("Primeira infância", "SM"): ("Recém-nascido, teste do pezinho e exames da criança, prematuridade, amamentação, mortalidade infantil", ""),
    ("Primeira infância", "PF"): ("Licenças parentais, benefícios para famílias com criança pequena, prioridade de atendimento a pais com criança de colo, visitação domiciliar", ""),
    ("Primeira infância", "PC"): ("Acidentes domésticos, segurança de produtos e piscinas, transporte seguro, identificação do recém-nascido, maus-tratos a criança pequena", ""),
    ("Primeira infância", "GV"): ("Marco Legal da Primeira Infância, Política Nacional Integrada da Primeira Infância, Orçamento Criança, fundos e planos", ""),
}
NIVEL = lambda n: "Não atua" if n == 0 else "Pouco" if n == 1 else "Atua" if n <= 4 else "Referência"


def main():
    # educação que a Casa classificou em outro tema (orçamento, segurança, tributos etc.) entra pela base completa
    extra = c.ler_csv("temas_clientes/educacao_outros_temas_lidos.csv")
    # busca local de 04/11/2026: projetos de Assembleia e câmara municipal lidos nos 45 subtemas
    extra = pd.concat([extra, c.ler_csv("temas_clientes/locais_lidos.csv")])
    # 06/11/2026: álcool ao volante (Transporte) entra em Tabaco e álcool pela base completa
    extra = pd.concat([extra, c.ler_csv("temas_clientes/outros_temas_lidos.csv")])
    lidos = pd.concat([c.ler_csv("temas_clientes/subtemas_lidos.csv"), extra])
    cod = {}
    for r in lidos.itertuples():
        for s in str(r.Subtemas).split(","):
            tema, s = JUNTA.get((r._1, s), (r._1, s))
            cod.setdefault((r.Casa, r.Matéria), set()).add((tema, s))
    # violência quase toda caiu em Segurança na etapa 18, fora das matérias dos quatro temas: entra pela base completa
    viol = c.ler_csv("temas_clientes/violencia_lidos.csv")
    for r in viol.itertuples():
        for s in str(r.Subtemas).split(","):
            tema, s = JUNTA.get((r._1, s), (r._1, s))
            cod.setdefault((r.Casa, r.Matéria), set()).add((tema, s))
    # direitos reprodutivos fora dos quatro temas (Constituição, Código Penal, Conanda, FGTS) entram pela base completa
    repro = c.ler_csv("temas_clientes/reprodutivos_lidos.csv")
    for r in repro.itertuples():
        for s in str(r.Subtemas).split(","):
            cod.setdefault((r.Casa, r.Matéria), set()).add(("Saúde", s))
    reg = c.ler_csv("levantamento_senado/registros.csv").drop_duplicates(["Parlamentar", "Casa", "Matéria", "Registro"])
    reg = reg.merge(pd.concat([viol, extra, repro])[["Casa", "Matéria"]].drop_duplicates(), on=["Casa", "Matéria"])
    reg = reg.assign(**{"Ano": reg.Matéria.str.extract(r"/(\d{4})")[0], "Virou lei": ""})
    mats = pd.concat([c.ler_csv(f"temas_clientes/materias_{g}.csv") for g in GRUPOS])
    mats = pd.concat([mats, reg[[k for k in mats.columns if k in reg.columns]]]).reindex(columns=mats.columns)
    nomes = list(dict.fromkeys(n for g in GRUPOS for n in c.ler_csv(f"temas_clientes/perfil_{g}.csv").Parlamentar))
    linhas, notas = [], {}
    for nome in nomes:
        m = mats[mats.Parlamentar == nome]
        linha = {"Senador": nome}
        for tema, s, rot in SUBTEMAS:
            achou = {}
            for r in m.itertuples():
                if (tema, s) in cod.get((r.Casa, r.Matéria), set()):
                    papel = "autor" if r.Registro == "Primeiro autor" else "relator"
                    k = (r.Casa, r.Matéria)
                    lei = str(r._10).strip() == "Sim"
                    achou.setdefault(k, [set(), lei, str(r.Ementa), int(str(r.Ano)[:4]) if str(r.Ano)[:4].isdigit() else 0])[0].add(papel)
            for sen, casa, proj, nums, ano, cod_e, faz in EMENDAS_LIDAS:
                if sen == nome and cod_e == (tema, s):
                    plural = "Emendas" if " e " in nums else "Emenda"
                    achou[(casa, f"{plural} {nums} ao {proj}")] = [{"autor"}, False, faz, ano]
            linha[f"{tema}|{rot}"] = NIVEL(len(achou))
            if achou:
                itens = sorted(achou.items(), key=lambda x: -x[1][3])
                txt = [f"{len(achou)} matéria{'s' if len(achou) > 1 else ''} no subtema (autor ou relator):"]
                for (casa, mat), (papeis, lei, ementa, ano) in itens[:12]:
                    ementa = ementa.replace("\n", " ").strip()
                    ementa = ementa if len(ementa) <= 160 else ementa[:157].rsplit(" ", 1)[0] + "..."
                    txt.append(f"{mat} ({' e '.join(sorted(papeis))}{', ' + casa if casa == 'Câmara' else ''}"
                               f"{', virou lei' if lei else ''}): {ementa}")
                if len(itens) > 12:
                    txt.append(f"e mais {len(itens) - 12}")
                notas[f"{nome}|{tema}|{rot}"] = "\n".join(txt)
        linhas.append(linha)
    assert set(DEFINICAO) == {(t, s) for t, s, _ in SUBTEMAS}
    c.salvar_csv(pd.DataFrame([{"Tema": t, "Subtema": rot, "O que entra": DEFINICAO[(t, s)][0], "O que fica fora": DEFINICAO[(t, s)][1]}
                               for t, s, rot in SUBTEMAS]), "temas_clientes/subtemas_definicao.csv")
    df = pd.DataFrame(linhas)
    c.salvar_csv(df, "temas_clientes/subtemas.csv")
    json.dump(notas, open(c.caminho("temas_clientes/subtemas_notas.json"), "w"), ensure_ascii=False, indent=1)
    for tema in dict.fromkeys(t for t, _, _ in SUBTEMAS):
        cols = [k for k in df.columns if k.startswith(tema + "|")]
        print(tema)
        print(df[cols].apply(lambda s: s.value_counts()).fillna(0).astype(int).T.to_string())


if __name__ == "__main__":
    main()
