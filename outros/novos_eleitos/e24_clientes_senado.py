"""Etapa 24. Classificação dos 81 senadores para cada cliente, a partir da base de pautas dos coordenadores.

Entrada: temas_clientes/base_pautas.csv (cópia da planilha "Eixo | Base de pautas por cliente", abas Clientes
Educação, Saúde e Esporte) e temas_clientes/posicoes_clientes_complemento.csv (posição informada fora da planilha,
a confirmar com a coordenação). Entram os projetos marcados Promotor ou Detrator e, quando a marcação está em Dúvida
ou em branco, os de pauta com posição A favor ou Contra, no sentido dessa posição (a confirmar); Depende do texto fica fora.

Sinais do senador em cada projeto (Senado e, para quem foi deputado, Câmara):
- forte: primeiro autor; voto nominal no próprio projeto ou no parecer dele (Sim conta no sentido do projeto,
  Não no sentido contrário), desde que a votação tenha dividido os parlamentares da base (consenso não conta). Votação de emenda, destaque ou requerimento não conta, porque o sentido para o
  cliente depende do texto da emenda;
- médio: coautor;
- só evidência, sem contar: relatoria (o sentido depende do parecer, que não foi lido).
Projeto Promotor dá sinal a favor do cliente; Detrator, contra.

Régua (a mesma da Metodologia): Promotor ou Detrator com dois sinais fortes na mesma direção ou um forte e um médio;
fortes nos dois sentidos: diferença de dois decide, de um decide com médio no mesmo sentido, senão Neutro;
abaixo disso, Não tem informação suficiente.

PEC tem numeração própria em cada Casa, e projeto de lei também até 2018: nesses casos a Casa da matéria tem de
bater com a anotada na base de pautas.

Saída: temas_clientes/clientes_classificacao.csv e clientes_resumo.csv
Rodar: python -m outros.novos_eleitos.e24_clientes_senado   (depois da 23)
"""
import re

import pandas as pd

from outros.novos_eleitos import comum as c

GRUPOS = ("curso", "reeleitos", "novos")
VOTO_PROJETO = r"Votação nominal d[oa] (?:Projeto|Proposta)|Aprovad[oa] o (?:Parecer|Substitutivo)|Rejeitad[oa] o (?:Parecer|Substitutivo)|nos termos do parecer"


def chaves(projeto):
    casa = "Câmara" if "Câmara" in projeto else "Senado" if "Senado" in projeto else None
    texto, sig, out = re.sub(r"\(.*?\)", "", projeto), None, []
    for m in re.finditer(r"(PLP|PEC|PL|RQS|REQ|PDL)?\s*(\d+)\s*/\s*(\d{4})", texto):
        sig = m.group(1) or sig
        if sig:
            k = f"{sig} {m.group(2)}/{m.group(3)}"
            exige = casa if (sig == "PEC" or int(m.group(3)) < 2019 or sig in ("REQ", "RQS")) else None
            out.append((k, exige))
    return out


# Sinais na pauta, além dos projetos listados: votações nominais do Senado que separam os senadores na pauta e
# projetos de autoria cujo sentido a ementa mostra. sentido +1 = Sim vai na direção de quem é a favor da pauta.
PAUTAS = {
    "Apostas online e saúde mental": {
        "votacoes": [("2023-12-12", "PL 3626", (37, 27), +1, "Emenda 139 da lei das bets (tirar jogos on-line e cassino)"),
                     ("2023-12-12", "PL 3626", (25, 36), +1, "Emenda 157 da lei das bets (proibir publicidade em arenas e patrocínio)"),
                     ("2025-12-17", "RQS 857", (28, 36), -1, "urgência do PL 2234 (legaliza cassinos e jogos)")],
        "comissao": [("2024-06-19", "CCJ", "PL 2234/2022", -1, "relatório do PL 2234 na CCJ (legaliza cassinos e jogos)")],
        "autoria": {"PL 2372/2025": "proíbe toda publicidade de apostas", "PL 2662/2025": "crime de operar e divulgar apostas virtuais",
                    "PL 2985/2023": "proíbe publicidade de apostas", "PL 3718/2024": "limitação das apostas",
                    "PL 3754/2025": "idade mínima de 21 anos para apostar", "PL 3765/2026": "limite mensal de depósitos em apostas",
                    "PL 4219/2024": "limite mensal de depósitos em apostas", "PL 626/2023": "veda pagamentos para jogos de azar",
                    "PL 3587/2024": "operador de apostas com representação legal no país"},
    },
}


# Sinais pela descrição do cliente (PDF "Monitor Legislativo e DOU - Segunda Etapa" e CLIENT_THEME_DATA do raspador
# do DOU), com o sentido lido na ementa: +1 a favor do cliente, -1 contra. Ficam de fora as ementas que não mostram
# o sentido e os projetos que já estão na base de pautas do cliente, que contam por lá. A confirmar com a
# coordenação. Chave: (matéria, Casa da autoria).
DESCRICAO = {
    ("PL 707/2015", "Câmara"): ({"ISG": 1, "IU": 1}, "ensino em tempo integral"),
    ("PL 1228/2015", "Câmara"): ({"IU": 1}, "fundo de educação ambiental"),
    ("PLP 243/2023", "Senado"): ({"ISG": 1, "IU": 1}, "permanência no ensino médio"),
    ("PEC 36/2025", "Senado"): ({"ISG": 1}, "ensino em tempo integral"),
    ("PL 1311/2023", "Senado"): ({"IU": 1}, "gestão democrática na distribuição do Fundeb"),
    ("PL 4414/2024", "Senado"): ({"IU": 1}, "magistério indígena e do campo"),
    ("PL 3756/2023", "Senado"): ({"IU": 1}, "bolsa permanência na educação básica"),
    ("PL 3166/2023", "Senado"): ({"ISG": 1}, "bolsa para estudantes do ensino médio"),
    ("PL 3785/2026", "Câmara"): ({"ISG": 1}, "educação profissional no ensino médio"),
    ("PL 4744/2023", "Senado"): ({"ISG": 1}, "educação profissional no ensino médio"),
    ("PL 1070/2021", "Senado"): ({"IU": 1}, "educação ambiental"),
    ("PL 3097/2021", "Senado"): ({"IU": 1}, "educação ambiental"),
    ("PL 4270/2020", "Câmara"): ({"IU": 1}, "educação ambiental"),
    ("PL 5264/2025", "Câmara"): ({"IU": 1}, "insalubridade para o magistério"),
    ("PL 1989/2023", "Senado"): ({"IU": 1}, "piso do magistério"),
    ("PL 4372/2020", "Câmara"): ({"IU": 1}, "regulamentação do Fundeb"),
    ("PL 3418/2021", "Câmara"): ({"IU": 1}, "atualização da lei do Fundeb"),
    ("PL 5665/2023", "Senado"): ({"IU": 1}, "prorrogação do PNE"),
    ("PL 3562/2020", "Câmara"): ({"IU": 1}, "Conselho Nacional de Educação"),
    ("PL 1884/2023", "Senado"): ({"IU": 1}, "internet nas escolas"),
    ("PL 810/2022", "Câmara"): ({"IU": 1}, "internet nas escolas"),
    ("PLP 59/2021", "Senado"): ({"IU": 1}, "internet nas escolas"),
    ("PL 3853/2020", "Senado"): ({"IU": 1}, "tablets e dados para estudantes"),
    ("PL 4978/2026", "Senado"): ({"IU": 1}, "educação étnico-racial e quilombola"),
    ("PL 3467/2025", "Senado"): ({"IU": 1}, "apoio às redes com baixo desempenho e alta vulnerabilidade"),
    ("PL 597/2022", "Câmara"): ({"IU": 1, "ISG": 1}, "recomposição da alfabetização"),
    ("PDL 237/2025", "Câmara"): ({"ISG": -1, "IU": -1}, "susta a resolução do CNE sobre os itinerários do ensino médio"),
    ("PL 10185/2018", "Câmara"): ({"ISG": -1, "FMCSV": -1}, "autoriza a educação domiciliar"),
    ("PL 5168/2026", "Câmara"): ({"ISG": -1, "FMCSV": -1}, "disciplina a educação domiciliar"),
    ("PL 2577/2026", "Câmara"): ({"ISG": -1, "FMCSV": -1}, "anistia a pais pela educação domiciliar"),
    ("PL 1477/2022", "Senado"): ({"FMCSV": 1}, "Benefício Primeira Infância"),
    ("PL 729/2022", "Senado"): ({"FMCSV": 1}, "prioridade em creche para famílias do CadÚnico"),
    ("PL 386/2023", "Senado"): ({"FMCSV": 1}, "licença-maternidade para prematuros"),
    ("PL 2681/2020", "Câmara"): ({"FMCSV": 1}, "licença-maternidade a partir da alta hospitalar"),
    ("PL 2534/2015", "Câmara"): ({"FMCSV": 1}, "licença-paternidade de quinze dias"),
    ("PL 7824/2017", "Câmara"): ({"FMCSV": 1}, "licença-paternidade de quinze dias"),
    ("PDL 25/2025", "Câmara"): ({"Coletivo Feminista": -1, "Infinis": -1}, "susta a resolução 258 do Conanda, a mesma do PDL 3/2025"),
    ("PDL 35/2025", "Senado"): ({"Coletivo Feminista": -1, "Infinis": -1}, "susta a resolução 258 do Conanda, a mesma do PDL 3/2025"),
    ("PDL 138/2025", "Câmara"): ({"Coletivo Feminista": -1, "Infinis": -1}, "susta resoluções do Conanda sobre o atendimento a vítimas de violência sexual"),
    ("PDL 384/2024", "Senado"): ({"Coletivo Feminista": -1, "Infinis": -1}, "susta resolução do Conanda sobre o atendimento a vítimas de violência sexual"),
    ("PLS 333/2003", "Senado"): ({"Vital Strategies": 1}, "proíbe patrocínio ao esporte e à cultura por fabricantes de fumígenos"),
    ("PLS 420/2005", "Senado"): ({"Vital Strategies": 1}, "proíbe fumar em bares e restaurantes"),
    ("PLS 151/2007", "Senado"): ({"Vital Strategies": 1}, "advertência nos rótulos de bebidas alcoólicas"),
    ("PLS 670/2011", "Senado"): ({"Vital Strategies": 1}, "penas mais rigorosas para embriaguez ao volante"),
    ("PL 4220/2024", "Senado"): ({"Vital Strategies": 1}, "crime doloso no trânsito sob efeito de álcool ou droga"),
    ("PLS 32/2016", "Senado"): ({"Vital Strategies": 1}, "ressarcimento ao SUS por condutor sob efeito de álcool"),
    ("PL 3010/2008", "Câmara"): ({"Vital Strategies": 1}, "proíbe venda de bebida alcoólica em rodovias federais"),
    ("PL 4535/2012", "Câmara"): ({"Vital Strategies": 1}, "advertência sobre álcool, cigarro e drogas nos livros didáticos"),
    ("PLS 508/2011", "Senado"): ({"Vital Strategies": 1}, "crime vender bebida alcoólica a menores"),
    ("PLS 568/2011", "Senado"): ({"Vital Strategies": 1}, "crime vender fumígenos a menores"),
    ("PLS 81/2015", "Senado"): ({"Vital Strategies": 1}, "proíbe fumar em carro com menor de 18 anos"),
    ("PL 2898/2019", "Senado"): ({"Vital Strategies": 1}, "Cide sobre derivados do tabaco"),
    ("PL 1612/2019", "Senado"): ({"Vital Strategies": 1}, "CNH de reincidente na lei seca"),
    ("PL 2157/2019", "Senado"): ({"Vital Strategies": 1}, "prevenção ao uso de álcool, tabaco e drogas nas escolas"),
    ("PL 4710/2019", "Senado"): ({"Vital Strategies": 1}, "amplia a definição de bebida alcoólica e proíbe álcool nas escolas"),
    ("PL 2502/2023", "Senado"): ({"Vital Strategies": 1}, "restringe propaganda de bebidas alcoólicas"),
    ("PL 4605/2023", "Senado"): ({"Vital Strategies": 1}, "proíbe venda de tabaco em certos locais"),
    ("PL 2183/2019", "Senado"): ({"Vital Strategies": 1}, "Cide sobre refrigerantes e bebidas açucaradas"),
    ("PLS 8/2015", "Senado"): ({"Vital Strategies": 1}, "advertência nas embalagens de bebidas açucaradas"),
    ("PL 1066/2019", "Câmara"): ({"Vital Strategies": 1}, "advertência nas embalagens de bebidas açucaradas"),
    ("PL 2906/2019", "Câmara"): ({"Vital Strategies": 1}, "proíbe fumar em locais públicos"),
    ("PL 7466/2017", "Câmara"): ({"Vital Strategies": 1}, "imagens de advertência nas embalagens de bebidas alcoólicas"),
    ("PL 1492/2022", "Câmara"): ({"Vital Strategies": 1, "Infinis": 1}, "proíbe os dispositivos eletrônicos para fumar"),
    ("PL 2900/2023", "Câmara"): ({"Vital Strategies": 1, "Infinis": 1}, "proíbe dispositivos eletrônicos para fumar em ambientes fechados"),
    ("PL 3966/2023", "Câmara"): ({"Vital Strategies": 1, "Umane": 1}, "reduz o consumo de ultraprocessados por crianças"),
    ("PL 4501/2020", "Senado"): ({"Vital Strategies": 1, "Umane": 1}, "ultraprocessados nas escolas"),
    ("PL 4622/2023", "Senado"): ({"Vital Strategies": -1}, "inclui bebidas alcoólicas no regime fiscal das áreas de livre comércio"),
    ("PDL 383/2024", "Senado"): ({"IEPS": -1}, "susta a proibição de acolher crianças em comunidades terapêuticas"),
    ("PDL 326/2024", "Câmara"): ({"IEPS": -1}, "susta a proibição de acolher adolescentes em comunidades terapêuticas"),
    ("PLS 271/2007", "Senado"): ({"IEPS": -1}, "funcionamento de comunidades terapêuticas"),
    ("PL 1287/2019", "Câmara"): ({"IEPS": -1}, "metade do Funad para comunidades terapêuticas"),
    ("PL 3272/2021", "Câmara"): ({"IEPS": -1}, "isenta comunidades terapêuticas de taxas"),
    ("PL 3383/2021", "Senado"): ({"IEPS": 1, "Infinis": 1, "Cactus": 1}, "atenção psicossocial nas escolas (Lei 14.819/2024)"),
    ("PDL 216/2019", "Senado"): ({"IEPS": 1, "Infinis": 1, "Cactus": 1}, "susta a mudança de 2017 na política de saúde mental"),
    ("PLS 147/2013", "Senado"): ({"UNFPA": -1, "Infinis": -1}, "tira a idade como fator de inimputabilidade"),
    ("PL 621/2023", "Senado"): ({"UNFPA": -1, "Infinis": -1}, "tira a idade como fator de inimputabilidade"),
    ("PEC 1/2024", "Senado"): ({"UNFPA": -1, "Infinis": -1}, "inimputabilidade aos 16 anos em crime hediondo"),
    ("PEC 4/2019", "Senado"): ({"UNFPA": -1, "Infinis": -1}, "reduz a maioridade penal"),
    ("PEC 15/2019", "Senado"): ({"UNFPA": -1, "Infinis": -1}, "reduz a maioridade penal"),
    ("PEC 32/2019", "Senado"): ({"UNFPA": -1, "Infinis": -1}, "reduz a maioridade penal para 16 anos"),
    ("PLS 46/2017", "Senado"): ({"UNFPA": -1}, "criminaliza o aborto em qualquer fase"),
    ("PEC 49/2023", "Senado"): ({"UNFPA": -1}, "direito à vida desde o nascituro"),
    ("PLP 61/2025", "Senado"): ({"UNFPA": -1}, "proteção do nascituro pelo Ministério Público"),
    ("PL 2873/2022", "Senado"): ({"UNFPA": 1}, "crime violar o sigilo do aborto legal"),
    ("PL 3415/2019", "Câmara"): ({"UNFPA": -1}, "agrava a pena de venda de remédio abortivo"),
    ("PDL 612/2021", "Câmara"): ({"UNFPA": -1}, "susta recomendação de aborto legal por telessaúde"),
    ("PL 5011/2023", "Câmara"): ({"UNFPA": -1}, "casamento na idade núbil com assinatura de um dos pais"),
    ("PL 4968/2019", "Câmara"): ({"UNFPA": 1}, "saúde menstrual nas escolas"),
    ("PL 5474/2019", "Câmara"): ({"UNFPA": 1}, "absorventes na atenção primária"),
    ("PL 2991/2021", "Câmara"): ({"UNFPA": 1}, "absorventes na atenção primária"),
    ("PL 217/2021", "Câmara"): ({"UNFPA": 1}, "zera PIS e Cofins de absorventes"),
    ("PL 984/2025", "Senado"): ({"UNFPA": 1}, "sala de escuta sensível nos núcleos de feminicídio"),
    ("PDL 466/2026", "Senado"): ({"UNFPA": -1}, "susta o decreto de proteção das mulheres na internet"),
    ("PDL 468/2026", "Câmara"): ({"UNFPA": -1}, "susta o decreto de proteção das mulheres na internet"),
    ("PDL 406/2026", "Câmara"): ({"UNFPA": -1}, "susta o decreto de proteção das mulheres na internet"),
    ("PDL 403/2026", "Câmara"): ({"UNFPA": -1}, "susta o decreto de proteção das mulheres na internet"),
    ("PDL 469/2026", "Câmara"): ({"UNFPA": -1, "IDEC": -1}, "susta os decretos de deveres das plataformas e proteção das mulheres na internet"),
    ("PDL 410/2026", "Câmara"): ({"UNFPA": -1, "IDEC": -1}, "susta os decretos de deveres das plataformas e proteção das mulheres na internet"),
    ("PL 2616/2025", "Senado"): ({"IDEC": 1}, "telemarketing"),
    ("PL 4232/2021", "Câmara"): ({"IDEC": 1}, "cadastro e identificação de telemarketing"),
    ("PL 5332/2025", "Câmara"): ({"IDEC": 1}, "bloqueio de telemarketing e chamadas automatizadas"),
    ("PL 5304/2023", "Senado"): ({"IDEC": 1}, "saúde de trabalhadores expostos a agrotóxicos"),
    ("PL 1053/2020", "Senado"): ({"IDEC": 1}, "Cide sobre agrotóxicos"),
    ("PDL 828/2021", "Senado"): ({"IDEC": 1}, "susta o decreto de 2021 sobre agrotóxicos"),
    ("PLS 438/2011", "Senado"): ({"IDEC": 1}, "crime a venda ilegal de agrotóxicos"),
    ("PL 5090/2019", "Senado"): ({"IDEC": 1}, "amplia a proteção da saúde na lei de agrotóxicos"),
    ("PL 1297/2015", "Câmara"): ({"IDEC": 1}, "informação sobre agrotóxicos nos alimentos"),
    ("PL 5620/2019", "Câmara"): ({"IDEC": 1}, "proíbe pulverização aérea de agrotóxicos"),
    ("PDL 337/2025", "Câmara"): ({"IDEC": -1}, "susta a rastreabilidade de agrotóxicos"),
    ("PL 1542/2020", "Senado"): ({"IDEC": 1}, "suspende reajuste de medicamentos e planos de saúde"),
    ("PL 7579/2014", "Câmara"): ({"IDEC": 1}, "regulação de preços de órteses e próteses pela CMED"),
    ("PL 3395/2020", "Câmara"): ({"IDEC": -1}, "proíbe plataformas de remover conteúdo sem ordem judicial"),
    ("PL 3017/2026", "Câmara"): ({"IDEC": 1}, "atendimento humano ao consumidor"),
    ("PL 2995/2026", "Câmara"): ({"IDEC": 1}, "proteção do idoso no crédito consignado"),
    ("PL 1355/2026", "Senado"): ({"IDEC": 1}, "arrependimento do idoso em contratação digital"),
    ("PL 2360/2023", "Senado"): ({"IDEC": 1}, "veda limite de consultas no plano de saúde"),
    ("PL 1174/2024", "Senado"): ({"IDEC": 1}, "obriga a venda de planos individuais"),
    ("PL 4222/2025", "Senado"): ({"IDEC": 1}, "transparência dos reajustes de planos coletivos"),
    ("PL 315/2026", "Senado"): ({"IDEC": 1}, "aposentado mantido no plano coletivo"),
    ("PL 4261/2021", "Senado"): ({"IDEC": 1}, "portabilidade de carências"),
    ("PL 3264/2024", "Senado"): ({"IDEC": 1}, "veda rescisão do plano durante tratamento"),
    ("PL 5235/2020", "Senado"): ({"IDEC": 1}, "veda reajuste de planos em 2021"),
    ("PDL 178/2022", "Câmara"): ({"IDEC": 1}, "susta o reajuste de 15,5% dos planos individuais"),
    ("PL 6040/2019", "Senado"): ({"IDEC": 1}, "cobertura de parto para gestantes que contratam plano"),
    ("PL 7175/2017", "Câmara"): ({"IDEC": 1}, "cobertura de parto para gestantes que contratam plano"),
    ("PL 3579/2023", "Senado"): ({"IDEC": 1}, "cobertura do plano em emergência de saúde pública"),
    ("PL 1318/2020", "Câmara"): ({"IDEC": 1}, "veda corte do plano por inadimplência na pandemia"),
    ("PLS 77/2017", "Senado"): ({"IDEC": 1}, "amplia a cobertura dos planos"),
    ("PL 1073/2019", "Câmara"): ({"IDEC": 1}, "amplia a cobertura dos planos"),
    ("PL 5412/2023", "Senado"): ({"IDEC": 1}, "veda recusa e rescisão unilateral de plano"),
    ("PEC 6/2026", "Senado"): ({"REMS": 1}, "regime tributário específico para organizações esportivas sem fins lucrativos"),
    ("PEC 14/2026", "Senado"): ({"REMS": 1}, "preserva o fomento indireto estadual e municipal ao esporte no IBS"),
    ("PLP 33/2026", "Senado"): ({"REMS": 1}, "entidades esportivas sem fins lucrativos fora do corte de benefícios"),
    ("PLP 260/2026", "Senado"): ({"REMS": 1}, "reserva de recursos incentivados para o esporte de mulheres"),
    ("PLS 43/2003", "Senado"): ({"REMS": 1}, "programa de prática esportiva para prevenir violência e drogas"),
    ("PLS 62/2015", "Senado"): ({"REMS": 1}, "mais recursos para paradesporto e inclusão de estudantes com deficiência"),
    ("PL 1709/2011", "Câmara"): ({"REMS": 1}, "mais recursos para paradesporto e inclusão de estudantes com deficiência"),
    ("PLS 488/2015", "Senado"): ({"REMS": 1}, "incentivo ao desporto escolar"),
    ("PL 982/2011", "Câmara"): ({"REMS": 1}, "incentivo ao desporto escolar"),
    ("PEC 10/2018", "Senado"): ({"REMS": 1}, "percentual das emendas para o esporte"),
    ("PL 6345/2016", "Câmara"): ({"REMS": 1}, "fundos patrimoniais para entidades esportivas sem fins lucrativos"),
    ("PL 3217/2026", "Câmara"): ({"REMS": 1}, "esporte inclusivo para crianças com TEA"),
    ("PEC 17/2023", "Senado"): ({"Infinis": 1}, "segurança alimentar como direito fundamental"),
    ("PL 1018/2024", "Senado"): ({"Infinis": 1}, "prevenção à insegurança alimentar nas escolas"),
    ("PL 2754/2023", "Senado"): ({"Infinis": 1}, "reajuste do per capita da alimentação escolar pelo IPCA de alimentos"),
    ("PL 996/2023", "Senado"): ({"Infinis": 1}, "cestas básicas para a rede de acolhimento a mulheres"),
    ("PL 1751/2023", "Senado"): ({"Infinis": 1}, "per capita da merenda por indicadores socioeconômicos"),
    ("PL 3098/2020", "Senado"): ({"Infinis": 1}, "direito à cesta básica no SISAN"),
    ("PL 1/2024", "Senado"): ({"Infinis": 1}, "compra mínima da agricultura familiar no PNAE"),
    ("PL 3547/2019", "Câmara"): ({"Infinis": 1}, "cooperativas da agricultura familiar no PNAE"),
    ("PL 1234/2007", "Câmara"): ({"Infinis": 1}, "educação nutricional e segurança alimentar"),
    ("PL 3580/2023", "Senado"): ({"Infinis": 1}, "prioridade no julgamento de violência sexual contra criança"),
    ("PLS 507/2011", "Senado"): ({"Infinis": 1}, "prioridade no julgamento de violência sexual contra criança"),
    ("PL 2892/2019", "Senado"): ({"Infinis": 1}, "política de enfrentamento à violência sexual contra crianças"),
    ("PL 3225/2023", "Senado"): ({"Infinis": 1}, "Rondas Henry Borel"),
    ("PL 4772/2023", "Senado"): ({"Infinis": 1, "Cactus": 1}, "prevenção da automutilação e do suicídio no ECA"),
    ("PL 2628/2022", "Senado"): ({"Infinis": 1}, "ECA Digital"),
    ("PL 4670/2025", "Câmara"): ({"Infinis": 1, "Cactus": 1}, "núcleo psicossocial nas escolas públicas"),
    ("PDL 74/2025", "Senado"): ({"Infinis": -1, "Coletivo Feminista": -1}, "susta a resolução 258 do Conanda, a mesma do PDL 3/2025"),
    ("PDL 53/2025", "Câmara"): ({"Infinis": -1, "Coletivo Feminista": -1}, "susta a resolução 258 do Conanda, a mesma do PDL 3/2025"),
    ("PL 1773/2022", "Senado"): ({"Cactus": 1}, "prevenção do suicídio de crianças e adolescentes"),
    ("PL 5705/2023", "Senado"): ({"Cactus": 1}, "apoio psicológico à mulher em situação de violência"),
    ("PL 715/2019", "Câmara"): ({"Cactus": 1}, "prioridade no atendimento psicológico pelo SUS"),
    ("PL 3282/2025", "Câmara"): ({"Cactus": 1}, "filtragem de conteúdo de automutilação nas plataformas"),
    ("PL 5328/2025", "Câmara"): ({"Cactus": 1}, "atenção integral à pessoa com depressão"),
    ("PL 3526/2024", "Senado"): ({"Umane": 1}, "medidor contínuo de glicose pelo SUS"),
    ("PL 5586/2023", "Senado"): ({"Umane": 1}, "Estatuto da Pessoa com Obesidade"),
    ("PL 140/2020", "Senado"): ({"Umane": 1}, "atendimento na política de prevenção do diabetes"),
    ("PL 1308/2026", "Senado"): ({"Umane": 1}, "Estatuto da Pessoa com Diabetes Tipo 1"),
    ("PL 1684/2003", "Câmara"): ({"Umane": 1}, "atendimento integral à obesidade no SUS"),
    ("PL 638/2021", "Câmara"): ({"Umane": 1}, "atenção à obesidade no SUS"),
    ("PL 3055/2026", "Senado"): ({"ASBAI": 1}, "cobertura de fórmula para alergia à proteína do leite"),
    ("PL 4467/2021", "Senado"): ({"ASBAI": 1}, "recursos para pesquisa e produção de imunobiológicos"),
}
# Sinais que não estão na autoria de projeto: emendas lidas e posições documentadas no relatório "Congresso 2027:
# apoiadores e detratores das agendas do Infinis" (Eixo, 05/10/2026). (senador, {cliente: sentido}, força, texto)
SINAIS_DIRETOS = [
    ("Efraim Filho", {"Vital Strategies": -1}, "forte", "autor da Emenda 1490 ao PLP 68/2024, que tira as bebidas açucaradas do Imposto Seletivo"),
    ("Laércio Oliveira", {"Vital Strategies": -1}, "forte", "autor da Emenda 642 ao PLP 68/2024, que limita a 1% o Imposto Seletivo das bebidas açucaradas"),
    ("Eduardo Gomes", {"Infinis": -1, "Vital Strategies": -1}, "forte", "relator, com parecer favorável, do PL 5008/2023, que regulamenta os cigarros eletrônicos (CAE)"),
    ("Veneziano Vital do Rêgo", {"Infinis": 1, "IEPS": 1}, "forte", "relator no Senado do PL 3383/2021, que virou a Lei 14.819/2024 (atenção psicossocial nas escolas)"),
    ("Carlos Portinho", {"Infinis": 1, "UNFPA": 1}, "forte", "autor da Emenda 741 ao PL 4/2025 (Código Civil), que fixa 18 anos como idade mínima para o casamento"),
    ("Veneziano Vital do Rêgo", {"Vital Strategies": -1}, "forte", "autor da Emenda 1730 ao PLP 68/2024, que tira as bebidas açucaradas do Imposto Seletivo"),
    ("Magno Malta", {"Vital Strategies": -1}, "forte", "autor da Emenda 1928 ao PLP 68/2024, que tira as bebidas açucaradas do Imposto Seletivo"),
    ("Tereza Cristina", {"Vital Strategies": -1}, "forte", "autora da Emenda 1873 ao PLP 68/2024, que escalona o Imposto Seletivo das bebidas alcoólicas para não passar a carga de 2023"),
    ("Beto Faro", {"Vital Strategies": 1}, "forte", "autor da Emenda 2197 ao PLP 68/2024, que estende o Imposto Seletivo às bebidas ultraprocessadas"),
    ("Omar Aziz", {"Infinis": 1, "Vital Strategies": 1}, "evidência", "contrário à regulamentação dos cigarros eletrônicos na CAE (declaração, sinal fraco)"),
]

# Votações no próprio projeto, pela descrição do cliente: (matéria, Casa, trecho do texto da votação, sentido do Sim
# para cada cliente, rótulo).
DESCRICAO_VOTOS = [
    ("PDL 3/2025", "Câmara", "Aprovado o Projeto de Decreto Legislativo", {"Coletivo Feminista": -1},
     "PDL 3/2025, que susta a resolução 258 do Conanda"),
]


def sinais_descricao(cliente, ja_na_base, senadores, aut, votos):
    out = {n: [] for n in senadores}
    for k, casa, trecho, sentidos, rot in DESCRICAO_VOTOS:
        if cliente not in sentidos or k in ja_na_base:
            continue
        v = votos[(votos.k == k) & (votos.Casa == casa) & votos["Votação"].astype(str).str.contains(trecho, regex=False)
                  & votos.Voto.isin(["Sim", "Não"])]
        for r in v.itertuples():
            if r.Parlamentar in out:
                s = sentidos[cliente] * (1 if r.Voto == "Sim" else -1)
                out[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {rot} (plenário da {casa}, {str(r.Data)[:4]}; "
                                                       f"{'a favor' if s > 0 else 'contra'} pela descrição do cliente, a confirmar)"))
    for n, sentidos, forca, texto in SINAIS_DIRETOS:
        if cliente in sentidos and n in out:
            s = sentidos[cliente] if forca != "evidência" else 0
            out[n].append((forca, s, f"{texto} ({'a favor' if sentidos[cliente] > 0 else 'contra'} pela descrição do cliente, a confirmar)"))
    for (k, casa), (sentidos, rot) in DESCRICAO.items():
        if cliente not in sentidos or k in ja_na_base:
            continue
        s = sentidos[cliente]
        for r in aut[(aut.k == k) & (aut.Casa == casa) & aut.Papel.isin(["Primeiro autor", "Coautor"])].sort_values("Papel", ascending=False).drop_duplicates("Parlamentar").itertuples():
            if r.Parlamentar in out:
                forte = r.Papel == "Primeiro autor"
                out[r.Parlamentar].append(("forte" if forte else "médio", s,
                    f"{'autor' if forte else 'coautor'} do {k} ({rot}; {'a favor' if s > 0 else 'contra'} pela descrição do cliente, a confirmar)"))
    return out


def sinais_pauta(pauta, a_favor, senadores, aut, com):
    """Sinais do senador na pauta (Senado), com o sentido para o cliente: a favor da pauta mantém, contra inverte."""
    from outros.novos_eleitos.e23_parlamentares_senado import painel, chave
    cfg, out = PAUTAS[pauta], {n: [] for n in senadores}
    lado = 1 if a_favor else -1
    nomes = {chave(n): n for n in senadores}
    for data, mat, (si, no), sentido, rot in cfg["votacoes"]:
        sig, num = mat.split()
        v = next(v for v in painel(data) if v["siglaTipoMateria"] == sig and v["numeroMateria"] == int(num)
                 and v["qtdVotosSim"] == si and v["qtdVotosNao"] == no)
        for p in v["votosParlamentar"]:
            n = nomes.get(chave(p["nomeParlamentar"]))
            voto = (p.get("voto") or "").upper()
            if n and voto in ("SIM", "NÃO"):
                s = sentido * lado * (1 if voto == "SIM" else -1)
                out[n].append(("forte", s, f"votou {voto.capitalize().replace('Nao', 'Não')} na {rot} (plenário do Senado, {data[:4]}; pauta {pauta.lower()})"))
    for data, colegiado, mat, sentido, rot in cfg["comissao"]:
        for r in com[(com.Data == data) & (com.Colegiado == colegiado) & (com["Matéria"] == mat)].itertuples():
            if r.Parlamentar in out and r.Voto in ("Sim", "Não"):
                s = sentido * lado * (1 if r.Voto == "Sim" else -1)
                out[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {rot} ({data[:4]}; pauta {pauta.lower()})"))
    for r in aut[aut.k.isin(cfg["autoria"]) & aut.Papel.isin(["Primeiro autor", "Coautor"])].sort_values("Papel", ascending=False).drop_duplicates(["Parlamentar", "k"]).itertuples():
        if r.Parlamentar in out:
            forte = r.Papel == "Primeiro autor"
            out[r.Parlamentar].append(("forte" if forte else "médio", lado,
                f"{'autor' if forte else 'coautor'} do {r.k} ({cfg['autoria'][r.k]}; pauta {pauta.lower()})"))
    return out


def placar(texto):
    """Sim e Não do resultado oficial, nos formatos da Câmara (plenário e comissões)."""
    t = str(texto)
    for rx in (r"Sim:?\s*(\d+);\s*[Nn]ão:?\s*(\d+)", r"(\d+) votos \"Sim\", (\d+) votos \"Não\"", r"Sim,\s*(\d+);\s*Não,\s*(\d+)"):
        m = re.search(rx, t)
        if m:
            return int(m.group(1)), int(m.group(2))
    m = re.search(r"Sim:?\s*(\d+);\s*Total", t)
    return (int(m.group(1)), 0) if m else None


def dividiu(r, amostra):
    """Votação de consenso não entra como posição. Vale o placar oficial do texto (Câmara); no Senado, as colunas
    Sim e Não; sem nenhum dos dois, se houve Sim e Não entre os parlamentares da base."""
    p = placar(r.Votação)
    if p is None:
        try:
            p = int(float(r.Sim)), int(float(getattr(r, "Não")))
        except (TypeError, ValueError, AttributeError):
            p = None
    if p is None:
        return amostra.get((r.k, str(r.Data), str(r.Colegiado), str(r.Votação)), False)
    return min(p) > 0


def classifica(fp, fn, mp, mn):
    if fp and fn:
        d = fp - fn
        if abs(d) >= 2:
            return "Promotor" if d > 0 else "Detrator"
        if d == 1 and mp > mn:
            return "Promotor"
        if d == -1 and mn > mp:
            return "Detrator"
        return "Neutro"
    if fp >= 2 or (fp == 1 and mp >= 1):
        return "Promotor"
    if fn >= 2 or (fn == 1 and mn >= 1):
        return "Detrator"
    return "Não tem informação suficiente"


def main():
    base = c.ler_csv("temas_clientes/base_pautas.csv")
    comp = c.ler_csv("temas_clientes/posicoes_clientes_complemento.csv")
    base["Fonte da posição"] = "Coordenação"
    for r in comp.itertuples():
        m = (base.Cliente == r.Cliente) & (base.Projeto == r.Projeto)
        base.loc[m, "Para o cliente"] = r._5
        base.loc[m, "Fonte da posição"] = f"{r.Fonte}, {r.Situação.lower()}"
    senadores = pd.concat([c.ler_csv(f"temas_clientes/perfil_{g}.csv")[["Parlamentar", "Partido", "UF"]] for g in GRUPOS]).drop_duplicates()
    ficha = c.ler_csv("temas_clientes/parlamentares.csv").set_index("Senador")

    aut = pd.concat([c.ler_csv(f"perfil_senado_{g}/autoria.csv").assign(Casa="Senado") for g in GRUPOS]
                    + [c.ler_csv("perfil_camara_senado/autoria.csv").assign(Casa="Câmara")])
    rel = pd.concat([c.ler_csv(f"perfil_senado_{g}/relatorias.csv").assign(Casa="Senado") for g in GRUPOS]
                    + [c.ler_csv("perfil_camara_senado/relatorias.csv").assign(Casa="Câmara")])
    vot = pd.concat([c.ler_csv(f"perfil_senado_{g}/votos.csv").assign(Casa="Senado", Colegiado="plenário do Senado") for g in ("curso", "reeleitos")]
                    + [c.ler_csv("perfil_senado_comissoes/votos.csv").assign(Casa="Senado")]
                    + [c.ler_csv("perfil_camara_senado/votos.csv").assign(Casa="Câmara")])
    for d in (aut, rel, vot):
        d["k"] = d["Matéria"].astype(str).str.extract(r"^(\S+ \d+/\d{4})")[0]
    votos_todos = vot.copy()
    vot = vot[vot["Votação"].astype(str).str.contains(VOTO_PROJETO, regex=True)]
    # votação de consenso não entra como posição: sem nenhum Não (ou nenhum Sim) entre os parlamentares da base
    amostra = vot.groupby(["k", vot.Data.astype(str), vot.Colegiado.astype(str), vot["Votação"].astype(str)]).Voto.agg(
        lambda x: bool((x == "Sim").any() and (x == "Não").any())).to_dict()
    vot = vot[[dividiu(r, amostra) for r in vot.itertuples()]]

    linhas, resumo = [], []
    for (area, cliente), g in base.groupby(["Área", "Cliente"], sort=False):
        marcados = g[g["Para o cliente"].isin(["Promotor", "Detrator"])]
        # sem marcação (Dúvida ou em branco): vale o sentido da posição do cliente na pauta, a confirmar
        pela_pauta = g[~g["Para o cliente"].isin(["Promotor", "Detrator"]) & g["Cliente é"].isin(["A favor", "Contra"])].copy()
        pela_pauta["Para o cliente"] = pela_pauta["Cliente é"].map({"A favor": "Promotor", "Contra": "Detrator"})
        pela_pauta["Fonte da posição"] = "pauta"
        sinais = {n: [] for n in senadores.Parlamentar}
        for p in pd.concat([marcados, pela_pauta]).itertuples():
            sentido = 1 if p._6 == "Promotor" else -1
            quem = f"{p._6.lower()} para o cliente" + (", pela posição na pauta; marcação da coordenação pendente" if p._8 == "pauta" else "")
            for k, casa in chaves(p.Projeto):
                ok = lambda d: (d.k == k) & ((d.Casa == casa) if casa else True)
                for r in aut[ok(aut) & aut.Papel.isin(["Primeiro autor", "Coautor"])].sort_values("Papel", ascending=False).drop_duplicates("Parlamentar").itertuples():
                    if r.Parlamentar in sinais:
                        forte = r.Papel == "Primeiro autor"
                        sinais[r.Parlamentar].append(("forte" if forte else "médio", sentido,
                            f"{'autor' if forte else 'coautor'} do {k} ({quem})"))
                for r in vot[ok(vot) & vot.Voto.isin(["Sim", "Não"])].itertuples():
                    if r.Parlamentar in sinais:
                        s = sentido if r.Voto == "Sim" else -sentido
                        onde = r.Colegiado if r.Colegiado != "PLEN" else "plenário da Câmara"
                        sinais[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {k} ({onde}, {str(r.Data)[:4]}; projeto {quem})"))
                for r in rel[ok(rel)].drop_duplicates(["Parlamentar", "Colegiado"]).itertuples():
                    if r.Parlamentar in sinais:
                        sinais[r.Parlamentar].append(("evidência", 0, f"relator do {k} na {r.Colegiado} (sentido do parecer não lido)"))
        com_votos = c.ler_csv("perfil_senado_comissoes/votos.csv")
        for pt in g.drop_duplicates("Pauta").itertuples():
            if pt.Pauta in PAUTAS and pt._4 in ("A favor", "Contra"):
                for n, s_ in sinais_pauta(pt.Pauta, pt._4 == "A favor", list(sinais), aut, com_votos).items():
                    sinais[n] += s_
        ja_na_base = {k for p in g.Projeto for k, _ in chaves(p)}
        for n, s_ in sinais_descricao(cliente, ja_na_base, list(sinais), aut, votos_todos).items():
            sinais[n] += s_
        com_registro = 0
        for n, s in sinais.items():
            if not s:
                continue
            com_registro += 1
            f = [x for k, x, _ in s if k == "forte"]
            m = [x for k, x, _ in s if k == "médio"]
            fp, fn, mp, mn = f.count(1), f.count(-1), m.count(1), m.count(-1)
            rot = (("forte a favor", "fortes a favor", fp), ("forte contra", "fortes contra", fn),
                   ("médio a favor", "médios a favor", mp), ("médio contra", "médios contra", mn))
            sinal_txt = "; ".join(f"{q} {um if q == 1 else varios}" for um, varios, q in rot if q) or "só relatoria"
            linhas.append({"Área": area, "Cliente": cliente, "Senador": n,
                           "Partido": ficha.loc[n, "Partido"], "UF": ficha.loc[n, "UF"], "Cadeira": ficha.loc[n, "Cadeira"],
                           "Classificação": classifica(fp, fn, mp, mn),
                           "Sinais": sinal_txt,
                           "Evidências": "\n".join(dict.fromkeys(t for _, _, t in s))})
        pautas = g.drop_duplicates("Pauta")
        resumo.append({"Área": area, "Cliente": cliente,
                       "Pautas": "; ".join(f"{r.Pauta} ({r._4.lower() or 'posição não informada'})" for r in pautas.itertuples()),
                       "Promotores": int((marcados["Para o cliente"] == "Promotor").sum()),
                       "Detratores": int((marcados["Para o cliente"] == "Detrator").sum()),
                       "Aguardando": int((~g["Para o cliente"].isin(["Promotor", "Detrator"])).sum()),
                       "Senadores com registro": com_registro,
                       "Posição fora da planilha": "; ".join(f"{r.Projeto}: {r._8}" for r in g.itertuples() if r._8 != "Coordenação")})
    c.salvar_csv(pd.DataFrame(linhas), "temas_clientes/clientes_classificacao.csv")
    c.salvar_csv(pd.DataFrame(resumo), "temas_clientes/clientes_resumo.csv")
    print(pd.DataFrame(linhas).groupby(["Cliente", "Classificação"]).size().unstack(fill_value=0).to_string())


if __name__ == "__main__":
    main()
