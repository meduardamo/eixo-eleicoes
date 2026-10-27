"""Etapa 24. Classificação dos 81 senadores para cada cliente, a partir da base de pautas dos coordenadores.

Entrada: temas_clientes/base_pautas.csv (cópia da planilha "Eixo | Base de pautas por cliente", abas Clientes
Educação, Saúde e Esporte) e temas_clientes/posicoes_clientes_complemento.csv (posição informada fora da planilha,
a confirmar com a coordenação). Entram os projetos marcados Promotor ou Detrator e, quando a marcação está em Dúvida
ou em branco, os de pauta com posição A favor ou Contra, no sentido dessa posição (a confirmar); Depende do texto fica fora,
e projeto marcado Neutro também.

Sinais do senador em cada projeto (Senado e, para quem foi deputado, Câmara):
- forte: primeiro autor; voto nominal no próprio projeto ou no parecer dele (Sim conta no sentido do projeto,
  Não no sentido contrário), desde que a votação tenha dividido os parlamentares da base (consenso não conta). Votação de emenda, destaque ou requerimento não conta, porque o sentido para o
  cliente depende do texto da emenda, salvo emenda com o texto lido (DESCRICAO_VOTOS); autor de emenda lida (SINAIS_DIRETOS);
  relator com parecer lido pela aprovação (PARECER_LIDO);
- médio: coautor (de PEC não conta: assinatura de PEC é apoiamento, aparece só como evidência);
- só evidência, sem contar: relatoria com parecer não lido.
Projeto Promotor dá sinal a favor do cliente; Detrator, contra.

Régua (a mesma da Metodologia): Promotor ou Detrator com dois sinais fortes na mesma direção ou um forte e um médio;
fortes nos dois sentidos: diferença de dois decide, de um decide com médio no mesmo sentido, senão Neutro;
abaixo disso, Um sinal a favor, Um sinal contra, Sinais opostos ou Registro sem sinal (só evidência que não conta).

PEC tem numeração própria em cada Casa, e projeto de lei também até 2018: nesses casos a Casa da matéria tem de
bater com a anotada na base de pautas.

Saída: temas_clientes/clientes_classificacao.csv e clientes_resumo.csv
Rodar: python -m outros.novos_eleitos.e24_clientes_senado   (depois da 23)
"""
import re
from difflib import SequenceMatcher

import pandas as pd

from outros.novos_eleitos import comum as c

GRUPOS = ("curso", "reeleitos", "novos")
VOTO_PROJETO = r"Votação nominal d[oa] (?:Projeto|Proposta|PEC|Substitutivo)|Votação do Projeto|Aprovad[oa] o (?:Parecer|Substitutivo|Projeto)|Rejeitad[oa] o (?:Parecer|Substitutivo|Projeto)|Subemenda Substitutiva Global|nos termos do parecer"


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


# Clientes do raspador do DOU (CLIENT_THEME_DATA) e do PDF de descrição que a base de pautas não tem.
# Manual e Mevo estão no PDF, mas não são mais clientes (Eduarda, 15/10/2026).
CLIENTES_FORA_DA_BASE = {
    "MegaEdu": ("Educação", "Conectividade das escolas e FUST"),
    "Fundação Itaú": ("Educação", "Acesso de jovens à educação profissional e tecnológica"),
    "Desiderata": ("Saúde", "Obesidade infantil, ultraprocessados e alimentação escolar"),
    # cliente do painel eleitoral 01; sem descrição no PDF: pauta pela missão pública da fundação e pelo pedido nos planos (06/10)
    "Fundação Lemann": ("Educação", "Alfabetização, aprendizagem, anos finais, professores, gestão escolar e conectividade"),
}

# O que faz cada projeto da base de pautas, para a evidência não sair só com o número (na falta, vai o nome da pauta).
ROTULO_BASE = {
    "PEC 36/2025": "ICMS educacional para expandir o tempo integral",
    "PL 3455/2024": "estende o Pé-de-Meia a mais alunos do ensino médio",
    "PL 2556/2023": "gestão democrática na educação básica",
    "PL 3385/2021": "programa emergencial de aprendizagem",
    "PL 5299/2025": "composição da Câmara de Educação Básica do CNE",
    "PL 1338/2022": "regulamenta a educação domiciliar",
    "RQS 495/2026": "urgência para o PL 1338/2022, de educação domiciliar",
    "PL 3262/2019": "tira a educação domiciliar do crime de abandono intelectual",
    "PL 1924/2025": "Política Nacional Integrada da Primeira Infância",
    "PL 4282/2025": "Política Nacional Integrada da Primeira Infância",
    "PL 2294/2024": "Exame Nacional de Proficiência em Medicina",
    "PL 2470/2026": "saúde mental e proteção do apostador nas bets",
    "PL 4/2025": "reforma do Código Civil",
    "PL 278/2026": "regime tributário dos datacenters (Redata)",
    "PL 2338/2023": "marco da inteligência artificial",
    "PL 4501/2020": "proíbe ultraprocessados nas escolas",
    "PEC 32/2015": "maioridade penal aos 16 anos",
    "PLP 11/2026": "entidades sem fins lucrativos fora do corte de benefícios fiscais",
    "PEC 14/2026": "fomento indireto ao esporte no IBS",
    "PL 7419/2006": "lei dos planos de saúde",
    "PL 6622/2025": "prevenção ao casamento infantil",
    "PL 3080/2020": "marco legal do autismo",
}

# Relatorias da base de pautas com o parecer lido: (matéria, relator) -> o que o parecer faz. Parecer pela aprovação
# conta como Sim no projeto (forte); as outras relatorias seguem como evidência.
PARECER_LIDO = {
    # os dois mantêm o art. 2º da Câmara ("primeira infância ... desde a gestação"), só com emendas de redação (lidos em 27/10/2026)
    ("PL 1924/2025", "Damares Alves"): "parecer pela aprovação, mantida a gestação na definição de primeira infância",
    ("PL 1924/2025", "Humberto Costa"): "parecer pela aprovação, mantida a gestação na definição de primeira infância",
}

# Sinais na pauta, além dos projetos listados: votações nominais do Senado que separam os senadores na pauta e
# projetos de autoria cujo sentido a ementa mostra. sentido +1 = Sim vai na direção de quem é a favor da pauta.
PAUTAS = {
    "Apostas online e saúde mental": {
        "lado": "o cliente defende restringir as apostas",
        "lado_contra": "o cliente defende ampliar as apostas",
        "votacoes": [("2023-12-12", "PL 3626", (37, 27), +1, "Emenda 139 à lei das bets, que tirava os jogos on-line e o cassino virtual da lei (Sim = restringir as apostas)"),
                     ("2023-12-12", "PL 3626", (25, 36), +1, "Emenda 157 à lei das bets, que proibia publicidade de apostas em arenas e patrocínio (Sim = restringir as apostas)"),
                     ("2025-12-17", "RQS 857", (28, 36), -1, "urgência do PL 2234, que legaliza cassinos e jogos de azar (Sim = acelerar a legalização)")],
        "comissao": [("2024-06-19", "CCJ", "PL 2234/2022", -1, "relatório do PL 2234 na CCJ, que legaliza cassinos e jogos de azar (Sim = aprovar a legalização)")],
        "autoria": {"PL 2372/2025": "proíbe toda publicidade de apostas", "PL 2662/2025": "crime de operar e divulgar apostas virtuais",
                    "PL 2985/2023": "proíbe publicidade de apostas", "PL 3718/2024": "limitação das apostas",
                    "PL 3754/2025": "idade mínima de 21 anos para apostar", "PL 3765/2026": "limite mensal de depósitos em apostas",
                    "PL 4219/2024": "limite mensal de depósitos em apostas", "PL 626/2023": "veda pagamentos para jogos de azar",
                    "PL 3587/2024": "operador de apostas com representação legal no país"},
    },
}


# Sinais pela descrição do cliente (PDF "Monitor Legislativo e DOU - Segunda Etapa" e CLIENT_THEME_DATA do raspador
# do DOU), com o sentido lido na ementa e conferido no inteiro teor (texto e justificação) em 15/10/2026: +1 a favor do cliente, -1 contra. Ficam de fora as ementas que não mostram
# o sentido e os projetos que já estão na base de pautas do cliente, que contam por lá. A confirmar com a
# coordenação. Chave: (matéria, Casa da autoria).
DESCRICAO = {
    ("PL 707/2015", "Câmara"): ({"ISG": 1, "IU": 1}, "ensino em tempo integral"),
    ("PL 1228/2015", "Câmara"): ({"IU": 1}, "fundo de educação ambiental"),
    ("PLP 243/2023", "Senado"): ({"ISG": 1, "IU": 1}, "permanência no ensino médio"),
    ("PEC 36/2025", "Senado"): ({"ISG": 1}, "ensino em tempo integral"),
    ("PL 1311/2023", "Senado"): ({"IU": 1}, "gestão democrática na distribuição do Fundeb"),
    ("PL 4414/2024", "Senado"): ({"IU": 1}, "magistério indígena e do campo"),
    ("PL 3756/2023", "Senado"): ({"IU": 1, "ISG": 1}, "bolsa permanência na educação básica"),
    ("PL 3166/2023", "Senado"): ({"ISG": 1, "IU": 1}, "bolsa para estudantes do ensino médio"),
    ("PL 1070/2021", "Senado"): ({"IU": 1}, "educação ambiental"),
    ("PL 3097/2021", "Senado"): ({"IU": 1}, "educação ambiental"),
    ("PL 4270/2020", "Câmara"): ({"IU": 1}, "educação ambiental"),
    ("PL 5264/2025", "Câmara"): ({"IU": 1}, "insalubridade para o magistério"),
    ("PL 1989/2023", "Senado"): ({"IU": 1, "Fundação Lemann": 1}, "piso do magistério"),
    ("PL 4372/2020", "Câmara"): ({"IU": 1}, "regulamentação do Fundeb"),
    ("PL 3418/2021", "Câmara"): ({"IU": 1}, "atualização da lei do Fundeb"),
    ("PL 5665/2023", "Senado"): ({"IU": 1}, "prorrogação do PNE"),
    ("PL 3562/2020", "Câmara"): ({"IU": 1}, "Conselho Nacional de Educação"),
    ("PL 1884/2023", "Senado"): ({"IU": 1, "MegaEdu": 1, "Fundação Lemann": 1}, "internet nas escolas"),
    ("PL 810/2022", "Câmara"): ({"IU": 1, "MegaEdu": 1, "Fundação Lemann": 1}, "internet nas escolas"),
    ("PLP 59/2021", "Senado"): ({"IU": 1, "MegaEdu": 1, "Fundação Lemann": 1}, "internet nas escolas"),
    ("PL 3853/2020", "Senado"): ({"IU": 1, "MegaEdu": 1, "Fundação Lemann": 1}, "tablets e dados para estudantes"),
    ("PL 4978/2026", "Senado"): ({"IU": 1, "ISG": 1}, "educação étnico-racial e quilombola"),
    ("PL 3467/2025", "Senado"): ({"IU": 1, "Fundação Lemann": 1}, "apoio às redes com baixo desempenho e alta vulnerabilidade"),
    ("PL 597/2022", "Câmara"): ({"IU": 1, "ISG": 1, "Fundação Lemann": 1}, "recomposição da alfabetização"),
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
    ("PDL 138/2025", "Câmara"): ({"Infinis": -1}, "susta as diretrizes do Conanda de proteção a adolescentes no sistema socioeducativo"),
    ("PDL 384/2024", "Senado"): ({"Infinis": -1}, "susta as diretrizes do Conanda de proteção a adolescentes no sistema socioeducativo"),
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
    ("PL 2183/2019", "Senado"): ({"Vital Strategies": 1, "Desiderata": 1}, "Cide sobre refrigerantes e bebidas açucaradas"),
    ("PLS 8/2015", "Senado"): ({"Vital Strategies": 1, "Desiderata": 1, "IDEC": 1}, "advertência nas embalagens de bebidas açucaradas"),
    ("PL 1066/2019", "Câmara"): ({"Vital Strategies": 1, "Desiderata": 1, "IDEC": 1}, "advertência nas embalagens de bebidas açucaradas"),
    ("PL 2906/2019", "Câmara"): ({"Vital Strategies": 1}, "proíbe fumar em locais públicos"),
    ("PL 7466/2017", "Câmara"): ({"Vital Strategies": 1}, "imagens de advertência nas embalagens de bebidas alcoólicas"),
    ("PL 1492/2022", "Câmara"): ({"Vital Strategies": 1, "Infinis": 1}, "proíbe os dispositivos eletrônicos para fumar"),
    ("PL 2900/2023", "Câmara"): ({"Vital Strategies": 1, "Infinis": 1}, "proíbe dispositivos eletrônicos para fumar em ambientes fechados"),
    ("PL 3966/2023", "Câmara"): ({"Vital Strategies": 1, "Umane": 1, "Desiderata": 1, "IDEC": 1}, "reduz o consumo de ultraprocessados por crianças"),
    ("PL 4501/2020", "Senado"): ({"Vital Strategies": 1, "Umane": 1, "Desiderata": 1}, "ultraprocessados nas escolas"),
    ("PL 4622/2023", "Senado"): ({"Vital Strategies": -1}, "inclui bebidas alcoólicas no regime fiscal das áreas de livre comércio"),
    ("PDL 383/2024", "Senado"): ({"IEPS": -1, "Cactus": -1, "Infinis": -1}, "susta a proibição de acolher crianças em comunidades terapêuticas"),
    ("PDL 326/2024", "Câmara"): ({"IEPS": -1, "Cactus": -1, "Infinis": -1}, "susta a proibição de acolher adolescentes em comunidades terapêuticas"),
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
    ("PLS 46/2017", "Senado"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "criminaliza o aborto em qualquer fase"),
    ("PEC 49/2023", "Senado"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "direito à vida desde o nascituro"),
    ("PLP 61/2025", "Senado"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "proteção do nascituro pelo Ministério Público"),
    ("PL 2873/2022", "Senado"): ({"UNFPA": 1, "Coletivo Feminista": 1}, "crime violar o sigilo do aborto legal"),
    ("PL 3415/2019", "Câmara"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "agrava a pena de venda de remédio abortivo"),
    ("PDL 612/2021", "Câmara"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "susta recomendação de aborto legal por telessaúde"),
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
    ("PLS 77/2017", "Senado"): ({"IDEC": 1, "FMCSV": 1}, "cobertura do plano ao recém-nascido nos primeiros 90 dias"),
    ("PL 1073/2019", "Câmara"): ({"IDEC": 1, "FMCSV": 1}, "cobertura do plano ao recém-nascido nos primeiros 90 dias"),
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
    ("PL 1018/2024", "Senado"): ({"Infinis": 1, "Umane": 1}, "prevenção à insegurança alimentar nas escolas"),
    ("PL 2754/2023", "Senado"): ({"Infinis": 1, "Desiderata": 1, "IDEC": 1}, "reajuste do per capita da alimentação escolar pelo IPCA de alimentos"),
    ("PL 996/2023", "Senado"): ({"Infinis": 1}, "cestas básicas para a rede de acolhimento a mulheres"),
    ("PL 1751/2023", "Senado"): ({"Infinis": 1, "Desiderata": 1, "IDEC": 1}, "per capita da merenda por indicadores socioeconômicos"),
    ("PL 3098/2020", "Senado"): ({"Infinis": 1}, "direito à cesta básica no SISAN"),
    ("PL 1/2024", "Senado"): ({"Infinis": 1, "IDEC": 1, "Desiderata": 1}, "compra mínima da agricultura familiar no PNAE"),
    ("PL 3547/2019", "Câmara"): ({"Infinis": 1, "IDEC": 1, "Desiderata": 1}, "cooperativas da agricultura familiar no PNAE"),
    ("PL 1234/2007", "Câmara"): ({"Infinis": 1, "IDEC": 1}, "educação nutricional e segurança alimentar"),
    ("PL 3580/2023", "Senado"): ({"Infinis": 1}, "prioridade no julgamento de violência sexual contra criança"),
    ("PLS 507/2011", "Senado"): ({"Infinis": 1}, "prioridade no julgamento de violência sexual contra criança"),
    ("PL 2892/2019", "Senado"): ({"Infinis": 1}, "política de enfrentamento à violência sexual contra crianças"),
    ("PL 3225/2023", "Senado"): ({"Infinis": 1}, "Rondas Henry Borel"),
    ("PL 4772/2023", "Senado"): ({"Infinis": 1, "Cactus": 1, "IEPS": 1}, "prevenção da automutilação e do suicídio no ECA"),
    ("PL 2628/2022", "Senado"): ({"Infinis": 1}, "ECA Digital"),
    ("PL 4670/2025", "Câmara"): ({"Infinis": 1, "Cactus": 1, "IEPS": 1}, "núcleo psicossocial nas escolas públicas"),
    ("PDL 74/2025", "Senado"): ({"Infinis": -1, "Coletivo Feminista": -1}, "susta a resolução 258 do Conanda, a mesma do PDL 3/2025"),
    ("PDL 53/2025", "Câmara"): ({"Infinis": -1, "Coletivo Feminista": -1}, "susta a resolução 258 do Conanda, a mesma do PDL 3/2025"),
    ("PL 1773/2022", "Senado"): ({"Cactus": 1}, "prevenção do suicídio de crianças e adolescentes"),
    ("PL 5705/2023", "Senado"): ({"Cactus": 1}, "apoio psicológico à mulher em situação de violência"),
    ("PL 715/2019", "Câmara"): ({"Cactus": 1}, "prioridade no atendimento psicológico pelo SUS"),
    ("PL 3282/2025", "Câmara"): ({"Cactus": 1}, "filtragem de conteúdo de automutilação nas plataformas"),
    ("PL 5328/2025", "Câmara"): ({"Cactus": 1}, "atenção integral à pessoa com depressão"),
    ("PL 3526/2024", "Senado"): ({"Umane": 1}, "medidor contínuo de glicose pelo SUS"),
    ("PL 5586/2023", "Senado"): ({"Umane": 1, "Desiderata": 1}, "Estatuto da Pessoa com Obesidade"),
    ("PL 140/2020", "Senado"): ({"Umane": 1}, "atendimento na política de prevenção do diabetes"),
    ("PL 1308/2026", "Senado"): ({"Umane": 1}, "Estatuto da Pessoa com Diabetes Tipo 1"),
    ("PL 1684/2003", "Câmara"): ({"Umane": 1, "Desiderata": 1}, "atendimento integral à obesidade no SUS"),
    ("PL 638/2021", "Câmara"): ({"Umane": 1, "Desiderata": 1}, "atenção à obesidade no SUS"),
    ("PL 3055/2026", "Senado"): ({"ASBAI": 1}, "cobertura de fórmula para alergia à proteína do leite"),
    ("PL 4467/2021", "Senado"): ({"ASBAI": 1}, "recursos para pesquisa e produção de imunobiológicos"),
    ("PEC 196/2016", "Câmara"): ({"FMCSV": 1}, "30 dias a mais de licença-maternidade por filho em parto múltiplo"),
    ("PL 2218/2019", "Câmara"): ({"FMCSV": 1}, "licença-paternidade de 20 dias para policiais e bombeiros militares"),
    ("PL 2679/2025", "Câmara"): ({"FMCSV": 1}, "licença-paternidade contada da alta da mãe ou do bebê"),
    ("PL 2786/2019", "Câmara"): ({"FMCSV": 1}, "licença-paternidade de até 90 dias"),
    ("PL 1575/2022", "Senado"): ({"IDEC": 1}, "rol da ANS exemplificativo"),
    ("PDC 341/2016", "Câmara"): ({"IDEC": 1}, "susta reajuste de 12,5% dos medicamentos acima da inflação"),
    ("PL 4231/2019", "Senado"): ({"IDEC": 1}, "limita novos registros de agrotóxicos"),
    ("PL 494/2022", "Senado"): ({"IDEC": 1}, "reavaliação periódica de agrotóxicos"),
    ("PL 1838/2022", "Câmara"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "exige autorização judicial para o aborto legal em caso de estupro"),
    ("PL 1648/2023", "Senado"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "comunicação à polícia de aborto autoprovocado"),
    # 27/10/2026: aborto e nascituro fora de qualquer lista até aqui
    ("PL 1904/2024", "Câmara"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "equipara ao homicídio o aborto após 22 semanas, inclusive em caso de estupro"),
    ("PEC 29/2015", "Senado"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "inviolabilidade do direito à vida desde a concepção"),
    # protocolos de acolhimento à vítima de violência sexual em lugares de lazer (inteiro teor lido): agenda do UNFPA;
    # o Coletivo fica de fora, a agenda declarada dele é aborto, contracepção e autodeterminação reprodutiva
    ("PL 999/2023", "Câmara"): ({"UNFPA": 1}, "protocolo de acolhimento às vítimas de violência sexual em lugares de lazer"),
    ("PL 544/2023", "Senado"): ({"UNFPA": 1}, "Protocolo Não Nos Calaremos contra a violência sexual e de gênero em lugares de lazer"),
    ("PDL 1154/2025", "Câmara"): ({"Infinis": -1, "UNFPA": -1}, "susta a resolução Conanda/CNDM que reconhece a violência vicária"),
    ("PL 3000/2025", "Senado"): ({"Vital Strategies": 1}, "destruição de máquinas de cigarro ilegal (protocolo da OMS)"),
    ("PL 5050/2013", "Câmara"): ({"Vital Strategies": 1}, "fotos de acidente nas embalagens de bebida alcoólica"),
    ("PL 3348/2012", "Câmara"): ({"Infinis": 1, "Vital Strategies": 1, "IDEC": 1, "Umane": 1, "Desiderata": 1}, "proíbe venda de alimentos de baixo teor nutricional nas escolas"),
    ("PL 354/2022", "Senado"): ({"Infinis": 1}, "Benefício de Erradicação da Fome"),
    ("PL 1971/2011", "Câmara"): ({"Infinis": 1, "Desiderata": 1}, "recálculo do PNAE quando as matrículas crescem"),
    ("PDL 521/2020", "Câmara"): ({"IU": 1}, "susta a redução de 8% do valor mínimo por aluno do Fundeb"),
    ("PDL 522/2020", "Senado"): ({"IU": 1}, "susta a redução de 8% do valor mínimo por aluno do Fundeb"),
    ("PL 2759/2015", "Câmara"): ({"IU": 1, "Fundação Lemann": 1}, "participação da comunidade escolar na escolha do diretor"),
    ("PL 3760/2026", "Senado"): ({"IU": 1}, "Fator Amazônico no financiamento da educação básica"),
    ("PL 3762/2026", "Senado"): ({"IU": 1, "Fundação Lemann": 1}, "Política Nacional Escola das Adolescências"),
    ("PLS 649/2015", "Senado"): ({"Cactus": -1, "IEPS": -1}, "obriga informar ao empregador o CID de transtorno mental do paciente"),
    ("PL 2935/2011", "Câmara"): ({"MegaEdu": 1, "IDEC": 1}, "recursos do FUST para internet de alunos e professores da rede pública"),
    ("PL 4394/2020", "Câmara"): ({"MegaEdu": 1}, "Programa Banda Larga Estudantil"),
    ("PL 263/2024", "Senado"): ({"MegaEdu": 1, "IDEC": 1}, "Programa Nacional de Conectividade com recursos do FUST"),
    ("PL 4942/2025", "Câmara"): ({"MegaEdu": 1}, "Plano Nacional de Inclusão Digital"),
    ("PL 3781/2023", "Senado"): ({"MegaEdu": -1}, "revoga a contribuição dos consumidores ao FUST"),
    ("PL 3349/2025", "Senado"): ({"Fundação Itaú": 1, "ISG": 1}, "poupança para estudantes de baixa renda da educação profissional de nível médio"),
    ("PL 3785/2026", "Câmara"): ({"Fundação Itaú": 1}, "Programa Conecta Trabalho, acesso à educação profissional"),
    ("PL 4744/2023", "Senado"): ({"Fundação Itaú": 1}, "patrocínio a estudantes da educação profissional e superior"),
    ("PL 7395/2010", "Câmara"): ({"Fundação Itaú": 1}, "Programa de Acesso ao Ensino Técnico"),
    ("PL 1288/2011", "Câmara"): ({"Fundação Itaú": 1}, "Programa de Acesso ao Ensino Técnico"),
    ("PL 4979/2026", "Senado"): ({"Fundação Itaú": 1}, "transporte para estudantes da rede federal de educação profissional"),
    ("PL 3118/2024", "Senado"): ({"Fundação Itaú": 1}, "assistência a estudantes da educação profissional federal com o Fundo Social"),
    ("PL 2813/2023", "Câmara"): ({"Fundação Itaú": 1}, "vaga no ensino noturno para trabalhadores e aprendizes"),
}
# Sinais que não estão na autoria de projeto: emendas lidas e posições documentadas no relatório "Congresso 2027:
# apoiadores e detratores das agendas do Infinis" (Eixo, 05/10/2026). (senador, {cliente: sentido}, força, texto)
SINAIS_DIRETOS = [
    ("Efraim Filho", {"Vital Strategies": -1, "Desiderata": -1}, "forte", "autor da Emenda 1490 ao PLP 68/2024, que tira as bebidas açucaradas do Imposto Seletivo"),
    ("Laércio Oliveira", {"Vital Strategies": -1, "Desiderata": -1}, "forte", "autor da Emenda 642 ao PLP 68/2024, que limita a 1% o Imposto Seletivo das bebidas açucaradas"),
    ("Eduardo Gomes", {"Infinis": -1, "Vital Strategies": -1}, "forte", "relator, com parecer favorável, do PL 5008/2023, que regulamenta os cigarros eletrônicos (CAE)"),
    ("Veneziano Vital do Rêgo", {"Infinis": 1, "IEPS": 1}, "forte", "relator no Senado do PL 3383/2021, que virou a Lei 14.819/2024 (atenção psicossocial nas escolas)"),
    ("Carlos Portinho", {"Infinis": 1, "UNFPA": 1}, "forte", "autor da Emenda 741 ao PL 4/2025 (Código Civil), que fixa 18 anos como idade mínima para o casamento"),
    ("Veneziano Vital do Rêgo", {"Vital Strategies": -1, "Desiderata": -1}, "forte", "autor da Emenda 1730 ao PLP 68/2024, que tira as bebidas açucaradas do Imposto Seletivo"),
    ("Magno Malta", {"Vital Strategies": -1, "Desiderata": -1}, "forte", "autor da Emenda 1928 ao PLP 68/2024, que tira as bebidas açucaradas do Imposto Seletivo"),
    ("Tereza Cristina", {"Vital Strategies": -1}, "forte", "autora da Emenda 1873 ao PLP 68/2024, que escalona o Imposto Seletivo das bebidas alcoólicas para não passar a carga de 2023"),
    ("Beto Faro", {"Vital Strategies": 1, "Desiderata": 1}, "forte", "autor da Emenda 2197 ao PLP 68/2024, que estende o Imposto Seletivo às bebidas ultraprocessadas"),
    ("Omar Aziz", {"Infinis": 1, "Vital Strategies": 1}, "evidência", "contrário à regulamentação dos cigarros eletrônicos na CAE (declaração, sinal fraco)"),
    # Emendas ao PL 4/2025 (Código Civil) lidas em 27/10/2026, a partir da planilha de emendas feita para o Coletivo
    # (1IaTT3RI…): contam as que põem a vida desde a concepção ou o nascituro no texto; as de efeito indireto ficam como registro.
    ("Damares Alves", {"Coletivo Feminista": -1}, "forte", "autora da Emenda 15 ao PL 4/2025 (Código Civil), que manda interpretar o Código pelo princípio da vida desde a concepção"),
    ("Damares Alves", {"Coletivo Feminista": -1}, "forte", "autora da Emenda 26 ao PL 4/2025 (Código Civil), que estende os direitos da personalidade ao nascituro e veda interpretação que legitime o aborto"),
    ("Damares Alves", {"Coletivo Feminista": -1}, "forte", "autora da Emenda 27 ao PL 4/2025 (Código Civil), que veda o descarte de embriões na reprodução assistida"),
    ("Damares Alves", {"Coletivo Feminista": -1}, "forte", "autora da Emenda 38 ao PL 4/2025 (Código Civil), que põe o direito à vida desde a concepção e veda interpretação que legitime o aborto"),
    ("Damares Alves", {"Coletivo Feminista": -1}, "forte", "autora da Emenda 40 ao PL 4/2025 (Código Civil), que dá personalidade civil desde a concepção"),
    ("Jorge Seif", {"Coletivo Feminista": -1}, "forte", "autor da Emenda 56 ao PL 4/2025 (Código Civil), que tira a vida pré-uterina e uterina como potencialidade, porque a vida começaria na concepção"),
    ("Jorge Seif", {"Coletivo Feminista": -1}, "forte", "autor da Emenda 61 ao PL 4/2025 (Código Civil), que faz do nascituro titular de direitos da personalidade, inclusive o direito à vida"),
    ("Jorge Seif", {"Coletivo Feminista": -1}, "forte", "autor da Emenda 67 ao PL 4/2025 (Código Civil), que tira o limite aos direitos da personalidade do nascituro"),
    ("Hamilton Mourão", {"Coletivo Feminista": -1}, "evidência", "autor da Emenda 139 ao PL 4/2025 (Código Civil), que repete a regra vigente do art. 2º sobre os direitos do nascituro desde a concepção"),
    ("Magno Malta", {"Coletivo Feminista": -1}, "evidência", "autor da Emenda 769 ao PL 4/2025 (Código Civil), que limita a recusa de tratamento a não violar direito de terceiros, com o nascituro na justificação"),
    ("Magno Malta", {"Coletivo Feminista": -1}, "evidência", "autor da Emenda 781 ao PL 4/2025 (Código Civil), que suprime os arts. 1.511-A a 1.511-G, inclusive a vida pré-uterina como potencialidade"),
    ("Teresa Leitão", {"Coletivo Feminista": 1}, "evidência", "autora da Emenda 697 ao PL 4/2025 (Código Civil), que reconhece a autonomia progressiva de crianças e adolescentes nas decisões sobre eles"),
]

# Votações pela descrição do cliente: (matéria, Casa, trecho do texto da votação, sentido do Sim para cada cliente,
# rótulo[, de onde vem o lado]). Sem o sexto campo, o lado vem da descrição do cliente e fica a confirmar. Votação de
# emenda só entra aqui com o texto da emenda lido; essas contam mesmo com o projeto na base de pautas, que não conta emenda.
DESCRICAO_VOTOS = [
    ("PDL 3/2025", "Câmara", "Aprovado o Projeto de Decreto Legislativo", {"Coletivo Feminista": -1},
     "no PDL 3/2025, que susta a resolução 258 do Conanda sobre o atendimento, inclusive o aborto legal, de meninas vítimas de violência sexual (Sim = sustar a resolução)"),
    ("PEC 164/2012", "Câmara", "Aprovado o Parecer", {"Coletivo Feminista": -1, "UNFPA": -1},
     "no parecer da PEC 164/2012, que põe na Constituição a inviolabilidade da vida desde a concepção (Sim = aprovar o parecer)"),
    # Emenda de Plenário 2 (Chris Tonietto): "primeira infância o período desde a gestação"; a coordenação marcou o PL 1924
    # como detrator para o Coletivo enquanto a gestação estiver no texto
    ("PL 1924/2025", "Câmara", "Aprovada a Emenda de Plenário nº 2", {"Coletivo Feminista": -1},
     "na Emenda de Plenário 2 ao PL 1924/2025, que põe a gestação na definição de primeira infância (Sim = pôr a gestação no texto)",
     "lado pelo comentário da coordenação na base de pautas"),
    # auditoria dos outros clientes em 27/10/2026: votações divididas do projeto inteiro e votações-chave da Câmara
    ("PL 104/2015", "Câmara", "Aprovado o Parecer", {"ISG": 1},
     "no parecer do PL 104/2015, que proíbe o celular na sala de aula (Sim = aprovar o parecer)"),
    ("PL 12/2021", "Câmara", "Aprovado o Substitutivo ao Projeto de Lei nº 12", {"IDEC": 1},
     "no PL 12/2021, licença compulsória de patentes na emergência de saúde (Sim = aprovar o texto)"),
    ("PL 12/2021", "Senado", "Votação nominal do Substitutivo da Câmara dos Deputados ao PL nº 12/2021", {"IDEC": 1},
     "no PL 12/2021, licença compulsória de patentes na emergência de saúde (Sim = aprovar o texto da Câmara)"),
    ("PLP 230/2025", "Câmara", "Aprovado o Substitutivo ao Projeto de Lei Complementar nº 230", {"MegaEdu": 1, "IDEC": 1},
     "no PLP 230/2025, que proíbe contingenciar o FUST (Sim = aprovar o texto)"),
    ("PL 4372/2020", "Câmara", "Aprovado o Substitutivo do Senado Federal ao Projeto de Lei nº 4.372", {"IU": 1},
     "no substitutivo do Senado ao PL 4372/2020, regulamentação do Fundeb (Sim = aprovar o texto do Senado)"),
    ("PL 3292/2020", "Câmara", "Aprovado o Substitutivo ao Projeto de Lei nº 3.292", {"IDEC": -1},
     "no PL 3292/2020, que reserva parte do PNAE para o leite fluido (Sim = aprovar o texto)",
     "contra pela posição pública do Idec, que assinou carta contra o projeto em 2021"),
    ("PL 3179/2012", "Câmara", "Aprovada a Subemenda Substitutiva Global ao Projeto de Lei nº 3.179", {"FMCSV": -1, "ISG": -1},
     "no PL 3179/2012, que permite a educação domiciliar (Sim = permitir)"),
    ("PLP 235/2019", "Câmara", "Aprovada a Subemenda Substitutiva Global ao Projeto de Lei Complementar nº 235", {"IU": 1, "ISG": 1},
     "no PLP 235/2019, que cria o Sistema Nacional de Educação (Sim = aprovar o texto)"),
    ("PLP 243/2023", "Câmara", "Aprovado o Projeto de Lei Complementar nº 243", {"ISG": 1, "IU": 1},
     "no PLP 243/2023, incentivo à permanência no ensino médio, o Pé-de-Meia (Sim = aprovar o texto)"),
    ("PL 3626/2023", "Câmara", "Rejeitada a Emenda de Plenário nº 26", {"IEPS": 1, "Vital Strategies": 1},
     "na Emenda de Plenário 26 à lei das bets, que proibia aposta de inadimplente, de inscrito no CadÚnico e de quem recebe BPC (Sim = restringir as apostas)",
     "a favor pela pauta Apostas online e saúde mental: o cliente defende restringir as apostas"),
]



# Projetos de Assembleia e de câmara municipal dos senadores novos (Inteligov e SAPL), lidos um a um pela ementa.
# Contam como autoria na Casa (primeiro autor forte, coautor médio). Ficaram fora os de lado ambíguo para os clientes
# (escola cívico-militar, Escola sem Partido, passaporte vacinal, campanhas de doença específica, datas comemorativas).
LOCAIS = "inteligov_senado/autoria.csv"
DESCRICAO_LOCAL = {
    ("André do Prado", "PL 1012/2015"): ({"Desiderata": 1, "Vital Strategies": 1, "Umane": 1}, "proíbe a venda de alimentos com gordura trans nas escolas"),
    ("Capitão Contar", "PL 4348/2021"): ({"UNFPA": 1}, "programa de proteção aos órfãos do feminicídio"),
    ("Capitão Contar", "PL 540/2020"): ({"UNFPA": 1}, "amplia o programa de enfrentamento à violência doméstica contra a mulher"),
    ("Capitão Contar", "PL 424/2021"): ({"UNFPA": 1}, "sigilo dos dados de servidoras com medida protetiva"),
    ("Capitão Contar", "PL 423/2021"): ({"Cactus": 1}, "política de diagnóstico e tratamento da depressão"),
    ("Capitão Contar", "PL 2866/2019"): ({"UNFPA": 1}, "proteção da gestante e da parturiente contra a violência obstétrica"),
    ("Carlos Bolsonaro", "PL 3/2021"): ({"ISG": -1, "FMCSV": -1}, "permite a educação domiciliar"),
    ("Carlos Bolsonaro", "PL 1296/2025"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "campanha permanente contra o aborto"),
    ("Carlos Bolsonaro", "PL 1888/2023"): ({"UNFPA": -1, "Coletivo Feminista": -1}, "impõe exigência às equipes médicas antes do aborto legal"),
    ("Carlos Bolsonaro", "PL 1451/2025"): ({"Vital Strategies": 1}, "campanha de prevenção ao alcoolismo juvenil"),
    ("Carlos Bolsonaro", "PL 1556/2025"): ({"Umane": 1}, "prevenção de doenças crônicas não transmissíveis nas redes de saúde e de educação"),
    ("Chicão", "PL 125/2016"): ({"FMCSV": 1}, "políticas públicas para a primeira infância"),
    ("Chicão", "PL 121/2016"): ({"Desiderata": 1, "Vital Strategies": 1, "Umane": 1}, "proíbe embutidos na merenda de escolas e creches"),
    ("Chicão", "PL 344/2015"): ({"Vital Strategies": 1}, "aviso \"Se beber, não dirija\" onde se vende bebida alcoólica"),
    ("Chicão", "PL 225/2015"): ({"UNFPA": 1}, "casas de acolhimento para mulheres vítimas de violência"),
    ("Chicão", "PL 152/2020"): ({"UNFPA": 1}, "condomínios comunicam agressões domésticas contra mulheres e crianças"),
    ("Samanda de Lula", "PL 115/2026"): ({"UNFPA": 1}, "formação dos servidores em prevenção à violência de gênero"),
    ("Samanda de Lula", "PL 116/2026"): ({"UNFPA": 1}, "prioridade nos processos de mulheres em situação de violência doméstica"),
    ("Samanda de Lula", "PL 117/2026"): ({"UNFPA": 1}, "selo para condomínios que previnem a violência contra a mulher"),
    ("Samanda de Lula", "PL 118/2026"): ({"UNFPA": 1}, "prioridade habitacional para mulheres em situação de violência doméstica"),
    ("Samanda de Lula", "PL 257/2025"): ({"UNFPA": 1}, "punição administrativa da violência contra a mulher"),
    ("Samanda de Lula", "PL 107/2025"): ({"UNFPA": 1}, "transporte público seguro para mulheres"),
    ("Samanda de Lula", "PL 108/2025"): ({"UNFPA": 1}, "divulgação de canais de denúncia de violência contra a mulher em eventos"),
    ("Samanda de Lula", "PL 46/2025"): ({"UNFPA": 1}, "gratuidade no transporte para mulheres vítimas de violência doméstica"),
    ("Samanda de Lula", "PL 820/2025"): ({"UNFPA": 1}, "reconstrução dentária para mulheres vítimas de violência doméstica"),
    ("Samanda de Lula", "PL 558/2026"): ({"UNFPA": 1}, "prevenção à violência contra a mulher nas escolas"),
    ("Samanda de Lula", "PL 986/2025"): ({"FMCSV": 1, "UNFPA": 1}, "proteção de gestantes e mães em situação de rua"),
    ("Samanda de Lula", "PL 822/2025"): ({"UNFPA": 1}, "transporte humanizado para puérperas após a alta"),
    ("Samanda de Lula", "PL 302/2025"): ({"Infinis": 1}, "selo para escolas que enfrentam a violência sexual contra crianças"),
    ("Samanda de Lula", "PL 303/2025"): ({"Infinis": 1}, "prevenção à violência sexual contra crianças nas redes sociais, nas escolas"),
    ("Samanda de Lula", "PL 304/2025"): ({"Infinis": 1}, "protocolo de escuta protegida para violência sexual contra crianças"),
    ("Samanda de Lula", "PL 620/2025"): ({"REMS": 1}, "política municipal de fomento ao esporte feminino"),
    ("Samanda de Lula", "PL 819/2025"): ({"REMS": 1}, "isonomia de premiação entre atletas homens e mulheres"),
}

# Posição pública do próprio cliente sobre o projeto (nota, carta, fala de dirigente): o lado vem dela e sai da checagem.
_DOMICILIAR = "contra pela posição pública da FMCSV, contrária a regulamentar a educação domiciliar (Mariana Luz, Brasil de Fato, 15/12/2025)"
_RES_258 = "contra pela posição pública das Católicas pelo Direito de Decidir, que pediram à ONU a volta da resolução 258 do Conanda (02/07/2026)"
POSICAO_PUBLICA = {
    **{("FMCSV", k): _DOMICILIAR for k in ("PL 3179/2012", "PL 10185/2018", "PL 5168/2026", "PL 2577/2026", "PL 3/2021")},
    **{("Coletivo Feminista", k): _RES_258 for k in ("PDL 3/2025", "PDL 25/2025", "PDL 35/2025", "PDL 53/2025", "PDL 74/2025")},
    ("IDEC", "PL 12/2021"): "a favor pela posição pública do Idec, que chamou a aprovação no Senado de vitória histórica (30/04/2021)",
}


def sinais_descricao(cliente, ja_na_base, senadores, aut, votos):
    out = {n: [] for n in senadores}
    vistos = set()
    for k, casa, trecho, sentidos, rot, *fonte in DESCRICAO_VOTOS:
        if cliente not in sentidos or (k in ja_na_base and "Emenda" not in trecho):
            continue
        v = votos[(votos.k == k) & (votos.Casa == casa) & votos["Votação"].astype(str).str.contains(trecho, regex=False)
                  & votos.Voto.isin(["Sim", "Não"])]
        for r in v.itertuples():
            if r.Parlamentar in out:
                s = sentidos[cliente] * (1 if r.Voto == "Sim" else -1)
                onde = ("plenário da Câmara" if casa == "Câmara" else "plenário do Senado") if str(r.Colegiado) in ("PLEN", "", "plenário do Senado") else str(r.Colegiado)
                lado = fonte[0] if fonte else POSICAO_PUBLICA.get((cliente, k), f"{'a favor' if s > 0 else 'contra'} pela descrição do cliente, a confirmar")
                out[r.Parlamentar].append(("forte", s, f"votou {r.Voto} {rot} ({onde}, {str(r.Data)[:4]}; {lado})"))
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
                peso = "forte" if forte else ("evidência" if k.startswith("PEC") else "médio")
                # a mesma proposta reapresentada (ementa igual, no Senado e na Câmara ou em anos diferentes) conta uma vez
                chave_texto = re.sub(r"\W+", " ", str(r.Ementa)).lower().strip()[:200]
                if any(p == r.Parlamentar and x == s and SequenceMatcher(None, t, chave_texto).ratio() > 0.9 for p, t, x in vistos):
                    out[r.Parlamentar].append(("evidência", 0, f"{'autor' if forte else 'coautor'} do {k} ({rot}; mesma proposta reapresentada, conta uma vez)"))
                    continue
                vistos.add((r.Parlamentar, chave_texto, s))
                lado = POSICAO_PUBLICA.get((cliente, k), f"{'a favor' if s > 0 else 'contra'} pela descrição do cliente, a confirmar")
                out[r.Parlamentar].append((peso, s if peso != "evidência" else 0,
                    f"{'autor' if forte else 'coautor'} do {k} ({rot}; {lado})"
                    + (" (assinatura de PEC, não conta)" if peso == "evidência" else "")))
    locais = c.ler_csv(LOCAIS)
    for r in locais[locais.Papel.isin(["Primeiro autor", "Coautor"])].itertuples():
        chave = (r.Parlamentar, str(r.Matéria).strip())
        if chave not in DESCRICAO_LOCAL or r.Parlamentar not in out:
            continue
        sentidos, rot = DESCRICAO_LOCAL[chave]
        if cliente not in sentidos:
            continue
        s, forte = sentidos[cliente], r.Papel == "Primeiro autor"
        out[r.Parlamentar].append(("forte" if forte else "médio", s,
            f"{'autor' if forte else 'coautor'} do {chave[1]} na {r.Casa} ({rot}; {POSICAO_PUBLICA.get((cliente, chave[1]), ('a favor' if s > 0 else 'contra') + ' pela descrição do cliente, a confirmar')})"))
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
                out[n].append(("forte", s, f"votou {voto.capitalize().replace('Nao', 'Não')} na {rot} (plenário do Senado, {data[:4]}); {cfg['lado'] if a_favor else cfg['lado_contra']}"))
    for data, colegiado, mat, sentido, rot in cfg["comissao"]:
        for r in com[(com.Data == data) & (com.Colegiado == colegiado) & (com["Matéria"] == mat)].itertuples():
            if r.Parlamentar in out and r.Voto in ("Sim", "Não"):
                s = sentido * lado * (1 if r.Voto == "Sim" else -1)
                out[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {rot} ({data[:4]}); {cfg['lado'] if a_favor else cfg['lado_contra']}"))
    for r in aut[aut.k.isin(cfg["autoria"]) & aut.Papel.isin(["Primeiro autor", "Coautor"])].sort_values("Papel", ascending=False).drop_duplicates(["Parlamentar", "k"]).itertuples():
        if r.Parlamentar in out:
            forte = r.Papel == "Primeiro autor"
            out[r.Parlamentar].append(("forte" if forte else "médio", lado,
                f"{'autor' if forte else 'coautor'} do {r.k} ({cfg['autoria'][r.k]}); {cfg['lado'] if a_favor else cfg['lado_contra']}"))
    return out


def evidencias(s):
    """Lista numerada em três blocos com título: a favor do cliente, contra o cliente e o que não conta.
    O texto de cada item diz de onde vem o lado (coordenação, descrição do cliente ou posição na pauta)."""
    def tira_aninhado(t):
        """Parêntese dentro de parêntese vira vírgula, para a frase não ficar ((...))."""
        out, nivel = [], 0
        for ch in t:
            if ch == "(":
                nivel += 1
                out.append("(" if nivel == 1 else ", ")
            elif ch == ")":
                out.append(")" if nivel == 1 else "")
                nivel = max(nivel - 1, 0)
            else:
                out.append(ch)
        return re.sub(r"\s+,", ",", "".join(out))

    def limpa(t):
        t = re.sub(r"; (a favor|contra) pela descrição do cliente, a confirmar\)", "; lado pela descrição do cliente, a confirmar)", t)
        t = re.sub(r"(projeto )?(promotor|detrator) para o cliente, pela posição na pauta; marcação da coordenação pendente",
                   "lado pela posição do cliente na pauta; precisa de checagem dos coordenadores", t)
        t = re.sub(r"(projeto )?(promotor|detrator) para o cliente", r"projeto \2 para o cliente", t)
        t = re.sub(r"; pauta ([^)]+)\)", r"; pauta \1, lado pela posição do cliente)", t)
        t = re.sub(r"\b(autora?|coautora?|relatora?) do PEC", r"\1 da PEC", t)
        t = t.replace(" na PLEN ", " no plenário ").replace("(sentido do parecer não lido)", "(parecer ainda não lido, por isso não conta)")
        t = re.sub(r"(a favor|contra) pela descrição do cliente, a confirmar", "lado tirado da descrição do cliente; precisa de checagem dos coordenadores", t)
        t = t.replace("lado pela descrição do cliente, a confirmar", "lado tirado da descrição do cliente; precisa de checagem dos coordenadores")
        t = t.replace("; CCJC,", "; CCJ da Câmara,").replace("(CCJC,", "(CCJ da Câmara,")
        t = tira_aninhado(t)
        t = re.sub(r"\) \(", "; ", t)
        return t
    blocos = [("A favor do cliente", [t for k, x, t in s if x > 0]),
              ("Contra o cliente", [t for k, x, t in s if x < 0]),
              ("Não conta (só registro)", [t for k, x, t in s if x == 0])]
    out, i = [], 0
    for titulo, itens in blocos:
        itens = list(dict.fromkeys(itens))
        if not itens:
            continue
        out.append(titulo.upper())
        for t in itens:
            i += 1
            out.append(f"{i}. {limpa(t)}")
    return "\n".join(out)


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
    # abaixo do piso: separa quem tem sinal que conta (e em que sentido) de quem só tem registro que não conta
    if (fp or mp) and (fn or mn):
        return "Sinais opostos"
    if fp or mp:
        return "Um sinal a favor" if fp + mp == 1 else "Só coautorias a favor"
    if fn or mn:
        return "Um sinal contra" if fn + mn == 1 else "Só coautorias contra"
    return "Registro sem sinal"


def main():
    base = c.ler_csv("temas_clientes/base_pautas.csv")
    # clientes do DOU e do PDF de descrição que não estão na base de pautas: entram só pelos sinais da descrição
    extra = pd.DataFrame([{"Área": a, "Cliente": cl, "Pauta": pa, "Cliente é": "", "Projeto": "", "Para o cliente": "", "Comentário": ""}
                          for cl, (a, pa) in CLIENTES_FORA_DA_BASE.items()])
    base = pd.concat([base, extra], ignore_index=True)
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
        pela_pauta = g[g["Para o cliente"].isin(["", "Dúvida"]) & g["Cliente é"].isin(["A favor", "Contra"])].copy()
        pela_pauta["Para o cliente"] = pela_pauta["Cliente é"].map({"A favor": "Promotor", "Contra": "Detrator"})
        pela_pauta["Fonte da posição"] = "pauta"
        sinais = {n: [] for n in senadores.Parlamentar}
        for p in pd.concat([marcados, pela_pauta]).itertuples():
            sentido = 1 if p._6 == "Promotor" else -1
            quem = f"{p._6.lower()} para o cliente" + (", pela posição na pauta; marcação da coordenação pendente" if p._8 == "pauta" else "")
            for k, casa in chaves(p.Projeto):
                desc = ROTULO_BASE.get(k, p.Pauta.lower())
                ok = lambda d: (d.k == k) & ((d.Casa == casa) if casa else True)
                for r in aut[ok(aut) & aut.Papel.isin(["Primeiro autor", "Coautor"])].sort_values("Papel", ascending=False).drop_duplicates("Parlamentar").itertuples():
                    if r.Parlamentar in sinais:
                        forte = r.Papel == "Primeiro autor"
                        peso = "forte" if forte else ("evidência" if k.startswith("PEC") else "médio")
                        sinais[r.Parlamentar].append((peso, sentido if peso != "evidência" else 0,
                            f"{'autor' if forte else 'coautor'} do {k} ({desc}; {quem})" + (" (assinatura de PEC, não conta)" if peso == "evidência" else "")))
                for r in vot[ok(vot) & vot.Voto.isin(["Sim", "Não"])].itertuples():
                    if r.Parlamentar in sinais:
                        s = sentido if r.Voto == "Sim" else -sentido
                        onde = r.Colegiado if r.Colegiado != "PLEN" else "plenário da Câmara"
                        sinais[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {k} ({desc}; {onde}, {str(r.Data)[:4]}; Sim = aprovar o projeto, {quem})"))
                for r in rel[ok(rel)].drop_duplicates(["Parlamentar", "Colegiado"]).itertuples():
                    if r.Parlamentar in sinais:
                        lido = PARECER_LIDO.get((k, r.Parlamentar))
                        if lido:
                            sinais[r.Parlamentar].append(("forte", sentido, f"relator do {k} ({desc}) na {r.Colegiado}, com {lido} ({quem})"))
                        else:
                            sinais[r.Parlamentar].append(("evidência", 0, f"relator do {k} ({desc}) na {r.Colegiado} (sentido do parecer não lido)"))
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
            sinal_txt = "; ".join(f"{q} {um if q == 1 else varios}" for um, varios, q in rot if q) or "só evidência, sem sinal que conte"
            linhas.append({"Área": area, "Cliente": cliente, "Senador": n,
                           "Partido": ficha.loc[n, "Partido"], "UF": ficha.loc[n, "UF"], "Cadeira": ficha.loc[n, "Cadeira"],
                           "Classificação": classifica(fp, fn, mp, mn),
                           "Sinais": sinal_txt,
                           "Evidências": evidencias(s)})
        pautas = g.drop_duplicates("Pauta")
        resumo.append({"Área": area, "Cliente": cliente,
                       "Pautas": "; ".join(f"{r.Pauta} ({r._4.lower() or 'posição não informada'})" if r.Projeto else f"{r.Pauta} (fora da base de pautas; só pela descrição do cliente)" for r in pautas.itertuples()),
                       "Promotores": int((marcados["Para o cliente"] == "Promotor").sum()),
                       "Detratores": int((marcados["Para o cliente"] == "Detrator").sum()),
                       "Aguardando": int((g["Para o cliente"].isin(["", "Dúvida"]) & (g.Projeto != "")).sum()),
                       "Senadores com registro": com_registro,
                       "Posição fora da planilha": "; ".join(f"{r.Projeto}: {r._8}" for r in g.itertuples() if r._8 != "Coordenação")})
    c.salvar_csv(pd.DataFrame(linhas), "temas_clientes/clientes_classificacao.csv")
    c.salvar_csv(pd.DataFrame(resumo), "temas_clientes/clientes_resumo.csv")
    print(pd.DataFrame(linhas).groupby(["Cliente", "Classificação"]).size().unstack(fill_value=0).to_string())


if __name__ == "__main__":
    main()
