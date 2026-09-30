"""Conferência, item a item, do que a pesquisa anterior dizia sobre os competitivos do Senado
e nenhuma ficha da Wikipedia confirmava (30/09/2026).

Cada entrada: o texto antigo que ela resolve ("substitui", vazio quando é cargo novo achado na
conferência), o cargo como a fonte escreve, tipo, período, fonte e resultado:
- "Confirmado": a fonte diz o mesmo;
- "Corrigido": a fonte diz outra coisa (órgão, esfera ou período) e vale a fonte;
- "Não confirmado": nenhuma fonte achada; o item sai do resumo e fica só na aba de cargos.
Chave: SQ_CANDIDATO.
"""

WIKI = "https://pt.wikipedia.org/wiki/"

VERIFICADOS = {
    "10002536441": [  # Jorge Viana
        dict(substitui="Presidente da ApexBrasil", cargo="Presidente da ApexBrasil", tipo="Nomeação ou função pública",
             periodo="2023", resultado="Confirmado",
             fonte="https://agenciabrasil.ebc.com.br/justica/noticia/2023-05/justica-anula-posse-do-presidente-da-apex"),
    ],
    "40002542686": [  # Capitão Alberto Neto
        dict(substitui="Capitão da Polícia Militar do Amazonas", cargo="Policial militar do Amazonas (capitão)",
             tipo="Carreira pública", periodo="2008", resultado="Confirmado", fonte=WIKI + "Capitão_Alberto_Neto"),
        dict(substitui="Comandante de Policiamento Comunitário",
             cargo="Comandante da Companhia Interativa Comunitária (Cicom) da PM-AM", tipo="Carreira pública",
             periodo="", resultado="Corrigido", fonte=WIKI + "Capitão_Alberto_Neto"),
    ],
    "30002530071": [  # Rayssa Furlan
        dict(substitui="Secretária Municipal de Mobilização e Participação Popular de Macapá",
             cargo="Secretária Municipal de Mobilização e Participação Popular de Macapá",
             tipo="Nomeação ou função pública", periodo="2021-2026", resultado="Confirmado",
             fonte="https://obrasilianista.com.br/2026/08/24/politica/saiba-quem-e-rayssa-furlan-candidata-ao-senado-pelo-amapa/"),
    ],
    "60002540753": [  # Capitão Wagner
        dict(substitui="Capitão da Polícia Militar do Ceará", cargo="Capitão da reserva da Polícia Militar do Ceará",
             tipo="Carreira pública", periodo="", resultado="Confirmado", fonte=WIKI + "Capitão_Wagner"),
    ],
    "70002552934": [  # Bia Kicis
        dict(substitui="Subprocuradora-Geral do Distrito Federal", cargo="Subprocuradora-geral do Distrito Federal",
             tipo="Carreira pública", periodo="até 2016", resultado="Confirmado", fonte=WIKI + "Bia_Kicis"),
    ],
    "70002552490": [  # Erika Kokay
        dict(substitui="Presidente do Sindicato dos Bancários e da CUT-DF",
             cargo="Presidente do Sindicato dos Bancários de Brasília", tipo="Entidade de classe ou sociedade civil",
             periodo="1992-1998", resultado="Confirmado",
             fonte="https://www.cnnbrasil.com.br/eleicoes/quem-e-erika-kokay-candidata-ao-senado-pelo-distrito-federal/"),
        dict(substitui="", cargo="Presidente da CUT-DF", tipo="Entidade de classe ou sociedade civil",
             periodo="", resultado="Confirmado",
             fonte="https://www.cnnbrasil.com.br/eleicoes/quem-e-erika-kokay-candidata-ao-senado-pelo-distrito-federal/"),
    ],
    "70002552936": [  # Michelle Bolsonaro
        dict(substitui="Presidente do Conselho do Programa Pátria Voluntária",
             cargo="Presidente do Conselho do Programa Nacional de Incentivo ao Voluntariado (Pátria Voluntária)",
             tipo="Nomeação ou função pública", periodo="2019-2022", resultado="Confirmado",
             fonte="https://www.poder360.com.br/governo/michelle-bolsonaro-vai-presidir-o-conselho-do-programa-de-incentivo-ao-voluntariado/"),
        dict(substitui="Presidente do PL Mulher", cargo="Presidente do PL Mulher", tipo="Direção partidária",
             periodo="2023-2026", resultado="Confirmado", fonte=WIKI + "Michelle_Bolsonaro"),
    ],
    "80002551370": [  # Renato Casagrande
        dict(substitui="Secretário Municipal de Meio Ambiente de Serra", cargo="Secretário de Meio Ambiente da Serra (ES)",
             tipo="Nomeação ou função pública", periodo="1999-2001", resultado="Corrigido",
             fonte=WIKI + "Renato_Casagrande"),
    ],
    "100002549583": [  # Roseana Sarney
        dict(substitui="Secretária de Assuntos Extraordinários do MA", cargo="Secretária Extraordinária do Estado do Maranhão",
             tipo="Nomeação ou função pública", periodo="1983-1984", resultado="Corrigido",
             fonte="https://www.camara.leg.br/deputados/73806/biografia"),
        dict(substitui="Chefe de Gabinete da Casa Civil da Presidência",
             cargo="Assessora parlamentar do Gabinete Civil da Presidência da República", tipo="Nomeação ou função pública",
             periodo="1985-1989", resultado="Corrigido", fonte="https://www.camara.leg.br/deputados/73806/biografia"),
        dict(substitui="", cargo="Secretária Extraordinária de Assuntos Legislativos do Maranhão",
             tipo="Nomeação ou função pública", periodo="2025", resultado="Confirmado",
             fonte="https://www.portaltela.com/politica/governo/2025/01/31/roseana-sarney-retorna-ao-governo-do-maranhao-como-secretaria-extraordinaria-de-assuntos-legislativos"),
    ],
    "130002554332": [  # Aécio Neves
        dict(substitui="Secretário Particular da Presidência da República",
             cargo="Secretário particular do governador Tancredo Neves (MG)", tipo="Nomeação ou função pública",
             periodo="1983-1984", resultado="Corrigido", fonte="https://neamp.pucsp.br/liderancas/aecio-neves-da-cunha"),
        dict(substitui="Diretor da Caixa Econômica do Estado de Minas Gerais",
             cargo="Diretor de Loterias da Caixa Econômica Federal", tipo="Nomeação ou função pública",
             periodo="1985-1986", resultado="Corrigido", fonte="https://neamp.pucsp.br/liderancas/aecio-neves-da-cunha"),
    ],
    "130002551786": [  # Domingos Sávio
        dict(substitui="Presidente do Sindicato Rural e de cooperativas de crédito",
             cargo="Presidente do Sindicato Rural de Divinópolis, da Cooperativa Agropecuária e da Crediverde",
             tipo="Entidade de classe ou sociedade civil", periodo="", resultado="Confirmado",
             fonte="https://jovempan.com.br/politica/domingos-savio-mira-o-senado-apos-mais-de-tres-decadas-na-politica-mineira/"),
    ],
    "130002550560": [  # Marília Campos
        dict(substitui="Psicóloga e dirigente do Sindicato dos Bancários de Belo Horizonte",
             cargo="Presidente do Sindicato dos Bancários de Belo Horizonte", tipo="Entidade de classe ou sociedade civil",
             periodo="1990-1995", resultado="Corrigido", fonte=WIKI + "Marília_Campos"),
    ],
    "120002535769": [  # Capitão Contar
        dict(substitui="Capitão do Exército Brasileiro (Oficial de Artilharia - Aman)",
             cargo="Oficial do Exército (Aman, 2006), capitão da reserva desde 2018", tipo="Carreira pública",
             periodo="2006-2018", resultado="Confirmado",
             fonte="https://correiodoestado.com.br/politica/entre-seis-nomes-ao-senado-por-ms-so-um-ja-e-aposentado-e-se/470102/"),
    ],
    "120002535764": [  # Reinaldo Azambuja
        dict(substitui="Presidente do Sindicato Rural de Maracaju", cargo="Presidente do Sindicato Rural de Maracaju",
             tipo="Entidade de classe ou sociedade civil", periodo="", resultado="Não confirmado", fonte=""),
    ],
    "110002551966": [  # Mauro Mendes
        dict(substitui="Presidente da Federação das Indústrias no Estado de Mato Grosso (FIEMT)",
             cargo="Presidente da Federação das Indústrias de Mato Grosso (Fiemt)",
             tipo="Entidade de classe ou sociedade civil", periodo="2007-2010", resultado="Confirmado",
             fonte=WIKI + "Mauro_Mendes"),
    ],
    "140002550780": [  # Chicão
        dict(substitui="Secretário de Estado de Obras Públicas do Pará (Sedop)",
             cargo="Secretário de Estado de Obras Públicas do Pará (Seop)", tipo="Nomeação ou função pública",
             periodo="2007-2010", resultado="Confirmado",
             fonte="https://aprovinciadopara.com.br/chicao-amplia-protagonismo-politico-e-chega-a-disputa-pelo-senado-com-trajetoria-de-mais-de-tres-decadas-de-servicos-prestados-ao-para/"),
        dict(substitui="", cargo="Secretário de Estado de Transportes do Pará (Setran)", tipo="Nomeação ou função pública",
             periodo="2011", resultado="Confirmado",
             fonte="https://aprovinciadopara.com.br/chicao-amplia-protagonismo-politico-e-chega-a-disputa-pelo-senado-com-trajetoria-de-mais-de-tres-decadas-de-servicos-prestados-ao-para/"),
    ],
    "140002544855": [  # Delegado Éder Mauro
        dict(substitui="Delegado da Polícia Civil do Pará", cargo="Delegado da Polícia Civil do Pará",
             tipo="Carreira pública", periodo="1984-2014", resultado="Confirmado", fonte=WIKI + "Éder_Mauro_Cardoso_Barra"),
        dict(substitui="Diretor de Polícia Metropolitana", cargo="Diretor de Polícia Metropolitana",
             tipo="Carreira pública", periodo="", resultado="Não confirmado", fonte=""),
    ],
    "150002549793": [  # João Azevêdo
        dict(substitui="Secretário de Planejamento de João Pessoa", cargo="Secretário de Planejamento de Bayeux (PB)",
             tipo="Nomeação ou função pública", periodo="2004", resultado="Corrigido", fonte=WIKI + "João_Azevêdo"),
        dict(substitui="", cargo="Secretário de Serviços Urbanos de João Pessoa", tipo="Nomeação ou função pública",
             periodo="1986-1989", resultado="Confirmado", fonte=WIKI + "João_Azevêdo"),
    ],
    "180002533964": [  # Júlio Cesar
        dict(substitui="Secretário de Estado da Agricultura, Abastecimento e Recursos Hídricos do Piauí",
             cargo="Secretário de Agricultura do Piauí", tipo="Nomeação ou função pública", periodo="1986",
             resultado="Corrigido", fonte=WIKI + "Júlio_César_(político,_1948)"),
    ],
    "160002547661": [  # Deltan Dallagnol
        dict(substitui="Coordenador da Força-Tarefa Lava Jato", cargo="Coordenador da força-tarefa da Lava Jato (MPF)",
             tipo="Carreira pública", periodo="", resultado="Confirmado", fonte=WIKI + "Deltan_Dallagnol"),
    ],
    "190002548145": [  # Pedro Paulo
        dict(substitui="Secretário Chefe da Casa Civil do RJ", cargo="Chefe da Casa Civil da Prefeitura do Rio de Janeiro",
             tipo="Nomeação ou função pública", periodo="", resultado="Corrigido", fonte=WIKI + "Pedro_Paulo_(político)"),
    ],
    "200002534447": [  # Cel. Hélio
        dict(substitui="Coronel da Polícia Militar do Rio Grande do Norte",
             cargo="Coronel aviador da reserva da Força Aérea Brasileira", tipo="Carreira pública", periodo="",
             resultado="Corrigido",
             fonte="https://tribunadonorte.com.br/politica/coronel-helio-se-lanca-ao-senado-e-busca-apoio-do-pl/"),
        dict(substitui="Subcomandante Geral da PM-RN", cargo="Subcomandante-geral da PM do RN", tipo="Carreira pública",
             periodo="", resultado="Não confirmado", fonte=""),
    ],
    "200002533843": [  # Rafael Motta
        dict(substitui="Secretário Municipal de Esporte e Lazer de Natal",
             cargo="Secretário adjunto de Esporte e Lazer do Rio Grande do Norte", tipo="Nomeação ou função pública",
             periodo="", resultado="Corrigido", fonte=WIKI + "Rafael_Motta"),
        dict(substitui="", cargo="Subsecretário da Juventude do Rio Grande do Norte", tipo="Nomeação ou função pública",
             periodo="", resultado="Confirmado", fonte=WIKI + "Rafael_Motta"),
    ],
    "230002534804": [  # Nicoletti
        dict(substitui="Policial Rodoviário Federal (PRF)", cargo="Policial rodoviário federal", tipo="Carreira pública",
             periodo="2015-2019", resultado="Confirmado", fonte="https://www.camara.leg.br/deputados/204479/biografia"),
        dict(substitui="", cargo="Militar de carreira do Exército", tipo="Carreira pública", periodo="2001-2015",
             resultado="Confirmado", fonte="https://www.camara.leg.br/deputados/204479/biografia"),
    ],
    "230002553006": [  # Teresa Surita
        dict(substitui="Secretária Nacional de Programas Urbanos do Ministério das Cidades",
             cargo="Secretária Nacional de Programas Urbanos do Ministério das Cidades", tipo="Nomeação ou função pública",
             periodo="2008-2009", resultado="Confirmado", fonte="https://www.camara.leg.br/deputados/160608/biografia"),
        dict(substitui="", cargo="Assessora especial do Ministério do Desenvolvimento Agrário",
             tipo="Nomeação ou função pública", periodo="1999-2000", resultado="Confirmado",
             fonte="https://www.camara.leg.br/deputados/160608/biografia"),
        dict(substitui="", cargo="Coordenadora de Ação Social do Governo de Roraima", tipo="Nomeação ou função pública",
             periodo="1989-1990", resultado="Confirmado", fonte="https://www.camara.leg.br/deputados/160608/biografia"),
    ],
    "210002533581": [  # Manuela D'Ávila
        dict(substitui="Presidente do Instituto E Se Fosse Você?", cargo="Fundadora do Instituto E Se Fosse Você",
             tipo="Entidade de classe ou sociedade civil", periodo="2019", resultado="Corrigido",
             fonte=WIKI + "Manuela_d'Ávila"),
    ],
    "210002543863": [  # Rigotto
        dict(substitui="Presidente do Conselho de Desenvolvimento Econômico e Social do RS",
             cargo="Presidente do Conselho de Desenvolvimento Econômico e Social do RS",
             tipo="Nomeação ou função pública", periodo="", resultado="Não confirmado", fonte=""),
    ],
    "210002547816": [  # Sanderson
        dict(substitui="Policial Federal (Escrivão de Polícia Federal)", cargo="Escrivão da Polícia Federal",
             tipo="Carreira pública", periodo="1996-2018", resultado="Confirmado", fonte=WIKI + "Ubiratan_Sanderson"),
        dict(substitui="Presidente do Sindicato dos Policiais Federais do RS",
             cargo="Presidente do Sindicato dos Policiais Federais do RS", tipo="Entidade de classe ou sociedade civil",
             periodo="", resultado="Confirmado", fonte=WIKI + "Ubiratan_Sanderson"),
    ],
    "260002547290": [  # André Moura
        dict(substitui="Secretário de Representação do Governo do RJ em Brasília",
             cargo="Secretário de Governo do Rio de Janeiro para representação em Brasília",
             tipo="Nomeação ou função pública", periodo="2019", resultado="Confirmado", fonte=WIKI + "André_Moura"),
    ],
    "260002549130": [  # Delegado André David
        dict(substitui="Delegado da Polícia Civil de Sergipe", cargo="Delegado da Polícia Civil de Sergipe",
             tipo="Carreira pública", periodo="", resultado="Confirmado",
             fonte="https://obrasilianista.com.br/2026/08/18/politica/delegado-andre-david-candidato-senado-sergipe/"),
        dict(substitui="Diretor do Departamento de Narcóticos (Denarc-SE)",
             cargo="Diretor do Departamento de Narcóticos (Denarc) de Sergipe", tipo="Carreira pública", periodo="",
             resultado="Confirmado",
             fonte="https://obrasilianista.com.br/2026/08/18/politica/delegado-andre-david-candidato-senado-sergipe/"),
        dict(substitui="", cargo="Secretário Municipal de Defesa Social e Cidadania de Aracaju",
             tipo="Nomeação ou função pública", periodo="2025-2026", resultado="Confirmado",
             fonte="https://www.nenoticias.com.br/andre-david-deixa-seguranca-aracaju-disputar-eleicao/"),
    ],
    "250002541312": [  # Guilherme Derrite
        dict(substitui="Capitão da PM-SP (Rota)", cargo="Oficial da PM de São Paulo, comandante de pelotão na Rota",
             tipo="Carreira pública", periodo="2003-2018", resultado="Confirmado", fonte=WIKI + "Capitão_Derrite"),
    ],
    "270002544629": [  # Paulo Mourão
        dict(substitui="Secretário de Estado da Fazenda do Tocantins", cargo="Secretário de Estado da Fazenda do Tocantins",
             tipo="Nomeação ou função pública", periodo="", resultado="Não confirmado", fonte=""),
        dict(substitui="Secretário Municipal de Infraestrutura de Palmas",
             cargo="Secretário Municipal de Infraestrutura de Palmas", tipo="Nomeação ou função pública", periodo="",
             resultado="Não confirmado", fonte=""),
    ],
    "270002546038": [  # Ronaldo Dimas
        dict(substitui="Secretário de Estado das Cidades do Tocantins",
             cargo="Secretário das Cidades e Desenvolvimento Urbano do Tocantins", tipo="Nomeação ou função pública",
             periodo="2011-2012", resultado="Confirmado", fonte=WIKI + "Ronaldo_Dimas"),
        dict(substitui="Presidente do Instituto de Planejamento de Palmas",
             cargo="Presidente do Instituto Municipal de Planejamento Urbano de Palmas (Impup)",
             tipo="Nomeação ou função pública", periodo="2025", resultado="Corrigido",
             fonte="https://clebertoledo.com.br/bilhete-do-ct/bilhete-do-ct-para-ronaldo-dimas-proximo-presidente-do-impup-e-maestro-da-orquestra-do-prefeito-eduardo/"),
        dict(substitui="", cargo="Secretário de Planejamento e Orçamento do Tocantins", tipo="Nomeação ou função pública",
             periodo="2025-2026", resultado="Confirmado",
             fonte="https://tocantins.jornalopcao.com.br/noticias/mais-mudancas-ronaldo-dimas-e-juarez-moreira-passam-a-integrar-a-gestao-do-governador-em-exercicio-568257/"),
    ],
}
