from outros.analise_planos import _norm_busca, paginas_do_trecho


def test_citacao_que_atravessa_pagina_aponta_as_duas_paginas():
    paginas = [
        "A política estadual vai ampliar a rede de atendimento",
        "às mulheres em todas as regiões do estado.",
        "Outro assunto sem relação.",
    ]

    assert paginas_do_trecho(
        [_norm_busca(p) for p in paginas],
        "ampliar a rede de atendimento às mulheres em todas as regiões",
    ) == [1, 2]


def test_citacao_contida_em_uma_pagina_mantem_uma_referencia():
    paginas = [
        "Texto da primeira página.",
        "Ampliar a rede de atendimento às mulheres em todas as regiões.",
    ]

    assert paginas_do_trecho(
        [_norm_busca(p) for p in paginas],
        "Ampliar a rede de atendimento às mulheres em todas as regiões",
    ) == [2]


def test_citacao_atravessa_cabecalho_da_pagina_seguinte():
    paginas = [
        "Criar o programa de permanência escolar com bolsas e apoio psicossocial",
        "35 Propostas para educação. Transporte, alimentação e busca ativa de estudantes.",
    ]

    assert paginas_do_trecho(
        [_norm_busca(p) for p in paginas],
        "Criar o programa de permanência escolar com bolsas e apoio psicossocial "
        "transporte alimentação e busca ativa de estudantes",
    ) == [1, 2]
