"""Todo termo das duas listas da MegaEdu cai no vocabulário do tema de destino.

Em 14/09/2026 a MegaEdu pediu Conectividade e Infraestrutura Pública Digital
como dois temas dentro de Educação, e o uso deles é saber, por plano, se trata
ou não de cada um. Termo que ficasse só num tema vizinho sairia como ausência
confirmada. Destinos e condições vêm das respostas da Gabrielle (MegaEdu) em
14/09; o comentário acima dos temas em analise_planos.py tem o detalhe.

fonte "lista": os 65 itens mandados ao Felipe (número do item na lista)
fonte "pdf": o guia "Palavras chaves para mapear campanhas" (bloco do guia)
destino: "C" Conectividade, "I" Infraestrutura Pública Digital, "CI" os dois
"""
import pytest

from outros.analise_planos import (EIXOS, EIXO_DO_TEMA, TERMOS_ANCORA,
                                   TERMOS_AUSENCIA, _AUSENCIA_RE, _norm_busca)

C, I, CI = "C", "I", "CI"

LISTA = [
    (1, "conectividade escolar", C), (2, "conectividade nas escolas", C),
    (3, "conexão de escolas", C), (3, "conexão escolar", C),
    (4, "internet nas escolas", C), (5, "internet na escola pública", C),
    (6, "acesso à internet escolar", C), (7, "banda larga escolar", C),
    (8, "banda larga nas escolas", C), (9, "rede escolar", C),
    (10, "infraestrutura de conectividade escolar", C), (11, "escolas conectadas", C),
    (12, "escola conectada", C), (13, "conectividade educacional", C),
    (14, "wifi escolar", C), (14, "wi-fi nas escolas", C), (15, "rede wifi escolar", C),
    (16, "fibra óptica nas escolas", C), (17, "conectividade rural", C),
    (18, "conectividade de banda larga em escolas públicas", C),
    (19, "tecnologia educacional", C), (20, "tecnologia na educação", C),
    (21, "tecnologia na escola", C), (22, "tecnologias digitais na educação", C),
    (23, "inovação educacional", C), (24, "inovação na educação", C),
    (25, "educação digital", C), (26, "transformação digital na educação", C),
    (27, "inclusão digital escolar", C), (28, "inclusão digital na educação", C),
    (29, "recursos tecnológicos educacionais", C), (30, "ferramentas digitais educacionais", C),
    (31, "equipamentos tecnológicos escolares", C), (32, "laboratório de informática escolar", C),
    (33, "dispositivos", C), (33, "tablets", C), (34, "ensino híbrido", C),
    (35, "cultura digital escolar", C),
    (36, "PIEC", C), (36, "Programa Interministerial de Conectividade", C),
    (37, "EACE", C), (37, "Estratégia de Aceleração da Conectividade nas Escolas", C),
    (38, "FUST", CI), (38, "Fundo de Universalização dos Serviços de Telecomunicações", CI),
    (39, "Programa Escolas Conectadas", C), (40, "Estratégia Nacional de Escolas Conectadas", C),
    (41, "Wi-Fi Brasil", CI), (42, "Internet para Todos", CI),
    (43, "infraestrutura pública digital", I), (44, "infraestrutura digital", I),
    (45, "infraestrutura de dados", I), (46, "governo digital", I),
    (47, "interoperabilidade de dados", I), (48, "interoperabilidade governamental", I),
    (49, "transformação digital do estado", I), (50, "soberania digital", I),
    (51, "dados abertos governamentais", I), (52, "sistema de gestão escolar", I),
    (54, "sistema de gestão educacional", I), (55, "gestão escolar digital", I),
    (56, "plataforma de gestão escolar", I), (57, "sistema de informação educacional", I),
    (58, "cadastro escolar digital", I), (59, "matrícula digital", I), (59, "matrícula online", I),
    (60, "sistema de registro escolar", I), (61, "censo escolar", I),
    (62, "dados de gestão escolar", I), (63, "interoperabilidade do SGE", I),
    (64, "modernização da gestão escolar", I),
    (65, "digitalização da gestão pública educacional", I),
]

PDF = {
    "Governo Digital": ["governo digital", "governo digital e infraestrutura", "estado digital",
        "transformação digital", "digitalização do governo", "digitalizar a relação cidadão-estado",
        "centro de governo digital", "virtualização"],
    "Infraestrutura Pública Digital": ["infraestrutura pública digital", "infraestrutura digital",
        "infraestrutura de dados", "conectar estados e municípios ao governo federal", "dados abertos",
        "interoperabilidade", "sistema único de informação", "dados como ativo estratégico",
        "dados como bem público"],
    "Interoperabilidade": ["integração de dados", "interoperabilidade dos dados",
        "interoperabilidade dos sistemas", "interoperabilidade com gov.br", "plano de interoperabilidade",
        "padrões mínimos de troca de dados", "padronizar sistemas e integrar bases de dados",
        "integrar e aprimorar sistemas e bases de dados já existentes", "padrões nacionais",
        "padrões de interoperabilidade", "integração de sistemas", "sistemas que conversam entre si",
        "dados que circulam"],
    "Dados Públicos e Abertos": ["dados públicos", "divulgação dos dados em formato aberto",
        "open data", "dados de domínio público informacional"],
    "Serviços Públicos Digitais": ["serviços públicos digitais", "digitalizar serviços federais",
        "serviços com protocolo digital", "serviços digitalizados", "atendimento multicanal",
        "um governo para cada pessoa", "serviços personalizados"],
    "Identidade Digital": ["identidade digital", "cpf como identificador único",
        "identidade e serviços digitais integrados", "vínculo ao cpf", "sistema nacional unificado"],
    "Segurança, Governança e Proteção de Dados": ["segurança cibernética", "proteção de dados pessoais",
        "lgpd", "soberania digital", "governança digital coordenada", "governança federativa",
        "registro de acesso", "responsabilização", "protocolos de segurança", "padrões abertos",
        "gestão de dados", "políticas de dados"],
    "Inclusão Digital": ["inclusão digital", "inclusão tecnológica", "autonomia digital",
        "acessibilidade digital", "capacitação tecnológica", "acesso universal a ferramentas digitais"],
    "INDE": ["INDE", "Infraestrutura Nacional de Dados da Educação", "EducaDados",
        "infraestrutura de dados educacionais"],
    "SNE": ["SNE", "Sistema Nacional de Educação", "novo pacto federativo da educação", "SUS da educação"],
    "CMDEB": ["CMDEB", "Conjunto Mínimo de Dados da Educação Básica",
        "conjunto mínimo de dados educacionais", "padrão nacional de dados educacionais"],
    "SIGEDs/SGEs": ["SIGED", "SGE", "sistemas de gestão educacional", "sistema de gestão escolar",
        "software de gestão acadêmica", "sistema de administração escolar",
        "plataforma de gestão pedagógica"],
    "Identificador único do estudante": ["identificador único do estudante", "CPF na educação",
        "rastreio de trajetória entre redes", "matrícula única", "cadastro único do aluno",
        "ID nacional do estudante"],
    "Jornada do estudante": ["jornada do estudante", "trajetória escolar", "percurso do estudante",
        "acompanhamento contínuo do aluno"],
    "Gestão Educacional": ["gestão educacional", "capacidades estatais das secretarias de educação",
        "maturidade de gestão"],
    "Metas, Indicadores e Equidade": ["metas de equidade", "resultados agregados por raça",
        "indicadores de equidade", "integração entre políticas", "participação multissetorial"],
    "Instrumentos e siglas": ["censo escolar", "educacenso", "SAEB",
        "Sistema de Avaliação da Educação Básica", "IDEB", "PNE", "Plano Nacional de Educação",
        "Novo PAR", "Plano de Ações Articuladas", "Fundeb", "VAAR", "Pé-de-Meia",
        "recomposição de aprendizagem", "evasão escolar", "abandono escolar", "sistema de alerta"],
    "Outros termos a rastrear": ["diário de classe digital", "matrícula digital", "matrícula online",
        "boletim digital", "carteirinha estudantil digital", "painel de gestão escolar",
        "painel de indicadores educacionais"],
    "Saúde Digital": ["saúde digital", "modelo de digitalização da saúde", "prontuário eletrônico",
        "telemedicina", "sistemas de gestão em saúde",
        "interoperabilidade entre sistemas público e privado", "registro nacional de saúde"],
    "PIX": ["pix", "infraestrutura de pagamentos instantâneos", "interoperabilidade financeira",
        "ecossistema de dados financeiros", "arranjo de pagamento universal", "integração bancária",
        "padronização de transações", "centralização de pagamentos"],
    "CIN": ["CIN", "carteira de identidade nacional", "identificação única e nacional",
        "integração de bases de dados civis", "documento de identidade digital",
        "registro único nacional", "biometria integrada", "identidade digital do cidadão",
        "padronização da identificação civil", "interoperabilidade de registros de identificação"],
}

TEMA = {"C": "Conectividade", "I": "Infraestrutura Pública Digital"}

CASOS = ([(f"lista {n}", termo, d) for n, termo, dest in LISTA for d in dest]
         + [(f"pdf {bloco}", termo, "I") for bloco, termos in PDF.items() for termo in termos])


@pytest.mark.parametrize("origem,termo,destino", CASOS)
def test_termo_no_vocabulario_do_tema(origem, termo, destino):
    tema = TEMA[destino]
    texto = _norm_busca(termo)
    assert any(p.search(texto) for _, p in _AUSENCIA_RE[tema]), (
        f"{origem}: '{termo}' não casa nenhum termo de TERMOS_AUSENCIA['{tema}']")


def test_temas_em_educacao_e_eixo_antigo_fora():
    for tema in ("Conectividade", "Infraestrutura Pública Digital", "Tecnologia na Educação"):
        assert EIXO_DO_TEMA[tema] == "Educação"
        assert tema in TERMOS_ANCORA and tema in TERMOS_AUSENCIA
    assert "Conectividade e Infraestrutura Digital" not in EIXOS
    for antigo in ("Conectividade Escolar", "Financiamento e Governança da Conectividade",
                   "Dados e Sistemas Educacionais", "Inclusão Digital"):
        assert antigo not in EIXO_DO_TEMA
        assert antigo not in TERMOS_ANCORA and antigo not in TERMOS_AUSENCIA
