"""Classificação temática do clipping do Instagram.

O caso que motivou o arquivo: até 28/08/2026 o casamento era substring pura, e
"rio" (Meio Ambiente) casava dentro de comentáRIOs e pRIOridade. No clipping de
28/08 isso pôs 155 dos 571 posts em Meio Ambiente, 89 só por essa substring.
Cada teste aqui é uma armadilha real vista na planilha, não caso inventado.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "outros"))

from instagram_relatorio import mapear_tema_principal, subtemas_educacao  # noqa: E402


def tema(tags="", legenda=""):
    return mapear_tema_principal(tags, legenda)


# ── Substring que não é a palavra ────────────────────────────────────────────

def test_rio_dentro_de_outra_palavra_nao_e_meio_ambiente():
    for legenda in ("Obrigado pelos comentários de incentivo!",
                    "Saúde é prioridade no nosso governo",
                    "Estamos no interior do estado",
                    "Feliz aniversário, companheiro",
                    "Reunião com o secretário e o ministério"):
        assert tema("Campanha eleitoral", legenda) != "Meio Ambiente", legenda


def test_saudade_nao_e_saude():
    assert tema("Campanha eleitoral", "Que saudade de vocês, meu povo") != "Saúde"


def test_escolha_nao_e_educacao():
    assert tema("Campanha eleitoral", "A escolha é do povo") != "Educação"


def test_redes_sociais_nao_e_assistencia_social():
    assert tema("Propaganda eleitoral",
                "Me segue nas redes sociais") != "Assistência Social"


def test_solidariedade_partido_nao_e_assistencia_social():
    assert tema("Campanha eleitoral",
                "Clécio Luís, do Solidariedade") != "Assistência Social"


def test_discriminacao_nao_e_seguranca():
    assert tema("Direitos humanos",
                "Não aceitamos discriminação") != "Segurança Pública"


def test_hospitalidade_nao_e_saude():
    assert tema("Campanha eleitoral",
                "A hospitalidade do povo daqui") != "Saúde"


# ── Nome de lugar não é assunto ──────────────────────────────────────────────

def test_toponimo_com_rio_ou_floresta_nao_e_meio_ambiente():
    for legenda in ("Comício em Nísia Floresta",
                    "Carreata em Mãe do Rio",
                    "Caminhada em Rio Branco",
                    "Agenda no Rio Grande do Norte"):
        assert tema("Campanha eleitoral", legenda) != "Meio Ambiente", legenda


def test_floresta_sozinha_continua_valendo():
    assert tema("Meio ambiente", "Vamos preservar a floresta") == "Meio Ambiente"


# ── Hashtag em CamelCase ─────────────────────────────────────────────────────

def test_hashtag_camelcase_vira_palavra():
    assert tema("", "#PreservacaoAmbiental #Floresta") == "Meio Ambiente"


# ── O assunto ganha da campanha ──────────────────────────────────────────────

def test_pauta_ganha_do_residual():
    assert tema("Propaganda eleitoral Segurança pública",
                "Concurso para 5 mil policiais militares") == "Segurança Pública"
    assert tema("Campanha eleitoral Saúde pública",
                "Vamos entregar o hospital") == "Saúde"


def test_post_so_de_campanha_fica_em_atos_de_campanha():
    assert tema("Campanha eleitoral Carreata",
                "Hoje tem carreata, bora pra rua!") == "Atos de Campanha e Propaganda"


def test_tag_do_gemini_pesa_mais_que_a_legenda():
    # A legenda cita trabalho (termo fraco); a tag diz do que o post é.
    assert tema("Educação em tempo integral",
                "Mais uma manhã de trabalho") == "Educação"


# ── Alianças ─────────────────────────────────────────────────────────────────

def test_apoio_da_rua_nao_e_alianca():
    assert tema("Campanha eleitoral",
                "Que apoio lindo do povo na caminhada") != "Alianças e Apoios Políticos"


def test_apoio_politico_e_alianca():
    assert tema("Apoio político Coligação",
                "Anunciamos o apoio do partido") == "Alianças e Apoios Políticos"


# ── Tema estreito ────────────────────────────────────────────────────────────

def test_causa_animal_ganha_de_seguranca():
    assert tema("Causa animal / Proteção animal Maus-tratos aos animais "
                "Segurança pública Delegacia especializada",
                "Delegacia de proteção animal") == "Causa Animal"


# ── Sem sinal nenhum ─────────────────────────────────────────────────────────

def test_sem_termo_cai_em_outros_assuntos():
    assert tema("", "") == "Outros Assuntos"
    assert tema("Bastidores", "Bom dia!") == "Outros Assuntos"


def test_detalhar_devolve_a_evidencia():
    resultado = mapear_tema_principal("Saúde pública", "Novo hospital", detalhar=True)
    tema_achado, pontos, termos = resultado
    assert tema_achado == "Saúde"
    assert pontos > 0
    assert "hospital" in termos


# ── Categorias dentro de Educação (27/10/2026) ───────────────────────────────

TI, EPT, CM = ("Tempo integral", "Educação profissional e tecnológica (EPT)",
               "Escolas cívico-militares")


def test_categorias_de_educacao_saem_de_tags_reais():
    assert subtemas_educacao("* educacao * escola em tempo integral * creche") == [TI]
    assert subtemas_educacao("educacao tecnica; ensino profissionalizante; trilhas de futuro") == [EPT]
    assert subtemas_educacao("* educacao * escolas civico-militares * juventude") == [CM]
    assert subtemas_educacao("ampliacao do ensino em tempo integral, do ensino tecnico "
                             "e das escolas civico-militares") == [TI, EPT, CM]


def test_desfile_civico_militar_nao_e_escola():
    assert subtemas_educacao("* 7 de setembro * desfile cívico-militar * campanha") == []


def test_integral_solto_nao_e_tempo_integral():
    assert subtemas_educacao("saúde integral da mulher e proteção integral da criança") == []


def test_curso_profissionalizante_sozinho_nao_e_ept():
    assert subtemas_educacao("cursos profissionalizantes para quem busca emprego") == []
    assert subtemas_educacao("parceria com Senai e Senac nos cursos profissionalizantes") == [EPT]


def test_professora_no_nome_de_urna_nao_e_educacao():
    # 27/10/2026: 13 dos 21 posts de Educação eram agradecimento eleitoral.
    assert tema("Eleições; Tocantins; Professora Dorinha; Aliança política; Agradecimento.") != "Educação"
    assert tema("Apoio político; Professora Maria do Carmo; Direita.") != "Educação"
    assert tema("", "Obrigada, Tocantins! #ProfessoraDorinha") != "Educação"


def test_professores_continuam_sendo_educacao():
    assert tema("Valorização dos professores; Piso salarial", "") == "Educação"
    assert tema("", "Reajuste para a professora da rede estadual") == "Educação"


def test_desfile_civico_militar_nao_vai_para_educacao():
    assert tema("7 de Setembro; Independência do Brasil; Desfile Cívico-Militar; "
                "Eleições / Campanha Eleitoral; Mato Grosso") != "Educação"


def test_vocabulario_que_nao_e_educacao():
    # Casos lidos um a um na revisão de 27/10/2026.
    assert tema("Família; Dia dos Pais", "meu pai me ensinou a viver com ética") != "Educação"
    assert tema("Religião; Fé", "o direito de cada um professar sua fé") != "Educação"
    assert tema("Trajetória política; movimento estudantil; democracia") != "Educação"
    assert tema("Caminhada de rua; bairro Universitário; Alan Rick") != "Educação"
    assert tema("Agenda de campanha; quadra da escola de samba Vitória Régia") != "Educação"


def test_educacao_com_tags_precisa_de_termo_nas_tags():
    assert tema("Propaganda eleitoral; Mato Grosso; Renovação política",
                "mudar a saúde, a educação e a segurança") != "Educação"
    # Sem tags (antes da análise do Gemini), a legenda decide.
    assert tema("", "boa notícia para a educação do DF! nomeados 2.681 novos professores") == "Educação"


def test_lista_de_bandeiras_vai_para_campanha():
    assert tema("Eleições; Saúde pública; Educação; Segurança pública; Infraestrutura (BR-319); "
                "Agradecimento de votos") == "Atos de Campanha e Propaganda"


def test_categoria_de_educacao_nas_tags_puxa_para_educacao():
    assert tema("Eleições estaduais (Ceará); Educação pública e tempo integral; Saúde e hospitais "
                "regionais; Geração de emprego e atração de indústrias") == "Educação"


def test_homenagem_de_dia_dos_pais_nao_e_educacao():
    assert tema("Paternidade; Dia dos Pais; Família; Gratidão; Educação; Trabalho") != "Educação"
    assert tema("Dia dos Pais; Família; Valores Familiares; Educação dos Filhos") != "Educação"
    assert tema("Dia dos Pais; Escola em tempo integral; Governo do Ceará") == "Educação"


def test_educacao_financeira_nao_e_educacao():
    assert tema("Valores familiares; Educação financeira; Poupança; Resiliência") != "Educação"


def test_obra_de_creche_e_educacao_lista_de_obras_e_infraestrutura():
    assert tema("Educação infantil; obras públicas; infraestrutura; Riacho das Almas; creche; "
                "Governo de Pernambuco.") == "Educação"
    assert tema("Infraestrutura e Obras Públicas (rodovias e pontes); Saúde pública (hospitais "
                "regionais); Educação (escolas)") == "Infraestrutura e Obras"


def test_reforma_de_escola_em_lista_de_obras_fica_em_infraestrutura():
    assert tema("Infraestrutura; Obras paradas; Pavimentação de rodovias; Pontes de concreto; "
                "Educação; Reforma de escolas") == "Infraestrutura e Obras"
