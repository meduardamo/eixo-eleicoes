"""Documento no timbrado: instruções da segunda fase (revisão manual) do mapeamento da 58ª Legislatura.
Para os coordenadores de Saúde, Educação e Esporte repassarem a analistas e estagiários. 16/11/2026.
Reaproveita os helpers de montar_doc_presidente_governos_2t.py (radar-congresso). Uso: python doc_instrucoes_revisao.py saida.docx"""
import sys
from pathlib import Path

GER = Path.home() / "Downloads/radar-congresso/resultado-2026/montar_doc_presidente_governos_2t.py"
_linhas = GER.read_text().splitlines()
TEMPLATE = Path.home() / "Downloads/eleicoes-2026/eixo-eleicoes/outros/templates/timbrado_eleicoes.docx"
SAIDA = Path(sys.argv[1])
# imports (8-23), cores (62-64) e helpers até cartoes() (197-442); fica de fora o sumário, que precisa de duas passadas
exec("\n".join(_linhas[7:23] + _linhas[61:64] + _linhas[196:442]))

from docx.oxml import OxmlElement as _El
from docx.oxml.ns import qn as _qn

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ids import valor  # IDs só em variável de ambiente (repositório público)
CAMARA = "https://docs.google.com/spreadsheets/d/" + valor("SPREADSHEET_ID_58_CAMARA")
SENADO = "https://docs.google.com/spreadsheets/d/" + valor("SPREADSHEET_ID_58_SENADO")


def link(p, texto, url, tam=9.5):
    rid = p.part.relate_to(url, RT.HYPERLINK, is_external=True)
    h = _El("w:hyperlink"); h.set(_qn("r:id"), rid)
    r = fonte(p.add_run(texto), tam, True, MARINHO)
    r.font.underline = True
    h.append(r._r); p._p.append(h)


def item(rotulo, texto, num=None):
    p = paragrafo("", 9.5, depois=4, linha=1.35)
    if num is not None:
        fonte(p.add_run(f"{num}.  "), 9.5, True, VINHO)
    else:
        fonte(p.add_run("•  "), 9.5, True, AMBAR)
    if rotulo:
        fonte(p.add_run(rotulo), 9.5, True, MARINHO)
    fonte(p.add_run(texto), 9.5)
    return p


# ---------------------------------------------------------------- título
paragrafo("Mapeamento da 58ª Legislatura", 20, True, MARINHO, depois=0, familia="Montserrat Black", linha=1.05)
paragrafo("Instruções da revisão manual", 20, True, VINHO, depois=6, familia="Montserrat Black", linha=1.05)
paragrafo("Segunda fase · Saúde, Educação e Esporte · 16 a 24 de novembro de 2026", 9, cor=CINZA, depois=10)

cartoes([("Prazo da revisão", "24/11", "terça-feira, fim do dia", VINHO),
         ("Por cliente", "25 nomes", "em cada planilha, Câmara e Senado", AMBAR),
         ("Tempo estimado", "5 min", "por cliente em cada planilha, mais os nomes que estranhar", MARINHO)])

# ---------------------------------------------------------------- o que é
secao_titulo("O que é a revisão")
paragrafo("A primeira fase classificou cada deputado e senador como Promotor, Neutro ou Detrator para cada cliente, "
          "a partir de dados públicos: projetos lidos no texto, relatorias, votações nominais, discursos e falas em "
          "comissão. A revisão traz o que a régua não vê e o monitoramento de vocês sabe.", depois=6)
paragrafo("Ninguém revisa a base inteira. Cada cliente tem uma lista de até 25 nomes por Casa: os de mais peso "
          "político e os de classificação frágil, que mudaria com um único sinal.", depois=4)

# ---------------------------------------------------------------- prazos
secao_titulo("Prazos")
tabela([("Dia", 1700, None), ("O quê", LARGURA - 1700, None)], [
    ["Ter 24/11", "Prazo final da revisão (sexta, 20/11, é feriado)"],
    ["Qua 25/11", "Marcações aplicadas, com retorno na coluna Situação"],
    ["Qui 26/11", "Responder só as linhas marcadas como Precisa de fonte"],
    ["Seg 30/11", "Envio aos clientes"],
])

# ---------------------------------------------------------------- onde
secao_titulo("Onde fazer")
p = paragrafo("", 9.5, depois=4, linha=1.35)
fonte(p.add_run("•  "), 9.5, True, AMBAR); link(p, "Planilha da Câmara", CAMARA)
fonte(p.add_run("   e   "), 9.5); link(p, "Planilha do Senado", SENADO)
item("Aba Revisão: lista prioritária. ", "É a única aba em que se escreve.")
item("Aba Revisão: andamento. ", "Mostra quanto cada cliente já andou. Atualiza sozinha.")
item("Para consulta: ", "Por parlamentar (dúvida sobre um nome) e Descrição dos clientes, no fim, com o que cada "
     "cliente defende. As demais abas não devem ser editadas.")

# ---------------------------------------------------------------- passo a passo
secao_titulo("Passo a passo")
item("", "Filtre a coluna Cliente pelo seu cliente.", 1)
item("", "Leia a linha: relevância e o que a define, classificação, sinais e por que o nome está na lista.", 2)
item("", "Passe o mouse na célula da Classificação para ler as evidências.", 3)
item("Está certo: ", "marque a caixa Conferido e siga para o próximo nome. A linha fica verde.", 4)
item("Está errado: ", "marque Conferido e preencha as colunas abaixo.", 5)

tabela([("Coluna", 2300, None), ("O que escrever", LARGURA - 2300, None)], [
    ["O que está errado", "Classificação errada (deveria ser outra) ou Evidência errada ou faltando (a classificação "
                          "pode estar certa)."],
    ["Como deveria ficar", "Promotor, Neutro, Detrator ou Sem posição clara. Em branco se só a evidência está errada."],
    ["O que você sabe e de onde vem", "O fato e a fonte: projeto, fala, notícia, reunião, conversa com o cliente."],
    ["Quem revisou", "Seu nome, para a dúvida chegar a quem marcou."],
])

# ---------------------------------------------------------------- regras
secao_titulo("Regras")
item("Só vale informação concreta, com fonte. ", "Partido, perfil e impressão não contam.")
item("Bastidor vale. ", "Muda a classificação, mas o cliente vê só “informação do monitoramento da Eixo”.")
item("Nome fora da lista: ", "mandar direto para a equipe de Dados, com a fonte.")
item("Coluna Situação: ", "traz o retorno de cada marcação. Aplicado, Precisa de fonte (completar a fonte) ou Não "
     "aplicado (o motivo fica na nota da célula).")

# ---------------------------------------------------------------- como ler
secao_titulo("Como a classificação foi feita").paragraph_format.page_break_before = True  # referência, sempre na última página
tabela([("Sinal", 1500, None), ("O que conta", LARGURA - 1500, None)], [
    ["Forte", "Autoria, relatoria com parecer, voto nominal."],
    ["Médio", "Coautoria, pedido de audiência que mostra direção."],
    ["Fraco", "Discurso, urgência, fala em comissão, imprensa. Sozinho, nunca classifica."],
])
item("Promotor ou Detrator: ", "dois sinais fortes do mesmo lado, ou um forte e um médio.")
item("Neutro: ", "sinais fortes dos dois lados, sem diferença que decida.")
item("Abaixo disso: ", "Um sinal a favor ou contra, Sinais opostos (pouco sinal, cada um num lado) e Registro sem "
     "sinal (atua no tema, mas nada dá lado para o cliente).")
item("Partido não entra. ", "O lado de cada projeto vem da base de pautas, da posição pública do cliente ou da "
     "descrição do cliente.")

doc.core_properties.title = "Mapeamento da 58ª Legislatura: instruções da revisão manual"
doc.core_properties.author = "Eixo"
doc.save(SAIDA)
print("salvo", SAIDA)
