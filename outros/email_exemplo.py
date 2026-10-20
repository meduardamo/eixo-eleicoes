"""Manda os emails automáticos com dados de exemplo para um endereço só.

Serve para ver a moldura (compartilhado/email_layout.py) num leitor de email de
verdade sem disparar para a lista. Os dados abaixo são inventados.

    python -m outros.email_exemplo pessoa@exemplo.com
"""

import sys
from datetime import datetime

from compartilhado.email_utils import enviar_email


def _pesquisas():
    from relatorios.relatorios_extracao_segmentos import _html
    pesq = [
        dict(numero_identificacao="BR-08431/2026", empresa_contratada="Quaest",
             abrangencia="BRASIL", cargos="Presidente"),
        dict(numero_identificacao="BR-08502/2026", empresa_contratada="Datafolha",
             abrangencia="BRASIL", cargos="Presidente"),
        dict(numero_identificacao="SP-04120/2026", empresa_contratada="Paraná Pesquisas",
             abrangencia="SP", cargos="Presidente"),
        dict(numero_identificacao="MG-03311/2026", empresa_contratada="Real Time Big Data",
             abrangencia="MG", cargos="Governador"),
    ]
    return "Pesquisas eleitorais previstas para hoje (4)", _html(pesq, datetime.now().strftime("%Y-%m-%d"))


def _noticias():
    from outros.noticias_eleicoes_scraper import ROTULO_TEMA, _html_alertas
    tema = list(ROTULO_TEMA)
    cab = "Alerta | EixoGov\n20/10/2026\nTítulo\n"
    alertas = [
        dict(alerta_tema=tema[0], titulo="Nova pesquisa mostra empate técnico no 2º turno para presidente",
             fonte="g1", data="20/10/2026", instituto="Quaest", link="https://g1.globo.com",
             resumo=cab + "Levantamento ouviu 2.400 eleitores entre 16 e 18 de outubro. A margem de erro é de 2 pontos."),
        dict(alerta_tema=tema[1], titulo="Governador eleito anuncia equipe de transição na Educação",
             fonte="O Globo", data="20/10/2026", candidato="Fulano de Tal", partido="PSD", uf="RJ",
             link="https://oglobo.globo.com",
             resumo=cab + "A equipe terá cinco integrantes e começa a trabalhar na próxima semana."),
        dict(alerta_tema=tema[1], titulo="Governador eleito no RS define coordenador da transição",
             fonte="GZH", data="19/10/2026", candidato="Beltrano", partido="MDB", uf="RS",
             link="https://gzh.clicrbs.com.br",
             resumo=cab + "O coordenador vai conduzir o levantamento das pastas de Saúde e Fazenda."),
    ]
    return "3 alertas de notícias", _html_alertas(alertas, datetime.now())


def _debates():
    from outros.resumo_debates import email_da_rodada
    feitos = [
        dict(titulo="Resumo do debate ao governo de São Paulo, Band, 19/10/2026", cargo="Governador",
             tipo="Debate", quando="19/10/2026, 22h", emissora="Band",
             participantes=["Candidato A", "Candidato B"], link="https://docs.google.com"),
        dict(titulo="Resumo do debate presidencial, Globo, 23/10/2026", cargo="Presidente",
             tipo="Debate", quando="23/10/2026, 22h", emissora="Globo",
             participantes=["Candidato C", "Candidato D"], link="https://docs.google.com",
             atualizado=True),
    ]
    return email_da_rodada(feitos, datetime.now().strftime("%d/%m/%Y, %Hh"))


def _instagram():
    from outros import instagram_relatorio as ig
    posts = [
        dict(candidato="Candidato A", partido="PL", uf="RJ", cargo="Senador", tipo="Vídeo",
             curtidas=48210, comentarios=3120, publicado="2026-10-19T14:00:00",
             resumo="Vídeo de agenda com promessa de ampliar escolas em tempo integral.",
             link="https://instagram.com", temas="Educação"),
        dict(candidato="Candidato B", partido="PT", uf="BA", cargo="Governador", tipo="Foto",
             curtidas=12044, comentarios=530, publicado="2026-10-19T09:00:00",
             resumo="Foto com prefeitos do interior; legenda agradece apoios do segundo turno.",
             link="https://instagram.com", temas="Campanha"),
    ]
    num = dict(posts=2, candidatos=2, ufs=2, curtidas=60254, comentarios=3650)
    return "Clipping do Instagram", ig.html_email(posts, num, "19/10/2026",
                                                  "clipping_instagram_exemplo.pdf",
                                                  "https://docs.google.com")


def main():
    if len(sys.argv) < 2 or "@" not in sys.argv[1]:
        sys.exit("uso: python -m outros.email_exemplo pessoa@exemplo.com")
    para = [sys.argv[1].strip()]
    falhas = 0
    for montar in (_pesquisas, _noticias, _debates, _instagram):
        assunto, html = montar()
        if not enviar_email(f"[EXEMPLO] {assunto}", html, para):
            falhas += 1
    if falhas:
        sys.exit(f"{falhas} email(s) não saíram, veja o log acima")


if __name__ == "__main__":
    main()
