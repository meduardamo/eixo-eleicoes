"""Etapa 15. De onde vem cada senador da próxima legislatura.

Junta, por senador, o que já existe sobre a origem e completa com os cargos não eletivos:
- origem e mandatos eletivos desde 2004 (aba "Senado eleito 2026", vindos do TSE);
- passagem pela Câmara (exercício desde 2003) e mandatos no Senado (etapas 13 e 14);
- redes sociais declaradas e planos de governo, pela ficha da candidatura de 2026 no DivulgaCand;
- cargos da ficha da Wikipedia: mandato anterior a 2004, ministério, secretaria, carreira
  pública. Para os nomes já pesquisados na etapa 11 vale o que está lá, que inclui a
  conferência manual. O verbete só é aceito se trouxer o ano de nascimento e a UF.

Saída em dados_novos_eleitos/perfil_senado_origem/: origem.csv (uma linha por senador) e
cargos.csv (uma linha por cargo, com a fonte). Sem verbete aceito, a linha diz isso.
Rodar: python -m outros.novos_eleitos.e15_origem_senado   (depois das etapas 13 e 14)
"""
import re

import pandas as pd

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.e6_insumos import planos
from outros.novos_eleitos.e11_trajetoria_competitivos import (ENTRA_NO_RESUMO, UF_NOME, WIKI_URL, busca, cargos_wiki,
                                                             pagina, pasta, texto_periodo, tipo_cargo)

ABA = "Senado eleito 2026"
GRUPOS = ("curso", "reeleitos", "novos")
CONJUGE = "Cônjuge de chefe do Executivo"
# cargos que a regra da etapa 11 deixa em "Nomeação ou função pública" e não são nomeação
RECLASSIFICAR = [(r"^vice-presidente d[oa] (brasil|rep[úu]blica)", "Mandato eletivo"),
                 (r"^(vice-)?l[íi]der d", "Função parlamentar (Mesa, frente)")]


def reclassificar(cargo, tipo):
    return next((novo for padrao, novo in RECLASSIFICAR if re.search(padrao, cargo.lower())), tipo)


def juntar(linhas):
    """Uma entrada por cargo: quem ocupou o mesmo cargo duas vezes fica com os dois períodos."""
    periodos = {}
    for x in linhas:
        periodos.setdefault(x["Cargo"], [])
        if x["Período"] and x["Período"] not in periodos[x["Cargo"]]:
            periodos[x["Cargo"]].append(x["Período"])
    return "; ".join(f"{cargo} ({', '.join(ps) or 'sem data'})" for cargo, ps in periodos.items())


def verbete(consultas, ano, uf):
    """Primeiro verbete com ficha que traga o ano de nascimento e o nome da UF."""
    if not ano:
        return ""
    for q in dict.fromkeys(q for q in consultas if q.strip()):
        for t in busca(q):
            p = pagina(t)
            if p and "{{Info/" in p["texto"][:6000] and ano in p["texto"][:6000] and UF_NOME[uf] in p["texto"]:
                return p["titulo"]
    return ""


def main():
    aba = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), ABA))
    senado = pd.concat([c.ler_csv(f"perfil_senado_{g}/resumo.csv") for g in GRUPOS]).drop_duplicates("Parlamentar")
    extras = senado[~senado.Parlamentar.isin(aba.Parlamentar)]
    aba = pd.concat([aba, extras[["Parlamentar", "Partido", "UF", "Situação", "Observação"]]], ignore_index=True).fillna("")
    senado = senado.set_index("Parlamentar")
    camara = c.ler_csv("perfil_camara_senado/resumo.csv").drop_duplicates("Parlamentar").set_index("Parlamentar")
    cands = c.ler_csv("candidaturas_2026.csv").drop_duplicates("sq_candidato").set_index("sq_candidato")
    feitos = c.ler_csv("competitivos_senado_cargos.csv")

    eleicao_2026, eleicao_2022 = c.codigo_eleicao_divulgacand(2026), c.codigo_eleicao_divulgacand(2022)
    # quem não disputou em 2026 (eleito em 2022 ou suplente) tem a ficha na candidatura de 2022
    tse_2022 = c.consulta_cand(2022)
    tse_2022["civil"] = tse_2022.NM_CANDIDATO.map(c.chave_nome)
    exercicio = c.ler_csv("exercicio_casas.csv")
    exercicio = exercicio[exercicio.casa == "Senado"]
    origem, cargos = [], []
    for l in aba.to_dict("records"):
        nome, sq = l["Parlamentar"], l["SQ_CANDIDATO"]
        # ficha da candidatura: redes declaradas e plano das eleições de governador ou prefeito vencidas
        ficha_tse, ano_ficha = (c.detalhe_divulgacand(2026, cands.uf[sq], eleicao_2026, sq), "2026") if sq in cands.index else (None, "")
        if not ficha_tse:
            e = exercicio[exercicio.id_casa == str(senado["Código no Senado"].get(nome, ""))]
            titulos, cpfs = set(e.titulo) - {""}, set(e.cpf) - {""}
            civis = {c.chave_nome(n) for n in e.nome_civil if n}
            achado = tse_2022[tse_2022.titulo.isin(titulos) | tse_2022.cpf.isin(cpfs)
                              | (tse_2022.civil.isin(civis) & (tse_2022.SG_UF == l["UF"]))]
            if len(achado):
                r = achado.iloc[0]
                ficha_tse, ano_ficha = c.detalhe_divulgacand(2022, r.SG_UE, eleicao_2022, r.SQ_CANDIDATO), "2022"
        perfis = c.perfis_declarados((ficha_tse or {}).get("sites"))
        ja = feitos[feitos.SQ_CANDIDATO == sq] if sq else feitos.iloc[0:0]
        if len(ja):
            linhas = ja.drop(columns=["Nome", "SQ_CANDIDATO"]).to_dict("records")
            fonte = next((f for f in ja.Fonte if "wikipedia" in f), "")
        else:
            nasc = (senado.Nascimento.get(nome) or (cands.nascimento.get(sq) if sq in cands.index else "") or "")[:4]
            titulo = verbete([f"{nome} político", l["Nome civil (TSE)"].title(), f"{l['Nome de urna (TSE)'].title()} político",
                              f"{nome} senador"], nasc, l["UF"])
            fonte = WIKI_URL + titulo.replace(" ", "_") if titulo else ""
            linhas = []
            for cargo, ps in (cargos_wiki(titulo) if titulo else []):
                tipo = reclassificar(cargo, tipo_cargo(cargo))
                linhas.append({"Partido": l["Partido"], "UF": l["UF"], "Cargo": cargo, "Tipo": tipo,
                               "Pasta (tema da Câmara)": pasta(cargo, tipo), "Período": texto_periodo(ps), "Fonte": fonte,
                               "Checagem": "Com fonte (Wikipedia)"})
        for x in linhas:
            x["Tipo"] = reclassificar(x["Cargo"], x["Tipo"])
            cargos.append({"Parlamentar": nome, "Situação": l["Situação"], **x})
        validos = [x for x in linhas if not str(x["Checagem"]).startswith("Não confirmado")]
        nao_eletivos = [x for x in validos if x["Tipo"] in ENTRA_NO_RESUMO and x["Tipo"] != CONJUGE]
        conjuge = [x for x in validos if x["Tipo"] == CONJUGE]
        eletivos = [x for x in validos if x["Tipo"] == "Mandato eletivo"]
        origem.append({
            "Parlamentar": nome, "Partido": l["Partido"], "UF": l["UF"], "Situação": l["Situação"],
            "De onde vem": l["De onde vem"], "Tipo de origem": l["Tipo de origem"],
            "Mandatos eletivos desde 2004 (TSE)": l["Mandatos anteriores (histórico TSE, desde 2004)"],
            "Câmara: legislaturas iniciadas em": camara["Legislaturas iniciadas em"].get(nome, ""),
            "Senado: mandatos": senado["Mandatos no Senado"].get(nome, ""),
            "Mandatos eletivos na ficha (Wikipedia)": juntar(eletivos),
            "Cargos não eletivos (Wikipedia)": juntar(nao_eletivos),
            "Cônjuge de chefe do Executivo (Wikipedia)": juntar(conjuge),
            "Pasta dos cargos não eletivos": "; ".join(dict.fromkeys(
                x["Pasta (tema da Câmara)"] for x in nao_eletivos if str(x["Pasta (tema da Câmara)"]).strip())),
            "Fonte dos cargos": fonte or "Sem verbete aceito na Wikipedia",
            "Plano de governo (eleições de governador ou prefeito que venceu)": planos(ficha_tse) if ficha_tse else "",
            "Instagram": perfis.pop("Instagram", ""),
            "Outras redes declaradas": "; ".join(f"{k}: {v}" for k, v in perfis.items()),
            "Ficha do TSE usada (redes e planos)": f"Candidatura de {ano_ficha}" if ficha_tse else "Sem candidatura em 2022 nem em 2026",
            "Observação": l["Observação"]})

    pasta_saida = c.caminho("perfil_senado_origem")
    pasta_saida.mkdir(parents=True, exist_ok=True)
    origem, cargos = pd.DataFrame(origem), pd.DataFrame(cargos)
    origem.to_csv(pasta_saida / "origem.csv", index=False)
    cargos.to_csv(pasta_saida / "cargos.csv", index=False)
    sem = origem[origem["Fonte dos cargos"].str.startswith("Sem verbete")]
    print(f"{len(origem)} senadores, {len(cargos)} cargos; sem verbete: {len(sem)}: " + "; ".join(sem.Parlamentar))


if __name__ == "__main__":
    main()
