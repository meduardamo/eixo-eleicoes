"""Etapa 21. Perfil dos senadores nos grandes temas dos clientes: saúde, educação, esporte e primeira infância.

Separa, por grupo de senadores (eleitos em 2022, reeleitos, novos), as matérias de iniciativa de
cada um nos quatro temas, com a situação atual de cada matéria (tramitando, arquivada, virou lei),
para a leitura que escreve o bloco "O que fez". Saúde, educação e esporte vêm da área do
levantamento (etapa 18). Primeira infância não é área de nenhuma das Casas: os candidatos são as
matérias com o público Crianças e adolescentes, e cada uma é lida e marcada à mão em
temas_clientes/primeira_infancia_lida.csv (coluna "Primeira infância": Sim ou Não).

Situação: API de processos do Senado (normaGerada, situacaoAtual) para matéria do Senado;
API da Câmara (statusProposicao) para matéria da Câmara. Assembleia e câmara municipal ficam sem.

Saída em dados_novos_eleitos/temas_clientes/: materias_<grupo>.csv, uma linha por senador,
tema e matéria.
Rodar: python -m outros.novos_eleitos.e21_temas_clientes_senado --grupo curso   (depois da 18)
"""
import argparse
import re

import pandas as pd

from outros.novos_eleitos import comum as c

GRUPOS = {"curso": ["Mandato em curso"], "reeleitos": ["Reeleito", "Reeleita"], "novos": ["Novo", "Nova"]}
TEMAS = {"Saúde": "Saúde", "Educação": "Educação", "Esporte e lazer": "Esporte"}
API_CAMARA = "https://dadosabertos.camara.leg.br/api/v2"
# relatoria só conta como feita quando o relator chegou ao parecer; redistribuição, troca na comissão,
# relator ad hoc e fim de legislatura encerram a designação sem relatório dele
PARECER = {"Deliberação da matéria", "Matéria deliberada no plenário", "Parecer Oferecido", "Parecer de Plenário",
           "Matéria com tramitação encerrada", "Fim de tramitação da matéria"}


def papel_relatoria():
    """(Parlamentar, Matéria) -> o que aconteceu com a relatoria no Senado, o melhor caso entre as designações."""
    rel = pd.concat([c.ler_csv(f"perfil_senado_{g}/relatorias.csv") for g in ("curso", "reeleitos", "novos")])
    ordem = {"Relatou (parecer)": 0, "Relator atual": 1, "Designado, saiu antes do parecer": 2}
    rel["papel"] = [("Relatou (parecer)" if m in PARECER else "Relator atual" if not m and not d
                     else "Designado, saiu antes do parecer") for m, d in zip(rel["Motivo do encerramento"], rel["Destituição"])]
    rel["o"] = rel.papel.map(ordem)
    return rel.sort_values("o").drop_duplicates(["Parlamentar", "Matéria"]).set_index(["Parlamentar", "Matéria"]).papel.to_dict()


def situacao_senado(codigo):
    d = c.em_cache("senado_processo_por_materia", codigo, lambda: c.buscar_json(
        "https://legis.senado.leg.br/dadosabertos/processo", params={"codigoMateria": codigo},
        headers={"Accept": "application/json"}))
    d = (d or [{}])[0] if isinstance(d, list) else (d or {})
    norma = d.get("normaGerada") or ""
    return norma or (d.get("situacaoAtual") or "").capitalize(), bool(norma)


def situacao_camara(id_prop):
    d = c.em_cache("camara_proposicao", id_prop, lambda: (c.buscar_json(f"{API_CAMARA}/proposicoes/{id_prop}") or {}).get("dados"))
    s = ((d or {}).get("statusProposicao") or {}).get("descricaoSituacao") or ""
    return s, "norma jur" in s.lower()


def situacao(link):
    m = re.search(r"materia/(\d+)", link)
    if m:
        return situacao_senado(m.group(1))
    m = re.search(r"idProposicao=(\d+)", link)
    if m:
        return situacao_camara(m.group(1))
    return "", False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grupo", choices=GRUPOS, required=True)
    grupo = ap.parse_args().grupo
    lev = c.ler_csv("levantamento_senado/levantamento.csv")
    nomes = lev[lev["Situação"].isin(GRUPOS[grupo])].Parlamentar.tolist()
    reg = c.ler_csv("levantamento_senado/registros.csv")
    reg = reg[reg.Parlamentar.isin(nomes)]

    por_area = reg[reg.Tema.isin(TEMAS)].assign(**{"Tema do cliente": lambda d: d.Tema.map(TEMAS)})
    criancas = reg[reg["Públicos"].str.contains("Crianças e adolescentes")].assign(**{"Tema do cliente": "Primeira infância"})
    lida = c.caminho("temas_clientes") / "primeira_infancia_lida.csv"
    if lida.exists():
        marcadas = pd.read_csv(lida, dtype=str).fillna("")
        sim = {k for k, v in zip(zip(marcadas.Casa, marcadas["Matéria"]), marcadas["Primeira infância"]) if v == "Sim"}
        criancas = criancas[[(a, b) in sim for a, b in zip(criancas.Casa, criancas["Matéria"])]]
    df = pd.concat([por_area, criancas]).drop_duplicates(["Parlamentar", "Tema do cliente", "Casa", "Registro", "Matéria"])
    papeis = papel_relatoria()
    df["Papel"] = [("Primeiro autor" if r == "Primeiro autor" else "Relator no último andamento (Câmara)"
                    if casa == "Câmara" else papeis.get((p, m), "Relator atual"))
                   for p, m, r, casa in zip(df.Parlamentar, df["Matéria"], df.Registro, df.Casa)]
    df["Ano"] = df["Matéria"].str.extract(r"/(\d{4})")[0].fillna("")
    sit = {l: situacao(l) for l in dict.fromkeys(df.Link)}
    df["Situação da matéria"] = df.Link.map(lambda l: sit[l][0])
    df["Virou lei"] = df.Link.map(lambda l: "Sim" if sit[l][1] else "")
    df = df.sort_values(["Parlamentar", "Tema do cliente", "Ano"], ascending=[True, True, False])
    colunas = ["Parlamentar", "Tema do cliente", "Casa", "Registro", "Papel", "Matéria", "Ano", "Ementa", "Situação da matéria",
               "Virou lei", "Públicos", "Link"]
    c.salvar_csv(df[colunas], f"temas_clientes/materias_{grupo}.csv")
    print(df.groupby("Tema do cliente").size().to_dict(), "| viraram lei:", (df["Virou lei"] == "Sim").sum())
    print(df.Papel.value_counts().to_dict())


if __name__ == "__main__":
    main()
