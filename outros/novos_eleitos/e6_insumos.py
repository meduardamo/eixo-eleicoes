"""Etapa 6. Insumos por origem: de onde vêm os temas de cada nome.

Universo:
  --universo pre      antes da urna. Senado: os competitivos que não disputam reeleição.
                      Câmara: quem hoje senta no Senado ou numa assembleia. Assembleias: quem
                      hoje senta no Senado ou na Câmara.
  --universo eleitos  depois da etapa 5: todo eleito que não é reeleição.

O que é buscado, conforme o tipo de origem:
- Câmara (hoje ou no passado): proposições PL, PLP, PEC e PDL desde 2019 em que a pessoa
  é autora ou coautora, com o tema oficial que a própria Câmara atribui a cada uma. A
  contagem de temas sai daqui, sem modelo.
- Senado: matérias de autoria e relatorias desde 2019 e as comissões em que relatou. A API
  do Senado não publica tema por matéria nesse endpoint.
- Governo estadual e prefeitura: link do plano de governo da eleição que a pessoa venceu,
  pelo histórico da ficha no DivulgaCand.
- Todos: redes sociais declaradas no registro de 2026.
Assembleia e Câmara Municipal não têm API única: a coluna "De onde puxar os temas" diz o caminho.

Saída: insumos_<universo>.csv; com --publicar, aba "Insumos por origem (pré-mapeados)" ou
"(eleitos)" na planilha "Pré mapeamento".
Rodar: python -m outros.novos_eleitos.e6_insumos --universo pre [--publicar]
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from outros.novos_eleitos import comum as c

ROTULO_UNIVERSO = {"pre": "pré-mapeados", "eleitos": "eleitos"}
TIPOS_CAMARA = ("PL", "PLP", "PEC", "PDL")
SIGLAS_SENADO = {"PL", "PLS", "PLP", "PLC", "PEC", "PDL", "PDS"}
DESDE = 2019


def universo(nome):
    """O pré-mapeamento que a Manu pediu antes da urna: no Senado, os competitivos que não
    disputam reeleição; na Câmara, quem hoje senta no Senado ou numa assembleia; nas
    assembleias, quem hoje senta no Senado ou na Câmara. Prefeito e governador ficam para
    depois da urna, no universo dos eleitos."""
    if nome == "pre":
        pre = c.ler_csv("pre_mapeamento.csv")
        nao_reeleicao = pre["Reeleição, volta ou novo"] != "Reeleição"
        cadeira_hoje = pre.Origem.str.contains("em exercício")
        senado = (pre["Casa disputada"] == "Senado") & (pre["É competitivo? (Senado)"] == "Sim")
        camara = (pre["Casa disputada"] == "Câmara") & pre["Tipo de origem"].isin(["Senado", "Assembleia"]) & cadeira_hoje
        assembleia = (pre["Casa disputada"] == "Assembleia") & pre["Tipo de origem"].isin(["Senado", "Câmara"]) & cadeira_hoje
        return pre[nao_reeleicao & (senado | camara | assembleia)].copy()
    novos = c.ler_csv("novos_eleitos.csv")
    return novos[novos["Reeleição, volta ou novo"] != "Reeleição"].copy()


def camara(id_camara):
    props = c.em_cache("camara_autoria", id_camara, lambda: list(c.paginar_camara("proposicoes", {
        "idDeputadoAutor": id_camara, "siglaTipo": ",".join(TIPOS_CAMARA),
        "dataApresentacaoInicio": f"{DESDE}-01-01", "ordem": "DESC", "ordenarPor": "id"})))
    buscar_temas = lambda pid: c.em_cache("camara_temas", pid, lambda: [
        t["tema"] for t in (c.buscar_json(f"{c.API_CAMARA}/proposicoes/{pid}/temas") or {}).get("dados", [])])
    with ThreadPoolExecutor(8) as ex:
        temas = Counter(t for lista in ex.map(buscar_temas, [str(p["id"]) for p in props]) for t in lista)
    recentes = [f"{p['siglaTipo']} {p['numero']}/{p['ano']}" for p in props[:3]]
    return {"Câmara: proposições desde 2019 (autoria ou coautoria)": str(len(props)),
            "Câmara: temas mais frequentes": "; ".join(f"{t} ({n})" for t, n in temas.most_common(5)),
            "Câmara: proposições mais recentes": "; ".join(recentes)}


def senado(cod):
    aut = c.senado(f"senador/{cod}/autorias", chave_cache=f"autorias_{cod}") or {}
    rel = c.senado(f"senador/{cod}/relatorias", chave_cache=f"relatorias_{cod}") or {}
    autorias = c.como_lista((aut.get("MateriasAutoriaParlamentar", {}).get("Parlamentar", {})
                             .get("Autorias") or {}).get("Autoria"))
    relatorias = c.como_lista((rel.get("MateriasRelatoriaParlamentar", {}).get("Parlamentar", {})
                               .get("Relatorias") or {}).get("Relatoria"))
    materia = lambda x: x.get("Materia", {})
    numero = lambda x: (int(materia(x).get("Ano", 0) or 0), int(materia(x).get("Numero", 0) or 0))
    # a API devolve fora de ordem: "mais recentes" tem que ser por ano e número
    autorias = sorted((a for a in autorias if materia(a).get("Sigla") in SIGLAS_SENADO
                       and numero(a)[0] >= DESDE), key=numero, reverse=True)
    # mesmos tipos da autoria, para as duas colunas medirem a mesma coisa
    relatorias = [r for r in relatorias if materia(r).get("Sigla") in SIGLAS_SENADO
                  and int(str(r.get("DataDesignacao", "0"))[:4] or 0) >= DESDE]
    comissoes = Counter(r.get("Comissao", {}).get("Sigla", "") for r in relatorias)
    return {"Senado: matérias de autoria desde 2019": str(len(autorias)),
            "Senado: matérias de autoria mais recentes": "; ".join(
                materia(a).get("DescricaoIdentificacao", "") for a in autorias[:3]),
            "Senado: relatorias desde 2019": str(len(relatorias)),
            "Senado: comissões em que relatou": "; ".join(
                f"{s} ({n})" for s, n in comissoes.most_common(5) if s)}


def ficha_2026(linha, eleicao_2026, casa_cands):
    cand = casa_cands.get(linha["SQ_CANDIDATO"])
    return c.detalhe_divulgacand(2026, cand["uf"], eleicao_2026, linha["SQ_CANDIDATO"]) if cand else None


def planos(ficha):
    """Plano de governo das eleições para governador ou prefeito que a pessoa venceu."""
    achados = []
    for e in (ficha or {}).get("eleicoesAnteriores") or []:
        cargo = str(e.get("cargo", ""))
        if (e.get("nrAno") and int(e["nrAno"]) < 2026 and cargo in ("Governador", "Prefeito")
                and str(e.get("situacaoTotalizacao", "")).startswith("Eleito")):
            det = c.detalhe_divulgacand(e["nrAno"], e["sgUe"], e["idEleicao"], e["id"])
            link = c.link_plano(det)
            achados.append(f"{cargo} {e['nrAno']} ({e.get('local', '')}): {link or 'sem plano no TSE'}")
    return " | ".join(achados)


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], required=True)
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    base = universo(a.universo)
    cands = c.ler_csv("candidaturas_2026.csv").set_index("sq_candidato").to_dict("index")
    eleicao_2026 = c.codigo_eleicao_divulgacand(2026)
    print(f"universo {a.universo}: {len(base)} nomes")
    linhas = []
    for i, (_, r) in enumerate(base.iterrows(), 1):
        saida = {col: r[col] for col in ["Casa disputada", "Nome", "Partido", "UF", "Reeleição, volta ou novo",
                                         "Origem", "Tipo de origem", "De onde puxar os temas"]}
        for id_camara in [x for x in str(r["ID na Câmara"]).split(";") if x]:
            saida.update(camara(id_camara))
        for cod in [x for x in str(r.get("Código no Senado", "")).split(";") if x]:
            saida.update(senado(cod))
        ficha = ficha_2026(r, eleicao_2026, cands)
        if r["Tipo de origem"] in ("Governo estadual", "Prefeitura"):
            saida["Plano de governo"] = planos(ficha)
        perfis = c.perfis_declarados((ficha or {}).get("sites"))
        saida["Instagram"] = perfis.pop("Instagram", "")
        saida["Outras redes declaradas"] = "; ".join(f"{k}: {v}" for k, v in perfis.items())
        saida["SQ_CANDIDATO"] = r["SQ_CANDIDATO"]
        linhas.append(saida)
        if i % 25 == 0:
            print(f"   {i}/{len(base)}")
    ordem = ["Casa disputada", "Nome", "Partido", "UF", "Reeleição, volta ou novo", "Origem", "Tipo de origem",
             "De onde puxar os temas", "Câmara: proposições desde 2019 (autoria ou coautoria)",
             "Câmara: temas mais frequentes", "Câmara: proposições mais recentes",
             "Senado: matérias de autoria desde 2019", "Senado: matérias de autoria mais recentes",
             "Senado: relatorias desde 2019", "Senado: comissões em que relatou", "Plano de governo",
             "Instagram", "Outras redes declaradas", "SQ_CANDIDATO"]
    df = pd.DataFrame(linhas).reindex(columns=ordem).fillna("")
    c.salvar_csv(df, f"insumos_{a.universo}.csv")
    if a.publicar:
        c.gravar_aba(c.planilha_destino(), f"Insumos por origem ({ROTULO_UNIVERSO[a.universo]})", df)


if __name__ == "__main__":
    main()
