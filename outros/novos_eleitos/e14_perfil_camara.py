"""Etapa 14. Perfil geral na Câmara dos Deputados, parlamentar por parlamentar.

Par da etapa 13: as mesmas tabelas, com as mesmas colunas, para o que o parlamentar fez na
Câmara. Serve para o senador que veio de lá (é o registro que diz o que ele pensa antes de
chegar ao Senado) e, depois, para os 513 deputados.

Universo:
  --universo senado   senadores da aba "Senado eleito 2026" que já exerceram na Câmara
                      (legislaturas iniciadas de 2003 em diante)
  --universo camara   deputados da aba "Câmara eleita 2026" que já exerceram na Câmara (reeleitos, os que
                      voltam e os novos que exerceram como suplente); discursos só desde 2023

O que é buscado por deputado:
- ficha: nascimento, escolaridade, profissão, legislaturas (API);
- colegiados: comissões e demais órgãos, com cargo e período, e frentes (API);
- autoria: toda proposição que assina, marcando o primeiro signatário (arquivos anuais);
- relatorias: só o relator do último andamento de cada proposição. A Câmara não publica o
  histórico de relatorias por deputado;
- emendas: as proposições de autoria cujo tipo é emenda, com a proposição emendada;
- votações nominais em plenário e em comissão (arquivos anuais);
- discursos, com sumário, palavras-chave e a nota taquigráfica (transcrição) da própria Câmara (API).

Tema: o que a Câmara atribui a cada proposição. Em votação, o tema é o da proposição votada.

Saída: dados_novos_eleitos/perfil_camara_<universo>/ com um CSV por tabela e um xlsx.
Rodar: python -m outros.novos_eleitos.e14_perfil_camara --universo senado
"""
import argparse
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
import requests

from outros.novos_eleitos import comum as c

ARQ = "https://dadosabertos.camara.leg.br/arquivos/{0}/csv/{0}-{1}.csv"
ANOS = range(2003, date.today().year + 1)
LEGISLATIVAS = {"PL", "PLP", "PEC", "PDL", "PDC", "PRC", "MPV", "PLV"}
PRIMEIRO_ANO = {52: 2003, 53: 2007, 54: 2011, 55: 2015, 56: 2019, 57: 2023}
FICHA_PROPOSICAO = "https://www.camara.leg.br/proposicoesWeb/fichadetramitacao?idProposicao="
ABA_SENADO = "Senado eleito 2026"
ABA_CAMARA = "Câmara eleita 2026"
DISCURSOS_DESDE = {"senado": 2003, "camara": 2023}


def arquivo(recurso, ano, pasta):
    p = c.caminho(pasta) / f"{recurso}-{ano}.csv"
    if not (p.exists() and p.stat().st_size):
        p.parent.mkdir(parents=True, exist_ok=True)
        r = requests.get(ARQ.format(recurso, ano), timeout=900)
        r.raise_for_status()
        p.write_bytes(r.content)
    return p


def ler(recurso, ano, pasta, colunas):
    return pd.read_csv(arquivo(recurso, ano, pasta), sep=";", dtype=str, keep_default_na=False, usecols=colunas)


def api(recurso, chave):
    return c.em_cache("camara_perfil", chave, lambda: (c.buscar_json(f"{c.API_CAMARA}/{recurso}") or {}).get("dados"))


def api_paginada(recurso, params, chave):
    return c.em_cache("camara_perfil", chave, lambda: list(c.paginar_camara(recurso, params)))


def id_final(uri):
    return str(uri).rstrip("/").rsplit("/", 1)[-1] if uri else ""


# ---------------------------------------------------------------- universo

def universo_senado():
    """Senadores da próxima legislatura que passaram pela Câmara. A ligação é pelo título ou
    CPF (candidatura de 2026 ou exercício no Senado) e, sem eles, pelo nome civil na UF."""
    from outros.novos_eleitos.e13_perfil_senado import codigos
    aba = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), ABA_SENADO))
    ex = c.ler_csv("exercicio_casas.csv")
    cam, sen = ex[ex.casa == "Câmara"], ex[ex.casa == "Senado"]
    cands = c.ler_csv("candidaturas_2026.csv").drop_duplicates("sq_candidato").set_index("sq_candidato")
    cod_senado = codigos()
    linhas = []
    for l in aba.to_dict("records"):
        titulos, cpfs = set(), set()
        if l["SQ_CANDIDATO"] in cands.index:
            titulos.add(cands.titulo[l["SQ_CANDIDATO"]]), cpfs.add(cands.cpf[l["SQ_CANDIDATO"]])
        cod = cod_senado.get((c.chave_nome(l["Parlamentar"]), l["UF"])) or cod_senado.get(
            (c.chave_nome(l["Nome civil (TSE)"]), l["UF"]))
        s = sen[sen.id_casa == str(cod)]
        titulos |= set(s.titulo)
        cpfs |= set(s.cpf)
        titulos, cpfs = {t for t in titulos if t}, {k for k in cpfs if k}
        civis = {c.chave_nome(n) for n in [l["Nome civil (TSE)"], *s.nome_civil] if n}
        achado = cam[cam.titulo.isin(titulos) | cam.cpf.isin(cpfs) | cam.nome_civil.map(c.chave_nome).isin(civis)]
        for d in achado.drop_duplicates("id_casa").to_dict("records"):
            linhas.append({"Parlamentar": l["Parlamentar"], "Partido": l["Partido"], "UF": l["UF"],
                           "Situação": l["Situação"], "id": d["id_casa"], "legislaturas": d["legislaturas"],
                           "Nome na Câmara": d["nome_casa"], "UF na Câmara": d["uf"]})
    return pd.DataFrame(linhas)


def universo_camara():
    """Deputados da próxima legislatura que já exerceram na Câmara. A ligação é pelo título ou CPF da
    candidatura de 2026 e, sem eles, pelo nome civil na UF."""
    aba = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), ABA_CAMARA))
    ex = c.ler_csv("exercicio_casas.csv")
    cam = ex[ex.casa == "Câmara"].copy()
    cam["chave"] = cam.nome_civil.map(c.chave_nome)
    cands = c.ler_csv("candidaturas_2026.csv").drop_duplicates("sq_candidato").set_index("sq_candidato")
    linhas = []
    for l in aba.to_dict("records"):
        t = cands.titulo.get(l["SQ_CANDIDATO"], "") if l["SQ_CANDIDATO"] in cands.index else ""
        k = cands.cpf.get(l["SQ_CANDIDATO"], "") if l["SQ_CANDIDATO"] in cands.index else ""
        achado = cam[(cam.titulo == t) & (t != "") | (cam.cpf == k) & (k != "")]
        if achado.empty:
            achado = cam[(cam.chave == c.chave_nome(l["Nome civil (TSE)"])) & (cam.uf == l["UF"])]
        for d in achado.drop_duplicates("id_casa").to_dict("records"):
            linhas.append({"Parlamentar": l["Parlamentar"], "Partido": l["Partido"], "UF": l["UF"],
                           "Situação": l["Situação"], "id": d["id_casa"], "legislaturas": d["legislaturas"],
                           "Nome na Câmara": d["nome_casa"], "UF na Câmara": d["uf"]})
    return pd.DataFrame(linhas)


# ---------------------------------------------------------------- tabelas pela API

def ficha(l):
    d = api(f"deputados/{l['id']}", f"deputado_{l['id']}") or {}
    prof = api(f"deputados/{l['id']}/profissoes", f"profissoes_{l['id']}") or []
    anos = [PRIMEIRO_ANO.get(int(x)) for x in str(l["legislaturas"]).split(";") if x]
    return {"Parlamentar": l["Parlamentar"], "Partido": l["Partido"], "UF": l["UF"], "Situação": l["Situação"],
            "Código na Câmara": l["id"], "Nome na Câmara": l["Nome na Câmara"], "UF pela qual exerceu": l["UF na Câmara"],
            "Legislaturas iniciadas em": "; ".join(str(a) for a in anos if a),
            "Situação na Câmara hoje": (d.get("ultimoStatus") or {}).get("situacao") or "",
            "Nascimento": d.get("dataNascimento") or "",
            "Naturalidade": f"{d.get('municipioNascimento') or ''}/{d.get('ufNascimento') or ''}".strip("/"),
            "Escolaridade": d.get("escolaridade") or "",
            "Profissão": "; ".join(dict.fromkeys(p.get("titulo") or "" for p in prof if p.get("titulo"))),
            "Página na Câmara": f"https://www.camara.leg.br/deputados/{l['id']}"}


def colegiados(l):
    hoje = str(date.today())
    org = api_paginada(f"deputados/{l['id']}/orgaos", {"dataInicio": "2003-01-01", "dataFim": hoje}, f"orgaos_{l['id']}_{hoje}")
    fre = api(f"deputados/{l['id']}/frentes", f"frentes_{l['id']}_{hoje}") or []
    exerce = ((api(f"deputados/{l['id']}", f"deputado_{l['id']}") or {}).get("ultimoStatus") or {}).get("situacao") == "Exercício"
    saida = [{"Parlamentar": l["Parlamentar"], "Sigla": o.get("siglaOrgao") or "", "Colegiado": o.get("nomeOrgao") or "",
              "Casa": "CD", "Tipo": "Comissão ou subcomissão", "Papel": o.get("titulo") or "",
              "Início": (o.get("dataInicio") or "")[:10], "Fim": (o.get("dataFim") or "")[:10],
              "Vigente": "Sim" if not o.get("dataFim") or o["dataFim"][:10] >= hoje else "Não"} for o in org]
    saida += [{"Parlamentar": l["Parlamentar"], "Sigla": "", "Colegiado": f.get("titulo") or "", "Casa": "CD",
               "Tipo": "Frente ou grupo", "Papel": "Membro",
               "Início": str(PRIMEIRO_ANO.get(f.get("idLegislatura"), "")), "Fim": "",
               "Vigente": "Sim" if exerce and f.get("idLegislatura") == max(PRIMEIRO_ANO) else "Não"} for f in fre]
    return saida


FALHAS = []


def discursos_periodo(id_camara, inicio, fim, chave):
    return api_paginada(f"deputados/{id_camara}/discursos", {"dataInicio": inicio, "dataFim": fim}, chave)


UFS = {"AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
       "RS", "RO", "RR", "SC", "SP", "SE", "TO"}


def do_proprio(transcricao, l):
    """A API de discursos por deputado mistura homônimos: no código do Júlio Cesar (PSD/PI) vieram 174 falas do
    Julio Cesar Ribeiro (Republicanos/DF). Fala cujo cabeçalho traz outra UF fica de fora."""
    cab = re.match(r"\s*(?:O SR\.|A SRA\.)[^(]*\(([^)]*)\)", transcricao)
    if not cab:
        return True
    ufs = {u for u in re.findall(r"\b[A-Z]{2}\b", cab.group(1)) if u in UFS}
    return not ufs or bool(ufs & set(f"{l.get('UF na Câmara') or ''} {l.get('UF') or ''}".replace(";", " ").replace(",", " ").split()))


def discursos(l, desde=2003):
    """Um ano por chamada e sem ordenação (com ordenarPor a API dá erro 500 da segunda página em
    diante). Ano que ainda assim dá erro é tentado mês a mês; o mês que falhar fica registrado."""
    saida = []
    anos = sorted({PRIMEIRO_ANO[int(x)] + k for x in str(l["legislaturas"]).split(";") if x for k in range(4)})
    for ano in [a for a in anos if desde <= a <= date.today().year]:
        sufixo = "" if ano < date.today().year else f"_{date.today()}"
        try:
            lista = discursos_periodo(l["id"], f"{ano}-01-01", f"{ano}-12-31", f"discursos_{l['id']}_{ano}{sufixo}")
        except RuntimeError:
            lista = []
            for mes in range(1, 13):
                fim = pd.Timestamp(ano, mes, 1) + pd.offsets.MonthEnd(0)
                try:
                    lista += discursos_periodo(l["id"], f"{ano}-{mes:02d}-01", str(fim.date()),
                                               f"discursos_{l['id']}_{ano}_{mes:02d}{sufixo}") or []
                except RuntimeError:
                    FALHAS.append({"Parlamentar": l["Parlamentar"], "Tabela": "discursos", "Período": f"{mes:02d}/{ano}",
                                   "Motivo": "A API da Câmara devolveu erro 500"})
        for p in lista or []:
            if not do_proprio(p.get("transcricao") or "", l):
                continue
            saida.append({"Parlamentar": l["Parlamentar"], "Data": (p.get("dataHoraInicio") or "")[:10],
                          "Tipo": p.get("tipoDiscurso") or "", "Casa": "CD", "Resumo": p.get("sumario") or "",
                          "Indexação": p.get("keywords") or "", "Link": p.get("urlTexto") or "",
                          "Texto": (p.get("transcricao") or "").strip()})
    return saida


# ---------------------------------------------------------------- tabelas pelos arquivos anuais

def arquivos(uni):
    ids = set(uni.id)
    nome = dict(zip(uni.id, uni.Parlamentar))
    aut, vot, vts = [], [], []
    for ano in ANOS:
        a = ler("proposicoesAutores", ano, "camara_autores", ["idProposicao", "idDeputadoAutor", "ordemAssinatura", "proponente", "nomeAutor"])
        todos = a.groupby("idProposicao").nomeAutor.agg(lambda s: ", ".join(s[:6]) + (" e outros" if len(s) > 6 else ""))
        a = a[a.idDeputadoAutor.isin(ids)].copy()
        a["Autoria"] = a.idProposicao.map(todos)
        aut.append(a)
        v = ler("votacoesVotos", ano, "camara_votos", ["idVotacao", "voto", "deputado_id"])
        vot.append(v[v.deputado_id.isin(ids)])
        vts.append(ler("votacoes", ano, "camara_votacoes", ["id", "data", "siglaOrgao", "votosSim", "votosNao", "votosOutros", "descricao"]))
        print(f"  arquivos de {ano} lidos", end="\r")
    aut, vot = pd.concat(aut, ignore_index=True), pd.concat(vot, ignore_index=True)
    vts = pd.concat(vts, ignore_index=True).drop_duplicates("id")
    vot["idProposicao"] = vot.idVotacao.str.split("-").str[0]

    precisa = set(aut.idProposicao) | set(vot.idProposicao)
    props, rel, indice, tem = [], [], [], []
    for ano in ANOS:
        p = ler("proposicoes", ano, "camara_proposicoes", ["id", "siglaTipo", "numero", "ano", "ementa", "dataApresentacao", "uriPropPrincipal",
                                                           "ultimoStatus_dataHora", "ultimoStatus_uriRelator", "ultimoStatus_siglaOrgao",
                                                           "ultimoStatus_descricaoSituacao"])
        p["id_relator"] = p.ultimoStatus_uriRelator.map(id_final)
        indice.append(p[["id", "siglaTipo", "numero", "ano"]])
        rel.append(p[p.id_relator.isin(ids)])
        props.append(p[p.id.isin(precisa)])
        t = ler("proposicoesTemas", ano, "camara_temas_ano", ["uriProposicao", "tema"])
        t["id"] = t.uriProposicao.map(id_final)
        tem.append(t[["id", "tema"]])
    props, rel = pd.concat(props).drop_duplicates("id").set_index("id"), pd.concat(rel).drop_duplicates("id")
    indice = pd.concat(indice).drop_duplicates("id")
    rotulo = dict(zip(indice.id, indice.siglaTipo + " " + indice.numero + "/" + indice.ano))
    tema = pd.concat(tem).drop_duplicates().groupby("id").tema.agg("; ".join).to_dict()
    campo = lambda i, k: props.at[i, k] if i in props.index else ""

    autoria, emendas = [], []
    for a in aut.drop_duplicates(["idProposicao", "idDeputadoAutor"]).to_dict("records"):
        i, sigla = a["idProposicao"], campo(a["idProposicao"], "siglaTipo")
        papel = ""
        if sigla in LEGISLATIVAS:
            papel = "Primeiro autor" if a["ordemAssinatura"] == "1" and a["proponente"] == "1" else "Coautor"
        linha = {"Parlamentar": nome[a["idDeputadoAutor"]], "Matéria": rotulo.get(i, ""), "Tipo": sigla,
                 "Apresentação": campo(i, "dataApresentacao")[:10], "Papel": papel, "Ementa": campo(i, "ementa"),
                 "Classificação do Senado": "", "Tema": tema.get(i, ""), "Tramitando": campo(i, "ultimoStatus_descricaoSituacao"),
                 "Autoria": a["Autoria"], "Link": FICHA_PROPOSICAO + i}
        autoria.append(linha)
        principal = id_final(campo(i, "uriPropPrincipal"))
        if sigla.startswith(("EM", "ES", "SB")) and principal:
            emendas.append({"Parlamentar": linha["Parlamentar"], "Emenda": linha["Matéria"], "Matéria emendada": rotulo.get(principal, ""),
                            "Apresentação": linha["Apresentação"], "Colegiado": campo(i, "ultimoStatus_siglaOrgao"),
                            "O que a emenda faz": linha["Ementa"], "Autoria": a["Autoria"], "Link": linha["Link"]})

    relatorias = [{"Parlamentar": nome[r["id_relator"]], "Matéria": rotulo.get(r["id"], ""), "Tipo": r["siglaTipo"],
                   "Colegiado": r["ultimoStatus_siglaOrgao"], "Papel": "Relator no último andamento",
                   "Designação": "", "Destituição": "", "Motivo do encerramento": r["ultimoStatus_descricaoSituacao"],
                   "Ementa": r["ementa"], "Autoria da matéria": "", "Classificação do Senado": "", "Tema": tema.get(r["id"], ""),
                   "Link": FICHA_PROPOSICAO + r["id"]} for r in rel.to_dict("records")]

    v = vot.merge(vts, left_on="idVotacao", right_on="id", how="left").fillna("")
    votos = [{"Parlamentar": nome[r["deputado_id"]], "Data": r["data"], "Matéria": rotulo.get(r["idProposicao"], ""),
              "Tipo": campo(r["idProposicao"], "siglaTipo"), "Votação": r["descricao"], "Voto": r["voto"], "Secreta": "Não",
              "Sim": r["votosSim"], "Não": r["votosNao"], "Abstenção": r["votosOutros"],
              "Ementa": campo(r["idProposicao"], "ementa"), "Classificação do Senado": "", "Tema": tema.get(r["idProposicao"], ""),
              "Link": FICHA_PROPOSICAO + r["idProposicao"], "Colegiado": r["siglaOrgao"]} for r in v.to_dict("records")]
    return {"autoria": pd.DataFrame(autoria), "relatorias": pd.DataFrame(relatorias), "emendas": pd.DataFrame(emendas),
            "votos": pd.DataFrame(votos)}


# ---------------------------------------------------------------- resumo

def contar(df):
    return Counter(t for v in df["Tema"] for t in str(v).split("; ") if t) if len(df) else Counter()


def resumo(f, tab):
    de = lambda t: tab[t][tab[t]["Parlamentar"] == f["Parlamentar"]] if len(tab[t]) else tab[t]
    col, aut, rel, vot = de("colegiados"), de("autoria"), de("relatorias"), de("votos")
    principal = aut[aut["Papel"] == "Primeiro autor"] if len(aut) else aut
    top = lambda d: "; ".join(f"{t} ({n})" for t, n in contar(d).most_common(5))
    com = col[col["Tipo"] == "Comissão ou subcomissão"] if len(col) else col
    plen = vot[vot["Colegiado"] == "PLEN"] if len(vot) else vot
    return {**f,
            "Órgãos em que mais esteve": "; ".join(f"{s} ({n})" for s, n in Counter(com["Sigla"]).most_common(6)) if len(com) else "",
            "Frentes": int((col["Tipo"] == "Frente ou grupo").sum()) if len(col) else 0,
            "Proposições legislativas como primeiro autor": len(principal),
            "Temas da autoria principal (5 maiores)": top(principal),
            "Proposições legislativas como coautor": int((aut["Papel"] == "Coautor").sum()) if len(aut) else 0,
            "Requerimentos, emendas e outros": int((aut["Papel"] == "").sum()) if len(aut) else 0,
            "Relator no último andamento": len(rel), "Temas das relatorias (5 maiores)": top(rel),
            "Emendas apresentadas": len(de("emendas")),
            "Votações nominais em plenário": len(plen), "Votações nominais em comissão": len(vot) - len(plen),
            "Votos Sim / Não / Abstenção (plenário)": " / ".join(
                str(int((plen["Voto"] == k).sum()) if len(plen) else 0) for k in ("Sim", "Não", "Abstenção")),
            "Discursos registrados": len(de("discursos"))}


def temas_por_parlamentar(tab):
    linhas = []
    for nome in tab["ficha"]["Parlamentar"]:
        de = lambda t: tab[t][tab[t]["Parlamentar"] == nome] if len(tab[t]) else tab[t]
        aut, vot = de("autoria"), de("votos")
        a = contar(aut[aut["Papel"] == "Primeiro autor"]) if len(aut) else Counter()
        k = contar(aut[aut["Papel"] == "Coautor"]) if len(aut) else Counter()
        r, v = contar(de("relatorias")), contar(vot)
        for t in sorted(set(a) | set(k) | set(r) | set(v)):
            linhas.append({"Parlamentar": nome, "Tema": t, "Primeiro autor": a[t], "Coautor": k[t],
                           "Relatorias": r[t], "Votações nominais abertas": v[t]})
    return pd.DataFrame(linhas)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universo", choices=["senado", "camara"], default="senado")
    nome_universo = ap.parse_args().universo
    uni = universo_senado() if nome_universo == "senado" else universo_camara()
    # quem exerceu por duas UFs ou com dois registros fica com uma linha por registro na Câmara
    print(f"{nome_universo}: {uni.Parlamentar.nunique()} nomes com passagem pela Câmara, {len(uni)} registros")
    linhas = uni.to_dict("records")

    tab = {"ficha": pd.DataFrame([ficha(l) for l in linhas])}
    desde = DISCURSOS_DESDE[nome_universo]
    for nome, f in (("colegiados", colegiados), ("discursos", lambda l: discursos(l, desde))):
        with ThreadPoolExecutor(3) as ex:
            tab[nome] = pd.DataFrame([x for bloco in ex.map(f, linhas) for x in bloco])
        print(f"  {nome}: {len(tab[nome])} linhas")
    tab.update(arquivos(uni))
    for nome in ("autoria", "relatorias", "emendas", "votos"):
        print(f"  {nome}: {len(tab[nome])} linhas")
    tab["temas"] = temas_por_parlamentar(tab)
    tab["resumo"] = pd.DataFrame([resumo(f, tab) for f in tab["ficha"].to_dict("records")])

    pasta = c.caminho(f"perfil_camara_{nome_universo}")
    pasta.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(pasta / f"perfil_camara_{nome_universo}.xlsx") as xl:
        for nome in ["resumo", "temas", "colegiados", "autoria", "relatorias", "emendas", "votos", "discursos"]:
            tab[nome].to_csv(pasta / f"{nome}.csv", index=False)
            # a nota taquigráfica fica só no CSV: passa do limite de caracteres de uma célula
            tab[nome].drop(columns=["Texto"], errors="ignore").to_excel(xl, sheet_name=nome, index=False)
    pd.DataFrame(FALHAS, columns=["Parlamentar", "Tabela", "Período", "Motivo"]).to_csv(pasta / "falhas.csv", index=False)
    print(f"Gravado em {pasta}; períodos que a API não devolveu: {len(FALHAS)}")


if __name__ == "__main__":
    main()
