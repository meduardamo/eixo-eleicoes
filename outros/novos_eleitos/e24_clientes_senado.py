"""Etapa 24. Classificação dos 81 senadores para cada cliente, a partir da base de pautas dos coordenadores.

Entrada: temas_clientes/base_pautas.csv (cópia da planilha "Eixo | Base de pautas por cliente", abas Clientes
Educação, Saúde e Esporte) e temas_clientes/posicoes_clientes_complemento.csv (posição informada fora da planilha,
a confirmar com a coordenação). Só entram projetos marcados Promotor ou Detrator; Dúvida e em branco aguardam.

Sinais do senador em cada projeto (Senado e, para quem foi deputado, Câmara):
- forte: primeiro autor; voto nominal no próprio projeto ou no parecer dele (Sim conta no sentido do projeto,
  Não no sentido contrário), desde que a votação tenha dividido os parlamentares da base (consenso não conta). Votação de emenda, destaque ou requerimento não conta, porque o sentido para o
  cliente depende do texto da emenda;
- médio: coautor;
- só evidência, sem contar: relatoria (o sentido depende do parecer, que não foi lido).
Projeto Promotor dá sinal a favor do cliente; Detrator, contra.

Régua (a mesma da Metodologia): Promotor ou Detrator com dois sinais fortes na mesma direção ou um forte e um médio;
fortes nos dois sentidos: diferença de dois decide, de um decide com médio no mesmo sentido, senão Neutro;
abaixo disso, Não tem informação suficiente.

PEC tem numeração própria em cada Casa, e projeto de lei também até 2018: nesses casos a Casa da matéria tem de
bater com a anotada na base de pautas.

Saída: temas_clientes/clientes_classificacao.csv e clientes_resumo.csv
Rodar: python -m outros.novos_eleitos.e24_clientes_senado   (depois da 23)
"""
import re

import pandas as pd

from outros.novos_eleitos import comum as c

GRUPOS = ("curso", "reeleitos", "novos")
VOTO_PROJETO = r"Votação nominal d[oa] (?:Projeto|Proposta)|Aprovad[oa] o (?:Parecer|Substitutivo)|Rejeitad[oa] o (?:Parecer|Substitutivo)|nos termos do parecer"


def chaves(projeto):
    casa = "Câmara" if "Câmara" in projeto else "Senado" if "Senado" in projeto else None
    texto, sig, out = re.sub(r"\(.*?\)", "", projeto), None, []
    for m in re.finditer(r"(PLP|PEC|PL|RQS|REQ|PDL)?\s*(\d+)\s*/\s*(\d{4})", texto):
        sig = m.group(1) or sig
        if sig:
            k = f"{sig} {m.group(2)}/{m.group(3)}"
            exige = casa if (sig == "PEC" or int(m.group(3)) < 2019 or sig in ("REQ", "RQS")) else None
            out.append((k, exige))
    return out


# Sinais na pauta, além dos projetos listados: votações nominais do Senado que separam os senadores na pauta e
# projetos de autoria cujo sentido a ementa mostra. sentido +1 = Sim vai na direção de quem é a favor da pauta.
PAUTAS = {
    "Apostas online e saúde mental": {
        "votacoes": [("2023-12-12", "PL 3626", (37, 27), +1, "Emenda 139 da lei das bets (tirar jogos on-line e cassino)"),
                     ("2023-12-12", "PL 3626", (25, 36), +1, "Emenda 157 da lei das bets (proibir publicidade em arenas e patrocínio)"),
                     ("2025-12-17", "RQS 857", (28, 36), -1, "urgência do PL 2234 (legaliza cassinos e jogos)")],
        "comissao": [("2024-06-19", "CCJ", "PL 2234/2022", -1, "relatório do PL 2234 na CCJ (legaliza cassinos e jogos)")],
        "autoria": {"PL 2372/2025": "proíbe toda publicidade de apostas", "PL 2662/2025": "crime de operar e divulgar apostas virtuais",
                    "PL 2985/2023": "proíbe publicidade de apostas", "PL 3718/2024": "limitação das apostas",
                    "PL 3754/2025": "idade mínima de 21 anos para apostar", "PL 3765/2026": "limite mensal de depósitos em apostas",
                    "PL 4219/2024": "limite mensal de depósitos em apostas", "PL 626/2023": "veda pagamentos para jogos de azar",
                    "PL 3587/2024": "operador de apostas com representação legal no país"},
    },
}


def sinais_pauta(pauta, a_favor, senadores, aut, com):
    """Sinais do senador na pauta (Senado), com o sentido para o cliente: a favor da pauta mantém, contra inverte."""
    from outros.novos_eleitos.e23_parlamentares_senado import painel, chave
    cfg, out = PAUTAS[pauta], {n: [] for n in senadores}
    lado = 1 if a_favor else -1
    nomes = {chave(n): n for n in senadores}
    for data, mat, (si, no), sentido, rot in cfg["votacoes"]:
        sig, num = mat.split()
        v = next(v for v in painel(data) if v["siglaTipoMateria"] == sig and v["numeroMateria"] == int(num)
                 and v["qtdVotosSim"] == si and v["qtdVotosNao"] == no)
        for p in v["votosParlamentar"]:
            n = nomes.get(chave(p["nomeParlamentar"]))
            voto = (p.get("voto") or "").upper()
            if n and voto in ("SIM", "NÃO"):
                s = sentido * lado * (1 if voto == "SIM" else -1)
                out[n].append(("forte", s, f"votou {voto.capitalize().replace('Nao', 'Não')} na {rot} (plenário do Senado, {data[:4]}; pauta {pauta.lower()})"))
    for data, colegiado, mat, sentido, rot in cfg["comissao"]:
        for r in com[(com.Data == data) & (com.Colegiado == colegiado) & (com["Matéria"] == mat)].itertuples():
            if r.Parlamentar in out and r.Voto in ("Sim", "Não"):
                s = sentido * lado * (1 if r.Voto == "Sim" else -1)
                out[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {rot} ({data[:4]}; pauta {pauta.lower()})"))
    for r in aut[aut.k.isin(cfg["autoria"]) & aut.Papel.isin(["Primeiro autor", "Coautor"])].drop_duplicates(["Parlamentar", "k"]).itertuples():
        if r.Parlamentar in out:
            forte = r.Papel == "Primeiro autor"
            out[r.Parlamentar].append(("forte" if forte else "médio", lado,
                f"{'autor' if forte else 'coautor'} do {r.k} ({cfg['autoria'][r.k]}; pauta {pauta.lower()})"))
    return out


def placar(texto):
    """Sim e Não do resultado oficial, nos formatos da Câmara (plenário e comissões)."""
    t = str(texto)
    for rx in (r"Sim:?\s*(\d+);\s*[Nn]ão:?\s*(\d+)", r"(\d+) votos \"Sim\", (\d+) votos \"Não\"", r"Sim,\s*(\d+);\s*Não,\s*(\d+)"):
        m = re.search(rx, t)
        if m:
            return int(m.group(1)), int(m.group(2))
    m = re.search(r"Sim:?\s*(\d+);\s*Total", t)
    return (int(m.group(1)), 0) if m else None


def dividiu(r, amostra):
    """Votação de consenso não entra como posição. Vale o placar oficial do texto (Câmara); no Senado, as colunas
    Sim e Não; sem nenhum dos dois, se houve Sim e Não entre os parlamentares da base."""
    p = placar(r.Votação)
    if p is None:
        try:
            p = int(float(r.Sim)), int(float(getattr(r, "Não")))
        except (TypeError, ValueError, AttributeError):
            p = None
    if p is None:
        return amostra.get((r.k, str(r.Data), str(r.Colegiado), str(r.Votação)), False)
    return min(p) > 0


def classifica(fp, fn, mp, mn):
    if fp and fn:
        d = fp - fn
        if abs(d) >= 2:
            return "Promotor" if d > 0 else "Detrator"
        if d == 1 and mp > mn:
            return "Promotor"
        if d == -1 and mn > mp:
            return "Detrator"
        return "Neutro"
    if fp >= 2 or (fp == 1 and mp >= 1):
        return "Promotor"
    if fn >= 2 or (fn == 1 and mn >= 1):
        return "Detrator"
    return "Não tem informação suficiente"


def main():
    base = c.ler_csv("temas_clientes/base_pautas.csv")
    comp = c.ler_csv("temas_clientes/posicoes_clientes_complemento.csv")
    base["Fonte da posição"] = "Coordenação"
    for r in comp.itertuples():
        m = (base.Cliente == r.Cliente) & (base.Projeto == r.Projeto)
        base.loc[m, "Para o cliente"] = r._5
        base.loc[m, "Fonte da posição"] = f"{r.Fonte}, {r.Situação.lower()}"
    senadores = pd.concat([c.ler_csv(f"temas_clientes/perfil_{g}.csv")[["Parlamentar", "Partido", "UF"]] for g in GRUPOS]).drop_duplicates()
    ficha = c.ler_csv("temas_clientes/parlamentares.csv").set_index("Senador")

    aut = pd.concat([c.ler_csv(f"perfil_senado_{g}/autoria.csv").assign(Casa="Senado") for g in GRUPOS]
                    + [c.ler_csv("perfil_camara_senado/autoria.csv").assign(Casa="Câmara")])
    rel = pd.concat([c.ler_csv(f"perfil_senado_{g}/relatorias.csv").assign(Casa="Senado") for g in GRUPOS]
                    + [c.ler_csv("perfil_camara_senado/relatorias.csv").assign(Casa="Câmara")])
    vot = pd.concat([c.ler_csv(f"perfil_senado_{g}/votos.csv").assign(Casa="Senado", Colegiado="plenário do Senado") for g in ("curso", "reeleitos")]
                    + [c.ler_csv("perfil_senado_comissoes/votos.csv").assign(Casa="Senado")]
                    + [c.ler_csv("perfil_camara_senado/votos.csv").assign(Casa="Câmara")])
    for d in (aut, rel, vot):
        d["k"] = d["Matéria"].astype(str).str.extract(r"^(\S+ \d+/\d{4})")[0]
    vot = vot[vot["Votação"].astype(str).str.contains(VOTO_PROJETO, regex=True)]
    # votação de consenso não entra como posição: sem nenhum Não (ou nenhum Sim) entre os parlamentares da base
    amostra = vot.groupby(["k", vot.Data.astype(str), vot.Colegiado.astype(str), vot["Votação"].astype(str)]).Voto.agg(
        lambda x: bool((x == "Sim").any() and (x == "Não").any())).to_dict()
    vot = vot[[dividiu(r, amostra) for r in vot.itertuples()]]

    linhas, resumo = [], []
    for (area, cliente), g in base.groupby(["Área", "Cliente"], sort=False):
        marcados = g[g["Para o cliente"].isin(["Promotor", "Detrator"])]
        sinais = {n: [] for n in senadores.Parlamentar}
        for p in marcados.itertuples():
            sentido = 1 if p._6 == "Promotor" else -1
            for k, casa in chaves(p.Projeto):
                ok = lambda d: (d.k == k) & ((d.Casa == casa) if casa else True)
                for r in aut[ok(aut) & aut.Papel.isin(["Primeiro autor", "Coautor"])].drop_duplicates(["Parlamentar", "Papel"]).itertuples():
                    if r.Parlamentar in sinais:
                        forte = r.Papel == "Primeiro autor"
                        sinais[r.Parlamentar].append(("forte" if forte else "médio", sentido,
                            f"{'autor' if forte else 'coautor'} do {k} ({p._6.lower()} para o cliente)"))
                for r in vot[ok(vot) & vot.Voto.isin(["Sim", "Não"])].itertuples():
                    if r.Parlamentar in sinais:
                        s = sentido if r.Voto == "Sim" else -sentido
                        onde = r.Colegiado if r.Colegiado != "PLEN" else "plenário da Câmara"
                        sinais[r.Parlamentar].append(("forte", s, f"votou {r.Voto} no {k} ({onde}, {str(r.Data)[:4]}; projeto {p._6.lower()} para o cliente)"))
                for r in rel[ok(rel)].drop_duplicates(["Parlamentar", "Colegiado"]).itertuples():
                    if r.Parlamentar in sinais:
                        sinais[r.Parlamentar].append(("evidência", 0, f"relator do {k} na {r.Colegiado} (sentido do parecer não lido)"))
        com_votos = c.ler_csv("perfil_senado_comissoes/votos.csv")
        for pt in g.drop_duplicates("Pauta").itertuples():
            if pt.Pauta in PAUTAS and pt._4 in ("A favor", "Contra"):
                for n, s_ in sinais_pauta(pt.Pauta, pt._4 == "A favor", list(sinais), aut, com_votos).items():
                    sinais[n] += s_
        com_registro = 0
        for n, s in sinais.items():
            if not s:
                continue
            com_registro += 1
            f = [x for k, x, _ in s if k == "forte"]
            m = [x for k, x, _ in s if k == "médio"]
            fp, fn, mp, mn = f.count(1), f.count(-1), m.count(1), m.count(-1)
            rot = (("forte a favor", "fortes a favor", fp), ("forte contra", "fortes contra", fn),
                   ("médio a favor", "médios a favor", mp), ("médio contra", "médios contra", mn))
            sinal_txt = "; ".join(f"{q} {um if q == 1 else varios}" for um, varios, q in rot if q) or "só relatoria"
            linhas.append({"Área": area, "Cliente": cliente, "Senador": n,
                           "Partido": ficha.loc[n, "Partido"], "UF": ficha.loc[n, "UF"], "Cadeira": ficha.loc[n, "Cadeira"],
                           "Classificação": classifica(fp, fn, mp, mn),
                           "Sinais": sinal_txt,
                           "Evidências": "\n".join(dict.fromkeys(t for _, _, t in s))})
        pautas = g.drop_duplicates("Pauta")
        resumo.append({"Área": area, "Cliente": cliente,
                       "Pautas": "; ".join(f"{r.Pauta} ({r._4.lower() or 'posição não informada'})" for r in pautas.itertuples()),
                       "Promotores": int((marcados["Para o cliente"] == "Promotor").sum()),
                       "Detratores": int((marcados["Para o cliente"] == "Detrator").sum()),
                       "Aguardando": int((~g["Para o cliente"].isin(["Promotor", "Detrator"])).sum()),
                       "Senadores com registro": com_registro,
                       "Posição fora da planilha": "; ".join(f"{r.Projeto}: {r._8}" for r in g.itertuples() if r._8 != "Coordenação")})
    c.salvar_csv(pd.DataFrame(linhas), "temas_clientes/clientes_classificacao.csv")
    c.salvar_csv(pd.DataFrame(resumo), "temas_clientes/clientes_resumo.csv")
    print(pd.DataFrame(linhas).groupby(["Cliente", "Classificação"]).size().unstack(fill_value=0).to_string())


if __name__ == "__main__":
    main()
