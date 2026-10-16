"""Etapa 23. Ficha dos 81 senadores (aba Parlamentares) e votações-chave em colunas (aba Votações-chave).

Parlamentares, uma linha por senador, em três blocos:
- Identidade: partido, UF, cadeira, de onde vem, profissão declarada ao TSE, trajetória (cargos com fonte);
- Força política: posição no Senado pelo governismo, relevância política de 1 a 4 e o cargo que a define,
  comissões de que é titular hoje e presidências vigentes;
- Proximidade de 1 a 4 em cada tema dos clientes, pelos números da etapa 22.

Regras (as mesmas da aba Metodologia):
- Governismo: votações nominais do plenário desde fev/2023 em que a liderança do Governo orientou Sim ou Não; conta
  o voto Sim/Não do senador igual à orientação. Base a partir de 85%, Oposição até 65%, Independente entre os dois.
  Menos de 20 votações (ministro no período, por exemplo) fica sem posição.
- Relevância: 4 = foi ou é governador, ministro, presidente do Senado ou da Câmara, presidente ou vice da República,
  ou é hoje líder do governo, da oposição, da maioria ou da minoria; 3 = prefeito, vice-governador, secretário de
  estado, presidente de Assembleia, Mesa do Senado, líder de partido ou bloco hoje ou presidente de comissão
  permanente hoje; 2 = outro mandato eletivo ou cargo anterior; 1 = primeiro mandato. Lideranças pela API do Senado
  (composicao/lideranca), vigentes na data da rodada.
- Proximidade no tema: pontos por comissão do tema (titular ou suplente 1, presidente 2), projetos como primeiro autor
  (5 ou mais 1, 15 ou mais 2), relatorias com parecer (2 ou mais 1) e lei aprovada (1); 0 ponto = 1, 1 a 2 = 2,
  3 a 4 = 3, 5 ou mais = 4.

Votações-chave: uma coluna por votação, com o voto de cada senador; a orientação do partido dele vai em nota.
Os eleitos em 2026 que eram deputados têm as votações-chave da Câmara em colunas próprias.

Saída: dados_novos_eleitos/temas_clientes/parlamentares.csv, votacoes_chave.csv e votacoes_chave_notas.json
Rodar: python -m outros.novos_eleitos.e23_parlamentares_senado   (depois da 22)
"""
import json
import re
import unicodedata
import urllib.request

import pandas as pd

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.e22_perfil_temas_senado import VOTACOES_CAMARA

GRUPOS = {"curso": "Mandato até 2031", "reeleitos": "Reeleito em 2026", "novos": "Eleito em 2026"}
TEMAS = ["Saúde", "Educação", "Esporte", "Primeira infância"]
PERMANENTES = {"CAE", "CAS", "CCJ", "CE", "CDH", "CRA", "CI", "CMA", "CDR", "CCT", "CRE", "CSP", "CTFC", "CEsp",
               "CCDD", "CCom", "CDIR"}
# (coluna, data, sigla e número no painel, placar Sim x Não, o que o Sim significa)
PLENARIO = [
    ("Lei Geral do Esporte (mai/2023)", "2023-05-09", "PL 1825", (43, 23), "Sim = manter o combate a racismo, homofobia, sexismo e xenofobia no texto"),
    ("Mais Médicos, Emenda 131 (jun/2023)", "2023-06-20", "MPV 1165", (29, 47), "Sim = barrar médico sem registro no CRM"),
    ("Cotas, Emenda 8 (out/2023)", "2023-10-24", "RQS 950", (24, 46), "Sim = trocar a cota racial por cota só de renda"),
    ("Bets, Emenda 139 (dez/2023)", "2023-12-12", "PL 3626", (37, 27), "Sim = tirar jogos on-line, como cassino, da lei"),
    ("Bets, Emenda 157 (dez/2023)", "2023-12-12", "PL 3626", (25, 36), "Sim = proibir publicidade de apostas em arenas e patrocínio a atletas e árbitros"),
    ("Celular na escola, Emenda 1 (dez/2024)", "2024-12-18", "PL 4932", (16, 42), "Sim = liberar o celular no ensino médio"),
    ("PLP 163/2025 (dez/2025)", "2025-12-03", "PLP 163", (47, 16), "Sim = tirar gasto temporário de saúde e educação do arcabouço fiscal"),
    ("Urgência dos cassinos, PL 2234 (dez/2025)", "2025-12-17", "RQS 857", (28, 36), "Sim = acelerar a legalização de cassinos e jogos"),
]
COMISSAO = [
    ("Vacinação nas escolas, CE (fev/2024)", "2024-02-20", "CE", "PL 826/2019", "PL 826/2019", "Sim = aprovar"),
    ("Exame de Proficiência em Medicina, CAS (dez/2025)", "2025-12-03", "CAS", "PL 2294/2024", "Substitutivo", "Sim = aprovar o substitutivo"),
]
VOTO = {"SIM": "Sim", "NÃO": "Não", "ABSTENÇÃO": "Abstenção", "P-NRV": "Presente, não votou", "AP": "Atividade parlamentar",
        "MIS": "Missão", "LS": "Licença saúde", "LP": "Licença particular", "NCOM": "Não compareceu",
        "PRESIDENTE (ART. 51 RISF)": "Presidia a sessão", "OBSTRUÇÃO": "Obstrução"}


APELIDOS = {"astrmarcospontes": "astronautamarcospontes", "profdorinhaseabra": "professoradorinhaseabra"}


def chave(s):
    k = re.sub(r"\W", "", unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower())
    return APELIDOS.get(k, k)


def painel(data):
    """Votações nominais do dia no painel do Senado (orientação de bancada e voto de cada senador)."""
    p = c.caminho(f"cache/orientacao_senado_{data}.json")
    if not p.exists():
        url = f"https://legis.senado.leg.br/dadosabertos/plenario/votacao/orientacaoBancada/{data.replace('-', '')}"
        p.write_bytes(urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=90).read())
    return json.loads(p.read_text()).get("votacoes") or []


def governismo(datas):
    tot, com = {}, {}
    for d in datas:
        for v in painel(d):
            g = {o["partido"]: o["voto"] for o in (v.get("orientacoesLideranca") or [])}.get("Governo")
            if g not in ("SIM", "NÃO"):
                continue
            for p in v["votosParlamentar"]:
                if (p.get("voto") or "").upper() in ("SIM", "NÃO"):
                    k = chave(p["nomeParlamentar"])
                    tot[k] = tot.get(k, 0) + 1
                    com[k] = com.get(k, 0) + ((p["voto"] or "").upper() == g)
    return tot, com


def liderancas_atuais():
    """Lideranças vigentes no Senado, na Câmara e no Congresso (dados abertos do Senado), por nome do parlamentar."""
    cache = c.DADOS / "cache" / "liderancas.json"
    try:
        req = urllib.request.Request("https://legis.senado.leg.br/dadosabertos/composicao/lideranca", headers={"Accept": "application/json"})
        dados = json.loads(urllib.request.urlopen(req, timeout=60).read())
        cache.write_text(json.dumps(dados, ensure_ascii=False))
    except Exception:
        dados = json.loads(cache.read_text())
    out = {}
    for d in dados:
        if d.get("dataTermino"):
            continue
        tipo, unidade = d.get("descricaoTipoLideranca", ""), d.get("descricaoTipoUnidadeLideranca", "")
        if not tipo.startswith("Líder"):
            continue
        casa = {"SF": "Senado", "CD": "Câmara", "CN": "Congresso"}.get(d.get("casa"), "")
        alvo = re.sub(r"^Liderança (d[oa] |de )", "", unidade)
        alvo = re.sub(r" n[oa] (Senado Federal|Câmara dos Deputados|Congresso Nacional)$", "", alvo)
        if alvo == "Partido" or alvo == "Bloco":
            alvo = f"{alvo.lower()} ({d.get('siglaPartidoFiliacao') or d.get('siglaPartido') or ''})"
        elif alvo.startswith("Bancada"):
            alvo = alvo.lower()
        else:
            alvo = alvo.lower()
        nivel = 4 if re.match(r"(?i)(governo|oposição|maioria|minoria)", alvo) else 3
        out.setdefault(d["nomeParlamentar"], []).append((nivel, f"Líder hoje: {alvo} no {casa}" if casa != "Câmara" else f"Líder hoje: {alvo} na Câmara"))
    return out


def limpa_cargo(x):
    x = re.sub(r"^[\s,;.]+", "", x)
    x = re.sub(r"(\d+\.ª,? (e )?)+", "", x)
    x = re.sub(r"^Vice-presidente do Brasil$", "Vice-presidente da República", x)
    x = re.sub(r" do Brasil\b", "", x)
    x = re.sub(r"\bde de\b", "de", x)
    return x[0].upper() + x[1:] if x else x


def relevancia(cargos, presidencias, liderancas):
    """4 = poder de agenda nacional (governador, ministro, presidente do Senado ou da Câmara, presidente ou vice da
    República, líder hoje do governo, da oposição, da maioria ou da minoria); 3 = prefeito, vice-governador, secretário
    de estado, presidente de Assembleia, Mesa do Senado, líder de partido ou bloco hoje, presidente de comissão
    permanente hoje; 2 = outro mandato eletivo ou cargo anterior; 1 = primeiro mandato eletivo."""
    cargos = [limpa_cargo(x) for x in cargos if str(x).strip()]
    regras = [(4, r"(?i)^Governador(a)? (do|da|de)\b|Ministr|(?<!vice-)presidente d[ao] (Senado|Câmara dos Deputados)|Líder do governo|Presidente da República|^Vice-presidente(?! d[oa] (Senado|Câmara|Clube|Conselho|Comissão))( d[oa] (Brasil|República))?$"),
              (3, r"(?i)Vice-governador|(?<!vice-)Prefeit|Secretári[oa] (de Estado|Estadual|.*d[oa] (Estado|Bahia|Ceará|Piauí|Pernambuco|Paraíba|Goiás|Maranhão))|Presidente da Assembleia|Vice-presidente d[oa] Senado|Secretári[oa] da Mesa")]
    for nivel, rx in regras:
        achou = [x for x in cargos if re.search(rx, x)]
        lid = [t for n, t in liderancas if n == nivel]
        if nivel == 3 and presidencias:
            lid.append("Preside hoje: " + ", ".join(presidencias))
        if achou or lid:
            return nivel, "; ".join(dict.fromkeys(lid + achou[:3]))
    anteriores = cargos[:3]
    return (2, "; ".join(dict.fromkeys(anteriores))) if anteriores else (1, "Primeiro mandato eletivo")


def profissao_recente(txt):
    """Só a ocupação declarada ao TSE na candidatura mais recente."""
    txt = str(txt)
    if not txt or "(" not in txt:
        return "Declarou só o cargo que exercia" if "cargo" in txt else txt
    itens = re.findall(r"([^;()]+?)\s*\(([\d, ]+)\)", txt)
    if not itens:
        return txt
    ocup, anos = max(itens, key=lambda x: max(int(a) for a in re.findall(r"\d{4}", x[1])))
    return f"{ocup.strip()} ({max(int(a) for a in re.findall(r'\d{4}', anos))})"


def proximidade(linha):
    """Nota de 1 a 4 e a conta que leva a ela (vai na nota da célula)."""
    com = str(linha["Comissão do tema hoje"])
    p_com = 2 if "Presidente" in com else (1 if com and com != "Não é membro" else 0)
    autor = int(linha["Primeiro autor"])
    p_aut = 2 if autor >= 15 else (1 if autor >= 5 else 0)
    rel = int(linha["Relatorias com parecer"])
    p_rel = int(rel >= 2)
    leis = int(linha["Viraram lei"])
    p_lei = int(leis >= 1)
    pts = p_com + p_aut + p_rel + p_lei
    nota = 1 if pts == 0 else 2 if pts <= 2 else 3 if pts <= 4 else 4
    conta = (f"{pts} ponto{'s' if pts != 1 else ''} (1 = 0 ponto; 2 = 1 a 2; 3 = 3 a 4; 4 = 5 ou mais)\n"
             f"Comissão do tema: {com if com else 'não é membro'} (+{p_com})\n"
             f"Projetos como primeiro autor no tema: {autor} (+{p_aut}; 5 ou mais = 1, 15 ou mais = 2)\n"
             f"Relatorias com parecer no tema: {rel} (+{p_rel}; 2 ou mais = 1)\n"
             f"Viraram lei: {leis} (+{p_lei})")
    return nota, conta


def main():
    perfis = pd.concat([c.ler_csv(f"temas_clientes/perfil_{g}.csv").assign(Cadeira=r) for g, r in GRUPOS.items()])
    lev = c.ler_csv("levantamento_senado/levantamento.csv").set_index("Parlamentar")
    cargos = c.ler_csv("perfil_senado_origem/cargos.csv")
    # o mandato de senador em curso não conta como experiência anterior, salvo para o reeleito (2019 a 2027);
    # cargo sem período na ficha é o de 2026 em diante: senador eleito sai, governador eleito fica com o rótulo
    cad = {}
    for g, r in GRUPOS.items():
        for n in c.ler_csv(f"temas_clientes/perfil_{g}.csv").Parlamentar:
            cad[n] = r
    cargos["Período"] = cargos["Período"].replace("", None)
    senado_agora = cargos.Cargo.str.contains("Senador", na=False) & (cargos["Período"].astype(str).str.contains("atual") | cargos["Período"].isna())
    cargos = cargos[~senado_agora | (cargos.Parlamentar.map(cad) == "Reeleito em 2026") & cargos["Período"].notna()].copy()
    futuro = cargos.Cargo.str.match(r"Governador(a)? ", na=False) & cargos["Período"].isna()
    cargos.loc[futuro, "Cargo"] = cargos.loc[futuro, "Cargo"] + " (eleito em 2026)"
    col = pd.concat([c.ler_csv(f"perfil_senado_{g}/colegiados.csv") for g in GRUPOS])
    col = col[col.Vigente == "Sim"]
    plen = pd.concat([c.ler_csv(f"perfil_senado_{g}/votos.csv") for g in ("curso", "reeleitos")])
    datas = sorted(set(plen[plen.Data >= "2023-02-01"].Data.str[:10]))
    tot, com_gov = governismo(datas)

    lideres = liderancas_atuais()
    notas_ficha = {}
    linhas = []
    for nome, g in perfis.groupby("Parlamentar", sort=False):
        l0 = g.iloc[0]
        meus_col = col[col.Parlamentar == nome]
        titular = sorted({r.Sigla for r in meus_col.itertuples() if r.Sigla in PERMANENTES and r.Papel in ("Titular", "Presidente", "Vice-presidente")})
        pres = sorted({r.Sigla for r in meus_col.itertuples() if r.Sigla in PERMANENTES and r.Papel == "Presidente"})
        nivel, porque = relevancia(cargos[cargos.Parlamentar == nome].Cargo.astype(str).tolist(), pres, lideres.get(nome, []))
        k = chave(nome)
        k = k if k in tot else next((x for x in tot if k in x or x in k), k)
        n = tot.get(k, 0)
        if l0["Cadeira"] == "Eleito em 2026":
            posicao, gov = "Sem votos no Senado", ""
        elif n < 20:
            posicao, gov = "Poucas votações no período", f"{n} votações"
        else:
            pct = round(100 * com_gov[k] / n)
            posicao = "Base" if pct >= 85 else "Oposição" if pct <= 65 else "Independente"
            gov = f"{pct}% ({n} votações)"
        prox = {t: proximidade(g[g.Tema == t].iloc[0]) for t in TEMAS}
        for t in TEMAS:
            notas_ficha[f"{nome}|{t}"] = prox[t][1]
        info = lev.loc[nome] if nome in lev.index else {}
        linhas.append({
            "Senador": nome, "Partido": l0["Partido"], "UF": l0["UF"], "Cadeira": l0["Cadeira"],
            "De onde vem": info.get("De onde vem", ""), "Profissão declarada (TSE)": profissao_recente(info.get("Profissão declarada (TSE)", "")),
            "Posição no Senado": posicao, "Governismo": gov, "Relevância política (1 a 4)": nivel, "Por quê": porque,
            "Comissões permanentes (titular)": ", ".join(titular), "Preside hoje": ", ".join(pres),
            **{f"Proximidade: {t} (1 a 4)": prox[t][0] for t in TEMAS},
        })
    ficha = pd.DataFrame(linhas).sort_values("Senador", key=lambda s: s.map(chave))
    c.salvar_csv(ficha, "temas_clientes/parlamentares.csv")
    (c.DADOS / "temas_clientes" / "parlamentares_notas.json").write_text(json.dumps(notas_ficha, ensure_ascii=False, indent=1))

    # votações-chave
    com = c.ler_csv("perfil_senado_comissoes/votos.csv")
    vcam = c.ler_csv("perfil_camara_senado/votos.csv")
    vcam = vcam[(vcam.Colegiado == "PLEN") & (vcam.Data >= "2023-02-01")]
    tab, notas = {n: {} for n in ficha.Senador}, {}
    nomes = {chave(n): n for n in ficha.Senador}
    for rot, data, mat, (si, no), sim in PLENARIO:
        notas[rot] = f"{sim}. Placar: {si} x {no}."
        sig, num = mat.split()
        v = next((v for v in painel(data) if v["siglaTipoMateria"] == sig and v["numeroMateria"] == int(num)
                  and v["qtdVotosSim"] == si and v["qtdVotosNao"] == no), None)
        if v is None:
            raise SystemExit(f"votação não achada no painel: {rot}")
        ori = {o["partido"]: o["voto"] for o in (v.get("orientacoesLideranca") or [])}
        for p in v["votosParlamentar"]:
            k = chave(p["nomeParlamentar"])
            n = nomes.get(k) or next((nomes[x] for x in nomes if k in x or x in k), None)
            if n:
                partido = {"Republica": "Republicanos"}.get(p["partido"], p["partido"])
                o = ori.get(p["partido"])
                tab[n][rot] = (VOTO.get((p.get("voto") or "").upper(), p.get("voto")),
                               f"Orientação do {partido}: {o.lower()}" if o else f"{partido} sem orientação registrada")
    for rot, data, colegiado, mat, trecho, sim in COMISSAO:
        notas[rot] = sim + "."
        v = com[(com.Data == data) & (com.Colegiado == colegiado) & (com["Matéria"] == mat) & com["Votação"].str.contains(trecho, regex=False)]
        for r in v.itertuples():
            if r.Parlamentar in tab:
                tab[r.Parlamentar][rot] = (r.Voto, "")
    for t, data, mat, trecho, rotulo in VOTACOES_CAMARA:
        rot = "Câmara: " + rotulo.split(" (Sim")[0].replace(" na Câmara", "")
        notas[rot] = rotulo[rotulo.find("(") + 1:-1] + "."
        v = vcam[(vcam.Data.str[:10] == data) & (vcam["Matéria"] == mat) & vcam["Votação"].str.contains(trecho, regex=False)]
        for r in v.itertuples():
            if r.Parlamentar in tab:
                tab[r.Parlamentar][rot] = ({"Artigo 17": "Presidia a sessão"}.get(r.Voto, r.Voto), "")
    colunas = list(dict.fromkeys([p[0] for p in PLENARIO] + [x[0] for x in COMISSAO] + list(notas)))
    cad = dict(zip(ficha.Senador, ficha.Cadeira))
    saida, nota_cel = [], {}
    for n in ficha.Senador:
        linha = {"Senador": n, "Partido": ficha.set_index("Senador").loc[n, "Partido"], "UF": ficha.set_index("Senador").loc[n, "UF"], "Cadeira": cad[n]}
        for rot in colunas:
            voto, nota = tab[n].get(rot, (None, ""))
            if voto is None:
                camara = rot.startswith("Câmara:")
                senado_comissao = rot in [x[0] for x in COMISSAO]
                voto = ("Não era deputado" if camara and cad[n] != "Eleito em 2026" else
                        "Sem voto registrado" if camara else
                        "Não era senador" if cad[n] == "Eleito em 2026" else
                        "Não era membro ou não votou" if senado_comissao else "Não votou")
            linha[rot] = voto
            if nota:
                nota_cel[f"{n}|{rot}"] = nota
        saida.append(linha)
    c.salvar_csv(pd.DataFrame(saida), "temas_clientes/votacoes_chave.csv")
    c.caminho("temas_clientes/votacoes_chave_notas.json").write_text(
        json.dumps({"cabecalho": notas, "celulas": nota_cel}, ensure_ascii=False, indent=1))
    print(ficha["Posição no Senado"].value_counts().to_string())
    print(ficha["Relevância política (1 a 4)"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
