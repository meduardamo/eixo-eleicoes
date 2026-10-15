"""Etapa 22. Perfil de cada senador nos quatro grandes temas dos clientes, uma linha por senador e tema.

Blocos de cada linha:
- O que fez: síntese escrita a partir da leitura das matérias da etapa 21 (temas_clientes/o_que_fez_<grupo>_*.json),
  com número da matéria e da lei; designação de relatoria que terminou sem parecer do senador não entra;
- O que defende: o que disse no plenário desde 2023, com data, pela leitura dos resumos de discurso do Senado
  (temas_clientes/o_que_defende_<grupo>_*.json). Resumo idêntico para vários senadores na mesma sessão só entra
  depois de conferido no texto da nota taquigráfica: o Senado repete o resumo de uma fala em quem falou depois;
- Números: matérias de primeira autoria, relatorias com parecer, relatorias em curso e quantas viraram lei;
- Como votou: voto nas votações-chave do tema validadas em 15/10/2026 (VOTACOES), no plenário e nas comissões;
- Força no tema: lugar hoje na comissão do tema e frentes do tema no Senado;
- Observação: o que muda a cadeira (2º turno, eleição para governador), da aba Senado eleito 2026.

Saída: dados_novos_eleitos/temas_clientes/perfil_<grupo>.csv
Rodar: python -m outros.novos_eleitos.e22_perfil_temas_senado --grupo curso   (depois da 21 e da leitura)
"""
import argparse
import json
import re

import pandas as pd

from outros.novos_eleitos import comum as c

TEMAS = ["Saúde", "Educação", "Esporte", "Primeira infância"]
GRUPOS = {"curso": ["Mandato em curso"], "reeleitos": ["Reeleito", "Reeleita"], "novos": ["Novo", "Nova"]}
COMISSOES = {"Saúde": ["CAS"], "Educação": ["CE"], "Esporte": ["CEsp"], "Primeira infância": ["CAS", "CE", "CDH"]}
FRENTES = {"Saúde": r"sa[úu]de|m[ée]dic|enferm|c[âa]ncer|doen|hospital|farmac|vacina", "Educação": r"educa|ensino|escola|professor",
           "Esporte": r"esport|futebol|desport|ol[íi]mp", "Primeira infância": r"inf[âa]ncia|crian"}
# (tema, onde, data, matéria, trecho da descrição da votação, rótulo com o que o Sim significa e o resultado)
VOTACOES = [
    ("Saúde", "plenário", "2023-06-20", "MPV 1165/2023", "Emenda nº 131",
     "Mais Médicos, Emenda 131 (Sim = barrar médico sem registro no CRM; rejeitada, 29 x 47)"),
    ("Saúde", "plenário", "2025-12-03", "PLP 163/2025", "Emenda nº 1",
     "PLP 163/2025 (Sim = tirar gasto temporário de saúde e educação do arcabouço fiscal e do cálculo dos pisos; aprovado, 47 x 16)"),
    ("Saúde", "CAS", "2025-12-03", "PL 2294/2024", "Substitutivo",
     "Exame Nacional de Proficiência em Medicina, na CAS em 03/12/2025 (Sim = aprovar o substitutivo)"),
    ("Saúde", "CE", "2024-02-20", "PL 826/2019", "PL 826/2019",
     "Vacinação nas escolas públicas, na CE em 20/02/2024 (Sim = aprovar)"),
    ("Educação", "plenário", "2023-10-24", "PL 5384/2020", "Requerimento nº 950",
     "Cotas, preferência para a Emenda 8 (Sim = trocar a cota racial por cota só de renda; rejeitada, 24 x 46)"),
    ("Educação", "plenário", "2024-12-18", "PL 4932/2024", "Emenda nº 1",
     "Celular na escola, Emenda 1 (Sim = liberar o celular no ensino médio; rejeitada, 16 x 42)"),
    ("Educação", "plenário", "2025-12-03", "PLP 163/2025", "Emenda nº 1",
     "PLP 163/2025 (Sim = tirar gasto temporário de saúde e educação do arcabouço fiscal e do cálculo dos pisos; aprovado, 47 x 16)"),
    ("Educação", "CE", "2024-02-20", "PL 826/2019", "PL 826/2019",
     "Vacinação nas escolas públicas, na CE em 20/02/2024 (Sim = aprovar)"),
    ("Esporte", "plenário", "2023-05-09", "PL 1825/2022 (Substitutivo-CD)", "expressões",
     "Lei Geral do Esporte (Sim = manter o combate a racismo, homofobia, sexismo e xenofobia no texto; aprovado, 43 x 23)"),
    ("Esporte", "plenário", "2023-12-12", "PL 3626/2023", "Emenda nº 157",
     "Lei das bets, Emenda 157 (Sim = proibir publicidade de apostas em arenas e patrocínio a atletas e árbitros; rejeitada, 25 x 36)"),
    ("Primeira infância", "CE", "2024-02-20", "PL 826/2019", "PL 826/2019",
     "Vacinação nas escolas públicas, na CE em 20/02/2024 (Sim = aprovar)"),
]
AUSENCIA = {"P-NRV": "presente, não votou", "AP": "em atividade parlamentar", "MIS": "em missão", "LS": "licença saúde",
            "LP": "licença particular", "NCom": "não compareceu", "Presidente (art. 51 RISF)": "presidia a sessão"}


def sinteses(grupo, bloco="o_que_fez"):
    d = {}
    for p in sorted(c.caminho("temas_clientes").glob(f"{bloco}_{grupo}_*.json")):
        d.update(json.loads(p.read_text()))
    return d


def votos_do(nome, plen, com, tema):
    linhas = []
    for t, onde, data, mat, trecho, rotulo in VOTACOES:
        if t != tema:
            continue
        if onde == "plenário":
            v = plen[(plen.Parlamentar == nome) & (plen.Data.str[:10] == data) & (plen["Matéria"] == mat)
                     & plen["Votação"].str.contains(trecho, regex=False)]
        else:
            v = com[(com.Parlamentar == nome) & (com.Data == data) & (com.Colegiado == onde) & (com["Matéria"] == mat)
                    & com["Votação"].str.contains(trecho, regex=False)]
        voto = v.Voto.iloc[0] if len(v) else ("não era senador" if data < "2023-02-01" else
                                               "não votou" if onde == "plenário" else "não era membro ou não votou")
        linhas.append(f"{rotulo}: {AUSENCIA.get(voto, voto)}")
    return "\n".join(linhas) if linhas else "Sem votação nominal que separe os senadores neste tema."


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grupo", choices=GRUPOS, required=True)
    grupo = ap.parse_args().grupo
    lev = c.ler_csv("levantamento_senado/levantamento.csv")
    lev = lev[lev["Situação"].isin(GRUPOS[grupo])]
    mats = c.ler_csv(f"temas_clientes/materias_{grupo}.csv")
    mats = mats[mats.Papel != "Designado, saiu antes do parecer"]
    texto = sinteses(grupo)
    fala = sinteses(grupo, "o_que_defende")
    c.falhar_se(set(lev.Parlamentar) - set(texto), f"sem síntese: {set(lev.Parlamentar) - set(texto)}")
    plen = pd.concat([c.ler_csv(f"perfil_senado_{g}/votos.csv") for g in GRUPOS])
    com = c.ler_csv("perfil_senado_comissoes/votos.csv")
    col = pd.concat([c.ler_csv(f"perfil_senado_{g}/colegiados.csv") for g in GRUPOS])
    col = col[col.Vigente == "Sim"]
    obs = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), "Senado eleito 2026"))
    # só o que muda a cadeira; as notas internas de conferência da planilha ficam de fora
    obs = {k: v for k, v in obs.set_index("Parlamentar")["Observação"].items() if v.startswith(("Disputa o 2º", "Eleito"))}

    # novos: a fala vem da Câmara (plenário desde 2023) ou, sem mandato federal, da imprensa da eleição;
    # nenhum era senador nas votações-chave, e o voto na Câmara entra na rodada da Câmara
    col_fala = "O que defende (plenário, desde 2023)" if grupo != "novos" else "O que defende (plenário da Câmara desde 2023 ou imprensa)"
    sem_fala = ("Nenhum discurso sobre o tema em plenário desde 2023." if grupo != "novos"
                else "Nenhum discurso sobre o tema no plenário da Câmara desde 2023.")
    linhas = []
    for l in lev.sort_values(["UF", "Parlamentar"]).to_dict("records"):
        nome = l["Parlamentar"]
        meus_col = col[col.Parlamentar == nome]
        for tema in TEMAS:
            m = mats[(mats.Parlamentar == nome) & (mats["Tema do cliente"] == tema)].drop_duplicates(["Casa", "Papel", "Matéria"])
            leis = m[m["Virou lei"] == "Sim"]
            comissao = [f"{r.Sigla} ({r.Papel})" for r in meus_col.itertuples()
                        if r.Sigla in COMISSOES[tema] and r.Tipo != "Frente ou grupo"]
            frentes = [r.Colegiado for r in meus_col.itertuples()
                       if r.Tipo == "Frente ou grupo" and re.search(FRENTES[tema], r.Colegiado, re.I)]
            linhas.append({
                "Parlamentar": nome, "Partido": l["Partido"], "UF": l["UF"], "Tema": tema,
                "O que fez": texto[nome][tema],
                col_fala: fala.get(nome, {}).get(tema, sem_fala),
                "Primeiro autor": int((m.Papel == "Primeiro autor").sum()),
                "Relatorias com parecer": int((m.Papel == "Relatou (parecer)").sum()),
                "Relatorias em curso": int((m.Papel == "Relator atual").sum()),
                "Viraram lei": len(leis),
                "Leis": "\n".join(f"{r['Matéria']}: {r['Situação da matéria']}" for r in leis.to_dict("records")),
                "Como votou (votações que separam os senadores)": (votos_do(nome, plen, com, tema) if grupo != "novos" else
                    "Não era senador nas votações-chave. Os votos na Câmara entram na rodada da Câmara."),
                "Comissão do tema hoje": "; ".join(dict.fromkeys(comissao)) or "Não é membro",
                "Frentes do tema no Senado": "; ".join(dict.fromkeys(frentes)),
                "Observação sobre a cadeira": obs.get(nome, ""),
                "Fontes (matérias)": "\n".join(dict.fromkeys(m.Link)),
            })
    df = pd.DataFrame(linhas)
    c.salvar_csv(df, f"temas_clientes/perfil_{grupo}.csv")
    print(df.groupby("Tema")[["Primeiro autor", "Relatorias com parecer", "Viraram lei"]].sum().to_string())


if __name__ == "__main__":
    main()
