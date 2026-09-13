"""Etapa 5. Eleitos: resultado do TSE cruzado com o pré-mapeamento.

Fonte: os JSON públicos de resultado (resultados.tse.jus.br), os mesmos do painel de
apuração. Cada candidato vem com sqcand, que é o SQ_CANDIDATO do registro, então o
cruzamento é por número e não por nome.

Cuidados:
- Em deputado federal e estadual ninguém aparece eleito antes de a totalização fechar
  (tf = "s"). Sem --parcial a etapa para em vez de publicar lista incompleta.
- Deputado distrital é o cargo 0008 e só existe no DF; o DF não tem o cargo 0007.
- A conferência exige o número exato de cadeiras por UF.

Teste antes da urna: python -m outros.novos_eleitos.e5_eleitos --ciclo ele2022
    confere contagem de cadeiras e o casamento sqcand x consulta_cand de 2022, sem publicar.
Na apuração: python -m outros.novos_eleitos.e5_eleitos --ciclo ele2026 [--publicar] [--parcial]
Saída: eleitos_<ciclo>.csv e, em 2026, novos_eleitos.csv e abas "Novos eleitos Senado",
"Novos eleitos Câmara" e "Novos eleitos Assembleias".
"""
import argparse
import html
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from outros.novos_eleitos import comum as c

ELEICAO_CONHECIDA = {"ele2022": "546"}
CARGOS = [("0005", "Senado"), ("0006", "Câmara"), ("0007", "Assembleia"), ("0008", "Assembleia")]


def codigo_eleicao(ciclo):
    if ciclo in ELEICAO_CONHECIDA:
        return ELEICAO_CONHECIDA[ciclo]
    catalogo = c.buscar_json("https://resultados.tse.jus.br/oficial/comum/config/ele-c.json") or {}
    ano = ciclo.replace("ele", "")
    for pleito in catalogo.get("pl", []):
        if pleito.get("dt", "")[-4:] != ano:
            continue
        for eleicao in pleito.get("e", []):
            cargos = {str(cp.get("ds", "")).strip().lower() for abr in eleicao.get("abr", [])
                      for cp in abr.get("cp", []) if cp.get("tp") in ("1", "2")}
            # a eleição estadual é a que tem senador e deputado estadual juntos; a suplementar
            # de governador de RR em 2026 usa o mesmo código de cargo e não tem os dois
            if {"senador", "deputado federal", "deputado estadual"} <= cargos:
                return eleicao["cd"]
    raise SystemExit(f"O TSE ainda não publicou a eleição estadual de {ano} no catálogo de resultados.")


def arquivo(ciclo, eleicao, uf, cargo):
    uf = uf.lower()
    url = (f"https://resultados.tse.jus.br/oficial/{ciclo}/{eleicao}/dados-simplificados/"
           f"{uf}/{uf}-c{cargo}-e{eleicao.zfill(6)}-r.json")
    return uf.upper(), cargo, c.buscar_json(url, headers={"User-Agent": "Mozilla/5.0"})


def baixar(ciclo):
    eleicao = codigo_eleicao(ciclo)
    pedidos = [(uf, cargo) for uf in c.UFS for cargo, _ in CARGOS
               if not (cargo == "0007" and uf == "DF") and not (cargo == "0008" and uf != "DF")]
    with ThreadPoolExecutor(12) as ex:
        respostas = list(ex.map(lambda p: arquivo(ciclo, eleicao, *p), pedidos))
    casa_do_cargo = dict(CARGOS)
    linhas, parciais = [], []
    for uf, cargo, dados in respostas:
        c.falhar_se(dados is None, f"sem arquivo de resultado para {uf} cargo {cargo}")
        if dados.get("tf") != "s":
            parciais.append(f"{uf}/{cargo}")
        for cand in dados.get("cand", []):
            linhas.append({"casa": casa_do_cargo[cargo], "uf": uf, "sq_candidato": str(cand.get("sqcand")),
                           "nome_urna_resultado": html.unescape(str(cand.get("nm", ""))),
                           "partido_resultado": html.unescape(str(cand.get("cc", ""))).split(" - ")[0].strip(),
                           "votos": str(cand.get("vap", "")), "situacao_totalizacao": str(cand.get("st", ""))})
    return pd.DataFrame(linhas), parciais


def conferir_vagas(eleitos, ano):
    esperado = {"Senado": {uf: 2 if (ano - 2018) % 8 == 0 else 1 for uf in c.UFS},
                "Câmara": c.VAGAS_CAMARA,
                "Assembleia": {uf: c.vagas_assembleia(uf) for uf in c.UFS}}
    erros = []
    for casa, vagas in esperado.items():
        contagem = eleitos[eleitos.casa == casa].groupby("uf").size()
        for uf, n in vagas.items():
            if contagem.get(uf, 0) != n:
                erros.append(f"{casa}/{uf}: {contagem.get(uf, 0)} eleitos, {n} cadeiras")
        print(f"{casa}: {len(eleitos[eleitos.casa == casa])} eleitos")
    return erros


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--ciclo", required=True)
    args.add_argument("--publicar", action="store_true")
    args.add_argument("--parcial", action="store_true")
    a = args.parse_args()
    ano = int(a.ciclo.replace("ele", ""))

    todos, parciais = baixar(a.ciclo)
    if parciais:
        print(f"totalização aberta em {len(parciais)} arquivos: {parciais[:10]}")
        c.falhar_se(not a.parcial, "totalização não fechou; use --parcial para gerar mesmo assim")
    eleitos = todos[todos.situacao_totalizacao.str.startswith("Eleito")].copy()
    erros = conferir_vagas(eleitos, ano)
    c.falhar_se(bool(erros) and not a.parcial, f"cadeiras não fecham: {erros}")
    c.salvar_csv(eleitos, f"eleitos_{a.ciclo}.csv")

    if ano != 2026:
        base = c.consulta_cand(ano)
        casados = eleitos.sq_candidato.isin(set(base.SQ_CANDIDATO))
        print(f"teste {a.ciclo}: {casados.sum()} de {len(eleitos)} eleitos casam com o consulta_cand por SQ")
        c.falhar_se(not casados.all(), f"sem casamento: {eleitos[~casados].nome_urna_resultado.tolist()[:20]}")
        return

    pre = c.ler_csv("pre_mapeamento.csv")
    novos = eleitos.merge(pre, left_on="sq_candidato", right_on="SQ_CANDIDATO", how="left")
    sem = novos[novos.Nome.isna()]
    c.falhar_se(len(sem) > 0, f"eleitos fora do pré-mapeamento: {sem.nome_urna_resultado.tolist()}")
    novos = novos.rename(columns={"votos": "Votos", "situacao_totalizacao": "Situação na totalização"})
    print(novos.groupby(["casa", "Reeleição, volta ou novo"]).size().to_string())
    c.salvar_csv(novos, "novos_eleitos.csv")

    if a.publicar:
        destino = c.planilha_destino()
        colunas = [col for col in pre.columns if col not in ("titulo", "Situação do registro")]
        colunas.insert(colunas.index("Reeleição, volta ou novo") + 1, "Votos")
        colunas.insert(colunas.index("Votos") + 1, "Situação na totalização")
        ordem = {"Novo na Casa": 0, "Volta à Casa": 1, "Reeleição": 2}
        for casa, aba in (("Senado", "Novos eleitos Senado"), ("Câmara", "Novos eleitos Câmara"),
                          ("Assembleia", "Novos eleitos Assembleias")):
            parte = novos[novos.casa == casa][colunas]
            parte = parte.sort_values(["Reeleição, volta ou novo", "UF", "Nome"],
                                      key=lambda s: s.map(ordem) if s.name == "Reeleição, volta ou novo" else s)
            c.gravar_aba(destino, aba, parte)


if __name__ == "__main__":
    main()
