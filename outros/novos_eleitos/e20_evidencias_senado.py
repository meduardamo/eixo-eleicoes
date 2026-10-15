"""Etapa 20. Evidência de cada nível do levantamento do Senado.

O levantamento (etapa 18) diz o nível e o número de registros, por exemplo "Média (4)". Esta etapa
diz quais são os 4: para cada senador e cada área ou público com registro, as matérias de que é
primeiro autor e as que relatou (número, ementa e link, da mais recente para a mais antiga), e o
que a imprensa registrou dele naquela área.

Saída em dados_novos_eleitos/levantamento_senado/:
- evidencias.csv: uma linha por senador e área ou público com registro;
- notas.json: o texto curto que vai como nota em cada célula de nível da aba do levantamento.
Rodar: python -m outros.novos_eleitos.e20_evidencias_senado   (depois da etapa 18)
"""
import json
import re

import pandas as pd

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.e18_levantamento_senado import IMPRENSA, PUBLICOS, TEMAS

NA_NOTA = 6  # matérias por tipo de registro na nota da célula; a aba traz todas


def ano(materia):
    m = re.search(r"/(\d{4})", materia)
    return int(m.group(1)) if m else 0


def curta(texto, limite=170):
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto if len(texto) <= limite else texto[:limite].rsplit(" ", 1)[0] + "…"


def item(r, limite=None):
    casa = "" if r["Casa"] == "Senado" else f" [{r['Casa']}]"
    ementa = curta(r["Ementa"], limite) if limite else re.sub(r"\s+", " ", r["Ementa"]).strip()
    return f"{r['Matéria']}{casa}: {ementa}"


def main():
    lev = c.ler_csv("levantamento_senado/levantamento.csv")
    reg = c.ler_csv("levantamento_senado/registros.csv")
    reg["ano"] = reg["Matéria"].map(ano)
    reg["autor"] = reg["Registro"] == "Primeiro autor"
    noticias = c.ler_csv("noticias_senado/noticias.csv")
    noticias["coluna"] = noticias["Área"].map(lambda a: IMPRENSA.get(a, ""))
    noticias["coluna"] = noticias["coluna"].map(lambda t: f"Público: {t}" if t in PUBLICOS else t)

    linhas, notas = [], {}
    for l in lev.to_dict("records"):
        nome = l["Parlamentar"]
        meus = reg[reg.Parlamentar == nome]
        imprensa = noticias[noticias.Parlamentar == nome]
        for coluna in TEMAS + [f"Público: {p}" for p in PUBLICOS]:
            if coluna in TEMAS:
                aqui = meus[meus.Tema == coluna]
            else:
                p = coluna.removeprefix("Público: ")
                aqui = meus[meus["Públicos"].map(lambda v: p in v.split("; "))] if len(meus) else meus
            aqui = aqui.drop_duplicates(["Casa", "Registro", "Matéria"]).sort_values("ano", ascending=False)
            autor, relator = aqui[aqui.autor], aqui[~aqui.autor]
            diz = imprensa[imprensa.coluna == coluna]
            if aqui.empty and diz.empty:
                continue
            linhas.append({
                "Parlamentar": nome, "Partido": l["Partido"], "UF": l["UF"], "Situação": l["Situação"],
                "Área ou público": coluna, "Nível no levantamento": l[coluna],
                "Primeiro autor (matérias)": len(autor), "Relatorias (matérias)": len(relator),
                "Matérias de que é primeiro autor (mais recentes primeiro)": "\n".join(item(r) for r in autor.to_dict("records")),
                "Matérias que relatou (mais recentes primeiro)": "\n".join(item(r) for r in relator.to_dict("records")),
                "O que a imprensa registrou": "\n".join(f"{d['O que a matéria diz']} ({d['Veículo']}, {d['Data']})"
                                                         for d in diz.to_dict("records")),
                "Links das matérias legislativas": "\n".join(dict.fromkeys(aqui.Link)),
                "Links da imprensa": "\n".join(dict.fromkeys(diz.Link)),
            })
            partes = []
            for titulo, df in (("Primeiro autor", autor), ("Relator", relator)):
                if len(df):
                    mais = f"\n(e mais {len(df) - NA_NOTA})" if len(df) > NA_NOTA else ""
                    partes.append(f"{titulo} ({len(df)}):\n" + "\n".join(
                        "• " + item(r, 170) for r in df.head(NA_NOTA).to_dict("records")) + mais)
            if len(diz):
                partes.append("Imprensa:\n" + "\n".join("• " + curta(d["O que a matéria diz"], 220) for d in diz.to_dict("records")))
            notas[f"{nome}|{coluna}"] = "\n\n".join(partes) + "\n\nLista completa: aba Evidências por área."

    df = pd.DataFrame(linhas)
    c.salvar_csv(df, "levantamento_senado/evidencias.csv")
    (c.caminho("levantamento_senado") / "notas.json").write_text(json.dumps(notas, ensure_ascii=False))
    print(len(df), "linhas de senador e área;", len(notas), "notas")


if __name__ == "__main__":
    main()
