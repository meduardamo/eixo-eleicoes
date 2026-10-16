"""Etapa 26. "Cabeças do Congresso" do DIAP para os 81 senadores: lista de 2026 (com o perfil principal que o DIAP
dá a cada um: debatedor, articulador/organizador, formulador, formador de opinião ou negociador) e de 2025.

Fontes (PDFs do DIAP):
- 2026: https://www.diap.org.br/images/stories/novo_doc_compressed.pdf (divulgado em 17/06/2026). O perfil vem da
  posição do X na tabela, lida pelas coordenadas das palavras; a contagem dos senadores por perfil confere com a
  tabela-resumo do próprio DIAP (3 debatedores, 13 articuladores, 10 formuladores, 1 formador de opinião, 4 negociadores).
- 2025: https://diap.org.br/images/stories/cabecas_2025_resumo_imprensa.pdf (só a lista, sem perfil).
Entram também os ex-deputados que estavam na lista quando eram deputados. Quem estava licenciado no período de
avaliação (ministro) não é avaliado pelo DIAP naquele ano.

Saída: temas_clientes/diap_cabecas.csv
Rodar: python -m outros.novos_eleitos.e26_diap_cabecas
"""
import collections
import re
import unicodedata
import urllib.request

import pymupdf

from outros.novos_eleitos import comum as c

URL_2026 = "https://www.diap.org.br/images/stories/novo_doc_compressed.pdf"
URL_2025 = "https://diap.org.br/images/stories/cabecas_2025_resumo_imprensa.pdf"
PERFIS = ["Debatedor", "Articulador", "Formulador", "Formador de opinião", "Negociador"]
GRUPOS = ["curso", "reeleitos", "novos"]


def norm(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", "", s.replace("dr. ", "")).strip()


def baixa(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return pymupdf.open(stream=urllib.request.urlopen(req, timeout=120).read(), filetype="pdf")


def lista_2026():
    """(nome, cargo, lista, perfil, novo na lista) das páginas da lista dos 100 e dos 50 em ascensão."""
    doc, saida, colunas, lista = baixa(URL_2026), [], None, None
    for pag in doc:
        palavras = pag.get_text("words")
        texto = pag.get_text()
        if "100 “CABEÇAS”" in texto and "ORDEM ALFABÉTICA" in texto:
            lista = "Cabeça"
        if "EM “ASCENSÃO” 2026 - ORDEM ALFABÉTICA" in texto:
            lista = "Em ascensão"
        elif lista == "Em ascensão" and "QUEM ENTROU" in texto.upper():
            lista = None
        if lista is None:
            continue
        cab = {w[4]: (w[0] + w[2]) / 2 for w in palavras if w[4] in ("Debatedor", "Articulador/", "Formulador", "Formador", "Negociador")}
        if len(cab) == 5:
            colunas = [cab["Debatedor"], cab["Articulador/"], cab["Formulador"], cab["Formador"], cab["Negociador"]]
        linhas = collections.defaultdict(list)
        for w in palavras:
            linhas[round(w[1] / 2)].append(w)
        for k in sorted(linhas):
            ws = sorted(linhas[k], key=lambda w: w[0])
            t = " ".join(w[4] for w in ws)
            m = re.match(r"(?i)(senador|senadora|deputado|deputada)\s+(.+)", t)
            if not m or m.group(2).upper().startswith("FEDERAIS") or not m.group(2)[:1].isupper():  # cabeçalho e texto corrido
                continue
            nome = re.split(r"\s+[–-]\s+", m.group(2))[0]
            xs = [(w[0] + w[2]) / 2 for w in ws if w[4] == "X"]
            if lista == "Em ascensão" and xs:
                continue  # a tabela de ascensão não tem X; linha com X é das tabelas por região ou de mulheres que vêm depois
            perfil = PERFIS[min(range(5), key=lambda i: abs(colunas[i] - xs[0]))] if xs and colunas and lista == "Cabeça" else ""
            saida.append((nome, m.group(1).capitalize(), lista, perfil, m.group(1).isupper()))
    return saida


def lista_2025():
    texto = "".join(p.get_text() for p in baixa(URL_2025))
    ini, asc = texto.find("100 “CABEÇAS” DO CONGRESSO NACIONAL 2025"), texto.find("EM “ASCENSÃO” 2025 - ORDEM ALFABÉTICA")
    fim = texto.find("QUEM ENTROU", asc)
    saida = []
    for trecho, lista in ((texto[ini:asc], "Cabeça"), (texto[asc:fim], "Em ascensão")):
        for m in re.finditer(r"(?im)^\s*\d+\.\s+(senador|senadora|deputado|deputada)\s+(.+?)\s*\(", trecho):
            saida.append((m.group(2).strip(), m.group(1).capitalize(), lista))
    return saida


def casa(nome, nomes):
    k = norm(nome)
    for kk, v in nomes.items():
        if kk == k or set(k.split()) <= set(kk.split()) or set(kk.split()) <= set(k.split()):
            return v
    return None


def main():
    import pandas as pd
    nomes = {norm(n): n for g in GRUPOS for n in c.ler_csv(f"temas_clientes/perfil_{g}.csv").Parlamentar}
    a26 = lista_2026()
    print("2026:", collections.Counter((cargo[:5], lista, perfil) for _, cargo, lista, perfil, _ in a26))
    a25 = lista_2025()
    print("2025:", collections.Counter((cargo[:5], lista) for _, cargo, lista in a25))
    linhas = {}
    for nome, cargo, lista, perfil, novo in a26:
        n = casa(nome, nomes)
        if n and "2026" not in linhas.get(n, {}):
            linhas.setdefault(n, {})["2026"] = (f"{lista}: {perfil.lower()}" if perfil else lista) + (" (entrou em 2026)" if novo and lista == "Cabeça" else "")
            linhas[n]["Cargo na lista 2026"] = cargo
    for nome, cargo, lista in a25:
        n = casa(nome, nomes)
        if n and "2025" not in linhas.get(n, {}):
            linhas.setdefault(n, {})["2025"] = lista
            linhas[n]["Cargo na lista 2025"] = cargo
    df = pd.DataFrame([{"Senador": n, **v} for n, v in linhas.items()]).fillna("")
    c.salvar_csv(df, "temas_clientes/diap_cabecas.csv")
    print(df.to_string())


if __name__ == "__main__":
    main()
