"""Etapa 8. Assembleias: cruzamento com o levantamento do Instituto Unibanco e com a
planilha de comissões de educação da Marcela.

- IU: aba Elements de "[IU] Levantamento_Deputados_Estaduais" (abr/2023). 673 estaduais
  eleitos em 2022, com proximidade temática, relevância política, temas, subtemas da
  educação e cargos anteriores (ex-vereador, ex-secretário de educação...). A linha 2 é a
  instrução de uso do mapa, não dado.
- Marcela: aba "nomes relevantes p/ ISG" de "Composição CE - Assembleias".
Os dois só cobrem quem já é deputado estadual: servem para reeleição e volta à Casa.

Nenhuma das duas tem CPF. O casamento é nome dentro da UF. No IU a data de nascimento
confirma quando o nome não é idêntico; a planilha grava a data sem padrão de dia e mês,
então as duas leituras são aceitas.

Universo: --universo pre (candidaturas às Assembleias no pré-mapeamento) ou eleitos.
Saída: assembleias_iu_marcela_<universo>.csv; com --publicar, aba "Assembleias, IU e Marcela
(pré-mapeados)" ou "(eleitos)" na planilha "Pré mapeamento".
Rodar: python -m outros.novos_eleitos.e8_assembleias_iu_marcela --universo pre [--publicar]
"""
import argparse
from datetime import date

import pandas as pd

from outros.novos_eleitos import comum as c

COLUNAS_IU = {"Proximidade Tematica": "IU: proximidade temática (1 a 4)",
              "Relevância Política": "IU: relevância política (1 a 4)", "Temas": "IU: temas",
              "Subtemas da educação": "IU: subtemas da educação", "Cargos": "IU: cargos anteriores",
              "Risco Jurídico": "IU: risco jurídico"}
COLUNAS_MARCELA = {"CE": "Marcela: comissão de educação", "candidato a quê?": "Marcela: candidato a quê",
                   "PL relevante": "Marcela: PL relevante", "RELAÇÃO COM ESSE PL": "Marcela: relação com o PL",
                   "Favorável ou contrário ao ISG": "Marcela: favorável ou contrário ao ISG"}


def datas_possiveis(texto):
    partes = [p for p in str(texto).split("/") if p.isdigit()]
    if len(partes) != 3:
        return set()
    a, b, ano = int(partes[0]), int(partes[1]), int(partes[2])
    saida = set()
    for dia, mes in ((a, b), (b, a)):
        try:
            saida.add(date(ano, mes, dia).isoformat())
        except ValueError:
            pass
    return saida


def casar(linha, tabela, col_nome, col_uf, nascimento=None, col_nasc=None):
    da_uf = tabela[tabela[col_uf].str.strip().str.upper() == linha["UF"]]
    chaves = {c.chave_nome(linha["Nome"], True), c.chave_nome(linha["Nome de urna (TSE)"], True)}
    exato = da_uf[da_uf[col_nome].map(lambda x: c.chave_nome(x, True)).isin(chaves)]
    if len(exato) == 1:
        return exato.iloc[0]
    if nascimento and col_nasc:
        mesma_data = da_uf[da_uf[col_nasc].map(lambda x: nascimento in datas_possiveis(x))]
        parecido = mesma_data[mesma_data[col_nome].map(
            lambda x: max(c.semelhanca(c.chave_nome(x, True), k) for k in chaves) >= 0.5)]
        if len(parecido) == 1:
            return parecido.iloc[0]
    return None


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], required=True)
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    fonte = c.ler_csv("pre_mapeamento.csv" if a.universo == "pre" else "novos_eleitos.csv")
    base = fonte[fonte["Casa disputada"] == "Assembleia"].copy()
    nasc = c.ler_csv("candidaturas_2026.csv").set_index("sq_candidato").nascimento

    iu_bruto = c.ler_aba(c.id_planilha("SPREADSHEET_ID_IU_ESTADUAIS"), "Elements")
    iu = iu_bruto.iloc[1:].copy()  # linha 2 da planilha é a instrução de uso
    marcela = c.ler_aba(c.id_planilha("SPREADSHEET_ID_CE_ASSEMBLEIAS"), "nomes relevantes p/ ISG")
    marcela = marcela[marcela["nome"].str.strip() != ""]

    linhas, casou_iu, casou_marcela = [], 0, 0
    for _, r in base.iterrows():
        saida = {col: r[col] for col in ["Nome", "Partido", "UF", "Reeleição, volta ou novo", "Origem"]}
        achado = casar(r, iu, "Label", "UF", nasc.get(r["SQ_CANDIDATO"], ""), "Nascimento")
        for orig, dest in COLUNAS_IU.items():
            saida[dest] = "" if achado is None else str(achado.get(orig, ""))
        casou_iu += achado is not None
        achado = casar(r, marcela, "nome", "ue")
        for orig, dest in COLUNAS_MARCELA.items():
            saida[dest] = "" if achado is None else str(achado.get(orig, ""))
        casou_marcela += achado is not None
        saida["SQ_CANDIDATO"] = r["SQ_CANDIDATO"]
        linhas.append(saida)
    df = pd.DataFrame(linhas)
    com_dado = df[(df[list(COLUNAS_IU.values())] != "").any(axis=1) | (df[list(COLUNAS_MARCELA.values())] != "").any(axis=1)]
    print(f"{len(base)} nomes; IU casou {casou_iu} de {len(iu)}; Marcela casou {casou_marcela} de {len(marcela)}")
    c.salvar_csv(com_dado, f"assembleias_iu_marcela_{a.universo}.csv")
    if not a.publicar:
        return
    rotulo = {"pre": "pré-mapeados", "eleitos": "eleitos"}[a.universo]
    c.gravar_aba(c.planilha_destino(), f"Assembleias, IU e Marcela ({rotulo})",
                 com_dado.sort_values(["UF", "Nome"]))


if __name__ == "__main__":
    main()
