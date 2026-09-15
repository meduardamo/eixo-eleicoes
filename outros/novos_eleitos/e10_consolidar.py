"""Etapa 10. Junta tudo numa aba só: uma linha por candidatura (ou por eleito), com os
insumos ao lado.

Base: pre_mapeamento.csv (--universo pre) ou novos_eleitos.csv (--universo eleitos). Ao lado,
pelo SQ_CANDIDATO e só quando o CSV existir: insumos por origem (etapa 6), IU e Marcela
(etapa 8) e organogramas das Seducs (etapa 9). Onde não se aplica, a célula fica vazia.
A fila de estreantes (etapa 7) fica em aba própria, porque é a única preenchida à mão.

Saída: mapeamento_<universo>.csv com as três Casas. Com --publicar, uma aba por Casa na
planilha "Pré mapeamento" ("Mapeamento Senado", "Mapeamento Câmara", "Mapeamento Assembleias",
ou "Eleitos ..." depois da urna), cada uma só com as colunas pertinentes, e as abas de antes
são apagadas. CPF e título não saem.
Rodar: python -m outros.novos_eleitos.e10_consolidar --universo pre [--publicar]
"""
import argparse

import gspread

from outros.novos_eleitos import comum as c

ABA = {"pre": "Mapeamento", "eleitos": "Eleitos"}
ABAS_POR_CASA = {"Senado": "Senado", "Câmara": "Câmara", "Assembleia": "Assembleias"}
# abas que existiram antes em 13/09/2026 e que esta etapa substitui (inclusive a versão
# com as três Casas juntas, que ela preferiu separar)
ABAS_ANTIGAS = ["Página1", "Mapeamento", "Eleitos", "Pré-mapeamento Senado", "Pré-mapeamento Câmara",
                "Pré-mapeamento Assembleias", "Novos eleitos Senado", "Novos eleitos Câmara",
                "Novos eleitos Assembleias"] + [
    f"{nome} ({rotulo})" for nome in ("Insumos por origem", "Assembleias, IU e Marcela", "Organogramas das Seducs")
    for rotulo in ("pré-mapeados", "eleitos")]
ORDEM_CASA = {"Senado": 0, "Câmara": 1, "Assembleia": 2}
ORDEM_CLASSE = {"Novo na Casa": 0, "Volta à Casa": 1, "Reeleição": 2}
NO_FIM = ["ID na Câmara", "Código no Senado", "SQ_CANDIDATO"]


def colunas_da_casa(parte, casa):
    """Só as colunas que fazem sentido para a Casa: competitividade só no Senado, e sai toda
    coluna vazia em todas as linhas da Casa (autoria no Senado continua nas assembleias só se
    alguém ali vier do Senado; IU e Marcela aparecem na Câmara e no Senado para quem vem de
    assembleia)."""
    fora = {"Casa disputada"}
    if casa != "Senado":
        fora.add("É competitivo? (Senado)")
    manter = [col for col in parte.columns
              if col not in fora and (col == "SQ_CANDIDATO" or (parte[col].astype(str) != "").any())]
    return parte[manter]


def juntar(df, arquivo, renomear=None):
    """Acrescenta as colunas novas do CSV pelo SQ_CANDIDATO. Coluna que a base já tem fica
    a da base."""
    if not c.caminho(arquivo).exists():
        print(f"aviso: {arquivo} não existe, as colunas dessa etapa ficam de fora")
        return df
    extra = c.ler_csv(arquivo).rename(columns=renomear or {})
    novas = [col for col in extra.columns if col not in df.columns]
    extra = extra[["SQ_CANDIDATO"] + novas].drop_duplicates("SQ_CANDIDATO")
    print(f"{arquivo}: {len(novas)} colunas, {len(extra)} linhas")
    return df.merge(extra, on="SQ_CANDIDATO", how="left")


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], required=True)
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()
    u = a.universo

    base = c.ler_csv("pre_mapeamento.csv" if u == "pre" else "novos_eleitos.csv")
    if u == "eleitos":
        base = base.drop(columns=[col for col in ("casa", "uf", "sq_candidato", "nome_urna_resultado",
                                                  "partido_resultado") if col in base.columns])
    base = base.drop(columns=[col for col in ("titulo",) if col in base.columns])
    total = len(base)

    df = juntar(base, f"insumos_{u}.csv")
    df = juntar(df, f"assembleias_iu_marcela_{u}.csv")
    df = juntar(df, f"organogramas_{u}.csv", renomear={
        "Como o nome casou (a conferir)": "Organograma da Seduc: como o nome casou (a conferir)",
        "Arquivo (Seduc, nov/2024)": "Organograma da Seduc: arquivo (nov/2024)",
        "Trecho do organograma": "Organograma da Seduc: trecho"})
    c.falhar_se(len(df) != total, f"a junção duplicou linhas: {len(df)} contra {total}")

    df = df[[col for col in df.columns if col not in NO_FIM] + [col for col in NO_FIM if col in df.columns]]
    df = df.fillna("").sort_values(
        ["Casa disputada", "Reeleição, volta ou novo", "UF", "Nome"],
        key=lambda s: s.map(ORDEM_CASA) if s.name == "Casa disputada"
        else s.map(ORDEM_CLASSE) if s.name == "Reeleição, volta ou novo" else s)
    print(df.groupby(["Casa disputada", "Reeleição, volta ou novo"], sort=False).size().to_string())
    print(f"{len(df)} linhas, {len(df.columns)} colunas")
    c.salvar_csv(df, f"mapeamento_{u}.csv")

    if a.publicar:
        destino = c.planilha_destino()
        for casa, nome_aba in ABAS_POR_CASA.items():
            parte = colunas_da_casa(df[df["Casa disputada"] == casa], casa)
            c.gravar_aba(destino, f"{ABA[u]} {nome_aba}", parte, congelar_colunas=1)  # Nome
        sh = c.cliente().open_by_key(destino)
        for aba in ABAS_ANTIGAS:
            try:
                sh.del_worksheet(sh.worksheet(aba))
                print(f"aba antiga apagada: {aba}")
            except gspread.WorksheetNotFound:
                pass


if __name__ == "__main__":
    main()
