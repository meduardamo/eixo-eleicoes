"""Etapa 7. Fila de estreantes para pesquisa manual de biografia.

Estreante, aqui, é quem não tem mandato eletivo desde 2006 nem exercício na Câmara ou no
Senado desde 2003. As bases do TSE baixadas começam em 2006: quem teve mandato antes aparece
na fila (Germano Rigotto, governador do RS de 2003 a 2006) e a coluna "Teve mandato antes
de 2006?" é para a equipe marcar. Para esses não há histórico de projeto na base: a temática
sai da biografia (se foi secretário de alguma pasta, por exemplo) e do Instagram.

A aba tem colunas de preenchimento da equipe. A cada rodada o que foi digitado volta,
casado pelo SQ_CANDIDATO, então rodar de novo não apaga pesquisa feita.

Entrada: insumos_<universo>.csv (etapa 6) e candidaturas_2026.csv (ocupação declarada).
Saída: estreantes_<universo>.csv; com --publicar, aba "Estreantes, pesquisa manual
(pré-mapeados)" ou "(eleitos)" na planilha "Pré mapeamento".
Rodar: python -m outros.novos_eleitos.e7_estreantes --universo pre|eleitos [--publicar]
"""
import argparse

from outros.novos_eleitos import comum as c

MANUAIS = ["Teve mandato antes de 2006?", "Cargo não eletivo anterior (ex.: secretário de pasta)", "Pasta ou área", "Período",
           "Temática principal", "Fonte da informação", "Observações"]


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], required=True)
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    insumos = c.ler_csv(f"insumos_{a.universo}.csv")
    ocupacao = c.ler_csv("candidaturas_2026.csv").set_index("sq_candidato").ocupacao
    fila = insumos[insumos["Tipo de origem"] == "Sem mandato eletivo"].copy()
    fila = fila[["Casa disputada", "Nome", "Partido", "UF", "Instagram", "Outras redes declaradas",
                 "SQ_CANDIDATO"]]
    fila.insert(4, "Ocupação declarada ao TSE", fila.SQ_CANDIDATO.map(ocupacao).fillna(""))
    for col in MANUAIS:
        fila[col] = ""
    fila = fila[[col for col in fila.columns if col != "SQ_CANDIDATO"] + ["SQ_CANDIDATO"]]
    fila = fila.sort_values(["Casa disputada", "UF", "Nome"])
    print(fila.groupby("Casa disputada").size().to_string())
    print(f"com Instagram declarado: {(fila.Instagram != '').sum()} de {len(fila)}")
    c.salvar_csv(fila, f"estreantes_{a.universo}.csv")
    if not a.publicar:
        return
    rotulo = {"pre": "pré-mapeados", "eleitos": "eleitos"}[a.universo]
    c.gravar_aba(c.planilha_destino(), f"Estreantes, pesquisa manual ({rotulo})",
                 fila, preservar=MANUAIS, chave="SQ_CANDIDATO", congelar_colunas=2,
                 largura_minima={col: 220 for col in MANUAIS})


if __name__ == "__main__":
    main()
