"""Etapa 7. Fila para pesquisa manual de biografia.

Antes da urna (--universo pre) a fila são os competitivos do Senado que não disputam
reeleição: para eles a equipe registra cargo não eletivo (secretário de pasta, ministro,
presidente de estatal) e mandato anterior a 2006, que as bases automáticas não trazem.
Depois da urna (--universo eleitos) a fila são os estreantes, quem não tem mandato eletivo
desde 2006 nem exercício na Câmara ou no Senado desde 2003.

As bases do TSE baixadas começam em 2006: quem teve mandato antes aparece como sem mandato
(Germano Rigotto, governador do RS de 2003 a 2006) e a coluna "Teve mandato antes de 2006?"
é para a equipe marcar. A temática sai da biografia (se foi secretário de alguma pasta, por
exemplo) e do Instagram.

A aba tem colunas de preenchimento da equipe. A cada rodada o que foi digitado volta,
casado pelo SQ_CANDIDATO, então rodar de novo não apaga pesquisa feita. O que estava na aba
antiga "Estreantes, pesquisa manual (pré-mapeados)" é levado para a aba nova antes de ela sair.

Entrada: mapeamento_pre.csv (etapa 10) para pre; insumos_eleitos.csv (etapa 6) e
candidaturas_2026.csv (ocupação declarada) para eleitos.
Saída: pesquisa_manual_<universo>.csv; com --publicar, a aba de ABA na planilha "Pré mapeamento".
Rodar: python -m outros.novos_eleitos.e7_estreantes --universo pre|eleitos [--publicar]
"""
import argparse

import gspread

from outros.novos_eleitos import comum as c

MANUAIS = ["Teve mandato antes de 2006?", "Cargo não eletivo anterior (ex.: secretário de pasta)", "Pasta ou área", "Período",
           "Temática principal", "Fonte da informação", "Observações"]
ABA = {"pre": "Senado competitivos, pesquisa manual (pré-mapeados)", "eleitos": "Estreantes, pesquisa manual (eleitos)"}
ABA_ANTIGA_PRE = "Estreantes, pesquisa manual (pré-mapeados)"
COLUNAS_PRE = ["Nome", "Partido", "UF", "Reeleição, volta ou novo", "Tipo de origem", "Origem",
               "Mandatos eletivos desde 2006 (TSE)", "Ocupação declarada ao TSE", "Instagram",
               "Outras redes declaradas", "SQ_CANDIDATO"]


def fila_pre():
    m = c.ler_csv("mapeamento_pre.csv")
    fila = m[(m["Casa disputada"] == "Senado") & (m["É competitivo? (Senado)"] == "Sim")
             & (m["Reeleição, volta ou novo"] != "Reeleição")]
    return fila[COLUNAS_PRE].sort_values(["UF", "Nome"])


def fila_eleitos():
    insumos = c.ler_csv("insumos_eleitos.csv")
    ocupacao = c.ler_csv("candidaturas_2026.csv").set_index("sq_candidato").ocupacao
    fila = insumos[insumos["Tipo de origem"] == "Sem mandato eletivo"].copy()
    fila = fila[["Casa disputada", "Nome", "Partido", "UF", "Instagram", "Outras redes declaradas",
                 "SQ_CANDIDATO"]]
    fila.insert(4, "Ocupação declarada ao TSE", fila.SQ_CANDIDATO.map(ocupacao).fillna(""))
    return fila.sort_values(["Casa disputada", "UF", "Nome"])


def trazer_da_aba_antiga(sh, fila):
    """Leva o que foi digitado na aba antiga para a fila, pelo SQ. Devolve a aba antiga ou None."""
    try:
        ws = sh.worksheet(ABA_ANTIGA_PRE)
    except gspread.WorksheetNotFound:
        return None
    valores = ws.get_all_values()
    if len(valores) > 1:
        cab = valores[0]
        for linha in valores[1:]:
            reg = dict(zip(cab, linha))
            alvo = fila.SQ_CANDIDATO == reg.get("SQ_CANDIDATO")
            for col in MANUAIS:
                if reg.get(col, "").strip():
                    c.falhar_se(not alvo.any(), f"{reg.get('Nome')} tem pesquisa na aba antiga e saiu da fila")
                    fila.loc[alvo, col] = reg[col]
    return ws


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], required=True)
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    # A aba dos competitivos do Senado é da etapa 11, com fonte por informação. Esta etapa
    # gravava por cima dela o texto antigo, sem fonte, e apagava as colunas novas.
    c.falhar_se(a.universo == "pre" and a.publicar,
                "a aba dos competitivos do Senado é gravada pela etapa 11 (e11_trajetoria_competitivos)")
    fila = fila_pre() if a.universo == "pre" else fila_eleitos()
    for col in MANUAIS:
        fila[col] = ""

    fila = fila[[col for col in fila.columns if col != "SQ_CANDIDATO"] + ["SQ_CANDIDATO"]]
    preenchidos = (fila["Cargo não eletivo anterior (ex.: secretário de pasta)"] != "").sum()
    print(f"{len(fila)} nomes; com Instagram declarado: {(fila.Instagram != '').sum()}; com cargo/bio pesquisados: {preenchidos}")
    c.salvar_csv(fila, f"pesquisa_manual_{a.universo}.csv")
    if not a.publicar:
        return
    destino = c.planilha_destino()
    sh = c.cliente().open_by_key(destino)
    antiga = trazer_da_aba_antiga(sh, fila) if a.universo == "pre" else None
    c.gravar_aba(destino, ABA[a.universo], fila, preservar=MANUAIS, chave="SQ_CANDIDATO", congelar_colunas=1,
                 largura_minima={col: 220 for col in MANUAIS})
    if antiga is not None:
        sh.del_worksheet(antiga)
        print(f"aba antiga apagada: {ABA_ANTIGA_PRE}")


if __name__ == "__main__":
    main()
