"""Etapa 1. Candidaturas de 2026 ao Senado, à Câmara e às Assembleias que seguem na disputa.

Base: consulta_cand de 2026 (dados abertos, traz CPF e título de eleitor) mais a situação
do registro no DivulgaCand, espelhada na aba candidaturas_divulgacand da planilha do TSE.
O CSV do TSE vem com #NE na situação e não apaga registro renunciado: Carlos Jordy (PL/RJ)
aparece como deputado federal e como senador, e só o DivulgaCand diz que o federal é Renúncia.

Saída: candidaturas_2026.csv
Rodar: python -m outros.novos_eleitos.e1_candidaturas [--atualizar]
    --atualizar baixa o consulta_cand de novo em vez de usar o que está em cache.
"""
import sys

import pandas as pd

from outros.novos_eleitos import comum as c

ORDEM_SITUACAO = {"Deferido": 0, "Deferido com recurso": 1, "Aguardando julgamento": 2,
                  "Pendente de julgamento": 2, "Indeferido em prazo recursal ou com recurso": 3,
                  "Indeferido": 4}


def main():
    base = c.consulta_cand(2026, atualizar="--atualizar" in sys.argv)
    base = base[base.DS_CARGO.isin(c.CASA_DO_CARGO) & (base.NR_TURNO == "1")].copy()

    divulgacand = c.ler_aba(c.id_planilha("SPREADSHEET_ID_TSE"), "candidaturas_divulgacand")
    situacao = divulgacand.drop_duplicates("sq_candidato").set_index("sq_candidato")["situacao"]
    base["situacao"] = base.SQ_CANDIDATO.map(situacao).fillna("")

    # Registro que está no CSV e não existe no DivulgaCand é registro velho que o TSE não
    # apagou. Em 13/09/2026: Gustavo Galassi aparecia como SENADOR/MG com "-4" em título e
    # CPF, e no DivulgaCand ele é 1º suplente de senador, com outro SQ.
    fantasma = base[base.situacao == ""]
    if len(fantasma):
        print(f"{len(fantasma)} registros do CSV que não existem no DivulgaCand (saem):",
              fantasma[["NM_URNA_CANDIDATO", "DS_CARGO", "SG_UF", "SQ_CANDIDATO"]].values.tolist())
    base = base[base.situacao != ""]

    fora = base.situacao.isin(c.FORA_DA_DISPUTA)
    print("fora da disputa:", base[fora].situacao.value_counts().to_dict())
    # a etapa 4 usa para explicar quem as abas do Radar ainda mostram como candidato
    c.salvar_csv(pd.DataFrame({"titulo": base[fora].titulo, "nome_urna": base[fora].NM_URNA_CANDIDATO,
                               "cargo": base[fora].DS_CARGO, "uf": base[fora].SG_UF,
                               "situacao": base[fora].situacao}), "fora_da_disputa_2026.csv")
    base = base[~fora].copy()
    base["casa"] = base.DS_CARGO.map(c.CASA_DO_CARGO)

    sem_titulo = base[base.titulo == ""]
    c.falhar_se(len(sem_titulo) > 0, "candidaturas sem título de eleitor: "
                f"{sem_titulo[['NM_URNA_CANDIDATO', 'DS_CARGO', 'SG_UF']].values.tolist()}")
    # O TSE mantém vivos dois registros da mesma pessoa para o mesmo cargo (13 casos em
    # 13/09/2026, como Cícero Conceição/AL e Sargento Igor Mandacaru/BA). Fica o de melhor
    # situação; empate, o SQ mais novo, que é sequencial.
    repetido = base[base.duplicated(["titulo", "casa"], keep=False)]
    if len(repetido):
        print(f"{len(repetido) // 2} pessoas com dois registros vivos na mesma Casa; fica o de melhor situação:")
        for _, grupo in repetido.groupby("titulo"):
            print("   ", grupo[["NM_URNA_CANDIDATO", "SG_UF", "situacao", "SQ_CANDIDATO"]].values.tolist())
    base["_ordem"] = base.situacao.map(ORDEM_SITUACAO).fillna(9)
    base["_sq"] = base.SQ_CANDIDATO.astype("int64")
    base = (base.sort_values(["_ordem", "_sq"], ascending=[True, False])
            .drop_duplicates(["titulo", "casa"]).drop(columns=["_ordem", "_sq"]))
    duas_casas = base[base.duplicated("titulo", keep=False)]
    if len(duas_casas):
        print("aviso, mesma pessoa viva em duas Casas:",
              duas_casas[["NM_URNA_CANDIDATO", "DS_CARGO", "SG_UF", "situacao"]].values.tolist())

    saida = pd.DataFrame({
        "sq_candidato": base.SQ_CANDIDATO, "titulo": base.titulo, "cpf": base.cpf,
        "nome_urna": base.NM_URNA_CANDIDATO, "nome_civil": base.NM_CANDIDATO,
        "nascimento": base.nascimento, "uf": base.SG_UF, "cargo": base.DS_CARGO,
        "casa": base.casa, "partido": base.SG_PARTIDO, "numero": base.NR_CANDIDATO,
        "situacao": base.situacao, "ocupacao": base.DS_OCUPACAO, "genero": base.DS_GENERO,
    })
    print(saida.groupby("casa").size().to_string())
    c.salvar_csv(saida, "candidaturas_2026.csv")


if __name__ == "__main__":
    main()
