"""Etapa 7b. Fila de candidatos à Câmara Federal vindos de outros cargos.

Abrange todos os candidatos que têm trajetória política/pública prévia e disputam a Câmara:
Governo estadual (governadores e vice-governadores), Senado, Assembleias Legislativas,
Prefeituras, Câmaras Municipais e Volta à Casa (ex-deputados federais).

Para figuras de destaque, o script já injeta dados pré-pesquisados de biografia,
cargos não eletivos (secretarias, ministérios) e mandatos anteriores a 2006.

Entrada: mapeamento_pre.csv (etapa 10) para pre.
Saída: pesquisa_manual_camara_outros_<universo>.csv; com --publicar, a aba
"Câmara outros cargos (pré-mapeados)" na planilha "Pré mapeamento".
Rodar: python -m outros.novos_eleitos.e7_camara_outros --universo pre [--publicar]
"""
import argparse

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.dados_pesquisa_camara_outros import PESQUISA_CAMARA_OUTROS

ABA_CAMARA_OUTROS = "Câmara outros cargos (pré-mapeados)"

COLUNAS_IDENTIFICACAO = [
    "Nome",
    "Nome de urna (TSE)",
    "Partido",
    "UF",
    "Reeleição, volta ou novo",
    "Tipo de origem",
    "Origem",
    "Mandatos eletivos desde 2006 (TSE)",
    "Ocupação declarada ao TSE",
    "Instagram",
    "Outras redes declaradas",
]

MANUAIS = [
    "Teve mandato antes de 2006?",
    "Cargo não eletivo anterior (ex.: secretário de pasta)",
    "Pasta ou área",
    "Período",
    "Temática principal",
    "Fonte da informação",
    "Observações",
]

COLUNAS_TEMAS = [
    "IU: temas",
    "IU: subtemas da educação",
    "IU: cargos anteriores",
    "IU: proximidade temática (1 a 4)",
    "IU: relevância política (1 a 4)",
    "Marcela: PL relevante",
    "Marcela: comissão de educação",
    "Organograma da Seduc: como o nome casou (a conferir)",
    "Organograma da Seduc: trecho",
    "Câmara: proposições desde 2019 (autoria ou coautoria)",
    "Câmara: relatorias com andamento mais recente",
    "Senado: matérias de autoria desde 2019",
]

COLUNAS_FIM = [
    "ID na Câmara",
    "Código no Senado",
    "SQ_CANDIDATO",
]

ORDEM_ORIGEM = {
    "Governo estadual": 0,
    "Senado": 1,
    "Assembleia": 2,
    "Prefeitura": 3,
    "Câmara": 4,
    "Câmara Municipal": 5,
}


def fila_camara_outros(universo="pre"):
    m = c.ler_csv(f"mapeamento_{universo}.csv")
    camara = m[m["Casa disputada"] == "Câmara"].copy()
    
    # Todos com histórico eletivo prévio que disputam a Câmara (excluindo apenas os sem mandato e os que tentam reeleição direta)
    fila = camara[(camara["Tipo de origem"] != "Sem mandato eletivo") & 
                  (camara["Reeleição, volta ou novo"] != "Reeleição")].copy()
    
    # Inicializa colunas manuais
    for col in MANUAIS:
        fila[col] = ""
    
    # Injeta dados pré-pesquisados
    for sq, dados in PESQUISA_CAMARA_OUTROS.items():
        alvo = fila["SQ_CANDIDATO"] == sq
        if alvo.any():
            for col, val in dados.items():
                if col in fila.columns:
                    fila.loc[alvo, col] = val
    
    # Ordena: Governo estadual -> Senado -> Assembleia -> Prefeitura -> Câmara -> Câmara Municipal, depois UF, depois Nome
    fila["_ordem_origem"] = fila["Tipo de origem"].map(ORDEM_ORIGEM).fillna(99)
    fila = fila.sort_values(["_ordem_origem", "UF", "Nome"]).drop(columns=["_ordem_origem"])
    
    # Mantém apenas colunas desejadas que existam
    colunas_ordenadas = [col for col in COLUNAS_IDENTIFICACAO if col in fila.columns] + \
                        MANUAIS + \
                        [col for col in COLUNAS_TEMAS if col in fila.columns] + \
                        [col for col in COLUNAS_FIM if col in fila.columns]
    
    return fila[colunas_ordenadas]


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], default="pre")
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    fila = fila_camara_outros(a.universo)
    preenchidos = (fila["Cargo não eletivo anterior (ex.: secretário de pasta)"] != "").sum()
    print(f"{len(fila)} nomes vindos de outros cargos/mandatos para a Câmara.")
    print(fila["Tipo de origem"].value_counts().to_string())
    print(f"Com Instagram declarado: {(fila['Instagram'] != '').sum()}; com biografia/cargo pesquisados: {preenchidos}")
    
    c.salvar_csv(fila, f"pesquisa_manual_camara_outros_{a.universo}.csv")
    
    if not a.publicar:
        return
    
    destino = c.planilha_destino()
    c.gravar_aba(
        destino,
        ABA_CAMARA_OUTROS,
        fila,
        preservar=MANUAIS,
        chave="SQ_CANDIDATO",
        congelar_colunas=1,
        largura_minima={col: 220 for col in MANUAIS},
    )
    print(f"Aba '{ABA_CAMARA_OUTROS}' publicada com sucesso!")


if __name__ == "__main__":
    main()
