"""Cadeira no feminino para as senadoras eleitas em 2026.

As etapas e23 e e24 tratam "Eleito em 2026" como rótulo do grupo (comparam o texto), então o CSV fica
como está e a troca acontece só na hora de publicar. Sem isto, republicar desfaz a correção feita na
planilha em 18/11/2026. Nenhuma das reeleitas em 2026 é mulher; se entrar suplente mulher, acrescentar aqui.
"""

SENADORAS_ELEITAS = {"Bia Kicis", "Caroline de Toni", "Gracinha Caiado", "Luizianne Lins", "Mara Rocha",
                     "Marília Arraes", "Marília Campos", "Marina JHC", "Michelle Bolsonaro", "Rayssa Furlan",
                     "Samanda de Lula", "Teresa Surita"}


def ajusta(df, col="Senador"):
    """Troca 'Eleito em 2026' por 'Eleita em 2026' nas linhas das senadoras. Devolve o mesmo DataFrame."""
    m = df[col].isin(SENADORAS_ELEITAS) & (df["Cadeira"] == "Eleito em 2026")
    df.loc[m, "Cadeira"] = "Eleita em 2026"
    return df
