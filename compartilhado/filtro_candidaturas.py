"""Tira do resultados_bi quem não está disputando o cargo.

Pesquisa testa pré-candidato que desiste, que vai disputar outro cargo ou que nem é
nome ("Ninguém", pergunta de avaliação colada como candidato). A regra dos 60 dias
já encerra a série de quem parou de ser testado, mas quem ainda aparece em alguma
pesquisa seguia no painel. Aqui a série é cortada a partir do fim do prazo de
registro (15/08/2026) quando o nome não casa com uma candidatura ao mesmo cargo e UF
no TSE, ou quando a candidatura foi renunciada ou indeferida sem recurso.

O histórico antes do prazo fica: em março o pré-candidato era parte da disputa.

A base é a aba `base_dadosabertos` da planilha de candidaturas, lida do cache Parquet
que o workflow 14 publica. Se a base não vier, o filtro não roda e o rebuild segue.
"""
from __future__ import annotations

import difflib
import io
import re
import unicodedata

import pandas as pd

DATA_CORTE = pd.Timestamp("2026-08-16")
CARGOS = {"governador": "GOVERNADOR", "senador": "SENADOR", "presidente": "PRESIDENTE"}
# Situação na coluna de tempo real do DivulgaCand. "Indeferido em prazo recursal ou
# com recurso" fica: o candidato segue na urna até a decisão final.
SITUACAO_FORA = {"renúncia", "renuncia", "indeferido", "cancelado", "falecido", "cassado"}
# Título, patente e profissão que entram no nome de urna e não identificam a pessoa.
PALAVRAS_VAZIAS = {
    "dr", "dra", "doutor", "doutora", "de", "da", "do", "dos", "das", "e", "prof", "professor",
    "professora", "tenente", "coronel", "delegado", "delegada", "capitao", "sargento", "cabo",
    "subtenente", "soldado", "pastor", "pastora", "policial", "bombeiro", "economista",
    "general", "major", "irma", "irmao", "padre", "jr",
}
PARECIDO = 0.75


def _tokens(texto: str) -> list[str]:
    texto = re.sub(r"\s*\([^)]*\)", "", str(texto or ""))
    texto = "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c)).lower()
    return [t for t in re.sub(r"[^a-z0-9]+", " ", texto).split() if t]


def _parecido(a: str, b: str) -> bool:
    return a == b or difflib.SequenceMatcher(None, a, b).ratio() >= PARECIDO


def _casa(nome: list[str], urna: list[str], civil: list[str]) -> bool:
    """Casa quando todo token útil do nome da pesquisa tem par parecido no nome de urna ou civil,
    ou quando todo token útil do nome de urna aparece no nome da pesquisa ("Neidinha" em
    "Neidinha Suruí", "Zema" em "Romeu Zema")."""
    uteis = [t for t in nome if t not in PALAVRAS_VAZIAS] or nome
    if not uteis:
        return False
    registro = urna + civil
    if all(any(_parecido(t, r) for r in registro) for t in uteis):
        return True
    urna_util = [t for t in urna if t not in PALAVRAS_VAZIAS and len(t) >= 4]
    return bool(urna_util) and all(any(_parecido(u, t) for t in uteis) for u in urna_util)


def baixar_base(gc) -> pd.DataFrame:
    from google.auth.transport.requests import AuthorizedSession

    from compartilhado.cache_parquet import API_ARQUIVOS, credenciais_do_cliente, pasta_cache

    sessao = AuthorizedSession(credenciais_do_cliente(gc))
    resposta = sessao.get(API_ARQUIVOS, params={
        "q": f"'{pasta_cache()}' in parents and trashed=false and name contains '__base_dadosabertos.parquet'",
        "fields": "files(id,name,modifiedTime)", "supportsAllDrives": "true", "includeItemsFromAllDrives": "true",
    }, timeout=30)
    resposta.raise_for_status()
    arquivos = resposta.json().get("files", [])
    if not arquivos:
        return pd.DataFrame()
    conteudo = sessao.get(f"{API_ARQUIVOS}/{arquivos[0]['id']}", params={"alt": "media", "supportsAllDrives": "true"}, timeout=120)
    conteudo.raise_for_status()
    return pd.read_parquet(io.BytesIO(conteudo.content))


def _indice(base: pd.DataFrame) -> dict:
    """(cargo, uf) -> lista de (tokens do nome de urna, tokens do nome civil, situação)."""
    indice: dict = {}
    for _, linha in base.iterrows():
        cargo = str(linha.get("DS_CARGO", "")).strip().upper()
        if cargo not in CARGOS.values():
            continue
        uf = "BR" if cargo == "PRESIDENTE" else str(linha.get("SG_UF", "")).strip().upper()
        urna = _tokens(linha.get("NM_URNA_CANDIDATO", ""))
        civil = _tokens(linha.get("NM_CANDIDATO", ""))
        situacao = str(linha.get("SITUACAO_TEMPO_REAL", "")).strip().lower()
        indice.setdefault((cargo, uf), []).append((urna, civil, situacao))
    return indice


def filtrar(df_bi: pd.DataFrame, base: pd.DataFrame) -> tuple[pd.DataFrame, list[tuple]]:
    """Devolve o resultados_bi sem as linhas cortadas e a lista (cargo, uf, candidato, motivo)."""
    if df_bi.empty or base is None or base.empty:
        return df_bi, []
    indice = _indice(base)
    if sum(len(v) for (c, _), v in indice.items() if c == "GOVERNADOR") < 100:
        print("  [candidaturas] base com poucos governadores; filtro não aplicado")
        return df_bi, []

    fora: dict = {}
    pares = df_bi[df_bi["cargo"].isin(CARGOS)][["cargo", "uf", "candidato_partido"]].drop_duplicates()
    for cargo, uf, candidato in pares.itertuples(index=False):
        cargo_tse = CARGOS[cargo]
        registros = indice.get((cargo_tse, "BR" if cargo_tse == "PRESIDENTE" else uf), [])
        nome = _tokens(candidato)
        casados = [sit for urna, civil, sit in registros if _casa(nome, urna, civil)]
        if not casados:
            fora[(cargo, uf, candidato)] = "sem registro no cargo"
        elif all(sit in SITUACAO_FORA for sit in casados):
            fora[(cargo, uf, candidato)] = casados[0]

    if not fora:
        return df_bi, []
    datas = pd.to_datetime(df_bi["data_campo"], errors="coerce")
    chave = list(zip(df_bi["cargo"], df_bi["uf"], df_bi["candidato_partido"]))
    cortar = pd.Series([k in fora for k in chave], index=df_bi.index) & datas.ge(DATA_CORTE)
    return df_bi[~cortar].copy(), sorted((c, u, n, m) for (c, u, n), m in fora.items())


def aplicar(gc, df_bi: pd.DataFrame) -> pd.DataFrame:
    """Chamado pelo rebuild. Qualquer falha devolve o resultados_bi intacto."""
    try:
        base = baixar_base(gc)
        filtrado, cortados = filtrar(df_bi, base)
    except Exception as erro:  # o rebuild não pode cair por causa do filtro
        print(f"  [candidaturas] filtro não aplicado: {erro}")
        return df_bi
    if cortados:
        print(f"  [candidaturas] {len(df_bi) - len(filtrado)} linhas cortadas a partir de {DATA_CORTE.date()}, "
              f"{len(cortados)} séries: " + "; ".join(f"{c}/{u} {n} ({m})" for c, u, n, m in cortados))
    return filtrado
