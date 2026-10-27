"""Junta na planilha de transição o que o time de monitoramento levanta e as
notícias de transição da raspagem.

Fontes (só leitura):
- planilha "Secretarias e ministérios 2026 > 2027" do monitoramento: abas
  Secretários e Ministros (nome para 2027, exoneração, titular de outubro);
- planilha "Equipes de transição" do monitoramento: integrantes por estado;
- aba noticias da planilha da raspagem: linhas de tipo ou alerta "transição".

Destino: três abas da planilha de transição, reescritas inteiras a cada rodada
para que correção feita na origem chegue sozinha. As abas curadas à mão
(Decretos de transição, Equipe 2027 etc.) não são tocadas.

    python -m outros.transicao_sincroniza [--seco]

IDs só por variável de ambiente (repo público): SPREADSHEET_ID_TRANSICAO,
SPREADSHEET_ID_MON_SECRETARIOS, SPREADSHEET_ID_MON_EQUIPES e
SPREADSHEET_ID_TSE (a da raspagem de notícias).
"""
import json
import os
import re
import sys
import unicodedata
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

SECO = "--seco" in sys.argv

ABA_NOMES = "Nomes da transição"
ABA_SECRETARIADO = "Secretariado 2027"
ABA_NOTICIAS = "Notícias de transição"

UF_POR_NOME = {
    "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM", "bahia": "BA",
    "ceara": "CE", "distrito federal": "DF", "espirito santo": "ES", "goias": "GO",
    "maranhao": "MA", "mato grosso": "MT", "mato grosso do sul": "MS",
    "minas gerais": "MG", "para": "PA", "paraiba": "PB", "parana": "PR",
    "pernambuco": "PE", "piaui": "PI", "rio de janeiro": "RJ",
    "rio grande do norte": "RN", "rio grande do sul": "RS", "rondonia": "RO",
    "roraima": "RR", "santa catarina": "SC", "sao paulo": "SP", "sergipe": "SE",
    "tocantins": "TO",
}

# Espelha _RE_TITULO_FEDERAL de noticias_eleicoes_scraper.py: transição com
# ministério no título é do governo federal, mesmo que o modelo tenha dado a UF
# de quem é cotado.
RE_TITULO_FEDERAL = re.compile(
    r"(?<!ex-)\bministr[oa]s?\b|\bministerios?\b(?!\s+publico)|\besplanada\b"
    r"|reforma ministerial|\blula 4\b|\bquarto mandato\b|governo lula"
    r"|(?<!secretaria da )(?<!secretario da )(?<!de estado da )\bfazenda\b")


def sem_acento(texto) -> str:
    return unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode().lower().strip()


def limpo(texto) -> str:
    return " ".join(str(texto or "").replace("\xa0", " ").split())


def _gc():
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    info = json.loads(creds_json) if creds_json else json.load(open("credentials.json", encoding="utf-8"))
    return gspread.authorize(Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]))


def _id(nome: str) -> str:
    valor = os.getenv(nome, "").strip()
    if not valor:
        sys.exit(f"falta a variável {nome}")
    return valor


def ler_com_links(planilha, aba: str) -> list:
    """Linhas da aba como [(texto, link)], com o link da célula quando houver
    (HYPERLINK, link inserido ou link num trecho do texto)."""
    meta = planilha.fetch_sheet_metadata(params={
        "includeGridData": "true", "ranges": f"'{aba}'",
        "fields": "sheets.data.rowData.values(formattedValue,hyperlink,"
                  "textFormatRuns.format.link.uri)"})
    linhas = []
    for linha in meta["sheets"][0]["data"][0].get("rowData", []):
        celulas = []
        for v in linha.get("values", []):
            link = v.get("hyperlink", "")
            if not link:
                link = next((r["format"]["link"]["uri"] for r in v.get("textFormatRuns", [])
                             if r.get("format", {}).get("link", {}).get("uri")), "")
            celulas.append((limpo(v.get("formattedValue", "")), link))
        linhas.append(celulas)
    return linhas


def _cabecalho(linhas: list) -> int:
    """Índice da primeira linha com 3 células preenchidas (a aba de equipes
    tem um título solto na linha 1)."""
    for i, l in enumerate(linhas):
        if sum(1 for t, _ in l if t) >= 3:
            return i
    return 0


def _por_nome(linhas: list):
    """Gera dicts {coluna normalizada: (texto, link)} a partir do cabeçalho."""
    i = _cabecalho(linhas)
    nomes = [sem_acento(t) for t, _ in linhas[i]]
    for l in linhas[i + 1:]:
        l = l + [("", "")] * (len(nomes) - len(l))
        if any(t for t, _ in l):
            yield {n: l[j] for j, n in enumerate(nomes) if n}


def _campo(d: dict, *inicios) -> tuple:
    """(texto, link) da primeira coluna cujo nome começa por um dos inícios."""
    for inicio in inicios:
        for nome, valor in d.items():
            if nome.startswith(sem_acento(inicio)):
                return valor
    return ("", "")


def _link(texto: str, link: str) -> str:
    return link or (texto if texto.startswith("http") else "")


def _situacao_do_nome(texto: str) -> tuple:
    """'Maria Gabriela Bessa - cotada' -> ('Maria Gabriela Bessa', 'Cotado')."""
    m = re.search(r"\s*[-–(]\s*(cotad|anunciad|nomead|confirmad)[oa]s?\)?\s*$", texto, re.I)
    if not m:
        return texto, "Anunciado"
    rotulo = {"cotad": "Cotado", "nomead": "Nomeado"}.get(sem_acento(m[1]), "Anunciado")
    return texto[:m.start()].strip(), rotulo


# ─── Monitoramento ───────────────────────────────────────────────────────────

def _equipes_com_colunas_soltas(planilha) -> list:
    """A aba de equipes tem duas colunas sem cabeçalho (tipo e situação).
    Dá nome a elas antes de ler por nome."""
    linhas = ler_com_links(planilha, "Equipes de Transição")
    i = _cabecalho(linhas)
    largura = max(len(l) for l in linhas)
    cab = list(linhas[i]) + [("", "")] * (largura - len(linhas[i]))
    vazias = [j for j, (t, _) in enumerate(cab) if not t and j > 0]
    for j, nome in zip(vazias, ("__tipo", "__situacao")):
        cab[j] = (nome, "")
    linhas[i] = cab
    return linhas


def nomes_da_transicao(planilha) -> list:
    """Integrantes das equipes de transição, governo atual e eleito."""
    linhas = _equipes_com_colunas_soltas(planilha)
    i = _cabecalho(linhas)
    nomes = [t if t.startswith("__") else sem_acento(t) for t, _ in linhas[i]]
    saida = []
    for l in linhas[i + 1:]:
        l = l + [("", "")] * (len(nomes) - len(l))
        d = {n: l[j] for j, n in enumerate(nomes) if n}
        estado = _campo(d, "estado")[0]
        uf = UF_POR_NOME.get(sem_acento(estado), estado.upper() if len(estado) == 2 else "")
        nome = _campo(d, "nome do integrante")[0]
        if not uf or not nome:
            continue
        ato, link = _campo(d, "ato de nomeacao")
        saida.append([uf, _campo(d, "governador eleito")[0], nome,
                      _campo(d, "tematica")[0], _campo(d, "representa")[0],
                      # tipo em branco: a aba é de integrantes de transição (Leia-me)
                      d.get("__tipo", ("", ""))[0] or "Transição", d.get("__situacao", ("", ""))[0],
                      ato, _link(ato, link)])
    return saida


def secretariado(planilha) -> list:
    """Linhas com nome para 2027 ou com mudança no titular de hoje (ato de
    exoneração ou titular de outubro). O '*' da coluna de exoneração marca
    pasta prioritária, não ato, e não conta."""
    saida = []
    for d in _por_nome(ler_com_links(planilha, "Secretários")):
        uf = _campo(d, "uf")[0]
        novo = _campo(d, "ocupante do cargo 2027")[0]
        exon = _campo(d, "decreto/ato de exoneracao")[0].strip("* ").strip()
        outubro = _campo(d, "ocupante do cargo out")[0]
        if not (novo or exon or outubro):
            continue
        nome, situacao = _situacao_do_nome(novo) if novo else ("", "")
        nomeacao, link_nom = _campo(d, "decreto/ato de nomeacao")
        if nome and nomeacao:
            situacao = "Nomeado"
        saida.append([uf, _campo(d, "secretaria de estado")[0], _campo(d, "cargo oficial")[0],
                      _campo(d, "ocupante do cargo set")[0], outubro, exon, nome, situacao,
                      # só o link do ato: a fonte oficial da linha é a página do
                      # titular de hoje, não a origem do nome para 2027
                      nomeacao, _link(nomeacao, link_nom)])
    for d in _por_nome(ler_com_links(planilha, "Ministros")):
        novo = _campo(d, "ocupante do cargo 2027")[0]
        ato, link = _campo(d, "decreto/ato")
        if not (novo or ato):
            continue
        nome, situacao = _situacao_do_nome(novo) if novo else ("", "")
        saida.append(["BR", _campo(d, "orgao federal")[0], _campo(d, "cargo oficial")[0],
                      _campo(d, "ocupante do cargo")[0], "", ato if not novo else "",
                      nome, situacao, ato if novo else "", _link(ato, link)])
    return saida


# ─── Notícias ────────────────────────────────────────────────────────────────

def _uf_da_busca(busca: str) -> str:
    """Busca de transição por estado: '<estado> ("equipe de transição" OR ...)'."""
    if '("equipe de transição"' not in busca:
        return ""
    return UF_POR_NOME.get(sem_acento(busca.split(' ("equipe de transição"')[0]), "")


def noticias(planilha) -> list:
    aba = planilha.worksheet("noticias")
    cab = aba.row_values(1)
    quero = ["titulo", "fonte", "data", "link", "link_real", "uf", "tipo",
             "alerta_tema", "candidato", "busca"]
    idx = {c: cab.index(c) for c in quero if c in cab}
    letras = {c: gspread.utils.rowcol_to_a1(1, i + 1).rstrip("1") for c, i in idx.items()}
    blocos = aba.batch_get([f"{letras[c]}2:{letras[c]}" for c in idx])
    colunas = dict(zip(idx, blocos))
    total = max(len(b) for b in blocos)

    def v(c, i):
        b = colunas.get(c) or []
        return limpo(b[i][0]) if i < len(b) and b[i] else ""

    vistas, saida = set(), []
    for i in range(total):
        if "transição" not in (v("tipo", i), v("alerta_tema", i)):
            continue
        titulo = v("titulo", i)
        manchete, _, veiculo = titulo.rpartition(" - ")
        manchete, veiculo = (manchete, veiculo) if manchete else (titulo, v("fonte", i))
        chave = re.sub(r"\W+", "", sem_acento(manchete))[:90]
        if not chave or chave in vistas:
            continue
        vistas.add(chave)
        uf = v("uf", i).upper()
        if RE_TITULO_FEDERAL.search(sem_acento(manchete)):
            uf = "BR"
        elif not uf:
            uf = _uf_da_busca(v("busca", i)) or "BR"
        saida.append([v("data", i)[:10], uf, "Gov. Federal" if uf == "BR" else "Subnacional",
                      v("candidato", i), manchete, veiculo, v("link_real", i) or v("link", i)])

    def quando(l):
        try:
            return datetime.strptime(l[0], "%d/%m/%Y")
        except ValueError:
            return datetime.min
    return sorted(saida, key=quando, reverse=True)


# ─── Gravação ────────────────────────────────────────────────────────────────

MARINHO = {"red": 25 / 255, "green": 45 / 255, "blue": 78 / 255}
GELO = {"red": 244 / 255, "green": 243 / 255, "blue": 239 / 255}
BRANCO = {"red": 1, "green": 1, "blue": 1}


def gravar(planilha, titulo: str, cabecalho: list, linhas: list, larguras: list):
    """Reescreve a aba inteira no padrão Eixo (Montserrat 10, cabeçalho
    marinho, corpo gelo, alinhamento no meio) e corta a grade no tamanho."""
    try:
        aba = planilha.worksheet(titulo)
    except gspread.WorksheetNotFound:
        aba = planilha.add_worksheet(titulo, rows=len(linhas) + 1, cols=len(cabecalho))
    n_linhas, n_cols = len(linhas) + 1, len(cabecalho)
    aba.resize(rows=max(n_linhas, 2), cols=n_cols)
    aba.update([cabecalho] + linhas, "A1", value_input_option="RAW")
    if n_linhas == 1:
        aba.batch_clear(["A2:Z2"])
    sid = aba.id
    tudo = {"sheetId": sid, "startRowIndex": 0, "endRowIndex": max(n_linhas, 2),
            "startColumnIndex": 0, "endColumnIndex": n_cols}
    pedidos = [
        {"repeatCell": {"range": tudo, "cell": {"userEnteredFormat": {
            "backgroundColor": GELO, "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
            "textFormat": {"fontFamily": "Montserrat", "fontSize": 10}}},
            "fields": "userEnteredFormat(backgroundColor,verticalAlignment,wrapStrategy,textFormat)"}},
        {"repeatCell": {"range": dict(tudo, endRowIndex=1), "cell": {"userEnteredFormat": {
            "backgroundColor": MARINHO, "verticalAlignment": "MIDDLE",
            "textFormat": {"fontFamily": "Montserrat", "fontSize": 10, "bold": True,
                           "foregroundColor": BRANCO}}},
            "fields": "userEnteredFormat(backgroundColor,verticalAlignment,textFormat)"}},
        {"updateSheetProperties": {"properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
                                   "fields": "gridProperties.frozenRowCount"}},
    ]
    for j, px in enumerate(larguras):
        pedidos.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": j, "endIndex": j + 1},
            "properties": {"pixelSize": px}, "fields": "pixelSize"}})
    planilha.batch_update({"requests": pedidos})


def main():
    gc = _gc()
    nomes = nomes_da_transicao(gc.open_by_key(_id("SPREADSHEET_ID_MON_EQUIPES")))
    secret = secretariado(gc.open_by_key(_id("SPREADSHEET_ID_MON_SECRETARIOS")))
    notas = noticias(gc.open_by_key(_id("SPREADSHEET_ID_TSE")))
    print(f"nomes da transição: {len(nomes)}; secretariado: {len(secret)}; notícias: {len(notas)}")
    if SECO:
        for l in nomes[:3] + secret[:3] + notas[:3]:
            print("  ", l)
        return
    destino = gc.open_by_key(_id("SPREADSHEET_ID_TRANSICAO"))
    gravar(destino, ABA_NOMES,
           ["UF", "Governador eleito", "Nome", "Temática", "Governo", "Tipo", "Situação",
            "Ato ou fonte", "Link"], nomes, [50, 150, 210, 200, 120, 150, 120, 320, 220])
    gravar(destino, ABA_SECRETARIADO,
           ["UF", "Órgão", "Cargo", "Titular (set/2026)", "Titular (out/2026)", "Exoneração",
            "Nome para 2027", "Situação", "Ato de nomeação", "Link"],
           secret, [50, 260, 240, 200, 180, 220, 200, 100, 220, 220])
    gravar(destino, ABA_NOTICIAS,
           ["Data", "UF", "Escopo", "Pessoa", "Manchete", "Veículo", "Link"],
           notas, [90, 50, 110, 160, 480, 160, 260])
    print("gravado")


if __name__ == "__main__":
    main()
