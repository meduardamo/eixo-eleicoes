"""
Coleta de candidaturas e planos de governo no TSE.

Três fontes:
  - base oficial (CSV consulta_cand), atualizada 1x/dia
  - candidaturas em tempo real pela API do DivulgaCand (atualiza a cada 60 min)
  - PDFs dos planos de governo (só presidente e governador entregam)

Salva as tabelas no Google Sheets se SPREADSHEET_ID_TSE estiver definido.
Autenticação pelo arquivo credentials.json (service account).
"""

import io
import os
import time
import zipfile
from pathlib import Path

import gspread
import requests
import pandas as pd

ANO = 2026

API = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1"
DOC = "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/doc"
CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"

HEADERS = {"User-Agent": "Mozilla/5.0"}
PAUSA = 0.3   # o TSE bloqueia se as requisições vierem rápido demais
PASTA = Path("dados_tse")

UFS = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB',
       'PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']

# códigos de cargo do TSE
CARGOS = {1: 'PRESIDENTE', 3: 'GOVERNADOR', 5: 'SENADOR',
          6: 'DEPUTADO FEDERAL', 7: 'DEPUTADO ESTADUAL', 8: 'DEPUTADO DISTRITAL'}


def _get(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=40)
            r.raise_for_status()
            time.sleep(PAUSA)
            return r.json()
        except Exception:
            time.sleep(1)
    return None


def cod_eleicao(ano, abrangencia='F'):
    # cada eleição tem um código próprio (muda a cada ano); 'F' geral, 'M' municipal
    for e in _get(f"{API}/eleicao/ordinarias") or []:
        if e.get('ano') == ano and e.get('tipoAbrangencia') == abrangencia:
            return e['id']
    raise ValueError(f"Eleição {ano}/{abrangencia} não encontrada")


def baixar_base_oficial(ano=ANO):
    """Baixa o consulta_cand (zip) e extrai o CSV nacional. Só rebaixa se mudou."""
    PASTA.mkdir(parents=True, exist_ok=True)
    url = f"{CDN}/consulta_cand_{ano}.zip"
    estado = PASTA / f"_ultimo_{ano}.txt"

    atual = requests.head(url, headers=HEADERS, timeout=30).headers.get("Last-Modified")
    if estado.exists() and estado.read_text() == (atual or ""):
        print("base sem alteração desde o último download")
        return PASTA / f"consulta_cand_{ano}_BRASIL.csv"

    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    alvo = f"consulta_cand_{ano}_BRASIL.csv"
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extract(alvo, PASTA)
    estado.write_text(atual or "")
    print(f"baixado: {alvo}")
    return PASTA / alvo


# cargos proporcionais que não interessam — filtrados da base oficial
CARGOS_IGNORAR = {"DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL"}


def carregar_base(csv_path):
    # o CSV vem com todos os cargos; cortamos deputado (não monitoramos)
    df = pd.read_csv(csv_path, encoding="ISO-8859-1", sep=";", low_memory=False)
    return df[~df["DS_CARGO"].isin(CARGOS_IGNORAR)].reset_index(drop=True)


def consolidar(df_api, csv_path):
    """Base-mãe: junta o CSV oficial (perfil completo) com a tabela da API
    (link do plano + situação em tempo real), pelo SQ_CANDIDATO.
    O CSV é a espinha; a API só acrescenta o que ele não tem."""
    base = carregar_base(csv_path)
    extra = (df_api[["sq_candidato", "link_plano", "situacao"]]
             .rename(columns={"sq_candidato": "SQ_CANDIDATO",
                              "link_plano": "LINK_PLANO",
                              "situacao": "SITUACAO_TEMPO_REAL"}))
    return base.merge(extra, on="SQ_CANDIDATO", how="left")


def extrair_candidaturas(ano=ANO, cargos=(1, 3, 5), enriquecer=True):
    """Lista candidaturas pela API. enriquecer busca o detalhe (partido, gênero, raça)
    de cada um — mais lento, vale a pena só no majoritário."""
    eleicao = cod_eleicao(ano)
    linhas = []
    for cargo in cargos:
        ues = ['BR'] if cargo == 1 else UFS   # presidente é nacional; resto é por UF
        for ue in ues:
            d = _get(f"{API}/candidatura/listar/{ano}/{ue}/{eleicao}/{cargo}/candidatos")
            cands = (d or {}).get('candidatos', [])
            for c in cands:
                row = {"ano": ano, "cargo": CARGOS[cargo], "ue": ue,
                       "sq_candidato": c['id'], "numero": c.get('numero'),
                       "nome_urna": c.get('nomeUrna'), "situacao": c.get('descricaoSituacao'),
                       "coligacao_federacao": c.get('nomeColigacao')}
                if enriquecer:
                    det = _get(f"{API}/candidatura/buscar/{ano}/{ue}/{eleicao}/candidato/{c['id']}")
                    if det:
                        row["partido"] = (det.get('partido') or {}).get('sigla')
                        row["genero"] = det.get('descricaoSexo')
                        row["raca"] = det.get('descricaoCorRaca')
                        # link do plano de governo (só presidente/governador têm)
                        idarq = _id_plano(det)
                        row["link_plano"] = f"{DOC}/{idarq}" if idarq else None
                linhas.append(row)
            print(f"{CARGOS[cargo]} {ue}: {len(cands)}")
    return pd.DataFrame(linhas)


def _id_plano(detalhe):
    # codTipo 5 = proposta de governo (os outros arquivos são certidões, bens etc.)
    for f in detalhe.get('arquivos') or []:
        if str(f.get('codTipo')) == "5":
            return f['idArquivo']
    return None


CREDS_FILE = Path("credentials.json")
SHEETS_ID  = os.getenv("SPREADSHEET_ID_TSE", "")


def salvar_no_sheets(df, aba):
    """Sobrescreve a aba no Google Sheets com os dados do DataFrame."""
    gc = gspread.service_account(filename=str(CREDS_FILE))
    sh = gc.open_by_key(SHEETS_ID)
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba, rows=len(df) + 1, cols=len(df.columns))
    ws.clear()
    # converte NaN para string vazia para evitar erros de serialização
    valores = df.where(df.notna(), "").astype(str)
    ws.update([valores.columns.tolist()] + valores.values.tolist())
    print(f"Sheets atualizado: aba '{aba}' ({len(df)} linhas)")


if __name__ == '__main__':
    csv = baixar_base_oficial()
    df = extrair_candidaturas(cargos=(1, 3, 5))
    base = consolidar(df, csv)
    print(f"base consolidada: {base.shape[0]} linhas")

    salvar_no_sheets(df, "candidaturas")
    salvar_no_sheets(base, "base_consolidada")
