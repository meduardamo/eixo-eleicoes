"""Planilha das médias da comunicação (Jess): abas `Série Semanal` e `Últimas Pesquisas`.

Governador, 1º turno. Lê o cache Parquet que o `05 - Rebuild BI` acabou de publicar
(`resultados_bi`, `resultados`, `pesquisas` da matriz T1) e a `base_dadosabertos` da
planilha de candidaturas, e regrava as duas abas inteiras. Não recalcula média: a série é
a `media_hibrida_30d` do `resultados_bi`, a mesma dos painéis.

Brancos, nulos e indecisos = 100 - `declarado_hibrido_30d` (a fatia que declarou candidato,
medida pesquisa a pesquisa com o mesmo peso da média).

Quem entra: nome testado em pesquisa que casa com uma candidatura a governador registrada no
TSE na mesma UF. Renúncia e indeferido saem; indeferido com recurso fica com `*`. O casamento
é pelo nome (urna ou civil) e, quando a grafia da pesquisa difere da do TSE, pelo partido mais
um nome parecido (partido tem no máximo um candidato a governador por UF). Nome que não casa
sai e aparece no log.

    python outros/planilha_medias.py            # só monta e imprime o resumo
    python outros/planilha_medias.py --gravar   # grava as duas abas
"""
import difflib
import io
import json
import os
import re
import sys
import unicodedata
from pathlib import Path

import gspread
import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
from compartilhado.cache_parquet import API_ARQUIVOS, _achar_arquivo, nome_arquivo, pasta_cache  # noqa: E402

PLANILHA_MEDIAS_ENV = "SPREADSHEET_ID_MEDIAS"
MATRIZ_T1_ENV = "SPREADSHEET_ID_POLLINGDATA"
CANDIDATURAS_ENV = "SPREADSHEET_ID_TSE"
INICIO_SERIE = pd.Timestamp("2026-07-01")
NV = "Brancos, nulos e indecisos"

SAI = {"Renúncia", "Indeferido", "Cancelado", "Falecido", "Cassado"}
COM_RECURSO = {"Indeferido em prazo recursal ou com recurso"}

# Sigla que a matriz usa -> sigla do TSE, quando diferem (sem acento, maiúscula).
SIGLA_TSE = {"REP": "REPUBLICANOS", "DEM": "DEMOCRATA", "MOB": "MOBILIZA", "SD": "SOLIDARIEDADE",
             "CID": "CIDADANIA", "PCDOB": "PC DO B"}
TITULO_PESSOA = {"dr", "dra", "doutor", "doutora", "professor", "professora", "prof", "delegado",
                 "delegada", "tenente", "coronel", "subtenente", "sargento", "cabo", "capitao",
                 "economista", "de", "da", "do", "dos", "das", "e", "jr", "junior", "filho", "neto"}

NOMES_UF = {'AC': 'Acre', 'AL': 'Alagoas', 'AM': 'Amazonas', 'AP': 'Amapá', 'BA': 'Bahia', 'CE': 'Ceará',
            'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás', 'MA': 'Maranhão', 'MG': 'Minas Gerais',
            'MS': 'Mato Grosso do Sul', 'MT': 'Mato Grosso', 'PA': 'Pará', 'PB': 'Paraíba', 'PE': 'Pernambuco',
            'PI': 'Piauí', 'PR': 'Paraná', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 'RO': 'Rondônia',
            'RR': 'Roraima', 'RS': 'Rio Grande do Sul', 'SC': 'Santa Catarina', 'SE': 'Sergipe', 'SP': 'São Paulo',
            'TO': 'Tocantins'}
MESES = ['', 'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro',
         'outubro', 'novembro', 'dezembro']


def sem_acento(s):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))


def chave(nome):
    s = sem_acento(re.sub(r'\s*\([^)]*\)', '', str(nome))).lower()
    return re.sub(r'[^a-z0-9]', '', s)


def tokens(nome):
    s = sem_acento(re.sub(r'\s*\([^)]*\)', '', str(nome))).lower()
    return [t for t in re.findall(r'[a-z0-9]+', s) if len(t) > 2 and t not in TITULO_PESSOA]


def sigla(candidato_partido):
    m = re.search(r'\(([^)]*)\)\s*$', str(candidato_partido))
    s = sem_acento(m.group(1)).upper().strip() if m else ''
    return SIGLA_TSE.get(s, s)


def nome_parecido(a, b):
    """Algum sobrenome/prenome da pesquisa bate (grafia quase igual) com algum do TSE."""
    return any(difflib.SequenceMatcher(None, x, y).ratio() >= 0.8 for x in tokens(a) for y in tokens(b))


# ---------------------------------------------------------------- dados

def credenciais():
    escopos = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    raw = os.getenv('GOOGLE_CREDENTIALS_JSON', '').strip()
    if raw:
        return Credentials.from_service_account_info(json.loads(raw), scopes=escopos)
    return Credentials.from_service_account_file(str(RAIZ / 'credentials.json'), scopes=escopos)


def baixar_cache(sessao, spreadsheet_id, aba):
    nome = nome_arquivo(spreadsheet_id, aba)
    arquivo = _achar_arquivo(sessao, pasta_cache(), nome)
    if not arquivo:
        raise RuntimeError(f'cache {nome} não existe na pasta')
    r = sessao.get(f'{API_ARQUIVOS}/{arquivo}', params={'alt': 'media', 'supportsAllDrives': 'true'}, timeout=120)
    r.raise_for_status()
    return pd.read_parquet(io.BytesIO(r.content))


def env(nome):
    valor = os.getenv(nome, '').strip()
    if not valor:
        raise RuntimeError(f'{nome} não definido')
    return valor


def carregar(creds):
    sessao = AuthorizedSession(creds)
    t1 = env(MATRIZ_T1_ENV)
    dados = {aba: baixar_cache(sessao, t1, aba) for aba in ('resultados_bi', 'resultados', 'pesquisas')}
    dados['base'] = baixar_cache(sessao, env(CANDIDATURAS_ENV), 'base_dadosabertos')
    return dados


# ---------------------------------------------------------------- TSE

def casar_com_tse(nomes_por_uf, base):
    """{(uf, candidato_partido): linha do TSE ou None}."""
    gov = base[base.DS_CARGO.str.upper().eq('GOVERNADOR')].copy()
    gov['_sigla'] = gov.SG_PARTIDO.map(lambda s: sem_acento(s).upper().strip())
    casados = {}
    for uf, cp in nomes_por_uf:
        g = gov[gov.SG_UF == uf]
        k = chave(cp)
        m = g[g.NM_URNA_CANDIDATO.map(chave).eq(k) | g.NM_CANDIDATO.map(chave).eq(k)]
        if m.empty:
            # Nomes da pesquisa contidos no nome do TSE. Pelo nome civil só vale com o mesmo
            # partido: "Carlos Brandão" cabe no nome civil de Orleans Brandão, e não é ele.
            ts = set(tokens(cp))
            m = g[g.NM_URNA_CANDIDATO.map(lambda x: bool(ts) and ts <= set(tokens(x)))
                  | (g._sigla.eq(sigla(cp)) & g.NM_CANDIDATO.map(lambda x: bool(ts) and ts <= set(tokens(x))))]
        if m.empty:
            m = g[g._sigla.eq(sigla(cp))
                  & (g.NM_URNA_CANDIDATO.map(lambda x: nome_parecido(cp, x))
                     | g.NM_CANDIDATO.map(lambda x: nome_parecido(cp, x)))]
        if len(m) > 1:
            # A mesma pessoa pode ter dois registros (o primeiro indeferido, o segundo em
            # recurso): vale o que ainda está na disputa.
            m = m[~m.SITUACAO_TEMPO_REAL.isin(SAI)] if (~m.SITUACAO_TEMPO_REAL.isin(SAI)).sum() == 1 else m
        casados[(uf, cp)] = m.iloc[0] if len(m) == 1 else None
        if len(m) > 1:
            print(f'  {uf} {cp}: casa com {len(m)} candidaturas no TSE, fica de fora')
    return casados


def filtrar_registrados(df, base, log=print):
    """Mantém só candidato registrado e na disputa; une grafias da mesma pessoa; marca `*`.

    `df` tem `uf` e `candidato_partido`. Devolve cópia com `candidato_partido` já no nome
    que vai para a planilha (a grafia mais frequente na matriz, com `*` se for o caso).
    """
    pares = sorted(set(zip(df.uf, df.candidato_partido)))
    casados = casar_com_tse(pares, base)
    sem_registro = [p for p in pares if casados[p] is None]
    if 'd' in df.columns:
        # No log, só quem ainda tem série na última data da UF: o resto já saiu pelos 60 dias.
        ultima = df.groupby(['uf', 'candidato_partido']).d.max()
        fim_uf = df.groupby('uf').d.max()
        sem_registro = [p for p in sem_registro if ultima[p] == fim_uf[p[0]]]
    if sem_registro:
        log(f'  sem registro de governador no TSE (fora): {sem_registro}')
    fora = [p for p in pares if casados[p] is not None and casados[p].SITUACAO_TEMPO_REAL in SAI]
    if fora:
        log(f'  renúncia/indeferido (fora): {[(p, casados[p].SITUACAO_TEMPO_REAL) for p in fora]}')

    freq = df.groupby(['uf', 'candidato_partido']).size()
    por_pessoa = {}
    for p in pares:
        c = casados[p]
        if c is None or c.SITUACAO_TEMPO_REAL in SAI:
            continue
        por_pessoa.setdefault(c.SQ_CANDIDATO, []).append(p)
    nome_final = {}
    for sq, ps in por_pessoa.items():
        principal = max(ps, key=lambda p: freq[p])
        nome = principal[1] + (' *' if casados[principal].SITUACAO_TEMPO_REAL in COM_RECURSO else '')
        if len(ps) > 1:
            log(f'  mesma pessoa no TSE, fica a grafia mais usada: {ps} -> {nome}')
        # Só a grafia principal segue: as outras são séries paralelas da mesma pessoa.
        nome_final[principal] = nome
    out = df[[p in nome_final for p in zip(df.uf, df.candidato_partido)]].copy()
    out['candidato_partido'] = [nome_final[p] for p in zip(out.uf, out.candidato_partido)]
    return out


# ---------------------------------------------------------------- Série Semanal

def num(s):
    return pd.to_numeric(s.astype(str).str.replace(',', '.'), errors='coerce')


def montar_serie_semanal(dados, hoje, log=print):
    bi = dados['resultados_bi']
    bi = bi[(bi.cargo == 'governador') & (bi.turno == 't1') & (bi.tipo == 'candidato')].copy()
    bi['h'] = num(bi.media_hibrida_30d)
    bi['nv'] = 100 - num(bi.declarado_hibrido_30d)
    bi['d'] = pd.to_datetime(bi.data_campo)
    bi = bi[bi.h.notna()]
    nv = bi.drop_duplicates(['uf', 'd'])[['uf', 'd', 'nv']].rename(columns={'nv': 'h'})
    nv = nv[nv.h.notna()]
    bi = filtrar_registrados(bi, dados['base'], log)

    datas = list(pd.date_range(INICIO_SERIE, hoje, freq='7D'))

    def valor(serie, dt):
        s = serie[serie.d <= dt]
        return '' if s.empty else round(float(s.sort_values('d').h.iloc[-1]), 1)

    mes_fim = MESES[datas[-1].month].upper()
    grid, fmt, datas_cel, series_cel = [], [], [], []
    grid.append([f'SÉRIE SEMANAL - MÉDIA PONDERADA (JULHO A {mes_fim})'])
    fmt.append((0, 0, 8, 'titulo'))
    grid.append(['Candidatos registrados no TSE e testados em pesquisa nos últimos 60 dias, mais brancos, '
                 'nulos e indecisos. Valores em %.'])
    fmt.append((1, 0, 8, 'sub'))
    grid.append(['* Candidatura indeferida pelo TSE, com recurso: segue na disputa até a decisão final. '
                 f'Situação das candidaturas em {hoje:%d/%m/%Y}.'])
    fmt.append((2, 0, 8, 'sub'))
    grid.append([])
    largura_max = 0
    ordem_ufs = sorted(NOMES_UF, key=lambda k: sem_acento(NOMES_UF[k]).lower())
    for uf in ordem_ufs:
        u = bi[bi.uf == uf]
        if u.empty:
            log(f'  {uf}: sem candidato com série')
            continue
        fim = u.d.max()
        vivos = u[u.d == fim].sort_values('h', ascending=False).candidato_partido.tolist()
        n = nv[nv.uf == uf]
        colunas = vivos + ([NV] if not n.empty else [])
        series = {cp: u[u.candidato_partido == cp] for cp in vivos}
        if not n.empty:
            series[NV] = n
        ncol = 1 + len(colunas)
        foto_col = ncol + 1
        largura_max = max(largura_max, foto_col + 2)
        r0 = len(grid)
        linha = [''] * (foto_col + 2)
        linha[0] = f'{NOMES_UF[uf]} ({uf}) - Evolução semanal'
        linha[foto_col] = f'{uf} - Foto atual'
        grid.append(linha)
        fmt += [(r0, 0, ncol, 'estado'), (r0, foto_col, foto_col + 2, 'estado')]
        grid.append(['Data'] + colunas + [''] + ['Candidato', '% Atual'])
        fmt += [(r0 + 1, 0, ncol, 'cabecalho'), (r0 + 1, foto_col, foto_col + 2, 'cabecalho')]
        atuais = [(cp, valor(series[cp], fim)) for cp in colunas]
        for i, dt in enumerate(datas):
            lin = [dt.strftime('%Y-%m-%d')]
            lin += [valor(series[cp], min(dt, fim)) if dt >= series[cp].d.min() else '' for cp in colunas] + ['']
            lin += list(atuais[i]) if i < len(atuais) else ['', '']
            grid.append(lin)
        for j in range(len(datas), len(atuais)):
            grid.append([''] * foto_col + list(atuais[j]))
        datas_cel.append((r0 + 2, r0 + 2 + len(datas)))
        series_cel.append((r0 + 2, r0 + 2 + len(datas), 1, ncol))
        grid.append([])
        log(f'  {uf}: {len(vivos)} candidatos, última data {fim:%d/%m}, líder {atuais[0]}')
    grid = [row + [''] * (largura_max - len(row)) for row in grid]
    return grid, fmt, largura_max, datas_cel, series_cel


def gravar_serie_semanal(sh, grid, fmt, largura_max, datas_cel, series_cel):
    ws = sh.worksheet('Série Semanal')
    sid = ws.id
    ws.clear()
    sh.batch_update({'requests': [{'unmergeCells': {'range': {'sheetId': sid}}}]})
    ws.resize(rows=len(grid) + 20, cols=largura_max + 1)
    C = {'marinho': {'red': 0.098, 'green': 0.176, 'blue': 0.306},
         'gelo': {'red': 0.957, 'green': 0.953, 'blue': 0.937}, 'branco': {'red': 1, 'green': 1, 'blue': 1},
         'sub': {'red': 0.463, 'green': 0.463, 'blue': 0.447}, 'preto': {'red': 0, 'green': 0, 'blue': 0}}
    sh.batch_update({'requests': [
        {'repeatCell': {'range': {'sheetId': sid}, 'cell': {'userEnteredFormat': {
            'backgroundColor': C['branco'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE',
            'textFormat': {'foregroundColor': C['preto'], 'fontSize': 10, 'fontFamily': 'Montserrat',
                           'bold': False, 'italic': False}}},
            'fields': 'userEnteredFormat'}},
        {'updateBorders': {'range': {'sheetId': sid}, 'top': {'style': 'NONE'}, 'bottom': {'style': 'NONE'},
                           'left': {'style': 'NONE'}, 'right': {'style': 'NONE'},
                           'innerHorizontal': {'style': 'NONE'}, 'innerVertical': {'style': 'NONE'}}}]})

    ws.update(values=grid, range_name='A1', value_input_option='USER_ENTERED')

    fmt = [(0, 0, largura_max, 'titulo'), (1, 0, largura_max, 'sub'), (2, 0, largura_max, 'sub')] + fmt[3:]
    reqs = []
    for linha, c0, c1, est in fmt:
        if est in ('titulo', 'sub', 'estado'):
            reqs.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': linha, 'endRowIndex': linha + 1,
                                                  'startColumnIndex': c0, 'endColumnIndex': c1},
                                        'mergeType': 'MERGE_ALL'}})
    ESTILO = {'titulo': (C['marinho'], C['branco'], True, 13, False), 'sub': (C['gelo'], C['sub'], False, 10, True),
              'estado': (C['gelo'], C['marinho'], True, 11, False),
              'cabecalho': (C['marinho'], C['branco'], True, 10, False)}
    for linha, c0, c1, est in fmt:
        bg, fg, bold, size, italic = ESTILO[est]
        wrap = 'WRAP' if est == 'cabecalho' else 'OVERFLOW_CELL'
        align = 'CENTER' if est == 'cabecalho' or (est == 'estado' and c0 > 0) else 'LEFT'
        reqs.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': linha, 'endRowIndex': linha + 1,
                      'startColumnIndex': c0, 'endColumnIndex': c1},
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg, 'wrapStrategy': wrap, 'verticalAlignment': 'MIDDLE',
                'horizontalAlignment': align,
                'textFormat': {'foregroundColor': fg, 'bold': bold, 'fontSize': size, 'italic': italic,
                               'fontFamily': 'Montserrat'}}},
            'fields': 'userEnteredFormat'}})
    # Formato que a Jess aplicou na aba: data dd/mmm e série sem casa decimal (a foto atual fica com 1 casa).
    for r0, r1 in datas_cel:
        reqs.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': 0, 'endColumnIndex': 1},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'DATE', 'pattern': 'dd/mmm'}}},
            'fields': 'userEnteredFormat.numberFormat'}})
    for r0, r1, c0, c1 in series_cel:
        reqs.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c0, 'endColumnIndex': c1},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}}},
            'fields': 'userEnteredFormat.numberFormat'}})

    def altura(i0, i1, px):
        return {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': i0,
                                                        'endIndex': i1},
                                              'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}
    reqs += [altura(0, 1, 40), altura(1, 2, 26), altura(2, 3, 26), altura(3, 4, 14),
             {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': 0,
                                                      'endIndex': 1},
                                            'properties': {'pixelSize': 90}, 'fields': 'pixelSize'}},
             {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': 1,
                                                      'endIndex': largura_max},
                                            'properties': {'pixelSize': 150}, 'fields': 'pixelSize'}}]
    for linha, c0, c1, est in fmt:
        if est == 'estado' and c0 == 0:
            reqs += [altura(linha, linha + 1, 28), altura(linha + 1, linha + 2, 34), altura(linha + 2, linha + 15, 22)]
    sh.batch_update({'requests': reqs})
    print(f'Série Semanal gravada: {len(grid)} linhas x {largura_max} colunas')


# ---------------------------------------------------------------- main

def main():
    from outros import gerar_aba_ultimas_pesquisas as ultimas

    gravar = '--gravar' in sys.argv
    hoje = pd.Timestamp.now(tz='America/Recife').tz_localize(None).normalize()
    creds = credenciais()
    dados = carregar(creds)
    print('Série Semanal')
    serie = montar_serie_semanal(dados, hoje)
    print('Últimas Pesquisas')
    tabela = ultimas.montar(dados, hoje)
    if not gravar:
        return
    gc = gspread.authorize(creds)
    gc.timeout = 120
    sh = gc.open_by_key(env(PLANILHA_MEDIAS_ENV))
    gravar_serie_semanal(sh, *serie)
    ultimas.gravar(sh, tabela)


if __name__ == '__main__':
    main()
