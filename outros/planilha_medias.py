"""Planilha das médias da comunicação (Jess): abas `Série Semanal 2º Turno` e
`Últimas Pesquisas 2º Turno`. As duas do 1º turno saíram em 06/10/2026.

Governador, 2º turno, um bloco por confronto. Lê o cache Parquet que o `05 - Rebuild BI` acabou de publicar
(`resultados_bi`, `resultados`, `pesquisas` da matriz T2) e a `base_dadosabertos` da
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
MATRIZ_T2_ENV = "SPREADSHEET_ID_POLLINGDATA_T2"
ABA_T2 = "Série Semanal 2º Turno"
ABA_ULTIMAS_T2 = "Últimas Pesquisas 2º Turno"
ORDEM_ABAS = [ABA_T2, ABA_ULTIMAS_T2]
CANDIDATURAS_ENV = "SPREADSHEET_ID_TSE"
INICIO_SERIE = pd.Timestamp("2026-07-01")

# Confrontos de 2º turno para governador que foram de fato às urnas (25/10/2026),
# pelo resultado do 1º turno no TSE: UF -> `disputa` da matriz T2. Os demais
# confrontos da matriz são hipóteses testadas antes de 04/10 e ficam de fora.
CONFRONTOS_2T = {"AC": "t2_rick-assis", "AM": "t2_seffair-aziz", "DF": "t2_leao-grass",
                 "ES": "t2_pazolini-ferraco", "RJ": "t2_ruas-paes", "RN": "t2_bezerra-xavier",
                 "TO": "t2_seabra-junior"}
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
    """Cada nome da pesquisa bate (grafia quase igual) com algum nome do TSE.

    Um nome só não basta: "Felipe Agwanã" e "Felipe Camarão" são do mesmo partido no MA,
    e "Valério Luiz" não é "Luís Cesar Bueno".
    """
    ta, tb = tokens(a), tokens(b)
    return bool(ta) and all(any(difflib.SequenceMatcher(None, x, y).ratio() >= 0.75 for y in tb) for x in ta)


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
    t2 = env(MATRIZ_T2_ENV)
    dados = {f'{aba}_t2': baixar_cache(sessao, t2, aba) for aba in ('resultados_bi', 'resultados', 'pesquisas')}
    dados['base'] = baixar_cache(sessao, env(CANDIDATURAS_ENV), 'base_dadosabertos')
    return dados


# ---------------------------------------------------------------- TSE

def casar_com_tse(nomes_por_uf, base, cargo='GOVERNADOR'):
    """{(uf, candidato_partido): linha do TSE ou None}."""
    gov = base[base.DS_CARGO.str.upper().eq(cargo)].copy()
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
            # Pesquisa que junta nome de urna e sobrenome civil: "Cadu Xavier" é CADU DE LULA /
            # CARLOS EDUARDO XAVIER, "Dorinha Seabra" é PROFESSORA DORINHA / MARIA AUXILIADORA SEABRA.
            m = g[g._sigla.eq(sigla(cp))
                  & (g.NM_URNA_CANDIDATO + ' ' + g.NM_CANDIDATO).map(lambda x: bool(ts) and ts <= set(tokens(x)))]
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


SIGLA_MATRIZ = {v: k for k, v in SIGLA_TSE.items()}


def nome_publicado(cp, tse):
    """Nome da matriz com a grafia e o partido do registro no TSE.

    Mantém o nome pelo qual a pesquisa chama a pessoa ("Tenente Coronel Zucco", não "ZUCCO"),
    mas corrige palavra escrita diferente do TSE (Luizinho -> Luisinho, Juriti -> Jurity) e
    troca o partido pelo do registro: a janela partidária fechou em abril, então partido da
    matriz diferente do TSE é erro de cadastro na matriz.
    """
    nome = re.sub(r'\s*\([^)]*\)\s*$', '', str(cp)).strip()
    do_tse = {}
    for w in re.findall(r'[^\W\d_]+', f'{tse.NM_URNA_CANDIDATO} {tse.NM_CANDIDATO}'):
        do_tse.setdefault(sem_acento(w).lower(), w)

    um_so = len(re.findall(r'[^\W\d_]+', nome)) == 1

    def troca(m):
        w = m.group(0)
        k = sem_acento(w).lower()
        if len(k) <= 3 and um_so and sem_acento(tse.NM_URNA_CANDIDATO).lower().strip() == k:
            return do_tse[k]  # nome de urna que é sigla (JHC), não "Jhc"
        if len(k) <= 3 or k in do_tse or k in TITULO_PESSOA:
            return w
        melhor = max(do_tse, key=lambda x: difflib.SequenceMatcher(None, k, x).ratio())
        if difflib.SequenceMatcher(None, k, melhor).ratio() >= 0.75:
            return do_tse[melhor].capitalize()
        return w

    nome = re.sub(r'[^\W\d_]+', troca, nome)
    partido = sem_acento(tse.SG_PARTIDO).upper().strip()
    return f'{nome} ({SIGLA_MATRIZ.get(partido, partido)})'


def nomes_publicados(df, base, log=print):
    """{(uf, candidato_partido da matriz): nome da planilha}, só para quem está na disputa.

    Todas as grafias da mesma pessoa no TSE levam ao mesmo nome: o da grafia mais frequente
    na matriz, corrigida por `nome_publicado`, com `*` se o indeferimento está em recurso.
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
        if c is not None and c.SITUACAO_TEMPO_REAL not in SAI:
            por_pessoa.setdefault(c.SQ_CANDIDATO, []).append(p)
    nomes = {}
    for ps in por_pessoa.values():
        principal = max(ps, key=lambda p: freq[p])
        tse = casados[principal]
        nome = nome_publicado(principal[1], tse) + (' *' if tse.SITUACAO_TEMPO_REAL in COM_RECURSO else '')
        if len(ps) > 1 or nome.removesuffix(' *') != principal[1]:
            log(f'  nome: {[p[1] for p in ps]} -> {nome}')
        for p in ps:
            nomes[p] = nome
    return nomes, {p: max(ps, key=lambda q: freq[q]) for ps in por_pessoa.values() for p in ps}


def filtrar_registrados(df, base, log=print):
    """Mantém só candidato registrado e na disputa, com o nome de `nomes_publicados`.

    Das grafias da mesma pessoa, só a mais frequente segue: as outras são séries paralelas
    da mesma pessoa no `resultados_bi`, e somá-las contaria a pessoa duas vezes.
    """
    nomes, principal = nomes_publicados(df, base, log)
    pares = list(zip(df.uf, df.candidato_partido))
    out = df[[p in nomes and principal[p] == p for p in pares]].copy()
    out['candidato_partido'] = [nomes[p] for p in zip(out.uf, out.candidato_partido)]
    return out


# ---------------------------------------------------------------- Série Semanal

def so_confrontos_reais(df):
    return df[df.uf.map(CONFRONTOS_2T) == df.disputa]


def num(s):
    return pd.to_numeric(s.astype(str).str.replace(',', '.'), errors='coerce')


def atualizado(agora):
    """Hora da rodada: as pesquisas da matriz e a situação no TSE são as desse momento."""
    return f'Atualizado em {agora:%d/%m/%Y} às {agora:%H:%M} (horário de Brasília).'


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
    # Semana ainda aberta: o último ponto é o dia da rodada, para a pesquisa com campo depois da
    # última quarta já aparecer. Na quarta seguinte ele vira o ponto semanal fechado.
    if datas[-1] < hoje.normalize():
        datas.append(hoje.normalize())

    def valor(serie, dt):
        s = serie[serie.d <= dt]
        return '' if s.empty else round(float(s.sort_values('d').h.iloc[-1]), 1)

    mes_fim = MESES[datas[-1].month].upper()
    grid, fmt, datas_cel, series_cel = [], [], [], []
    grid.append([f'SÉRIE SEMANAL - MÉDIA PONDERADA (JULHO A {mes_fim})'])
    fmt.append((0, 0, 8, 'titulo'))
    grid.append(['Candidatos registrados no TSE e testados em pesquisa nos últimos 60 dias, mais brancos, '
                 f'nulos e indecisos. Valores em %. {atualizado(hoje)}'])
    fmt.append((1, 0, 8, 'sub'))
    grid.append(['* Candidatura indeferida pelo TSE, com recurso: segue na disputa até a decisão final.'])
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


def montar_serie_segundo_turno(dados, hoje, log=print):
    """Mesma série do 1º turno, um bloco por confronto de 2º turno de governador (`disputa`)."""
    bi = dados['resultados_bi_t2']
    bi = so_confrontos_reais(bi[(bi.cargo == 'governador') & (bi.turno == 't2') & (bi.tipo == 'candidato')]).copy()
    bi['h'] = num(bi.media_hibrida_30d)
    bi['nv'] = 100 - num(bi.declarado_hibrido_30d)
    bi['d'] = pd.to_datetime(bi.data_campo)
    bi = bi[bi.h.notna()]
    nv = bi.drop_duplicates(['uf', 'disputa', 'd'])[['uf', 'disputa', 'd', 'nv']].rename(columns={'nv': 'h'})
    nv = nv[nv.h.notna()]
    bi = filtrar_registrados(bi, dados['base'], log)

    datas = list(pd.date_range(INICIO_SERIE, hoje, freq='7D'))
    if datas[-1] < hoje.normalize():
        datas.append(hoje.normalize())

    def valor(serie, dt):
        s = serie[serie.d <= dt]
        return '' if s.empty else round(float(s.sort_values('d').h.iloc[-1]), 1)

    mes_fim = MESES[datas[-1].month].upper()
    grid, fmt, datas_cel, series_cel = [], [], [], []
    grid.append([f'SÉRIE SEMANAL 2º TURNO - MÉDIA PONDERADA (JULHO A {mes_fim})'])
    fmt.append((0, 0, 8, 'titulo'))
    grid.append(['Confrontos de 2º turno para governador, com brancos, nulos e indecisos. '
                 f'Valores em %. {atualizado(hoje)}'])
    fmt.append((1, 0, 8, 'sub'))
    grid.append(['Sem pesquisa nova do confronto, a série repete o último valor: veja a data da última pesquisa '
                 'no título de cada bloco. * Candidatura indeferida pelo TSE, com recurso.'])
    fmt.append((2, 0, 8, 'sub'))
    grid.append([])
    largura_max = 0
    ordem_ufs = sorted(NOMES_UF, key=lambda k: sem_acento(NOMES_UF[k]).lower())
    for uf in ordem_ufs:
        for disputa in sorted(bi[bi.uf == uf].disputa.unique()):
            u = bi[(bi.uf == uf) & (bi.disputa == disputa)]
            fim = u.d.max()
            if fim < hoje.normalize() - pd.Timedelta(days=60):
                continue
            vivos = u[u.d == fim].sort_values('h', ascending=False).candidato_partido.tolist()
            if len(vivos) != 2:
                log(f'  {uf} {disputa}: {len(vivos)} candidatos na última data, fica de fora')
                continue
            n = nv[(nv.uf == uf) & (nv.disputa == disputa)]
            colunas = vivos + ([NV] if not n.empty else [])
            series = {cp: u[u.candidato_partido == cp] for cp in vivos}
            if not n.empty:
                series[NV] = n
            ncol = 1 + len(colunas)
            foto_col = ncol + 1
            largura_max = max(largura_max, foto_col + 2)
            r0 = len(grid)
            linha = [''] * (foto_col + 2)
            curto = ' x '.join(re.sub(r'\s*\([^)]*\)\s*\*?$', '', c) for c in vivos)
            linha[0] = f'{NOMES_UF[uf]} ({uf}) - {curto} - última pesquisa em {fim:%d/%m}'
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
            datas_cel.append((r0 + 2, r0 + 2 + len(datas)))
            series_cel.append((r0 + 2, r0 + 2 + len(datas), 1, ncol))
            grid.append([])
            log(f'  {uf} {disputa}: última data {fim:%d/%m}, {atuais[:2]}')
    grid = [row + [''] * (largura_max - len(row)) for row in grid]
    return grid, fmt, largura_max, datas_cel, series_cel


def montar_ultimas_t2(dados, hoje, log=print):
    """Últimas Pesquisas do 2º turno: por confronto, a Média Eixo e as duas pesquisas mais recentes."""
    from compartilhado import pollingdata_scraper as ps
    res = dados['resultados_t2']
    res = so_confrontos_reais(res[(res.cargo == 'governador') & (res.turno == 't2')]).copy()
    res['p'] = num(res.percentual)
    res['data_campo'] = pd.to_datetime(res.data_campo)
    pesq = dados['pesquisas_t2'].drop_duplicates('poll_id').set_index('poll_id')
    bi = dados['resultados_bi_t2']
    bi = so_confrontos_reais(bi[(bi.cargo == 'governador') & (bi.turno == 't2') & (bi.tipo == 'candidato')]).copy()
    bi['h'] = num(bi.media_hibrida_30d)
    bi['nv'] = 100 - num(bi.declarado_hibrido_30d)
    bi['d'] = pd.to_datetime(bi.data_campo)
    bi = bi[bi.h.notna()]
    nomes, _ = nomes_publicados(pd.concat([bi[['uf', 'candidato_partido']],
                                           res[res.tipo == 'candidato'][['uf', 'candidato_partido']]]),
                                dados['base'], log=lambda *_: None)
    nome = lambda uf, cp: nomes.get((uf, cp), cp)
    curto = lambda n: re.sub(r'\s*\([^)]*\)\s*\*?$', '', n)
    f1 = lambda x: f'{x:.1f}'.replace('.', ',')

    linhas = []
    for uf in sorted(NOMES_UF, key=lambda k: sem_acento(NOMES_UF[k]).lower()):
        for disputa in sorted(res[res.uf == uf].disputa.dropna().unique()):
            u_bi = bi[(bi.uf == uf) & (bi.disputa == disputa)]
            if u_bi.empty:
                continue
            fim = u_bi.d.max()
            ult = u_bi[u_bi.d == fim].sort_values('h', ascending=False)
            if len(ult) != 2:
                continue
            a, b = (nome(uf, cp) for cp in ult.candidato_partido)
            ma, mb, mnv = ult.h.iloc[0], ult.h.iloc[1], ult.nv.iloc[0]
            u = res[(res.uf == uf) & (res.disputa == disputa)]
            polls = u.drop_duplicates('poll_id').copy()
            polls['_nota'] = polls.classificacao_instituto.apply(ps.score_instituto)
            polls['_amostra'] = num(polls.poll_id.map(pesq.amostra)).fillna(0)
            polls = polls.sort_values(['data_campo', '_nota', '_amostra'], ascending=[False, False, False]).head(2)
            ps_lin = []
            for _, p in polls.iterrows():
                c = u[(u.scenario_id == p.scenario_id)]
                cand = c[c.tipo == 'candidato']
                val = {nome(uf, cp): v for cp, v in zip(cand.candidato_partido, cand.p)}
                ps_lin.append((f"{p.instituto} ({p.data_campo:%d/%m})", p.registro_tse, val.get(a), val.get(b),
                               c[c.tipo != 'candidato'].p.sum()))
            while len(ps_lin) < 2:
                ps_lin.append(('-', '-', None, None, None))
            lid = [None if x[2] is None or x[3] is None else (a if x[2] >= x[3] else b) for x in ps_lin]
            marg = [None if x[2] is None or x[3] is None else abs(x[2] - x[3]) for x in ps_lin]
            dias = (hoje.normalize() - fim).days
            if lid[1] is not None and lid[0] != lid[1]:
                obs = f'Pesquisas divergem: a mais recente aponta {curto(lid[0])}, a anterior {curto(lid[1])}'
            elif lid[0] is not None and lid[0] != a:
                obs = f'Pesquisa mais recente aponta {curto(lid[0])}, a Média Eixo {curto(a)}'
            elif all(m is not None and m <= 3 for m in marg):
                obs = 'Margem estreita nas duas pesquisas'
            else:
                obs = f'{curto(a)} à frente nas duas pesquisas e na Média Eixo'
            if dias > 21:
                obs += f'. Última pesquisa há {dias} dias'
            fmtp = lambda v: '-' if v is None else f1(v) + '%'
            linhas.append([f'{NOMES_UF[uf]} ({uf})', f'{curto(a)} x {curto(b)}',
                           a, f1(ma) + '%', b, f1(mb) + '%', f1(ma - mb) + ' p.p.', f1(mnv) + '%', f'{fim:%d/%m}']
                          + [x for pl in ps_lin for x in (pl[0], pl[1], fmtp(pl[2]), fmtp(pl[3]), fmtp(pl[4]))]
                          + [obs])
    N = len(linhas[0]) if linhas else 20
    grid = [[f'ÚLTIMAS PESQUISAS 2º TURNO - COMPARATIVO ENTRE LEVANTAMENTOS RECENTES ({MESES[hoje.month].upper()}/{hoje.year})'],
            ['Por confronto de 2º turno para governador: a Média Ponderada Eixo e as duas pesquisas mais recentes registradas no TSE. '
             + atualizado(hoje)],
            ['Os percentuais das pesquisas estão na ordem da Média (candidato A e candidato B). Brancos/Nulos inclui indecisos. '
             '* Candidatura com recurso.'],
            [],
            ['ESTADO', 'CONFRONTO', 'MÉDIA PONDERADA (EIXO)', '', '', '', '', '', '', '1ª PESQUISA (MAIS RECENTE)', '', '', '', '',
             '2ª PESQUISA (ANTERIOR)', '', '', '', '', 'OBSERVAÇÕES'],
            ['Estado', 'Confronto', 'Candidato A', '%', 'Candidato B', '%', 'Margem', 'Brancos/Nulos', 'Última pesquisa',
             'Instituto (Data)', 'Registro TSE', 'A', 'B', 'Brancos/Nulos',
             'Instituto (Data)', 'Registro TSE', 'A', 'B', 'Brancos/Nulos', 'Observações']] + linhas
    grid = [r + [''] * (N - len(r)) for r in grid]
    log(f'  {len(linhas)} confrontos')
    grupos = [(0, 1), (1, 2), (2, 9), (9, 14), (14, 19), (19, 20)]
    return grid, N, grupos


def gravar_tabela(sh, aba, grid, N, grupos):
    """Tabela simples no padrão visual das outras abas (título marinho, notas em gelo, cabeçalho marinho)."""
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(aba, rows=len(grid) + 20, cols=N)
    sid = ws.id
    ws.clear()
    sh.batch_update({'requests': [{'unmergeCells': {'range': {'sheetId': sid}}}]})
    ws.resize(rows=len(grid) + 20, cols=N)
    ws.update(values=grid, range_name='A1', value_input_option='RAW')
    C = {'marinho': {'red': 0.098, 'green': 0.176, 'blue': 0.306}, 'gelo': {'red': 0.957, 'green': 0.953, 'blue': 0.937},
         'branco': {'red': 1, 'green': 1, 'blue': 1}, 'sub': {'red': 0.463, 'green': 0.463, 'blue': 0.447},
         'preto': {'red': 0, 'green': 0, 'blue': 0}, 'zebra': {'red': 0.98, 'green': 0.98, 'blue': 0.972},
         'borda': {'red': 0.847, 'green': 0.839, 'blue': 0.812}}

    def cel(r0, r1, c0, c1, bg, fg, bold=False, size=10, italic=False, wrap='OVERFLOW_CELL', align='LEFT'):
        return {'repeatCell': {'range': {'sheetId': sid, 'startRowIndex': r0, 'endRowIndex': r1, 'startColumnIndex': c0,
                                         'endColumnIndex': c1},
                               'cell': {'userEnteredFormat': {'backgroundColor': bg, 'wrapStrategy': wrap,
                                                              'verticalAlignment': 'MIDDLE', 'horizontalAlignment': align,
                                                              'textFormat': {'foregroundColor': fg, 'bold': bold,
                                                                             'fontSize': size, 'italic': italic,
                                                                             'fontFamily': 'Montserrat'}}},
                               'fields': 'userEnteredFormat'}}

    def merge(r, c0, c1):
        return {'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': c0,
                                         'endColumnIndex': c1}, 'mergeType': 'MERGE_ALL'}}
    n = len(grid)
    reqs = [cel(0, n + 20, 0, N, C['branco'], C['preto']),
            {'updateBorders': {'range': {'sheetId': sid}, 'top': {'style': 'NONE'}, 'bottom': {'style': 'NONE'},
                               'left': {'style': 'NONE'}, 'right': {'style': 'NONE'},
                               'innerHorizontal': {'style': 'NONE'}, 'innerVertical': {'style': 'NONE'}}},
            merge(0, 0, N), merge(1, 0, N), merge(2, 0, N),
            cel(0, 1, 0, N, C['marinho'], C['branco'], True, 13),
            cel(1, 3, 0, N, C['gelo'], C['sub'], False, 10, True),
            cel(4, 6, 0, N, C['marinho'], C['branco'], True, 10, wrap='WRAP', align='CENTER')]
    reqs += [merge(4, c0, c1) for c0, c1 in grupos if c1 - c0 > 1]
    for i in range(6, n):
        reqs.append(cel(i, i + 1, 0, N, C['zebra'] if i % 2 else C['branco'], C['preto'], wrap='WRAP'))
    reqs.append({'updateBorders': {'range': {'sheetId': sid, 'startRowIndex': 5, 'endRowIndex': n, 'startColumnIndex': 0,
                                             'endColumnIndex': N},
                                   'innerHorizontal': {'style': 'SOLID', 'color': C['borda']},
                                   'bottom': {'style': 'SOLID', 'color': C['borda']}}})
    for c0, c1 in grupos[1:]:
        reqs.append({'updateBorders': {'range': {'sheetId': sid, 'startRowIndex': 4, 'endRowIndex': n,
                                                 'startColumnIndex': c0, 'endColumnIndex': c1},
                                       'left': {'style': 'SOLID', 'color': C['borda']}}})
    reqs += [{'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 1},
                                            'properties': {'pixelSize': 40}, 'fields': 'pixelSize'}},
             {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': 0,
                                                      'endIndex': N},
                                            'properties': {'pixelSize': 120}, 'fields': 'pixelSize'}},
             {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': N - 1,
                                                      'endIndex': N},
                                            'properties': {'pixelSize': 340}, 'fields': 'pixelSize'}}]
    sh.batch_update({'requests': reqs})
    print(f'{aba} gravada: {len(grid)} linhas x {N} colunas')


def padronizar_abas(sh):
    """Mesma cor de aba, 3 linhas congeladas e ordem fixa nas abas geradas."""
    marinho = {'red': 0.098, 'green': 0.176, 'blue': 0.306}
    abas = {w.title: w for w in sh.worksheets()}
    reqs = []
    for i, nome in enumerate(ORDEM_ABAS):
        if nome in abas:
            reqs.append({'updateSheetProperties': {
                'properties': {'sheetId': abas[nome].id, 'index': i, 'tabColorStyle': {'rgbColor': marinho},
                               'gridProperties': {'frozenRowCount': 3}},
                'fields': 'index,tabColorStyle,gridProperties.frozenRowCount'}})
    if reqs:
        sh.batch_update({'requests': reqs})


def gravar_serie_semanal(sh, grid, fmt, largura_max, datas_cel, series_cel, aba='Série Semanal'):
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(aba, rows=len(grid) + 20, cols=largura_max + 1)
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
    print(f'{aba} gravada: {len(grid)} linhas x {largura_max} colunas')


# ---------------------------------------------------------------- main

def main():
    gravar = '--gravar' in sys.argv
    hoje = pd.Timestamp.now(tz='America/Sao_Paulo').tz_localize(None)
    creds = credenciais()
    dados = carregar(creds)
    print(ABA_T2)
    serie_t2 = montar_serie_segundo_turno(dados, hoje)
    print(ABA_ULTIMAS_T2)
    ultimas_t2 = montar_ultimas_t2(dados, hoje)
    if not gravar:
        return
    gc = gspread.authorize(creds)
    gc.timeout = 120
    sh = gc.open_by_key(env(PLANILHA_MEDIAS_ENV))
    gravar_serie_semanal(sh, *serie_t2, aba=ABA_T2)
    gravar_tabela(sh, ABA_ULTIMAS_T2, *ultimas_t2)
    padronizar_abas(sh)


if __name__ == '__main__':
    main()
