"""Aba `Últimas Pesquisas` da planilha das médias (governador, 1º turno).

Compara as duas pesquisas mais recentes de cada UF (cenário principal, como publicado) com
a Média Ponderada Eixo (`media_hibrida_30d`). Chamado por `outros/planilha_medias.py`, que
carrega os dados e filtra a média pelos candidatos registrados no TSE.
"""
import re
import time

import pandas as pd

from compartilhado import pollingdata_scraper as ps


def montar(dados, hoje, log=print):
    from outros.planilha_medias import MESES, casar_com_tse, chave, filtrar_registrados, nomes_publicados

    pesq = dados['pesquisas']
    res = dados['resultados']
    bi = dados['resultados_bi']

    gov_r = res[(res.cargo == 'governador') & (res.turno == 't1')].copy()
    gov_r['percentual'] = pd.to_numeric(gov_r.percentual.astype(str).str.replace(',', '.'), errors='coerce')
    gov_r['data_campo'] = pd.to_datetime(gov_r.data_campo)
    # Nome dos candidatos nas pesquisas com a grafia e o partido do TSE, igual à média.
    # Quem não tem registro ou saiu da disputa fica como a matriz traz (é o que a pesquisa testou).
    publicados, _ = nomes_publicados(gov_r, dados['base'], log=lambda *_: None)
    gov_r['candidato_partido'] = [publicados.get(p, p[1]) for p in zip(gov_r.uf, gov_r.candidato_partido)]
    princ = ps.selecionar_cenario_principal(gov_r)
    princ = ps._anexar_metadados_pesquisa(princ, pesq)

    bi_gov = bi[(bi.cargo == 'governador') & (bi.turno == 't1') & (bi.tipo == 'candidato')].copy()
    bi_gov['d'] = pd.to_datetime(bi_gov.data_campo)
    bi_gov['h'] = pd.to_numeric(bi_gov.media_hibrida_30d.astype(str).str.replace(',', '.'), errors='coerce')
    bi_gov = filtrar_registrados(bi_gov[bi_gov.h.notna()], dados['base'], log=lambda *_: None)

    # Mesma pessoa com grafias diferentes (Alysson e Allyson Bezerra) não é líder diferente.
    pares = set(zip(princ.uf, princ.candidato_partido)) | {
        (uf, cp.removesuffix(' *')) for uf, cp in zip(bi_gov.uf, bi_gov.candidato_partido)}
    tse = casar_com_tse(sorted(pares), dados['base'])

    def pessoa(uf, cp):
        cp = str(cp).removesuffix(' *')
        c = tse.get((uf, cp))
        return c.SQ_CANDIDATO if c is not None else chave(cp)

    def recentes(u):
        # Mais recente pelo fim do campo; no mesmo dia, a de melhor nota no Pindograma e,
        # depois, a de maior amostra.
        u = u.drop_duplicates('poll_id').copy()
        u['_nota'] = u.classificacao_instituto.apply(ps.score_instituto)
        u['_amostra'] = pd.to_numeric(u.amostra, errors='coerce').fillna(0)
        return u.sort_values(['data_campo', '_nota', '_amostra', 'poll_id'], ascending=[False, False, False, True])

    def ufs(n):
        return f'{n} UF' if n == 1 else f'{n} UFs'

    def nome(cp):
        return re.sub(r'\s*\([^)]*\)', '', str(cp)).removesuffix(' *').strip()

    nomes_uf = {'AC': 'Acre', 'AL': 'Alagoas', 'AM': 'Amazonas', 'AP': 'Amapá', 'BA': 'Bahia', 'CE': 'Ceará',
                'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás', 'MA': 'Maranhão', 'MG': 'Minas Gerais',
                'MS': 'Mato Grosso do Sul', 'MT': 'Mato Grosso', 'PA': 'Pará', 'PB': 'Paraíba', 'PE': 'Pernambuco',
                'PI': 'Piauí', 'PR': 'Paraná', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 'RO': 'Rondônia',
                'RR': 'Roraima', 'RS': 'Rio Grande do Sul', 'SC': 'Santa Catarina', 'SE': 'Sergipe', 'SP': 'São Paulo', 'TO': 'Tocantins'}

    linhas_estados = []
    divergentes = []
    altas_disparidades = []
    empates = []
    primeiro_turno = []

    for uf in sorted(nomes_uf.keys()):
        u = princ[princ.uf == uf].copy()
        if u.empty: continue
    
        # Média agregada Eixo
        u_bi = bi_gov[bi_gov.uf == uf]
        fim_bi = u_bi.d.max()
        vivos_bi = u_bi[u_bi.d == fim_bi].sort_values('h', ascending=False)
        lider_bi_nome = vivos_bi.iloc[0]['candidato_partido'] if len(vivos_bi) > 0 else '-'
        lider_bi_pct = f"{vivos_bi.iloc[0]['h']:.1f}%" if len(vivos_bi) > 0 else '-'
        vice_bi_nome = vivos_bi.iloc[1]['candidato_partido'] if len(vivos_bi) > 1 else '-'
        vice_bi_pct = f"{vivos_bi.iloc[1]['h']:.1f}%" if len(vivos_bi) > 1 else '-'
        dif_bi = (vivos_bi.iloc[0]['h'] - vivos_bi.iloc[1]['h']) if len(vivos_bi) > 1 else 0.0
        dif_bi_txt = f"{dif_bi:.1f} p.p."
        lider_nome_bi = vivos_bi.iloc[0]['candidato_partido'].removesuffix(' *') if len(vivos_bi) > 0 else ''
    
        polls_summary = recentes(u)
    
        # 1ª Pesquisa
        p1 = polls_summary.iloc[0]
        p1_rows = gov_r[gov_r.scenario_id == p1['scenario_id']]
        p1_cands = p1_rows[p1_rows.tipo == 'candidato'].sort_values('percentual', ascending=False)
        p1_nv = p1_rows[p1_rows.tipo != 'candidato']['percentual'].sum()
        p1_inst = p1['instituto']
        p1_data = p1['data_campo'].strftime('%d/%m')
        p1_reg = p1['registro_tse']
    
        p1_lider_nome = p1_cands.iloc[0]['candidato_partido'] if len(p1_cands) > 0 else '-'
        p1_lider_pct_val = p1_cands.iloc[0]['percentual'] if len(p1_cands) > 0 else 0.0
        p1_lider_pct = f"{p1_lider_pct_val:.1f}%" if len(p1_cands) > 0 else '-'
    
        p1_vice_nome = p1_cands.iloc[1]['candidato_partido'] if len(p1_cands) > 1 else '-'
        p1_vice_pct_val = p1_cands.iloc[1]['percentual'] if len(p1_cands) > 1 else 0.0
        p1_vice_pct = f"{p1_vice_pct_val:.1f}%" if len(p1_cands) > 1 else '-'
        p1_nv_txt = f"{p1_nv:.1f}%"
        p1_dif = p1_lider_pct_val - p1_vice_pct_val
    
        # 2ª Pesquisa
        if len(polls_summary) > 1:
            p2 = polls_summary.iloc[1]
            p2_rows = gov_r[gov_r.scenario_id == p2['scenario_id']]
            p2_cands = p2_rows[p2_rows.tipo == 'candidato'].sort_values('percentual', ascending=False)
            p2_nv = p2_rows[p2_rows.tipo != 'candidato']['percentual'].sum()
            p2_inst = p2['instituto']
            p2_data = p2['data_campo'].strftime('%d/%m')
            p2_reg = p2['registro_tse']
        
            p2_lider_nome = p2_cands.iloc[0]['candidato_partido'] if len(p2_cands) > 0 else '-'
            p2_lider_pct_val = p2_cands.iloc[0]['percentual'] if len(p2_cands) > 0 else 0.0
            p2_lider_pct = f"{p2_lider_pct_val:.1f}%" if len(p2_cands) > 0 else '-'
        
            p2_vice_nome = p2_cands.iloc[1]['candidato_partido'] if len(p2_cands) > 1 else '-'
            p2_vice_pct_val = p2_cands.iloc[1]['percentual'] if len(p2_cands) > 1 else 0.0
            p2_vice_pct = f"{p2_vice_pct_val:.1f}%" if len(p2_cands) > 1 else '-'
            p2_nv_txt = f"{p2_nv:.1f}%"
            p2_dif = p2_lider_pct_val - p2_vice_pct_val
        else:
            p2_inst, p2_data, p2_reg, p2_lider_nome, p2_lider_pct, p2_vice_nome, p2_vice_pct, p2_nv_txt = '-', '-', '-', '-', '-', '-', '-', '-'
            p2_lider_pct_val, p2_dif = 0.0, 0.0
        
        l1, l2, lm = pessoa(uf, p1_lider_nome), pessoa(uf, p2_lider_nome), pessoa(uf, lider_nome_bi)
        if l1 == l2 or p2_lider_nome == '-':
            delta_lider = p1_lider_pct_val - p2_lider_pct_val
        else:
            delta_lider = p1_lider_pct_val - p2_cands[[pessoa(uf, c) == l1 for c in p2_cands.candidato_partido]]['percentual'].sum()

        n1 = nome(p1_lider_nome) if p1_lider_nome != '-' else ''
        n2 = nome(p2_lider_nome) if p2_lider_nome != '-' else ''
        n_bi = nome(lider_nome_bi) if lider_nome_bi else ''

        if l1 != l2 and p2_lider_nome != '-':
            obs = f"Líderes diferentes entre as pesquisas: {p1_inst} aponta {n1} e {p2_inst} aponta {n2}"
            divergentes.append(f"{uf} ({n1} x {n2})")
        elif l1 != lm and l2 != lm:
            obs = f"Líder recente diferente da Média Eixo: pesquisas apontam {n1}, enquanto a Média aponta {n_bi}"
            divergentes.append(f"{uf} ({n1} x {n_bi} no Eixo)")
        elif abs(delta_lider) >= 6.0:
            obs = f"Variação de {abs(delta_lider):.1f} p.p. no líder entre os institutos"
            altas_disparidades.append(f"{uf} ({delta_lider:+.1f} p.p.)")
        elif p1_dif <= 3.0 and p2_dif <= 3.0:
            obs = "Margem estreita entre os primeiros colocados"
            empates.append(uf)
        elif p1_lider_pct_val >= 50.0 and p2_lider_pct_val >= 50.0:
            obs = "Líder com 50% ou mais nas duas pesquisas"
            primeiro_turno.append(f"{uf} ({n1})")
        elif abs(delta_lider) <= 2.0:
            obs = "Concordância entre os levantamentos"
        else:
            obs = f"Variação de {abs(delta_lider):.1f} p.p. entre os levantamentos"

        linhas_estados.append({
            'uf': uf,
            'estado_label': f"{nomes_uf[uf]} ({uf})",
            'lider_bi_nome': lider_bi_nome, 'lider_bi_pct': lider_bi_pct,
            'vice_bi_nome': vice_bi_nome, 'vice_bi_pct': vice_bi_pct,
            'dif_bi': dif_bi_txt,
            'p1_inst_nome': p1_inst, 'p2_inst_nome': p2_inst,
            'p1_inst': f"{p1_inst} ({p1_data})", 'p1_reg': p1_reg,
            'p1_lider_nome': p1_lider_nome, 'p1_lider_pct': p1_lider_pct,
            'p1_vice_nome': p1_vice_nome, 'p1_vice_pct': p1_vice_pct,
            'p1_nv': p1_nv_txt,
            'p2_inst': f"{p2_inst} ({p2_data})", 'p2_reg': p2_reg,
            'p2_lider_nome': p2_lider_nome, 'p2_lider_pct': p2_lider_pct,
            'p2_vice_nome': p2_vice_nome, 'p2_vice_pct': p2_vice_pct,
            'p2_nv': p2_nv_txt,
            'delta': f"{delta_lider:+.1f} p.p.", 'obs': obs
        })

    # Montar Grid de 22 Colunas (A a V)
    N_COLS = 22
    grid = []

    # Row 0: Title
    grid.append([f'ÚLTIMAS PESQUISAS - COMPARATIVO ENTRE LEVANTAMENTOS RECENTES ({MESES[hoje.month].upper()}/{hoje.year})'] + [''] * (N_COLS - 1))
    # Row 1: Subtitle
    grid.append(['Comparativo direto entre os dois levantamentos mais recentes registrados no TSE e a Média Ponderada Eixo nos 27 estados.'] + [''] * (N_COLS - 1))
    # Row 2: Note
    grid.append(['Amostras estimuladas de 1º turno (cenário principal). Δ Líder = variação percentual do líder na 1ª pesquisa vs 2ª pesquisa. * Candidatura com recurso.'] + [''] * (N_COLS - 1))
    # Row 3: Spacer
    grid.append([''] * N_COLS)

    # Row 4: Cards Header
    grid.append(['PAINEL INFORMATIVO: SÍNTESE DO CENÁRIO ELEITORAL ESTADUAL'] + [''] * (N_COLS - 1))

    # Row 5: Cards Title (4 blocos)
    # Col 0-5 (6 cols), Col 5-11 (6 cols), Col 11-16 (5 cols), Col 16-22 (6 cols) -> total 22
    grid.append([
        f'LÍDERES DIFERENTES ENTRE AS PESQUISAS ({ufs(len(divergentes))})', '', '', '', '',
        f'VARIAÇÃO ACIMA DE 6 p.p. ({ufs(len(altas_disparidades))})', '', '', '', '', '',
        f'MARGEM ESTREITA ({ufs(len(empates))})', '', '', '', '',
        f'LÍDER COM 50% OU MAIS ({ufs(len(primeiro_turno))})', '', '', '', '', ''
    ])

    # Row 6: Cards Detail
    c1_txt = "Líderes diferentes entre as pesquisas: " + ", ".join(divergentes)
    c2_txt = "Variação > 6 p.p. no líder: " + ", ".join(altas_disparidades)
    c3_txt = "Diferença de até 3 p.p. nas duas pesquisas: " + (", ".join(empates) or "nenhuma UF")
    c4_txt = "Líder com 50% ou mais nas duas: " + (", ".join(primeiro_turno) or "nenhuma UF")
    grid.append([
        c1_txt, '', '', '', '',
        c2_txt, '', '', '', '', '',
        c3_txt, '', '', '', '',
        c4_txt, '', '', '', '', ''
    ])

    # Row 7: Spacer
    grid.append([''] * N_COLS)

    # Row 8: Super Headers
    super_hdr = [
        'ESTADO / UF',
        'MÉDIA PONDERADA (EIXO)', '', '', '', '',
        '1ª PESQUISA (MAIS RECENTE)', '', '', '', '', '', '',
        '2ª PESQUISA (ANTERIOR)', '', '', '', '', '', '',
        'QUADRO COMPARATIVO', ''
    ]
    grid.append(super_hdr)

    # Row 9: Column Headers
    col_hdr = [
        'Estado',
        'Líder (Média)', '%', 'Vice (Média)', '%', 'Margem',
        'Instituto (Data)', 'Registro TSE', 'Líder (1ª)', '%', 'Vice (1ª)', '%', 'Brancos/Nulos',
        'Instituto (Data)', 'Registro TSE', 'Líder (2ª)', '%', 'Vice (2ª)', '%', 'Brancos/Nulos',
        'Δ Líder', 'Observações'
    ]
    grid.append(col_hdr)

    # Rows 10 to 36: Data Rows
    start_data_row = len(grid)
    for item in linhas_estados:
        row = [
            item['estado_label'],
            item['lider_bi_nome'], item['lider_bi_pct'], item['vice_bi_nome'], item['vice_bi_pct'], item['dif_bi'],
            item['p1_inst'], item['p1_reg'], item['p1_lider_nome'], item['p1_lider_pct'], item['p1_vice_nome'], item['p1_vice_pct'], item['p1_nv'],
            item['p2_inst'], item['p2_reg'], item['p2_lider_nome'], item['p2_lider_pct'], item['p2_vice_nome'], item['p2_vice_pct'], item['p2_nv'],
            item['delta'], item['obs']
        ]
        grid.append(row)

    end_data_row = len(grid)

    # Spacers and Section 2: Detalhamento
    grid.append([''] * N_COLS)
    grid.append(['DETALHAMENTO: ESTADOS COM LÍDERES DIFERENTES ENTRE AS PESQUISAS'] + [''] * (N_COLS - 1))
    grid.append(['Estados em que as duas pesquisas mais recentes, ou elas e a Média Ponderada Eixo, apontam líderes diferentes.'] + [''] * (N_COLS - 1))
    grid.append([''] * N_COLS)

    def sem_partido(cp):
        return re.sub(r'\s*\([^)]*\)', '', str(cp)).removesuffix(' *').strip()

    # UFs com líder diferente (entre as pesquisas ou delas para a média), na ordem da tabela.
    estados_foco, observacoes_foco = [], {}
    for item in linhas_estados:
        if not ("Líderes diferentes" in item['obs'] or "Líder recente diferente" in item['obs']):
            continue
        uf = item['uf']
        estados_foco.append(uf)
        a, b, m = (sem_partido(item[k]) for k in ('p1_lider_nome', 'p2_lider_nome', 'lider_bi_nome'))
        i1, i2 = item['p1_inst_nome'], item['p2_inst_nome']
        if pessoa(uf, item['p1_lider_nome']) != pessoa(uf, item['p2_lider_nome']):
            txt = f"{i1} indica {a} em 1º lugar, enquanto {i2} aponta {b}."
        else:
            txt = f"{i1} e {i2} apontam {a} em 1º lugar."
        observacoes_foco[uf] = f"Observação: {txt} Na Média Ponderada Eixo, {m} lidera."
    estados_alerta = list(estados_foco)

    detalhe_info_estados = []

    for uf in estados_foco:
        u = princ[princ.uf == uf].copy()
        polls_summary = recentes(u).head(2)
    
        r_hdr = len(grid)
        grid.append([f"{nomes_uf[uf].upper()} ({uf}) - COMPARATIVO DE LEVANTAMENTOS RECENTES"] + [''] * (N_COLS - 1))
    
        r_subhdr = len(grid)
        # Colunas Section 2:
        # 0,1,2 (A:C): Levantamento | 3 (D): Data | 4,5 (E:F): Registro TSE | 6 (G): Amostra
        # 7,8 (H:I): 1º Lugar | 9 (J): % | 10,11 (K:L): 2º Lugar | 12 (M): % | 13,14 (N:O): 3º Lugar | 15 (P): %
        # 16,17 (Q:R): 4º Lugar | 18 (S): % | 19 (T): Brancos/Nulos | 20,21 (U:V): Margem
        grid.append([
            'Levantamento / Métrica', '', '',
            'Data',
            'Registro TSE', '',
            'Amostra',
            '1º Colocado', '', '%',
            '2º Colocado', '', '%',
            '3º Colocado', '', '%',
            '4º Colocado', '', '%',
            'Brancos/Nulos',
            'Margem', ''
        ])
    
        # Linha Média Ponderada
        u_bi = bi_gov[bi_gov.uf == uf]
        fim_bi = u_bi.d.max()
        vivos_bi = u_bi[u_bi.d == fim_bi].sort_values('h', ascending=False)
    
        c1_n = vivos_bi.iloc[0]['candidato_partido'] if len(vivos_bi) > 0 else '-'
        c1_p = f"{vivos_bi.iloc[0]['h']:.1f}%" if len(vivos_bi) > 0 else '-'
        c2_n = vivos_bi.iloc[1]['candidato_partido'] if len(vivos_bi) > 1 else '-'
        c2_p = f"{vivos_bi.iloc[1]['h']:.1f}%" if len(vivos_bi) > 1 else '-'
        c3_n = vivos_bi.iloc[2]['candidato_partido'] if len(vivos_bi) > 2 else '-'
        c3_p = f"{vivos_bi.iloc[2]['h']:.1f}%" if len(vivos_bi) > 2 else '-'
        c4_n = vivos_bi.iloc[3]['candidato_partido'] if len(vivos_bi) > 3 else '-'
        c4_p = f"{vivos_bi.iloc[3]['h']:.1f}%" if len(vivos_bi) > 3 else '-'
    
        dif_media = (vivos_bi.iloc[0]['h'] - vivos_bi.iloc[1]['h']) if len(vivos_bi) > 1 else 0.0
        margem_media_txt = f"+{dif_media:.1f} p.p."
    
        r_media = len(grid)
        grid.append([
            'Média Ponderada (Eixo)', '', '',
            fim_bi.strftime('%d/%m'),
            'Média Eixo', '',
            'Ponderada (30d)',
            c1_n, '', c1_p,
            c2_n, '', c2_p,
            c3_n, '', c3_p,
            c4_n, '', c4_p,
            'Linha agregada',
            margem_media_txt, ''
        ])
    
        # Linhas das Pesquisas
        r_pesqs = []
        for p_idx, (_, p) in enumerate(polls_summary.iterrows()):
            p_rows = gov_r[gov_r.scenario_id == p['scenario_id']]
            p_cands = p_rows[p_rows.tipo == 'candidato'].sort_values('percentual', ascending=False)
            p_nv = p_rows[p_rows.tipo != 'candidato']['percentual'].sum()
        
            pc1_n = p_cands.iloc[0]['candidato_partido'] if len(p_cands) > 0 else '-'
            pc1_p = f"{p_cands.iloc[0]['percentual']:.1f}%" if len(p_cands) > 0 else '-'
            pc2_n = p_cands.iloc[1]['candidato_partido'] if len(p_cands) > 1 else '-'
            pc2_p = f"{p_cands.iloc[1]['percentual']:.1f}%" if len(p_cands) > 1 else '-'
            pc3_n = p_cands.iloc[2]['candidato_partido'] if len(p_cands) > 2 else '-'
            pc3_p = f"{p_cands.iloc[2]['percentual']:.1f}%" if len(p_cands) > 2 else '-'
            pc4_n = p_cands.iloc[3]['candidato_partido'] if len(p_cands) > 3 else '-'
            pc4_p = f"{p_cands.iloc[3]['percentual']:.1f}%" if len(p_cands) > 3 else '-'
        
            dif_p = (p_cands.iloc[0]['percentual'] - p_cands.iloc[1]['percentual']) if len(p_cands) > 1 else 0.0
            margem_p_txt = f"+{dif_p:.1f} p.p."
        
            try:
                a_val = float(p['amostra'])
                amostra_txt = f"{int(a_val):,} ent.".replace(',', '.') if a_val > 0 else '-'
            except:
                amostra_txt = '-'
        
            r_p = len(grid)
            r_pesqs.append(r_p)
            grid.append([
                f"{p_idx+1}ª Pesq: {p['instituto']}", '', '',
                p['data_campo'].strftime('%d/%m'),
                p['registro_tse'], '',
                amostra_txt,
                pc1_n, '', pc1_p,
                pc2_n, '', pc2_p,
                pc3_n, '', pc3_p,
                pc4_n, '', pc4_p,
                f"{p_nv:.1f}%",
                margem_p_txt, ''
            ])
        
        # Linha de Observação
        r_diag = len(grid)
        diag_txt = observacoes_foco[uf]
        grid.append([diag_txt] + [''] * (N_COLS - 1))
    
        # Spacer
        grid.append([''] * N_COLS)
    
        detalhe_info_estados.append({
            'uf': uf, 'r_hdr': r_hdr, 'r_subhdr': r_subhdr, 'r_media': r_media, 'r_pesqs': r_pesqs, 'r_diag': r_diag
        })

    log(f"  {len(linhas_estados)} UFs, {len(estados_foco)} no detalhamento: {', '.join(estados_foco)}")
    # Vírgula decimal nos percentuais e p.p. (a amostra, "1.500 ent.", fica como está).
    grid = [[re.sub(r'(\d)\.(\d)(?=\s?(?:%|p\.p\.))', r'\1,\2', c) if isinstance(c, str) else c for c in row]
            for row in grid]
    return {'grid': grid, 'N_COLS': N_COLS, 'start_data_row': start_data_row, 'end_data_row': end_data_row,
            'linhas_estados': linhas_estados, 'detalhe_info_estados': detalhe_info_estados,
            'estados_alerta': estados_alerta}


def gravar(sh, t):
    grid, N_COLS = t['grid'], t['N_COLS']
    start_data_row, end_data_row = t['start_data_row'], t['end_data_row']
    linhas_estados, detalhe_info_estados = t['linhas_estados'], t['detalhe_info_estados']
    estados_alerta = t['estados_alerta']
    ws = sh.worksheet('Últimas Pesquisas')
    sid = ws.id

    # Limpar e redimensionar
    ws.clear()
    sh.batch_update({'requests': [{'unmergeCells': {'range': {'sheetId': sid}}}]})
    ws.resize(rows=len(grid) + 20, cols=N_COLS)

    C = {
        'marinho': {'red': 0.098, 'green': 0.176, 'blue': 0.306},   # #192d4e
        'vinho': {'red': 0.588, 'green': 0.180, 'blue': 0.302},     # #962e4d
        'gelo': {'red': 0.957, 'green': 0.953, 'blue': 0.937},      # #f4f3ef
        'gelo_escuro': {'red': 0.91, 'green': 0.906, 'blue': 0.882},# #e8e7e1
        'azul_suave': {'red': 0.933, 'green': 0.953, 'blue': 0.973}, # #eef3f8
        'branco': {'red': 1, 'green': 1, 'blue': 1},
        'preto': {'red': 0, 'green': 0, 'blue': 0},
        'sub': {'red': 0.463, 'green': 0.463, 'blue': 0.447},       # #767672
        'zebra': {'red': 0.98, 'green': 0.98, 'blue': 0.972},
        'alerta_bg': {'red': 0.992, 'green': 0.949, 'blue': 0.949}, # #fdf2f2
        'alerta_fg': {'red': 0.588, 'green': 0.180, 'blue': 0.302}, # #962e4d
        'amarelo_bg': {'red': 0.988, 'green': 0.973, 'blue': 0.929},# #fcf8ed
        'amarelo_fg': {'red': 0.541, 'green': 0.345, 'blue': 0.0},  # #8a5800
        'sucesso_bg': {'red': 0.941, 'green': 0.969, 'blue': 0.949},# #f0f7f2
        'sucesso_fg': {'red': 0.106, 'green': 0.369, 'blue': 0.125},# #1b5e20
        'borda': {'red': 0.847, 'green': 0.839, 'blue': 0.812},     # #d8d6cf
        'borda_forte': {'red': 0.627, 'green': 0.620, 'blue': 0.592}# #a09e97
    }

    # Reset básico
    reset_req = [
        {'repeatCell': {
            'range': {'sheetId': sid},
            'cell': {'userEnteredFormat': {
                'backgroundColor': C['branco'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE',
                'textFormat': {'foregroundColor': C['preto'], 'fontSize': 10, 'fontFamily': 'Montserrat', 'bold': False, 'italic': False}
            }},
            'fields': 'userEnteredFormat'
        }},
        {'updateBorders': {
            'range': {'sheetId': sid},
            'top': {'style': 'NONE'}, 'bottom': {'style': 'NONE'}, 'left': {'style': 'NONE'}, 'right': {'style': 'NONE'},
            'innerHorizontal': {'style': 'NONE'}, 'innerVertical': {'style': 'NONE'}
        }}
    ]
    sh.batch_update({'requests': reset_req})

    # Escrever dados como RAW (garante zero erro de fórmula)
    ws.update(values=grid, range_name='A1', value_input_option='RAW')
    
    requests = []

    # ==================== PARTE 1: CABEÇALHOS GERAIS ====================
    # Linhas 0, 1, 2: Título, subtítulo, notas
    for r_idx in [0, 1, 2]:
        requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_idx, 'endRowIndex': r_idx + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS}, 'mergeType': 'MERGE_ALL'}})

    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['marinho'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
            'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 12, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['gelo'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
            'textFormat': {'foregroundColor': C['sub'], 'bold': False, 'italic': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 2, 'endRowIndex': 3, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['gelo'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
            'textFormat': {'foregroundColor': C['sub'], 'bold': False, 'italic': True, 'fontSize': 9, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})

    # Linha 4: Título dos Cards
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 0, 'endColumnIndex': N_COLS}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['vinho'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
            'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 11, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})

    # Linhas 5-6: 4 Cards Executivos
    # 0-5 (A:E), 5-11 (F:K), 11-16 (L:P), 16-22 (Q:V)
    cards_conf = [
        (0, 5, C['alerta_bg'], C['alerta_fg']),
        (5, 11, C['amarelo_bg'], C['amarelo_fg']),
        (11, 16, C['gelo'], C['marinho']),
        (16, 22, C['sucesso_bg'], C['sucesso_fg'])
    ]
    for c_s, c_e, bg, fg in cards_conf:
        # Merge Title
        requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': c_s, 'endColumnIndex': c_e}, 'mergeType': 'MERGE_ALL'}})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': c_s, 'endColumnIndex': c_e},
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg, 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
                'textFormat': {'foregroundColor': fg, 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
            }}, 'fields': 'userEnteredFormat'
        }})
        # Merge Detail
        requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 6, 'endRowIndex': 7, 'startColumnIndex': c_s, 'endColumnIndex': c_e}, 'mergeType': 'MERGE_ALL'}})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': 6, 'endRowIndex': 7, 'startColumnIndex': c_s, 'endColumnIndex': c_e},
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg, 'wrapStrategy': 'WRAP', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
                'textFormat': {'foregroundColor': fg, 'bold': False, 'fontSize': 9, 'fontFamily': 'Montserrat'}
            }}, 'fields': 'userEnteredFormat'
        }})
        # Border
        borda_card = {'style': 'SOLID', 'width': 1, 'color': fg}
        requests.append({'updateBorders': {
            'range': {'sheetId': sid, 'startRowIndex': 5, 'endRowIndex': 7, 'startColumnIndex': c_s, 'endColumnIndex': c_e},
            'top': borda_card, 'bottom': borda_card, 'left': borda_card, 'right': borda_card
        }})

    # ==================== PARTE 2: TABELA GERAL (27 UFs) ====================
    # Super Headers (Row 8)
    # Col 0 (Estado)
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 0, 'endColumnIndex': 1},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['gelo'], 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
            'textFormat': {'foregroundColor': C['marinho'], 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})
    # Media (Cols 1-6)
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 1, 'endColumnIndex': 6}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 1, 'endColumnIndex': 6},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['marinho'], 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
            'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})
    # P1 (Cols 6-13)
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 6, 'endColumnIndex': 13}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 6, 'endColumnIndex': 13},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['vinho'], 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
            'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})
    # P2 (Cols 13-20)
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 13, 'endColumnIndex': 20}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 13, 'endColumnIndex': 20},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['marinho'], 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
            'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})
    # Quadro Comparativo (Cols 20-22)
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 20, 'endColumnIndex': 22}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 20, 'endColumnIndex': 22},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['gelo_escuro'], 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
            'textFormat': {'foregroundColor': C['marinho'], 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})

    # Column Headers (Row 9)
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 9, 'endRowIndex': 10, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['gelo'], 'wrapStrategy': 'WRAP', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
            'textFormat': {'foregroundColor': C['marinho'], 'bold': True, 'fontSize': 9, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})

    # Table Data Formatting (Rows 10 to 36)
    borda_leve = {'style': 'SOLID', 'width': 1, 'color': C['borda']}
    borda_div = {'style': 'SOLID', 'width': 1, 'color': C['borda_forte']}

    for r in range(start_data_row, end_data_row):
        bg_row = C['zebra'] if (r % 2 == 1) else C['branco']
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg_row, 'verticalAlignment': 'MIDDLE',
                'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 9}
            }}, 'fields': 'userEnteredFormat(backgroundColor,verticalAlignment,textFormat)'
        }})

    # Specific column alignments in Table 1
    # Col 0 (Estado): Left bold
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': 0, 'endColumnIndex': 1},
        'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT', 'textFormat': {'bold': True}}},
        'fields': 'userEnteredFormat(horizontalAlignment,textFormat.bold)'
    }})
    # Col 1, 3 (Nomes Média): Left
    for c in [1, 3]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 2, 4 (Percentuais Média): Right
    for c in [2, 4]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'RIGHT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 5 (Margem Eixo): Right
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': 5, 'endColumnIndex': 6},
        'cell': {'userEnteredFormat': {'horizontalAlignment': 'RIGHT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
    }})
    # Col 6, 13 (Instituto/Data): Left
    for c in [6, 13]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 7, 14 (Registro TSE): Center
    for c in [7, 14]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 8, 10, 15, 17 (Nomes Candidatos P1 e P2): Left
    for c in [8, 10, 15, 17]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 9, 11, 16, 18 (Percentuais Candidatos P1 e P2): Right
    for c in [9, 11, 16, 18]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'RIGHT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 12, 19 (Brancos/Nulos): Right
    for c in [12, 19]:
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'RIGHT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    # Col 20 (Delta): Center bold
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': 20, 'endColumnIndex': 21},
        'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER', 'textFormat': {'bold': True}}},
        'fields': 'userEnteredFormat(horizontalAlignment,textFormat.bold)'
    }})
    # Col 21 (Observações): Left
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': start_data_row, 'endRowIndex': end_data_row, 'startColumnIndex': 21, 'endColumnIndex': 22},
        'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
    }})

    # Destacar UFs divergentes e com alta variação na Tabela 1
    for idx, item in enumerate(linhas_estados):
        r_curr = start_data_row + idx
        if "Líderes diferentes" in item['obs'] or "Líder recente diferente" in item['obs']:
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_curr, 'endRowIndex': r_curr + 1, 'startColumnIndex': 20, 'endColumnIndex': 22},
                'cell': {'userEnteredFormat': {
                    'backgroundColor': C['alerta_bg'],
                    'textFormat': {'foregroundColor': C['alerta_fg'], 'bold': True}
                }}, 'fields': 'userEnteredFormat(backgroundColor,textFormat)'
            }})
        elif "Variação de" in item['obs'] and "entre os institutos" in item['obs']:
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_curr, 'endRowIndex': r_curr + 1, 'startColumnIndex': 20, 'endColumnIndex': 21},
                'cell': {'userEnteredFormat': {
                    'backgroundColor': C['amarelo_bg'],
                    'textFormat': {'foregroundColor': C['amarelo_fg'], 'bold': True}
                }}, 'fields': 'userEnteredFormat(backgroundColor,textFormat)'
            }})

    # Bordas na Tabela 1
    requests.append({'updateBorders': {
        'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': end_data_row, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'top': borda_leve, 'bottom': borda_leve, 'left': borda_leve, 'right': borda_leve,
        'innerHorizontal': borda_leve, 'innerVertical': borda_leve
    }})
    # Divisórias verticais de blocos (Cols 6, 13, 20)
    for c_div in [6, 13, 20]:
        requests.append({'updateBorders': {
            'range': {'sheetId': sid, 'startRowIndex': 8, 'endRowIndex': end_data_row, 'startColumnIndex': c_div, 'endColumnIndex': c_div + 1},
            'left': borda_div
        }})

    # ==================== PARTE 3: DETALHAMENTO (SEÇÃO 2) ====================
    # Título da Seção 2
    r_sec2_title = end_data_row + 1
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_sec2_title, 'endRowIndex': r_sec2_title + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': r_sec2_title, 'endRowIndex': r_sec2_title + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['marinho'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
            'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 11, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})

    # Subtítulo da Seção 2
    r_sec2_sub = r_sec2_title + 1
    requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_sec2_sub, 'endRowIndex': r_sec2_sub + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS}, 'mergeType': 'MERGE_ALL'}})
    requests.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': r_sec2_sub, 'endRowIndex': r_sec2_sub + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
        'cell': {'userEnteredFormat': {
            'backgroundColor': C['gelo'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
            'textFormat': {'foregroundColor': C['sub'], 'bold': False, 'italic': True, 'fontSize': 9, 'fontFamily': 'Montserrat'}
        }}, 'fields': 'userEnteredFormat'
    }})

    # Formatar cada Card de Estado na Seção 2
    # Pares de merge da tabela de confronto (22 cols):
    # 0-3 (A:C): Levantamento | 3 (D): Data | 4-6 (E:F): Reg TSE | 6 (G): Amostra
    # 7-9 (H:I): 1º Lugar | 9 (J): % | 10-12 (K:L): 2º Lugar | 12 (M): % | 13-15 (N:O): 3º Lugar | 15 (P): %
    # 16-18 (Q:R): 4º Lugar | 18 (S): % | 19 (T): Brancos/Nulos | 20-22 (U:V): Margem
    subhdr_merges = [
        (0, 3),   # Levantamento (A:C)
        (4, 6),   # Registro TSE (E:F)
        (7, 9),   # 1º Colocado (H:I)
        (10, 12), # 2º Colocado (K:L)
        (13, 15), # 3º Colocado (N:O)
        (16, 18), # 4º Colocado (Q:R)
        (20, 22)  # Margem (U:V)
    ]

    for info in detalhe_info_estados:
        r_h = info['r_hdr']
        r_sh = info['r_subhdr']
        r_m = info['r_media']
        r_ps = info['r_pesqs']
        r_d = info['r_diag']
    
        # 1. State Header
        requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_h, 'endRowIndex': r_h + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS}, 'mergeType': 'MERGE_ALL'}})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_h, 'endRowIndex': r_h + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
            'cell': {'userEnteredFormat': {
                'backgroundColor': C['marinho'], 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
                'textFormat': {'foregroundColor': C['branco'], 'bold': True, 'fontSize': 10, 'fontFamily': 'Montserrat'}
            }}, 'fields': 'userEnteredFormat'
        }})
    
        # 2. Subheader
        for c_s, c_e in subhdr_merges:
            requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_sh, 'endRowIndex': r_sh + 1, 'startColumnIndex': c_s, 'endColumnIndex': c_e}, 'mergeType': 'MERGE_ALL'}})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_sh, 'endRowIndex': r_sh + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
            'cell': {'userEnteredFormat': {
                'backgroundColor': C['gelo'], 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'CENTER',
                'textFormat': {'foregroundColor': C['marinho'], 'bold': True, 'fontSize': 9, 'fontFamily': 'Montserrat'}
            }}, 'fields': 'userEnteredFormat'
        }})
        # Alinhamento à esquerda para título do levantamento
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_sh, 'endRowIndex': r_sh + 1, 'startColumnIndex': 0, 'endColumnIndex': 3},
            'cell': {'userEnteredFormat': {'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
        }})
    
        # 3. Média Ponderada Row
        for c_s, c_e in subhdr_merges:
            requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': c_s, 'endColumnIndex': c_e}, 'mergeType': 'MERGE_ALL'}})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
            'cell': {'userEnteredFormat': {
                'backgroundColor': C['azul_suave'], 'verticalAlignment': 'MIDDLE',
                'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 9, 'foregroundColor': C['marinho']}
            }}, 'fields': 'userEnteredFormat'
        }})
        # Negrito no nome e no líder
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': 0, 'endColumnIndex': 3},
            'cell': {'userEnteredFormat': {'textFormat': {'bold': True}, 'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat(textFormat.bold,horizontalAlignment)'
        }})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': 7, 'endColumnIndex': 10},
            'cell': {'userEnteredFormat': {'textFormat': {'bold': True}}}, 'fields': 'userEnteredFormat.textFormat.bold'
        }})
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': 20, 'endColumnIndex': 22},
            'cell': {'userEnteredFormat': {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat(textFormat.bold,horizontalAlignment)'
        }})
        # Alinhamentos numéricos Média
        for c_pct in [9, 12, 15, 18, 19]:
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': c_pct, 'endColumnIndex': c_pct + 1},
                'cell': {'userEnteredFormat': {'horizontalAlignment': 'RIGHT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
            }})
        for c_ctr in [3, 4, 6]:
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_m, 'endRowIndex': r_m + 1, 'startColumnIndex': c_ctr, 'endColumnIndex': c_ctr + 1},
                'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
            }})
    
        # 4. Pesquisas Rows
        for p_r_idx, r_p in enumerate(r_ps):
            for c_s, c_e in subhdr_merges:
                requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': c_s, 'endColumnIndex': c_e}, 'mergeType': 'MERGE_ALL'}})
            bg_p = C['branco'] if p_r_idx == 0 else C['zebra']
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
                'cell': {'userEnteredFormat': {
                    'backgroundColor': bg_p, 'verticalAlignment': 'MIDDLE',
                    'textFormat': {'fontFamily': 'Montserrat', 'fontSize': 9}
                }}, 'fields': 'userEnteredFormat'
            }})
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': 0, 'endColumnIndex': 3},
                'cell': {'userEnteredFormat': {'textFormat': {'bold': True}, 'horizontalAlignment': 'LEFT'}}, 'fields': 'userEnteredFormat(textFormat.bold,horizontalAlignment)'
            }})
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': 7, 'endColumnIndex': 10},
                'cell': {'userEnteredFormat': {'textFormat': {'bold': True}}}, 'fields': 'userEnteredFormat.textFormat.bold'
            }})
            requests.append({'repeatCell': {
                'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': 20, 'endColumnIndex': 22},
                'cell': {'userEnteredFormat': {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat(textFormat.bold,horizontalAlignment)'
            }})
            # Alinhamentos
            for c_pct in [9, 12, 15, 18, 19]:
                requests.append({'repeatCell': {
                    'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': c_pct, 'endColumnIndex': c_pct + 1},
                    'cell': {'userEnteredFormat': {'horizontalAlignment': 'RIGHT'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
                }})
            for c_ctr in [3, 4, 6]:
                requests.append({'repeatCell': {
                    'range': {'sheetId': sid, 'startRowIndex': r_p, 'endRowIndex': r_p + 1, 'startColumnIndex': c_ctr, 'endColumnIndex': c_ctr + 1},
                    'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat.horizontalAlignment'
                }})

        # 5. Observation Row
        requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r_d, 'endRowIndex': r_d + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS}, 'mergeType': 'MERGE_ALL'}})
        bg_diag = C['alerta_bg'] if (info['uf'] in estados_alerta) else C['gelo']
        fg_diag = C['alerta_fg'] if (info['uf'] in estados_alerta) else C['marinho']
        requests.append({'repeatCell': {
            'range': {'sheetId': sid, 'startRowIndex': r_d, 'endRowIndex': r_d + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
            'cell': {'userEnteredFormat': {
                'backgroundColor': bg_diag, 'wrapStrategy': 'OVERFLOW_CELL', 'verticalAlignment': 'MIDDLE', 'horizontalAlignment': 'LEFT',
                'textFormat': {'foregroundColor': fg_diag, 'bold': False, 'italic': True, 'fontSize': 9, 'fontFamily': 'Montserrat'}
            }}, 'fields': 'userEnteredFormat'
        }})
    
        # 6. Bordas do bloco do estado
        requests.append({'updateBorders': {
            'range': {'sheetId': sid, 'startRowIndex': r_h, 'endRowIndex': r_d + 1, 'startColumnIndex': 0, 'endColumnIndex': N_COLS},
            'top': borda_div, 'bottom': borda_div, 'left': borda_div, 'right': borda_div,
            'innerHorizontal': borda_leve, 'innerVertical': borda_leve
        }})

    # ==================== PARTE 4: DIMENSÕES (LARGURAS E ALTURAS) ====================
    # Column Widths (22 cols)
    col_w = [
        (0, 1, 130),   # A: Estado / UF
        (1, 2, 130),   # B: Líder Média
        (2, 3, 65),    # C: %
        (3, 4, 130),   # D: Vice Média
        (4, 5, 65),    # E: %
        (5, 6, 75),    # F: Margem
        (6, 7, 145),   # G: Instituto 1ª
        (7, 8, 110),   # H: Reg 1ª
        (8, 9, 130),   # I: Líder 1ª
        (9, 10, 65),   # J: %
        (10, 11, 130), # K: Vice 1ª
        (11, 12, 65),  # L: %
        (12, 13, 85),  # M: NV 1ª
        (13, 14, 145), # N: Instituto 2ª
        (14, 15, 110), # O: Reg 2ª
        (15, 16, 130), # P: Líder 2ª
        (16, 17, 65),  # Q: %
        (17, 18, 130), # R: Vice 2ª
        (18, 19, 65),  # S: %
        (19, 20, 85),  # T: NV 2ª
        (20, 21, 80),  # U: Delta
        (21, 22, 320), # V: Observações
    ]
    for c_s, c_e, w in col_w:
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': c_s, 'endIndex': c_e},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'
        }})

    # Row Heights
    dim_rows = [
        (0, 1, 38),   # Title
        (1, 2, 24),   # Subtitle
        (2, 3, 24),   # Note
        (3, 4, 14),   # Spacer
        (4, 5, 26),   # Card Section
        (5, 6, 24),   # Card Title
        (6, 7, 34),   # Card Content
        (7, 8, 14),   # Spacer
        (8, 9, 28),   # Super Header
        (9, 10, 32),  # Column Header
        (start_data_row, end_data_row, 24), # Data rows (27 UFs)
        (end_data_row, end_data_row + 1, 16), # Spacer
        (r_sec2_title, r_sec2_title + 1, 32), # Sec 2 Title
        (r_sec2_sub, r_sec2_sub + 1, 24),     # Sec 2 Subtitle
    ]
    for r_s, r_e, h in dim_rows:
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': r_s, 'endIndex': r_e},
            'properties': {'pixelSize': h}, 'fields': 'pixelSize'
        }})

    for info in detalhe_info_estados:
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': info['r_hdr'], 'endIndex': info['r_hdr'] + 1},
            'properties': {'pixelSize': 28}, 'fields': 'pixelSize'
        }})
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': info['r_subhdr'], 'endIndex': info['r_subhdr'] + 1},
            'properties': {'pixelSize': 24}, 'fields': 'pixelSize'
        }})
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': info['r_media'], 'endIndex': info['r_media'] + 1},
            'properties': {'pixelSize': 24}, 'fields': 'pixelSize'
        }})
        for r_p in info['r_pesqs']:
            requests.append({'updateDimensionProperties': {
                'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': r_p, 'endIndex': r_p + 1},
                'properties': {'pixelSize': 24}, 'fields': 'pixelSize'
            }})
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': info['r_diag'], 'endIndex': info['r_diag'] + 1},
            'properties': {'pixelSize': 26}, 'fields': 'pixelSize'
        }})

    for i in range(0, len(requests), 40):
        batch = requests[i:i+40]
        sh.batch_update({'requests': batch})
        time.sleep(1)

    print(f'Últimas Pesquisas gravada: {len(grid)} linhas x {N_COLS} colunas')
