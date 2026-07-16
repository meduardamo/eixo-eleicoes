"""
APOSENTADO em 16/07/2026 - extração automática de topline (voto estimulado
geral) via PDF+Gemini. Nenhum workflow do GitHub Actions chama este arquivo;
o cadastro de topline agora é 100% manual, via Polling Manual
(gerador-de-envios/pages/5_Polling_Manual.py), que fecha o loop marcando
"Intenção de voto cadastrada?" = sim na fila 'relatorios' quando alguém
salva por lá (marcar_topline_extraida_manual).

Mantido só como referência/pra rodar na mão se um dia fizer sentido reativar
extração automática de topline. Se for reativar de verdade, primeiro
confira se o schema de 'relatorios' (relatorios_sheets_utils.RELATORIOS_COLUNAS)
ainda bate com o que este arquivo espera - ele foi escrito pro schema de
antes da aposentadoria (sem coluna de erro/tentativas de topline).

Uso (manual, sem workflow):
  python relatorios_extracao_topline_aposentado.py topline    # extrai topline dos relatórios
  python relatorios_extracao_topline_aposentado.py publicar   # publica toplines liberadas nas matrizes T1/T2

Secrets: GOOGLE_CREDENTIALS_JSON, GEMINI_API_KEY, SPREADSHEET_ID_RELATORIOS,
SPREADSHEET_ID_POLLINGDATA, SPREADSHEET_ID_POLLINGDATA_T2.
"""

import json
import os
import re
import sys
from datetime import datetime

import gspread

from relatorios_sheets_utils import (
    BRT, CABECALHO_RELATORIOS, CARGOS_MONITORADOS, CARGO_ROTULO, STATUS_TOPLINE_MANUAL,
    _aba, _append_rows_compacto, _ativar_checkbox, _baixar_pdf, _blocos_ativos_cargo,
    _blocos_pdf, _cargo_norm, _cargos_monitorados, _colorir_por_valor, _creds_info,
    _garantir_coluna, _garantir_coluna_relatorios, _int0, _n_paginas_pdf,
    _norm_registro, _registros_tse_texto, _rel_display, _rel_records, _resumo_uso_tokens,
    _sem_acento, _sheets, _sufixo_registro, _ultima_linha_com_dados,
    _ultima_linha_com_registro, _validar_registro_pdf, _verdadeiro,
    link_status_topline_manual,
)

CABECALHOS = {
    "relatorios": CABECALHO_RELATORIOS,
    "topline_pesquisas": ["registro_tse", "ano", "cargo", "uf", "turno", "disputa",
                          "instituto", "classificacao_instituto", "data_campo",
                          "scenario_label", "descricao", "votos_por_entrevistado",
                          "modo", "amostra", "margem_erro",
                          "confianca", "metodologia", "poll_id", "scenario_id", "fonte_url",
                          "fonte_url_original", "horario_raspagem",
                          "validacao", "origem", "publicado", "liberado"],
    "topline_resultados": ["registro_tse", "ano", "cargo", "uf", "turno", "disputa",
                           "instituto", "classificacao_instituto", "data_campo",
                           "scenario_label", "candidato", "partido", "candidato_partido",
                           "tipo", "percentual", "poll_id", "scenario_id", "fonte_url",
                           "horario_raspagem", "origem"],
}


POLLING_PESQUISAS_COLS = [
    "scenario_id", "poll_id", "ano", "uf", "cargo", "turno", "disputa",
    "instituto", "classificacao_instituto", "registro_tse", "data_campo",
    "modo", "amostra", "margem_erro", "confianca", "scenario_label",
    "fonte_url", "fonte_url_original", "horario_raspagem", "metodologia", "origem",
]


POLLING_RESULTADOS_COLS = [
    "scenario_id", "poll_id", "ano", "uf", "cargo", "turno", "disputa",
    "data_campo", "instituto", "classificacao_instituto", "registro_tse",
    "scenario_label", "candidato", "partido", "candidato_partido", "tipo",
    "percentual", "fonte_url", "horario_raspagem", "origem",
]


def _migrar_topline_sem_conferida(ws):
    """Transfere a marca legada para `origem` e remove `conferida` do staging.

    A exclusão usa a API estrutural do Sheets, preservando as validações e a
    formatação das demais colunas, inclusive o checkbox `liberado`.
    """
    valores = ws.get_all_values()
    if not valores or "conferida" not in valores[0]:
        return

    header = valores[0]
    if "origem" not in header:
        _garantir_coluna(ws, header, "origem")
        valores = ws.get_all_values()
        header = valores[0]

    i_conferida = header.index("conferida")
    i_origem = header.index("origem")
    atualizacoes = []
    for linha, row in enumerate(valores[1:], start=2):
        origem = row[i_origem].strip() if len(row) > i_origem else ""
        legado = row[i_conferida].strip().lower() if len(row) > i_conferida else ""
        if origem:
            continue
        if "manual" in legado:
            origem = "polling_manual"
        elif legado:
            origem = "PDF (relatório do instituto)"
        else:
            # Esta aba recebe exclusivamente a extração de relatórios.
            origem = "PDF (relatório do instituto)"
        atualizacoes.append(gspread.Cell(linha, i_origem + 1, origem))

    if atualizacoes:
        ws.update_cells(atualizacoes, value_input_option="RAW")

    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": i_conferida,
                        "endIndex": i_conferida + 1,
                    }
                }
            }]
        })
        print(f"[migracao] {ws.title}: coluna 'conferida' removida")
    except Exception as e:
        print(f"[AVISO] não foi possível remover 'conferida' de {ws.title}: {e}")


def _scenario_ids_na_aba(ws):
    """Retorna os scenario_id já existentes sem depender de ordem ou formato da aba."""
    header = ws.row_values(1)
    if "scenario_id" not in header:
        return set()
    i_scenario = header.index("scenario_id")
    vals = ws.get_all_values()
    return {
        row[i_scenario].strip()
        for row in vals[1:]
        if len(row) > i_scenario and row[i_scenario].strip()
    }


def _texto_chave_publicacao(valor):
    """Chave estável para comparar staging e legado sem mudar seus valores."""
    return re.sub(r"\s+", " ", str(valor or "").strip()).casefold()


def _chave_legado_publicacao(linha):
    """Chave semântica de cenário usada apenas como rede de segurança do legado.

    ``scenario_id`` e ``poll_id`` são as travas primárias. O histórico contém
    alguns ids produzidos por versões antigas; por isso, para t1 com mesmo
    Registro TSE + cargo + turno + cenário também tratamos como já publicado.
    Em t2 a disputa é obrigatória nessa chave, para nunca confundir confrontos
    hipotéticos diferentes do mesmo relatório.
    """
    registro = _texto_chave_publicacao(linha.get("registro_tse", ""))
    cargo = _texto_chave_publicacao(linha.get("cargo", ""))
    turno = _texto_chave_publicacao(linha.get("turno", ""))
    cenario = _texto_chave_publicacao(linha.get("scenario_label", ""))
    disputa = _texto_chave_publicacao(linha.get("disputa", ""))
    if not all((registro, cargo, turno, cenario)):
        return None
    if turno == "t2":
        return (registro, cargo, turno, cenario, disputa) if disputa else None
    return (registro, cargo, turno, cenario, "")


def _indice_publicacao_destino(ws):
    """Indexa as chaves já existentes de uma aba ``pesquisas`` de destino."""
    header = ws.row_values(1)
    valores = ws.get_all_values()
    registros = []
    for row in valores[1:]:
        registros.append({c: row[i] if i < len(row) else "" for i, c in enumerate(header)})
    return {
        "scenario_id": {
            _texto_chave_publicacao(r.get("scenario_id")) for r in registros
            if _texto_chave_publicacao(r.get("scenario_id"))
        },
        "poll_id": {
            _texto_chave_publicacao(r.get("poll_id")) for r in registros
            if _texto_chave_publicacao(r.get("poll_id"))
        },
        "legado": {k for r in registros if (k := _chave_legado_publicacao(r))},
    }


def _motivo_colisao_publicacao(linha, indice_destino):
    """Retorna a trava que bloqueia um cenário já existente, ou vazio."""
    scenario_id = _texto_chave_publicacao(linha.get("scenario_id", ""))
    if scenario_id and scenario_id in indice_destino["scenario_id"]:
        return "scenario_id já existe no destino"
    poll_id = _texto_chave_publicacao(linha.get("poll_id", ""))
    if poll_id and poll_id in indice_destino["poll_id"]:
        return "poll_id já existe no destino"
    chave_legado = _chave_legado_publicacao(linha)
    if chave_legado and chave_legado in indice_destino["legado"]:
        return "cenário legado equivalente já existe no destino"
    return ""


def _marcar_origem(ws, label, scenario_ids):
    """Marca somente as linhas recém-inseridas nesta publicação.

    O recorte por ``scenario_id`` impede que uma publicação de PDF reescreva a
    origem de linhas já existentes, em especial as salvas pelo Polling Manual.
    """
    ids = {str(s).strip() for s in scenario_ids if str(s).strip()}
    if not ids:
        return
    header = ws.row_values(1)
    if "scenario_id" not in header:
        raise ValueError("Aba de destino sem coluna scenario_id.")
    col_o = _garantir_coluna(ws, header, "origem")
    i_scenario, i_o = header.index("scenario_id"), col_o - 1
    vals = ws.get_all_values()
    updates = []
    for linha, row in enumerate(vals[1:], start=2):
        scenario_id = row[i_scenario].strip() if len(row) > i_scenario else ""
        atual = row[i_o] if len(row) > i_o else ""
        if scenario_id in ids and atual != label:
            updates.append(gspread.Cell(linha, col_o, label))
    if updates:
        ws.update_cells(updates, value_input_option="RAW")


POLLING_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA", "")


FLAG_TOPLINE = "topline_extraido"


CARGOS_POLLING = {"presidente", "governador", "senador"}


def _cargos_da_linha(valor):
    return [_cargo_norm(c) for c in _cargos_monitorados(valor)]


def _cargos_presentes(texto, cargo_fila):
    """Cargos a extrair.

    Com a fila separada por registro+cargo, respeita o cargo da linha. Só detecta
    pelo texto quando a célula de cargo estiver vazia.
    """
    cargos_linha = _cargos_da_linha(cargo_fila)
    if cargos_linha:
        return cargos_linha
    cargos = set()
    low = (texto or "").lower()
    if not low:
        return ["presidente", "governador", "senador"]
    if "presiden" in low:
        cargos.add("presidente")
    if "governador" in low or "governo do estado" in low:
        cargos.add("governador")
    if "senador" in low:
        cargos.add("senador")
    return [c for c in ("presidente", "governador", "senador") if c in cargos]


def _extrair_topline_pdf(pdf, texto, link, escopo, cargo):
    """Extrai o topline de um cargo/turno. PDF pequeno: manda inteiro (comportamento
    de sempre). PDF grande: lê em blocos de 5 páginas e junta os cenários, senão o
    modelo perde o cargo que está no fim do PDF (ex.: VOX senador na pág 49, Meio/Ideia
    92 páginas). Pré-filtra blocos pelo texto do cabeçalho pra economizar chamada."""
    from relatorios_topline_core import classificar_tipo_resultado, extrair_dados_polling_gemini
    if _n_paginas_pdf(pdf) <= 10:
        return extrair_dados_polling_gemini(texto, url_original=link, escopo=escopo, pdf_bytes=pdf)

    def _mapa_candidatos(c):
        """dict candidato_normalizado -> percentual, só candidatos de verdade com número.
        'Não válido' fica fora: é o item mais instável entre duas leituras do mesmo
        cenário (some, muda de posição), e incluí-lo faria comparações de conteúdo
        falharem em cenários claramente idênticos nos candidatos."""
        mapa = {}
        for it in (c.get("itens") or []):
            if classificar_tipo_resultado(str(it.get("candidato", "")), it.get("tipo", "")) == "nao_valido":
                continue
            if it.get("percentual") is None:
                continue
            mapa[str(it.get("candidato", "")).strip().lower()] = it.get("percentual")
        return mapa

    payload_final, cenarios, vistos = None, [], set()
    mapas_aceitos = []   # paralelo a 'cenarios': mapa candidato->pct de cada cenário aceito
    blocos_falhos = 0
    for bloco, txt_bloco in _blocos_ativos_cargo(pdf, cargo):
        try:
            p = extrair_dados_polling_gemini(txt_bloco, url_original=link, escopo=escopo, pdf_bytes=bloco)
        except Exception as e:
            # falha de UM bloco não pode ser invisível: os cenários daquelas páginas
            # somem e a linha ainda seria marcada como sucesso (ex.: BR-05628 perdeu
            # os confrontos Lula x Renan e Lula x Joaquim das págs. 42/44 sem nenhum
            # rastro no log). Conta e propaga pro aviso da linha.
            blocos_falhos += 1
            print(f"    [bloco falhou] {e}", flush=True)
            continue
        for c in (p.get("cenarios") or []):
            mapa = _mapa_candidatos(c)
            # cenário sem nenhum candidato com percentual numérico é lixo: ou é o
            # placeholder vazio que normalizar_payload_polling injeta quando um bloco
            # não tem dado, ou é gráfico de que o modelo só leu os nomes (sem números).
            # Se passasse, viraria linha órfã em topline_pesquisas sem resultado nenhum.
            if not mapa:
                continue
            chave = f"{c.get('scenario_label','')}|{c.get('descricao','')}|{c.get('disputa','')}"
            # blocos diferentes às vezes capturam o MESMO cenário do PDF com rótulo
            # diferente ("1º CENÁRIO - COM FULANO" num bloco, "Cenário 1 (com Fulano)"
            # no seguinte), e páginas de síntese/destaque repetem só os líderes do
            # cenário (ex.: capa de capítulo "36% × 36%"). Dedup em dois níveis:
            # conteúdo idêntico (fingerprint) e SUBCONJUNTO de um cenário já aceito
            # (mesmos candidatos com os mesmos números, só que menos candidatos =
            # fragmento/resumo do mesmo cenário, não um cenário novo).
            fingerprint = tuple(sorted(mapa.items())) if len(mapa) >= 2 else ()
            if chave in vistos or (fingerprint and fingerprint in vistos):
                continue
            eh_subconjunto = any(
                mapa.keys() <= m.keys() and all(m[k] == v for k, v in mapa.items())
                for m in mapas_aceitos)
            if eh_subconjunto:
                continue
            # caso inverso: o cenário NOVO é a versão completa de um fragmento aceito
            # antes (o resumo veio num bloco anterior à tabela cheia). Substitui.
            for idx, m in enumerate(mapas_aceitos):
                if m.keys() <= mapa.keys() and all(mapa[k] == v for k, v in m.items()):
                    cenarios[idx] = c
                    mapas_aceitos[idx] = mapa
                    break
            else:
                cenarios.append(c)
                mapas_aceitos.append(mapa)
            vistos.add(chave)
            if fingerprint:
                vistos.add(fingerprint)
            if payload_final is None:
                payload_final = p   # metadados (uf, instituto, data) do 1º bloco com dado
    if payload_final is None:
        return {"cenarios": [], "blocos_falhos": blocos_falhos}
    payload_final["cenarios"] = cenarios
    payload_final["blocos_falhos"] = blocos_falhos
    return payload_final


def cmd_topline():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    import pandas as pd
    from relatorios_topline_core import (
        USO_TOKENS as USO_TOKENS_TOPLINE, extrair_dados_polling_gemini,
        extrair_texto_pdf_bytes, montar_dataframes_polling, ficha_instituto,
        resolver_data_campo_deterministica,
    )

    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = _aba(sh, "relatorios", CABECALHOS, manutencao=False)

    header = fila.row_values(1)
    col_flag = _garantir_coluna_relatorios(fila, header, FLAG_TOPLINE)
    col_data = _garantir_coluna_relatorios(fila, header, "topline_data_extracao")
    col_erro = _garantir_coluna_relatorios(fila, header, "topline_erro")
    col_tent = _garantir_coluna_relatorios(fila, header, "topline_tentativas")
    _colorir_por_valor(fila, _rel_display("topline_extraido"), header, _ultima_linha_com_registro(fila), {
        "sim": (0.82, 0.93, 0.82),
        STATUS_TOPLINE_MANUAL: (1.0, 0.82, 0.68),
    })

    linhas = _rel_records(fila)
    todos_p, todos_r = [], []
    updates = []
    ok_regs, err_regs = [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")
    print(f"Topline: {len(linhas)} linha(s) na fila.", flush=True)

    LOTE = 5
    _contador = {"n": 0}

    def _gravar(dfs, aba_nome, chaves):
        if not dfs:
            return
        cols = CABECALHOS[aba_nome]
        df = pd.concat(dfs, ignore_index=True).reindex(columns=cols).fillna("")
        # dedup DENTRO do próprio lote: um PDF grande, lido em blocos, pode devolver o
        # mesmo cenário (mesmo scenario_id) mais de uma vez vindo de blocos diferentes
        # (ex.: t2 do mesmo confronto aparece em duas seções do relatório). Sem isso, os
        # dois vão pra planilha e a soma do cenário dobra (ex.: ~200% em vez de ~100%).
        antes = len(df)
        df = df.drop_duplicates(subset=chaves, keep="first")
        if len(df) < antes:
            print(f"  [dedup {aba_nome} intra-lote] {antes - len(df)} linha(s) duplicada(s) no mesmo lote")
        ws = _aba(sh, aba_nome, CABECALHOS)
        # dedup contra o que já está na aba (protege reprocessamento com staging não apagado)
        existentes = {tuple(str(e.get(c, "")).strip() for c in chaves)
                      for e in ws.get_all_records()}
        if existentes:
            mask = df.apply(lambda rw: tuple(str(rw[c]).strip() for c in chaves) not in existentes,
                            axis=1)
            if (~mask).any():
                print(f"  [dedup {aba_nome}] {int((~mask).sum())} linha(s) já existiam")
            df = df[mask]
        if df.empty:
            return
        _append_rows_compacto(ws, df.astype(str).values.tolist())
        if aba_nome == "topline_pesquisas":
            _ativar_checkbox(ws, "liberado", cols, _ultima_linha_com_dados(ws))
        print(f"{len(df)} linha(s) gravadas na aba '{aba_nome}'.")

    # Grava em lotes: se o passo estourar o tempo (timeout do Actions), o que já foi
    # extraído fica salvo e a marcação segue junto, então a próxima rodada continua de
    # onde parou em vez de perder tudo. Marca a linha DEPOIS de gravar os cenários dela.
    def _flush_topline(ctx=""):
        _gravar(todos_p, "topline_pesquisas", ["scenario_id"])
        _gravar(todos_r, "topline_resultados", ["scenario_id", "candidato_partido", "tipo"])
        if updates:
            # O aviso de lançamento manual é uma fórmula HYPERLINK; USER_ENTERED
            # preserva o link sem mudar os demais textos e datas do lote.
            fila.update_cells(updates, value_input_option="USER_ENTERED")
        todos_p.clear(); todos_r.clear(); updates.clear()
        _contador["n"] = 0

    def _falha(row_i, registro, tentativas, msg):
        updates.extend([gspread.Cell(row_i, col_erro, msg[:300]),
                        gspread.Cell(row_i, col_tent, tentativas + 1)])
        err_regs.append(f"{registro} [{msg[:80]}]")
        print(f"linha {row_i} ({registro}): erro: {msg}")
        _contador["n"] += 1

    for i, r in enumerate(linhas, start=2):
        if _contador["n"] >= LOTE:
            _flush_topline("parcial")
        link = str(r.get("link", "")).strip()
        registro_fila = str(r.get("registro", "")).strip()
        if (not link or not _verdadeiro(r.get("conferido"))
                or _verdadeiro(r.get(FLAG_TOPLINE))
                or str(r.get(FLAG_TOPLINE, "")).strip() == STATUS_TOPLINE_MANUAL):
            continue
        # Topline automático só p/ RELATÓRIO de instituto COM FICHA. Notícia vai
        # para o Polling Manual; N/A/em branco significa que não foi localizado
        # relatório e não entra em nenhuma fila. Segmento/rejeição/aprovação NÃO
        # têm gate (rodam em qualquer PDF conferido, no cmd_extrair).
        tipo = _sem_acento(r.get("tipo_fonte")).strip().lower()
        if not tipo.startswith("relat"):
            if tipo.startswith("not"):
                # Notícia conferida: há números disponíveis, mas sem a ficha de
                # relatório necessária para extração automática confiável.
                updates.extend([
                    gspread.Cell(i, col_flag, link_status_topline_manual()),
                    gspread.Cell(i, col_erro, ""),
                ])
            continue
        if not ficha_instituto(r.get("instituto", "")).strip():
            # RELATÓRIO de instituto SEM ficha: topline não automatiza. Sinaliza UMA vez
            # diretamente em 'Topline extraída?', para a ação manual ficar visível.
            updates.extend([
                gspread.Cell(i, col_flag, link_status_topline_manual()),
                gspread.Cell(i, col_erro, ""),
            ])
            continue
        tentativas = _int0(r.get("topline_tentativas"))
        if tentativas >= 3:   # desiste após 3 falhas; limpe a coluna pra tentar de novo
            continue
        try:
            print(f"linha {i} ({registro_fila} / {r.get('cargo')}): baixando PDF para topline...", flush=True)
            pdf = _baixar_pdf(link)
            texto = extrair_texto_pdf_bytes(pdf)
        except Exception as e:
            _falha(i, registro_fila, tentativas, f"baixar/ler PDF: {e}")
            continue
        registros_pdf = _registros_tse_texto(texto)
        registro_fila_norm = _norm_registro(registro_fila)
        sufixo_fila = _sufixo_registro(registro_fila_norm)
        if (registros_pdf and registro_fila_norm not in registros_pdf
                and not any(_sufixo_registro(r) == sufixo_fila for r in registros_pdf)):
            regs = ", ".join(sorted(registros_pdf))
            _falha(i, registro_fila, tentativas,
                   f"registro da fila não aparece no PDF; registros encontrados: {regs}")
            continue
        if len(texto) < 200:   # scan/sem camada de texto: manda o PDF pro Gemini (visão)
            print(f"linha {i} ({registro_fila}): PDF sem texto útil, usando visão")
            texto = ""
        cargos = _cargos_presentes(texto, r.get("cargo"))
        if not cargos:
            _falha(i, registro_fila, tentativas, "cargo da linha vazio ou não monitorado")
            continue
        print(f"linha {i} ({registro_fila} / {r.get('cargo')}): texto={len(texto)} caracteres; cargos={', '.join(cargos)}", flush=True)

        n_cen, avisos, houve_erro = 0, [], False
        linha_p, linha_r = [], []

        def _aviso(msg):
            if msg not in avisos:
                avisos.append(msg)

        def _norm_reg(s):
            import re as _re
            return _re.sub(r"[^A-Z0-9]", "", str(s).upper())

        # um mesmo PDF costuma ter 1º e 2º turno; extrai cada turno separado,
        # senão o Gemini fixa um turno só no payload e descarta o outro. Senador NUNCA
        # tem 2º turno no Brasil: nem pergunta pelo t2 (mesmo que o relatório traga uma
        # "simulação de 2º turno para Senador", isso não é um resultado t2 publicável).
        for cargo in cargos:
            turnos_cargo = ("t1",) if cargo == "senador" else ("t1", "t2")
            for turno in turnos_cargo:
                try:
                    escopo = {"cargo": cargo, "turno": turno, "instituto": r.get("instituto", ""),
                              "registro_tse": registro_fila}
                    if cargo != "presidente":   # governador/senador são estaduais: restrição obrigatória
                        escopo["uf"] = r.get("uf", "")
                    else:   # presidente pode ser nacional ou lido dentro do estado da fila: só referência
                        escopo["uf_referencia"] = r.get("uf", "")
                    print(f"linha {i} ({registro_fila} / {r.get('cargo')}): extraindo topline {cargo}/{turno}...", flush=True)
                    payload = _extrair_topline_pdf(pdf, texto, link, escopo, cargo)
                    n_falhos = payload.get("blocos_falhos") or 0
                    if n_falhos:
                        houve_erro = True
                        _aviso(f"{cargo}/{turno}: {n_falhos} bloco(s) do PDF falharam na "
                               "leitura; cenários dessas páginas podem estar faltando")
                    payload["turno"] = turno   # garante o turno pedido no rótulo/poll_id
                    # registro da fila é a fonte da verdade; só avisa se o registro da
                    # fila NÃO estiver entre os do PDF (compara sem hífen/pontuação;
                    # relatórios grafam BA04848 e podem trazer mais de um registro)
                    reg_pdf = str(payload.get("registro_tse", "")).strip()
                    if (not registros_pdf and reg_pdf and registro_fila and
                            _norm_reg(registro_fila) not in _norm_reg(reg_pdf)):
                        _aviso(f"registro no PDF ({reg_pdf}) difere da fila")
                    payload["registro_tse"] = registro_fila or reg_pdf
                    # A data do Gemini nunca é gravada sem validação. Quando o PDF
                    # declara o período de campo, o último dia desse período prevalece;
                    # sem período, a divulgação vira fallback. Datas futuras ou
                    # posteriores à divulgação retêm o cenário em vez de deslocar a série.
                    data_campo, aviso_data = resolver_data_campo_deterministica(
                        payload.get("data_campo", ""),
                        texto,
                        r.get("data_divulgacao", ""),
                        data_referencia=datetime.now(BRT).date(),
                    )
                    if not data_campo:
                        raise ValueError(aviso_data or "data_campo inválida")
                    payload["data_campo"] = data_campo
                    if aviso_data:
                        _aviso(f"{cargo}/{turno}: {aviso_data}")
                    df_p, df_r = montar_dataframes_polling(
                        payload, fonte_url=link, instituto_fonte=r.get("instituto", ""))
                    # dedup ANTES de qualquer checagem de soma: o dedup de _gravar (mesmas
                    # chaves) só roda no flush do lote, no fim do processamento da linha, e
                    # até lá o mesmo cenário real pode ter entrado mais de uma vez em df_r
                    # (blocos diferentes do PDF capturando o mesmo confronto sem que o dedup
                    # de _extrair_topline_pdf pegasse - rótulo/precisão do número variando
                    # entre leituras). Sem isso, a soma calculada aqui conta as duplicatas e
                    # gera "conferir: soma 300%/600%" pra dado que, já deduplicado, soma 100%
                    # certinho (achado no BR-05628/2026, Boas Ideias, presidente/t2: 13
                    # cenário(s) extraídos pra só 6 confrontos reais).
                    if not df_r.empty:
                        df_r = df_r.drop_duplicates(
                            subset=["scenario_id", "candidato_partido", "tipo"], keep="first")
                    if not df_p.empty:
                        df_p = df_p.drop_duplicates(subset=["scenario_id"], keep="first")
                except Exception as e:
                    print(f"linha {i} ({registro_fila}) [{cargo}/{turno}]: erro na extração: {e}")
                    houve_erro = True
                    # falha parcial não pode ficar invisível: registra no aviso da linha
                    _aviso(f"extração falhou em {cargo}/{turno}")
                    continue
                # o cargo pedido (escopo) é a fonte da verdade; o Gemini às vezes devolve
                # um cargo EXPLICITAMENTE diferente do pedido no payload, e esse cenário
                # NÃO pertence à linha da fila que estamos processando (ex.: um relatório
                # de presidente devolvendo um cenário rotulado "governador" por engano).
                # Descarta antes de gravar, senão o dado vai pra aba errada.
                # Cargo VAZIO é diferente: o prompt já restringe explicitamente qual cargo
                # extrair (FOCO DA EXTRAÇÃO), então "não rotulou" não é sinal de vazamento,
                # é só um campo que o modelo deixou em branco numa resposta mais fraca
                # (comum em PDF sem texto, extração por visão). Tratar vazio como
                # "diferente" descartaria dado real (MS-06247 Governador, texto=0).
                if not df_p.empty:
                    mask_cargo = (df_p["cargo"] == cargo) | (df_p["cargo"] == "")
                    if (~mask_cargo).any():
                        ids_ruins = set(df_p.loc[~mask_cargo, "scenario_id"])
                        _aviso(f"{cargo}/{turno}: descartado(s) {len(ids_ruins)} cenário(s) "
                               f"com cargo diferente do esperado ({cargo})")
                        df_p = df_p[mask_cargo].copy()
                        # df_r pode vir sem NENHUMA coluna (pd.DataFrame([]) quando o
                        # cenário só tinha o placeholder vazio, sem nenhum item de
                        # resultado) - "scenario_id" nem existe nesse caso, e indexar por
                        # ele estoura KeyError em vez de simplesmente não ter nada pra
                        # filtrar (MS-06247 Governador, PDF sem texto).
                        if "scenario_id" in df_r.columns:
                            df_r = df_r[~df_r["scenario_id"].isin(ids_ruins)].copy()
                    # preenche o cargo assumido (era vazio, aceito pela restrição do
                    # prompt) pra não sobrar célula em branco em topline_pesquisas/resultados
                    df_p.loc[df_p["cargo"] == "", "cargo"] = cargo
                    if "cargo" in df_r.columns:
                        df_r.loc[df_r["cargo"] == "", "cargo"] = cargo
                if df_r.empty:
                    continue
                # linha de cenário sem NENHUM resultado é órfã (placeholder de bloco
                # vazio ou gráfico lido sem números): não grava, senão vira linha morta
                # em topline_pesquisas.
                orfaos = ~df_p["scenario_id"].isin(df_r["scenario_id"])
                if orfaos.any():
                    df_p = df_p[~orfaos]
                # pergunta filtro ("o candidato que você votaria é um desses nomes ou
                # outro candidato?") vazando como cenário: a assinatura é um item de
                # resposta "Outro candidato" (não confundir com "Outros", legítimo).
                # A regra do prompt (filtro só entra se for a única medição do cargo/
                # turno) falha no chunking, porque cada bloco é avaliado isolado e o
                # bloco da pergunta filtro não vê as outras medições; avalia aqui no
                # consolidado (AM-03497 senador, Verità).
                cands_norm = df_r["candidato"].map(lambda s: _sem_acento(str(s)).strip().lower())
                filtro_ids = set(df_r.loc[cands_norm.str.match(r"outros? candidatos?$"), "scenario_id"])
                if filtro_ids and df_p["scenario_id"].nunique() > len(filtro_ids):
                    _aviso(f"{cargo}/{turno}: descartado(s) {len(filtro_ids)} cenário(s) de "
                           "pergunta filtro (resposta 'outro candidato'; há medição formal)")
                    df_p = df_p[~df_p["scenario_id"].isin(filtro_ids)]
                    df_r = df_r[~df_r["scenario_id"].isin(filtro_ids)]
                if df_r.empty:
                    continue
                # cenário de t1 somando muito pouco é fragmento, não cenário: rabo de
                # tabela cortada na fronteira de blocos (ex.: metade de baixo de uma
                # tabela de REJEIÇÃO sem o título, que o modelo interpreta como intenção
                # de voto - RO-07927 governador, soma 13.6). Nenhum cenário legítimo soma
                # menos de 50 nem sem o "Não válido".
                somas_sid = df_r.groupby("scenario_id")["percentual"].sum()
                fragmentos = set(somas_sid[somas_sid < 50].index)
                if fragmentos:
                    _aviso(f"{cargo}/{turno}: descartado(s) {len(fragmentos)} fragmento(s) "
                           f"com soma abaixo de 50% (lista parcial/sem contexto)")
                    df_p = df_p[~df_p["scenario_id"].isin(fragmentos)]
                    df_r = df_r[~df_r["scenario_id"].isin(fragmentos)]
                if df_r.empty:
                    continue
                # votos_por_entrevistado=2 com soma ~100 é marcação errada: o modelo viu
                # "senador de 2 vagas" e marcou voto duplo, mas os números são de uma
                # pergunta de voto único (soma ~100). Corrige pra 1, senão o downstream
                # divide/normaliza errado (SP-01703 Datafolha senador).
                somas_sid = df_r.groupby("scenario_id")["percentual"].sum()
                for sid_v in df_p.loc[df_p["votos_por_entrevistado"].astype(str) == "2", "scenario_id"]:
                    if somas_sid.get(sid_v, 0) < 140:
                        df_p.loc[df_p["scenario_id"] == sid_v, "votos_por_entrevistado"] = 1
                        _aviso(f"{cargo}/{turno}: votos_por_entrevistado corrigido 2->1 "
                               f"(soma ~100, voto único)")
                # sanidade: percentual fora de 0-100 e soma do cenário fora da faixa.
                # Cenário simples deve somar ~100; senador com voto duplo declarado
                # pode somar bem menos que 200. Isso pega mistura de "Porcentagem válida"
                # com "Porcentual" dos inválidos, como 50.3+49.7+23.9.
                if (df_r["percentual"] < 0).any() or (df_r["percentual"] > 100).any():
                    _aviso(f"{cargo}/{turno}: percentual fora de 0-100")
                votos_map = dict(zip(df_p["scenario_id"], df_p["votos_por_entrevistado"]))
                for sid, grupo in df_r.groupby("scenario_id"):
                    soma = grupo["percentual"].sum()
                    votos = int(votos_map.get(sid) or 1)
                    # "poderia citar ATÉ 2 candidatos" não é "sempre cita 2": quem citou só
                    # 1 (ou nenhum) puxa a soma pra baixo de 200% legitimamente. Faixa
                    # 185-215 rejeitava dado real da Paraná Pesquisas que soma 150-180%
                    # (RJ-04259, PR-01166, BA-04848, AL-04491 — confirmado no PDF original,
                    # nota "*Cada entrevistado poderia citar até 2 candidatos"). Mesma faixa
                    # 150-210 já usada em _avisos_soma_voto (segmentos) pro mesmo padrão.
                    minimo, teto = (150, 215) if votos >= 2 else (97, 103)
                    if minimo <= soma <= teto:
                        continue
                    lbl = grupo["scenario_label"].iloc[0]
                    if cargo == "senador" and votos == 1 and 115 < soma <= 215:
                        # soma de voto duplo, mas o relatório não declarou (ou o Gemini
                        # não achou a nota): não silencia, pede conferência
                        _aviso(f"{cargo}/{turno} cenário {lbl}: soma {soma:.1f} "
                               "(possível voto duplo não sinalizado; conferir)")
                    else:
                        _aviso(f"{cargo}/{turno} cenário {lbl}: soma {soma:.1f}")
                df_p["validacao"] = "; ".join(avisos[-3:]) if avisos else ""
                linha_p.append(df_p)
                linha_r.append(df_r)
                n_cen += len(df_p)
                print(f"linha {i} ({registro_fila} / {r.get('cargo')}) [{cargo}/{turno}]: {len(df_p)} cenário(s)", flush=True)

        if n_cen == 0:
            msg = "todas as extrações falharam" if houve_erro else f"nenhum cenário de topline encontrado para {r.get('cargo')}"
            _falha(i, registro_fila, tentativas, msg)
            continue
        nota_validacao = "; ".join(avisos[:3]) if avisos else ""
        todos_p.extend(linha_p)
        todos_r.extend(linha_r)
        updates.extend([gspread.Cell(i, col_flag, "sim"),
                        gspread.Cell(i, col_data, agora),
                        gspread.Cell(i, col_erro, nota_validacao[:300])])
        ok_regs.append(registro_fila)
        aviso_txt = f" | avisos: {'; '.join(avisos[:2])}" if avisos else ""
        print(f"linha {i} ({registro_fila}): {n_cen} cenário(s) de topline{aviso_txt}")
        _contador["n"] += 1

    _flush_topline("fim")

    print("\n───────── resumo ─────────")
    print(f"processados: {len(ok_regs)}  {ok_regs}")
    print(f"com erro:    {len(err_regs)}  {err_regs}")
    _resumo_uso_tokens("topline", USO_TOKENS_TOPLINE)


T1_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA", "")


T2_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA_T2", "")


def cmd_publicar():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    import pandas as pd
    from pollingdata_scraper import (
        classificar_instituto, gs_client_from_env, normalizar_instituto, salvar_tudo,
    )
    from relatorios_topline_core import ORIGEM

    gc = gs_client_from_env()
    sh = gc.open_by_key(RELATORIOS_ID)
    ws_p = _aba(sh, "topline_pesquisas", CABECALHOS)
    ws_r = _aba(sh, "topline_resultados", CABECALHOS)

    df_p = pd.DataFrame(ws_p.get_all_records())
    df_r = pd.DataFrame(ws_r.get_all_records())
    if df_p.empty:
        print("topline_pesquisas vazia; nada a publicar.")
        return

    header_p = ws_p.row_values(1)
    col_pub = _garantir_coluna(ws_p, header_p, "publicado")
    col_validacao = _garantir_coluna(ws_p, header_p, "validacao")
    if "publicado" not in df_p.columns:
        df_p["publicado"] = ""
    # Gate de liberação humana OBRIGATÓRIO: nenhum cenário extraído de PDF segue para
    # as matrizes sem 'liberado=sim' na aba topline_pesquisas, inclusive os cenários
    # sem aviso em 'validacao'. Isso deixa a revisão humana como etapa explícita.
    _garantir_coluna(ws_p, header_p, "liberado")
    if "liberado" not in df_p.columns:
        df_p["liberado"] = ""

    feito = df_p["publicado"].astype(str).str.strip().str.lower().isin(["sim", "true", "1", "x"])
    pendentes = df_p[~feito]
    if pendentes.empty:
        print("Tudo já publicado.")
        return

    def _preparar(df, colunas_destino):
        # Tira somente controles do staging. A origem já vem da extração e segue
        # junto para a matriz; `_marcar_origem` permanece como salvaguarda.
        df = df.drop(columns=[
            "publicado", "liberado", "validacao", "descricao", "votos_por_entrevistado"
        ], errors="ignore").copy()
        if "instituto" in df.columns:
            df["instituto"] = df["instituto"].apply(normalizar_instituto)
            df["classificacao_instituto"] = df["instituto"].apply(classificar_instituto)
        # Envia métricas como números reais para as matrizes. As abas de origem
        # podem exibir vírgula ou ponto decimal; transformar explicitamente evita
        # que 45,9/45.9 seja gravado como 459 em uma publicação futura.
        for coluna in ("percentual", "margem_erro", "confianca", "amostra", "ano"):
            if coluna in df.columns:
                bruto = df[coluna].astype(str).str.strip().str.replace("%", "", regex=False)
                bruto = bruto.str.replace(",", ".", regex=False)
                df[coluna] = pd.to_numeric(bruto, errors="coerce")
        for coluna in colunas_destino:
            if coluna not in df.columns:
                df[coluna] = ""
        return df.reindex(columns=colunas_destino)

    publicados = []
    retidos_por_colisao = []
    novos_por_destino = {}
    for turno, sheet_id in (("t1", T1_ID), ("t2", T2_ID)):
        if not sheet_id:
            continue
        pt_all = pendentes[pendentes["turno"].astype(str).str.lower() == turno]
        if pt_all.empty:
            continue
        liberado = pt_all["liberado"].astype(str).str.strip().str.lower().isin(["sim", "true", "1", "x"])
        retidos = ~liberado
        if retidos.any():
            print(f"{turno}: {int(retidos.sum())} cenário(s) RETIDOS aguardando "
                  "'liberado=sim' na topline_pesquisas")
        pt = pt_all[liberado]
        if pt.empty:
            continue

        sh_destino = gc.open_by_key(sheet_id)
        indice_destino = _indice_publicacao_destino(sh_destino.worksheet("pesquisas"))
        motivos = pt.apply(lambda linha: _motivo_colisao_publicacao(linha, indice_destino), axis=1)
        tem_colisao = motivos.ne("")
        if tem_colisao.any():
            for indice, motivo in motivos[tem_colisao].items():
                retidos_por_colisao.append((indice, motivo))
            print(f"{turno}: {int(tem_colisao.sum())} cenário(s) RETIDO(s): já existe(m) "
                  "no destino (scenario_id, poll_id ou chave legado).")
            pt = pt[~tem_colisao].copy()
        if pt.empty:
            continue

        ids = set(pt["scenario_id"].astype(str))
        rt = df_r[df_r["scenario_id"].astype(str).isin(ids)]
        # Após a trava acima, todos estes ids são novos no destino; manter os
        # valores originais aqui é importante para _marcar_origem encontrá-los.
        ids_novos = ids
        salvar_tudo(gc, sheet_id, _preparar(pt, POLLING_PESQUISAS_COLS),
                    _preparar(rt, POLLING_RESULTADOS_COLS))
        if ids_novos:
            novos_por_destino.setdefault(sheet_id, set()).update(ids_novos)
        publicados += list(pt.index)
        print(f"{turno}: {len(pt)} cenário(s), {len(rt)} resultado(s) -> planilha de {turno}")

    # Origem como última coluna (após metodologia), restrita às linhas inseridas
    # nesta execução; nunca reclassifica registros manuais já existentes.
    for sheet_id, scenario_ids in novos_por_destino.items():
        sh_t = gc.open_by_key(sheet_id)
        for tab in ("pesquisas", "resultados"):
            try:
                _marcar_origem(sh_t.worksheet(tab), ORIGEM, scenario_ids)
            except Exception as e:
                print(f"  aviso: origem em {tab} ({sheet_id[:6]}...): {e}")

    if publicados:   # índice 0-based do df = linha (i+2) na planilha
        ws_p.update_cells([gspread.Cell(i + 2, col_pub, "sim") for i in publicados],
                          value_input_option="RAW")
    if retidos_por_colisao:
        updates_validacao = []
        for indice, motivo in retidos_por_colisao:
            anterior = str(df_p.at[indice, "validacao"]).strip() if "validacao" in df_p.columns else ""
            nota = f"não publicado: {motivo}"
            if nota not in anterior:
                nota = f"{anterior}; {nota}".strip("; ")
            updates_validacao.append(gspread.Cell(indice + 2, col_validacao, nota[:300]))
        ws_p.update_cells(updates_validacao, value_input_option="RAW")
    print(f"\n{len(publicados)} cenário(s) publicado(s); "
          f"{len(retidos_por_colisao)} retido(s) por colisão.")


RELATORIOS_ID = os.getenv("SPREADSHEET_ID_RELATORIOS", "")




if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "topline":
        cmd_topline()
    elif cmd == "publicar":
        cmd_publicar()
    else:
        print("uso: python relatorios_extracao_topline_aposentado.py [topline|publicar]")
