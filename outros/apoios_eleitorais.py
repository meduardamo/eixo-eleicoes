"""
Monitoramento de apoios entre candidaturas nas eleicoes de 2026.

Cada linha da base e UMA RELACAO DIRECIONADA: alguem (pessoa, partido, lideranca,
movimento ou grupo) apoia, negocia apoio, rompe ou se opoe a uma candidatura.
"A apoia B" e diferente de "B apoia A": apoio mutuo vira DUAS linhas, nunca uma.

A saida e uma lista de arestas pronta pra virar grafo de rede com seta do apoiador
pro apoiado, e tambem pra ser lida como tabela pesquisavel.

Uso:
  python -m outros.apoios_eleitorais
  python -m outros.apoios_eleitorais --max-linhas 30
  python -m outros.apoios_eleitorais --dry-run
  python -m outros.apoios_eleitorais --force        # reconsulta quem ja tem relacao gravada
  python -m outros.apoios_eleitorais --incluir-sem-convencao   # ignora o filtro de convencao

Secrets/env:
  GOOGLE_CREDENTIALS_JSON
  GEMINI_API_KEY
  SPREADSHEET_ID_CONVENCOES
  CONVENCOES_ABA  (origem dos nomes, padrao "Convenções partidárias")
  APOIOS_ABA      (destino, padrao "Apoios por candidatura")

Por padrao so processa candidato cuja convencao JA ACONTECEU (Data convenção <=
hoje na aba de origem): e na convencao que a alianca vira fato registrado.
"""

import argparse
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone

import gspread
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials


BRT = timezone(timedelta(hours=-3))
GEMINI_MODEL = os.getenv("GEMINI_MODEL_APOIOS", "gemini-2.5-flash")
ABA_ORIGEM_PADRAO = "Convenções partidárias"
ABA_DESTINO_PADRAO = "Apoios por candidatura"
ANO_ELEICAO = 2026

USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}

CABECALHO = [
    "estado",
    "apoiador",
    "tipo de apoiador",
    "cargo do apoiador",
    "partido do apoiador",
    "apoiado",
    "cargo do apoiado",
    "partido do apoiado",
    "tipo de relação",
    "status",
    "data",
    "fonte",
    "link da fonte",
    "observação",
]

# Vocabularios fechados. O que vier fora deles e normalizado pelo alias ou cai
# no valor mais conservador, nunca num rotulo novo inventado pelo modelo.
TIPOS_APOIADOR = {
    "candidato": "candidato(a)", "candidata": "candidato(a)",
    "candidatoa": "candidato(a)", "candidatura": "candidato(a)",
    "partido": "partido", "federacao": "partido", "diretorio": "partido",
    "lideranca": "liderança", "politico": "liderança", "personalidade": "liderança",
    "movimento": "movimento",
    "grupopolitico": "grupo político", "grupo": "grupo político",
    "gruppolitico": "grupo político",
}
TIPOS_RELACAO = {
    "apoiodeclarado": "apoio declarado", "apoio": "apoio declarado",
    "declaracaodeapoio": "apoio declarado",
    "apoiopartidario": "apoio partidário", "apoiodopartido": "apoio partidário",
    "atoconjunto": "ato conjunto", "evento": "ato conjunto", "palanque": "ato conjunto",
    "negociacao": "negociação", "conversas": "negociação", "tratativas": "negociação",
    "alianca": "aliança", "coligacao": "aliança", "federacao": "aliança",
    "rompimento": "rompimento", "ruptura": "rompimento",
    "oposicao": "oposição", "adversario": "oposição",
}
STATUS = {
    "confirmado": "confirmado", "oficializado": "confirmado", "fechado": "confirmado",
    "emnegociacao": "em negociação", "negociacao": "em negociação",
    "emtratativas": "em negociação",
    "especulacao": "especulação", "bastidor": "especulação", "cotado": "especulação",
    "encerrado": "encerrado", "rompido": "encerrado", "desfeito": "encerrado",
}


def _registrar_uso(resp):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    USO_TOKENS["chamadas"] += 1
    USO_TOKENS["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    USO_TOKENS["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    USO_TOKENS["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _custo_estimado(entrada, saida, pensamento):
    # Faixa "flash": ~$0,30/1M de entrada, ~$2,50/1M de saida (pensamento cobra como
    # saida). O grounding do Google Search e cobrado POR REQUISICAO, fora dos tokens,
    # entao este numero e um PISO. O valor real esta no billing do Google.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _resumo_uso_tokens():
    if not USO_TOKENS["chamadas"]:
        return
    custo = _custo_estimado(USO_TOKENS["entrada"], USO_TOKENS["saida"], USO_TOKENS["pensamento"])
    print(f"\nGemini (apoios): {USO_TOKENS['chamadas']} chamada(s) · "
          f"{USO_TOKENS['entrada']:,} tokens entrada · {USO_TOKENS['saida']:,} saída · "
          f"{USO_TOKENS['pensamento']:,} pensamento · custo estimado ${custo:.4f} "
          f"(sem a tarifa de busca do Google)")


def _sem_acento(valor):
    return unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode()


def _norm(valor):
    return re.sub(r"[^a-z0-9]+", "", _sem_acento(valor).strip().lower())


def _creds_info():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não encontrado.")
    return json.loads(raw)


def _normalizar_spreadsheet_id(valor):
    s = str(valor or "").strip()
    for padrao in (r"/spreadsheets/d/([A-Za-z0-9_-]+)", r"\b([A-Za-z0-9_-]{30,})\b"):
        m = re.search(padrao, s)
        if m:
            return m.group(1)
    return s


def _planilha():
    creds = Credentials.from_service_account_info(
        _creds_info(), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    sheet_id = _normalizar_spreadsheet_id(os.getenv("SPREADSHEET_ID_CONVENCOES", ""))
    if not sheet_id:
        raise RuntimeError("Defina SPREADSHEET_ID_CONVENCOES.")
    try:
        return gspread.authorize(creds).open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        email = str(_creds_info().get("client_email", "")).strip()
        raise RuntimeError(
            f"Planilha não encontrada pela service account. Confira o ID ({sheet_id}) "
            f"e compartilhe com {email}."
        ) from exc


def _gemini():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não encontrada.")
    return genai.Client(api_key=api_key)


def _extrair_json_objeto(texto):
    bruto = re.sub(r"^```(?:json)?\s*|\s*```$", "", (texto or "").strip(), flags=re.I)
    decoder = json.JSONDecoder()
    for m in re.finditer(r"\{", bruto):
        try:
            obj, _ = decoder.raw_decode(bruto[m.start():])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError("JSON não encontrado na resposta do Gemini")


def _data_busca():
    return datetime.now(BRT).strftime("%Y-%m-%d %H:%M")


def _hoje():
    return datetime.now(BRT).date()


def _data_convencao(valor):
    """Primeira data concreta do campo livre de convenção da aba de origem.

    A coluna traz formatos soltos: "25/07", "01/08/2026", "01/08/2026 ou
    02/08/2026", "agosto de 2026", "Início de agosto de 2026". Só serve quando
    tem dia E mês; o resto devolve None e a linha fica fora da rodada.
    """
    s = str(valor or "").strip()
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b", s)
    if not m:
        return None
    dia, mes, ano = m.group(1), m.group(2), m.group(3)
    if not ano:
        ano = str(ANO_ELEICAO)
    elif len(ano) == 2:
        ano = f"20{ano}"
    try:
        return datetime(int(ano), int(mes), int(dia)).date()
    except ValueError:
        return None


def _limpar_data(valor):
    s = str(valor or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except ValueError:
            pass
    return s


def _de_vocabulario(valor, mapa, padrao):
    return mapa.get(_norm(valor), padrao)


def _tipo_relacao(valor):
    """Traduz pro vocabulário fechado, ou devolve "" pra linha ser descartada.

    Rótulo fora da lista significa que o modelo saiu do script. Cair pra "apoio
    declarado" nesse caso afirmaria apoio a partir de texto que ninguém validou,
    que é exatamente o que a regra da base proíbe. Só aceita o palpite quando o
    próprio texto fala em apoio ("apoio informal", "apoio tácito").
    """
    conhecido = TIPOS_RELACAO.get(_norm(valor))
    if conhecido:
        return conhecido
    return "apoio declarado" if "apoi" in _norm(valor) else ""


def _mapa_header(header):
    return {_norm(nome): pos for pos, nome in enumerate(header)}


def _ler_origem(sh):
    aba = os.getenv("CONVENCOES_ABA", ABA_ORIGEM_PADRAO)
    try:
        ws = sh.worksheet(aba)
    except gspread.exceptions.WorksheetNotFound as exc:
        abas = ", ".join(w.title for w in sh.worksheets())
        raise RuntimeError(f"Aba '{aba}' não encontrada. Disponíveis: {abas}") from exc

    valores = ws.get_all_values()
    if not valores:
        return []
    header, linhas = valores[0], valores[1:]
    mapa = _mapa_header(header)

    def campo(row, *nomes):
        for nome in nomes:
            pos = mapa.get(_norm(nome))
            if pos is not None and pos < len(row) and str(row[pos]).strip():
                return str(row[pos]).strip()
        return ""

    candidatos = []
    for row in linhas:
        nome = campo(row, "Pré-candidato", "Candidato")
        if not nome:
            continue
        candidatos.append({
            "estado": campo(row, "Estado", "UF"),
            "candidato": nome,
            "cargo": campo(row, "Cargo"),
            "partido": campo(row, "Partido/Federação", "Partido"),
            "data_convencao": campo(row, "Data convenção"),
        })
    return candidatos


def _abrir_destino(sh):
    aba = os.getenv("APOIOS_ABA", ABA_DESTINO_PADRAO)
    try:
        ws = sh.worksheet(aba)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba, rows=2000, cols=len(CABECALHO))
        ws.update(range_name="A1", values=[CABECALHO], value_input_option="RAW")
        ws.freeze(rows=1)
        return ws, list(CABECALHO)

    header = ws.row_values(1)
    if not header:
        ws.update(range_name="A1", values=[CABECALHO], value_input_option="RAW")
        return ws, list(CABECALHO)

    faltando = [c for c in CABECALHO if _norm(c) not in _mapa_header(header)]
    if faltando:
        novo = header + faltando
        if ws.col_count < len(novo):
            ws.add_cols(len(novo) - ws.col_count)
        ws.update(range_name="A1", values=[novo], value_input_option="RAW")
        header = novo
    return ws, header


def _chave(estado, apoiador, apoiado, relacao):
    """Identidade da aresta. Inclui o tipo de relação porque um mesmo par pode ter
    mais de um vínculo ao longo do tempo (negociação que vira apoio, apoio que
    vira rompimento). Não inclui a direção invertida: A→B e B→A são duas arestas
    distintas de propósito."""
    return (_norm(estado), _norm(apoiador), _norm(apoiado), _norm(relacao))


def _prompt(cand):
    return f"""
Você é um pesquisador eleitoral. Busque na web relações de APOIO entre candidaturas das eleições de 2026 envolvendo:

Estado/UF: {cand['estado']}
Candidato(a): {cand['candidato']}
Cargo: {cand['cargo']}
Partido/Federação: {cand['partido']}

Registre tanto quem apoia essa candidatura quanto quem essa candidatura apoia.

CADA RELAÇÃO É DIRECIONADA. "A apoia B" é diferente de "B apoia A". Nunca presuma reciprocidade: se houver apoio mútuo documentado, devolva DUAS relações, uma em cada direção.

tipo_apoiador, use exatamente um destes:
- "candidato(a)": pessoa que é candidata ou pré-candidata
- "partido": legenda, federação ou diretório
- "liderança": pessoa com peso político que não é candidata (ex-governador, prefeito, senador que não disputa, cacique partidário)
- "movimento": movimento social, sindical, religioso ou de base
- "grupo político": grupo, bancada, ala ou família política sem personalidade jurídica

tipo_relacao, use exatamente um destes:
- "apoio declarado": manifestação pública e nominal de apoio
- "apoio partidário": decisão formal do partido/federação de apoiar
- "ato conjunto": participaram juntos de evento, agenda ou palanque
- "negociação": conversas em curso, ainda sem decisão
- "aliança": composição formal (coligação, chapa, acordo anunciado)
- "rompimento": relação de apoio que existia e foi desfeita
- "oposição": declaração pública de que atuará contra a candidatura

status, use exatamente um destes:
- "confirmado": oficializado ou declarado publicamente pelas partes
- "em negociação": em curso, sem decisão anunciada
- "especulação": noticiado como bastidor, sem confirmação das partes
- "encerrado": vínculo que acabou (use junto com rompimento)

REGRAS:
1. Só devolva relação que tenha FONTE e LINK. Sem link verificável, não devolva.
2. NÃO infira apoio por afinidade partidária, por serem do mesmo partido, por serem aliados históricos ou por menção indireta. Só conta declaração, ato ou decisão documentada.
3. Não confunda estar no mesmo evento institucional (posse, inauguração) com ato conjunto de campanha.
4. Não organize por palanque nem agrupe candidaturas: uma linha por vínculo entre duas pontas.
5. "data" é a data do fato (declaração, ato, decisão), não a data da publicação, quando as duas aparecerem. Formato DD/MM/AAAA.
6. "fonte" é o nome do veículo ou da instituição; "link_fonte" é a URL.
7. "observacao" é uma frase curta com o que a fonte diz. Sem opinião.
8. Se não encontrar nenhuma relação documentada, devolva "relacoes": [].

Retorne APENAS JSON válido:
{{
  "relacoes": [
    {{
      "estado": "",
      "apoiador": "",
      "tipo_apoiador": "",
      "cargo_apoiador": "",
      "partido_apoiador": "",
      "apoiado": "",
      "cargo_apoiado": "",
      "partido_apoiado": "",
      "tipo_relacao": "",
      "status": "",
      "data": "",
      "fonte": "",
      "link_fonte": "",
      "observacao": ""
    }}
  ]
}}
""".strip()


def buscar_apoios(client, cand):
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=_prompt(cand),
        config=types.GenerateContentConfig(
            temperature=0.1,
            tools=[types.Tool(google_search=types.GoogleSearch())],
        ),
    )
    _registrar_uso(resp)
    relacoes = _extrair_json_objeto(getattr(resp, "text", "") or "").get("relacoes")
    return relacoes if isinstance(relacoes, list) else []


def _linhas_existentes(ws, header):
    mapa = _mapa_header(header)
    valores = ws.get_all_values()[1:]

    def campo(row, nome):
        pos = mapa.get(_norm(nome))
        return str(row[pos]).strip() if pos is not None and pos < len(row) else ""

    chaves, ja_buscados = set(), set()
    for row in valores:
        if not any(str(c).strip() for c in row):
            continue
        chaves.add(_chave(campo(row, "estado"), campo(row, "apoiador"),
                          campo(row, "apoiado"), campo(row, "tipo de relação")))
        for nome in (campo(row, "apoiador"), campo(row, "apoiado")):
            ja_buscados.add((_norm(campo(row, "estado")), _norm(nome)))
    return chaves, ja_buscados


def atualizar(max_linhas=40, force=False, incluir_sem_convencao=False, dry_run=False):
    sh = _planilha()
    candidatos = _ler_origem(sh)
    ws, header = _abrir_destino(sh)
    chaves, ja_buscados = _linhas_existentes(ws, header)

    hoje = _hoje()
    fila, sem_convencao, ja_feitos = [], 0, 0
    for c in candidatos:
        # Só entra quem já teve convenção. É na convenção que a aliança vira
        # fato registrado, e é esse histórico que a base precisa capturar. Quem
        # tem convenção marcada pra frente entra sozinho nas rodadas seguintes,
        # conforme a data chega.
        if not incluir_sem_convencao:
            data = _data_convencao(c["data_convencao"])
            if data is None or data > hoje:
                sem_convencao += 1
                continue
        if not force and (_norm(c["estado"]), _norm(c["candidato"])) in ja_buscados:
            ja_feitos += 1
            continue
        fila.append(c)
    fila = fila[:max_linhas]
    print(f"Apoios: {len(candidatos)} candidato(s) na origem · {len(fila)} para buscar · "
          f"{sem_convencao} com convenção futura ou sem data · {ja_feitos} já buscado(s).")
    if not fila:
        return

    client = _gemini()
    mapa = _mapa_header(header)
    novas, sem_resultado, erros, descartadas = [], 0, 0, 0

    for cand in fila:
        print(f"{cand['estado']} / {cand['candidato']} ({cand['partido']})...")
        try:
            relacoes = buscar_apoios(client, cand)
        except Exception as e:
            erros += 1
            print(f"  [erro] {str(e)[:180]}")
            continue

        adicionadas = 0
        for r in relacoes:
            if not isinstance(r, dict):
                continue
            apoiador = str(r.get("apoiador") or "").strip()
            apoiado = str(r.get("apoiado") or "").strip()
            link = str(r.get("link_fonte") or "").strip()
            fonte = str(r.get("fonte") or "").strip()
            # Sem as duas pontas, sem fonte ou sem link a relação não entra: a base
            # existe pra ser auditável, e aresta sem origem verificável contamina o grafo.
            if not (apoiador and apoiado and fonte and link.lower().startswith("http")):
                descartadas += 1
                continue
            if _norm(apoiador) == _norm(apoiado):
                descartadas += 1
                continue

            relacao = _tipo_relacao(r.get("tipo_relacao"))
            if not relacao:
                descartadas += 1
                continue
            estado = str(r.get("estado") or cand["estado"]).strip()
            chave = _chave(estado, apoiador, apoiado, relacao)
            if chave in chaves:
                continue
            chaves.add(chave)

            valores = {
                "estado": estado,
                "apoiador": apoiador,
                "tipo de apoiador": _de_vocabulario(r.get("tipo_apoiador"), TIPOS_APOIADOR, "liderança"),
                "cargo do apoiador": str(r.get("cargo_apoiador") or "").strip(),
                "partido do apoiador": str(r.get("partido_apoiador") or "").strip().upper(),
                "apoiado": apoiado,
                "cargo do apoiado": str(r.get("cargo_apoiado") or "").strip(),
                "partido do apoiado": str(r.get("partido_apoiado") or "").strip().upper(),
                "tipo de relação": relacao,
                "status": _de_vocabulario(r.get("status"), STATUS, "especulação"),
                "data": _limpar_data(r.get("data")),
                "fonte": fonte,
                "link da fonte": link,
                "observação": str(r.get("observacao") or "").strip()[:500],
            }
            linha = [""] * len(header)
            for nome_col, valor in valores.items():
                pos = mapa.get(_norm(nome_col))
                if pos is not None:
                    linha[pos] = valor
            novas.append(linha)
            adicionadas += 1
            print(f"  [{valores['status']}] {apoiador} --{relacao}--> {apoiado}")

        if not adicionadas:
            sem_resultado += 1
            print("  [vazio] nenhuma relação com fonte e link")

    if novas and not dry_run:
        ws.append_rows(novas, value_input_option="USER_ENTERED",
                       table_range=f"A{len(ws.get_all_values())}")

    print("\nResumo:")
    print(f"* relações novas: {len(novas)}")
    print(f"* descartadas por falta de fonte/link ou por auto-referência: {descartadas}")
    print(f"* candidatos sem relação encontrada: {sem_resultado}")
    print(f"* erros técnicos: {erros}")
    if dry_run:
        print("* dry-run: nada foi gravado.")

    _resumo_uso_tokens()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-linhas", type=int, default=int(os.getenv("APOIOS_MAX_LINHAS", "40")))
    parser.add_argument("--force", action="store_true",
                        default=os.getenv("APOIOS_FORCE", "").lower() == "true")
    parser.add_argument("--incluir-sem-convencao", action="store_true",
                        default=os.getenv("APOIOS_INCLUIR_SEM_CONVENCAO", "").lower() == "true",
                        help="busca também quem ainda não teve convenção")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    atualizar(max_linhas=args.max_linhas, force=args.force,
              incluir_sem_convencao=args.incluir_sem_convencao, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
