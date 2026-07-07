"""
Extração headless do topline (voto estimulado por candidato) de PDFs de pesquisa.

Reaproveita a lógica da página Streamlit `5_Polling_Manual.py` (gerador-de-envios),
sem depender de Streamlit. Produz DataFrames no formato do `scraper_polling`
(pesquisas + resultados), tagueados como manual (conferida='manual_streamlit'),
prontos pra reconciliar com o dado oficial do PollingData quando a assinatura sair.
"""

import hashlib
import json
import os
import re
import time
import unicodedata
from datetime import datetime

# fitz (PyMuPDF) e pandas são importados dentro das funções que os usam, pra este
# módulo poder ser importado só pelos helpers (sigla_uf, instituto_canonico) em
# ambientes sem essas libs.

# Este módulo é independente do scraper_polling.py de propósito: ele fica
# intocado. As 3 funções abaixo são cópias pequenas do que era usado de lá.

GEMINI_MODEL = "gemini-2.5-flash"
TIPOS_RESULTADO = ["candidato", "nao_valido"]

# Marca a procedência: estes dados vêm dos PDFs dos relatórios, NÃO do PollingData.
ORIGEM = "PDF (relatório do instituto)"


def _slug(s: str) -> str:
    s = normalizar_texto_simples(s).lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash, disputa=""):
    uf = str(uf).upper()
    data_campo = normalizar_texto_simples(data_campo)
    turno_key = disputa if disputa else turno
    if id_pesquisa and str(id_pesquisa).lower() not in ("sem registro", "sem_registro", "semregistro", "nan", ""):
        return f"{uf}|{cargo}|{turno_key}|{id_pesquisa}|{data_campo}"
    return f"{uf}|{cargo}|{turno_key}|{_slug(instituto)}|{data_campo}"


def gerar_scenario_id(poll_id, scenario_label):
    return f"{poll_id}|{normalizar_texto_simples(scenario_label)}"


def normalizar_data_campo_segura(valor) -> str:
    """Aceita YYYY-MM-DD ou M/D/YYYY (nunca D/M/YYYY) e devolve YYYY-MM-DD."""
    s = normalizar_texto_simples(valor)
    if not s:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return s
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        try:
            return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return s
    return s


# ─────────────────────────── normalizadores ───────────────────────────

def normalizar_texto_simples(valor) -> str:
    return re.sub(r"\s+", " ", str(valor or "")).strip()


def normalizar_percentual_simples(valor):
    s = normalizar_texto_simples(valor).replace("%", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def normalizar_inteiro_simples(valor):
    s = re.sub(r"[^\d]", "", normalizar_texto_simples(valor))
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def classificar_tipo_resultado(nome: str, tipo_informado: str = "") -> str:
    tipo = normalizar_texto_simples(tipo_informado).lower()
    if tipo in TIPOS_RESULTADO:
        return tipo
    nome_norm = normalizar_texto_simples(nome).lower()
    marcadores = ["branco", "nulo", "nulos", "ns/nr", "nsnr", "não sabe", "nao sabe",
                  "indeciso", "indecisos", "nenhum", "não válido", "nao valido",
                  "não valido", "nao válido", "não respond", "nao respond"]
    if any(tag in nome_norm for tag in marcadores):
        return "nao_valido"
    return "candidato"


def extrair_json_de_texto_bruto(texto: str) -> dict:
    bruto = (texto or "").strip()
    if not bruto:
        raise RuntimeError("O Gemini não retornou JSON.")
    bruto = re.sub(r"^```json\s*", "", bruto, flags=re.IGNORECASE)
    bruto = re.sub(r"^```\s*", "", bruto)
    bruto = re.sub(r"\s*```$", "", bruto)
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", bruto):
        try:
            obj, _ = decoder.raw_decode(bruto[match.start():])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError("Não localizei um objeto JSON válido na resposta do Gemini.")


# ─────────────────────────── leitura do PDF ───────────────────────────

def extrair_texto_pdf_bytes(pdf_bytes, page_indices=None) -> str:
    import fitz  # PyMuPDF
    partes = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        pages = page_indices if page_indices is not None else list(range(doc.page_count))
        for idx in pages:
            if idx < 0 or idx >= doc.page_count:
                continue
            raw = doc.load_page(idx).get_text("text") or ""
            raw = raw.replace("-\n", "").replace("\n", " ")
            raw = re.sub(r"\s{2,}", " ", raw).strip()
            if raw:
                partes.append(raw)
    return " ".join(partes).strip()


# ─────────────────────────── Gemini ───────────────────────────

_CLIENT = None


def _gemini_client():
    global _CLIENT
    if _CLIENT is None:
        from google import genai
        key = os.environ.get("GEMINI_API_KEY", "")
        if not key:
            raise RuntimeError("GEMINI_API_KEY não definido.")
        _CLIENT = genai.Client(api_key=key)
    return _CLIENT


def gerar_conteudo_gemini(contents, tentativas: int = 3, backoff: float = 1.5):
    from google.genai import types
    client = _gemini_client()
    ultimo = None
    for t in range(1, tentativas + 1):
        try:
            try:
                cfg = types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0,
                    thinking_config=types.ThinkingConfig(thinking_budget=4000))
                resp = client.models.generate_content(model=GEMINI_MODEL, contents=contents, config=cfg)
            except Exception:
                resp = client.models.generate_content(model=GEMINI_MODEL, contents=contents)
            if getattr(resp, "text", None):
                return resp
            ultimo = RuntimeError("resposta vazia")
        except Exception as e:
            ultimo = e
        if t < tentativas:
            time.sleep(backoff * (2 ** (t - 1)))
    raise RuntimeError(f"Gemini falhou após {tentativas} tentativas: {ultimo}")


NOME_UF = {
    "brasil": "BR", "acre": "AC", "alagoas": "AL", "amapá": "AP", "amapa": "AP",
    "amazonas": "AM", "bahia": "BA", "ceará": "CE", "ceara": "CE",
    "distrito federal": "DF", "espírito santo": "ES", "espirito santo": "ES",
    "goiás": "GO", "goias": "GO", "maranhão": "MA", "maranhao": "MA",
    "mato grosso": "MT", "mato grosso do sul": "MS", "minas gerais": "MG",
    "pará": "PA", "para": "PA", "paraíba": "PB", "paraiba": "PB", "paraná": "PR",
    "parana": "PR", "pernambuco": "PE", "piauí": "PI", "piaui": "PI",
    "rio de janeiro": "RJ", "rio grande do norte": "RN", "rio grande do sul": "RS",
    "rondônia": "RO", "rondonia": "RO", "roraima": "RR", "santa catarina": "SC",
    "são paulo": "SP", "sao paulo": "SP", "sergipe": "SE", "tocantins": "TO",
}


def sigla_uf(valor) -> str:
    v = normalizar_texto_simples(valor)
    if len(v) == 2:
        return v.upper()
    return NOME_UF.get(v.lower(), v.upper())


_CANONICO = None


def _canonico() -> dict:
    global _CANONICO
    if _CANONICO is None:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), "canonico.json")
        try:
            with open(caminho, encoding="utf-8") as f:
                _CANONICO = json.load(f)
        except Exception:
            _CANONICO = {"institutos": [], "presidente": [], "governador": {}, "senador": {}}
    return _CANONICO


def _referencia(cargo, uf):
    """Retorna (lista de candidatos canônicos do cargo/UF, lista de institutos canônicos)."""
    c = _canonico()
    cargo = (cargo or "").lower()
    if cargo == "presidente":
        cands = c.get("presidente", [])
    elif cargo in ("governador", "senador"):
        cands = c.get(cargo, {}).get(sigla_uf(uf), [])
    else:
        cands = []
    return cands, c.get("institutos", [])


# palavras genéricas ignoradas ao casar o nome legal do PesqEle com o canônico
_STOP_INST = {
    "de", "da", "do", "e", "ltda", "instituto", "pesquisas", "pesquisa", "consultoria",
    "me", "eireli", "opiniao", "publica", "analise", "tecnologia", "comunicacao",
    "marketing", "associados", "midia", "publicidade", "estatistica", "informacao",
    "consumidor", "dados", "eventos", "agencia",
}


def _tokens_inst(s):
    s = unicodedata.normalize("NFKD", normalizar_texto_simples(s))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return [t for t in re.sub(r"[^a-z0-9 ]", " ", s).split() if t]


def instituto_canonico(nome):
    """Mapeia o nome legal do PesqEle (ex: 'REAL TIME MIDIA LTDA / REAL TIME BIG DATA')
    para o nome canônico do PollingData (ex: 'Real Time Big Data'). Se não achar, devolve
    a marca (parte após '/') em Title Case."""
    nome = normalizar_texto_simples(nome)
    if not nome:
        return ""
    alvo = set(_tokens_inst(nome))
    melhor, tam = None, 0
    for canon in _canonico().get("institutos", []):
        sig = [t for t in _tokens_inst(canon) if t not in _STOP_INST] or _tokens_inst(canon)
        if sig and all(t in alvo for t in sig) and len(canon) > tam:
            melhor, tam = canon, len(canon)   # prefere o canônico mais específico
    return melhor or (nome.split("/")[-1].strip() or nome).title()


def extrair_dados_polling_gemini(texto_fonte: str, url_original: str = "",
                                 escopo: dict | None = None,
                                 pdf_bytes: bytes | None = None) -> dict:
    """Extrai via Gemini.

    Quando houver texto_fonte e pdf_bytes, envia ambos: o texto ajuda no foco e
    o PDF visual cobre tabelas/gráficos/imagens que a extração textual não pegou.
    Se texto_fonte vier vazio, o PDF sozinho cobre relatório escaneado.
    """
    escopo = escopo or {}
    restricoes = []
    for chave, rotulo in (("cargo", "cargo"), ("uf", "uf"), ("turno", "turno"), ("instituto", "instituto")):
        val = normalizar_texto_simples(escopo.get(chave))
        if val:
            restricoes.append(f"- {rotulo} = {val}")
    bloco = ""
    if restricoes:
        bloco = ("FOCO DA EXTRAÇÃO (restrições obrigatórias):\n" + "\n".join(restricoes) +
                 "\nExtraia APENAS o bloco que casa com essas restrições. Ignore outros estados, "
                 "cargos ou turnos que apareçam no material.\nSe não houver bloco que case, "
                 "retorne cenarios=[].\n\n")

    cands, institutos = _referencia(escopo.get("cargo"), escopo.get("uf"))
    bloco_ref = ""
    if cands:
        bloco_ref += ("CANDIDATOS CANÔNICOS deste cargo/UF (quando o candidato do relatório for "
                      "um destes, use EXATAMENTE o mesmo nome curto e a mesma sigla; o texto entre "
                      "parênteses é o partido):\n" + "\n".join(cands) + "\n\n")
    if institutos:
        bloco_ref += ("INSTITUTOS CANÔNICOS (quando o instituto do relatório corresponder a um "
                      "destes, use EXATAMENTE este nome):\n" + ", ".join(institutos) + "\n\n")

    prompt = f"""
Você recebe o texto completo de um PDF de uma pesquisa eleitoral brasileira.
Extraia os dados estruturados para inserção em planilha.

{bloco}{bloco_ref}REGRAS:
- Responda somente com JSON válido.
- Não invente dados ausentes. Use string vazia ou null.
- Datas devem sair em YYYY-MM-DD quando possível.
- cargo deve ser governador, senador ou presidente.
- turno deve ser t1 ou t2.
- uf deve estar em caixa alta. Para presidente nacional use BR.
- percentual deve ser numérico, sem %. Preserve os números EXATAMENTE como no material;
  não arredonde, não recalcule, não normalize para somar 100.
- t1 = cenários de primeiro turno (vários candidatos testados); t2 = simulações de segundo
  turno (confrontos diretos, tipicamente entre 2 candidatos).
- tipo deve ser candidato ou nao_valido.
- PADRONIZAÇÃO DE NOMES: se o candidato corresponder a um da lista canônica acima, use o nome
  curto e a sigla EXATAMENTE como lá (campo 'candidato' = a parte antes do parêntese; 'partido' =
  a sigla dentro do parêntese). Se não estiver na lista, use o nome curto usual + a sigla do partido.
- 'partido' é SEMPRE a SIGLA curta em caixa alta (PT, PL, MDB, REP, UNIAO, PSD...), nunca o nome
  por extenso (use REP, não 'Republicanos'; UNIAO, não 'União Brasil'). Sem partido: 'SEM PARTIDO'.
- INSTITUTO: se corresponder a um da lista canônica, use o nome exato de lá.
- INVÁLIDOS: consolide TODOS os votos inválidos (branco, nulo, indeciso, não sabe, não respondeu,
  ns/nr) em UM ÚNICO item por cenário: candidato='Não válido', partido='', tipo='nao_valido',
  percentual = a soma deles.
- modo é o método de coleta (ex.: Presencial, Telefônica (CATI), Online, Misto). Vazio se não houver.
- metodologia é a descrição da metodologia conforme reportada (plano amostral, universo, técnica). Texto livre.
- Extraia SOMENTE cenários de voto ESTIMULADO (aqueles em que os nomes dos candidatos são
  apresentados ao entrevistado). NÃO inclua voto ESPONTÂNEO (sem lista de nomes), nem rejeição,
  nem avaliação/aprovação de governo. Cada cenário estimulado vira um item de "cenarios".
- votos_por_entrevistado: use 2 quando o relatório indicar que o entrevistado podia citar/votar
  em até 2 nomes naquele cenário (comum para senador em eleição com 2 vagas; procure notas como
  'cada entrevistado poderia citar até 2 candidatos'). Caso contrário, use 1.

FORMATO:
{{
  "cargo": "", "turno": "", "uf": "", "instituto": "", "registro_tse": "",
  "data_campo": "", "amostra": null, "margem_erro": null, "confianca": null,
  "modo": "", "metodologia": "", "fonte_url_original": "{url_original}",
  "observacoes": "", "pendencias": [],
  "cenarios": [
    {{ "scenario_label": "", "descricao": "", "votos_por_entrevistado": 1,
       "itens": [ {{ "candidato": "", "partido": "", "percentual": null, "tipo": "candidato" }} ] }}
  ]
}}
""".strip()

    if texto_fonte and pdf_bytes:
        from google.genai import types
        contents = [
            prompt + (
                "\n\nTEXTO EXTRAÍDO DO PDF/PÁGINA:\n"
                f"{texto_fonte}\n\n"
                "PDF ANEXO: confira visualmente as páginas, inclusive tabelas, gráficos "
                "e imagens. Se algum número aparecer só no anexo visual, extraia do anexo. "
                "Se houver conflito entre texto extraído e visual do PDF, prefira o PDF."
            ),
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        ]
    elif texto_fonte:
        contents = prompt + f"\n\nTEXTO FONTE:\n{texto_fonte}"
    elif pdf_bytes:
        from google.genai import types
        contents = [prompt + "\n\nO material é o PDF anexo (leia as páginas, inclusive tabelas e gráficos).",
                    types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")]
    else:
        raise RuntimeError("Passe texto_fonte ou pdf_bytes.")

    resp = gerar_conteudo_gemini(contents)
    payload = extrair_json_de_texto_bruto(getattr(resp, "text", "") or "")
    return normalizar_payload_polling(payload)


def normalizar_payload_polling(payload: dict) -> dict:
    payload = payload or {}
    cenarios_norm = []
    for idx, cenario in enumerate(payload.get("cenarios") or [], start=1):
        label = normalizar_texto_simples(cenario.get("scenario_label") or cenario.get("cenario") or idx)
        itens_norm = []
        for item in (cenario.get("itens") or cenario.get("resultados") or []):
            candidato = normalizar_texto_simples(
                item.get("candidato") or item.get("nome") or item.get("opcao") or item.get("candidato_partido"))
            partido = normalizar_texto_simples(item.get("partido"))
            percentual = normalizar_percentual_simples(item.get("percentual"))
            tipo = classificar_tipo_resultado(candidato, item.get("tipo", ""))
            if not candidato and percentual is None:
                continue
            itens_norm.append({"candidato": candidato, "partido": partido,
                               "percentual": percentual, "tipo": tipo})
        votos = normalizar_inteiro_simples(cenario.get("votos_por_entrevistado")) or 1
        cenarios_norm.append({"scenario_label": label or str(idx),
                              "descricao": normalizar_texto_simples(cenario.get("descricao") or cenario.get("titulo")),
                              "votos_por_entrevistado": max(1, min(votos, 3)),
                              "itens": itens_norm})
    return {
        "cargo": normalizar_texto_simples(payload.get("cargo")).lower() or "governador",
        "turno": normalizar_texto_simples(payload.get("turno")).lower() or "t1",
        "uf": normalizar_texto_simples(payload.get("uf")).upper() or "BR",
        "instituto": normalizar_texto_simples(payload.get("instituto")),
        "registro_tse": normalizar_texto_simples(payload.get("registro_tse")),
        "data_campo": normalizar_texto_simples(payload.get("data_campo")),
        "amostra": normalizar_inteiro_simples(payload.get("amostra")),
        "margem_erro": normalizar_percentual_simples(payload.get("margem_erro")),
        "confianca": normalizar_inteiro_simples(payload.get("confianca")),
        "modo": normalizar_texto_simples(payload.get("modo")),
        "metodologia": normalizar_texto_simples(payload.get("metodologia")),
        "fonte_url_original": normalizar_texto_simples(payload.get("fonte_url_original")),
        "observacoes": normalizar_texto_simples(payload.get("observacoes")),
        "pendencias": payload.get("pendencias") or [],
        "cenarios": cenarios_norm or [{"scenario_label": "1", "descricao": "", "itens": []}],
    }


# ─────────────────────── payload -> DataFrames ───────────────────────

def montar_dataframes_polling(payload: dict, fonte_url: str, fonte_url_original: str = "",
                              instituto_fonte: str = "") -> tuple:
    import pandas as pd
    cargo = normalizar_texto_simples(payload.get("cargo")).lower()
    turno = normalizar_texto_simples(payload.get("turno")).lower()
    uf = sigla_uf(payload.get("uf"))   # PesqEle manda "ALAGOAS"; polling usa "AL"
    # instituto vem do PesqEle (fila), normalizado pro canônico; alguns relatórios só
    # trazem o nome no logo, então não dá pra confiar no que o Gemini leu do PDF.
    instituto = instituto_canonico(instituto_fonte or payload.get("instituto"))
    registro_tse = normalizar_texto_simples(payload.get("registro_tse"))
    data_campo = normalizar_data_campo_segura(normalizar_texto_simples(payload.get("data_campo")))
    amostra = normalizar_inteiro_simples(payload.get("amostra"))
    margem_erro = normalizar_percentual_simples(payload.get("margem_erro"))
    confianca = normalizar_inteiro_simples(payload.get("confianca"))
    horario_raspagem = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ano_calc = datetime.now().year
    if data_campo and len(data_campo) >= 4 and data_campo[:4].isdigit():
        try:
            ano_calc = int(data_campo[:4])
        except ValueError:
            pass

    modo_payload = normalizar_texto_simples(payload.get("modo"))
    metodologia = normalizar_texto_simples(payload.get("metodologia"))

    registro_base = registro_tse
    if registro_base.lower() in {"sem registro", "sem_registro", "semregistro"}:
        registro_base = ""

    block_hash = hashlib.sha1(
        f"manual|{uf}|{cargo}|{turno}|{instituto}|{registro_tse}|{data_campo}".encode("utf-8", errors="ignore")
    ).hexdigest()[:10]

    poll_id = gerar_poll_id(uf, instituto, registro_base, data_campo, cargo, turno, block_hash)
    fonte_url_final = normalizar_texto_simples(fonte_url) or f"pdf://relatorio/{poll_id}"
    fonte_url_original_final = normalizar_texto_simples(fonte_url_original) or normalizar_texto_simples(
        payload.get("fonte_url_original"))
    classificacao = ""   # preenchida no roteamento (parte 2), a partir do scraper_polling

    pesquisas_rows, resultados_rows = [], []
    for idx, cenario in enumerate(payload.get("cenarios") or [], start=1):
        # scenario_label numérico (1, 2, ...), igual PollingData. O texto do relatório
        # (ex: "Estimulada Presidente") vai pra 'descricao'.
        scenario_label = str(idx)
        descricao = normalizar_texto_simples(cenario.get("scenario_label")) or \
            normalizar_texto_simples(cenario.get("descricao"))
        scenario_id = gerar_scenario_id(poll_id, scenario_label)

        pesquisas_rows.append({
            "origem": ORIGEM,
            "scenario_id": scenario_id, "poll_id": poll_id, "ano": ano_calc, "uf": uf,
            "cargo": cargo, "turno": turno, "instituto": instituto,
            "classificacao_instituto": classificacao, "registro_tse": registro_tse,
            "data_campo": data_campo, "modo": modo_payload, "amostra": amostra,
            "margem_erro": margem_erro, "confianca": confianca,
            "scenario_label": scenario_label, "descricao": descricao,
            "votos_por_entrevistado": cenario.get("votos_por_entrevistado", 1),
            "fonte_url": fonte_url_final, "fonte_url_original": fonte_url_original_final,
            "horario_raspagem": horario_raspagem, "conferida": "manual_streamlit",
            "metodologia": metodologia,
        })

        def _linha_resultado(candidato, partido, candidato_partido, tipo, percentual):
            resultados_rows.append({
                "origem": ORIGEM,
                "scenario_id": scenario_id, "poll_id": poll_id, "ano": ano_calc, "uf": uf,
                "cargo": cargo, "turno": turno, "data_campo": data_campo, "instituto": instituto,
                "classificacao_instituto": classificacao, "registro_tse": registro_tse,
                "scenario_label": scenario_label, "candidato": candidato, "partido": partido,
                "candidato_partido": candidato_partido, "tipo": tipo, "percentual": percentual,
                "fonte_url": fonte_url_final, "horario_raspagem": horario_raspagem,
            })

        invalido, tem_invalido = 0.0, False
        for item in cenario.get("itens") or []:
            candidato = normalizar_texto_simples(item.get("candidato"))
            partido = normalizar_texto_simples(item.get("partido"))
            percentual = normalizar_percentual_simples(item.get("percentual"))
            if percentual is None:
                continue
            if classificar_tipo_resultado(candidato, item.get("tipo", "")) == "nao_valido":
                invalido += percentual          # consolida todos os inválidos
                tem_invalido = True
                continue
            if not candidato:
                continue
            cp = f"{candidato} ({partido})" if partido else candidato
            _linha_resultado(candidato, partido, cp, "candidato", percentual)
        if tem_invalido:
            _linha_resultado("Não válido", "", "Não válido", "nao_valido", round(invalido, 1))

    return pd.DataFrame(pesquisas_rows), pd.DataFrame(resultados_rows)
