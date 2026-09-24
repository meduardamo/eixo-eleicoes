# -*- coding: utf-8 -*-
"""
Confere, frase a frase, se o resumo de cada plano (coluna `resumo` e
`resumos_eixos` da aba coerencia_planos) diz o que as citações gravadas em
analise_planos dizem. NÃO ESCREVE NA PLANILHA: imprime as frases marcadas e
grava conferir_resumos.csv para ler à mão.

Por que existe: em 24/09/2026 o resumo dos candidatos do PCO dizia "teto de
aposentadoria" onde o plano diz "aposentadoria com no máximo 25 anos de
trabalho", "terceirizados" onde diz "servidores contratados precariamente", e
o do Pedro Abib (RO) trazia "80 milhões" e "78%" onde o plano diz 60 milhões
e 71,87%. A conferência de número pega o segundo tipo; o primeiro só lendo.

O resumo é escrito a partir das citações (resumir_plano recebe a
classificação, não o PDF), então a régua é a citação: frase que diz mais, ou
outra coisa, do que as citações do candidato é marcada. A marcação é do
modelo e serve de fila, não de veredito: cada uma é conferida no plano antes
de mexer no texto.

    python -m outros.conferir_resumos_planos [--sq a,b,c] [--limite N]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor

import gspread
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analise_planos as ap  # noqa: E402

PROMPT = """Você confere um resumo de plano de governo contra as citações do próprio plano.

CITAÇÕES DO PLANO (tema | nível | citação | programa | prazo | público):
{citacoes}

FRASES DO RESUMO (numeradas):
{frases}

Para CADA frase, diga se as citações sustentam o que ela afirma. Marque só quando a frase:
- traz número, valor, percentual, prazo ou quantidade que não está nas citações ou está diferente;
- muda o sentido do que o plano diz (ex.: "teto" onde o plano diz "no máximo", "terceirizados" onde diz "servidores precários", "imediato" que o plano não diz, trocar quem é beneficiado, transformar diagnóstico em promessa ou promessa em diagnóstico);
- atribui ao candidato algo que não aparece em nenhuma citação.
Paráfrase fiel, resumo de várias citações e omissão de detalhe NÃO são erro. Frase genérica que as citações sustentam NÃO é erro.

Responda só JSON: uma lista com as frases marcadas, cada item
{{"n": <número da frase>, "problema": "numero" | "sentido" | "sem_base", "o_que_a_citacao_diz": "<trecho curto e literal da citação que contradiz ou a palavra 'nenhuma'>", "motivo": "<uma frase>"}}.
Se nenhuma frase tiver problema, responda [].
"""


def frases_de(texto: str) -> list[str]:
    partes = re.split(r"(?<=[.!?])\s+(?=[A-ZÁÉÍÓÚÂÊÔÃÕÇ])", texto.strip())
    return [p.strip() for p in partes if p.strip()]


def conferir(cand: dict, citacoes: list[dict]) -> list[dict]:
    frases, onde = [], []
    vistas = set()
    for eixo, texto in [("resumo", cand["resumo"])] + list(cand["eixos"].items()):
        for f in frases_de(texto):
            if f in vistas:
                continue
            vistas.add(f)
            frases.append(f)
            onde.append(eixo)
    if not frases:
        return []
    cit = "\n".join(
        f"- {c['tema']} | {c['nivel']} | {c['trecho']} | {c['programa_nome']} | {c['prazo']} | {c['publico_alvo']}"
        for c in citacoes)
    lista = "\n".join(f"{i + 1}. {f}" for i, f in enumerate(frases))
    from google.genai import types
    resp = ap._gerar(ap.GEMINI_MODEL, PROMPT.format(citacoes=cit, frases=lista),
                     types.GenerateContentConfig(temperature=0,
                                                 response_mime_type="application/json"))
    try:
        marcadas = json.loads(resp.text)
    except Exception:
        return [{"eixo": "", "frase": "", "problema": "resposta_ilegivel",
                 "o_que_a_citacao_diz": "", "motivo": (resp.text or "")[:200]}]
    saida = []
    for m in marcadas if isinstance(marcadas, list) else []:
        try:
            n = int(m.get("n")) - 1
        except Exception:
            continue
        if 0 <= n < len(frases):
            saida.append({"eixo": onde[n], "frase": frases[n],
                          "problema": m.get("problema", ""),
                          "o_que_a_citacao_diz": m.get("o_que_a_citacao_diz", ""),
                          "motivo": m.get("motivo", "")})
    return saida


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--sq", default="")
    p.add_argument("--limite", type=int, default=0)
    p.add_argument("--paralelo", type=int, default=4)
    args = p.parse_args()

    cred = Credentials.from_service_account_file(
        "credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    sh = gspread.authorize(cred).open_by_key(os.environ["SPREADSHEET_ID_TSE"])
    an = sh.worksheet("analise_planos").get_all_records()
    co = sh.worksheet("coerencia_planos").get_all_records()

    cit_de: dict[str, list] = {}
    for r in an:
        if str(r.get("nivel", "")).strip() != "Não menciona" and str(r.get("trecho", "")).strip():
            cit_de.setdefault(str(r["sq_candidato"]), []).append(
                {k: str(r.get(k, "")) for k in
                 ("tema", "nivel", "trecho", "programa_nome", "prazo", "publico_alvo")})

    alvos = {x.strip() for x in args.sq.split(",") if x.strip()}
    cands = []
    for r in co:
        sq = str(r["sq_candidato"])
        if alvos and sq not in alvos:
            continue
        try:
            eixos = json.loads(r.get("resumos_eixos") or "{}")
        except Exception:
            eixos = {}
        cands.append({"sq": sq, "candidato": r["candidato"], "partido": r["partido"],
                      "uf": r["uf"], "resumo": str(r.get("resumo", "")), "eixos": eixos})
    if args.limite:
        cands = cands[:args.limite]
    print(f"{len(cands)} candidatos", flush=True)

    def um(c):
        try:
            return c, conferir(c, cit_de.get(c["sq"], []))
        except Exception as e:  # noqa: BLE001
            return c, [{"eixo": "", "frase": "", "problema": "erro",
                        "o_que_a_citacao_diz": "", "motivo": f"{type(e).__name__}: {e}"[:200]}]

    linhas = []
    with ThreadPoolExecutor(max_workers=args.paralelo) as ex:
        for i, (c, marcadas) in enumerate(ex.map(um, cands), start=1):
            print(f"[{i}/{len(cands)}] {c['uf']} {c['candidato']}: {len(marcadas)} marcadas", flush=True)
            for m in marcadas:
                linhas.append({"sq_candidato": c["sq"], "candidato": c["candidato"],
                               "partido": c["partido"], "uf": c["uf"], **m})
                print(f"    [{m['problema']}] {m['eixo']} | {m['frase']}\n"
                      f"        citação: {m['o_que_a_citacao_diz']} | {m['motivo']}", flush=True)

    campos = ["sq_candidato", "candidato", "partido", "uf", "eixo", "frase",
              "problema", "o_que_a_citacao_diz", "motivo"]
    with open("conferir_resumos.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(linhas)
    print(f"\n{len(linhas)} frases marcadas em {len({l['sq_candidato'] for l in linhas})} candidatos. "
          f"Tokens: {ap._TOKENS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
