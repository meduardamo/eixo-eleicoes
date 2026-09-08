"""Afere se reperguntar corrige os "Propõe ação" que são só menção.

Conjunto de aferição fechado: as 20 linhas que a leitura de 250 citações
aleatórias, em 25/08/2026, marcou como classificadas em "Propõe ação" sem que
a citação proponha ação nenhuma. São título de seção ("PROGRAMA 6 —
AGROINDÚSTRIA BAIANA"), fragmento de lista ("Qualificação e reinserção
profissional"), diagnóstico ("Essa centralização sobrecarrega o transporte
sanitário") e intenção sem meio ("Acelerar e expandir a redução do tempo de
espera, consolidando os avanços já alcançados").

Existe porque três peneiras já reprovaram para achar essa classe por regra:
citação sem termo-âncora do tema (47% em todos os níveis), citação sem verbo de
ação (14,7%, mas proposta escrita como substantivo é legítima) e citação curta
(pega 6 dos 20 e marca 222 linhas). Ver [[project-planos-guardas]]. Sobrou
reperguntar ao modelo, e antes de gastar a leva inteira nos 207 planos convém
saber quanto ela rende: se reperguntar corrigir 3 de 20, não vale a rodada.

NÃO ESCREVE NADA. Só imprime o antes e o depois, linha a linha.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
from collections import Counter

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from outros.analise_planos import (  # noqa: E402
    TEMAS, acao_na_citacao, RespostaIlegivel, _norm_busca, contexto_do_tema, extrair_paginas_url,
    paginas_do_trecho, reanalisar_tema, verificar_trecho,
)


def contexto_pela_citacao(paginas: list[str], paginas_norm: list[str],
                          trecho: str, vizinhas: int = 1) -> str:
    """As páginas onde a citação gravada está, mais as vizinhas.

    Metade do conjunto de aferição não tem nenhum termo-âncora do tema no plano,
    e aí contexto_do_tema volta vazio e não há o que reperguntar. Isso não quer
    dizer que o plano não trate o assunto: TERMOS_ANCORA pega 31% das ausências
    e o resto passa porque o plano usa outras palavras, limite conhecido desde
    07/08/2026. A citação gravada, essa sim, está no plano e foi verificada.
    Então quando a âncora falha o entorno vem de onde a frase mora.
    """
    nums = paginas_do_trecho(paginas_norm, trecho)
    if not nums:
        return ""
    querer = set()
    for n in nums:
        for k in range(n - vizinhas, n + vizinhas + 1):
            if 1 <= k <= len(paginas):
                querer.add(k)
    return " ".join(paginas[k - 1] for k in sorted(querer))

CONJUNTO = pathlib.Path(__file__).with_name("aferir_acao.json")


def main() -> int:
    # Cópia do PDF no Drive antes do TSE. O DivulgaCand devolve 403 para IP de
    # datacenter, e a primeira rodada deste script no Actions caiu nos 19 planos
    # por isso, com o log dizendo "corrigiu 0 de 20" sem ter medido nada. É o
    # mesmo motivo do espelho no processar_planos, ver 19/08/2026.
    try:
        from outros.espelhar_planos import registrar_espelho
        n = registrar_espelho()
        print(f"espelho do Drive ativo para {n} planos\n")
    except Exception as e:  # noqa: BLE001
        print(f"espelho não pôde ser ligado ({str(e)[:120]}); seguindo no TSE\n")

    casos = json.loads(CONJUNTO.read_text(encoding="utf-8"))
    # Amostra ALEATÓRIA da base, em vez do conjunto fechado. O conjunto de 20 é
    # só de erros conhecidos, então mede recuperação e não diz nada sobre o
    # estrago: quantas das linhas CERTAS a repergunta mexeria à toa. Sem esse
    # segundo número não dá para decidir a rodada completa, porque 90% dos
    # "Propõe ação" da base estão certos. Aqui a amostra sai da aba, sem
    # julgamento prévio, e o que interessa é a taxa de mudança.
    if os.getenv("AFERIR_AMOSTRA", "").strip().isdigit():
        import random
        import gspread
        from google.oauth2.service_account import Credentials
        n = int(os.environ["AFERIR_AMOSTRA"])
        cr = Credentials.from_service_account_file(
            "credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        aba = gspread.authorize(cr).open_by_key(
            os.environ["SPREADSHEET_ID_TSE"]).worksheet("analise_planos")
        vals = aba.get_all_values()
        cab = vals[0]
        col = {c: cab.index(c) for c in cab if c}
        pool = [v for v in vals[1:]
                if v[col["nivel"]].strip() == "Propõe ação"
                and v[col["trecho"]].strip()
                and v[col["link"]].strip().startswith("http")]
        random.seed(int(os.getenv("AFERIR_SEMENTE", "7")))
        casos = [{"sq_candidato": v[col["sq_candidato"]],
                  "candidato": v[col["candidato"]], "partido": v[col["partido"]],
                  "uf": v[col["uf"]], "tema": v[col["tema"]],
                  "nivel": v[col["nivel"]], "trecho": v[col["trecho"]],
                  "link": v[col["link"]]}
                 for v in random.sample(pool, min(n, len(pool)))]
        print(f"amostra aleatória de {len(casos)} 'Propõe ação' "
              f"de um universo de {len(pool)}\n")
    # Agrupa por plano: baixar e OCRar o PDF é o caro, e dois casos do mesmo
    # candidato não podem custar dois downloads.
    por_plano: dict[str, list[dict]] = {}
    for c in casos:
        por_plano.setdefault(c["sq_candidato"], []).append(c)

    conta = Counter()
    for n, (sq, itens) in enumerate(por_plano.items(), 1):
        cab = f"[{n}/{len(por_plano)}] {itens[0]['uf']} · {itens[0]['candidato']}"
        try:
            paginas = extrair_paginas_url(itens[0]["link"])
        except Exception as e:  # noqa: BLE001
            print(f"{cab}: plano indisponível ({e})")
            conta["indisponível"] += len(itens)
            continue
        texto = " ".join(paginas)
        texto_norm = _norm_busca(texto)
        paginas_norm = [_norm_busca(pg) for pg in paginas]

        for c in itens:
            tema = c["tema"]
            # Pergunta estreita: o modelo lê a citação que está na planilha e
            # responde só se ela propõe ação, sem poder trocá-la. Ver
            # acao_na_citacao. É o caminho que não estraga citação boa.
            if os.getenv("AFERIR_ESTREITO", "").strip():
                r = acao_na_citacao(c["trecho"], tema, TEMAS.get(tema, ""))
                if r is None:
                    conta["ilegível"] += 1
                    print(f"{cab} · {tema}: resposta ilegível")
                    continue
                novo_nivel = "Propõe ação" if r == "acao" else "Menciona vagamente"
                conta[f"{c['nivel']} -> {novo_nivel}"] += 1
                if novo_nivel != c["nivel"]:
                    conta["mudou"] += 1
                    print(f"\n{cab} · {tema}")
                    print(f"   {c['nivel']} -> {novo_nivel}: {c['trecho'][:150]}")
                continue
            contexto = contexto_do_tema(texto, texto_norm, tema)
            origem = "âncora"
            if not contexto:
                contexto = contexto_pela_citacao(paginas, paginas_norm, c["trecho"])
                origem = "página da citação"
            if not contexto:
                print(f"{cab} · {tema}: sem contexto (sem âncora e citação não localizada)")
                conta["sem contexto"] += 1
                continue
            try:
                novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
            except (RespostaIlegivel, json.JSONDecodeError, ValueError) as e:
                print(f"{cab} · {tema}: resposta ilegível ({e})")
                conta["ilegível"] += 1
                continue

            ver = verificar_trecho(paginas_norm, novo["trecho"])
            mudou = novo["nivel"] != c["nivel"]
            conta[f"{c['nivel']} -> {novo['nivel']}"] += 1
            if mudou:
                conta["mudou"] += 1
            conta[f"contexto por {origem}"] += 1
            print(f"\n{cab} · {tema}  (contexto por {origem})")
            print(f"   antes ({c['nivel']}): {c['trecho'][:150]}")
            print(f"   depois ({novo['nivel']}, {ver or 'sem citação'}): "
                  f"{novo['trecho'][:150]}")

    print("\n" + "=" * 70)
    print(f"conjunto: {len(casos)} linhas em {len(por_plano)} planos")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    corrigidas = sum(v for k, v in conta.items()
                     if k.startswith("Propõe ação -> ")
                     and not k.endswith("-> Propõe ação"))
    print(f"\nreperguntar corrigiu {corrigidas} de {len(casos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
