"""Etapa 12. Tema por autoria e cargos com fonte dos deputados competitivos à reeleição.

O universo são os deputados em exercício da aba "Competitividade Câmara (em exercício)" do Radar
do Congresso que disputam a Câmara (reeleição ou outra UF) com índice Alta ou Média. Pesquisa de
cargo para os 1.600 candidatos à Câmara com cargo prévio ficou fora de propósito: são muitos.

Mesmo método da etapa 11:
- tema: só autoria principal (primeiro signatário) na Câmara, 2003-2026, tema oficial da Câmara,
  até 3 temas entre os que a pessoa apresenta acima da média da Casa, mínimo de 10 proposições;
- cargos e mandatos até 2006: ficha do verbete na Wikipedia, aceito só com ano de nascimento e UF.
O deputado é achado na API da Câmara (legislatura 57) por nome parlamentar dentro da UF; o
partido desempata. Nome civil e nascimento vêm da mesma API.

A aba também recebe quem chega à Câmara (não disputa reeleição) e tem autoria na Câmara ou no
Senado, só com o tema: é quem a tela de renovação do painel conta.

Saídas: abas "Câmara competitivos, autoria e cargos" (uma linha por deputado) e
"Câmara competitivos, cargos com fonte" (uma linha por cargo) na planilha "Pré mapeamento".
Rodar: python -m outros.novos_eleitos.e12_camara_competitivos [--faixas Alta,Média] [--publicar]
"""
import argparse

import pandas as pd

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.e11_trajetoria_competitivos import (
    ENTRA_NO_RESUMO, WIKI_URL, achar_verbete, autoria_principal_camara, cargos_wiki, chegam_com_autoria, colunas_tema,
    pasta, tema_autoria, texto_periodo, tipo_cargo)

ABA_RADAR = "Competitividade Câmara (em exercício)"
ABA_PESSOA = "Câmara competitivos, autoria e cargos"
ABA_CARGOS = "Câmara competitivos, cargos com fonte"
DISPUTA_CAMARA = ("Disputa reeleição para a Câmara", "tenta a Câmara em")


def deputados_57():
    lista = c.em_cache("camara_legislatura", "57", lambda: list(c.paginar_camara("deputados", {"idLegislatura": 57})))
    # Uma linha por nome e partido que o deputado já teve na legislatura: não deduplicar por id.
    d = pd.DataFrame(lista)[["id", "nome", "siglaUf", "siglaPartido"]].drop_duplicates()
    d["chave"] = d.nome.map(lambda x: c.chave_nome(x, tirar_titulo=True))
    return d


def universo(faixas):
    radar = c.ler_aba(c.id_planilha("SPREADSHEET_ID_RECANDIDATURAS"), ABA_RADAR)
    alvo = radar["O que disputa em 2026"].map(lambda s: any(t in s for t in DISPUTA_CAMARA))
    radar = radar[alvo & radar["Alta, média ou baixa"].isin(faixas)].copy()
    deps = deputados_57()
    ids, sem = [], []
    for _, r in radar.iterrows():
        chave = c.chave_nome(r.Parlamentar, tirar_titulo=True)
        hit = deps[(deps.siglaUf == r.UF) & (deps.chave == chave)].drop_duplicates("id")
        if len(hit) > 1:
            hit = hit[hit.siglaPartido == r.Partido].drop_duplicates("id")
        if len(hit) != 1:
            # nome parlamentar que mudou: semelhança dentro da UF, partido igual
            da_uf = deps[(deps.siglaUf == r.UF) & (deps.siglaPartido == r.Partido)]
            nota = da_uf.chave.map(lambda k: c.semelhanca(k, chave))
            hit = (da_uf[nota == nota.max()] if len(nota) and nota.max() >= 0.85 else da_uf.iloc[:0]).drop_duplicates("id")
        ids.append(str(hit.id.iloc[0]) if len(hit) == 1 else "")
        if len(hit) != 1:
            sem.append(f"{r.Parlamentar} ({r.Partido}/{r.UF})")
    radar["ID na Câmara"] = ids
    c.falhar_se(len(sem) > 5, f"deputados sem ID na Câmara: {sem}")
    if sem:
        print(f"sem ID na Câmara, ficam sem tema e sem cargo: {sem}")
    detalhe = {i: (c.deputado(i) or {}) for i in radar["ID na Câmara"] if i}
    radar["nome_civil"] = radar["ID na Câmara"].map(lambda i: detalhe.get(i, {}).get("nomeCivil", ""))
    radar["nascimento"] = radar["ID na Câmara"].map(lambda i: detalhe.get(i, {}).get("dataNascimento") or "")
    return radar


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--faixas", default="Alta,Média")
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    base = universo(a.faixas.split(","))
    camara = autoria_principal_camara()
    media = camara.tema.value_counts() / camara.idProposicao.nunique()

    # SQ do registro de 2026, pela aba Mapeamento Câmara: é a chave com que o painel cruza.
    mapa = c.ler_aba(c.planilha_destino(), "Mapeamento Câmara")
    sq_por_id = dict(zip(mapa["ID na Câmara"], mapa["SQ_CANDIDATO"]))
    cod_por_sq = dict(zip(mapa.SQ_CANDIDATO, mapa["Código no Senado"]))

    pessoas, longos = [], []
    for _, r in base.iterrows():
        idc = r["ID na Câmara"]
        titulo = (achar_verbete(f"camara_{idc}", r.Parlamentar, r.Parlamentar, r.nome_civil, r.nascimento, r.UF)
                  if idc and r.nascimento else "")
        url = WIKI_URL + titulo.replace(" ", "_") if titulo else ""
        antes_2006, nao_eletivos, pastas, periodos = [], [], [], []
        for cargo, ps in (cargos_wiki(titulo) if titulo else []):
            tipo = tipo_cargo(cargo)
            p = pasta(cargo, tipo)
            longos.append({"Parlamentar": r.Parlamentar, "Partido": r.Partido, "UF": r.UF, "Cargo": cargo, "Tipo": tipo,
                           "Pasta (tema da Câmara)": p, "Período": texto_periodo(ps), "Fonte": url,
                           "ID na Câmara": idc})
            if tipo == "Mandato eletivo":
                velhos = [(i, f) for i, f in ps if i <= 2006]
                if velhos:
                    antes_2006.append(f"{cargo} ({texto_periodo(velhos)})")
            elif tipo in ENTRA_NO_RESUMO:
                nao_eletivos.append(cargo)
                periodos.append(texto_periodo(ps) or "sem data")
                if p:
                    pastas.append(p)

        sq = sq_por_id.get(idc, "")
        cod_senado = cod_por_sq.get(sq, "")
        temas, medida, fontes = tema_autoria(idc, cod_senado, r.Parlamentar, camara, media)
        pessoas.append({
            "Parlamentar": r.Parlamentar, "Partido": r.Partido, "UF": r.UF,
            "O que disputa em 2026": r["O que disputa em 2026"],
            "Índice de competitividade à reeleição": r["Índice de competitividade à reeleição"],
            "Alta, média ou baixa": r["Alta, média ou baixa"],
            **colunas_tema(temas, medida, []),
            "Cargo não eletivo anterior (ex.: secretário de pasta)": "; ".join(nao_eletivos),
            "Pasta ou área": "; ".join(dict.fromkeys(pastas)),
            "Período": "; ".join(periodos),
            "Teve mandato antes de 2006?": "; ".join(antes_2006) or ("Não" if titulo else ""),
            "Fonte da informação": " ".join(filter(None, [url] + fontes)),
            "Checagem": "Com fonte" if titulo else "Sem verbete com ficha na Wikipedia: cargos não pesquisados",
            "ID na Câmara": idc,
            "SQ_CANDIDATO": sq,
        })

    pessoas, longos = pd.DataFrame(pessoas), pd.DataFrame(longos)
    pessoas.insert(3, "Grupo", "Deputado competitivo à reeleição (Radar)")
    extra = chegam_com_autoria("Mapeamento Câmara", set(pessoas.SQ_CANDIDATO), camara, media)
    extra = extra.rename(columns={"Nome": "Parlamentar"})
    extra.insert(3, "Grupo", "Chega à Câmara com autoria na Câmara ou no Senado")
    print(f"chegam à Câmara com autoria: {len(extra)}; com tema: {(extra['Temática principal'] != '').sum()}")
    pessoas = pd.concat([pessoas, extra], ignore_index=True).fillna("")
    pessoas = pessoas[[col for col in pessoas.columns if col not in ("ID na Câmara", "SQ_CANDIDATO")] + ["ID na Câmara", "SQ_CANDIDATO"]]
    print(f"{len(pessoas)} linhas; com verbete: {(pessoas.Checagem == 'Com fonte').sum()}; "
          f"com tema: {(pessoas['Temática principal'] != '').sum()}; "
          f"com cargo não eletivo: {(pessoas['Cargo não eletivo anterior (ex.: secretário de pasta)'] != '').sum()}; "
          f"sem SQ: {(pessoas.SQ_CANDIDATO == '').sum()}; cargos: {len(longos)}")
    c.salvar_csv(pessoas, "camara_competitivos.csv")
    c.salvar_csv(longos, "camara_competitivos_cargos.csv")
    if a.publicar:
        destino = c.planilha_destino()
        c.gravar_aba(destino, ABA_PESSOA, pessoas.sort_values(["Grupo", "UF", "Parlamentar"], ascending=[False, True, True]),
                     congelar_colunas=1)
        c.gravar_aba(destino, ABA_CARGOS, longos.sort_values(["UF", "Parlamentar", "Tipo"]), congelar_colunas=1)


if __name__ == "__main__":
    main()
