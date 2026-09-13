"""Etapa 2. Trajetória: todo mandato eletivo ganho desde 2006 e todo período de exercício
na Câmara e no Senado desde 2003.

Por que as duas fontes: o TSE diz quem foi eleito (inclusive governador, prefeito e
vereador), mas não diz quem assumiu como suplente. As Casas dizem quem exerceu.

Chaves: o título de eleitor liga os anos do TSE entre si (2024 veio sem CPF). A Câmara
publica CPF na ficha do deputado. O Senado não publica CPF: a ligação é nome civil mais
data de nascimento, e quem não casar sai listado no fim.

Saídas: mandatos_tse.csv, pessoas_tse.csv, exercicio_casas.csv
Rodar: python -m outros.novos_eleitos.e2_trajetoria [--renovar]
    --renovar apaga o cache das fichas da Câmara e do Senado antes de rodar.
"""
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd

from outros.novos_eleitos import comum as c

ANOS = range(2006, 2025, 2)
LEGISLATURAS = range(52, 58)  # 52ª começou em 2003; 57ª é a atual, até 2027


def ano_inicio_legislatura(numero):
    return 2023 - 4 * (57 - int(numero))


def legislatura_da_data(texto):
    d = date.fromisoformat(texto[:10])
    ano = d.year - (1 if d.month < 2 else 0)
    return 57 + (ano - 2023) // 4


def tse():
    mandatos, pessoas = [], []
    for ano in list(ANOS) + [2026]:
        base = c.consulta_cand(ano)
        if ano % 4 == 2:
            gerais = base[base.DS_CARGO.isin([k for k in c.ROTULO_CARGO if k not in c.MUNICIPAIS])]
            pessoas.append(gerais[["titulo", "cpf", "NM_CANDIDATO", "nascimento", "SG_UF", "DS_CARGO"]]
                           .assign(ano=ano))
        if ano == 2026:
            continue
        eleitos = base[base.DS_SIT_TOT_TURNO.isin(c.ELEITO) & base.DS_CARGO.isin(c.ROTULO_CARGO)]
        # O 2º turno repete a linha. Não deduplicar por SQ_CANDIDATO: em 2006 o mesmo SQ
        # aparece em pessoas diferentes (2.359 casos) e metade dos eleitos sumia.
        eleitos = eleitos.drop_duplicates(["titulo", "DS_CARGO", "SG_UE"])
        print(ano, eleitos.DS_CARGO.value_counts().to_dict())
        mandatos.append(pd.DataFrame({
            "titulo": eleitos.titulo, "cpf": eleitos.cpf, "nome_civil": eleitos.NM_CANDIDATO,
            "nascimento": eleitos.nascimento, "ano": ano, "cargo": eleitos.DS_CARGO,
            "onde": [ue if cargo in c.MUNICIPAIS else uf for ue, uf, cargo in
                     zip(eleitos.NM_UE, eleitos.SG_UF, eleitos.DS_CARGO)],
            "uf": eleitos.SG_UF, "partido": eleitos.SG_PARTIDO,
            "fim": [ano + c.duracao(cargo) for cargo in eleitos.DS_CARGO],
        }))
    mandatos = pd.concat(mandatos, ignore_index=True)
    mandatos = mandatos[mandatos.titulo != ""]
    pessoas = pd.concat(pessoas, ignore_index=True)
    pessoas = pessoas[pessoas.titulo != ""]
    pessoas["chave"] = pessoas.NM_CANDIDATO.map(c.chave_nome)
    return mandatos, pessoas


def canonicos(pessoas):
    """Título antigo -> título mais recente da mesma pessoa, ligados pelo CPF. Sem isso,
    quem transferiu o título (Enio Verri, André Amaral) vira duas pessoas e perde a
    trajetória. Título ligado a dois CPFs é dado sujo e não entra na unificação."""
    com = pessoas[pessoas.cpf != ""]
    sujos = com.groupby("titulo").cpf.nunique()
    com = com[~com.titulo.isin(sujos[sujos > 1].index)]
    ultimo = com.sort_values("ano").groupby("cpf").titulo.last()
    return {t: ultimo[doc] for t, doc in zip(com.titulo, com.cpf) if ultimo[doc] != t}


def mapas(pessoas):
    """cpf -> título e (nome civil, nascimento) -> título, já com o título canônico.
    Chave de nome que aponta para dois títulos é homônimo e fica fora."""
    com_cpf = pessoas[pessoas.cpf != ""].drop_duplicates(["cpf", "titulo"])
    cpf_titulo = com_cpf[~com_cpf.duplicated("cpf", keep=False)].set_index("cpf").titulo.to_dict()
    nome = pessoas[pessoas.nascimento != ""].drop_duplicates(["chave", "nascimento", "titulo"])
    nome = nome[~nome.duplicated(["chave", "nascimento"], keep=False)]
    nome_titulo = {(k, n): t for k, n, t in zip(nome.chave, nome.nascimento, nome.titulo)}
    return cpf_titulo, nome_titulo


def camara(cpf_titulo, nome_titulo):
    legislaturas = {}
    for leg in LEGISLATURAS:
        for dep in c.paginar_camara("deputados", {"idLegislatura": leg}):
            legislaturas.setdefault(str(dep["id"]), set()).add(leg)
    print(f"Câmara: {len(legislaturas)} deputados que exerceram entre a {min(LEGISLATURAS)}ª e a 57ª")
    with ThreadPoolExecutor(8) as ex:
        fichas = dict(zip(legislaturas, ex.map(c.deputado, legislaturas)))
    linhas = []
    for id_camara, legs in legislaturas.items():
        f = fichas[id_camara] or {}
        status = f.get("ultimoStatus") or {}
        doc = c.cpf(f.get("cpf"))
        nasc = c.data_iso(f.get("dataNascimento"))
        tit = cpf_titulo.get(doc) or nome_titulo.get((c.chave_nome(f.get("nomeCivil")), nasc), "")
        linhas.append({"casa": "Câmara", "id_casa": id_camara, "nome_casa": status.get("nome", ""),
                       "partido_casa": status.get("siglaPartido", ""), "uf": status.get("siglaUf", ""),
                       "legislaturas": ";".join(str(x) for x in sorted(legs)), "titulo": tit, "cpf": doc,
                       "nome_civil": f.get("nomeCivil", ""), "nascimento": nasc})
    return pd.DataFrame(linhas)


def senado(nome_titulo):
    lista = c.senado(f"senador/lista/legislatura/{min(LEGISLATURAS)}/57")
    parlamentares = c.como_lista(lista["ListaParlamentarLegislatura"]["Parlamentares"]["Parlamentar"])
    codigos = sorted({p["IdentificacaoParlamentar"]["CodigoParlamentar"] for p in parlamentares})

    def buscar(cod):
        mand = c.senado(f"senador/{cod}/mandatos", chave_cache=f"mandatos_{cod}")
        det = c.senado(f"senador/{cod}", chave_cache=f"detalhe_{cod}")
        return cod, mand, det

    with ThreadPoolExecutor(4) as ex:
        respostas = list(ex.map(buscar, codigos))
    linhas = []
    for cod, mand, det in respostas:
        mandatos = c.como_lista(((mand or {}).get("MandatoParlamentar", {}).get("Parlamentar", {})
                                 .get("Mandatos") or {}).get("Mandato"))
        legs, uf = set(), ""
        for m in mandatos:
            # Mandato sem Exercicios é suplente que nunca assumiu: não conta.
            for e in c.como_lista((m.get("Exercicios") or {}).get("Exercicio")):
                inicio = legislatura_da_data(e["DataInicio"])
                fim = legislatura_da_data(e.get("DataFim") or date.today().isoformat())
                legs.update(range(inicio, fim + 1))
                uf = m.get("UfParlamentar", uf)
        legs = {l for l in legs if l in LEGISLATURAS}
        if not legs:
            continue
        p = (det or {}).get("DetalheParlamentar", {}).get("Parlamentar", {})
        ident = p.get("IdentificacaoParlamentar", {})
        nasc = c.data_iso(p.get("DadosBasicosParlamentar", {}).get("DataNascimento"))
        civil = ident.get("NomeCompletoParlamentar", "")
        linhas.append({"casa": "Senado", "id_casa": cod, "nome_casa": ident.get("NomeParlamentar", ""),
                       "partido_casa": ident.get("SiglaPartidoParlamentar", ""), "uf": uf,
                       "legislaturas": ";".join(str(x) for x in sorted(legs)),
                       "titulo": nome_titulo.get((c.chave_nome(civil), nasc), ""), "cpf": "",
                       "nome_civil": civil, "nascimento": nasc})
    return pd.DataFrame(linhas)


def main():
    if "--renovar" in sys.argv:
        for pasta in ("camara_deputado", "senado"):
            shutil.rmtree(c.caminho("cache") / pasta, ignore_errors=True)
    mandatos, pessoas = tse()
    canon = canonicos(pessoas)
    c.salvar_csv(pd.DataFrame({"titulo": list(canon), "canonico": list(canon.values())}),
                 "titulos_canonicos.csv")
    print(f"{len(canon)} títulos antigos unificados no título mais recente da pessoa")
    pessoas["titulo"] = pessoas.titulo.map(lambda t: canon.get(t, t))
    mandatos["titulo"] = mandatos.titulo.map(lambda t: canon.get(t, t))
    c.salvar_csv(mandatos, "mandatos_tse.csv")
    c.salvar_csv(pessoas, "pessoas_tse.csv")
    cpf_titulo, nome_titulo = mapas(pessoas)

    exercicio = pd.concat([camara(cpf_titulo, nome_titulo), senado(nome_titulo)], ignore_index=True)
    for casa, grupo in exercicio.groupby("casa"):
        sem = grupo[grupo.titulo == ""]
        atual = sem[sem.legislaturas.str.contains("57")]
        print(f"{casa}: {len(grupo)} pessoas, {len(sem)} sem ligação com o TSE, "
              f"{len(atual)} delas na 57ª legislatura")
        if len(atual):
            print("   ", atual[["nome_casa", "uf", "nome_civil", "nascimento"]].values.tolist())
    c.salvar_csv(exercicio, "exercicio_casas.csv")


if __name__ == "__main__":
    main()
