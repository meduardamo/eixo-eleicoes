"""Etapa 4. Pré-mapeamento: para cada candidatura de 2026, se é reeleição, volta ou
novo na Casa, e de onde a pessoa vem.

Regras:
- Reeleição: está hoje em exercício na Casa que disputa.
- Volta à Casa: não está hoje, mas já exerceu nela (Câmara e Senado desde 2003, pelas
  Casas; Assembleia, eleita titular pelo TSE desde 2006 ou fora do exercício na aba do Radar).
- Novo na Casa: o resto.
- Origem: o mandato mais relevante que a pessoa tem hoje (cadeira em outra Casa, depois
  mandato eleito em curso) e, sem mandato hoje, o último que teve. Suplente de senador
  só conta se exerceu.

Os tipos de origem dizem onde buscar os temas (etapa 6). O texto é fato de base oficial:
"Prefeito eleito em 2024" não afirma que a pessoa ainda está no cargo, porque quem
disputa outro cargo teve de renunciar.

Saída: pre_mapeamento.csv, que a etapa 10 publica na aba "Mapeamento". CPF e título ficam só
no CSV local, nunca na planilha.
Rodar: python -m outros.novos_eleitos.e4_pre_mapeamento
"""
import re

import pandas as pd

from outros.novos_eleitos import comum as c

try:
    from outros.tse_candidaturas import _nome_publicado as _publicado_base
except Exception:  # noqa: BLE001 - fora do repo, cai para capitalização simples
    def _publicado_base(nome):
        return " ".join(p if p.lower() in {"de", "da", "do", "das", "dos", "e"} else p.capitalize()
                        for p in str(nome).lower().split())


def _nome_publicado(urna):
    """Nome de urna na forma da planilha de nomes competitivos. Palavra sem vogal é sigla
    e fica em maiúscula ("MARINA JHC" vira "Marina JHC"), e depois do apóstrofo a letra
    também sobe ("D'ÁVILA" vira "D'Ávila")."""
    siglas = {c.sem_acento(p).upper() for p in str(urna).split()
              if p.isalpha() and not set(c.sem_acento(p).upper()) & set("AEIOUY")}
    partes = []
    for p in _publicado_base(urna).split():
        if c.sem_acento(p).upper() in siglas:
            p = p.upper()
        elif "'" in p:
            p = "'".join(s[:1].upper() + s[1:] for s in p.split("'"))
        partes.append(p)
    return " ".join(partes)


def nome_exibido(nome_casa, urna):
    """Nome da Casa quando a pessoa passou por ela; as fichas antigas guardam o nome em
    maiúscula ("RUI COSTA", "ROSE DE FREITAS"), e esses passam pela mesma formatação. Se o
    nome antigo só difere da urna de 2026 pelo acento ("ANDRE MOURA"), vale a urna."""
    nome = nome_casa or urna
    if nome != nome.upper():
        return nome
    if c.sem_acento(nome).upper() == c.sem_acento(urna).upper():
        nome = urna
    return _nome_publicado(nome)


FEMININO = [(r"\b([Dd])eputado\b", r"\1eputada"), (r"\b([Ss])enador\b", r"\1enadora"),
            (r"\b([Gg])overnador\b", r"\1overnadora"), (r"\b([Pp])refeito\b", r"\1refeita"),
            (r"\b([Vv])ereador\b", r"\1ereadora"), (r"\beleito\b", "eleita")]


def no_feminino(texto):
    """O rótulo de cargo concorda com o gênero do registro no TSE. "Suplente de senador"
    é o nome do cargo e não muda."""
    for padrao, troca in FEMININO:
        texto = re.sub(padrao, troca, texto)
    return texto.replace("suplente de senadora", "suplente de senador")

# prioridade da origem: menor vence
TIPO_DO_CARGO = {
    "SENADOR": ("Senado", 1), "DEPUTADO FEDERAL": ("Câmara", 2),
    "DEPUTADO ESTADUAL": ("Assembleia", 3), "DEPUTADO DISTRITAL": ("Assembleia", 3),
    "PRESIDENTE": ("Presidência", 4), "VICE-PRESIDENTE": ("Presidência", 5),
    "GOVERNADOR": ("Governo estadual", 6), "VICE-GOVERNADOR": ("Governo estadual", 7),
    "PREFEITO": ("Prefeitura", 8), "VICE-PREFEITO": ("Prefeitura", 9),
    "VEREADOR": ("Câmara Municipal", 10),
}
ONDE_PUXAR = {
    "Senado": "API do Senado: autoria e relatoria",
    "Câmara": "API da Câmara: autoria, coautoria e temas",
    "Assembleia": "Assembleia: dados abertos ou Inteligov; levantamentos IU e Marcela",
    "Presidência": "Programa de governo e notícias",
    "Governo estadual": "Plano de governo (TSE) e políticas do estado",
    "Prefeitura": "Plano de governo (TSE) e notícias",
    "Câmara Municipal": "Câmara Municipal: dados abertos e notícias",
    "Sem mandato eletivo": "Biografia (pesquisa manual) e Instagram",
}
CASA_HOJE = {"Senado": "Senador em exercício", "Câmara": "Deputado federal em exercício",
             "Assembleia": "Deputado estadual ou distrital em exercício"}
ABA_SENADO_TODAS = "Competitividade Senado (todas as candidaturas)"


def anos_legislatura(legs):
    """A Casa informa em quais legislaturas a pessoa exerceu, não por quanto tempo: suplente
    que ficou meses conta a legislatura inteira. O texto diz só o que o dado sustenta."""
    inicios = [str(2023 - 4 * (57 - int(x))) for x in sorted(int(x) for x in str(legs).split(";") if x)]
    if not inicios:
        return ""
    lista = inicios[0] if len(inicios) == 1 else f"{', '.join(inicios[:-1])} e {inicios[-1]}"
    return f"{'na legislatura iniciada' if len(inicios) == 1 else 'nas legislaturas iniciadas'} em {lista}"


def trajetoria(mand):
    """'Deputado federal: 2010, 2014, 2022 | Vereador (SALVADOR): 2004'"""
    partes = []
    for (cargo, onde), g in mand.sort_values("ano").groupby(["cargo", "onde"], sort=False):
        rotulo = c.ROTULO_CARGO[cargo] + (f" ({onde})" if cargo in c.MUNICIPAIS else "")
        partes.append(f"{rotulo}: {', '.join(g.ano.astype(str))}")
    return " | ".join(partes)


def competitivos_senado(cands):
    tab = c.ler_aba(c.id_planilha("SPREADSHEET_ID_RECANDIDATURAS"), ABA_SENADO_TODAS)
    senado = cands[cands.casa == "Senado"]
    marca, sem = {}, []
    for _, r in tab.iterrows():
        uf, chave = r["UF"], c.chave_nome(r["Candidato"], tirar_titulo=True)
        da_uf = senado[senado.uf == uf]
        # sem espaço: a urna escreve "MANUELA D ÁVILA" e a aba escreve "Manuela D'Ávila"
        colado = chave.replace(" ", "")
        hit = da_uf[da_uf.nome_urna.map(lambda x: c.chave_nome(x, True).replace(" ", "")) == colado]
        if len(hit) != 1:
            hit = da_uf[da_uf.nome_civil.map(c.chave_nome).map(lambda civil: set(chave.split()) <= set(civil.split()))]
        if len(hit) == 1:
            marca[hit.sq_candidato.iloc[0]] = c.campo(r, "É competitivo?")
        else:
            sem.append((uf, r["Candidato"]))
    if sem:
        print(f"{len(sem)} linhas da aba de competitividade sem candidatura casada: {sem}")
    return marca


def classificar(cands, mandatos, exercicio, comp):
    hoje = comp[(comp.em_exercicio == "Sim") & (comp.titulo != "")]
    hoje_por_titulo = hoje.groupby("titulo")
    fora_assembleia = set(comp[(comp.casa == "Assembleia") & (comp.em_exercicio == "Não")].titulo)
    exer_por_titulo = exercicio[exercicio.titulo != ""].groupby("titulo")
    mand_por_titulo = mandatos.groupby("titulo")
    linhas = []
    for _, r in cands.iterrows():
        tit, casa = r.titulo, r.casa
        agora = hoje_por_titulo.get_group(tit) if tit in hoje_por_titulo.groups else comp.iloc[0:0]
        exer = exer_por_titulo.get_group(tit) if tit in exer_por_titulo.groups else exercicio.iloc[0:0]
        mand = mand_por_titulo.get_group(tit) if tit in mand_por_titulo.groups else mandatos.iloc[0:0]
        mand = mand.assign(fim=mand.fim.astype(int), ano=mand.ano.astype(int))
        # suplente de senador só vale se exerceu
        mand_valido = mand[~mand.cargo.isin(["1º SUPLENTE", "2º SUPLENTE"])]

        if casa in set(agora.casa):
            classe = "Reeleição"
        elif (casa in set(exer.casa)
              or (casa == "Assembleia" and (mand.cargo.isin(["DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL"]).any()
                                            or tit in fora_assembleia))):
            classe = "Volta à Casa"
        else:
            classe = "Novo na Casa"

        # origem: 1) cadeira hoje, 2) mandato eleito em curso, 3) último mandato
        tipo, origem, prioridade = "Sem mandato eletivo", "Sem mandato eletivo desde 2006", 99
        for _, a in agora.iterrows():
            p = {"Senado": 1, "Câmara": 2, "Assembleia": 3}[a.casa]
            if p < prioridade:
                tipo, origem, prioridade = a.casa, CASA_HOJE[a.casa], p
        if prioridade == 99:
            em_curso = mand_valido[mand_valido.fim >= 2026]
            for _, m in em_curso.iterrows():
                t, p = TIPO_DO_CARGO[m.cargo]
                if p < prioridade:
                    onde = f" em {m.onde}" if m.cargo in c.MUNICIPAIS else f" ({m.uf})"
                    tipo, origem, prioridade = t, f"{c.ROTULO_CARGO[m.cargo]}{onde}, eleito em {m.ano}", p
        if prioridade == 99:
            passado = []
            for _, m in mand_valido.iterrows():
                passado.append((m.fim, -TIPO_DO_CARGO[m.cargo][1], TIPO_DO_CARGO[m.cargo][0],
                                f"Ex-{c.ROTULO_CARGO[m.cargo].lower()}, eleito em {m.ano}"))
            for _, e in exer.iterrows():
                ultima = max(int(x) for x in e.legislaturas.split(";"))
                passado.append((2027 - 4 * (57 - ultima), -{"Senado": 1, "Câmara": 2}[e.casa], e.casa,
                                f"Passou {'pelo Senado' if e.casa == 'Senado' else 'pela Câmara'} "
                                f"{anos_legislatura(e.legislaturas)}"))
            if passado:
                _, _, tipo, origem = max(passado)

        nome_casa = ""
        for fonte in (agora, exer):
            if len(fonte):
                nome_casa = fonte.sort_values("casa").nome_casa.iloc[0]
                break
        nome = nome_exibido(nome_casa, r.nome_urna)
        exer_txt = "; ".join(f"{e.casa}, {anos_legislatura(e.legislaturas)}" for _, e in exer.iterrows())
        genero = no_feminino if r.get("genero", "") == "FEMININO" else (lambda t: t)
        linhas.append({
            "Casa disputada": casa, "Nome": nome, "Nome de urna (TSE)": r.nome_urna,
            "Partido": r.partido, "UF": r.uf, "Cargo disputado": genero(c.ROTULO_CARGO[r.cargo]),
            "Situação do registro": r.situacao, "Reeleição, volta ou novo": classe,
            "Origem": "Mesma Casa" if classe == "Reeleição" else genero(origem),
            "Tipo de origem": casa if classe == "Reeleição" else tipo,
            "De onde puxar os temas": ("Histórico na própria Casa" if classe == "Reeleição"
                                       else ONDE_PUXAR[tipo]),
            "Mandatos eletivos desde 2006 (TSE)": genero(trajetoria(mand)),
            "Exercício na Câmara ou no Senado": exer_txt,
            "ID na Câmara": ";".join(exer[exer.casa == "Câmara"].id_casa),
            "Código no Senado": ";".join(exer[exer.casa == "Senado"].id_casa),
            "Ocupação declarada ao TSE": r.ocupacao,
            "SQ_CANDIDATO": r.sq_candidato, "titulo": tit,
        })
    return pd.DataFrame(linhas)


def conferir(pre, comp):
    """Reeleição contra as abas do Radar, que foram conferidas uma a uma. As abas são foto
    de uma data: depois delas houve renúncia de candidatura (Gilvan da Federal, Geraldo
    Mendes e Pastor Gil em 13/09/2026) e troca de ocupante (Hildo Rocha entrou na Câmara).
    Divergência só passa com uma dessas duas explicações; qualquer outra para a etapa."""
    radar = c.id_planilha("SPREADSHEET_ID_RECANDIDATURAS")
    renuncia = set(c.ler_csv("fora_da_disputa_2026.csv").pipe(lambda d: c.canonizar(d.titulo)))
    abas = {"Senado": ("Competitividade Senado (em exercício)", ("Disputa reeleição para o Senado",)),
            "Câmara": ("Competitividade Câmara (em exercício)", ("Disputa reeleição para a Câmara", "Não disputa reeleição pelo mesmo estado, tenta a Câmara")),
            "Assembleia": ("Competitividade Assembleias (em exercício)", ("Disputa reeleição estadual/distrital",))}
    for casa, (aba, textos) in abas.items():
        tab = c.ler_aba(radar, aba)
        if casa == "Assembleia":
            tab = tab[tab["Em exercício na assembleia?"] == "Sim"]
        titulo = comp[comp.casa == casa].set_index(["uf", "nome_casa"]).titulo
        tab = tab.assign(titulo=[titulo.get((uf, nome), "") for uf, nome in zip(tab.UF, tab.Parlamentar)])
        no_radar = set(tab.titulo)
        esperado = set(tab[tab["O que disputa em 2026"].str.startswith(textos)].titulo)
        achado = set(pre[(pre["Casa disputada"] == casa) & (pre["Reeleição, volta ou novo"] == "Reeleição")].titulo)
        faltam, sobram = esperado - achado, achado - esperado
        sem_explicacao = [t for t in faltam if t not in renuncia] + [t for t in sobram if t in no_radar]
        nome = dict(zip(tab.titulo, tab.Parlamentar)) | dict(zip(pre.titulo, pre.Nome))
        print(f"{casa}: {len(achado)} reeleições no pré-mapeamento, {len(esperado)} na aba do Radar. "
              f"Renunciaram depois da aba: {sorted(nome.get(t, t) for t in faltam & renuncia)}. "
              f"Entraram na Casa depois da aba: {sorted(nome.get(t, t) for t in sobram - no_radar)}")
        c.falhar_se(bool(sem_explicacao), f"{casa}: divergência sem explicação com o Radar: "
                    f"{sorted(nome.get(t, t) for t in sem_explicacao)}")


def main():
    cands = c.ler_csv("candidaturas_2026.csv")
    cands["titulo"] = c.canonizar(cands.titulo)
    pre = classificar(cands, c.ler_csv("mandatos_tse.csv"), c.ler_csv("exercicio_casas.csv"),
                      c.ler_csv("composicao_atual.csv"))
    marca = competitivos_senado(cands)
    pre.insert(8, "É competitivo? (Senado)", [marca.get(sq, "") if casa == "Senado" else ""
                                               for sq, casa in zip(pre.SQ_CANDIDATO, pre["Casa disputada"])])
    print(pre.groupby(["Casa disputada", "Reeleição, volta ou novo"]).size().to_string())
    print(pre.groupby(["Casa disputada", "Tipo de origem"]).size().to_string())
    conferir(pre, c.ler_csv("composicao_atual.csv"))
    c.salvar_csv(pre, "pre_mapeamento.csv")


if __name__ == "__main__":
    main()
