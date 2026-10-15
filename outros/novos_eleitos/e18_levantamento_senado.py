"""Etapa 18. Levantamento geral do Senado numa tabela só: uma linha por senador.

Junta o que as etapas 13 a 17 e a leitura de imprensa produziram e mede onde cada senador atua,
em dois eixos: 22 áreas de política pública e 8 públicos (mulheres, crianças e adolescentes, jovens,
pessoas idosas, pessoas com deficiência, população negra, povos indígenas, população LGBT+). Não classifica posição: Promotor, Neutro, Detrator ou sem informação
suficiente só se aplica a uma pauta definida, e entra depois, em colunas próprias.

O que conta como atuação num tema (registro de iniciativa):
- matéria legislativa de que é o primeiro autor, no Senado, na Câmara, em assembleia ou câmara municipal;
- matéria legislativa que relatou, no Senado ou na Câmara (na Câmara, só o relator do último andamento).
Voto não conta: todo senador vota, e o voto mede posição, não iniciativa. Homenagem, data
comemorativa, utilidade pública e outorga de rádio e TV ficam fora.

Nível por tema: a parte que o tema ocupa nos registros de iniciativa do próprio senador.
Alta a partir de 15%, Média de 7% a menos de 15%, Baixa abaixo de 7%, Sem registro quando não há
nenhum. Uma matéria pode ter mais de uma área, e as partes somam em média 145% por senador: dividida
igualmente entre as 22 áreas, cada uma ficaria com 6,6%, daí o corte da Média em 7%. A célula traz o nível e o
número de registros. A área vem da classificação da própria Casa; o público vem da classificação do
Senado, quando ela diz, e de termos da ementa. O grau de informação diz quanto há de registro no
total: com menos de 10, os níveis valem pouco.

Direitos humanos e cidadania: as Casas jogam nessa área toda matéria cuja classificação só diz o
público (Proteção Social do Senado, Direitos Humanos e Minorias da Câmara). Cada uma dessas matérias
foi lida e recebeu a área do assunto, em leitura_direitos_humanos.csv: violência e crime contra um
público vão para Segurança, atendimento e serviço para a área do serviço, data e homenagem saem. Fica
em Direitos humanos o que trata de discriminação, igualdade, direitos e garantias, acessibilidade,
liberdades, migração e memória. O público continua marcado na coluna dele.

Profissão: ocupações declaradas ao TSE e a área ligada a elas, da etapa 19 (perfil_pessoal.csv).

Saída: dados_novos_eleitos/levantamento_senado/levantamento.csv e registros.csv (cada registro
contado, com o tema atribuído).
Rodar: python -m outros.novos_eleitos.e18_levantamento_senado
"""
import re

import pandas as pd

from outros.novos_eleitos import comum as c

# Dois eixos, como nas classificações oficiais (Senado, Comparative Agendas Project): a área de
# política pública e o público a que a matéria se dirige. "Mulheres" é público, não área: um
# projeto sobre violência doméstica é Segurança para o público Mulheres.
(SAU, EDU, ESP, CUL, ASS, PRE, TRA, DHC, SEG, DEF, TRI, ORC, ECO, AGR, AMB, ENE, TRP, CID, CTC, ADM, POL, CIV) = TEMAS = [
    "Saúde", "Educação", "Esporte e lazer", "Cultura e turismo", "Assistência social", "Previdência",
    "Trabalho e emprego", "Direitos humanos e cidadania", "Segurança pública e direito penal",
    "Defesa e relações exteriores", "Tributos", "Orçamento e finanças públicas", "Economia, indústria e comércio",
    "Agropecuária e questão fundiária", "Meio ambiente e clima", "Energia e mineração", "Transporte e trânsito",
    "Cidades, habitação e saneamento", "Ciência, tecnologia e comunicações", "Administração pública e servidores",
    "Política, Justiça e instituições", "Direito civil e do consumidor"]
FORA = None  # não entra na contagem

PUBLICOS = {
    "Mulheres": r"\bmulher|feminic|maria da penha|gestante|parturiente|puerper|maternidade|licenca-maternidade|violencia domestica|igualdade de genero|menstrua",
    "Crianças e adolescentes": r"crianca|adolescent|\binfan|primeira infancia|creche|pedofil|conselho tutelar|menores? de idade",
    "Jovens": r"\bjovens?\b|juventude",
    "Pessoas idosas": r"\bidos[oa]s?\b|pessoa idosa|terceira idade|envelhecimento",
    "Pessoas com deficiência": r"deficien|autis|espectro autista|\blibras\b|\bsurd|acessibilidade|doencas? raras?|sindrome de down",
    "População negra": r"racial|racismo|\bnegr[ao]s?\b|quilombol|afrodescend",
    "Povos indígenas": r"indigen|povos originarios",
    "População LGBT+": r"lgbt|homofob|transfob|transex|travesti|identidade de genero|orientacao sexual|homoafet",
}
PROT = "Política Social / Proteção Social / "
PUBLICO_SENADO = {PROT + "Mulheres": "Mulheres", PROT + "Crianças e Adolescentes": "Crianças e adolescentes",
                  PROT + "Idosos": "Pessoas idosas", PROT + "Pessoas com Deficiência": "Pessoas com deficiência",
                  PROT + "População Indígena": "Povos indígenas"}

# Classificação do Senado: vale o prefixo mais longo que casar.
SENADO = {
    "Honorífico": FORA, "Infraestrutura / Comunicações / Rádio e TV": FORA,
    "Política Social / Saúde / Saneamento Básico": CID, "Política Social / Saúde": SAU,
    "Política Social / Educação": EDU, "Política Social / Desporto e Lazer": ESP, "Política Social / Cultura": CUL,
    "Política Social / Previdência Social": PRE,
    PROT + "Assistência Social": ASS, PROT + "Desenvolvimento Social e Combate à Fome": ASS,
    PROT + "Calamidade Pública e Emergência Social": ASS, "Política Social / Proteção Social": DHC,
    "Política Social / Trabalho e Emprego": TRA,
    "Política Social / Desenvolvimento Urbano": CID, "Política Social / Habitação": CID, "Política Social": ASS,
    "Jurídico / Direito Penal e Penitenciário": SEG, "Jurídico / Processo / Processo Penal": SEG,
    "Jurídico / Processo / Processo do Trabalho": TRA, "Jurídico / Direito de Trânsito": TRP,
    "Jurídico / Direitos e Garantias": DHC, "Jurídico / Direito Empresarial e Econômico": ECO,
    "Jurídico / Direito Eleitoral": POL, "Jurídico / Processo / Processo Legislativo": POL,
    "Jurídico / Processo / Processo Administrativo": ADM, "Jurídico": CIV,
    "Soberania, Defesa Nacional e Ordem Pública / Defesa do Estado e das Instituições Democráticas / Segurança Pública": SEG,
    "Soberania, Defesa Nacional e Ordem Pública": DEF,
    "Economia e Desenvolvimento / Agropecuária e Abastecimento": AGR, "Economia e Desenvolvimento / Política Fundiária e Reforma Agrária": AGR,
    "Economia e Desenvolvimento / Ciência, Tecnologia e Informática": CTC,
    "Economia e Desenvolvimento / Tributos": TRI, "Economia e Desenvolvimento / Finanças Públicas": ORC,
    "Economia e Desenvolvimento / Indústria, Comércio e Serviços / Turismo": CUL, "Economia e Desenvolvimento": ECO,
    "Orçamento Público": ORC, "Meio Ambiente": AMB,
    "Infraestrutura / Comunicações": CTC, "Infraestrutura / Minas e Energia": ENE, "Infraestrutura / Viação e Transportes": TRP,
    "Infraestrutura": TRP, "Administração Pública": ADM, "Organização do Estado": POL,
}
CAMARA = {
    "Saúde": SAU, "Educação": EDU, "Esporte e Lazer": ESP, "Arte, Cultura e Religião": CUL, "Turismo": CUL,
    "Previdência e Assistência Social": ASS, "Direitos Humanos e Minorias": DHC,
    "Direito Penal e Processual Penal": SEG, "Defesa e Segurança": SEG, "Relações Internacionais e Comércio Exterior": DEF,
    "Finanças Públicas e Orçamento": ORC, "Economia": ECO, "Indústria, Comércio e Serviços": ECO,
    "Trabalho e Emprego": TRA, "Agricultura, Pecuária, Pesca e Extrativismo": AGR, "Estrutura Fundiária": AGR,
    "Meio Ambiente e Desenvolvimento Sustentável": AMB, "Energia, Recursos Hídricos e Minerais": ENE,
    "Viação, Transporte e Mobilidade": TRP, "Cidades e Desenvolvimento Urbano": CID,
    "Ciência, Tecnologia e Inovação": CTC, "Comunicações": CTC, "Administração Pública": ADM,
    "Política, Partidos e Eleições": POL, "Processo Legislativo e Atuação Parlamentar": POL, "Direito e Justiça": POL,
    "Direito Constitucional": POL, "Direito Civil e Processual Civil": CIV, "Direito e Defesa do Consumidor": CIV,
    "Homenagens e Datas Comemorativas": FORA,
}
# Três temas da Câmara juntam o que o Senado separa: a ementa decide para que lado vai.
DESEMPATE = {
    "Previdência e Assistência Social": (r"previd|aposent|pensao por morte|pensionist|\binss\b|regime geral|regime proprio", PRE),
    "Finanças Públicas e Orçamento": (r"tribut|imposto|\bicms\b|\bipi\b|\biss\b|\bpis\b|cofins|aliquota|isencao|simples nacional|contribuicao social|\bcide\b", TRI),
    "Defesa e Segurança": (r"forcas armadas|exercito|marinha do brasil|aeronautica|defesa nacional|servico militar", DEF),
}
LOCAL = {
    "Saúde": SAU, "Educação": EDU, "Esporte e Lazer": ESP, "Assistência social": ASS, "Mulheres": DHC,
    "Pessoas com deficiência": DHC, "Direitos humanos e população LGBT+": DHC, "Costumes": DHC, "Segurança pública": SEG,
    "Tributos e orçamento": TRI, "Trabalho e economia": TRA, "Agropecuária": AGR, "Meio ambiente": AMB,
    "Transporte e trânsito": TRP, "Cidade e serviços urbanos": CID,
    "Administração pública, transparência e Legislativo": ADM, "Turismo": CUL, "Cultura": CUL, "Consumidor": CIV,
    "Homenagem, denominação e utilidade pública": FORA, "Ementa não diz o tema": FORA,
}
IMPRENSA = {
    "Saúde": SAU, "Educação": EDU, "Esporte e Lazer": ESP, "Assistência social": ASS, "Primeira infância": ASS,
    "Mulheres": "Mulheres", "Pessoas com deficiência": "Pessoas com deficiência", "Costumes": DHC, "Segurança pública": SEG,
    "Economia": ECO, "Trabalho": TRA, "Agropecuária": AGR, "Meio ambiente": AMB, "Energia": ENE, "Infraestrutura": TRP,
    "Saneamento": CID, "Justiça e instituições": POL, "Apostas (bets)": "Apostas (bets)",
}
OUTORGA = {"PDS", "PDL", "PDC", "TVR"}


def publicos(ementa, classificacao=""):
    """Públicos a que a matéria se dirige: pela classificação do Senado, quando ela diz, e por termo da ementa."""
    achados = [v for k, v in PUBLICO_SENADO.items() if k in str(classificacao)]
    texto = c.sem_acento(str(ementa)).lower()
    achados += [p for p, padrao in PUBLICOS.items() if re.search(padrao, texto)]
    return list(dict.fromkeys(achados))


def tema_senado(classificacao):
    """Temas das classificações da matéria, pela regra do prefixo mais longo."""
    achados = []
    for classe in [k for k in str(classificacao).split("; ") if k]:
        niveis = classe.split(" / ")
        for n in range(len(niveis), 0, -1):
            chave = " / ".join(niveis[:n])
            if chave in SENADO:
                achados.append(SENADO[chave])
                break
    return [t for t in dict.fromkeys(achados) if t]


def registros():
    """Uma linha por registro de iniciativa e tema."""
    linhas = []

    def soma(df, casa, tipo, temas_de):
        for r in df.to_dict("records"):
            pub = "; ".join(publicos(r.get("Ementa", ""), r.get("Classificação do Senado", "")))
            for t in temas_de(r):
                linhas.append({"Parlamentar": r["Parlamentar"], "Casa": casa, "Registro": tipo, "Tema": t, "Públicos": pub,
                               "Matéria": r.get("Matéria", ""), "Ementa": str(r.get("Ementa", ""))[:300], "Link": r.get("Link", "")})

    for g in ("curso", "reeleitos", "novos"):
        aut = c.ler_csv(f"perfil_senado_{g}/autoria.csv")
        soma(aut[aut.Papel == "Primeiro autor"], "Senado", "Primeiro autor", lambda r: tema_senado(r["Classificação do Senado"]))
        rel = c.ler_csv(f"perfil_senado_{g}/relatorias.csv")
        soma(rel[rel["Classificação do Senado"] != ""], "Senado", "Relatoria", lambda r: tema_senado(r["Classificação do Senado"]))

    def tema_camara(r):
        ementa = c.sem_acento(str(r.get("Ementa", ""))).lower()
        temas = []
        for t in [t for t in str(r["Tema"]).split("; ") if t]:
            padrao, alternativa = DESEMPATE.get(t, (None, None))
            temas.append(alternativa if padrao and re.search(padrao, ementa) else CAMARA.get(t))
        if r["Tipo"] in OUTORGA:
            temas = [t for t in temas if t != CTC]
        return [t for t in dict.fromkeys(temas) if t]

    aut = c.ler_csv("perfil_camara_senado/autoria.csv")
    soma(aut[aut.Papel == "Primeiro autor"], "Câmara", "Primeiro autor", tema_camara)
    soma(c.ler_csv("perfil_camara_senado/relatorias.csv"), "Câmara", "Relatoria (último andamento)", tema_camara)

    loc = c.ler_csv("inteligov_senado/autoria.csv")
    soma(loc[loc.Papel == "Primeiro autor"], "Assembleia ou câmara municipal", "Primeiro autor",
         lambda r: [t for t in [LOCAL.get(r["Área"])] if t])
    df = pd.DataFrame(linhas)

    # Direitos humanos e cidadania: vale a área dada na leitura de cada matéria
    lidas = c.ler_csv("levantamento_senado/leitura_direitos_humanos.csv")
    area_lida = dict(zip(zip(lidas.Casa, lidas["Matéria"]), lidas["Área pela leitura"]))
    dh = df.Tema == DHC
    chave = list(zip(df.Casa, df["Matéria"]))
    nova = pd.Series([area_lida.get(k) for k in chave], index=df.index)
    sem_leitura = dh & nova.isna()
    if sem_leitura.any():
        print(f"{df[sem_leitura].drop_duplicates(['Casa', 'Matéria']).shape[0]} matérias em Direitos humanos sem leitura: ficam na área")
    df.loc[dh & nova.notna(), "Tema"] = nova[dh & nova.notna()]
    df = df[df.Tema != "Fora (homenagem ou data)"]
    return df.drop_duplicates(["Parlamentar", "Casa", "Registro", "Tema", "Matéria"])


def nivel(n, total):
    if not n:
        return "Sem registro"
    parte = n / total
    return f"{'Alta' if parte >= 0.15 else 'Média' if parte >= 0.07 else 'Baixa'} ({n})"


PLANO = {
    "Saúde": SAU, "Educação": EDU, "Esporte e Lazer": ESP, "Segurança Pública": SEG, "Assistência Social": ASS,
    "Direitos Humanos e Minorias": DHC, "Meio Ambiente": AMB, "Agropecuária": AGR, "Economia, Indústria e Emprego": ECO,
    "Infraestrutura e Transporte": TRP, "Habitação e Cidades": CID, "Ciência, Tecnologia e Inovação": CTC,
    "Cultura": CUL, "Gestão e Finanças Públicas": ADM, "Energia": ENE,
}


def sem_registro():
    """Para quem tem grau de informação Baixo: o que o plano de governo e a imprensa dizem, por coluna.

    Fica fora da régua Alta/Média/Baixa, que mede registro de iniciativa. Do plano, vale o mais recente
    com texto: área citada em metade das páginas ou mais é prioridade, de um quarto a metade é citada.
    Da imprensa, cada trecho foi lido e marcado como o que declara ou o que já fez em cargo anterior
    (levantamento_senado/imprensa_sem_registro.csv).
    """
    notas = {}
    planos = c.ler_csv("planos_senado/planos.csv")
    planos = planos[planos["Situação"] == "Lido"].copy()
    planos["ano"] = planos["Eleição"].str.extract(r"(\d{4})")[0].astype(int)
    areas = c.ler_csv("planos_senado/areas.csv")
    for nome, g in planos.sort_values("ano").groupby("Parlamentar"):
        eleicao = g.iloc[-1]["Eleição"]
        for r in areas[(areas.Parlamentar == nome) & (areas["Eleição"] == eleicao)].to_dict("records"):
            citam, paginas = int(r["Páginas que citam"]), int(r["Páginas com texto"])
            parte = citam / paginas if paginas else 0
            if parte >= 0.25:
                rotulo = "Prioridade no plano de governo" if parte >= 0.5 else "Citada no plano de governo"
                notas.setdefault(nome, {}).setdefault(PLANO[r["Área"]], []).append(
                    f"{rotulo} ({eleicao}, {citam} de {paginas} páginas)")
    lidas = c.ler_csv("levantamento_senado/imprensa_sem_registro.csv")
    for r in lidas.to_dict("records"):
        alvo = IMPRENSA.get(r["Área"])
        coluna = f"Público: {alvo}" if alvo in PUBLICOS else alvo if alvo in TEMAS else None
        if not coluna:
            continue
        rotulos = (["Declara na imprensa"] if "Declara" in r["Tipo"] else []) + \
                  (["Atuação anterior (imprensa)"] if "atuação" in r["Tipo"].lower() else [])
        lista = notas.setdefault(r["Parlamentar"], {}).setdefault(coluna, [])
        lista += [x for x in rotulos if x not in lista]
    return notas


def main():
    origem = c.ler_csv("perfil_senado_origem/origem.csv")
    senado = pd.concat([c.ler_csv(f"perfil_senado_{g}/resumo.csv") for g in ("curso", "reeleitos", "novos")]).drop_duplicates("Parlamentar").set_index("Parlamentar")
    noticias = c.ler_csv("noticias_senado/noticias.csv")
    pessoal = c.ler_csv("perfil_senado_origem/perfil_pessoal.csv").set_index("Parlamentar")
    reg = registros()
    ordem = {"Mandato em curso": 0, "Reeleito": 1, "Reeleita": 1, "Novo": 2, "Nova": 2}
    linhas = []
    for o in origem.to_dict("records"):
        nome = o["Parlamentar"]
        meus = reg[reg.Parlamentar == nome]
        # o total é de matérias, não de pares matéria e tema: matéria com dois temas conta uma vez
        total = meus.drop_duplicates(["Casa", "Registro", "Matéria"]).shape[0]
        por_tema = meus.groupby("Tema").size().to_dict()
        unicos = meus.drop_duplicates(["Casa", "Registro", "Matéria"])
        por_publico = {p: int(unicos["Públicos"].str.split("; ").map(lambda xs: p in xs).sum()) for p in PUBLICOS}
        principais = sorted(por_tema.items(), key=lambda kv: -kv[1])[:3]
        declara = [IMPRENSA.get(a) for a in noticias[noticias.Parlamentar == nome]["Área"]]
        declara = list(dict.fromkeys(t for t in declara if t))
        casas = meus.Casa.value_counts().to_dict()
        linhas.append({
            "Parlamentar": nome, "Partido": o["Partido"], "UF": o["UF"], "Situação": o["Situação"],
            "De onde vem": o["De onde vem"], "Cargos não eletivos": o["Cargos não eletivos (Wikipedia)"],
            "Profissão declarada (TSE)": pessoal["Ocupações declaradas ao TSE (ano da candidatura)"].get(nome, ""),
            "Área ligada à profissão": pessoal["Área ligada à profissão"].get(nome, ""),
            "Câmara: legislaturas iniciadas em": o["Câmara: legislaturas iniciadas em"],
            "Grau de informação": "Alto" if total >= 50 else "Médio" if total >= 10 else "Baixo",
            "Registros de iniciativa": total,
            "De onde vêm os registros": "; ".join(f"{k} ({v})" for k, v in casas.items()),
            "Três temas principais": "; ".join(f"{t} ({n})" for t, n in principais),
            **{t: nivel(por_tema.get(t, 0), total) for t in TEMAS},
            **{f"Público: {p}": nivel(por_publico[p], total) for p in PUBLICOS},
            "Temas que declara na imprensa": "; ".join(declara),
            "Comissões hoje (titular)": senado["Comissões hoje (titular)"].get(nome, ""),
            "_o": ordem.get(o["Situação"], 9)})
    df = pd.DataFrame(linhas).sort_values(["_o", "UF", "Parlamentar"]).drop(columns="_o")
    for nome, colunas in sem_registro().items():
        linha = (df.Parlamentar == nome) & (df["Grau de informação"] == "Baixo")
        for coluna, textos in colunas.items():
            atual = df.loc[linha, coluna]
            extra = "; ".join(textos)
            df.loc[linha, coluna] = atual.map(lambda v: extra if v == "Sem registro" else f"{v}; {extra}")
    pasta = c.caminho("levantamento_senado")
    pasta.mkdir(parents=True, exist_ok=True)
    df.to_csv(pasta / "levantamento.csv", index=False)
    reg.to_csv(pasta / "registros.csv", index=False)
    print(f"{len(df)} senadores, {len(reg)} registros de iniciativa com tema")
    print(df["Grau de informação"].value_counts().to_dict())


if __name__ == "__main__":
    main()
