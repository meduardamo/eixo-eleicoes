"""Etapa 3. Quem está hoje nas cadeiras, com a chave que liga cada pessoa ao TSE.

Câmara: os 513 em exercício pela API, ligados pela ficha (CPF).
Senado: os 81 em exercício pela API, ligados pela etapa 2 (nome civil + nascimento).
Assembleias: aba "Competitividade Assembleias (em exercício)" do Radar do Congresso. Os
sites das assembleias não publicam CPF, então a ligação é o nome dentro da UF, conferido
pelo partido de 2022. A janela partidária fechou em abril, então partido que não bate é
sinal de casamento errado. Quem não casar vai para MAPA_MANUAL, com o título de eleitor.

Saída: composicao_atual.csv
Rodar: python -m outros.novos_eleitos.e3_composicao_atual
"""
import pandas as pd

from outros.novos_eleitos import comum as c

ABA_ASSEMBLEIAS = "Competitividade Assembleias (em exercício)"

# (UF, nome na aba do Radar) -> nome de urna em 2022. Só para quem o nome não casa. O
# repositório é público: aqui vai nome de urna, nunca título ou CPF. Cada par foi conferido
# em 13/09/2026 pelo mesmo título e nascimento no registro de 2022 e no de 2026.
MAPA_MANUAL = {
    ("SP", "Fabiana Bolsonaro"): "FABIANA B.",          # nome civil de 2022: Fabiana de Lima Barroso
    ("CE", "Carmelo Bolsonaro"): "CARMELO NETO",
    ("CE", "MISSIAS DIAS"): "MISSIAS DO MST",
    ("MT", "CARLOS AVALLONE"): "CARLOS AVALONE",        # o TSE escreve com um L
    ("SE", "Luizão Donatrampi"): "LUIZAO DONA TRAMPI",
}


def camara(exercicio):
    por_id = exercicio[exercicio.casa == "Câmara"].set_index("id_casa")
    linhas = []
    for dep in c.paginar_camara("deputados", {}):
        id_camara = str(dep["id"])
        tit = por_id.titulo.get(id_camara, "")
        if not tit:
            ficha = c.deputado(id_camara) or {}
            tit = ""  # ficha nova sem ligação: aparece na conferência
            print("sem ligação na etapa 2:", dep["nome"], ficha.get("nomeCivil"))
        linhas.append({"casa": "Câmara", "id_casa": id_camara, "nome_casa": dep["nome"],
                       "partido_casa": dep["siglaPartido"], "uf": dep["siglaUf"], "titulo": tit,
                       "em_exercicio": "Sim"})
    return pd.DataFrame(linhas)


def senado(exercicio):
    por_id = exercicio[exercicio.casa == "Senado"].set_index("id_casa")
    lista = c.senado("senador/lista/atual")
    linhas = []
    for p in c.como_lista(lista["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]):
        ident = p["IdentificacaoParlamentar"]
        cod = ident["CodigoParlamentar"]
        linhas.append({"casa": "Senado", "id_casa": cod, "nome_casa": ident["NomeParlamentar"],
                       "partido_casa": ident.get("SiglaPartidoParlamentar", ""),
                       "uf": ident.get("UfParlamentar", ""), "titulo": por_id.titulo.get(cod, ""),
                       "em_exercicio": "Sim"})
    return pd.DataFrame(linhas)


def assembleias():
    radar = c.ler_aba(c.id_planilha("SPREADSHEET_ID_RECANDIDATURAS"), ABA_ASSEMBLEIAS)
    base = c.consulta_cand(2022)
    base = base[base.DS_CARGO.isin(["DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL"]) & (base.NR_TURNO == "1")]
    base = base.assign(chave_civil=base.NM_CANDIDATO.map(c.chave_nome),
                       chave_urna=base.NM_URNA_CANDIDATO.map(lambda x: c.chave_nome(x, tirar_titulo=True)))
    base = base.assign(urna_inteira=base.NM_URNA_CANDIDATO.map(c.chave_nome))
    linhas, sem = [], []
    for _, r in radar.iterrows():
        uf, nome = r["UF"].strip(), r["Parlamentar"].strip()
        partido22 = c.campo(r, "Partido em 2022")
        da_uf = base[base.SG_UF == uf]
        tit = ligar(da_uf, nome, partido22)
        if not tit:
            sem.append((uf, nome, partido22))
        elif partido22:
            # casamento certo tem o partido de 2022 igual ao do registro ligado
            partido_tse = da_uf[da_uf.titulo == tit].SG_PARTIDO.iloc[0]
            c.falhar_se(partido_tse != partido22, f"{nome} ({uf}) ligado a registro de outro partido em "
                        f"2022: {partido_tse} no TSE, {partido22} no Radar")
        exercicio = c.campo(r, "Em exercício na assembleia?")
        linhas.append({"casa": "Assembleia", "id_casa": "", "nome_casa": nome,
                       "partido_casa": c.campo(r, "Partido", "Partido atual"), "uf": uf, "titulo": tit,
                       # o texto é "Não, eleito em 2022 e fora da lista de hoje"
                       "em_exercicio": "Não" if exercicio.startswith("Não") else "Sim"})
    saida = pd.DataFrame(linhas)
    saida["titulo"] = c.canonizar(saida.titulo)  # o registro de 2022 pode ter título antigo
    return saida, sem


def unico(candidatos, partido22):
    """Título se só sobrar uma pessoa; empate se resolve pelo partido de 2022."""
    if candidatos.titulo.nunique() > 1 and partido22:
        candidatos = candidatos[candidatos.SG_PARTIDO == partido22]
    return candidatos.titulo.iloc[0] if candidatos.titulo.nunique() == 1 else ""


def ligar(da_uf, nome, partido22):
    manual = MAPA_MANUAL.get((da_uf.SG_UF.iloc[0] if len(da_uf) else "", nome))
    if manual:
        return unico(da_uf[da_uf.urna_inteira == c.chave_nome(manual)], partido22)
    # 1) nome inteiro, com título: "CORONEL CHAGAS" não pode empatar com "CHAGAS ENFERMEIRO"
    inteira = c.chave_nome(nome)
    tit = unico(da_uf[(da_uf.urna_inteira == inteira) | (da_uf.chave_civil == inteira)], partido22)
    if tit:
        return tit
    # 2) sem título (Dr., Delegado...), com desempate pelo partido
    chave = c.chave_nome(nome, tirar_titulo=True)
    tit = unico(da_uf[(da_uf.chave_civil == chave) | (da_uf.chave_urna == chave)], partido22)
    if tit:
        return tit
    # 3) aproximado. Dentro do partido de 2022 o corte pode ser mais baixo ("Jorge Caruso" ->
    # CARUSO). Nome curto dá nota 1 contra qualquer um que o contenha, então o desempate é a
    # proporção sobre o nome maior: "Marcus Vinícius Kalume" fica com MARCUS VINICIUS
    # MALHEIROS KALUME e não com um "VINICIUS" do mesmo partido.
    pool = da_uf[da_uf.SG_PARTIDO == partido22] if partido22 else da_uf
    if pool.empty:
        return ""
    def nota(b):
        melhor = (0.0, 0.0)
        for outro in (b.chave_civil, b.chave_urna):
            ta, tb = set(chave.split()), set(outro.split())
            if ta and tb:
                comum = len(ta & tb)
                melhor = max(melhor, (comum / min(len(ta), len(tb)), comum / max(len(ta), len(tb))))
        return melhor
    notas = pool.apply(nota, axis=1)
    topo = max(notas)
    vencedores = pool[notas == topo].titulo.unique()
    corte = 0.6 if partido22 else 0.85
    return vencedores[0] if topo[0] >= corte and len(vencedores) == 1 else ""


def main():
    exercicio = c.ler_csv("exercicio_casas.csv")
    cam, sen = camara(exercicio), senado(exercicio)
    ass, sem_ligacao = assembleias()
    comp = pd.concat([cam, sen, ass], ignore_index=True)
    print(comp.groupby(["casa", "em_exercicio"]).size().to_string())

    c.falhar_se(len(cam) != 513, f"Câmara com {len(cam)} em exercício, esperado 513")
    c.falhar_se(len(sen) != 81, f"Senado com {len(sen)} em exercício, esperado 81")
    for casa, grupo in (("Câmara", cam), ("Senado", sen)):
        faltam = grupo[grupo.titulo == ""]
        c.falhar_se(len(faltam) > 0, f"{casa} sem ligação com o TSE: {faltam.nome_casa.tolist()}")
    repetidos = ass[(ass.titulo != "") & ass.duplicated("titulo", keep=False)]
    c.falhar_se(len(repetidos) > 0, "duas linhas das assembleias casaram com a mesma pessoa: "
                f"{repetidos[['uf', 'nome_casa']].values.tolist()}")
    if sem_ligacao:
        print(f"{len(sem_ligacao)} linhas das assembleias sem ligação (preencher MAPA_MANUAL):")
        for item in sem_ligacao:
            print("   ", item)
    c.salvar_csv(comp, "composicao_atual.csv")


if __name__ == "__main__":
    main()
