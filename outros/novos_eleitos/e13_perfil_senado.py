"""Etapa 13. Perfil geral dos senadores da próxima legislatura, parlamentar por parlamentar.

Primeira camada do mapeamento: tudo o que o Senado publica sobre cada nome, sem recorte de
tema. O afunilamento (saúde, depois imposto seletivo dentro de saúde) vem depois e parte
destas tabelas.

Universo: a aba "Senado eleito 2026" da planilha Congresso Eleito, pela coluna Situação.
  --grupo curso      eleitos em 2022, mandato até 2031 (27)
  --grupo reeleitos  reeleitos em 2026 (14)
  --grupo novos      novos no Senado (40); só têm registro aqui os que já exerceram como suplente

O que é buscado por senador (API de dados abertos do Senado, sem corte de data):
- ficha: nascimento, naturalidade, profissão, formação, mandatos, suplentes, bloco;
- colegiados: comissões, subcomissões, frentes e grupos de que é ou foi membro, e os cargos;
- autoria: toda matéria em que assina, marcando se é o primeiro autor;
- relatorias, com comissão e datas;
- emendas apresentadas;
- votações nominais (o voto em votação secreta vem só como "Votou");
- discursos, com o resumo e a indexação do próprio Senado e a nota taquigráfica (texto integral).

Tema: a classificação que o Senado atribui a cada matéria, traduzida para os temas oficiais da
Câmara (mesma tabela da etapa 11). Vale para autoria, relatoria e votação. Discurso e emenda
não têm classificação oficial e saem sem tema.

Saída: dados_novos_eleitos/perfil_senado_<grupo>/ com um CSV por tabela e um xlsx com todas.
Nada é gravado em planilha do Google.
Rodar: python -m outros.novos_eleitos.e13_perfil_senado --grupo curso
"""
import argparse
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
import requests

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.e11_trajetoria_competitivos import SENADO, senado_json, senado_para_camara

ABA = "Senado eleito 2026"
GRUPOS = {"curso": ("Mandato em curso",), "reeleitos": ("Reeleito", "Reeleita"), "novos": ("Novo", "Nova")}
LEGISLATIVAS = {"PL", "PLS", "PLP", "PLC", "PEC", "PDL", "PDS", "PRS", "MPV", "PLV"}
COLETIVOS = ("Frente Parlamentar", "Grupo Parlamentar")
PRIMEIRO_ANO_DISCURSO = 1995


def get(recurso, pasta, chave):
    return c.em_cache(pasta, chave, lambda: senado_json(f"{SENADO}/{recurso}"))


def processo(id_processo):
    return get(f"processo/{id_processo}", "senado_processo", str(id_processo)) or {}


# A tabela da etapa 11 joga toda a "Proteção Social" em previdência e assistência. Aqui cada
# subclasse vai para o tema da Câmara que lhe corresponde; calamidade não tem tema equivalente
# e fica só na classificação do Senado.
PROTECAO = "Política Social / Proteção Social / "
TRADUCAO_PROPRIA = {
    PROTECAO + "Assistência Social": "Previdência e Assistência Social",
    PROTECAO + "Desenvolvimento Social e Combate à Fome": "Previdência e Assistência Social",
    PROTECAO + "Crianças e Adolescentes": "Direitos Humanos e Minorias",
    PROTECAO + "Direitos Humanos e Minorias": "Direitos Humanos e Minorias",
    PROTECAO + "Idosos": "Direitos Humanos e Minorias",
    PROTECAO + "Mulheres": "Direitos Humanos e Minorias",
    PROTECAO + "Pessoas com Deficiência": "Direitos Humanos e Minorias",
    PROTECAO + "População Indígena": "Direitos Humanos e Minorias",
    PROTECAO + "Calamidade Pública e Emergência Social": "",
}


def traduzir(hierarquia):
    return TRADUCAO_PROPRIA[hierarquia] if hierarquia in TRADUCAO_PROPRIA else senado_para_camara(hierarquia)


def temas(id_processo):
    """Classificação do Senado e o tema correspondente no vocabulário da Câmara."""
    classes = [k["descricaoHierarquia"] for k in processo(id_processo).get("classificacoes") or []]
    traduzidos = [t for t in dict.fromkeys(traduzir(k) for k in classes) if t]
    return "; ".join(classes), "; ".join(traduzidos)


def codigos():
    """Código do Senado por nome parlamentar: em exercício e, para quem está fora, a legislatura."""
    atual = get("senador/lista/atual", "senado_perfil", f"lista_atual_{date.today()}")
    lista = atual["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]
    leg = get("senador/lista/legislatura/57", "senado_perfil", f"lista_57_{date.today()}")
    lista = lista + leg["ListaParlamentarLegislatura"]["Parlamentares"]["Parlamentar"]
    mapa = {}
    for p in lista:
        i = p["IdentificacaoParlamentar"]
        # na lista da legislatura quem não está em exercício traz a UF só dentro dos mandatos
        ufs = {i.get("UfParlamentar"), p.get("Mandato", {}).get("UfParlamentar")}
        ufs |= {m.get("UfParlamentar") for m in c.como_lista((p.get("Mandatos") or {}).get("Mandato"))}
        for nome in (i["NomeParlamentar"], i.get("NomeCompletoParlamentar", "")):
            for uf in ufs - {None}:
                mapa.setdefault((c.chave_nome(nome), uf), i["CodigoParlamentar"])
    return mapa


def em_exercicio():
    atual = get("senador/lista/atual", "senado_perfil", f"lista_atual_{date.today()}")
    return {p["IdentificacaoParlamentar"]["CodigoParlamentar"]: p
            for p in atual["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]}


def universo(grupo):
    aba = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), ABA))
    aba = aba[aba["Situação"].isin(GRUPOS[grupo])].copy()
    mapa = codigos()
    aba["cod"] = [mapa.get((c.chave_nome(n), uf)) or mapa.get((c.chave_nome(civil), uf))
                  for n, civil, uf in zip(aba["Parlamentar"], aba["Nome civil (TSE)"], aba["UF"])]
    # quem passou pelo Senado em legislatura anterior (ex-senador, suplente que exerceu): etapa 2,
    # ligado pelo título ou CPF da candidatura de 2026 e, sem eles, pelo nome civil
    sen = c.ler_csv("exercicio_casas.csv")
    sen = sen[sen.casa == "Senado"]
    cands = c.ler_csv("candidaturas_2026.csv").drop_duplicates("sq_candidato").set_index("sq_candidato")
    for i, l in aba[aba["cod"].isna()].iterrows():
        sq = l["SQ_CANDIDATO"]
        titulo, cpf = (cands.titulo.get(sq, ""), cands.cpf.get(sq, "")) if sq else ("", "")
        civil = c.chave_nome(l["Nome civil (TSE)"])
        achado = sen[((sen.titulo == titulo) & (titulo != "")) | ((sen.cpf == cpf) & (cpf != ""))
                     | ((sen.nome_civil.map(c.chave_nome) == civil) & (civil != ""))]
        if len(achado):
            aba.at[i, "cod"] = achado.id_casa.iloc[0]
    # quem a lista do Senado dá em exercício e a aba não traz entra marcado, sem mexer na aba
    todas = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), ABA))
    na_aba = {(c.chave_nome(n), uf) for n, uf in zip(todas["Parlamentar"], todas["UF"])}
    na_aba |= {(c.chave_nome(n), uf) for n, uf in zip(todas["Nome civil (TSE)"], todas["UF"])}
    extras = []
    for cod, p in em_exercicio().items():
        i = p["IdentificacaoParlamentar"]
        chaves = {(c.chave_nome(i["NomeParlamentar"]), i["UfParlamentar"]),
                  (c.chave_nome(i["NomeCompletoParlamentar"]), i["UfParlamentar"])}
        fim = (p["Mandato"].get("SegundaLegislaturaDoMandato") or {}).get("DataFim", "")[:4]
        if grupo == "curso" and fim == "2031" and not chaves & na_aba:
            extras.append({"Parlamentar": i["NomeParlamentar"], "Partido": i.get("SiglaPartidoParlamentar", ""),
                           "UF": i["UfParlamentar"], "Situação": "Mandato em curso", "Mandato até": "2031",
                           "Observação": "Em exercício na lista do Senado e fora da aba Senado eleito 2026.",
                           "cod": cod})
    exercendo = set(em_exercicio())
    aba["Em exercício hoje (lista do Senado)"] = ["Sim" if k in exercendo else "Não" for k in aba["cod"]]
    if extras:
        aba = pd.concat([aba, pd.DataFrame(extras).assign(**{"Em exercício hoje (lista do Senado)": "Sim"})],
                        ignore_index=True).fillna({"Observação": ""})
    return aba


def ficha(linha):
    cod = linha["cod"]
    d = get(f"senador/{cod}", "senado_perfil", f"detalhe_{cod}")["DetalheParlamentar"]["Parlamentar"]
    basicos = d.get("DadosBasicosParlamentar", {})
    prof = get(f"senador/{cod}/profissao", "senado_perfil", f"profissao_{cod}")
    prof = c.como_lista((prof["HistoricoAcademicoParlamentar"]["Parlamentar"].get("Profissoes") or {}).get("Profissao"))
    curso = get(f"senador/{cod}/historicoAcademico", "senado_perfil", f"academico_{cod}")
    curso = c.como_lista((curso["HistoricoAcademicoParlamentar"]["Parlamentar"].get("HistoricoAcademico") or {}).get("Curso"))
    mand = get(f"senador/{cod}/mandatos", "senado_perfil", f"mandatos_{cod}")
    mand = c.como_lista((mand["MandatoParlamentar"]["Parlamentar"].get("Mandatos") or {}).get("Mandato"))
    descr = []
    for m in mand:
        ini = m.get("PrimeiraLegislaturaDoMandato", {}).get("DataInicio", "")[:4]
        fim = (m.get("SegundaLegislaturaDoMandato") or m.get("PrimeiraLegislaturaDoMandato", {})).get("DataFim", "")[:4]
        descr.append(f"{m.get('DescricaoParticipacao', '')} {m.get('UfParlamentar', '')} {ini} a {fim}")
    atual = max(mand, key=lambda m: m.get("PrimeiraLegislaturaDoMandato", {}).get("DataInicio", ""), default={})
    sup = c.como_lista((atual.get("Suplentes") or {}).get("Suplente"))
    return {
        "Parlamentar": linha["Parlamentar"], "Partido": linha["Partido"], "UF": linha["UF"],
        "Situação": linha["Situação"], "Mandato até": linha["Mandato até"], "Código no Senado": cod,
        "Em exercício hoje (lista do Senado)": linha["Em exercício hoje (lista do Senado)"],
        "Nascimento": basicos.get("DataNascimento", ""),
        "Naturalidade": f"{basicos.get('Naturalidade', '')}/{basicos.get('UfNaturalidade', '')}".strip("/"),
        "Profissão": "; ".join(p.get("NomeProfissao", "") for p in prof),
        "Formação": "; ".join(f"{k.get('NomeCurso', '')} ({k.get('GrauInstrucao', '')})" for k in curso),
        "Mandatos no Senado": "; ".join(descr),
        "Participação no mandato atual": atual.get("DescricaoParticipacao", ""),
        "Suplentes ou titular do mandato atual": "; ".join(
            f"{s.get('DescricaoParticipacao', '')}: {s.get('NomeParlamentar', '')}" for s in sup),
        "Observação": linha.get("Observação", ""),
        "Página no Senado": f"https://www25.senado.leg.br/web/senadores/senador/-/perfil/{cod}",
    }


def colegiados(linha):
    cod = linha["cod"]
    hoje = str(date.today())
    com = get(f"senador/{cod}/comissoes", "senado_perfil", f"comissoes_{cod}")
    com = c.como_lista((com["MembroComissaoParlamentar"]["Parlamentar"].get("MembroComissoes") or {}).get("Comissao"))
    car = get(f"senador/{cod}/cargos", "senado_perfil", f"cargos_{cod}")
    car = c.como_lista((car["CargoParlamentar"]["Parlamentar"].get("Cargos") or {}).get("Cargo"))
    saida = []
    for x, papel in [(k, k.get("DescricaoParticipacao", "")) for k in com] + \
                    [(k, k.get("DescricaoCargo", "").capitalize()) for k in car]:
        i = x.get("IdentificacaoComissao", {})
        nome = i.get("NomeComissao", "")
        saida.append({
            "Parlamentar": linha["Parlamentar"], "Sigla": i.get("SiglaComissao", ""), "Colegiado": nome,
            "Casa": i.get("SiglaCasaComissao", ""),
            "Tipo": "Frente ou grupo" if nome.startswith(COLETIVOS) else "Comissão ou subcomissão",
            "Papel": papel, "Início": x.get("DataInicio", ""), "Fim": x.get("DataFim", ""),
            "Vigente": "Sim" if not x.get("DataFim") or x["DataFim"] >= hoje else "Não"})
    return saida


def autoria(linha):
    cod = linha["cod"]
    lista = get(f"processo?codigoParlamentarAutor={cod}", "senado_processos", cod) or []
    saida = []
    for p in lista:
        sigla = p.get("identificacao", "").split(" ")[0]
        papel, senado_tema, tema = "", "", ""
        if sigla in LEGISLATIVAS:
            autores = (processo(p["id"]).get("documento") or {}).get("autoria") or []
            primeiro = min(autores, key=lambda a: a.get("ordem", 99), default={})
            papel = "Primeiro autor" if str(primeiro.get("codigoParlamentar")) == str(cod) else "Coautor"
            senado_tema, tema = temas(p["id"])
        saida.append({
            "Parlamentar": linha["Parlamentar"], "Matéria": p.get("identificacao", ""), "Tipo": sigla,
            "Apresentação": p.get("dataApresentacao", ""), "Papel": papel, "Ementa": p.get("ementa", ""),
            "Classificação do Senado": senado_tema, "Tema": tema,
            "Tramitando": p.get("tramitando", ""), "Autoria": p.get("autoria", "")[:300],
            "Link": f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{p.get('codigoMateria', '')}"})
    return saida


def relatorias(linha):
    cod = linha["cod"]
    lista = get(f"processo/relatoria?codigoParlamentar={cod}", "senado_perfil", f"relatoria_{cod}") or []
    saida = []
    for r in lista:
        sigla = r.get("identificacaoProcesso", "").split(" ")[0]
        senado_tema, tema = temas(r["idProcesso"]) if sigla in LEGISLATIVAS else ("", "")
        saida.append({
            "Parlamentar": linha["Parlamentar"], "Matéria": r.get("identificacaoProcesso", ""), "Tipo": sigla,
            "Colegiado": r.get("siglaColegiado", ""), "Papel": r.get("descricaoTipoRelator", ""),
            "Designação": str(r.get("dataDesignacao") or "")[:10], "Destituição": str(r.get("dataDestituicao") or "")[:10],
            "Motivo do encerramento": r.get("descricaoTipoEncerramento") or "",
            "Ementa": r.get("ementaProcesso", ""), "Autoria da matéria": (r.get("autoriaProcesso") or "")[:200],
            "Classificação do Senado": senado_tema, "Tema": tema,
            "Link": f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{r.get('codigoMateria', '')}"})
    return saida


def emendas(linha):
    cod = linha["cod"]
    lista = get(f"processo/emenda?codigoParlamentarAutor={cod}", "senado_perfil", f"emenda_{cod}") or []
    return [{
        "Parlamentar": linha["Parlamentar"], "Emenda": e.get("identificacao", ""),
        "Matéria emendada": e.get("identificacao", "").split(" - ")[-1],
        "Apresentação": e.get("dataApresentacao", ""), "Colegiado": e.get("siglaColegiado", ""),
        "O que a emenda faz": e.get("descricaoDocumentoEmenda") or "", "Autoria": (e.get("autoria") or "")[:200],
        "Link": e.get("urlDocumentoEmenda") or ""} for e in lista]


def votos(linha):
    cod = linha["cod"]
    lista = get(f"votacao?codigoParlamentar={cod}&dataInicio=1995-01-01&dataFim={date.today()}",
                "senado_perfil", f"votacao_{cod}_{date.today()}") or []
    saida = []
    for v in lista:
        voto = next((x for x in v.get("votos") or [] if str(x.get("codigoParlamentar")) == str(cod)), {})
        senado_tema, tema = temas(v["idProcesso"]) if v.get("idProcesso") else ("", "")
        saida.append({
            "Parlamentar": linha["Parlamentar"], "Data": v.get("dataSessao", ""), "Matéria": v.get("identificacao", ""),
            "Tipo": v.get("sigla", ""), "Votação": v.get("descricaoVotacao", ""),
            "Voto": voto.get("siglaVotoParlamentar", ""), "Secreta": "Sim" if v.get("votacaoSecreta") == "S" else "Não",
            "Sim": v.get("totalVotosSim"), "Não": v.get("totalVotosNao"), "Abstenção": v.get("totalVotosAbstencao"),
            "Ementa": v.get("ementa", ""), "Classificação do Senado": senado_tema, "Tema": tema,
            "Link": f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{v.get('codigoMateria', '')}"})
    return saida


def discursos(linha, primeiro_ano):
    cod = linha["cod"]
    saida = []
    # a API devolve no máximo um ano por chamada
    for ano in range(primeiro_ano, date.today().year + 1):
        fechado = ano < date.today().year
        d = get(f"senador/{cod}/discursos?dataInicio={ano}0101&dataFim={ano}1231", "senado_perfil",
                f"discursos_{cod}_{ano}" + ("" if fechado else f"_{date.today()}"))
        lista = c.como_lista((d["DiscursosParlamentar"]["Parlamentar"].get("Pronunciamentos") or {}).get("Pronunciamento"))
        for p in lista:
            saida.append({
                "Parlamentar": linha["Parlamentar"], "Data": p.get("DataPronunciamento", ""),
                "Tipo": p.get("TipoUsoPalavra", {}).get("Descricao", ""), "Casa": p.get("SiglaCasaPronunciamento", ""),
                "Resumo": p.get("TextoResumo", ""), "Indexação": p.get("Indexacao", ""), "Link": p.get("UrlTexto", ""),
                "Código": p.get("CodigoPronunciamento", "")})
    return saida


def texto_integral(codigo):
    """Nota taquigráfica do pronunciamento, em texto puro. Vazio quando o Senado não publicou."""
    if not codigo:
        return ""
    p = c.caminho("cache") / "senado_discurso_texto" / f"{codigo}.txt"
    if p.exists():
        return p.read_text()
    texto = ""
    for tentativa in range(4):
        try:
            r = requests.get(f"{SENADO}/discurso/texto-integral/{codigo}", timeout=60)
            if r.status_code in (200, 404):
                texto = r.content.decode("utf-8", "replace").replace("\xa0", " ").strip() if r.status_code == 200 else ""
                break
        except requests.RequestException:
            pass
        time.sleep(3 * (tentativa + 1))
    else:
        return ""  # sem resposta: não grava cache, a próxima rodada tenta de novo
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.{threading.get_ident()}.tmp")
    tmp.write_text(texto)
    os.replace(tmp, p)
    return texto


def contar(df, coluna="Tema"):
    return Counter(t for v in df[coluna] for t in str(v).split("; ") if t)


def resumo(f, tab):
    """Uma linha por senador com as contagens; nenhuma leitura de posição."""
    nome = f["Parlamentar"]
    de = lambda t: tab[t][tab[t]["Parlamentar"] == nome] if len(tab[t]) else tab[t]
    col, aut, rel, vot = de("colegiados"), de("autoria"), de("relatorias"), de("votos")
    vigentes = col[col["Vigente"] == "Sim"] if len(col) else col
    membro = vigentes[vigentes["Papel"].isin(["Titular", "Suplente"])] if len(col) else col
    lista = lambda d, tipo, papeis=None: "; ".join(sorted(
        f"{s} ({p})" for s, p, t in zip(d["Sigla"], d["Papel"], d["Tipo"]) if t == tipo and (not papeis or p in papeis)))
    principal = aut[aut["Papel"] == "Primeiro autor"] if len(aut) else aut
    top = lambda d: "; ".join(f"{t} ({n})" for t, n in contar(d).most_common(5)) if len(d) else ""
    abertas = vot[vot["Secreta"] == "Não"] if len(vot) else vot
    return {**f,
            "Comissões hoje (titular)": lista(membro, "Comissão ou subcomissão", ["Titular"]) if len(col) else "",
            "Comissões hoje (suplente)": lista(membro, "Comissão ou subcomissão", ["Suplente"]) if len(col) else "",
            "Cargos em colegiados hoje": "; ".join(sorted(
                f"{p} de {s}" for s, p in zip(vigentes["Sigla"], vigentes["Papel"]) if p not in ("Titular", "Suplente")))
            if len(col) else "",
            "Frentes e grupos hoje": str(len(membro[membro["Tipo"] == "Frente ou grupo"])) if len(col) else "0",
            "Matérias legislativas como primeiro autor": len(principal),
            "Temas da autoria principal (5 maiores)": top(principal),
            "Matérias legislativas como coautor": len(aut[aut["Papel"] == "Coautor"]) if len(aut) else 0,
            "Requerimentos e outros": len(aut[aut["Papel"] == ""]) if len(aut) else 0,
            "Relatorias": len(rel),
            "Relatorias de matéria legislativa": len(rel[rel["Tema"].notna() & rel["Tipo"].isin(LEGISLATIVAS)]) if len(rel) else 0,
            "Temas das relatorias (5 maiores)": top(rel),
            "Colegiados em que mais relatou": "; ".join(f"{s} ({n})" for s, n in Counter(rel["Colegiado"]).most_common(5)) if len(rel) else "",
            "Emendas apresentadas": len(de("emendas")),
            "Votações nominais abertas": len(abertas),
            "Votos Sim / Não / Abstenção": "{Sim} / {Não} / {Abstenção}".format(
                **{k: int((abertas["Voto"] == k).sum()) if len(abertas) else 0 for k in ("Sim", "Não", "Abstenção")}),
            "Discursos e apartes registrados": len(de("discursos"))}


def temas_por_senador(tab):
    linhas = []
    for nome in tab["ficha"]["Parlamentar"]:
        de = lambda t: tab[t][tab[t]["Parlamentar"] == nome] if len(tab[t]) else tab[t]
        aut, rel, vot = de("autoria"), de("relatorias"), de("votos")
        a = contar(aut[aut["Papel"] == "Primeiro autor"]) if len(aut) else Counter()
        k = contar(aut[aut["Papel"] == "Coautor"]) if len(aut) else Counter()
        r = contar(rel) if len(rel) else Counter()
        v = contar(vot[vot["Secreta"] == "Não"]) if len(vot) else Counter()
        for t in sorted(set(a) | set(k) | set(r) | set(v)):
            linhas.append({"Parlamentar": nome, "Tema": t, "Primeiro autor": a[t], "Coautor": k[t],
                           "Relatorias": r[t], "Votações nominais abertas": v[t]})
    return pd.DataFrame(linhas)


def classificacao_por_senador(tab):
    """A mesma contagem na classificação do próprio Senado, que é mais fina (Saúde / Saúde Pública,
    Tributos / Desoneração Fiscal). É por ela que o funil por subtema começa."""
    linhas = []
    col = "Classificação do Senado"
    for nome in tab["ficha"]["Parlamentar"]:
        de = lambda t: tab[t][tab[t]["Parlamentar"] == nome] if len(tab[t]) else tab[t]
        aut, rel, vot = de("autoria"), de("relatorias"), de("votos")
        a = contar(aut[aut["Papel"] == "Primeiro autor"], col) if len(aut) else Counter()
        k = contar(aut[aut["Papel"] == "Coautor"], col) if len(aut) else Counter()
        r = contar(rel, col) if len(rel) else Counter()
        v = contar(vot[vot["Secreta"] == "Não"], col) if len(vot) else Counter()
        for t in sorted(set(a) | set(k) | set(r) | set(v)):
            niveis = t.split(" / ")
            linhas.append({"Parlamentar": nome, "Área": niveis[0], "Assunto": " / ".join(niveis[1:]),
                           "Primeiro autor": a[t], "Coautor": k[t], "Relatorias": r[t], "Votações nominais abertas": v[t]})
    return pd.DataFrame(linhas)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grupo", choices=GRUPOS, default="curso")
    ap.add_argument("--desde-discursos", type=int, default=PRIMEIRO_ANO_DISCURSO)
    grupo = ap.parse_args().grupo
    desde = ap.parse_args().desde_discursos

    uni = universo(grupo)
    sem = uni[uni["cod"].isna()]
    if len(sem):
        print(f"Sem código no Senado (nunca exerceram): {len(sem)}: " + "; ".join(sem["Parlamentar"]))
    linhas = uni[uni["cod"].notna()].to_dict("records")
    print(f"{grupo}: {len(uni)} nomes, {len(linhas)} com registro no Senado")
    if not linhas:
        return

    partes = {"colegiados": colegiados, "autoria": autoria, "relatorias": relatorias, "emendas": emendas,
              "votos": votos, "discursos": lambda l: discursos(l, desde)}
    tab = {"ficha": pd.DataFrame([ficha(l) for l in linhas])}
    for nome, f in partes.items():
        with ThreadPoolExecutor(3) as ex:
            tab[nome] = pd.DataFrame([x for bloco in ex.map(f, linhas) for x in bloco])
        print(f"  {nome}: {len(tab[nome])} linhas")
    if len(tab["discursos"]):
        with ThreadPoolExecutor(4) as ex:
            tab["discursos"]["Texto"] = list(ex.map(texto_integral, tab["discursos"]["Código"]))
        print(f"  texto integral em {int((tab['discursos']['Texto'] != '').sum())} de {len(tab['discursos'])} discursos")
    tab["temas"] = temas_por_senador(tab)
    tab["classificacao"] = classificacao_por_senador(tab)
    tab["resumo"] = pd.DataFrame([resumo(f, tab) for f in tab["ficha"].to_dict("records")])

    pasta = c.caminho(f"perfil_senado_{grupo}")
    pasta.mkdir(parents=True, exist_ok=True)
    ordem = ["resumo", "temas", "classificacao", "colegiados", "autoria", "relatorias", "emendas", "votos", "discursos"]
    with pd.ExcelWriter(pasta / f"perfil_senado_{grupo}.xlsx") as xl:
        for nome in ordem:
            tab[nome].to_csv(pasta / f"{nome}.csv", index=False)
            # a nota taquigráfica fica só no CSV: passa do limite de caracteres de uma célula
            tab[nome].drop(columns=["Texto"], errors="ignore").to_excel(xl, sheet_name=nome, index=False)
    print(f"Gravado em {pasta}")


if __name__ == "__main__":
    main()
