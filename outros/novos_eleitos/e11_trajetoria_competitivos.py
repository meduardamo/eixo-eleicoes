"""Etapa 11. Trajetória e temas dos competitivos do Senado, com fonte.

Substitui o preenchimento de memória que estava em dados_pesquisa_senado.py nas colunas de
pesquisa da aba "Senado competitivos, pesquisa manual (pré-mapeados)". Cada informação agora
vem de uma fonte com link, e o que não tem fonte fica marcado "a conferir".

- Cargos e mandatos: ficha (infobox) do verbete na Wikipedia em português. A página só é
  aceita se trouxer o ano de nascimento do TSE e o nome da UF. Vale para cargo não eletivo
  (ministro, secretário, presidente de estatal ou autarquia) e para mandato que começou até
  2006, que as bases do TSE baixadas não cobrem.
- Pasta: o cargo não eletivo é traduzido para a lista de temas oficiais da Câmara, para a
  pasta e o tema legislativo falarem a mesma língua e o painel poder contar.
- Tema: proposições de autoria principal (primeiro signatário) na Câmara, 2003 a 2026, com o
  tema que a própria Câmara atribui, e no Senado, com a classificação do Senado traduzida para
  os temas da Câmara. Fora homenagem e data comemorativa. Entram até 3 temas, por número de
  proposições, entre os que a pessoa apresenta acima da média da Casa. Com menos de 10
  proposições com tema não sai tema. Só autoria: levantamento do IU e coautoria não entram.
- O que a pesquisa anterior dizia e nenhuma fonte confirma vai para a aba longa com
  "A conferir (pesquisa anterior sem fonte)". Profissão não entra: está em "Ocupação declarada
  ao TSE".

Saídas:
- a aba de pesquisa manual, uma linha por pessoa, com as colunas que o painel de apuração lê
  (origens_eleitos.MANUAIS) e duas novas: "Temática: como foi medida" e "Checagem";
- a aba "Senado competitivos, cargos com fonte", uma linha por cargo.
Rodar: python -m outros.novos_eleitos.e11_trajetoria_competitivos [--publicar]
Precisa: e10 publicada (lê a aba Mapeamento Senado e a de pesquisa manual) e, na primeira vez,
os arquivos anuais da Câmara em dados_novos_eleitos/camara_autores e camara_temas_ano
(baixados sozinhos se faltarem).
"""
import argparse
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.dados_pesquisa_senado import PESQUISA_SENADO_74
from outros.novos_eleitos.verificacao_competitivos import VERIFICADOS

ABA_PESSOA = "Senado competitivos, pesquisa manual (pré-mapeados)"
ABA_CARGOS = "Senado competitivos, cargos com fonte"
ABA_MAPEAMENTO = "Mapeamento Senado"
WIKI_API = "https://pt.wikipedia.org/w/api.php"
WIKI_URL = "https://pt.wikipedia.org/wiki/"
UA = "EixoTrajetoriaBot/1.0 (https://github.com/meduardamo; pesquisa de trajetoria de candidatos) python-requests"
ARQ_CAMARA = "https://dadosabertos.camara.leg.br/arquivos/{0}/csv/{0}-{1}.csv"
ANOS_CAMARA = range(2003, 2027)
TIPOS = {"PL", "PLP", "PEC", "PDL"}
SIGLAS_SENADO = {"PL", "PLS", "PLP", "PEC", "PDL", "PDS"}
FORA = {"Homenagens e Datas Comemorativas"}
MINIMO_PROPOSICOES = 10
A_CONFERIR = "A conferir (pesquisa anterior sem fonte)"

UF_NOME = {"AC": "Acre", "AL": "Alagoas", "AM": "Amazonas", "AP": "Amapá", "BA": "Bahia", "CE": "Ceará",
           "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás", "MA": "Maranhão",
           "MG": "Minas Gerais", "MS": "Mato Grosso do Sul", "MT": "Mato Grosso", "PA": "Pará",
           "PB": "Paraíba", "PE": "Pernambuco", "PI": "Piauí", "PR": "Paraná", "RJ": "Rio de Janeiro",
           "RN": "Rio Grande do Norte", "RO": "Rondônia", "RR": "Roraima", "RS": "Rio Grande do Sul",
           "SC": "Santa Catarina", "SE": "Sergipe", "SP": "São Paulo", "TO": "Tocantins"}

# Verbete escolhido à mão quando a busca não acha ou acha errado. Chave: SQ_CANDIDATO.
WIKI_MANUAL = {}

# Tipo do cargo, na ordem: o primeiro padrão que casar decide.
TIPO_CARGO = [
    ("Função parlamentar (Mesa, frente)", r"presidente d[ao] (câmara|assembl[ée]ia|senado|parlamento|ale[a-z]{2,3}\b)|"
                                         r"secretári[oa] da mesa|mesa diretora|frente parlamentar"),
    ("Entidade de classe ou sociedade civil", r"sindicato|\bcut\b|federa[çc][ãa]o das ind|cooperativ|instituto e se|\bune\b|estudant"),
    ("Direção partidária", r"presidente (nacional|estadual|municipal) d|funda[çc][ãa]o ulysses|presidente.*\b(partido|psdb|psb|pt|pdt|mdb|pl|solidariedade|uni[ãa]o progressista|federa[çc][ãa]o)\b"),
    ("Cônjuge de chefe do Executivo", r"primeir[ao][ -](dama|cavalheiro)"),
    ("Mandato eletivo", r"deputad|senador|vereador|prefeit|governador|constituinte|presidente da república"),
    ("Carreira pública", r"procurador|delegad|policial|capitão|coronel|oficial d|general|brigada|comandante d"),
    ("Profissão", r"^(advogad|jornalista|empresári|engenheir|médic|produtor|publicitári|psicólog|professor|"
                  r"bancári|comerciante|pastor|treinador|servidor|cientista|radialista|veterinári|apresentador)"),
]
ENTRA_NO_RESUMO = {"Nomeação ou função pública", "Carreira pública", "Cônjuge de chefe do Executivo"}

# Pasta de cargo não eletivo -> tema oficial da Câmara. Primeiro padrão que casar decide.
PASTA = [
    (r"sa[úu]de", "Saúde"),
    (r"educa[çc][ãa]o|ensino", "Educação"),
    (r"seguran[çc]a|pol[íi]cia|delegad|policial|capitão|coronel|\bpm\b|narc[óo]ticos|denarc|defesa civil|brigada|"
     r"ex[ée]rcito|militar|estrat[ée]gic", "Defesa e Segurança"),
    (r"reitor|universidade", "Educação"),
    (r"agr[áa]ri", "Estrutura Fundiária"),
    (r"ind[íi]gena", "Direitos Humanos e Minorias"),
    (r"procurador|justi[çc]a|minist[ée]rio p[úu]blico|lava jato", "Direito e Justiça"),
    (r"comunica[çc]|secom", "Comunicações"),
    (r"fazenda|planejamento|or[çc]amento|financeir|finan[çc]as|tesouro|caixa econ", "Finanças Públicas e Orçamento"),
    (r"agricultura|pesca|aquicultura|pecu[áa]ria|abastecimento", "Agricultura, Pecuária, Pesca e Extrativismo"),
    (r"meio ambiente|mudan[çc]a do clima", "Meio Ambiente e Desenvolvimento Sustentável"),
    (r"itaipu|energia|recursos h[íi]dricos|minas", "Energia, Recursos Hídricos e Minerais"),
    (r"infraestrutura|obras|portos|transporte", "Viação, Transporte e Mobilidade"),
    (r"cidades|urban|habita[çc][ãa]o|integra[çc][ãa]o nacional|reconstru[çc][ãa]o", "Cidades e Desenvolvimento Urbano"),
    (r"turismo", "Turismo"),
    (r"trabalho|emprego|qualifica[çc][ãa]o", "Trabalho e Emprego"),
    (r"desenvolvimento econ[ôo]mico|desenvolvimento( de)?\s*$", "Economia"),
    (r"assist[êe]ncia|social|cidadania|volunt[áa]ri|desenvolvimento social", "Previdência e Assistência Social"),
    (r"direitos humanos|juventude|mulher|igualdade", "Direitos Humanos e Minorias"),
    (r"metrologia|ind[úu]stria|com[ée]rcio(?! exterior)", "Indústria, Comércio e Serviços"),
    (r"apex|com[ée]rcio exterior|rela[çc][õo]es exteriores", "Relações Internacionais e Comércio Exterior"),
    (r"ci[êe]ncia|tecnologia", "Ciência, Tecnologia e Inovação"),
    (r"cultura", "Arte, Cultura e Religião"),
    (r"esporte", "Esporte e Lazer"),
    (r"casa civil|governo|rela[çc][õo]es institucionais|gest[ãa]o|administra|reestrutura|cons[óo]rcio|"
     r"munic[íi]pios|appm|particular|extraordin|participa[çc][ãa]o popular", "Administração Pública"),
]

# Classificação do Senado (primeiro e segundo nível) -> tema oficial da Câmara. Procura do nível
# mais fino para o mais geral; "" descarta (homenagem, classe genérica demais).
SENADO_PARA_CAMARA = {
    "Organização do Estado / Organização Federativa": "Administração Pública",
    "Organização do Estado / Poder Judiciário": "Direito e Justiça",
    "Organização do Estado / Poder Legislativo": "Processo Legislativo e Atuação Parlamentar",
    "Organização do Estado / Fiscalização e Controle": "Administração Pública",
    "Organização do Estado / Funções Essenciais à Justiça": "Direito e Justiça",
    "Organização do Estado": "Administração Pública",
    "Política Social / Proteção Social": "Previdência e Assistência Social",
    "Política Social / Previdência Social": "Previdência e Assistência Social",
    "Política Social / Trabalho e Emprego": "Trabalho e Emprego",
    "Política Social / Saúde": "Saúde",
    "Política Social / Educação": "Educação",
    "Política Social / Desporto e Lazer": "Esporte e Lazer",
    "Política Social / Desenvolvimento Urbano": "Cidades e Desenvolvimento Urbano",
    "Política Social / Habitação": "Cidades e Desenvolvimento Urbano",
    "Política Social / Cultura": "Arte, Cultura e Religião",
    "Política Social": "",
    "Jurídico / Direito Eleitoral": "Política, Partidos e Eleições",
    "Jurídico / Processo": "Direito Civil e Processual Civil",
    "Jurídico / Direito Penal e Penitenciário": "Direito Penal e Processual Penal",
    "Jurídico / Direitos e Garantias": "Direitos Humanos e Minorias",
    "Jurídico / Direito do Consumidor": "Direito e Defesa do Consumidor",
    "Jurídico / Direito Civil": "Direito Civil e Processual Civil",
    "Jurídico / Direito de Trânsito": "Viação, Transporte e Mobilidade",
    "Jurídico / Direito Empresarial e Econômico": "Economia",
    "Jurídico / Direito Notarial e Registral": "Direito Civil e Processual Civil",
    "Jurídico": "Direito e Justiça",
    "Economia e Desenvolvimento / Tributos": "Finanças Públicas e Orçamento",
    "Economia e Desenvolvimento / Finanças Públicas": "Finanças Públicas e Orçamento",
    "Economia e Desenvolvimento / Desenvolvimento Regional": "Economia",
    "Economia e Desenvolvimento / Fiscalização e Controle da Atividade Econômica": "Economia",
    "Economia e Desenvolvimento / Sistema Financeiro Nacional": "Economia",
    "Economia e Desenvolvimento / Indústria, Comércio e Serviços": "Indústria, Comércio e Serviços",
    "Economia e Desenvolvimento / Ciência, Tecnologia e Informática": "Ciência, Tecnologia e Inovação",
    "Economia e Desenvolvimento / Agropecuária e Abastecimento": "Agricultura, Pecuária, Pesca e Extrativismo",
    "Economia e Desenvolvimento / Política Fundiária e Reforma Agrária": "Estrutura Fundiária",
    "Economia e Desenvolvimento": "Economia",
    "Administração Pública": "Administração Pública",
    "Meio Ambiente / Recursos Hídricos": "Energia, Recursos Hídricos e Minerais",
    "Meio Ambiente": "Meio Ambiente e Desenvolvimento Sustentável",
    "Soberania, Defesa Nacional e Ordem Pública / Relações Internacionais": "Relações Internacionais e Comércio Exterior",
    "Soberania, Defesa Nacional e Ordem Pública": "Defesa e Segurança",
    "Infraestrutura / Minas e Energia": "Energia, Recursos Hídricos e Minerais",
    "Infraestrutura / Comunicações": "Comunicações",
    "Infraestrutura / Viação e Transportes": "Viação, Transporte e Mobilidade",
    "Infraestrutura": "Viação, Transporte e Mobilidade",
    "Orçamento Público": "Finanças Públicas e Orçamento",
    "Honorífico": "",
}



def sem_acento(s):
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch)).lower()


# ---------------------------------------------------------------- Wikipedia

_S = requests.Session()
_S.headers["User-Agent"] = UA


def wiki(**params):
    """A Wikipedia devolve 429 com pressa: um pedido por segundo e espera o Retry-After."""
    for i in range(8):
        r = _S.get(WIKI_API, params=dict(params, format="json", maxlag=5), timeout=60)
        if r.status_code == 200 and r.text.startswith("{"):
            time.sleep(1.0)
            return r.json()
        time.sleep(int(r.headers.get("Retry-After", 0) or 0) or 10 * (i + 1))
    raise RuntimeError(f"Wikipedia não respondeu: {params}")


def chave_cache(t):
    return re.sub(r"\W+", "_", t)


def busca(q):
    return c.em_cache("wiki_busca", chave_cache(q), lambda: [
        x["title"] for x in wiki(action="query", list="search", srsearch=q, srlimit=5)["query"]["search"]])


def pagina(titulo):
    def buscar():
        p = wiki(action="query", prop="revisions", rvprop="content", rvslots="main", titles=titulo,
                 redirects=1, formatversion=2)["query"]["pages"][0]
        return {"titulo": p.get("title"), "texto": p["revisions"][0]["slots"]["main"]["content"]} if "revisions" in p else None
    return c.em_cache("wiki_texto", chave_cache(titulo), buscar)


def achar_verbete(sq, nome, urna, civil, nascimento, uf):
    """Primeiro verbete com ficha que traga o ano de nascimento e o nome da UF."""
    if sq in WIKI_MANUAL:
        return WIKI_MANUAL[sq]
    ano = nascimento[:4]
    for q in (f"{nome} político", civil.title(), f"{urna.title()} político"):
        for t in busca(q):
            p = pagina(t)
            if p and "{{Info/" in p["texto"][:6000] and ano in p["texto"][:6000] and UF_NOME[uf] in p["texto"]:
                return p["titulo"]
    return ""


def ficha(texto):
    i = texto.find("{{Info/")
    profundidade, j = 0, i
    while j < len(texto):
        if texto.startswith("{{", j):
            profundidade += 1
            j += 2
        elif texto.startswith("}}", j):
            profundidade -= 1
            j += 2
            if profundidade == 0:
                break
        else:
            j += 1
    campos = {}
    # Ficha escrita um campo por linha: separar por quebra de linha seguida de |. Contar
    # chaves falha com {{efn}} e ref aberta. Campo repetido: vale o primeiro.
    for parte in re.split(r"\n\s*\|", texto[i:j])[1:]:
        if "=" in parte:
            k, v = parte.split("=", 1)
            campos.setdefault(k.strip(), v.strip())
    return campos


def limpar(s):
    s = re.sub(r"<ref[^>]*/>", "", s)
    s = re.sub(r"<ref.*?</ref>", "", s, flags=re.S)
    s = re.sub(r"\{\{[Nn]ota de rodapé.*?\}\}", "", s, flags=re.S)
    s = re.sub(r"\{\{dtlink\|([^|}]*)\|([^|}]*)\|([^|}]*)[^}]*\}\}", r"\1/\2/\3", s)
    s = re.sub(r"\{\{efn.*", "", s, flags=re.S)
    s = re.sub(r"\{\{(?:[Nn]owrap|[Nn]obr)\|(.*?)\}\}", r"\1", s)
    s = re.sub(r"\{\{[^{}]*\}\}", "", s)
    s = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", s)
    s = re.sub(r"\[https?://\S+\s*\]", "", s)
    s = re.sub(r"<br\s*/?>", " ", s)
    s = re.sub(r"<[^>]+>|'''?|\{\{|\}\}", "", s)
    return re.sub(r"\s+", " ", s).strip()


def periodos(mandato):
    """'1.º- 1987 até 2002 2.º- 2019 até a atualidade' -> [(1987, 2002), (2019, None)]."""
    trechos = [t for t in re.split(r"\d\s*[.º°•]+\s*-|\d•", mandato) if re.search(r"\d{4}", t)] or [mandato]
    saida = []
    for t in trechos:
        anos = [int(a) for a in re.findall(r"\b(19\d\d|20\d\d)\b", t)]
        if not anos:
            continue
        aberto = re.search(r"atualidade|exerc[íi]cio|presente|atual\b", t)
        saida.append((anos[0], None if aberto else (anos[-1] if len(anos) > 1 else None)))
    return saida


def texto_periodo(ps):
    return ", ".join(f"{a}-{'atual' if b is None else b}" if b != a else str(a) for a, b in ps)


def tipo_cargo(cargo):
    s = cargo.lower()
    for tipo, padrao in TIPO_CARGO:
        if re.search(padrao, s):
            return tipo
    return "Nomeação ou função pública"


def pasta(cargo, tipo):
    if tipo not in ENTRA_NO_RESUMO or tipo == "Cônjuge de chefe do Executivo":
        return ""
    # O nome do estado não diz a pasta: "Casa Civil de Minas Gerais" não é Minas e Energia.
    s = cargo.lower()
    for nome_uf in sorted(UF_NOME.values(), key=len, reverse=True):
        s = s.replace(nome_uf.lower(), "")
    return next((tema for padrao, tema in PASTA if re.search(padrao, s)), "Sem pasta na fonte")


def cargos_wiki(titulo):
    campos = ficha(pagina(titulo)["texto"])
    numeros = sorted({int(k[6:]) for k in campos if re.fullmatch(r"título\d+", k)})
    linhas = []
    for t, m in [("título", "mandato")] + [(f"título{n}", f"mandato{n}") for n in numeros]:
        if campos.get(t):
            cargo = re.sub(r"^\d+[.º°ª⁰]*\s*(e \d+[.º°ª]*\s*)*", "", limpar(campos[t])).strip()
            cargo = re.sub(r"^(\d+[.º°ª]*,?\s*)+(e \d+[.º°ª]*\s*)?", "", cargo).strip()
            linhas.append((cargo, periodos(limpar(campos.get(m, "")))))
    return linhas


# ---------------------------------------------------------------- pesquisa anterior

PALAVRAS_VAZIAS = {"de", "da", "do", "das", "dos", "e", "estado", "estadual", "municipal", "secretario", "secretaria",
                   "ministro", "ministra", "chefe", "brasil", "presidente", "diretor", "diretora", "geral", "nacional"}


SIGLAS = {"secom": "comunicacao social", "sedop": "obras publicas", "seag": "agricultura"}


def palavras(s):
    s = sem_acento(s)
    for sigla, extenso in SIGLAS.items():
        s = s.replace(sigla, extenso)
    return {p for p in re.findall(r"[a-z]{3,}", s) if p not in PALAVRAS_VAZIAS}


def confirmado(item, cargos):
    """O item da pesquisa anterior bate com algum cargo da ficha: mesmo tipo de função e uma
    palavra de conteúdo em comum (Turismo, Casa Civil, Saúde...)."""
    funcao = lambda s: re.search(r"ministr|secret|president|diretor|procurador|delegad|capit|coronel|policial|primeir|"
                                 r"coorden|subcomand", sem_acento(s))
    for cargo, _ in cargos:
        f1, f2 = funcao(item), funcao(cargo)
        if f1 and f2 and f1.group(0)[:5] == f2.group(0)[:5] and palavras(item) & palavras(cargo):
            return True
    return False


# ---------------------------------------------------------------- temas

def baixar_camara():
    for recurso, pasta_local in (("proposicoesAutores", "camara_autores"), ("proposicoesTemas", "camara_temas_ano")):
        for ano in ANOS_CAMARA:
            p = c.caminho(pasta_local) / f"{recurso}-{ano}.csv"
            if p.exists() and p.stat().st_size:
                continue
            p.parent.mkdir(parents=True, exist_ok=True)
            r = requests.get(ARQ_CAMARA.format(recurso, ano), timeout=600)
            r.raise_for_status()
            p.write_bytes(r.content)


def autoria_principal_camara():
    """Proposição x tema, só primeiro signatário deputado, PL/PLP/PEC/PDL, 2003 a 2026."""
    baixar_camara()
    aut = pd.concat(pd.read_csv(f, sep=";", dtype=str, usecols=["idProposicao", "idDeputadoAutor", "ordemAssinatura", "proponente"])
                    for f in sorted(c.caminho("camara_autores").glob("*.csv")))
    tem = pd.concat(pd.read_csv(f, sep=";", dtype=str, usecols=["uriProposicao", "siglaTipo", "tema"])
                    for f in sorted(c.caminho("camara_temas_ano").glob("*.csv")))
    tem["idProposicao"] = tem.uriProposicao.str.rsplit("/", n=1).str[-1]
    tem = tem[tem.siglaTipo.isin(TIPOS) & ~tem.tema.isin(FORA)].drop_duplicates(["idProposicao", "tema"])
    principal = aut[(aut.ordemAssinatura == "1") & (aut.proponente == "1") & aut.idDeputadoAutor.notna()]
    principal = principal.drop_duplicates(["idProposicao", "idDeputadoAutor"])
    return principal.merge(tem[["idProposicao", "tema"]], on="idProposicao")


SENADO = "https://legis.senado.leg.br/dadosabertos"


def senado_json(url):
    for i in range(6):
        try:
            r = requests.get(url, headers={"Accept": "application/json"}, timeout=60)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            pass
        time.sleep(5 * (i + 1))
    raise RuntimeError(f"Senado não respondeu: {url}")


def autoria_principal_senado(cod, nome_casa):
    """Matérias do Senado em que o senador é o primeiro autor, com a classificação do Senado."""
    lista = c.em_cache("senado_processos", cod, lambda: senado_json(f"{SENADO}/processo?codigoParlamentarAutor={cod}"))
    alvo = sem_acento(nome_casa)
    proprias = [p for p in lista if p.get("casaIdentificadora") == "SF"
                and p.get("identificacao", "").split(" ")[0] in SIGLAS_SENADO
                and alvo in sem_acento(re.split(r",| e outros", p.get("autoria", ""))[0])]
    detalhe = lambda p: c.em_cache("senado_processo", str(p["id"]), lambda: senado_json(f"{SENADO}/processo/{p['id']}"))
    with ThreadPoolExecutor(2) as ex:
        detalhes = list(ex.map(detalhe, proprias))
    linhas = []
    for p, d in zip(proprias, detalhes):
        for k in (d or {}).get("classificacoes") or []:
            linhas.append((str(p["id"]), k["descricaoHierarquia"]))
    return pd.DataFrame(linhas, columns=["idProposicao", "classificacao"]), len(proprias)


def senado_para_camara(hierarquia):
    niveis = hierarquia.split(" / ")
    for n in range(len(niveis), 0, -1):
        tema = SENADO_PARA_CAMARA.get(" / ".join(niveis[:n]))
        if tema is not None:
            return tema or None
    return None


def escolher_temas(contagem, n_props, media_casa):
    """Até 3 temas, por número de proposições, entre os acima da média da Casa."""
    if n_props < MINIMO_PROPOSICOES:
        return []
    parcela = contagem / n_props
    acima = contagem[(parcela >= media_casa.reindex(contagem.index).fillna(0)) & (contagem >= 3)]
    return list(acima.sort_values(ascending=False).head(3).items())


def tema_autoria(id_camara, cod_senado, nome, camara, media_camara):
    """Até 3 temas de autoria principal na Câmara (2003-2026) e no Senado, com a frase de como
    foi medido e os links. Sem ID nas duas Casas não há tema."""
    temas, medida, fontes = [], "", []
    p = camara[camara.idDeputadoAutor == id_camara] if id_camara else camara.iloc[:0]
    if id_camara:
        n = p.idProposicao.nunique()
        temas = escolher_temas(p.tema.value_counts(), n, media_camara)
        medida = f"Câmara: {n} proposições de autoria principal com tema (2003-2026)"
        fontes.append(f"https://www.camara.leg.br/deputados/{id_camara}")
    if cod_senado:
        info = c.senado(f"senador/{cod_senado}", chave_cache=f"senador_{cod_senado}") or {}
        nome_casa = (info.get("DetalheParlamentar", {}).get("Parlamentar", {})
                     .get("IdentificacaoParlamentar", {}).get("NomeParlamentar", nome))
        s, n_sen = autoria_principal_senado(cod_senado, nome_casa)
        s["tema"] = s.classificacao.map(senado_para_camara)
        s = s.dropna(subset=["tema"]).drop_duplicates(["idProposicao", "tema"])
        if n_sen:
            junto = pd.concat([p[["idProposicao", "tema"]], s[["idProposicao", "tema"]]])
            temas = escolher_temas(junto.tema.value_counts(), junto.idProposicao.nunique(), media_camara)
            medida = "; ".join(filter(None, [medida, f"Senado: {s.idProposicao.nunique()} matérias de autoria principal com classificação"]))
            fontes.append(f"https://www25.senado.leg.br/web/senadores/senador/-/perfil/{cod_senado}")
    if not temas:
        medida = (medida + "; poucas proposições para dar tema") if medida else "Sem autoria na Câmara ou no Senado"
    return temas, medida, fontes


def colunas_tema(temas, medida, fontes):
    return {"Temática principal": "; ".join(f"{t} ({n})" for t, n in temas),
            "Temática: como foi medida": medida, "Fonte da informação": " ".join(fontes)}


def chegam_com_autoria(aba_mapeamento, ja_tem, camara, media_camara):
    """Quem chega à Casa (não disputa reeleição) e já tem autoria na Câmara ou no Senado: só o
    tema, sem pesquisa de cargo. É o que a tela de renovação do painel conta."""
    mp = c.ler_aba(c.planilha_destino(), aba_mapeamento)
    alvo = mp[(mp["Reeleição, volta ou novo"] != "Reeleição") & ~mp.SQ_CANDIDATO.isin(ja_tem)
              & ((mp["ID na Câmara"] != "") | (mp["Código no Senado"] != ""))]
    linhas = []
    for _, r in alvo.iterrows():
        temas, medida, fontes = tema_autoria(r["ID na Câmara"], r["Código no Senado"], r.Nome, camara, media_camara)
        linhas.append({"Nome": r.Nome, "Partido": r.Partido, "UF": r.UF, "Reeleição, volta ou novo": r["Reeleição, volta ou novo"],
                       "Tipo de origem": r["Tipo de origem"], "Origem": r.Origem, **colunas_tema(temas, medida, fontes),
                       "Checagem": "Só tema de autoria; cargos não pesquisados", "SQ_CANDIDATO": r.SQ_CANDIDATO})
    return pd.DataFrame(linhas)


# ---------------------------------------------------------------- montagem

def universo():
    destino = c.planilha_destino()
    man = c.ler_aba(destino, ABA_PESSOA)
    mp = c.ler_aba(destino, ABA_MAPEAMENTO)
    base = man[["Nome", "Partido", "UF", "Reeleição, volta ou novo", "Tipo de origem", "Origem",
                "Mandatos eletivos desde 2006 (TSE)", "Ocupação declarada ao TSE", "Instagram",
                "Outras redes declaradas", "SQ_CANDIDATO"]]
    extra = mp[["SQ_CANDIDATO", "Nome de urna (TSE)", "ID na Câmara", "Código no Senado"]]
    base = base.merge(extra, on="SQ_CANDIDATO", how="left", validate="one_to_one").fillna("")
    civil = c.ler_csv("candidaturas_2026.csv").set_index("sq_candidato")
    base["nome_civil"] = base.SQ_CANDIDATO.map(civil.nome_civil)
    base["nascimento"] = base.SQ_CANDIDATO.map(civil.nascimento)
    c.falhar_se(base.nascimento.isna().any(), "SQ sem registro em candidaturas_2026.csv")
    return base


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--publicar", action="store_true")
    a = args.parse_args()

    base = universo()
    camara = autoria_principal_camara()
    n_casa = camara.idProposicao.nunique()
    media_camara = camara.tema.value_counts() / n_casa

    pessoas, cargos_longos = [], []
    for _, r in base.iterrows():
        sq = r.SQ_CANDIDATO
        titulo = achar_verbete(sq, r.Nome, r["Nome de urna (TSE)"], r.nome_civil, r.nascimento, r.UF)
        url = WIKI_URL + titulo.replace(" ", "_") if titulo else ""
        cargos = cargos_wiki(titulo) if titulo else []

        antes_2006, nao_eletivos, pastas, periodos_ne = [], [], [], []
        for cargo, ps in cargos:
            tipo = tipo_cargo(cargo)
            p = pasta(cargo, tipo)
            cargos_longos.append({"Nome": r.Nome, "Partido": r.Partido, "UF": r.UF, "Cargo": cargo, "Tipo": tipo,
                                  "Pasta (tema da Câmara)": p, "Período": texto_periodo(ps), "Fonte": url,
                                  "Checagem": "Com fonte (Wikipedia)", "SQ_CANDIDATO": sq})
            if tipo == "Mandato eletivo":
                velhos = [(i, f) for i, f in ps if i <= 2006]
                if velhos:
                    antes_2006.append(f"{cargo} ({texto_periodo(velhos)})")
            elif tipo in ENTRA_NO_RESUMO:
                nao_eletivos.append(cargo)
                periodos_ne.append(texto_periodo(ps) or "sem data")
                if p:
                    pastas.append(p)

        # Conferência item a item do que a ficha não trazia (verificacao_competitivos.py).
        verificados = VERIFICADOS.get(sq, [])
        resolvidos = {v["substitui"] for v in verificados}
        nao_confirmados, fontes_verif = [], []
        for v in verificados:
            confirmado_ = v["resultado"] != "Não confirmado"
            cargos_longos.append({"Nome": r.Nome, "Partido": r.Partido, "UF": r.UF, "Cargo": v["cargo"], "Tipo": v["tipo"],
                                  "Pasta (tema da Câmara)": pasta(v["cargo"], v["tipo"]) if confirmado_ else "",
                                  "Período": v["periodo"], "Fonte": v["fonte"],
                                  "Checagem": v["resultado"] + (f" (antes: {v['substitui']})"
                                                                if v["resultado"] == "Corrigido" and v["substitui"] else ""),
                                  "SQ_CANDIDATO": sq})
            if not confirmado_:
                nao_confirmados.append(v["cargo"])
                continue
            fontes_verif.append(v["fonte"])
            if v["tipo"] in ENTRA_NO_RESUMO:
                nao_eletivos.append(v["cargo"])
                periodos_ne.append(v["periodo"] or "sem data")
                if pasta(v["cargo"], v["tipo"]):
                    pastas.append(pasta(v["cargo"], v["tipo"]))

        anterior = PESQUISA_SENADO_74.get(sq, {})
        pendentes = []
        for item in re.split(r";\s*", anterior.get("Cargo não eletivo anterior (ex.: secretário de pasta)", "")):
            item = item.strip()
            if not item or item.lower().startswith("sem registro"):
                continue
            tipo = tipo_cargo(item)
            if (item in resolvidos or tipo in {"Profissão", "Mandato eletivo", "Função parlamentar (Mesa, frente)"}
                    or confirmado(item, cargos)):
                continue
            if tipo in ENTRA_NO_RESUMO:
                pendentes.append(item)
            cargos_longos.append({"Nome": r.Nome, "Partido": r.Partido, "UF": r.UF, "Cargo": item, "Tipo": tipo,
                                  "Pasta (tema da Câmara)": pasta(item, tipo), "Período": "", "Fonte": "",
                                  "Checagem": A_CONFERIR, "SQ_CANDIDATO": sq})
        if not titulo and anterior.get("Teve mandato antes de 2006?", "Não") not in ("", "Não"):
            antes_2006 = [f"{anterior['Teve mandato antes de 2006?']} (a conferir)"]

        temas, medida, fontes_tema = tema_autoria(r["ID na Câmara"], r["Código no Senado"], r.Nome, camara, media_camara)

        if pendentes:
            checagem = "Parte a conferir: ver aba de cargos"
        elif nao_confirmados:
            checagem = "Parte não confirmada: ver aba de cargos"
        elif titulo or fontes_verif:
            checagem = "Com fonte"
        else:
            checagem = "Sem cargo não eletivo achado em fonte"
        pessoas.append({
            **{k: r[k] for k in ["Nome", "Partido", "UF", "Reeleição, volta ou novo", "Tipo de origem", "Origem",
                                 "Mandatos eletivos desde 2006 (TSE)", "Ocupação declarada ao TSE", "Instagram",
                                 "Outras redes declaradas"]},
            "Teve mandato antes de 2006?": "; ".join(antes_2006) or ("Não" if titulo else ""),
            "Cargo não eletivo anterior (ex.: secretário de pasta)": "; ".join(nao_eletivos + [f"{i} (a conferir)" for i in pendentes]),
            "Pasta ou área": "; ".join(dict.fromkeys(pastas)),
            "Período": "; ".join(periodos_ne),
            "Temática principal": "; ".join(f"{t} ({n})" if n else t for t, n in temas),
            "Temática: como foi medida": medida,
            "Fonte da informação": " ".join(dict.fromkeys(filter(None, [url] + fontes_verif + fontes_tema))),
            "Checagem": checagem,
            "Observações": "",
            "SQ_CANDIDATO": sq,
        })

    pessoas = pd.DataFrame(pessoas)
    longos = pd.DataFrame(cargos_longos)
    extra = chegam_com_autoria(ABA_MAPEAMENTO, set(pessoas.SQ_CANDIDATO), camara, media_camara)
    print(f"chegam ao Senado com autoria, fora dos competitivos: {len(extra)}; com tema: {(extra['Temática principal'] != '').sum()}")
    pessoas = pd.concat([pessoas, extra], ignore_index=True).fillna("")
    pessoas = pessoas[[col for col in pessoas.columns if col != "SQ_CANDIDATO"] + ["SQ_CANDIDATO"]]
    c.falhar_se(len(pessoas) < len(base), "perdeu gente na montagem")
    print(f"{len(pessoas)} pessoas; checagem: {pessoas.Checagem.value_counts().to_dict()}; "
          f"com tema: {(pessoas['Temática principal'] != '').sum()}; cargos: {len(longos)}, "
          f"a conferir: {(longos.Checagem == A_CONFERIR).sum()}")
    print(longos.Tipo.value_counts().to_string())
    c.salvar_csv(pessoas, "competitivos_senado_trajetoria.csv")
    c.salvar_csv(longos, "competitivos_senado_cargos.csv")
    if not a.publicar:
        return
    destino = c.planilha_destino()
    c.gravar_aba(destino, ABA_PESSOA, pessoas, congelar_colunas=1,
                 largura_minima={"Observações": 220})
    c.gravar_aba(destino, ABA_CARGOS, longos.sort_values(["UF", "Nome", "Tipo"]), congelar_colunas=1)


if __name__ == "__main__":
    main()
