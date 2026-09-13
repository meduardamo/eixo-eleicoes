"""Etapa 9. Organogramas das secretarias estaduais de educação: quem aparece neles.

Fonte: pasta "IN/Estaduais" da Marcela, um PDF por UF com o organograma da Seduc feito para
o Instituto Natura em nov/2024 (PA e RR em rascunho). O PDF tem texto: o nome de cada setor
com o responsável e uma biografia do secretário.

Serve para a pergunta da Manu sobre quem foi secretário de alguma pasta, só na educação e
só na foto de nov/2024. O casamento é por nome dentro da UF, e nome parecido não prova que
é a mesma pessoa: toda linha sai "a conferir", com o trecho do organograma. Três classes, da
mais forte para a mais fraca:
  1. a linha de nome do organograma é igual ao nome civil do registro;
  2. a linha de nome está contida no nome civil, com a mesma primeira palavra e pelo menos
     três palavras ("Igor de Alvarenga Oliveira"), ou é igual ao nome de urna ("Eliel Faustino");
  3. o nome é citado numa frase do organograma, em geral a biografia do secretário
     ("substituindo Faisal Karam"). Governador citado como chefe do secretário cai aqui.
Casar só primeiro e último nome ou só nome de urna de duas palavras foi testado em
13/09/2026 e reprovado: "Maria das Graças Silva" casava com "Cláudia Maria da Silva Lobo".

A pasta tem que estar compartilhada com eixoraspagem@raspagemdou.iam.gserviceaccount.com.

Entrada: pre_mapeamento.csv (--universo pre) ou novos_eleitos.csv (--universo eleitos).
Saída: organogramas_<universo>.csv, que a etapa 10 junta na aba "Mapeamento" ou "Eleitos".
Rodar: python -m outros.novos_eleitos.e9_organogramas --universo pre
Variável: DRIVE_PASTA_ORGANOGRAMAS (id da pasta Estaduais).
"""
import argparse
import io
import re

import pandas as pd
import pymupdf
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from outros.novos_eleitos import comum as c

IGUAL, CONTIDO, CITADO = ("Nome do organograma igual ao nome civil", "Nome do organograma contido no nome civil",
                          "Citado no texto do organograma")
# palavra de cargo ou setor: linha que tem uma dessas não é nome de pessoa
INSTITUCIONAL = set("""SECRETARIA SECRETARIO SUBSECRETARIA SUBSECRETARIO DEPARTAMENTO DIRETORIA DIRETOR DIRETORA
COORDENACAO COORDENADORIA COORDENADOR COORDENADORA DIVISAO NUCLEO GABINETE ASSESSORIA ASSESSOR ASSESSORA GERENCIA
GERENTE SUBGERENTE SUPERINTENDENCIA SUPERINTENDENTE ESTADO EDUCACAO ENSINO CHEFE UNIDADE CONSELHO OUVIDORIA CONTROLE
INTERNO CONSULTORIA JURIDICA ADJUNTO ADJUNTA PROGRAMA GESTAO EXECUTIVO EXECUTIVA COMISSAO PRESIDENTE ATUALIZACAO
TELEFONE MAIL ESCOLAR ESCOLAS EDUCACIONAL EDUCACIONAIS ASSUNTOS PLANEJAMENTO FINANCEIRO FINANCEIRA TECNOLOGIA
INFRAESTRUTURA LOGISTICA ESPORTE ESPORTES CULTURA JUVENTUDE ARTICULACAO AVALIACAO ADMINISTRACAO SETOR REDE""".split())


def baixar_pdfs():
    """{UF: (nome do arquivo, texto)} de todos os PDFs da pasta, com cache local."""
    pasta = c.id_planilha("DRIVE_PASTA_ORGANOGRAMAS")
    cred = Credentials.from_service_account_file(str(c.CRED), scopes=["https://www.googleapis.com/auth/drive.readonly"])
    drive = build("drive", "v3", credentials=cred, cache_discovery=False)
    arquivos = drive.files().list(q=f"'{pasta}' in parents and trashed=false and mimeType='application/pdf'",
                                  fields="files(id,name)", supportsAllDrives=True,
                                  includeItemsFromAllDrives=True, pageSize=100).execute().get("files", [])
    c.falhar_se(not arquivos, "a pasta não devolveu nenhum PDF: confira se está compartilhada com a conta de serviço")
    destino = c.caminho("organogramas")
    destino.mkdir(exist_ok=True)
    textos = {}
    for a in arquivos:
        uf = re.sub(r"^RASCUNHO_", "", a["name"]).split("_")[0].upper()
        c.falhar_se(uf not in c.UFS, f"arquivo sem UF reconhecível: {a['name']}")
        local = destino / a["name"]
        if not local.exists():
            buffer = io.BytesIO()
            baixador = MediaIoBaseDownload(buffer, drive.files().get_media(fileId=a["id"], supportsAllDrives=True))
            pronto = False
            while not pronto:
                _, pronto = baixador.next_chunk()
            local.write_bytes(buffer.getvalue())
        with pymupdf.open(local) as pdf:
            textos[uf] = (a["name"], "\n".join(pagina.get_text() for pagina in pdf))
    print(f"{len(textos)} organogramas lidos: {sorted(textos)}")
    return textos


def parece_nome(linha):
    """Duas a sete palavras com inicial maiúscula (partícula liberada), sem número, sem
    pontuação de frase e sem palavra de cargo ou setor."""
    palavras = linha.replace(".", " ").split()
    if not 2 <= len(palavras) <= 7 or re.search(r"[\d@:/(),;]", linha):
        return False
    if any(p.lower() not in {"de", "da", "do", "das", "dos", "e"} and not p[:1].isupper() for p in palavras):
        return False
    chave = c.chave_nome(linha).split()
    return len(chave) >= 2 and not set(chave) & INSTITUCIONAL


def nomes_do_pdf(texto):
    """(índice da linha, nome) de cada linha que parece nome, e também da linha somada à
    seguinte, porque o PDF quebra nome comprido ("Ana Paula Lopes / Monteiro")."""
    linhas = [l.strip() for l in texto.splitlines()]
    nomes = []
    for i, linha in enumerate(linhas):
        if parece_nome(linha):
            nomes.append((i, linha))
            if i + 1 < len(linhas) and parece_nome(linhas[i + 1]):
                nomes.append((i, f"{linha} {linhas[i + 1]}"))
    return linhas, nomes


def contigua(tokens, texto_tokens):
    n = len(tokens)
    return any(texto_tokens[i:i + n] == tokens for i in range(len(texto_tokens) - n + 1))


def janela(linhas, i, antes=3, depois=1):
    return " / ".join(x for x in linhas[max(0, i - antes):i + depois + 1] if x)


def casar(civil, urna, linhas, nomes, texto_tokens):
    """(classe, trecho) do casamento mais forte, ou None."""
    melhor = None
    for i, nome in nomes:
        t = c.chave_nome(nome).split()
        if t == civil:
            return IGUAL, janela(linhas, i)
        # linha de duas palavras só casa pelo nome de urna: "José de Sousa" casava com José
        # Wilson Oliveira Sousa (Samaritano da Ripa, RR) só pelo primeiro e último nome
        contido = len(t) >= 3 and t[0] == civil[0] and set(t) <= set(civil)
        # linha igual ao nome de urna, com as palavras no nome civil: "Eliel Faustino"
        de_urna = len(urna) >= 2 and t == urna and set(urna) <= set(civil)
        if melhor is None and (contido or de_urna):
            melhor = (CONTIDO, janela(linhas, i))
    if melhor:
        return melhor
    # citação só vale em frase: nome de urna curto dentro da linha de nome de outra pessoa
    # ("Ana Paula" em "Ana Paula Lopes Monteiro") não é citação
    for tokens in ([civil] if len(civil) >= 3 else []) + ([urna] if len(urna) >= 2 else []):
        if not contigua(tokens, texto_tokens):
            continue
        for i in range(len(linhas)):
            trecho = " ".join(linhas[i:i + 3])  # a frase pode quebrar entre linhas
            if contigua(tokens, c.chave_nome(trecho).split()) and not any(
                    parece_nome(l) and set(tokens) & set(c.chave_nome(l).split()) for l in linhas[i:i + 3]):
                return CITADO, janela(linhas, i, antes=1, depois=2)
    return None


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--universo", choices=["pre", "eleitos"], required=True)
    a = args.parse_args()

    base = c.ler_csv("pre_mapeamento.csv" if a.universo == "pre" else "novos_eleitos.csv")
    civil = c.ler_csv("candidaturas_2026.csv").set_index("sq_candidato").nome_civil
    saida = []
    for uf, (arquivo, texto) in baixar_pdfs().items():
        linhas, nomes = nomes_do_pdf(texto)
        texto_tokens = c.chave_nome(texto).split()
        for _, r in base[base.UF == uf].iterrows():
            nome_civil = c.chave_nome(civil.get(r.SQ_CANDIDATO, "")).split()
            if not nome_civil:
                continue
            achado = casar(nome_civil, c.chave_nome(r["Nome de urna (TSE)"], tirar_titulo=True).split(),
                           linhas, nomes, texto_tokens)
            if not achado:
                continue
            saida.append({"Casa disputada": r["Casa disputada"], "Nome": r.Nome, "Partido": r.Partido, "UF": uf,
                          "Reeleição, volta ou novo": r["Reeleição, volta ou novo"], "Origem": r.Origem,
                          "Como o nome casou (a conferir)": achado[0], "Trecho do organograma": achado[1],
                          "Arquivo (Seduc, nov/2024)": arquivo, "SQ_CANDIDATO": r.SQ_CANDIDATO})
    df = pd.DataFrame(saida)
    if df.empty:
        print("nenhum nome do universo aparece nos organogramas")
        return
    ordem = {IGUAL: 0, CONTIDO: 1, CITADO: 2}
    df = df.sort_values(["Como o nome casou (a conferir)", "UF", "Nome"],
                        key=lambda s: s.map(ordem) if s.name == "Como o nome casou (a conferir)" else s)
    print(df.groupby(["Como o nome casou (a conferir)", "Casa disputada"]).size().to_string())
    c.salvar_csv(df, f"organogramas_{a.universo}.csv")


if __name__ == "__main__":
    main()
