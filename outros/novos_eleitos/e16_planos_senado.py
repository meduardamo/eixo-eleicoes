"""Etapa 16. Planos de governo dos senadores que foram governadores ou prefeitos.

Parte dos links da etapa 15 (plano registrado no TSE em cada eleição de governador ou prefeito
que o senador venceu), baixa o PDF e extrai o texto página a página. Página sem texto (PDF
escaneado) passa por OCR e fica marcada.

O texto integral serve à busca por tema e subtema, como a nota taquigráfica. A contagem por
área que sai aqui é só de menção: em quantas páginas do plano aparece algum termo da área. Não
mede prioridade nem posição.

Saída em dados_novos_eleitos/planos_senado/: pdf/ (os arquivos), planos.csv (uma linha por
plano), paginas.csv (uma linha por página, com o texto) e areas.csv (plano x área).
Rodar: python -m outros.novos_eleitos.e16_planos_senado   (depois da etapa 15)
"""
import re
import subprocess
import tempfile
from pathlib import Path

import pymupdf as fitz
import pandas as pd
from curl_cffi import requests as navegador

from outros.novos_eleitos import comum as c

COLUNA = "Plano de governo (eleições de governador ou prefeito que venceu)"
MINIMO_TEXTO = 80  # página com menos caracteres que isso é tratada como imagem

# Termos que localizam a área no texto, já sem acento e em minúscula. A lista é curta de propósito:
# conta menção, e o trecho é lido depois.
AREAS = {
    "Saúde": r"\bsaude\b|\bsus\b|hospita|\bubs\b|\bupa\b|vacina",
    "Educação": r"educac|\bescola|\bensino\b|creche|universi|alfabetiz",
    "Esporte e Lazer": r"\besporte|desport|\blazer\b|atividade fisica",
    "Segurança Pública": r"seguranca publica|\bpolicia|violencia|criminalidade|presidi|penitenciar",
    "Assistência Social": r"assistencia social|\bcras\b|\bcreas\b|vulnerabil|combate a fome|seguranca alimentar|pobreza",
    "Direitos Humanos e Minorias": r"direitos humanos|\bmulher|igualdade racial|\bnegr[oa]s?\b|indigena|quilombol|lgbt|pessoas? com deficiencia|\bidos[oa]s?\b|juventude|crianca",
    "Meio Ambiente": r"meio ambiente|ambiental|desmatament|sustentab|mudancas? climatic|saneamento|residuos",
    "Agropecuária": r"agricultur|agropecuar|agroneg|pecuaria|\brural\b|pesca",
    "Economia, Indústria e Emprego": r"\bemprego|geracao de renda|industria|\bcomercio\b|turismo|empreended|desenvolvimento economico",
    "Infraestrutura e Transporte": r"infraestrutura|rodovia|estrada|mobilidade|transporte|\bporto|aeroporto|ferrovia",
    "Habitação e Cidades": r"habitac|moradia|urbaniz|desenvolvimento urbano",
    "Ciência, Tecnologia e Inovação": r"ciencia e tecnologia|\binovacao|tecnolog|\bdigital",
    "Cultura": r"\bcultura\b|cultural|patrimonio historico",
    "Gestão e Finanças Públicas": r"gestao publica|equilibrio fiscal|\bfiscal\b|tribut|\bimposto|transparencia|servidor",
    "Energia": r"\benergia|energetic|\bgas natural|\bpetroleo",
}


# Conferido na leitura: o arquivo que o TSE guarda no lugar do plano não é um plano.
NAO_E_PLANO = {("Teresa Surita", "2012"): "O arquivo registrado no TSE como proposta é uma certidão judicial, não um plano de governo"}


def baixar(url, destino):
    if destino.exists() and destino.stat().st_size:
        return
    r = navegador.get(url, headers=c.CABECALHOS_DIVULGACAND, impersonate="chrome", timeout=180)
    r.raise_for_status()
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(r.content)


def ocr(pdf, pagina):
    """Texto de uma página que é imagem. Devolve vazio se as ferramentas não estiverem instaladas."""
    try:
        with tempfile.TemporaryDirectory() as d:
            subprocess.run(["pdftoppm", "-r", "200", "-f", str(pagina), "-l", str(pagina), "-png", str(pdf), f"{d}/p"],
                           check=True, capture_output=True, timeout=120)
            imagem = next(Path(d).glob("p*.png"))
            return subprocess.run(["tesseract", str(imagem), "-", "-l", "por"], check=True, capture_output=True,
                                  timeout=180).stdout.decode("utf-8", "replace")
    except (FileNotFoundError, StopIteration, subprocess.SubprocessError):
        return ""


def paginas(pdf):
    cache = pdf.with_suffix(".paginas.csv")
    if cache.exists():
        return pd.read_csv(cache, dtype=str, keep_default_na=False).to_dict("records")
    saida = []
    with fitz.open(pdf) as doc:
        for i, pg in enumerate(doc, 1):
            texto, origem = pg.get_text().strip(), "Texto do PDF"
            if len(texto) < MINIMO_TEXTO:
                lido = ocr(pdf, i).strip()
                if len(lido) > len(texto):
                    texto, origem = lido, "OCR"
            saida.append({"Página": str(i), "Origem do texto": origem, "Texto": texto})
    pd.DataFrame(saida).to_csv(cache, index=False)
    return saida


def main():
    origem = c.ler_csv("perfil_senado_origem/origem.csv")
    pasta = c.caminho("planos_senado")
    planos, todas, areas = [], [], []
    for l in origem[origem[COLUNA].str.contains("http")].to_dict("records"):
        for parte in l[COLUNA].split(" | "):
            achado = re.match(r"(.+?) (\d{4}) \((.+?)\): (https?://\S+)", parte)
            if not achado:
                continue
            cargo, ano, local, url = achado.groups()
            pdf = pasta / "pdf" / f"{c.chave_nome(l['Parlamentar']).replace(' ', '_')}_{cargo}_{ano}.pdf"
            try:
                baixar(url, pdf)
                if not pdf.stat().st_size:
                    raise ValueError("o TSE devolve o arquivo vazio")
                if not pdf.read_bytes().startswith(b"%PDF"):
                    raise ValueError("o arquivo do TSE não é um PDF")
                pgs = paginas(pdf)
            except Exception as erro:  # noqa: BLE001 - plano que não baixa ou não abre fica registrado
                planos.append({"Parlamentar": l["Parlamentar"], "Eleição": f"{cargo} {ano} ({local.title()})", "Páginas": "",
                               "Páginas com texto": "", "Páginas lidas por OCR": "", "Caracteres": "",
                               "Situação": f"Não foi possível ler: {erro}", "Link": url})
                continue
            base = {"Parlamentar": l["Parlamentar"], "Eleição": f"{cargo} {ano} ({local.title()})"}
            if (l["Parlamentar"], ano) in NAO_E_PLANO:
                planos.append({**base, "Páginas": len(pgs), "Páginas com texto": "", "Páginas lidas por OCR": "",
                               "Caracteres": "", "Situação": NAO_E_PLANO[(l["Parlamentar"], ano)], "Link": url})
                continue
            com_texto = [p for p in pgs if len(p["Texto"]) >= MINIMO_TEXTO]
            planos.append({**base, "Páginas": len(pgs), "Páginas com texto": len(com_texto),
                           "Páginas lidas por OCR": sum(p["Origem do texto"] == "OCR" for p in pgs),
                           "Caracteres": sum(len(p["Texto"]) for p in pgs),
                           "Situação": "Lido" if com_texto else "Sem texto extraível", "Link": url})
            todas += [{**base, **p} for p in pgs]
            limpos = [c.sem_acento(p["Texto"]).lower() for p in com_texto]
            for area, padrao in AREAS.items():
                n = sum(bool(re.search(padrao, t)) for t in limpos)
                areas.append({**base, "Área": area, "Páginas que citam": n, "Páginas com texto": len(com_texto),
                              "Menções": sum(len(re.findall(padrao, t)) for t in limpos)})
    pd.DataFrame(planos).to_csv(pasta / "planos.csv", index=False)
    pd.DataFrame(todas).to_csv(pasta / "paginas.csv", index=False)
    pd.DataFrame(areas).to_csv(pasta / "areas.csv", index=False)
    print(pd.DataFrame(planos).drop(columns="Link").to_string())


if __name__ == "__main__":
    main()
