"""Etapa 19. Perfil pessoal de cada senador da próxima legislatura, pelo que ele declarou ao TSE.

Uma linha por senador com nascimento, idade na posse, gênero, cor/raça, grau de instrução, as
ocupações declaradas em todas as candidaturas desde 2006 e o total de bens declarados na última
candidatura. Completa com profissão e formação da página do Senado, quando há.

Ocupação é autodeclarada e muda de uma eleição para outra. Quem tem mandato costuma declarar o
cargo (DEPUTADO, SENADOR), que não diz nada da profissão: essas declarações e OUTROS saem da lista.
Nenhuma declaração é descartada por divergir de outra; todas aparecem com o ano.

A coluna "Área ligada à profissão" só marca as ligações diretas entre ocupação e uma das 22 áreas
do levantamento (médico e enfermeiro em Saúde, professor em Educação, policial em Segurança,
militar em Defesa, agricultor e veterinário em Agropecuária, jornalista e radialista em
Comunicações, empresário e comerciante em Economia). Advogado, engenheiro, economista e
administrador não têm área própria e ficam sem marca.

A especialidade médica não está no TSE nem no Senado; está na busca de médicos do CFM, que pede
reCAPTCHA e por isso é consulta manual.

Saída: dados_novos_eleitos/perfil_senado_origem/perfil_pessoal.csv
Rodar: python -m outros.novos_eleitos.e19_perfil_pessoal_senado   (depois das etapas 13 e 15)
"""
import io
import re
import zipfile

import pandas as pd

from outros.novos_eleitos import comum as c

ABA = "Senado eleito 2026"
GRUPOS = ("curso", "reeleitos", "novos")
POSSE = pd.Timestamp("2027-02-01")
ANOS = range(2006, 2027, 2)
CDN_BENS = "https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato"
CARGO_DECLARADO = re.compile(r"^(SENADOR|DEPUTADO|VEREADOR|PREFEITO|GOVERNADOR|MINISTRO DE ESTADO|OUTROS"
                             r"|OCUPANTE DE CARGO EM COMISS)")
AREA_DA_OCUPACAO = [
    (r"M[ÉE]DICO|ENFERMEIR|PSIC[ÓO]LOGO|ODONT[ÓO]LOGO|FARMAC[ÊE]UTICO|FISIOTERAP|NUTRICION|FONOAUDI"
     r"|T[ÉE]CNICO DE ENFERMAGEM|AGENTE DE SA[ÚU]DE", "Saúde"),
    (r"PROFESSOR|PEDAGOGO", "Educação"),
    (r"POLICIAL|DELEGADO|BOMBEIRO|AGENTE PENITENCI|GUARDA MUNICIPAL", "Segurança pública e direito penal"),
    (r"FOR[ÇC]AS ARMADAS|MILITAR REFORMADO", "Defesa e relações exteriores"),
    (r"AGRICULTOR|PECUARISTA|PRODUTOR AGROPECU|AGR[ÔO]NOMO|T[ÉE]CNICO EM AGRONOMIA|VETERIN[ÁA]RIO", "Agropecuária e questão fundiária"),
    (r"JORNALISTA|LOCUTOR|RADIALISTA|PUBLICIT[ÁA]RIO|ANALISTA DE SISTEMAS", "Ciência, tecnologia e comunicações"),
    (r"EMPRES[ÁA]RIO|COMERCIANTE|INDUSTRIAL|DIRETOR DE EMPRESAS", "Economia, indústria e comércio"),
    (r"SERVIDOR P[ÚU]BLICO", "Administração pública e servidores"),
]


def bens(ano):
    arquivo = c.TSE_CACHE / f"bem_candidato_{ano}.zip"
    if not arquivo.exists():
        r = c.navegador.get(f"{CDN_BENS}/bem_candidato_{ano}.zip", impersonate="chrome", timeout=1800)
        r.raise_for_status()
        arquivo.write_bytes(r.content)
    with zipfile.ZipFile(arquivo) as z:
        nome = [x for x in z.namelist() if x.endswith("BRASIL.csv")][0]
        df = pd.read_csv(io.BytesIO(z.read(nome)), sep=";", encoding="latin-1", dtype=str, keep_default_na=False)
    df["valor"] = pd.to_numeric(df.VR_BEM_CANDIDATO.str.replace(",", "."), errors="coerce").fillna(0)
    return df.groupby("SQ_CANDIDATO").valor.sum()


def reais(v):
    return "R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def main():
    aba = pd.DataFrame(c.ler_aba(c.id_planilha("SPREADSHEET_ID_CONGRESSO_ELEITO"), ABA))
    senado = pd.concat([c.ler_csv(f"perfil_senado_{g}/resumo.csv") for g in GRUPOS]).drop_duplicates("Parlamentar")
    senado = senado.set_index("Parlamentar")
    exercicio = c.ler_csv("exercicio_casas.csv")
    exercicio = exercicio[exercicio.casa == "Senado"]
    tse = pd.concat([c.consulta_cand(a) for a in ANOS], ignore_index=True)
    tse["ano"] = tse.ANO_ELEICAO.astype(int)
    total_bens = {a: bens(a) for a in (2022, 2026)}

    linhas = []
    for l in aba.to_dict("records"):
        nome = l["Parlamentar"]
        # título e CPF pela candidatura de 2026 e pelo cadastro do Senado; a pessoa é o título, não o nome
        proprio = tse[tse.SQ_CANDIDATO == l["SQ_CANDIDATO"]] if l["SQ_CANDIDATO"] else tse.iloc[0:0]
        e = exercicio[exercicio.id_casa == str(senado["Código no Senado"].get(nome, ""))]
        titulos = (set(proprio.titulo) | set(e.titulo)) - {""}
        cpfs = (set(proprio.cpf) | set(e.cpf)) - {"", "00000000004"}
        cands = tse[tse.titulo.isin(titulos) | tse.cpf.isin(cpfs)].sort_values("ano")
        c.falhar_se(cands.empty, f"{nome}: nenhuma candidatura no TSE")
        ultima = cands.iloc[-1]

        declaradas = {}
        for ano, ocup in zip(cands.ano, cands.DS_OCUPACAO):
            if ocup and not CARGO_DECLARADO.match(ocup):
                declaradas.setdefault(ocup.capitalize(), []).append(str(ano))
        ocupacoes = "; ".join(f"{o} ({', '.join(dict.fromkeys(a))})" for o, a in declaradas.items())
        prof_senado = senado["Profissão"].get(nome, "") if "Profissão" in senado else ""
        areas = dict.fromkeys(area for o in list(declaradas) + [prof_senado] for padrao, area in AREA_DA_OCUPACAO
                              if re.search(padrao, c.sem_acento(o).upper()) or re.search(padrao, o.upper()))

        nasc = pd.to_datetime(ultima.nascimento, errors="coerce")
        idade = (POSSE.year - nasc.year - ((POSSE.month, POSSE.day) < (nasc.month, nasc.day))) if pd.notna(nasc) else ""
        bem_ultima = next(((ano, total_bens[ano].get(sq)) for ano, sq in
                           zip(cands.ano[::-1], cands.SQ_CANDIDATO[::-1]) if ano in total_bens), (None, None))
        linhas.append({
            "Parlamentar": nome, "Partido": l["Partido"], "UF": l["UF"], "Situação": l["Situação"],
            "Nascimento": nasc.strftime("%d/%m/%Y") if pd.notna(nasc) else "",
            "Idade na posse (01/02/2027)": str(idade),
            "Gênero (TSE)": ultima.DS_GENERO.capitalize(), "Cor/raça (TSE)": ultima.DS_COR_RACA.capitalize(),
            "Grau de instrução (TSE)": ultima.DS_GRAU_INSTRUCAO.capitalize(),
            "Formação (página do Senado)": senado["Formação"].get(nome, "") if "Formação" in senado else "",
            "Profissão (página do Senado)": prof_senado,
            "Ocupações declaradas ao TSE (ano da candidatura)": ocupacoes or "Só o cargo que exercia",
            "Área ligada à profissão": "; ".join(areas),
            "Bens declarados na última candidatura": reais(bem_ultima[1]) if bem_ultima[1] is not None
            else ("Nenhum bem no arquivo do TSE" if bem_ultima[0] else ""),
            "Ano da declaração de bens": str(bem_ultima[0] or ""),
            "Candidaturas no TSE desde 2006": str(cands.ano.nunique()),
        })
    df = pd.DataFrame(linhas)
    c.salvar_csv(df, "perfil_senado_origem/perfil_pessoal.csv")
    print(len(df), "senadores;", (df["Área ligada à profissão"] != "").sum(), "com área ligada à profissão")
    print(df["Área ligada à profissão"].str.split("; ").explode().value_counts().to_dict())


if __name__ == "__main__":
    main()
