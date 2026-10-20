"""Etapa 25. Atuação de cada senador nos subtemas dos clientes, dentro dos quatro grandes temas.

Subtemas tirados da taxonomia dos planos de governo (outros/analise_planos.py) e das pautas dos clientes:
tempo integral e recomposição das aprendizagens (ISG, IU), alfabetização (Lemann), conectividade (MegaEdu),
tabaco e álcool (Vital), alimentação saudável e ultraprocessados (Desiderata, IDEC, Umane), violência contra
crianças, adolescentes e mulheres e casamento infantil (Vital, Infinis, Coletivo, UNFPA), planos de saúde (IDEC, IEPS),
saúde mental, drogas e apostas (Cactus, IEPS), incentivo ao esporte (REMS), primeira infância (FMCSV) etc.

Cada matéria da etapa 21 (primeira autoria e relatorias com parecer, Senado e Câmara) foi lida pela ementa e
recebeu um ou dois subtemas: dados_novos_eleitos/temas_clientes/subtemas_lidos.csv (2.060 matérias, leitura
de 16/10/2026; em 19/10 o antigo FR foi relido e dividido em TA e AS). Violência (VI) vem de outra leitura,
temas_clientes/violencia_lidos.csv (19/10/2026): as 525 matérias da base completa do levantamento (etapa 18) com
público mulheres ou crianças em segurança, direitos humanos, direito civil ou assistência, ou com termo de
violência, lidas uma a uma; 330 entraram. Em 20/10 as 865 ementas de Educação foram relidas para dois subtemas novos,
pedidos por ISG e IU: infraestrutura escolar (IN: obras, reforma, condições mínimas, água, mobiliário, normas de
segurança do prédio; 12 matérias, uma delas lida em Primeira infância) e adaptação e emergência climática nas escolas
(CL: 2 matérias). Defesa civil no currículo e o programa do RS para o ensino superior ficaram fora. Ensino superior e o que não cabe em subtema de cliente ficam fora das colunas.

Nível por subtema, pelo número de matérias distintas do senador no subtema (autor ou relator):
0 = Não atua; 1 = Pouco; 2 a 4 = Atua; 5 ou mais = Referência. A nota da célula lista as matérias.

Saída: temas_clientes/subtemas.csv (uma linha por senador) e temas_clientes/subtemas_notas.json
Rodar: python -m outros.novos_eleitos.e25_subtemas_senado   (depois da 21)
"""
import json

import pandas as pd

from outros.novos_eleitos import comum as c

GRUPOS = ["curso", "reeleitos", "novos"]
# (grande tema, código, rótulo da coluna). Educação infantil fica em Primeira infância e junta as duas leituras.
SUBTEMAS = [
    ("Saúde", "AP", "Atenção primária à saúde"),
    ("Saúde", "MA", "Hospitais e alta complexidade no SUS"),
    ("Saúde", "UE", "Urgência e emergência em saúde"),
    ("Saúde", "SME", "Saúde mental, drogas e apostas"),
    ("Saúde", "TA", "Tabaco e álcool"),
    ("Saúde", "AS", "Alimentação saudável, ultraprocessados e agrotóxicos"),
    ("Saúde", "VI", "Violência contra crianças, adolescentes e mulheres e casamento infantil"),
    ("Saúde", "PS", "Planos de saúde"),
    ("Saúde", "MV", "Medicamentos e vacinas"),
    ("Saúde", "DC", "Doenças e condições de saúde específicas"),
    ("Saúde", "MU", "Saúde da mulher e direitos reprodutivos"),
    ("Saúde", "PR", "Profissionais e formação em saúde"),
    ("Saúde", "FG", "Financiamento e gestão do SUS"),
    ("Saúde", "VS", "Vigilância e emergências sanitárias"),
    ("Educação", "TI", "Educação em tempo integral"),
    ("Educação", "AL", "Alfabetização"),
    ("Educação", "RA", "Recomposição das aprendizagens e avaliação educacional"),
    ("Educação", "EM", "Ensino médio e educação profissional e tecnológica"),
    ("Educação", "PR", "Profissionais da educação"),
    ("Educação", "FI", "Financiamento da educação"),
    ("Educação", "EQ", "Equidade na educação e educação inclusiva"),
    ("Educação", "TC", "Conectividade e tecnologia nas escolas"),
    ("Educação", "GG", "Gestão e governança da educação (PNE, SNE, CNE)"),
    ("Educação", "IS", "Alimentação, transporte e segurança escolar"),
    ("Educação", "IN", "Infraestrutura escolar"),
    ("Educação", "CL", "Adaptação e emergência climática nas escolas"),
    ("Educação", "CU", "Currículo e conteúdos escolares"),
    ("Educação", "ED", "Educação domiciliar, escola sem partido e escolas cívico-militares"),
    ("Esporte", "FN", "Financiamento e incentivo ao esporte"),
    ("Esporte", "EC", "Esporte educacional, comunitário e inclusão pelo esporte"),
    ("Esporte", "AR", "Atletas e esporte de alto rendimento"),
    ("Esporte", "GR", "Gestão, regulação e integridade do esporte"),
    ("Esporte", "AP", "Apostas esportivas"),
    ("Primeira infância", "EI", "Educação infantil e creche"),
    ("Primeira infância", "SM", "Saúde materno-infantil"),
    ("Primeira infância", "PF", "Parentalidade e apoio às famílias"),
    ("Primeira infância", "PC", "Proteção e segurança da criança na primeira infância"),
    ("Primeira infância", "GV", "Governança e orçamento da primeira infância"),
]
# códigos lidos em outro tema que contam no subtema: (tema da leitura, código) -> (tema da coluna, código)
JUNTA = {("Educação", "EI"): ("Primeira infância", "EI"), ("Saúde", "BT"): ("Saúde", "SME"),
         ("Primeira infância", "IN"): ("Educação", "IN")}
NIVEL = lambda n: "Não atua" if n == 0 else "Pouco" if n == 1 else "Atua" if n <= 4 else "Referência"


def main():
    lidos = c.ler_csv("temas_clientes/subtemas_lidos.csv")
    cod = {}
    for r in lidos.itertuples():
        for s in str(r.Subtemas).split(","):
            tema, s = JUNTA.get((r._1, s), (r._1, s))
            cod.setdefault((r.Casa, r.Matéria), set()).add((tema, s))
    # violência quase toda caiu em Segurança na etapa 18, fora das matérias dos quatro temas: entra pela base completa
    viol = c.ler_csv("temas_clientes/violencia_lidos.csv")
    for r in viol.itertuples():
        cod.setdefault((r.Casa, r.Matéria), set()).add(("Saúde", "VI"))
    reg = c.ler_csv("levantamento_senado/registros.csv").drop_duplicates(["Parlamentar", "Casa", "Matéria", "Registro"])
    reg = reg.merge(viol[["Casa", "Matéria"]], on=["Casa", "Matéria"])
    reg = reg.assign(**{"Ano": reg.Matéria.str.extract(r"/(\d{4})")[0], "Virou lei": ""})
    mats = pd.concat([c.ler_csv(f"temas_clientes/materias_{g}.csv") for g in GRUPOS])
    mats = pd.concat([mats, reg[[k for k in mats.columns if k in reg.columns]]]).reindex(columns=mats.columns)
    nomes = list(dict.fromkeys(n for g in GRUPOS for n in c.ler_csv(f"temas_clientes/perfil_{g}.csv").Parlamentar))
    linhas, notas = [], {}
    for nome in nomes:
        m = mats[mats.Parlamentar == nome]
        linha = {"Senador": nome}
        for tema, s, rot in SUBTEMAS:
            achou = {}
            for r in m.itertuples():
                if (tema, s) in cod.get((r.Casa, r.Matéria), set()):
                    papel = "autor" if r.Registro == "Primeiro autor" else "relator"
                    k = (r.Casa, r.Matéria)
                    lei = str(r._10).strip() == "Sim"
                    achou.setdefault(k, [set(), lei, str(r.Ementa), int(str(r.Ano)[:4]) if str(r.Ano)[:4].isdigit() else 0])[0].add(papel)
            linha[f"{tema}|{rot}"] = NIVEL(len(achou))
            if achou:
                itens = sorted(achou.items(), key=lambda x: -x[1][3])
                txt = [f"{len(achou)} matéria{'s' if len(achou) > 1 else ''} no subtema (autor ou relator):"]
                for (casa, mat), (papeis, lei, ementa, ano) in itens[:12]:
                    ementa = ementa.replace("\n", " ").strip()
                    ementa = ementa if len(ementa) <= 160 else ementa[:157].rsplit(" ", 1)[0] + "..."
                    txt.append(f"{mat} ({' e '.join(sorted(papeis))}{', ' + casa if casa == 'Câmara' else ''}"
                               f"{', virou lei' if lei else ''}): {ementa}")
                if len(itens) > 12:
                    txt.append(f"e mais {len(itens) - 12}")
                notas[f"{nome}|{tema}|{rot}"] = "\n".join(txt)
        linhas.append(linha)
    df = pd.DataFrame(linhas)
    c.salvar_csv(df, "temas_clientes/subtemas.csv")
    json.dump(notas, open(c.caminho("temas_clientes/subtemas_notas.json"), "w"), ensure_ascii=False, indent=1)
    for tema in dict.fromkeys(t for t, _, _ in SUBTEMAS):
        cols = [k for k in df.columns if k.startswith(tema + "|")]
        print(tema)
        print(df[cols].apply(lambda s: s.value_counts()).fillna(0).astype(int).T.to_string())


if __name__ == "__main__":
    main()
