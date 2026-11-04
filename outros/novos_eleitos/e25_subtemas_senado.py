"""Etapa 25. Atuação de cada senador nos subtemas dos clientes, dentro dos quatro grandes temas.

Subtemas tirados da taxonomia dos planos de governo (outros/analise_planos.py) e das pautas dos clientes:
tempo integral e recomposição das aprendizagens (ISG, IU), alfabetização (Lemann), conectividade (MegaEdu),
tabaco e álcool (Vital), alimentação saudável e ultraprocessados (Desiderata, IDEC, Umane), violência contra
crianças e adolescentes, violência contra a mulher e casamento infantil (Vital, Cactus, Infinis, Coletivo, UNFPA), planos de saúde (IDEC, IEPS),
saúde mental, drogas e apostas (Cactus, IEPS), incentivo ao esporte (REMS), primeira infância (FMCSV) etc.

Cada matéria da etapa 21 (primeira autoria e relatorias com parecer, Senado e Câmara) foi lida pela ementa e
recebeu um ou dois subtemas: dados_novos_eleitos/temas_clientes/subtemas_lidos.csv (2.060 matérias, leitura
de 16/10/2026; em 19/10 o antigo FR foi relido e dividido em TA e AS). Violência (VI) vem de outra leitura,
temas_clientes/violencia_lidos.csv (19/10/2026): as 525 matérias da base completa do levantamento (etapa 18) com
público mulheres ou crianças em segurança, direitos humanos, direito civil ou assistência, ou com termo de
violência, lidas uma a uma; 330 entraram. Em 20/10 as 865 ementas de Educação foram relidas para dois subtemas novos,
pedidos por ISG e IU: infraestrutura escolar (IN: obras, reforma, condições mínimas, água, mobiliário, normas de
segurança do prédio; 12 matérias, uma delas lida em Primeira infância) e adaptação e emergência climática nas escolas
(CL: 2 matérias). Defesa civil no currículo e o programa do RS para o ensino superior ficaram fora.
Também em 20/10: as 170 matérias da base completa com termo escolar que a Casa classificou fora de Educação foram lidas
uma a uma; 40 são de educação básica e entraram com o código do subtema (temas_clientes/educacao_outros_temas_lidos.csv;
o PL 4501/2020, ultraprocessados nas escolas, conta também em Saúde).
Em 04/11/2026 "Saúde da mulher e direitos reprodutivos" (MU) virou seis subtemas, a pedido do Coletivo Feminista:
aborto legal: acesso e garantia (AB); restrições ao aborto legal e criminalização (RE); vida desde a concepção e
direitos do nascituro (NC); gravidez infantil e aborto em meninas (GI); planejamento reprodutivo e contracepção (PP);
saúde da mulher, gestação e parto (MU, o que sobrou). As 65 matérias de MU foram relidas (11 mudaram de código; os
PDLs das portarias do Ministério da Saúde lidos pelo texto: 2.561/2020 e a nota técnica de 2024 sobre idade gestacional
são RE, a Rede de Atenção Materna e Infantil é MU) e as 128 matérias da base completa com termo reprodutivo foram lidas
uma a uma; 20 entraram (temas_clientes/reprodutivos_lidos.csv). Estupro de vulnerável sem gravidez fica em VC;
direito trabalhista e previdenciário da gestante fica fora. Ensino superior e o que não cabe em subtema de cliente ficam fora das colunas.

Também em 04/11/2026 a violência (VI) virou três subtemas: violência contra crianças e adolescentes (VC), violência
contra a mulher (VM) e casamento infantil e uniões precoces (CI). Foram lidas uma a uma as 330 matérias de VI, as 150
da leitura de 19/10 que tinham ficado fora, as 139 que só batiam com palavra-chave de cliente e nunca tinham sido lidas
e as 236 da base completa com termo de violência (estupro, assédio, dignidade sexual, feminicídio etc.) que nenhuma
leitura tinha pego. Regras: crime contra a dignidade sexual sem vítima especificada é VM, e VC quando a vítima é
vulnerável; medida de segurança para mulheres em transporte, bar ou evento é VM; violência sem recorte de mulher ou
criança, maus-tratos a animais, idosos, tráfico de pessoas sem recorte, LGBT, desaparecidos, assistência, adoção,
socioeducativo e direito trabalhista ficam fora. 415 entraram (temas_clientes/violencia_lidos.csv, agora com o código;
algumas levam também código de outro tema, como TA para propaganda de bebida). O que ficou fora em cada leitura, com o
motivo, está em temas_clientes/leituras_fora.csv.

Emendas lidas pelo texto (EMENDAS_LIDAS, 04/11/2026) contam como item; as do senador a um mesmo projeto contam uma vez.
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
    ("Saúde", "VC", "Violência contra crianças e adolescentes"),
    ("Saúde", "VM", "Violência contra a mulher"),
    ("Saúde", "CI", "Casamento infantil e uniões precoces"),
    ("Saúde", "PS", "Planos de saúde"),
    ("Saúde", "MV", "Medicamentos e vacinas"),
    ("Saúde", "DC", "Doenças e condições de saúde específicas"),
    ("Saúde", "AB", "Aborto legal: acesso e garantia"),
    ("Saúde", "RE", "Restrições ao aborto legal e criminalização"),
    ("Saúde", "NC", "Vida desde a concepção e direitos do nascituro"),
    ("Saúde", "GI", "Gravidez infantil e aborto em meninas"),
    ("Saúde", "PP", "Planejamento reprodutivo e contracepção"),
    ("Saúde", "MU", "Saúde da mulher, gestação e parto"),
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
# Emendas lidas pelo texto (as mesmas dos sinais diretos da etapa 24). Todas as emendas do senador a um mesmo projeto
# contam como um item no subtema, para emenda não pesar mais que projeto. A 697 (Teresa Leitão, autonomia progressiva) e a
# 769 (Magno Malta, recusa de tratamento) ao Código Civil não têm subtema.
EMENDAS_LIDAS = [
    # (senador, Casa, projeto emendado, números, ano, (tema, código), o que fazem)
    ("Efraim Filho", "Senado", "PLP 68/2024", "1490", 2024, ("Saúde", "AS"), "tira as bebidas açucaradas do Imposto Seletivo"),
    ("Laércio Oliveira", "Senado", "PLP 68/2024", "642", 2024, ("Saúde", "AS"), "limita a 1% o Imposto Seletivo das bebidas açucaradas"),
    ("Veneziano Vital do Rêgo", "Senado", "PLP 68/2024", "1730", 2024, ("Saúde", "AS"), "tira as bebidas açucaradas do Imposto Seletivo"),
    ("Magno Malta", "Senado", "PLP 68/2024", "1928", 2024, ("Saúde", "AS"), "tira as bebidas açucaradas do Imposto Seletivo"),
    ("Beto Faro", "Senado", "PLP 68/2024", "2197", 2024, ("Saúde", "AS"), "estende o Imposto Seletivo às bebidas ultraprocessadas"),
    ("Tereza Cristina", "Senado", "PLP 68/2024", "1873", 2024, ("Saúde", "TA"), "escalona o Imposto Seletivo das bebidas alcoólicas"),
    ("Carlos Portinho", "Senado", "PL 4/2025", "741", 2025, ("Saúde", "CI"), "fixa 18 anos como idade mínima para o casamento"),
    ("Damares Alves", "Senado", "PL 4/2025", "15, 26, 27, 38 e 40", 2025, ("Saúde", "NC"),
     "vida desde a concepção, personalidade civil e direitos do nascituro, vedação ao descarte de embriões"),
    ("Jorge Seif", "Senado", "PL 4/2025", "56, 61 e 67", 2025, ("Saúde", "NC"), "nascituro titular de direitos da personalidade, vida desde a concepção"),
    ("Hamilton Mourão", "Senado", "PL 4/2025", "139", 2025, ("Saúde", "NC"), "repete a regra vigente sobre os direitos do nascituro desde a concepção"),
    ("Magno Malta", "Senado", "PL 4/2025", "781", 2025, ("Saúde", "NC"), "suprime a vida pré-uterina como potencialidade"),
]
NIVEL = lambda n: "Não atua" if n == 0 else "Pouco" if n == 1 else "Atua" if n <= 4 else "Referência"


def main():
    # educação que a Casa classificou em outro tema (orçamento, segurança, tributos etc.) entra pela base completa
    extra = c.ler_csv("temas_clientes/educacao_outros_temas_lidos.csv")
    lidos = pd.concat([c.ler_csv("temas_clientes/subtemas_lidos.csv"), extra])
    cod = {}
    for r in lidos.itertuples():
        for s in str(r.Subtemas).split(","):
            tema, s = JUNTA.get((r._1, s), (r._1, s))
            cod.setdefault((r.Casa, r.Matéria), set()).add((tema, s))
    # violência quase toda caiu em Segurança na etapa 18, fora das matérias dos quatro temas: entra pela base completa
    viol = c.ler_csv("temas_clientes/violencia_lidos.csv")
    for r in viol.itertuples():
        for s in str(r.Subtemas).split(","):
            tema, s = JUNTA.get((r._1, s), (r._1, s))
            cod.setdefault((r.Casa, r.Matéria), set()).add((tema, s))
    # direitos reprodutivos fora dos quatro temas (Constituição, Código Penal, Conanda, FGTS) entram pela base completa
    repro = c.ler_csv("temas_clientes/reprodutivos_lidos.csv")
    for r in repro.itertuples():
        for s in str(r.Subtemas).split(","):
            cod.setdefault((r.Casa, r.Matéria), set()).add(("Saúde", s))
    reg = c.ler_csv("levantamento_senado/registros.csv").drop_duplicates(["Parlamentar", "Casa", "Matéria", "Registro"])
    reg = reg.merge(pd.concat([viol, extra, repro])[["Casa", "Matéria"]].drop_duplicates(), on=["Casa", "Matéria"])
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
            for sen, casa, proj, nums, ano, cod_e, faz in EMENDAS_LIDAS:
                if sen == nome and cod_e == (tema, s):
                    plural = "Emendas" if " e " in nums else "Emenda"
                    achou[(casa, f"{plural} {nums} ao {proj}")] = [{"autor"}, False, faz, ano]
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
