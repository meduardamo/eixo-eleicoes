import unittest
import sys
import os

# Adiciona o diretório 'outros' ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "outros"))

import analise_planos as ap
import processar_planos as pp


class TestTaxonomiaETemas(unittest.TestCase):
    def test_eixos_e_temas_totais(self):
        """Garante 14 eixos e 66 temas únicos."""
        self.assertEqual(len(ap.EIXOS), 14)
        todos_temas = [t for temas in ap.EIXOS.values() for t in temas]
        self.assertEqual(len(todos_temas), 66)
        self.assertEqual(len(set(todos_temas)), 66)
        self.assertEqual(len(ap.TEMAS), 66)
        self.assertEqual(len(ap.EIXO_DO_TEMA), 66)

    def test_termos_ancora_e_ausencia_completos(self):
        """Todos os 66 temas devem ter termos de âncora e de ausência."""
        for tema in ap.TEMAS:
            self.assertIn(tema, ap.TERMOS_ANCORA, f"Faltando âncora para {tema}")
            self.assertIn(tema, ap.TERMOS_AUSENCIA, f"Faltando ausência para {tema}")
            self.assertTrue(len(ap.TERMOS_ANCORA[tema]) > 0)
            self.assertTrue(len(ap.TERMOS_AUSENCIA[tema]) > 0)

    def test_tema_matematica(self):
        """Matemática deve estar configurada em Educação e em TEMAS_EDUCACAO."""
        self.assertIn("Matemática", ap.EIXOS["Educação"])
        self.assertIn("Matemática", ap.TEMAS_EDUCACAO)
        self.assertIn("matematic*", ap.TERMOS_ANCORA["Matemática"])
        self.assertIn("obmep", ap.TERMOS_AUSENCIA["Matemática"])

    def test_eixos_novos_lemann(self):
        """Lideranças públicas e Gestão de pessoas no setor público presentes e consistentes."""
        self.assertIn("Lideranças públicas", ap.EIXOS)
        self.assertEqual(
            list(ap.EIXOS["Lideranças públicas"].keys()),
            ["Seleção de Dirigentes", "Formação de Lideranças"],
        )

        self.assertIn("Gestão de pessoas no setor público", ap.EIXOS)
        self.assertEqual(
            list(ap.EIXOS["Gestão de pessoas no setor público"].keys()),
            ["Carreira e Remuneração", "Concurso e Quadro de Pessoal", "Desempenho e Capacitação"],
        )

        # Gestão pública e transparência deve ter Relação com Municípios e não Servidores e Municípios
        self.assertIn("Relação com Municípios", ap.EIXOS["Gestão pública e transparência"])
        self.assertNotIn("Servidores e Municípios", ap.TEMAS)


class TestSegmentosFundamental(unittest.TestCase):
    def test_constantes(self):
        self.assertEqual(ap.SEGMENTOS_FUNDAMENTAL, ("Anos iniciais", "Anos finais"))
        self.assertEqual(ap.SEGMENTO_NAO_ESPECIFICADO, "Segmento não especificado")
        self.assertEqual(ap.ETAPA_NAO_SE_APLICA, "Não se aplica")

    def test_segmentos_escritos_reconhecimento(self):
        # Nomes explícitos
        self.assertEqual(
            ap.segmentos_escritos("Melhorar o fundamental 1 e fundamental 2."),
            ["Anos iniciais", "Anos finais"],
        )
        self.assertEqual(
            ap.segmentos_escritos("Atenção aos anos iniciais do ensino fundamental."),
            ["Anos iniciais"],
        )
        self.assertEqual(
            ap.segmentos_escritos("Investimento nas séries finais da rede escolar."),
            ["Anos finais"],
        )
        # Ordinais com contexto escolar
        self.assertEqual(
            ap.segmentos_escritos("Ampliar vagas na escola do 1º ao 5º ano."),
            ["Anos iniciais"],
        )
        self.assertEqual(
            ap.segmentos_escritos("Ensino de qualidade do 6º ao 9º ano."),
            ["Anos finais"],
        )
        self.assertEqual(
            ap.segmentos_escritos("Apoio aos estudantes do 1º ao 9º ano."),
            ["Anos iniciais", "Anos finais"],
        )

    def test_segmentos_escritos_falsos_positivos(self):
        # Ano de governo / mandato não conta
        self.assertEqual(
            ap.segmentos_escritos("No primeiro ano de governo teremos metas fiscais."),
            [],
        )
        # Marco de alfabetização sem fundamental não conta como anos iniciais
        self.assertEqual(
            ap.segmentos_escritos("Alfabetizar todas as crianças até o 2º ano."),
            [],
        )
        # Ensino médio não conta
        self.assertEqual(
            ap.segmentos_escritos("Apoio ao 1º ano do ensino médio."),
            [],
        )

    def test_conferir_segmentos_fundamental(self):
        # Não menciona
        nm = ap.conferir_segmentos_fundamental([], "", "contexto qualquer", "Não menciona")
        self.assertEqual(nm["segmentos_fundamental"], ap.ETAPA_NAO_SE_APLICA)
        self.assertEqual(nm["evidencia_segmentos_fundamental"], "")

        # Sem evidência sustentada
        vazio = ap.conferir_segmentos_fundamental(
            ["Anos iniciais"], "evidencia inventada", "contexto real", "Propõe ação"
        )
        self.assertEqual(vazio["segmentos_fundamental"], ap.SEGMENTO_NAO_ESPECIFICADO)

        # Evidência válida
        ctx = "Na educação básica, vamos reformar as escolas de anos iniciais e anos finais."
        ev = "reformar as escolas de anos iniciais e anos finais."
        valido = ap.conferir_segmentos_fundamental(
            ["Anos iniciais", "Anos finais"], ev, ctx, "Propõe ação"
        )
        self.assertEqual(valido["segmentos_fundamental"], "Anos iniciais | Anos finais")
        self.assertEqual(valido["evidencia_segmentos_fundamental"], ev)

    def test_segmentos_e_propostas_do_plano_nao_menciona(self):
        res = ap.segmentos_e_propostas_do_plano("", "Não menciona")
        self.assertEqual(res["segmentos_fundamental"], "Não se aplica")
        self.assertEqual(res["evidencia_segmentos_fundamental"], "")
        self.assertEqual(res["segmentos_propostos_fundamental"], "")


class TestColunasProcessarPlanos(unittest.TestCase):
    def test_cols_contem_segmentos_fundamental(self):
        self.assertIn("segmentos_fundamental", pp.COLS)
        self.assertIn("evidencia_segmentos_fundamental", pp.COLS)
        self.assertIn("segmentos_propostos_fundamental", pp.COLS)


if __name__ == "__main__":
    unittest.main()
