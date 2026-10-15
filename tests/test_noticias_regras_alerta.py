import unittest
from outros.noticias_eleicoes_scraper import aplicar_regra_alerta


class TestRegrasAlertaNoticias(unittest.TestCase):
    def test_transicao_rotina_fiscal_sem_anuncio_rebaixada(self):
        """Prestação de contas rotineira de secretário em exercício na ALEP não é transição de governo."""
        dados = {
            "alerta_tema": "transição",
            "cargo": "governador",
            "candidato": "Sergio Moro",
            "uf": "PR",
            "status": "cobertura geral",
        }
        texto = (
            "Paraná encerra agosto com R$ 27,6 bilhões em caixa, o 3º maior do Brasil. "
            "Os dados integram a prestação de contas do segundo quadrimestre na Assembleia Legislativa. "
            "O resultado compõe o cenário fiscal que será recebido pelo governador eleito Sergio Moro. "
            "Durante a apresentação do relatório, o secretário da Fazenda em exercício, Luiz Paulo Budal, "
            "atribuiu os resultados ao planejamento fiscal. Disponibilidade de caixa e investimentos."
        )
        res = aplicar_regra_alerta(dados, texto=texto, titulo="Paraná encerra agosto com R$ 27,6 bilhões em caixa")
        self.assertEqual(res["alerta"], "não")
        self.assertEqual(res["alerta_tema"], "")

    def test_transicao_movimento_real_mantida(self):
        """Indicação ou cotação para pasta prioritária de novo governo vira alerta de transição."""
        # Caso RJ (Saúde)
        dados_rj = {
            "alerta_tema": "transição",
            "cargo": "outro",
            "candidato": "Jorge Miranda",
            "uf": "RJ",
            "status": "cobertura geral",
        }
        texto_rj = (
            "Ex-prefeito de Mesquita, Jorge Miranda deve ser chamado para resolver a Saúde. "
            "Ele é cotado como o nome que ganhou espaço no secretariado para a Secretaria estadual de Saúde "
            "no governo Douglas Ruas, integrando as discussões da equipe de transição."
        )
        res_rj = aplicar_regra_alerta(dados_rj, texto=texto_rj, titulo="Jorge Miranda deve ser chamado para resolver a Saúde")
        self.assertEqual(res_rj["alerta"], "sim")
        self.assertEqual(res_rj["alerta_tema"], "transição")

        # Caso RS (Fazenda)
        dados_rs = {
            "alerta_tema": "transição",
            "cargo": "outro",
            "candidato": "Claudio Branchieri",
            "uf": "RS",
            "status": "cobertura geral",
        }
        texto_rs = (
            "Claudio Branchieri é aliado de Zucco, governador eleito do Rio Grande do Sul. "
            "Questionado se a proximidade vai se tornar um convite para ser secretário da Fazenda, "
            "ele afirmou estar à disposição da equipe de transição."
        )
        res_rs = aplicar_regra_alerta(dados_rs, texto=texto_rs, titulo="Quem é o deputado Claudio Branchieri")
        self.assertEqual(res_rs["alerta"], "sim")
        self.assertEqual(res_rs["alerta_tema"], "transição")

    def test_fato_eleitoral_pec_6x1_rebaixada(self):
        """Votação parlamentar da PEC 6x1 em Brasília não é fato eleitoral da disputa executiva."""
        dados = {
            "alerta_tema": "fato-eleitoral",
            "cargo": "senador",
            "candidato": "Cleitinho Azevedo",
            "uf": "MG",
            "status": "cobertura geral",
        }
        texto = "Senado aprova proposta em primeiro turno. Cleitinho Azevedo diz que fim da escala 6x1 representa dignidade."
        res = aplicar_regra_alerta(dados, texto=texto, titulo="Cleitinho Azevedo diz que fim da escala 6x1 representa 'dignidade'")
        self.assertEqual(res["alerta"], "não")
        self.assertEqual(res["alerta_tema"], "")

    def test_fato_eleitoral_vagas_tce_rebaixada(self):
        """Vagas burocráticas em Tribunal de Contas do Estado não são fatos eleitorais da eleição."""
        dados = {
            "alerta_tema": "fato-eleitoral",
            "cargo": "governador",
            "candidato": "Carlos Brandão",
            "uf": "MA",
            "status": "cobertura geral",
        }
        texto = "Aposentadoria de conselheiro abre mais uma vaga no TCE-MA a partir de novembro."
        res = aplicar_regra_alerta(dados, texto=texto, titulo="TCE-MA terá mais uma vaga de conselheiro aberta")
        self.assertEqual(res["alerta"], "não")
        self.assertEqual(res["alerta_tema"], "")

    def test_fato_eleitoral_sem_candidato_rebaixada(self):
        """Fato eleitoral sem candidato identificado é informe burocrático e não vira alerta."""
        dados = {
            "alerta_tema": "fato-eleitoral",
            "cargo": None,
            "candidato": None,
            "uf": "SP",
            "status": "cobertura geral",
        }
        texto = "Auditoria das urnas para o 2º turno será realizada nesta quinta-feira pelo Tribunal Regional Eleitoral."
        res = aplicar_regra_alerta(dados, texto=texto, titulo="Auditoria das urnas para o 2º turno")
        self.assertEqual(res["alerta"], "não")
        self.assertEqual(res["alerta_tema"], "")

    def test_visita_presidencial_visita_familiar_rebaixada(self):
        """Visita privada ou familiar em Brasília não é comício/visita eleitoral de campanha a estado."""
        dados = {
            "alerta_tema": "visita-presidencial",
            "cargo": "presidente",
            "candidato": "Flávio Bolsonaro",
            "uf": "DF",
            "status": "cobertura geral",
        }
        texto = "Flávio visita Bolsonaro pela segunda vez após fim de suspensão autorizada pelo STF em Brasília."
        res = aplicar_regra_alerta(dados, texto=texto, titulo="Flávio visita Bolsonaro pela segunda vez")
        self.assertEqual(res["alerta"], "não")
        self.assertEqual(res["alerta_tema"], "")

    def test_visita_presidencial_coluna_desconexa_rebaixada(self):
        """Coluna de jornal com manchete alheia (ex: Papa) não gera alerta com título desconexo."""
        dados = {
            "alerta_tema": "visita-presidencial",
            "cargo": "presidente",
            "candidato": "Flávio Bolsonaro",
            "uf": "PA",
            "status": "cobertura geral",
        }
        texto = (
            "Coluna Repórter 70. Papa Leão XIV pede orações após cirurgia no pulmão. "
            "Em nota de bastidores, o candidato Flávio Bolsonaro realizará comício em Marabá na próxima semana."
        )
        res = aplicar_regra_alerta(dados, texto=texto, titulo="R70: 'Rezem por mim', pede Papa Leão XIV após diagnóstico de câncer")
        self.assertEqual(res["alerta"], "não")
        self.assertEqual(res["alerta_tema"], "")

    def test_visita_presidencial_legitima_mantida(self):
        """Visita presidencial de campanha real em estado com menção política no título segue como alerta."""
        dados = {
            "alerta_tema": "visita-presidencial",
            "cargo": "presidente",
            "candidato": "Flávio Bolsonaro",
            "uf": "MG",
            "status": "cobertura geral",
        }
        texto = "Flávio Bolsonaro participa de manifestações do Bloquinho do 22 com Nikolas Ferreira em Belo Horizonte para disputar votos do segundo turno."
        res = aplicar_regra_alerta(dados, texto=texto, titulo="Flávio e Lula retornam a Minas para disputar votos decisivos do 2º turno")
        self.assertEqual(res["alerta"], "sim")
        self.assertEqual(res["alerta_tema"], "visita-presidencial")


if __name__ == "__main__":
    unittest.main()
