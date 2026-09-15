# Mapeamento dos novos eleitos (Senado, Câmara e Assembleias)

Pedido da Manu para o pós-eleição: saber quem chega em 2027 sem ser reeleição, de onde vem
e onde buscar os temas em que trabalha. O código roda antes da urna (pré-mapeamento dos
candidatos) e depois dela (eleitos).

Rodar sempre da raiz do repositório, na ordem:

| Etapa | Comando | Quando | O que faz |
|---|---|---|---|
| 1 | `python -m outros.novos_eleitos.e1_candidaturas` | uma vez, e de novo se houver renúncia | Candidaturas de 2026 que seguem na disputa (TSE + situação do DivulgaCand) |
| 2 | `python -m outros.novos_eleitos.e2_trajetoria` | uma vez | Mandatos eleitos desde 2006 (TSE) e exercício na Câmara e no Senado desde 2003 |
| 3 | `python -m outros.novos_eleitos.e3_composicao_atual` | uma vez, e de novo se mudar a composição | Quem está hoje nas cadeiras, ligado ao TSE |
| 4 | `python -m outros.novos_eleitos.e4_pre_mapeamento` | depois de 1 a 3 | Reeleição, volta ou novo, e a origem de cada candidatura. Confere a reeleição contra o Radar |
| 5 | `python -m outros.novos_eleitos.e5_eleitos --ciclo ele2026` | quando a totalização fechar | Eleitos por `sqcand`, com a classificação da etapa 4. Teste antes da urna: `--ciclo ele2022` |
| 6 | `python -m outros.novos_eleitos.e6_insumos --universo pre` | agora (`pre`) e depois da 5 (`eleitos`) | Autoria e temas na Câmara, relator atual na Câmara, autoria e relatoria no Senado, plano de governo, redes declaradas. `pre` é o que a Manu pediu antes da urna: competitivos do Senado que não disputam reeleição, quem hoje senta no Senado ou numa assembleia e disputa a Câmara, e quem hoje senta no Senado ou na Câmara e disputa uma assembleia |
| 7 | `python -m outros.novos_eleitos.e7_estreantes --universo pre --publicar` | depois da 6 | Aba "Estreantes, pesquisa manual": fila dos sem mandato, preservando o que a equipe digitou |
| 8 | `python -m outros.novos_eleitos.e8_assembleias_iu_marcela --universo pre` | depois da 4 ou da 5 | Assembleias, e quem sai da assembleia para a Câmara ou o Senado, cruzados com o levantamento do IU e com a planilha da Marcela |
| 9 | `python -m outros.novos_eleitos.e9_organogramas --universo pre` | depois da 4 ou da 5 | Nomes que aparecem nos organogramas das secretarias estaduais de educação (pasta IN da Marcela, nov/2024). Todo casamento é "a conferir" |
| 10 | `python -m outros.novos_eleitos.e10_consolidar --universo pre --publicar` | por último | Junta 4, 6, 8 e 9 pelo SQ_CANDIDATO e publica uma aba por Casa, só com as colunas pertinentes: "Mapeamento Senado/Câmara/Assembleias" (`pre`) ou "Eleitos Senado/Câmara/Assembleias" (`eleitos`) |

Só as etapas 7 e 10 gravam na planilha "Pré mapeamento". As outras geram CSV em `dados_novos_eleitos/`.
Toda aba sai com o padrão das abas de dado do Radar: cabeçalho de 54 px, linhas de 21 px, Montserrat,
largura de coluna pelo tamanho do conteúdo e primeiras colunas congeladas.

## Variáveis de ambiente

O repositório é público: nenhum id de planilha vai no código.

- `GOOGLE_CREDENTIALS_PATH` (padrão `credentials.json`)
- `SPREADSHEET_ID_TSE`: Candidaturas 2026 (TSE + Notícias), aba `candidaturas_divulgacand`
- `SPREADSHEET_ID_RECANDIDATURAS`: Radar do Congresso 2026, só leitura (abas de competitividade, para a composição e a conferência)
- `SPREADSHEET_ID_NOVOS_ELEITOS`: planilha "Pré mapeamento", onde todas as abas desta pasta são gravadas
- `SPREADSHEET_ID_IU_ESTADUAIS`: levantamento de deputados estaduais do Instituto Unibanco (etapa 8)
- `SPREADSHEET_ID_CE_ASSEMBLEIAS`: Composição CE - Assembleias, da Marcela (etapa 8)
- `DRIVE_PASTA_ORGANOGRAMAS`: pasta "IN/Estaduais" da Marcela (etapa 9); precisa estar compartilhada com a conta de serviço
- `NOVOS_ELEITOS_DADOS` (padrão `dados_novos_eleitos/`): CSVs intermediários e cache de API
- `TSE_CACHE` (padrão `dados_novos_eleitos/tse/`): zips do consulta_cand de 2006 a 2026

## Classificação

- **Reeleição**: está hoje em exercício na Casa que disputa.
- **Volta à Casa**: não está hoje, mas já exerceu nela.
- **Novo na Casa**: o resto.
- **Origem**: a cadeira que a pessoa ocupa hoje em outra Casa; sem ela, o mandato eleito em
  curso; sem ele, o último mandato. Suplente de senador só conta se exerceu.

A origem define onde buscar os temas:

| Tipo de origem | Onde buscar |
|---|---|
| Câmara | API da Câmara: autoria, coautoria e tema oficial de cada proposição; relatoria pelos arquivos anuais de proposições (2003 em diante), que só dão o relator do último andamento, não o histórico |
| Senado | API do Senado: autoria e relatoria |
| Governo estadual, prefeitura | Plano de governo no TSE e notícias |
| Assembleia | Dados abertos da assembleia ou Inteligov; levantamentos IU e Marcela |
| Câmara Municipal | Dados abertos da câmara municipal |
| Sem mandato eletivo | Biografia (pesquisa manual) e Instagram declarado no registro |

## Cuidados que já custaram erro

- **Chave de pessoa é o título de eleitor**, não o nome. Nome de urna não bate com nome
  parlamentar (Gaguim é CARLOS HENRIQUE AMORIM). 2024 veio sem CPF. CPF perde o zero da frente
  em alguns anos e no Sheets; `00000000004` é máscara.
- **O consulta_cand não apaga registro renunciado.** A situação vem do DivulgaCand.
- **Só o `_BRASIL.csv`.** Os arquivos por UF dentro do zip duplicam tudo.
- **Registro no CSV que não existe no DivulgaCand sai.** É registro velho: em 13/09/2026
  Gustavo Galassi aparecia como senador por MG com "-4" no título, e no DivulgaCand é 1º suplente.
- **Dois registros vivos da mesma pessoa no mesmo cargo** (13 casos em 13/09/2026): fica o de
  melhor situação e, no empate, o SQ mais novo. A etapa 1 lista cada par.
- **Não deduplicar eleitos por `SQ_CANDIDATO`.** Em 2006 o mesmo SQ aparece em pessoas diferentes.
- **Vice e suplente vêm `#NULO` em 2006 e 2010.** Nesses anos a trajetória não mostra vice-governador,
  vice-prefeito nem suplente de senador. Suplente que exerceu aparece pelo Senado.
- **O TSE regrava o desfecho de quem foi cassado.** Selma Arruda (MT, 2018) aparece como "NÃO ELEITO"
  e "INAPTO", então os eleitos de 2018 no TSE têm 53 senadores. Quem exerceu aparece pelo Senado.
- **Assembleias não publicam CPF.** A ligação é nome dentro da UF conferido pelo partido de
  2022. Quem não casar sai listado na etapa 3 e vai para `MAPA_MANUAL`.
- **Proporcional só tem eleito com a totalização fechada.** A etapa 5 para sem `--parcial`.
- **CPF e título ficam só nos CSVs locais**, nunca nas abas.
- Temas da Câmara saem da própria Câmara e são contados no Python. Nada de modelo inventando tema.
