# Metodologias dos agregadores presidenciais de 2026

Pesquisa documental realizada em 13 de agosto de 2026, com a descrição do Eixo atualizada em 9 de setembro de 2026, usando somente páginas oficiais dos próprios agregadores e o código-fonte do Eixo. O objetivo é separar o que foi efetivamente publicado do que seria necessário inferir para reproduzir os números.

## Resumo executivo

| Agregador | Fórmula pública | Reproduzível de forma independente? | Principal diferença para o Eixo |
|---|---|---|---|
| ABC Dados | Sim: `w = √n × 2^(-d/30)` e média ponderada | Quase. A fórmula e os critérios correntes são públicos, mas não há regra geral publicada para futuras exclusões de outliers | Pondera por amostra e aplica decaimento contínuo; não pondera pela qualidade histórica do instituto |
| Índice CNN/Ipespe | Não. Divulga apenas as famílias de fatores do modelo | Não | Usa modelo bayesiano/machine learning, amostra, recência, histórico/metodologia do instituto e outliers, mas não publica parâmetros |
| Eixo | Sim, no código e na aba de metodologia | Sim | Peso por classificação do instituto × √amostra × decaimento exponencial com meia-vida de 30 dias |

As diferenças numéricas entre os agregadores **não demonstram, por si só, erro no Eixo**. Os três estimadores usam universos, cenários e pesos diferentes. Não é tecnicamente recomendável alterar a fórmula apenas para fazer o resultado convergir a outro painel.

## ABC Dados

### O que é público

O ABC publica a fórmula completa do peso de cada pesquisa:

```text
w_i = √n_i × 2^(-d_i/30)

MP_c = Σ(w_i × x_i,c) / Σw_i
```

Nessa definição, `n` é o tamanho da amostra, `d` é a quantidade de dias entre o início da coleta e a data de referência, e `x_i,c` é o percentual do candidato `c`. O fator temporal tem meia-vida de 30 dias: depois de 30, 60 e 90 dias, a pesquisa conserva, respectivamente, 50%, 25% e 12,5% de seu peso temporal. Pesquisas nas quais um candidato não foi testado são retiradas do denominador desse candidato. A meia-vida é apresentada como um parâmetro ajustável na planilha do agregador. [Fonte oficial: fórmula e parâmetros do ABC Dados](https://abcdados.com.br/agregador2026/).

O ABC afirma que não corrige pelo método de coleta nem pelo viés histórico de cada instituto; portanto, institutos diferentes são tratados como igualmente confiáveis antes dos componentes de amostra e recência. [Fonte oficial: limitações declaradas pelo ABC Dados](https://abcdados.com.br/agregador2026/).

### Inclusão e cenários

Para a corrida presidencial, o ABC declara três requisitos de inclusão:

1. pesquisa nacional registrada no TSE e com metodologia conhecida;
2. cenário estimulado de primeiro turno que teste simultaneamente Lula, Flávio Bolsonaro e Ronaldo Caiado;
3. se a mesma rodada trouxer vários cenários, seleção apenas do cenário principal — definido como o mais amplo, com mais candidatos — para não duplicar o peso do instituto.

A página também informa que a base considerada parte de pesquisas divulgadas desde janeiro de 2026 e identifica os principais institutos usados. [Fonte oficial: pesquisas e critérios considerados pelo ABC Dados](https://abcdados.com.br/agregador2026/).

Há uma inconsistência temporal na própria nota pública: essa passagem fala em janeiro de 2026, enquanto outras seções descrevem uma série desde agosto de 2025 e a exclusão de uma pesquisa de dezembro de 2025. A data inicial efetiva precisa ser confirmada na base usada em cada atualização.

### Outliers

O ABC documenta uma exclusão editorial específica: a Atlas Intel de 18 de dezembro de 2025 foi retirada porque o resultado de Lula, 47,3%, estava mais de oito pontos acima das demais pesquisas do período, que marcavam de 38% a 41%. A página atribui a discrepância ao método online e ao tamanho excepcional da amostra. [Fonte oficial: tratamento de exclusões do ABC Dados](https://abcdados.com.br/agregador2026/).

Isso permite reproduzir a exclusão já feita, mas **não define um algoritmo geral de outlier**. Não são publicados, por exemplo, um limiar que valha para todos os candidatos e períodos, uma janela de comparação ou uma regra automática para futuras exclusões. Esse componente editorial não é prospectivamente reproduzível.

### Grau de reprodutibilidade

A parte matemática é reproduzível: com a mesma base, a mesma data de referência e as mesmas exclusões, um terceiro consegue recalcular o resultado. Permanecem dependentes de decisão editorial a identificação do “cenário principal”, a composição atualizada da base e eventuais novas exclusões por outlier. Portanto, o método é **quase integralmente reproduzível**, mas não inteiramente automático a partir do texto público.

## Índice CNN/Ipespe Analítica

### O que é público

A CNN informa que o modelo foi desenvolvido pelo Ipespe Analítica e usa estatística bayesiana e machine learning. A descrição oficial afirma que o cálculo:

- dá mais peso às pesquisas recentes;
- dá mais peso a amostras maiores;
- classifica institutos segundo histórico e metodologia, favorecendo institutos tradicionais;
- detecta e pondera resultados atípicos, em vez de permitir que um ponto destoante domine o agregado;
- é atualizado quando uma nova pesquisa é divulgada.

[Fonte oficial: metodologia do Índice CNN 2026](https://www.cnnbrasil.com.br/eleicoes/indice-cnn-saiba-como-funciona-o-agregador-de-pesquisas-das-eleicoes-2026/).

### Inclusão e cenários

O critério explícito de inclusão é que a pesquisa esteja registrada na Justiça Eleitoral. A CNN publica ainda uma lista de institutos que fazem parte do índice. [Fonte oficial: pesquisas registradas e institutos do Índice CNN](https://www.cnnbrasil.com.br/eleicoes/indice-cnn-saiba-como-funciona-o-agregador-de-pesquisas-das-eleicoes-2026/).

A documentação oficial consultada **não especifica** para a disputa presidencial:

- qual cenário é escolhido quando uma rodada apresenta vários;
- se candidatos ausentes são retirados do denominador, imputados ou tratados por um modelo conjunto;
- se há exigência de um conjunto mínimo de candidatos no mesmo cenário;
- como pesquisas nacionais, estaduais e eventuais recortes são separados no algoritmo.

### O que não é reproduzível

A CNN/Ipespe não publica a equação do estimador, o código ou os seguintes parâmetros necessários a uma reprodução independente:

- forma e intensidade do decaimento temporal;
- função exata do tamanho amostral;
- notas e coeficientes atribuídos a cada instituto;
- variáveis que definem “histórico”, “metodologia” e “instituto tradicional”;
- priors, estrutura do modelo bayesiano, dados e hiperparâmetros do machine learning;
- método, janela e limiar usados para detectar e ponderar outliers;
- regra de seleção de cenários e tratamento de candidatos não testados.

Assim, é possível compreender a direção dos ajustes do Índice CNN, mas não recalcular seus números. A própria aplicação pública remete à mesma explicação geral, sem oferecer nota técnica adicional ou fórmula. [Fonte oficial: aplicação do Índice CNN](https://indice-cnn.vercel.app/).

## Comparação com o Eixo

O painel usa a coluna `media_hibrida_30d`, calculada por pesquisa e candidato com `w = score do instituto × √amostra × 2^(-dias/30)` e `Σ(w × percentual) / Σ(w)`. A meia-vida de 30 dias não é uma janela com corte: pesquisas antigas continuam contribuindo com peso menor. A coluna legada `media_movel_13d` ainda existe na base, mas não é a série híbrida exibida no gráfico de média ponderada.

Seleciona-se o cenário principal mais amplo por pesquisa, turno e confronto. Candidatos não testados não entram no denominador. No primeiro turno presidencial, o cenário selecionado precisa incluir Lula e Flávio juntos. No segundo turno, o confronto também faz parte do `poll_id` e permanece separado no agrupamento. Pesquisas sem amostra positiva ou percentual/data utilizáveis ficam fora do modelo híbrido.

Os pesos atuais são A+ = 0,85; A = 0,76; A− = 0,67; B+ = 0,58; B = 0,49; B− = 0,40; C+ = 0,31; C = 0,22; C− = 0,135; D = 0,05. Sem classificação, o peso é 0,25. Esses pesos não são correções explícitas de viés por candidato. [Fonte primária: implementação](../compartilhado/pollingdata_scraper.py), funções `score_instituto`, `selecionar_cenario_principal` e `calcular_agregadores_paralelos_resultados_bi`.

### Datas e leitura retrospectiva

A pesquisa entra na série a partir do **fim do campo**, não da divulgação. A idade no fator temporal é contada desde o **início da coleta**; se ausente, usa-se o fim. A série é retrospectiva: não reproduz necessariamente as informações que já eram públicas em cada data. Para reconstruir o conhecimento disponível naquele momento, seria necessário registrar e usar a data de divulgação como critério de entrada. Esta atualização documenta o comportamento existente e não altera o cálculo.

| Decisão | Eixo | ABC Dados | CNN/Ipespe |
|---|---|---|---|
| Recência | Decaimento exponencial, meia-vida de 30 dias; sem corte aos 30 dias | Decaimento exponencial contínuo, meia-vida de 30 dias; não há corte adicional divulgado dentro da base elegível | Mais recentes pesam mais; função não publicada |
| Amostra | Peso proporcional a `√n` | Peso proporcional a `√n` | Amostras maiores pesam mais; função não publicada |
| Instituto | Peso fixo pela classificação; não avaliado = `0,25` | Sem correção por histórico/qualidade do instituto | Histórico, metodologia e tradição entram no modelo; notas não publicadas |
| Cenários da mesma rodada | Um cenário principal por pesquisa/confronto; presidencial de 1º turno exige Lula e Flávio juntos | Um único cenário principal e mais amplo; exige Lula, Flávio e Caiado juntos | Regra não publicada |
| Outliers | Sem regra adicional de exclusão ou redução; o peso do instituto continua valendo | Exclusão editorial específica documentada, sem regra geral | Detecta e pondera; algoritmo não publicado |

O Eixo e o ABC compartilham componentes de amostra e recência. A classificação dos institutos e os critérios de inclusão podem produzir resultados diferentes. Não se pode concluir qual modelo responde melhor sem validação histórica. As descrições dos outros agregadores acima refletem a consulta documental original; seus critérios podem mudar.

## Implicações para calibração do Eixo

1. **Não usar convergência como alvo.** Alterar parâmetros até coincidir com CNN ou ABC produziria sobreajuste a resultados externos calculados com universos diferentes.
2. **Reproduzir o ABC como série de controle.** A fórmula publicada permite implementar um cálculo paralelo, sem substituir o indicador oficial do Eixo, e decompor a diferença em amostra, recência e cenário.
3. **Equalizar a entrada antes de comparar.** O diagnóstico deve usar a mesma data de referência, o mesmo conjunto de pesquisas, o mesmo cenário e as mesmas exclusões. Sem isso, a diferença não mede apenas a fórmula.
4. **Testar alternativas por backtest.** A escolha da meia-vida e dos pesos por instituto e amostra deve ser avaliada contra eleições anteriores e por estabilidade/erro fora da amostra, não pela proximidade momentânea de outro agregador.
5. **Tratar a CNN como benchmark externo, não como especificação.** Sem fórmula e parâmetros públicos, qualquer tentativa de copiá-la seria uma aproximação não verificável.

O experimento de menor risco é manter o Eixo atual e gerar, lado a lado, três séries internas: configuração vigente, fórmula exata do ABC e uma ablação do Eixo sem peso por instituto. Isso mostra qual decisão produz a divergência antes de qualquer mudança pública.
