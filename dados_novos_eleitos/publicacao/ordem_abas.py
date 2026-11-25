"""Ordem e cor das abas da planilha da 58ª Legislatura, em dois níveis:
consulta (escolhe o cliente ou o senador e lê a ficha, aba marinho), base (tabelas com filtro, aba cinza) e
referência no fim (subtemas por cliente, lado dos projetos e Metodologia, aba clara). EM_REVISAO vai para a frente enquanto
os coordenadores revisam; esvaziar a lista devolve a aba ao fim. Todos os scripts de publicação chamam daqui, para a ordem não depender de qual rodou
por último."""
# 16/11/2026: segunda etapa (revisão manual da equipe, até 23/11), na ordem de uso: onde se trabalha (lista e andamento),
# a referência do cliente, a ficha de quem gera dúvida (Por parlamentar antes de Por cliente), a base e, no fim, a
# referência de regra. Lado dos projetos fica logo depois da descrição enquanto a Leka responde (Câmara); depois vai para EM_REVISAO = REVISAO_MANUAL.
# Subtemas por cliente saiu da revisão (decisão dela em 16/11) e foi para o fim.
# Ao fechar a segunda etapa: esvaziar REVISAO_MANUAL e EM_REVISAO e voltar CONSULTA para ['Por cliente', 'Por parlamentar'].
REVISAO_MANUAL = ['Revisão: lista prioritária', 'Revisão: andamento']
EM_REVISAO = REVISAO_MANUAL  # 16/11: Leka respondeu tudo, Lado da Câmara vazio e oculto
CONSULTA = ['Por parlamentar', 'Por cliente']
BASE = ['Parlamentares', 'Perfil por tema', 'Votações-chave', 'Ajuste fiscal', 'Clientes']
# 16/11: Descrição dos clientes junto das referências de subtema (pedido dela)
FIM = ['Definição dos subtemas', 'Subtemas por cliente', 'Descrição dos clientes', 'Lado dos projetos', 'Metodologia']
# ocultas: o conteúdo já aparece em Por cliente e Por parlamentar, que leem delas por fórmula
OCULTAS = ['Perfil por tema', 'Clientes']
COR = {**{t: '#E8A600' for t in REVISAO_MANUAL}, **{t: '#192D4E' for t in CONSULTA}, **{t: '#9A9A94' for t in BASE}, **{t: '#E1E1E1' for t in FIM}}
cor = lambda h: {'red': int(h[1:3], 16) / 255, 'green': int(h[3:5], 16) / 255, 'blue': int(h[5:7], 16) / 255}


def ordenar(sh):
    abas = sh.worksheets()
    por = {w.title: w for w in abas}
    inicio = [por[t] for t in EM_REVISAO + CONSULTA + BASE if t in por]
    fim = [por[t] for t in FIM if t in por and t not in EM_REVISAO]
    sh.reorder_worksheets(inicio + [w for w in abas if w not in inicio + fim] + fim)
    sh.batch_update({'requests': [{'updateSheetProperties': {'properties': {'sheetId': por[t].id, 'tabColorStyle': {'rgbColor': cor(c)}},
                                                             'fields': 'tabColorStyle'}} for t, c in COR.items() if t in por]
                    + [{'updateSheetProperties': {'properties': {'sheetId': w.id, 'hidden': w.title in OCULTAS}, 'fields': 'hidden'}} for w in abas]
                    # alinhamento vertical no meio em toda a planilha, inclusive células sem formato próprio
                    + [{'repeatCell': {'range': {'sheetId': w.id}, 'cell': {'userEnteredFormat': {'verticalAlignment': 'MIDDLE'}},
                                       'fields': 'userEnteredFormat.verticalAlignment'}} for w in abas]})
    return [w.title for w in sh.worksheets()]
