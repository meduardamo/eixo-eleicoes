"""Ordem e cor das abas da planilha da 58ª Legislatura, em dois níveis:
consulta (escolhe o cliente ou o senador e lê a ficha, aba marinho), base (tabelas com filtro, aba cinza) e
referência no fim (subtemas por cliente, lado dos projetos e Metodologia, aba clara). EM_REVISAO vai para a frente enquanto
os coordenadores revisam; esvaziar a lista devolve a aba ao fim. Todos os scripts de publicação chamam daqui, para a ordem não depender de qual rodou
por último."""
CONSULTA = ['Por cliente', 'Por parlamentar']
BASE = ['Parlamentares', 'Perfil por tema', 'Votações-chave', 'Clientes']
FIM = ['Subtemas por cliente', 'Lado dos projetos', 'Definição dos subtemas', 'Metodologia']
EM_REVISAO = ['Subtemas por cliente', 'Lado dos projetos']
# ocultas: o conteúdo já aparece em Por cliente e Por parlamentar, que leem delas por fórmula
OCULTAS = ['Perfil por tema', 'Clientes']
COR = {**{t: '#192D4E' for t in CONSULTA}, **{t: '#9A9A94' for t in BASE}, **{t: '#E1E1E1' for t in FIM}}
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
