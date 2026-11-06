"""Textos de subtema usados por várias abas (Parlamentares, Perfil por tema, Clientes, Por cliente, Por parlamentar).
Base: temas_clientes/subtemas.csv (etapa 25, nível por senador e subtema) e temas_clientes/subtemas_clientes.csv
(subtemas de cada cliente, conferidos com a base de pautas e com as palavras do monitor do DOU em 19/10/2026)."""
import pandas as pd

D = 'dados_novos_eleitos/temas_clientes/'
TEMAS = ['Saúde', 'Educação', 'Esporte', 'Primeira infância']
NIVEIS = ['Referência', 'Atua', 'Pouco']

_st = pd.read_csv(D + 'subtemas.csv', dtype=str).fillna('').set_index('Senador')
SUB = {t: [c.split('|')[1] for c in _st.columns if c.split('|')[0] == t] for t in TEMAS}
_cli = pd.read_csv(D + 'subtemas_clientes.csv', dtype=str)
SUB_CLIENTE = {c: list(zip(g.Tema, g.Subtema)) for c, g in _cli.groupby('Cliente', sort=False)}


def nivel(senador, tema, sub):
    return _st.at[senador, f'{tema}|{sub}'] if senador in _st.index else ''


def _por_nivel(senador, pares, niveis):
    grupos = {k: [s for t, s in pares if nivel(senador, t, s) == k] for k in niveis}
    return [(k, v) for k, v in grupos.items() if v]


def destaques(senador):
    """Parlamentares: só os subtemas em que o senador atua ou é referência, nos quatro temas."""
    pares = [(t, s) for t in TEMAS for s in SUB[t]]
    partes = [f"{k}: {'; '.join(v)}" for k, v in _por_nivel(senador, pares, ['Referência', 'Atua'])]
    return '\n'.join(partes) if partes else 'Nenhum subtema com mais de um projeto'


def _topicos(grupos):
    """Um bloco por nível, com título em maiúsculas e um subtema por linha."""
    return '\n\n'.join(k.upper() + '\n' + '\n'.join(f'• {s}' for s in v) for k, v in grupos)


def por_tema(senador, tema):
    """Perfil por tema: todos os subtemas do tema com algum projeto, por nível."""
    grupos = _por_nivel(senador, [(tema, s) for s in SUB[tema]], NIVEIS)
    return _topicos(grupos) if grupos else 'Nenhum projeto nos subtemas deste tema'


def por_cliente(senador, cliente):
    """Clientes: atuação do senador só nos subtemas do cliente."""
    pares = SUB_CLIENTE.get(cliente, [])
    grupos = _por_nivel(senador, pares, NIVEIS)
    if not grupos:
        return f"Não atua em nenhum dos {len(pares)} subtemas do cliente" if len(pares) > 1 else "Não atua no subtema do cliente"
    return _topicos(grupos)


def lista_cliente(cliente):
    return '; '.join(s for _, s in SUB_CLIENTE.get(cliente, []))
