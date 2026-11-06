"""Transforma os textos corridos do perfil por tema (etapa 22) em tópicos para leitura na planilha: um item por
linha, com título de bloco em maiúsculas quando a célula tem partes diferentes (plenário e comissão, cada votação).
Só muda a apresentação; o conteúdo é o mesmo do CSV."""
import re

PONTO = '•'
# fim de frase: ponto seguido de espaço e maiúscula, sem cortar abreviação (art., nº, Dr., Sr., Sra., Profa.)
FRASE = re.compile(r'(?<!\bart\.)(?<!\bDr\.)(?<!\bSr\.)(?<!\bSra\.)(?<!\bProfa\.)(?<!\bnº\.)(?<!\bn\.)(?<=\.)\s+(?=[A-ZÁÉÍÓÚÂÊÔÃÕÇ])')


def frases(t):
    return [f.strip() for f in FRASE.split(t.strip()) if f.strip()]


def itens(lista):
    return '\n'.join(f'{PONTO} {x}' for x in lista)


def o_que_fez(t):
    fs = frases(t)
    return itens(fs) if len(fs) > 1 else t.strip()


def o_que_defende(t):
    plen, _, com = t.partition(' Em comissão: ')
    if not com:
        fs = frases(plen)
        return itens(fs) if len(fs) > 1 else plen.strip()
    return f'NO PLENÁRIO\n{itens(frases(plen))}\n\nEM COMISSÃO\n{itens(frases(com))}'


VOTO = re.compile(r'^(?P<tit>.+?) \((?P<sim>Sim = .+?)\): (?P<voto>.+?)(?: \((?P<ori>orientação [^)]*|[^)]* sem orientação registrada)\))?$')


def como_votou(t):
    blocos = []
    for linha in [l for l in t.split('\n') if l.strip()]:
        m = VOTO.match(linha.strip())
        if not m:
            blocos.append(linha.strip())
            continue
        voto = m['voto']
        voto = f'votou {voto}' if voto in ('Sim', 'Não', 'Abstenção') else voto
        detalhe = m['sim'] + (f'; {m["ori"]}' if m['ori'] else '')
        blocos.append(f'{PONTO} {m["tit"]}: {voto}\n   {detalhe}')
    return '\n\n'.join(blocos)


def requerimentos(t):
    partes = []
    for f in frases(t):
        cab, sep, resto = f.partition(': ')
        n = re.match(r'(?:Assinou )?(\d+) ', cab)
        lista = resto.rstrip('.').split('; ') if sep else []
        if n and sep and len(lista) == int(n.group(1)) and len(lista) > 1:
            partes.append(f'{cab}:\n{itens(lista)}')
        else:
            partes.append(f'{PONTO} {f}' if sep else f)
    return '\n\n'.join(partes)
