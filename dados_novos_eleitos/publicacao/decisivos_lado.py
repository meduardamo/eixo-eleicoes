"""Quais itens 'precisa de checagem' mudam a classificação se o coordenador disser que não conta ou que o lado é o
contrário. Lê as evidências de clientes_classificacao.csv."""
import re, sys, pandas as pd
sys.path.insert(0, '.')
from outros.novos_eleitos.e24_clientes_senado import classifica
d = pd.read_csv('dados_novos_eleitos/temas_clientes/clientes_classificacao.csv', dtype=str).fillna('')
ID = re.compile(r'(Emenda (?:de Plenário )?\d+ ao \S+ \d+/\d{4}|(?:PLS|PLP|PEC|PDL|PDC|PL|RQS|REQ) \d+/\d{4})')
out = []
for r in d.itertuples():
    itens, bloco = [], None
    for l in r.Evidências.split('\n'):
        if l in ('A FAVOR DO CLIENTE', 'CONTRA O CLIENTE', 'NÃO CONTA (SÓ REGISTRO)'):
            bloco = {'A FAVOR DO CLIENTE': 1, 'CONTRA O CLIENTE': -1}.get(l, 0); continue
        if not bloco: continue
        t = re.sub(r'^\d+\. ', '', l)
        peso = 'médio' if t.startswith('coautor') else 'forte'
        m = ID.search(t)
        itens.append((peso, bloco, t, m.group(1) if m else t[:60], 'checagem' in t))
    # decisivo é o que muda Promotor, Neutro ou Detrator; as faixas abaixo do piso contam como uma só
    def cls(its, bruto=False):
        f = [x for k, x, *_ in its if k == 'forte']; m = [x for k, x, *_ in its if k == 'médio']
        c = classifica(f.count(1), f.count(-1), m.count(1), m.count(-1))
        return c if bruto or c in ('Promotor', 'Neutro', 'Detrator') else 'abaixo do piso'
    assert cls(itens, True) == r.Classificação, (r.Senador, r.Cliente, cls(itens, True), r.Classificação)
    atual = cls(itens)
    for i, it in enumerate(itens):
        if not it[4]: continue
        sem = cls(itens[:i] + itens[i + 1:])
        inv = cls(itens[:i] + [(it[0], -it[1], *it[2:])] + itens[i + 1:])
        out.append({'Cliente': r.Cliente, 'Área': r.Área, 'Senador': r.Senador, 'Projeto': it[3], 'lado': it[1], 'texto': it[2],
                    'atual': atual, 'se não conta': sem, 'se inverte': inv, 'decisivo': sem != atual or inv != atual})
o = pd.DataFrame(out)
o.to_csv('dados_novos_eleitos/publicacao/decisivos_lado.csv', index=False)
print('itens com checagem:', len(o), '| pares cliente x projeto:', len(o.drop_duplicates(['Cliente', 'Projeto'])))
dec = o[o.decisivo]
print('decisivos:', len(dec), '| pares cliente x projeto decisivos:', len(dec.drop_duplicates(['Cliente', 'Projeto'])))
print(dec.drop_duplicates(['Cliente', 'Projeto']).groupby(['Área', 'Cliente']).size().to_string())
