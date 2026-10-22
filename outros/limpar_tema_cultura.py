"""Tira da aba analise_planos as linhas do tema "Cultura", que em 22/10/2026
virou sete temas no eixo Cultura, e limpa as chaves velhas do resumos_eixos.

No resumos_eixos saem "Cultura" (a frase do tema antigo, que tem o mesmo nome do
eixo novo e impediria o texto do eixo de nascer) e "Cultura, esporte e turismo"
(o eixo que deixou de existir). Depois disso o workflow 14 com
so_resumo_eixo="Cultura|Esporte e turismo" escreve os dois eixos novos.

Backup das duas abas em ~/Downloads/eleicoes-2026/backups antes de gravar.

Rodar de ~/Downloads/eleicoes-2026/eixo-eleicoes:  python3 outros/limpar_tema_cultura.py
"""
import collections
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "outros"))
sys.path.insert(0, str(RAIZ))
from compartilhado.relatorios_sheets_utils import reescrever_aba  # noqa: E402
from compartilhado.cache_parquet import publicar_abas  # noqa: E402
from processar_planos import ABAS_CACHE, ANALISE_ABA, COERENCIA_ABA  # noqa: E402

SID = "1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA"
SAI = "Cultura"
CHAVES_VELHAS = ("Cultura", "Cultura, esporte e turismo")
BACKUP = Path.home() / "Downloads" / "eleicoes-2026" / "backups"

gc = gspread.authorize(Credentials.from_service_account_file("credentials.json", scopes=[
    "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))
sh = gc.open_by_key(SID)
carimbo = datetime.now().strftime("%Y%m%d_%H%M")
BACKUP.mkdir(parents=True, exist_ok=True)

# ── analise_planos ──
ws = sh.worksheet(ANALISE_ABA)
v = ws.get_all_values()
cab = v[0]
it = cab.index("tema")
with open(BACKUP / f"analise_planos_{carimbo}.csv", "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(v)
fica = [r for r in v[1:] if r[it] != SAI]
sai = len(v) - 1 - len(fica)
print("analise_planos: antes", len(v) - 1, "saem", sai, "ficam", len(fica))
# Um por plano: fora dessa faixa é sinal de aba lida pela metade ou duplicada.
if not 150 <= sai <= 260:
    raise SystemExit(f"{sai} linhas do tema {SAI!r} fora do esperado. Nada foi gravado.")
reescrever_aba(ws, [cab] + fica, ANALISE_ABA)
v2 = ws.get_all_values()
assert len(v2) - 1 == len(fica) and v2[0] == cab
print("analise_planos: depois", len(v2) - 1, "linhas do tema antigo:",
      collections.Counter(r[it] for r in v2[1:])[SAI])

# ── coerencia_planos: chaves velhas do resumos_eixos ──
wc = sh.worksheet(COERENCIA_ABA)
c = wc.get_all_values()
with open(BACKUP / f"coerencia_planos_{carimbo}.csv", "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(c)
ic = c[0].index("resumos_eixos")
mudou = 0
for r in c[1:]:
    if ic >= len(r) or not r[ic].strip():
        continue
    try:
        d = json.loads(r[ic])
    except ValueError:
        continue
    if any(k in d for k in CHAVES_VELHAS):
        for k in CHAVES_VELHAS:
            d.pop(k, None)
        r[ic] = json.dumps(d, ensure_ascii=False)
        mudou += 1
print("coerencia_planos: resumos com chave velha", mudou)
if mudou:
    reescrever_aba(wc, c, COERENCIA_ABA)
    assert len(wc.get_all_values()) == len(c)

print(publicar_abas(gc, SID, ABAS_CACHE))
