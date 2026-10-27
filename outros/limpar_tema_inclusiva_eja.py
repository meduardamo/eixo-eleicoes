"""Tira da aba analise_planos as linhas do tema "Educação Inclusiva e EJA", que em
27/10/2026 virou três (Educação Inclusiva, EJA e Educação do Campo, Indígena e
Quilombola), e republica o cache Parquet dos painéis.

Backup da aba antes da limpeza (15.336 linhas, 213 do tema antigo):
~/Downloads/eleicoes-2026/backups-analise-planos/backup_analise_planos_20261027.csv

Rodar de ~/Downloads/eleicoes-2026/eixo-eleicoes:  python3 outros/limpar_tema_inclusiva_eja.py
"""
import collections
import sys
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "outros"))
sys.path.insert(0, str(RAIZ))
from compartilhado.relatorios_sheets_utils import reescrever_aba  # noqa: E402
from compartilhado.cache_parquet import publicar_abas  # noqa: E402
from processar_planos import ABAS_CACHE  # noqa: E402

SID = "1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA"
SAEM = {"Educação Inclusiva e EJA"}

gc = gspread.authorize(Credentials.from_service_account_file("credentials.json", scopes=[
    "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))
ws = gc.open_by_key(SID).worksheet("analise_planos")
v = ws.get_all_values()
cab = v[0]
it = cab.index("tema")
fica = [r for r in v[1:] if r[it] not in SAEM]
sai = len(v) - 1 - len(fica)
print("antes", len(v) - 1, "saem", sai, "ficam", len(fica))
if sai != 213:
    raise SystemExit(f"Esperava 213 linhas a sair e achei {sai}. Nada foi gravado.")
reescrever_aba(ws, [cab] + fica, "analise_planos")
v2 = ws.get_all_values()
c = collections.Counter(r[it] for r in v2[1:])
print("depois", len(v2) - 1, "linhas do tema antigo:", sum(c[t] for t in SAEM))
assert len(v2) - 1 == len(fica) and v2[0] == cab
print(publicar_abas(gc, SID, ABAS_CACHE))
