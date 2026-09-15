"""Remove da aba analise_planos e coerencia_planos os 10 candidatos que saíram
da corrida (7 indeferidos definitivos + 3 renúncias) e republica o cache
Parquet dos painéis.

Rodar de ~/eixo-eleicoes:  python3 outros/limpar_candidatos_fora.py
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

# 7 Indeferidos definitivos + 3 Renúncias
SAEM = {
    "100002551399",   # MA | ROBERTO ROCHA | PRTB | Indeferido
    "110002553937",   # MT | SARGENTO LAUDICÉRIO (LAU) | AGIR | Indeferido
    "140002538631",   # PA | CLEBER RABELO | PSTU | Renúncia
    "140002551357",   # PA | JOSÉ MOITA | DEMOCRATA | Renúncia
    "170002541258",   # PE | PROFESSOR JEREMIAS DO BANCO | DEMOCRATA | Indeferido
    "180002550421",   # PI | GUSTAVO PELO PIAUÍ OU GUSTAVO | AVANTE | Indeferido
    "200002550223",   # RN | CARLOS JARARACA | DC | Indeferido
    "270002546368",   # TO | SUBTENENTE LUIZ CARLOS | DEMOCRATA | Indeferido
    "50002536579",    # BA | ESTÊVÃO | DC | Indeferido
    "60002540336",    # CE | PEDRO BRITO | NOVO | Renúncia
}

gc = gspread.authorize(Credentials.from_service_account_file("credentials.json", scopes=[
    "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))
sh = gc.open_by_key(SID)

# ── 1. analise_planos ──
print("─── analise_planos ───")
ws_an = sh.worksheet("analise_planos")
v = ws_an.get_all_values()
cab = v[0]
isq = cab.index("sq_candidato")
fica = [r for r in v[1:] if str(r[isq]).strip() not in SAEM]
sai = len(v) - 1 - len(fica)
print(f"antes {len(v)-1}  saem {sai}  ficam {len(fica)}")
if sai != 600:
    raise SystemExit(f"Esperava 600 linhas a sair e achei {sai}. Nada foi gravado.")
reescrever_aba(ws_an, [cab] + fica, "analise_planos")
v2 = ws_an.get_all_values()
restantes = {str(r[isq]).strip() for r in v2[1:]}
saiu_ok = not (SAEM & restantes)
print(f"depois {len(v2)-1}  candidatos removidos presentes: {not saiu_ok}")
assert len(v2) - 1 == len(fica) and saiu_ok

# ── 2. coerencia_planos ──
print("─── coerencia_planos ───")
ws_co = sh.worksheet("coerencia_planos")
vc = ws_co.get_all_values()
cab_c = vc[0]
isq_c = cab_c.index("sq_candidato")
fica_c = [r for r in vc[1:] if str(r[isq_c]).strip() not in SAEM]
sai_c = len(vc) - 1 - len(fica_c)
print(f"antes {len(vc)-1}  saem {sai_c}  ficam {len(fica_c)}")
if sai_c != 10:
    raise SystemExit(f"Esperava 10 linhas a sair e achei {sai_c}. Nada foi gravado.")
reescrever_aba(ws_co, [cab_c] + fica_c, "coerencia_planos")
vc2 = ws_co.get_all_values()
restantes_c = {str(r[isq_c]).strip() for r in vc2[1:]}
saiu_ok_c = not (SAEM & restantes_c)
print(f"depois {len(vc2)-1}  candidatos removidos presentes: {not saiu_ok_c}")
assert len(vc2) - 1 == len(fica_c) and saiu_ok_c

# ── 3. Republicar Parquet ──
print("─── Parquet ───")
print(publicar_abas(gc, SID, ABAS_CACHE))
print("✓ Concluído.")
