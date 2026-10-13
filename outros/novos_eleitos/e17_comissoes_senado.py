"""Etapa 17. O que cada senador falou e votou nas comissões do Senado.

Completa a etapa 13, que só traz o plenário:
- falas: notas taquigráficas de todas as reuniões de comissão realizadas desde fevereiro de 2019
  (legislaturas 56 e 57), recortadas por orador. Fica a fala de quem está na lista de senadores da
  próxima legislatura; convidado e senador que sai não entram;
- votos: votações nominais em comissão de cada senador.

A nota vem em blocos de quatro minutos, sem separar orador. O recorte é pelo cabeçalho que a
taquigrafia põe em cada intervenção ("O SR. FULANO (Bloco/PARTIDO - UF. Para discutir.) –"); quem
preside aparece como "O SR. PRESIDENTE (Fulano. Bloco...)". O nome é casado com o nome parlamentar.

Saída em dados_novos_eleitos/perfil_senado_comissoes/: reunioes.csv, falas.csv (com o texto),
falas_resumo.csv (senador x colegiado) e votos.csv.
Rodar: python -m outros.novos_eleitos.e17_comissoes_senado   (depois da etapa 13)
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
import requests

from outros.novos_eleitos import comum as c
from outros.novos_eleitos.e11_trajetoria_competitivos import SENADO, senado_json

INICIO = (2019, 2)
GRUPOS = ("curso", "reeleitos", "novos")
CABECALHO = re.compile(r"(?:^|\n)[ \t]*(?:O SR\.|A SRA\.) ([^\(\n–]{2,70}?)\s*(\(|–)")
VOTO = {"S": "Sim", "N": "Não", "A": "Abstenção", "P": "Presente"}


def meses():
    ano, mes = INICIO
    hoje = date.today()
    while (ano, mes) <= (hoje.year, hoje.month):
        yield ano, mes
        ano, mes = (ano + 1, 1) if mes == 12 else (ano, mes + 1)


def reunioes_do_mes(ano, mes):
    """Reuniões realizadas no mês. A agenda mensal tem dezenas de megabytes: guarda só o que usa."""
    fechado = (ano, mes) < (date.today().year, date.today().month)

    def buscar():
        j = senado_json(f"{SENADO}/comissao/agenda/mes/{ano}{mes:02d}")
        lista = c.como_lista(((j.get("AgendaReuniao") or {}).get("reunioes") or {}).get("reuniao"))
        return [{"codigo": r["codigo"], "data": (r.get("dataInicio") or "")[:10], "titulo": r.get("titulo", ""),
                 "sigla": (r.get("colegiadoCriador") or {}).get("sigla", ""),
                 "colegiado": (r.get("colegiadoCriador") or {}).get("nome", "")}
                for r in lista if r.get("situacao") == "Realizada"]
    return c.em_cache("senado_agenda", f"{ano}{mes:02d}" + ("" if fechado else f"_{date.today()}"), buscar) or []


def notas(codigo):
    """Texto corrido da nota taquigráfica da reunião. Reunião sem nota publicada responde 404:
    isso é resposta, não falha, e fica guardado como vazio para não ser pedido de novo."""
    def buscar():
        for tentativa in range(4):
            try:
                r = requests.get(f"{SENADO}/taquigrafia/notas/reuniao/{codigo}", headers={"Accept": "application/json"}, timeout=120)
            except requests.RequestException:
                time.sleep(3 * (tentativa + 1))
                continue
            if r.status_code == 404:
                return {"texto": "", "situacao": "sem nota publicada"}
            if r.status_code == 200:
                quartos = c.como_lista((r.json().get("notasTaquigraficas") or {}).get("quartos"))
                quartos = sorted(quartos, key=lambda q: int(q.get("sequencia") or 0))
                return {"texto": "\n".join(q.get("texto") or "" for q in quartos)}
            time.sleep(3 * (tentativa + 1))
        return None  # sem resposta: não guarda, a próxima rodada tenta de novo
    return (c.em_cache("senado_notas_reuniao", str(codigo), buscar) or {}).get("texto", "")


def falas(texto):
    """[(orador, papel, fala)] na ordem em que aparecem."""
    marcas = list(CABECALHO.finditer(texto))
    saida = []
    for i, m in enumerate(marcas):
        corpo = texto[m.end():marcas[i + 1].start() if i + 1 < len(marcas) else len(texto)]
        nome, papel = m.group(1).strip(), "Orador"
        if nome.upper().startswith("PRESIDENTE"):
            nome, papel = corpo.split(".", 1)[0], "Presidente da reunião"
        # com parêntese, a fala vem depois do travessão que o fecha (antes dele: partido, UF e tipo de
        # intervenção; o hífen de "PSD) - DF" não conta). Sem parêntese, o cabeçalho já terminou no travessão.
        fala = (re.split(r"\)\s*–\s*", corpo, maxsplit=1)[-1] if m.group(2) == "(" else corpo).strip()
        saida.append((nome, papel, fala))
    return saida


def main():
    senadores = pd.concat([c.ler_csv(f"perfil_senado_{g}/resumo.csv") for g in GRUPOS]).drop_duplicates("Parlamentar")
    chave = {c.chave_nome(n): n for n in senadores.Parlamentar}
    codigo = dict(zip(senadores.Parlamentar, senadores["Código no Senado"]))

    reunioes = [r for ano, mes in meses() for r in reunioes_do_mes(ano, mes)]
    reunioes = list({r["codigo"]: r for r in reunioes}.values())
    print(f"{len(reunioes)} reuniões realizadas desde {INICIO[1]:02d}/{INICIO[0]}")
    with ThreadPoolExecutor(4) as ex:
        textos = list(ex.map(lambda r: notas(r["codigo"]), reunioes))
    com_nota = sum(bool(t) for t in textos)
    print(f"  com nota taquigráfica: {com_nota}")

    linhas = []
    for r, texto in zip(reunioes, textos):
        r["Nota publicada"] = "Sim" if texto else "Não"
        for nome, papel, fala in falas(texto):
            quem = chave.get(c.chave_nome(nome))
            if quem and fala:
                linhas.append({"Parlamentar": quem, "Data": r["data"], "Colegiado": r["sigla"], "Reunião": r["titulo"],
                               "Papel": papel, "Caracteres": len(fala), "Texto": fala,
                               "Link": f"https://www25.senado.leg.br/web/atividade/notas-taquigraficas/-/notas/r/{r['codigo']}"})
    tab_falas = pd.DataFrame(linhas)
    resumo = (tab_falas.groupby(["Parlamentar", "Colegiado"])
              .agg(Reuniões=("Link", "nunique"), Falas=("Texto", "size"), Caracteres=("Caracteres", "sum"),
                   **{"Primeira": ("Data", "min"), "Última": ("Data", "max")})
              .reset_index().sort_values(["Parlamentar", "Caracteres"], ascending=[True, False]))

    votos = []
    for nome, cod in codigo.items():
        j = c.em_cache("senado_perfil", f"votacao_comissao_{cod}_{date.today()}",
                       lambda: senado_json(f"{SENADO}/votacaoComissao/parlamentar/{cod}")) or {}
        for v in c.como_lista(((j.get("VotacoesComissao") or {}).get("Votacoes") or {}).get("Votacao")):
            meu = next((x for x in c.como_lista((v.get("Votos") or {}).get("Voto")) if str(x.get("CodigoParlamentar")) == str(cod)), None)
            if meu:
                q = meu.get("QualidadeVoto", "")
                votos.append({"Parlamentar": nome, "Data": (v.get("DataHoraInicioReuniao") or "")[:10], "Colegiado": v.get("SiglaColegiado", ""),
                              "Matéria": v.get("IdentificacaoMateria", ""), "Votação": v.get("DescricaoVotacao", ""), "Voto": VOTO.get(q, q)})

    pasta = c.caminho("perfil_senado_comissoes")
    pasta.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(reunioes).to_csv(pasta / "reunioes.csv", index=False)
    tab_falas.to_csv(pasta / "falas.csv", index=False)
    resumo.to_csv(pasta / "falas_resumo.csv", index=False)
    pd.DataFrame(votos).to_csv(pasta / "votos.csv", index=False)
    print(f"  falas de senadores da próxima legislatura: {len(tab_falas)}, de {tab_falas.Parlamentar.nunique() if len(tab_falas) else 0} nomes")
    print(f"  votos em comissão: {len(votos)}")


if __name__ == "__main__":
    main()
