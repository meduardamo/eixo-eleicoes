"""IDs de planilha e documento vêm de variável de ambiente (o repositório é público). No GitHub, Secrets do repositório;
na máquina, a variável exportada ou o arquivo .env na raiz do repositório (fora do git), uma linha NOME=valor."""
import os
from pathlib import Path

_ENV = Path(__file__).resolve().parents[2] / '.env'


def valor(nome):
    v = os.getenv(nome, '').strip()
    if not v and _ENV.exists():
        for linha in _ENV.read_text().splitlines():
            k, _, x = linha.partition('=')
            if k.strip() == nome: v = x.strip().strip('"\''); break
    if not v:
        raise RuntimeError(f'Variável {nome} não configurada (Secrets do GitHub ou .env na raiz do repositório).')
    return v
