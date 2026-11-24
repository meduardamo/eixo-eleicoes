"""Trava dos scripts que regravam abas inteiras da 58ª (Parlamentares, Perfil por tema, Votações-chave, Clientes).
Em 24/11/2026 as planilhas receberam correções feitas direto nelas, que os dados de origem desses scripts não têm:
suplentes no lugar dos senadores eleitos governadores, 245 linhas de cliente que faltavam, rótulos iguais nas duas Casas,
padrão de texto, ementas completas, governismo da Câmara no Senado e as classificações ajustadas pela equipe.
Republicar desfaz tudo isso. Para republicar mesmo assim: rodar com --reaplicar-2411-depois e, em seguida,
dados_novos_eleitos/buscas_locais_2026-11-04/reaplicar_2411.sh (e reaplicar as revisões da equipe, que ficam nos
scripts revisao_*/aplicar_*.py)."""
import sys


def checa(script: str) -> None:
    if '--reaplicar-2411-depois' in sys.argv:
        sys.argv.remove('--reaplicar-2411-depois')
        print(f'{script}: republicando; rode depois dados_novos_eleitos/buscas_locais_2026-11-04/reaplicar_2411.sh')
        return
    sys.exit(f'{script} desfaz as correções de 24/11/2026 na planilha (ver trava_2411.py). '
             'Para seguir: --reaplicar-2411-depois e, em seguida, reaplicar_2411.sh.')
