import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import gspread
import requests
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from google import genai
from google.genai import types

BRT = timezone(timedelta(hours=-3))

# Cabeçalhos para fingir ser um usuário real do Windows/Chrome + IP do Googlebot (Fallback)
STEALTH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Forwarded-For": "66.249.66.1" # IP do Googlebot para furar alguns paywalls híbridos
}

MAPA_UF = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPÁ": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARÁ": "CE", "DISTRITO FEDERAL": "DF", "ESPÍRITO SANTO": "ES", "GOIÁS": "GO",
    "MARANHÃO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARÁ": "PA", "PARAÍBA": "PB", "PARANÁ": "PR", "PERNAMBUCO": "PE", "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO", "RORAIMA": "RR", "SANTA CATARINA": "SC", "SÃO PAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO", "BRASIL": "BR"
}

DRIVE_ID = '0AH-94UFLKIFPUk9PVA'
PASTA_PRESIDENCIAVEIS = '1apgniY4undEtkqjDYOEdf1aoMU7HDfOh'
PASTA_GOV_SEN = '1MmeVz63PG9imU_oDqk7thw5gHha0xAWa'

def obter_credenciais():
    """Lê as credenciais exclusivamente do ambiente (GitHub Secrets)"""
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não encontrado nas variáveis de ambiente.")
    
    info = json.loads(creds_json)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    creds.refresh(Request())
    return creds

def obter_cliente_gemini():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não encontrada nas variáveis de ambiente.")
    return genai.Client(api_key=api_key)

def agente_buscar_link_faltante(gemini_client, registro, instituto, cargo, uf, data):
    prompt = f"""
    Você é um pesquisador sênior de dados eleitorais da Eixo. Localize a publicação do relatório completo de resultados da seguinte pesquisa eleitoral de 2026:
    Registro TSE: {registro} | Instituto: {instituto} | Cargo: {cargo} - UF: {uf} | Data prevista: ~{data}
    
    ESTRATÉGIA DE BUSCA E REGRAS CRÍTICAS:
    1. FONTES: Priorize o site oficial do instituto (ex: paranapesquisas.com.br, realtimebigdata.com.br, eleicoes26.institutoverita.com.br). Se não houver, busque cobertura jornalística aberta (Poder360, G1, CNN, Gazeta do Povo, Metrópoles, blogs locais).
    2. PROIBIDO TSE: NÃO retorne links do sistema PesqEle do TSE ou PDFs que sejam apenas o "Recibo de Registro/Questionário". O alvo é o relatório com os RESULTADOS (gráficos, intenção de voto).
    3. FALSO PAYWALL: Se a primeira fonte encontrada for paga (Estadão, Folha, O Globo), ESCARAFUNCHE a internet buscando fontes abertas secundárias sobre a mesma pesquisa. Retorne o paywall apenas como último recurso.
    4. ALUCINAÇÃO DE DATA: Só aceite matérias ou relatórios que citem dados de 2026. Ignore notícias velhas de 2022 ou 2024.
    
    Retorne APENAS um JSON válido (sem tags markdown de bloco de código):
    {{
      "link": "URL_COMPLETA_AQUI ou deixe vazio se não achar",
      "tipo": "pdf", "materia", "paywall" ou "nao_encontrado",
      "origem_texto": "Descrição breve (ex: 'Relatório no site do instituto' ou 'Matéria G1')"
    }}
    """
    config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1, tools=[types.Tool(google_search=types.GoogleSearch())])
    res = gemini_client.models.generate_content(model="gemini-2.5-flash", contents=prompt, config=config)
    try:
        return json.loads((getattr(res, "text", "") or "").strip().replace("```json", "").replace("```", ""))
    except:
        return {"tipo": "nao_encontrado"}

def agente_buscar_pesquisas_dia(gemini_client):
    hoje = datetime.now(BRT).strftime("%d/%m/%Y")
    prompt = f"""
    Você é um agente de inteligência da Eixo monitorando o ciclo eleitoral de 2026.
    Sua missão é varrer a web atrás de pesquisas eleitorais de intenção de voto divulgadas EXATAMENTE HOJE ({hoje}) ou ontem.
    
    COBERTURA AMPLA E IRRESTRITA:
    1. TODOS OS ESTADOS: Você deve monitorar o cenário de forma igualitária para TODOS os 27 estados do Brasil (UFs) e para a Presidência (Nacional). Trate todos os estados com a mesma importância e dedicação.
    2. TODOS OS INSTITUTOS: Embora grandes institutos (Datafolha, Quaest, AtlasIntel, Paraná Pesquisas, Veritá, Real Time Big Data, Futura, Vox Brasil, etc.) exijam buscas nominais dedicadas para evitar que passem despercebidos, NÃO limite a sua coleta a eles. Capture igualmente pesquisas de institutos locais, regionais, estaduais ou de menor porte (como Doxa, Ranking Brasil, Gerp, IRG Pesquisas, IPESPE, etc.) que tenham sido publicadas hoje ou ontem.
    
    REGRAS DE OURO:
    1. IGNORE TEASERS: Descarte matérias que dizem "vai ser divulgada", "foi registrada" ou "está em campo". Colete apenas resultados JÁ publicados com números de intenção de voto ou rejeição.
    2. ALUCINAÇÃO DE ANO: Filtre rigorosamente pesquisas passadas (2022/2024). Queremos apenas 2026.
    3. REGISTRO DUPLO: Se uma pesquisa estadual testou Governador e Presidente, ela geralmente tem um registro Estadual (UF-00000/2026) e um Nacional (BR-00000/2026). Se a matéria citar ambos, crie DOIS itens separados na lista JSON abaixo.
    
    Retorne APENAS um JSON válido (sem tags markdown):
    {{
      "pesquisas": [
        {{
          "registro": "BR-12345/2026", 
          "cargo": "Presidente", 
          "uf": "BRASIL", 
          "instituto": "Nome do Instituto",
          "data_divulgacao": "DD/MM/YYYY", 
          "link": "URL_DA_FONTE", 
          "tipo": "pdf", "materia" ou "paywall",
          "origem_texto": "Reportagem do site X"
        }}
      ]
    }}
    """
    config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.15, tools=[types.Tool(google_search=types.GoogleSearch())])
    res = gemini_client.models.generate_content(model="gemini-2.5-flash", contents=prompt, config=config)
    try:
        return json.loads((getattr(res, "text", "") or "").strip().replace("```json", "").replace("```", "")).get("pesquisas", [])
    except:
        return []

def baixar_pdf_ou_gerar_headless(url):
    """
    Tenta achar PDFs escondidos na página. Se falhar ou der erro 403, 
    usa o Headless Chrome para imprimir a página de notícias em formato PDF.
    """
    try:
        req = requests.get(url, headers=STEALTH_HEADERS, timeout=30)
        req.raise_for_status()
        
        # Se for um PDF direto
        if req.headers.get('Content-Type', '').startswith('application/pdf') or url.lower().endswith('.pdf'):
            return req.content

        # Verifica se tem link de PDF no corpo da matéria HTML (Com Filtro Anti-TSE)
        links_na_pagina = re.findall(r'href=[\'"]?([^\'" >]+)', req.text)
        links_pdf = [urljoin(url, link) for link in links_na_pagina if link.lower().endswith('.pdf')]
        
        for link_pdf in links_pdf:
            if "registro" in link_pdf.lower() or "tse" in link_pdf.lower():
                continue # Pula PDFs de recibo de registro (Ex: Paraná Pesquisas)
            
            # Baixa o primeiro PDF válido encontrado
            pdf_req = requests.get(link_pdf, headers=STEALTH_HEADERS, timeout=30)
            if pdf_req.status_code == 200:
                return pdf_req.content
                
    except requests.exceptions.RequestException:
        pass # Erro de acesso (ex: 403 do Ranking Pesquisa). Cai para o Headless Chrome.

    # Fallback: Headless Chrome
    # Adapta o comando dependendo se está rodando no Mac ou no GitHub Actions (Linux)
    chrome_cmd = "google-chrome-stable" if os.name == 'posix' and not os.path.exists("/Applications") else "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        pdf_temp_path = tmp.name

    subprocess.run([
        chrome_cmd, "--headless=new", "--disable-gpu", 
        "--no-pdf-header-footer", f"--print-to-pdf={pdf_temp_path}", url
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    with open(pdf_temp_path, "rb") as f:
        pdf_bytes = f.read()
    
    os.remove(pdf_temp_path)
    return pdf_bytes

def resolver_pasta_drive(creds, cargo, uf_extenso):
    """Retorna o ID da subpasta correta, criando-a se necessário"""
    headers = {'Authorization': f'Bearer {creds.token}'}
    
    pasta_mae = PASTA_PRESIDENCIAVEIS if "presidente" in cargo.lower() else PASTA_GOV_SEN
    
    uf_extenso_limpo = uf_extenso.strip().upper()
    if "presidente" in cargo.lower() and uf_extenso_limpo == "BRASIL":
        nome_subpasta = "Nacional"
    else:
        nome_subpasta = MAPA_UF.get(uf_extenso_limpo, uf_extenso_limpo)

    query = f"name='{nome_subpasta}' and '{pasta_mae}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    
    # Procura a pasta
    r = requests.get(
        "https://www.googleapis.com/drive/v3/files",
        params={"q": query, "corpora": "drive", "driveId": DRIVE_ID, "includeItemsFromAllDrives": "true", "supportsAllDrives": "true"},
        headers=headers
    ).json()

    if r.get("files"):
        return r["files"][0]["id"]
    
    # Se não existir, cria
    meta = {
        'name': nome_subpasta,
        'parents': [pasta_mae],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    r_create = requests.post(
        "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true",
        headers=headers,
        json=meta
    ).json()
    return r_create["id"]

def fazer_upload_drive(creds, pasta_id, nome_arquivo, pdf_bytes):
    headers = {'Authorization': f'Bearer {creds.token}'}
    
    # Verifica duplicidade
    query = f"name='{nome_arquivo}' and '{pasta_id}' in parents and trashed=false"
    r = requests.get(
        "https://www.googleapis.com/drive/v3/files",
        params={"q": query, "corpora": "drive", "driveId": DRIVE_ID, "includeItemsFromAllDrives": "true", "supportsAllDrives": "true", "fields": "files(id, webViewLink)"},
        headers=headers
    ).json()

    if r.get("files"):
        return r["files"][0]["webViewLink"] # Já existe

    # Upload Novo
    meta = {'name': nome_arquivo, 'parents': [pasta_id]}
    files = {
        'metadata': ('m', json.dumps(meta), 'application/json'),
        'file': (nome_arquivo, pdf_bytes, 'application/pdf')
    }
    r_upload = requests.post(
        'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true&fields=id,name,webViewLink',
        headers=headers, 
        files=files
    )
    return r_upload.json().get('webViewLink', '')

def normalizar_nome_arquivo(registro, data_div):
    reg_limpo = registro.replace("/", "-")
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", data_div)
    data_iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else data_div
    return f"{reg_limpo}_{data_iso}.pdf"

def atualizar_planilha():
    print("Iniciando automação de busca de relatórios...")
    creds = obter_credenciais()
    gemini_client = obter_cliente_gemini()
    
    sheet_id = os.getenv("SPREADSHEET_ID_RELATORIOS")
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID_RELATORIOS ausente nos Secrets.")
        
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("relatorios")
    
    dados = ws.get_all_records()
    registros_existentes = {str(r.get("registro", "")).strip() for r in dados}
    
    pdfs_salvos = 0
    links_preenchidos = 0
    linhas_novas = []
    pendentes_finais = []

    celulas_para_atualizar = [] # Para o Batch Update

    # 1. PROCESSAR LINHAS PENDENTES (SEM LINK)
    print(f"\n--- Fase 1: Processando Pendentes ({len(dados)} linhas analisadas) ---")
    for i, linha in enumerate(dados, start=2): # +2 porque o sheets começa no 1 e tem o cabeçalho
        registro = str(linha.get("registro", "")).strip()
        link_atual = str(linha.get("link", "")).strip()
        
        if not registro or link_atual:
            continue
            
        print(f"Buscando: {registro}...")
        resultado = agente_buscar_link_faltante(
            gemini_client, registro, linha.get("instituto", ""), 
            linha.get("cargo", ""), linha.get("uf", ""), linha.get("data_divulgacao", "")
        )
        
        if resultado.get("tipo") == "paywall" and not resultado.get("link"):
            pendentes_finais.append(f"{registro} - Paywall, sem matéria aberta localizada.")
            celulas_para_atualizar.append(gspread.Cell(i, 16, "paywall, aguardando fonte aberta")) # Coluna P
            continue
            
        link_fonte = resultado.get("link")
        if not link_fonte:
            pendentes_finais.append(f"{registro} - Nenhum relatório ou matéria encontrada na web hoje.")
            continue
            
        try:
            pdf_bytes = baixar_pdf_ou_gerar_headless(link_fonte)
            pasta_id = resolver_pasta_drive(creds, linha.get("cargo", ""), linha.get("uf", ""))
            nome_pdf = normalizar_nome_arquivo(registro, str(linha.get("data_divulgacao", "")))
            
            link_drive = fazer_upload_drive(creds, pasta_id, nome_pdf, pdf_bytes)
            
            # Salva na lista de Batch Update (Col F=6, Col P=16)
            celulas_para_atualizar.append(gspread.Cell(i, 6, link_drive))
            celulas_para_atualizar.append(gspread.Cell(i, 16, resultado.get("origem_texto", "Capturado na Web")))
            
            pdfs_salvos += 1
            links_preenchidos += 1
            print(f"  [OK] Salvo no Drive: {nome_pdf}")
        except Exception as e:
            pendentes_finais.append(f"{registro} - Erro técnico ao baixar/salvar PDF: {str(e)}")

    print("\n--- Fase 2: Varredura de Mídia por Pesquisas Novas ---")
    pesquisas_novas = agente_buscar_pesquisas_dia(gemini_client)
    print(f"O Gemini identificou {len(pesquisas_novas)} citação(ões) de pesquisa hoje.")
    
    for p in pesquisas_novas:
        registro = p.get("registro", "").strip()
        if not registro or registro in registros_existentes:
            continue
            
        print(f"Nova descoberta! {registro} - {p.get('instituto')} - {p.get('uf')}")
        
        link_drive_final = ""
        origem = p.get("origem_texto", "")
        
        if p.get("tipo") == "paywall":
            origem = f"Paywall detectado. Fonte: {p.get('link')}"
            pendentes_finais.append(f"{registro} (NOVO) - {origem}")
        elif p.get("link"):
            try:
                pdf_bytes = baixar_pdf_ou_gerar_headless(p.get("link"))
                pasta_id = resolver_pasta_drive(creds, p.get("cargo", ""), p.get("uf", ""))
                nome_pdf = normalizar_nome_arquivo(registro, str(p.get("data_divulgacao", "")))
                link_drive_final = fazer_upload_drive(creds, pasta_id, nome_pdf, pdf_bytes)
                pdfs_salvos += 1
            except Exception as e:
                origem = f"Erro no PDF: {str(e)}. Fonte: {p.get('link')}"
                pendentes_finais.append(f"{registro} (NOVO) - {origem}")
        
        nova_linha = [
            registro, p.get("cargo", ""), p.get("uf", ""), p.get("instituto", ""), 
            p.get("data_divulgacao", ""), link_drive_final, "", "", "", "", "", "", "", "", "", origem
        ]
        linhas_novas.append(nova_linha)
        registros_existentes.add(registro)

    if celulas_para_atualizar:
        ws.update_cells(celulas_para_atualizar, value_input_option="USER_ENTERED")
        
    if linhas_novas:
        ws.append_rows(linhas_novas, value_input_option="USER_ENTERED")

    hoje_fmt = datetime.now(BRT).strftime("%d/%m/%Y")
    print(f"\n--- Relatório de Automação – {hoje_fmt} ---")
    print(f"* PDFs salvos no Drive: {pdfs_salvos}")
    print(f"* Links preenchidos em linhas existentes: {links_preenchidos}")
    print(f"* Linhas novas adicionadas: {len(linhas_novas)}")
    if pendentes_finais:
        print("* Registros que seguem pendentes:")
        for pend em pendentes_finais:
            print(f"  * {pend}")
    else:
        print("* Registros que seguem pendentes: 0")

if __name__ == "__main__":
    atualizar_planilha()
