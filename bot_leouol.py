# ------------------------------
# BOT LEOUOL - Clube UOL Ofertas
# Envia novas ofertas do Clube UOL para o Telegram
# ------------------------------

import requests
import json
import os
import time
import re
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"
MAX_CAPTION_LENGTH = 1024  # Limite do Telegram para fotos
MAX_OFFERS_PER_RUN = 8      # Pega apenas as 8 ofertas mais recentes
MAX_HISTORY_SIZE = 200       # Mantém apenas os últimos 200 IDs

# Lista de User Agents para parecer um navegador real
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ==============================================
# FUNÇÕES DE HISTÓRICO (CORRIGIDAS COM LIMITE)
# ==============================================
def load_history():
    """Carrega histórico de IDs já enviados"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                # Garante que o histórico não ultrapasse o limite
                if len(data.get("ids", [])) > MAX_HISTORY_SIZE:
                    data["ids"] = data["ids"][-MAX_HISTORY_SIZE:]
                return data
    except Exception as e:
        print(f"⚠️ Erro ao carregar histórico: {e}")
    
    # Retorna histórico vazio se não existir ou der erro
    return {"ids": []}

def save_history(history):
    """Salva histórico com limite de tamanho"""
    try:
        # Aplica limite antes de salvar
        if len(history.get("ids", [])) > MAX_HISTORY_SIZE:
            history["ids"] = history["ids"][-MAX_HISTORY_SIZE:]
        
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2)
        
        print(f"✅ Histórico salvo: {len(history['ids'])} IDs (limite {MAX_HISTORY_SIZE})")
        return True
    except Exception as e:
        print(f"⚠️ Erro ao salvar histórico: {e}")
        return False

# ==============================================
# FUNÇÕES DE COMPORTAMENTO HUMANO
# ==============================================
def human_like_delay(min_seconds=1, max_seconds=3):
    """Pausa com comportamento humano (aleatório)"""
    time.sleep(random.uniform(min_seconds, max_seconds))

# ==============================================
# CONFIGURAÇÃO DO CHROME (COM CORREÇÃO SSL)
# ==============================================
def setup_driver():
    """Configura o Chrome com todas as correções anti-detecção e SSL"""
    chrome_options = Options()
    
    # Modo headless (sem interface gráfica)
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    # Tamanho de tela realista
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Remove vestígios de automação
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # CORREÇÃO: Ignorar erros de certificado SSL
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument('--ignore-ssl-errors=yes')
    
    # User Agent aleatório
    user_agent = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    # Idioma português do Brasil
    chrome_options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en;q=0.8")
    
    # Inicia o Chrome
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Script para esconder que é um robô
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

# ==============================================
# EXTRAÇÃO DE DADOS DA PÁGINA
# ==============================================
def extract_page_title(driver):
    """Extrai o título da página da oferta"""
    try:
        # Tenta pegar o H1 da página
        h1_elements = driver.find_elements(By.CSS_SELECTOR, "h1")
        if h1_elements and h1_elements[0].text.strip():
            title = h1_elements[0].text.strip()
            # Remove "Clube UOL" se estiver no título
            title = re.sub(r'\s*[–—-]\s*Clube UOL\s*$', '', title)
            return title
        
        # Se não achar H1, pega o título da página
        title = driver.title
        title = re.sub(r'\s*[–—-]\s*Clube UOL\s*$', '', title)
        return title.strip()
    except:
        return None

def extract_validity(driver):
    """Extrai apenas a validade do benefício"""
    try:
        # Pega todo o texto da página
        page_text = driver.find_element(By.TAG_NAME, "body").text
        
        # Padrões comuns de validade
        patterns = [
            r'[Vv]álido até[^.!?]*[.!?]',
            r'[Vv]alidade[^.!?]*[.!?]',
            r'[Bb]enefício válido[^.!?]*[.!?]',
            r'[Pp]romoção válida[^.!?]*[.!?]',
            r'[Cc]upom válido[^.!?]*[.!?]',
            r'[Dd]esconto válido[^.!?]*[.!?]',
            r'[Vv]álido de[^.!?]*[.!?]',
            r'[Vv]álido para compras[^.!?]*[.!?]',
            r'[Vv]álido até \d{1,2}/\d{1,2}/\d{4}',
            r'[Vv]álido de \d{1,2}/\d{1,2}/\d{4}'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, page_text)
            if match:
                return match.group(0).strip()
        
        # Se não achou com padrões, procura em parágrafos
        keywords = ['válido', 'validade', 'até', 'válida']
        paragraphs = driver.find_elements(By.TAG_NAME, "p")
        
        for p in paragraphs:
            text = p.text.strip()
            if any(keyword in text.lower() for keyword in keywords) and len(text) < 200:
                return text
                
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair validade: {e}")
    
    return None

# ==============================================
# DOWNLOAD DA IMAGEM
# ==============================================
def download_image(img_url):
    """Baixa a imagem da oferta"""
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://clube.uol.com.br/'
        }
        response = requests.get(img_url, headers=headers, timeout=15)
        if response.status_code == 200:
            temp_path = f"/tmp/leouol_{random.randint(1000,9999)}.jpg"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            return temp_path
    except Exception as e:
        print(f"Erro ao baixar imagem: {e}")
    return None

# ==============================================
# CONSTRUÇÃO DA LEGENDA
# ==============================================
def build_caption(page_title, validity, link):
    """Constrói a legenda da mensagem"""
    parts = []
    
    if page_title:
        parts.append(f"*{page_title}*")
    else:
        return None
    
    if validity and len(validity) > 5:
        validity_clean = re.sub(r'^(benef[ií]cio\s+v[aá]lido\s*:\s*)', '', validity, flags=re.IGNORECASE)
        parts.append(f"📅 {validity_clean}")
    
    parts.append(f"🔗 [Acessar oferta]({link})")
    
    caption = "\n\n".join(parts)
    
    if len(caption) > MAX_CAPTION_LENGTH:
        link_pos = caption.rfind("🔗 [Acessar oferta]")
        if link_pos > 0:
            truncated = caption[:MAX_CAPTION_LENGTH - len(caption[link_pos:]) - 3] + "..."
            caption = truncated + "\n\n" + caption[link_pos:]
        else:
            caption = caption[:MAX_CAPTION_LENGTH-3] + "..."
    
    print(f"📝 Legenda: {len(caption)} caracteres")
    return caption
# ==============================================
# ENVIO PARA O TELEGRAM (COMENTADO PARA TESTES)
# ==============================================
def send_to_telegram(img_path, caption):
    """Envia a imagem com legenda para o Telegram (DESATIVADO PARA TESTES)"""
    print("🔴 MODO TESTE - Envio desativado")
    print(f"📤 Simulação de envio:")
    print(f"   Chat: {TELEGRAM_CHAT_ID}")
    print(f"   Legenda: {caption[:100]}...")
    return True  # Simula sucesso
    
    # Código original comentado:
    # try:
    #     url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    #     with open(img_path, 'rb') as photo:
    #         files = {'photo': photo}
    #         data = {
    #             'chat_id': TELEGRAM_CHAT_ID,
    #             'caption': caption,
    #             'parse_mode': 'Markdown'
    #         }
    #         response = requests.post(url, data=data, files=files, timeout=30)
    #     return response.ok
    # except Exception as e:
    #     print(f"❌ Erro no envio: {e}")
    #     return False

# ==============================================
# BUSCA OFERTAS NA PÁGINA PRINCIPAL (VERSÃO CORRIGIDA)
# ==============================================
def fetch_offers():
    """Pega as 8 ofertas mais recentes da página principal"""
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()

        print(f"📱 Acessando: {TARGET_URL}")
        driver.get(TARGET_URL)

        human_like_delay(3, 5)

        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)

        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Total de ofertas na página: {len(containers)}")

        if not containers:
            print("❌ Nenhuma oferta encontrada")
            return []

        offers = []
        for i, container in enumerate(containers[:MAX_OFFERS_PER_RUN]):
            try:
                # Título da oferta (prévia)
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                preview_title = title_elem.text.strip()

                if not preview_title:
                    continue

                # Link da oferta
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")

                # --- NOVO: Criar um ID ESTÁVEL baseado no título e no parceiro ---
                # Tenta extrair o nome do parceiro do próprio preview_title
                # Ex: "Título por Parceiro" ou "Título - Parceiro"
                partner_name = "desconhecido"
                partner_match = re.search(r'[Pp]or\s+([A-Za-z0-9\s]+)$|[-–]\s*([A-Za-z0-9\s]+)$', preview_title)
                if partner_match:
                    partner_name = partner_match.group(1) or partner_match.group(2)
                    partner_name = re.sub(r'\s+', '', partner_name).lower()[:15]

                # Limpa o título para criar uma base segura
                title_clean = re.sub(r'[^a-zA-Z0-9]', '', preview_title).lower()[:30]

                # Combina título limpo + parceiro para criar um ID único e estável
                offer_id = f"{title_clean}_{partner_name}"
                # ------------------------------------------

                # IMAGEM GRANDE (mantém igual)
                img_url = None
                elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                for el in elements_with_bg:
                    style = el.get_attribute("style")
                    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if match:
                        img_url = match.group(1)
                        print(f"  📸 Oferta {i+1}: Imagem (background)")
                        break

                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                    if imgs:
                        img_url = imgs[0].get_attribute("data-src")
                        print(f"  📸 Oferta {i+1}: Imagem (data-src)")

                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img")
                    if imgs:
                        img_url = imgs[0].get_attribute("src")
                        print(f"  📸 Oferta {i+1}: Imagem (fallback)")

                if img_url and img_url.startswith('//'):
                    img_url = 'https:' + img_url

                offers.append({
                    "id": offer_id,
                    "preview_title": preview_title,
                    "link": link,
                    "imagem_url": img_url
                })

                print(f"     Título: {preview_title[:50]}...")
                print(f"     ID: {offer_id}")

            except Exception as e:
                print(f"  ⚠️ Erro na oferta {i+1}: {e}")
                continue

        return offers

    except Exception as e:
        print(f"❌ Erro geral: {e}")
        return []

    finally:
        if driver:
            driver.quit()

# ==============================================
# PROCESSAMENTO DE CADA OFERTA
# ==============================================
def process_offer(offer):
    """Acessa a página da oferta e extrai título e validade"""
    print(f"\n🔍 Acessando página: {offer['preview_title'][:50]}...")
    
    driver = None
    try:
        driver = setup_driver()
        driver.get(offer['link'])
        
        human_like_delay(2, 4)
        
        page_title = driver.title
        
        if "Your connection is not private" in page_title:
            print(f"  ⚠️ Página com erro SSL (ignorado)")
            try:
                h1_elements = driver.find_elements(By.CSS_SELECTOR, "h1")
                if h1_elements and h1_elements[0].text.strip():
                    page_title = h1_elements[0].text.strip()
                else:
                    page_title = offer['preview_title']
            except:
                page_title = offer['preview_title']
        else:
            page_title = extract_page_title(driver)
            if not page_title:
                page_title = offer['preview_title']
        
        print(f"  📌 Título: {page_title[:50]}...")
        
        validity = extract_validity(driver)
        if validity:
            print(f"  📅 Validade: {validity[:50]}...")
        
        return page_title, validity
        
    except Exception as e:
        print(f"  ⚠️ Erro: {e}")
        return offer['preview_title'], None
        
    finally:
        if driver:
            driver.quit()

# ==============================================
# FUNÇÃO PRINCIPAL
# ==============================================
def main():
    """Função principal do bot"""
    print("=" * 70)
    print(f"🤖 BOT LEOUOL - Clube UOL Ofertas")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70)
    
    # Carrega histórico
    history = load_history()
    seen_ids = set(history.get("ids", []))
    print(f"📋 IDs no histórico: {len(seen_ids)} (limite {MAX_HISTORY_SIZE})")
    
    # Busca ofertas
    print("\n🔍 Buscando ofertas...")
    offers = fetch_offers()
    
    if not offers:
        print("❌ Nenhuma oferta encontrada")
        return
    
    print(f"\n📊 Encontradas: {len(offers)} ofertas")
    
    new_offers = [o for o in offers if o['id'] not in seen_ids]
    
    if not new_offers:
        print("\n📭 Nenhuma oferta nova")
        return
    
    print(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
    
    successful = 0
    processed_ids = set(seen_ids)
    
    for i, offer in enumerate(new_offers, 1):
        print(f"\n{'='*50}")
        print(f"📦 Oferta {i}/{len(new_offers)}")
        print(f"{'='*50}")
        print(f"Título: {offer['preview_title']}")
        print(f"ID: {offer['id']}")
        
        if not offer.get('imagem_url'):
            print("❌ Sem imagem")
            processed_ids.add(offer['id'])
            continue
        
        print("\n📥 Baixando imagem...")
        img_path = download_image(offer['imagem_url'])
        
        if not img_path:
            print("❌ Falha no download")
            processed_ids.add(offer['id'])
            continue
        
        page_title, validity = process_offer(offer)
        
        caption = build_caption(page_title, validity, offer['link'])
        
        if not caption:
            print("❌ Falha na legenda")
            if os.path.exists(img_path):
                os.remove(img_path)
            processed_ids.add(offer['id'])
            continue
        
        print("\n📤 Enviando...")
        if send_to_telegram(img_path, caption):
            successful += 1
            print(f"✅ Oferta enviada!")
        else:
            print(f"❌ Falha no envio")
        
        processed_ids.add(offer['id'])
        
        if os.path.exists(img_path):
            os.remove(img_path)
        
        if i < len(new_offers):
            pausa = random.randint(3, 6)
            print(f"\n⏱️ Aguardando {pausa}s...")
            human_like_delay(pausa, pausa+1)
    
    # Atualiza histórico
    if processed_ids:
        history["ids"] = list(processed_ids)
        if save_history(history):
            print(f"\n✅ Histórico atualizado: {len(processed_ids)} IDs")
    
    print("\n" + "=" * 70)
    print(f"✅ FINALIZADO! {successful}/{len(new_offers)} enviadas")
    print("=" * 70)

if __name__ == "__main__":
    main()
