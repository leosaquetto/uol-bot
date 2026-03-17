# ------------------------------
# Clube UOL Bot - VERSÃO DE TESTE (com mais informações)
# ------------------------------

import requests
import json
import os
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# CONFIGURAÇÕES
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "history_test.json"  # Histórico separado para teste

def load_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
    return {"lastIds": []}

def save_history(history):
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)

def extract_partner_from_title(title):
    """Extrai o parceiro do título (ex: 'por Urban Pet' ou '- Urban Pet')"""
    patterns = [
        r'[–—-]\s*([^–—-]+)$',  # traço no final
        r'por\s+([^–—-]+)$',      # 'por' no final
        r'via\s+([^–—-]+)$'       # 'via' no final
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

def clean_title(title):
    """Remove o 'por Parceiro' do título principal"""
    # Remove padrões do final do título
    title = re.sub(r'\s*[–—-]\s*[^–—-]+$', '', title)
    title = re.sub(r'\s*por\s+[^–—-]+$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*via\s+[^–—-]+$', '', title, flags=re.IGNORECASE)
    return title.strip()

def fetch_offer_details(link):
    """Tenta buscar detalhes adicionais da página da oferta"""
    # Isso é mais complexo e pode falhar
    # Por enquanto, vamos retornar None
    return None

def send_to_telegram_enhanced(offer):
    """Versão melhorada com mais informações"""
    try:
        # Extrai parceiro do título
        partner = extract_partner_from_title(offer['title'])
        clean_title_text = clean_title(offer['title'])
        
        # Monta mensagem melhorada
        message = f"*{clean_title_text}*\n\n"
        
        if partner:
            message += f"🏷️ *Parceiro:* {partner}\n"
        
        # Se tivesse data, adicionaria aqui
        # message += f"📅 *Publicado:* {date}\n"
        
        message += f"\n🔗 [Acessar oferta]({offer['link']})\n"
        
        # Linha extra para separar da preview
        message += f"\n"
        
        print(f"\n📝 Mensagem formatada:")
        print(message)
        
        # Envia pro Telegram (opcional - descomentar para testar)
        # url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        # payload = {
        #     "chat_id": TELEGRAM_CHAT_ID,
        #     "text": message,
        #     "parse_mode": "Markdown"
        # }
        # response = requests.post(url, json=payload, timeout=30)
        # return response.ok
        
        return True
        
    except Exception as e:
        print(f"Erro ao formatar: {e}")
        return False

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def fetch_offers():
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        time.sleep(3)
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(2)
        
        containers = driver.find_elements(By.CSS_SELECTOR, 
            "div.beneficio, article, .card-oferta, [class*='offer'], [class*='card']")
        
        print(f"📦 Containers encontrados: {len(containers)}")
        
        offers = []
        for container in containers[:5]:  # Testa só 5 ofertas
            try:
                # Título
                title_elem = container.find_element(By.CSS_SELECTOR, 
                    ".titulo, h2, h3, p, .name, [class*='title']")
                title = title_elem.text.strip()
                
                # Link
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                # Tenta pegar imagem
                img_elem = container.find_element(By.CSS_SELECTOR, "img")
                img_url = img_elem.get_attribute("src") or img_elem.get_attribute("data-src")
                
                print(f"\n🔍 Analisando: {title[:50]}...")
                print(f"  Link: {link}")
                if img_url:
                    print(f"  Imagem: {img_url[:50]}...")
                
                # Extrai parceiro
                partner = extract_partner_from_title(title)
                if partner:
                    print(f"  Parceiro detectado: {partner}")
                
                offers.append({
                    "title": title,
                    "link": link,
                    "img_url": img_url,
                    "partner": partner,
                    "clean_title": clean_title(title)
                })
                    
            except Exception as e:
                print(f"  ⚠️ Erro: {e}")
                continue
        
        return offers
        
    except Exception as e:
        print(f"❌ Erro no Selenium: {e}")
        return []
        
    finally:
        if driver:
            driver.quit()

def run_test():
    print("=" * 60)
    print(f"🧪 TESTE - Bot UOL Enhanced - {datetime.now()}")
    print("=" * 60)
    
    # Busca ofertas
    print("\n🔍 Buscando ofertas...")
    offers = fetch_offers()
    
    if not offers:
        print("❌ Nenhuma oferta encontrada")
        return
    
    print(f"\n📊 Total: {len(offers)} ofertas para teste")
    
    # Testa formatação melhorada
    print("\n" + "=" * 60)
    print("📝 TESTE DE FORMATAÇÃO MELHORADA")
    print("=" * 60)
    
    for i, offer in enumerate(offers, 1):
        print(f"\n--- OFERTA {i} ---")
        print(f"Título original: {offer['title']}")
        print(f"Título limpo: {offer['clean_title']}")
        print(f"Parceiro: {offer['partner'] or 'Não detectado'}")
        print(f"Link: {offer['link']}")
        if offer['img_url']:
            print(f"Imagem: {offer['img_url']}")
        
        # Testa o envio (só print, não envia de verdade)
        send_to_telegram_enhanced(offer)
    
    print("\n" + "=" * 60)
    print("✅ Teste concluído!")
    print("=" * 60)

if __name__ == "__main__":
    run_test()
