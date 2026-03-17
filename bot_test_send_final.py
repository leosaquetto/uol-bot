# ------------------------------
# Clube UOL Bot - TESTE FINAL (cópia do que funcionou)
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
from webdriver_manager.chrome import ChromeDriverManager

# CONFIGURAÇÕES
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_URL = "https://clube.uol.com.br/?order=new"

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def extract_partner(title):
    patterns = [
        r'[–—-]\s*([^–—-]+)$',
        r'por\s+([^–—-]+)$',
        r'via\s+([^–—-]+)$'
    ]
    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

def clean_title(title):
    title = re.sub(r'\s*[–—-]\s*[^–—-]+$', '', title)
    title = re.sub(r'\s*por\s+[^–—-]+$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*via\s+[^–—-]+$', '', title, flags=re.IGNORECASE)
    return title.strip()

def send_to_telegram(offer):
    try:
        partner = extract_partner(offer['title'])
        clean = clean_title(offer['title'])
        
        message = f"*{clean}*\n\n"
        if partner:
            message += f"🏷️ *Parceiro:* {partner}\n"
        message += f"\n🔗 [Acessar oferta]({offer['link']})\n"
        
        print(f"\n📤 Enviando mensagem:")
        print(message)
        
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.ok:
            print(f"✅ Enviado com sucesso!")
        else:
            print(f"❌ Erro: {response.text}")
            
    except Exception as e:
        print(f"❌ Erro: {e}")

def fetch_offers():
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        time.sleep(5)
        driver.execute_script("window.scrollBy(0, 1500);")
        time.sleep(3)
        
        # Usando o mesmo seletor que funcionou no bot_test2
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Containers encontrados: {len(containers)}")
        
        if not containers:
            print("❌ Nenhum container encontrado")
            return []
        
        offers = []
        for i, container in enumerate(containers[:2]):  # Só 2 para teste
            try:
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                title = title_elem.text.strip()
                
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                print(f"\n📦 Oferta {i+1}: {title[:50]}...")
                offers.append({"title": title, "link": link})
                
            except Exception as e:
                print(f"  ⚠️ Erro: {e}")
                continue
        
        return offers
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        return []
        
    finally:
        if driver:
            driver.quit()

def run_test():
    print("=" * 60)
    print(f"🧪 TESTE FINAL - {datetime.now()}")
    print("=" * 60)
    
    offers = fetch_offers()
    
    if not offers:
        print("\n❌ Nenhuma oferta encontrada")
        return
    
    print(f"\n📊 Encontradas {len(offers)} ofertas")
    
    for i, offer in enumerate(offers, 1):
        print(f"\n--- OFERTA {i} ---")
        print(f"Título: {offer['title']}")
        print(f"Link: {offer['link']}")
        
        partner = extract_partner(offer['title'])
        if partner:
            print(f"Parceiro: {partner}")
        
        send_to_telegram(offer)
        
        if i < len(offers):
            time.sleep(3)
    
    print("\n" + "=" * 60)
    print("✅ Teste concluído!")
    print("=" * 60)

if __name__ == "__main__":
    run_test()
