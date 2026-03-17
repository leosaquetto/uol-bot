# ------------------------------
# Clube UOL Bot - TESTE DE ENVIO REAL (CORRIGIDO)
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
    """Envia mensagem com formatação melhorada"""
    try:
        partner = extract_partner(offer['title'])
        clean = clean_title(offer['title'])
        
        # Monta mensagem
        message = f"*{clean}*\n\n"
        if partner:
            message += f"🏷️ *Parceiro:* {partner}\n"
        message += f"\n🔗 [Acessar oferta]({offer['link']})\n"
        
        print(f"\n📤 Mensagem que será enviada:")
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
            return True
        else:
            print(f"❌ Erro da API Telegram: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erro no envio: {e}")
        return False

def fetch_two_offers():
    """Pega apenas 2 ofertas para teste (usando mesmo seletor do teste2)"""
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL...")
        driver.get(TARGET_URL)
        
        # Aguarda carregar
        time.sleep(5)
        
        # Rola a página
        driver.execute_script("window.scrollBy(0, 1500);")
        time.sleep(3)
        
        # Usa o MESMO seletor que funcionou no teste2
        print("🔍 Buscando containers com seletor 'div.beneficio'...")
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        
        print(f"📦 Containers encontrados: {len(containers)}")
        
        if not containers:
            # Tenta outros seletores como fallback
            print("⚠️ Tentando outros seletores...")
            selectors = ["article", ".card-oferta", "[class*='offer']"]
            for selector in selectors:
                containers = driver.find_elements(By.CSS_SELECTOR, selector)
                if containers:
                    print(f"✅ Seletor alternativo funcionou: {selector} - {len(containers)} encontrados")
                    break
        
        if not containers:
            print("❌ Nenhum container encontrado")
            return []
        
        offers = []
        for i, container in enumerate(containers[:2]):  # Só 2 ofertas
            try:
                # Título
                title_selectors = [".titulo", "h2", "h3", "p", ".name", "[class*='title']"]
                title = None
                for sel in title_selectors:
                    elems = container.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        title = elems[0].text.strip()
                        break
                
                if not title:
                    continue
                
                # Link
                link = TARGET_URL
                link_elems = container.find_elements(By.CSS_SELECTOR, "a")
                if link_elems:
                    link = link_elems[0].get_attribute("href")
                
                print(f"\n📦 Oferta {i+1}: {title[:50]}...")
                offers.append({"title": title, "link": link})
                
            except Exception as e:
                print(f"  ⚠️ Erro no container: {e}")
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
    print(f"🧪 TESTE DE ENVIO REAL - {datetime.now()}")
    print("=" * 60)
    
    # Pega 2 ofertas
    offers = fetch_two_offers()
    
    if not offers:
        print("\n❌ Nenhuma oferta para testar")
        return
    
    print(f"\n📊 Enviando {len(offers)} oferta(s) para o canal @leouol...")
    
    # Envia cada uma
    for i, offer in enumerate(offers, 1):
        print(f"\n--- OFERTA {i} ---")
        print(f"Título original: {offer['title']}")
        
        partner = extract_partner(offer['title'])
        clean = clean_title(offer['title'])
        
        if partner:
            print(f"Parceiro detectado: {partner}")
        print(f"Título limpo: {clean}")
        
        # Envia de verdade!
        success = send_to_telegram(offer)
        
        if success:
            print(f"✅ Oferta {i} enviada!")
        else:
            print(f"❌ Falha ao enviar oferta {i}")
        
        if i < len(offers):
            print("\n⏱️ Aguardando 3 segundos...")
            time.sleep(3)
    
    print("\n" + "=" * 60)
    print("✅ Teste concluído! Verifique o canal @leouol")
    print("=" * 60)

if __name__ == "__main__":
    run_test()
