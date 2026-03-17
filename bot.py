# ------------------------------
# Clube UOL Bot - Versão Selenium para GitHub Actions
# ------------------------------

import requests
import json
import os
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# CONFIGURAÇÕES
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "history.json"

def load_history():
    """Carrega histórico de IDs já enviados"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
    return {"lastIds": []}

def save_history(history):
    """Salva histórico"""
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)

def send_to_telegram(offer):
    """Envia mensagem para o Telegram"""
    try:
        message = f"*{offer['title']}*\n\n🔗 [Acessar oferta]({offer['link']})\n"
        
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, json=payload, timeout=30)
        return response.ok
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        return False

def setup_driver():
    """Configura o Chrome driver para o GitHub Actions"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Roda sem abrir janela
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def fetch_offers():
    """Busca ofertas usando Selenium (executa JavaScript)"""
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        # Rola a página para carregar as imagens
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(2)
        
        # Espera os containers carregarem
        wait = WebDriverWait(driver, 10)
        containers = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 
                "div.beneficio, article, .card-oferta, [class*='offer'], [class*='card']"))
        )
        
        print(f"📦 Containers encontrados: {len(containers)}")
        
        offers = []
        for container in containers[:8]:  # Pega até 8 ofertas
            try:
                # Título
                title_elem = container.find_element(By.CSS_SELECTOR, 
                    ".titulo, h2, h3, p, .name, [class*='title']")
                title = title_elem.text.strip()
                
                # Link
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                if title and link:
                    offers.append({
                        "title": title,
                        "link": link
                    })
                    print(f"  ✅ {title[:50]}...")
                    
            except Exception as e:
                continue
        
        return offers
        
    except Exception as e:
        print(f"❌ Erro no Selenium: {e}")
        return []
        
    finally:
        if driver:
            driver.quit()

def run_bot():
    """Função principal"""
    print("=" * 50)
    print(f"🤖 Bot UOL iniciado - {datetime.now()}")
    print("=" * 50)
    
    # Carrega histórico
    history = load_history()
    seen_ids = set(history.get("lastIds", []))
    print(f"📋 IDs no histórico: {len(seen_ids)}")
    
    # Busca ofertas
    print("\n🔍 Buscando ofertas...")
    current_offers = fetch_offers()
    
    if not current_offers:
        print("❌ Nenhuma oferta encontrada")
        return
    
    print(f"\n📊 Total: {len(current_offers)} ofertas")
    
    # Cria IDs e filtra novas
    offers_with_ids = []
    for offer in current_offers:
        offer_id = re.sub(r'[^a-z0-9]', '', offer['title'].lower())[:40]
        offers_with_ids.append({
            "id": offer_id,
            "title": offer['title'],
            "link": offer.get('link', TARGET_URL)
        })
    
    new_offers = [o for o in offers_with_ids if o['id'] not in seen_ids]
    
    if new_offers:
        print(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
        
        for i, offer in enumerate(new_offers, 1):
            print(f"\n📤 ({i}/{len(new_offers)}) {offer['title'][:50]}...")
            if send_to_telegram(offer):
                print(f"  ✅ Enviado")
            else:
                print(f"  ❌ Falha no envio")
            
            if i < len(new_offers):
                time.sleep(2)
        
        # Atualiza histórico
        all_ids = [o['id'] for o in offers_with_ids]
        save_history({"lastIds": all_ids})
        print("\n✅ Concluído!")
    else:
        print("\n📭 Nenhuma oferta nova")
    
    print("\n" + "=" * 50)
    print(f"✅ Bot finalizado - {datetime.now()}")
    print("=" * 50)

if __name__ == "__main__":
    run_bot()
    
