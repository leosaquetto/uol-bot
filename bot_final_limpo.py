# ------------------------------
# Clube UOL Bot - VERSÃO LIMPA (sem duplicações)
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
HISTORY_FILE = "history.json"
MAX_CAPTION_LENGTH = 1024

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

def extract_offer_details(driver):
    """Extrai APENAS o conteúdo relevante, sem duplicar"""
    unique_content = []
    seen = set()
    
    try:
        # Pega todos os parágrafos
        elements = driver.find_elements(By.CSS_SELECTOR, "p, .description, [class*='descricao']")
        
        for el in elements:
            text = el.text.strip()
            if len(text) < 20:  # Ignora textos curtos
                continue
            
            # Verifica se é relevante (tem palavras-chave)
            text_lower = text.lower()
            if ('benefício' in text_lower or 
                'regra' in text_lower or 
                'válido' in text_lower or
                'validade' in text_lower or
                'resgate' in text_lower):
                
                # Remove duplicatas
                if text not in seen:
                    seen.add(text)
                    unique_content.append(text)
        
        # Limita a 3 parágrafos
        return unique_content[:3]
        
    except Exception as e:
        print(f"Erro ao extrair detalhes: {e}")
        return []

def download_image(img_url):
    try:
        response = requests.get(img_url, timeout=10)
        if response.status_code == 200:
            temp_path = "/tmp/temp_image.jpg"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            return temp_path
    except:
        pass
    return None

def send_to_telegram(offer, img_path, details):
    try:
        partner = extract_partner(offer['title'])
        clean = clean_title(offer['title'])
        
        caption = f"*{clean}*\n\n"
        
        if partner:
            caption += f"🏷️ *Parceiro:* {partner}\n\n"
        
        # Adiciona detalhes ÚNICOS
        for detail in details:
            caption += f"{detail}\n\n"
        
        caption += f"🔗 [Acessar oferta]({offer['link']})"
        
        # Trunca se necessário
        if len(caption) > MAX_CAPTION_LENGTH:
            caption = caption[:MAX_CAPTION_LENGTH-50] + "...\n\n" + f"🔗 [Acessar oferta]({offer['link']})"
        
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        
        with open(img_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': caption,
                'parse_mode': 'Markdown'
            }
            response = requests.post(url, data=data, files=files, timeout=30)
        
        return response.ok
        
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        return False

def fetch_offers():
    driver = None
    try:
        driver = setup_driver()
        driver.get(TARGET_URL)
        time.sleep(5)
        driver.execute_script("window.scrollBy(0, 1500);")
        time.sleep(3)
        
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Containers encontrados: {len(containers)}")
        
        if not containers:
            return []
        
        offers = []
        for container in containers[:4]:
            try:
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                title = title_elem.text.strip()
                
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                # Pega imagem grande
                img_url = None
                bg_elements = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                for el in bg_elements:
                    style = el.get_attribute("style")
                    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if match:
                        img_url = match.group(1)
                        break
                
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                    if imgs:
                        img_url = imgs[0].get_attribute("data-src")
                
                if img_url and title and link:
                    offers.append({
                        "title": title,
                        "link": link,
                        "img_url": img_url
                    })
                    
            except Exception as e:
                continue
        
        return offers
        
    finally:
        if driver:
            driver.quit()

def run_bot():
    print("🤖 Bot UOL iniciado...")
    
    history = load_history()
    seen_ids = set(history.get("lastIds", []))
    
    offers = fetch_offers()
    
    if not offers:
        print("❌ Nenhuma oferta encontrada")
        return
    
    print(f"📊 Total: {len(offers)} ofertas")
    
    # Cria IDs e filtra novas
    new_offers = []
    for offer in offers:
        offer_id = re.sub(r'[^a-z0-9]', '', offer['title'].lower())[:40]
        if offer_id not in seen_ids:
            new_offers.append(offer)
    
    if new_offers:
        print(f"🎉 {len(new_offers)} nova(s) oferta(s)!")
        
        for i, offer in enumerate(new_offers, 1):
            print(f"\n📤 ({i}/{len(new_offers)}) {offer['title'][:50]}...")
            
            # Baixa imagem
            img_path = download_image(offer['img_url'])
            if not img_path:
                print("  ❌ Falha ao baixar imagem")
                continue
            
            # Pega detalhes da página
            details = []
            try:
                detail_driver = setup_driver()
                detail_driver.get(offer['link'])
                time.sleep(3)
                details = extract_offer_details(detail_driver)
            except Exception as e:
                print(f"  ⚠️ Erro ao pegar detalhes: {e}")
            finally:
                if 'detail_driver' in locals():
                    detail_driver.quit()
            
            # Envia
            if send_to_telegram(offer, img_path, details):
                print("  ✅ Enviado")
            else:
                print("  ❌ Falha no envio")
            
            # Limpa
            if os.path.exists(img_path):
                os.remove(img_path)
            
            if i < len(new_offers):
                time.sleep(3)
        
        # Atualiza histórico
        all_ids = [re.sub(r'[^a-z0-9]', '', o['title'].lower())[:40] for o in offers]
        save_history({"lastIds": all_ids})
        print("\n✅ Concluído!")
    else:
        print("📭 Nenhuma oferta nova")

if __name__ == "__main__":
    run_bot()
