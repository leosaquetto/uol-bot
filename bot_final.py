# ------------------------------
# Clube UOL Bot - VERSÃO FINAL (seletores genéricos)
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
    chrome_options.add_argument("--window-size=1920,1080")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def extract_offers_from_page(driver):
    """Método GENÉRICO: pega qualquer elemento que pareça uma oferta"""
    offers = []
    
    try:
        # Tenta encontrar QUALQUER coisa que tenha link e imagem
        all_links = driver.find_elements(By.TAG_NAME, "a")
        
        for link in all_links[:20]:  # Limite para não pegar tudo
            try:
                href = link.get_attribute("href")
                if not href or "clube.uol.com.br" not in href:
                    continue
                
                # Tenta encontrar título dentro ou próximo do link
                title = None
                
                # Procura por texto no próprio link ou em elementos próximos
                if link.text and len(link.text) > 10:
                    title = link.text.strip()
                else:
                    # Procura por elementos de título nas redondezas
                    parent = link.find_element(By.XPATH, "..")
                    titles = parent.find_elements(By.CSS_SELECTOR, 
                        "h1, h2, h3, h4, .titulo, .title, .name, strong, b, p")
                    for t in titles:
                        if t.text and len(t.text) > 10:
                            title = t.text.strip()
                            break
                
                if not title:
                    continue
                
                # Tenta encontrar imagem
                img = None
                imgs = link.find_elements(By.TAG_NAME, "img")
                if imgs:
                    img = imgs[0].get_attribute("src") or imgs[0].get_attribute("data-src")
                
                if not img:
                    # Procura por background image
                    elements = link.find_elements(By.XPATH, ".//*[@style]")
                    for el in elements:
                        style = el.get_attribute("style")
                        match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                        if match:
                            img = match.group(1)
                            break
                
                if img and title and href:
                    offers.append({
                        "title": title,
                        "link": href,
                        "img_url": img
                    })
                    
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"Erro ao extrair ofertas: {e}")
    
    return offers

def extract_page_content(driver):
    """Extrai conteúdo relevante da página da oferta"""
    content = []
    
    try:
        # Pega TODOS os parágrafos e textos significativos
        elements = driver.find_elements(By.CSS_SELECTOR, 
            "p, div:not([class*='menu']):not([class*='footer']):not([class*='header'])")
        
        for el in elements:
            text = el.text.strip()
            if len(text) > 20 and "benefício" in text.lower() or "regra" in text.lower() or "válido" in text.lower():
                if text not in content:
                    content.append(text)
                    
    except Exception as e:
        print(f"Erro ao extrair conteúdo: {e}")
    
    return content

def extract_partner(text):
    """Extrai parceiro do texto"""
    match = re.search(r'(?:por|via|[-–—])\s*([^-–—]+?)(?:\s*[-–—]|$)', text, re.IGNORECASE)
    return match.group(1).strip() if match else None

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

def send_to_telegram(offer, img_path, content):
    """Envia mensagem com imagem"""
    try:
        partner = extract_partner(offer['title'])
        
        caption = f"*{offer['title']}*\n\n"
        if partner:
            caption += f"🏷️ *Parceiro:* {partner}\n\n"
        
        for c in content[:3]:  # Máximo 3 parágrafos
            caption += f"{c}\n\n"
        
        caption += f"🔗 [Acessar oferta]({offer['link']})"
        
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

def run_bot():
    print("🤖 Bot UOL iniciado...")
    
    history = load_history()
    seen_ids = set(history.get("lastIds", []))
    
    # Busca ofertas
    driver = None
    try:
        driver = setup_driver()
        driver.get(TARGET_URL)
        time.sleep(5)
        driver.execute_script("window.scrollBy(0, 1500);")
        time.sleep(3)
        
        offers = extract_offers_from_page(driver)
        
        if not offers:
            print("❌ Nenhuma oferta encontrada")
            return
        
        # Filtra novas
        new_offers = []
        for offer in offers[:4]:  # Limite de 4 ofertas
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
                    continue
                
                # Pega conteúdo da página
                content = []
                try:
                    detail_driver = setup_driver()
                    detail_driver.get(offer['link'])
                    time.sleep(3)
                    content = extract_page_content(detail_driver)
                except:
                    pass
                finally:
                    if 'detail_driver' in locals():
                        detail_driver.quit()
                
                # Envia
                if send_to_telegram(offer, img_path, content):
                    print(f"  ✅ Enviado")
                else:
                    print(f"  ❌ Falha")
                
                # Limpa
                if os.path.exists(img_path):
                    os.remove(img_path)
                
                if i < len(new_offers):
                    time.sleep(3)
            
            # Atualiza histórico
            all_ids = [re.sub(r'[^a-z0-9]', '', o['title'].lower())[:40] for o in offers[:4]]
            save_history({"lastIds": all_ids})
            print("\n✅ Concluído!")
        else:
            print("📭 Nenhuma oferta nova")
            
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    run_bot()
