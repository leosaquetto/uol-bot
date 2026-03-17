# ------------------------------
# Clube UOL Bot - TESTE AVANÇADO (com imagem e dados do link)
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
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def download_image(img_url):
    """Baixa imagem para enviar como anexo"""
    try:
        response = requests.get(img_url, timeout=10)
        if response.status_code == 200:
            # Salva temporariamente
            temp_path = "/tmp/temp_image.jpg"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            return temp_path
    except Exception as e:
        print(f"Erro ao baixar imagem: {e}")
    return None

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

def fetch_offer_details(link):
    """Acessa a página da oferta para pegar mais detalhes"""
    driver = None
    try:
        print(f"  🔍 Acessando detalhes: {link[:50]}...")
        driver = setup_driver()
        driver.get(link)
        time.sleep(3)
        
        # Tenta pegar descrição
        description = None
        desc_selectors = [".description", "[class*='descricao']", "p", ".text"]
        for sel in desc_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            if elems and len(elems[0].text) > 20:
                description = elems[0].text.strip()[:200] + "..."
                break
        
        # Tenta pegar validade
        validity = None
        date_selectors = [".validity", "[class*='validade']", ".date", "time"]
        for sel in date_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            if elems:
                validity = elems[0].text.strip()
                break
        
        return {
            "description": description,
            "validity": validity
        }
    except Exception as e:
        print(f"  ⚠️ Erro ao acessar detalhes: {e}")
        return {}
    finally:
        if driver:
            driver.quit()

def send_to_telegram_advanced(offer, img_path, details):
    """Envia mensagem com imagem anexada e detalhes"""
    try:
        partner = extract_partner(offer['title'])
        clean = clean_title(offer['title'])
        
        # Monta legenda
        caption = f"*{clean}*\n\n"
        if partner:
            caption += f"🏷️ *Parceiro:* {partner}\n"
        if details.get('description'):
            caption += f"📝 *Descrição:* {details['description']}\n"
        if details.get('validity'):
            caption += f"⏳ *Validade:* {details['validity']}\n"
        caption += f"\n🔗 [Acessar oferta]({offer['link']})"
        
        print(f"\n📤 Enviando imagem com legenda:")
        print(caption)
        
        # Envia foto com legenda
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        
        with open(img_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': caption,
                'parse_mode': 'Markdown'
            }
            response = requests.post(url, data=data, files=files, timeout=30)
        
        if response.ok:
            print(f"✅ Imagem enviada com sucesso!")
            return True
        else:
            print(f"❌ Erro: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

def fetch_offers_with_images():
    """Pega ofertas com suas imagens"""
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        time.sleep(5)
        driver.execute_script("window.scrollBy(0, 1500);")
        time.sleep(3)
        
        # Pega containers
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Containers encontrados: {len(containers)}")
        
        if not containers:
            print("❌ Nenhum container encontrado")
            return []
        
        offers = []
        for i, container in enumerate(containers[:2]):  # Só 2 para teste
            try:
                # Título
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                title = title_elem.text.strip()
                
                # Link
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                # Imagem
                img_elem = container.find_element(By.CSS_SELECTOR, "img")
                img_url = img_elem.get_attribute("src") or img_elem.get_attribute("data-src")
                
                print(f"\n📦 Oferta {i+1}: {title[:50]}...")
                print(f"  Link: {link}")
                print(f"  Imagem: {img_url[:50]}...")
                
                offers.append({
                    "title": title,
                    "link": link,
                    "img_url": img_url
                })
                
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
    print(f"🧪 TESTE AVANÇADO - {datetime.now()}")
    print("=" * 60)
    
    # Pega ofertas
    offers = fetch_offers_with_images()
    
    if not offers:
        print("\n❌ Nenhuma oferta encontrada")
        return
    
    print(f"\n📊 Encontradas {len(offers)} ofertas")
    
    for i, offer in enumerate(offers, 1):
        print(f"\n--- OFERTA {i} ---")
        print(f"Título: {offer['title']}")
        print(f"Link: {offer['link']}")
        print(f"Imagem: {offer['img_url']}")
        
        # Baixa imagem
        print("\n📥 Baixando imagem...")
        img_path = download_image(offer['img_url'])
        
        if not img_path:
            print("❌ Falha ao baixar imagem")
            continue
        
        # Pega detalhes do link
        details = fetch_offer_details(offer['link'])
        
        # Envia com imagem
        success = send_to_telegram_advanced(offer, img_path, details)
        
        # Limpa arquivo temporário
        if os.path.exists(img_path):
            os.remove(img_path)
        
        if i < len(offers):
            print("\n⏱️ Aguardando 5 segundos...")
            time.sleep(5)
    
    print("\n" + "=" * 60)
    print("✅ Teste avançado concluído!")
    print("=" * 60)

if __name__ == "__main__":
    run_test()
