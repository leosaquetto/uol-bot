# ------------------------------
# Clube UOL Bot - VERSÃO DE TESTE 2 (com seletores corrigidos)
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
HISTORY_FILE = "history_test.json"

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

def extract_partner_from_title(title):
    """Extrai o parceiro do título"""
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
    """Remove o 'por Parceiro' do título"""
    title = re.sub(r'\s*[–—-]\s*[^–—-]+$', '', title)
    title = re.sub(r'\s*por\s+[^–—-]+$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*via\s+[^–—-]+$', '', title, flags=re.IGNORECASE)
    return title.strip()

def fetch_offers():
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        # Aguarda carregar
        time.sleep(5)
        
        # Rola a página
        driver.execute_script("window.scrollBy(0, 1500);")
        time.sleep(3)
        
        # Vários seletores para tentar
        selectors = [
            "div.beneficio",
            "article",
            ".card-oferta",
            "[class*='offer']",
            "[class*='card']",
            ".product-card",
            "div[class*='beneficio']",
            "div[class*='oferta']"
        ]
        
        containers = []
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                containers = elements
                print(f"✅ Seletor funcionou: {selector} - {len(containers)} encontrados")
                break
        
        if not containers:
            print("❌ Nenhum container encontrado")
            print("📄 Título da página:", driver.title)
            return []
        
        offers = []
        for i, container in enumerate(containers[:5]):  # Pega só 5 para teste
            try:
                print(f"\n--- Container {i+1} ---")
                
                # Tenta encontrar título
                title_selectors = [".titulo", "h2", "h3", "p", ".name", "[class*='title']", "[class*='nome']"]
                title = None
                for sel in title_selectors:
                    elems = container.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        title = elems[0].text.strip()
                        print(f"  Título encontrado: {title[:50]}...")
                        break
                
                if not title:
                    print("  ⚠️ Sem título")
                    continue
                
                # Tenta encontrar link
                link = TARGET_URL
                link_elems = container.find_elements(By.CSS_SELECTOR, "a")
                if link_elems:
                    link = link_elems[0].get_attribute("href")
                    print(f"  Link: {link}")
                
                # Tenta encontrar imagem
                img_elems = container.find_elements(By.CSS_SELECTOR, "img")
                img_url = None
                if img_elems:
                    img_url = img_elems[0].get_attribute("src") or img_elems[0].get_attribute("data-src")
                    if img_url:
                        print(f"  Imagem: {img_url[:50]}...")
                
                # Extrai parceiro
                partner = extract_partner_from_title(title)
                clean_title_text = clean_title(title)
                
                if partner:
                    print(f"  Parceiro detectado: {partner}")
                
                offers.append({
                    "title": title,
                    "clean_title": clean_title_text,
                    "link": link,
                    "img_url": img_url,
                    "partner": partner
                })
                
            except Exception as e:
                print(f"  ⚠️ Erro no container: {e}")
                continue
        
        return offers
        
    except Exception as e:
        print(f"❌ Erro no Selenium: {e}")
        return []
        
    finally:
        if driver:
            driver.quit()

def test_formatting(offers):
    """Testa formatação sem enviar"""
    print("\n" + "=" * 60)
    print("📝 TESTE DE FORMATAÇÃO")
    print("=" * 60)
    
    for i, offer in enumerate(offers, 1):
        print(f"\n--- OFERTA {i} ---")
        print(f"Título original: {offer['title']}")
        print(f"Título limpo: {offer['clean_title']}")
        print(f"Parceiro: {offer['partner'] or 'Não detectado'}")
        print(f"Link: {offer['link']}")
        if offer['img_url']:
            print(f"Imagem: {offer['img_url']}")
        
        # Monta mensagem
        message = f"*{offer['clean_title']}*\n\n"
        if offer['partner']:
            message += f"🏷️ *Parceiro:* {offer['partner']}\n"
        message += f"\n🔗 [Acessar oferta]({offer['link']})\n"
        
        print("\n📤 Mensagem que seria enviada:")
        print(message)
        print("-" * 40)

def run_test():
    print("=" * 60)
    print(f"🧪 TESTE 2 - Bot UOL Enhanced - {datetime.now()}")
    print("=" * 60)
    
    offers = fetch_offers()
    
    if not offers:
        print("\n❌ Nenhuma oferta encontrada")
        return
    
    print(f"\n📊 Total: {len(offers)} ofertas para teste")
    test_formatting(offers)
    
    print("\n" + "=" * 60)
    print("✅ Teste concluído!")
    print("=" * 60)

if __name__ == "__main__":
    run_test()
