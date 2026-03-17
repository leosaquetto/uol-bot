# ------------------------------
# Clube UOL Bot - TESTE AVANÇADO 2 (imagem grande e formatação correta)
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
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def extract_partner_from_page(driver):
    """Extrai o nome do parceiro da página da oferta"""
    try:
        # Tenta encontrar o nome do parceiro no topo da página
        selectors = [
            "h1[class*='partner']",
            ".partner-name",
            ".beneficio-header h2",
            "h2[class*='partner']",
            "[class*='parceiro'] h2",
            "[class*='parceiro'] h1"
        ]
        
        for selector in selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                return elems[0].text.strip()
        
        # Se não achar, tenta extrair do título da página
        title = driver.title
        match = re.search(r'[–—-]\s*([^–—-]+)$', title)
        if match:
            return match.group(1).strip()
            
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair parceiro: {e}")
    return None

def extract_offer_details(driver):
    """Extrai benefício, regras e validade"""
    details = {
        "beneficio": None,
        "regras": [],
        "validade": None
    }
    
    try:
        # Procura por parágrafos que contenham informações
        paragraphs = driver.find_elements(By.CSS_SELECTOR, "p, .description, .text, [class*='descricao']")
        
        for p in paragraphs:
            text = p.text.strip()
            if not text:
                continue
                
            # Identifica se é benefício
            if "benefício" in text.lower() or "Benefício" in text:
                details["beneficio"] = text
                
            # Identifica regras
            elif "regra" in text.lower() or "não é válido" in text.lower() or "não se aplica" in text.lower():
                details["regras"].append(text)
                
        # Procura por validade
        validity_selectors = [".validity", "[class*='validade']", ".date", "time"]
        for selector in validity_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                details["validade"] = elems[0].text.strip()
                break
                
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair detalhes: {e}")
    
    return details

def download_image(img_url):
    """Baixa imagem para enviar como anexo"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(img_url, headers=headers, timeout=10)
        if response.status_code == 200:
            temp_path = "/tmp/temp_image.jpg"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            return temp_path
    except Exception as e:
        print(f"Erro ao baixar imagem: {e}")
    return None

def send_to_telegram_advanced(offer, img_path, partner, details):
    """Envia mensagem com imagem anexada e detalhes formatados"""
    try:
        # Título principal em negrito
        caption = f"*{offer['title']}*\n\n"
        
        # Nome do parceiro (se encontrado)
        if partner:
            caption += f"🏷️ *Parceiro:* {partner}\n\n"
        
        # Benefício
        if details.get("beneficio"):
            caption += f"*Benefício:* {details['beneficio']}\n\n"
        
        # Regras
        if details.get("regras"):
            for regra in details["regras"]:
                caption += f"*Regras:* {regra}\n\n"
        
        # Validade
        if details.get("validade"):
            caption += f"📅 *Benefício válido de:* {details['validade']}\n\n"
        
        # Link
        caption += f"🔗 [Acessar oferta]({offer['link']})"
        
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
    """Pega ofertas com suas imagens GRANDES"""
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
                
                # Tenta encontrar a IMAGEM GRANDE (background ou data-src)
                img_url = None
                
                # Procura por imagens com data-src (lazy loading)
                imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                if imgs:
                    img_url = imgs[0].get_attribute("data-src")
                    print(f"  📸 Imagem GRANDE encontrada (data-src)")
                
                # Se não achou, tenta background image
                if not img_url:
                    elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                    for el in elements_with_bg:
                        style = el.get_attribute("style")
                        match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                        if match:
                            img_url = match.group(1)
                            print(f"  📸 Imagem GRANDE encontrada (background)")
                            break
                
                # Último recurso: pegar qualquer imagem
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img")
                    if imgs:
                        img_url = imgs[0].get_attribute("src")
                        print(f"  📸 Imagem PEQUENA encontrada (fallback)")
                
                print(f"\n📦 Oferta {i+1}: {title[:50]}...")
                print(f"  Link: {link}")
                if img_url:
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
    print("=" * 70)
    print(f"🧪 TESTE AVANÇADO 2 - {datetime.now()}")
    print("=" * 70)
    
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
        
        # Acessa a página da oferta para pegar detalhes
        print(f"\n🔍 Acessando página da oferta...")
        driver = None
        partner = None
        details = {}
        
        try:
            driver = setup_driver()
            driver.get(offer['link'])
            time.sleep(3)
            
            # Extrai parceiro
            partner = extract_partner_from_page(driver)
            
            # Extrai detalhes (benefício, regras, validade)
            details = extract_offer_details(driver)
            
        except Exception as e:
            print(f"  ⚠️ Erro ao processar página: {e}")
        finally:
            if driver:
                driver.quit()
        
        # Envia com imagem
        success = send_to_telegram_advanced(offer, img_path, partner, details)
        
        # Limpa arquivo temporário
        if os.path.exists(img_path):
            os.remove(img_path)
        
        if i < len(offers):
            print("\n⏱️ Aguardando 5 segundos...")
            time.sleep(5)
    
    print("\n" + "=" * 70)
    print("✅ Teste avançado 2 concluído!")
    print("=" * 70)

if __name__ == "__main__":
    run_test()
