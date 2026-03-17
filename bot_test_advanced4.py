# ------------------------------
# Clube UOL Bot - TESTE AVANÇADO 4 (legenda otimizada - 1024 chars)
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
MAX_CAPTION_LENGTH = 1024  # Limite do Telegram

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

def extract_partner_from_page(driver):
    """Extrai o nome do parceiro da página da oferta"""
    try:
        selectors = [
            "h1[class*='partner']",
            ".partner-name",
            ".beneficio-header h2",
            "h2[class*='partner']",
            "[class*='parceiro'] h2",
            ".beneficio-header strong"
        ]
        
        for selector in selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                partner = elems[0].text.strip()
                # Remove " - Clube UOL" se existir
                partner = re.sub(r'\s*[–—-]\s*Clube UOL.*$', '', partner, flags=re.IGNORECASE)
                return partner
        
        # Tenta extrair do título
        title = driver.title
        match = re.search(r'(?:por|via|[-–—])\s*([^-–—]+?)(?:\s*[-–—]\s*Clube UOL|$)', title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
            
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair parceiro: {e}")
    return None

def extract_relevant_content(driver):
    """Extrai apenas o conteúdo relevante (benefício, regras, validade)"""
    content_parts = []
    
    try:
        # Procura por elementos que contenham palavras-chave relevantes
        keywords = ['benefício', 'regra', 'válido', 'validade', 'não é válido', 'não se aplica', 'uso por cpf']
        
        # Pega todos os parágrafos
        elements = driver.find_elements(By.CSS_SELECTOR, "p, .description, .text, [class*='descricao'], [class*='regra'], [class*='validade']")
        
        for el in elements:
            text = el.text.strip()
            if not text or len(text) < 15:  # Ignora textos muito curtos
                continue
            
            # Só adiciona se tiver palavras-chave relevantes
            text_lower = text.lower()
            if any(keyword in text_lower for keyword in keywords):
                # Se já temos este texto, não adiciona duplicado
                if text not in content_parts:
                    content_parts.append(text)
        
        # Se não achou nada com keywords, pega os últimos parágrafos (geralmente onde está a validade)
        if not content_parts and len(elements) > 0:
            # Pega os últimos 3 parágrafos
            for el in elements[-3:]:
                text = el.text.strip()
                if text and len(text) > 15 and text not in content_parts:
                    content_parts.append(text)
        
        return content_parts
        
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair conteúdo: {e}")
        return []

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

def send_to_telegram_advanced(offer, img_path, partner, content_parts):
    """Envia mensagem com imagem anexada e conteúdo relevante"""
    try:
        # Título principal em negrito
        caption = f"*{offer['title']}*\n\n"
        
        # Parceiro (se encontrado)
        if partner:
            caption += f"🏷️ *Parceiro:* {partner}\n\n"
        
        # Conteúdo relevante (sem duplicatas)
        seen = set()
        for part in content_parts:
            if part not in seen:
                seen.add(part)
                caption += f"{part}\n\n"
        
        # Link
        caption += f"🔗 [Acessar oferta]({offer['link']})"
        
        # Verifica tamanho e trunca se necessário (mantendo o link)
        if len(caption) > MAX_CAPTION_LENGTH:
            # Mantém título, parceiro e parte do conteúdo
            base_caption = f"*{offer['title']}*\n\n"
            if partner:
                base_caption += f"🏷️ *Parceiro:* {partner}\n\n"
            
            # Calcula espaço restante para conteúdo (reservando espaço para o link)
            remaining = MAX_CAPTION_LENGTH - len(base_caption) - 100  # -100 para o link
            
            # Adiciona conteúdo truncado
            content_text = ""
            for part in content_parts:
                if len(content_text) + len(part) + 4 < remaining:
                    content_text += f"{part}\n\n"
                else:
                    # Trunca o último parágrafo
                    available = remaining - len(content_text)
                    if available > 30:  # Só adiciona se tiver espaço mínimo
                        content_text += part[:available-3] + "...\n\n"
                    break
            
            caption = base_caption + content_text + f"🔗 [Acessar oferta]({offer['link']})"
        
        print(f"\n📤 Enviando imagem com legenda ({len(caption)} caracteres):")
        print(caption[:300] + "..." if len(caption) > 300 else caption)
        
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
                
                # Tenta encontrar a IMAGEM GRANDE
                img_url = None
                
                # Procura por background image
                elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                for el in elements_with_bg:
                    style = el.get_attribute("style")
                    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if match:
                        img_url = match.group(1)
                        print(f"  📸 Imagem GRANDE encontrada (background)")
                        break
                
                # Se não achou, tenta data-src
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                    if imgs:
                        img_url = imgs[0].get_attribute("data-src")
                        print(f"  📸 Imagem GRANDE encontrada (data-src)")
                
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
    print(f"🧪 TESTE AVANÇADO 4 - {datetime.now()}")
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
        content_parts = []
        
        try:
            driver = setup_driver()
            driver.get(offer['link'])
            time.sleep(3)
            
            # Extrai parceiro
            partner = extract_partner_from_page(driver)
            
            # Extrai conteúdo relevante
            content_parts = extract_relevant_content(driver)
            
        except Exception as e:
            print(f"  ⚠️ Erro ao processar página: {e}")
        finally:
            if driver:
                driver.quit()
        
        # Envia com imagem
        success = send_to_telegram_advanced(offer, img_path, partner, content_parts)
        
        # Limpa arquivo temporário
        if os.path.exists(img_path):
            os.remove(img_path)
        
        if i < len(offers):
            print("\n⏱️ Aguardando 5 segundos...")
            time.sleep(5)
    
    print("\n" + "=" * 70)
    print("✅ Teste avançado 4 concluído!")
    print("=" * 70)

if __name__ == "__main__":
    run_test()
