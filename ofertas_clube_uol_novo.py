# ------------------------------
# ofertas_clube_uol_novo.py
# Clube UOL Bot - Versão Simplificada com Título da Página
# ------------------------------

import requests
import json
import os
import time
import re
import random
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
HISTORY_FILE = "historico_ids.json"
MAX_CAPTION_LENGTH = 1024  # Telegram: 1024 para fotos
MAX_OFFERS_PER_RUN = 8  # Limite de 8 ofertas por execução

# Lista de User Agents variados
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def load_history():
    """Carrega histórico de IDs já enviados"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                # Mantém apenas os últimos 100 IDs para não crescer muito
                if len(data.get("ids", [])) > 100:
                    data["ids"] = data["ids"][-100:]
                return data
    except:
        pass
    return {"ids": []}

def save_history(history):
    """Salva histórico"""
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

def human_like_delay(min_seconds=1, max_seconds=3):
    """Pausa com comportamento humano"""
    time.sleep(random.uniform(min_seconds, max_seconds))

def setup_driver():
    """Configura Chrome com anti-detecção"""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User Agent aleatório
    user_agent = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    chrome_options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en;q=0.8")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Remove vestígios de automação
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def extract_page_title(driver):
    """Extrai o título da página da oferta"""
    try:
        # Tenta pegar o H1 da página (geralmente tem o título completo)
        h1_elements = driver.find_elements(By.CSS_SELECTOR, "h1")
        if h1_elements and h1_elements[0].text.strip():
            return h1_elements[0].text.strip()
        
        # Se não achar H1, pega o título da página
        title = driver.title
        # Remove "Clube UOL" do título se estiver no final
        title = re.sub(r'\s*[–—-]\s*Clube UOL\s*$', '', title)
        return title.strip()
    except:
        return None

def extract_validity(driver):
    """Extrai apenas a validade do benefício"""
    try:
        # Procura por validade em vários formatos
        validity_selectors = [
            ".validity", 
            "[class*='validade']", 
            ".date", 
            "time", 
            "[class*='prazo']",
            "[class*='valido']",
            ".periodo",
            "p:contains('válido')",
            "span:contains('válido')"
        ]
        
        for selector in validity_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                text = elems[0].text.strip()
                # Filtra apenas textos que parecem validade
                if any(word in text.lower() for word in ['válido', 'validade', 'até', 'de', 'a partir de']):
                    return text[:100]
        
        # Procura em parágrafos que contenham "válido"
        paragraphs = driver.find_elements(By.CSS_SELECTOR, "p")
        for p in paragraphs:
            text = p.text.strip()
            if "válido" in text.lower() and len(text) < 150:
                return text
                
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair validade: {e}")
    
    return None

def download_image(img_url):
    """Baixa imagem para enviar como anexo"""
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://clube.uol.com.br/'
        }
        response = requests.get(img_url, headers=headers, timeout=15)
        if response.status_code == 200:
            temp_path = f"/tmp/oferta_img_{random.randint(1000,9999)}.jpg"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            return temp_path
    except Exception as e:
        print(f"Erro ao baixar imagem: {e}")
    return None

def build_caption(page_title, validity, link):
    """Constrói legenda simples: Título da página + Validade (se houver) + Link"""
    parts = []
    
    # 1. Título da página (já vem com parceiro incluso)
    if page_title:
        parts.append(f"*{page_title}*")
    else:
        return None  # Não conseguiu título, não envia
    
    # 2. Validade (se encontrou)
    if validity:
        # Remove palavras repetitivas se necessário
        validity_clean = re.sub(r'^(benef[ií]cio\s+v[aá]lido\s*:\s*)', '', validity, flags=re.IGNORECASE)
        parts.append(f"📅 {validity_clean}")
    
    # 3. Link
    parts.append(f"🔗 [Acessar oferta]({link})")
    
    # Junta tudo
    caption = "\n\n".join(parts)
    
    # Verifica limite de caracteres
    if len(caption) > MAX_CAPTION_LENGTH:
        print(f"⚠️ Legenda com {len(caption)} caracteres, truncando...")
        # Trunca mantendo o link
        link_pos = caption.rfind("🔗 [Acessar oferta]")
        if link_pos > 0:
            truncated = caption[:MAX_CAPTION_LENGTH - len(caption[link_pos:]) - 3] + "..."
            caption = truncated + "\n\n" + caption[link_pos:]
        else:
            caption = caption[:MAX_CAPTION_LENGTH-3] + "..."
    
    print(f"📝 Legenda final: {len(caption)} caracteres")
    return caption

def send_to_telegram_with_image(img_path, caption):
    """Envia mensagem com imagem anexada"""
    try:
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
            print(f"❌ Erro Telegram: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erro no envio: {e}")
        return False

def fetch_offers_from_main_page():
    """Pega lista de ofertas da página principal (máximo 8)"""
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Carregando URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        # Comportamento humano
        human_like_delay(3, 5)
        
        # Scroll suave
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        
        # Pega containers de oferta
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Containers encontrados: {len(containers)}")
        
        if not containers:
            print("❌ Nenhum container encontrado")
            return []
        
        # Pega apenas as 8 primeiras ofertas
        offers = []
        for i, container in enumerate(containers[:MAX_OFFERS_PER_RUN]):
            try:
                # Título (da página principal, só para referência)
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                preview_title = title_elem.text.strip()
                
                if not preview_title:
                    continue
                
                # Link
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                # IMAGEM GRANDE
                img_url = None
                
                # 1. Tenta background image (é o que estava funcionando)
                elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                for el in elements_with_bg:
                    style = el.get_attribute("style")
                    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if match:
                        img_url = match.group(1)
                        print(f"  📸 Oferta {i+1}: Imagem GRANDE (background)")
                        break
                
                # 2. Tenta data-src (lazy loading)
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                    if imgs:
                        img_url = imgs[0].get_attribute("data-src")
                        print(f"  📸 Oferta {i+1}: Imagem GRANDE (data-src)")
                
                # 3. Fallback: qualquer imagem
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img")
                    if imgs:
                        img_url = imgs[0].get_attribute("src")
                        print(f"  📸 Oferta {i+1}: Imagem (fallback)")
                
                if img_url and img_url.startswith('//'):
                    img_url = 'https:' + img_url
                
                offers.append({
                    "preview_title": preview_title,
                    "link": link,
                    "imagem_url": img_url
                })
                
                print(f"     Preview: {preview_title[:50]}...")
                
            except Exception as e:
                print(f"  ⚠️ Erro na oferta {i+1}: {e}")
                continue
        
        return offers
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        return []
        
    finally:
        if driver:
            driver.quit()

def process_offer_details(offer):
    """Acessa página da oferta e extrai título e validade"""
    print(f"\n🔍 Acessando página da oferta: {offer['preview_title'][:50]}...")
    
    driver = None
    try:
        driver = setup_driver()
        driver.get(offer['link'])
        
        human_like_delay(2, 4)
        
        # Extrai título da página (já vem com parceiro)
        page_title = extract_page_title(driver)
        if page_title:
            print(f"  📌 Título da página: {page_title[:50]}...")
        
        # Extrai apenas a validade
        validity = extract_validity(driver)
        if validity:
            print(f"  📅 Validade: {validity[:50]}...")
        
        return page_title, validity
        
    except Exception as e:
        print(f"  ⚠️ Erro ao processar página: {e}")
        return None, None
        
    finally:
        if driver:
            driver.quit()

def executar_bot():
    """Função principal"""
    print("=" * 70)
    print(f"🤖 Clube UOL Bot - NOVA VERSÃO - {datetime.now()}")
    print("=" * 70)
    
    # Carrega histórico
    history = load_history()
    seen_ids = set(history.get("ids", []))
    print(f"📋 IDs no histórico: {len(seen_ids)}")
    
    # Pega ofertas da página principal (máximo 8)
    print("\n🔍 Buscando ofertas na página principal...")
    offers = fetch_offers_from_main_page()
    
    if not offers:
        print("❌ Nenhuma oferta encontrada na página principal")
        return
    
    print(f"\n📊 Total de ofertas encontradas: {len(offers)}")
    
    # Cria IDs e filtra novas
    offers_with_ids = []
    for offer in offers:
        # Cria ID baseado no link (mais estável que título)
        offer_id = re.sub(r'[^a-zA-Z0-9]', '', offer['link'].split('/')[-1])[:30]
        if not offer_id:  # Se não conseguir extrair do link, usa título
            offer_id = re.sub(r'[^a-z0-9]', '', offer['preview_title'].lower())[:30]
        
        offers_with_ids.append({
            "id": offer_id,
            **offer
        })
    
    new_offers = [o for o in offers_with_ids if o['id'] not in seen_ids]
    
    if not new_offers:
        print("\n📭 Nenhuma oferta nova")
        return
    
    print(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
    
    # Processa cada nova oferta
    successful = 0
    for i, offer in enumerate(new_offers, 1):
        print(f"\n{'='*50}")
        print(f"📦 Processando oferta {i}/{len(new_offers)}")
        print(f"{'='*50}")
        print(f"Preview: {offer['preview_title']}")
        
        # Baixa imagem
        if not offer.get('imagem_url'):
            print("❌ Sem URL de imagem, pulando...")
            continue
        
        print("\n📥 Baixando imagem...")
        img_path = download_image(offer['imagem_url'])
        
        if not img_path:
            print("❌ Falha ao baixar imagem")
            continue
        
        # Acessa página da oferta para detalhes
        page_title, validity = process_offer_details(offer)
        
        # Se não conseguiu título da página, usa o preview mesmo
        if not page_title:
            page_title = offer['preview_title']
            print(f"  ⚠️ Usando título da página principal")
        
        # Constrói legenda simplificada
        caption = build_caption(page_title, validity, offer['link'])
        
        if not caption:
            print("❌ Falha ao construir legenda")
            if os.path.exists(img_path):
                os.remove(img_path)
            continue
        
        # Envia para o Telegram
        print("\n📤 Enviando para o Telegram...")
        if send_to_telegram_with_image(img_path, caption):
            successful += 1
            # Adiciona ao histórico apenas se enviou com sucesso
            seen_ids.add(offer['id'])
        else:
            print("❌ Falha no envio")
        
        # Limpa arquivo temporário
        if os.path.exists(img_path):
            os.remove(img_path)
        
        # Pausa entre ofertas
        if i < len(new_offers):
            pausa = random.randint(3, 6)
            print(f"\n⏱️ Aguardando {pausa} segundos...")
            human_like_delay(pausa, pausa+1)
    
    # Atualiza histórico
    if successful > 0:
        history["ids"] = list(seen_ids)
        save_history(history)
        print(f"\n✅ Histórico atualizado com {len(seen_ids)} IDs")
    
    print("\n" + "=" * 70)
    print(f"✅ Bot concluído! {successful}/{len(new_offers)} ofertas enviadas")
    print("=" * 70)

if __name__ == "__main__":
    executar_bot()
