# ------------------------------
# ofertas_clube_uol.py
# Clube UOL Bot - Busca ofertas e envia para Telegram com imagem
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
HISTORY_FILE = "historico_ofertas.json"
MAX_CAPTION_LENGTH = 1024  # Telegram: 1024 para fotos

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
                return json.load(f)
    except:
        pass
    return {"ultimos_ids": []}

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

def extract_partner_from_page(driver):
    """Extrai o nome do parceiro da página da oferta"""
    try:
        # Prioridade 1: Elementos específicos de parceiro
        selectors = [
            "h1[class*='partner']",
            ".partner-name",
            ".beneficio-header h2",
            ".parceiro-nome",
            "[class*='parceiro'] h1",
            "[class*='parceiro'] h2",
            ".offer-partner",
            ".empresa-parceira"
        ]
        
        for selector in selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems and elems[0].text.strip():
                partner = elems[0].text.strip()
                # Ignora se for "Clube UOL" ou muito genérico
                if partner and "clube uol" not in partner.lower() and len(partner) > 3:
                    return partner
        
        # Prioridade 2: Extrair do breadcrumb/navegação
        breadcrumb = driver.find_elements(By.CSS_SELECTOR, ".breadcrumb, .navegacao, [class*='breadcrumb']")
        if breadcrumb:
            items = breadcrumb[0].find_elements(By.TAG_NAME, "span")
            if len(items) >= 2:
                return items[-1].text.strip()
        
        # Prioridade 3: Extrair do título da página (formato "Benefício - Parceiro")
        title = driver.title
        match = re.search(r'[–—-]\s*([^–—-]+)(?:\s*[–—-]\s*Clube UOL)?$', title)
        if match:
            partner = match.group(1).strip()
            if partner and "clube uol" not in partner.lower():
                return partner
                    
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
        # Procura por todos os textos relevantes
        all_texts = driver.find_elements(By.CSS_SELECTOR, 
            "p, .description, .text, .content, [class*='descricao'], [class*='info'], li")
        
        for elem in all_texts:
            text = elem.text.strip()
            if not text or len(text) < 10:
                continue
            
            text_lower = text.lower()
            
            # Identifica benefício
            if "benefício" in text_lower and not details["beneficio"]:
                # Remove a palavra "Benefício" se vier primeiro
                clean_text = re.sub(r'^benef[ií]cio:?\s*', '', text, flags=re.IGNORECASE)
                details["beneficio"] = clean_text[:200]
                
            # Identifica regras (evitando duplicatas)
            elif any(word in text_lower for word in ['regra', 'não é válido', 'não se aplica', 'exceto', 'válido apenas']):
                if len(text) > 20 and "benefício" not in text_lower:
                    clean_text = re.sub(r'^regras?:?\s*', '', text, flags=re.IGNORECASE)
                    if clean_text not in details["regras"]:  # Evita duplicatas
                        details["regras"].append(clean_text[:150])
        
        # Procura por validade (PRIORIDADE: último item)
        validity_selectors = [
            ".validity", 
            "[class*='validade']", 
            ".date", 
            "time", 
            "[class*='prazo']",
            "[class*='valido']",
            ".periodo"
        ]
        for selector in validity_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                details["validade"] = elems[0].text.strip()[:100]
                break
                
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair detalhes: {e}")
    
    return details

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

def build_caption(offer_title, partner, details, link):
    """Constrói legenda na ordem: Título, Parceiro, Benefício, Regras, Validade, Link"""
    parts = []
    
    # 1. Título principal (em negrito)
    parts.append(f"*{offer_title}*")
    
    # 2. Parceiro (se encontrado e não for Clube UOL)
    if partner and "clube uol" not in partner.lower():
        parts.append(f"🏷️ *Parceiro:* {partner}")
    
    # 3. Benefício
    if details.get("beneficio"):
        beneficio = details["beneficio"]
        # Se não começar com "Benefício", adiciona o label
        if not beneficio.lower().startswith("benefício"):
            parts.append(f"*Benefício:* {beneficio}")
        else:
            parts.append(f"*{beneficio}")
    
    # 4. Regras (máximo 2 para não estourar limite)
    if details.get("regras"):
        for regra in details["regras"][:2]:
            if not regra.lower().startswith("regra"):
                parts.append(f"*Regras:* {regra}")
            else:
                parts.append(f"*{regra}")
    
    # 5. Validade (sempre por último, antes do link)
    if details.get("validade"):
        validade = details["validade"]
        # Remove "Válido:" se já estiver no texto
        if not validade.lower().startswith("válido"):
            parts.append(f"📅 *Benefício válido:* {validade}")
        else:
            parts.append(f"📅 *{validade}")
    
    # 6. Link (sempre no final)
    parts.append(f"🔗 [Acessar oferta]({link})")
    
    # Junta tudo com quebras de linha
    caption = "\n\n".join(parts)
    
    # Verifica limite de caracteres
    if len(caption) > MAX_CAPTION_LENGTH:
        print(f"⚠️ Legenda com {len(caption)} caracteres, ajustando...")
        
        # Remove regras gradativamente até caber
        while len(caption) > MAX_CAPTION_LENGTH and "Regras:" in caption:
            # Remove a última regra
            lines = caption.split("\n\n")
            for j, line in enumerate(lines):
                if line.startswith("*Regras:*") or line.startswith("*Regra:*"):
                    lines.pop(j)
                    caption = "\n\n".join(lines)
                    break
        
        # Se ainda estiver grande, trunca no final (mantendo o link)
        if len(caption) > MAX_CAPTION_LENGTH:
            # Encontra a posição do link
            link_pos = caption.rfind("🔗 [Acessar oferta]")
            if link_pos > 0:
                # Trunca antes do link
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
    """Pega lista de ofertas da página principal"""
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
            print(f"📄 Título da página: {driver.title}")
            return []
        
        offers = []
        for i, container in enumerate(containers):
            try:
                # Título
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                title = title_elem.text.strip()
                
                if not title:
                    continue
                
                # Link
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                # IMAGEM GRANDE (prioridade para data-src)
                img_url = None
                
                # 1. Tenta data-src (lazy loading)
                imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                if imgs:
                    img_url = imgs[0].get_attribute("data-src")
                    print(f"  📸 Oferta {i+1}: Imagem GRANDE (data-src)")
                
                # 2. Tenta background image
                if not img_url:
                    elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                    for el in elements_with_bg:
                        style = el.get_attribute("style")
                        match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                        if match:
                            img_url = match.group(1)
                            print(f"  📸 Oferta {i+1}: Imagem GRANDE (background)")
                            break
                
                # 3. Fallback: qualquer imagem
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img")
                    if imgs:
                        img_url = imgs[0].get_attribute("src")
                        print(f"  📸 Oferta {i+1}: Imagem (fallback)")
                
                if img_url:
                    # Ajusta URL da imagem se necessário
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                
                offers.append({
                    "titulo": title,
                    "link": link,
                    "imagem_url": img_url
                })
                
                print(f"     Título: {title[:50]}...")
                
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
    """Acessa página da oferta e extrai detalhes"""
    print(f"\n🔍 Acessando página da oferta: {offer['titulo'][:50]}...")
    
    driver = None
    try:
        driver = setup_driver()
        driver.get(offer['link'])
        
        human_like_delay(2, 4)
        
        # Extrai parceiro
        partner = extract_partner_from_page(driver)
        if partner:
            print(f"  👤 Parceiro: {partner}")
        
        # Extrai detalhes
        details = extract_offer_details(driver)
        if details.get("beneficio"):
            print(f"  💰 Benefício: {details['beneficio'][:50]}...")
        if details.get("regras"):
            print(f"  📋 Regras: {len(details['regras'])} encontradas")
        if details.get("validade"):
            print(f"  📅 Validade: {details['validade']}")
        
        return partner, details
        
    except Exception as e:
        print(f"  ⚠️ Erro ao processar página: {e}")
        return None, {"beneficio": None, "regras": [], "validade": None}
        
    finally:
        if driver:
            driver.quit()

def executar_bot():
    """Função principal"""
    print("=" * 70)
    print(f"🤖 Clube UOL Bot - OFERTAS CLUBE UOL - {datetime.now()}")
    print("=" * 70)
    
    # Carrega histórico
    history = load_history()
    seen_ids = set(history.get("ultimos_ids", []))
    print(f"📋 IDs no histórico: {len(seen_ids)}")
    
    # Pega ofertas da página principal
    print("\n🔍 Buscando ofertas na página principal...")
    offers = fetch_offers_from_main_page()
    
    if not offers:
        print("❌ Nenhuma oferta encontrada na página principal")
        return
    
    print(f"\n📊 Total de ofertas encontradas: {len(offers)}")
    
    # Cria IDs e filtra novas
    offers_with_ids = []
    for offer in offers:
        offer_id = re.sub(r'[^a-z0-9]', '', offer['titulo'].lower())[:40]
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
        print(f"Título: {offer['titulo']}")
        
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
        partner, details = process_offer_details(offer)
        
        # Constrói legenda na ordem correta
        caption = build_caption(
            offer['titulo'],
            partner,
            details,
            offer['link']
        )
        
        # Envia para o Telegram
        print("\n📤 Enviando para o Telegram...")
        if send_to_telegram_with_image(img_path, caption):
            successful += 1
        else:
            print("❌ Falha no envio")
        
        # Limpa arquivo temporário
        if os.path.exists(img_path):
            os.remove(img_path)
        
        # Pausa entre ofertas
        if i < len(new_offers):
            print(f"\n⏱️ Aguardando {random.randint(3,6)} segundos...")
            human_like_delay(3, 6)
    
    # Atualiza histórico (apenas com as ofertas processadas com sucesso)
    if successful > 0:
        # Pega IDs de todas as ofertas encontradas (não só as novas)
        all_ids = [o['id'] for o in offers_with_ids]
        save_history({"ultimos_ids": all_ids})
        print(f"\n✅ Histórico atualizado com {len(all_ids)} IDs")
    
    print("\n" + "=" * 70)
    print(f"✅ Bot concluído! {successful}/{len(new_offers)} ofertas enviadas")
    print("=" * 70)

if __name__ == "__main__":
    executar_bot()
