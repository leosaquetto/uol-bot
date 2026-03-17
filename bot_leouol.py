# ------------------------------
# BOT LEOUOL - Clube UOL Ofertas
# VERSÃO COM COMENTÁRIOS - Envia oferta + descrição completa
# ------------------------------

import requests
import json
import os
import time
import re
import random
import unicodedata
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"
MAX_CAPTION_LENGTH = 1024
MAX_OFFERS_PER_RUN = 8
MAX_HISTORY_SIZE = 200
MAX_COMMENT_LENGTH = 4096  # Limite do Telegram para mensagens de texto

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ==============================================
# FUNÇÃO PARA NORMALIZAR LINKS
# ==============================================
def normalize_link(link):
    try:
        link = unicodedata.normalize('NFKD', link).encode('ASCII', 'ignore').decode('ASCII')
        link = re.sub(r'[^a-zA-Z0-9/:.%_-]', '', link)
        return link
    except:
        return link

# ==============================================
# FUNÇÕES DE HISTÓRICO
# ==============================================
def load_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                if len(data.get("ids", [])) > MAX_HISTORY_SIZE:
                    data["ids"] = data["ids"][-MAX_HISTORY_SIZE:]
                return data
    except Exception as e:
        print(f"⚠️ Erro ao carregar histórico: {e}")
    return {"ids": []}

def save_history(history):
    try:
        if len(history.get("ids", [])) > MAX_HISTORY_SIZE:
            history["ids"] = history["ids"][-MAX_HISTORY_SIZE:]
        
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2)
        
        print(f"✅ Histórico salvo: {len(history['ids'])} IDs (limite {MAX_HISTORY_SIZE})")
        return True
    except Exception as e:
        print(f"⚠️ Erro ao salvar histórico: {e}")
        return False

# ==============================================
# FUNÇÕES DE COMPORTAMENTO HUMANO
# ==============================================
def human_like_delay(min_seconds=1, max_seconds=3):
    time.sleep(random.uniform(min_seconds, max_seconds))

# ==============================================
# CONFIGURAÇÃO DO CHROME
# ==============================================
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument('--ignore-ssl-errors=yes')
    
    user_agent = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"user-agent={user_agent}")
    chrome_options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en;q=0.8")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

# ==============================================
# EXTRAÇÃO DE DADOS DA PÁGINA
# ==============================================
def extract_page_title(driver):
    try:
        h1_elements = driver.find_elements(By.CSS_SELECTOR, "h1")
        if h1_elements and h1_elements[0].text.strip():
            title = h1_elements[0].text.strip()
            title = re.sub(r'\s*[–—-]\s*Clube UOL\s*$', '', title)
            return title
        title = driver.title
        title = re.sub(r'\s*[–—-]\s*Clube UOL\s*$', '', title)
        return title.strip()
    except:
        return None

def extract_validity(driver):
    try:
        page_text = driver.find_element(By.TAG_NAME, "body").text
        
        patterns = [
            r'[Vv]álido até[^.!?]*[.!?]',
            r'[Vv]alidade[^.!?]*[.!?]',
            r'[Bb]enefício válido[^.!?]*[.!?]',
            r'[Pp]romoção válida[^.!?]*[.!?]',
            r'[Cc]upom válido[^.!?]*[.!?]',
            r'[Dd]esconto válido[^.!?]*[.!?]',
            r'[Vv]álido de[^.!?]*[.!?]',
            r'[Vv]álido para compras[^.!?]*[.!?]',
            r'[Vv]álido até \d{1,2}/\d{1,2}/\d{4}',
            r'[Vv]álido de \d{1,2}/\d{1,2}/\d{4}'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, page_text)
            if match:
                return match.group(0).strip()
        
        keywords = ['válido', 'validade', 'até', 'válida']
        paragraphs = driver.find_elements(By.TAG_NAME, "p")
        
        for p in paragraphs:
            text = p.text.strip()
            if any(keyword in text.lower() for keyword in keywords) and len(text) < 200:
                return text
                
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair validade: {e}")
    
    return None

# ==============================================
# NOVA FUNÇÃO: EXTRAIR DESCRIÇÃO COMPLETA DA PÁGINA
# ==============================================
def extract_full_description(driver):
    """Extrai TODO o texto relevante da página da oferta"""
    try:
        # Tenta encontrar o conteúdo principal
        main_selectors = [
            "main",
            "article",
            ".content",
            ".description",
            "[class*='descricao']",
            "[class*='beneficio']",
            "div[class*='info']"
        ]
        
        full_text = []
        
        # Tenta cada seletor de conteúdo principal
        for selector in main_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elements:
                text = elem.text.strip()
                if text and len(text) > 50:  # Se encontrar texto relevante
                    full_text.append(text)
        
        # Se não encontrou com seletores, pega parágrafos importantes
        if not full_text:
            paragraphs = driver.find_elements(By.TAG_NAME, "p")
            for p in paragraphs:
                text = p.text.strip()
                if text and len(text) > 30:
                    full_text.append(text)
        
        # Junta tudo com quebras de linha
        result = "\n\n".join(full_text)
        
        # Remove linhas muito curtas ou repetitivas
        lines = result.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line and len(line) > 15 and "Clube UOL" not in line:
                cleaned_lines.append(line)
        
        result = "\n".join(cleaned_lines)
        
        # Limita ao tamanho máximo do Telegram
        if len(result) > MAX_COMMENT_LENGTH - 100:  # Reserva espaço para cabeçalho
            result = result[:MAX_COMMENT_LENGTH-150] + "...\n\n[Descrição truncada devido ao limite do Telegram]"
        
        return result if result else "Descrição detalhada não disponível."
        
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair descrição completa: {e}")
        return "Descrição detalhada não disponível."

# ==============================================
# DOWNLOAD DA IMAGEM
# ==============================================
def download_image(img_url):
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://clube.uol.com.br/'
        }
        response = requests.get(img_url, headers=headers, timeout=15)
        if response.status_code == 200:
            temp_path = f"/tmp/leouol_{random.randint(1000,9999)}.jpg"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            return temp_path
    except Exception as e:
        print(f"Erro ao baixar imagem: {e}")
    return None

# ==============================================
# CONSTRUÇÃO DA LEGENDA PRINCIPAL
# ==============================================
def build_caption(page_title, validity, link):
    parts = []
    
    if page_title:
        parts.append(f"*{page_title}*")
    else:
        return None
    
    if validity and len(validity) > 5:
        validity_clean = re.sub(r'^(benef[ií]cio\s+v[aá]lido\s*:\s*)', '', validity, flags=re.IGNORECASE)
        parts.append(f"📅 {validity_clean}")
    
    parts.append(f"🔗 [Acessar oferta]({link})")
    parts.append("💬 *Veja os detalhes completos nos comentários abaixo*")
    
    caption = "\n\n".join(parts)
    
    if len(caption) > MAX_CAPTION_LENGTH:
        link_pos = caption.rfind("🔗 [Acessar oferta]")
        if link_pos > 0:
            truncated = caption[:MAX_CAPTION_LENGTH - len(caption[link_pos:]) - 3] + "..."
            caption = truncated + "\n\n" + caption[link_pos:]
        else:
            caption = caption[:MAX_CAPTION_LENGTH-3] + "..."
    
    print(f"📝 Legenda: {len(caption)} caracteres")
    return caption

# ==============================================
# ENVIO PRINCIPAL + COMENTÁRIO (NOVA VERSÃO!)
# ==============================================
def send_offer_with_details(img_path, main_caption, full_description, link):
    """Envia a imagem com legenda + comentário com descrição completa"""
    
    try:
        # 1️⃣ ENVIA A FOTO COM LEGENDA PRINCIPAL
        photo_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        
        with open(img_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': main_caption,
                'parse_mode': 'Markdown'
            }
            photo_response = requests.post(photo_url, data=data, files=files, timeout=30)
        
        if not photo_response.ok:
            print(f"❌ Erro ao enviar foto: {photo_response.text}")
            return False
        
        # Pega o message_id da mensagem enviada
        message_id = photo_response.json()['result']['message_id']
        print(f"✅ Foto enviada (ID: {message_id})")
        
        # 2️⃣ PREPARA O TEXTO DO COMENTÁRIO
        # Limpa a descrição para evitar problemas com Markdown
        full_description = full_description.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
        
        comment_text = (
            f"📋 *DESCRIÇÃO COMPLETA DA OFERTA*\n\n"
            f"{full_description}\n\n"
            f"🔗 [Link original da oferta]({link})"
        )
        
        # Garante que não ultrapasse o limite
        if len(comment_text) > MAX_COMMENT_LENGTH:
            comment_text = comment_text[:MAX_COMMENT_LENGTH-50] + "...\n\n*Descrição truncada*"
        
        # 3️⃣ ENVIA O COMENTÁRIO
        comment_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        comment_data = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': comment_text,
            'parse_mode': 'Markdown',
            'reply_to_message_id': message_id  # 🔥 ISSO FAZ SER COMENTÁRIO!
        }
        
        comment_response = requests.post(comment_url, data=comment_data, timeout=30)
        
        if comment_response.ok:
            print("✅ Descrição completa enviada como comentário!")
            return True
        else:
            print(f"❌ Erro ao enviar comentário: {comment_response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erro no envio: {e}")
        return False

# ==============================================
# BUSCA OFERTAS NA PÁGINA PRINCIPAL
# ==============================================
def fetch_offers():
    """Pega as 8 ofertas mais recentes da página principal"""
    driver = None
    try:
        print("🌐 Iniciando Chrome...")
        driver = setup_driver()
        
        print(f"📱 Acessando: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        human_like_delay(3, 5)
        
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Total de ofertas na página: {len(containers)}")
        
        if not containers:
            print("❌ Nenhuma oferta encontrada")
            return []
        
        offers = []
        for i, container in enumerate(containers[:MAX_OFFERS_PER_RUN]):
            try:
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                preview_title = title_elem.text.strip()
                
                if not preview_title:
                    continue
                
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                
                normalized_link = normalize_link(link)
                
                # Imagem
                img_url = None
                elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
                for el in elements_with_bg:
                    style = el.get_attribute("style")
                    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if match:
                        img_url = match.group(1)
                        print(f"  📸 Oferta {i+1}: Imagem (background)")
                        break
                
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
                    if imgs:
                        img_url = imgs[0].get_attribute("data-src")
                        print(f"  📸 Oferta {i+1}: Imagem (data-src)")
                
                if not img_url:
                    imgs = container.find_elements(By.CSS_SELECTOR, "img")
                    if imgs:
                        img_url = imgs[0].get_attribute("src")
                        print(f"  📸 Oferta {i+1}: Imagem (fallback)")
                
                if img_url and img_url.startswith('//'):
                    img_url = 'https:' + img_url
                
                offers.append({
                    "id": normalized_link,
                    "preview_title": preview_title,
                    "link": link,
                    "imagem_url": img_url
                })
                
                print(f"     Título: {preview_title[:50]}...")
                print(f"     ID: {normalized_link[:60]}...")
                
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

# ==============================================
# PROCESSAMENTO DE CADA OFERTA
# ==============================================
def process_offer(offer):
    print(f"\n🔍 Acessando página: {offer['preview_title'][:50]}...")
    
    driver = None
    try:
        driver = setup_driver()
        driver.get(offer['link'])
        
        human_like_delay(2, 4)
        
        page_title = driver.title
        
        if "Your connection is not private" in page_title:
            print(f"  ⚠️ Página com erro SSL (ignorado)")
            try:
                h1_elements = driver.find_elements(By.CSS_SELECTOR, "h1")
                if h1_elements and h1_elements[0].text.strip():
                    page_title = h1_elements[0].text.strip()
                else:
                    page_title = offer['preview_title']
            except:
                page_title = offer['preview_title']
        else:
            page_title = extract_page_title(driver)
            if not page_title:
                page_title = offer['preview_title']
        
        print(f"  📌 Título da página: {page_title[:50]}...")
        
        validity = extract_validity(driver)
        if validity:
            print(f"  📅 Validade: {validity[:50]}...")
        
        # 🔥 NOVO: Extrai descrição completa
        full_description = extract_full_description(driver)
        print(f"  📋 Descrição completa: {len(full_description)} caracteres")
        
        return page_title, validity, full_description
        
    except Exception as e:
        print(f"  ⚠️ Erro: {e}")
        return offer['preview_title'], None, "Descrição não disponível devido a erro."
        
    finally:
        if driver:
            driver.quit()

# ==============================================
# FUNÇÃO PRINCIPAL
# ==============================================
def main():
    print("=" * 70)
    print(f"🤖 BOT LEOUOL - Clube UOL Ofertas (COM COMENTÁRIOS)")
    print(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70)
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    print(f"📋 IDs no histórico: {len(seen_ids)} (limite {MAX_HISTORY_SIZE})")
    
    max_attempts = 3
    attempt = 1
    offers = []
    
    while attempt <= max_attempts:
        print(f"\n🔄 Tentativa {attempt}/{max_attempts} de buscar ofertas...")
        offers = fetch_offers()
        
        if offers:
            print(f"✅ Sucesso na tentativa {attempt}!")
            break
        else:
            print(f"⚠️ Tentativa {attempt} falhou (0 ofertas)")
            if attempt < max_attempts:
                wait_time = random.randint(10, 20)
                print(f"⏱️ Aguardando {wait_time} segundos antes de tentar novamente...")
                time.sleep(wait_time)
            attempt += 1
    
    if not offers:
        print("❌ Todas as tentativas falharam. Site pode estar fora do ar.")
        return
    
    print(f"\n📊 Encontradas: {len(offers)} ofertas")
    
    new_offers = [o for o in offers if o['id'] not in seen_ids]
    
    if not new_offers:
        print("\n📭 Nenhuma oferta nova")
        return
    
    print(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
    
    successful = 0
    processed_ids = set(seen_ids)
    
    for i, offer in enumerate(new_offers, 1):
        print(f"\n{'='*50}")
        print(f"📦 Oferta {i}/{len(new_offers)}")
        print(f"{'='*50}")
        print(f"Título: {offer['preview_title']}")
        print(f"ID: {offer['id']}")
        
        if not offer.get('imagem_url'):
            print("❌ Sem imagem")
            processed_ids.add(offer['id'])
            continue
        
        print("\n📥 Baixando imagem...")
        img_path = download_image(offer['imagem_url'])
        
        if not img_path:
            print("❌ Falha no download")
            processed_ids.add(offer['id'])
            continue
        
        # 🔥 Processa a página e pega TUDO
        page_title, validity, full_description = process_offer(offer)
        
        # Constrói legenda principal (resumida)
        main_caption = build_caption(page_title, validity, offer['link'])
        
        if not main_caption:
            print("❌ Falha na legenda")
            if os.path.exists(img_path):
                os.remove(img_path)
            processed_ids.add(offer['id'])
            continue
        
        # 📤 Envia com imagem + comentário
        print("\n📤 Enviando oferta com descrição completa...")
        if send_offer_with_details(img_path, main_caption, full_description, offer['link']):
            successful += 1
            print(f"✅ Oferta enviada com sucesso!")
        else:
            print(f"❌ Falha no envio")
        
        processed_ids.add(offer['id'])
        
        if os.path.exists(img_path):
            os.remove(img_path)
        
        if i < len(new_offers):
            pausa = random.randint(3, 6)
            print(f"\n⏱️ Aguardando {pausa}s...")
            human_like_delay(pausa, pausa+1)
    
    if processed_ids:
        history["ids"] = list(processed_ids)
        if save_history(history):
            print(f"\n✅ Histórico atualizado: {len(processed_ids)} IDs")
    
    print("\n" + "=" * 70)
    print(f"✅ FINALIZADO! {successful}/{len(new_offers)} ofertas enviadas com descrição completa")
    print("=" * 70)

if __name__ == "__main__":
    main()
