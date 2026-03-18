# ------------------------------
# BOT LEOUOL - Clube UOL Ofertas
# VERSÃO FINAL - Canal + Grupo Privado com Logo do Parceiro
# ATUALIZADO: Evadir Anti-Bots com Undetected Chromedriver
# ------------------------------

import requests
import json
import os
import time
import re
import random
import unicodedata
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import undetected_chromedriver as uc

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CANAL_ID = os.environ.get('TELEGRAM_CHAT_ID')  # Canal principal (ex: -3723320790)
GRUPO_COMENTARIOS_ID = os.environ.get('GRUPO_COMENTARIOS_ID', '-3802235343')  # Grupo para os comentários

TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"
MAX_CAPTION_LENGTH = 1024
MAX_OFFERS_PER_RUN = 8
MAX_HISTORY_SIZE = 200
MAX_COMMENT_LENGTH = 4096

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
# CONFIGURAÇÃO DO CHROME (OTIMIZADO ANTI-BOT COM UC)
# ==============================================
def setup_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    window_sizes = ["1920,1080", "1366,768", "1440,900", "1536,864"]
    options.add_argument(f"--window-size={random.choice(window_sizes)}")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--ignore-ssl-errors=yes')
    
    user_agent = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    # 🔥 FORÇA A VERSÃO 145 do ChromeDriver
    driver = uc.Chrome(options=options, version_main=145)
    
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

def extract_logo_url(driver):
    try:
        logo_selectors = [
            "img[class*='logo']",
            "img[alt*='logo']",
            ".partner-logo img",
            "[class*='parceiro'] img",
            ".beneficio-header img",
            "header img",
            "figure img"
        ]
        
        for selector in logo_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elements:
                src = elem.get_attribute("src") or elem.get_attribute("data-src")
                if src and ('.png' in src or '.jpg' in src or '.jpeg' in src or '.svg' in src):
                    if src.startswith('//'):
                        src = 'https:' + src
                    print(f"  🖼️ Logo encontrado: {src[:50]}...")
                    return src
        return None
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair logo: {e}")
        return None

# ==============================================
# FUNÇÃO: EXTRAIR DESCRIÇÃO COMPLETA (HTML)
# ==============================================
def escape_html(text):
    if not text:
        return ""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def extract_full_description(driver):
    try:
        description_parts = []
        seen_texts = set()
        
        partner_selectors = [".partner-description", "[class*='parceiro'] p", ".about-partner"]
        for selector in partner_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elems:
                    text = elem.text.strip()
                    if text and len(text) > 20 and text not in seen_texts:
                        seen_texts.add(text)
                        safe_text = escape_html(text)
                        description_parts.append(f"🏢 <b>Sobre o parceiro:</b>\n{safe_text}")
                        break
            except:
                continue
        
        benefit_selectors = [".benefit-description", "[class*='beneficio'] p", ".offer-description"]
        for selector in benefit_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elems:
                    text = elem.text.strip()
                    if text and len(text) > 15 and text not in seen_texts:
                        seen_texts.add(text)
                        safe_text = escape_html(text)
                        description_parts.append(f"🎁 <b>Benefício:</b>\n{safe_text}")
                        break
            except:
                continue
        
        rule_selectors = [".rules", "[class*='regras']", ".terms", "li"]
        for selector in rule_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elems:
                    text = elem.text.strip()
                    if text and ("regra" in text.lower() or "não é válido" in text.lower()) and len(text) > 15 and text not in seen_texts:
                        seen_texts.add(text)
                        safe_text = escape_html(text)
                        description_parts.append(f"📋 <b>Regra:</b>\n{safe_text}")
            except:
                continue
        
        validity_text = extract_validity(driver)
        if validity_text and validity_text not in seen_texts:
            seen_texts.add(validity_text)
            safe_text = escape_html(validity_text)
            description_parts.append(f"⏳ <b>Validade:</b>\n{safe_text}")
        
        if len(description_parts) < 2:
            paragraphs = driver.find_elements(By.TAG_NAME, "p")
            for p in paragraphs[:8]:
                text = p.text.strip()
                if text and len(text) > 30 and text not in seen_texts and "Clube UOL" not in text:
                    seen_texts.add(text)
                    safe_text = escape_html(text)
                    description_parts.append(safe_text)
        
        result = "\n\n".join(description_parts)
        
        if len(result) > MAX_COMMENT_LENGTH - 150:
            result = result[:MAX_COMMENT_LENGTH-200] + "...\n\n<i>Descrição truncada devido ao limite do Telegram</i>"
        
        return result if result else "Descrição detalhada não disponível."
        
    except Exception as e:
        print(f"  ⚠️ Erro ao extrair descrição completa: {e}")
        return "Descrição detalhada não disponível."

# ==============================================
# DOWNLOAD DA IMAGEM PRINCIPAL
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
# CONSTRUÇÃO DA LEGENDA PRINCIPAL (HTML)
# ==============================================
def build_caption(page_title, validity, link):
    parts = []
    
    if page_title:
        safe_title = escape_html(page_title)
        parts.append(f"<b>{safe_title}</b>")
    else:
        return None
    
    if validity and len(validity) > 5:
        validity_clean = re.sub(r'^(benef[ií]cio\s+v[aá]lido\s*:\s*)', '', validity, flags=re.IGNORECASE)
        safe_validity = escape_html(validity_clean)
        parts.append(f"📅 {safe_validity}")
    
    parts.append(f"🔗 <a href='{link}'>Acessar oferta</a>")
    
    caption = "\n\n".join(parts)
    
    if len(caption) > MAX_CAPTION_LENGTH:
        link_pos = caption.rfind("🔗 <a href=")
        if link_pos > 0:
            truncated = caption[:MAX_CAPTION_LENGTH - len(caption[link_pos:]) - 3] + "..."
            caption = truncated + "\n\n" + caption[link_pos:]
        else:
            caption = caption[:MAX_CAPTION_LENGTH-3] + "..."
    
    print(f"📝 Legenda: {len(caption)} caracteres")
    return caption

# ==============================================
# Envio de comentário e logo via HTML e Reply
# ==============================================
def send_logo_and_description(logo_url, full_description, link, channel_message_id):
    try:
        logo_path = None
        if logo_url:
            print("  📥 Baixando logo do parceiro...")
            try:
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                response = requests.get(logo_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    logo_path = f"/tmp/logo_{random.randint(1000,9999)}.jpg"
                    with open(logo_path, 'wb') as f:
                        f.write(response.content)
                    print("  ✅ Logo baixado")
            except Exception as e:
                print(f"  ⚠️ Erro ao baixar logo: {e}")

        comment_text = (
            f"📋 <b>DESCRIÇÃO COMPLETA DA OFERTA</b>\n\n"
            f"{full_description}\n\n"
            f"🔗 <a href='{link}'>Link original</a>"
        )
        
        if len(comment_text) > MAX_COMMENT_LENGTH:
            comment_text = comment_text[:MAX_COMMENT_LENGTH-50] + "...\n\n<i>Descrição truncada</i>"

        reply_params = json.dumps({
            "chat_id": CANAL_ID,
            "message_id": channel_message_id
        })

        if logo_path:
            photo_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
            with open(logo_path, 'rb') as photo:
                files = {'photo': photo}
                data = {
                    'chat_id': GRUPO_COMENTARIOS_ID,
                    'caption': "🏢 <b>Logo do parceiro</b>",
                    'parse_mode': 'HTML',
                    'reply_parameters': reply_params
                }
                logo_response = requests.post(photo_url, data=data, files=files, timeout=30)
                
                if logo_response.ok:
                    print("  ✅ Logo enviado como comentário no grupo")
                else:
                    print(f"  ⚠️ Erro ao enviar logo: {logo_response.text}")
            
            if os.path.exists(logo_path):
                os.remove(logo_path)

        comment_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        comment_data = {
            'chat_id': GRUPO_COMENTARIOS_ID,
            'text': comment_text,
            'parse_mode': 'HTML',
            'reply_parameters': reply_params,
            'disable_web_page_preview': True
        }
        
        comment_response = requests.post(comment_url, data=comment_data, timeout=30)
        
        if comment_response.ok:
            print("  ✅ Descrição enviada como comentário no grupo")
            message_id = comment_response.json()['result']['message_id']
            
            # Formatação do link dependendo se é username (@) ou ID numérico privado (-)
            group_str = str(GRUPO_COMENTARIOS_ID)
            if group_str.startswith('@'):
                link_url = f"https://t.me/{group_str.replace('@', '')}/{message_id}"
            else:
                clean_id = group_str.replace('-100', '').replace('-', '')
                link_url = f"https://t.me/c/{clean_id}/{message_id}"
                
            print(f"  🔗 Link do comentário: {link_url}")
            return True
        else:
            print(f"  ❌ Erro ao enviar descrição: {comment_response.text}")
            
            if "can't parse entities" in comment_response.text:
                print("  ⚠️ Tentando novamente sem formatação HTML...")
                comment_data['parse_mode'] = None
                clean_text = re.sub('<[^<]+>', '', comment_text)
                comment_data['text'] = clean_text
                comment_response = requests.post(comment_url, data=comment_data, timeout=30)
                
                if comment_response.ok:
                    print("  ✅ Descrição enviada como comentário (sem formatação)")
                    return True
            
            return False
            
    except Exception as e:
        print(f"  ❌ Erro no envio do comentário: {e}")
        return False

# ==============================================
# FUNÇÃO PRINCIPAL DE ENVIO
# ==============================================
def send_offer_with_details(img_path, main_caption, logo_url, full_description, link):
    try:
        photo_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        
        with open(img_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': CANAL_ID,
                'caption': main_caption,
                'parse_mode': 'HTML'
            }
            photo_response = requests.post(photo_url, data=data, files=files, timeout=30)
        
        if not photo_response.ok:
            print(f"❌ Erro ao enviar foto pro canal: {photo_response.text}")
            return False
        
        message_id = photo_response.json()['result']['message_id']
        print(f"✅ Foto enviada no canal (ID: {message_id})")
        
        print("  ⏱️ Aguardando 3 segundos para sincronização de fórum/grupo...")
        time.sleep(3)
        
        return send_logo_and_description(logo_url, full_description, link, message_id)
            
    except Exception as e:
        print(f"❌ Erro geral no fluxo de envio: {e}")
        return False

# ==============================================
# BUSCA OFERTAS NA PÁGINA PRINCIPAL (COM ESPERA EXPLÍCITA E REFRESH)
# ==============================================
def fetch_offers():
    driver = None
    try:
        print("🌐 Iniciando Chrome Undetected...")
        driver = setup_driver()
        
        print(f"📱 Acessando: {TARGET_URL}")
        driver.get(TARGET_URL)
        
        try:
            print("⏳ Aguardando carregamento dinâmico das ofertas...")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.beneficio"))
            )
        except TimeoutException:
            print("⚠️ Tempo excedido. Tentando recarregar a página (Refresh)...")
            driver.refresh()
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.beneficio"))
                )
            except TimeoutException:
                print("❌ Página carregou, mas as ofertas não apareceram. Possível bloqueio de IP/Anti-bot.")
                return []
        
        human_like_delay(2, 4)
        
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        driver.execute_script("window.scrollBy(0, 800);")
        human_like_delay(1, 2)
        
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        print(f"📦 Total de ofertas na página: {len(containers)}")
        
        if not containers:
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
        print(f"❌ Erro geral ao buscar ofertas: {e}")
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
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1, .partner-description, .benefit-description"))
            )
        except TimeoutException:
            pass
            
        human_like_delay(1, 3)
        
        page_title = driver.title
        
        if "Your connection is not private" in page_title or "Cloudflare" in page_title:
            print(f"  ⚠️ Página com erro de SSL ou Anti-bot bloqueando os detalhes internos")
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
        
        logo_url = extract_logo_url(driver)
        full_description = extract_full_description(driver)
        print(f"  📋 Descrição completa: {len(full_description)} caracteres")
        
        return page_title, validity, logo_url, full_description
        
    except Exception as e:
        print(f"  ⚠️ Erro: {e}")
        return offer['preview_title'], None, None, "Descrição não disponível devido a erro."
        
    finally:
        if driver:
            driver.quit()

# ==============================================
# FUNÇÃO PRINCIPAL
# ==============================================
def main():
    print("=" * 70)
    print(f"🤖 BOT LEOUOL - Clube UOL Ofertas (CANAL + GRUPO COM LOGO)")
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
                wait_time = random.randint(15, 30)
                print(f"⏱️ Aguardando {wait_time} segundos antes de tentar novamente para despistar o anti-bot...")
                time.sleep(wait_time)
            attempt += 1
    
    if not offers:
        print("❌ Todas as tentativas falharam. Site bloqueou o acesso ou está demorando muito para carregar.")
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
        
        page_title, validity, logo_url, full_description = process_offer(offer)
        main_caption = build_caption(page_title, validity, offer['link'])
        
        if not main_caption:
            print("❌ Falha na legenda")
            if os.path.exists(img_path):
                os.remove(img_path)
            processed_ids.add(offer['id'])
            continue
        
        print("\n📤 Enviando oferta com logo e descrição...")
        if send_offer_with_details(img_path, main_caption, logo_url, full_description, offer['link']):
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
    print(f"✅ FINALIZADO! {successful}/{len(new_offers)} ofertas enviadas")
    print("=" * 70)

if __name__ == "__main__":
    main()
