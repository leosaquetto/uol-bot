# bot_leouol.py - Versão com import condicional
# ==============================================
# IMPORTAÇÕES CONDICIONAIS
# ==============================================
import sys
import requests
import json
import os
import time
import re
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Verifica se é modo consumer (não precisa de selenium)
IS_CONSUMER = len(sys.argv) > 1 and sys.argv[1] == "--pending"

if not IS_CONSUMER:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
else:
    # Placeholders para evitar erros de import
    webdriver = None
    Options = None
    Service = None
    By = None
    WebDriverWait = None
    EC = None
    ChromeDriverManager = None

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIOS_ID = os.environ.get("GRUPO_COMENTARIO_ID", "-1003802235343")

TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"

MAX_OFFERS_PER_RUN = 8
MAX_HISTORY_SIZE = 200
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# ==============================================
# FUNÇÕES UTILITÁRIAS
# ==============================================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def human_delay(min_s: float = 1.0, max_s: float = 2.5) -> None:
    time.sleep(random.uniform(min_s, max_s))

def normalize_spaces(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def escape_html(text: str) -> str:
    if not text:
        return ""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))

def get_offer_id(link: str) -> str:
    try:
        link_clean = link.split('?')[0]
        return link_clean.rstrip('/').split('/')[-1]
    except:
        return link

def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = data.get("ids", [])
        if not isinstance(ids, list):
            return {"ids": []}
        ids = [get_offer_id(str(x)) for x in ids]
        return {"ids": list(dict.fromkeys(ids))[-MAX_HISTORY_SIZE:]}
    except Exception as e:
        log(f"⚠️ Erro ao carregar histórico: {e}")
        return {"ids": []}

def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = [get_offer_id(str(x)) for x in history.get("ids", [])]
        ids = list(dict.fromkeys(ids))[-MAX_HISTORY_SIZE:]
        
        Path(HISTORY_FILE).write_text(
            json.dumps({"ids": ids}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except Exception as e:
        log(f"⚠️ Erro ao salvar histórico: {e}")
        return False

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len:
        return text
    if max_len <= len(suffix):
        return suffix[:max_len]
    return text[:max_len - len(suffix)] + suffix

# ==============================================
# FUNÇÕES DO FALLBACK (SELENIUM)
# ==============================================
if not IS_CONSUMER:
    def setup_driver():
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--lang=pt-BR")
        
        random_ua = random.choice(USER_AGENTS)
        chrome_options.add_argument(f"--user-agent={random_ua}")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--ignore-ssl-errors=yes")
        
        log(f"🚀 Iniciando Chrome")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        except:
            pass
        
        return driver

    def extract_page_title(driver) -> str:
        try:
            h1 = driver.find_elements(By.CSS_SELECTOR, "h1")
            if h1:
                title = normalize_spaces(h1[0].text)
                if title:
                    return title
            title = driver.title
            if title:
                title = re.sub(r'\s*[–—-]\s*Clube UOL\s*$', '', title, flags=re.IGNORECASE)
                return normalize_spaces(title)
        except Exception as e:
            log(f"  ⚠️ Erro ao extrair título: {e}")
        return "Oferta Clube UOL"

    def extract_validity(driver) -> Optional[str]:
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
            patterns = [
                r"[Bb]enefício válido de[^.!?\n]*[.!?]?",
                r"[Vv]álido até[^.!?\n]*[.!?]?",
                r"[Vv]alidade[^.!?\n]*[.!?]?",
                r"\d{2}/\d{2}/\d{4}.*?\d{2}/\d{2}/\d{4}",
            ]
            for pattern in patterns:
                match = re.search(pattern, body)
                if match:
                    return normalize_spaces(match.group(0))
        except Exception as e:
            log(f"  ⚠️ Erro ao extrair validade: {e}")
        return None

    def extract_full_description(driver) -> str:
        try:
            text_parts = []
            seen = set()
            
            selectors = [
                ".partner-description",
                ".benefit-description", 
                ".offer-description",
                "[class*='descricao']",
                "[class*='description']",
                "p"
            ]
            
            for selector in selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    text = normalize_spaces(elem.text)
                    if text and len(text) > 20:
                        key = text[:100].lower()
                        if key not in seen:
                            seen.add(key)
                            text_parts.append(text)
            
            if text_parts:
                full_text = "\n\n".join(text_parts)
                return truncate_text(full_text, MAX_COMMENT_LENGTH - 150, "...")
            
            body = driver.find_element(By.TAG_NAME, "body").text
            return truncate_text(normalize_spaces(body), MAX_COMMENT_LENGTH - 150, "...")
        except Exception as e:
            log(f"  ⚠️ Erro ao extrair descrição: {e}")
            return "Descrição detalhada não disponível."

    def extract_offer_image(container) -> Optional[str]:
        try:
            imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
            if imgs:
                img_url = imgs[0].get_attribute("data-src")
                if img_url:
                    if img_url.startswith("//"):
                        return "https:" + img_url
                    return img_url
            
            elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
            for el in elements_with_bg:
                style = el.get_attribute("style")
                if style:
                    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if match:
                        img_url = match.group(1)
                        if img_url.startswith("//"):
                            return "https:" + img_url
                        return img_url
        except Exception as e:
            log(f"  ⚠️ Erro ao extrair imagem: {e}")
        return None

    def fetch_offers(driver) -> List[Dict[str, str]]:
        log(f"🌐 Carregando: {TARGET_URL}")
        driver.get(TARGET_URL)
        human_delay(3, 5)
        
        driver.execute_script("window.scrollBy(0, 500);")
        human_delay(1, 2)
        driver.execute_script("window.scrollBy(0, 500);")
        human_delay(1, 2)
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.beneficio"))
            )
        except Exception:
            log("⚠️ Timeout aguardando containers")
            return []
        
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        log(f"📦 Containers encontrados: {len(containers)}")
        
        if not containers:
            return []
        
        offers = []
        seen_ids = set()
        
        for container in containers[:MAX_OFFERS_PER_RUN * 2]:
            try:
                title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
                preview_title = normalize_spaces(title_elem.text)
                if not preview_title:
                    continue
                
                link_elem = container.find_element(By.CSS_SELECTOR, "a")
                link = link_elem.get_attribute("href")
                if not link:
                    continue
                
                offer_id = get_offer_id(link)
                if offer_id in seen_ids:
                    continue
                seen_ids.add(offer_id)
                
                img_url = extract_offer_image(container)
                
                offers.append({
                    "id": offer_id,
                    "preview_title": preview_title,
                    "link": link,
                    "img_url": img_url
                })
                
                log(f"  📦 {preview_title[:50]}...")
                
            except Exception as e:
                log(f"  ⚠️ Erro ao processar container: {e}")
                continue
        
        return offers[:MAX_OFFERS_PER_RUN]

    def process_offer_details(driver, offer: Dict) -> Tuple[str, Optional[str], str]:
        try:
            log(f"🔍 Acessando: {offer['preview_title'][:50]}...")
            driver.get(offer['link'])
            human_delay(2, 4)
            
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except:
                pass
            
            page_title = extract_page_title(driver)
            validity = extract_validity(driver)
            full_description = extract_full_description(driver)
            
            log(f"  📝 Título: {page_title[:50]}...")
            if validity:
                log(f"  📅 Validade encontrada")
            
            return page_title, validity, full_description
            
        except Exception as e:
            log(f"  ⚠️ Erro: {e}")
            return offer['preview_title'], None, "Descrição não disponível"

    def run_fallback_scraper():
        log("=" * 70)
        log("🤖 BOT LEOUOL - Fallback Scraper (Selenium)")
        log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        log("=" * 70)
        
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log("❌ Variáveis TELEGRAM_TOKEN e TELEGRAM_CHAT_ID são obrigatórias")
            return
        
        history = load_history()
        seen_ids = set(history.get("ids", []))
        
        driver = None
        
        try:
            driver = setup_driver()
            offers = fetch_offers(driver)
            
            if not offers:
                log("📭 Nenhuma oferta encontrada")
                return
            
            new_offers = [o for o in offers if o["id"] not in seen_ids]
            
            if not new_offers:
                log("📭 Nenhuma oferta nova")
                return
            
            log(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
            
            processed_ids = set(seen_ids)
            success_count = 0
            
            for idx, offer in enumerate(new_offers, 1):
                log(f"\n{'=' * 50}")
                log(f"📦 Oferta {idx}/{len(new_offers)}")
                
                if not offer.get("img_url"):
                    log("⚠️ Sem imagem, ignorando")
                    processed_ids.add(offer["id"])
                    continue
                
                img_path = download_image(offer["img_url"])
                if not img_path:
                    log("⚠️ Falha ao baixar imagem")
                    processed_ids.add(offer["id"])
                    continue
                
                page_title, validity, full_description = process_offer_details(driver, offer)
                caption = build_caption(page_title, validity, offer["link"])
                message_id = send_photo_to_channel(img_path, caption)
                
                if message_id:
                    success = send_description_comment(full_description, offer["link"], message_id)
                    if success:
                        success_count += 1
                        processed_ids.add(offer["id"])
                        log(f"✅ Oferta {idx} enviada!")
                    else:
                        log(f"⚠️ Foto enviada mas comentário falhou")
                        processed_ids.add(offer["id"])
                else:
                    log(f"❌ Falha ao enviar foto")
                
                try:
                    Path(img_path).unlink(missing_ok=True)
                except:
                    pass
                
                if idx < len(new_offers):
                    human_delay(2, 4)
            
            history["ids"] = list(processed_ids)
            save_history(history)
            
            log(f"\n✅ Fim. {success_count}/{len(new_offers)} ofertas enviadas.")
            
        except Exception as e:
            log(f"💥 Erro fatal: {e}")
            
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

# ==============================================
# FUNÇÕES COMPARTILHADAS (Consumer + Fallback)
# ==============================================
def download_image(img_url: str) -> Optional[str]:
    if not img_url:
        return None
    
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://clube.uol.com.br/',
        }
        
        response = requests.get(img_url, headers=headers, timeout=10)
        if response.status_code == 200 and response.content:
            temp_path = f"/tmp/leouol_{int(time.time())}_{random.randint(1000, 9999)}.jpg"
            Path(temp_path).write_bytes(response.content)
            return temp_path
    except Exception as e:
        log(f"  ⚠️ Erro ao baixar imagem: {e}")
    
    return None

def build_caption(title: str, validity: Optional[str], link: str) -> str:
    parts = [f"<b>{escape_html(title)}</b>"]
    
    if validity and len(validity) > 5:
        parts.append(f"📅 {escape_html(validity)}")
    
    parts.append(f"🔗 <a href='{escape_html(link)}'>Acessar oferta</a>")
    parts.append(f"💬 Veja os detalhes completos nos comentários abaixo")
    
    caption = "\n\n".join(parts)
    
    if len(caption) > MAX_CAPTION_LENGTH:
        caption = caption[:MAX_CAPTION_LENGTH - 3] + "..."
    return caption

def send_photo_to_channel(img_path: str, caption: str) -> Optional[int]:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        
        with open(img_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': caption,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, data=data, files=files, timeout=30)
        
        if response.ok:
            result = response.json()
            message_id = result.get("result", {}).get("message_id")
            log(f"✅ Foto enviada")
            return message_id
        else:
            log(f"❌ Erro: {response.text}")
            return None
            
    except Exception as e:
        log(f"❌ Erro: {e}")
        return None

def send_description_comment(full_description: str, link: str, channel_message_id: int) -> bool:
    log("⏳ Enviando comentário com descrição completa...")
    
    group_message_id = None
    max_retries = 3
    
    for attempt in range(max_retries):
        log(f"⏳ Aguardando 3 segundos para o forward (Tentativa {attempt+1}/{max_retries})...")
        time.sleep(3)
        
        try:
            url_updates = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            response = requests.get(url_updates, timeout=10)
            updates = response.json()
            
            if updates.get("ok"):
                for update in reversed(updates.get("result", [])):
                    msg = update.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id"))
                    if chat_id != str(GRUPO_COMENTARIOS_ID):
                        continue
                    
                    is_automatic_forward = msg.get("is_automatic_forward", False)
                    forward_from_msg_id = msg.get("forward_from_message_id")
                    
                    if not forward_from_msg_id and "forward_origin" in msg:
                        origin = msg.get("forward_origin", {})
                        if origin.get("type") == "channel":
                            forward_from_msg_id = origin.get("message_id")
                    
                    if is_automatic_forward and forward_from_msg_id == channel_message_id:
                        group_message_id = msg.get("message_id")
                        break
                        
        except Exception as e:
            log(f"⚠️ Erro ao buscar ID no grupo: {e}")
            
        if group_message_id:
            log(f"✅ ID encontrado no grupo: {group_message_id}")
            break

    if not group_message_id:
        log("⚠️ Não foi possível encontrar o ID da mensagem no grupo")
    
    comment_text = (
        "📋 <b>DESCRIÇÃO COMPLETA DA OFERTA</b>\n\n"
        f"{full_description}\n\n"
        f"🔗 <a href='{escape_html(link)}'>Link original</a>"
    )
    
    data = {
        "chat_id": GRUPO_COMENTARIOS_ID,
        "text": truncate_text(comment_text, MAX_COMMENT_LENGTH),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    
    if group_message_id:
        data["reply_to_message_id"] = group_message_id
        log(f"💬 Enviando comentário como reply ao ID {group_message_id}")
    else:
        log("💬 Enviando comentário sem reply (fallback)")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    try:
        response = requests.post(url, data=data, timeout=35)
        if response.ok:
            log("✅ Comentário enviado com sucesso!")
            return True
        else:
            log(f"❌ Erro ao enviar comentário: {response.text}")
            return False
    except Exception as e:
        log(f"❌ Erro: {e}")
        return False

# ==============================================
# FUNÇÃO CONSUMER
# ==============================================
def run_consumer():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Consumer (Processando pendentes)")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log("❌ Variáveis TELEGRAM_TOKEN e TELEGRAM_CHAT_ID são obrigatórias")
        return
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    
    pending_file = Path("pending_offers.json")
    if not pending_file.exists():
        log("📭 Nenhuma oferta pendente")
        return
    
    with open(pending_file, 'r') as f:
        data = json.load(f)
    
    offers = data.get("offers", [])
    if not offers:
        log("📭 Nenhuma oferta pendente")
        return
    
    log(f"🎉 {len(offers)} ofertas pendentes encontradas!")
    
    processed_ids = set(seen_ids)
    success_count = 0
    failed_ids = []
    
    for idx, offer in enumerate(offers, 1):
        log(f"\n{'=' * 50}")
        log(f"📦 Oferta {idx}/{len(offers)}")
        log(f"🏷️ {offer.get('title', offer.get('preview_title', ''))[:80]}")
        
        if offer["id"] in seen_ids:
            log("  ⏭️ Oferta já enviada anteriormente")
            processed_ids.add(offer["id"])
            continue
        
        if not offer.get("img_url"):
            log("  ⚠️ Sem imagem, ignorando")
            processed_ids.add(offer["id"])
            continue
        
        img_path = download_image(offer["img_url"])
        if not img_path:
            log("  ⚠️ Falha ao baixar imagem")
            processed_ids.add(offer["id"])
            continue
        
        page_title = offer.get("title", offer.get("preview_title", "Oferta"))
        validity = offer.get("validity")
        full_description = offer.get("description", "Descrição não disponível")
        
        caption = build_caption(page_title, validity, offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id:
            success = send_description_comment(full_description, offer["link"], message_id)
            if success:
                success_count += 1
                processed_ids.add(offer["id"])
                log(f"  ✅ Oferta {idx} enviada!")
            else:
                log(f"  ⚠️ Foto enviada mas comentário falhou")
                processed_ids.add(offer["id"])
        else:
            log(f"  ❌ Falha ao enviar foto")
            failed_ids.append(offer["id"])
        
        try:
            Path(img_path).unlink(missing_ok=True)
        except:
            pass
        
        time.sleep(2)
    
    history["ids"] = list(processed_ids)
    save_history(history)
    
    remaining_offers = [o for o in offers if o["id"] in failed_ids]
    
    if remaining_offers:
        log(f"⚠️ {len(remaining_offers)} ofertas falharam, mantendo no pending")
        with open(pending_file, 'w') as f:
            json.dump({"last_update": datetime.now().isoformat(), "offers": remaining_offers}, f, indent=2)
    else:
        with open(pending_file, 'w') as f:
            json.dump({"last_update": datetime.now().isoformat(), "offers": []}, f, indent=2)
        log("✅ Arquivo pending_offers.json limpo")
    
    log(f"\n✅ Fim. {success_count}/{len(offers)} ofertas enviadas.")

# ==============================================
# PONTO DE ENTRADA
# ==============================================
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--pending":
        run_consumer()
    else:
        if IS_CONSUMER:
            # Isso não deve acontecer, mas por segurança
            log("⚠️ Modo consumer detectado mas selenium não disponível")
            run_consumer()
        else:
            run_fallback_scraper()
