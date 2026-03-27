# bot_leouol.py - Versão Ultra Rápida (Foco em Ingressos)
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

# NOVAS URLs DIRETAS PARA INGRESSOS (Muito mais rápido, sem scroll infinito)
TARGET_URLS = [
    "https://clube.uol.com.br/?categoria=ingressosexclusivos",
    "https://clube.uol.com.br/campanhasdeingresso/"
]

HISTORY_FILE = "historico_leouol.json"
MAX_OFFERS_PER_RUN = 10
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

def human_delay(min_s: float = 0.5, max_s: float = 1.5) -> None:
    time.sleep(random.uniform(min_s, max_s))

def normalize_spaces(text: Optional[str]) -> str:
    if not text: return ""
    return re.sub(r"\s+", " ", text).strip()

def escape_html(text: str) -> str:
    if not text: return ""
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;"))

def get_offer_id(link: str) -> str:
    try:
        link_clean = link.split('?')[0]
        return link_clean.rstrip('/').split('/')[-1]
    except:
        return link

def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists(): return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = [get_offer_id(str(x)) for x in data.get("ids", []) if isinstance(data.get("ids", []), list)]
        return {"ids": list(dict.fromkeys(ids))[-MAX_HISTORY_SIZE:]}
    except Exception:
        return {"ids": []}

def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = list(dict.fromkeys([get_offer_id(str(x)) for x in history.get("ids", [])]))[-MAX_HISTORY_SIZE:]
        Path(HISTORY_FILE).write_text(json.dumps({"ids": ids}, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except:
        return False

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len: return text
    return text[:max_len - len(suffix)] + suffix

# ==============================================
# FUNÇÕES DO SCRAPER (SELENIUM)
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
        chrome_options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def extract_page_title(driver, preview_title) -> str:
        try:
            h1 = driver.find_elements(By.CSS_SELECTOR, "h1")
            if h1 and h1[0].text:
                return normalize_spaces(h1[0].text)
        except: pass
        return preview_title

    def extract_validity(driver) -> Optional[str]:
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
            for pattern in [r"[Bb]enefício válido de[^.!?\n]*[.!?]?", r"[Vv]álido até[^.!?\n]*[.!?]?", r"\d{2}/\d{2}/\d{4}.*?\d{2}/\d{2}/\d{4}"]:
                match = re.search(pattern, body)
                if match: return normalize_spaces(match.group(0))
        except: pass
        return None

    def extract_full_description(driver) -> str:
        try:
            text_parts, seen = [], set()
            for selector in [".partner-description", ".benefit-description", "p"]:
                for elem in driver.find_elements(By.CSS_SELECTOR, selector):
                    text = normalize_spaces(elem.text)
                    if text and len(text) > 20:
                        if text[:100].lower() not in seen:
                            seen.add(text[:100].lower())
                            text_parts.append(text)
            if text_parts: return truncate_text("\n\n".join(text_parts), MAX_COMMENT_LENGTH - 150)
            return truncate_text(normalize_spaces(driver.find_element(By.TAG_NAME, "body").text), MAX_COMMENT_LENGTH - 150)
        except:
            return "Descrição detalhada não disponível."

    # SOLUÇÃO DA IMAGEM: Filtra 'beneficios' e ignora 'parceiros'
    def extract_offer_image(container) -> Optional[str]:
        try:
            imgs = container.find_elements(By.CSS_SELECTOR, "img")
            fallback_img = None
            
            for img in imgs:
                src = img.get_attribute("data-src") or img.get_attribute("src")
                if not src or "data:image" in src: continue
                
                # Se achou a imagem certa da pasta beneficios, retorna na hora!
                if "beneficios/" in src:
                    return src if not src.startswith("//") else "https:" + src
                
                # Guarda as que NÃO são da logo (parceiros) como plano B
                if "parceiros/" not in src:
                    fallback_img = src if not src.startswith("//") else "https:" + src
            
            return fallback_img
        except: return None

    def fetch_offers(driver) -> List[Dict[str, str]]:
        offers = []
        seen_ids = set()
        
        for url in TARGET_URLS:
            log(f"\n🌐 Carregando URL Direta: {url}")
            driver.get(url)
            human_delay(2, 4)
            
            # Busca containers baseados nas suas novas seleções!
            containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio, [data-categoria='Ingressos Exclusivos'], .item")
            log(f"📦 Containers encontrados nesta página: {len(containers)}")
            
            for container in containers:
                try:
                    # TÍTULO
                    title_elem = container.find_elements(By.CSS_SELECTOR, ".titulo, h2, h3")
                    if not title_elem: continue
                    preview_title = normalize_spaces(title_elem[0].text)
                    if not preview_title: continue
                    
                    # LINK (SOLUÇÃO DO LINK VAZIO: Foco em 'a.btn' ou no href do card)
                    link_elem = container.find_elements(By.CSS_SELECTOR, "a.btn, .btn")
                    if not link_elem:
                        link_elem = container.find_elements(By.CSS_SELECTOR, "a")
                    
                    if not link_elem: continue
                    
                    link = link_elem[0].get_attribute("href")
                    if not link or "javascript" in link: continue
                    
                    offer_id = get_offer_id(link)
                    if offer_id in seen_ids: continue
                    seen_ids.add(offer_id)
                    
                    # IMAGEM
                    img_url = extract_offer_image(container)
                    
                    offers.append({
                        "id": offer_id,
                        "preview_title": preview_title,
                        "link": link,
                        "img_url": img_url
                    })
                    log(f"  🎫 Achei: {preview_title[:50]}...")
                    
                except Exception as e:
                    continue
                    
        return offers[:MAX_OFFERS_PER_RUN]

    def process_offer_details(driver, offer: Dict) -> Tuple[str, Optional[str], str]:
        try:
            driver.get(offer['link'])
            human_delay(1, 2) # Muito mais rápido agora
            
            page_title = extract_page_title(driver, offer['preview_title'])
            validity = extract_validity(driver)
            full_description = extract_full_description(driver)
            return page_title, validity, full_description
        except:
            return offer['preview_title'], None, "Descrição não disponível"

    def run_fallback_scraper():
        log("=" * 70)
        log("🤖 BOT LEOUOL - Otimizado para Ingressos (V2)")
        log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        log("=" * 70)
        
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
        
        history = load_history()
        seen_ids = set(history.get("ids", []))
        driver = None
        
        try:
            driver = setup_driver()
            offers = fetch_offers(driver)
            new_offers = [o for o in offers if o["id"] not in seen_ids]
            
            if not new_offers:
                log("📭 Nenhuma oferta nova nas categorias de ingresso.")
                return
            
            log(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
            processed_ids, success_count = set(seen_ids), 0
            
            for idx, offer in enumerate(new_offers, 1):
                log(f"\n{'=' * 50}\n📦 Oferta {idx}/{len(new_offers)}: {offer['preview_title']}")
                
                if not offer.get("img_url"):
                    log("⚠️ Sem imagem, pulando...")
                    processed_ids.add(offer["id"])
                    continue
                
                img_path = download_image(offer["img_url"])
                page_title, validity, full_description = process_offer_details(driver, offer)
                
                caption = build_caption(page_title, validity, offer["link"])
                message_id = send_photo_to_channel(img_path, caption)
                
                if message_id and send_description_comment(full_description, offer["link"], message_id):
                    success_count += 1
                    log(f"✅ Oferta enviada com sucesso!")
                else:
                    log(f"❌ Falha ao enviar")
                    
                processed_ids.add(offer["id"])
                try: Path(img_path).unlink(missing_ok=True)
                except: pass
            
            history["ids"] = list(processed_ids)
            save_history(history)
            log(f"\n✅ Fim. {success_count}/{len(new_offers)} enviadas.")
            
        finally:
            if driver: driver.quit()

# ==============================================
# FUNÇÕES COMPARTILHADAS (Telegram e afins)
# ==============================================
def download_image(img_url: str) -> Optional[str]:
    try:
        response = requests.get(img_url, headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=10)
        if response.ok:
            path = f"/tmp/leouol_{int(time.time())}.jpg"
            Path(path).write_bytes(response.content)
            return path
    except: pass
    return None

def build_caption(title: str, validity: Optional[str], link: str) -> str:
    parts = [f"<b>{escape_html(title)}</b>"]
    if validity: parts.append(f"📅 {escape_html(validity)}")
    
    # AGORA O LINK É GARANTIDO:
    parts.append(f"🔗 <a href='{escape_html(link)}'>Acessar oferta</a>")
    parts.append(f"💬 Veja os detalhes completos nos comentários abaixo")
    return truncate_text("\n\n".join(parts), MAX_CAPTION_LENGTH)

def send_photo_to_channel(img_path: str, caption: str) -> Optional[int]:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(img_path, 'rb') as photo:
            res = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'}, files={'photo': photo}, timeout=30)
        return res.json().get("result", {}).get("message_id") if res.ok else None
    except: return None

def send_description_comment(desc: str, link: str, channel_msg_id: int) -> bool:
    group_msg_id = None
    for _ in range(3):
        time.sleep(3)
        try:
            updates = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", timeout=10).json()
            for update in reversed(updates.get("result", [])):
                msg = update.get("message", {})
                if str(msg.get("chat", {}).get("id")) == str(GRUPO_COMENTARIOS_ID):
                    origin_id = msg.get("forward_origin", {}).get("message_id") or msg.get("forward_from_message_id")
                    if origin_id == channel_msg_id:
                        group_msg_id = msg.get("message_id")
                        break
        except: pass
        if group_msg_id: break

    text = f"📋 <b>DESCRIÇÃO COMPLETA</b>\n\n{desc}\n\n🔗 <a href='{escape_html(link)}'>Link original</a>"
    data = {"chat_id": GRUPO_COMENTARIOS_ID, "text": truncate_text(text, MAX_COMMENT_LENGTH), "parse_mode": "HTML", "disable_web_page_preview": True}
    if group_msg_id: data["reply_to_message_id"] = group_msg_id
    
    try:
        return requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data=data, timeout=30).ok
    except: return False

# ==============================================
# ENTRY POINT
# ==============================================
if __name__ == "__main__":
    if IS_CONSUMER:
        run_consumer()
    else:
        run_fallback_scraper()
