# ------------------------------
# BOT LEOUOL - Clube UOL Ofertas
# VERSÃO ULTRA ROBUSTA - Com fallback progressivo
# Canal + thread de comentários no Telegram
# ------------------------------

import json
import os
import random
import re
import time
import unicodedata
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests
import undetected_chromedriver as uc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ==============================================
# FIX: Forçar versão compatível do ChromeDriver
# ==============================================
import subprocess
try:
    chrome_version_output = subprocess.check_output(['google-chrome', '--version']).decode().strip()
    chrome_version = re.search(r'(\d+)\.', chrome_version_output).group(1)
    os.environ['CHROME_VERSION'] = chrome_version
    print(f"✅ Chrome detectado: versão {chrome_version}")
except:
    os.environ['CHROME_VERSION'] = '145'
    print("⚠️ Usando fallback para Chrome versão 145")

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CANAL_ID = os.environ.get('TELEGRAM_CHAT_ID')
GRUPO_COMENTARIOS_ID = os.environ.get('GRUPO_COMENTARIO_ID', '-1003802235343')

TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"
TMP_DIR = Path("/tmp")

MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096
MAX_OFFERS_PER_RUN = 8
MAX_HISTORY_SIZE = 200

# Timeouts - aumentados para mais robustez
LIST_WAIT_SECONDS = 30  # Aumentado de 18
DETAIL_WAIT_SECONDS = 20  # Aumentado de 12
PAGE_LOAD_TIMEOUT = 45  # Aumentado de 35

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",  # Firefox
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",  # Firefox
]

# ==============================================
# UTILITÁRIOS
# ==============================================
def log(msg: str) -> None:
    print(msg, flush=True)

def human_like_delay(action="simple"):
    """Delays que simulam comportamento humano real"""
    if action == "scroll":
        time.sleep(random.uniform(0.3, 0.8))
    elif action == "click":
        time.sleep(random.uniform(0.1, 0.3))
    elif action == "read":
        time.sleep(random.uniform(2.0, 4.0))
    elif action == "page_load":
        time.sleep(random.uniform(1.5, 3.0))
    else:
        time.sleep(random.uniform(0.5, 1.5))

def ensure_env() -> None:
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not CANAL_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Variáveis de ambiente ausentes: {', '.join(missing)}")

def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="124", "Google Chrome";v="124"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    })
    return session

HTTP = build_http_session()

def escape_html(text: Optional[str]) -> str:
    return escape(text or "", quote=False)

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len:
        return text
    if max_len <= len(suffix):
        return suffix[:max_len]
    return text[: max_len - len(suffix)] + suffix

def normalize_spaces(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def normalize_link(link: str) -> str:
    try:
        parsed = urlparse(link.strip())
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc.lower()
        path = unicodedata.normalize("NFKD", parsed.path).encode("ASCII", "ignore").decode("ASCII")
        path = re.sub(r"//+", "/", path)
        path = re.sub(r"[^a-zA-Z0-9/_\-.~:%]", "", path)
        normalized = urlunparse((scheme, netloc, path.rstrip("/"), "", "", ""))
        return normalized
    except Exception:
        return link

def is_possible_block_page(driver) -> bool:
    try:
        title = (driver.title or "").lower()
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        signals = [
            "cloudflare", "access denied", "forbidden", 
            "attention required", "your connection is not private",
            "verifique se você é humano", "captcha", "just a moment",
        ]
        return any(s in title or s in body for s in signals)
    except Exception:
        return False

# ==============================================
# HISTÓRICO
# ==============================================
def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = data.get("ids", [])
        if not isinstance(ids, list):
            return {"ids": []}
        ids = [str(x) for x in ids][-MAX_HISTORY_SIZE:]
        return {"ids": ids}
    except Exception as e:
        log(f"⚠️ Erro ao carregar histórico: {e}")
        return {"ids": []}

def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", [])
        ids = [str(x) for x in ids][-MAX_HISTORY_SIZE:]
        temp_path = Path(f"{HISTORY_FILE}.tmp")
        final_path = Path(HISTORY_FILE)
        temp_path.write_text(
            json.dumps({"ids": ids}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(final_path)
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except Exception as e:
        log(f"⚠️ Erro ao salvar histórico: {e}")
        return False

# ==============================================
# CHROME - COM MÚLTIPLOS NÍVEIS DE STEALTH
# ==============================================
def apply_stealth_js(driver):
    """Aplica múltiplos patches JavaScript para esconder automação"""
    try:
        stealth_js = """
        // Remove webdriver property
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        
        // Adiciona plugins falsos
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Simular idiomas reais
        Object.defineProperty(navigator, 'languages', {
            get: () => ['pt-BR', 'pt', 'en-US', 'en']
        });
        
        // Simular hardware real
        window.chrome = { runtime: {} };
        
        // Sobrescrever permissoes
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        
        // Remover vestigios de automação
        delete window.cdc_adoQpoasnfaaypNdKZ3E;
        delete window.cdc_adoQpoasnfaaypNdKZ3F;
        """
        driver.execute_script(stealth_js)
        log("  ✅ Patches stealth aplicados")
    except Exception as e:
        log(f"  ⚠️ Erro ao aplicar stealth JS: {e}")

def setup_driver(stealth_level=1):
    """
    Configura driver com diferentes níveis de stealth
    level 1: básico
    level 2: médio (com stealth JS)
    level 3: avançado (com todas as proteções)
    """
    options = uc.ChromeOptions()
    
    # Opções básicas sempre presentes
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=pt-BR")
    
    # Tamanho de janela variável (evita fingerprinting)
    if stealth_level >= 2:
        window_sizes = ["1920,1080", "1366,768", "1440,900", "1536,864"]
        options.add_argument(f"--window-size={random.choice(window_sizes)}")
    else:
        options.add_argument("--window-size=1366,768")
    
    # User-Agent variável
    options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    # Stealth nível 2 e 3
    if stealth_level >= 2:
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
    
    # Stealth nível 3 - argumentos extras
    if stealth_level >= 3:
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        options.add_argument("--disable-site-isolation-trials")
    
    chrome_version = int(os.environ.get('CHROME_VERSION', '145'))
    
    try:
        driver = uc.Chrome(
            options=options, 
            headless=True, 
            version_main=chrome_version
        )
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        
        # Aplica stealth JS para níveis 2 e 3
        if stealth_level >= 2:
            apply_stealth_js(driver)
        
        return driver
    except Exception as e:
        log(f"⚠️ Erro ao criar driver com stealth level {stealth_level}: {e}")
        raise

def safe_get(driver, url: str, wait_after: Tuple[float, float] = (1.2, 2.2)) -> bool:
    try:
        driver.get(url)
        human_like_delay("page_load")
        return True
    except Exception as e:
        log(f"⚠️ Erro ao abrir {url}: {e}")
        return False

# ==============================================
# EXTRAÇÃO
# ==============================================
def extract_page_title(driver, fallback: str = "") -> str:
    try:
        selectors = ["h1", ".titulo", "title"]
        for selector in selectors:
            try:
                if selector == "title":
                    text = normalize_spaces(driver.title)
                else:
                    elems = driver.find_elements(By.CSS_SELECTOR, selector)
                    text = normalize_spaces(elems[0].text) if elems else ""
                if text:
                    text = re.sub(r"\s*[–—-]\s*Clube UOL\s*$", "", text, flags=re.IGNORECASE)
                    return text
            except Exception:
                continue
    except Exception:
        pass
    return fallback

def extract_validity(driver) -> Optional[str]:
    try:
        body = driver.find_element(By.TAG_NAME, "body").text
        patterns = [
            r"[Vv]álido até[^.!?\n]*[.!?]?",
            r"[Vv]alidade[^.!?\n]*[.!?]?",
            r"[Bb]enefício válido[^.!?\n]*[.!?]?",
            r"[Pp]romoção válida[^.!?\n]*[.!?]?",
            r"[Cc]upom válido[^.!?\n]*[.!?]?",
            r"[Dd]esconto válido[^.!?\n]*[.!?]?",
            r"[Vv]álido de[^.!?\n]*[.!?]?",
            r"[Vv]álido para compras[^.!?\n]*[.!?]?",
            r"[Vv]álido até \d{1,2}/\d{1,2}/\d{2,4}",
            r"[Vv]álido de \d{1,2}/\d{1,2}/\d{2,4}",
        ]
        for pattern in patterns:
            match = re.search(pattern, body)
            if match:
                return normalize_spaces(match.group(0))
        keywords = ["válido", "validade", "até", "válida"]
        for p in driver.find_elements(By.TAG_NAME, "p"):
            text = normalize_spaces(p.text)
            if text and len(text) < 220 and any(k in text.lower() for k in keywords):
                return text
    except Exception as e:
        log(f"  ⚠️ Erro ao extrair validade: {e}")
    return None

def extract_full_description(driver) -> str:
    try:
        parts = []
        seen = set()
        
        def add_block(prefix: str, text: str) -> None:
            clean = normalize_spaces(text)
            if not clean:
                return
            key = clean.lower()
            if key in seen:
                return
            seen.add(key)
            parts.append(f"{prefix}\n{escape_html(clean)}")
        
        partner_selectors = [".partner-description", "[class*='parceiro'] p", ".about-partner"]
        for selector in partner_selectors:
            for elem in driver.find_elements(By.CSS_SELECTOR, selector):
                text = elem.text.strip()
                if len(text) > 20:
                    add_block("🏢 <b>Sobre o parceiro:</b>", text)
                    break
            if parts:
                break
        
        benefit_selectors = [".benefit-description", "[class*='beneficio'] p", ".offer-description"]
        for selector in benefit_selectors:
            for elem in driver.find_elements(By.CSS_SELECTOR, selector):
                text = elem.text.strip()
                if len(text) > 15:
                    add_block("🎁 <b>Benefício:</b>", text)
                    break
        
        validity = extract_validity(driver)
        if validity:
            add_block("⏳ <b>Validade:</b>", validity)
        
        for elem in driver.find_elements(By.CSS_SELECTOR, ".rules, [class*='regras'], .terms, li"):
            text = normalize_spaces(elem.text)
            if not text:
                continue
            low = text.lower()
            if ("regra" in low or "não é válido" in low or "nao e valido" in low or "exceto" in low) and len(text) > 15:
                add_block("📋 <b>Regra:</b>", text)
        
        if len(parts) < 2:
            for p in driver.find_elements(By.TAG_NAME, "p")[:10]:
                text = normalize_spaces(p.text)
                if text and len(text) > 30 and "clube uol" not in text.lower():
                    add_block("📝 <b>Detalhes:</b>", text)
        
        result = "\n\n".join(parts).strip()
        if not result:
            return "Descrição detalhada não disponível."
        
        return truncate_text(
            result,
            MAX_COMMENT_LENGTH - 180,
            "...\n\n<i>Descrição truncada devido ao limite do Telegram</i>",
        )
    except Exception as e:
        log(f"  ⚠️ Erro ao extrair descrição completa: {e}")
        return "Descrição detalhada não disponível."

# ==============================================
# DOWNLOAD DE IMAGEM
# ==============================================
def download_image(img_url: str) -> Optional[str]:
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": "https://clube.uol.com.br/",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        response = HTTP.get(img_url, headers=headers, timeout=20)
        if response.ok and response.content:
            TMP_DIR.mkdir(parents=True, exist_ok=True)
            temp_path = TMP_DIR / f"leouol_{random.randint(1000, 9999)}.jpg"
            temp_path.write_bytes(response.content)
            return str(temp_path)
    except Exception as e:
        log(f"⚠️ Erro ao baixar imagem: {e}")
    return None

def build_caption(page_title: str, validity: Optional[str], link: str) -> Optional[str]:
    page_title = normalize_spaces(page_title)
    if not page_title:
        return None
    
    parts = [f"<b>{escape_html(page_title)}</b>"]
    
    if validity and len(validity) > 5:
        validity_clean = re.sub(
            r"^(benef[ií]cio\s+v[aá]lido\s*:\s*)",
            "",
            validity,
            flags=re.IGNORECASE,
        )
        parts.append(f"📅 {escape_html(normalize_spaces(validity_clean))}")
    
    parts.append(f"🔗 <a href='{escape_html(link)}'>Acessar oferta</a>")
    
    caption = "\n\n".join(parts)
    if len(caption) <= MAX_CAPTION_LENGTH:
        return caption
    
    link_block = f"🔗 <a href='{escape_html(link)}'>Acessar oferta</a>"
    room = MAX_CAPTION_LENGTH - len(link_block) - 4
    short_title = truncate_text(f"<b>{escape_html(page_title)}</b>", max(room, 20))
    return f"{short_title}\n\n{link_block}"

# ==============================================
# TELEGRAM
# ==============================================
def telegram_api(method: str, data=None, files=None, timeout: int = 30) -> Optional[dict]:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        response = HTTP.post(url, data=data, files=files, timeout=timeout)
        payload = response.json()
        if not payload.get("ok"):
            log(f"⚠️ Telegram {method} falhou: {payload}")
        return payload
    except Exception as e:
        log(f"⚠️ Erro de requisição Telegram ({method}): {e}")
        return None

def get_updates(offset: Optional[int] = None) -> List[dict]:
    params = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset
    payload = telegram_api("getUpdates", data=params, timeout=15)
    if payload and payload.get("ok"):
        return payload.get("result", [])
    return []

def clear_telegram_updates() -> None:
    try:
        updates = get_updates()
        if not updates:
            return
        last_id = updates[-1]["update_id"]
        telegram_api("getUpdates", data={"offset": last_id + 1}, timeout=10)
    except Exception:
        pass

def send_description_comment(full_description: str, link: str, channel_message_id: int) -> bool:
    log("  ⏳ Aguardando thread do grupo...")
    
    group_message_id = None
    offset = None
    
    for _ in range(8):  # Aumentado de 6 para 8 tentativas
        time.sleep(3)
        updates = get_updates(offset=offset)
        if updates:
            offset = updates[-1]["update_id"] + 1
        
        for update in updates:
            msg = update.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id"))
            
            if chat_id != str(GRUPO_COMENTARIOS_ID):
                continue
            
            if msg.get("is_automatic_forward"):
                forward_origin = msg.get("forward_origin", {})
                if isinstance(forward_origin, dict):
                    origin_message_id = forward_origin.get("message_id")
                else:
                    origin_message_id = None
                
                forward_from_msg_id = msg.get("forward_from_message_id")
                
                if origin_message_id == channel_message_id or forward_from_msg_id == channel_message_id:
                    group_message_id = msg.get("message_id")
                    break
            
            if not group_message_id:
                entities = msg.get("entities", [])
                for entity in entities:
                    if entity.get("type") == "text_link" and str(channel_message_id) in entity.get("url", ""):
                        group_message_id = msg.get("message_id")
                        break
        
        if group_message_id:
            break
    
    if group_message_id:
        log(f"  ✅ Thread encontrada no grupo (ID: {group_message_id})")
    else:
        log("  ⚠️ Thread não encontrada a tempo. Comentário será enviado sem reply.")
    
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
        data["reply_parameters"] = json.dumps({"message_id": group_message_id})
    
    payload = telegram_api("sendMessage", data=data, timeout=35)
    if payload and payload.get("ok"):
        log("  ✅ Comentário enviado com sucesso!")
        return True
    
    log("  ⚠️ Tentando fallback sem HTML...")
    fallback_data = {
        "chat_id": GRUPO_COMENTARIOS_ID,
        "text": truncate_text(strip_html(comment_text), MAX_COMMENT_LENGTH),
        "disable_web_page_preview": True,
    }
    if group_message_id:
        fallback_data["reply_parameters"] = json.dumps({"message_id": group_message_id})
    
    payload = telegram_api("sendMessage", data=fallback_data, timeout=35)
    return bool(payload and payload.get("ok"))

def send_offer_with_details(img_path: str, main_caption: str, full_description: str, link: str) -> bool:
    try:
        clear_telegram_updates()
        
        with open(img_path, "rb") as photo:
            payload = telegram_api(
                "sendPhoto",
                data={
                    "chat_id": CANAL_ID,
                    "caption": main_caption,
                    "parse_mode": "HTML",
                },
                files={"photo": photo},
                timeout=40,
            )
        
        if not payload or not payload.get("ok"):
            log("❌ Falha ao enviar foto para o canal. Tentando enviar só texto...")
            text_payload = telegram_api(
                "sendMessage",
                data={
                    "chat_id": CANAL_ID,
                    "text": strip_html(main_caption),
                    "disable_web_page_preview": False,
                },
                timeout=30,
            )
            if not text_payload or not text_payload.get("ok"):
                return False
            message_id = text_payload["result"]["message_id"]
        else:
            message_id = payload["result"]["message_id"]
        
        log(f"✅ Oferta enviada ao canal (ID: {message_id})")
        return send_description_comment(full_description, link, message_id)
    
    except Exception as e:
        log(f"❌ Erro geral no envio: {e}")
        return False

# ==============================================
# BUSCA DE OFERTAS
# ==============================================
def extract_offer_image(container) -> Optional[str]:
    try:
        for el in container.find_elements(By.CSS_SELECTOR, "[style*='background']"):
            style = el.get_attribute("style") or ""
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                img_url = match.group(1)
                if img_url.startswith("//"):
                    return "https:" + img_url
                return img_url
        
        for selector in ["img[data-src]", "img[src]"]:
            imgs = container.find_elements(By.CSS_SELECTOR, selector)
            for img in imgs:
                img_url = img.get_attribute("data-src") or img.get_attribute("src")
                if img_url:
                    if img_url.startswith("//"):
                        return "https:" + img_url
                    return img_url
    except Exception:
        pass
    return None

def fetch_offers(driver) -> List[Dict[str, str]]:
    try:
        log("🌐 Abrindo listagem de ofertas...")
        if not safe_get(driver, TARGET_URL):
            return []
        
        # Aguarda com timeout aumentado
        try:
            WebDriverWait(driver, LIST_WAIT_SECONDS).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.beneficio"))
            )
        except TimeoutException:
            log("⚠️ Listagem não carregou a tempo.")
            return []
        
        if is_possible_block_page(driver):
            log("⚠️ Página parece ter retornado bloqueio/challenge.")
            return []
        
        # Scroll mais natural
        for _ in range(3):
            driver.execute_script("window.scrollBy(0, 400);")
            human_like_delay("scroll")
        
        containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
        if not containers:
            return []
        
        offers = []
        seen_run_ids = set()
        
        for container in containers[:MAX_OFFERS_PER_RUN * 2]:
            try:
                preview_title = ""
                for selector in [".titulo", "h2", "h3", "p"]:
                    elems = container.find_elements(By.CSS_SELECTOR, selector)
                    if elems:
                        preview_title = normalize_spaces(elems[0].text)
                        if preview_title:
                            break
                
                if not preview_title:
                    continue
                
                a_elems = container.find_elements(By.CSS_SELECTOR, "a[href]")
                if not a_elems:
                    continue
                
                link = a_elems[0].get_attribute("href")
                if not link:
                    continue
                
                offer_id = normalize_link(link)
                if offer_id in seen_run_ids:
                    continue
                seen_run_ids.add(offer_id)
                
                img_url = extract_offer_image(container)
                
                offers.append({
                    "id": offer_id,
                    "preview_title": preview_title,
                    "link": link,
                    "imagem_url": img_url or "",
                })
            except Exception:
                continue
        
        return offers[:MAX_OFFERS_PER_RUN]
    except Exception as e:
        log(f"❌ Erro geral ao buscar ofertas: {e}")
        return []

# ==============================================
# PROCESSAMENTO DE OFERTA
# ==============================================
def process_offer(driver, offer: Dict[str, str]) -> Tuple[str, Optional[str], str]:
    log(f"\n🔍 Acessando página: {offer['preview_title'][:60]}...")
    
    try:
        if not safe_get(driver, offer["link"], wait_after=(1.0, 2.0)):
            return offer["preview_title"], None, "Descrição não disponível devido a erro de navegação."
        
        # Delay após carregar
        human_like_delay("read")
        
        try:
            WebDriverWait(driver, DETAIL_WAIT_SECONDS).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1, p, .partner-description, .benefit-description"))
            )
        except TimeoutException:
            pass
        
        if is_possible_block_page(driver):
            log("  ⚠️ Página de detalhe parece ser challenge/bloqueio.")
            return offer["preview_title"], None, "Descrição não disponível."
        
        page_title = extract_page_title(driver, fallback=offer["preview_title"])
        validity = extract_validity(driver)
        full_description = extract_full_description(driver)
        
        return page_title, validity, full_description
    
    except Exception as e:
        log(f"  ⚠️ Erro ao processar oferta: {e}")
        return offer["preview_title"], None, "Descrição não disponível devido a erro."

# ==============================================
# FUNÇÃO PRINCIPAL - COM FALLBACK PROGRESSIVO
# ==============================================
def main():
    log("=" * 72)
    log("🤖 BOT LEOUOL - Clube UOL Ofertas")
    log("📢 Canal + thread de comentários")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 72)
    
    ensure_env()
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    
    driver = None
    offers = []
    
    # Tenta com níveis progressivos de stealth
    for stealth_level in [1, 2, 3]:
        try:
            log(f"\n🛡️ Tentando com stealth level {stealth_level}...")
            
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            
            driver = setup_driver(stealth_level=stealth_level)
            
            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                log(f"\n🔄 Tentativa {attempt}/{max_attempts} de buscar ofertas...")
                offers = fetch_offers(driver)
                if offers:
                    break
                if attempt < max_attempts:
                    wait_time = random.randint(15, 30)
                    log(f"⏳ Aguardando {wait_time}s antes da próxima tentativa...")
                    time.sleep(wait_time)
            
            if offers:
                break
            else:
                log(f"⚠️ Stealth level {stealth_level} não funcionou, tentando próximo nível...")
                
        except Exception as e:
            log(f"⚠️ Erro com stealth level {stealth_level}: {e}")
            continue
    
    if not offers:
        log("\n📭 Nenhuma oferta encontrada após todas as tentativas.")
        return
    
    new_offers = [o for o in offers if o["id"] not in seen_ids]
    if not new_offers:
        log("\n📭 Nenhuma oferta nova.")
        return
    
    log(f"\n🎉 {len(new_offers)} nova(s) oferta(s) encontrada(s)!")
    
    processed_ids = set(seen_ids)
    success_count = 0
    
    for idx, offer in enumerate(new_offers, start=1):
        log(f"\n{'=' * 50}\n📦 Oferta {idx}/{len(new_offers)}\n{'=' * 50}")
        
        try:
            if not offer.get("imagem_url"):
                log("⚠️ Oferta sem imagem. Marcando como vista.")
                processed_ids.add(offer["id"])
                continue
            
            img_path = download_image(offer["imagem_url"])
            if not img_path:
                log("⚠️ Não foi possível baixar a imagem. Marcando como vista.")
                processed_ids.add(offer["id"])
                continue
            
            page_title, validity, full_description = process_offer(driver, offer)
            main_caption = build_caption(page_title, validity, offer["link"])
            
            if not main_caption:
                log("⚠️ Caption inválida. Marcando como vista.")
                processed_ids.add(offer["id"])
                Path(img_path).unlink(missing_ok=True)
                continue
            
            log("\n📤 Enviando oferta e comentário...")
            ok = send_offer_with_details(img_path, main_caption, full_description, offer["link"])
            if ok:
                success_count += 1
                log("✅ Oferta publicada com sucesso.")
            else:
                log("⚠️ Falha no envio da oferta.")
            
            processed_ids.add(offer["id"])
            Path(img_path).unlink(missing_ok=True)
            
            if idx < len(new_offers):
                time.sleep(random.uniform(3, 6))
        
        except Exception as e:
            log(f"⚠️ Erro inesperado nesta oferta: {e}")
            processed_ids.add(offer["id"])
    
    history["ids"] = list(processed_ids)[-MAX_HISTORY_SIZE:]
    save_history(history)
    
    log(f"\n🏁 Finalizado. Sucessos: {success_count}/{len(new_offers)}")

if __name__ == "__main__":
    main()
