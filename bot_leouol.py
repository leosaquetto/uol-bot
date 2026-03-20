# ------------------------------
# BOT LEOUOL - Clube UOL Ofertas
# Modo PRO: Evasão Avançada (Non-Headless + Randomização)
# ------------------------------

import json
import os
import random
import re
import shutil
import subprocess
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
# CONFIG
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CANAL_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIOS_ID = os.environ.get("GRUPO_COMENTARIO_ID", "-1003802235343")

TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"
TMP_DIR = Path("/tmp")
DEBUG_DIR = Path("debug")

MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096
MAX_OFFERS_PER_RUN = 8
MAX_HISTORY_SIZE = 200

LIST_WAIT_SECONDS = 15
DETAIL_WAIT_SECONDS = 12
PAGE_LOAD_TIMEOUT = 30

# User-Agents modernos e variados para dificultar o fingerprint
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

# ==============================================
# LOG / DEBUG
# ==============================================
def log(msg: str) -> None:
    print(msg, flush=True)

def ensure_debug_dir() -> None:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def save_debug_text(name: str, text: str) -> None:
    try:
        ensure_debug_dir()
        path = DEBUG_DIR / f"{timestamp()}_{name}.txt"
        path.write_text(text, encoding="utf-8")
        log(f"📝 Texto salvo em {path}")
    except Exception as e:
        log(f"⚠️ Falha ao salvar texto de debug: {e}")

def save_page_text_debug(driver, name: str) -> None:
    try:
        title = driver.title or ""
        body_text = driver.find_element(By.TAG_NAME, "body").text[:4000]
        save_debug_text(name, f"TITLE:\n{title}\n\nBODY:\n{body_text}")
    except Exception as e:
        log(f"⚠️ Falha ao capturar texto da página: {e}")

# ==============================================
# UTIL
# ==============================================
def ensure_env() -> None:
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not CANAL_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Variáveis ausentes: {', '.join(missing)}")

def human_delay(min_s: float = 1.0, max_s: float = 2.5) -> None:
    time.sleep(random.uniform(min_s, max_s))

def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
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
        return urlunparse((scheme, netloc, path.rstrip("/"), "", "", ""))
    except Exception:
        return link

def get_chrome_major_version() -> int:
    for binary in ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium"]:
        if shutil.which(binary):
            try:
                out = subprocess.check_output([binary, "--version"], text=True).strip()
                match = re.search(r"(\d+)\.", out)
                if match:
                    version = int(match.group(1))
                    log(f"✅ Chrome detectado: {binary} versão {version}")
                    return version
            except Exception:
                continue
    log("⚠️ Não foi possível detectar Chrome.")
    return 122 # Fallback seguro

def is_privacy_error_page(driver) -> bool:
    try:
        title = (driver.title or "").lower()
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        return (
            "privacy error" in title
            or "your connection is not private" in body
            or "net::err_cert_authority_invalid" in body
        )
    except Exception:
        return False

def is_human_verification_page(driver) -> bool:
    try:
        title = (driver.title or "").lower()
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        signals = [
            "human verification",
            "vamos confirmar que você é humano",
            "conclua a verificação de segurança antes de continuar",
        ]
        return any(s in title or s in body for s in signals)
    except Exception:
        return False

def is_possible_block_page(driver) -> bool:
    try:
        title = (driver.title or "").lower()
        body = driver.find_element(By.TAG_NAME, "body").text.lower()

        if is_privacy_error_page(driver):
            return False

        signals = [
            "cloudflare",
            "access denied",
            "forbidden",
            "attention required",
            "verifique se você é humano",
            "captcha",
            "just a moment",
            "temporarily blocked",
            "human verification",
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
        return {"ids": [str(x) for x in ids][-MAX_HISTORY_SIZE:]}
    except Exception as e:
        log(f"⚠️ Erro ao carregar histórico: {e}")
        return {"ids": []}

def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = [str(x) for x in history.get("ids", [])][-MAX_HISTORY_SIZE:]
        tmp = Path(f"{HISTORY_FILE}.tmp")
        final = Path(HISTORY_FILE)
        tmp.write_text(json.dumps({"ids": ids}, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(final)
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except Exception as e:
        log(f"⚠️ Erro ao salvar histórico: {e}")
        return False

# ==============================================
# DRIVER - Foco em Evasão Pro
# ==============================================
def setup_driver():
    chrome_major = get_chrome_major_version()

    options = uc.ChromeOptions()
    
    # Técnicas de Evasão
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    
    # Randomizando o tamanho da janela para evitar fingerprinting de viewport
    width = random.randint(1300, 1920)
    height = random.randint(700, 1080)
    options.add_argument(f"--window-size={width},{height}")
    
    options.add_argument("--lang=pt-BR")
    options.add_argument("--accept-lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    # Randomiza o User-Agent
    ua = random.choice(USER_AGENTS)
    options.add_argument(f"--user-agent={ua}")

    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors=yes")
    options.add_argument("--allow-running-insecure-content")

    # Inicialização do UC. Notar que Headless está FALSE.
    # O Xvfb no workflow vai absorver a janela visual.
    driver = uc.Chrome(
        options=options,
        headless=False, 
        version_main=chrome_major,
    )
    
    # Injeta JS para esconder variáveis que gritam "SOU UM ROBÔ"
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            window.navigator.chrome = {
                runtime: {},
            };
        """
    })

    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def try_bypass_privacy_error(driver) -> None:
    try:
        if not is_privacy_error_page(driver):
            return

        log("⚠️ Tela de certificado detectada. Tentando prosseguir...")

        try:
            driver.execute_script("document.getElementById('details-button')?.click();")
            time.sleep(1)
        except Exception:
            pass

        try:
            driver.execute_script("document.getElementById('proceed-link')?.click();")
            time.sleep(2)
        except Exception:
            pass
    except Exception as e:
        log(f"⚠️ Erro ao tentar contornar tela de certificado: {e}")

def safe_get(driver, url: str, label: str) -> bool:
    try:
        driver.get(url)
        human_delay(1.5, 3.0) # Espera humana mais natural

        if is_privacy_error_page(driver):
            try_bypass_privacy_error(driver)
            human_delay(1.0, 2.0)

        return True

    except Exception as e:
        log(f"⚠️ Erro ao abrir {url}: {e}")
        save_debug_text(f"{label}_error", str(e))
        return False

# ==============================================
# EXTRAÇÃO
# ==============================================
def extract_page_title(driver, fallback: str = "") -> str:
    try:
        for selector in ["h1", ".titulo"]:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                text = normalize_spaces(elems[0].text)
                if text:
                    return re.sub(r"\s*[–—-]\s*Clube UOL\s*$", "", text, flags=re.IGNORECASE)
        title = normalize_spaces(driver.title)
        if title:
            return re.sub(r"\s*[–—-]\s*Clube UOL\s*$", "", title, flags=re.IGNORECASE)
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
        ]
        for pattern in patterns:
            match = re.search(pattern, body)
            if match:
                return normalize_spaces(match.group(0))
    except Exception as e:
        log(f"⚠️ Erro ao extrair validade: {e}")
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

        for selector in [".partner-description", "[class*='parceiro'] p", ".about-partner"]:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elems:
                text = elem.text.strip()
                if len(text) > 20:
                    add_block("🏢 <b>Sobre o parceiro:</b>", text)
                    break
            if parts:
                break

        for selector in [".benefit-description", "[class*='beneficio'] p", ".offer-description"]:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elems:
                text = elem.text.strip()
                if len(text) > 15:
                    add_block("🎁 <b>Benefício:</b>", text)
                    break

        validity = extract_validity(driver)
        if validity:
            add_block("⏳ <b>Validade:</b>", validity)

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
        log(f"⚠️ Erro ao extrair descrição completa: {e}")
        return "Descrição detalhada não disponível."

# ==============================================
# IMAGEM / CAPTION
# ==============================================
def download_image(img_url: str) -> Optional[str]:
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": "https://clube.uol.com.br/",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        response = HTTP.get(img_url, headers=headers, timeout=12)
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
        parts.append(f"📅 {escape_html(normalize_spaces(validity))}")
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
        log(f"⚠️ Erro Telegram ({method}): {e}")
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
        if updates:
            last_id = updates[-1]["update_id"]
            telegram_api("getUpdates", data={"offset": last_id + 1}, timeout=10)
    except Exception:
        pass

def send_description_comment(full_description: str, link: str, channel_message_id: int) -> bool:
    log("⏳ Aguardando thread do grupo...")
    group_message_id = None
    offset = None

    for _ in range(6):
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
                forward_origin = msg.get("forward_origin", {}) or {}
                origin_message_id = forward_origin.get("message_id")
                forward_from_msg_id = msg.get("forward_from_message_id")

                if origin_message_id == channel_message_id or forward_from_msg_id == channel_message_id:
                    group_message_id = msg.get("message_id")
                    break

        if group_message_id:
            break

    if group_message_id:
        log(f"✅ Thread encontrada no grupo (ID: {group_message_id})")
    else:
        log("⚠️ Thread não encontrada a tempo. Comentário será enviado sem reply.")

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
        return True

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
            log("❌ Falha ao enviar foto. Tentando só texto...")
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
# OFERTAS
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
    log("🌐 Abrindo listagem de ofertas...")
    if not safe_get(driver, TARGET_URL, "listagem"):
        return []

    if is_privacy_error_page(driver):
        log("⚠️ Ainda ficou preso na tela de certificado.")
        save_page_text_debug(driver, "listagem_privacy_error_info")
        return []

    if is_human_verification_page(driver):
        log("⚠️ Human Verification detectado. Encerrando tentativa.")
        save_page_text_debug(driver, "listagem_human_verification_info")
        return []

    try:
        WebDriverWait(driver, LIST_WAIT_SECONDS).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.beneficio"))
        )
    except TimeoutException:
        log("⚠️ Listagem não carregou a tempo.")
        save_page_text_debug(driver, "listagem_timeout_info")
        return []

    if is_possible_block_page(driver):
        log("⚠️ Página parece bloqueio/challenge.")
        save_page_text_debug(driver, "listagem_block_info")
        return []

    # Scroll para simular ação humana
    driver.execute_script("window.scrollBy(0, 500);")
    human_delay(1.0, 1.5)
    driver.execute_script("window.scrollBy(0, 500);")
    human_delay(1.0, 1.5)

    containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
    if not containers:
        save_page_text_debug(driver, "listagem_sem_cards_info")
        return []

    offers = []
    seen_run_ids = set()

    for container in containers[: MAX_OFFERS_PER_RUN * 2]:
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

            offers.append(
                {
                    "id": offer_id,
                    "preview_title": preview_title,
                    "link": link,
                    "imagem_url": img_url or "",
                }
            )
        except Exception:
            continue

    return offers[:MAX_OFFERS_PER_RUN]

def process_offer(driver, offer: Dict[str, str]) -> Tuple[str, Optional[str], str]:
    log(f"🔍 Acessando página: {offer['preview_title'][:60]}...")

    if not safe_get(driver, offer["link"], "detalhe"):
        return offer["preview_title"], None, "Descrição não disponível devido a erro de navegação."

    if is_privacy_error_page(driver):
        save_page_text_debug(driver, "detalhe_privacy_error_info")
        return offer["preview_title"], None, "Descrição não disponível."

    if is_human_verification_page(driver):
        save_page_text_debug(driver, "detalhe_human_verification_info")
        return offer["preview_title"], None, "Descrição não disponível."

    human_delay(1.5, 2.5)

    try:
        WebDriverWait(driver, DETAIL_WAIT_SECONDS).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1, p, .partner-description, .benefit-description"))
        )
    except TimeoutException:
        save_page_text_debug(driver, "detalhe_timeout_info")

    if is_possible_block_page(driver):
        save_page_text_debug(driver, "detalhe_block_info")
        return offer["preview_title"], None, "Descrição não disponível."

    page_title = extract_page_title(driver, fallback=offer["preview_title"])
    validity = extract_validity(driver)
    full_description = extract_full_description(driver)

    return page_title, validity, full_description

# ==============================================
# MAIN
# ==============================================
def main():
    log("=" * 72)
    log("🤖 BOT LEOUOL - Clube UOL Ofertas")
    log("📢 Modo PRO Evasão")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 72)

    ensure_env()
    ensure_debug_dir()

    history = load_history()
    seen_ids = set(history.get("ids", []))

    driver = None

    try:
        driver = setup_driver()

        log("\n🔄 Tentativa única de buscar ofertas...")
        offers = fetch_offers(driver)

        if not offers:
            log("\n📭 Nenhuma oferta encontrada (Pode ser bloqueio).")
            return

        new_offers = [o for o in offers if o["id"] not in seen_ids]
        if not new_offers:
            log("\n📭 Nenhuma oferta nova.")
            return

        log(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")

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
                    log("⚠️ Não foi possível baixar imagem. Marcando como vista.")
                    processed_ids.add(offer["id"])
                    continue

                page_title, validity, full_description = process_offer(driver, offer)
                main_caption = build_caption(page_title, validity, offer["link"])

                if not main_caption:
                    Path(img_path).unlink(missing_ok=True)
                    processed_ids.add(offer["id"])
                    continue

                log("📤 Enviando oferta e comentário...")
                ok = send_offer_with_details(img_path, main_caption, full_description, offer["link"])
                if ok:
                    success_count += 1
                    processed_ids.add(offer["id"])
                    
                # Deleta a imagem logo após usar para economizar espaço
                Path(img_path).unlink(missing_ok=True)
                
            except Exception as e:
                log(f"❌ Erro processando oferta {offer['id']}: {e}")

        # Salva histórico apenas das que foram enviadas com sucesso
        history["ids"] = list(processed_ids)
        save_history(history)
        
        log(f"\n✅ Fim. {success_count} ofertas processadas com sucesso.")

    except Exception as e:
        log(f"💥 Erro fatal: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

if __name__ == "__main__":
    main()
