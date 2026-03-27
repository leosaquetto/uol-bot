# bot_leouol.py
# consumidor do pending_offers.json para envio ao telegram

import json
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID", "").strip()

PENDING_FILE = "pending_offers.json"
HISTORY_FILE = "historico_leouol.json"

MAX_OFFERS_PER_RUN = 10
MAX_HISTORY_SIZE = 500
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_offer_id(link: str) -> str:
    try:
        parsed = urlparse(link)
        path = parsed.path.rstrip("/")
        parts = path.split("/")
        return parts[-1] if parts and parts[-1] else link
    except Exception:
        return str(link).split("?")[0].rstrip("/").split("/")[-1]


def clean_text(text: str) -> str:
    return " ".join(str(text or "").split()).strip()


def escape_html(text: str) -> str:
    return (
        str(text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    text = str(text or "")
    if len(text) <= max_len:
        return text
    return text[: max_len - len(suffix)] + suffix


def read_json_file(path_str: str, default):
    path = Path(path_str)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"⚠️ erro lendo {path_str}: {e}")
        return default


def write_json_file(path_str: str, payload) -> bool:
    try:
        Path(path_str).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        return True
    except Exception as e:
        log(f"❌ erro escrevendo {path_str}: {e}")
        return False


def load_history():
    payload = read_json_file(HISTORY_FILE, {"ids": []})

    if isinstance(payload, list):
        original_urls = [x for x in payload if x]
    elif isinstance(payload, dict):
        original_urls = payload.get("ids", []) or []
    else:
        original_urls = []

    return {"original_urls": [u for u in original_urls if u]}


def save_history(history) -> bool:
    try:
        unique = {}
        for url in history.get("original_urls", []):
            if not url:
                continue
            unique[get_offer_id(url)] = url

        ids = list(unique.values())[-MAX_HISTORY_SIZE:]
        ok = write_json_file(HISTORY_FILE, {"ids": ids})
        if ok:
            log(f"✅ histórico salvo: {len(ids)} ids")
        return ok
    except Exception as e:
        log(f"❌ erro em save_history: {e}")
        return False


def append_history_from_offers(history, offers):
    original_urls = history.get("original_urls", []) or []
    for offer in offers:
        url = offer.get("original_link") or offer.get("link")
        if url:
            original_urls.append(url)
    history["original_urls"] = original_urls
    return history


def load_pending():
    payload = read_json_file(PENDING_FILE, {"last_update": None, "offers": []})

    if not isinstance(payload, dict):
        payload = {"last_update": None, "offers": []}

    offers = payload.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    clean_offers = []
    for offer in offers:
        if isinstance(offer, dict) and (offer.get("link") or offer.get("original_link")):
            clean_offers.append(offer)

    payload["offers"] = clean_offers
    return payload


def save_pending(payload) -> bool:
    payload["last_update"] = datetime.utcnow().isoformat() + "Z"
    ok = write_json_file(PENDING_FILE, payload)
    if ok:
        log(f"✅ pending salvo: {len(payload.get('offers', []))} ofertas")
    return ok


def telegram_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def build_caption(offer: dict) -> str:
    title = escape_html(offer.get("title") or offer.get("preview_title") or "oferta")
    validity = clean_text(offer.get("validity") or "")
    description = clean_text(offer.get("description") or "")
    link = offer.get("link") or offer.get("original_link") or ""

    parts = [f"<b>{title}</b>"]

    if validity:
        parts.append(f"<i>{escape_html(validity)}</i>")

    if description:
        parts.append(escape_html(truncate_text(description, 700)))

    if link:
        parts.append(f'<a href="{escape_html(link)}">abrir oferta</a>')

    return truncate_text("\n\n".join(parts), MAX_CAPTION_LENGTH)


def build_comment_text(offer: dict) -> str:
    title = escape_html(offer.get("title") or offer.get("preview_title") or "oferta")
    description = clean_text(offer.get("description") or "")
    link = offer.get("link") or offer.get("original_link") or ""

    body = f"<b>{title}</b>\n\n{escape_html(truncate_text(description, 3500))}"
    if link:
        body += f'\n\n<a href="{escape_html(link)}">abrir oferta</a>'

    return truncate_text(body, MAX_COMMENT_LENGTH)


def send_main_message(offer: dict):
    caption = build_caption(offer)
    img_url = offer.get("img_url") or ""
    link = offer.get("link") or offer.get("original_link") or ""

    if img_url:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "photo": img_url,
            "caption": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        response = requests.post(telegram_api_url("sendPhoto"), data=payload, timeout=30)
        data = response.json()
        if data.get("ok"):
            return True, data
        log(f"⚠️ sendPhoto falhou para {link}: {data}")

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": caption,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    response = requests.post(telegram_api_url("sendMessage"), data=payload, timeout=30)
    data = response.json()

    if data.get("ok"):
        return True, data

    log(f"❌ sendMessage falhou para {link}: {data}")
    return False, data


def send_comment_if_configured(parent_message_data, offer: dict):
    if not GRUPO_COMENTARIO_ID:
        return True

    try:
        message = parent_message_data.get("result", {})
        message_id = message.get("message_id")
        if not message_id:
            return True

        payload = {
            "chat_id": GRUPO_COMENTARIO_ID,
            "text": build_comment_text(offer),
            "parse_mode": "HTML",
            "reply_to_message_id": message_id
        }

        response = requests.post(telegram_api_url("sendMessage"), data=payload, timeout=30)
        data = response.json()

        if not data.get("ok"):
            log(f"⚠️ comentário não enviado: {data}")

        return True
    except Exception as e:
        log(f"⚠️ erro no comentário: {e}")
        return True


def validate_env() -> bool:
    if not TELEGRAM_TOKEN:
        log("❌ TELEGRAM_TOKEN não configurado")
        return False
    if not TELEGRAM_CHAT_ID:
        log("❌ TELEGRAM_CHAT_ID não configurado")
        return False
    return True


def dedupe_offers(offers: list) -> list:
    unique = {}
    for offer in offers:
        link = offer.get("link") or offer.get("original_link")
        if not link:
            continue
        unique[get_offer_id(link)] = offer
    return list(unique.values())


def main():
    log("=" * 60)
    log("🤖 bot leouol - consumer do pending")
    log("=" * 60)

    if not validate_env():
        raise SystemExit(1)

    pending = load_pending()
    history = load_history()

    offers = dedupe_offers(pending.get("offers", []))
    log(f"📦 pending atual: {len(offers)} ofertas")

    if not offers:
        log("📭 nada para enviar")
        return

    batch = offers[:MAX_OFFERS_PER_RUN]
    remaining = offers[MAX_OFFERS_PER_RUN:]

    sent_success = []
    sent_failed = []

    for index, offer in enumerate(batch, start=1):
        link = offer.get("link") or offer.get("original_link") or ""
        slug = get_offer_id(link)
        log(f"📌 {index}/{len(batch)} | {slug}")

        try:
            ok, parent_message_data = send_main_message(offer)
            if ok:
                send_comment_if_configured(parent_message_data, offer)
                sent_success.append(offer)
                log(f"✅ enviada: {slug}")
            else:
                sent_failed.append(offer)
                log(f"❌ falhou: {slug}")
        except Exception as e:
            sent_failed.append(offer)
            log(f"❌ exceção ao enviar {slug}: {e}")

        time.sleep(2)

    pending["offers"] = dedupe_offers(sent_failed + remaining)

    if not save_pending(pending):
        raise SystemExit(1)

    if sent_success:
        history = append_history_from_offers(history, sent_success)
        if not save_history(history):
            raise SystemExit(1)

    log("-" * 60)
    log(f"✅ enviadas com sucesso: {len(sent_success)}")
    log(f"⚠️ falharam: {len(sent_failed)}")
    log(f"📦 restantes no pending: {len(pending['offers'])}")
    log("🏁 fim")


if __name__ == "__main__":
    main()    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def get_offer_id(link: str) -> str:
    try:
      parsed = urlparse(link)
      path = parsed.path.rstrip("/")
      parts = path.split("/")
      return parts[-1] if parts and parts[-1] else link
    except Exception:
      return str(link).split("?")[0].rstrip("/").split("/")[-1]

def clean_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(str(text).split()).strip()

def escape_html(text: str) -> str:
    if not text:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    text = str(text or "")
    if len(text) <= max_len:
        return text
    return text[: max_len - len(suffix)] + suffix

def read_json_file(path_str: str, default):
    path = Path(path_str)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"⚠️ erro lendo {path_str}: {e}")
        return default

def write_json_file(path_str: str, payload) -> bool:
    try:
        Path(path_str).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        return True
    except Exception as e:
        log(f"❌ erro escrevendo {path_str}: {e}")
        return False

# ==============================================
# histórico
# ==============================================
def load_history():
    payload = read_json_file(HISTORY_FILE, {"ids": []})

    if isinstance(payload, list):
        original_urls = [x for x in payload if x]
    elif isinstance(payload, dict):
        original_urls = payload.get("ids", []) or []
    else:
        original_urls = []

    original_urls = [u for u in original_urls if u]
    return {"original_urls": original_urls}

def save_history(history) -> bool:
    try:
        original_urls = history.get("original_urls", []) or []
        unique = {}

        for url in original_urls:
            if not url:
                continue
            unique[get_offer_id(url)] = url

        ids = list(unique.values())[-MAX_HISTORY_SIZE:]
        ok = write_json_file(HISTORY_FILE, {"ids": ids})
        if ok:
            log(f"✅ histórico salvo: {len(ids)} ids")
        return ok
    except Exception as e:
        log(f"❌ erro no save_history: {e}")
        return False

def append_history_from_offers(history, offers):
    original_urls = history.get("original_urls", []) or []
    for offer in offers:
        url = offer.get("original_link") or offer.get("link")
        if url:
            original_urls.append(url)
    history["original_urls"] = original_urls
    return history

# ==============================================
# pending
# ==============================================
def load_pending():
    payload = read_json_file(PENDING_FILE, {"last_update": None, "offers": []})

    if not isinstance(payload, dict):
        payload = {"last_update": None, "offers": []}

    offers = payload.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    clean_offers = []
    for offer in offers:
        if isinstance(offer, dict) and offer.get("link"):
            clean_offers.append(offer)

    payload["offers"] = clean_offers
    return payload

def save_pending(payload) -> bool:
    payload["last_update"] = datetime.utcnow().isoformat() + "Z"
    ok = write_json_file(PENDING_FILE, payload)
    if ok:
        log(f"✅ pending salvo: {len(payload.get('offers', []))} ofertas")
    return ok

# ==============================================
# telegram
# ==============================================
def telegram_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

def build_caption(offer: dict) -> str:
    title = escape_html(offer.get("title") or offer.get("preview_title") or "oferta")
    validity = clean_text(offer.get("validity") or "")
    description = clean_text(offer.get("description") or "")
    link = offer.get("link") or offer.get("original_link") or ""

    parts = [f"<b>{title}</b>"]

    if validity:
        parts.append(f"<i>{escape_html(validity)}</i>")

    if description:
        parts.append(escape_html(truncate_text(description, 700)))

    if link:
        parts.append(f'<a href="{escape_html(link)}">abrir oferta</a>')

    caption = "\n\n".join(parts)
    return truncate_text(caption, MAX_CAPTION_LENGTH)

def build_comment_text(offer: dict) -> str:
    title = escape_html(offer.get("title") or offer.get("preview_title") or "oferta")
    description = clean_text(offer.get("description") or "")
    link = offer.get("link") or offer.get("original_link") or ""

    body = f"<b>{title}</b>\n\n{escape_html(truncate_text(description, 3500))}"
    if link:
        body += f'\n\n<a href="{escape_html(link)}">abrir oferta</a>'

    return truncate_text(body, MAX_COMMENT_LENGTH)

def send_photo_message(offer: dict):
    caption = build_caption(offer)
    img_url = offer.get("img_url") or ""
    link = offer.get("link") or offer.get("original_link") or ""

    if img_url:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "photo": img_url,
            "caption": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        response = requests.post(
            telegram_api_url("sendPhoto"),
            data=payload,
            timeout=30,
        )
        data = response.json()
        if data.get("ok"):
            return True, data
        log(f"⚠️ sendPhoto falhou para {link}: {data}")
    else:
        log(f"⚠️ sem imagem para {link}")

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": caption,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    response = requests.post(
        telegram_api_url("sendMessage"),
        data=payload,
        timeout=30,
    )
    data = response.json()
    if data.get("ok"):
        return True, data

    log(f"❌ sendMessage falhou para {link}: {data}")
    return False, data

def send_comment_reply_if_configured(parent_message_data, offer: dict):
    if not GRUPO_COMENTARIOS_ID:
        return True

    try:
        message = parent_message_data.get("result", {})
        message_id = message.get("message_id")
        if not message_id:
            return True

        payload = {
            "chat_id": GRUPO_COMENTARIOS_ID,
            "text": build_comment_text(offer),
            "parse_mode": "HTML",
        }

        if isinstance(message.get("is_topic_message"), bool):
            payload["reply_to_message_id"] = message_id

        response = requests.post(
            telegram_api_url("sendMessage"),
            data=payload,
            timeout=30,
        )
        data = response.json()

        if not data.get("ok"):
            log(f"⚠️ comentário não enviado: {data}")

        return True
    except Exception as e:
        log(f"⚠️ erro no comentário: {e}")
        return True

# ==============================================
# processamento principal
# ==============================================
def validate_env() -> bool:
    if not TELEGRAM_TOKEN:
        log("❌ TELEGRAM_TOKEN não configurado")
        return False
    if not TELEGRAM_CHAT_ID:
        log("❌ TELEGRAM_CHAT_ID não configurado")
        return False
    return True

def dedupe_offers(offers: list) -> list:
    unique = {}
    for offer in offers:
        link = offer.get("link") or offer.get("original_link")
        if not link:
            continue
        unique[get_offer_id(link)] = offer
    return list(unique.values())

def main():
    log("=" * 60)
    log("🤖 bot leouol - consumidor do pending")
    log("=" * 60)

    if not validate_env():
        raise SystemExit(1)

    pending = load_pending()
    history = load_history()

    offers = dedupe_offers(pending.get("offers", []))
    total_pending = len(offers)

    log(f"📦 pending atual: {total_pending} ofertas")

    if total_pending == 0:
        log("📭 nada para enviar")
        return

    batch = offers[:MAX_OFFERS_PER_RUN]
    remaining = offers[MAX_OFFERS_PER_RUN:]

    log(f"🚀 enviando lote de {len(batch)} oferta(s)")

    sent_success = []
    sent_failed = []

    for index, offer in enumerate(batch, start=1):
        link = offer.get("link") or offer.get("original_link") or ""
        slug = get_offer_id(link)
        log(f"📌 {index}/{len(batch)} | {slug}")

        try:
            ok, parent_message_data = send_photo_message(offer)
            if ok:
                send_comment_reply_if_configured(parent_message_data, offer)
                sent_success.append(offer)
                log(f"✅ enviada: {slug}")
            else:
                sent_failed.append(offer)
                log(f"❌ falhou: {slug}")
        except Exception as e:
            sent_failed.append(offer)
            log(f"❌ exceção ao enviar {slug}: {e}")

        time.sleep(2)

    # reconstroi pending: falhas do lote + resto que ainda não foi processado
    new_pending_offers = sent_failed + remaining
    pending["offers"] = dedupe_offers(new_pending_offers)

    if not save_pending(pending):
        raise SystemExit(1)

    if sent_success:
        history = append_history_from_offers(history, sent_success)
        save_history(history)

    log("-" * 60)
    log(f"✅ enviadas com sucesso: {len(sent_success)}")
    log(f"⚠️ falharam: {len(sent_failed)}")
    log(f"📦 sobraram no pending: {len(pending['offers'])}")
    log("🏁 fim")

if __name__ == "__main__":
    main()
