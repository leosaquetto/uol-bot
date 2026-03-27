# bot_leouol.py
# consumer do pending_offers.json
# envia no formato de TEXTO + preview do link
# sem upload de imagem e sem comentário automático separado

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_HISTORY_SIZE = 500
MAX_MESSAGE_LENGTH = 4096

# quanto da descrição vai no post principal
MAX_DESCRIPTION_IN_POST = 2600


# ==============================================
# utilitários
# ==============================================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def normalize_spaces(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t]+", " ", text).strip()


def clean_description(text: Optional[str]) -> str:
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # remove excesso de espaços sem destruir completamente os parágrafos
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]

    cleaned_lines: List[str] = []
    previous_blank = False

    for line in lines:
        if not line:
            if not previous_blank:
                cleaned_lines.append("")
            previous_blank = True
            continue

        previous_blank = False
        cleaned_lines.append(line)

    text = "\n".join(cleaned_lines).strip()

    # corta o lixo final do formulário, se aparecer
    markers = [
        "enviar cupons por e-mail",
        "preencha os campos abaixo",
        "e-mail",
        "mensagem",
        "enviar",
    ]

    lower_text = text.lower()
    for marker in markers:
        idx = lower_text.find(marker)
        if idx != -1:
            text = text[:idx].strip()
            lower_text = text.lower()
            break

    return text.strip()


def escape_html(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def get_offer_id(link: str) -> str:
    try:
        clean = link.split("?")[0].rstrip("/")
        return clean.split("/")[-1]
    except Exception:
        return link


def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - len(suffix)].rstrip() + suffix


# ==============================================
# histórico / pending
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

        normalized = []
        for item in ids:
            normalized.append(get_offer_id(str(item)))

        normalized = list(dict.fromkeys(normalized))[-MAX_HISTORY_SIZE:]
        return {"ids": normalized}
    except Exception:
        return {"ids": []}


def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", [])
        ids = [get_offer_id(str(x)) for x in ids]
        ids = list(dict.fromkeys(ids))[-MAX_HISTORY_SIZE:]

        Path(HISTORY_FILE).write_text(
            json.dumps({"ids": ids}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"✅ histórico salvo: {len(ids)} ids")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar histórico: {e}")
        return False


def load_pending() -> Dict:
    path = Path(PENDING_FILE)
    if not path.exists():
        return {"last_update": None, "offers": []}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        offers = data.get("offers", [])
        if not isinstance(offers, list):
            offers = []
        return {
            "last_update": data.get("last_update"),
            "offers": offers,
        }
    except Exception:
        return {"last_update": None, "offers": []}


def save_pending(offers: List[Dict]) -> bool:
    try:
        payload = {
            "last_update": datetime.now().isoformat(),
            "offers": offers,
        }
        Path(PENDING_FILE).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"✅ pending salvo: {len(offers)} ofertas")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar pending: {e}")
        return False


# ==============================================
# telegram
# ==============================================
def build_channel_text(
    title: str,
    validity: Optional[str],
    description: str,
    link: str,
) -> str:
    title = normalize_spaces(title) or "oferta"
    validity = normalize_spaces(validity or "")
    description = clean_description(description)

    if description:
        description = truncate_text(description, MAX_DESCRIPTION_IN_POST)
    else:
        description = "descrição não disponível."

    parts: List[str] = [f"<b>{escape_html(title)}</b>"]

    if validity:
        parts.append(f"<i>{escape_html(validity)}</i>")

    parts.append(escape_html(description))

    # manter o link clicável e com preview
    parts.append(f"<a href=\"{escape_html(link)}\">abrir oferta</a>")

    text = "\n\n".join(parts)
    return truncate_text(text, MAX_MESSAGE_LENGTH)


def send_message_to_channel(text: str) -> Optional[int]:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        response = requests.post(url, data=data, timeout=30)

        if not response.ok:
            log(f"❌ telegram sendMessage falhou: {response.status_code} | {response.text}")
            return None

        payload = response.json()
        return payload.get("result", {}).get("message_id")
    except Exception as e:
        log(f"❌ erro ao enviar mensagem ao telegram: {e}")
        return None


# ==============================================
# consumer principal
# ==============================================
def run_consumer() -> None:
    log("=" * 60)
    log("🤖 bot leouol - consumer do pending")
    log("=" * 60)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log("❌ TELEGRAM_TOKEN e TELEGRAM_CHAT_ID são obrigatórios")
        return

    history = load_history()
    seen_ids = set(history.get("ids", []))

    pending_data = load_pending()
    offers = pending_data.get("offers", [])

    log(f"📦 pending atual: {len(offers)} ofertas")

    if not offers:
        log("📭 nada para enviar")
        return

    success_count = 0
    failed_ids = []
    processed_ids = set(seen_ids)

    for index, offer in enumerate(offers, start=1):
        offer_id = get_offer_id(str(offer.get("id") or offer.get("link", "")))
        title = offer.get("title") or offer.get("preview_title") or "oferta"
        validity = offer.get("validity")
        description = offer.get("description") or ""
        link = offer.get("link") or offer.get("original_link") or ""

        log("-" * 60)
        log(f"📌 oferta {index}/{len(offers)}")
        log(f"   id: {offer_id}")
        log(f"   título: {title[:80]}")

        if not link:
            log("   ⚠️ sem link, pulando")
            processed_ids.add(offer_id)
            continue

        if offer_id in seen_ids:
            log("   ⏭️ já estava no histórico, pulando")
            processed_ids.add(offer_id)
            continue

        message_text = build_channel_text(title, validity, description, link)
        message_id = send_message_to_channel(message_text)

        if message_id:
            log(f"   ✅ enviado com sucesso (message_id: {message_id})")
            processed_ids.add(offer_id)
            success_count += 1
        else:
            log("   ❌ falha no envio")
            failed_ids.append(offer_id)

        time.sleep(2)

    history["ids"] = list(processed_ids)
    save_history(history)

    remaining_offers = []
    failed_set = set(failed_ids)
    for offer in offers:
        offer_id = get_offer_id(str(offer.get("id") or offer.get("link", "")))
        if offer_id in failed_set:
            remaining_offers.append(offer)

    save_pending(remaining_offers)

    log("-" * 60)
    log(f"✅ fim. {success_count}/{len(offers)} ofertas enviadas")


# ==============================================
# entry point
# ==============================================
if __name__ == "__main__":
    run_consumer()
