# bot_leouol.py
# consumer do pending_offers.json + envio para telegram
# versão corrigida:
# - processa tudo que estiver no pending
# - mantém comentário no grupo vinculado ao canal
# - normaliza ids do histórico
# - limpa pending apenas do que foi enviado com sucesso
# - compatível com python bot_leouol.py --pending

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional

import requests

# ==============================================
# configurações
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_HISTORY_SIZE = 500
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096

REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# ==============================================
# utilidades
# ==============================================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def log_separator() -> None:
    print("-" * 60, flush=True)


def normalize_spaces(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def clean_multiline_text(text: Optional[str]) -> str:
    if not text:
        return ""

    text = str(text)
    text = unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"•\s*\n\s*", "• ", text)
    text = re.sub(r"\n\s*•\s*", "\n• ", text)
    text = text.strip()

    lixo = [
        "Enviar cupons por e-mail",
        "Preencha os campos abaixo",
        "E-mail\n\nMensagem\n\nEnviar",
    ]
    for marker in lixo:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx].rstrip()

    return text.strip()


def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len:
        return text
    if max_len <= len(suffix):
        return suffix[:max_len]
    return text[: max_len - len(suffix)] + suffix


def escape_html(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def safe_json_load(path: Path, fallback):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


# ==============================================
# normalização
# ==============================================
def slugify_piece(text: str) -> str:
    text = unescape(text or "").lower().strip()

    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)

    text = re.sub(r"[^a-z0-9\-_/]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    text = text.strip("-/")
    return text


def get_offer_id(link: str) -> str:
    try:
        clean_link = str(link).split("?")[0].rstrip("/")
        return clean_link.split("/")[-1]
    except Exception:
        return str(link or "").strip()


def normalize_offer_key(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)

    raw = slugify_piece(raw)
    parts = [p for p in raw.split("-") if p]
    if not parts:
        return raw

    return "-".join(parts)


# ==============================================
# histórico
# ==============================================
def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": []}

    data = safe_json_load(path, {"ids": []})
    ids = data.get("ids", [])
    if not isinstance(ids, list):
        ids = []

    normalized = []
    seen = set()
    for item in ids:
        key = normalize_offer_key(str(item))
        if key and key not in seen:
            seen.add(key)
            normalized.append(key)

    return {"ids": normalized[-MAX_HISTORY_SIZE:]}


def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", [])
        if not isinstance(ids, list):
            ids = []

        cleaned = []
        seen = set()
        for item in ids:
            key = normalize_offer_key(str(item))
            if key and key not in seen:
                seen.add(key)
                cleaned.append(key)

        cleaned = cleaned[-MAX_HISTORY_SIZE:]

        Path(HISTORY_FILE).write_text(
            json.dumps({"ids": cleaned}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"✅ histórico salvo: {len(cleaned)} ids")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar histórico: {e}")
        return False


# ==============================================
# pending
# ==============================================
def load_pending() -> Dict:
    path = Path(PENDING_FILE)
    if not path.exists():
        return {"last_update": None, "offers": []}

    data = safe_json_load(path, {"last_update": None, "offers": []})
    offers = data.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    return {
        "last_update": data.get("last_update"),
        "offers": offers,
    }


def save_pending(offers: List[Dict]) -> bool:
    try:
        payload = {
            "last_update": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
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
def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def download_image(img_url: str, prefix: str = "leouol") -> Optional[str]:
    if not img_url:
        return None

    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(img_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        suffix = ".jpg"
        lower = img_url.lower()
        if ".png" in lower:
            suffix = ".png"
        elif ".webp" in lower:
            suffix = ".webp"
        elif ".jpeg" in lower:
            suffix = ".jpeg"

        path = f"/tmp/{prefix}_{int(time.time() * 1000)}{suffix}"
        Path(path).write_bytes(response.content)
        return path
    except Exception as e:
        log(f"   ⚠️ falha ao baixar imagem: {e}")
        return None


def build_caption(title: str, validity: Optional[str], link: str) -> str:
    parts = [f"<b>{escape_html(title)}</b>"]

    if validity:
        parts.append(f"📅 {escape_html(validity)}")

    parts.append(f"🔗 <a href=\"{escape_html(link)}\">Acessar oferta</a>")
    parts.append("💬 Veja os detalhes completos nos comentários abaixo")

    return truncate_text("\n\n".join(parts), MAX_CAPTION_LENGTH)


def build_comment_text(description: str, validity: Optional[str], link: str) -> str:
    desc = clean_multiline_text(description)
    parts = ["📋 <b>descrição completa</b>", "", escape_html(desc)]

    if validity:
        parts.extend(["", f"📅 {escape_html(validity)}"])

    parts.extend(["", f"🔗 <a href=\"{escape_html(link)}\">Link original</a>"])

    return truncate_text("\n".join(parts), MAX_COMMENT_LENGTH)


def send_photo_to_channel(img_path: str, caption: str) -> Optional[int]:
    try:
        url = telegram_api("sendPhoto")
        with open(img_path, "rb") as photo:
            response = requests.post(
                url,
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "caption": caption,
                    "parse_mode": "HTML",
                },
                files={"photo": photo},
                timeout=REQUEST_TIMEOUT,
            )

        if not response.ok:
            log(f"   ❌ falha sendPhoto: {response.text}")
            return None

        data = response.json()
        message_id = data.get("result", {}).get("message_id")
        log(f"   ✅ foto enviada ao canal (message_id {message_id})")
        return message_id
    except Exception as e:
        log(f"   ❌ erro sendPhoto: {e}")
        return None


def find_group_mirror_message_id(
    channel_message_id: int,
    attempts: int = 6,
    delay: int = 3
) -> Optional[int]:
    """
    tenta descobrir qual mensagem apareceu no grupo vinculado
    correspondente ao post do canal.

    regra:
    - aceita apenas forward automático real
    - confere se veio do canal correto
    - casa pelo message_id original do post no canal
    """

    expected_channel_chat_id = str(TELEGRAM_CHAT_ID)

    for attempt in range(1, attempts + 1):
        log(f"   ⏳ aguardando espelhamento no grupo ({attempt}/{attempts})...")
        time.sleep(delay)

        try:
            response = requests.get(
                telegram_api("getUpdates"),
                timeout=REQUEST_TIMEOUT,
            )
            if not response.ok:
                log(f"   ⚠️ getUpdates falhou: {response.text}")
                continue

            data = response.json()
            updates = data.get("result", [])

            for update in reversed(updates):
                for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
                    msg = update.get(key, {})
                    if not msg:
                        continue

                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if chat_id != str(GRUPO_COMENTARIO_ID):
                        continue

                    if not msg.get("is_automatic_forward", False):
                        continue

                    msg_id = msg.get("message_id")

                    origin_message_id = None
                    origin_chat_id = None

                    # api mais nova
                    forward_origin = msg.get("forward_origin", {}) or {}
                    if isinstance(forward_origin, dict):
                        origin_message_id = forward_origin.get("message_id")

                        chat_data = forward_origin.get("chat", {}) or {}
                        sender_chat_data = forward_origin.get("sender_chat", {}) or {}

                        if chat_data.get("id") is not None:
                            origin_chat_id = str(chat_data.get("id"))
                        elif sender_chat_data.get("id") is not None:
                            origin_chat_id = str(sender_chat_data.get("id"))

                    # api antiga
                    if origin_message_id is None:
                        origin_message_id = msg.get("forward_from_message_id")

                    forward_from_chat = msg.get("forward_from_chat", {}) or {}
                    if origin_chat_id is None and forward_from_chat.get("id") is not None:
                        origin_chat_id = str(forward_from_chat.get("id"))

                    if origin_chat_id and origin_chat_id != expected_channel_chat_id:
                        continue

                    if origin_message_id == channel_message_id:
                        log(f"   ✅ id espelhado encontrado no grupo por forward real: {msg_id}")
                        return msg_id

        except Exception as e:
            log(f"   ⚠️ erro ao consultar getUpdates: {e}")

    return None


def send_partner_logo_reply(group_msg_id: int, partner_img_url: Optional[str]) -> None:
    if not partner_img_url:
        return

    logo_path = download_image(partner_img_url, prefix="partner_logo")
    if not logo_path:
        log("   ⚠️ não foi possível baixar a logo do parceiro")
        return

    try:
        with open(logo_path, "rb") as photo:
            resp = requests.post(
                telegram_api("sendPhoto"),
                data={
                    "chat_id": GRUPO_COMENTARIO_ID,
                    "reply_to_message_id": group_msg_id,
                    "allow_sending_without_reply": "true",
                },
                files={"photo": photo},
                timeout=REQUEST_TIMEOUT,
            )

        if resp.ok:
            log("   ✅ logo do parceiro enviada no reply")
        else:
            log(f"   ⚠️ falha ao enviar logo do parceiro: {resp.text}")
    except Exception as e:
        log(f"   ⚠️ erro ao enviar logo do parceiro: {e}")
    finally:
        try:
            Path(logo_path).unlink(missing_ok=True)
        except Exception:
            pass


def send_description_comment(
    description: str,
    validity: Optional[str],
    link: str,
    channel_message_id: int,
    partner_img_url: Optional[str] = None,
) -> bool:
    """
    envia comentário no grupo vinculado.
    ordem:
    1. acha a mensagem espelhada no grupo
    2. opcionalmente manda a logo do parceiro em reply
    3. manda a descrição completa em reply
    """

    group_msg_id = find_group_mirror_message_id(
        channel_message_id=channel_message_id,
        attempts=6,
        delay=3,
    )

    if not group_msg_id:
        log("   ❌ não foi possível localizar a mensagem espelhada no grupo, mantendo no pending")
        return False

    # opcional: manda a logo antes
    if partner_img_url:
        send_partner_logo_reply(group_msg_id, partner_img_url)

    text = build_comment_text(description, validity, link)

    data = {
        "chat_id": GRUPO_COMENTARIO_ID,
        "text": truncate_text(text, MAX_COMMENT_LENGTH),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_to_message_id": group_msg_id,
        "allow_sending_without_reply": "true",
    }

    try:
        resp = requests.post(
            telegram_api("sendMessage"),
            data=data,
            timeout=REQUEST_TIMEOUT,
        )

        if resp.ok:
            log(f"   ✅ comentário enviado como reply ao id {group_msg_id}")
            return True

        log(f"   ❌ erro ao enviar comentário: {resp.text}")
        return False

    except Exception as e:
        log(f"   ❌ exceção ao enviar comentário: {e}")
        return False


# ==============================================
# consumer
# ==============================================
def run_consumer() -> None:
    log("=" * 70)
    log("🤖 bot leouol - consumer do pending")
    log("=" * 70)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not GRUPO_COMENTARIO_ID:
        log("❌ TELEGRAM_TOKEN, TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórios")
        return

    history = load_history()
    processed_keys = set(history.get("ids", []))

    pending_data = load_pending()
    offers = pending_data.get("offers", [])

    log(f"📦 pending atual: {len(offers)} ofertas")

    if not offers:
        log("📭 nada para enviar")
        return

    success_count = 0
    failed_offers: List[Dict] = []

    for index, offer in enumerate(offers, start=1):
        log_separator()
        log(f"📌 oferta {index}/{len(offers)}")

        offer_id = offer.get("id") or get_offer_id(offer.get("link", ""))
        title = offer.get("title") or offer.get("preview_title") or "oferta"
        link = offer.get("link") or offer.get("original_link") or ""
        img_url = offer.get("img_url") or ""
        validity = offer.get("validity")
        description = offer.get("description") or "descrição não disponível."

        offer_key = normalize_offer_key(offer_id or link or title)

        log(f"   id: {offer_id}")
        log(f"   título: {title}")

        if not link:
            log("   ⚠️ oferta sem link, mantendo no pending")
            failed_offers.append(offer)
            continue

        if not img_url:
            log("   ⚠️ oferta sem imagem, mantendo no pending")
            failed_offers.append(offer)
            continue

        img_path = download_image(img_url, prefix="offer")
        if not img_path:
            log("   ⚠️ falha ao baixar imagem, mantendo no pending")
            failed_offers.append(offer)
            continue

        caption = build_caption(title, validity, link)
        channel_message_id = send_photo_to_channel(img_path, caption)

        try:
            Path(img_path).unlink(missing_ok=True)
        except Exception:
            pass

        if not channel_message_id:
            log("   ❌ falha ao postar foto, mantendo no pending")
            failed_offers.append(offer)
            continue

        comment_ok = send_description_comment(
            description=description,
            validity=validity,
            link=link,
            channel_message_id=channel_message_id,
            partner_img_url=offer.get("partner_img_url"),
        )

        if not comment_ok:
            failed_offers.append(offer)
            continue

        processed_keys.add(offer_key)
        success_count += 1
        log("   ✅ enviada com sucesso")

        time.sleep(2)

    save_history({"ids": list(processed_keys)})
    save_pending(failed_offers)

    log_separator()
    log(f"✅ fim. {success_count}/{len(offers)} ofertas enviadas")


# ==============================================
# entry point
# ==============================================
if __name__ == "__main__":
    if "--pending" in sys.argv:
        run_consumer()
    else:
        run_consumer()
