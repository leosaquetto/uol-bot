# bot_leouol.py
# consumer do pending_offers.json
# envia:
# 1) foto + legenda no canal
# 2) comentário em resposta no grupo de comentários vinculado ao canal

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
GRUPO_COMENTARIOS_ID = os.environ.get("GRUPO_COMENTARIO_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_HISTORY_SIZE = 500
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096


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

    # remove o formulário inútil do final, se vier
    trash_markers = [
        "enviar cupons por e-mail",
        "preencha os campos abaixo",
    ]

    lower = text.lower()
    for marker in trash_markers:
        idx = lower.find(marker)
        if idx != -1:
            text = text[:idx].strip()
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
        .replace("'", "&#39;")
    )


def get_offer_id(value: str) -> str:
    try:
        clean = value.split("?")[0].rstrip("/")
        return clean.split("/")[-1]
    except Exception:
        return value


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

        normalized = [get_offer_id(str(x)) for x in ids]
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
def download_image(img_url: str) -> Optional[str]:
    try:
        response = requests.get(img_url, timeout=20)
        if not response.ok:
            log(f"   ⚠️ falha ao baixar imagem: {response.status_code}")
            return None

        ext = ".jpg"
        content_type = response.headers.get("content-type", "").lower()
        if "png" in content_type:
            ext = ".png"
        elif "webp" in content_type:
            ext = ".webp"

        path = f"/tmp/leouol_{int(time.time() * 1000)}{ext}"
        Path(path).write_bytes(response.content)
        return path
    except Exception as e:
        log(f"   ⚠️ erro ao baixar imagem: {e}")
        return None


def build_caption(title: str, validity: Optional[str], link: str) -> str:
    parts = [f"<b>{escape_html(normalize_spaces(title) or 'oferta')}</b>"]

    if validity:
        parts.append(f"📅 {escape_html(normalize_spaces(validity))}")

    parts.append(f"🔗 <a href=\"{escape_html(link)}\">Acessar oferta</a>")
    parts.append("💬 Veja os detalhes completos nos comentários abaixo")

    caption = "\n\n".join(parts)
    return truncate_text(caption, MAX_CAPTION_LENGTH)


def build_comment_text(
    title: str,
    validity: Optional[str],
    description: str,
    link: str,
) -> str:
    parts = ["📋 <b>DESCRIÇÃO COMPLETA</b>"]

    if title:
        parts.append(f"<b>{escape_html(normalize_spaces(title))}</b>")

    if validity:
        parts.append(f"📅 {escape_html(normalize_spaces(validity))}")

    cleaned_desc = clean_description(description)
    if cleaned_desc:
        parts.append(escape_html(cleaned_desc))
    else:
        parts.append("descrição não disponível.")

    parts.append(f"🔗 <a href=\"{escape_html(link)}\">Link original</a>")

    text = "\n\n".join(parts)
    return truncate_text(text, MAX_COMMENT_LENGTH)


def send_photo_to_channel(img_path: str, caption: str) -> Optional[int]:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(img_path, "rb") as photo:
            response = requests.post(
                url,
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "caption": caption,
                    "parse_mode": "HTML",
                },
                files={"photo": photo},
                timeout=40,
            )

        if not response.ok:
            log(f"   ❌ sendPhoto falhou: {response.status_code} | {response.text}")
            return None

        payload = response.json()
        return payload.get("result", {}).get("message_id")
    except Exception as e:
        log(f"   ❌ erro no sendPhoto: {e}")
        return None


def find_group_message_id(channel_msg_id: int) -> Optional[int]:
    """
    espera o telegram replicar o post do canal no grupo vinculado
    e tenta achar o message_id correspondente no grupo de comentários
    """
    if not GRUPO_COMENTARIOS_ID:
        log("   ⚠️ GRUPO_COMENTARIO_ID não configurado")
        return None

    log("   💬 aguardando o telegram rotear a mensagem para o grupo...")

    # mais tolerante que antes
    for attempt in range(8):
        time.sleep(4)

        try:
            response = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                timeout=20,
            )

            if not response.ok:
                log(f"   ⚠️ getUpdates falhou: {response.status_code}")
                continue

            payload = response.json()
            updates = payload.get("result", [])

            for update in reversed(updates):
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))

                if chat_id != str(GRUPO_COMENTARIOS_ID):
                    continue

                # padrão novo
                forward_origin = msg.get("forward_origin", {})
                if (
                    forward_origin.get("type") == "channel"
                    and forward_origin.get("message_id") == channel_msg_id
                ):
                    return msg.get("message_id")

                # padrão antigo
                if msg.get("forward_from_message_id") == channel_msg_id:
                    return msg.get("message_id")

                # alguns casos usam info de reply/thread
                reply = msg.get("reply_to_message", {})
                if reply.get("forward_from_message_id") == channel_msg_id:
                    return msg.get("message_id")

        except Exception as e:
            log(f"   ⚠️ erro ao buscar message_id no grupo: {e}")

        log(f"   ⏳ tentativa {attempt + 1}/8 sem achar ainda...")

    return None


def send_description_comment(
    title: str,
    validity: Optional[str],
    description: str,
    link: str,
    channel_msg_id: int,
) -> bool:
    comment_text = build_comment_text(title, validity, description, link)
    group_msg_id = find_group_message_id(channel_msg_id)

    data = {
        "chat_id": GRUPO_COMENTARIOS_ID,
        "text": comment_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    if group_msg_id:
        data["reply_to_message_id"] = group_msg_id
        log("   💬 enviando comentário como resposta no tópico...")
    else:
        log("   ⚠️ não achei o id no grupo. enviando comentário solto.")

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=data,
            timeout=40,
        )

        if not response.ok:
            log(f"   ❌ sendMessage falhou: {response.status_code} | {response.text}")
            return False

        return True
    except Exception as e:
        log(f"   ❌ erro ao enviar comentário: {e}")
        return False


# ==============================================
# consumer principal
# ==============================================
def run_consumer() -> None:
    log("=" * 60)
    log("🤖 bot leouol - consumer do pending")
    log("=" * 60)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not GRUPO_COMENTARIOS_ID:
        log("❌ TELEGRAM_TOKEN, TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórios")
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
        img_url = offer.get("img_url") or ""

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

        if not img_url:
            log("   ⚠️ sem img_url, não dá para postar como foto")
            failed_ids.append(offer_id)
            continue

        img_path = download_image(img_url)
        if not img_path:
            log("   ⚠️ falha ao baixar a imagem principal")
            failed_ids.append(offer_id)
            continue

        caption = build_caption(title, validity, link)
        channel_msg_id = send_photo_to_channel(img_path, caption)

        try:
            Path(img_path).unlink(missing_ok=True)
        except Exception:
            pass

        if not channel_msg_id:
            log("   ❌ falha ao enviar a foto no canal")
            failed_ids.append(offer_id)
            time.sleep(2)
            continue

        comment_ok = send_description_comment(
            title=title,
            validity=validity,
            description=description,
            link=link,
            channel_msg_id=channel_msg_id,
        )

        if comment_ok:
            log("   ✅ foto + comentário enviados com sucesso")
            processed_ids.add(offer_id)
            success_count += 1
        else:
            log("   ⚠️ foto enviada, mas comentário falhou")
            # aqui eu mantive como falha, para tentar de novo depois
            failed_ids.append(offer_id)

        time.sleep(2)

    history["ids"] = list(processed_ids)
    save_history(history)

    failed_set = set(failed_ids)
    remaining_offers = []

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
