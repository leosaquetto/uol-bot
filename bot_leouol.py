# bot_leouol.py
# consumer do pending_offers.json + envio para telegram
# versão corrigida final:
# - processa tudo que estiver no pending
# - pula oferta já presente no histórico antes de enviar
# - aguarda e localiza SOMENTE pelo ID de encaminhamento automático
# - usa apenas reply_to_message_id (sem message_thread_id)
# - limpa pending apenas do que falhou
# - limpa blocos residuais de formulário na descrição
# - mantém um feed persistente com as últimas ofertas completas para widget

import json
import os
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional

import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
LATEST_FILE = "latest_offers.json"

MAX_HISTORY_SIZE = 500
MAX_LATEST_OFFERS = 10
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096
REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def log_separator() -> None:
    print("-" * 60, flush=True)


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

    cut_markers = [
        "Enviar cupons por e-mail",
        "Preencha os campos abaixo",
        "para enviar os seus cupons resgatados por e-mail",
        "E-mail\n\nMensagem\n\nEnviar",
    ]
    for marker in cut_markers:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx].rstrip()
            break

    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"\bREGRAS DE RESGATE\s+:", "REGRAS DE RESGATE:", text)
    text = re.sub(r"\bproibida\s+\.", "proibida.", text, flags=re.I)
    text = re.sub(r"\bresgatados\s+\.", "resgatados.", text, flags=re.I)
    text = re.sub(r"\b23:59\s+\.", "23:59.", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

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


def slugify_piece(text: str) -> str:
    text = unescape(text or "").lower().strip()
    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e", "í": "i",
        "ó": "o", "ô": "o", "õ": "o", "ú": "u", "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"[^a-z0-9\-_\/]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-/")


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


def compact_offer_for_latest(offer: Dict) -> Dict:
    return {
        "id": offer.get("id"),
        "title": offer.get("title") or offer.get("preview_title") or "oferta",
        "link": offer.get("link") or offer.get("original_link") or "",
        "img_url": offer.get("img_url") or "",
        "partner_img_url": offer.get("partner_img_url") or "",
        "validity": clean_multiline_text(offer.get("validity") or ""),
        "scraped_at": offer.get("scraped_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


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
        log(f"   ✅ histórico salvo: {len(cleaned)} IDs")
        return True
    except Exception as e:
        log(f"   ❌ erro ao salvar histórico: {e}")
        return False


def load_pending() -> Dict:
    path = Path(PENDING_FILE)
    if not path.exists():
        return {"last_update": None, "offers": []}
    data = safe_json_load(path, {"last_update": None, "offers": []})
    offers = data.get("offers", [])
    if not isinstance(offers, list):
        offers = []
    return {"last_update": data.get("last_update"), "offers": offers}


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
        if not offers:
            log("   ✅ arquivo pending_offers.json limpo")
        else:
            log(f"   ✅ pending salvo com {len(offers)} ofertas restantes")
        return True
    except Exception as e:
        log(f"   ❌ erro ao salvar pending: {e}")
        return False


def load_latest_offers() -> Dict:
    path = Path(LATEST_FILE)
    if not path.exists():
        return {"last_update": None, "offers": []}
    data = safe_json_load(path, {"last_update": None, "offers": []})
    offers = data.get("offers", [])
    if not isinstance(offers, list):
        offers = []
    return {"last_update": data.get("last_update"), "offers": offers}


def save_latest_offers(offers: List[Dict]) -> bool:
    try:
        payload = {
            "last_update": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "offers": offers[:MAX_LATEST_OFFERS],
        }
        Path(LATEST_FILE).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"   ✅ latest_offers salvo: {len(payload['offers'])} ofertas")
        return True
    except Exception as e:
        log(f"   ❌ erro ao salvar latest_offers: {e}")
        return False


def update_latest_offers(processed_offers: List[Dict]) -> bool:
    latest = load_latest_offers()
    existing = latest.get("offers", [])

    combined: List[Dict] = []
    seen = set()

    for offer in processed_offers + existing:
        compact = compact_offer_for_latest(offer)
        key = normalize_offer_key(compact.get("id") or compact.get("link") or compact.get("title"))
        if not key or key in seen:
            continue
        seen.add(key)
        combined.append(compact)

    return save_latest_offers(combined[:MAX_LATEST_OFFERS])


def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def download_image(img_url: str) -> Optional[str]:
    if not img_url:
        return None
    try:
        headers = {
            "User-Agent": USER_AGENT,
            "Referer": "https://clube.uol.com.br/",
        }
        response = requests.get(img_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        suffix = ".jpg"
        if ".png" in img_url.lower():
            suffix = ".png"
        elif ".webp" in img_url.lower():
            suffix = ".webp"

        path = f"/tmp/leouol_{int(time.time() * 1000)}{suffix}"
        Path(path).write_bytes(response.content)
        return path
    except Exception as e:
        log(f"   ⚠️ falha ao baixar imagem: {e}")
        return None


def build_caption(title: str, validity: Optional[str], link: str) -> str:
    parts = [f"<b>{escape_html(title)}</b>"]
    if validity:
        parts.append(f"📅 {escape_html(clean_multiline_text(validity))}")
    parts.append(f"🔗 <a href=\"{escape_html(link)}\">acessar oferta</a>")
    parts.append("💬 veja os detalhes completos nos comentários abaixo")
    return truncate_text("\n\n".join(parts), MAX_CAPTION_LENGTH)


def build_comment_text(description: str, validity: Optional[str], link: str) -> str:
    desc = clean_multiline_text(description)
    parts = ["📋 <b>descrição completa</b>", "", escape_html(desc)]
    if validity:
        parts.extend(["", f"📅 {escape_html(clean_multiline_text(validity))}"])
    parts.extend(["", f"🔗 <a href=\"{escape_html(link)}\">link original</a>"])
    return truncate_text("\n".join(parts), MAX_COMMENT_LENGTH)


def send_photo_to_channel(img_path: str, caption: str) -> Optional[int]:
    try:
        with open(img_path, "rb") as photo:
            response = requests.post(
                telegram_api("sendPhoto"),
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "caption": caption,
                    "parse_mode": "HTML",
                },
                files={"photo": photo},
                timeout=REQUEST_TIMEOUT,
            )
        if not response.ok:
            log(f"   ❌ falha sendPhoto (canal): {response.text}")
            return None

        data = response.json()
        message_id = data.get("result", {}).get("message_id")
        log(f"   ✅ foto enviada ao canal (message_id {message_id})")
        return message_id
    except Exception as e:
        log(f"   ❌ erro sendPhoto (canal): {e}")
        return None


def send_photo_to_group(img_path: str, reply_to_id: int) -> Optional[int]:
    try:
        with open(img_path, "rb") as photo:
            response = requests.post(
                telegram_api("sendPhoto"),
                data={
                    "chat_id": GRUPO_COMENTARIO_ID,
                    "reply_to_message_id": reply_to_id,
                },
                files={"photo": photo},
                timeout=REQUEST_TIMEOUT,
            )
        if not response.ok:
            log(f"   ⚠️ falha ao postar logo no grupo: {response.text}")
            return None

        data = response.json()
        message_id = data.get("result", {}).get("message_id")
        log(f"   ✅ logo do parceiro postada no grupo (message_id {message_id})")
        return message_id
    except Exception as e:
        log(f"   ⚠️ erro ao postar logo no grupo: {e}")
        return None


def find_group_mirror_message(channel_message_id: int, attempts: int = 10, delay: float = 3.0) -> Optional[int]:
    last_update_id = None

    for attempt in range(1, attempts + 1):
        log(f"   ⏳ aguardando {delay} segundos para o forward (Tentativa {attempt}/{attempts})...")
        time.sleep(delay)

        try:
            params = {
                "limit": 100,
                "timeout": 0,
                "allowed_updates": json.dumps(["message"]),
            }
            if last_update_id:
                params["offset"] = last_update_id + 1

            response = requests.get(telegram_api("getUpdates"), params=params, timeout=REQUEST_TIMEOUT)
            if not response.ok:
                continue

            data = response.json()
            updates = data.get("result", [])

            for update in updates:
                update_id = update.get("update_id")
                if update_id:
                    last_update_id = update_id

                msg = update.get("message")
                if not msg:
                    continue

                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(GRUPO_COMENTARIO_ID):
                    continue

                msg_id = msg.get("message_id")
                forward_origin = msg.get("forward_origin", {}) or {}
                origin_message_id = forward_origin.get("message_id")
                legacy_forward_id = msg.get("forward_from_message_id")
                is_auto = msg.get("is_automatic_forward", False)

                if (is_auto or origin_message_id or legacy_forward_id):
                    if origin_message_id == channel_message_id or legacy_forward_id == channel_message_id:
                        log(f"   ✅ ID encontrado no grupo: {msg_id}")
                        return msg_id

        except Exception as e:
            log(f"   ⚠️ erro ao consultar getUpdates: {e}")

    return None


def send_text_comment(description: str, validity: Optional[str], link: str, reply_to_id: int) -> bool:
    text = build_comment_text(description, validity, link)

    data = {
        "chat_id": GRUPO_COMENTARIO_ID,
        "text": truncate_text(text, MAX_COMMENT_LENGTH),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_to_message_id": reply_to_id,
    }

    try:
        log(f"   💬 enviando texto de descrição como reply ao ID {reply_to_id}")
        resp = requests.post(telegram_api("sendMessage"), data=data, timeout=REQUEST_TIMEOUT)

        if resp.ok:
            log("   ✅ texto de descrição enviado com sucesso!")
            return True

        log(f"   ❌ erro ao enviar texto de descrição: {resp.text}")
        return False

    except Exception as e:
        log(f"   ❌ exceção ao enviar texto de descrição: {e}")
        return False


def run_consumer() -> None:
    log("=" * 60)
    log("🤖 BOT LEOUOL - Consumer (Processando pendentes)")
    log("=" * 60)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not GRUPO_COMENTARIO_ID:
        log("❌ TELEGRAM_TOKEN, TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórios")
        return

    history = load_history()
    processed_keys = set(history.get("ids", []))
    pending_data = load_pending()
    offers = pending_data.get("offers", [])

    if not offers:
        log("📭 nenhuma oferta pendente")
        return

    log(f"🎉 {len(offers)} ofertas pendentes encontradas!\n")

    success_count = 0
    failed_offers: List[Dict] = []
    newly_processed_offers: List[Dict] = []

    for index, offer in enumerate(offers, start=1):
        log_separator()
        log(f"📦 Oferta {index}/{len(offers)}")

        offer_id = offer.get("id") or get_offer_id(offer.get("link", ""))
        title = offer.get("title") or offer.get("preview_title") or "oferta"
        link = offer.get("link") or offer.get("original_link") or ""
        img_url = offer.get("img_url") or ""
        validity = offer.get("validity")
        description = offer.get("description") or "descrição não disponível."
        offer_key = normalize_offer_key(offer_id or link or title)

        log(f"🏷️ {title[:50]}")

        if offer_key in processed_keys:
            log("   ⏭️ oferta já está no histórico, pulando")
            continue

        if not link or not img_url:
            log("   ⚠️ oferta sem link ou imagem principal, mantendo no pending")
            failed_offers.append(offer)
            continue

        img_path = download_image(img_url)
        if not img_path:
            log("   ⚠️ falha ao baixar imagem principal, mantendo no pending")
            failed_offers.append(offer)
            continue

        caption = build_caption(title, validity, link)
        channel_message_id = send_photo_to_channel(img_path, caption)

        try:
            Path(img_path).unlink(missing_ok=True)
        except Exception:
            pass

        if not channel_message_id:
            log("   ❌ falha ao postar foto no canal, mantendo no pending")
            failed_offers.append(offer)
            continue

        mirror_id = find_group_mirror_message(channel_message_id=channel_message_id, attempts=10, delay=3.0)

        if not mirror_id:
            log("   ❌ não foi possível localizar o forward no grupo. Mantendo no pending.")
            failed_offers.append(offer)
            continue

        current_reply_target = mirror_id

        partner_img_url = offer.get("partner_img_url")
        if partner_img_url:
            logo_path = download_image(partner_img_url)
            if logo_path:
                send_photo_to_group(logo_path, current_reply_target)
                try:
                    Path(logo_path).unlink(missing_ok=True)
                except Exception:
                    pass

        comment_ok = send_text_comment(description=description, validity=validity, link=link, reply_to_id=current_reply_target)

        if not comment_ok:
            failed_offers.append(offer)
            continue

        processed_keys.add(offer_key)
        newly_processed_offers.append(offer)
        success_count += 1
        log(f"   ✅ Oferta {index} concluída com sucesso na thread!")
        time.sleep(2)

    save_history({"ids": list(processed_keys)})
    save_pending(failed_offers)
    if newly_processed_offers:
        update_latest_offers(newly_processed_offers)

    log("\n" + "=" * 60)
    log(f"✅ Fim. {success_count}/{len(offers)} ofertas processadas com sucesso.")


if __name__ == "__main__":
    run_consumer()
