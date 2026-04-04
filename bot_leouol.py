import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
STATUS_RUNTIME_FILE = "status_runtime.json"
LATEST_FILE = "latest_offers.json"

REQUEST_TIMEOUT = 30
LATEST_LIMIT = 20

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CANAL_ID = os.environ.get("CANAL_ID") or os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

BR_TZ = ZoneInfo("America/Sao_Paulo")


def now_br() -> datetime:
    return datetime.now(BR_TZ)


def log(msg: str) -> None:
    print(f"[{now_br().strftime('%H:%M:%S')}] {msg}", flush=True)


def now_br_datetime() -> str:
    return now_br().strftime("%d/%m/%Y às %H:%M")


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = str(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"^ +| +$", "", text, flags=re.MULTILINE)
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


def normalize_text_key(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""

    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        raw = raw.replace(src, dst)

    raw = re.sub(r"https?://", "", raw)
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw)
    raw = raw.strip("-")
    return raw


def get_offer_id(link: str) -> str:
    try:
        clean_link = str(link).split("?")[0].rstrip("/")
        return clean_link.split("/")[-1]
    except Exception:
        return str(link or "").strip()


def normalize_offer_key(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)
    return normalize_text_key(raw)


def canonical_offer_key(value: str) -> str:
    base = normalize_offer_key(value)
    if not base:
        return ""
    variants = {base, base.replace("-de-", "-")}
    if "joo" in base:
        variants.add(base.replace("joo", "joao"))
    if "joao" in base:
        variants.add(base.replace("joao", "joo"))
    return sorted(x for x in variants if x)[0]


def pick_description_anchor(description: str) -> str:
    if not description:
        return ""
    lines = [clean_text(x) for x in str(description).splitlines()]
    filtered = []
    blacklist_starts = (
        "beneficio valido",
        "válido até",
        "local",
        "quando",
        "importante",
        "regras de resgate",
        "atencao",
        "atenção",
        "enviar cupons por e-mail",
        "preencha os campos abaixo",
        "e-mail",
        "mensagem",
        "enviar",
    )
    for line in lines:
        low = normalize_text_key(line)
        if not low or len(low) < 12:
            continue
        if any(low.startswith(normalize_text_key(x)) for x in blacklist_starts):
            continue
        filtered.append(low)
    return filtered[0][:160] if filtered else ""


def build_dedupe_key(title: str, validity: Optional[str], description: str) -> str:
    title_key = normalize_text_key(title)
    validity_key = normalize_text_key(validity or "")
    desc_key = pick_description_anchor(description)
    parts = [x for x in [title_key, validity_key, desc_key] if x]
    return "|".join(parts)


def build_loose_dedupe_key(title: str, description: str) -> str:
    title_key = normalize_text_key(title)
    desc_key = pick_description_anchor(description)
    parts = [x for x in [title_key, desc_key] if x]
    return "|".join(parts)


def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def telegram_post(method: str, data: Dict[str, Any], timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    return requests.post(telegram_api(method), data=data, timeout=timeout)


def telegram_get(method: str, params: Dict[str, Any], timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    return requests.get(telegram_api(method), params=params, timeout=timeout)


def build_main_caption(offer: Dict[str, Any]) -> str:
    title = clean_text(offer.get("title") or offer.get("preview_title") or "oferta uol")
    validity = clean_text(offer.get("validity") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    parts = [f"<b>{escape_html(title)}</b>"]
    if validity:
        parts.append(f"📅 {escape_html(validity)}")
    if link:
        parts.append(f'🔗 <a href="{escape_html(link)}">acessar oferta</a>')
    parts.append("💬 veja os detalhes completos nos comentários abaixo")
    return "\n".join(parts)[:1024]


def build_comment_text(offer: Dict[str, Any]) -> str:
    title = clean_text(offer.get("title") or offer.get("preview_title") or "oferta uol")
    validity = clean_text(offer.get("validity") or "")
    description = clean_text(offer.get("description") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    parts = [f"<b>{escape_html(title)}</b>"]
    if description:
        parts.append(escape_html(description[:3500]))
    if validity:
        parts.append(f"📅 {escape_html(validity)}")
    if link:
        parts.append(f'🔗 <a href="{escape_html(link)}">acessar oferta</a>')

    return "\n\n".join(parts)[:3900]


def is_offer_complete_for_send(offer: Dict[str, Any]) -> bool:
    img_url = clean_text(offer.get("img_url") or "")
    validity = clean_text(offer.get("validity") or "")
    description = clean_text(offer.get("description") or "")
    title = clean_text(offer.get("title") or offer.get("preview_title") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    if not title or not link:
        return False
    if not img_url:
        return False
    if not validity:
        return False
    if not description or len(description) < 40:
        return False
    if "descrição não disponível" in description.lower():
        return False
    return True


def send_photo_raw(chat_id: str, photo_url: str, caption: str) -> requests.Response:
    return telegram_post(
        "sendPhoto",
        {
            "chat_id": chat_id,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": "HTML",
        },
    )


def send_message_raw(chat_id: str, text: str, reply_to_message_id: Optional[int] = None) -> requests.Response:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    return telegram_post("sendMessage", payload)


def normalize_chat_id(value: Any) -> str:
    return str(value or "").strip()


def as_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def get_updates(offset: Optional[int] = None, timeout: int = 0) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "limit": 100,
        "timeout": timeout,
        "allowed_updates": json.dumps(["message", "edited_message", "channel_post", "edited_channel_post"]),
    }
    if offset is not None:
        params["offset"] = offset

    try:
        resp = telegram_get("getUpdates", params=params, timeout=max(timeout + 5, REQUEST_TIMEOUT))
        if not resp.ok:
            log(f"getUpdates falhou: {resp.text}")
            return []
        data = resp.json()
        if not data.get("ok"):
            log(f"getUpdates não-ok: {data}")
            return []
        result = data.get("result", [])
        return result if isinstance(result, list) else []
    except Exception as e:
        log(f"erro em getUpdates: {e}")
        return []


def find_mirrored_group_message_id(channel_message_id: int, caption_hint: str, sent_at_ts: float, max_wait_seconds: int = 25) -> Optional[int]:
    if not GRUPO_COMENTARIO_ID:
        return None

    target_group_id = normalize_chat_id(GRUPO_COMENTARIO_ID)
    deadline = time.time() + max_wait_seconds
    offset = None
    best_candidate = None

    while time.time() < deadline:
        updates = get_updates(offset=offset, timeout=2)
        if updates:
            offset = max(int(u.get("update_id", 0)) for u in updates) + 1

        for upd in updates:
            for key in ("message", "edited_message"):
                msg = upd.get(key)
                if not isinstance(msg, dict):
                    continue

                chat = msg.get("chat", {}) or {}
                if normalize_chat_id(chat.get("id")) != target_group_id:
                    continue

                msg_date = int(msg.get("date") or 0)
                if msg_date and msg_date < int(sent_at_ts) - 15:
                    continue

                forward_origin = msg.get("forward_origin") or {}
                if isinstance(forward_origin, dict):
                    forwarded_channel_mid = as_int(forward_origin.get("message_id"))
                    if forwarded_channel_mid == channel_message_id:
                        return as_int(msg.get("message_id"))

                old_forward_mid = as_int(msg.get("forward_from_message_id"))
                if old_forward_mid == channel_message_id:
                    return as_int(msg.get("message_id"))

                if msg.get("is_automatic_forward"):
                    blob = clean_text(msg.get("caption") or msg.get("text") or "")
                    if blob and caption_hint and clean_text(caption_hint)[:120] in blob:
                        best_candidate = as_int(msg.get("message_id"))

        if best_candidate:
            return best_candidate

        time.sleep(1.2)

    return best_candidate


def send_offer_main(offer: Dict[str, Any]) -> tuple[bool, Optional[int], float, str]:
    if not TELEGRAM_TOKEN or not CANAL_ID:
        return False, None, 0.0, "variáveis do telegram ausentes"

    caption = build_main_caption(offer)
    sent_at_ts = time.time()
    img_url = clean_text(offer.get("img_url") or "")

    try:
        resp = send_photo_raw(CANAL_ID, img_url, caption)
        if not resp.ok:
            return False, None, sent_at_ts, f"sendPhoto falhou: {resp.text}"
        data = resp.json()
        if not data.get("ok"):
            return False, None, sent_at_ts, f"sendPhoto não-ok: {data}"
        return True, data.get("result", {}).get("message_id"), sent_at_ts, ""
    except Exception as e:
        return False, None, sent_at_ts, str(e)


def send_offer_comment(offer: Dict[str, Any], channel_message_id: Optional[int], sent_at_ts: float) -> tuple[bool, str]:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return False, "comentário sem configuração"
    if not channel_message_id:
        return False, "comentário sem message_id do canal"

    mirror_message_id = find_mirrored_group_message_id(
        channel_message_id=channel_message_id,
        caption_hint=build_main_caption(offer),
        sent_at_ts=sent_at_ts,
        max_wait_seconds=25,
    )

    if not mirror_message_id:
        return False, "não encontrei a mensagem espelhada no grupo"

    try:
        comment_text = build_comment_text(offer)
        resp = send_message_raw(GRUPO_COMENTARIO_ID, comment_text, reply_to_message_id=mirror_message_id)
        if not resp.ok:
            return False, f"falha no comentário: {resp.text}"
        data = resp.json()
        if not data.get("ok"):
            return False, f"telegram comentário não-ok: {data}"
        return True, ""
    except Exception as e:
        return False, str(e)


def append_history_entries(history: Dict[str, Any], offer: Dict[str, Any]) -> Dict[str, Any]:
    ids = history.get("ids", [])
    dedupe_keys = history.get("dedupe_keys", [])
    loose_dedupe_keys = history.get("loose_dedupe_keys", [])

    if not isinstance(ids, list):
        ids = []
    if not isinstance(dedupe_keys, list):
        dedupe_keys = []
    if not isinstance(loose_dedupe_keys, list):
        loose_dedupe_keys = []

    offer_id = clean_text(offer.get("id") or "")
    dedupe_key = clean_text(offer.get("dedupe_key") or "")
    loose_key = clean_text(offer.get("loose_dedupe_key") or "")

    if not dedupe_key:
        dedupe_key = build_dedupe_key(
            clean_text(offer.get("title") or offer.get("preview_title") or ""),
            clean_text(offer.get("validity") or ""),
            clean_text(offer.get("description") or ""),
        )
    if not loose_key:
        loose_key = build_loose_dedupe_key(
            clean_text(offer.get("title") or offer.get("preview_title") or ""),
            clean_text(offer.get("description") or ""),
        )

    if offer_id:
        ids.append(offer_id)
    if dedupe_key:
        dedupe_keys.append(dedupe_key)
    if loose_key:
        loose_dedupe_keys.append(loose_key)

    history["ids"] = ids[-1500:]
    history["dedupe_keys"] = dedupe_keys[-1500:]
    history["loose_dedupe_keys"] = loose_dedupe_keys[-1500:]
    return history


def update_latest(latest: Dict[str, Any], offer: Dict[str, Any]) -> Dict[str, Any]:
    offers = latest.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    enriched = dict(offer)
    enriched["sent_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    offers.append(enriched)

    bucket: Dict[str, Dict[str, Any]] = {}
    for item in offers:
        key = canonical_offer_key(item.get("id") or item.get("link") or "")
        if not key:
            continue
        prev = bucket.get(key)
        if not prev:
            bucket[key] = item
        else:
            prev_ts = str(prev.get("sent_at") or prev.get("scraped_at") or "")
            item_ts = str(item.get("sent_at") or item.get("scraped_at") or "")
            bucket[key] = item if item_ts >= prev_ts else prev

    final_offers = list(bucket.values())
    final_offers.sort(key=lambda x: str(x.get("sent_at") or x.get("scraped_at") or ""))
    latest["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    latest["offers"] = final_offers[-LATEST_LIMIT:]
    return latest


def update_status_runtime(summary: str, status_value: str, processed: int, sent: int, failed: int, pending_count: int, last_error: str = "") -> None:
    status = load_json(STATUS_RUNTIME_FILE, {
        "scriptable": {},
        "scraper": {},
        "consumer": {},
        "global": {},
    })
    prev = status.get("consumer", {}) if isinstance(status, dict) else {}
    if not isinstance(status, dict):
        status = {"scriptable": {}, "scraper": {}, "consumer": {}, "global": {}}

    last_success_at = prev.get("last_success_at", "")
    if status_value in {"ok", "sem_novidade", "parcial"} and not last_error:
        last_success_at = now_br_datetime()

    status["consumer"] = {
        "last_started_at": prev.get("last_started_at", now_br_datetime()),
        "last_finished_at": now_br_datetime(),
        "last_success_at": last_success_at,
        "status": status_value,
        "summary": summary,
        "processed": processed,
        "sent": sent,
        "failed": failed,
        "pending_count": pending_count,
        "last_error": last_error,
    }
    save_json(STATUS_RUNTIME_FILE, status)


def consume_pending() -> int:
    pending = load_json(PENDING_FILE, {"last_update": None, "offers": []})
    if not isinstance(pending.get("offers"), list):
        pending["offers"] = []

    history = load_json(HISTORY_FILE, {"ids": [], "dedupe_keys": [], "loose_dedupe_keys": []})
    latest = load_json(LATEST_FILE, {"last_update": None, "offers": []})

    offers = pending["offers"]

    status = load_json(STATUS_RUNTIME_FILE, {"scriptable": {}, "scraper": {}, "consumer": {}, "global": {}})
    if not isinstance(status, dict):
        status = {"scriptable": {}, "scraper": {}, "consumer": {}, "global": {}}
    prev_consumer = status.get("consumer", {})
    status["consumer"] = {
        "last_started_at": now_br_datetime(),
        "last_finished_at": prev_consumer.get("last_finished_at", ""),
        "last_success_at": prev_consumer.get("last_success_at", ""),
        "status": "running",
        "summary": "consumer iniciado",
        "processed": 0,
        "sent": 0,
        "failed": 0,
        "pending_count": len(offers),
        "last_error": "",
    }
    save_json(STATUS_RUNTIME_FILE, status)

    if not offers:
        update_status_runtime("pending vazio", "sem_novidade", 0, 0, 0, 0, "")
        return 0

    remaining: List[Dict[str, Any]] = []
    processed = 0
    sent = 0
    failed = 0
    skipped = 0
    last_error = ""

    for offer in offers:
        processed += 1

        if not is_offer_complete_for_send(offer):
            skipped += 1
            log(f"oferta incompleta descartada do pending: {offer.get('title') or offer.get('preview_title')}")
            continue

        sent_main, main_message_id, sent_at_ts, err = send_offer_main(offer)

        if sent_main:
            sent += 1
            history = append_history_entries(history, offer)
            latest = update_latest(latest, offer)

            sent_comment, comment_err = send_offer_comment(offer, main_message_id, sent_at_ts)
            if not sent_comment:
                failed += 1
                last_error = comment_err or "comentário falhou após envio principal"
                log(f"comentário falhou, mas principal já foi enviado: {last_error}")
            else:
                log(f"oferta enviada com sucesso: {offer.get('title') or offer.get('preview_title')}")
        else:
            failed += 1
            last_error = err or "falha desconhecida"
            remaining.append(offer)
            log(f"oferta mantida no pending por falha total: {last_error}")

    pending["offers"] = remaining
    pending["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    save_json(PENDING_FILE, pending)
    save_json(HISTORY_FILE, history)
    save_json(LATEST_FILE, latest)

    if sent > 0 and failed == 0:
        update_status_runtime(
            summary=f"enviadas: {sent} | descartadas: {skipped}",
            status_value="ok",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error="",
        )
    elif sent > 0 and failed > 0:
        update_status_runtime(
            summary=f"enviadas: {sent} | falhas: {failed} | descartadas: {skipped}",
            status_value="parcial",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error=last_error,
        )
    elif failed > 0:
        update_status_runtime(
            summary=f"falhas: {failed} | descartadas: {skipped}",
            status_value="erro",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error=last_error or "nenhuma oferta enviada",
        )
    else:
        update_status_runtime(
            summary=f"nenhuma oferta pronta | descartadas: {skipped}",
            status_value="sem_novidade",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error="",
        )

    return 0 if failed == 0 else 1


def main() -> None:
    if "--pending" in sys.argv:
        raise SystemExit(consume_pending())

    log("uso esperado: python bot_leouol.py --pending")
    raise SystemExit(2)


if __name__ == "__main__":
    main()
