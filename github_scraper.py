import json
import os
import re
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
DAILY_LOG_FILE = "daily_log.json"
STATUS_RUNTIME_FILE = "status_runtime.json"
LATEST_FILE = "latest_offers.json"

REQUEST_TIMEOUT = 30
MAX_DASHBOARD_LENGTH = 3900
MAX_HISTORY_IDS = 1500
MAX_HISTORY_DEDUPE = 1500
MAX_HISTORY_LOOSE = 1500
LATEST_LIMIT = 20

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CANAL_ID = os.environ.get("CANAL_ID") or os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

BR_TZ = ZoneInfo("America/Sao_Paulo")


def now_br() -> datetime:
    return datetime.now(BR_TZ)


def log(msg: str) -> None:
    print(f"[{now_br().strftime('%H:%M:%S')}] {msg}", flush=True)


def now_br_date() -> str:
    return now_br().strftime("%d/%m/%Y")


def now_br_time() -> str:
    return now_br().strftime("%H:%M")


def now_br_datetime() -> str:
    return now_br().strftime("%d/%m/%Y às %H:%M")


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return deepcopy(default)


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


def parse_any_datetime(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw or raw == "—":
        return None

    fmts = [
        "%d/%m/%Y às %H:%M",
        "%d/%m/%Y %H:%M",
        "%d/%m às %H:%M",
        "%d/%m %H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(raw, fmt)
            if "%Y" not in fmt:
                dt = dt.replace(year=now_br().year)
            return dt.replace(tzinfo=BR_TZ)
        except Exception:
            pass

    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(BR_TZ)
    except Exception:
        return None


def format_relative_time(value: str) -> str:
    dt = parse_any_datetime(value)
    if not dt:
        return "Sem dados"
    delta = now_br() - dt
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return "Agora"
    minutes = seconds // 60
    if minutes < 60:
        return f"Há {minutes}min"
    hours = minutes // 60
    rem_minutes = minutes % 60
    if hours < 24:
        return f"Há {hours}h{rem_minutes:02d}min" if rem_minutes else f"Há {hours}h"
    days = hours // 24
    rem_hours = hours % 24
    return f"Há {days}d{rem_hours:02d}h" if rem_hours else f"Há {days}d"


def truncate_text(text: str, max_len: int) -> str:
    return text if len(text) <= max_len else text[:max_len]


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


def slug_tail_variants(value: str) -> set[str]:
    base = normalize_offer_key(value)
    if not base:
        return set()
    variants = {base}
    if "joo" in base:
        variants.add(base.replace("joo", "joao"))
    if "joao" in base:
        variants.add(base.replace("joao", "joo"))
    variants.add(base.replace("-de-", "-"))
    return {x for x in variants if x}


def canonical_offer_key(value: str) -> str:
    variants = sorted(slug_tail_variants(value))
    return variants[0] if variants else ""


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


def load_daily_log() -> Dict:
    path = Path(DAILY_LOG_FILE)
    default = {
        "date": "",
        "message_id": None,
        "last_success_check": "",
        "last_new_offer_at": "",
        "pending_count": 0,
        "last_consumer_run": "",
        "last_rendered_text": "",
        "lines": [],
    }
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = default
    lines = data.get("lines", [])
    if not isinstance(lines, list):
        lines = []
    return {
        "date": str(data.get("date") or ""),
        "message_id": data.get("message_id"),
        "last_success_check": str(data.get("last_success_check") or ""),
        "last_new_offer_at": str(data.get("last_new_offer_at") or ""),
        "pending_count": int(data.get("pending_count") or 0),
        "last_consumer_run": str(data.get("last_consumer_run") or ""),
        "last_rendered_text": str(data.get("last_rendered_text") or ""),
        "lines": [str(x) for x in lines][-30:],
    }


def save_daily_log(data: Dict) -> None:
    Path(DAILY_LOG_FILE).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_status_runtime() -> Dict:
    path = Path(STATUS_RUNTIME_FILE)
    default = {
        "scriptable": {
            "last_started_at": "",
            "last_finished_at": "",
            "status": "",
            "summary": "",
            "offers_seen": 0,
            "new_offers": 0,
            "pending_count": 0,
            "last_error": "",
        },
        "scraper": {
            "last_started_at": "",
            "last_finished_at": "",
            "last_success_at": "",
            "status": "",
            "summary": "",
            "offers_seen": 0,
            "new_offers": 0,
            "pending_count": 0,
            "last_error": "",
        },
        "consumer": {
            "last_started_at": "",
            "last_finished_at": "",
            "last_success_at": "",
            "status": "",
            "summary": "",
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "pending_count": 0,
            "last_error": "",
        },
        "global": {
            "last_offer_title": "",
            "last_offer_at": "",
            "last_offer_id": "",
        },
    }
    if not path.exists():
        return deepcopy(default)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = deepcopy(default)
    for key, value in default.items():
        if key not in data or not isinstance(data[key], dict):
            data[key] = deepcopy(value)
    if "last_success_at" not in data["scraper"]:
        data["scraper"]["last_success_at"] = ""
    if "last_success_at" not in data["consumer"]:
        data["consumer"]["last_success_at"] = ""
    return data


def save_status_runtime(data: Dict) -> None:
    Path(STATUS_RUNTIME_FILE).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def status_consumer_start(pending_count: int) -> None:
    status = load_status_runtime()
    prev = status.get("consumer", {})
    status["consumer"] = {
        "last_started_at": now_br_datetime(),
        "last_finished_at": prev.get("last_finished_at", ""),
        "last_success_at": prev.get("last_success_at", ""),
        "status": "running",
        "summary": "consumer iniciado",
        "processed": 0,
        "sent": 0,
        "failed": 0,
        "pending_count": pending_count,
        "last_error": "",
    }
    save_status_runtime(status)


def status_consumer_finish(
    summary: str,
    status_value: str,
    processed: int,
    sent: int,
    failed: int,
    pending_count: int,
    last_error: str = "",
) -> None:
    status = load_status_runtime()
    prev = status.get("consumer", {})
    last_success_at = prev.get("last_success_at", "")
    if status_value in {"ok", "sem_novidade", "parcial"} and not last_error:
        last_success_at = now_br_datetime()
    status["consumer"] = {
        "last_started_at": prev.get("last_started_at", ""),
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
    save_status_runtime(status)


def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def telegram_post(method: str, data: Dict[str, Any], timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    return requests.post(telegram_api(method), data=data, timeout=timeout)


def telegram_get(method: str, params: Dict[str, Any], timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    return requests.get(telegram_api(method), params=params, timeout=timeout)


def map_operation_status(source: str, status_block: Dict, fallback_detail: str) -> tuple[str, str, str]:
    status_value = str(status_block.get("status") or "").strip().lower()
    detail = str(status_block.get("summary") or fallback_detail or "Sem atualização registrada.").strip()
    started_at = str(status_block.get("last_started_at") or "")
    finished_at = str(status_block.get("last_finished_at") or "")
    started_dt = parse_any_datetime(started_at)
    finished_dt = parse_any_datetime(finished_at)
    stale_running = status_value == "running" and started_dt and (not finished_dt or finished_dt < started_dt)

    if source == "scriptable":
        if stale_running:
            return ("🟡 Instável", "última execução ainda não consolidada", started_at or finished_at)
        if status_value in {"ok", "running", "sem_novidade", "sem_novidades"}:
            return ("🟢 Online", detail, finished_at or started_at)
        if status_value == "erro":
            err = str(status_block.get("last_error") or detail or "Erro")
            return ("🔴 Erro", err, finished_at or started_at)
        return ("⚪ Sem dados", detail, finished_at or started_at)

    if source == "scraper":
        last_success = str(status_block.get("last_success_at") or "").strip()
        if stale_running:
            return ("🟡 Instável", "rodada iniciada sem fechamento consistente", started_at or finished_at or last_success)
        if status_value == "ok":
            return ("🟢 Online", detail, finished_at or started_at or last_success)
        if status_value == "sem_novidade":
            return ("⚪ Ocioso", detail, finished_at or started_at or last_success)
        if status_value == "erro":
            extra = f"Último sucesso às {last_success.split(' às ')[-1]}" if last_success else "Sem sucesso recente"
            return ("🟡 Bloqueado", f"{extra} (check cloudflare)", finished_at or started_at or last_success)
        return ("⚪ Sem dados", detail, finished_at or started_at or last_success)

    if source == "consumer":
        if stale_running:
            return ("🟡 Instável", "processamento iniciou mas não fechou corretamente", started_at or finished_at)
        if status_value == "running":
            return ("🔵 Ativo", detail, started_at or finished_at)
        if status_value == "ok":
            return ("✅ Concluído", detail, finished_at or started_at or str(status_block.get("last_success_at") or ""))
        if status_value == "sem_novidade":
            return ("⚪ Ocioso", detail, finished_at or started_at or str(status_block.get("last_success_at") or ""))
        if status_value == "parcial":
            return ("🟡 Parcial", detail, finished_at or started_at or str(status_block.get("last_success_at") or ""))
        if status_value == "erro":
            err = str(status_block.get("last_error") or detail or "Erro")
            return ("🔴 Erro", err, finished_at or started_at)
        return ("⚪ Sem dados", detail, finished_at or started_at)

    return ("⚪ Sem dados", detail, finished_at or started_at)


def get_last_offer_snapshot(status: Dict) -> tuple[str, str]:
    global_block = status.get("global", {}) or {}
    title = str(global_block.get("last_offer_title") or "").strip()
    detected_at = str(global_block.get("last_offer_at") or "").strip()
    if title and detected_at:
        return title, detected_at

    latest_data = load_json(LATEST_FILE, {"offers": []})
    latest_offers = latest_data.get("offers", []) if isinstance(latest_data, dict) else []
    if isinstance(latest_offers, list) and latest_offers:
        last_offer = latest_offers[-1] or {}
        latest_title = str(last_offer.get("title") or last_offer.get("preview_title") or "").strip()
        latest_detected = str(last_offer.get("sent_at") or last_offer.get("scraped_at") or "").strip()

        if latest_title:
            if latest_detected:
                dt = parse_any_datetime(latest_detected)
                if dt:
                    return latest_title, dt.strftime("%d/%m/%Y às %H:%M")
            return latest_title, detected_at or "—"

    history_data = load_json(HISTORY_FILE, {"ids": []})
    history_ids = history_data.get("ids", []) if isinstance(history_data, dict) else []
    if isinstance(history_ids, list) and history_ids:
        return str(history_ids[-1]).strip(), detected_at or "—"

    return "Não disponível", "—"


def format_elapsed_since(value: str) -> str:
    dt = parse_any_datetime(value)
    if not dt:
        return "sem oferta nova recente"
    delta = now_br() - dt
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return f"{seconds}s sem oferta nova"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}min sem oferta nova"
    hours = minutes // 60
    rem_minutes = minutes % 60
    if hours < 24:
        return f"{hours}h{rem_minutes:02d}m sem oferta nova"
    days = hours // 24
    rem_hours = hours % 24
    return f"{days}d{rem_hours:02d}h sem oferta nova"


def format_monitor_dashboard(state: Dict, status: Dict) -> str:
    st = status.get("scriptable", {})
    sc = status.get("scraper", {})
    co = status.get("consumer", {})

    s_status, _s_detail, s_dt = map_operation_status("scriptable", st, str(st.get("summary") or "Sem atualização registrada."))
    sc_status, _sc_detail, sc_dt = map_operation_status("scraper", sc, str(sc.get("summary") or "Sem atualização registrada."))
    c_status, _c_detail, c_dt = map_operation_status("consumer", co, str(co.get("summary") or "Sem atualização registrada."))

    def fmt(dt_str: str) -> str:
        rel = format_relative_time(dt_str)
        dt = parse_any_datetime(dt_str)
        if not dt:
            return str(rel).lower() if rel != "Sem dados" else rel
        rel_txt = "agora" if str(rel).lower() == "agora" else str(rel).lower()
        return f"{rel_txt} às {dt.strftime('%H:%M')}"

    last_title, last_at = get_last_offer_snapshot(status)
    pending_count = state.get("pending_count", 0)
    scraper_line_status = ("🟠 Sob restrição" if "Bloqueado" in sc_status else sc_status).replace("⚪ Ocioso", "⚪ Em espera")
    consumer_line_status = "✅ Pronto" if pending_count == 0 and ("Ocioso" in c_status or "Concluído" in c_status or "sem_novidade" in str(co.get("status", "")).lower()) else c_status

    dash = [
        f"📊 <b>Monitor Clube Uol</b> ({escape_html(now_br_time())})",
        "",
        f"📱 <b>Scriptable</b> {escape_html(s_status)} <i>({escape_html(fmt(s_dt))})</i>",
        f"🤖 <b>Scraper</b> {escape_html(scraper_line_status)} <i>({escape_html(fmt(sc_dt))})</i>",
        f"📦 <b>Consumer</b> {escape_html(consumer_line_status)} <i>({escape_html(fmt(c_dt))})</i>",
        "",
        f"🎯 <b>Última captura</b> 🕒 {escape_html(last_at)}",
        f"↳ <code>{escape_html(last_title)}</code>",
        f"⏳ <i>{escape_html(format_elapsed_since(last_at))}</i>",
        "",
        f"📦 <b>Fila de processamento:</b> {('🚀 ' + str(pending_count) + ' ofertas aguardando') if pending_count > 0 else '📭 Limpa'}",
        "",
        f"🌤️ <b>Humor do sistema:</b> {'Atenção no scraper' if 'Bloqueado' in sc_status or 'restrição' in scraper_line_status.lower() or 'Erro' in sc_status else ('Fila aquecida' if pending_count > 0 or 'Ativo' in consumer_line_status else 'Tudo calmo')}",
        f"🧭 <b>Leitura do ambiente:</b> {'Parcial' if 'Bloqueado' in sc_status or 'restrição' in scraper_line_status.lower() else ('Alta' if 'Online' in s_status else 'Moderada')}",
    ]
    return truncate_text("\n".join(dash), MAX_DASHBOARD_LENGTH)


def send_new_dashboard_message(state: Dict, text: str, action_label: str) -> None:
    try:
        resp = telegram_post(
            "sendMessage",
            {
                "chat_id": GRUPO_COMENTARIO_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": "true",
                "disable_web_page_preview": "true",
            },
        )
        if resp.ok:
            data = resp.json()
            if data.get("ok"):
                state["message_id"] = data.get("result", {}).get("message_id")
                state["last_rendered_text"] = text
                save_daily_log(state)
                return
        log(f"falha ao {action_label}: {resp.text}")
    except Exception as e:
        log(f"falha ao {action_label}: {e}")


def sync_daily_dashboard(state: Dict) -> None:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return

    status = load_status_runtime()
    text = format_monitor_dashboard(state, status)
    current_text = str(state.get("last_rendered_text") or "")
    if current_text == text:
        save_daily_log(state)
        return

    if state["date"] != now_br_date() or not state["message_id"]:
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = state.get("lines", [])[-12:]
        send_new_dashboard_message(state, text, "criar dashboard diário")
        return

    try:
        resp = telegram_post(
            "editMessageText",
            {
                "chat_id": GRUPO_COMENTARIO_ID,
                "message_id": state["message_id"],
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )
        if resp.ok:
            state["last_rendered_text"] = text
            save_daily_log(state)
            return

        try:
            error_data = resp.json()
        except Exception:
            error_data = {}

        description = str(error_data.get("description") or "")
        low_desc = description.lower()

        if "message is not modified" in low_desc:
            state["last_rendered_text"] = text
            save_daily_log(state)
            return

        if "message to edit not found" in low_desc or "message to delete not found" in low_desc:
            state["message_id"] = None
            state["last_rendered_text"] = ""
            save_daily_log(state)
            send_new_dashboard_message(state, text, "recriar dashboard diário")
            return

        log(f"falha ao editar dashboard diário: {resp.text}")
    except Exception as e:
        log(f"falha ao editar dashboard diário: {e}")


def append_dashboard_line(source: str, status_line: str) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state = {
            "date": now_br_date(),
            "message_id": None,
            "last_success_check": "",
            "last_new_offer_at": state.get("last_new_offer_at", ""),
            "pending_count": 0,
            "last_consumer_run": state.get("last_consumer_run", ""),
            "last_rendered_text": "",
            "lines": [],
        }
    line = f"[{now_br_time()}] {source}: {status_line}"
    filtered = [l for l in state.get("lines", []) if f"] {source}:" not in l]
    filtered.append(line)
    state["lines"] = filtered[-12:]
    sync_daily_dashboard(state)


def set_dashboard_pending_count(count: int) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
        state["last_rendered_text"] = ""
    state["pending_count"] = count
    sync_daily_dashboard(state)


def set_dashboard_consumer_run() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
        state["last_rendered_text"] = ""
    state["last_consumer_run"] = now_br_datetime()
    sync_daily_dashboard(state)


def update_global_last_offer(offer: Dict[str, Any]) -> None:
    status = load_status_runtime()
    status["global"] = {
        "last_offer_title": clean_text(offer.get("title") or offer.get("preview_title") or ""),
        "last_offer_at": now_br_datetime(),
        "last_offer_id": offer.get("id") or "",
    }
    save_status_runtime(status)


def build_main_caption(offer: Dict[str, Any]) -> str:
    title = clean_text(offer.get("title") or offer.get("preview_title") or "oferta uol")
    validity = clean_text(offer.get("validity") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    parts = [f"<b>{escape_html(title)}</b>"]
    if validity:
        parts.append(f"🕒 {escape_html(validity)}")
    if link:
        parts.append(f'<a href="{escape_html(link)}">abrir oferta</a>')
    return "\n".join(parts)


def build_main_text_fallback(offer: Dict[str, Any]) -> str:
    title = clean_text(offer.get("title") or offer.get("preview_title") or "oferta uol")
    validity = clean_text(offer.get("validity") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    parts = [f"🎟️ <b>{escape_html(title)}</b>"]
    if validity:
        parts.append(f"🕒 {escape_html(validity)}")
    if link:
        parts.append(f'<a href="{escape_html(link)}">abrir oferta</a>')
    return "\n".join(parts)


def build_comment_text(offer: Dict[str, Any]) -> str:
    title = clean_text(offer.get("title") or offer.get("preview_title") or "oferta uol")
    validity = clean_text(offer.get("validity") or "")
    description = clean_text(offer.get("description") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    parts = [f"<b>{escape_html(title)}</b>"]
    if validity:
        parts.append(f"🕒 {escape_html(validity)}")
    if description:
        parts.append(escape_html(description[:3500]))
    if link:
        parts.append(f'<a href="{escape_html(link)}">abrir oferta</a>')
    return truncate_text("\n\n".join(parts), 3900)


def send_photo_raw(chat_id: str, photo_url: str, caption: str) -> requests.Response:
    return telegram_post(
        "sendPhoto",
        {
            "chat_id": chat_id,
            "photo": photo_url,
            "caption": caption[:1024],
            "parse_mode": "HTML",
        },
    )


def send_message_raw(chat_id: str, text: str, reply_to_message_id: Optional[int] = None) -> requests.Response:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "false",
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    return telegram_post("sendMessage", payload)


def normalize_chat_id(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw


def normalize_channel_post_id(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def extract_updates_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = data.get("result", [])
    return result if isinstance(result, list) else []


def get_updates(offset: Optional[int] = None, limit: int = 100, timeout: int = 0) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "limit": limit,
        "timeout": timeout,
        "allowed_updates": json.dumps(["message", "edited_message", "channel_post", "edited_channel_post"]),
    }
    if offset is not None:
        params["offset"] = offset
    try:
        resp = telegram_get("getUpdates", params=params, timeout=max(timeout + 5, REQUEST_TIMEOUT))
        if not resp.ok:
            log(f"getUpdates falhou: http {resp.status_code} {resp.text}")
            return []
        data = resp.json()
        if not data.get("ok"):
            log(f"getUpdates retornou não-ok: {data}")
            return []
        return extract_updates_items(data)
    except Exception as e:
        log(f"erro em getUpdates: {e}")
        return []


def find_discussion_mirror_message_id(
    channel_message_id: Optional[int],
    caption_hint: str,
    sent_after_ts: float,
    max_wait_seconds: int = 20,
) -> Optional[int]:
    if not channel_message_id or not GRUPO_COMENTARIO_ID:
        return None

    target_group = normalize_chat_id(GRUPO_COMENTARIO_ID)
    caption_hint = clean_text(caption_hint)
    deadline = time.time() + max_wait_seconds
    offset: Optional[int] = None
    best_candidate: Optional[int] = None

    while time.time() < deadline:
        updates = get_updates(offset=offset, limit=100, timeout=2)
        if updates:
            max_update_id = max(int(u.get("update_id", 0)) for u in updates)
            offset = max_update_id + 1

        for upd in updates:
            for key in ("message", "edited_message"):
                msg = upd.get(key)
                if not isinstance(msg, dict):
                    continue

                chat = msg.get("chat", {}) or {}
                chat_id = normalize_chat_id(chat.get("id"))
                if chat_id != target_group:
                    continue

                msg_date = int(msg.get("date") or 0)
                if msg_date and msg_date < int(sent_after_ts) - 15:
                    continue

                forward_origin = msg.get("forward_origin") or {}
                if isinstance(forward_origin, dict):
                    origin_mid = normalize_channel_post_id(forward_origin.get("message_id"))
                    if origin_mid == channel_message_id:
                        return normalize_channel_post_id(msg.get("message_id"))

                forwarded_mid = normalize_channel_post_id(msg.get("forward_from_message_id"))
                if forwarded_mid == channel_message_id:
                    return normalize_channel_post_id(msg.get("message_id"))

                is_auto = bool(msg.get("is_automatic_forward"))
                if is_auto:
                    text_blob = clean_text(msg.get("text") or msg.get("caption") or "")
                    if caption_hint and text_blob and caption_hint[:120] in text_blob:
                        candidate_mid = normalize_channel_post_id(msg.get("message_id"))
                        if candidate_mid:
                            best_candidate = candidate_mid

        if best_candidate:
            return best_candidate

        time.sleep(1.2)

    return best_candidate


def try_send_photo(chat_id: str, photo_url: str, caption: str) -> tuple[bool, Optional[int], str]:
    if not photo_url:
        return False, None, "url de foto vazia"
    try:
        resp = send_photo_raw(chat_id, photo_url, caption)
        if not resp.ok:
            return False, None, resp.text
        data = resp.json()
        if not data.get("ok"):
            return False, None, str(data)
        return True, data.get("result", {}).get("message_id"), ""
    except Exception as e:
        return False, None, str(e)


def send_offer_main(offer: Dict[str, Any]) -> tuple[bool, Optional[int], float, str]:
    if not TELEGRAM_TOKEN or not CANAL_ID:
        return False, None, 0.0, "variáveis do telegram ausentes"

    main_caption = build_main_caption(offer)
    main_text = build_main_text_fallback(offer)

    img_url = clean_text(offer.get("img_url") or "")
    partner_img_url = clean_text(offer.get("partner_img_url") or "")
    sent_at_ts = time.time()

    for candidate in [img_url, partner_img_url]:
        if not candidate:
            continue
        ok, message_id, err = try_send_photo(CANAL_ID, candidate, main_caption)
        if ok:
            return True, message_id, sent_at_ts, ""
        log(f"sendPhoto falhou para {candidate}: {err}")

    log("fotos falharam; tentando fallback em texto curto")

    try:
        resp = send_message_raw(CANAL_ID, main_text)
        if not resp.ok:
            return False, None, sent_at_ts, f"falha no envio principal: {resp.text}"
        data = resp.json()
        if not data.get("ok"):
            return False, None, sent_at_ts, f"telegram principal não-ok: {data}"
        return True, data.get("result", {}).get("message_id"), sent_at_ts, ""
    except Exception as e:
        return False, None, sent_at_ts, str(e)


def send_offer_comment(offer: Dict[str, Any], channel_message_id: Optional[int], sent_at_ts: float) -> tuple[bool, str]:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return False, "comentário sem configuração"
    if not channel_message_id:
        return False, "comentário sem message_id do canal"

    reply_to_message_id = find_discussion_mirror_message_id(
        channel_message_id=channel_message_id,
        caption_hint=build_main_caption(offer),
        sent_after_ts=sent_at_ts,
        max_wait_seconds=20,
    )

    if not reply_to_message_id:
        return False, "não encontrei a mensagem espelhada no grupo para responder"

    try:
        comment_text = build_comment_text(offer)
        resp = send_message_raw(GRUPO_COMENTARIO_ID, comment_text, reply_to_message_id=reply_to_message_id)
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

    if offer_id:
        ids.append(offer_id)
    if dedupe_key:
        dedupe_keys.append(dedupe_key)
    if loose_key:
        loose_dedupe_keys.append(loose_key)

    history["ids"] = ids[-MAX_HISTORY_IDS:]
    history["dedupe_keys"] = dedupe_keys[-MAX_HISTORY_DEDUPE:]
    history["loose_dedupe_keys"] = loose_dedupe_keys[-MAX_HISTORY_LOOSE:]
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


def consume_pending() -> int:
    pending = load_json(PENDING_FILE, {"last_update": None, "offers": []})
    if not isinstance(pending.get("offers"), list):
        pending["offers"] = []

    history = load_json(HISTORY_FILE, {"ids": [], "dedupe_keys": [], "loose_dedupe_keys": []})
    latest = load_json(LATEST_FILE, {"last_update": None, "offers": []})

    offers = pending["offers"]
    set_dashboard_pending_count(len(offers))
    status_consumer_start(len(offers))

    if not offers:
        append_dashboard_line("consumer", "📭 pending vazio")
        set_dashboard_consumer_run()
        status_consumer_finish(
            summary="pending vazio",
            status_value="sem_novidade",
            processed=0,
            sent=0,
            failed=0,
            pending_count=0,
            last_error="",
        )
        return 0

    remaining: List[Dict[str, Any]] = []
    processed = 0
    sent = 0
    failed = 0
    last_error = ""

    for offer in offers:
        processed += 1

        sent_main, main_message_id, sent_at_ts, err = send_offer_main(offer)

        if sent_main:
            sent += 1
            history = append_history_entries(history, offer)
            latest = update_latest(latest, offer)
            update_global_last_offer(offer)

            sent_comment, comment_err = send_offer_comment(offer, main_message_id, sent_at_ts)
            if not sent_comment:
                failed += 1
                last_error = comment_err or "comentário falhou após envio principal"
                log(f"comentário falhou, mas principal já foi enviado. não voltará ao pending: {last_error}")
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

    set_dashboard_pending_count(len(remaining))
    set_dashboard_consumer_run()

    if sent > 0 and failed == 0:
        append_dashboard_line("consumer", f"✅ enviadas: {sent}")
        status_consumer_finish(
            summary=f"enviadas: {sent}",
            status_value="ok",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error="",
        )
    elif sent > 0 and failed > 0:
        append_dashboard_line("consumer", f"🟡 enviadas: {sent} | falhas: {failed}")
        status_consumer_finish(
            summary=f"enviadas: {sent} | falhas: {failed}",
            status_value="parcial",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error=last_error,
        )
    else:
        append_dashboard_line("consumer", f"❌ falhas: {failed}")
        status_consumer_finish(
            summary=f"falhas: {failed}",
            status_value="erro",
            processed=processed,
            sent=sent,
            failed=failed,
            pending_count=len(remaining),
            last_error=last_error or "nenhuma oferta enviada",
        )

    return 0 if failed == 0 else 1


def main() -> None:
    if "--pending" in sys.argv:
        raise SystemExit(consume_pending())

    log("uso esperado: python bot_leouol.py --pending")
    raise SystemExit(2)


if __name__ == "__main__":
    main()
