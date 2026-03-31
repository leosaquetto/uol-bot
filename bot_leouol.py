# bot_leouol.py
# consumer do pending_offers.json + envio para telegram + dashboard diário com cabeçalho vivo
# + status_runtime.json

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

BR_TZ = ZoneInfo("America/Sao_Paulo")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
LATEST_FILE = "latest_offers.json"
DAILY_LOG_FILE = "daily_log.json"
STATUS_RUNTIME_FILE = "status_runtime.json"

MAX_HISTORY_SIZE = 500
MAX_DEDUPE_HISTORY_SIZE = 1000
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096
MAX_DASHBOARD_LENGTH = 3900
REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

HASHTAG_RULES_BODY = {
    "#ingresso": ["ingresso", "ingressos"],
    "#show": ["c6fest", "lollapalooza", "carnauol", "show"],
    "#teatro": ["teatro"],
    "#entretenimentoviagens": ["cinema", "ingressos", "espetáculo", "espetaculo"],
    "#standup": ["stand-up", "stand up", "comediante", "humor"],
}

HASHTAG_RULES_TITLE_ONLY = {
    "#servicos": ["terapia"],
    "#beleza": ["depilação", "depilacao", "axilas", "beleza", "barba"],
    "#comerbeber": ["bloomin onion", "cinnamon oblivion", "vinho", "vinhos", "sobremesa"],
    "#compraspresentes": ["ovo de páscoa", "ovo de pascoa", "vivara"],
    "#educacao": ["graduações", "graduacoes", "graduação", "graduacao", "ead", "aprender", "enem"],
    "#eletrodomesticoseletronicos": ["dell", "lg"],
}

SILENT_HASHTAGS = {
    "#servicos",
    "#beleza",
    "#educacao",
    "#eletrodomesticoseletronicos",
}

HASHTAG_PRIORITY = [
    "#campanhasdeingresso",
    "#ingresso",
    "#show",
    "#teatro",
    "#standup",
    "#entretenimentoviagens",
    "#comerbeber",
    "#compraspresentes",
    "#servicos",
    "#beleza",
    "#educacao",
    "#eletrodomesticoseletronicos",
]

SECTION_EMOJIS = {
    "quando": "🗓️",
    "local": "📍",
    "atenção": "⚠️",
    "atencao": "⚠️",
    "importante": "❗",
    "regras de resgate": "📌",
}


def now_br() -> datetime:
    return datetime.now(BR_TZ)


def log(msg: str) -> None:
    timestamp = now_br().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def log_separator() -> None:
    print("-" * 60, flush=True)


def now_br_date() -> str:
    return now_br().strftime("%d/%m/%Y")


def now_br_time() -> str:
    return now_br().strftime("%H:%M")


def now_br_datetime() -> str:
    return now_br().strftime("%d/%m/%Y às %H:%M")


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


def strip_html_for_compare(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


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
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)
    return normalize_text_key(raw)


def pick_description_anchor(description: str) -> str:
    if not description:
        return ""
    lines = [clean_multiline_text(x) for x in str(description).splitlines()]
    filtered = []
    blacklist_starts = (
        "beneficio-valido",
        "valido-ate",
        "local",
        "quando",
        "importante",
        "regras-de-resgate",
        "atencao",
        "enviar-cupons-por-e-mail",
        "preencha-os-campos-abaixo",
        "e-mail",
        "mensagem",
        "enviar",
    )
    for line in lines:
        low = normalize_text_key(line)
        if not low or len(low) < 12:
            continue
        if any(low.startswith(x) for x in blacklist_starts):
            continue
        filtered.append(low)
    return filtered[0][:160] if filtered else ""


def build_dedupe_key(title: str, validity: Optional[str], description: str) -> str:
    title_key = normalize_text_key(title)
    validity_key = normalize_text_key(validity or "")
    desc_key = pick_description_anchor(description)
    parts = [x for x in [title_key, validity_key, desc_key] if x]
    return "|".join(parts)


def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": [], "dedupe_keys": []}
    data = safe_json_load(path, {"ids": [], "dedupe_keys": []})
    ids = data.get("ids", []) if isinstance(data.get("ids", []), list) else []
    dedupe_keys = data.get("dedupe_keys", []) if isinstance(data.get("dedupe_keys", []), list) else []
    normalized_ids, seen_ids = [], set()
    for item in ids:
        key = normalize_offer_key(str(item))
        if key and key not in seen_ids:
            seen_ids.add(key)
            normalized_ids.append(key)
    normalized_dedupe, seen_dedupe = [], set()
    for item in dedupe_keys:
        key = str(item).strip()
        if key and key not in seen_dedupe:
            seen_dedupe.add(key)
            normalized_dedupe.append(key)
    return {"ids": normalized_ids[-MAX_HISTORY_SIZE:], "dedupe_keys": normalized_dedupe[-MAX_DEDUPE_HISTORY_SIZE:]}


def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", []) if isinstance(history.get("ids", []), list) else []
        dedupe_keys = history.get("dedupe_keys", []) if isinstance(history.get("dedupe_keys", []), list) else []
        cleaned_ids, seen_ids = [], set()
        for item in ids:
            key = normalize_offer_key(str(item))
            if key and key not in seen_ids:
                seen_ids.add(key)
                cleaned_ids.append(key)
        cleaned_dedupe, seen_dedupe = [], set()
        for item in dedupe_keys:
            key = str(item).strip()
            if key and key not in seen_dedupe:
                seen_dedupe.add(key)
                cleaned_dedupe.append(key)
        Path(HISTORY_FILE).write_text(json.dumps({"ids": cleaned_ids[-MAX_HISTORY_SIZE:], "dedupe_keys": cleaned_dedupe[-MAX_DEDUPE_HISTORY_SIZE:]}, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ histórico salvo: {len(cleaned_ids)} ids / {len(cleaned_dedupe)} dedupe_keys")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar histórico: {e}")
        return False


def load_pending() -> Dict:
    path = Path(PENDING_FILE)
    if not path.exists():
        return {"last_update": None, "offers": []}
    data = safe_json_load(path, {"last_update": None, "offers": []})
    offers = data.get("offers", []) if isinstance(data.get("offers", []), list) else []
    return {"last_update": data.get("last_update"), "offers": offers}


def save_pending(offers: List[Dict]) -> bool:
    try:
        payload = {"last_update": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "offers": offers}
        Path(PENDING_FILE).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ pending salvo: {len(offers)} ofertas")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar pending: {e}")
        return False


def save_latest(offers: List[Dict]) -> bool:
    try:
        payload = {"last_update": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "offers": offers}
        Path(LATEST_FILE).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ latest_offers salvo: {len(offers)} ofertas")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar latest_offers: {e}")
        return False


def load_daily_log() -> Dict:
    path = Path(DAILY_LOG_FILE)
    default = {"date": "", "message_id": None, "last_success_check": "", "last_new_offer_at": "", "pending_count": 0, "last_consumer_run": "", "lines": []}
    if not path.exists():
        return default
    data = safe_json_load(path, default)
    if not isinstance(data, dict):
        return default
    lines = data.get("lines", []) if isinstance(data.get("lines", []), list) else []
    return {
        "date": str(data.get("date") or ""),
        "message_id": data.get("message_id"),
        "last_success_check": str(data.get("last_success_check") or ""),
        "last_new_offer_at": str(data.get("last_new_offer_at") or ""),
        "pending_count": int(data.get("pending_count") or 0),
        "last_consumer_run": str(data.get("last_consumer_run") or ""),
        "lines": [str(x) for x in lines][-30:],
    }


def save_daily_log(data: Dict) -> bool:
    try:
        Path(DAILY_LOG_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        log("✅ daily_log.json salvo")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar daily_log.json: {e}")
        return False


def load_status_runtime() -> Dict:
    path = Path(STATUS_RUNTIME_FILE)
    default = {
        "scriptable": {"last_started_at": "", "last_finished_at": "", "status": "", "summary": "", "offers_seen": 0, "new_offers": 0, "pending_count": 0, "last_error": ""},
        "scraper": {"last_started_at": "", "last_finished_at": "", "last_success_at": "", "status": "", "summary": "", "offers_seen": 0, "new_offers": 0, "pending_count": 0, "last_error": ""},
        "consumer": {"last_started_at": "", "last_finished_at": "", "last_success_at": "", "status": "", "summary": "", "processed": 0, "sent": 0, "failed": 0, "pending_count": 0, "last_error": ""},
        "global": {"last_offer_title": "", "last_offer_at": "", "last_offer_id": ""},
    }
    if not path.exists():
        return default
    data = safe_json_load(path, default)
    if not isinstance(data, dict):
        return default
    for key, value in default.items():
        if key not in data or not isinstance(data[key], dict):
            data[key] = value
    if "last_success_at" not in data["scraper"]:
        data["scraper"]["last_success_at"] = ""
    if "last_success_at" not in data["consumer"]:
        data["consumer"]["last_success_at"] = ""
    return data


def save_status_runtime(data: Dict) -> bool:
    try:
        Path(STATUS_RUNTIME_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        log("✅ status_runtime.json salvo")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar status_runtime.json: {e}")
        return False


def status_consumer_start(pending_count: int) -> None:
    status = load_status_runtime()
    status["consumer"] = {
        "last_started_at": now_br_datetime(),
        "last_finished_at": status["consumer"].get("last_finished_at", ""),
        "last_success_at": status["consumer"].get("last_success_at", ""),
        "status": "running",
        "summary": "consumer iniciado",
        "processed": 0,
        "sent": 0,
        "failed": 0,
        "pending_count": pending_count,
        "last_error": "",
    }
    save_status_runtime(status)


def status_consumer_finish(summary: str, processed: int, sent: int, failed: int, pending_count: int, status_value: str, last_error: str = "") -> None:
    status = load_status_runtime()
    prev_success = status["consumer"].get("last_success_at", "")
    should_mark_success = status_value in {"ok", "sem_novidade"} and last_error == ""
    status["consumer"] = {
        "last_started_at": status["consumer"].get("last_started_at", ""),
        "last_finished_at": now_br_datetime(),
        "last_success_at": now_br_datetime() if should_mark_success else prev_success,
        "status": status_value,
        "summary": summary,
        "processed": processed,
        "sent": sent,
        "failed": failed,
        "pending_count": pending_count,
        "last_error": last_error,
    }
    save_status_runtime(status)


def parse_br_datetime(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw or raw == "—":
        return None
    for fmt in ("%d/%m/%Y às %H:%M", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=BR_TZ)
        except Exception:
            continue
    for fmt in ("%d/%m às %H:%M", "%d/%m %H:%M"):
        try:
            partial = datetime.strptime(raw, fmt)
            return partial.replace(year=now_br().year, tzinfo=BR_TZ)
        except Exception:
            continue
    return None


def format_relative_time(value: str) -> str:
    dt = parse_br_datetime(value)
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


def extract_latest_line(lines: List[str], source: str) -> str:
    latest = ""
    for line in lines:
        if f"] {source}:" in line:
            latest = line
    return latest


def parse_dashboard_line(line: str, source: str) -> Tuple[str, str]:
    if not line:
        return ("—", "Sem atualização registrada.")
    match = re.match(rf"^\[(\d{{2}}:\d{{2}})\] {source}:\s*(.*)$", line)
    if not match:
        return ("—", line)
    return (match.group(1), match.group(2).strip() or "Sem atualização registrada.")


def map_operation_status(source: str, status_block: Dict, fallback_detail: str) -> Tuple[str, str, str]:
    status_value = str(status_block.get("status") or "").strip().lower()
    detail = str(status_block.get("summary") or fallback_detail or "Sem atualização registrada.").strip()
    started = str(status_block.get("last_started_at") or "")
    finished = str(status_block.get("last_finished_at") or "")
    started_dt = parse_br_datetime(started)
    finished_dt = parse_br_datetime(finished)
    stale_running = status_value == "running" and started_dt and (not finished_dt or finished_dt < started_dt)

    if source == "scriptable":
        if stale_running:
            return ("🟡 Instável", "última execução ainda não consolidada", started or finished)
        if status_value in {"ok", "running", "sem_novidade"}:
            return ("🟢 Online", detail, finished or started)
        if status_value == "erro":
            err = str(status_block.get("last_error") or detail or "Erro")
            return ("🔴 Erro", err, finished or started)
        return ("⚪ Sem dados", detail, finished or started)

    if source == "scraper":
        last_success = str(status_block.get("last_success_at") or "").strip()
        if stale_running:
            return ("🟡 Instável", "rodada iniciada sem fechamento consistente", started or finished or last_success)
        if status_value == "ok":
            return ("🟢 Online", detail, finished or started or last_success)
        if status_value == "sem_novidade":
            return ("⚪ Ocioso", detail, finished or started or last_success)
        if status_value == "erro":
            extra = f"Último sucesso às {last_success.split(' às ')[-1]}" if last_success else "Sem sucesso recente"
            return ("🟡 Bloqueado", f"{extra} (check cloudflare)", finished or started or last_success)
        return ("⚪ Sem dados", detail, finished or started or last_success)

    if source == "consumer":
        if stale_running:
            return ("🟡 Instável", "processamento iniciou mas não fechou corretamente", started or finished)
        if status_value == "running":
            return ("🔵 Ativo", detail, started or finished)
        if status_value == "ok":
            return ("✅ Concluído", detail, finished or started or str(status_block.get("last_success_at") or ""))
        if status_value == "sem_novidade":
            return ("⚪ Ocioso", detail, finished or started or str(status_block.get("last_success_at") or ""))
        if status_value == "parcial":
            return ("🟡 Parcial", detail, finished or started or str(status_block.get("last_success_at") or ""))
        if status_value == "erro":
            err = str(status_block.get("last_error") or detail or "Erro")
            return ("🔴 Erro", err, finished or started)
        return ("⚪ Sem dados", detail, finished or started)

    return ("⚪ Sem dados", detail, "")


def compute_monitor_mood(scriptable_status: str, scraper_status: str, consumer_status: str, pending_count: int) -> str:
    if pending_count > 0 or "Ativo" in consumer_status:
        return "Fila aquecida"
    if "Erro" in scraper_status:
        return "Atenção no scraper"
    if "Bloqueado" in scraper_status or "restrição" in scraper_status.lower():
        return "Bloqueio esperado no github"
    if "Online" in scriptable_status:
        return "Tudo calmo"
    return "Monitorando normalmente"


def compute_monitor_confidence(scriptable_status: str, scraper_status: str, consumer_status: str) -> str:
    if "Erro" in scriptable_status or "Erro" in scraper_status or "Erro" in consumer_status:
        return "Revisar agora"
    if "Bloqueado" in scraper_status or "restrição" in scraper_status.lower():
        return "Limitada pelo github"
    if "Online" in scriptable_status:
        return "Alta"
    return "Moderada"


def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def get_last_offer_snapshot(status: Dict) -> Tuple[str, str]:
    global_block = status.get("global", {}) or {}
    title = str(global_block.get("last_offer_title") or "").strip()
    detected_at = str(global_block.get("last_offer_at") or "").strip()
    if title and detected_at:
        return title, detected_at
    latest = safe_json_load(Path(LATEST_FILE), {"offers": []})
    latest_offers = latest.get("offers", []) if isinstance(latest, dict) else []
    if isinstance(latest_offers, list) and latest_offers:
        last_offer = latest_offers[-1] or {}
        latest_title = str(last_offer.get("title") or last_offer.get("preview_title") or "").strip()
        latest_detected = str(last_offer.get("scraped_at") or "").strip()
        if latest_title:
            if latest_detected:
                try:
                    dt = datetime.fromisoformat(latest_detected.replace("Z", "+00:00")).astimezone(BR_TZ)
                    return latest_title, dt.strftime("%d/%m às %H:%M")
                except Exception:
                    pass
            return latest_title, detected_at or "—"
    history = safe_json_load(Path(HISTORY_FILE), {"ids": []})
    history_ids = history.get("ids", []) if isinstance(history, dict) else []
    if isinstance(history_ids, list) and history_ids:
        return str(history_ids[-1]).strip(), detected_at or "—"
    return "Não disponível", "—"


def format_elapsed_since(value: str) -> str:
    dt = parse_br_datetime(value)
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
    lines = state.get("lines", [])
    s_line = extract_latest_line(lines, "scriptable")
    sc_line = extract_latest_line(lines, "scraper")
    c_line = extract_latest_line(lines, "consumer")
    _s_time, s_msg = parse_dashboard_line(s_line, "scriptable")
    sc_time, sc_msg = parse_dashboard_line(sc_line, "scraper")
    _c_time, c_msg = parse_dashboard_line(c_line, "consumer")
    st = status.get("scriptable", {})
    sc = status.get("scraper", {})
    co = status.get("consumer", {})
    if sc_time == "—" and (sc.get("summary") or sc.get("status") or sc.get("last_finished_at") or sc.get("last_started_at")):
        sc_msg = str(sc.get("summary") or "Sem log recente.").strip() or "Sem log recente."
    s_status, _s_detail, s_dt = map_operation_status("scriptable", st, s_msg)
    sc_status, _sc_detail, sc_dt = map_operation_status("scraper", sc, sc_msg)
    c_status, _c_detail, c_dt = map_operation_status("consumer", co, c_msg)

    def fmt(dt_str: str) -> str:
        rel = format_relative_time(dt_str)
        dt = parse_br_datetime(dt_str)
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
        f"🌤️ <b>Humor do sistema:</b> {escape_html(compute_monitor_mood(s_status, sc_status, c_status, pending_count))}",
        f"🧭 <b>Leitura do ambiente:</b> {escape_html(compute_monitor_confidence(s_status, sc_status, c_status))}",
    ]
    return truncate_text("\n".join(dash), MAX_DASHBOARD_LENGTH)


def send_new_dashboard_message(state: Dict, text: str, action_label: str) -> None:
    try:
        resp = requests.post(telegram_api("sendMessage"), data={"chat_id": GRUPO_COMENTARIO_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": "true"}, timeout=REQUEST_TIMEOUT)
        if resp.ok:
            data = resp.json()
            if data.get("ok"):
                state["message_id"] = data.get("result", {}).get("message_id")
                save_daily_log(state)
                log(f"✅ dashboard diário {action_label}")
            else:
                log(f"⚠️ telegram recusou {action_label} do dashboard diário: {data}")
        else:
            log(f"⚠️ falha ao {action_label} dashboard diário: {resp.text}")
    except Exception as e:
        log(f"⚠️ erro ao {action_label} dashboard diário: {e}")


def sync_daily_dashboard(state: Dict) -> None:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return
    status = load_status_runtime()
    text = format_monitor_dashboard(state, status)
    if state["date"] != now_br_date() or not state["message_id"]:
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = state.get("lines", [])[-20:]
        text = format_monitor_dashboard(state, load_status_runtime())
        send_new_dashboard_message(state, text, "criado")
        return
    try:
        delete_ok = False
        delete_not_found = False
        try:
            delete_resp = requests.post(telegram_api("deleteMessage"), data={"chat_id": GRUPO_COMENTARIO_ID, "message_id": state["message_id"]}, timeout=REQUEST_TIMEOUT)
            if delete_resp.ok:
                delete_data = delete_resp.json()
                delete_ok = bool(delete_data.get("ok"))
                if not delete_ok:
                    description = delete_data.get("description", "")
                    error_code = delete_data.get("error_code")
                    if error_code == 400 and "message to delete not found" in description.lower():
                        delete_not_found = True
                        log(f"⚠️ mensagem do dashboard não existe mais no telegram: {delete_data}")
                    else:
                        log(f"⚠️ telegram não apagou o dashboard anterior: {delete_data}")
            else:
                log(f"⚠️ falha HTTP ao apagar dashboard anterior: {delete_resp.text}")
        except Exception as e:
            log(f"⚠️ erro ao apagar dashboard anterior: {e}")
        if not delete_ok:
            if delete_not_found:
                log("⚠️ mensagem anterior não encontrada no telegram; recriando dashboard")
                state["message_id"] = None
                save_daily_log(state)
                send_new_dashboard_message(state, text, "recriado")
            else:
                log("⚠️ dashboard anterior não foi apagado; não vou criar outro para evitar duplicata")
            return
        send_new_dashboard_message(state, text, "atualizado")
    except Exception as e:
        log(f"⚠️ erro ao atualizar dashboard diário: {e}")


def append_dashboard_line(source: str, status_line: str) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state = {"date": now_br_date(), "message_id": None, "last_success_check": "", "last_new_offer_at": state.get("last_new_offer_at", ""), "pending_count": 0, "last_consumer_run": "", "lines": []}
    line = f"[{now_br_time()}] {source}: {status_line}"
    filtered_lines = [existing for existing in state.get("lines", []) if f"] {source}:" not in existing]
    filtered_lines.append(line)
    state["lines"] = filtered_lines[-12:]
    save_daily_log(state)


def set_dashboard_pending_count(count: int) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["pending_count"] = count
    save_daily_log(state)


def set_dashboard_last_consumer_run() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["last_consumer_run"] = now_br_datetime()
    save_daily_log(state)


def build_smart_hashtags(title: str, description: str, link: str) -> List[str]:
    title_text = (title or "").lower()
    full_text = f"{title}\n{description}".lower()
    tags = []
    if "/campanhasdeingresso/" in (link or "").lower():
        tags.append("#campanhasdeingresso")
    for tag, keywords in HASHTAG_RULES_BODY.items():
        if any(kw.lower() in full_text for kw in keywords):
            tags.append(tag)
    for tag, keywords in HASHTAG_RULES_TITLE_ONLY.items():
        if any(kw.lower() in title_text for kw in keywords):
            tags.append(tag)
    seen, ordered = set(), []
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            ordered.append(tag)
    ordered.sort(key=lambda x: HASHTAG_PRIORITY.index(x) if x in HASHTAG_PRIORITY else 999)
    return ordered


def should_send_silent(tags: List[str]) -> bool:
    tag_set = set(tags)
    if "#campanhasdeingresso" in tag_set:
        return False
    return any(tag in tag_set for tag in SILENT_HASHTAGS)


def decorate_main_title(title: str, link: str) -> str:
    return f"‼️ {title} ‼️" if "/campanhasdeingresso/" in (link or "").lower() else title


def build_caption(title: str, description: str, validity: Optional[str], link: str) -> str:
    tags = build_smart_hashtags(title, description, link)
    decorated_title = decorate_main_title(title, link)
    parts = [f"<b>{escape_html(decorated_title)}</b>"]
    if tags:
        parts.append(escape_html(" ".join(tags)))
    body = []
    if validity:
        body.append(f"📅 {escape_html(validity)}")
    body.append(f"🔗 {escape_html(link)}")
    body.append("💬 Veja os detalhes completos dentro dos comentários.")
    return truncate_text("\n".join(parts) + "\n\n" + "\n\n".join(body), MAX_CAPTION_LENGTH)


def build_comment_text(title: str, description: str, validity: Optional[str], link: str) -> str:
    desc = clean_multiline_text(description)
    lines = desc.splitlines()
    out = [f"📋 <b>{escape_html(title)}</b>", ""]
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            out.append("")
            continue
        section_match = re.match(r"^([A-Za-zÀ-ÿ0-9 /-]{1,35}:)\s*(.*)$", line)
        if section_match:
            label = section_match.group(1).strip()
            rest = section_match.group(2).strip()
            key = label[:-1].strip().lower()
            emoji = SECTION_EMOJIS.get(key, "")
            prefix = f"{emoji} " if emoji else ""
            rendered = f"{prefix}<b>{escape_html(label)}</b>"
            if rest:
                rendered += f" {escape_html(rest)}"
            out.append(rendered)
            continue
        out.append(escape_html(line))
    if validity:
        out.extend(["", f"📅 {escape_html(validity)}"])
    out.extend(["", f"🔗 {escape_html(link)}"])
    return truncate_text("\n".join(out), MAX_COMMENT_LENGTH)


def download_image(img_url: str) -> Optional[str]:
    if not img_url:
        return None
    try:
        headers = {"User-Agent": USER_AGENT, "Referer": "https://clube.uol.com.br/"}
        response = requests.get(img_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        lower = img_url.lower()
        suffix = ".png" if ".png" in lower else (".webp" if ".webp" in lower else ".jpg")
        path = f"/tmp/leouol_{int(time.time() * 1000)}{suffix}"
        Path(path).write_bytes(response.content)
        return path
    except Exception as e:
        log(f"   ⚠️ falha ao baixar imagem: {e}")
        return None


def send_photo_to_channel(img_path: str, caption: str, disable_notification: bool) -> Optional[int]:
    try:
        with open(img_path, "rb") as photo:
            response = requests.post(telegram_api("sendPhoto"), data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "HTML", "disable_notification": "true" if disable_notification else "false"}, files={"photo": photo}, timeout=REQUEST_TIMEOUT)
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


def send_partner_photo_reply(partner_img_url: str, reply_to_message_id: int, disable_notification: bool) -> bool:
    if not partner_img_url:
        return True
    img_path = download_image(partner_img_url)
    if not img_path:
        log("   ⚠️ não consegui baixar a imagem do parceiro")
        return False
    try:
        with open(img_path, "rb") as photo:
            response = requests.post(telegram_api("sendPhoto"), data={"chat_id": GRUPO_COMENTARIO_ID, "reply_to_message_id": reply_to_message_id, "disable_notification": "true" if disable_notification else "false"}, files={"photo": photo}, timeout=REQUEST_TIMEOUT)
        if not response.ok:
            log(f"   ❌ falha ao enviar foto do parceiro: {response.text}")
            return False
        log("   ✅ foto do parceiro enviada nos comentários")
        return True
    except Exception as e:
        log(f"   ❌ erro ao enviar foto do parceiro: {e}")
        return False
    finally:
        try:
            Path(img_path).unlink(missing_ok=True)
        except Exception:
            pass


def find_group_mirror_message_id(channel_message_id: int, expected_caption: str, sent_at_ts: int, attempts: int = 8, delay: float = 4.0) -> Optional[int]:
    expected_caption_cmp = strip_html_for_compare(expected_caption)
    for attempt in range(1, attempts + 1):
        log(f"   ⏳ aguardando espelhamento no grupo ({attempt}/{attempts})...")
        time.sleep(delay)
        try:
            response = requests.get(telegram_api("getUpdates"), params={"offset": -200, "timeout": 0, "allowed_updates": json.dumps(["message", "edited_message", "channel_post", "edited_channel_post"])}, timeout=REQUEST_TIMEOUT)
            if not response.ok:
                log(f"   ⚠️ getUpdates falhou: {response.text}")
                continue
            data = response.json()
            updates = data.get("result", [])
            recent_group_candidates = []
            for update in reversed(updates):
                for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
                    msg = update.get(key, {}) or {}
                    if not msg:
                        continue
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if chat_id != str(GRUPO_COMENTARIO_ID):
                        continue
                    msg_id = msg.get("message_id")
                    msg_date = int(msg.get("date", 0))
                    if msg_date < sent_at_ts - 20 or msg_date > sent_at_ts + 180:
                        continue
                    caption = msg.get("caption") or msg.get("text") or ""
                    caption_cmp = strip_html_for_compare(caption)
                    forward_origin = msg.get("forward_origin", {}) or {}
                    origin_message_id = forward_origin.get("message_id")
                    legacy_forward_id = msg.get("forward_from_message_id")
                    is_auto = msg.get("is_automatic_forward", False)
                    if (is_auto or origin_message_id or legacy_forward_id) and (origin_message_id == channel_message_id or legacy_forward_id == channel_message_id):
                        log(f"   ✅ id espelhado encontrado no grupo por forward: {msg_id}")
                        return msg_id
                    recent_group_candidates.append({"message_id": msg_id, "caption_cmp": caption_cmp})
            for candidate in recent_group_candidates:
                cap = candidate["caption_cmp"]
                if not cap or not expected_caption_cmp:
                    continue
                if cap == expected_caption_cmp:
                    log(f"   ✅ id espelhado encontrado no grupo por caption recente exata: {candidate['message_id']}")
                    return candidate["message_id"]
                if (expected_caption_cmp[:120] and expected_caption_cmp[:120] in cap) or (cap[:120] and cap[:120] in expected_caption_cmp):
                    log(f"   ✅ id espelhado encontrado no grupo por caption recente aproximada: {candidate['message_id']}")
                    return candidate["message_id"]
        except Exception as e:
            log(f"   ⚠️ erro ao consultar getUpdates: {e}")
    return None


def send_description_comment(title: str, description: str, validity: Optional[str], link: str, partner_img_url: str, channel_message_id: int, caption: str, sent_at_ts: int, disable_notification: bool) -> bool:
    group_msg_id = find_group_mirror_message_id(channel_message_id=channel_message_id, expected_caption=caption, sent_at_ts=sent_at_ts, attempts=8, delay=4.0)
    if not group_msg_id:
        log("   ❌ não foi possível localizar a mensagem espelhada no grupo")
        return False
    if partner_img_url:
        partner_ok = send_partner_photo_reply(partner_img_url=partner_img_url, reply_to_message_id=group_msg_id, disable_notification=disable_notification)
        if not partner_ok:
            log("   ⚠️ seguindo sem a imagem do parceiro")
    text = build_comment_text(title, description, validity, link)
    data = {"chat_id": GRUPO_COMENTARIO_ID, "text": truncate_text(text, MAX_COMMENT_LENGTH), "parse_mode": "HTML", "disable_web_page_preview": True, "reply_to_message_id": group_msg_id, "disable_notification": "true" if disable_notification else "false"}
    try:
        resp = requests.post(telegram_api("sendMessage"), data=data, timeout=REQUEST_TIMEOUT)
        if resp.ok:
            log(f"   ✅ comentário enviado como reply ao id {group_msg_id}")
            return True
        log(f"   ❌ erro ao enviar comentário: {resp.text}")
        return False
    except Exception as e:
        log(f"   ❌ exceção ao enviar comentário: {e}")
        return False


def history_sets(history: Dict[str, List[str]]) -> Tuple[set, set]:
    ids = history.get("ids", [])
    dedupe_keys = history.get("dedupe_keys", [])
    return {normalize_offer_key(x) for x in ids if normalize_offer_key(x)}, {str(x).strip() for x in dedupe_keys if str(x).strip()}


def run_consumer() -> None:
    log("=" * 70)
    log("🤖 bot leouol - consumer do pending")
    log("=" * 70)
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not GRUPO_COMENTARIO_ID:
        log("❌ TELEGRAM_TOKEN, TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórios")
        status_consumer_finish(summary="variáveis obrigatórias ausentes", processed=0, sent=0, failed=0, pending_count=0, status_value="erro", last_error="TELEGRAM_TOKEN / TELEGRAM_CHAT_ID / GRUPO_COMENTARIO_ID ausentes")
        return
    pending_data = load_pending()
    offers = pending_data.get("offers", [])
    status_consumer_start(len(offers))
    set_dashboard_last_consumer_run()
    history = load_history()
    processed_keys, processed_dedupe = history_sets(history)
    log(f"📦 pending atual: {len(offers)} ofertas")
    set_dashboard_pending_count(len(offers))
    if offers:
        append_dashboard_line("consumer", f"📦 pending atual: {len(offers)}")
    if not offers:
        log("📭 nada para enviar")
        append_dashboard_line("consumer", "📭 pending vazio")
        status_consumer_finish(summary="pending vazio", processed=0, sent=0, failed=0, pending_count=0, status_value="sem_novidade", last_error="")
        sync_daily_dashboard(load_daily_log())
        return

    success_count = 0
    failed_offers: List[Dict] = []
    successful_offers: List[Dict] = []

    for index, offer in enumerate(offers, start=1):
        log_separator()
        log(f"📌 oferta {index}/{len(offers)}")
        offer_id = offer.get("id") or get_offer_id(offer.get("link", ""))
        title = offer.get("title") or offer.get("preview_title") or "oferta"
        link = offer.get("link") or offer.get("original_link") or ""
        img_url = offer.get("img_url") or ""
        partner_img_url = offer.get("partner_img_url") or ""
        validity = offer.get("validity")
        description = offer.get("description") or "descrição não disponível."
        offer_key = normalize_offer_key(offer_id or link or title)
        dedupe_key = str(offer.get("dedupe_key") or "").strip() or build_dedupe_key(title=title, validity=validity, description=description)
        log(f"   id: {offer_id}")
        log(f"   título: {title}")
        if (offer_key and offer_key in processed_keys) or (dedupe_key and dedupe_key in processed_dedupe):
            log("   ⚠️ já consta no histórico por id/dedupe_key, removendo do pending sem repostar")
            continue
        if not link:
            log("   ⚠️ oferta sem link, mantendo no pending")
            failed_offers.append(offer)
            continue
        if not img_url:
            log("   ⚠️ oferta sem imagem, mantendo no pending")
            failed_offers.append(offer)
            continue
        tags = build_smart_hashtags(title, description, link)
        disable_notification = should_send_silent(tags)
        img_path = download_image(img_url)
        if not img_path:
            log("   ⚠️ falha ao baixar imagem, mantendo no pending")
            failed_offers.append(offer)
            continue
        caption = build_caption(title, description, validity, link)
        sent_at_ts = int(time.time())
        channel_message_id = send_photo_to_channel(img_path, caption, disable_notification)
        try:
            Path(img_path).unlink(missing_ok=True)
        except Exception:
            pass
        if not channel_message_id:
            log("   ❌ falha ao postar foto, mantendo no pending")
            failed_offers.append(offer)
            continue
        if offer_key:
            processed_keys.add(offer_key)
        if dedupe_key:
            processed_dedupe.add(dedupe_key)
        comment_ok = send_description_comment(title=title, description=description, validity=validity, link=link, partner_img_url=partner_img_url, channel_message_id=channel_message_id, caption=caption, sent_at_ts=sent_at_ts, disable_notification=disable_notification)
        if not comment_ok:
            log("   ⚠️ canal enviado, mas comentário falhou; não vou repostar a oferta")
        successful_offers.append(offer)
        success_count += 1
        log("   ✅ enviada com sucesso")
        time.sleep(2)

    save_history({"ids": list(processed_keys), "dedupe_keys": list(processed_dedupe)})
    save_pending(failed_offers)
    save_latest(successful_offers)
    if successful_offers:
        latest_offer = successful_offers[-1]
        status = load_status_runtime()
        status["global"] = {"last_offer_title": latest_offer.get("title") or latest_offer.get("preview_title") or "", "last_offer_at": now_br_datetime(), "last_offer_id": latest_offer.get("id") or get_offer_id(latest_offer.get("link", ""))}
        save_status_runtime(status)
    set_dashboard_pending_count(len(failed_offers))
    append_dashboard_line("consumer", f"✅ enviadas: {success_count} | ❌ pendentes: {len(failed_offers)}")
    processed_count = len(offers)
    status_value = "ok" if success_count > 0 and len(failed_offers) == 0 else ("sem_novidade" if success_count == 0 and len(failed_offers) == 0 else "parcial")
    summary = "pending vazio" if processed_count == 0 else f"processadas {processed_count}, enviadas {success_count}, pendentes {len(failed_offers)}"
    status_consumer_finish(summary=summary, processed=processed_count, sent=success_count, failed=len(failed_offers), pending_count=len(failed_offers), status_value=status_value, last_error="")
    sync_daily_dashboard(load_daily_log())
    log_separator()
    log(f"✅ fim. {success_count}/{len(offers)} ofertas enviadas")


if __name__ == "__main__":
    if "--pending" in sys.argv:
        run_consumer()
    else:
        run_consumer()
