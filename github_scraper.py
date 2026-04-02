import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import certifi
import requests
import urllib3
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError, RequestException, SSLError

BASE_URL = "https://clube.uol.com.br"
LIST_URL = f"{BASE_URL}/?order=new"
FALLBACK_LIST_URL = f"{BASE_URL}/"

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
DAILY_LOG_FILE = "daily_log.json"
STATUS_RUNTIME_FILE = "status_runtime.json"

SNAPSHOT_DIR = "snapshots"
SNAPSHOT_CONTROL_FILE = "snapshots_control.json"

REQUEST_TIMEOUT = 30
MAX_DASHBOARD_LENGTH = 3900

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_snapshot_control() -> Dict[str, Any]:
    return load_json(SNAPSHOT_CONTROL_FILE, {"processed_snapshot_ids": []})


def save_snapshot_control(data: Dict[str, Any]) -> None:
    save_json(SNAPSHOT_CONTROL_FILE, data)


def list_snapshot_ids() -> List[str]:
    if not os.path.exists(SNAPSHOT_DIR):
        return []

    ids = []
    for name in os.listdir(SNAPSHOT_DIR):
        if name.startswith("snapshot_") and name.endswith(".json"):
            snapshot_id = name[len("snapshot_") : -len(".json")]
            ids.append(snapshot_id)

    ids.sort()
    return ids


def load_snapshot(snapshot_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    meta_path = os.path.join(SNAPSHOT_DIR, f"snapshot_{snapshot_id}.json")

    if not os.path.exists(meta_path):
        return None, None

    meta = load_json(meta_path, None)
    if not isinstance(meta, dict):
        return None, None

    html_path = str(meta.get("html_path") or "").strip()
    if not html_path or not os.path.exists(html_path):
        return meta, None

    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        return meta, html
    except Exception:
        return meta, None


def get_unprocessed_snapshot_ids() -> tuple[List[str], Dict[str, Any]]:
    control = load_snapshot_control()
    processed = set(control.get("processed_snapshot_ids", []))
    all_ids = list_snapshot_ids()
    pending_ids = [snapshot_id for snapshot_id in all_ids if snapshot_id not in processed]
    return pending_ids, control


def mark_snapshot_processed(snapshot_id: str, control: Dict[str, Any]) -> None:
    processed = control.get("processed_snapshot_ids", [])
    if not isinstance(processed, list):
        processed = []

    if snapshot_id not in processed:
        processed.append(snapshot_id)

    control["processed_snapshot_ids"] = processed[-500:]
    save_snapshot_control(control)


def cleanup_snapshot_files(snapshot_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
    candidates = [
        os.path.join(SNAPSHOT_DIR, f"snapshot_{snapshot_id}.json"),
        os.path.join(SNAPSHOT_DIR, f"snapshot_{snapshot_id}.html"),
        os.path.join(SNAPSHOT_DIR, f"detail_{snapshot_id}.json"),
    ]

    if isinstance(meta, dict):
        html_path = str(meta.get("html_path") or "").strip()
        if html_path:
            candidates.append(html_path)

    seen = set()
    for path in candidates:
        normalized = str(path or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        try:
            if os.path.exists(normalized):
                os.remove(normalized)
                log(f"snapshot limpo: {normalized}")
        except Exception as e:
            log(f"falha ao limpar snapshot {normalized}: {e}")


def load_detail_payload(snapshot_id: str) -> Dict[str, Any]:
    path = os.path.join(SNAPSHOT_DIR, f"detail_{snapshot_id}.json")
    data = load_json(path, {})
    return data if isinstance(data, dict) else {}


def build_detail_lookup(detail_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    offers = detail_payload.get("offers", []) if isinstance(detail_payload, dict) else []
    if not isinstance(offers, list):
        offers = []

    lookup: Dict[str, Dict[str, Any]] = {}
    for item in offers:
        if not isinstance(item, dict):
            continue

        raw_link = str(item.get("link") or "").strip()
        if not raw_link:
            continue

        link = absolutize_url(raw_link)
        title = clean_text(
            item.get("detail_title")
            or item.get("card_title")
            or item.get("title")
            or ""
        )

        validity = clean_text(item.get("validity") or "") or None
        description = clean_text(item.get("description") or item.get("description_preview") or "")
        detail_img_url = absolutize_url(
            item.get("detail_img_url")
            or item.get("card_img_url")
            or ""
        )
        partner_img_url = absolutize_url(item.get("partner_img_url") or "")

        lookup[link] = {
            "title": title,
            "validity": validity,
            "description": description,
            "detail_img_url": detail_img_url,
            "partner_img_url": partner_img_url,
        }

    return lookup


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = str(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"^ +| +$", "", text, flags=re.MULTILINE)
    return text.strip()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    text = html
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n\n", text, flags=re.I)
    text = re.sub(r"</div>", "\n", text, flags=re.I)
    text = re.sub(r"<li[^>]*>", "\n• ", text, flags=re.I)
    text = re.sub(r"</li>", "", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return clean_text(text)


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


def truncate_text(text: str, max_len: int) -> str:
    return text if len(text) <= max_len else text[:max_len]


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
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = default
    for key, value in default.items():
        if key not in data or not isinstance(data[key], dict):
            data[key] = value
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


def status_scraper_start() -> None:
    status = load_status_runtime()
    prev = status.get("scraper", {})
    status["scraper"] = {
        "last_started_at": now_br_datetime(),
        "last_finished_at": prev.get("last_finished_at", ""),
        "last_success_at": prev.get("last_success_at", ""),
        "status": "running",
        "summary": "scraper iniciado",
        "offers_seen": 0,
        "new_offers": 0,
        "pending_count": prev.get("pending_count", 0),
        "last_error": "",
    }
    save_status_runtime(status)


def status_scraper_finish(
    summary: str,
    status_value: str,
    offers_seen: int,
    new_offers: int,
    pending_count: int,
    last_error: str = "",
) -> None:
    status = load_status_runtime()
    prev = status.get("scraper", {})
    last_success_at = prev.get("last_success_at", "")
    if status_value in {"ok", "sem_novidade"} and not last_error:
        last_success_at = now_br_datetime()
    status["scraper"] = {
        "last_started_at": prev.get("last_started_at", ""),
        "last_finished_at": now_br_datetime(),
        "last_success_at": last_success_at,
        "status": status_value,
        "summary": summary,
        "offers_seen": offers_seen,
        "new_offers": new_offers,
        "pending_count": pending_count,
        "last_error": last_error,
    }
    save_status_runtime(status)


def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def map_operation_status(source: str, status_block: Dict, fallback_detail: str) -> tuple[str, str, str]:
    status_value = str(status_block.get("status") or "").strip().lower()
    detail = str(status_block.get("summary") or fallback_detail or "Sem atualização registrada.").strip()
    started_at = str(status_block.get("last_started_at") or "")
    finished_at = str(status_block.get("last_finished_at") or "")
    last_success = str(status_block.get("last_success_at") or "").strip()

    started_dt = parse_br_datetime(started_at)
    finished_dt = parse_br_datetime(finished_at)
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
        if stale_running:
            return ("🟡 Instável", "rodada iniciada sem fechamento consistente", started_at or finished_at or last_success)
        if status_value == "running":
            return ("🔵 Ativo", detail or "processando snapshots", started_at or finished_at or last_success)
        if status_value == "ok":
            return ("🟢 Online", detail, finished_at or started_at or last_success)
        if status_value == "sem_novidade":
            return ("⚪ Em espera", detail, finished_at or started_at or last_success)
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

    latest_data = load_json("latest_offers.json", {"offers": []})
    latest_offers = latest_data.get("offers", []) if isinstance(latest_data, dict) else []
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

    history_data = load_json(HISTORY_FILE, {"ids": []})
    history_ids = history_data.get("ids", []) if isinstance(history_data, dict) else []
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
    st = status.get("scriptable", {})
    sc = status.get("scraper", {})
    co = status.get("consumer", {})

    s_status, _s_detail, s_dt = map_operation_status("scriptable", st, str(st.get("summary") or "Sem atualização registrada."))
    sc_status, _sc_detail, sc_dt = map_operation_status("scraper", sc, str(sc.get("summary") or "Sem atualização registrada."))
    c_status, _c_detail, c_dt = map_operation_status("consumer", co, str(co.get("summary") or "Sem atualização registrada."))

    def fmt(dt_str: str) -> str:
        rel = format_relative_time(dt_str)
        dt = parse_br_datetime(dt_str)
        if not dt:
            return str(rel).lower() if rel != "Sem dados" else rel
        rel_txt = "agora" if str(rel).lower() == "agora" else str(rel).lower()
        return f"{rel_txt} às {dt.strftime('%H:%M')}"

    last_title, last_at = get_last_offer_snapshot(status)
    pending_count = state.get("pending_count", 0)
    consumer_line_status = "✅ Pronto" if pending_count == 0 and ("Ocioso" in c_status or "Concluído" in c_status or "sem_novidade" in str(co.get("status", "")).lower()) else c_status

    dash = [
        f"📊 <b>Monitor Clube Uol</b> ({escape_html(now_br_time())})",
        "",
        f"📱 <b>Scriptable</b> {escape_html(s_status)} <i>({escape_html(fmt(s_dt))})</i>",
        f"🤖 <b>Scraper</b> {escape_html(sc_status)} <i>({escape_html(fmt(sc_dt))})</i>",
        f"📦 <b>Consumer</b> {escape_html(consumer_line_status)} <i>({escape_html(fmt(c_dt))})</i>",
        "",
        f"🎯 <b>Última captura</b> 🕒 {escape_html(last_at)}",
        f"↳ <code>{escape_html(last_title)}</code>",
        f"⏳ <i>{escape_html(format_elapsed_since(last_at))}</i>",
        "",
        f"📦 <b>Fila de processamento:</b> {('🚀 ' + str(pending_count) + ' ofertas aguardando') if pending_count > 0 else '📭 Limpa'}",
        "",
        f"🌤️ <b>Humor do sistema:</b> {'Atenção no scraper' if 'Bloqueado' in sc_status or 'Erro' in sc_status else ('Fila aquecida' if pending_count > 0 or 'Ativo' in consumer_line_status else 'Tudo calmo')}",
        f"🧭 <b>Leitura do ambiente:</b> {'Parcial' if 'Bloqueado' in sc_status else ('Alta' if 'Online' in s_status or 'Online' in sc_status or 'Em espera' in sc_status else 'Moderada')}",
    ]
    return truncate_text("\n".join(dash), MAX_DASHBOARD_LENGTH)


def send_new_dashboard_message(state: Dict, text: str, action_label: str) -> bool:
    try:
        resp = requests.post(
            telegram_api("sendMessage"),
            data={
                "chat_id": GRUPO_COMENTARIO_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": "true",
                "disable_web_page_preview": "true",
            },
            timeout=REQUEST_TIMEOUT,
        )
        if not resp.ok:
            log(f"falha ao {action_label} dashboard diário: {resp.text}")
            return False

        data = resp.json()
        if not data.get("ok"):
            log(f"falha ao {action_label} dashboard diário: {data}")
            return False

        state["message_id"] = data.get("result", {}).get("message_id")
        state["last_rendered_text"] = text
        save_daily_log(state)
        return True
    except Exception as e:
        log(f"falha ao {action_label} dashboard diário: {e}")
        return False


def sync_daily_dashboard(state: Dict) -> None:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return
    status = load_status_runtime()
    text = format_monitor_dashboard(state, status)
    current_text = str(state.get("last_rendered_text") or "")
    if current_text == text:
        save_daily_log(state)
        return

    if not state["message_id"]:
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = state.get("lines", [])[-12:]
        text = format_monitor_dashboard(state, load_status_runtime())
        send_new_dashboard_message(state, text, "criar")
        return

    try:
        resp = requests.post(
            telegram_api("editMessageText"),
            data={
                "chat_id": GRUPO_COMENTARIO_ID,
                "message_id": state["message_id"],
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
            timeout=REQUEST_TIMEOUT,
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

        if "message is not modified" in description.lower():
            state["last_rendered_text"] = text
            save_daily_log(state)
            return

        if "message to delete not found" in description.lower() or "message to edit not found" in description.lower():
            state["message_id"] = None
            save_daily_log(state)
            send_new_dashboard_message(state, text, "recriar")
            return

        log(f"falha ao editar dashboard diário: {resp.text}")
    except Exception as e:
        log(f"falha ao editar dashboard diário: {e}")


def append_dashboard_line(source: str, status_line: str) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state = {
            "date": now_br_date(),
            "message_id": state.get("message_id"),
            "last_success_check": state.get("last_success_check", ""),
            "last_new_offer_at": state.get("last_new_offer_at", ""),
            "pending_count": state.get("pending_count", 0),
            "last_consumer_run": state.get("last_consumer_run", ""),
            "last_rendered_text": "",
            "lines": [],
        }
    line = f"[{now_br_time()}] {source}: {status_line}"
    filtered = [l for l in state.get("lines", []) if f"] {source}:" not in l]
    filtered.append(line)
    state["lines"] = filtered[-12:]
    sync_daily_dashboard(state)


def set_dashboard_success_check() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["lines"] = []
        state["last_rendered_text"] = ""
    state["last_success_check"] = now_br_datetime()
    sync_daily_dashboard(state)


def set_dashboard_last_new_offer() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["lines"] = []
        state["last_rendered_text"] = ""
    state["last_new_offer_at"] = now_br_datetime()
    sync_daily_dashboard(state)


def set_dashboard_pending_count(count: int) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["lines"] = []
        state["last_rendered_text"] = ""
    state["pending_count"] = count
    sync_daily_dashboard(state)


def absolutize_url(url: Optional[str]) -> str:
    if not url:
        return ""
    url = str(url).strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return BASE_URL + url
    return f"{BASE_URL}/{url}"


def get_offer_id(link: str) -> str:
    try:
        clean_link = str(link).split("?")[0].rstrip("/")
        return clean_link.split("/")[-1]
    except Exception:
        return str(link or "").strip()


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


def normalize_offer_key(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)
    return normalize_text_key(raw)


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


def uniq_by(items: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    out = []
    seen = set()
    for item in items:
        key = key_fn(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def is_bad_banner_url(url: Optional[str]) -> bool:
    u = str(url or "").lower()
    if not u:
        return True
    return (
        "loader.gif" in u
        or "/static/images/loader.gif" in u
        or "/parceiros/" in u
        or "/rodape/" in u
        or "icon-instagram" in u
        or "icon-facebook" in u
        or "icon-twitter" in u
        or "icon-youtube" in u
        or "instagram.png" in u
        or "facebook.png" in u
        or "twitter.png" in u
        or "youtube.png" in u
        or "share-" in u
        or "social" in u
        or "logo-uol" in u
        or "logo_uol" in u
    )


def is_likely_benefit_banner(url: Optional[str]) -> bool:
    u = str(url or "").lower()
    if not u or is_bad_banner_url(u):
        return False
    return (
        "/beneficios/" in u
        or "/campanhasdeingresso/" in u
        or "cloudfront.net" in u
    )


def build_headers(referer: Optional[str] = None) -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": referer or (BASE_URL + "/"),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def fetch_once(session: requests.Session, url: str, referer: Optional[str], verify_value) -> requests.Response:
    headers = build_headers(referer)
    response = session.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
        verify=verify_value,
        allow_redirects=True,
    )
    return response


def fetch_with_fallback(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:
    try:
        r = fetch_once(session, url, referer, certifi.where())
        r.raise_for_status()
        return r.text
    except SSLError as e:
        log(f"ssl falhou com verificação padrão, tentando fallback sem verify: {e}")
        try:
            r = fetch_once(session, url, referer, False)
            r.raise_for_status()
            return r.text
        except HTTPError as http_e:
            status_code = getattr(http_e.response, "status_code", None)
            log(f"fallback sem verify retornou http {status_code} para {url}")
            return None
        except RequestException as req_e:
            log(f"fallback sem verify falhou para {url}: {req_e}")
            return None
    except HTTPError as e:
        status_code = getattr(e.response, "status_code", None)
        log(f"http {status_code} ao buscar {url}")
        return None
    except RequestException as e:
        log(f"erro de rede ao buscar {url}: {e}")
        return None


def get_html(url: str) -> Optional[str]:
    session = requests.Session()
    candidates = [(url, BASE_URL + "/")]
    if url == LIST_URL:
        candidates.append((FALLBACK_LIST_URL, BASE_URL + "/"))
    for candidate_url, referer in candidates:
        html = fetch_with_fallback(session, candidate_url, referer)
        if html:
            return html
    return None


def extract_all_img_meta(block) -> List[Dict[str, Any]]:
    imgs: List[Dict[str, Any]] = []
    for img in block.select("img"):
        src = (
            img.get("data-src")
            or img.get("data-original")
            or img.get("data-lazy")
            or img.get("src")
            or ""
        ).strip()
        if not src or src.startswith("data:image"):
            continue
        full_src = absolutize_url(src)
        class_names = " ".join(img.get("class", [])).lower()
        title = (img.get("title") or "").strip().lower()
        alt = (img.get("alt") or "").strip().lower()
        try:
            width = int(img.get("width") or 0)
        except Exception:
            width = 0
        try:
            height = int(img.get("height") or 0)
        except Exception:
            height = 0
        imgs.append(
            {
                "src": full_src,
                "title": title,
                "alt": alt,
                "class_name": class_names,
                "width": width,
                "height": height,
                "is_partner_path": "/parceiros/" in full_src,
                "is_partner_like": (
                    "/parceiros/" in full_src
                    or "logo" in class_names
                    or "brand" in class_names
                    or "parceiro" in class_names
                    or "logo" in alt
                    or bool(title)
                    or (0 < width <= 220)
                    or (0 < height <= 120)
                ),
            }
        )
    return uniq_by(imgs, lambda x: x["src"])


def choose_images_from_block(block) -> Dict[str, str]:
    all_imgs = extract_all_img_meta(block)
    partner_img_url = ""
    img_url = ""
    partner_candidates = [img for img in all_imgs if img["is_partner_like"] or img["is_partner_path"]]
    if partner_candidates:
        partner_img_url = partner_candidates[0]["src"]
    banner_candidates = [
        img for img in all_imgs
        if (not partner_img_url or img["src"] != partner_img_url) and is_likely_benefit_banner(img["src"])
    ]
    if banner_candidates:
        img_url = banner_candidates[-1]["src"]
    if not img_url:
        fallback_candidates = [
            img for img in all_imgs
            if (not partner_img_url or img["src"] != partner_img_url) and not is_bad_banner_url(img["src"])
        ]
        if fallback_candidates:
            img_url = fallback_candidates[-1]["src"]
    if not partner_img_url and len(all_imgs) >= 2:
        for img in all_imgs:
            if img["src"] != img_url:
                partner_img_url = img["src"]
                break
    return {"img_url": img_url, "partner_img_url": partner_img_url}


def parse_offers(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    offers: List[Dict[str, Any]] = []
    blocks = soup.select('[data-categoria="Ingressos Exclusivos"]')
    if not blocks:
        log("fallback: buscando blocos com menção a ingresso")
        candidate_blocks = soup.select("[data-categoria], .beneficio, .item-oferta, .oferta")
        filtered = []
        for block in candidate_blocks:
            low = block.get_text(" ", strip=True).lower()
            hrefs = " ".join(a.get("href", "") for a in block.select("a[href]")).lower()
            if "ingresso" in low or "ingressos" in low or "campanhasdeingresso" in hrefs:
                filtered.append(block)
        blocks = filtered
    log(f"blocos candidatos: {len(blocks)}")
    for block in blocks:
        try:
            title_el = block.select_one(".titulo") or block.select_one("h3") or block.select_one("h2")
            link_el = block.select_one("a[href]")
            if not title_el or not link_el:
                continue
            title = clean_text(title_el.get_text(" ", strip=True))
            link = absolutize_url(link_el.get("href"))
            images = choose_images_from_block(block)
            offer_id = get_offer_id(link)
            log(f"     main url: {images['img_url'] or 'vazia'}")
            log(f"     partner url: {images['partner_img_url'] or 'vazia'}")
            offers.append({
                "id": offer_id,
                "original_link": link,
                "preview_title": title,
                "title": title,
                "link": link,
                "img_url": images["img_url"],
                "partner_img_url": images["partner_img_url"],
            })
            log(f"extraído: {title[:60]}")
        except Exception as e:
            log(f"erro ao parsear bloco: {e}")
    return uniq_by(offers, lambda o: normalize_offer_key(o.get("id") or o.get("link")))


def extract_offer_details(url: str, preview_title: str) -> Dict[str, Any]:
    full_url = absolutize_url(url)
    log(f"acessando detalhes: {preview_title[:50]}...")
    try:
        html = get_html(full_url)
        if not html:
            return {"title": preview_title, "validity": None, "description": "descrição não disponível.", "detail_img_url": ""}
        page_title = preview_title
        for regex in [re.compile(r"<h2[^>]*>([\s\S]*?)</h2>", re.I), re.compile(r"<h1[^>]*>([\s\S]*?)</h1>", re.I)]:
            m = regex.search(html)
            if m:
                candidate_title = clean_text(re.sub(r"<[^>]+>", " ", m.group(1)))
                if candidate_title:
                    page_title = candidate_title
                    break
        all_imgs = []
        for m in re.finditer(r'<img[^>]+(?:data-src|data-original|data-lazy|src)=["\']([^"\']+)["\']', html, re.I):
            src = absolutize_url(m.group(1))
            if src and not src.startswith("data:image"):
                all_imgs.append(src)
        detail_img_url = ""
        detail_candidates = [src for src in all_imgs if is_likely_benefit_banner(src)]
        if detail_candidates:
            detail_img_url = detail_candidates[-1]
        else:
            fallback_detail = [src for src in all_imgs if not is_bad_banner_url(src)]
            if fallback_detail:
                detail_img_url = fallback_detail[-1]
        validity = None
        for regex in [
            re.compile(r"[Bb]enefício válido de[^.!?\n]*[.!?]?", re.I),
            re.compile(r"[Vv]álido até[^.!?\n]*[.!?]?", re.I),
            re.compile(r"\d{2}/\d{2}/\d{4}[\s\S]{0,80}\d{2}/\d{2}/\d{4}", re.I),
        ]:
            m = regex.search(html)
            if m:
                validity = clean_text(re.sub(r"<[^>]+>", " ", m.group(0)))
                break
        description = ""
        for regex in [
            re.compile(r'class=["\'][^"\']*info-beneficio[^"\']*["\'][^>]*>([\s\S]*?)(?:<script|<footer|class=["\'][^"\']*box-compartilhar)', re.I),
            re.compile(r'id=["\']beneficio["\'][^>]*>([\s\S]*?)(?:<script|<footer)', re.I),
        ]:
            m = regex.search(html)
            if m:
                description = html_to_text(m.group(1))
                if len(description) >= 20:
                    break
        if not description or len(description) < 20:
            description = "descrição detalhada não disponível."
        return {
            "title": page_title,
            "validity": validity,
            "description": description[:4000],
            "detail_img_url": detail_img_url,
        }
    except Exception as e:
        log(f"erro ao extrair detalhes: {e}")
        return {"title": preview_title, "validity": None, "description": "descrição não disponível.", "detail_img_url": ""}


def extract_history_sets(history_data: Dict[str, Any]) -> tuple[set, set]:
    ids = history_data.get("ids", [])
    dedupe_keys = history_data.get("dedupe_keys", [])
    if not isinstance(ids, list):
        ids = []
    if not isinstance(dedupe_keys, list):
        dedupe_keys = []
    id_set = {normalize_offer_key(x) for x in ids if normalize_offer_key(x)}
    dedupe_set = {str(x).strip() for x in dedupe_keys if str(x).strip()}
    return id_set, dedupe_set


def extract_pending_sets(pending_data: Dict[str, Any]) -> tuple[set, set]:
    offers = pending_data.get("offers", [])
    if not isinstance(offers, list):
        offers = []
    id_set = set()
    dedupe_set = set()
    for o in offers:
        offer_key = normalize_offer_key(o.get("id") or o.get("link"))
        if offer_key:
            id_set.add(offer_key)
        dedupe_key = str(o.get("dedupe_key") or "").strip()
        if not dedupe_key:
            dedupe_key = build_dedupe_key(
                title=o.get("title") or o.get("preview_title") or "",
                validity=o.get("validity"),
                description=o.get("description") or "",
            )
        if dedupe_key:
            dedupe_set.add(dedupe_key)
    return id_set, dedupe_set


def finish_without_snapshots(pending_count: int) -> None:
    log("nenhum snapshot pendente; encerrando sem scraping direto do uol")
    set_dashboard_pending_count(pending_count)
    append_dashboard_line("scraper", "📭 sem snapshots pendentes")
    status_scraper_finish(
        summary="sem snapshots pendentes",
        status_value="sem_novidade",
        offers_seen=0,
        new_offers=0,
        pending_count=pending_count,
        last_error="",
    )


def main() -> None:
    log("iniciando scraper")
    status_scraper_start()

    historico = load_json(HISTORY_FILE, {"ids": [], "dedupe_keys": []})
    pending = load_json(PENDING_FILE, {"last_update": None, "offers": []})
    if not isinstance(pending.get("offers"), list):
        pending["offers"] = []

    historico_keys, historico_dedupe = extract_history_sets(historico)
    pending_keys, pending_dedupe = extract_pending_sets(pending)

    snapshot_ids, snapshot_control = get_unprocessed_snapshot_ids()

    if snapshot_ids:
        log(f"snapshots pendentes encontrados: {len(snapshot_ids)}")
    else:
        finish_without_snapshots(len(pending.get("offers", [])))
        return

    all_offers = []
    loaded_snapshot_ids = []
    offer_snapshot_map: Dict[str, str] = {}
    snapshot_meta_map: Dict[str, Optional[Dict[str, Any]]] = {}
    detail_lookup_by_snapshot: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for snapshot_id in snapshot_ids:
        meta, html = load_snapshot(snapshot_id)
        snapshot_meta_map[snapshot_id] = meta
        source_label = snapshot_id

        if not html:
            log(f"snapshot inválido ou sem html: {snapshot_id}")
            mark_snapshot_processed(snapshot_id, snapshot_control)
            cleanup_snapshot_files(snapshot_id, meta)
            continue

        set_dashboard_success_check()
        offers = parse_offers(html)
        log(f"total encontradas em {source_label}: {len(offers)}")

        all_offers.extend(offers)
        for offer in offers:
            offer_key = normalize_offer_key(offer.get("id") or offer.get("link"))
            if offer_key and offer_key not in offer_snapshot_map:
                offer_snapshot_map[offer_key] = snapshot_id

        detail_payload = load_detail_payload(snapshot_id)
        detail_lookup_by_snapshot[snapshot_id] = build_detail_lookup(detail_payload)
        loaded_snapshot_ids.append(snapshot_id)

    offers = uniq_by(all_offers, lambda o: normalize_offer_key(o.get("id") or o.get("link")))
    log(f"total consolidado após unir snapshots: {len(offers)}")

    candidates = []
    seen_new_offer_keys = set()
    seen_new_dedupe_keys = set()

    for offer in offers:
        offer_key = normalize_offer_key(offer.get("id") or offer.get("link"))
        snapshot_id = offer_snapshot_map.get(offer_key, "")
        detail_lookup = detail_lookup_by_snapshot.get(snapshot_id, {})
        details = detail_lookup.get(absolutize_url(offer["link"]), {})

        if details:
            details = {
                "title": details.get("title") or offer["preview_title"],
                "validity": details.get("validity"),
                "description": details.get("description") or "descrição não disponível.",
                "detail_img_url": details.get("detail_img_url") or "",
                "partner_img_url": details.get("partner_img_url") or "",
            }
        else:
            details = extract_offer_details(offer["link"], offer["preview_title"])

        final_title = details["title"] or offer["title"]
        final_partner = absolutize_url(details.get("partner_img_url") or offer.get("partner_img_url") or "")
        final_img = absolutize_url(details["detail_img_url"] or "")
        if not final_img or is_bad_banner_url(final_img) or final_img == final_partner:
            fallback_img = absolutize_url(offer.get("img_url") or "")
            if fallback_img and not is_bad_banner_url(fallback_img) and fallback_img != final_partner:
                final_img = fallback_img
        if not final_img or is_bad_banner_url(final_img) or final_img == final_partner:
            final_img = ""

        offer_key = normalize_offer_key(offer.get("id") or offer.get("link"))
        dedupe_key = build_dedupe_key(
            title=final_title,
            validity=details["validity"],
            description=details["description"],
        )
        if not offer_key and not dedupe_key:
            continue
        if offer_key and (offer_key in historico_keys or offer_key in pending_keys or offer_key in seen_new_offer_keys):
            continue
        if dedupe_key and (dedupe_key in historico_dedupe or dedupe_key in pending_dedupe or dedupe_key in seen_new_dedupe_keys):
            continue

        if offer_key:
            seen_new_offer_keys.add(offer_key)
        if dedupe_key:
            seen_new_dedupe_keys.add(dedupe_key)

        candidates.append(
            {
                "id": offer["id"],
                "original_link": offer["original_link"],
                "preview_title": offer["preview_title"] or final_title,
                "title": final_title,
                "link": offer["link"],
                "img_url": final_img,
                "partner_img_url": final_partner,
                "validity": details["validity"],
                "description": details["description"],
                "dedupe_key": dedupe_key,
                "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
        )

    log(f"novas fora de histórico/pending: {len(candidates)}")
    if not candidates:
        log("nenhuma oferta nova para adicionar")

        for snapshot_id in loaded_snapshot_ids:
            mark_snapshot_processed(snapshot_id, snapshot_control)
            cleanup_snapshot_files(snapshot_id, snapshot_meta_map.get(snapshot_id))

        set_dashboard_pending_count(len(pending.get("offers", [])))
        append_dashboard_line("scraper", "💤 sem ofertas novas")
        status_scraper_finish(
            summary="sem ofertas novas",
            status_value="sem_novidade",
            offers_seen=len(offers),
            new_offers=0,
            pending_count=len(pending.get("offers", [])),
            last_error="",
        )
        return

    pending["offers"].extend(candidates)
    pending["offers"] = uniq_by(
        pending["offers"],
        lambda o: str(o.get("dedupe_key") or "").strip() or normalize_offer_key(o.get("id") or o.get("link")),
    )
    pending["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    save_json(PENDING_FILE, pending)

    for snapshot_id in loaded_snapshot_ids:
        mark_snapshot_processed(snapshot_id, snapshot_control)
        cleanup_snapshot_files(snapshot_id, snapshot_meta_map.get(snapshot_id))

    set_dashboard_last_new_offer()
    set_dashboard_pending_count(len(pending["offers"]))
    append_dashboard_line("scraper", f"✅ novas no pending: {len(candidates)}")
    status_scraper_finish(
        summary=f"novas no pending: {len(candidates)}",
        status_value="ok",
        offers_seen=len(offers),
        new_offers=len(candidates),
        pending_count=len(pending["offers"]),
        last_error="",
    )
    log(f"adicionadas ao pending: {len(candidates)}")
    log("finalizado")


if __name__ == "__main__":
    main()
