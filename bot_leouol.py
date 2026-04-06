# bot_leouol.py
# consumer do pending_offers.json + envio para telegram + dashboard diário
# com upload real de imagem, retry para 429, comentário em múltiplas mensagens
# e histórico atualizado só após sucesso real

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

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

MAX_HISTORY_SIZE = 1500
MAX_DEDUPE_HISTORY_SIZE = 1500
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096
MAX_DASHBOARD_LENGTH = 3900
REQUEST_TIMEOUT = 30
RETRY_429_EXTRA_SECONDS = 1
BETWEEN_OFFERS_DELAY_SECONDS = 2
DISCUSSION_WAIT_ATTEMPTS = 3
DISCUSSION_WAIT_SLEEP_SECONDS = 2

HASHTAG_RULES_BODY = {
    "#ingresso": ["ingresso", "ingressos"],
    "#show": ["show", "festival", "musical", "turnê", "turne", "apresentação", "apresentacao"],
    "#teatro": ["teatro", "musical", "espetáculo", "espetaculo", "peça", "peca"],
    "#entretenimentoviagens": ["cinema", "ingressos", "espetáculo", "espetaculo", "evento"],
    "#standup": ["stand-up", "stand up", "comediante", "humor"],
}

HASHTAG_RULES_TITLE_ONLY = {
    "#servicos": ["terapia"],
    "#beleza": ["depilação", "depilacao", "axilas", "beleza", "barba"],
    "#comerbeber": ["bloomin onion", "cinnamon oblivion", "vinho", "vinhos", "sobremesa", "restaurante"],
    "#compraspresentes": ["ovo de páscoa", "ovo de pascoa", "vivara", "presente"],
    "#educacao": ["graduações", "graduacoes", "graduação", "graduacao", "ead", "aprender", "enem"],
    "#eletrodomesticoseletronicos": ["dell", "lg", "eletro", "geladeira", "lavadora"],
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

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://clube.uol.com.br/",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


def now_br() -> datetime:
    return datetime.now(BR_TZ)


def log(msg: str) -> None:
    timestamp = now_br().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def now_br_date() -> str:
    return now_br().strftime("%d/%m/%Y")


def now_br_time() -> str:
    return now_br().strftime("%H:%M")


def now_br_datetime() -> str:
    return now_br().strftime("%d/%m/%Y às %H:%M")


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
        str(text)
        .replace("&", "&amp;")
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


def clean_multiline_text(text: Optional[str]) -> str:
    if not text:
        return ""

    text = str(text)
    text = unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"•\s*\n\s*", "• ", text)
    text = re.sub(r"\n\s*•\s*", "\n• ", text)
    text = re.sub(r" \.", ".", text)
    text = text.strip()

    lixo_markers = [
        "Enviar cupons por e-mail",
        "Preencha os campos abaixo",
        "E-mail\n\nMensagem\n\nEnviar",
        "E-mail\nMensagem\nEnviar",
        "Mensagem\n\nEnviar",
    ]
    for marker in lixo_markers:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx].rstrip()

    text = re.sub(r"\n{3,}", "\n\n", text).strip()
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


def build_dedupe_key(title: str, validity: Optional[str], description: str) -> str:
    title_key = normalize_text_key(title)
    validity_key = normalize_text_key(validity or "")
    desc_key = normalize_text_key(clean_multiline_text(description)[:180])
    return "|".join([x for x in [title_key, validity_key, desc_key] if x])


def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": [], "dedupe_keys": []}

    data = safe_json_load(path, {"ids": [], "dedupe_keys": []})
    ids = data.get("ids", [])
    dedupe_keys = data.get("dedupe_keys", [])

    if not isinstance(ids, list):
        ids = []
    if not isinstance(dedupe_keys, list):
        dedupe_keys = []

    cleaned_ids = []
    seen_ids = set()
    for item in ids:
        key = normalize_offer_key(str(item))
        if key and key not in seen_ids:
            seen_ids.add(key)
            cleaned_ids.append(key)

    cleaned_dedupe = []
    seen_dedupe = set()
    for item in dedupe_keys:
        key = str(item).strip()
        if key and key not in seen_dedupe:
            seen_dedupe.add(key)
            cleaned_dedupe.append(key)

    return {
        "ids": cleaned_ids[-MAX_HISTORY_SIZE:],
        "dedupe_keys": cleaned_dedupe[-MAX_DEDUPE_HISTORY_SIZE:],
    }


def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", [])
        dedupe_keys = history.get("dedupe_keys", [])

        if not isinstance(ids, list):
            ids = []
        if not isinstance(dedupe_keys, list):
            dedupe_keys = []

        cleaned_ids = []
        seen_ids = set()
        for item in ids:
            key = normalize_offer_key(str(item))
            if key and key not in seen_ids:
                seen_ids.add(key)
                cleaned_ids.append(key)

        cleaned_dedupe = []
        seen_dedupe = set()
        for item in dedupe_keys:
            key = str(item).strip()
            if key and key not in seen_dedupe:
                seen_dedupe.add(key)
                cleaned_dedupe.append(key)

        payload = {
            "ids": cleaned_ids[-MAX_HISTORY_SIZE:],
            "dedupe_keys": cleaned_dedupe[-MAX_DEDUPE_HISTORY_SIZE:],
        }

        Path(HISTORY_FILE).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"✅ histórico salvo: {len(payload['ids'])} ids / {len(payload['dedupe_keys'])} dedupe_keys")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar histórico: {e}")
        return False


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


def save_latest(offers: List[Dict]) -> bool:
    try:
        payload = {
            "last_update": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "offers": offers,
        }
        Path(LATEST_FILE).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"✅ latest_offers salvo: {len(offers)} ofertas")
        return True
    except Exception as e:
        log(f"❌ erro ao salvar latest_offers: {e}")
        return False


def load_daily_log() -> Dict:
    path = Path(DAILY_LOG_FILE)
    default = {
        "date": "",
        "message_id": None,
        "last_success_check": "",
        "last_new_offer_at": "",
        "pending_count": 0,
        "last_consumer_run": "",
        "lines": [],
    }
    if not path.exists():
        return default

    data = safe_json_load(path, default)
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
        "lines": [str(x) for x in lines][-30:],
    }


def save_daily_log(data: Dict) -> bool:
    try:
        Path(DAILY_LOG_FILE).write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return True
    except Exception as e:
        log(f"❌ erro ao salvar daily_log.json: {e}")
        return False


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

    data = safe_json_load(path, default)
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
        Path(STATUS_RUNTIME_FILE).write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
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


def status_consumer_finish(
    summary: str,
    processed: int,
    sent: int,
    failed: int,
    pending_count: int,
    status_value: str,
    last_error: str = "",
) -> None:
    status = load_status_runtime()
    last_success_at = status["consumer"].get("last_success_at", "")
    if status_value in {"ok", "sem_novidade", "parcial"} and sent > 0:
        last_success_at = now_br_datetime()

    status["consumer"] = {
        "last_started_at": status["consumer"].get("last_started_at", ""),
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


def build_dashboard_text(state: Dict) -> str:
    status = load_status_runtime()
    scriptable = status.get("scriptable", {}) if isinstance(status, dict) else {}
    scraper = status.get("scraper", {}) if isinstance(status, dict) else {}
    consumer = status.get("consumer", {}) if isinstance(status, dict) else {}
    global_status = status.get("global", {}) if isinstance(status, dict) else {}

    pending_count = int(state.get("pending_count") or 0)

    def parse_br_dt(value: str) -> Optional[datetime]:
        raw = str(value or "").strip()
        if not raw:
            return None
        for fmt in ("%d/%m/%Y às %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=BR_TZ)
            except Exception:
                pass
        return None

    def fmt_relative(value: str) -> str:
        dt = parse_br_dt(value)
        if not dt:
            return "—"
        delta = now_br() - dt
        total_min = max(0, int(delta.total_seconds() // 60))
        if total_min < 1:
            rel = "agora"
        elif total_min < 60:
            rel = f"há {total_min}min"
        elif total_min < 1440:
            h = total_min // 60
            m = total_min % 60
            rel = f"há {h}h{m:02d}min" if m else f"há {h}h"
        else:
            d = total_min // 1440
            rem = total_min % 1440
            h = rem // 60
            rel = f"há {d}d{h:02d}h" if h else f"há {d}d"
        return f"{rel} às {dt.strftime('%H:%M')}"

    def component_line(label: str, data: Dict) -> str:
        status_value = str(data.get("status") or "").strip().lower()
        finished = str(data.get("last_finished_at") or "")
        started = str(data.get("last_started_at") or "")
        when = finished or started

        if status_value == "ok":
            icon = "🟢" if label != "consumer" else "✅"
            text = "online" if label != "consumer" else "pronto"
        elif status_value == "sem_novidade":
            icon = "⚪"
            text = "em espera" if label != "consumer" else "ocioso"
        elif status_value == "running":
            icon = "🔵"
            text = "ativo"
        elif status_value == "parcial":
            icon = "🟡"
            text = "instável"
        elif status_value == "erro":
            icon = "🔴" if label == "scriptable" else "🟡"
            text = "erro" if label == "scriptable" else "instável"
        else:
            icon = "⚪"
            text = "em espera"

        return f"{icon} {text.capitalize()} ({fmt_relative(when)})"

    def mood_text() -> str:
        if pending_count > 0:
            return "Fila aquecida"
        if str(consumer.get("status") or "") in {"erro", "parcial"}:
            return "Atenção no consumer"
        if str(scraper.get("status") or "") in {"erro", "parcial"}:
            return "Atenção no scraper"
        if str(scriptable.get("status") or "") in {"erro", "parcial"}:
            return "Atenção no scriptable"
        return "Tudo calmo"

    def environment_text() -> str:
        if pending_count > 0:
            return "Alta"
        if str(scraper.get("status") or "") == "ok" and str(scriptable.get("status") or "") == "ok":
            return "Alta"
        if str(scraper.get("status") or "") in {"parcial", "erro"} or str(scriptable.get("status") or "") in {"parcial", "erro"}:
            return "Moderada"
        return "Baixa"

    def silence_since_text() -> str:
        value = str(state.get("last_new_offer_at") or global_status.get("last_offer_at") or "").strip()
        dt = parse_br_dt(value)
        if not dt:
            return "—"
        delta = now_br() - dt
        total_min = max(0, int(delta.total_seconds() // 60))
        days = total_min // 1440
        hours = (total_min % 1440) // 60
        mins = total_min % 60
        if days > 0:
            return f"{days}d{hours:02d}h sem oferta nova"
        if hours > 0:
            return f"{hours}h{mins:02d}m sem oferta nova"
        return f"{mins}min sem oferta nova"

    last_offer_title = str(global_status.get("last_offer_title") or "").strip() or "Não disponível"
    last_offer_at = str(global_status.get("last_offer_at") or state.get("last_new_offer_at") or "").strip() or "—"
    pending_label = "📭 Limpa" if pending_count == 0 else f"🚀 {pending_count} ofertas aguardando"

    lines = [
        f"📊 <b>Monitor Clube Uol ({escape_html(now_br().strftime('%H:%M'))})</b>",
        "",
        f"📱 Scriptable {escape_html(component_line('scriptable', scriptable))}",
        f"🤖 Scraper {escape_html(component_line('scraper', scraper))}",
        f"📦 Consumer {escape_html(component_line('consumer', consumer))}",
        "",
        f"🎯 Última captura 🕒 {escape_html(last_offer_at)}",
        f"↳ {escape_html(last_offer_title)}",
        f"⏳ {escape_html(silence_since_text())}",
        "",
        f"📦 Fila de processamento: {escape_html(pending_label)}",
        "",
        f"🌤️ Humor do sistema: {escape_html(mood_text())}",
        f"🧭 Leitura do ambiente: {escape_html(environment_text())}",
    ]

    return truncate_text("\n".join(lines), MAX_DASHBOARD_LENGTH)


def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def parse_retry_after(response: Optional[requests.Response]) -> int:
    try:
        if response is None:
            return 0
        data = response.json()
        return int(data.get("parameters", {}).get("retry_after") or 0)
    except Exception:
        return 0


def telegram_post(method: str, data=None, files=None, retry_429: bool = True) -> requests.Response:
    resp = requests.post(
        telegram_api(method),
        data=data or {},
        files=files,
        timeout=REQUEST_TIMEOUT,
    )
    if retry_429 and resp.status_code == 429:
        retry_after = parse_retry_after(resp)
        if retry_after > 0:
            wait_s = retry_after + RETRY_429_EXTRA_SECONDS
            log(f"429 no {method}, aguardando {wait_s}s para tentar de novo")
            time.sleep(wait_s)
            resp = requests.post(
                telegram_api(method),
                data=data or {},
                files=files,
                timeout=REQUEST_TIMEOUT,
            )
    return resp


def sync_daily_dashboard(state: Dict) -> None:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return

    old_message_id = state.get("message_id")
    state["date"] = now_br_date()
    state["lines"] = state.get("lines", [])[-20:]
    text = build_dashboard_text(state)

    try:
        resp = telegram_post(
            "sendMessage",
            data={
                "chat_id": GRUPO_COMENTARIO_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": "true",
                "disable_web_page_preview": "true",
            },
        )
        if resp.ok:
            data = resp.json()
            new_message_id = data.get("result", {}).get("message_id")
            state["message_id"] = new_message_id
            save_daily_log(state)

            if old_message_id and str(old_message_id) != str(new_message_id):
                try:
                    del_resp = telegram_post(
                        "deleteMessage",
                        data={
                            "chat_id": GRUPO_COMENTARIO_ID,
                            "message_id": str(old_message_id),
                        },
                        retry_429=False,
                    )
                    if not del_resp.ok and '"message to delete not found"' not in del_resp.text:
                        log(f"⚠️ falha ao deletar dashboard anterior: {del_resp.text}")
                except Exception as e:
                    log(f"⚠️ erro ao deletar dashboard anterior: {e}")
        else:
            log(f"⚠️ falha ao publicar dashboard: {resp.text}")
    except Exception as e:
        log(f"⚠️ erro ao publicar dashboard: {e}")


def append_dashboard_line(source: str, status_line: str) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state = {
            "date": now_br_date(),
            "message_id": None,
            "last_success_check": "",
            "last_new_offer_at": state.get("last_new_offer_at", ""),
            "pending_count": 0,
            "last_consumer_run": "",
            "lines": [],
        }

    line = f"[{now_br_time()}] {source}: {status_line}"
    state["lines"].append(line)
    state["lines"] = state["lines"][-30:]
    sync_daily_dashboard(state)


def set_dashboard_pending_count(count: int) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["pending_count"] = count
    sync_daily_dashboard(state)


def set_dashboard_last_consumer_run() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["last_consumer_run"] = now_br_datetime()
    sync_daily_dashboard(state)


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

    seen = set()
    ordered = []
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
    if "/campanhasdeingresso/" in (link or "").lower():
        return f"‼️ {title} ‼️"
    return title


def normalize_validity(validity: Optional[str]) -> str:
    val = clean_multiline_text(validity or "")
    if not val:
        return ""
    if not val.endswith("."):
        val += "."
    val = re.sub(r" \.", ".", val)
    return val


def build_main_caption(title: str, description: str, validity: Optional[str], link: str, sold_out_at: Optional[str] = None) -> str:
    tags = build_smart_hashtags(title, description, link)
    decorated_title = decorate_main_title(title, link)

    if str(sold_out_at or "").strip():
        decorated_title = f"[ESGOTADO] {decorated_title}"

    body = [f"<b>{escape_html(decorated_title)}</b>"]

    if tags:
        body.append(escape_html(" ".join(tags)))

    val = normalize_validity(validity)
    if val:
        body.append(f"📅 {escape_html(val)}")

    if str(sold_out_at or "").strip():
        body.append(f"❌ Oferta esgotada às {escape_html(str(sold_out_at).strip())}.")

    body.append(f"🔗 {escape_html(link)}")
    body.append("💬 Veja os detalhes completos dentro dos comentários.")

    return truncate_text("\n\n".join(body), MAX_CAPTION_LENGTH)


def split_description_sections(description: str) -> List[str]:
    desc = clean_multiline_text(description)
    if not desc:
        return []

    desc = re.sub(r"\s*•\s*", "\n• ", desc)
    desc = re.sub(r"(?i)\s*(Atenção,\s*Assinante UOL!)", r"\n\n\1", desc)
    desc = re.sub(r"(?i)\s*(Essa prática pode resultar)", r"\n\n\1", desc)
    desc = re.sub(r"(?i)\s*(Valorize seu benefício\.?\s*Use com responsabilidade!?)", r"\n\n\1", desc)
    desc = re.sub(r"\n{3,}", "\n\n", desc).strip()

    lines = [x.strip() for x in desc.splitlines() if x.strip()]
    sections = []
    current = []

    def flush():
        nonlocal current
        if current:
            section = "\n".join(current).strip()
            if section:
                sections.append(section)
            current = []

    section_starts = [
        "sobre o parceiro:",
        "sobre a cacau show:",
        "benefício:",
        "beneficio:",
        "regras:",
        "regras do benefício:",
        "regras do beneficio:",
        "como resgatar",
        "passo a passo para resgate:",
        "data:",
        "quando:",
        "local:",
        "atenção!",
        "atencao!",
        "atenção:",
        "atencao:",
        "atenção, assinante uol!",
        "importante:",
        "📌 regras de resgate:",
        "regras de resgate:",
        "essa prática pode resultar",
        "valorize seu benefício",
        "•",
    ]

    for line in lines:
        low = line.lower()
        if any(low.startswith(s) for s in section_starts):
            flush()
        current.append(line)
    flush()

    normalized_sections = []
    bullet_buffer = []
    for section in sections:
        low = section.lower()
        if low.startswith("•"):
            bullet_buffer.append(section)
            continue
        if bullet_buffer:
            normalized_sections.append("\n".join(bullet_buffer))
            bullet_buffer = []
        normalized_sections.append(section)
    if bullet_buffer:
        normalized_sections.append("\n".join(bullet_buffer))

    return normalized_sections


def beautify_section(section: str) -> str:
    raw = clean_multiline_text(section).strip()
    low = raw.lower()

    def split_label(text: str) -> tuple[str, str]:
        m = re.match(r"^\s*([^:!]+?)\s*[:!]\s*(.*)$", text, flags=re.S)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return text.strip(), ""

    if low.startswith("data"):
        title, rest = split_label(raw)
        return f"🗓️ <b>{escape_html(title)}:</b>\n{escape_html(rest)}" if rest else f"🗓️ <b>{escape_html(title)}:</b>"

    if low.startswith("quando"):
        title, rest = split_label(raw)
        return f"🗓️ <b>{escape_html(title)}:</b>\n{escape_html(rest)}" if rest else f"🗓️ <b>{escape_html(title)}:</b>"

    if low.startswith("local"):
        title, rest = split_label(raw)
        return f"📍 <b>{escape_html(title)}:</b>\n{escape_html(rest)}" if rest else f"📍 <b>{escape_html(title)}:</b>"

    if low.startswith("importante"):
        title, rest = split_label(raw)
        return f"❗ <b>{escape_html(title)}:</b> {escape_html(rest)}".strip()

    if low.startswith("regras de resgate") or low.startswith("📌 regras de resgate"):
        cleaned = raw.replace("📌", "").strip()
        title, rest = split_label(cleaned)
        rendered = [f"📌 <b>{escape_html(title.upper())}:</b>"]
        if rest:
            rendered.append(escape_html(rest))
        return "\n\n".join(rendered)

    if low.startswith("•"):
        bullets = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if not line.startswith("•"):
                line = f"• {line}"
            bullets.append(escape_html(line))
        return "\n".join(bullets)

    if low.startswith("atenção") or low.startswith("atencao"):
        title, rest = split_label(raw)
        if rest:
            return f"❗ <b>{escape_html(title)}:</b> {escape_html(rest)}"
        return f"❗ <b>{escape_html(raw)}</b>"

    if low.startswith("essa prática pode resultar"):
        return escape_html(raw)

    if low.startswith("valorize seu benefício"):
        return escape_html(raw)

    if low.startswith("benefício") or low.startswith("beneficio"):
        title, rest = split_label(raw)
        return f"<b>{escape_html(title)}:</b> {escape_html(rest)}".strip()

    if low.startswith("sobre o parceiro"):
        title, rest = split_label(raw)
        return f"<b>{escape_html(title)}:</b> {escape_html(rest)}".strip()

    return escape_html(raw)


def build_comment_text(title: str, description: str, validity: Optional[str], link: str) -> str:
    desc = clean_multiline_text(description)

    replacements = [
        (r"\b(Data)\s*:\s*", r"\n\nData: "),
        (r"\b(Quando)\s*:\s*", r"\n\nQuando: "),
        (r"\b(Local)\s*:\s*", r"\n\nLocal: "),
        (r"\b(Importante)\s*:\s*", r"\n\nImportante: "),
        (r"\b(REGRAS DE RESGATE)\s*:\s*", r"\n\nREGRAS DE RESGATE: "),
        (r"\b(Atenção,\s*Assinante UOL!)\s*", r"\n\nAtenção, Assinante UOL! "),
        (r"\b(Essa prática pode resultar)\s*", r"\n\nEssa prática pode resultar"),
        (r"\b(Valorize seu benefício\.?\s*Use com responsabilidade!?)\s*", r"\n\n\1"),
    ]
    for pattern, repl in replacements:
        desc = re.sub(pattern, repl, desc, flags=re.I)

    desc = re.sub(r"(?<!\n)•\s*", r"\n• ", desc)
    desc = re.sub(r"\n{3,}", "\n\n", desc).strip()

    sections = split_description_sections(desc)
    out = [f"📋 <b>{escape_html(title)}</b>", ""]

    if sections:
        for idx, section in enumerate(sections):
            rendered = beautify_section(section)
            if rendered:
                out.append(rendered)
                if idx != len(sections) - 1:
                    out.append("")
    else:
        paragraphs = [p.strip() for p in desc.split("\n\n") if p.strip()]
        for idx, p in enumerate(paragraphs):
            out.append(escape_html(p))
            if idx != len(paragraphs) - 1:
                out.append("")

    text = "\n".join(out)

    text = re.sub(
        r"(Atenção,\s*Assinante UOL!)",
        r"<b>\1</b>",
        text,
        flags=re.I,
    )
    text = re.sub(
        r"(A venda)(.*?)(é proibida\.?)",
        r"<b>\1</b>\2<b>\3</b>",
        text,
        flags=re.I | re.S,
    )
    text = re.sub(
        r"(banimento imediato)",
        r"<b>\1</b>",
        text,
        flags=re.I,
    )
    text = re.sub(
        r"(cancelamento dos ingressos já resgatados\.?)",
        r"<b>\1</b>",
        text,
        flags=re.I,
    )

    val = normalize_validity(validity)
    if val:
        strong_val = re.sub(
            r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})",
            r"<b>\1</b>",
            escape_html(val),
        )
        out_tail = ["", f"📅 {strong_val}", "", f"🔗 {escape_html(link)}"]
        text = text.rstrip() + "\n" + "\n".join(out_tail)
    else:
        text = text.rstrip() + f"\n\n🔗 {escape_html(link)}"

    return truncate_text(text.strip(), MAX_COMMENT_LENGTH)


def send_message_text(chat_id: str, text: str, disable_notification: bool = False, reply_to_message_id: Optional[int] = None) -> requests.Response:
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
        "disable_notification": "true" if disable_notification else "false",
    }
    if reply_to_message_id:
        data["reply_to_message_id"] = str(reply_to_message_id)
    return telegram_post("sendMessage", data=data)


def download_image_bytes(url: str) -> Optional[Tuple[bytes, str]]:
    if not url:
        return None
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        if not resp.ok or not resp.content:
            return None
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if "png" in content_type:
            ext = "png"
        elif "webp" in content_type:
            ext = "webp"
        else:
            ext = "jpg"
        return resp.content, ext
    except Exception as e:
        log(f"erro ao baixar imagem: {e}")
        return None


def send_photo_bytes(
    chat_id: str,
    image_bytes: bytes,
    ext: str,
    caption: Optional[str] = None,
    disable_notification: bool = False,
    reply_to_message_id: Optional[int] = None,
) -> requests.Response:
    filename = f"offer.{ext or 'jpg'}"
    data = {
        "chat_id": chat_id,
        "disable_notification": "true" if disable_notification else "false",
    }
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    if reply_to_message_id:
        data["reply_to_message_id"] = str(reply_to_message_id)
    files = {
        "photo": (filename, image_bytes),
    }
    return telegram_post("sendPhoto", data=data, files=files)


def send_media_group_bytes(
    chat_id: str,
    media_items: List[Tuple[bytes, str]],
    disable_notification: bool = False,
    reply_to_message_id: Optional[int] = None,
) -> requests.Response:
    media = []
    files = {}

    for idx, (image_bytes, ext) in enumerate(media_items):
        attach_name = f"file{idx}"
        filename = f"offer_{idx}.{ext or 'jpg'}"
        media.append({
            "type": "photo",
            "media": f"attach://{attach_name}",
        })
        files[attach_name] = (filename, image_bytes)

    data = {
        "chat_id": chat_id,
        "media": json.dumps(media, ensure_ascii=False),
        "disable_notification": "true" if disable_notification else "false",
    }
    if reply_to_message_id:
        data["reply_to_message_id"] = str(reply_to_message_id)

    return telegram_post("sendMediaGroup", data=data, files=files)


def wait_for_discussion_message_id(channel_message_id: int, attempts: int = DISCUSSION_WAIT_ATTEMPTS, sleep_s: int = DISCUSSION_WAIT_SLEEP_SECONDS) -> Optional[int]:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return None

    for attempt in range(1, attempts + 1):
        time.sleep(sleep_s)
        try:
            resp = requests.get(telegram_api("getUpdates"), timeout=REQUEST_TIMEOUT)
            if not resp.ok:
                log(f"getUpdates falhou na tentativa {attempt}: {resp.text}")
                continue

            updates = resp.json().get("result", [])
            for update in reversed(updates):
                msg = update.get("message") or update.get("channel_post")
                if msg and msg.get("forward_from_message_id") == channel_message_id:
                    discussion_id = msg.get("message_id")
                    if discussion_id:
                        return discussion_id
        except Exception as e:
            log(f"erro ao buscar discussion_message_id na tentativa {attempt}: {e}")

    log(f"⚠️ discussion_message_id não encontrado para channel_message_id={channel_message_id}; usando fallback sem reply")
    return None


def send_offer_main(offer: Dict) -> Tuple[bool, Optional[int], str]:
    title = offer.get("title") or offer.get("preview_title") or "Oferta"
    description = offer.get("description") or ""
    validity = offer.get("validity")
    link = offer.get("link") or offer.get("original_link") or ""

    caption = build_main_caption(
        title,
        description,
        validity,
        link,
        sold_out_at=offer.get("sold_out_at"),
    )

    img_url = (offer.get("img_url") or "").strip()
    partner_img_url = (offer.get("partner_img_url") or "").strip()

    candidates = []
    if img_url:
        candidates.append(("img_url", img_url))
    if partner_img_url:
        candidates.append(("partner_img_url", partner_img_url))

    tags = build_smart_hashtags(title, description, link)
    silent = should_send_silent(tags)

    for label, candidate in candidates:
        img = download_image_bytes(candidate)
        if not img:
            log(f"falha ao baixar imagem via {label}")
            continue

        image_bytes, ext = img
        try:
            resp = send_photo_bytes(
                TELEGRAM_CHAT_ID,
                image_bytes,
                ext,
                caption=caption,
                disable_notification=silent,
            )
            if resp.ok:
                data = resp.json()
                return True, data.get("result", {}).get("message_id"), f"sendPhoto ok via {label}"
            log(f"sendPhoto upload falhou via {label}: {resp.text}")
        except Exception as e:
            log(f"sendPhoto upload exception via {label}: {e}")

    try:
        resp = send_message_text(
            TELEGRAM_CHAT_ID,
            caption,
            disable_notification=silent,
        )
        if resp.ok:
            data = resp.json()
            return True, data.get("result", {}).get("message_id"), "fallback sendMessage ok"
        return False, None, f"sendMessage falhou: {resp.text}"
    except Exception as e:
        return False, None, f"sendMessage exception: {e}"


def send_offer_comment(offer: Dict, channel_message_id: int) -> Tuple[bool, str]:
    title = offer.get("title") or offer.get("preview_title") or "Oferta"
    description = offer.get("description") or ""
    validity = offer.get("validity")
    link = offer.get("link") or offer.get("original_link") or ""

    discussion_message_id = wait_for_discussion_message_id(channel_message_id)
    reply_target = discussion_message_id
    events = []

    media_items = []

    offer_img_url = (offer.get("img_url") or "").strip()
    if offer_img_url:
        img = download_image_bytes(offer_img_url)
        if img:
            media_items.append(img)
        else:
            events.append("foto da oferta indisponível")

    partner_img_url = (offer.get("partner_img_url") or "").strip()
    if partner_img_url:
        img = download_image_bytes(partner_img_url)
        if img:
            media_items.append(img)
        else:
            events.append("foto do parceiro indisponível")

    if len(media_items) >= 2:
        try:
            resp = send_media_group_bytes(
                GRUPO_COMENTARIO_ID,
                media_items[:2],
                disable_notification=True,
                reply_to_message_id=reply_target,
            )
            if resp.ok:
                events.append("álbum com oferta + parceiro enviado")
            else:
                log(f"álbum falhou: {resp.text}")
                events.append("álbum falhou")
        except Exception as e:
            log(f"álbum exception: {e}")
            events.append("álbum exception")
    elif len(media_items) == 1:
        image_bytes, ext = media_items[0]
        try:
            resp = send_photo_bytes(
                GRUPO_COMENTARIO_ID,
                image_bytes,
                ext,
                caption=None,
                disable_notification=True,
                reply_to_message_id=reply_target,
            )
            if resp.ok:
                events.append("foto única enviada")
            else:
                log(f"foto única falhou: {resp.text}")
                events.append("foto única falhou")
        except Exception as e:
            log(f"foto única exception: {e}")
            events.append("foto única exception")

    text = build_comment_text(title, description, validity, link)
    try:
        resp = send_message_text(
            GRUPO_COMENTARIO_ID,
            text,
            disable_notification=True,
            reply_to_message_id=reply_target,
        )
        if resp.ok:
            events.append("descrição completa enviada")
            return True, " | ".join(events)
        return False, f"descrição completa falhou: {resp.text}"
    except Exception as e:
        return False, f"descrição completa exception: {e}"


def mark_offer_success(history: Dict[str, List[str]], offer: Dict) -> None:
    offer_id = normalize_offer_key(offer.get("id") or offer.get("link") or "")
    title = offer.get("title") or offer.get("preview_title") or ""
    validity = offer.get("validity")
    description = offer.get("description") or ""
    dedupe_key = offer.get("dedupe_key") or build_dedupe_key(title, validity, description)

    if offer_id:
        history.setdefault("ids", []).append(offer_id)
    if dedupe_key:
        history.setdefault("dedupe_keys", []).append(dedupe_key)


def refresh_sent_offers_with_sold_out() -> None:
    latest = safe_json_load(Path(LATEST_FILE), {"last_update": None, "offers": []})
    offers = latest.get("offers", [])
    if not isinstance(offers, list):
        return

    changed = False
    for offer in offers:
        sold_out_at = str(offer.get("sold_out_at") or "").strip()
        message_id = offer.get("channel_message_id")
        if not sold_out_at or not message_id:
            continue

        if offer.get("_sold_out_synced_to_telegram") is True:
            continue

        title = offer.get("title") or offer.get("preview_title") or "Oferta"
        description = offer.get("description") or ""
        validity = offer.get("validity")
        link = offer.get("link") or offer.get("original_link") or ""

        caption = build_main_caption(
            title,
            description,
            validity,
            link,
            sold_out_at=offer.get("sold_out_at"),
        )

        try:
            resp = telegram_post(
                "editMessageCaption",
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "message_id": str(message_id),
                    "caption": caption,
                    "parse_mode": "HTML",
                },
            )
            if resp.ok:
                offer["_sold_out_synced_to_telegram"] = True
                changed = True
            else:
                log(f"falha ao editar oferta esgotada no telegram: {resp.text}")
        except Exception as e:
            log(f"erro ao editar oferta esgotada no telegram: {e}")

    if changed:
        latest["offers"] = offers
        latest["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        Path(LATEST_FILE).write_text(
            json.dumps(latest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def consume_pending() -> int:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not GRUPO_COMENTARIO_ID:
        log("❌ variáveis TELEGRAM_TOKEN, TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórias")
        return 1

    refresh_sent_offers_with_sold_out()

    pending_data = load_pending()
    offers = pending_data.get("offers", [])

    if not offers:
        set_dashboard_pending_count(0)
        set_dashboard_last_consumer_run()
        status_consumer_finish(
            summary="pending vazio",
            processed=0,
            sent=0,
            failed=0,
            pending_count=0,
            status_value="sem_novidade",
        )
        append_dashboard_line("consumer", "📭 pending vazio")
        log("📭 nenhuma oferta pendente")
        return 0

    history = load_history()
    history_ids = set(history.get("ids", []))
    history_dedupe = set(history.get("dedupe_keys", []))

    status_consumer_start(len(offers))
    append_dashboard_line("consumer", f"▶️ processando {len(offers)} ofertas")
    set_dashboard_pending_count(len(offers))

    latest_snapshot = safe_json_load(Path(LATEST_FILE), {"last_update": None, "offers": []})
    latest_sent = latest_snapshot.get("offers", []) if isinstance(latest_snapshot.get("offers", []), list) else []
    remaining = []
    processed = 0
    sent = 0
    failed = 0
    last_error = ""

    try:
        for idx, offer in enumerate(offers, 1):
            processed += 1

            offer_id = normalize_offer_key(offer.get("id") or offer.get("link") or "")
            title = offer.get("title") or offer.get("preview_title") or ""
            validity = offer.get("validity")
            description = offer.get("description") or ""
            dedupe_key = offer.get("dedupe_key") or build_dedupe_key(title, validity, description)

            if offer_id in history_ids or (dedupe_key and dedupe_key in history_dedupe):
                log(f"oferta já está no histórico, removendo do pending: {title}")
                continue

            try:
                ok_main, channel_message_id, detail_main = send_offer_main(offer)
            except Exception as e:
                ok_main, channel_message_id, detail_main = False, None, f"send_offer_main exception: {e}"

            if not ok_main or not channel_message_id:
                failed += 1
                last_error = detail_main
                remaining.append(offer)
                append_dashboard_line("consumer", f"⚠️ falha principal: {title[:70]}")
                log(f"oferta mantida no pending por falha total: {detail_main}")
                continue

            offer["channel_message_id"] = channel_message_id

            try:
                ok_comment, detail_comment = send_offer_comment(offer, channel_message_id)
            except Exception as e:
                ok_comment, detail_comment = False, f"send_offer_comment exception: {e}"

            if not ok_comment:
                failed += 1
                last_error = detail_comment
                remaining.append(offer)
                append_dashboard_line("consumer", f"⚠️ falha comentário: {title[:70]}")
                log(f"oferta mantida no pending por falha no comentário: {detail_comment}")
                continue

            sent += 1
            latest_sent = [x for x in latest_sent if normalize_offer_key(x.get("id") or x.get("link") or "") != offer_id]
            latest_sent.append(offer)
            latest_sent = latest_sent[-20:]

            mark_offer_success(history, offer)
            history_ids.add(offer_id)
            if dedupe_key:
                history_dedupe.add(dedupe_key)

            append_dashboard_line("consumer", f"✅ enviada: {title[:80]}")
            log(f"oferta enviada com sucesso: {title}")

            if idx < len(offers):
                time.sleep(BETWEEN_OFFERS_DELAY_SECONDS)

    except Exception as e:
        failed += 1
        last_error = f"loop principal exception: {e}"
        log(f"❌ erro inesperado no consume_pending: {e}")

    save_history(history)
    save_pending(remaining)
    save_latest(latest_sent)

    if sent > 0:
        status = load_status_runtime()
        last_offer = latest_sent[-1] if latest_sent else None
        if last_offer:
            status["global"] = {
                "last_offer_title": str(last_offer.get("title") or last_offer.get("preview_title") or ""),
                "last_offer_at": now_br_datetime(),
                "last_offer_id": str(last_offer.get("id") or ""),
            }
            save_status_runtime(status)

            state = load_daily_log()
            state["last_new_offer_at"] = now_br_datetime()
            save_daily_log(state)

    set_dashboard_pending_count(len(remaining))
    set_dashboard_last_consumer_run()

    if sent > 0 and failed == 0:
        status_value = "ok"
        summary = f"{sent} enviada(s) com sucesso"
    elif sent > 0 and failed > 0:
        status_value = "parcial"
        summary = f"{sent} enviada(s), {failed} falha(s)"
    elif failed > 0:
        status_value = "erro"
        summary = f"nenhuma enviada, {failed} falha(s)"
    else:
        status_value = "sem_novidade"
        summary = "nenhuma oferta nova enviada"

    status_consumer_finish(
        summary=summary,
        processed=processed,
        sent=sent,
        failed=failed,
        pending_count=len(remaining),
        status_value=status_value,
        last_error=last_error,
    )

    append_dashboard_line(
        "consumer",
        f"{'✅' if sent > 0 else '⚠️'} processadas {processed} | enviadas {sent} | falhas {failed}",
    )

    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--pending":
        raise SystemExit(consume_pending())
    else:
        log("este arquivo está configurado para o modo consumer (--pending)")
        raise SystemExit(0)
