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
    today = state["date"] or now_br_date()
    last_success = state["last_success_check"] or "—"
    last_new = state["last_new_offer_at"] or "—"
    pending_count = state["pending_count"]
    last_consumer = state["last_consumer_run"] or "—"

    header = [
        f"📊 <b>relatório diário uol - {escape_html(today)}</b>",
        "",
        f"última leitura do site sem bloqueio: {escape_html(last_success)}",
        f"última oferta nova encontrada: {escape_html(last_new)}",
        f"pending atual: {escape_html(str(pending_count))}",
        f"última execução do consumer: {escape_html(last_consumer)}",
        "",
    ]

    lines = state.get("lines", [])
    body = [escape_html(x) for x in lines[-20:]] if lines else ["sem registros ainda"]
    return truncate_text("\n".join(header + body), MAX_DASHBOARD_LENGTH)


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

    text = build_dashboard_text(state)

    if state["date"] != now_br_date() or not state["message_id"]:
        state["date"] = now_br_date()
        state["message_id"] = None
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
                state["message_id"] = data.get("result", {}).get("message_id")
                save_daily_log(state)
            else:
                log(f"⚠️ falha ao criar dashboard diário: {resp.text}")
        except Exception as e:
            log(f"⚠️ erro ao criar dashboard diário: {e}")
        return

    try:
        resp = telegram_post(
            "editMessageText",
            data={
                "chat_id": GRUPO_COMENTARIO_ID,
                "message_id": state["message_id"],
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )
        if resp.ok:
            save_daily_log(state)
        else:
            if '"message is not modified"' not in resp.text:
                log(f"⚠️ falha ao editar dashboard diário: {resp.text}")
    except Exception as e:
        log(f"⚠️ erro ao editar dashboard diário: {e}")


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


def build_main_caption(title: str, description: str, validity: Optional[str], link: str) -> str:
    tags = build_smart_hashtags(title, description, link)
    decorated_title = decorate_main_title(title, link)

    body = [f"<b>{escape_html(decorated_title)}</b>"]
    if tags:
        body.append(escape_html(" ".join(tags)))

    val = normalize_validity(validity)
    if val:
        body.append(f"📅 {escape_html(val)}")
    body.append(f"🔗 {escape_html(link)}")
    body.append("💬 Veja os detalhes completos dentro dos comentários.")

    return truncate_text("\n\n".join(body), MAX_CAPTION_LENGTH)


def split_description_sections(description: str) -> List[str]:
    desc = clean_multiline_text(description)
    if not desc:
        return []

    lines = [x.strip() for x in desc.splitlines() if x.strip()]
    sections = []
    current = []

    def flush():
        nonlocal current
        if current:
            sections.append("\n".join(current).strip())
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
        "local:",
        "atenção!",
        "atencao!",
        "atenção:",
        "atencao:",
        "importante:",
        "📌 regras de resgate:",
        "regras de resgate:",
    ]

    for line in lines:
        low = line.lower()
        if any(low.startswith(s) for s in section_starts):
            flush()
        current.append(line)
    flush()

    return sections


def beautify_section(section: str) -> str:
    raw = section.strip()
    low = raw.lower()

    if low.startswith("data:"):
        title, rest = raw.split(":", 1)
        return f"<b>{escape_html(title.strip())}:</b>\n{escape_html(rest.strip())}"

    if low.startswith("local:"):
        title, rest = raw.split(":", 1)
        return f"📍 <b>{escape_html(title.strip())}:</b>\n{escape_html(rest.strip())}"

    if low.startswith("importante:"):
        title, rest = raw.split(":", 1)
        return f"❗ <b>{escape_html(title.strip())}:</b> {escape_html(rest.strip())}"

    if low.startswith("atenção:") or low.startswith("atencao:"):
        title, rest = raw.split(":", 1)
        return f"❗ <b>{escape_html(title.strip())}:</b> {escape_html(rest.strip())}"

    if low.startswith("atenção!") or low.startswith("atencao!"):
        title, rest = raw.split("!", 1)
        rest = rest.strip()
        if rest:
            return f"❗ <b>{escape_html(title.strip())}!</b>\n{escape_html(rest)}"
        return f"❗ <b>{escape_html(title.strip())}!</b>"

    if low.startswith("regras de resgate:") or low.startswith("📌 regras de resgate:"):
        cleaned = raw.replace("📌", "").strip()
        title, rest = cleaned.split(":", 1)
        rest = rest.strip()
        if rest:
            return f"📌 <b>{escape_html(title.strip().upper())}:</b>\n{escape_html(rest)}"
        return f"📌 <b>{escape_html(title.strip().upper())}:</b>"

    if low.startswith("benefício:") or low.startswith("beneficio:"):
        title, rest = raw.split(":", 1)
        return f"<b>{escape_html(title.strip())}:</b> {escape_html(rest.strip())}"

    if low.startswith("sobre o parceiro:"):
        title, rest = raw.split(":", 1)
        return f"<b>{escape_html(title.strip())}:</b> {escape_html(rest.strip())}"

    return escape_html(raw)


def build_comment_text(title: str, description: str, validity: Optional[str], link: str) -> str:
    sections = split_description_sections(description)
    out = [f"📋 <b>{escape_html(title)}</b>", ""]

    if sections:
        for idx, section in enumerate(sections):
            rendered = beautify_section(section)
            out.append(rendered)

            if idx != len(sections) - 1:
                out.append("")
    else:
        desc = clean_multiline_text(description)
        if desc:
            paragraphs = [p.strip() for p in desc.split("\n\n") if p.strip()]
            for idx, p in enumerate(paragraphs):
                out.append(escape_html(p))
                if idx != len(paragraphs) - 1:
                    out.append("")

    val = normalize_validity(validity)
    if val:
        out.append("")
        out.append(f"📅 {escape_html(val)}")

    out.append("")
    out.append(f"🔗 {escape_html(link)}")

    text = "\n".join(out).strip()
    return truncate_text(text, MAX_COMMENT_LENGTH)


def wait_for_discussion_message_id(channel_message_id: int, attempts: int = 5, sleep_s: int = 2) -> Optional[int]:
    if not TELEGRAM_TOKEN or not GRUPO_COMENTARIO_ID:
        return None

    for _ in range(attempts):
        time.sleep(sleep_s)
        try:
            resp = requests.get(telegram_api("getUpdates"), timeout=REQUEST_TIMEOUT)
            if not resp.ok:
                continue

            updates = resp.json().get("result", [])
            for update in reversed(updates):
                msg = update.get("message", {}) or {}
                chat = msg.get("chat", {}) or {}
                if str(chat.get("id")) != str(GRUPO_COMENTARIO_ID):
                    continue

                reply = msg.get("reply_to_message", {}) or {}
                if reply.get("message_id") == channel_message_id:
                    return msg.get("message_id")

                forward_origin = msg.get("forward_origin", {}) or {}
                if forward_origin.get("message_id") == channel_message_id:
                    return msg.get("message_id")
        except Exception:
            continue

    return None


def download_image_bytes(url: str) -> Optional[Tuple[bytes, str]]:
    if not url:
        return None
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            return None

        content_type = resp.headers.get("content-type", "").lower()
        if "image/" not in content_type and not resp.content:
            return None

        ext = "jpg"
        if "png" in content_type:
            ext = "png"
        elif "webp" in content_type:
            ext = "webp"
        elif "jpeg" in content_type:
            ext = "jpg"

        return resp.content, ext
    except Exception:
        return None


def send_message_text(chat_id: str, text: str, disable_notification: bool = False, reply_to_message_id: Optional[int] = None) -> requests.Response:
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }
    if disable_notification:
        data["disable_notification"] = "true"
    if reply_to_message_id:
        data["reply_to_message_id"] = str(reply_to_message_id)
    return telegram_post("sendMessage", data=data)


def send_photo_bytes(
    chat_id: str,
    image_bytes: bytes,
    ext: str,
    caption: Optional[str] = None,
    disable_notification: bool = False,
    reply_to_message_id: Optional[int] = None,
) -> requests.Response:
    data = {
        "chat_id": chat_id,
    }
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    if disable_notification:
        data["disable_notification"] = "true"
    if reply_to_message_id:
        data["reply_to_message_id"] = str(reply_to_message_id)

    files = {
        "photo": (f"image.{ext}", image_bytes),
    }
    return telegram_post("sendPhoto", data=data, files=files)


def send_offer_main(offer: Dict) -> Tuple[bool, Optional[int], str]:
    title = offer.get("title") or offer.get("preview_title") or "Oferta"
    description = offer.get("description") or ""
    validity = offer.get("validity")
    link = offer.get("link") or offer.get("original_link") or ""
    caption = build_main_caption(title, description, validity, link)

    img_url = (offer.get("img_url") or "").strip()
    partner_img_url = (offer.get("partner_img_url") or "").strip()

    candidates = []
    if img_url:
        candidates.append(("img_url", img_url))
    if partner_img_url and partner_img_url != img_url:
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

    partner_img_url = (offer.get("partner_img_url") or "").strip()

    if partner_img_url:
        img = download_image_bytes(partner_img_url)
        if img:
            image_bytes, ext = img
            try:
                resp = send_photo_bytes(
                    GRUPO_COMENTARIO_ID,
                    image_bytes,
                    ext,
                    caption=None,
                    disable_notification=True,
                    reply_to_message_id=reply_target,
                )
                if not resp.ok:
                    log(f"foto do parceiro falhou: {resp.text}")
            except Exception as e:
                log(f"foto do parceiro exception: {e}")

    text = build_comment_text(title, description, validity, link)
    try:
        resp = send_message_text(
            GRUPO_COMENTARIO_ID,
            text,
            disable_notification=True,
            reply_to_message_id=reply_target,
        )
        if resp.ok:
            return True, "descrição completa enviada"
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


def consume_pending() -> int:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not GRUPO_COMENTARIO_ID:
        log("❌ variáveis TELEGRAM_TOKEN, TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórias")
        return 1

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

    latest_sent = []
    remaining = []
    processed = 0
    sent = 0
    failed = 0
    last_error = ""

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

        ok_main, channel_message_id, detail_main = send_offer_main(offer)
        if not ok_main or not channel_message_id:
            failed += 1
            last_error = detail_main
            remaining.append(offer)
            log(f"oferta mantida no pending por falha total: {detail_main}")
            continue

        ok_comment, detail_comment = send_offer_comment(offer, channel_message_id)
        if not ok_comment:
            failed += 1
            last_error = detail_comment
            remaining.append(offer)
            log(f"oferta mantida no pending por falha no comentário: {detail_comment}")
            continue

        sent += 1
        latest_sent.append(offer)
        mark_offer_success(history, offer)
        history_ids.add(offer_id)
        if dedupe_key:
            history_dedupe.add(dedupe_key)

        append_dashboard_line("consumer", f"✅ enviada: {title[:80]}")
        log(f"oferta enviada com sucesso: {title}")

        if idx < len(offers):
            time.sleep(BETWEEN_OFFERS_DELAY_SECONDS)

    save_history(history)
    save_pending(remaining)

    if latest_sent:
        save_latest(latest_sent[-20:])

        status = load_status_runtime()
        last_offer = latest_sent[-1]
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
    else:
        status_value = "erro"
        summary = f"nenhuma enviada, {failed} falha(s)"

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
