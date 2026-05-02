# bot_leouol.py
# consumer do pending_offers.json + envio para telegram + dashboard diário
# com upload real de imagem, retry para 429, comentário em múltiplas mensagens
# e histórico atualizado só após sucesso real

import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

import requests
from status_runtime_utils import load_status_runtime_file, merge_component_status_file

BR_TZ = ZoneInfo("America/Sao_Paulo")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
CANAL2_ID = os.environ.get("CANAL2_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")
DASHBOARD_CHAT_ID = os.environ.get("DASHBOARD_CHAT_ID") or GRUPO_COMENTARIO_ID
ENABLE_SOLD_OUT_UNDERLINE = str(os.environ.get("ENABLE_SOLD_OUT_UNDERLINE") or "").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_AGGRESSIVE_HASHTAGS = str(os.environ.get("ENABLE_AGGRESSIVE_HASHTAGS") or "").strip().lower() in {"1", "true", "yes", "on"}

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
LATEST_FILE = "latest_offers.json"
DAILY_LOG_FILE = "daily_log.json"
STATUS_RUNTIME_FILE = "status_runtime.json"
PIPELINE_AUDIT_FILE = "pipeline_audit.jsonl"

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
MAX_PENDING_AGE_HOURS = max(1, int(str(os.environ.get("MAX_PENDING_AGE_HOURS") or "6").strip() or "6"))
MAX_CONSUMER_BATCH = max(1, int(str(os.environ.get("MAX_CONSUMER_BATCH") or "8").strip() or "8"))
MAX_VALID_FROM_AGE_HOURS = max(1, int(str(os.environ.get("MAX_VALID_FROM_AGE_HOURS") or "36").strip() or "36"))
RECENT_RESEND_BLOCK_HOURS = max(1, int(str(os.environ.get("RECENT_RESEND_BLOCK_HOURS") or "168").strip() or "168"))

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
    "#comerbeber": [
        "bloomin onion",
        "cinnamon oblivion",
        "vinho",
        "vinhos",
        "cerveja",
        "cervejas",
        "banquete",
        "jantar",
        "almoço",
        "almoco",
        "sobremesa",
        "restaurante",
    ],
    "#cursos": ["curso", "cursos", "inglês", "ingles", "english"],
    "#compraspresentes": ["ovo de páscoa", "ovo de pascoa", "vivara", "presente"],
    "#educacao": [
        "curso",
        "cursos",
        "inglês",
        "ingles",
        "english",
        "graduações",
        "graduacoes",
        "graduação",
        "graduacao",
        "pós",
        "pos",
        "ead",
        "aprender",
        "enem",
    ],
    "#viagem": ["viagem", "viagens"],
    "#eletrodomesticoseletronicos": ["dell", "lg", "eletro", "geladeira", "lavadora"],
}

SILENT_HASHTAGS = {
    "#servicos",
    "#beleza",
    "#cursos",
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
    "#viagem",
    "#cursos",
    "#compraspresentes",
    "#servicos",
    "#beleza",
    "#educacao",
    "#eletrodomesticoseletronicos",
]

AGGRESSIVE_HASHTAG_HINTS = {
    "#comerbeber": ["ifood", "restaurante", "hamburguer", "pizza", "cupom", "desconto em comida"],
    "#entretenimentoviagens": ["viagem", "hotel", "hospedagem", "passagem", "turismo"],
    "#compraspresentes": ["presente", "joia", "perfume", "relogio", "relógio"],
    "#eletrodomesticoseletronicos": ["notebook", "smartphone", "fone", "tv", "monitor", "airfryer"],
    "#servicos": ["consulta", "servico", "serviço", "assistencia", "assistência"],
}

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://clube.uol.com.br/",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


def is_bad_offer_image_url(url: Optional[str]) -> bool:
    u = str(url or "").strip().lower()
    if not u:
        return True
    return (
        "/static/images/clubes/uol/categorias/" in u
        or "ingressosexclusivos-hover" in u
        or "ingressos-hover" in u
        or "loader.gif" in u
    )


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_trace_id(value: str) -> str:
    key = normalize_offer_key(value)
    return f"trace_{key}" if key else ""


def get_offer_trace_id(offer: Dict) -> str:
    trace_id = str(offer.get("trace_id") or "").strip()
    if trace_id:
        return trace_id
    source = str(offer.get("id") or offer.get("link") or offer.get("original_link") or "")
    trace_id = build_trace_id(source)
    if trace_id:
        offer["trace_id"] = trace_id
    return trace_id


def append_pipeline_audit(stage: str, trace_id: str, extra: Optional[Dict] = None) -> None:
    trace = str(trace_id or "").strip()
    if not trace:
        return
    payload = {"timestamp_utc": utc_now_iso(), "stage": str(stage or "").strip(), "trace_id": trace}
    if isinstance(extra, dict):
        payload.update(extra)
    try:
        with Path(PIPELINE_AUDIT_FILE).open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as e:
        log(f"⚠️ falha ao registrar auditoria ({stage}): {e}")


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


def canonical_key(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    raw = unquote(raw)
    raw = raw.replace("\u00a0", " ")
    mojibake_replacements = {
        "Ã¡": "á",
        "Ã ": "à",
        "Ã¢": "â",
        "Ã£": "ã",
        "Ã©": "é",
        "Ãª": "ê",
        "Ã­": "í",
        "Ã³": "ó",
        "Ã´": "ô",
        "Ãµ": "õ",
        "Ãº": "ú",
        "Ã§": "ç",
        "Ã‰": "É",
        "Ã‡": "Ç",
    }
    for bad, good in mojibake_replacements.items():
        raw = raw.replace(bad, good)

    raw = raw.lower()
    raw = raw.split("?")[0].split("#")[0]
    raw = raw.replace("&", " e ")
    raw = raw.replace("º", "o").replace("ª", "a")
    raw = re.sub(r"[\s_]+", "-", raw)
    raw = unicodedata.normalize("NFKD", raw)
    raw = "".join(ch for ch in raw if unicodedata.category(ch) != "Mn")
    raw = re.sub(r"[^a-z0-9-]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw)
    raw = raw.strip("-")

    known_fixes = {
        "ltima": "ultima",
        "ltimo": "ultimo",
        "seleo": "selecao",
        "graduao": "graduacao",
        "grtis": "gratis",
        "ms": "imas",
        "preo": "preco",
    }
    for bad, good in known_fixes.items():
        raw = re.sub(rf"(^|-){re.escape(bad)}(?=-|$)", lambda m: f"{m.group(1)}{good}", raw)
    raw = re.sub(r"(^|-)ps(?=-|$)", lambda m: f"{m.group(1)}pos", raw)
    raw = raw.replace("-at-", "-ate-")
    raw = re.sub(r"-{2,}", "-", raw).strip("-")
    return raw


def normalize_text_key(value: Optional[str]) -> str:
    return canonical_key(value)


def get_offer_id(link: str) -> str:
    try:
        raw = str(link or "").strip()
        if not raw:
            return ""
        parsed = urlparse(raw)
        if parsed.scheme and parsed.netloc:
            path = parsed.path or ""
        else:
            path = raw.split("?")[0].split("#")[0]
        return unquote(path.rstrip("/").split("/")[-1])
    except Exception:
        return str(link or "").strip()


def normalize_offer_key_base(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)
    return canonical_key(raw)


def slug_tail_variants(value: str) -> Set[str]:
    base = normalize_offer_key_base(value)
    if not base:
        return set()

    variants = {base}
    if "joo" in base:
        variants.add(base.replace("joo", "joao"))
    if "joao" in base:
        variants.add(base.replace("joao", "joo"))
    variants.add(base.replace("-de-", "-"))
    return {x for x in variants if x}


def normalize_offer_key(value: str) -> str:
    variants = sorted(slug_tail_variants(value))
    return variants[0] if variants else ""


def build_dedupe_key(title: str, validity: Optional[str], description: str) -> str:
    title_key = normalize_text_key(title)
    validity_key = normalize_text_key(validity or "")
    desc_key = normalize_text_key(clean_multiline_text(description)[:180])
    return "|".join([x for x in [title_key, validity_key, desc_key] if x])


def parse_utc_datetime(value: Optional[str]) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_validity_start(value: Optional[str]) -> Optional[datetime]:
    raw = clean_multiline_text(value or "")
    if not raw:
        return None

    match = re.search(r"v[aá]lido\s+de\s+(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", raw, flags=re.IGNORECASE)
    if not match:
        return None
    start_raw = match.group(1).strip()
    try:
        dt_local = datetime.strptime(start_raw, "%d/%m/%Y %H:%M").replace(tzinfo=BR_TZ)
        return dt_local.astimezone(timezone.utc)
    except Exception:
        return None


def parse_validity_window(value: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    raw = clean_multiline_text(value or "")
    if not raw:
        return None, None

    match = re.search(
        r"v[aá]lido\s+de\s+(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})\s+at[eé]\s+(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        try:
            start_local = datetime.strptime(match.group(1).strip(), "%d/%m/%Y %H:%M").replace(tzinfo=BR_TZ)
            end_local = datetime.strptime(match.group(2).strip(), "%d/%m/%Y %H:%M").replace(tzinfo=BR_TZ)
            return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
        except Exception:
            return None, None

    return parse_validity_start(raw), None


def build_sent_indexes(history: Dict, latest: Dict) -> Dict[str, Set[str]]:
    indexes: Dict[str, Set[str]] = {
        "ids": set(),
        "links": set(),
        "dedupe_keys": set(),
        "loose_dedupe_keys": set(),
        "title_validity": set(),
    }

    def add(index_name: str, value: Optional[str]) -> None:
        raw = str(value or "").strip()
        if not raw:
            return
        indexes[index_name].add(raw)
        canon = canonical_key(raw)
        if canon:
            indexes[index_name].add(canon)

    for item in history.get("ids", []) if isinstance(history.get("ids", []), list) else []:
        add("ids", item)
    for item in history.get("dedupe_keys", []) if isinstance(history.get("dedupe_keys", []), list) else []:
        add("dedupe_keys", item)
    for item in history.get("loose_dedupe_keys", []) if isinstance(history.get("loose_dedupe_keys", []), list) else []:
        add("loose_dedupe_keys", item)
    for item in history.get("links", []) if isinstance(history.get("links", []), list) else []:
        add("links", item)

    latest_offers = latest.get("offers", []) if isinstance(latest.get("offers", []), list) else []
    for offer in latest_offers:
        if not isinstance(offer, dict):
            continue
        add("ids", offer.get("id"))
        add("ids", offer.get("offer_id"))
        add("links", offer.get("link"))
        add("links", offer.get("original_link"))
        add("dedupe_keys", offer.get("dedupe_key"))
        add("loose_dedupe_keys", offer.get("loose_dedupe_key"))

        title = str(offer.get("title") or offer.get("preview_title") or "").strip()
        validity = str(offer.get("validity") or "").strip()
        description = str(offer.get("description") or "").strip()
        computed_dedupe = build_dedupe_key(title, validity, description)
        add("dedupe_keys", computed_dedupe)
        if title or validity:
            add("title_validity", f"{canonical_key(title)}|{canonical_key(validity)}")

    return indexes


def build_recent_sent_indexes(latest: Dict, now_utc: datetime) -> Dict[str, Set[str]]:
    threshold = now_utc - timedelta(hours=RECENT_RESEND_BLOCK_HOURS)
    out: Dict[str, Set[str]] = {
        "trace_ids": set(),
        "ids": set(),
        "links": set(),
        "dedupe_keys": set(),
        "title_validity": set(),
    }

    def add(index_name: str, value: Optional[str]) -> None:
        raw = str(value or "").strip()
        if not raw:
            return
        out[index_name].add(raw)
        canon = canonical_key(raw)
        if canon:
            out[index_name].add(canon)

    latest_offers = latest.get("offers", []) if isinstance(latest.get("offers", []), list) else []
    for offer in latest_offers:
        if not isinstance(offer, dict):
            continue
        sent_candidates = [
            offer.get("sent_at"),
            offer.get("channel_sent_at"),
            offer.get("created_at"),
            latest.get("last_update"),
        ]
        sent_at = None
        for candidate in sent_candidates:
            parsed = parse_utc_datetime(candidate)
            if parsed:
                sent_at = parsed
                break
        if not sent_at or sent_at < threshold:
            continue

        add("trace_ids", offer.get("trace_id"))
        add("ids", offer.get("id"))
        add("ids", offer.get("offer_id"))
        add("links", offer.get("link"))
        add("links", offer.get("original_link"))
        add("dedupe_keys", offer.get("dedupe_key"))
        add("dedupe_keys", build_dedupe_key(
            str(offer.get("title") or offer.get("preview_title") or ""),
            str(offer.get("validity") or ""),
            str(offer.get("description") or ""),
        ))
        title = str(offer.get("title") or offer.get("preview_title") or "").strip()
        validity = str(offer.get("validity") or "").strip()
        add("title_validity", f"{canonical_key(title)}|{canonical_key(validity)}")

    path = Path(PIPELINE_AUDIT_FILE)
    if path.exists():
        try:
            for raw in path.read_text(encoding="utf-8").splitlines():
                row = str(raw or "").strip()
                if not row:
                    continue
                try:
                    event = json.loads(row)
                except Exception:
                    continue
                if str(event.get("stage") or "").strip() != "bot.send_success":
                    continue
                event_ts = parse_utc_datetime(event.get("timestamp_utc"))
                if not event_ts or event_ts < threshold:
                    continue
                add("trace_ids", event.get("trace_id"))
                add("ids", event.get("offer_id"))
                add("links", event.get("link"))
                add("dedupe_keys", event.get("dedupe_key"))
                title = str(event.get("title") or "").strip()
                validity = str(event.get("validity") or "").strip()
                add("title_validity", f"{canonical_key(title)}|{canonical_key(validity)}")
        except Exception as e:
            log(f"⚠️ falha ao montar índice de reenvio recente: {e}")

    return out


def should_skip_pending_offer(
    offer: Dict,
    sent_indexes: Dict[str, Set[str]],
    recent_sent_indexes: Dict[str, Set[str]],
    now_utc: datetime,
    round_started_at: Optional[datetime],
    backlog_size: int,
) -> Tuple[bool, str]:
    offer_id = str(offer.get("id") or offer.get("offer_id") or "").strip()
    link = str(offer.get("link") or offer.get("original_link") or "").strip()
    dedupe_key = str(offer.get("dedupe_key") or "").strip()
    loose_dedupe_key = str(offer.get("loose_dedupe_key") or "").strip()
    title = str(offer.get("title") or offer.get("preview_title") or "").strip()
    validity = str(offer.get("validity") or "").strip()
    title_validity = f"{canonical_key(title)}|{canonical_key(validity)}"
    trace_id = get_offer_trace_id(offer)

    recent_checks = [
        ("reenvio_bloqueado_7d", "trace_ids", trace_id),
        ("reenvio_bloqueado_7d", "ids", offer_id),
        ("reenvio_bloqueado_7d", "ids", canonical_key(offer_id)),
        ("reenvio_bloqueado_7d", "links", link),
        ("reenvio_bloqueado_7d", "links", canonical_key(link)),
        ("reenvio_bloqueado_7d", "dedupe_keys", dedupe_key),
        ("reenvio_bloqueado_7d", "dedupe_keys", canonical_key(dedupe_key)),
        ("reenvio_bloqueado_7d", "title_validity", title_validity),
    ]
    for reason, index_name, value in recent_checks:
        if value and value in recent_sent_indexes.get(index_name, set()):
            return True, reason

    checks = [
        ("id_duplicado", "ids", offer_id),
        ("id_duplicado", "ids", canonical_key(offer_id)),
        ("link_duplicado", "links", link),
        ("link_duplicado", "links", canonical_key(link)),
        ("dedupe_duplicado", "dedupe_keys", dedupe_key),
        ("dedupe_duplicado", "dedupe_keys", canonical_key(dedupe_key)),
        ("loose_dedupe_duplicado", "loose_dedupe_keys", loose_dedupe_key),
        ("loose_dedupe_duplicado", "loose_dedupe_keys", canonical_key(loose_dedupe_key)),
        ("title_validity_duplicado", "title_validity", title_validity),
    ]
    for reason, index_name, value in checks:
        if value and value in sent_indexes.get(index_name, set()):
            return True, reason

    scraped_at = parse_utc_datetime(offer.get("scraped_at"))
    valid_from, valid_until = parse_validity_window(offer.get("validity"))
    if valid_until:
        if now_utc > valid_until:
            return True, "validade_expirada"
        return False, ""
    if valid_from:
        age_hours = (now_utc - valid_from).total_seconds() / 3600
        if age_hours > MAX_VALID_FROM_AGE_HOURS:
            return True, "inicio_validade_antigo_sem_fim"
    elif scraped_at:
        age_hours = (now_utc - scraped_at).total_seconds() / 3600
        if age_hours > MAX_VALID_FROM_AGE_HOURS:
            return True, "scraped_at_antigo"
    else:
        if backlog_size > MAX_CONSUMER_BATCH:
            return True, "sem_scraped_at_em_backlog"
        if round_started_at:
            created_at = parse_utc_datetime(offer.get("created_at"))
            if created_at and created_at < round_started_at:
                return True, "fora_da_rodada_atual"

    return False, ""


def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": [], "dedupe_keys": [], "loose_dedupe_keys": []}

    data = safe_json_load(path, {"ids": [], "dedupe_keys": [], "loose_dedupe_keys": []})
    ids = data.get("ids", [])
    dedupe_keys = data.get("dedupe_keys", [])
    loose_dedupe_keys = data.get("loose_dedupe_keys", [])

    if not isinstance(ids, list):
        ids = []
    if not isinstance(dedupe_keys, list):
        dedupe_keys = []
    if not isinstance(loose_dedupe_keys, list):
        loose_dedupe_keys = []

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
        key = canonical_key(item)
        if key and key not in seen_dedupe:
            seen_dedupe.add(key)
            cleaned_dedupe.append(key)

    cleaned_loose = []
    seen_loose = set()
    for item in loose_dedupe_keys:
        key = canonical_key(item)
        if key and key not in seen_loose:
            seen_loose.add(key)
            cleaned_loose.append(key)

    return {
        "ids": cleaned_ids[-MAX_HISTORY_SIZE:],
        "dedupe_keys": cleaned_dedupe[-MAX_DEDUPE_HISTORY_SIZE:],
        "loose_dedupe_keys": cleaned_loose[-MAX_DEDUPE_HISTORY_SIZE:],
    }


def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", [])
        dedupe_keys = history.get("dedupe_keys", [])
        loose_dedupe_keys = history.get("loose_dedupe_keys", [])

        if not isinstance(ids, list):
            ids = []
        if not isinstance(dedupe_keys, list):
            dedupe_keys = []
        if not isinstance(loose_dedupe_keys, list):
            loose_dedupe_keys = []

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
            key = canonical_key(item)
            if key and key not in seen_dedupe:
                seen_dedupe.add(key)
                cleaned_dedupe.append(key)

        cleaned_loose = []
        seen_loose = set()
        for item in loose_dedupe_keys:
            key = canonical_key(item)
            if key and key not in seen_loose:
                seen_loose.add(key)
                cleaned_loose.append(key)

        payload = {
            "ids": cleaned_ids[-MAX_HISTORY_SIZE:],
            "dedupe_keys": cleaned_dedupe[-MAX_DEDUPE_HISTORY_SIZE:],
            "loose_dedupe_keys": cleaned_loose[-MAX_DEDUPE_HISTORY_SIZE:],
        }

        Path(HISTORY_FILE).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(
            "✅ histórico salvo: "
            f"{len(payload['ids'])} ids / {len(payload['dedupe_keys'])} dedupe_keys / "
            f"{len(payload['loose_dedupe_keys'])} loose_dedupe_keys"
        )
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
        "previous_message_id": None,
        "last_success_check": "",
        "last_new_offer_at": "",
        "pending_count": 0,
        "last_consumer_run": "",
        "sold_out_edited_today": 0,
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
        "previous_message_id": data.get("previous_message_id"),
        "last_success_check": str(data.get("last_success_check") or ""),
        "last_new_offer_at": str(data.get("last_new_offer_at") or ""),
        "pending_count": int(data.get("pending_count") or 0),
        "last_consumer_run": str(data.get("last_consumer_run") or ""),
        "sold_out_edited_today": int(data.get("sold_out_edited_today") or 0),
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
    return load_status_runtime_file(STATUS_RUNTIME_FILE)


def status_consumer_start(pending_count: int) -> None:
    status = load_status_runtime()
    merge_component_status_file(STATUS_RUNTIME_FILE, "consumer", {
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
    }, logger=log)


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
    if (
        (status_value in {"ok", "parcial"} and sent > 0)
        or (status_value == "sem_novidade" and failed == 0)
    ):
        last_success_at = now_br_datetime()

    merge_component_status_file(STATUS_RUNTIME_FILE, "consumer", {
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
    }, logger=log)


def _parse_utc_iso(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def build_pipeline_flow_summary(limit: int = 5) -> List[str]:
    path = Path(PIPELINE_AUDIT_FILE)
    if not path.exists():
        return []

    tracked_stages = {"mac.capture", "github.candidate", "bot.send_success", "github.discard", "bot.discard"}
    trace_last_event: Dict[str, Dict] = {}
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(BR_TZ)
    local_today = now_local.date()
    max_age = timedelta(hours=24)

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []

    for line in lines:
        raw = str(line).strip()
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except Exception:
            continue
        if not isinstance(event, dict):
            continue

        trace_id = str(event.get("trace_id") or "").strip()
        stage = str(event.get("stage") or "").strip()
        if not trace_id or stage not in tracked_stages:
            continue

        ts_raw = str(event.get("timestamp_utc") or "")
        ts = _parse_utc_iso(ts_raw)
        if not ts:
            continue
        ts_local = ts.astimezone(BR_TZ)
        is_recent = (now_utc - ts) <= max_age or ts_local.date() == local_today
        if not is_recent:
            continue

        item = trace_last_event.setdefault(trace_id, {"captured": False, "pending": False, "sent": False, "discarded": False, "ts": ""})
        if stage == "mac.capture":
            item["captured"] = True
        elif stage == "github.candidate":
            item["pending"] = True
        elif stage == "bot.send_success":
            item["sent"] = True
        elif stage in {"github.discard", "bot.discard"}:
            item["discarded"] = True

        if ts_raw and ts_raw >= str(item.get("ts") or ""):
            item["ts"] = ts_raw

    if not trace_last_event:
        return []

    ordered = sorted(trace_last_event.items(), key=lambda kv: str(kv[1].get("ts") or ""), reverse=True)[:max(1, min(5, int(limit)))]
    out = []
    for trace_id, item in ordered:
        c = "✅ capturada" if item.get("captured") else "◻️ não capturada"
        if item.get("discarded") and not item.get("pending"):
            p = "🧹 filtrada/descartada"
        else:
            p = "✅ em pending" if item.get("pending") else "◻️ não entrou em pending"
        e = "✅ enviada" if item.get("sent") else "◻️ não enviada"
        out.extend(
            [
                f"• {trace_id}",
                f"  captura: {c}",
                f"  pending: {p}",
                f"  envio: {e}",
            ]
        )
    return out


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
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(BR_TZ)
        except Exception:
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
        finished = str(data.get("last_finished_at") or "").strip()
        started = str(data.get("last_started_at") or "").strip()
        success = str(data.get("last_success_at") or "").strip()

        when = finished or started or success

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

        when_text = fmt_relative(when) if when else "agora"
        # Adiciona a tag bold (<b>) aos nomes para melhorar o layout
        return f"{icon} <b>{text.capitalize()}</b> ({when_text})"

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
    last_offer_link = str(global_status.get("last_offer_link") or "").strip()
    sold_out_edited_today = int(state.get("sold_out_edited_today") or 0)
    recent_lines = [str(x).strip() for x in (state.get("lines") or []) if str(x).strip()][-5:]

    # Montagem do Layout Clean
    lines = [
        f"<b>─── 🖥️ MONITOR CLUBE UOL ───</b>",
        f"🕒 <i>Atualizado às {escape_html(now_br().strftime('%H:%M'))}</i>",
        "",
        "<b>STATUS DO SISTEMA</b>",
        f"📱 <b>Scriptable:</b> {escape_html(component_line('scriptable', scriptable))}",
        f"🍎 <b>Scraper Mac:</b> {escape_html(component_line('scraper', scraper))}",
        f"📦 <b>Consumer:</b> {escape_html(component_line('consumer', consumer))}",
    ]

    if pending_count > 0:
        lines.append(f"⏳ <b>Fila de processamento:</b> {pending_count} aguardando")
    if sold_out_edited_today > 0:
        lines.append(f"🧷 <b>Esgotadas editadas hoje:</b> {sold_out_edited_today}")

    lines.extend(["", "<b>ÚLTIMA ATIVIDADE</b>"])

    if global_status.get("last_offer_title") or state.get("last_new_offer_at"):
        title_html = escape_html(last_offer_title)
        if last_offer_link:
            title_html = f'<a href="{escape_html(last_offer_link)}">{title_html}</a>'
        
        lines.append(f"🎯 {title_html}")
        lines.append(f"📅 <b>Enviada:</b> {escape_html(last_offer_at)}")
        lines.append(f"⏳ {escape_html(silence_since_text())}")
    else:
        lines.append("<i>Nenhuma oferta recente registrada.</i>")

    lines.extend(["", "<b>DETALHES TÉCNICOS</b>"])

    if recent_lines:
        # Pega a última linha para o detalhe técnico
        last_event = recent_lines[-1]
        clean_event = re.sub(r'^\[.*?\]\s*', '', last_event)
        lines.append(f"<code>• {escape_html(clean_event)}</code>")
    else:
        lines.append("<i>Aguardando processamento...</i>")

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


def sync_daily_dashboard(state, force=False):
    if not TELEGRAM_TOKEN or not DASHBOARD_CHAT_ID:
        log("⚠️ dashboard não enviado: credenciais ausentes")
        return False

    state["date"] = now_br_date()
    state["lines"] = state.get("lines", [])[-20:]
    
    text = build_dashboard_text(state)
    old_message_id = state.get("message_id") or state.get("previous_message_id")

    # Trava local: evita request pro Telegram se o texto for idêntico
    if state.get("last_rendered_text") == text and not force:
        return True

    def send_new_dashboard_message() -> None:
        resp = telegram_post(
            "sendMessage",
            data={
                "chat_id": DASHBOARD_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )
        if not resp.ok:
            log(f"⚠️ falha ao publicar dashboard: {resp.text}")
            return

        data = resp.json()
        new_message_id = data.get("result", {}).get("message_id")
        state["message_id"] = new_message_id
        state["previous_message_id"] = None
        state["last_rendered_text"] = text
        save_daily_log(state)

        if old_message_id and str(old_message_id) != str(new_message_id):
            try:
                del_resp = telegram_post(
                    "deleteMessage",
                    data={
                        "chat_id": DASHBOARD_CHAT_ID,
                        "message_id": str(old_message_id),
                    },
                    retry_429=False,
                )
                error_text_del = del_resp.text.lower()
                if not del_resp.ok and "message to delete not found" not in error_text_del:
                    log(f"⚠️ falha ao deletar dashboard anterior: {del_resp.text}")
            except Exception as e:
                log(f"⚠️ erro ao deletar dashboard anterior: {e}")

    try:
        if old_message_id:
            resp = telegram_post(
                "editMessageText",
                data={
                    "chat_id": DASHBOARD_CHAT_ID,
                    "message_id": str(old_message_id),
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
            )
            if resp.ok:
                state["last_rendered_text"] = text
                save_daily_log(state)
                return True

            # Correção para o "message is not modified"
            error_text = resp.text.lower()
            if "message to edit not found" in error_text or "message is not modified" in error_text:
                if "message is not modified" in error_text:
                    state["last_rendered_text"] = text
                    save_daily_log(state)
                    return True
                state["message_id"] = None
                send_new_dashboard_message()
                return True

            log(f"⚠️ falha ao editar dashboard atual: {resp.text}")
            state["message_id"] = None
            send_new_dashboard_message()
            return True

        send_new_dashboard_message()
        return True
    except Exception as e:
        log(f"⚠️ erro ao publicar dashboard: {e}")
        return False


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
            "previous_message_id": state.get("message_id"),
            "lines": [],
        }

    line = f"[{now_br_time()}] {source}: {status_line}"
    source_prefix = f"] {source}:"
    filtered = [item for item in state.get("lines", []) if source_prefix not in str(item)]
    filtered.append(line)
    state["lines"] = filtered
    state["lines"] = state["lines"][-30:]
    sync_daily_dashboard(state)


def set_dashboard_pending_count(count: int) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["previous_message_id"] = state.get("message_id")
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["pending_count"] = count
    sync_daily_dashboard(state)


def set_dashboard_last_consumer_run() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["previous_message_id"] = state.get("message_id")
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["last_consumer_run"] = now_br_datetime()
    sync_daily_dashboard(state)


def increment_dashboard_sold_out_count(delta: int) -> None:
    if int(delta or 0) <= 0:
        return
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["previous_message_id"] = state.get("message_id")
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
        state["sold_out_edited_today"] = 0
    state["sold_out_edited_today"] = int(state.get("sold_out_edited_today") or 0) + int(delta)
    sync_daily_dashboard(state)


def build_smart_hashtags(title: str, description: str, link: str) -> List[str]:
    title_text = str(title or "")
    full_text = f"{title}\n{description}"
    title_text_lower = title_text.lower()
    full_text_lower = full_text.lower()
    title_norm = normalize_text_key(title_text)
    full_norm = normalize_text_key(full_text)

    def keyword_in_text(keyword: str, text_lower: str, text_norm: str) -> bool:
        kw = str(keyword or "").strip()
        if not kw:
            return False
        kw_lower = kw.lower()
        kw_norm = normalize_text_key(kw)
        return (kw_lower in text_lower) or (kw_norm in text_norm)

    tags = []

    if "/campanhasdeingresso/" in (link or "").lower():
        tags.append("#campanhasdeingresso")

    for tag, keywords in HASHTAG_RULES_BODY.items():
        if any(keyword_in_text(kw, full_text_lower, full_norm) for kw in keywords):
            tags.append(tag)

    for tag, keywords in HASHTAG_RULES_TITLE_ONLY.items():
        if any(keyword_in_text(kw, title_text_lower, title_norm) for kw in keywords):
            tags.append(tag)

    if ENABLE_AGGRESSIVE_HASHTAGS:
        for tag, keywords in AGGRESSIVE_HASHTAG_HINTS.items():
            if any(keyword_in_text(kw, full_text_lower, full_norm) for kw in keywords):
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


def build_comment_link(group_chat_id: str, comment_message_id: int, discussion_message_id: Optional[int] = None) -> str:
    raw = str(group_chat_id or "").strip()
    if raw.startswith("-100"):
        public_group_id = raw[4:]
    elif raw.startswith("-"):
        public_group_id = raw[1:]
    else:
        public_group_id = raw

    base = f"https://t.me/c/{public_group_id}/{comment_message_id}"
    if discussion_message_id:
        return f"{base}?thread={discussion_message_id}"
    return base


def build_channel_message_link(channel_chat_id: str, channel_message_id: int) -> str:
    raw = str(channel_chat_id or "").strip()
    if raw.startswith("-100"):
        public_chat_id = raw[4:]
    elif raw.startswith("-"):
        public_chat_id = raw[1:]
    else:
        public_chat_id = raw

    return f"https://t.me/c/{public_chat_id}/{channel_message_id}"


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


def extract_post_location(description: str) -> str:
    desc = clean_multiline_text(description or "")
    if not desc:
        return ""

    city_state_pattern = re.compile(
        r"([A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+?)\s*[-/]\s*([A-Za-z]{2})(?=$|[.,;)\n])",
        flags=re.I,
    )

    has_explicit_local_line = False

    for line in desc.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue

        low = line_clean.lower()
        if not (low.startswith("local:") or low.startswith("local ")):
            continue
        has_explicit_local_line = True

        line_value = re.sub(r"(?i)^\s*local\s*:?\s*", "", line_clean).strip()
        if not line_value:
            return ""

        match = city_state_pattern.search(line_value)
        if match:
            city = re.sub(r"\s+", " ", match.group(1).strip(" -.,;"))
            state = match.group(2).upper()
            return f"{city} - {state}" if city else state

        state_only = re.search(r"\b([A-Za-z]{2})\b", line_value)
        if state_only:
            return state_only.group(1).upper()

    # Busca global para "Local:" em qualquer trecho do texto, não só no começo de linha.
    for match_local in re.finditer(r"(?i)\blocal\s*:\s*([^\n|]+)", desc):
        line_value = clean_multiline_text(match_local.group(1) or "")
        if not line_value:
            continue

        match = city_state_pattern.search(line_value)
        if match:
            city = re.sub(r"\s+", " ", match.group(1).strip(" -.,;"))
            state = match.group(2).upper()
            return f"{city} - {state}" if city else state

    # Fallback: quando não houver linha explícita iniciando com "Local:",
    # tenta reconhecer cidade/UF no texto completo.
    if not has_explicit_local_line:
        match = city_state_pattern.search(desc)
        if match:
            city = re.sub(r"\s+", " ", match.group(1).strip(" -.,;"))
            state = match.group(2).upper()
            return f"{city} - {state}" if city else state

    return ""


def build_main_caption(
    title: str,
    description: str,
    validity: Optional[str],
    link: str,
    location_summary: Optional[str] = None,
    sold_out_at: Optional[str] = None,
    comment_link: Optional[str] = None,
) -> str:
    tags = build_smart_hashtags(title, description, link)
    decorated_title = decorate_main_title(title, link)

    is_sold_out = bool(str(sold_out_at or "").strip())
    if is_sold_out:
        decorated_title = f"[ESGOTADO] {decorated_title}"

    title_html = escape_html(decorated_title)
    if is_sold_out:
        title_html = f"<s>{title_html}</s>"

    body = [f"<b>{title_html}</b>"]

    if tags:
        body.append(escape_html(" ".join(tags)))

    post_location = clean_multiline_text(location_summary or "") or extract_post_location(description)
    if post_location:
        body.append(f"📍 {escape_html(post_location)}")

    val = normalize_validity(validity)
    if val:
        body.append(f"📅 {escape_html(val)}")

    if str(sold_out_at or "").strip():
        sold_out_label = "<u>esgotada</u>" if ENABLE_SOLD_OUT_UNDERLINE else "esgotada"
        body.append(f"❌ Oferta {sold_out_label} às {escape_html(str(sold_out_at).strip())}.")

    body.append(f"🔗 {escape_html(link)}")

    if str(comment_link or "").strip():
        body.append(
            f'💬 Veja os <a href="{escape_html(str(comment_link).strip())}">detalhes completos</a> nos comentários.'
        )
    else:
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
        "como utilizar",
        "como resgatar",
        "passo a passo para resgate:",
        "data do show:",
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

    if low.startswith("regras:"):
        title, rest = split_label(raw)
        rendered = [f"📌 <b>{escape_html(title)}:</b>"]
        if rest:
            rendered.append(escape_html(rest))
        return "\n\n".join(rendered)

    if low.startswith("como utilizar") or low.startswith("como resgatar") or low.startswith("passo a passo para resgate"):
        title, rest = split_label(raw)
        rendered = [f"🧩 <b>{escape_html(title)}:</b>"]
        if rest:
            rendered.append(escape_html(rest))
        return "\n\n".join(rendered)

    if low.startswith("data do show"):
        title, rest = split_label(raw)
        return f"🗓️ <b>{escape_html(title)}:</b>\n{escape_html(rest)}" if rest else f"🗓️ <b>{escape_html(title)}:</b>"

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
        (r"\s*(Sobre o Parceiro)\s*:\s*", r"\n\nSobre o Parceiro: "),
        (r"\s*(Sobre a Cacau Show)\s*:\s*", r"\n\nSobre a Cacau Show: "),
        (r"\s*(Benef[ií]cio)\s*:\s*", r"\n\nBenefício: "),
        (r"\s*(Regras(?: do benef[ií]cio)?)\s*:\s*", r"\n\nRegras: "),
        (r"\s*(Como utilizar)\s*:\s*", r"\n\nComo utilizar: "),
        (r"\s*(Como resgatar)\s*:\s*", r"\n\nComo resgatar: "),
        (r"\s*(Passo a passo para resgate)\s*:\s*", r"\n\nPasso a passo para resgate: "),
        (r"\s*(Data do Show)\s*:\s*", r"\n\nData do Show: "),
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


def send_offer_main(offer: Dict) -> Tuple[bool, Optional[int], str, Optional[Dict]]:
    title = offer.get("title") or offer.get("preview_title") or "Oferta"
    description = offer.get("description") or ""
    validity = offer.get("validity")
    link = offer.get("link") or offer.get("original_link") or ""

    caption = build_main_caption(
        title,
        description,
        validity,
        link,
        location_summary=offer.get("location_summary"),
        sold_out_at=offer.get("sold_out_at"),
    )

    img_url = (offer.get("img_url") or "").strip()
    candidates = []
    if img_url and not is_bad_offer_image_url(img_url):
        candidates.append(("img_url", img_url))

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
                result = data.get("result", {}) or {}
                return True, result.get("message_id"), f"sendPhoto ok via {label}", result
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
            result = data.get("result", {}) or {}
            return True, result.get("message_id"), "fallback sendMessage ok", result
        return False, None, f"sendMessage falhou: {resp.text}", None
    except Exception as e:
        return False, None, f"sendMessage exception: {e}", None


def extract_discussion_message_id(channel_result: Optional[Dict], channel_message_id: Optional[int]) -> Optional[int]:
    if not isinstance(channel_result, dict):
        return None

    candidates = [
        channel_result.get("message_thread_id"),
        channel_result.get("forward_from_message_id"),
    ]

    reply_to = channel_result.get("reply_to_message")
    if isinstance(reply_to, dict):
        candidates.append(reply_to.get("message_id"))
        candidates.append(reply_to.get("forward_from_message_id"))

    for candidate in candidates:
        if isinstance(candidate, int) and candidate > 0 and candidate != channel_message_id:
            return candidate

    return None


def send_offer_comment(offer: Dict, channel_message_id: int, channel_result: Optional[Dict] = None) -> Tuple[bool, str]:
    title = offer.get("title") or offer.get("preview_title") or "Oferta"
    description = offer.get("description") or ""
    validity = offer.get("validity")
    link = offer.get("link") or offer.get("original_link") or ""

    discussion_message_id = wait_for_discussion_message_id(channel_message_id)
    if not discussion_message_id:
        discussion_message_id = extract_discussion_message_id(channel_result, channel_message_id)
    reply_target = discussion_message_id
    events = []

    if not isinstance(reply_target, int) or reply_target <= 0:
        msg = "comentário não enviado: discussion_message_id ausente"
        log(msg)
        offer["comment_status"] = "failed"
        offer["comment_error"] = msg
        return False, msg

    media_items: List[Tuple[bytes, str]] = []
    media_labels: List[str] = []
    media_candidates = [
        ("foto da oferta", (offer.get("img_url") or "").strip()),
        ("foto do parceiro", (offer.get("partner_img_url") or "").strip()),
    ]
    seen_media_urls: Set[str] = set()
    for label, url in media_candidates:
        if not url or is_bad_offer_image_url(url):
            continue
        if url in seen_media_urls:
            continue
        seen_media_urls.add(url)
        img = download_image_bytes(url)
        if not img:
            events.append(f"{label} indisponível")
            continue
        media_items.append(img)
        media_labels.append(label)

    if media_items:
        try:
            if len(media_items) >= 2:
                resp = send_media_group_bytes(
                    GRUPO_COMENTARIO_ID,
                    media_items[:2],
                    disable_notification=True,
                    reply_to_message_id=reply_target,
                )
                if resp.ok:
                    events.append(" + ".join(media_labels[:2]) + " enviada(s)")
                else:
                    log(f"fotos do reply falharam: {resp.text}")
                    events.append("fotos do reply falharam")
            else:
                image_bytes, ext = media_items[0]
                resp = send_photo_bytes(
                    GRUPO_COMENTARIO_ID,
                    image_bytes,
                    ext,
                    caption=None,
                    disable_notification=True,
                    reply_to_message_id=reply_target,
                )
                if resp.ok:
                    events.append(f"{media_labels[0]} enviada")
                else:
                    log(f"{media_labels[0]} falhou: {resp.text}")
                    events.append(f"{media_labels[0]} falhou")
        except Exception as e:
            log(f"erro no envio de mídia do reply: {e}")
            events.append("mídia do reply exception")

    text = build_comment_text(title, description, validity, link)
    try:
        resp = send_message_text(
            GRUPO_COMENTARIO_ID,
            text,
            disable_notification=True,
            reply_to_message_id=reply_target,
        )
        if resp.ok:
            data = resp.json()
            comment_message_id = data.get("result", {}).get("message_id")
            if comment_message_id:
                offer["comment_message_id"] = comment_message_id
                offer["discussion_message_id"] = discussion_message_id
                offer["comment_status"] = "sent"
                offer["comment_link"] = build_comment_link(
                    GRUPO_COMENTARIO_ID,
                    comment_message_id,
                    discussion_message_id,
                )
            events.append("descrição completa enviada")
            return True, " | ".join(events)
        error_msg = f"descrição completa falhou: {resp.text}"
        offer["comment_status"] = "failed"
        offer["comment_error"] = error_msg
        return False, error_msg
    except Exception as e:
        error_msg = f"descrição completa exception: {e}"
        offer["comment_status"] = "failed"
        offer["comment_error"] = error_msg
        return False, error_msg


def is_campaign_for_canal2(offer: Dict) -> bool:
    title = str(offer.get("title") or offer.get("preview_title") or "")
    link = str(offer.get("link") or offer.get("original_link") or "")
    merged = normalize_offer_key(f"{title} {link}")
    if "teatro" in merged:
        return False
    return "#campanhasdeingresso" in merged or "/campanhasdeingresso" in merged


def _same_offer_identity(a: Dict, b: Dict) -> bool:
    a_id = normalize_offer_key(a.get("id") or a.get("link") or "")
    b_id = normalize_offer_key(b.get("id") or b.get("link") or "")
    if a_id and b_id and a_id == b_id:
        return True
    a_dedupe = canonical_key(a.get("dedupe_key") or "")
    b_dedupe = canonical_key(b.get("dedupe_key") or "")
    if a_dedupe and b_dedupe and a_dedupe == b_dedupe:
        return True
    return False


def already_sent_to_canal2(offer: Dict, latest_sent: List[Dict]) -> bool:
    if offer.get("canal2_message_id"):
        return True
    for existing in latest_sent:
        if not isinstance(existing, dict):
            continue
        if not existing.get("canal2_message_id"):
            continue
        if _same_offer_identity(offer, existing):
            return True
    return False


def forward_offer_to_canal2(offer: Dict, channel_message_id: int, latest_sent: List[Dict]) -> Tuple[bool, str]:
    if not CANAL2_ID:
        return True, "CANAL2_ID ausente; skip"
    if not is_campaign_for_canal2(offer):
        return True, "não elegível para CANAL2"
    if already_sent_to_canal2(offer, latest_sent):
        return True, "já enviada no CANAL2; skip"
    try:
        resp = telegram_post(
            "forwardMessage",
            data={
                "chat_id": CANAL2_ID,
                "from_chat_id": TELEGRAM_CHAT_ID,
                "message_id": str(channel_message_id),
                "disable_notification": True,
            },
        )
        if not resp.ok:
            return False, f"forwardMessage falhou: {resp.text}"
        result = resp.json().get("result", {}) or {}
        offer["canal2_message_id"] = result.get("message_id")
        offer["canal2_forward_status"] = "sent"
        offer["canal2_forwarded_at"] = utc_now_iso()
        return True, "encaminhada para CANAL2"
    except Exception as e:
        return False, f"forwardMessage exception: {e}"



def _caption_fields_from_offer(offer: Dict) -> Dict[str, Optional[str]]:
    return {
        "title": offer.get("title") or offer.get("preview_title") or "Oferta",
        "description": offer.get("description") or "",
        "validity": offer.get("validity"),
        "link": offer.get("link") or offer.get("original_link") or "",
        "location_summary": offer.get("location_summary"),
        "sold_out_at": offer.get("sold_out_at"),
        "comment_link": str(offer.get("comment_link") or "").strip() or None,
    }


def _should_skip_less_complete_caption_update(offer: Dict, caption_fields: Dict[str, Optional[str]]) -> bool:
    if not caption_fields.get("comment_link"):
        return False
    title = str(caption_fields.get("title") or "").strip()
    description = str(caption_fields.get("description") or "").strip()
    link = str(caption_fields.get("link") or "").strip()
    return not title or not description or not link

def update_main_offer_caption_with_comment_link(offer: Dict) -> None:
    channel_message_id = offer.get("channel_message_id")
    comment_link = str(offer.get("comment_link") or "").strip()
    if not channel_message_id or not comment_link:
        return

    caption_fields = _caption_fields_from_offer(offer)
    if _should_skip_less_complete_caption_update(offer, caption_fields):
        return

    caption = build_main_caption(
        caption_fields["title"],
        caption_fields["description"],
        caption_fields["validity"],
        caption_fields["link"],
        location_summary=caption_fields["location_summary"],
        sold_out_at=caption_fields["sold_out_at"],
        comment_link=caption_fields["comment_link"],
    )

    try:
        resp = telegram_post(
            "editMessageCaption",
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": str(channel_message_id),
                "caption": caption,
                "parse_mode": "HTML",
            },
        )
        if not resp.ok:
            log(f"falha ao editar caption com link do comentário: {resp.text}")
    except Exception as e:
        log(f"erro ao editar caption com link do comentário: {e}")


def mark_offer_success(history: Dict[str, List[str]], offer: Dict) -> None:
    offer_id = normalize_offer_key(offer.get("id") or offer.get("link") or "")
    title = offer.get("title") or offer.get("preview_title") or ""
    validity = offer.get("validity")
    description = offer.get("description") or ""
    dedupe_key = canonical_key(offer.get("dedupe_key") or build_dedupe_key(title, validity, description))
    loose_dedupe_key = canonical_key(offer.get("loose_dedupe_key") or "")

    if offer_id:
        history.setdefault("ids", []).append(offer_id)
    if dedupe_key:
        history.setdefault("dedupe_keys", []).append(dedupe_key)
    if loose_dedupe_key:
        history.setdefault("loose_dedupe_keys", []).append(loose_dedupe_key)


def refresh_sent_offers_with_sold_out() -> int:
    latest = safe_json_load(Path(LATEST_FILE), {"last_update": None, "offers": []})
    offers = latest.get("offers", [])
    if not isinstance(offers, list):
        return 0

    changed = False
    edited_count = 0
    for offer in offers:
        sold_out_at = str(offer.get("sold_out_at") or "").strip()
        message_id = offer.get("channel_message_id")
        if not sold_out_at or not message_id:
            continue

        if offer.get("_sold_out_synced_to_telegram") is True:
            continue

        caption_fields = _caption_fields_from_offer(offer)
        if _should_skip_less_complete_caption_update(offer, caption_fields):
            continue

        caption = build_main_caption(
            caption_fields["title"],
            caption_fields["description"],
            caption_fields["validity"],
            caption_fields["link"],
            location_summary=caption_fields["location_summary"],
            sold_out_at=caption_fields["sold_out_at"],
            comment_link=caption_fields["comment_link"],
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
                edited_count += 1
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
    return edited_count


def consume_pending() -> int:
    if not TELEGRAM_TOKEN:
        log("❌ variável TELEGRAM_TOKEN é obrigatória")
        return 1

    pending_data = load_pending()
    offers = pending_data.get("offers", [])

    can_send_offers = bool(TELEGRAM_CHAT_ID and GRUPO_COMENTARIO_ID)

    if can_send_offers:
        sold_out_edited = refresh_sent_offers_with_sold_out()
        if sold_out_edited > 0:
            increment_dashboard_sold_out_count(sold_out_edited)
            append_dashboard_line("consumer", f"🧷 {sold_out_edited} oferta(s) marcada(s) como esgotada(s)")

    if not offers:
        runtime_status = load_status_runtime()
        scriptable_status = runtime_status.get("scriptable", {}) if isinstance(runtime_status, dict) else {}
        scriptable_error = str(scriptable_status.get("last_error") or "").strip()
        scriptable_state = str(scriptable_status.get("status") or "").strip().lower()

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
        if scriptable_state in {"erro", "parcial"} and scriptable_error:
            log(
                "ℹ️ sem envio para o Telegram porque não há ofertas pendentes; "
                f"último erro do scriptable: {scriptable_error}"
            )
        log("📭 nenhuma oferta pendente")
        return 0

    if not can_send_offers:
        log(
            "❌ variáveis TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID são obrigatórias para envio "
            "das ofertas pendentes"
        )
        append_dashboard_line(
            "consumer",
            "⚠️ envio pausado: configure TELEGRAM_CHAT_ID e GRUPO_COMENTARIO_ID",
        )
        return 1

    status_consumer_start(len(offers))
    set_dashboard_pending_count(len(offers))

    history = load_history()
    latest_snapshot = safe_json_load(Path(LATEST_FILE), {"last_update": None, "offers": []})
    latest_sent = latest_snapshot.get("offers", []) if isinstance(latest_snapshot.get("offers", []), list) else []
    sent_indexes = build_sent_indexes(history, latest_snapshot)
    now_utc = datetime.now(timezone.utc)
    recent_sent_indexes = build_recent_sent_indexes(latest_snapshot, now_utc=now_utc)
    pending_last_update = parse_utc_datetime(pending_data.get("last_update"))

    eligible_offers: List[Dict] = []
    resend_blocked_removed = 0
    duplicate_removed = 0
    expired_removed = 0
    validity_start_age_removed = 0
    scraped_age_removed = 0
    missing_scraped_removed = 0
    for offer in offers:
        trace_id = get_offer_trace_id(offer)
        title = str(offer.get("title") or offer.get("preview_title") or "").strip()
        skip, reason = should_skip_pending_offer(
            offer=offer,
            sent_indexes=sent_indexes,
            recent_sent_indexes=recent_sent_indexes,
            now_utc=now_utc,
            round_started_at=pending_last_update,
            backlog_size=len(offers),
        )
        if skip:
            append_pipeline_audit("bot.discard", trace_id, {"title": title, "reason": reason})
            if reason == "validade_expirada":
                expired_removed += 1
            elif reason == "reenvio_bloqueado_7d":
                resend_blocked_removed += 1
            elif reason == "inicio_validade_antigo_sem_fim":
                validity_start_age_removed += 1
            elif reason == "scraped_at_antigo":
                scraped_age_removed += 1
            elif reason == "sem_scraped_at_em_backlog":
                missing_scraped_removed += 1
            else:
                duplicate_removed += 1
            continue
        eligible_offers.append(offer)

    offers_to_process = eligible_offers[:MAX_CONSUMER_BATCH]
    deferred_offers = eligible_offers[MAX_CONSUMER_BATCH:]
    deferred_count = len(deferred_offers)
    total_discarded = (
        duplicate_removed
        + resend_blocked_removed
        + expired_removed
        + validity_start_age_removed
        + scraped_age_removed
        + missing_scraped_removed
    )
    log(
        "consumer pre-processamento: "
        f"pending_total={len(offers)} | elegiveis={len(eligible_offers)} | descartadas={total_discarded} "
        f"(bloqueio_7d={resend_blocked_removed}, duplicadas={duplicate_removed}, validade_expirada={expired_removed}, "
        f"inicio_validade_antigo_sem_fim={validity_start_age_removed}, scraped_at_antigo={scraped_age_removed}, "
        f"sem_scraped_at_em_backlog={missing_scraped_removed})"
    )

    remaining = list(deferred_offers)
    processed = 0
    sent = 0
    failed = 0
    last_error = ""
    history_ids = set(history.get("ids", []))
    history_dedupe = set(history.get("dedupe_keys", []))
    history_loose = set(history.get("loose_dedupe_keys", []))

    try:
        for idx, offer in enumerate(offers_to_process, 1):
            processed += 1

            offer_id = normalize_offer_key(offer.get("id") or offer.get("link") or "")
            trace_id = get_offer_trace_id(offer)
            title = offer.get("title") or offer.get("preview_title") or ""
            validity = offer.get("validity")
            description = offer.get("description") or ""
            link = offer.get("link") or offer.get("original_link") or ""
            dedupe_key = canonical_key(offer.get("dedupe_key") or build_dedupe_key(title, validity, description))
            loose_dedupe_key = canonical_key(offer.get("loose_dedupe_key") or "")

            latest_history = load_history()
            history_ids.update(latest_history.get("ids", []))
            history_dedupe.update(latest_history.get("dedupe_keys", []))
            history_loose.update(latest_history.get("loose_dedupe_keys", []))

            latest_snapshot_live = safe_json_load(Path(LATEST_FILE), {"last_update": None, "offers": []})
            sent_indexes_live = build_sent_indexes(latest_history, latest_snapshot_live)
            sent_indexes_live["ids"].update(history_ids)
            sent_indexes_live["dedupe_keys"].update(history_dedupe)
            sent_indexes_live["loose_dedupe_keys"].update(history_loose)

            skip_live, reason_live = should_skip_pending_offer(
                offer=offer,
                sent_indexes=sent_indexes_live,
                recent_sent_indexes=recent_sent_indexes,
                now_utc=now_utc,
                round_started_at=pending_last_update,
                backlog_size=len(offers),
            )
            if skip_live and reason_live in {
                "id_duplicado",
                "link_duplicado",
                "dedupe_duplicado",
                "loose_dedupe_duplicado",
                "title_validity_duplicado",
            }:
                append_pipeline_audit("bot.skip_live_duplicate", trace_id, {"title": title, "reason": reason_live})
                log(f"oferta duplicada detectada em checagem live, removendo do pending: {title} ({reason_live})")
                continue

            if (
                offer_id in history_ids
                or (dedupe_key and dedupe_key in history_dedupe)
                or (loose_dedupe_key and loose_dedupe_key in history_loose)
            ):
                append_pipeline_audit("bot.skip_history", trace_id, {"title": title})
                log(f"oferta já está no histórico, removendo do pending: {title}")
                continue

            already_marked_as_sent = bool(
                offer.get("channel_message_id")
                or str(offer.get("channel_message_link") or "").strip()
                or str(offer.get("comment_status") or "").strip().lower() == "sent"
            )
            if already_marked_as_sent:
                append_pipeline_audit(
                    "bot.skip_pending_already_sent_mark",
                    trace_id,
                    {"title": title},
                )
                log("pending já tinha marca de envio; removendo sem reenviar")
                mark_offer_success(history, offer)
                history_ids.add(offer_id)
                if dedupe_key:
                    history_dedupe.add(dedupe_key)
                if loose_dedupe_key:
                    history_loose.add(loose_dedupe_key)
                save_history(history)
                continue

            append_pipeline_audit("bot.send_main_start", trace_id, {"title": title})
            try:
                ok_main, channel_message_id, detail_main, channel_result = send_offer_main(offer)
            except Exception as e:
                ok_main, channel_message_id, detail_main, channel_result = False, None, f"send_offer_main exception: {e}", None

            if not ok_main or not channel_message_id:
                append_pipeline_audit("bot.send_main_fail", trace_id, {"detail": detail_main})
                failed += 1
                last_error = detail_main
                remaining.append(offer)
                log(f"oferta mantida no pending por falha total: {detail_main}")
                continue

            offer["channel_message_id"] = channel_message_id
            offer["channel_message_link"] = build_channel_message_link(TELEGRAM_CHAT_ID, channel_message_id)
            canal2_ok, canal2_detail = forward_offer_to_canal2(offer, channel_message_id, latest_sent)
            if not canal2_ok:
                log(f"falha ao encaminhar para CANAL2: {canal2_detail}")
            elif "skip" not in canal2_detail.lower():
                log(canal2_detail)

            append_pipeline_audit("bot.send_comment_start", trace_id, {"channel_message_id": channel_message_id})
            try:
                ok_comment, detail_comment = send_offer_comment(offer, channel_message_id, channel_result=channel_result)
            except Exception as e:
                ok_comment, detail_comment = False, f"send_offer_comment exception: {e}"

            if not ok_comment:
                append_pipeline_audit("bot.send_comment_fail", trace_id, {"detail": detail_comment})
                failed += 1
                last_error = detail_comment
                offer["comment_status"] = "failed"
                offer["comment_error"] = detail_comment
                log(f"comentário falhou, mas oferta principal foi enviada: {detail_comment}")

            update_main_offer_caption_with_comment_link(offer)
            offer["sent_at"] = utc_now_iso()

            append_pipeline_audit(
                "bot.send_success",
                trace_id,
                {
                    "channel_message_id": channel_message_id,
                    "title": title,
                    "validity": str(validity or ""),
                    "offer_id": offer_id,
                    "link": str(link or ""),
                    "dedupe_key": dedupe_key,
                },
            )
            sent += 1
            latest_sent = [x for x in latest_sent if normalize_offer_key(x.get("id") or x.get("link") or "") != offer_id]
            latest_sent.append(offer)
            latest_sent = latest_sent[-20:]

            mark_offer_success(history, offer)
            history_ids.add(offer_id)
            if dedupe_key:
                history_dedupe.add(dedupe_key)
            if loose_dedupe_key:
                history_loose.add(loose_dedupe_key)
            save_history(history)

            log(f"oferta enviada com sucesso: {title}")

            if idx < len(offers_to_process):
                time.sleep(BETWEEN_OFFERS_DELAY_SECONDS)

    except Exception as e:
        failed += 1
        last_error = f"loop principal exception: {e}"
        log(f"❌ erro inesperado no consume_pending: {e}")

    save_history(history)
    save_pending(remaining)
    save_latest(latest_sent)

    if sent > 0:
        last_offer = latest_sent[-1] if latest_sent else None
        if last_offer:
            merge_component_status_file(STATUS_RUNTIME_FILE, "global", {
                "last_offer_title": str(last_offer.get("title") or last_offer.get("preview_title") or ""),
                "last_offer_at": now_br_datetime(),
                "last_offer_id": str(last_offer.get("id") or ""),
                "last_offer_link": str(last_offer.get("channel_message_link") or ""),
            }, logger=log)

            state = load_daily_log()
            state["last_new_offer_at"] = now_br_datetime()
            save_daily_log(state)

    set_dashboard_pending_count(len(remaining))
    set_dashboard_last_consumer_run()

    if sent > 0 and failed == 0 and len(remaining) == 0:
        status_value = "ok"
        summary = f"{sent} enviada(s) com sucesso"
    elif sent > 0 and (failed > 0 or len(remaining) > 0):
        status_value = "parcial"
        summary = f"{sent} enviada(s), {failed} falha(s), {len(remaining)} pendente(s)"
    elif failed > 0:
        status_value = "erro"
        summary = f"nenhuma enviada, {failed} falha(s)"
    elif len(offers) > 0 and sent == 0 and total_discarded > 0:
        status_value = "sem_novidade"
        summary = (
            "nenhuma enviada; descartadas "
            f"{total_discarded} (reenvio_bloqueado_7d={resend_blocked_removed}, validade_expirada={expired_removed}, "
            f"inicio_validade_antigo_sem_fim={validity_start_age_removed}, "
            f"scraped_at_antigo={scraped_age_removed}, duplicadas={duplicate_removed}, "
            f"sem_scraped_at_em_backlog={missing_scraped_removed})"
        )
        if not last_error:
            last_error = (
                "consumer_sem_envio_com_pending: "
                f"pending={len(offers)} descartadas={total_discarded} falhas_telegram={failed}"
            )
    elif len(offers) > 0 and sent == 0:
        status_value = "sem_novidade"
        summary = (
            f"nenhuma enviada; pending={len(offers)} processadas={processed} "
            f"falhas_telegram={failed} pendentes={len(remaining)}"
        )
        if not last_error:
            last_error = (
                "consumer_sem_envio_com_pending: "
                f"pending={len(offers)} descartadas={total_discarded} falhas_telegram={failed}"
            )
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
        (
            f"{'✅' if sent > 0 else '⚠️'} filtradas "
            f"{total_discarded} "
            f"(bloqueio 7d {resend_blocked_removed} / dup {duplicate_removed} / validade expirada {expired_removed} / "
            f"início antigo sem fim {validity_start_age_removed} / scraped_at antigo {scraped_age_removed} "
            f"/ sem scraped_at {missing_scraped_removed}) | "
            f"processadas {processed} | enviadas {sent} | falhas {failed} | adiadas {deferred_count} "
            f"| pendentes {len(remaining)}"
        ),
    )
    log(
        "consumer resumo: "
        f"pending_total={len(offers)} enviadas={sent} descartadas={total_discarded} "
        f"falhas_telegram={failed} pendentes_finais={len(remaining)}"
    )

    return 0


if __name__ == "__main__":
    args = set(sys.argv[1:])
    if args and "--pending" not in args:
        log("uso: python bot_leouol.py [--pending]")
        raise SystemExit(2)

    # Compatibilidade: sem argumentos também executa o consumer.
    raise SystemExit(consume_pending())
