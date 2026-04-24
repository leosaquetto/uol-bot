import json
import os
import re
import unicodedata
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

import certifi
import requests
import urllib3
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError, RequestException, SSLError
from status_runtime_utils import merge_component_status_file

BASE_URL = "https://clube.uol.com.br"
LIST_URL = f"{BASE_URL}/?order=new"
FALLBACK_LIST_URL = f"{BASE_URL}/"

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
DAILY_LOG_FILE = "daily_log.json"
STATUS_RUNTIME_FILE = "status_runtime.json"
LATEST_FILE = "latest_offers.json"
SOLD_OUT_UPDATES_FILE = "sold_out_updates.json"
SCRAPER_DIAGNOSTICS_FILE = "scraper_diagnostics.json"
PIPELINE_AUDIT_FILE = "pipeline_audit.jsonl"

SNAPSHOT_DIR = "snapshots"
SNAPSHOT_CONTROL_FILE = "snapshots_control.json"
MAC_SNAPSHOT_FILE = os.path.join(SNAPSHOT_DIR, "mac-uol-offers.json")
MAX_PROCESSED_SNAPSHOTS = 5000
SNAPSHOT_CLEANUP_ENABLED = True
SNAPSHOT_RETENTION_MIN_RECENT = 20
SNAPSHOT_RETENTION_HOURS = 24

REQUEST_TIMEOUT = 30
MAX_HISTORY_IDS = 1500
MAX_HISTORY_DEDUPE = 1500
MAX_HISTORY_LOOSE = 1500

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


def now_br_datetime() -> str:
    return now_br().strftime("%d/%m/%Y às %H:%M")


def now_br_date() -> str:
    return now_br().strftime("%d/%m/%Y")


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


def save_scraper_diagnostics(data: Dict[str, Any]) -> None:
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        **(data or {}),
    }
    save_json(SCRAPER_DIAGNOSTICS_FILE, payload)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def append_pipeline_audit(stage: str, trace_id: str, extra: Optional[Dict[str, Any]] = None) -> None:
    trace = str(trace_id or "").strip()
    if not trace:
        return
    payload = {"timestamp_utc": utc_now_iso(), "stage": str(stage or "").strip(), "trace_id": trace}
    if isinstance(extra, dict):
        payload.update(extra)
    try:
        with open(PIPELINE_AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as e:
        log(f"aviso: falha ao registrar auditoria ({stage}): {e}")


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


def extract_location_summary(text: Optional[str]) -> str:
    desc = clean_text(text or "")
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
            continue
        match = city_state_pattern.search(line_value)
        if match:
            city = re.sub(r"\s+", " ", match.group(1).strip(" -.,;"))
            state = match.group(2).upper()
            return f"{city} - {state}" if city else state

    for match_local in re.finditer(r"(?i)\blocal\s*:\s*([^\n|]+)", desc):
        line_value = clean_text(match_local.group(1) or "")
        if not line_value:
            continue
        match = city_state_pattern.search(line_value)
        if match:
            city = re.sub(r"\s+", " ", match.group(1).strip(" -.,;"))
            state = match.group(2).upper()
            return f"{city} - {state}" if city else state

    if not has_explicit_local_line:
        match = city_state_pattern.search(desc)
        if match:
            city = re.sub(r"\s+", " ", match.group(1).strip(" -.,;"))
            state = match.group(2).upper()
            return f"{city} - {state}" if city else state

    return ""


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
        raw = str(link or "").strip()
        if not raw:
            return ""
        parsed = urlparse(raw)
        if parsed.scheme and parsed.netloc:
            path = parsed.path or ""
        else:
            path = raw.split("?")[0].split("#")[0]
        tail = path.rstrip("/").split("/")[-1]
        return unquote(tail)
    except Exception:
        return str(link or "").strip()


def normalize_text_key(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""

    raw = unquote(raw)
    raw = raw.split("?")[0].split("#")[0]
    raw = unicodedata.normalize("NFD", raw)
    raw = "".join(ch for ch in raw if unicodedata.category(ch) != "Mn")
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


def build_trace_id(value: str) -> str:
    key = canonical_offer_key(value)
    return f"trace_{key}" if key else ""


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
        or "/static/images/clubes/uol/categorias/" in u
        or "ingressosexclusivos-hover" in u
        or "ingressos-hover" in u
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


def offer_richness_score(offer: Dict[str, Any]) -> int:
    score = 0
    if clean_text(offer.get("title") or ""):
        score += 2
    desc = clean_text(offer.get("description") or "")
    if desc and "descrição não disponível" not in desc.lower() and "enriquecimento pendente" not in desc.lower():
        score += 5
    if clean_text(offer.get("validity") or ""):
        score += 3
    if clean_text(offer.get("img_url") or ""):
        score += 2
    if clean_text(offer.get("partner_img_url") or ""):
        score += 1
    if clean_text(offer.get("dedupe_key") or ""):
        score += 1
    return score


def choose_richer_offer(a: Optional[Dict[str, Any]], b: Dict[str, Any]) -> Dict[str, Any]:
    if not a:
        return b
    sa = offer_richness_score(a)
    sb = offer_richness_score(b)
    if sb > sa:
        return b
    if sa > sb:
        return a

    a_ts = str(a.get("scraped_at") or "")
    b_ts = str(b.get("scraped_at") or "")
    return b if b_ts > a_ts else a


def dedupe_keep_richest(offers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    fallback_bucket: Dict[str, Dict[str, Any]] = {}

    for offer in offers:
        id_key = canonical_offer_key(offer.get("id") or offer.get("link") or "")
        strict_key = str(offer.get("dedupe_key") or "").strip()
        loose_key = str(offer.get("loose_dedupe_key") or "").strip()

        if id_key:
            prev = by_id.get(id_key)
            by_id[id_key] = choose_richer_offer(prev, offer)
            continue

        fb = strict_key or loose_key
        if not fb:
            continue
        prev = fallback_bucket.get(fb)
        fallback_bucket[fb] = choose_richer_offer(prev, offer)

    merged = list(by_id.values())
    for _, offer in fallback_bucket.items():
        strict_key = str(offer.get("dedupe_key") or "").strip()
        loose_key = str(offer.get("loose_dedupe_key") or "").strip()
        duplicate = False
        for existing in merged:
            if strict_key and strict_key == str(existing.get("dedupe_key") or "").strip():
                duplicate = True
                break
            if loose_key and loose_key == str(existing.get("loose_dedupe_key") or "").strip():
                duplicate = True
                break
        if not duplicate:
            merged.append(offer)

    merged.sort(key=lambda x: str(x.get("scraped_at") or ""))
    return merged


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


def load_snapshot(snapshot_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
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


def load_mac_snapshot_meta() -> Optional[Dict[str, Any]]:
    if not os.path.exists(MAC_SNAPSHOT_FILE):
        return None
    data = load_json(MAC_SNAPSHOT_FILE, None)
    if not isinstance(data, dict):
        return None
    if not isinstance(data.get("offers"), list):
        return None
    return data


def load_detail_for_snapshot(snapshot_id: str) -> Dict[str, Dict[str, Any]]:
    path = os.path.join(SNAPSHOT_DIR, f"detail_{snapshot_id}.json")
    data = load_json(path, {})
    offers = []

    if isinstance(data, dict):
        if isinstance(data.get("offers"), list):
            offers = data["offers"]
        elif isinstance(data.get("details"), list):
            offers = data["details"]
        elif all(isinstance(v, dict) for v in data.values()):
            offers = list(data.values())

    lookup: Dict[str, Dict[str, Any]] = {}
    for item in offers:
        if not isinstance(item, dict):
            continue

        link = str(item.get("link") or "").strip()
        if not link:
            continue

        normalized_item = {
            "link": link,
            "title": clean_text(item.get("detail_title") or item.get("title") or item.get("card_title") or ""),
            "preview_title": clean_text(item.get("card_title") or item.get("title") or ""),
            "validity": clean_text(item.get("validity") or ""),
            "description": clean_text(item.get("description") or item.get("description_preview") or ""),
            "detail_img_url": absolutize_url(item.get("detail_img_url") or ""),
            "partner_img_url": absolutize_url(item.get("partner_img_url") or ""),
            "img_url": absolutize_url(item.get("card_img_url") or ""),
            "detail_ok": bool(item.get("detail_ok")),
            "error": clean_text(item.get("error") or ""),
        }

        candidates = [
            canonical_offer_key(link),
            canonical_offer_key(item.get("id") or ""),
        ]
        for key in candidates:
            if key:
                lookup[key] = normalized_item
    return lookup

def load_offers_from_snapshot_meta(meta: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(meta, dict):
        return []

    raw_offers = meta.get("offers", [])
    if not isinstance(raw_offers, list):
        return []

    normalized: List[Dict[str, Any]] = []

    for item in raw_offers:
        if not isinstance(item, dict):
            continue

        link = absolutize_url(item.get("link") or item.get("original_link") or "")
        title = clean_text(item.get("title") or item.get("preview_title") or "")
        if not link or not title:
            continue

        offer_id = get_offer_id(link)

        normalized.append({
            "id": offer_id,
            "original_link": link,
            "preview_title": clean_text(item.get("preview_title") or title),
            "title": clean_text(item.get("title") or title),
            "link": link,
            "img_url": absolutize_url(item.get("img_url") or item.get("card_img_url") or ""),
            "detail_img_url": absolutize_url(item.get("detail_img_url") or ""),
            "partner_img_url": absolutize_url(item.get("partner_img_url") or ""),
            "validity": clean_text(item.get("validity") or ""),
            "description": clean_text(item.get("description") or ""),
            "detail_ok": bool(item.get("detail_ok")),
            "detail_error": clean_text(item.get("detail_error") or ""),
            "scraped_at": str(item.get("scraped_at") or meta.get("generated_at") or ""),
            "trace_id": str(item.get("trace_id") or build_trace_id(offer_id or link) or ""),
        })

    bucket: Dict[str, Dict[str, Any]] = {}
    for offer in normalized:
        key = canonical_offer_key(offer.get("id") or offer.get("link") or "")
        if not key:
            continue
        prev = bucket.get(key)
        bucket[key] = choose_richer_offer(prev, offer)

    return list(bucket.values())


def cleanup_snapshot_files(snapshot_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
    if not SNAPSHOT_CLEANUP_ENABLED:
        return

    paths = {
        os.path.join(SNAPSHOT_DIR, f"snapshot_{snapshot_id}.json"),
        os.path.join(SNAPSHOT_DIR, f"detail_{snapshot_id}.json"),
    }

    if isinstance(meta, dict):
        html_path = str(meta.get("html_path") or "").strip()
        if html_path:
            paths.add(html_path)
    else:
        paths.add(os.path.join(SNAPSHOT_DIR, f"snapshot_{snapshot_id}.html"))

    for path in paths:
        if not path:
            continue
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            log(f"aviso: não consegui limpar arquivo processado {path}: {e}")


def cleanup_old_snapshot_files() -> None:
    if not SNAPSHOT_CLEANUP_ENABLED:
        return
    if not os.path.isdir(SNAPSHOT_DIR):
        return

    try:
        now_ts = datetime.now(timezone.utc).timestamp()
        max_age_seconds = SNAPSHOT_RETENTION_HOURS * 3600

        grouped: Dict[str, Dict[str, Any]] = {}
        for entry in Path(SNAPSHOT_DIR).iterdir():
            if not entry.is_file():
                continue

            name = entry.name
            snapshot_id = ""
            if name.startswith("snapshot_") and name.endswith(".json"):
                snapshot_id = name[len("snapshot_") : -len(".json")]
            elif name.startswith("snapshot_") and name.endswith(".html"):
                snapshot_id = name[len("snapshot_") : -len(".html")]
            elif name.startswith("detail_") and name.endswith(".json"):
                snapshot_id = name[len("detail_") : -len(".json")]

            if not snapshot_id:
                continue

            try:
                mtime = entry.stat().st_mtime
            except Exception as e:
                log(f"aviso: não consegui ler mtime de {entry}: {e}")
                continue

            group = grouped.setdefault(snapshot_id, {"files": [], "latest_mtime": 0.0})
            group["files"].append(entry)
            group["latest_mtime"] = max(float(group["latest_mtime"]), float(mtime))

        if not grouped:
            return

        sorted_ids = sorted(grouped.keys(), key=lambda sid: grouped[sid]["latest_mtime"], reverse=True)
        keep_ids = set(sorted_ids[:SNAPSHOT_RETENTION_MIN_RECENT])
        for snapshot_id, group in grouped.items():
            if (now_ts - float(group["latest_mtime"])) < max_age_seconds:
                keep_ids.add(snapshot_id)

        removed = 0
        for snapshot_id, group in grouped.items():
            if snapshot_id in keep_ids:
                continue
            for path in group["files"]:
                try:
                    path.unlink()
                    removed += 1
                except Exception as e:
                    log(f"aviso: não consegui apagar snapshot antigo {path}: {e}")

        log(f"limpeza snapshots: removidos {removed} arquivos antigos (retenção: {SNAPSHOT_RETENTION_MIN_RECENT} mais recentes e últimos {SNAPSHOT_RETENTION_HOURS}h)")
    except Exception as e:
        log(f"aviso: falha na limpeza de snapshots antigos: {e}")


def get_unprocessed_snapshot_ids() -> Tuple[List[str], Dict[str, Any]]:
    control = load_snapshot_control()
    processed = set(control.get("processed_snapshot_ids", []))
    all_ids = list_snapshot_ids()
    pending_ids = [snapshot_id for snapshot_id in all_ids if snapshot_id not in processed]
    return pending_ids, control


def mark_snapshot_processed(snapshot_id: str, control: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> None:
    processed = control.get("processed_snapshot_ids", [])
    if not isinstance(processed, list):
        processed = []
    if snapshot_id not in processed:
        processed.append(snapshot_id)
    control["processed_snapshot_ids"] = processed[-MAX_PROCESSED_SNAPSHOTS:]
    save_snapshot_control(control)


def status_scraper_start() -> None:
    prev = load_json(STATUS_RUNTIME_FILE, {}).get("scraper", {})
    if not isinstance(prev, dict):
        prev = {}
    merge_component_status_file(STATUS_RUNTIME_FILE, "scraper", {
        "last_started_at": now_br_datetime(),
        "last_finished_at": prev.get("last_finished_at", ""),
        "last_success_at": prev.get("last_success_at", ""),
        "status": "running",
        "summary": "scraper iniciado",
        "offers_seen": 0,
        "new_offers": 0,
        "pending_count": prev.get("pending_count", 0),
        "last_error": "",
    }, logger=log)


def status_scraper_finish(summary: str, status_value: str, offers_seen: int, new_offers: int, pending_count: int, last_error: str = "") -> None:
    prev = load_json(STATUS_RUNTIME_FILE, {}).get("scraper", {})
    if not isinstance(prev, dict):
        prev = {}
    last_success_at = prev.get("last_success_at", "")
    if status_value in {"ok", "sem_novidade"} and not last_error:
        last_success_at = now_br_datetime()
    merge_component_status_file(STATUS_RUNTIME_FILE, "scraper", {
        "last_started_at": prev.get("last_started_at", ""),
        "last_finished_at": now_br_datetime(),
        "last_success_at": last_success_at,
        "status": status_value,
        "summary": summary,
        "offers_seen": offers_seen,
        "new_offers": new_offers,
        "pending_count": pending_count,
        "last_error": last_error,
    }, logger=log)


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
            offer_id = get_offer_id(link)

            imgs = []
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
                imgs.append(full_src)

            partner_img_url = ""
            img_url = ""

            for src in imgs:
                if "/parceiros/" in src and not partner_img_url:
                    partner_img_url = src
            for src in imgs:
                if src != partner_img_url and is_likely_benefit_banner(src):
                    img_url = src
                    break
            if not img_url:
                for src in imgs:
                    if src != partner_img_url and not is_bad_banner_url(src):
                        img_url = src
                        break

            offers.append({
                "id": offer_id,
                "original_link": link,
                "preview_title": title,
                "title": title,
                "link": link,
                "img_url": img_url,
                "partner_img_url": partner_img_url,
            })
        except Exception as e:
            log(f"erro ao parsear bloco: {e}")

    bucket: Dict[str, Dict[str, Any]] = {}
    for offer in offers:
        key = canonical_offer_key(offer.get("id") or offer.get("link") or "")
        if not key:
            continue
        prev = bucket.get(key)
        bucket[key] = choose_richer_offer(prev, offer)
    return list(bucket.values())


def extract_offer_details_live(url: str, preview_title: str) -> Dict[str, Any]:
    full_url = absolutize_url(url)
    log(f"acessando detalhes ao vivo: {preview_title[:50]}...")

    try:
        html = get_html(full_url)
        if not html:
            return {
                "title": preview_title,
                "validity": None,
                "description": "",
                "detail_img_url": "",
            }

        page_title = preview_title
        for regex in [
            re.compile(r"<h2[^>]*>([\s\S]*?)</h2>", re.I),
            re.compile(r"<h1[^>]*>([\s\S]*?)</h1>", re.I),
        ]:
            m = regex.search(html)
            if m:
                candidate_title = clean_text(re.sub(r"<[^>]+>", " ", m.group(1)))
                if candidate_title:
                    page_title = candidate_title
                    break

        all_imgs = []
        for m in re.finditer(r"""<img[^>]+(?:data-src|data-original|data-lazy|src)=["']([^"']+)["']""", html, re.I):
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
            re.compile(
                r"""class=["'][^"']*info-beneficio[^"']*["'][^>]*>([\s\S]*?)(?:<script|<footer|class=["'][^"']*box-compartilhar)""",
                re.I,
            ),
            re.compile(r"""id=["']beneficio["'][^>]*>([\s\S]*?)(?:<script|<footer)""", re.I),
        ]:
            m = regex.search(html)
            if m:
                description = html_to_text(m.group(1))
                if len(description) >= 20:
                    break

        return {
            "title": page_title,
            "validity": validity,
            "description": description[:4000],
            "detail_img_url": detail_img_url,
        }
    except Exception as e:
        log(f"erro ao extrair detalhes ao vivo: {e}")
        return {
            "title": preview_title,
            "validity": None,
            "description": "",
            "detail_img_url": "",
        }


def normalize_detail_payload(detail: Dict[str, Any], fallback_offer: Dict[str, Any]) -> Dict[str, Any]:
    title = clean_text(
        detail.get("title")
        or detail.get("preview_title")
        or fallback_offer.get("title")
        or fallback_offer.get("preview_title")
        or ""
    )
    validity = clean_text(detail.get("validity") or "") or None
    description = clean_text(detail.get("description") or "")

    detail_img_url = absolutize_url(
        detail.get("detail_img_url")
        or detail.get("img_url")
        or detail.get("main_img")
        or ""
    )

    partner_img_url = absolutize_url(
        detail.get("partner_img_url")
        or detail.get("logo_img")
        or fallback_offer.get("partner_img_url")
        or ""
    )

    return {
        "title": title or fallback_offer.get("title") or fallback_offer.get("preview_title") or "",
        "validity": validity,
        "description": description[:4000],
        "detail_img_url": detail_img_url,
        "partner_img_url": partner_img_url,
    }


def extract_history_sets(history_data: Dict[str, Any]) -> tuple[set, set, set]:
    ids = history_data.get("ids", [])
    dedupe_keys = history_data.get("dedupe_keys", [])
    loose_dedupe_keys = history_data.get("loose_dedupe_keys", [])
    if not isinstance(ids, list):
        ids = []
    if not isinstance(dedupe_keys, list):
        dedupe_keys = []
    if not isinstance(loose_dedupe_keys, list):
        loose_dedupe_keys = []

    id_set = set()
    for x in ids:
        id_set.update(slug_tail_variants(x))

    dedupe_set = {str(x).strip() for x in dedupe_keys if str(x).strip()}
    loose_set = {str(x).strip() for x in loose_dedupe_keys if str(x).strip()}
    return id_set, dedupe_set, loose_set


def extract_pending_sets(pending_data: Dict[str, Any]) -> tuple[set, set, set]:
    offers = pending_data.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    id_set = set()
    dedupe_set = set()
    loose_set = set()

    for o in offers:
        for variant in slug_tail_variants(o.get("id") or o.get("link")):
            id_set.add(variant)

        dedupe_key = str(o.get("dedupe_key") or "").strip()
        if not dedupe_key:
            dedupe_key = build_dedupe_key(
                title=o.get("title") or o.get("preview_title") or "",
                validity=o.get("validity"),
                description=o.get("description") or "",
            )
        if dedupe_key:
            dedupe_set.add(dedupe_key)

        loose_key = str(o.get("loose_dedupe_key") or "").strip()
        if not loose_key:
            loose_key = build_loose_dedupe_key(
                title=o.get("title") or o.get("preview_title") or "",
                description=o.get("description") or "",
            )
        if loose_key:
            loose_set.add(loose_key)

    return id_set, dedupe_set, loose_set


def merge_offer_data(base_offer: Dict[str, Any], details: Dict[str, Any]) -> Dict[str, Any]:
    final_title = clean_text(details.get("title") or base_offer.get("title") or base_offer.get("preview_title") or "")
    final_partner = absolutize_url(details.get("partner_img_url") or base_offer.get("partner_img_url") or "")
    final_img = absolutize_url(details.get("detail_img_url") or "")
    if not final_img or is_bad_banner_url(final_img) or final_img == final_partner:
        fallback_img = absolutize_url(base_offer.get("img_url") or "")
        if fallback_img and not is_bad_banner_url(fallback_img) and fallback_img != final_partner:
            final_img = fallback_img
    if not final_img or is_bad_banner_url(final_img) or final_img == final_partner:
        final_img = ""

    validity = clean_text(details.get("validity") or "") or None
    description = clean_text(details.get("description") or "")
    location_summary = extract_location_summary(description)

    dedupe_key = build_dedupe_key(final_title, validity, description)
    loose_dedupe_key = build_loose_dedupe_key(final_title, description)

    offer_id = base_offer.get("id") or get_offer_id(base_offer.get("link") or "")
    offer_link = base_offer.get("link") or base_offer.get("original_link") or ""

    return {
        "id": offer_id,
        "original_link": base_offer.get("original_link") or offer_link,
        "preview_title": base_offer.get("preview_title") or final_title,
        "title": final_title,
        "link": offer_link,
        "img_url": final_img,
        "partner_img_url": final_partner,
        "validity": validity,
        "description": description,
        "location_summary": location_summary,
        "dedupe_key": dedupe_key,
        "loose_dedupe_key": loose_dedupe_key,
        "scraped_at": utc_now_iso(),
        "trace_id": str(base_offer.get("trace_id") or build_trace_id(offer_id or offer_link) or ""),
    }


def is_offer_ready_for_pending(offer: Dict[str, Any]) -> bool:
    title = clean_text(offer.get("title") or offer.get("preview_title") or "")
    link = clean_text(offer.get("link") or offer.get("original_link") or "")

    if not title or not link:
        return False

    return True


def is_same_day_offer(scraped_at: str) -> bool:
    raw = str(scraped_at or "").strip()
    if not raw:
        return False
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(BR_TZ)
        return dt.strftime("%d/%m/%Y") == now_br().strftime("%d/%m/%Y")
    except Exception:
        return False

def load_sold_out_updates() -> List[Dict[str, Any]]:
    data = load_json(SOLD_OUT_UPDATES_FILE, {"updates": []})
    updates = data.get("updates", [])
    if not isinstance(updates, list):
        return []
    out = []
    for item in updates:
        if not isinstance(item, dict):
            continue
        link = str(item.get("link") or "").strip()
        sold_out_at = str(item.get("sold_out_at") or "").strip()
        date = str(item.get("date") or "").strip()
        if not link or not sold_out_at or not date:
            continue
        out.append({
            "link": link,
            "sold_out_at": sold_out_at,
            "date": date,
        })
    return out


def apply_scriptable_sold_out_updates() -> bool:
    latest = load_json(LATEST_FILE, {"last_update": None, "offers": []})
    offers = latest.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    updates = load_sold_out_updates()
    if not updates:
        return False

    updates_by_key: Dict[str, Dict[str, Any]] = {}
    for item in updates:
        link_key = canonical_offer_key(item.get("link") or "")
        if link_key:
            updates_by_key[link_key] = item

    changed = False
    today = now_br_date()

    for offer in offers:
        if str(offer.get("sold_out_at") or "").strip():
            continue

        scraped_at = str(offer.get("scraped_at") or "").strip()
        if not is_same_day_offer(scraped_at):
            continue

        offer_key = canonical_offer_key(offer.get("link") or offer.get("original_link") or offer.get("id") or "")
        if not offer_key:
            continue

        update = updates_by_key.get(offer_key)
        if not update:
            continue

        if str(update.get("date") or "").strip() != today:
            continue

        offer["sold_out_at"] = str(update.get("sold_out_at") or "").strip()
        changed = True
        log(f"oferta marcada como esgotada via scriptable: {offer.get('title') or offer.get('preview_title')} às {offer['sold_out_at']}")

    if changed:
        latest["offers"] = offers
        latest["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        save_json(LATEST_FILE, latest)

    return changed


def main() -> None:
    log("iniciando scraper")
    sold_out_changed = apply_scriptable_sold_out_updates()
    status_scraper_start()

    historico = load_json(HISTORY_FILE, {"ids": [], "dedupe_keys": [], "loose_dedupe_keys": []})
    pending = load_json(PENDING_FILE, {"last_update": None, "offers": []})

    historico_keys, historico_dedupe, historico_loose = extract_history_sets(historico)
    pending_keys, pending_dedupe, pending_loose = extract_pending_sets(pending)

    snapshot_ids, snapshot_control = get_unprocessed_snapshot_ids()
    mac_meta = load_mac_snapshot_meta()
    mac_offers = load_offers_from_snapshot_meta(mac_meta) if mac_meta else []

    if snapshot_ids:
        log(f"snapshots pendentes encontrados: {len(snapshot_ids)}")
    if mac_offers:
        log(f"snapshot mac disponível: {len(mac_offers)} ofertas enriquecidas")

    save_scraper_diagnostics(
        {
            "snapshot_ids_pending": len(snapshot_ids),
            "mac_offers_loaded": len(mac_offers),
            "offers_consolidated": 0,
            "candidates_added": 0,
            "dropped_incomplete": 0,
            "discard_reasons": {},
            "discarded_offers": [],
        }
    )

    if not snapshot_ids and not mac_offers:
        status_scraper_finish(
            summary="sem snapshots pendentes" + (" | esgotadas atualizadas" if sold_out_changed else ""),
            status_value="sem_novidade",
            offers_seen=0,
            new_offers=0,
            pending_count=len(pending.get("offers", [])),
            last_error="",
        )
        return

    all_offers: List[Dict[str, Any]] = []
    loaded_snapshot_ids: List[str] = []
    snapshot_meta_map: Dict[str, Optional[Dict[str, Any]]] = {}

    if mac_offers:
        all_offers.extend(mac_offers)

    for snapshot_id in snapshot_ids:
        meta, html = load_snapshot(snapshot_id)
        snapshot_meta_map[snapshot_id] = meta

        offers_from_meta = load_offers_from_snapshot_meta(meta)

        if offers_from_meta:
            log(f"usando offers do snapshot meta em {snapshot_id}: {len(offers_from_meta)}")
            all_offers.extend(offers_from_meta)
            loaded_snapshot_ids.append(snapshot_id)
            continue

        if not html:
            log(f"snapshot inválido ou sem html/meta útil: {snapshot_id}")
            mark_snapshot_processed(snapshot_id, snapshot_control, meta)
            continue

        offers = parse_offers(html)
        log(f"total encontradas via parse html em {snapshot_id}: {len(offers)}")
        all_offers.extend(offers)
        loaded_snapshot_ids.append(snapshot_id)

    if not all_offers:
        for snapshot_id in loaded_snapshot_ids:
            mark_snapshot_processed(snapshot_id, snapshot_control, snapshot_meta_map.get(snapshot_id))
        status_scraper_finish(
            summary="sem ofertas novas" + (" | esgotadas atualizadas" if sold_out_changed else ""),
            status_value="sem_novidade",
            offers_seen=0,
            new_offers=0,
            pending_count=len(pending.get("offers", [])),
            last_error="",
        )
        cleanup_old_snapshot_files()
        return

    base_bucket: Dict[str, Dict[str, Any]] = {}
    for offer in all_offers:
        key = canonical_offer_key(offer.get("id") or offer.get("link") or "")
        if not key:
            continue
        prev = base_bucket.get(key)
        base_bucket[key] = choose_richer_offer(prev, offer)

    offers = list(base_bucket.values())
    log(f"total consolidado após unir snapshots: {len(offers)}")

    candidates: List[Dict[str, Any]] = []
    seen_new_offer_keys = set()
    seen_new_dedupe_keys = set()
    seen_new_loose_keys = set()

    detail_lookups = {snapshot_id: load_detail_for_snapshot(snapshot_id) for snapshot_id in loaded_snapshot_ids}
    merged_detail_lookup: Dict[str, Dict[str, Any]] = {}
    for lookup in detail_lookups.values():
        merged_detail_lookup.update(lookup)

    dropped_incomplete = 0
    discard_reasons: Dict[str, int] = {"incompleta": 0, "historico": 0, "pending": 0, "dedupe": 0}
    discarded_offers: List[Dict[str, str]] = []

    for offer in offers:
        offer_key = canonical_offer_key(offer.get("id") or offer.get("link") or "")
        trace_id = str(offer.get("trace_id") or build_trace_id(offer_key or offer.get("link") or offer.get("id") or "") or "")
        append_pipeline_audit("github.ingest", trace_id, {"offer_key": offer_key, "link": str(offer.get("link") or "")})
        detail = merged_detail_lookup.get(offer_key)

        if detail and detail.get("detail_ok"):
            details = normalize_detail_payload(detail, offer)
        elif (
            bool(offer.get("detail_ok"))
            or clean_text(offer.get("validity") or "")
            or clean_text(offer.get("description") or "")
            or absolutize_url(offer.get("detail_img_url") or "")
        ):
            details = normalize_detail_payload(offer, offer)
        else:
            details = extract_offer_details_live(offer["link"], offer["preview_title"])
            details = normalize_detail_payload(details, offer)

        normalized_offer = merge_offer_data(offer, details)
        trace_id = str(normalized_offer.get("trace_id") or trace_id or "")
        offer_title = str(normalized_offer.get("title") or normalized_offer.get("preview_title") or "").strip()
        offer_link = str(normalized_offer.get("link") or normalized_offer.get("original_link") or "").strip()

        if not is_offer_ready_for_pending(normalized_offer):
            dropped_incomplete += 1
            discard_reasons["incompleta"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "incompleta", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "incompleta", "title": offer_title, "link": offer_link})
            log(f"descartada por pacote incompleto: {normalized_offer.get('title') or normalized_offer.get('preview_title')}")
            continue

        offer_key = canonical_offer_key(normalized_offer.get("id") or normalized_offer.get("link") or "")
        strict_key = str(normalized_offer.get("dedupe_key") or "").strip()
        loose_key = str(normalized_offer.get("loose_dedupe_key") or "").strip()

        if not offer_key and not strict_key and not loose_key:
            continue

        if offer_key and offer_key in seen_new_offer_keys:
            discard_reasons["dedupe"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "dedupe", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "dedupe", "title": offer_title, "link": offer_link})
            continue
        if offer_key and offer_key in historico_keys:
            discard_reasons["historico"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "historico", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "historico", "title": offer_title, "link": offer_link})
            continue
        if offer_key and offer_key in pending_keys:
            discard_reasons["pending"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "pending", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "pending", "title": offer_title, "link": offer_link})
            continue
        if strict_key and strict_key in seen_new_dedupe_keys:
            discard_reasons["dedupe"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "dedupe", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "dedupe", "title": offer_title, "link": offer_link})
            continue
        if strict_key and strict_key in historico_dedupe:
            discard_reasons["historico"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "historico", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "historico", "title": offer_title, "link": offer_link})
            continue
        if strict_key and strict_key in pending_dedupe:
            discard_reasons["pending"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "pending", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "pending", "title": offer_title, "link": offer_link})
            continue
        if not offer_key and not strict_key and loose_key and loose_key in historico_loose:
            discard_reasons["historico"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "historico", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "historico", "title": offer_title, "link": offer_link})
            continue
        if not offer_key and not strict_key and loose_key and loose_key in pending_loose:
            discard_reasons["pending"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "pending", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "pending", "title": offer_title, "link": offer_link})
            continue
        if not offer_key and not strict_key and loose_key and loose_key in seen_new_loose_keys:
            discard_reasons["dedupe"] += 1
            discarded_offers.append({"title": offer_title, "link": offer_link, "reason": "dedupe", "trace_id": trace_id})
            append_pipeline_audit("github.discard", trace_id, {"reason": "dedupe", "title": offer_title, "link": offer_link})
            continue
            
        if offer_key:
            seen_new_offer_keys.add(offer_key)
        if strict_key:
            seen_new_dedupe_keys.add(strict_key)
        if loose_key:
            seen_new_loose_keys.add(loose_key)

        candidates.append(normalized_offer)
        append_pipeline_audit("github.candidate", trace_id, {"title": offer_title, "link": offer_link})

    candidates = dedupe_keep_richest(candidates)
    log(f"novas fora de histórico/pending: {len(candidates)}")
    log(f"descartadas por incompletas: {dropped_incomplete}")
    save_scraper_diagnostics(
        {
            "snapshot_ids_pending": len(snapshot_ids),
            "mac_offers_loaded": len(mac_offers),
            "offers_consolidated": len(offers),
            "candidates_added": len(candidates),
            "dropped_incomplete": dropped_incomplete,
            "discard_reasons": discard_reasons,
            "discarded_offers": discarded_offers,
        }
    )

    if mac_offers and not candidates:
        log("diagnóstico mac_offers>0 e candidates==0:")
        for item in discarded_offers:
            log(
                "descarte oferta | "
                f"motivo={item.get('reason') or '-'} | "
                f"titulo={clean_text(item.get('title') or '') or '-'} | "
                f"link={item.get('link') or '-'}"
            )

    for snapshot_id in loaded_snapshot_ids:
        mark_snapshot_processed(snapshot_id, snapshot_control, snapshot_meta_map.get(snapshot_id))

    if not candidates:
        status_scraper_finish(
            summary=f"sem ofertas prontas | incompletas descartadas: {dropped_incomplete}" + (" | esgotadas atualizadas" if sold_out_changed else ""),
            status_value="sem_novidade",
            offers_seen=len(offers),
            new_offers=0,
            pending_count=len(pending.get("offers", [])),
            last_error="",
        )
        cleanup_old_snapshot_files()
        return

    pending["offers"].extend(candidates)
    pending["offers"] = dedupe_keep_richest(pending["offers"])
    pending["last_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    save_json(PENDING_FILE, pending)

    status_scraper_finish(
        summary=f"novas no pending: {len(candidates)} | incompletas descartadas: {dropped_incomplete}" + (" | esgotadas atualizadas" if sold_out_changed else ""),
        status_value="ok",
        offers_seen=len(offers),
        new_offers=len(candidates),
        pending_count=len(pending["offers"]),
        last_error="",
    )
    cleanup_old_snapshot_files()

    log(f"adicionadas ao pending: {len(candidates)}")
    log("finalizado")


if __name__ == "__main__":
    main()
