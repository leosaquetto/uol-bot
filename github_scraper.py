import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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

REQUEST_TIMEOUT = 30

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def now_br_date() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def now_br_time() -> str:
    return datetime.now().strftime("%H:%M")


def now_br_datetime() -> str:
    return datetime.now().strftime("%d/%m/%Y às %H:%M")


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
            "status": "",
            "summary": "",
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "pending_count": 0,
            "last_error": "",
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

    return data


def save_status_runtime(data: Dict) -> None:
    Path(STATUS_RUNTIME_FILE).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def status_scraper_start() -> None:
    status = load_status_runtime()
    status["scraper"] = {
        "last_started_at": now_br_datetime(),
        "last_finished_at": status["scraper"].get("last_finished_at", ""),
        "status": "running",
        "summary": "scraper iniciado",
        "offers_seen": 0,
        "new_offers": 0,
        "pending_count": status["scraper"].get("pending_count", 0),
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
    status["scraper"] = {
        "last_started_at": status["scraper"].get("last_started_at", ""),
        "last_finished_at": now_br_datetime(),
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


def build_dashboard_text(state: Dict) -> str:
    today = state["date"] or now_br_date()

    last_success = state["last_success_check"] or "—"
    last_new = state["last_new_offer_at"] or "—"
    pending_count = state["pending_count"]
    last_consumer = state["last_consumer_run"] or "—"

    header = [
        f"📊 <b>relatório diário uol - {escape_html(today)}</b>",
        "",
        f"última verificação com sucesso: {escape_html(last_success)}",
        f"última oferta nova encontrada: {escape_html(last_new)}",
        f"pending atual: {escape_html(str(pending_count))}",
        f"última execução do consumer: {escape_html(last_consumer)}",
        "",
    ]

    lines = state.get("lines", [])
    body = [escape_html(x) for x in lines[-20:]] if lines else ["sem registros ainda"]

    text = "\n".join(header + body)
    if len(text) > 3900:
        text = text[:3900]
    return text


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
            if resp.ok:
                data = resp.json()
                state["message_id"] = data.get("result", {}).get("message_id")
                save_daily_log(state)
            else:
                log(f"falha ao criar dashboard diário: {resp.text}")
        except Exception as e:
            log(f"falha ao criar dashboard diário: {e}")
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
            save_daily_log(state)
        else:
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
            "lines": [],
        }

    line = f"[{now_br_time()}] {source}: {status_line}"
    state["lines"].append(line)
    state["lines"] = state["lines"][-30:]

    sync_daily_dashboard(state)


def set_dashboard_success_check() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["last_success_check"] = now_br_datetime()
    sync_daily_dashboard(state)


def set_dashboard_last_new_offer() -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
    state["last_new_offer_at"] = now_br_datetime()
    sync_daily_dashboard(state)


def set_dashboard_pending_count(count: int) -> None:
    state = load_daily_log()
    if state["date"] != now_br_date():
        state["date"] = now_br_date()
        state["message_id"] = None
        state["lines"] = []
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
        if not low:
            continue
        if len(low) < 12:
            continue
        if any(low.startswith(normalize_text_key(x)) for x in blacklist_starts):
            continue
        filtered.append(low)

    if not filtered:
        return ""

    return filtered[0][:160]


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

    candidates = [
        (url, BASE_URL + "/"),
    ]

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

    return {
        "img_url": img_url,
        "partner_img_url": partner_img_url,
    }


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
            if (
                "ingresso" in low
                or "ingressos" in low
                or "campanhasdeingresso" in hrefs
            ):
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
            return {
                "title": preview_title,
                "validity": None,
                "description": "descrição não disponível.",
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

        description = description[:4000]

        return {
            "title": page_title,
            "validity": validity,
            "description": description,
            "detail_img_url": detail_img_url,
        }
    except Exception as e:
        log(f"erro ao extrair detalhes: {e}")
        return {
            "title": preview_title,
            "validity": None,
            "description": "descrição não disponível.",
            "detail_img_url": "",
        }


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


def main() -> None:
    log("iniciando scraper")
    status_scraper_start()
    append_dashboard_line("scraper", "▶️ rodada iniciada")

    historico = load_json(HISTORY_FILE, {"ids": [], "dedupe_keys": []})
    pending = load_json(PENDING_FILE, {"last_update": None, "offers": []})

    if not isinstance(pending.get("offers"), list):
        pending["offers"] = []

    historico_keys, historico_dedupe = extract_history_sets(historico)
    pending_keys, pending_dedupe = extract_pending_sets(pending)

    html = get_html(LIST_URL)
    if not html:
        log("não foi possível obter html da lista nesta rodada; encerrando sem alterações")
        append_dashboard_line("scraper", "⚠️ html indisponível / 405 / ssl")
        status_scraper_finish(
            summary="html indisponível / 405 / ssl",
            status_value="erro",
            offers_seen=0,
            new_offers=0,
            pending_count=len(pending.get("offers", [])),
            last_error="falha ao obter html da lista",
        )
        return

    set_dashboard_success_check()

    offers = parse_offers(html)
    log(f"total encontradas: {len(offers)}")

    candidates = []
    seen_new_offer_keys = set()
    seen_new_dedupe_keys = set()

    for offer in offers:
        details = extract_offer_details(offer["link"], offer["preview_title"])
        final_title = details["title"] or offer["title"]
        final_partner = absolutize_url(offer.get("partner_img_url") or "")

        final_img = absolutize_url(details["detail_img_url"] or "")
        if (
            not final_img
            or is_bad_banner_url(final_img)
            or final_img == final_partner
        ):
            fallback_img = absolutize_url(offer.get("img_url") or "")
            if (
                fallback_img
                and not is_bad_banner_url(fallback_img)
                and fallback_img != final_partner
            ):
                final_img = fallback_img

        if (
            not final_img
            or is_bad_banner_url(final_img)
            or final_img == final_partner
        ):
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

        candidates.append({
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
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        })

    log(f"novas fora de histórico/pending: {len(candidates)}")

    if not candidates:
        log("nenhuma oferta nova para adicionar")
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
        lambda o: (
            str(o.get("dedupe_key") or "").strip()
            or normalize_offer_key(o.get("id") or o.get("link"))
        )
    )
    pending["last_update"] = datetime.utcnow().isoformat() + "Z"

    save_json(PENDING_FILE, pending)

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
