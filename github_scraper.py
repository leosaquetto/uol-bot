# github_scraper.py
# produtor automático do sistema híbrido
# função:
# - buscar ofertas no clube uol
# - extrair detalhes
# - ler historico_leouol.json
# - ler pending_offers.json
# - considerar "já visto" = histórico + pending
# - salvar novas ofertas no pending
# - atualizar histórico
#
# observação:
# este script não envia nada ao telegram.
# quem faz isso é o bot_leouol.py

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ==============================================
# configurações
# ==============================================
TARGET_URL = "https://clube.uol.com.br/?order=new"
BASE_URL = "https://clube.uol.com.br"

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_HISTORY_SIZE = 500
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_DETAIL_REQUESTS = 1.2

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": BASE_URL + "/",
}

# ==============================================
# utilidades
# ==============================================
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def safe_json_load(path_str: str, default: Any) -> Any:
    path = Path(path_str)
    if not path.exists():
        return default

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"⚠️ erro lendo {path_str}: {e}")
        return default


def safe_json_save(path_str: str, payload: Any) -> bool:
    try:
        Path(path_str).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return True
    except Exception as e:
        log(f"❌ erro escrevendo {path_str}: {e}")
        return False


def get_offer_id(link: str) -> str:
    try:
        parsed = urlparse(str(link))
        path = parsed.path.split("?")[0].rstrip("/")
        if not path:
            return str(link).split("?")[0].rstrip("/").split("/")[-1]
        return path.split("/")[-1]
    except Exception:
        return str(link).split("?")[0].rstrip("/").split("/")[-1]


def absolutize_url(url: str | None) -> str:
    if not url:
        return ""
    return urljoin(BASE_URL + "/", url)


def unique_by_offer_id_from_urls(urls: list[str]) -> list[str]:
    unique: dict[str, str] = {}
    for url in urls:
        if not url:
            continue
        unique[get_offer_id(url)] = url
    return list(unique.values())


def unique_by_offer_id_from_offers(offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for offer in offers:
        link = offer.get("link") or offer.get("original_link")
        if not link:
            continue
        unique[get_offer_id(link)] = offer
    return list(unique.values())


# ==============================================
# histórico / pending
# ==============================================
def load_history() -> dict[str, list[str]]:
    payload = safe_json_load(HISTORY_FILE, {"ids": []})

    if isinstance(payload, list):
        original_urls = [str(x) for x in payload if x]
    elif isinstance(payload, dict):
        raw_ids = payload.get("ids", []) or []
        original_urls = [str(x) for x in raw_ids if x]
    else:
        original_urls = []

    original_urls = unique_by_offer_id_from_urls(original_urls)
    ids = [get_offer_id(url) for url in original_urls]

    log(f"📚 histórico carregado: {len(ids)} ids")
    return {"ids": ids, "original_urls": original_urls}


def save_history(original_urls: list[str]) -> bool:
    deduped_urls = unique_by_offer_id_from_urls(original_urls)[-MAX_HISTORY_SIZE:]
    ok = safe_json_save(HISTORY_FILE, {"ids": deduped_urls})
    if ok:
        log(f"✅ histórico salvo: {len(deduped_urls)} ids")
    return ok


def load_pending() -> dict[str, Any]:
    payload = safe_json_load(PENDING_FILE, {"last_update": None, "offers": []})

    if not isinstance(payload, dict):
        payload = {"last_update": None, "offers": []}

    offers = payload.get("offers", [])
    if not isinstance(offers, list):
        offers = []

    normalized: list[dict[str, Any]] = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        link = offer.get("link") or offer.get("original_link")
        if not link:
            continue
        normalized.append(offer)

    normalized = unique_by_offer_id_from_offers(normalized)

    result = {
        "last_update": payload.get("last_update"),
        "offers": normalized,
    }
    log(f"📦 pending carregado: {len(normalized)} ofertas")
    return result


def save_pending(offers: list[dict[str, Any]]) -> bool:
    deduped = unique_by_offer_id_from_offers(offers)
    payload = {
        "last_update": datetime.utcnow().isoformat() + "Z",
        "offers": deduped,
    }
    ok = safe_json_save(PENDING_FILE, payload)
    if ok:
        log(f"✅ pending salvo: {len(deduped)} ofertas")
    return ok


def load_seen_ids() -> tuple[set[str], list[str], list[dict[str, Any]]]:
    history = load_history()
    pending_payload = load_pending()

    history_ids = set(history["ids"])
    pending_offers = pending_payload["offers"]
    pending_ids = {
        get_offer_id(o.get("link") or o.get("original_link") or "")
        for o in pending_offers
        if (o.get("link") or o.get("original_link"))
    }

    seen_ids = history_ids | pending_ids
    log(f"👀 ids conhecidos: {len(seen_ids)} (histórico + pending)")
    return seen_ids, history["original_urls"], pending_offers


# ==============================================
# requests helpers
# ==============================================
def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def get_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


# ==============================================
# scraping listagem
# ==============================================
def scrape_offers_list(session: requests.Session) -> list[dict[str, Any]]:
    log("🌐 buscando ofertas no clube uol...")

    try:
        html = get_html(session, TARGET_URL)
        log(f"✅ página baixada: {len(html)} caracteres")
    except Exception as e:
        log(f"❌ erro ao baixar listagem: {e}")
        return []

    soup = BeautifulSoup(html, "lxml")

    offers: list[dict[str, Any]] = []
    seen_links: set[str] = set()

    candidate_blocks = soup.select("[data-categoria]")

    log(f"📦 blocos encontrados: {len(candidate_blocks)}")

    for block in candidate_blocks:
        try:
            anchor = block.find("a", href=True)
            if not anchor:
                continue

            raw_link = clean_text(anchor.get("href"))
            if not raw_link:
                continue
            if raw_link == "#":
                continue
            if "javascript" in raw_link.lower():
                continue
            if "minhas-recompensas" in raw_link.lower():
                continue

            link = absolutize_url(raw_link)
            if not link or link in seen_links:
                continue

            title = ""
            title_node = block.select_one("[class*='titulo']")
            if title_node:
                title = clean_text(title_node.get_text(" ", strip=True))

            if not title:
                btn_node = block.select_one("a[class*='btn'], .btn")
                if btn_node:
                    title = clean_text(btn_node.get_text(" ", strip=True))

            if not title:
                anchor_text = clean_text(anchor.get_text(" ", strip=True))
                if anchor_text:
                    title = anchor_text

            if not title:
                continue

            img_url = ""
            img = block.find("img")
            if img:
                candidate = img.get("data-src") or img.get("src") or ""
                candidate = clean_text(candidate)
                if candidate and "data:image" not in candidate and "/parceiros/" not in candidate:
                    img_url = absolutize_url(candidate)

            seen_links.add(link)

            offer = {
                "id": get_offer_id(link),
                "original_link": link,
                "preview_title": title,
                "title": title,
                "link": link,
                "img_url": img_url,
            }
            offers.append(offer)
            log(f"  🎫 {title[:60]}...")
        except Exception:
            continue

    unique_offers = unique_by_offer_id_from_offers(offers)
    log(f"📊 total de ofertas únicas na listagem: {len(unique_offers)}")
    return unique_offers


# ==============================================
# scraping detalhe
# ==============================================
def extract_validity_from_text(text: str) -> str | None:
    if not text:
        return None

    patterns = [
        r"[Bb]enef[ií]cio v[aá]lido de[^.!?\n]*[.!?]?",
        r"[Vv][aá]lido at[eé][^.!?\n]*[.!?]?",
        r"\d{2}/\d{2}/\d{4}.*?\d{2}/\d{2}/\d{4}",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            validity = clean_text(match.group(0))
            if validity:
                return validity

    return None


def scrape_offer_detail(
    session: requests.Session,
    url: str,
    preview_title: str,
    fallback_img_url: str = "",
) -> dict[str, Any]:
    log(f"   🔍 acessando: {preview_title[:55]}...")

    try:
        html = get_html(session, absolutize_url(url))
    except Exception as e:
        log(f"   ⚠️ erro no detalhe: {e}")
        return {
            "title": preview_title,
            "validity": None,
            "description": "descrição não disponível.",
            "detail_img_url": fallback_img_url or "",
        }

    soup = BeautifulSoup(html, "lxml")

    page_title = preview_title
    h2 = soup.find("h2")
    if h2:
        h2_text = clean_text(h2.get_text(" ", strip=True))
        if h2_text:
            page_title = h2_text

    detail_img_url = fallback_img_url or ""
    img = soup.select_one("img.responsive") or soup.find("img")
    if img:
        candidate = img.get("data-src") or img.get("src") or ""
        candidate = clean_text(candidate)
        if candidate and "data:image" not in candidate:
            detail_img_url = absolutize_url(candidate)

    full_text = clean_text(soup.get_text("\n", strip=True))
    validity = extract_validity_from_text(full_text)

    description = ""
    info_node = soup.select_one(".info-beneficio")
    if info_node:
        for br in info_node.find_all("br"):
            br.replace_with("\n")

        for li in info_node.find_all("li"):
            li.insert_before("\n• ")

        description = clean_text(info_node.get_text("\n", strip=True))

    if not description:
        paragraphs = [
            clean_text(p.get_text(" ", strip=True))
            for p in soup.find_all("p")
        ]
        paragraphs = [p for p in paragraphs if len(p) > 30]
        if paragraphs:
            description = "\n\n".join(paragraphs[:6])

    if not description or len(description) < 20:
        description = "descrição detalhada não disponível."

    description = description[:4000]

    return {
        "title": page_title,
        "validity": validity,
        "description": description,
        "detail_img_url": detail_img_url,
    }


# ==============================================
# merge e persistência
# ==============================================
def merge_new_offers_into_pending(
    existing_pending: list[dict[str, Any]],
    new_offers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged = existing_pending + new_offers
    return unique_by_offer_id_from_offers(merged)


# ==============================================
# main
# ==============================================
def main() -> None:
    log("=" * 60)
    log("🤖 github scraper - produtor automático")
    log("=" * 60)

    session = build_session()

    seen_ids, history_original_urls, existing_pending = load_seen_ids()

    all_offers = scrape_offers_list(session)
    if not all_offers:
        log("📭 nenhuma oferta encontrada")
        return

    new_preview_offers = [offer for offer in all_offers if offer["id"] not in seen_ids]

    log(f"📊 total encontradas: {len(all_offers)}")
    log(f"📊 novas ofertas: {len(new_preview_offers)}")

    if not new_preview_offers:
        log("📭 nenhuma oferta nova")
        log("🏁 fim")
        return

    log(f"🎉 {len(new_preview_offers)} ofertas novas")
    print("-" * 60)

    complete_offers: list[dict[str, Any]] = []

    for index, offer in enumerate(new_preview_offers, start=1):
        log(f"📌 oferta {index}/{len(new_preview_offers)}")
        log(f"   id: {offer['id']}")

        detail = scrape_offer_detail(
            session=session,
            url=offer["link"],
            preview_title=offer.get("preview_title") or offer.get("title") or "",
            fallback_img_url=offer.get("img_url") or "",
        )

        final_title = detail.get("title") or offer.get("preview_title") or offer.get("title") or "oferta"
        final_img_url = detail.get("detail_img_url") or offer.get("img_url") or ""

        complete_offers.append(
            {
                "id": offer["id"],
                "original_link": offer["original_link"],
                "preview_title": offer.get("preview_title") or final_title,
                "title": final_title,
                "link": offer["link"],
                "img_url": final_img_url,
                "validity": detail.get("validity"),
                "description": detail.get("description") or "descrição não disponível.",
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            }
        )

        time.sleep(SLEEP_BETWEEN_DETAIL_REQUESTS)

    print("-" * 60)
    log(f"✅ detalhes extraídos: {len(complete_offers)}")

    merged_pending = merge_new_offers_into_pending(existing_pending, complete_offers)
    if not save_pending(merged_pending):
        raise SystemExit(1)

    updated_history_urls = history_original_urls + [
        offer["original_link"] for offer in complete_offers if offer.get("original_link")
    ]
    if not save_history(updated_history_urls):
        raise SystemExit(1)

    log(f"✅ {len(complete_offers)} ofertas adicionadas ao pending")
    log("🏁 fim")


if __name__ == "__main__":
    main()
