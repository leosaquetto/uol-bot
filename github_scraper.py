import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://clube.uol.com.br"
LIST_URL = f"{BASE_URL}/?order=new"

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


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


def normalize_offer_key(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""

    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)

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

    raw = re.sub(r"[^a-z0-9\-_\/]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw)
    raw = raw.strip("-/")
    return raw


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


def get_html(url: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": BASE_URL + "/",
    }
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def extract_all_img_urls(block) -> List[str]:
    urls = []
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
        urls.append(absolutize_url(src))

    deduped = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def choose_images_from_block(block) -> Dict[str, str]:
    all_imgs = extract_all_img_urls(block)
    partner_img_url = ""
    img_url = ""

    if len(all_imgs) == 1:
        img_url = all_imgs[0]
    elif len(all_imgs) >= 2:
        partner_img_url = all_imgs[0]
        img_url = all_imgs[-1]

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

        page_title = preview_title
        for regex in [
            re.compile(r"<h2[^>]*>([\s\S]*?)</h2>", re.I),
            re.compile(r"<h1[^>]*>([\s\S]*?)</h1>", re.I),
        ]:
            m = regex.search(html)
            if m:
                page_title = clean_text(re.sub(r"<[^>]+>", " ", m.group(1)))
                break

        all_imgs = []
        for m in re.finditer(r'<img[^>]+(?:data-src|data-original|data-lazy|src)=["\']([^"\']+)["\']', html, re.I):
            src = absolutize_url(m.group(1))
            if src and not src.startswith("data:image"):
                all_imgs.append(src)
        detail_img_url = all_imgs[-1] if all_imgs else ""

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


def main() -> None:
    log("iniciando scraper")

    historico = load_json(HISTORY_FILE, {"ids": []})
    pending = load_json(PENDING_FILE, {"last_update": None, "offers": []})

    if not isinstance(historico.get("ids"), list):
        historico["ids"] = []

    if not isinstance(pending.get("offers"), list):
        pending["offers"] = []

    historico_keys = set(normalize_offer_key(x) for x in historico["ids"])
    pending_keys = set(normalize_offer_key(o.get("id") or o.get("link")) for o in pending["offers"])

    html = get_html(LIST_URL)
    offers = parse_offers(html)

    log(f"total encontradas: {len(offers)}")

    candidates = []
    for offer in offers:
        key = normalize_offer_key(offer.get("id") or offer.get("link"))
        if not key:
            continue
        if key in historico_keys:
            continue
        if key in pending_keys:
            continue
        candidates.append(offer)

    log(f"novas fora de histórico/pending: {len(candidates)}")

    complete = []
    for offer in candidates:
        details = extract_offer_details(offer["link"], offer["preview_title"])
        final_title = details["title"] or offer["title"]
        final_img = absolutize_url(details["detail_img_url"] or offer["img_url"] or "")
        final_partner = absolutize_url(offer.get("partner_img_url") or "")

        complete.append({
            "id": offer["id"],
            "original_link": offer["original_link"],
            "preview_title": offer["preview_title"] or final_title,
            "title": final_title,
            "link": offer["link"],
            "img_url": final_img,
            "partner_img_url": final_partner,
            "validity": details["validity"],
            "description": details["description"],
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        })

    if not complete:
        log("nenhuma oferta nova para adicionar")
        return

    pending["offers"].extend(complete)
    pending["offers"] = uniq_by(
        pending["offers"],
        lambda o: normalize_offer_key(o.get("id") or o.get("link"))
    )
    pending["last_update"] = datetime.utcnow().isoformat() + "Z"

    save_json(PENDING_FILE, pending)

    # importante:
    # não grava no histórico aqui.
    # o histórico deve continuar sendo responsabilidade do consumer,
    # apenas após envio real no telegram.

    log(f"adicionadas ao pending: {len(complete)}")
    log("finalizado")


if __name__ == "__main__":
    main()
