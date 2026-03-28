import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import certifi
import requests
import urllib3
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError, SSLError

BASE_URL = "https://clube.uol.com.br"
LIST_URL = f"{BASE_URL}/?order=new"
FALLBACK_LIST_URL = f"{BASE_URL}/"

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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


def canonicalize_known_slug_issues(raw: str) -> str:
    if not raw:
        return ""

    replacements = {
        "teatro-joo-caetano": "teatro-joao-caetano",
    }

    for wrong, right in replacements.items():
        raw = raw.replace(wrong, right)

    return raw


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
    raw = canonicalize_known_slug_issues(raw)
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


def fetch_with_fallback(session: requests.Session, url: str, referer: Optional[str] = None) -> str:
    headers = build_headers(referer)
    try:
        r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=certifi.where(), allow_redirects=True)
        r.raise_for_status()
        return r.text
    except SSLError as e:
        log(f"ssl falhou com verificação padrão, tentando fallback sem verify: {e}")
        r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=False, allow_redirects=True)
        r.raise_for_status()
        return r.text


def get_html(url: str) -> str:
    session = requests.Session()

    try:
        return fetch_with_fallback(session, url, BASE_URL + "/")
    except HTTPError as e:
        response = getattr(e, "response", None)
        status_code = getattr(response, "status_code", None)
        if url == LIST_URL and status_code == 405:
            log("lista com ?order=new retornou 405, tentando fallback pela home")
            return fetch_with_fallback(session, FALLBACK_LIST_URL, BASE_URL + "/")
        raise


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
                "is_benefit_path": "/beneficios/" in full_src,
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
    seen_new_keys = set()

    for offer in offers:
        key = normalize_offer_key(offer.get("id") or offer.get("link"))
        if not key:
            continue
        if key in historico_keys:
            continue
        if key in pending_keys:
            continue
        if key in seen_new_keys:
            continue

        seen_new_keys.add(key)
        candidates.append(offer)

    log(f"novas fora de histórico/pending: {len(candidates)}")

    complete = []
    for offer in candidates:
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

    log(f"adicionadas ao pending: {len(complete)}")
    log("finalizado")


if __name__ == "__main__":
    main()    try:
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
        "á": "a",
        "à": "a",
        "ã": "a",
        "â": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "ó": "o",
        "ô": "o",
        "õ": "o",
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


def fetch_with_fallback(session: requests.Session, url: str, referer: Optional[str] = None) -> str:
    headers = build_headers(referer)
    try:
        r = session.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=certifi.where(),
            allow_redirects=True,
        )
        r.raise_for_status()
        return r.text
    except SSLError as e:
        log(f"ssl falhou com verificação padrão, tentando fallback sem verify: {e}")
        r = session.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False,
            allow_redirects=True,
        )
        r.raise_for_status()
        return r.text


def get_html(url: str) -> str:
    session = requests.Session()

    try:
        return fetch_with_fallback(session, url, BASE_URL + "/")
    except HTTPError as e:
        response = getattr(e, "response", None)
        status_code = getattr(response, "status_code", None)
        if url == LIST_URL and status_code == 405:
            log("lista com ?order=new retornou 405, tentando fallback pela home")
            return fetch_with_fallback(session, FALLBACK_LIST_URL, BASE_URL + "/")
        raise


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
                "is_benefit_path": "/beneficios/" in full_src,
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
        img
        for img in all_imgs
        if (not partner_img_url or img["src"] != partner_img_url) and is_likely_benefit_banner(img["src"])
    ]
    if banner_candidates:
        img_url = banner_candidates[-1]["src"]

    if not img_url:
        fallback_candidates = [
            img
            for img in all_imgs
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

            offers.append(
                {
                    "id": offer_id,
                    "original_link": link,
                    "preview_title": title,
                    "title": title,
                    "link": link,
                    "img_url": images["img_url"],
                    "partner_img_url": images["partner_img_url"],
                }
            )

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
    seen_new_keys = set()

    for offer in offers:
        key = normalize_offer_key(offer.get("id") or offer.get("link"))
        if not key:
            continue
        if key in historico_keys:
            continue
        if key in pending_keys:
            continue
        if key in seen_new_keys:
            continue

        seen_new_keys.add(key)
        candidates.append(offer)

    log(f"novas fora de histórico/pending: {len(candidates)}")

    complete = []
    for offer in candidates:
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

        complete.append(
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
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            }
        )

    if not complete:
        log("nenhuma oferta nova para adicionar")
        return

    pending["offers"].extend(complete)
    pending["offers"] = uniq_by(
        pending["offers"],
        lambda o: normalize_offer_key(o.get("id") or o.get("link")),
    )
    pending["last_update"] = datetime.utcnow().isoformat() + "Z"

    save_json(PENDING_FILE, pending)

    log(f"adicionadas ao pending: {len(complete)}")
    log("finalizado")


if __name__ == "__main__":
    main()
