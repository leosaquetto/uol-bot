import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime

URL = "https://clube.uol.com.br/?order=new"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def normalize_id(link):
    if not link:
        return None

    slug = link.split("?")[0].rstrip("/").split("/")[-1]
    slug = slug.lower().strip()

    while "--" in slug:
        slug = slug.replace("--", "-")

    return slug

def get_html():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "pt-BR,pt;q=0.9"
    }
    r = requests.get(URL, headers=headers, timeout=20)
    return r.text

def parse_offers(html):
    soup = BeautifulSoup(html, "lxml")

    offers = []
    blocks = soup.select("[data-categoria]")

    for block in blocks:
        try:
            title_el = block.select_one(".titulo")
            link_el = block.select_one("a")
            img_el = block.select_one("img")

            if not title_el or not link_el:
                continue

            title = title_el.get_text(strip=True)
            link = link_el.get("href")

            if link and not link.startswith("http"):
                link = "https://clube.uol.com.br" + link

            img = img_el.get("src") if img_el else None

            offer_id = normalize_id(link)

            offers.append({
                "id": offer_id,
                "title": title,
                "link": link,
                "image": img
            })

        except Exception as e:
            log(f"erro ao parsear bloco: {e}")

    return offers

def main():
    log("iniciando scraper")

    historico = load_json("historico_leouol.json", {"ids": []})
    pending = load_json("pending_offers.json", {"last_update": None, "offers": []})

    historico_ids = set(historico["ids"])

    html = get_html()
    offers = parse_offers(html)

    log(f"total encontradas: {len(offers)}")

    novas = []

    for o in offers:
        if not o["id"]:
            continue

        if o["id"] not in historico_ids:
            novas.append(o)
            historico_ids.add(o["id"])

    log(f"novas: {len(novas)}")

    pending["offers"].extend(novas)
    pending["last_update"] = datetime.utcnow().isoformat() + "Z"

    historico["ids"] = list(historico_ids)

    save_json("pending_offers.json", pending)
    save_json("historico_leouol.json", historico)

    log("finalizado")

if __name__ == "__main__":
    main()
