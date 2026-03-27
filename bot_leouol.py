import requests
import json
import os
import time
from datetime import datetime

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

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
    return slug.lower().strip()

def send_photo(photo, caption):
    url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
    r = requests.post(url, data={
        "chat_id": CHAT_ID,
        "photo": photo,
        "caption": caption,
        "parse_mode": "HTML"
    })
    return r.json()

def send_comment(text, reply_to):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": CHAT_ID,
        "text": text,
        "reply_to_message_id": reply_to,
        "parse_mode": "HTML"
    })

def format_caption(o):
    return f"""<b>{o['title']}</b>

🔗 <a href="{o['link']}">Acessar oferta</a>

💬 Veja os detalhes completos nos comentários abaixo"""

def format_comment(o):
    return f"""<b>{o['title']}</b>

🔗 {o['link']}"""

def main():
    log("consumer iniciado")

    historico = load_json("historico_leouol.json", {"ids": []})
    pending = load_json("pending_offers.json", {"last_update": None, "offers": []})

    historico_ids = set(historico["ids"])

    enviados = 0
    novos_pending = []

    for o in pending["offers"]:
        oid = normalize_id(o["link"])

        if oid in historico_ids:
            log(f"já existe: {oid}")
            continue

        log(f"enviando: {o['title']}")

        caption = format_caption(o)
        res = send_photo(o["image"], caption)

        try:
            msg_id = res["result"]["message_id"]
        except:
            log("erro telegram")
            novos_pending.append(o)
            continue

        time.sleep(2)

        comment = format_comment(o)
        send_comment(comment, msg_id)

        historico_ids.add(oid)
        enviados += 1

    historico["ids"] = list(historico_ids)
    pending["offers"] = novos_pending

    save_json("historico_leouol.json", historico)
    save_json("pending_offers.json", pending)

    log(f"fim: {enviados} enviados")

if __name__ == "__main__":
    main()
