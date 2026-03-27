# (arquivo corrigido - versão limpa sem duplicação)

# bot_leouol.py
# consumer do pending_offers.json + envio para telegram

import json
import os
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional

import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_HISTORY_SIZE = 500
REQUEST_TIMEOUT = 30

def log(msg: str):
    print(msg, flush=True)

def normalize_offer_key(value: str) -> str:
    return str(value or "").strip().lower()

def load_history():
    try:
        return json.load(open(HISTORY_FILE))
    except:
        return {"ids": []}

def save_history(data):
    json.dump(data, open(HISTORY_FILE, "w"), indent=2)

def load_pending():
    try:
        return json.load(open(PENDING_FILE))
    except:
        return {"offers": []}

def save_pending(data):
    json.dump(data, open(PENDING_FILE, "w"), indent=2)

def run_consumer():
    history = load_history()
    processed = set(history.get("ids", []))

    pending = load_pending().get("offers", [])

    new_pending = []

    for offer in pending:
        offer_id = normalize_offer_key(offer.get("id") or offer.get("link"))

        if offer_id in processed:
            continue

        # simulação envio telegram
        log(f"enviando: {offer_id}")

        processed.add(offer_id)

    save_history({"ids": list(processed)[-MAX_HISTORY_SIZE:]})
    save_pending({"offers": new_pending})

if __name__ == "__main__":
    run_consumer()
