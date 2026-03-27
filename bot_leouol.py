# bot_leouol.py
# consumer do pending_offers.json + envio para telegram
# versão final:
# - processa tudo que estiver no pending
# - localiza somente o espelhamento automático real no grupo vinculado
# - espera de 3 em 3 segundos, até 6 tentativas
# - envia logo do parceiro por upload local
# - envia descrição completa como reply da mensagem espelhada
# - limpa pending apenas do que foi enviado com sucesso
# - compatível com: python bot_leouol.py --pending

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional

import requests

# ==============================================
# configurações
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIO_ID = os.environ.get("GRUPO_COMENTARIO_ID")

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_HISTORY_SIZE = 500
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096
REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


# ==============================================
# utilidades
# ==============================================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def log_separator() -> None:
    print("-" * 60, flush=True)


def normalize_spaces(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def clean_multiline_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = str(text)
    text = unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"•\s*\n\s*", "• ", text)
    text = re.sub(r"\n\s*•\s*", "\n• ", text)
    text = text.strip()

    lixo = [
        "Enviar cupons por e-mail",
        "Preencha os campos abaixo",
        "E-mail\n\nMensagem\n\nEnviar",
    ]
    for marker in lixo:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx].rstrip()

    return text.strip()


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
        text.replace("&", "&amp;")
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


# ==============================================
# normalização
# ==============================================
def slugify_piece(text: str) -> str:
    text = unescape(text or "").lower().strip()

    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)

    text = re.sub(r"[^a-z0-9\-_/]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    text = text.strip("-/")
    return text


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

    raw = slugify_piece(raw)
    parts = [p for p in raw.split("-") if p]
    if not parts:
        return raw

    return "-".join(parts)


# ==============================================
# histórico
# ==============================================
def load_history() -> Dict[str, List[str]]:
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": []}

    data = safe_json_load(path, {"ids": []})
    ids = data.get("ids", [])
    if not isinstance(ids, list):
        ids = []

    normalized = []
    seen = set()
    for item in ids:
        key = normalize_offer_key(str(item))
        if key and key not in seen:
            seen.add(key)
            normalized.append(key)

    return {"ids": normalized[-MAX_HISTORY_SIZE:]}


def save_history(history: Dict[str, List[str]]) -> bool:
    try:
        ids = history.get("ids", [])
        if not isinstance(ids, list):
            ids = []

        cleaned =
