#!/usr/bin/env python3
"""Scoped local Beeper adapter. Never exposes the general Desktop API."""
import hmac
import json
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CHAT = "imsg##thread:0d1d661d521ad54a15db20440c5a00c0782c639cc90ea3d1"
TEXT = "Você entrou em uma Smart Fit"
CHAT_PATH = "/v1/chats/" + urllib.parse.quote(CHAT, safe="")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Relay:
    def __init__(self, config, upstream=None):
        self.token = config["gateway_token"]
        self.oauth_path = Path(config["beeper_oauth_path"])
        self.database = config["database"]
        Path(self.database).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS sends (key TEXT PRIMARY KEY, state TEXT NOT NULL, receipt TEXT)")
        os.chmod(self.database, 0o600)
        self.upstream = upstream or self.beeper

    def connect(self):
        return sqlite3.connect(self.database, timeout=10)

    def beeper(self, method, path, payload=None):
        token = json.loads(self.oauth_path.read_text())["access_token"]
        req = urllib.request.Request(
            "http://127.0.0.1:23373" + path,
            data=json.dumps(payload).encode() if payload is not None else None,
            method=method,
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        )
        with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(req, timeout=6) as response:
            body = response.read(1_048_577)
            if len(body) > 1_048_576:
                raise ValueError("oversize")
            return json.loads(body)

    def handle(self, method, path, authorization, key="", body=None):
        if not hmac.compare_digest(authorization.encode(), ("Bearer " + self.token).encode()):
            return 401, {"error": "unauthorized"}
        # Exact paths only; no general proxy, account enumeration, or arbitrary chat access.
        if method == "GET" and path == CHAT_PATH:
            try:
                chat = self.upstream("GET", CHAT_PATH)
                if chat.get("id") != CHAT or chat.get("network") != "iMessage":
                    return 502, {"error": "destination_mismatch"}
                return 200, {"id": CHAT, "network": "iMessage"}
            except Exception:
                return 503, {"error": "beeper_unavailable"}
        if method == "POST" and path == CHAT_PATH + "/messages":
            if body != {"text": TEXT} or not re.fullmatch(r"gymrats:336445:[1-9]\d{0,19}", key):
                return 400, {"error": "invalid_notification"}
            with self.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT state, receipt FROM sends WHERE key=?", (key,)).fetchone()
                if row:
                    if row[1]:
                        return 200, {"chatID": CHAT, "pendingMessageID": row[1]}
                    return 409, {"error": "delivery_requires_review"}
                # Commit before crossing the network. A crash cannot release this key.
                db.execute("INSERT INTO sends(key,state) VALUES (?, 'attempted')", (key,))
            try:
                result = self.upstream("POST", CHAT_PATH + "/messages", {"text": TEXT})
                receipt = result.get("pendingMessageID")
                if not isinstance(receipt, str) or not receipt or len(receipt) > 512:
                    raise ValueError("missing_receipt")
                with self.connect() as db:
                    db.execute("UPDATE sends SET state='pending',receipt=? WHERE key=?", (receipt, key))
                return 200, {"chatID": CHAT, "pendingMessageID": receipt}
            except Exception:
                with self.connect() as db:
                    db.execute("UPDATE sends SET state='uncertain' WHERE key=?", (key,))
                return 502, {"error": "delivery_requires_review"}
        prefix = CHAT_PATH + "/messages/"
        if method == "GET" and path.startswith(prefix):
            receipt = urllib.parse.unquote(path[len(prefix):])
            with self.connect() as db:
                row = db.execute("SELECT key FROM sends WHERE receipt=?", (receipt,)).fetchone()
            if not row:
                return 404, {"error": "receipt_not_found"}
            try:
                message = self.upstream("GET", prefix + urllib.parse.quote(receipt, safe=""))
                if message.get("chatID") != CHAT or message.get("isSender") is not True or message.get("text") != TEXT:
                    return 502, {"error": "receipt_mismatch"}
                status = (message.get("sendStatus") or {}).get("status")
                return 200, {"chatID": CHAT, "isSender": True, "text": TEXT, "sendStatus": {"status": status}}
            except Exception:
                return 503, {"error": "receipt_unavailable"}
        return 404, {"error": "not_found"}


def serve(config):
    relay = Relay(config)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # No URLs, tokens, identifiers, or message content in logs.

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def do_GET(self):
            self.dispatch()

        def do_POST(self):
            self.dispatch()

        def dispatch(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > 1024 or self.headers.get("Transfer-Encoding"):
                    status, value = 413, {"error": "invalid_body"}
                else:
                    payload = json.loads(self.rfile.read(length)) if length else None
                    status, value = relay.handle(
                        self.command, self.path, self.headers.get("Authorization", ""),
                        self.headers.get("Idempotency-Key", ""), payload,
                    )
            except (ValueError, UnicodeError):
                status, value = 400, {"error": "invalid_body"}
            except Exception:
                status, value = 500, {"error": "internal_error"}
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    ThreadingHTTPServer(("127.0.0.1", 18788), Handler).serve_forever()


if __name__ == "__main__":
    import sys
    os.umask(0o077)
    serve(json.loads(Path(sys.argv[1]).read_text()))
