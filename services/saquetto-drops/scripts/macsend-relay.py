#!/usr/bin/env python3
"""SSH stdin relay: fixed MacSend destination, credentials stay on the server."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from urllib.parse import quote
import uuid

MAX_FILE = 100 * 1024 * 1024
ALIAS = "mac-send"
STATE_DIR = Path("/var/lib/saquetto-drops/macsend-relay")
ENV_FILE = Path("/etc/saquetto-drops/service.env")


class RelayError(Exception):
    def __init__(self, code):
        self.code = code


def credentials():
    for line in ENV_FILE.read_text().splitlines():
        if line.startswith("DROPS_MANUAL_TOKEN="):
            token = line.split("=", 1)[1].strip().strip("\"").strip("'")
            if re.fullmatch(r"[a-f0-9]{64}", token):
                return token
    raise RelayError("manual_credentials_unavailable")


def api(method, path, token, body=None, stream=None, size=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", 8788, timeout=40)
    request_headers = {"Authorization": "Bearer " + token, **(headers or {})}
    if stream is not None:
        request_headers["Content-Length"] = str(size)
    elif body is not None:
        body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    try:
        connection.request(method, path, body=stream if stream is not None else body,
                           headers=request_headers)
        response = connection.getresponse()
        value = json.loads(response.read(131072))
        return response.status, value
    except (OSError, ValueError, http.client.HTTPException):
        raise RelayError("transport_unconfirmed") from None
    finally:
        connection.close()


def status_response(request_id, code, value):
    if code == 404:
        return {"requestId": request_id, "state": "not_found"}
    if code != 200 and code != 202:
        return {"requestId": request_id, "state": "blocked", "code": safe_code(value)}
    jobs = value.get("jobs", [])
    if len(jobs) != 1 or jobs[0].get("destination") != ALIAS:
        raise RelayError("unexpected_request_destination")
    job = jobs[0]
    state = job.get("state")
    if state not in {"queued", "dispatching", "accepted", "confirmed", "unknown", "failed"}:
        raise RelayError("unexpected_request_state")
    return {"requestId": request_id, "state": state,
            "code": safe_code(job), "confirmation": safe_code(job, "confirmation")}


def safe_code(value, key="code"):
    code = value.get(key)
    return code if isinstance(code, str) and re.fullmatch(r"[a-zA-Z0-9_:-]{1,96}", code) else None


def atomic_write(path, value):
    descriptor, temporary = tempfile.mkstemp(prefix=".state-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as target:
            json.dump(value, target, ensure_ascii=False)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_envelope(source):
    raw = source.readline(65537)
    if not raw.endswith(b"\n") or len(raw) > 65536:
        raise RelayError("invalid_envelope")
    try:
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError()
        return value
    except (ValueError, TypeError):
        raise RelayError("invalid_envelope") from None


def execute(source, token, envelope=None):
    envelope = read_envelope(source) if envelope is None else envelope
    try:
        request_id = str(uuid.UUID(envelope["requestId"]))
    except (ValueError, TypeError, KeyError):
        raise RelayError("invalid_request_id") from None
    action = envelope.get("action")
    if action == "status":
        code, body = api("GET", "/v1/whatsapp/requests/" + request_id, token)
        return status_response(request_id, code, body)
    if action != "send" or envelope.get("source") not in {"macsend", "localsend"}:
        raise RelayError("invalid_action")
    text = envelope.get("text", "")
    size = envelope.get("size", 0)
    filename = envelope.get("filename", "")
    mime = envelope.get("mime", "application/octet-stream")
    if not isinstance(text, str) or len(text) > 8000:
        raise RelayError("text_too_long")
    if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= MAX_FILE:
        raise RelayError("file_too_large")
    has_file = bool(filename)
    if has_file and (not isinstance(filename, str) or len(filename.encode("utf-8")) > 1024
                     or any(c in filename for c in "\r\n\x00/\\")):
        raise RelayError("invalid_filename")
    if not isinstance(mime, str) or not re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+", mime):
        raise RelayError("invalid_mime")
    if (size and not has_file) or (not has_file and not text.strip()) or (has_file and len(text) > 1024):
        raise RelayError("invalid_content")

    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(STATE_DIR, 0o700)
    # Only a private temporary copy is removed; original files stay on the Mac.
    with contextlib.ExitStack() as stack:
        stream = stack.enter_context(tempfile.TemporaryFile(dir=STATE_DIR)) if has_file else None
        digest = hashlib.sha256()
        remaining = size
        while remaining:
            chunk = source.read(min(65536, remaining))
            if not chunk:
                raise RelayError("incomplete_upload")
            stream.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
        if stream is not None:
            stream.seek(0)
        fingerprint = hashlib.sha256(json.dumps({"source": envelope["source"], "text": text,
            "filename": filename, "mime": mime, "size": size, "sha256": digest.hexdigest()},
            sort_keys=True).encode()).hexdigest()
        lock = stack.enter_context(open(STATE_DIR / (request_id + ".lock"), "a+b"))
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        state_path = STATE_DIR / (request_id + ".json")
        ledger = json.loads(state_path.read_text()) if state_path.exists() else None
        if ledger and ledger.get("fingerprint") != fingerprint:
            raise RelayError("idempotency_conflict")
        code, body = api("GET", "/v1/whatsapp/requests/" + request_id, token)
        if code == 200:
            return status_response(request_id, code, body)
        if code != 404:
            return status_response(request_id, code, body)
        if ledger is None:
            ledger = {"fingerprint": fingerprint, "payload": {
                "requestId": request_id, "destinations": [ALIAS], "text": text, "mediaIds": []}}
            if has_file:
                code, body = api("POST", "/v1/whatsapp/media", token, stream=stream, size=size,
                    headers={"Content-Type": "application/octet-stream",
                             "X-Macsend-Filename": quote(filename, safe=""), "X-Macsend-Mime": mime})
                if code != 201 or not isinstance(body.get("mediaId"), str):
                    return {"requestId": request_id, "state": "blocked", "code": safe_code(body)}
                ledger["payload"]["mediaIds"] = [body["mediaId"]]
            atomic_write(state_path, ledger)
        code, body = api("POST", "/v1/whatsapp/send", token, body=ledger["payload"])
        return status_response(request_id, code, body)


def main():
    os.umask(0o077)
    request_id = None
    try:
        envelope = read_envelope(sys.stdin.buffer)
        request_id = str(uuid.UUID(envelope["requestId"]))
        result = execute(sys.stdin.buffer, credentials(), envelope=envelope)
    except RelayError as error:
        # A lost acknowledgement requires reconciliation, never a fresh send.
        result = {"state": "unconfirmed" if error.code == "transport_unconfirmed" else "blocked",
                  "code": error.code}
    except Exception:
        result = {"state": "blocked", "code": "relay_unavailable"}
    if request_id:
        result.setdefault("requestId", request_id)
    print(json.dumps(result, ensure_ascii=True), flush=True)


if __name__ == "__main__":
    main()
