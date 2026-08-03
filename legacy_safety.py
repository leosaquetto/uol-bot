"""Safety helpers shared by the archived Python fallback programs.

The active offer delivery runs in Cloudflare. These helpers exist so that a
manual legacy recovery cannot copy credentials into logs, state or audit files.
"""

from __future__ import annotations

import os
import re
from typing import Any


REDACTED = "[REDACTED]"

_SENSITIVE_KEY_RE = re.compile(
    r"(?:authorization|password|passwd|secret|token|api[_-]?key|credential|cookie)",
    re.IGNORECASE,
)
_TELEGRAM_BOT_URL_RE = re.compile(
    r"(https?://api\.telegram\.org/bot)[^/\s]+",
    re.IGNORECASE,
)
_AUTH_HEADER_RE = re.compile(r"\b(Bearer|Basic|token)\s+[^\s,;]+", re.IGNORECASE)
_KNOWN_TOKEN_RE = re.compile(
    r"\b(?:github_pat_[A-Za-z0-9_]{16,}|gh[pousr]_[A-Za-z0-9_]{16,})\b"
)
_SENSITIVE_QUERY_RE = re.compile(
    r"([?&](?:access[_-]?token|token|api[_-]?key|apikey|secret|password|authorization)=)[^&#\s\"']+",
    re.IGNORECASE,
)
_URL_USERINFO_RE = re.compile(r"(https?://)[^/@\s:]+:[^/@\s]+@", re.IGNORECASE)


def _configured_secrets() -> list[str]:
    values: list[str] = []
    for name, value in os.environ.items():
        if _SENSITIVE_KEY_RE.search(name) and value and len(value) >= 4:
            values.append(value)
    return sorted(set(values), key=len, reverse=True)


def redact_sensitive_text(value: Any) -> str:
    """Return a printable string with credentials and secret URL parts removed."""

    text = str(value if value is not None else "")
    for secret in _configured_secrets():
        text = text.replace(secret, REDACTED)
    text = _TELEGRAM_BOT_URL_RE.sub(r"\1[REDACTED]", text)
    text = _AUTH_HEADER_RE.sub(lambda match: f"{match.group(1)} {REDACTED}", text)
    text = _KNOWN_TOKEN_RE.sub(REDACTED, text)
    text = _SENSITIVE_QUERY_RE.sub(lambda match: f"{match.group(1)}{REDACTED}", text)
    text = _URL_USERINFO_RE.sub(r"\1[REDACTED]@", text)
    return text


def sanitize_audit_value(value: Any, key_hint: str = "") -> Any:
    """Recursively sanitize a JSON-compatible value before persistence."""

    if key_hint and _SENSITIVE_KEY_RE.search(str(key_hint)):
        return REDACTED
    if isinstance(value, dict):
        return {str(key): sanitize_audit_value(item, str(key)) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_audit_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_audit_value(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_text(value)
    return value
