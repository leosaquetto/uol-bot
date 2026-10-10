"""Bounded, read-only Instagram Story transport using an existing session.

This module never logs in, follows other profiles, queries UOL, or sends alerts.
Cookie values belong only in the protected session file and request headers.
"""

import datetime
import email.utils
import html.parser
import http.cookiejar
import http.cookies
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

URL = "https://www.instagram.com/stories/clubeuol/"
MAX_BYTES = 4 * 1024 * 1024
MAX_SESSION_BYTES = 256 * 1024
MAX_DEPTH = 50
MAX_NODES = 100000
TIMEOUT = 20
COOKIE_NAMES = frozenset(("sessionid", "csrftoken", "ds_user_id", "mid", "ig_did",
                          "rur", "datr", "ig_nrcb", "ps_l", "ps_n", "wd", "dpr"))
COOKIE_DOMAINS = frozenset((".instagram.com", "instagram.com", "www.instagram.com"))
HEADER_NAMES = frozenset((
    "accept", "accept-language", "upgrade-insecure-requests", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site", "sec-fetch-user", "dpr", "viewport-width",
    "sec-ch-prefers-color-scheme", "sec-ch-ua", "sec-ch-ua-full-version-list",
    "sec-ch-ua-mobile", "sec-ch-ua-model", "sec-ch-ua-platform",
    "sec-ch-ua-platform-version",
))
TRACKING_NAMES = frozenset(("fbclid", "igshid", "igsh", "utm_source", "utm_medium",
                            "utm_campaign", "utm_term", "utm_content", "utm_id"))
PRIVATE_QUERY_NAMES = frozenset(("token", "key", "code", "codigo", "coupon", "cupom",
                                "password", "senha", "session", "sessionid", "auth",
                                "authorization", "jwt", "access_token", "signature",
                                "sig", "ticket"))
PUBLIC_PATH = re.compile(r"/[a-z][a-z0-9-]*/[A-Za-z0-9]{2,6}-[A-Za-z0-9][A-Za-z0-9-]*")
MEDIA_PATH = re.compile(r"/v/[A-Za-z0-9_./-]+\.(?:jpe?g|webp)", re.IGNORECASE)
REDIRECT_CODES = frozenset((301, 302, 303, 307, 308))
MAX_RETRY_AFTER_SECONDS = 365 * 24 * 60 * 60


class SessionError(Exception):
    """An operational error whose public message never contains session data."""

    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class JSONScripts(html.parser.HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.active = False
        self.chunks = []
        self.scripts = []
        self.login_form = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script":
            self.active = attrs.get("type") == "application/json"
            self.chunks = []
        if tag == "form":
            action = attrs.get("action", "")
            if isinstance(action, str) and action.startswith("/accounts/login"):
                self.login_form = True

    def handle_data(self, data):
        if self.active:
            self.chunks.append(data)

    def handle_endtag(self, tag):
        if tag == "script" and self.active:
            self.scripts.append("".join(self.chunks))
            self.active = False


def normalize_destination(value):
    """Accept structurally public Clube UOL destinations; never return trackers."""
    if not isinstance(value, str) or len(value) > 8192 or any(c.isspace() for c in value):
        return None
    try:
        parsed = urllib.parse.urlsplit(value)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port is not None:
            return None
        if parsed.hostname == "l.instagram.com":
            if parsed.path not in ("", "/"):
                return None
            wrapped = urllib.parse.parse_qs(parsed.query, max_num_fields=100).get("u", [])
            return normalize_destination(wrapped[0]) if len(wrapped) == 1 else None
        if parsed.hostname != "clube.uol.com.br" or not PUBLIC_PATH.fullmatch(parsed.path):
            return None
        pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True, max_num_fields=100)
        if any(name.lower() in PRIVATE_QUERY_NAMES or name.lower() not in TRACKING_NAMES
               for name, _ in pairs):
            return None
        return urllib.parse.urlunsplit(("https", "clube.uol.com.br", parsed.path, "", ""))
    except (ValueError, RecursionError):
        return None


def safe_media_url(value):
    """Validate a CDN image URL, retaining its signature only for private delivery."""
    if (not isinstance(value, str) or len(value) > 8192
            or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)):
        return None
    try:
        parsed = urllib.parse.urlsplit(value)
        host = parsed.hostname or ""
        if (parsed.scheme != "https" or parsed.username or parsed.password
                or parsed.port is not None or parsed.netloc.lower() != host
                or parsed.fragment or len(host) > 253
                or not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                           for label in host.split("."))
                or not host.endswith((".cdninstagram.com", ".fbcdn.net"))
                or not MEDIA_PATH.fullmatch(parsed.path)
                or any(part in (".", "..", "") for part in parsed.path[1:].split("/"))):
            return None
        return value
    except ValueError:
        return None


def _image_dimensions(width, height):
    return (type(width) is int and type(height) is int
            and 0 < width <= 100000 and 0 < height <= 100000)


def _story_image(item):
    """Select the full Story image (or video cover), avoiding square crop variants."""
    versions = item.get("image_versions2")
    candidates = versions.get("candidates") if isinstance(versions, dict) else None
    if not isinstance(candidates, list):
        return {"imageUrl": "", "imageWidth": 0, "imageHeight": 0}
    valid = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        width, height = candidate.get("width"), candidate.get("height")
        url = safe_media_url(candidate.get("url"))
        if url and _image_dimensions(width, height):
            valid.append({"imageUrl": url, "imageWidth": width, "imageHeight": height})
    original_width, original_height = item.get("original_width"), item.get("original_height")
    if _image_dimensions(original_width, original_height):
        # Resized candidates round their dimensions; allow that small ratio change.
        valid = [candidate for candidate in valid
                 if abs(candidate["imageWidth"] * original_height
                        - candidate["imageHeight"] * original_width) * 100
                 <= 2 * candidate["imageHeight"] * original_width]
    else:
        portrait = [candidate for candidate in valid
                    if candidate["imageHeight"] > candidate["imageWidth"]]
        if portrait:
            valid = portrait
    return max(valid, key=lambda candidate: (candidate["imageHeight"], candidate["imageWidth"]),
               default={"imageUrl": "", "imageWidth": 0, "imageHeight": 0})


def _parse_item(item):
    if not isinstance(item, dict):
        return None
    code = item.get("pk")
    if isinstance(code, bool) or not isinstance(code, (str, int)):
        return None
    code = str(code)
    if not re.fullmatch(r"[0-9]{1,40}", code):
        return None
    taken, expires = item.get("taken_at"), item.get("expiring_at")
    if (type(taken) is not int or type(expires) is not int or taken <= 0 or expires <= taken):
        return None
    try:
        published = datetime.datetime.fromtimestamp(taken, datetime.timezone.utc).isoformat()
        expiry = datetime.datetime.fromtimestamp(expires, datetime.timezone.utc).isoformat()
    except (ValueError, OverflowError, OSError):
        return None
    stickers = item.get("story_link_stickers")
    if stickers is None:
        stickers = []
    if not isinstance(stickers, list):
        return None
    destinations = set()
    for sticker in stickers:
        if not isinstance(sticker, dict) or not isinstance(sticker.get("story_link"), dict):
            continue
        destination = normalize_destination(sticker["story_link"].get("url"))
        if destination:
            destinations.add(destination)
    return {"storyId": code, "publishedAt": published, "expiresAt": expiry,
            "destinations": sorted(destinations), **_story_image(item)}


def parse_stories(body):
    """Parse inert JSON scripts, retaining only the exact profile's validated data."""
    diagnostics = {"jsonScriptCount": 0, "targetOwnerOccurrences": 0,
                   "targetItemsListOccurrences": 0, "targetItemsCount": 0,
                   "authPositiveObserved": False, "authNegativeObserved": False,
                   "reelsMediaOccurrences": 0, "reelsMediaListOccurrences": 0,
                   "reelsMediaObjectOccurrences": 0, "reelsMediaNullOccurrences": 0,
                   "reelsMediaOtherOccurrences": 0, "reelsMediaItemsCount": 0}
    unknown = {"status": "unknown", "reason": "no_validated_story_structure", "stories": [],
               "structuralDiagnostics": diagnostics}
    if not isinstance(body, str):
        return {**unknown, "reason": "malformed_html"}
    try:
        body_size = len(body.encode("utf-8"))
    except UnicodeEncodeError:
        return {**unknown, "reason": "malformed_html"}
    if body_size > MAX_BYTES:
        return {**unknown, "reason": "body_limit"}
    parser = JSONScripts()
    try:
        parser.feed(body)
    except (ValueError, RecursionError):
        return {**unknown, "reason": "malformed_html"}
    visited = 0
    reels = []
    auth = parser.login_form
    diagnostics["jsonScriptCount"] = len(parser.scripts)
    diagnostics["authNegativeObserved"] = auth
    for script in parser.scripts:
        try:
            value = json.loads(script)
        except (ValueError, RecursionError):
            continue
        pending = [(value, 0)]
        while pending:
            value, depth = pending.pop()
            visited += 1
            if visited > MAX_NODES or depth > MAX_DEPTH:
                return {**unknown, "reason": "structure_limit"}
            if isinstance(value, dict):
                if value.get("username") == "clubeuol":
                    diagnostics["targetOwnerOccurrences"] += 1
                if value.get("is_logged_in") is True or value.get("isLoggedIn") is True:
                    diagnostics["authPositiveObserved"] = True
                if (value.get("is_logged_in") is False or value.get("isLoggedIn") is False
                        or value.get("login_required") is True or value.get("challenge_required") is True
                        or value.get("message") in ("login_required", "challenge_required")):
                    auth = True
                    diagnostics["authNegativeObserved"] = True
                if "reels_media" in value:
                    diagnostics["reelsMediaOccurrences"] += 1
                    media = value["reels_media"]
                    kind = ("List" if isinstance(media, list) else "Object" if isinstance(media, dict)
                            else "Null" if media is None else "Other")
                    diagnostics["reelsMedia" + kind + "Occurrences"] += 1
                    if isinstance(media, list):
                        diagnostics["reelsMediaItemsCount"] += len(media)
                user = value.get("user")
                if isinstance(user, dict) and user.get("username") == "clubeuol":
                    if "items" in value:
                        if not isinstance(value["items"], list):
                            return {**unknown, "reason": "malformed_story_payload"}
                        diagnostics["targetItemsListOccurrences"] += 1
                        diagnostics["targetItemsCount"] += len(value["items"])
                        reels.append(value["items"])
                if visited + len(pending) + len(value) > MAX_NODES:
                    return {**unknown, "reason": "structure_limit"}
                pending.extend((child, depth + 1) for child in value.values())
            elif isinstance(value, list):
                if visited + len(pending) + len(value) > MAX_NODES:
                    return {**unknown, "reason": "structure_limit"}
                pending.extend((child, depth + 1) for child in value)
    if auth:
        return {"status": "auth_required", "reason": "login_payload", "stories": []}
    if not reels:
        return unknown
    stories = {}
    for items in reels:
        for item in items:
            parsed = _parse_item(item)
            if parsed is None:
                return {**unknown, "reason": "malformed_story_payload"}
            previous = stories.get(parsed["storyId"])
            if previous is not None and previous != parsed:
                return {**unknown, "reason": "conflicting_story_payload"}
            stories[parsed["storyId"]] = parsed
    result = sorted(stories.values(), key=lambda item: (item["publishedAt"], item["storyId"]))
    return {"status": "found" if result else "empty", "reason": "", "stories": result}


def _header_value(value, limit=8192):
    return (isinstance(value, str) and len(value) <= limit and "\r" not in value
            and "\n" not in value and "\0" not in value)


def _cookie(data):
    if not isinstance(data, dict):
        raise SessionError("invalid_session_file")
    name, value, domain, path = (data.get("name"), data.get("value"),
                                 data.get("domain", ".instagram.com"), data.get("path", "/"))
    if name not in COOKIE_NAMES or domain not in COOKIE_DOMAINS:
        return None
    if (not _header_value(value) or ";" in value or not isinstance(path, str)
            or not path.startswith("/") or not _header_value(path, 2048)):
        raise SessionError("invalid_session_file")
    expires = data.get("expires")
    if expires is not None and type(expires) not in (int, float):
        raise SessionError("invalid_session_file")
    try:
        expires = int(expires) if expires is not None and expires > 0 else None
    except (ValueError, OverflowError):
        raise SessionError("invalid_session_file") from None
    return http.cookiejar.Cookie(0, name, value, None, False, domain, domain.startswith("."),
                                 domain.startswith("."), path, True, bool(data.get("secure", True)),
                                 expires, expires is None, None, None,
                                 {"HttpOnly": None} if data.get("httpOnly", False) else {}, False)


def _cookie_data(cookie):
    return {"name": cookie.name, "value": cookie.value, "domain": cookie.domain,
            "path": cookie.path, "secure": cookie.secure, "expires": cookie.expires,
            "httpOnly": cookie.has_nonstandard_attr("HttpOnly")}


def _allowed_redirect(location, base):
    if not isinstance(location, str) or not location or len(location) > 8192 or any(c.isspace() for c in location):
        return None
    try:
        target = urllib.parse.urljoin(base, location)
        parsed = urllib.parse.urlsplit(target)
        if (parsed.scheme == "https" and parsed.hostname == "www.instagram.com"
                and parsed.path == "/stories/clubeuol/" and parsed.port is None
                and not parsed.username and not parsed.password and not parsed.fragment
                and target != base):
            return target
    except ValueError:
        pass
    return None


def _auth_location(location):
    try:
        return urllib.parse.urlsplit(location or "").path.startswith(
            ("/accounts/login", "/challenge", "/auth_platform", "/checkpoint"))
    except (TypeError, ValueError):
        return False


def _retry_after_seconds(value):
    """Read a bounded server delay without retaining the raw response header."""
    if not isinstance(value, str) or len(value) > 8192:
        return 3600
    value = value.strip()
    if re.fullmatch(r"[0-9]+", value):
        digits = value.lstrip("0") or "0"
        if len(digits) > 8:
            return MAX_RETRY_AFTER_SECONDS
        return min(int(digits), MAX_RETRY_AFTER_SECONDS)
    try:
        until = email.utils.parsedate_to_datetime(value)
        if until.tzinfo is None:
            return 3600
        now = datetime.datetime.now(datetime.timezone.utc)
        return min(max(0, math.ceil((until - now).total_seconds())), MAX_RETRY_AFTER_SECONDS)
    except (TypeError, ValueError, OverflowError):
        return 3600


class InstagramClient:
    def __init__(self, session_path, opener_factory=None, clock=None):
        self.session_path = Path(session_path)
        self.clock = clock or time.monotonic
        self.jar = http.cookiejar.CookieJar()
        self.config = self._load_session()
        self.opener = opener_factory() if opener_factory else urllib.request.build_opener(NoRedirect())

    def _load_session(self):
        try:
            fd = os.open(self.session_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(fd, "rb") as handle:
                info = os.fstat(handle.fileno())
                if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                        or info.st_uid != os.getuid()):
                    raise SessionError("insecure_session_file")
                raw = handle.read(MAX_SESSION_BYTES + 1)
            if len(raw) > MAX_SESSION_BYTES:
                raise SessionError("invalid_session_file")
            config = json.loads(raw)
            if (not isinstance(config, dict) or not config.get("user_agent")
                    or not _header_value(config.get("user_agent"), 2048)):
                raise SessionError("invalid_session_file")
            headers = config.get("headers", {})
            cookies = config.get("cookies", [])
            if not isinstance(headers, dict) or not isinstance(cookies, list):
                raise SessionError("invalid_session_file")
            clean_headers = {name.lower(): value for name, value in headers.items()
                             if isinstance(name, str) and name.lower() in HEADER_NAMES and _header_value(value)}
            for data in cookies:
                cookie = _cookie(data)
                if cookie:
                    self.jar.set_cookie(cookie)
            observed = config.get("cookie_header", "")
            if not _header_value(observed, MAX_SESSION_BYTES) or "\0" in observed:
                raise SessionError("invalid_session_file")
            if observed:
                parsed = http.cookies.SimpleCookie()
                parsed.load(observed)
                if not parsed:
                    raise SessionError("invalid_session_file")
                for name, morsel in parsed.items():
                    existing = next((c for c in self.jar if c.name == name), None)
                    data = _cookie_data(existing) if existing else {"name": name}
                    data["value"] = morsel.coded_value
                    cookie = _cookie(data)
                    if cookie:
                        self.jar.set_cookie(cookie)
            if not any(c.name == "sessionid" and c.value for c in self.jar):
                raise SessionError("invalid_session_file")
            return {"user_agent": config["user_agent"], "headers": clean_headers}
        except SessionError:
            raise
        except (OSError, ValueError, TypeError, http.cookies.CookieError, RecursionError):
            raise SessionError("invalid_session_file") from None

    def _cookies(self):
        return sorted((_cookie_data(cookie) for cookie in self.jar
                       if cookie.name in COOKIE_NAMES and cookie.domain in COOKIE_DOMAINS),
                      key=lambda data: (data["domain"], data["path"], data["name"]))

    def _persist_session(self):
        temporary = None
        try:
            info = self.session_path.lstat()
            if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_uid != os.getuid()):
                raise SessionError("insecure_session_file")
            data = {**self.config, "cookies": self._cookies(), "cookie_header": ""}
            fd, temporary = tempfile.mkstemp(prefix=".instagram-session-", dir=self.session_path.parent)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump(data, handle, ensure_ascii=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.session_path)
            temporary = None
        except SessionError:
            raise
        except (OSError, ValueError, TypeError):
            raise SessionError("session_write_failed") from None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def collect(self):
        started = self.clock()
        result = {"checkedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "status": "unknown", "reason": "transport_error", "requests": 0,
                  "duration_ms": 0, "body_bytes": 0, "stories": []}
        before = self._cookies()
        target = URL
        try:
            for attempt in range(2):
                headers = {"Accept": "text/html", "Cache-Control": "no-cache",
                           "User-Agent": self.config["user_agent"], **self.config["headers"]}
                request = urllib.request.Request(target, headers=headers, method="GET")
                self.jar.add_cookie_header(request)
                result["requests"] += 1
                try:
                    response = self.opener.open(request, timeout=TIMEOUT)
                except urllib.error.HTTPError as error:
                    response = error
                try:
                    self.jar.extract_cookies(response, request)
                    status = getattr(response, "status", getattr(response, "code", None))
                    location = response.headers.get("Location", "")
                    if status == 429:
                        result.update(status="rate_limited", reason="http_429",
                                      retryAfterSeconds=_retry_after_seconds(response.headers.get("Retry-After")))
                        break
                    if status in (401, 403) or (status in REDIRECT_CODES and _auth_location(location)):
                        result.update(status="auth_required", reason="http_auth_required")
                        break
                    if status in REDIRECT_CODES:
                        redirect = _allowed_redirect(location, target)
                        if attempt == 0 and redirect:
                            target = redirect
                            continue
                        result.update(reason="redirect_limit" if attempt else "redirect_not_allowed")
                        break
                    if status != 200:
                        result.update(reason="http_status")
                        break
                    body = response.read(MAX_BYTES + 1)
                    result["body_bytes"] = len(body)
                    if len(body) > MAX_BYTES:
                        result.update(reason="body_limit")
                    else:
                        result.update(parse_stories(body.decode("utf-8", errors="replace")))
                    break
                finally:
                    response.close()
        except (urllib.error.URLError, TimeoutError, OSError):
            result.update(status="unknown", reason="network_error", stories=[])
        except Exception:
            result.update(status="unknown", reason="transport_error", stories=[])
        finally:
            if self._cookies() != before:
                try:
                    self._persist_session()
                except SessionError:
                    result.update(status="unknown", reason="session_write_failed", stories=[])
            result["duration_ms"] = max(0, round((self.clock() - started) * 1000))
        return result
