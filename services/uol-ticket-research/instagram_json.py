"""Bounded Instagram JSON reads using the server's existing protected session.

Only the configured Clube UOL profile is queried. No login, redirect, alternate
transport or retry is attempted here; scheduling and budgets belong to Monitor.
"""

import datetime
import json
import re
import urllib.error
import urllib.parse
import urllib.request

from instagram import (InstagramClient, MAX_BYTES, REDIRECT_CODES, TIMEOUT,
                       SessionError, _auth_location, _retry_after_seconds,
                       _story_image, normalize_destination)

PROFILE = "clubeuol"
ORIGIN = "https://www.instagram.com"
PROFILE_URL = ORIGIN + "/api/v1/users/web_profile_info/?username=" + PROFILE
MIN_RATE_LIMIT_DELAY = 6 * 60 * 60
MAX_STORIES = 100
MAX_REELS = 100
MAX_IMAGE_CANDIDATES = 100
MAX_STICKERS = 100
IDENTIFIER = re.compile(r"[1-9][0-9]{0,23}")
STORY_IDENTIFIER = re.compile(r"[0-9]{10,24}")
CAMPAIGN_PATH = re.compile(r"/campanhasdeingresso/p[A-Za-z0-9]{2,5}-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
AUTH_MESSAGES = frozenset(("login_required", "challenge_required", "checkpoint_required",
                           "checkpoint_challenge_required", "consent_required"))


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def _identifier(value):
    if type(value) is int:
        value = str(value)
    return value if isinstance(value, str) and IDENTIFIER.fullmatch(value) else None


def _identity(value):
    """A cached identity is usable only when its exact username and ID agree."""
    if (not isinstance(value, dict) or value.get("username") != PROFILE
            or not isinstance(value.get("id"), str) or not _identifier(value["id"])):
        return None
    return {"username": PROFILE, "id": value["id"]}


def _owner_matches(value, profile):
    if not isinstance(value, dict) or value.get("username") != PROFILE:
        return False
    ids = [value[name] for name in ("id", "pk") if name in value]
    return bool(ids) and all(_identifier(identifier) == profile["id"] for identifier in ids)


def _auth_payload(value):
    if not isinstance(value, dict):
        return False
    message = value.get("message")
    return ((isinstance(message, str) and message in AUTH_MESSAGES)
            or any(value.get(name) is True for name in
                   ("login_required", "challenge_required", "checkpoint_required"))
            or value.get("is_logged_in") is False or value.get("isLoggedIn") is False)


def _unknown(reason):
    return {"status": "unknown", "reason": reason, "stories": []}


def _campaign_link(value):
    # Unwrap only the known public Instagram redirect, with a fixed depth.
    for _ in range(3):
        if (not isinstance(value, str) or len(value) > 8192
                or any(character.isspace() for character in value) or "\\" in value):
            return None
        try:
            parsed = urllib.parse.urlsplit(value)
            if parsed.fragment:
                return None
            if parsed.hostname == "l.instagram.com":
                if (parsed.scheme != "https" or parsed.username or parsed.password
                        or parsed.port is not None or parsed.path not in ("", "/")):
                    return None
                values = urllib.parse.parse_qs(parsed.query, max_num_fields=100).get("u", [])
                if len(values) != 1:
                    return None
                value = values[0]
                continue
            destination = normalize_destination(value)
            return (destination if destination and
                    CAMPAIGN_PATH.fullmatch(urllib.parse.urlsplit(destination).path) else None)
        except ValueError:
            return None
    return None


def _story_id(item, owner_id):
    identifiers = []
    if "pk" in item:
        value = item["pk"]
        if type(value) is int:
            value = str(value)
        if not isinstance(value, str) or not STORY_IDENTIFIER.fullmatch(value):
            return None
        identifiers.append(value)
    if "id" in item:
        value = item["id"]
        if type(value) is int:
            value = str(value)
        if not isinstance(value, str):
            return None
        parts = value.split("_")
        if (len(parts) not in (1, 2) or not STORY_IDENTIFIER.fullmatch(parts[0])
                or (len(parts) == 2 and parts[1] != owner_id)):
            return None
        identifiers.append(parts[0])
    return identifiers[0] if identifiers and len(set(identifiers)) == 1 else None


def _parse_item(item, profile, now_epoch):
    if not isinstance(item, dict):
        return None
    if "user" in item and not _owner_matches(item["user"], profile):
        return None
    if "owner" in item and not _owner_matches(item["owner"], profile):
        return None
    story_id = _story_id(item, profile["id"])
    taken = item.get("taken_at")
    expiry = item.get("expiring_at", taken + 86400 if type(taken) is int else None)
    if (story_id is None or type(taken) is not int or type(expiry) is not int
            or taken <= 0 or taken > now_epoch + 60 or expiry <= taken
            or expiry - taken > 86400):
        return None
    try:
        published_at = datetime.datetime.fromtimestamp(taken, datetime.timezone.utc).isoformat()
        expires_at = datetime.datetime.fromtimestamp(expiry, datetime.timezone.utc).isoformat()
    except (ValueError, OverflowError, OSError):
        return None
    stickers = item.get("story_link_stickers", [])
    if not isinstance(stickers, list) or len(stickers) > MAX_STICKERS:
        return None
    destinations = set()
    for sticker in stickers:
        link = sticker.get("story_link") if isinstance(sticker, dict) else None
        destination = _campaign_link(link.get("url")) if isinstance(link, dict) else None
        if destination:
            destinations.add(destination)
    versions = item.get("image_versions2")
    if isinstance(versions, dict):
        candidates = versions.get("candidates")
        if not isinstance(candidates, list) or len(candidates) > MAX_IMAGE_CANDIDATES:
            return None
        candidates = [candidate for candidate in candidates if isinstance(candidate, dict)
                      and type(candidate.get("width")) is int
                      and type(candidate.get("height")) is int
                      and min(candidate["width"], candidate["height"]) > 0
                      and candidate["width"] + candidate["height"] <= 10000
                      and max(candidate["width"], candidate["height"]) <=
                          20 * min(candidate["width"], candidate["height"])]
        item = {**item, "image_versions2": {"candidates": candidates}}
    image = _story_image(item)
    return {"storyId": story_id, "publishedAt": published_at, "expiresAt": expires_at,
            "destinations": sorted(destinations), **image}


def parse_reels(value, profile_identity, now=None):
    """Read only the two known JSON shapes; never infer an empty missing reel."""
    profile = _identity(profile_identity)
    if profile is None or not isinstance(value, dict):
        return _unknown("no_validated_story_structure")
    if _auth_payload(value):
        return {"status": "auth_required", "reason": "login_payload", "stories": []}
    if value.get("status") not in (None, "ok"):
        return _unknown("api_error")
    reels = []
    if "reels" in value:
        by_id = value["reels"]
        if not isinstance(by_id, dict) or len(by_id) > MAX_REELS:
            return _unknown("malformed_story_payload")
        if profile["id"] in by_id:
            reels.append(by_id[profile["id"]])
    if "reels_media" in value:
        media = value["reels_media"]
        if not isinstance(media, list) or len(media) > MAX_REELS:
            return _unknown("malformed_story_payload")
        # Foreign reels are never used as proof about the configured profile.
        for reel in media:
            if not isinstance(reel, dict):
                return _unknown("malformed_story_payload")
            owner = reel.get("user")
            if _owner_matches(owner, profile):
                reels.append(reel)
            elif isinstance(owner, dict) and (owner.get("username") == PROFILE
                    or any(_identifier(owner.get(name)) == profile["id"] for name in ("id", "pk"))):
                return _unknown("owner_mismatch")
    if not reels:
        return _unknown("no_validated_story_structure")
    now_value = now or _utc_now()
    if not isinstance(now_value, datetime.datetime) or now_value.tzinfo is None:
        return _unknown("invalid_clock")
    now_epoch = now_value.timestamp()
    stories = {}
    total = 0
    for reel in reels:
        if not isinstance(reel, dict) or not _owner_matches(reel.get("user"), profile):
            return _unknown("owner_mismatch")
        if "id" in reel and _identifier(reel["id"]) != profile["id"]:
            return _unknown("owner_mismatch")
        items = reel.get("items")
        if not isinstance(items, list):
            return _unknown("malformed_story_payload")
        total += len(items)
        if total > MAX_STORIES:
            return _unknown("structure_limit")
        for item in items:
            parsed = _parse_item(item, profile, now_epoch)
            if parsed is None:
                return _unknown("malformed_story_payload")
            previous = stories.get(parsed["storyId"])
            if previous is not None and previous != parsed:
                return _unknown("conflicting_story_payload")
            stories[parsed["storyId"]] = parsed
    active = [story for story in stories.values()
              if datetime.datetime.fromisoformat(story["expiresAt"]).timestamp() > now_epoch]
    active.sort(key=lambda story: (story["publishedAt"], story["storyId"]))
    return {"status": "found" if active else "empty", "reason": "", "stories": active}


class JsonInstagramClient(InstagramClient):
    def __init__(self, session_path, profile_identity=None, opener_factory=None, clock=None, now=None):
        super().__init__(session_path, opener_factory=opener_factory, clock=clock)
        self.profile_identity = _identity(profile_identity)
        self.now = now or _utc_now

    def _get_json(self, target, result):
        headers = {"Accept": "application/json", "X-IG-App-ID": "936619743392459",
                   "X-ASBD-ID": "359341", "User-Agent": self.config["user_agent"]}
        language = self.config["headers"].get("accept-language")
        if language:
            headers["Accept-Language"] = language
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
                              retryAfterSeconds=max(MIN_RATE_LIMIT_DELAY,
                                  _retry_after_seconds(response.headers.get("Retry-After"))))
                return None
            if status in (401, 403) or (status in REDIRECT_CODES and _auth_location(location)):
                result.update(status="auth_required", reason="http_auth_required")
                return None
            if status in REDIRECT_CODES:
                result.update(reason="redirect_not_allowed")
                return None
            if status != 200:
                result.update(reason="http_status")
                return None
            response_url = response.geturl() if hasattr(response, "geturl") else target
            if response_url and response_url != target:
                result.update(status="auth_required" if _auth_location(response_url) else "unknown",
                              reason="unexpected_response_url")
                return None
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json" and not (
                    content_type.startswith("application/") and content_type.endswith("+json")):
                result.update(reason="non_json_response")
                return None
            length = response.headers.get("Content-Length", "")
            if length.isdigit() and int(length) > MAX_BYTES:
                result.update(reason="body_limit")
                return None
            body = response.read(MAX_BYTES + 1)
            result["body_bytes"] += len(body)
            if len(body) > MAX_BYTES:
                result.update(reason="body_limit")
                return None
            try:
                value = json.loads(body.decode("utf-8"))
            except (ValueError, UnicodeDecodeError, RecursionError):
                result.update(reason="malformed_json")
                return None
            if not isinstance(value, dict):
                result.update(reason="no_validated_story_structure")
                return None
            if _auth_payload(value):
                result.update(status="auth_required", reason="login_payload")
                return None
            if value.get("status") not in (None, "ok"):
                result.update(reason="api_error")
                return None
            return value
        finally:
            response.close()

    def collect(self):
        started = self.clock()
        now = self.now()
        result = {"checkedAt": now.isoformat(), "status": "unknown", "reason": "transport_error",
                  "requests": 0, "duration_ms": 0, "body_bytes": 0, "stories": []}
        before = self._cookies()
        profile = self.profile_identity
        try:
            if profile is None:
                value = self._get_json(PROFILE_URL, result)
                if value is None:
                    return result
                data = value.get("data")
                user = data.get("user") if isinstance(data, dict) else None
                if not isinstance(user, dict) or user.get("username") != PROFILE:
                    result.update(reason="profile_identity_mismatch")
                    return result
                ids = [user[name] for name in ("id", "pk") if name in user]
                user_id = _identifier(ids[0]) if ids else None
                if not user_id or any(_identifier(identifier) != user_id for identifier in ids):
                    result.update(reason="profile_identity_mismatch")
                    return result
                profile = {"username": PROFILE, "id": user_id}
                self.profile_identity = profile
                result["profileIdentity"] = dict(profile)
            target = ORIGIN + "/api/v1/feed/reels_media/?" + urllib.parse.urlencode({"reel_ids": profile["id"]})
            value = self._get_json(target, result)
            if value is not None:
                result.update(parse_reels(value, profile, now=now))
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
