from email.message import Message
import datetime
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse

import instagram
import instagram_json as transport

NOW = datetime.datetime(2026, 10, 16, 18, 0, tzinfo=datetime.timezone.utc)
EPOCH = int(NOW.timestamp())
PROFILE = {"username": "clubeuol", "id": "123456789"}
STORY_ID = "4004185955500703427"
TICKET = "https://clube.uol.com.br/campanhasdeingresso/pPM-2-ingressos-bgs"
MEDIA = "https://instagram.fixture-region.fna.fbcdn.net/v/t51.2885-15/story.jpg?signature=private"
SECRET = "private-session-do-not-output"


def story(**changes):
    return {"pk": int(STORY_ID), "id": STORY_ID + "_" + PROFILE["id"],
            "taken_at": EPOCH - 60, "expiring_at": EPOCH + 86340,
            "story_link_stickers": [{"story_link": {"url": TICKET + "?utm_source=ig"}}],
            "original_width": 1080, "original_height": 1920,
            "image_versions2": {"candidates": [{"width": 1080, "height": 1920, "url": MEDIA}]},
            **changes}


def reels(items=None, shape="reels", owner=None):
    reel = {"id": PROFILE["id"], "user": owner or {"username": "clubeuol", "pk": int(PROFILE["id"])},
            "items": [story()] if items is None else items}
    return {"status": "ok", shape: {PROFILE["id"]: reel} if shape == "reels" else [reel]}


class Response:
    def __init__(self, body=None, status=200, headers=None, url=None):
        if body is None:
            body = reels()
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = "application/json; charset=utf-8"
        for name, value in (headers or {}).items():
            self.headers.replace_header(name, value) if name in self.headers else self.headers.add_header(name, value)
        self.body = io.BytesIO(body)
        self.url = url
        self.closed = False
        self.read_limits = []

    def read(self, limit):
        self.read_limits.append(limit)
        return self.body.read(limit)

    def info(self):
        return self.headers

    def geturl(self):
        return self.url

    def close(self):
        self.closed = True


class Opener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        if timeout != instagram.TIMEOUT:
            raise AssertionError("unbounded timeout")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ParserTests(unittest.TestCase):
    def parse(self, value):
        return transport.parse_reels(value, PROFILE, now=NOW)

    def test_both_shapes_preserve_exact_large_ids_and_only_public_destinations(self):
        for shape in ("reels", "reels_media"):
            with self.subTest(shape=shape):
                result = self.parse(reels(shape=shape))
                self.assertEqual((result["status"], result["reason"]), ("found", ""))
                self.assertEqual(result["stories"][0]["storyId"], STORY_ID)
                self.assertEqual(result["stories"][0]["destinations"], [TICKET])
                self.assertEqual(result["stories"][0]["imageUrl"], MEDIA)

    def test_empty_requires_explicit_reel_with_exact_username_and_id(self):
        self.assertEqual(self.parse(reels(items=[]))["status"], "empty")
        self.assertEqual(self.parse(reels(items=[], shape="reels_media"))["status"], "empty")
        for value in ({"reels": {}}, {"reels_media": []}, {"items": []},
                      reels(items=[], owner={"username": "clubeuol"}),
                      reels(items=[], owner={"username": "other", "id": PROFILE["id"]}),
                      reels(items=[], owner={"username": "ClubeUOL", "id": PROFILE["id"]}),
                      reels(items=[], owner={"username": "clubeuol", "id": "999"}),
                      reels(items=[], owner={"username": "clubeuol", "id": PROFILE["id"], "pk": "999"})):
            with self.subTest(value=value):
                self.assertEqual(self.parse(value)["status"], "unknown")

    def test_foreign_item_or_conflicting_story_id_fails_whole_profile(self):
        invalid = (story(pk=True), story(pk=float(STORY_ID)), story(pk="1"),
                   story(id=STORY_ID + "_999"), story(id="4004185955500703428"),
                   story(user={"username": "foreign", "id": PROFILE["id"]}),
                   story(owner={"username": "clubeuol", "id": "999"}))
        for entry in invalid:
            result = self.parse(reels([story(), entry]))
            self.assertEqual((result["status"], result["stories"]), ("unknown", []))

    def test_epoch_bounds_expiry_default_and_expired_filter(self):
        value = story()
        del value["expiring_at"]
        result = self.parse(reels([value]))
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["stories"][0]["expiresAt"],
                         datetime.datetime.fromtimestamp(EPOCH + 86340, datetime.timezone.utc).isoformat())
        self.assertEqual(self.parse(reels([story(taken_at=EPOCH - 86400, expiring_at=EPOCH)]))["status"], "empty")
        for entry in (story(taken_at=EPOCH + 61, expiring_at=EPOCH + 100),
                      story(expiring_at=EPOCH + 86401), story(taken_at=True),
                      story(expiring_at=True), story(expiring_at=EPOCH - 60),
                      story(taken_at=10**100, expiring_at=10**100 + 10)):
            self.assertEqual(self.parse(reels([entry]))["status"], "unknown")

    def test_duplicates_coalesce_and_conflicts_never_emit_partial_success(self):
        self.assertEqual(len(self.parse(reels([story(), story()]))["stories"]), 1)
        result = self.parse(reels([story(), story(expiring_at=EPOCH + 10)]))
        self.assertEqual((result["reason"], result["stories"]), ("conflicting_story_payload", []))

    def test_campaign_links_are_exact_and_sensitive_queries_are_not_retained(self):
        links = [TICKET, TICKET + "?fbclid=x", "https://clube.uol.com.br/outro/pAB-offer",
                 TICKET + "?token=" + SECRET, TICKET + "/", TICKET + "#fragment"]
        value = story(story_link_stickers=[{"story_link": {"url": link}} for link in links])
        result = self.parse(reels([value]))
        self.assertEqual(result["stories"][0]["destinations"], [TICKET])
        self.assertNotIn(SECRET, json.dumps(result))
        for link in links[2:]:
            result = self.parse(reels([story(story_link_stickers=[{"story_link": {"url": link}}])]))
            self.assertEqual(result["stories"][0]["destinations"], [])
        wrapped = "https://l.instagram.com/?u=" + urllib.parse.quote(TICKET + "?utm_source=ig", safe="")
        result = self.parse(reels([story(story_link_stickers=[{"story_link": {"url": wrapped}}])]))
        self.assertEqual(result["stories"][0]["destinations"], [TICKET])

    def test_images_enforce_full_story_aspect_and_worker_dimension_limits(self):
        candidates = [{"width": 6000, "height": 12000, "url": MEDIA},
                      {"width": 100, "height": 3000, "url": MEDIA},
                      {"width": 1080, "height": 1080, "url": MEDIA},
                      {"width": 1080, "height": 1920, "url": "https://attacker.invalid/image.jpg"}]
        result = self.parse(reels([story(image_versions2={"candidates": candidates})]))
        self.assertEqual(result["stories"][0]["imageUrl"], "")
        candidates.append({"width": 540, "height": 960, "url": MEDIA})
        result = self.parse(reels([story(image_versions2={"candidates": candidates})]))
        self.assertEqual(result["stories"][0]["imageWidth"], 540)

    def test_auth_error_and_bounded_known_structures(self):
        for value in ({"message": "login_required"}, {"challenge_required": True}, {"is_logged_in": False}):
            self.assertEqual(self.parse(value)["status"], "auth_required")
        for value in (reels([story()] * 101), reels([story(story_link_stickers=[{}] * 101)]),
                      reels([story(image_versions2={"candidates": [{}] * 101})]),
                      {"reels_media": [{}] * 101}, {"status": "fail", "message": {"secret": SECRET}}):
            result = self.parse(value)
            self.assertEqual((result["status"], result["stories"]), ("unknown", []))
            self.assertNotIn(SECRET, json.dumps(result))


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.session = Path(self.temporary.name) / "session.json"
        self.session.write_text(json.dumps({"user_agent": "Existing browser UA", "cookie_header": "sessionid=" + SECRET,
            "headers": {"accept": "text/html", "accept-language": "pt-BR", "sec-fetch-site": "none",
                        "Authorization": "private-header", "Cookie": "foreign=secret"}}))
        self.session.chmod(0o600)

    def collect(self, responses, identity=PROFILE):
        opener = Opener(responses)
        client = transport.JsonInstagramClient(self.session, profile_identity=identity,
            opener_factory=lambda: opener, clock=iter((10, 10.125)).__next__, now=lambda: NOW)
        return client.collect(), opener

    def test_cold_two_gets_warm_one_and_only_verified_lookup_emits_identity(self):
        lookup = {"status": "ok", "data": {"user": {"username": "clubeuol", "id": PROFILE["id"]}}}
        result, opener = self.collect([Response(lookup), Response()], identity=None)
        self.assertEqual((result["status"], result["requests"], result["duration_ms"]), ("found", 2, 125))
        self.assertEqual(result["profileIdentity"], PROFILE)
        self.assertEqual(opener.requests[0].full_url, transport.PROFILE_URL)
        self.assertEqual(opener.requests[1].full_url,
                         transport.ORIGIN + "/api/v1/feed/reels_media/?reel_ids=" + PROFILE["id"])
        result, _ = self.collect([Response()], identity={**PROFILE, "verifiedEpoch": 1})
        self.assertEqual(result["requests"], 1)
        self.assertNotIn("profileIdentity", result)

    def test_verified_identity_survives_reels_failure_but_foreign_lookup_stops(self):
        lookup = {"data": {"user": {"username": "clubeuol", "pk": int(PROFILE["id"])}}}
        result, _ = self.collect([Response(lookup), Response(status=429)], identity=None)
        self.assertEqual(result["profileIdentity"], PROFILE)
        self.assertEqual((result["status"], result["retryAfterSeconds"]), ("rate_limited", 21600))
        result, opener = self.collect([Response({"data": {"user": {"username": "other", "id": PROFILE["id"]}}})], identity=None)
        self.assertEqual((result["reason"], result["requests"]), ("profile_identity_mismatch", 1))
        self.assertEqual(len(opener.requests), 1)

    def test_header_and_cookie_boundary_never_copies_arbitrary_session_headers(self):
        result, opener = self.collect([Response()])
        self.assertEqual(result["status"], "found")
        request = opener.requests[0]
        self.assertEqual(urllib.parse.urlsplit(request.full_url).hostname, "www.instagram.com")
        headers = {name.lower(): value for name, value in request.header_items()}
        self.assertEqual(set(headers), {"accept", "x-ig-app-id", "x-asbd-id", "user-agent", "accept-language", "cookie"})
        self.assertEqual(headers["accept"], "application/json")
        self.assertEqual(headers["x-ig-app-id"], "936619743392459")
        self.assertEqual(headers["x-asbd-id"], "359341")
        self.assertEqual(headers["cookie"], "sessionid=" + SECRET)
        self.assertNotIn(SECRET, json.dumps(result))

    def test_429_never_falls_back_and_retry_after_has_six_hour_floor(self):
        for value, expected in (("1", 21600), ("99999", 99999), ("bad", 21600)):
            response = Response(status=429, headers={"Retry-After": value})
            result, opener = self.collect([response], identity=None)
            self.assertEqual((result["status"], result["reason"], result["retryAfterSeconds"]),
                             ("rate_limited", "http_429", expected))
            self.assertEqual(len(opener.requests), 1)
            self.assertTrue(response.closed)
            self.assertFalse(response.read_limits)
        headers = Message()
        headers["Retry-After"] = "43200"
        error = urllib.error.HTTPError(transport.PROFILE_URL, 429, SECRET, headers, io.BytesIO(b"private"))
        result, opener = self.collect([error], identity=None)
        self.assertEqual((result["status"], result["retryAfterSeconds"], len(opener.requests)),
                         ("rate_limited", 43200, 1))
        self.assertNotIn(SECRET, json.dumps(result))

    def test_auth_redirects_are_terminal_and_no_redirect_is_requested(self):
        for status, location, expected in ((401, "", "auth_required"), (403, "", "auth_required"),
                (302, "/accounts/login/", "auth_required"), (302, "https://evil.invalid/", "unknown"),
                (301, "/api/v1/feed/reels_media/", "unknown")):
            response = Response(status=status, headers={"Location": location})
            result, opener = self.collect([response])
            self.assertEqual(result["status"], expected)
            self.assertEqual(len(opener.requests), 1)
            self.assertFalse(response.read_limits)
        with patch("instagram.urllib.request.build_opener") as build:
            transport.JsonInstagramClient(self.session)
            self.assertIsInstance(build.call_args.args[0], instagram.NoRedirect)

    def test_html_bad_json_and_changed_response_url_fail_closed(self):
        responses = (Response(b"<html>private</html>", headers={"Content-Type": "text/html"}),
                     Response(b"{invalid"), Response(b"\xff"), Response([]),
                     Response(url="https://evil.invalid/"),
                     Response({"message": "checkpoint_required"}))
        for response in responses:
            result, opener = self.collect([response])
            self.assertIn(result["status"], ("unknown", "auth_required"))
            self.assertEqual(result["stories"], [])
            self.assertEqual(len(opener.requests), 1)
            self.assertTrue(response.closed)

    def test_body_limit_works_without_content_length_and_closes_response(self):
        response = Response(b" " * (instagram.MAX_BYTES + 100))
        result, _ = self.collect([response])
        self.assertEqual((result["reason"], result["body_bytes"]), ("body_limit", instagram.MAX_BYTES + 1))
        self.assertEqual(response.read_limits, [instagram.MAX_BYTES + 1])
        self.assertTrue(response.closed)
        response = Response(headers={"Content-Length": str(instagram.MAX_BYTES + 1)})
        result, _ = self.collect([response])
        self.assertEqual(result["reason"], "body_limit")
        self.assertFalse(response.read_limits)

    def test_only_changed_cookies_are_persisted_and_write_failure_cannot_succeed(self):
        before = self.session.read_bytes()
        with patch.object(transport.JsonInstagramClient, "_persist_session", autospec=True) as persist:
            result, _ = self.collect([Response()])
            persist.assert_not_called()
        self.assertEqual(self.session.read_bytes(), before)
        response = Response(headers={"Set-Cookie": "mid=new-value; Path=/; Domain=.instagram.com; Secure"})
        result, _ = self.collect([response])
        self.assertEqual(result["status"], "found")
        stored = json.loads(self.session.read_text())
        self.assertTrue(any(cookie["name"] == "mid" and cookie["value"] == "new-value" for cookie in stored["cookies"]))
        response = Response(headers={"Set-Cookie": "mid=other-value; Path=/; Domain=.instagram.com; Secure"})
        with patch.object(transport.JsonInstagramClient, "_persist_session", side_effect=instagram.SessionError("session_write_failed")):
            result, _ = self.collect([response])
        self.assertEqual((result["status"], result["reason"], result["stories"]), ("unknown", "session_write_failed", []))

    def test_foreign_cookie_domain_is_not_persisted_or_forwarded(self):
        before = self.session.read_bytes()
        response = Response(headers={"Set-Cookie": "mid=foreign-value; Path=/; Domain=evil.invalid; Secure"})
        result, opener = self.collect([response])
        self.assertEqual(result["status"], "found")
        self.assertEqual(self.session.read_bytes(), before)
        self.assertNotIn("foreign-value", str(opener.requests[0].header_items()))

    def test_network_failure_does_not_retry_or_expose_error(self):
        result, opener = self.collect([urllib.error.URLError(SECRET)])
        self.assertEqual((result["status"], result["reason"], result["requests"]), ("unknown", "network_error", 1))
        self.assertNotIn(SECRET, json.dumps(result))
        self.assertEqual(len(opener.requests), 1)


if __name__ == "__main__":
    unittest.main()
