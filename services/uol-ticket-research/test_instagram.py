from email.message import Message
import datetime
import email.utils
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse

import instagram

STORY_ID = "4004185955500703427"
TICKET_URL = "https://clube.uol.com.br/campanhasdeingresso/pPM-2-ingressos-bgs-distrito-anhembi-sp"
SECRET = "private-session-value-do-not-output"
MEDIA_URL = "https://instagram.fixture-region.fna.fbcdn.net/v/t51.2885-15/story.jpg"


def item(**changes):
    return {"pk": STORY_ID, "taken_at": 1791556183, "expiring_at": 1791642583,
            "story_link_stickers": [{"story_link": {"url": TICKET_URL + "?fbclid=test&utm_source=ig"}}],
            **changes}


def page(items=None, username="clubeuol"):
    payload = {"__bbox": {"data": {"xdt_api__v1__feed__reels_media": {
        "reels_media": [{"user": {"username": username}, "items": [item()] if items is None else items}]}}}}
    return '<html><script type="application/json">' + json.dumps(payload) + "</script></html>"


class FakeResponse:
    def __init__(self, status=200, body=None, location=None, cookies=()):
        self.status = status
        self.headers = Message()
        if location:
            self.headers["Location"] = location
        for cookie in cookies:
            self.headers["Set-Cookie"] = cookie
        self.body = io.BytesIO((page() if body is None else body).encode("utf-8"))
        self.closed = False

    def read(self, limit):
        return self.body.read(limit)

    def info(self):
        return self.headers

    def close(self):
        self.closed = True


class FakeOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ParserTests(unittest.TestCase):
    def test_profile_ids_times_and_tracking(self):
        result = instagram.parse_stories(page())
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["stories"], [{"storyId": STORY_ID,
                         "publishedAt": "2026-10-09T14:29:43+00:00",
                         "expiresAt": "2026-10-10T14:29:43+00:00",
                         "destinations": [TICKET_URL], "imageUrl": "",
                         "imageWidth": 0, "imageHeight": 0}])
        self.assertEqual(instagram.parse_stories(page([item(pk=int(STORY_ID))]))["stories"][0]["storyId"], STORY_ID)

    def test_empty_needs_exact_owner_and_explicit_list(self):
        self.assertEqual(instagram.parse_stories(page([]))["status"], "empty")
        self.assertEqual(instagram.parse_stories(page([], "other"))["status"], "unknown")
        for payload in ({"user": {"username": "clubeuol"}}, {"items": []}, {"username": "clubeuol", "items": []}):
            body = '<script type="application/json">' + json.dumps(payload) + "</script>"
            self.assertEqual(instagram.parse_stories(body)["status"], "unknown")
        self.assertEqual(instagram.parse_stories("<html>App bootstrap only</html>")["status"], "unknown")

    def test_unknown_diagnostics_are_only_counts_and_flags_without_private_values(self):
        payload = {"is_logged_in": True, "user": {"username": "clubeuol", "private": SECRET},
                   "reels_media": [], "nested": [{"reels_media": None}, {"reels_media": {}},
                                                  {"reels_media": SECRET}], "private": SECRET}
        body = '<script type="application/json">' + json.dumps(payload) + '</script>'
        result = instagram.parse_stories(body)
        self.assertEqual((result['status'],result['reason']), ('unknown','no_validated_story_structure'))
        diagnostics = result['structuralDiagnostics']
        self.assertEqual(diagnostics['jsonScriptCount'], 1)
        self.assertEqual(diagnostics['targetOwnerOccurrences'], 1)
        self.assertEqual(diagnostics['targetItemsListOccurrences'], 0)
        self.assertEqual(diagnostics['reelsMediaOccurrences'], 4)
        for kind in ('List','Object','Null','Other'):
            self.assertEqual(diagnostics['reelsMedia'+kind+'Occurrences'], 1)
        self.assertTrue(diagnostics['authPositiveObserved'])
        self.assertFalse(diagnostics['authNegativeObserved'])
        self.assertTrue(all(type(value) in (int,bool) for value in diagnostics.values()))
        self.assertNotIn(SECRET,json.dumps(result))

    def test_diagnostics_preserve_found_and_explicit_empty_contract(self):
        for items, status in ([item()], 'found'), ([], 'empty'):
            result = instagram.parse_stories(page(items))
            self.assertEqual(result['status'], status)
            self.assertNotIn('structuralDiagnostics',result)
        result = instagram.parse_stories(page([item(), None]))
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['structuralDiagnostics']['targetItemsListOccurrences'], 1)
        self.assertEqual(result['structuralDiagnostics']['targetItemsCount'], 2)

    def test_wrong_profile_is_never_used(self):
        self.assertEqual(instagram.parse_stories(page(username="other"))["stories"], [])
        self.assertEqual(instagram.parse_stories(page(username="ClubeUOL"))["status"], "unknown")

    def test_malformed_items_do_not_become_empty_or_partial_success(self):
        for malformed in (None, "item", [], {}, item(pk=True), item(taken_at=True),
                          item(expiring_at=0), item(expiring_at=10**40),
                          item(story_link_stickers="bad")):
            result = instagram.parse_stories(page([item(), malformed]))
            self.assertEqual(result["status"], "unknown", repr(malformed))
            self.assertEqual(result["stories"], [])
        body = '<script type="application/json">{"user":{"username":"clubeuol"},"items":{}}</script>'
        self.assertEqual(instagram.parse_stories(body)["reason"], "malformed_story_payload")

    def test_json_is_inert_and_malformed_is_unknown(self):
        self.assertEqual(instagram.parse_stories('<script>throw new Error("secret")</script>')["status"], "unknown")
        self.assertEqual(instagram.parse_stories('<script type="application/json">{bad}</script>')["status"], "unknown")
        self.assertEqual(instagram.parse_stories("\ud800")["reason"], "malformed_html")

    def test_duplicates_are_deduplicated_but_conflicts_fail(self):
        self.assertEqual(len(instagram.parse_stories(page([item(), item()]))["stories"]), 1)
        self.assertEqual(instagram.parse_stories(page([item(), item(expiring_at=1791642590)]))["reason"],
                         "conflicting_story_payload")

    def test_auth_payload_and_form(self):
        for value in ({"is_logged_in": False}, {"login_required": True}, {"message": "challenge_required"}):
            result = instagram.parse_stories('<script type="application/json">' + json.dumps(value) + "</script>")
            self.assertEqual(result["status"], "auth_required")
        self.assertEqual(instagram.parse_stories('<form action="/accounts/login/"></form>')["status"], "auth_required")

    def test_structure_depth_nodes_and_body_limits(self):
        value = {"user": {"username": "clubeuol"}, "items": []}
        for _ in range(instagram.MAX_DEPTH + 1):
            value = [value]
        result = instagram.parse_stories('<script type="application/json">' + json.dumps(value) + "</script>")
        self.assertEqual(result["reason"], "structure_limit")
        with patch.object(instagram, "MAX_NODES", 8):
            self.assertEqual(instagram.parse_stories(page())["reason"], "structure_limit")
        self.assertEqual(instagram.parse_stories("x" * (instagram.MAX_BYTES + 1))["reason"], "body_limit")

    def test_public_destination_validation(self):
        variants = [TICKET_URL.replace("https:", "http:"), TICKET_URL.replace("clube.uol.com.br", "clube.uol.com.br.evil"),
                    TICKET_URL.replace("clube.uol.com.br", "user@clube.uol.com.br"),
                    TICKET_URL.replace("clube.uol.com.br", "clube.uol.com.br:443"),
                    TICKET_URL.replace("clube.uol.com.br", "clube.uol.com.br:bad"),
                    TICKET_URL + "/../private", TICKET_URL.replace("pPM-", "%70PM-"),
                    TICKET_URL + "?sessionid=" + SECRET, TICKET_URL + "\n", "javascript:alert(1)"]
        for value in variants:
            self.assertIsNone(instagram.normalize_destination(value), value)
        self.assertIsNone(instagram.normalize_destination(TICKET_URL + "?ref=public&fbclid=test#tracking"))
        self.assertIsNone(instagram.normalize_destination(TICKET_URL + "?foo=" + SECRET))
        unsafe_story = item(story_link_stickers=[{"story_link": {"url": TICKET_URL + "?foo=" + SECRET}}])
        result = instagram.parse_stories(page([unsafe_story]))
        self.assertEqual(result["stories"][0]["destinations"], [])
        self.assertNotIn(SECRET, json.dumps(result))
        wrapped = "https://l.instagram.com/?u=" + urllib.parse.quote(TICKET_URL + "?utm_id=test", safe="")
        self.assertEqual(instagram.normalize_destination(wrapped), TICKET_URL)

    def test_story_image_prefers_original_aspect_and_highest_resolution(self):
        candidates = [
            {"width": 2400, "height": 2400, "url": MEDIA_URL.replace("story", "square")},
            {"width": 750, "height": 1333, "url": MEDIA_URL.replace("story", "small")},
            {"width": 1080, "height": 1920, "url": MEDIA_URL},
        ]
        story = item(original_width=1080, original_height=1920,
                     image_versions2={"candidates": candidates})
        result = instagram.parse_stories(page([story]))["stories"][0]
        self.assertEqual((result["imageUrl"], result["imageWidth"], result["imageHeight"]),
                         (MEDIA_URL, 1080, 1920))
        # Without original dimensions, prefer the portrait image over a square crop.
        result = instagram.parse_stories(page([item(image_versions2={"candidates": candidates})]))["stories"][0]
        self.assertEqual(result["imageUrl"], MEDIA_URL)

    def test_video_uses_its_image_cover(self):
        cover = MEDIA_URL.replace("story.jpg", "cover.webp")
        story = item(media_type=2, original_width=1080, original_height=1920,
                     video_versions=[{"url": "https://evil.example/video.mp4"}],
                     image_versions2={"candidates": [{"width": 1080, "height": 1920, "url": cover}]})
        result = instagram.parse_stories(page([story]))["stories"][0]
        self.assertEqual(result["imageUrl"], cover)
        self.assertNotIn("video.mp4", json.dumps(result))

    def test_missing_or_invalid_image_keeps_valid_story(self):
        variants = [None, {}, {"candidates": "invalid"}, {"candidates": [None, {}]},
                    {"candidates": [{"width": True, "height": 1920, "url": MEDIA_URL}]},
                    {"candidates": [{"width": 1080, "height": -1, "url": MEDIA_URL}]},
                    {"candidates": [{"width": 1080, "height": 1920, "url": "https://evil.example/story.jpg"}]}]
        for versions in variants:
            result = instagram.parse_stories(page([item(image_versions2=versions)]))
            self.assertEqual(result["status"], "found")
            self.assertEqual(result["stories"][0]["imageUrl"], "")
            self.assertEqual(result["stories"][0]["destinations"], [TICKET_URL])
        # A square crop cannot stand in for a known portrait original.
        square = item(original_width=1080, original_height=1920,
                      image_versions2={"candidates": [{"width": 1080, "height": 1080, "url": MEDIA_URL}]})
        self.assertEqual(instagram.parse_stories(page([square]))["stories"][0]["imageUrl"], "")

    def test_media_url_boundaries_and_signed_query(self):
        signed = MEDIA_URL + "?oh=fixture-signature%2Bvalue&oe=fixture-expiry"
        self.assertEqual(instagram.safe_media_url(signed), signed)
        cdninstagram = signed.replace("instagram.fixture-region.fna.fbcdn.net", "fixture.cdninstagram.com")
        self.assertEqual(instagram.safe_media_url(cdninstagram), cdninstagram)
        self.assertEqual(instagram.safe_media_url(MEDIA_URL.replace(".jpg", ".jpeg")),
                         MEDIA_URL.replace(".jpg", ".jpeg"))
        host = "instagram.fixture-region.fna.fbcdn.net"
        variants = [MEDIA_URL.replace("https:", "http:"), MEDIA_URL.replace(host, "evil.example"),
                    MEDIA_URL.replace(host, host + ".evil.example"), MEDIA_URL.replace(host, "fbcdn.net"),
                    MEDIA_URL.replace(host, "user@" + host), MEDIA_URL.replace(host, "user:private@" + host),
                    MEDIA_URL.replace(host, host + ":443"), MEDIA_URL.replace(host, host + ":"),
                    MEDIA_URL.replace(host, host + ":bad"), MEDIA_URL + "#private",
                    MEDIA_URL + "\n", MEDIA_URL + "\0", MEDIA_URL.replace("story.jpg", "../story.jpg"),
                    MEDIA_URL.replace("story.jpg", "%2e%2e/story.jpg"), MEDIA_URL.replace(".jpg", ".mp4"),
                    "javascript:alert(1)"]
        for value in variants:
            self.assertIsNone(instagram.safe_media_url(value))
        story = item(image_versions2={"candidates": [{"width": 1080, "height": 1920, "url": signed}]})
        self.assertEqual(instagram.parse_stories(page([story]))["stories"][0]["imageUrl"], signed)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "session.json"
        self.config = {"user_agent": "Test-UA", "headers": {"Accept-Language": "pt-BR", "Authorization": SECRET},
                       "cookies": [{"name": "sessionid", "value": SECRET, "domain": ".instagram.com",
                                    "path": "/", "secure": True, "httpOnly": True}], "cookie_header": ""}
        self.save()

    def tearDown(self):
        self.directory.cleanup()

    def save(self):
        self.path.write_text(json.dumps(self.config), encoding="utf-8")
        self.path.chmod(0o600)

    def collect(self, responses):
        opener = FakeOpener(responses)
        client = instagram.InstagramClient(self.path, opener_factory=lambda: opener)
        return client.collect(), opener

    def assert_safe_result(self, result):
        expected = {"checkedAt", "status", "reason", "requests", "duration_ms", "body_bytes", "stories"}
        if result["status"] == "rate_limited":
            expected.add("retryAfterSeconds")
        if 'structuralDiagnostics' in result:
            expected.add('structuralDiagnostics')
        self.assertEqual(set(result), expected)
        self.assertNotIn(SECRET, json.dumps(result))
        self.assertNotIn("r=private", json.dumps(result))

    def test_single_get_and_header_whitelist(self):
        result, opener = self.collect([FakeResponse()])
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["requests"], 1)
        self.assertEqual(opener.requests[0].full_url, instagram.URL)
        self.assertEqual(opener.requests[0].get_method(), "GET")
        self.assertIn(SECRET, opener.requests[0].get_header("Cookie"))
        self.assertIsNone(opener.requests[0].get_header("Authorization"))
        self.assert_safe_result(result)

    def test_only_one_same_page_redirect(self):
        result, opener = self.collect([FakeResponse(302, location=instagram.URL + "?r=private"), FakeResponse()])
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["requests"], 2)
        self.assertEqual([r.full_url for r in opener.requests], [instagram.URL, instagram.URL + "?r=private"])
        self.assert_safe_result(result)
        result, opener = self.collect([FakeResponse(302, location="?r=one"), FakeResponse(302, location="?r=two")])
        self.assertEqual(result["reason"], "redirect_limit")
        self.assertEqual(len(opener.requests), 2)

    def test_redirect_boundaries_never_open_unsafe_url(self):
        locations = ["https://evil.example/stories/clubeuol/", "http://www.instagram.com/stories/clubeuol/",
                     "https://user@www.instagram.com/stories/clubeuol/", "https://www.instagram.com:443/stories/clubeuol/",
                     "/stories/other/", "/stories/clubeuol/../other/", "/stories/clubeuol/%2e%2e/",
                     instagram.URL, "?r=one#fragment", "/stories/clubeuol/?r=bad\n"]
        for location in locations:
            result, opener = self.collect([FakeResponse(302, location=location)])
            self.assertEqual(result["status"], "unknown", location)
            self.assertEqual(len(opener.requests), 1)

    def test_auth_rate_limit_and_other_errors(self):
        for status in (401, 403, 429, 500):
            result, opener = self.collect([FakeResponse(status)])
            self.assertEqual(result["status"], "rate_limited" if status == 429 else "unknown" if status == 500 else "auth_required")
            self.assertEqual(len(opener.requests), 1)
        for location in ("/accounts/login/?secret=" + SECRET, "/challenge/", "/checkpoint/"):
            result, opener = self.collect([FakeResponse(302, location=location)])
            self.assertEqual(result["status"], "auth_required")
            self.assertEqual(len(opener.requests), 1)
            self.assert_safe_result(result)
        result, _ = self.collect([FakeResponse(302, location="?r=one"), FakeResponse(302, location="/accounts/login/")])
        self.assertEqual(result["status"], "auth_required")

    def test_retry_after_integer_date_and_invalid_values(self):
        until = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) + datetime.timedelta(hours=2)
        dated = FakeResponse(429)
        dated.headers["Retry-After"] = email.utils.format_datetime(until, usegmt=True)
        result, opener = self.collect([dated])
        self.assertEqual(result["status"], "rate_limited")
        self.assertLessEqual(result["retryAfterSeconds"], 7200)
        self.assertGreaterEqual(result["retryAfterSeconds"], 7199)
        self.assertEqual(len(opener.requests), 1)
        self.assert_safe_result(result)
        for value, seconds in (("86400", 86400), ("0", 0), (" 7200 ", 7200),
                               ("999999999999999999999999", instagram.MAX_RETRY_AFTER_SECONDS),
                               (None, 3600), ("invalid-private-value", 3600), ("-10", 3600), ("1.5", 3600)):
            response = FakeResponse(429)
            if value is not None:
                response.headers["Retry-After"] = value
            result, opener = self.collect([response])
            self.assertEqual(result["retryAfterSeconds"], seconds)
            self.assertEqual(len(opener.requests), 1)
            self.assert_safe_result(result)
            self.assertNotIn("invalid-private-value", json.dumps(result))

    def test_real_urllib_http_error_is_consumed_and_closed(self):
        headers = Message()
        headers["Location"] = "?r=private"
        error = urllib.error.HTTPError(instagram.URL, 302, SECRET, headers, io.BytesIO(b""))
        result, opener = self.collect([error, FakeResponse()])
        self.assertEqual(result["status"], "found")
        self.assertEqual(len(opener.requests), 2)
        self.assert_safe_result(result)

    def test_network_failure_never_exposes_exception(self):
        for error in (urllib.error.URLError(SECRET), TimeoutError(SECRET), ValueError(SECRET)):
            result, _ = self.collect([error])
            self.assertEqual(result["status"], "unknown")
            self.assert_safe_result(result)

    def test_body_bound_and_bootstrap(self):
        result, _ = self.collect([FakeResponse(body="x" * (instagram.MAX_BYTES + 30))])
        self.assertEqual(result["reason"], "body_limit")
        self.assertEqual(result["body_bytes"], instagram.MAX_BYTES + 1)
        result, _ = self.collect([FakeResponse(body="<html>bootstrap</html>")])
        self.assertEqual(result["status"], "unknown")

    def test_set_cookie_renews_session_atomically_and_second_request_uses_it(self):
        renewal = "renewed-private-session-value"
        self.config["cookie_header"] = "sessionid=" + SECRET
        self.save()
        response = FakeResponse(302, location="?r=one", cookies=(
            "sessionid=" + renewal + "; Domain=.instagram.com; Path=/; Secure; HttpOnly",))
        result, opener = self.collect([response, FakeResponse()])
        self.assertEqual(result["status"], "found")
        self.assertIn(renewal, opener.requests[1].get_header("Cookie"))
        stored = json.loads(self.path.read_text())
        self.assertEqual(stored["cookie_header"], "")
        self.assertEqual(stored["cookies"][0]["value"], renewal)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(self.path.parent.glob(".instagram-session-*")), [])
        result2, opener2 = self.collect([FakeResponse()])
        self.assertIn(renewal, opener2.requests[0].get_header("Cookie"))
        self.assertNotIn(renewal, json.dumps(result2))

    def test_cookie_header_quotes_and_browser_import(self):
        self.config["cookie_header"] = 'sessionid=' + SECRET + '; rur="RVA\\054dummy"'
        self.config["cookies"].append({"name": "sessionid", "value": "other", "domain": "evil.example"})
        self.save()
        _, opener = self.collect([FakeResponse()])
        self.assertIn('rur="RVA\\054dummy"', opener.requests[0].get_header("Cookie"))
        self.assertNotIn("other", opener.requests[0].get_header("Cookie"))

    def test_failed_session_write_preserves_collection_telemetry(self):
        response = FakeResponse(cookies=(
            "sessionid=renewed-private-value; Domain=.instagram.com; Path=/; Secure; HttpOnly",))
        opener = FakeOpener([response])
        times = iter((100.0, 101.5))
        client = instagram.InstagramClient(self.path, opener_factory=lambda: opener,
                                           clock=lambda: next(times))
        with patch.object(client, "_persist_session", side_effect=instagram.SessionError("session_write_failed")):
            result = client.collect()
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["reason"], "session_write_failed")
        self.assertEqual(result["requests"], 1)
        self.assertEqual(result["duration_ms"], 1500)
        self.assertGreater(result["body_bytes"], 0)
        self.assertEqual(result["stories"], [])
        self.assertTrue(response.closed)
        self.assert_safe_result(result)
        self.assertNotIn("renewed-private-value", json.dumps(result))

    def test_session_file_modes_malformed_and_symlinks(self):
        self.path.chmod(0o644)
        with self.assertRaises(instagram.SessionError) as raised:
            instagram.InstagramClient(self.path)
        self.assertEqual(str(raised.exception), "insecure_session_file")
        self.path.chmod(0o600)
        self.path.write_text(SECRET)
        with self.assertRaises(instagram.SessionError) as raised:
            instagram.InstagramClient(self.path)
        self.assertEqual(str(raised.exception), "invalid_session_file")
        self.save()
        link = self.path.parent / "link"
        link.symlink_to(self.path)
        with self.assertRaises(instagram.SessionError):
            instagram.InstagramClient(link)
        self.assertNotIn(SECRET, str(raised.exception))

    def test_invalid_cookie_headers_are_sanitized(self):
        for value in ("sessionid=" + SECRET + "\r\nInjected: yes", {"secret": SECRET}):
            self.config["cookie_header"] = value
            self.save()
            with self.assertRaises(instagram.SessionError) as raised:
                instagram.InstagramClient(self.path)
            self.assertEqual(str(raised.exception), "invalid_session_file")


if __name__ == "__main__":
    unittest.main()
