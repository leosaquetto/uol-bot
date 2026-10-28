import importlib.util
import io
import json
import pathlib
import unittest

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "personal-mcp-bridge.py"
spec = importlib.util.spec_from_file_location("personal_mcp_bridge", SCRIPT)
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


class FakeAPI:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, path, token, body=None):
        self.calls.append((method, path, token, body))
        return self.responses.pop(0)


class PersonalMCPBridgeTests(unittest.TestCase):
    def test_status_is_sanitized_and_never_returns_jid(self):
        api = FakeAPI([(200, {"ok": True, "whatsapp": "connected", "connected": True, "pairingRequired": False, "sender": "****5577"})])
        result = bridge.execute({"action": "status"}, "a" * 64, api)
        self.assertEqual(result, {"ok": True, "whatsapp": "connected", "connected": True, "pairingRequired": False, "sender": "****5577"})
        self.assertEqual(api.calls, [("GET", "/v1/status", "a" * 64, None)])

    def test_recipients_translate_only_server_side_aliases(self):
        api = FakeAPI([(200, {"destinations": [{"id": "self", "name": "Self", "type": "contact"}]})])
        result = bridge.execute({"action": "recipients"}, "b" * 64, api)
        self.assertEqual(result, {"ok": True, "recipients": [{"alias": "self", "label": "Self"}]})
        self.assertNotIn("type", json.dumps(result))

    def test_send_accepts_alias_only_and_preserves_exact_text(self):
        api = FakeAPI([(202, {"requestId": "123e4567-e89b-42d3-a456-426614174000", "jobs": [{"destination": "self", "state": "queued", "code": None, "confirmation": None}]})])
        envelope = {"action": "send", "recipient": "self", "message": "  oi\nleo  ", "requestId": "123e4567-e89b-42d3-a456-426614174000"}
        result = bridge.execute(envelope, "c" * 64, api)
        self.assertEqual(result, {"ok": True, "requestId": envelope["requestId"], "recipient": "self", "state": "queued"})
        self.assertEqual(api.calls[0][0:3], ("POST", "/v1/whatsapp/send", "c" * 64))
        self.assertEqual(api.calls[0][3], {"requestId": envelope["requestId"], "destinations": ["self"], "text": "  oi\nleo  ", "mediaIds": []})

    def test_request_status_reconciles_same_request_without_resending(self):
        request_id = "123e4567-e89b-42d3-a456-426614174000"
        api = FakeAPI([(200, {"requestId": request_id, "jobs": [{"destination": "self", "state": "accepted", "code": None, "confirmation": None}]})])
        result = bridge.execute({"action": "request_status", "requestId": request_id}, "e" * 64, api)
        self.assertEqual(result, {"ok": True, "requestId": request_id, "recipient": "self", "state": "accepted"})
        self.assertEqual(api.calls, [("GET", f"/v1/whatsapp/requests/{request_id}", "e" * 64, None)])

        uncertain = FakeAPI([(200, {"requestId": request_id, "jobs": [{"destination": "self", "state": "unknown", "code": "dispatch_unconfirmed", "confirmation": None}]})])
        self.assertEqual(bridge.execute({"action": "request_status", "requestId": request_id}, "f" * 64, uncertain), {"ok": True, "requestId": request_id, "recipient": "self", "state": "unknown"})

    def test_send_rejects_numbers_jids_unknown_fields_and_controls(self):
        good = {"action": "send", "recipient": "self", "message": "oi", "requestId": "123e4567-e89b-42d3-a456-426614174000"}
        for patch in [
            {"recipient": "5511999999999"},
            {"recipient": "x@s.whatsapp.net"},
            {"message": "oi\x00"},
            {"host": "example.com"},
        ]:
            value = dict(good)
            value.update(patch)
            with self.assertRaises(bridge.RelayError):
                bridge.execute(value, "d" * 64, FakeAPI([]))

    def test_read_envelope_is_one_bounded_json_line(self):
        value = bridge.read_envelope(io.BytesIO(b'{"action":"status"}\n'))
        self.assertEqual(value, {"action": "status"})
        for raw in [b'{"action":"status"}', b'[]\n', b'{"action":"status"}\nextra']:
            with self.assertRaises(bridge.RelayError):
                bridge.read_envelope(io.BytesIO(raw))


if __name__ == "__main__":
    unittest.main()
