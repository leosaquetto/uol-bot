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


class PersonalMCPInboxBridgeTests(unittest.TestCase):
    CHAT = "11111111-1111-4111-8111-111111111111"
    CHAT2 = "22222222-2222-4222-8222-222222222222"
    MESSAGE = "33333333-3333-4333-8333-333333333333"
    SNAPSHOT = "44444444-4444-4444-8444-444444444444"
    REQUEST = "55555555-5555-4555-8555-555555555555"

    def test_chats_messages_and_search_are_sanitized_and_use_opaque_refs(self):
        chat = {"chatRef": self.CHAT, "name": "Diego Acaki Arquiteto", "kind": "contact", "lastMessageAt": 200, "indexedMessageCount": 2, "lastMessageRef": self.MESSAGE}
        message = {"messageRef": self.MESSAGE, "messageTimestamp": 200, "fromMe": False, "contentType": "conversation", "text": "oi", "deleted": False}
        api = FakeAPI([
            (200, {"chats": [chat]}),
            (200, {"messages": [message]}),
            (200, {"messages": [message]}),
        ])
        chats = bridge.execute({"action": "chats", "query": "Diego", "limit": 10}, "a" * 64, api)
        messages = bridge.execute({"action": "messages", "chatRef": self.CHAT, "limit": 20}, "a" * 64, api)
        search = bridge.execute({"action": "search_messages", "query": "oi", "chatRef": self.CHAT, "limit": 5}, "a" * 64, api)
        self.assertEqual(chats, {"ok": True, "chats": [chat]})
        self.assertEqual(messages, {"ok": True, "messages": [message]})
        self.assertEqual(search, {"ok": True, "messages": [message]})
        serialized = json.dumps([chats, messages, search])
        self.assertNotIn("@s.whatsapp.net", serialized)
        self.assertEqual(api.calls[0][0], "GET")
        self.assertIn("/v1/whatsapp/chats?", api.calls[0][1])
        self.assertIn("query=Diego", api.calls[0][1])
        self.assertEqual(api.calls[1][1], f"/v1/whatsapp/chats/{self.CHAT}/messages?limit=20")
        self.assertIn("chatRef=" + self.CHAT, api.calls[2][1])

    def test_history_more_and_send_chat_preserve_exact_opaque_chat(self):
        api = FakeAPI([
            (202, {"accepted": True, "chatRef": self.CHAT, "count": 25}),
            (202, {"requestId": self.REQUEST, "jobs": [{"destination": self.CHAT, "state": "queued", "code": None, "confirmation": None}]}),
        ])
        history = bridge.execute({"action": "history_more", "chatRef": self.CHAT, "count": 25}, "b" * 64, api)
        sent = bridge.execute({"action": "send_chat", "chatRef": self.CHAT, "message": "  oi\nDiego  ", "requestId": self.REQUEST}, "b" * 64, api)
        self.assertEqual(history, {"ok": True, "accepted": True, "chatRef": self.CHAT, "count": 25})
        self.assertEqual(sent, {"ok": True, "requestId": self.REQUEST, "recipient": self.CHAT, "state": "queued"})
        self.assertEqual(api.calls[0], ("POST", f"/v1/whatsapp/chats/{self.CHAT}/history", "b" * 64, {"count": 25}))
        self.assertEqual(api.calls[1], ("POST", "/v1/whatsapp/send-chat", "b" * 64, {"requestId": self.REQUEST, "chatRef": self.CHAT, "text": "  oi\nDiego  "}))

    def test_archive_delete_chat_and_delete_message_are_exact_mutations(self):
        api = FakeAPI([
            (200, {"state": "archived", "chatRef": self.CHAT}),
            (200, {"state": "deleted", "chatRef": self.CHAT}),
            (200, {"state": "deleted", "messageRef": self.MESSAGE, "mode": "for_me"}),
        ])
        archived = bridge.execute({"action": "archive_chat", "chatRef": self.CHAT}, "c" * 64, api)
        deleted = bridge.execute({"action": "delete_chat", "chatRef": self.CHAT, "lastMessageRef": self.MESSAGE}, "c" * 64, api)
        message = bridge.execute({"action": "delete_message", "messageRef": self.MESSAGE, "mode": "for_me"}, "c" * 64, api)
        self.assertEqual(archived, {"ok": True, "state": "archived", "chatRef": self.CHAT})
        self.assertEqual(deleted, {"ok": True, "state": "deleted", "chatRef": self.CHAT})
        self.assertEqual(message, {"ok": True, "state": "deleted", "messageRef": self.MESSAGE, "mode": "for_me"})
        self.assertEqual(api.calls[0][1:], (f"/v1/whatsapp/chats/{self.CHAT}/archive", "c" * 64, {}))
        self.assertEqual(api.calls[1][1:], (f"/v1/whatsapp/chats/{self.CHAT}/delete", "c" * 64, {"lastMessageRef": self.MESSAGE}))
        self.assertEqual(api.calls[2][1:], (f"/v1/whatsapp/messages/{self.MESSAGE}/delete", "c" * 64, {"mode": "for_me"}))

    def test_delete_message_everyone_uses_current_runtime_mode(self):
        api = FakeAPI([(200, {"state": "deleted", "messageRef": self.MESSAGE, "mode": "everyone"})])
        result = bridge.execute({"action": "delete_message", "messageRef": self.MESSAGE, "mode": "everyone"}, "c" * 64, api)
        self.assertEqual(result, {"ok": True, "state": "deleted", "messageRef": self.MESSAGE, "mode": "everyone"})
        self.assertEqual(api.calls[0][3], {"mode": "everyone"})

    def test_cleanup_preview_and_apply_preserve_approved_snapshot_only(self):
        chat = {"chatRef": self.CHAT, "name": "Old chat", "kind": "contact", "lastMessageAt": 100, "indexedMessageCount": 1, "lastMessageRef": self.MESSAGE, "deletable": True}
        api = FakeAPI([
            (200, {"snapshotId": self.SNAPSHOT, "expiresAt": 1700000900000, "chats": [chat]}),
            (200, {"receipts": [{"chatRef": self.CHAT, "state": "deleted"}, {"chatRef": self.CHAT2, "state": "skipped", "code": "last_message_unavailable"}]}),
        ])
        preview = bridge.execute({"action": "cleanup_preview", "inactiveBefore": 500, "maxIndexedMessages": 3, "limit": 50}, "d" * 64, api)
        applied = bridge.execute({"action": "cleanup_apply", "snapshotId": self.SNAPSHOT, "chatRefs": [self.CHAT, self.CHAT2]}, "d" * 64, api)
        self.assertEqual(preview, {"ok": True, "snapshotId": self.SNAPSHOT, "expiresAt": 1700000900000, "chats": [chat]})
        self.assertEqual(applied["receipts"][0], {"chatRef": self.CHAT, "state": "deleted"})
        self.assertEqual(applied["receipts"][1]["state"], "skipped")
        self.assertEqual(api.calls[0], ("POST", "/v1/whatsapp/cleanup/preview", "d" * 64, {"inactiveBefore": 500, "maxIndexedMessages": 3, "limit": 50}))
        self.assertEqual(api.calls[1], ("POST", "/v1/whatsapp/cleanup/apply", "d" * 64, {"snapshotId": self.SNAPSHOT, "chatRefs": [self.CHAT, self.CHAT2]}))

    def test_inbox_actions_reject_jids_unknown_fields_and_unbounded_values(self):
        bad = [
            {"action": "messages", "chatRef": "5511999999999@s.whatsapp.net", "limit": 10},
            {"action": "history_more", "chatRef": self.CHAT, "count": 51},
            {"action": "send_chat", "chatRef": self.CHAT, "message": "oi", "requestId": self.REQUEST, "jid": "x@s.whatsapp.net"},
            {"action": "delete_chat", "chatRef": self.CHAT, "lastMessageRef": "x@s.whatsapp.net"},
            {"action": "cleanup_apply", "snapshotId": self.SNAPSHOT, "chatRefs": [self.CHAT] * 101},
        ]
        for envelope in bad:
            with self.assertRaises(bridge.RelayError):
                bridge.execute(envelope, "e" * 64, FakeAPI([]))

    def test_response_with_unexpected_jid_field_is_rejected(self):
        chat = {"chatRef": self.CHAT, "name": "Diego", "kind": "contact", "lastMessageAt": 200, "indexedMessageCount": 1, "lastMessageRef": self.MESSAGE, "jid": "5511999999999@s.whatsapp.net"}
        with self.assertRaises(bridge.RelayError):
            bridge.execute({"action": "chats", "limit": 10}, "f" * 64, FakeAPI([(200, {"chats": [chat]})]))


if __name__ == "__main__":
    unittest.main()
