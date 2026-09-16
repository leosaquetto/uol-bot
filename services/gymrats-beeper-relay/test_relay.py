import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from relay import Relay, CHAT, CHAT_PATH, TEXT


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config = {"gateway_token": "test", "beeper_oauth_path": "/unused", "database": str(Path(self.tmp.name) / "state.sqlite")}
        self.sends = 0
        self.fail = False

    def tearDown(self):
        self.tmp.cleanup()

    def upstream(self, method, path, body=None):
        if method == "POST":
            self.sends += 1
            self.assertEqual(body, {"text": TEXT})
            if self.fail:
                raise TimeoutError()
            return {"pendingMessageID": "p-1"}
        if path == CHAT_PATH:
            return {"id": CHAT, "network": "iMessage", "title": "private"}
        return {"chatID": CHAT, "isSender": True, "text": TEXT, "sendStatus": {"status": "SUCCESS"}, "senderID": "private"}

    def send(self, relay):
        return relay.handle("POST", CHAT_PATH + "/messages", "Bearer test", "gymrats:336445:1", {"text": TEXT})

    def test_auth_and_scope(self):
        r = Relay(self.config, self.upstream)
        self.assertEqual(r.handle("GET", CHAT_PATH, "bad")[0], 401)
        self.assertEqual(r.handle("GET", "/v1/accounts", "Bearer test")[0], 404)
        self.assertEqual(r.handle("GET", CHAT_PATH + "/messages/arbitrary", "Bearer test")[0], 404)
        self.assertEqual(r.handle("POST", CHAT_PATH + "/messages", "Bearer test", "gymrats:336445:1", {"text": "other"})[0], 400)
        self.assertEqual(self.sends, 0)
        self.assertEqual(r.handle("GET", CHAT_PATH, "Bearer test")[1], {"id": CHAT, "network": "iMessage"})

    def test_concurrent_and_restart_dedupe(self):
        r = Relay(self.config, self.upstream)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: self.send(r), range(2)))
        self.assertEqual(self.sends, 1)
        restarted = Relay(self.config, self.upstream)
        self.assertEqual(self.send(restarted)[0], 200)
        self.assertEqual(self.sends, 1)
        receipt = restarted.handle("GET", CHAT_PATH + "/messages/p-1", "Bearer test")
        self.assertEqual(receipt[1]["sendStatus"]["status"], "SUCCESS")
        self.assertNotIn("senderID", receipt[1])

    def test_ambiguous_never_retried(self):
        self.fail = True
        self.assertEqual(self.send(Relay(self.config, self.upstream))[0], 502)
        self.fail = False
        self.assertEqual(self.send(Relay(self.config, self.upstream))[0], 409)
        self.assertEqual(self.sends, 1)


if __name__ == "__main__":
    unittest.main()
