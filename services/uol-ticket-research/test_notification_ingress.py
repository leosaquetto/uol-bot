from contextlib import closing
import datetime as dt
import http.client
from http.server import HTTPServer
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

from monitor import Monitor
from notification_ingress import IngressError, NotificationStore, handler, relay_token
from push_signals import PushSignals


SCHEMA = '''CREATE TABLE signals(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE NOT NULL,
 received_at INTEGER NOT NULL,profile TEXT NOT NULL,story_id TEXT,evidence TEXT NOT NULL);
 CREATE TABLE receiver_state(id INTEGER PRIMARY KEY,status TEXT,observed_at INTEGER);'''


class NotificationIngressTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.now = 1792176500
        self.path = self.directory / 'push.sqlite'
        with closing(sqlite3.connect(self.path)) as db:
            db.executescript(SCHEMA)
        os.chmod(self.path, 0o600)
        self.store = NotificationStore(self.directory, clock=lambda: self.now)
        self.server = HTTPServer(('127.0.0.1', 0), handler(self.store, 'a' * 64))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def hint(self, **overrides):
        return {'eventId': str(uuid.uuid4()), 'profile': 'clubeuol', 'source': 'scriptable',
                'observedAt': dt.datetime.fromtimestamp(self.now, dt.timezone.utc).isoformat(), **overrides}

    def request(self, value=None, *, token='a' * 64, method='POST', path='/instagram-wake', raw=None, headers=None):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        body = raw if raw is not None else json.dumps(value or self.hint()).encode()
        fields = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', **(headers or {})}
        connection.request(method, path, body=body if method == 'POST' else None, headers=fields)
        response = connection.getresponse()
        result = response.status, json.loads(response.read())
        connection.close()
        return result

    def test_authenticated_hint_persists_before_ack_and_replays_after_restart(self):
        hint = self.hint()
        status, result = self.request(hint)
        self.assertEqual(status, 202)
        self.assertTrue(result['persisted'])
        self.assertFalse(result['duplicate'])
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute('SELECT event_id,received_at,profile,story_id,evidence FROM signals').fetchone()
            self.assertEqual(row[:4], ('ios:' + hint['eventId'], self.now * 1000, 'clubeuol', ''))
            self.assertEqual(json.loads(row[4]), {'origin': 'external_notification', 'source': 'scriptable',
                                                 'observedAt': hint['observedAt']})
            self.assertEqual(db.execute('SELECT count(*) FROM receiver_state').fetchone()[0], 0)
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='push_events'").fetchone())
        restarted = NotificationStore(self.directory, clock=lambda: self.now)
        self.assertTrue(restarted.enqueue(hint)['duplicate'])
        # A lost ACK can be recovered after downtime without creating a new wake.
        self.assertTrue(NotificationStore(self.directory, clock=lambda: self.now + 86401).enqueue(hint)['duplicate'])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM signals').fetchone()[0], 1)

    def test_replay_conflict_and_rate_limit_do_not_change_saved_signal(self):
        hint = self.hint()
        self.store.enqueue(hint)
        with self.assertRaises(IngressError) as error:
            self.store.enqueue({**hint, 'source': 'ios_shortcuts'})
        self.assertEqual(error.exception.status, 409)
        with patch('notification_ingress.MAX_HOURLY_SIGNALS', 1):
            self.assertTrue(self.store.enqueue(hint)['duplicate'])
            with self.assertRaises(IngressError) as error:
                NotificationStore(self.directory, clock=lambda: self.now).enqueue(self.hint())
            self.assertEqual(error.exception.status, 429)
        with patch('notification_ingress.MAX_SIGNALS', 1):
            with self.assertRaises(IngressError) as error:
                self.store.enqueue(self.hint())
            self.assertEqual(error.exception.status, 503)

    def test_http_auth_body_limit_json_and_profile_allowlist(self):
        self.assertEqual(self.request(token='wrong')[0], 401)
        self.assertEqual(self.request(raw=b'x' * 4097)[0], 413)
        self.assertEqual(self.request(raw=b'not-json')[0], 400)
        self.assertEqual(self.request(headers={'Content-Type': 'text/plain'})[0], 415)
        self.assertEqual(self.request(self.hint(profile='someone_else'))[0], 400)
        self.assertEqual(self.request(self.hint(source='native_webpush'))[0], 400)
        self.assertEqual(self.request(self.hint(extra='unneeded'))[0], 400)
        self.assertEqual(self.request(path='/other')[0], 404)
        self.assertEqual(self.request(method='GET', path='/healthz')[0], 200)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM signals').fetchone()[0], 0)

    def test_unknown_length_and_chunked_body_rejected_without_reading(self):
        for headers in ({}, {'Transfer-Encoding': 'chunked'}):
            with self.subTest(headers=headers):
                connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
                connection.putrequest('POST', '/instagram-wake')
                connection.putheader('Authorization', 'Bearer ' + 'a' * 64)
                connection.putheader('Content-Type', 'application/json')
                for key, value in headers.items():
                    connection.putheader(key, value)
                connection.endheaders()
                response = connection.getresponse()
                self.assertEqual(response.status, 411)
                response.read()
                connection.close()

    def test_invalid_timestamp_or_identity_never_persisted(self):
        for fields in ({'eventId': 'not-a-uuid'}, {'observedAt': 'not-a-time'}, {'observedAt': '2026-10-01'},
                       {'observedAt': dt.datetime.fromtimestamp(self.now + 301, dt.timezone.utc).isoformat()},
                       {'observedAt': '1970-01-01T00:00:00+00:00'}):
            with self.subTest(fields=fields):
                self.assertEqual(self.request(self.hint(**fields))[0], 400)

    def test_first_delayed_hint_is_current_wake_and_replay_remains_idempotent(self):
        hint = self.hint(observedAt=dt.datetime.fromtimestamp(self.now - 3 * 86400, dt.timezone.utc).isoformat())
        status, receipt = self.request(hint)
        self.assertEqual(status, 202)
        self.assertTrue(receipt['persisted'])
        self.assertFalse(receipt['duplicate'])
        self.assertTrue(self.request(hint)[1]['duplicate'])
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute('SELECT received_at,evidence FROM signals').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], self.now * 1000)
        self.assertEqual(json.loads(rows[0][1])['observedAt'], hint['observedAt'])

    def test_storage_lock_fails_closed_and_token_is_private(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN IMMEDIATE')
            self.assertEqual(self.request()[0], 503)
            db.rollback()
        config = self.directory / 'notification-relay.json'
        config.write_text(json.dumps({'token': 'a' * 64}))
        os.chmod(config, 0o600)
        self.assertEqual(relay_token(self.directory), 'a' * 64)
        os.chmod(config, 0o644)
        with self.assertRaises(ValueError):
            relay_token(self.directory)

    def test_existing_schema_required_without_creation_or_migration(self):
        other = self.directory / 'other'
        other.mkdir()
        with self.assertRaises(OSError):
            NotificationStore(other)
        self.assertFalse((other / 'push.sqlite').exists())
        with closing(sqlite3.connect(other / 'push.sqlite')) as db:
            db.execute('CREATE TABLE signals(seq INTEGER PRIMARY KEY,event_id TEXT)')
        os.chmod(other / 'push.sqlite', 0o600)
        with self.assertRaises(ValueError):
            NotificationStore(other)

    def monitor(self):
        monitor = Monitor(self.directory, {'pushMode': 'push_only'}, clock=lambda: self.now)
        self.addCleanup(monitor.db.close)
        return monitor

    def result(self, story_id='4004391581161830448'):
        iso = lambda offset: dt.datetime.fromtimestamp(self.now + offset, dt.timezone.utc).isoformat()
        return {'checkedAt': iso(0), 'status': 'found', 'requests': 2, 'reason': '',
                'duration_ms': 1, 'body_bytes': 10,
                'stories': [{'storyId': story_id, 'publishedAt': iso(-60), 'expiresAt': iso(3600),
                             'imageUrl': 'https://fixture.fbcdn.net/story.jpg',
                             'destinations': ['https://clube.uol.com.br/campanhasdeingresso/pQg-teatro']}]}

    def test_external_hint_wakes_disconnected_monitor_once_and_cannot_prove_native_push(self):
        self.store.enqueue(self.hint())
        signals = PushSignals(self.directory)
        self.assertFalse(signals.snapshot(self.now)['connected'])
        self.assertEqual(signals.signals()[0]['origin'], 'external_notification')
        monitor = self.monitor()
        self.assertTrue(monitor.reserve_poll())
        monitor.record(self.result())
        self.assertEqual(monitor.state['pushSettlement']['status'], 'external_hint_processed')
        self.assertFalse(monitor.reserve_poll())
        self.assertEqual(monitor.state['coverage']['correspondingPushStories'], 0)
        self.assertEqual(monitor.state['coverage']['genericProfileSignalStories'], 0)
        self.assertEqual(monitor.state['coverage']['externalNotificationSignalStories'], 1)
        entry = json.loads(monitor.db.execute('SELECT value FROM story_coverage').fetchone()[0])
        self.assertEqual(entry['pushCorrelation'], 'external_notification_hint_uncertain')
        # Even a corrupted external hint containing a Story ID cannot promote native proof.
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('UPDATE signals SET story_id=?', ('4004391581161830448',))
            db.commit()
        proof = {'signalSequence': 1, 'storyId': '4004391581161830448', 'browserClosed': True,
                 'observedAt': dt.datetime.fromtimestamp(self.now, dt.timezone.utc).isoformat()}
        self.assertFalse(signals.proves(proof, now=self.now))

    def test_later_phone_signal_survives_current_collection_and_existing_delivery_not_repeated(self):
        self.store.enqueue(self.hint())
        monitor = self.monitor()
        self.assertTrue(monitor.reserve_poll())
        self.store.enqueue(self.hint())
        monitor.record(self.result())
        self.assertEqual(monitor.state['consumedSignalSequence'], 1)
        sent = []
        monitor.flush(lambda *_: sent.append(True) or {'status': 'delivered', 'targets': {}},
                      lambda *_: {'imageBase64': '/9j/4A==', 'imageMime': 'image/jpeg'})
        self.now += 16
        self.assertTrue(monitor.reserve_poll())
        monitor.record(self.result())
        monitor.flush(lambda *_: sent.append(True) or {'status': 'delivered', 'targets': {}}, lambda *_: {})
        self.assertEqual(len(sent), 1)
        self.assertFalse(monitor.reserve_poll())

    def test_phone_hint_never_bypasses_source_backoff_or_request_budget(self):
        self.store.enqueue(self.hint())
        monitor = self.monitor()
        monitor.state.update(retryNotBefore=self.now + 3600, nextPoll=self.now + 3600)
        self.assertFalse(monitor.reserve_poll())
        self.assertEqual(monitor.state.get('consumedSignalSequence', 0), 0)
        monitor.state.update(retryNotBefore=0, nextPoll=0)
        monitor.db.execute('INSERT INTO polls VALUES(?,?,NULL)', (self.now, monitor.daily_limit()))
        monitor.commit()
        self.assertFalse(monitor.reserve_poll())
        self.assertEqual(monitor.state['sourceStatus'], 'budget_wait')
        self.assertEqual(monitor.state.get('consumedSignalSequence', 0), 0)

    def test_source_rate_limit_after_phone_wake_preserves_batch_until_guard_releases(self):
        self.store.enqueue(self.hint())
        monitor = self.monitor()
        self.assertTrue(monitor.reserve_poll())
        result = self.result()
        result.update(status='rate_limited', reason='http_429', stories=[], retryAfterSeconds=21600)
        monitor.record(result)
        self.now += 601
        self.assertFalse(monitor.reserve_poll())
        self.assertIn('pushBatch', monitor.state)
        self.assertEqual(monitor.state.get('consumedSignalSequence', 0), 0)
        self.assertGreater(monitor.state['pushBatch']['deadlineEpoch'], monitor.state['retryNotBefore'])
        self.now += 21000
        self.assertTrue(monitor.reserve_poll())
        monitor.record(self.result())
        self.assertEqual(monitor.state['pushSettlement']['status'], 'external_hint_processed')


if __name__ == '__main__':
    unittest.main()
