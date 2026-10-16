"""Server JSON cadence, durable budget and diagnostic isolation contracts."""
import datetime as dt
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from monitor import Monitor, private_config, INGEST_URL
from push_signals import PushSignals


class JsonMonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.now = 1792170000
        self.config = {'collectionBackend': 'json', 'pushMode': 'poll', 'pollIntervalSeconds': 300, 'maxDailyRequests': 300}
        self.m = self.open()

    def open(self):
        m = Monitor(self.path, self.config, clock=lambda: self.now)
        self.addCleanup(m.db.close)
        return m

    def identity(self):
        return {'username': 'clubeuol', 'id': '1234567890', 'verifiedEpoch': self.now}

    def result(self, **extra):
        return {'status': 'empty', 'reason': '', 'stories': [], 'requests': 1, 'duration_ms': 1,
                'body_bytes': 100, 'checkedAt': dt.datetime.fromtimestamp(self.now, dt.timezone.utc).isoformat(), **extra}

    def test_reservation_and_restart_keep_cadence_and_cache_budget(self):
        self.assertTrue(self.m.reserve_poll())
        self.assertEqual(self.m.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 2)
        self.m.record(self.result(profileIdentity={'username': 'clubeuol', 'id': '1234567890'}))
        restart = self.open()
        self.assertFalse(restart.reserve_poll())
        self.now += 311
        self.assertTrue(restart.reserve_poll())
        self.assertEqual(restart.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 3)
        self.assertFalse(self.open().reserve_poll(), 'crash cannot immediately repeat a reserved request')

    def test_full_day_fits_budget_without_two_request_charge_on_warm_cycles(self):
        for index in range(288):
            self.assertTrue(self.m.reserve_poll(), f'cycle {index} must fit')
            result = self.result()
            if index == 0:
                result['profileIdentity'] = {'username': 'clubeuol', 'id': '1234567890'}
            with patch('monitor.random.uniform', return_value=0):
                self.m.record(result)
            self.now += 300
        self.assertEqual(self.m.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 289)
        self.assertIsNone(self.m.profile_identity(), 'warm responses do not renew the identity TTL')

    def test_budget_counts_uncertain_reservations_and_stale_cache_costs_two(self):
        self.m.state['profileIdentity'] = self.identity()
        self.m.db.execute('INSERT INTO polls VALUES(?,?,NULL)', (self.now, 299))
        self.m.db.commit()
        self.assertTrue(self.m.reserve_poll())
        restart = self.open()
        self.now += 301
        self.assertFalse(restart.reserve_poll())
        self.assertEqual(restart.state['sourceStatus'], 'budget_wait')
        self.assertEqual(restart.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 300)

    def test_rate_limit_survives_restart_and_never_becomes_success(self):
        self.assertTrue(self.m.reserve_poll())
        self.m.record(self.result(status='rate_limited', reason='http_429', retryAfterSeconds=21600))
        restart = self.open()
        self.now += 21599
        self.assertFalse(restart.reserve_poll())
        self.assertEqual(restart.state['sourceStatus'], 'rate_limited')
        self.assertIsNone(restart.state['lastSuccessAt'])
        self.now += 1
        self.assertTrue(restart.reserve_poll())

    def test_identity_selected_by_one_request_reservation_survives_ttl_boundary(self):
        identity = self.identity()
        identity['verifiedEpoch'] = self.now - 86400 + 1
        self.m.state['profileIdentity'] = identity
        self.assertTrue(self.m.reserve_poll())
        self.assertEqual(self.m.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 1)
        self.now += 2
        self.assertIsNone(self.m.profile_identity())
        self.assertEqual(self.m._reserved_profile_identity, identity)

    def test_retention_preserves_unresolved_rows_and_corresponding_coverage(self):
        expiry = self.now - 31 * 86400
        for key, status in [('uncertain', 'unknown'), ('pending', 'pending'), ('complete', 'delivered')]:
            payload = {'storyId': key, 'publishedAt': dt.datetime.fromtimestamp(expiry - 60, dt.timezone.utc).isoformat()}
            self.m.db.execute('INSERT INTO outbox VALUES(?,?,?,?,?,?,?,?)', (key, key, expiry, json.dumps(payload), status, 0, 1, '{"evidence":"preserve"}'))
            self.m.write_coverage(key, {'firstSeenEpoch': expiry, 'baseline': True, 'targets': {}, 'publishedEpoch': expiry})
        self.m.commit()
        self.m.record(self.result(status='rate_limited', reason='http_429'))
        self.assertEqual(self.m.db.execute('SELECT key FROM outbox ORDER BY key').fetchall(), [('pending',), ('uncertain',)])
        self.assertEqual(self.m.db.execute('SELECT story_id FROM story_coverage ORDER BY story_id').fetchall(), [('pending',), ('uncertain',)])
        self.m.flush(send=lambda *args: self.fail('expired or uncertain row must not send'))
        self.assertEqual(self.m.db.execute("SELECT status,receipt FROM outbox WHERE key='uncertain'").fetchone(), ('unknown', '{"evidence":"preserve"}'))

    def test_private_json_config_rejects_unbounded_polling(self):
        path = self.path / 'config.json'
        config = {**self.config, 'ingestUrl': INGEST_URL, 'ingestToken': 'a' * 64}
        def write(changes):
            path.write_text(json.dumps({**config, **changes})); path.chmod(0o600)
        write({})
        self.assertEqual(private_config(path)['collectionBackend'], 'json')
        for changes in [{'pollIntervalSeconds': 1}, {'pollIntervalSeconds': True}, {'maxDailyRequests': 301}, {'collectionBackend': 'arbitrary'}]:
            write(changes)
            with self.assertRaises(ValueError): private_config(path)

    def test_html_policy_reserves_two_requests_and_preserves_global_backoff(self):
        self.config.update(collectionBackend='html', pushMode='pilot', maxDailyRequests=600)
        m = self.open()
        self.assertTrue(m.reserve_poll())
        self.assertEqual(m.db.execute('SELECT sum(reserved) FROM polls').fetchone()[0], 2)
        with patch('monitor.random.uniform', return_value=0):
            m.record(self.result())
        self.assertEqual(m.state['nextPoll'], self.now + 300)
        self.assertEqual(m.state['activePollPeriodSeconds'], 300)
        self.now += 300
        self.assertTrue(m.reserve_poll())
        m.record(self.result(status='rate_limited', reason='http_429', retryAfterSeconds=60))
        self.config['collectionBackend'] = 'json'
        restart = self.open()
        self.now += 21599
        self.assertFalse(restart.reserve_poll(), 'backend switch cannot bypass the global 429')
        self.now += 1
        self.assertTrue(restart.reserve_poll())

    def test_json_health_reason_keeps_remote_enum_and_detailed_local_error(self):
        self.m.state.update(sourceStatus='unknown', lastObservationAt=self.result()['checkedAt'],
            lastResult={'reason': 'profile_identity_mismatch'})
        payloads = []
        self.m.publish_health(lambda config, payload: payloads.append(payload) or True)
        self.assertEqual(payloads[0]['reason'], 'malformed_story_payload')
        self.assertEqual(self.m.state['lastResult']['reason'], 'profile_identity_mismatch')

    def test_fbns_registration_429_blocks_all_source_backends(self):
        path = self.path / 'fbns-state.json'
        path.write_text(json.dumps({'httpRetryNotBefore': self.now + 21600})); path.chmod(0o600)
        self.assertFalse(self.m.reserve_poll())
        self.assertEqual(self.m.state['retryNotBefore'], self.now + 21600)
        self.assertEqual(self.m.db.execute('SELECT count(*) FROM polls').fetchone()[0], 0)
        self.config.update(collectionBackend='html', pushMode='pilot')
        self.assertFalse(self.open().reserve_poll())

    def test_diagnostic_profile_never_enters_production_signal_consumer(self):
        path = self.path / 'push.sqlite'
        with sqlite3.connect(path) as db:
            db.executescript('CREATE TABLE signals(seq INTEGER PRIMARY KEY,profile TEXT,story_id TEXT,received_at INTEGER); CREATE TABLE receiver_state(id INTEGER PRIMARY KEY,status TEXT,observed_at INTEGER);')
            db.execute('INSERT INTO signals VALUES(1,?,?,?)', ('leotheblueblur', '4004185955500703427', self.now * 1000))
            db.execute('INSERT INTO receiver_state VALUES(1,?,?)', ('connected', self.now * 1000))
        os.chmod(path, 0o600)
        signals = PushSignals(self.path)
        self.assertEqual(signals.snapshot(self.now)['sequence'], 0)
        self.assertEqual(signals.pending(0, 10), [])
        self.assertEqual(signals.signals(), [])


if __name__ == '__main__':
    unittest.main()
