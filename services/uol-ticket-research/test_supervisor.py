import datetime as dt
import json
import os
from pathlib import Path
import tempfile
import unittest

from supervisor import NTFY_URL, SERVICES, Supervisor, iso, private_json, push_summary


NOW = dt.datetime(2026, 10, 10, 12, tzinfo=dt.timezone.utc).timestamp()


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = NOW
        self.sent = []
        self.config = {'ntfyUrl': NTFY_URL, 'credentialExpiries': [
            {'name': 'analytics-read', 'expiresAt': '2026-11-09T00:00:00Z'}]}
        self.supervisor = Supervisor(self.root, self.config, clock=lambda: self.now,
            sender=lambda packet: self.sent.append(dict(packet)) or {'outcome': 'accepted'})

    def healthy(self):
        return {'units': {x: 'active' for x in SERVICES},
                'loop': {'loopAliveAt': iso(self.now)},
                'status': {'sourceStatus': 'empty', 'outboxProgress': {'pending': 0}},
                'push': {'status': 'connected', 'observedEpoch': self.now,
                         'eventCount': 0, 'pendingCount': 0, 'payloadBytes': 0},
                'usage': {'lastSample': {'observedAt': iso(self.now),
                          'day': iso(self.now)[:10]}, 'lastOutcome': 'published'}}

    def resume(self, sender=None):
        self.supervisor = Supervisor(self.root, self.config, clock=lambda: self.now,
            sender=sender or (lambda packet: self.sent.append(dict(packet)) or {'outcome': 'accepted'}))

    def test_no_stories_healthy_is_silent_and_open_recovery_are_once(self):
        self.supervisor.run_once(self.healthy())
        self.assertFalse(self.sent)
        bad = self.healthy()
        bad['status']['sourceStatus'] = 'auth_required'
        self.supervisor.run_once(bad)
        self.assertEqual(len(self.sent), 1)
        self.now += 120
        self.resume()
        self.supervisor.run_once(bad)
        self.assertEqual(len(self.sent), 1)
        self.supervisor.run_once(self.healthy())
        self.assertEqual(len(self.sent), 2)
        self.assertIn('RECUPERADO', self.sent[-1]['message'])

    def test_service_requires_two_consecutive_failures(self):
        bad = self.healthy()
        bad['units'][SERVICES[0]] = 'inactive'
        self.supervisor.run_once(bad)
        self.assertFalse(self.sent)
        self.now += 120
        self.resume()
        self.supervisor.run_once(bad)
        self.assertEqual(len(self.sent), 1)
        self.assertTrue(self.sent[0]['critical'])

    def test_unknown_collection_is_not_treated_as_empty_story_weekend(self):
        sources = self.healthy()
        sources['status'].update(sourceStatus='unknown', failures=3,
                                 lastSuccessAt=iso(self.now - 1200))
        self.assertIn('instagram_collection', self.supervisor.evaluate(sources, self.now))
        sources['status']['sourceStatus'] = 'empty'
        self.assertNotIn('instagram_collection', self.supervisor.evaluate(sources, self.now))

    def test_liveness_push_and_metrics_grace_and_freshness(self):
        old = self.healthy()
        old['loop'] = {}
        old['usage'] = {}
        old['push'] = {'status': 'disconnected'}
        self.supervisor.run_once(old)
        self.assertFalse(self.sent)
        self.now += 301
        issues = self.supervisor.evaluate(old, self.now)
        self.assertIn('monitor_loop', issues)
        self.assertNotIn('push_connection', issues)
        self.now += 300
        self.assertIn('push_connection', self.supervisor.evaluate(old, self.now))
        self.now += 1200
        self.assertIn('metrics_stale', self.supervisor.evaluate(old, self.now))

    def test_backlog_progress_resets_stagnation_not_retries(self):
        bad = self.healthy()
        bad['status']['outboxProgress'] = {'pending': 1, 'lastProgressAt': iso(self.now),
                                        'nextRetryAt': iso(self.now + 3600)}
        self.supervisor.run_once(bad)
        self.now += 901
        self.assertIn('delivery_backlog', self.supervisor.evaluate(bad, self.now))
        bad['status']['outboxProgress']['lastProgressAt'] = iso(self.now)
        self.assertNotIn('delivery_backlog', self.supervisor.evaluate(bad, self.now))

    def test_existing_backlog_age_is_not_reset_by_supervisor_installation(self):
        bad = self.healthy()
        bad['status']['outboxProgress'] = {'pending': 1, 'oldestPendingEpoch': self.now - 3600,
                                        'lastProgressAt': iso(self.now - 1800)}
        self.assertIn('delivery_backlog', self.supervisor.evaluate(bad, self.now))

    def test_unknown_evidence_does_not_close_incident(self):
        bad = self.healthy()
        bad['status']['sourceStatus'] = 'auth_required'
        self.supervisor.run_once(bad)
        missing = self.healthy()
        missing['status'] = {}
        self.now += 120
        self.supervisor.run_once(missing)
        self.assertEqual(len(self.sent), 1)
        self.assertTrue(self.supervisor.state['incidents']['instagram_auth']['active'])
        self.supervisor.run_once(self.healthy())
        self.assertEqual(len(self.sent), 2)

    def test_clock_forward_values_are_not_fresh(self):
        self.now += 1801
        sources = self.healthy()
        sources['loop']['loopAliveAt'] = iso(self.now + 3600)
        sources['push']['observedEpoch'] = self.now + 3600
        sources['usage']['lastSample']['observedAt'] = iso(self.now + 3600)
        issues = self.supervisor.evaluate(sources, self.now)
        self.assertTrue({'monitor_loop', 'push_connection', 'metrics_stale'} <= issues.keys())

    def test_budget_capacity_and_unknown_storage(self):
        bad = self.healthy()
        bad['usage']['budgetState'] = {'optionalWorkDeferred': True,
                                      'warnings': ['rowsWritten', 'storageBytes']}
        bad['push']['payloadBytes'] = int(0.8 * 32 * 1024 * 1024) + 1
        issues = self.supervisor.evaluate(bad, self.now)
        self.assertIn('quota_deferred', issues)
        self.assertIn('quota:storageBytes', issues)
        self.assertIn('capacity:payloadBytes', issues)
        self.assertNotIn('capacity:eventCount', issues)

    def test_credential_escalation_has_no_false_recovery(self):
        expiry = dt.datetime(2026, 11, 9, tzinfo=dt.timezone.utc).timestamp()
        for remaining, key in [(7 * 86400, '7d'), (48 * 3600, '48h'), (0, 'expired')]:
            self.now = expiry - remaining
            self.supervisor.state['lastRunAt'] = iso(self.now - 120)
            self.supervisor.run_once(self.healthy())
            self.assertIn('credential:analytics-read:' + key, self.supervisor.state['incidents'])
            self.assertNotIn('RECUPERADO', self.sent[-1]['message'])

    def test_notifications_reserve_five_critical_attempts(self):
        self.supervisor.state['publicationAttempts'] = [self.now] * 15
        self.supervisor.queue([('aviso', False)], self.now)
        self.supervisor.flush()
        self.assertFalse(self.sent)
        self.supervisor.queue([('falha crítica', True)], self.now)
        self.supervisor.flush()
        self.assertEqual(len(self.sent), 1)
        self.assertTrue(self.sent[0]['critical'])
        self.supervisor.state['publicationAttempts'] = [self.now] * 20
        self.supervisor.queue([('outra falha', True)], self.now)
        self.supervisor.flush()
        self.assertEqual(len(self.sent), 1)

    def test_retry_after_three_attempts_and_uncertain_crash(self):
        outcomes = [{'outcome': 'retry', 'retryAfter': 600}, {'outcome': 'retry'}, {'outcome': 'retry'}]
        self.supervisor.sender = lambda packet: outcomes.pop(0)
        self.supervisor.queue([('falha', True)], self.now)
        self.supervisor.flush()
        entry = self.supervisor.state['outbox'][0]
        self.assertEqual(entry['due'], self.now + 600)
        self.now += 599
        self.supervisor.flush()
        self.assertEqual(entry['attempts'], 1)
        self.now += 1
        self.supervisor.flush()
        self.assertEqual(entry['due'], self.now + 300)
        self.now += 300
        self.supervisor.flush()
        self.assertEqual(entry['outcome'], 'failed')
        self.assertEqual(entry['attempts'], 3)
        self.supervisor.queue([('interrompida', True)], self.now)
        self.supervisor.state['outbox'][-1]['outcome'] = 'in_flight'
        self.supervisor.save()
        self.resume()
        self.supervisor.flush()
        self.assertEqual(self.supervisor.state['outbox'][-1]['outcome'], 'uncertain')
        self.assertFalse(self.sent)

    def test_retry_after_applies_to_new_incidents_and_survives_restart(self):
        self.supervisor.sender = lambda packet: {'outcome': 'retry', 'retryAfter': 600}
        self.supervisor.queue([('premier', True)], self.now)
        self.supervisor.flush()
        self.now += 120
        self.resume()
        self.supervisor.queue([('second', True)], self.now)
        self.supervisor.flush()
        self.assertFalse(self.sent)
        self.assertEqual(len(self.supervisor.state['publicationAttempts']), 1)
        self.now += 480
        self.supervisor.flush()
        self.assertEqual(len(self.sent), 1)

    def test_gap_after_restart_reported_once(self):
        self.supervisor.run_once(self.healthy())
        self.now += 601
        self.resume()
        self.supervisor.run_once(self.healthy())
        self.assertEqual(len(self.sent), 1)
        self.assertIn('causa', self.sent[-1]['message'])
        self.now += 120
        self.supervisor.run_once(self.healthy())
        self.assertEqual(len(self.sent), 1)

    def test_private_files_and_destination_are_enforced(self):
        with self.assertRaises(ValueError):
            Supervisor(self.root, {'ntfyUrl': 'https://example.com'})
        path = self.root / 'private.json'
        path.write_text('{}')
        os.chmod(path, 0o644)
        with self.assertRaises(ValueError):
            private_json(path)
        os.chmod(path, 0o600)
        self.assertEqual(private_json(path), {})
        alias = self.root / 'alias.json'
        alias.symlink_to(path)
        with self.assertRaises(OSError):
            private_json(alias)

    def test_push_summary_reads_legacy_counts_without_payload_exposure(self):
        import sqlite3
        path = self.root / 'push.sqlite'
        with sqlite3.connect(path) as db:
            db.executescript('CREATE TABLE receiver_state(id INTEGER,status TEXT,observed_at INTEGER);'
                             'CREATE TABLE push_events(packet TEXT,state TEXT);')
            db.execute('INSERT INTO receiver_state VALUES(1,?,?)', ('connected', self.now * 1000))
            db.execute('INSERT INTO push_events VALUES(?,?)', ('encrypted-test-packet', 'pending'))
        os.chmod(path, 0o600)
        result = push_summary(self.root)
        self.assertEqual(result['pendingCount'], 1)
        self.assertEqual(result['payloadBytes'], 21)
        self.assertNotIn('packet', result)


if __name__ == '__main__':
    unittest.main()
