import json
from pathlib import Path
import tempfile
import unittest

from trial import Trial, MAX_REQUESTS


class TrialTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.now = 1000
        self.trial = Trial(self.path, clock=lambda: self.now)
        self.addCleanup(self.trial.db.close)

    def result(self, ids=('4004185955500703427',), status='found'):
        return {'checkedAt': '2026-10-09T19:39:26+00:00', 'status': status,
                'reason': '', 'requests': 2, 'duration_ms': 1064, 'body_bytes': 500,
                'stories': [{'storyId': i, 'publishedAt': '2026-10-09T14:29:43+00:00',
                             'expiresAt': '2026-10-10T14:29:43+00:00',
                             'destinations': ['https://clube.uol.com.br/campanhasdeingresso/pPM-bgs']}
                            for i in ids]}

    def test_baseline_not_new_then_new_and_restart_deduplicate(self):
        self.trial.record(self.result())
        self.assertEqual(self.trial.state['newStories'], 0)
        self.trial.record(self.result(('4004185955500703427', '4004282829240101142')))
        self.assertEqual(self.trial.state['newStories'], 1)
        restart = Trial(self.path, clock=lambda: self.now)
        self.addCleanup(restart.db.close)
        restart.record(self.result(('4004185955500703427', '4004282829240101142')))
        self.assertEqual(restart.state['newStories'], 1)
        self.assertEqual(restart.db.execute('SELECT count(*) FROM events WHERE kind="new_story"').fetchone()[0], 1)

    def test_changed_destination_recorded_once(self):
        self.trial.record(self.result())
        r = self.result()
        r['stories'][0]['destinations'] = ['https://clube.uol.com.br/campanhasdeingresso/pPQ-zayn']
        self.trial.record(r)
        self.trial.record(r)
        self.assertEqual(self.trial.state['changedStories'], 1)

    def test_unknown_preserves_history_and_backoff_then_suspends(self):
        self.trial.record(self.result())
        ids = self.trial.state['currentIds'][:]
        for _ in range(6):
            self.trial.record(self.result((), 'unknown'))
        self.assertEqual(self.trial.state['currentIds'], ids)
        self.assertEqual(self.trial.gate(), 'suspended')

    def test_auth_and_rate_limit_stop_without_retry(self):
        self.trial.record(self.result((), 'auth_required'))
        self.assertEqual(self.trial.gate(), 'auth_required')
        self.trial.state['status'] = 'running'
        self.trial.record(self.result((), 'rate_limited'))
        self.assertEqual(self.trial.gate(), 'rate_limited')

    def test_deadline_and_budget_are_durable(self):
        self.trial.state['requestsReserved'] = MAX_REQUESTS
        self.assertEqual(self.trial.gate(), 'budget_exhausted')
        self.trial.state['status'] = 'running'
        self.now = self.trial.state['endEpoch']
        self.assertEqual(self.trial.gate(), 'completed')
        self.assertEqual(json.loads((self.path / 'status.json').read_text())['status'], 'completed')

    def test_crash_reservation_preserves_interval_and_budget(self):
        self.trial.reserve()
        restart = Trial(self.path, clock=lambda: self.now)
        self.addCleanup(restart.db.close)
        self.assertEqual(restart.gate(), 'waiting')
        self.assertEqual(restart.state['requestsReserved'], 2)
        self.assertEqual(restart.state['attempts'], 1)

    def test_cookie_persistence_failure_stops_with_actual_request_count(self):
        r = self.result((), 'unknown')
        r['reason'] = 'session_write_failed'
        self.trial.record(r)
        self.assertEqual(self.trial.gate(), 'suspended')
        self.assertEqual(self.trial.state['requests'], 2)


if __name__ == '__main__':
    unittest.main()
