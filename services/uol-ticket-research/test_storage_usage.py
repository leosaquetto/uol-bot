import datetime as dt
import json
import os
from pathlib import Path
import tempfile
import unittest

from storage_usage import StorageUsageObserver, aggregate


def response(rows=None):
    return {'data': {'viewer': {'accounts': [{'durableObjectsPeriodicGroups': rows or []}]}},
            'errors': None}


class StorageUsageTests(unittest.TestCase):
    def test_aggregate_every_namespace_and_reject_failure(self):
        row = {'dimensions': {'date': '2026-10-10'},
               'sum': {'rowsRead': 12, 'rowsWritten': 9, 'duration': 1.5}}
        current, _ = aggregate(response([row, row]), '2026-10-10')
        self.assertEqual(current, {'rowsRead': 24, 'rowsWritten': 18, 'duration': 3})
        with self.assertRaises(ValueError):
            aggregate({'errors': [{'message': 'denied'}]}, '2026-10-10')
        with self.assertRaises(ValueError):
            aggregate({'data': {'viewer': {'accounts': []}}}, '2026-10-10')

    def test_restarts_preserve_reservation_and_failed_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ['analytics', 'ingest']:
                (root / name).write_text('test-only')
                os.chmod(root / name, 0o600)
            config = root / 'storage-usage.json'
            config.write_text(json.dumps({'accountId': 'a' * 32,
                'workerOrigin': 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev',
                'analyticsTokenFile': str(root / 'analytics'), 'ingestTokenFile': str(root / 'ingest')}))
            os.chmod(config, 0o600)
            now = [dt.datetime(2026, 10, 10, 12, tzinfo=dt.timezone.utc).timestamp()]
            calls = []
            def request(url, token, data):
                calls.append(url)
                if url.endswith('/graphql'):
                    return response([{'dimensions': {'date': '2026-10-10'},
                        'sum': {'rowsRead': 2, 'rowsWritten': 81000, 'duration': 1}}])
                raise OSError('private transport failure')
            observer = StorageUsageObserver(config, clock=lambda: now[0], requester=request)
            self.assertEqual(observer.run_if_due()['error'], 'ingestion_unavailable')
            resumed = StorageUsageObserver(config, clock=lambda: now[0], requester=request)
            self.assertEqual(resumed.run_if_due()['outcome'], 'not_due')
            self.assertEqual(len(calls), 2)
            self.assertEqual(resumed.state['lastSample']['accountRowsWritten'], 81000)
            now[0] += 901
            self.assertEqual(resumed.run_if_due()['error'], 'ingestion_unavailable')
            self.assertEqual(resumed.state['attempts'], 2)

    def test_missing_current_day_preserves_sample_without_ingestion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ['analytics', 'ingest']:
                (root / name).write_text('test-only')
                os.chmod(root / name, 0o600)
            config = root / 'storage-usage.json'
            config.write_text(json.dumps({'accountId': 'a' * 32,
                'workerOrigin': 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev',
                'analyticsTokenFile': str(root / 'analytics'), 'ingestTokenFile': str(root / 'ingest')}))
            os.chmod(config, 0o600)
            now = [dt.datetime(2026, 10, 10, 12, tzinfo=dt.timezone.utc).timestamp()]
            rows = [{'dimensions': {'date': '2026-10-10'},
                     'sum': {'rowsRead': 2, 'rowsWritten': 81000, 'duration': 1}}]
            calls = []
            def request(url, token, data):
                calls.append(url)
                return response(rows) if url.endswith('/graphql') else {'ok': True}
            observer = StorageUsageObserver(config, clock=lambda: now[0], requester=request)
            self.assertEqual(observer.run_if_due()['outcome'], 'published')
            prior_sample = dict(observer.state['lastSample'])
            prior_totals = dict(observer.state['dailyTotals'])
            prior_published = observer.state['lastPublishedAt']
            for missing in [[], [{'dimensions': {'date': '2026-10-09'},
                                 'sum': {'rowsRead': 1, 'rowsWritten': 1, 'duration': 1}}]]:
                with self.subTest(rows=missing):
                    rows = missing
                    now[0] += 901
                    calls.clear()
                    self.assertEqual(observer.run_if_due(),
                                     {'outcome': 'failed', 'error': 'analytics_unavailable'})
                    self.assertEqual(len(calls), 1)
                    self.assertTrue(calls[0].endswith('/graphql'))
                    observer = StorageUsageObserver(config, clock=lambda: now[0], requester=request)
                    self.assertEqual(observer.state['lastSample'], prior_sample)
                    self.assertEqual(observer.state['dailyTotals'], prior_totals)
                    self.assertEqual(observer.state['lastPublishedAt'], prior_published)
            self.assertEqual(len(list((root / 'storage-usage-snapshots').glob('*.json'))), 1)

    def test_explicit_current_day_zero_is_valid_evidence(self):
        current, _ = aggregate(response([{'dimensions': {'date': '2026-10-10'},
            'sum': {'rowsRead': 0, 'rowsWritten': 0, 'duration': 0}}]), '2026-10-10')
        self.assertEqual(current, {'rowsRead': 0, 'rowsWritten': 0, 'duration': 0})


if __name__ == '__main__':
    unittest.main()
