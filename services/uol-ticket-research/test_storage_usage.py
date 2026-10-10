import datetime as dt
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest

from storage_usage import (StorageUsageObserver, aggregate, aggregate_details, budget_state,
                           ACCOUNT_LIMIT, DETAIL_LIMIT, DETAIL_QUERY, GRAPHQL_URL)


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
            self.assertEqual(len(calls), 3)
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

    def test_essential_truncation_rejected_and_optional_duration_does_not_erase_rows(self):
        row = {'dimensions': {'date': '2026-10-10'},
               'sum': {'rowsRead': 12, 'rowsWritten': 9, 'duration': None}}
        with self.assertRaisesRegex(ValueError, 'analytics_truncated'):
            aggregate(response([row] * ACCOUNT_LIMIT), '2026-10-10')
        raw = response([row])
        raw['errors'] = [{'path': ['viewer', 'accounts', 0, 'durableObjectsPeriodicGroups', 0, 'sum', 'duration']}]
        current, _ = aggregate(raw, '2026-10-10')
        self.assertEqual(current, {'rowsRead': 12, 'rowsWritten': 9})


def details_response():
    return {'data': {'viewer': {'accounts': [{
        'projectUsage': [{'dimensions': {'date': '2026-10-10', 'namespaceId': 'fixture', 'name': 'Project'},
                          'sum': {'rowsRead': 2, 'rowsWritten': 3, 'duration': 4,
                                  'activeTime': 5, 'inboundWebsocketMsgCount': 0}}],
        'workersUsage': [{'dimensions': {'date': '2026-10-10'}, 'sum': {'requests': 120}}],
        'workersProjects': [{'dimensions': {'date': '2026-10-10', 'scriptName': 'uol-fixture'},
                             'sum': {'requests': 120}}],
        'doUsage': [{'dimensions': {'date': '2026-10-10', 'namespaceId': 'fixture',
                                   'scriptName': 'uol-fixture', 'type': kind}, 'sum': {'requests': 10}}
                    for kind in ('http', 'alarm', 'jsrpc')],
        'storageUsage': [{'dimensions': {'date': '2026-10-10'}, 'max': {'storedBytes': 1024}}],
    }]}}}


class DetailTests(unittest.TestCase):
    def test_partial_alias_error_keeps_other_metrics_and_unknown_is_not_zero(self):
        raw = details_response()
        raw['errors'] = [{'path': ['viewer', 'accounts', 0, 'projectUsage']}]
        raw['data']['viewer']['accounts'][0]['storageUsage'] = []
        result = aggregate_details(raw, '2026-10-10')
        self.assertIsNone(result['namespaces'])
        self.assertEqual(result['values'], {'workersRequests': 120, 'doRequestsRaw': 30, 'doRequests': 30})
        self.assertEqual(result['status']['storageBytes'], 'unknown')
        self.assertEqual(result['status']['namespaces'], 'unknown')

    def test_each_detail_cap_is_unknown_instead_of_partial_total(self):
        for alias, key, limit in [('projectUsage', 'namespaces', DETAIL_LIMIT),
                                  ('workersUsage', 'workersRequests', ACCOUNT_LIMIT),
                                  ('doUsage', 'doRequests', DETAIL_LIMIT),
                                  ('storageUsage', 'storageBytes', ACCOUNT_LIMIT)]:
            raw = details_response()
            groups = raw['data']['viewer']['accounts'][0]
            groups[alias] = [groups[alias][0]] * limit
            result = aggregate_details(raw, '2026-10-10')
            self.assertEqual(result['status'][key], 'unknown')
            self.assertNotIn(key, result['values'])

    def test_unknown_request_type_preserves_raw_counts_without_billable_claim(self):
        raw = details_response()
        raw['data']['viewer']['accounts'][0]['doUsage'][0]['dimensions']['type'] = 'websocket'
        result = aggregate_details(raw, '2026-10-10')
        self.assertEqual(result['values']['doRequestsRaw'], 30)
        self.assertNotIn('doRequests', result['values'])
        self.assertEqual(result['status']['doRequests'], 'billing_unknown')

    def test_project_mapping_uses_script_evidence_and_collision_never_misattributes_namespace(self):
        raw = details_response()
        result = aggregate_details(raw, '2026-10-10')
        self.assertEqual(result['projectMapping'], {'fixture': 'uol-fixture'})
        self.assertEqual(result['namespaces']['fixture']['project'], 'uol-fixture')
        self.assertEqual(result['projects']['uol-fixture'], {'namespaces': ['fixture'],
            'rowsRead': 2, 'rowsWritten': 3, 'duration': 4, 'activeTime': 5,
            'inboundWebsocketMsgCount': 0, 'doRequestsRaw': 30, 'doRequests': 30, 'workersRequests': 120})
        collision = copy.deepcopy(raw['data']['viewer']['accounts'][0]['doUsage'][0])
        collision['dimensions']['scriptName'] = 'leo-remote-fixture'
        raw['data']['viewer']['accounts'][0]['doUsage'].append(collision)
        result = aggregate_details(raw, '2026-10-10')
        self.assertEqual(result['projectMapping'], {'fixture': None})
        self.assertIsNone(result['namespaces']['fixture']['project'])
        self.assertEqual(result['unassignedNamespaces'], ['fixture'])
        self.assertNotIn('rowsRead', result['projects']['uol-fixture'])
        self.assertEqual(result['values']['doRequests'], 40)
        self.assertEqual(result['values']['workersRequests'], 120)

    def test_project_alias_truncation_and_failure_cannot_replace_global_request_total(self):
        raw = details_response()
        account = raw['data']['viewer']['accounts'][0]
        account['workersProjects'] *= DETAIL_LIMIT
        account['doUsage'] = [account['doUsage'][0]] * DETAIL_LIMIT
        result = aggregate_details(raw, '2026-10-10')
        self.assertEqual(result['values']['workersRequests'], 120)
        self.assertEqual(result['status']['workersProjects'], 'unknown')
        self.assertEqual(result['status']['projectMapping'], 'unknown')
        self.assertIsNone(result['namespaces']['fixture']['project'])
        raw = details_response()
        raw['errors'] = [{'path': ['viewer', 'accounts', 0, 'workersProjects']}]
        result = aggregate_details(raw, '2026-10-10')
        self.assertEqual(result['values']['workersRequests'], 120)
        self.assertEqual(result['projects']['uol-fixture']['rowsRead'], 2)
        self.assertNotIn('workersRequests', result['projects']['uol-fixture'])


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime(2026, 10, 10, 12, tzinfo=dt.timezone.utc).timestamp()

    def test_warning_actual_latch_and_utc_reset(self):
        state = budget_state({}, {'rowsRead': 10, 'rowsWritten': 70_000}, self.now)
        self.assertEqual(state['warnings'], ['rowsWritten'])
        self.assertFalse(state['optionalWorkDeferred'])
        state = budget_state(state, {'rowsRead': 10, 'rowsWritten': 80_000}, self.now + 900)
        self.assertEqual(state['optionalWorkReason'], 'quota_actual')
        state = budget_state(state, {'rowsRead': 10, 'rowsWritten': 70_000}, self.now + 1800)
        self.assertEqual(state['actualLatched'], ['rowsWritten'])
        reset = budget_state(state, {'rowsRead': 1, 'rowsWritten': 1}, self.now + 43200)
        self.assertFalse(reset['optionalWorkDeferred'])
        self.assertEqual(reset['history'], [{'at': self.now + 43200, 'values': {'rowsRead': 1, 'rowsWritten': 1}}])

    def test_forecast_needs_hour_and_two_samples_then_two_low_samples(self):
        state = {}
        for step in range(6):
            state = budget_state(state, {'rowsRead': 10, 'rowsWritten': 20_000 + step * 5000}, self.now + step * 900)
            if step < 4:
                self.assertIsNone(state['metrics']['rowsWritten']['forecast'])
            self.assertEqual(state['optionalWorkDeferred'], step == 5)
        self.assertEqual(state['optionalWorkReason'], 'quota_forecast')
        self.assertIs(budget_state(state, {'rowsRead': 10, 'rowsWritten': 45_000}, self.now + 4500), state)
        for step in range(6, 12):
            state = budget_state(state, {'rowsRead': 10, 'rowsWritten': 45_000}, self.now + step * 900)
            self.assertEqual(state['optionalWorkDeferred'], step < 11)
        self.assertEqual(state['forecastLowSamples'], 2)

    def test_stale_and_missing_metrics_keep_value_and_never_synthesize_zero(self):
        state = budget_state({}, {'rowsRead': 10, 'rowsWritten': 20}, self.now)
        stale = budget_state(state, {}, self.now + 1801)
        self.assertEqual(stale['optionalWorkReason'], 'quota_metrics_stale')
        self.assertEqual(stale['metrics']['rowsWritten']['value'], 20)
        self.assertIsNone(stale['metrics']['storageBytes']['value'])
        self.assertIsNone(stale['metrics']['doRequests']['forecast'])

    def test_missing_forecast_cannot_release_existing_forecast_guard(self):
        state = {}
        for step in range(6):
            state = budget_state(state, {'rowsRead': 1, 'rowsWritten': 1,
                                        'workersRequests': 20_000 + step * 5000}, self.now + step * 900)
        for step in range(6, 14):
            state = budget_state(state, {'rowsRead': 1, 'rowsWritten': 1}, self.now + step * 900)
        self.assertTrue(state['forecastDeferred'])


class ObserverContractTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for name in ('analytics', 'ingest'):
            path = self.root / name
            path.write_text('fixture-only')
            path.chmod(0o600)
        self.config = self.root / 'storage-usage.json'
        self.config.write_text(json.dumps({'accountId': 'a' * 32,
            'workerOrigin': 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev',
            'analyticsTokenFile': str(self.root / 'analytics'), 'ingestTokenFile': str(self.root / 'ingest')}))
        self.config.chmod(0o600)
        self.now = dt.datetime(2026, 10, 10, 12, tzinfo=dt.timezone.utc).timestamp()
        self.sent = []

    def request(self, url, token, data):
        if url != GRAPHQL_URL:
            self.sent.append(data)
            return {'ok': True}
        if data['query'] == DETAIL_QUERY:
            raise OSError('fixture details unavailable')
        return response([{'dimensions': {'date': '2026-10-10'},
                          'sum': {'rowsRead': 900, 'rowsWritten': 1200, 'duration': 1}}])

    def test_additional_failure_does_not_poison_global_sample_or_expose_details(self):
        observer = StorageUsageObserver(self.config, clock=lambda: self.now, requester=self.request)
        self.assertEqual(observer.run_if_due()['outcome'], 'published')
        self.assertEqual(observer.state['dailyTotals']['2026-10-10']['rowsWritten'], 1200)
        self.assertEqual(observer.state['budgetState']['metrics']['storageBytes']['status'], 'unknown')
        self.assertEqual(set(self.sent[0]), {'day', 'observedAt', 'accountRowsRead', 'accountRowsWritten',
                                            'optionalWorkDeferred', 'optionalWorkReason'})

    def test_namespace_breakdown_cannot_replace_account_total_and_failure_keeps_last_verified_value(self):
        def request(url, token, data):
            return details_response() if url == GRAPHQL_URL and data['query'] == DETAIL_QUERY else self.request(url, token, data)
        observer = StorageUsageObserver(self.config, clock=lambda: self.now, requester=request)
        self.assertEqual(observer.run_if_due()['outcome'], 'published')
        self.assertEqual(observer.state['lastDetails']['namespaces']['fixture']['rowsRead'], 2)
        self.assertEqual(observer.state['lastSample']['accountRowsRead'], 900)
        observer.requester = self.request
        self.now += 901
        self.assertEqual(observer.run_if_due()['outcome'], 'published')
        self.assertEqual(observer.state['dailyTotals']['2026-10-10']['storageBytes'], 1024)
        storage = observer.state['budgetState']['metrics']['storageBytes']
        self.assertEqual(storage['status'], 'unknown')
        self.assertEqual(storage['value'], 1024)

    def test_prior_day_details_are_refreshed_and_missing_history_is_explicitly_unknown(self):
        raw = details_response()
        for rows in raw['data']['viewer']['accounts'][0].values():
            prior = copy.deepcopy(rows[0])
            prior['dimensions']['date'] = '2026-10-09'
            if 'requests' in prior.get('sum', {}):
                prior['sum']['requests'] = 22
            rows.append(prior)
        def request(url, token, data):
            return raw if url == GRAPHQL_URL and data['query'] == DETAIL_QUERY else self.request(url, token, data)
        observer = StorageUsageObserver(self.config, clock=lambda: self.now, requester=request)
        self.assertEqual(observer.run_if_due()['outcome'], 'published')
        self.assertEqual(observer.state['dailyTotals']['2026-10-09']['workersRequests'], 22)
        self.assertEqual(observer.state['dailyDetails']['2026-10-09']['projects']['uol-fixture']['workersRequests'], 22)
        self.assertEqual(observer.state['dailyMetricStatus']['2026-10-08']['workersRequests'], 'unknown')
        observer.requester = self.request
        self.now += 901
        self.assertEqual(observer.run_if_due()['outcome'], 'published')
        self.assertEqual(observer.state['dailyTotals']['2026-10-09']['workersRequests'], 22)
        self.assertEqual(observer.state['dailyMetricStatus']['2026-10-09']['workersRequests'], 'unknown')
        latest = sorted((self.root / 'storage-usage-snapshots').glob('*.json'))[-1]
        snapshot = json.loads(latest.read_text())
        self.assertEqual(snapshot['dailyTotals']['2026-10-09']['workersRequests'], 22)
        self.assertEqual(snapshot['dailyMetricStatus']['2026-10-09']['workersRequests'], 'unknown')

    def test_no_overlap_and_reservation_is_shared_between_instances(self):
        other = StorageUsageObserver(self.config, clock=lambda: self.now, requester=self.request)
        overlap = []
        def request(url, token, data):
            overlap.append(other.run_if_due()['outcome'])
            return self.request(url, token, data)
        observer = StorageUsageObserver(self.config, clock=lambda: self.now, requester=request)
        self.assertEqual(observer.run_if_due()['outcome'], 'published')
        self.assertEqual(overlap, ['in_progress'] * 3)
        self.assertEqual(other.run_if_due()['outcome'], 'not_due')

    def test_midnight_during_collection_never_ingests_previous_day(self):
        self.now = dt.datetime(2026, 10, 10, 23, 59, 59, tzinfo=dt.timezone.utc).timestamp()
        def request(url, token, data):
            result = self.request(url, token, data)
            self.now += 2
            return result
        observer = StorageUsageObserver(self.config, clock=lambda: self.now, requester=request)
        self.assertEqual(observer.run_if_due()['outcome'], 'failed')
        self.assertEqual(self.sent, [])
        self.assertIsNone(observer.state['lastSample'])


if __name__ == '__main__':
    unittest.main()
