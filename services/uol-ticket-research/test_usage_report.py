import copy
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from usage_report import build_report, iso, LIMITS, SQL_STORAGE_SOURCE, UTC

DAY = '2026-10-10'
START = dt.datetime(2026, 10, 10, tzinfo=UTC)
NOW = START + dt.timedelta(days=1, hours=4)


def snapshot(at, value=0):
    day = at.date().isoformat()
    values = {key: value for key in LIMITS}
    values['storageBytes'] = 1024
    statuses = {key: 'known' for key in LIMITS}
    detail = {'values': values.copy(), 'status': {
        'projects': 'known', 'workersProjects': 'known', 'storageProjects': 'known', 'doProjects': 'known'},
        'storageMetric': {'source': SQL_STORAGE_SOURCE},
        'projects': {'uol-fixture': {**values, 'storageStatus': 'known',
                    'namespaces': ['namespace-private-fixture']}},
        'projectMapping': {'namespace-private-fixture': 'uol-fixture'}}
    return {'observedAt': iso(at), 'budgetState': {'day': day},
            'dailyTotals': {day: values}, 'dailyMetricStatus': {day: statuses},
            'dailyDetails': {day: detail}, 'details': copy.deepcopy(detail),
            'accountId': 'account-private-fixture', 'payload': 'private-body-fixture'}


def full_day():
    rows = [snapshot(START + dt.timedelta(minutes=15 * index), index * 10)
            for index in range(96)]
    closed = snapshot(START + dt.timedelta(days=1, minutes=15))
    closed['dailyTotals'][DAY] = {**rows[-1]['dailyTotals'][DAY], 'rowsWritten': 1000}
    closed['dailyMetricStatus'][DAY] = rows[-1]['dailyMetricStatus'][DAY].copy()
    closed['dailyDetails'][DAY] = copy.deepcopy(rows[-1]['dailyDetails'][DAY])
    return rows + [closed]


class UsageReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.snapshots = self.root / 'storage-usage-snapshots'
        self.snapshots.mkdir()

    def write(self, rows):
        for index, row in enumerate(rows):
            (self.snapshots / f'{index}.json').write_text(json.dumps(row))

    def report(self, now=NOW):
        return build_report(self.root, DAY, now)

    def test_complete_day_is_confirmed_without_summing_cumulative_samples(self):
        self.write(full_day())
        report = self.report()
        self.assertEqual(report['status'], 'consumption_verified')
        self.assertEqual(report['coverage']['sampleCount'], 96)
        self.assertEqual(report['coverage']['maxGapSeconds'], 900)
        self.assertEqual(report['coverage']['endDistanceSeconds'], 900)
        writes = report['metrics']['rowsWritten']
        self.assertEqual(writes['latestInDay']['value'], 950)
        self.assertEqual(writes['value'], 1000)
        self.assertEqual(writes['percentOfLimit'], 1)
        self.assertEqual(writes['writeTarget'], 70000)
        self.assertTrue(writes['writeTargetMet'])
        self.assertFalse(report['evidence']['workerGuardOrDeliveryVerified'])
        self.assertFalse(report['evidence']['totalAcceptanceVerified'])
        self.assertEqual(report['evidence']['snapshotStage'], 'before_ingestion')

    def test_partial_day_and_missing_post_day_confirmation_are_not_verified(self):
        self.write(full_day()[:49])
        report = self.report(START + dt.timedelta(hours=12))
        self.assertEqual(report['status'], 'incomplete')
        self.assertFalse(report['coverage']['dayComplete'])
        self.assertIsNone(report['metrics']['rowsWritten']['writeTargetMet'])
        self.assertFalse(report['metrics']['rowsWritten']['consumptionVerified'])
        for path in self.snapshots.glob('*.json'):
            path.unlink()
        self.write(full_day()[:-1])
        report = self.report()
        self.assertEqual(report['status'], 'coverage_insufficient')
        self.assertTrue(report['coverage']['sufficient'])
        self.assertEqual(report['metrics']['rowsWritten']['postDayConfirmation']['status'], 'unknown')

    def test_historical_totals_in_next_day_never_count_as_samples_in_target_day(self):
        self.write([full_day()[-1]])
        report = self.report()
        self.assertEqual(report['coverage']['sampleCount'], 0)
        self.assertIsNone(report['coverage']['firstObservedAt'])
        self.assertEqual(report['metrics']['rowsWritten']['value'], 1000)
        self.assertEqual(report['status'], 'coverage_insufficient')

    def test_legacy_records_before_day_do_not_block_complete_day_or_change_files(self):
        rows = full_day()
        old = {'observedAt': iso(START - dt.timedelta(seconds=1)),
               'dailyTotals': {'2026-10-09': {'rowsWritten': 10}}}
        contradictory_old = {**old, 'budgetState': {'day': DAY}, 'dailyDetails': []}
        rows.extend([old, contradictory_old])
        self.write(rows)
        before = {path: path.read_bytes() for path in self.snapshots.glob('*.json')}
        report = self.report()
        self.assertEqual(report['status'], 'consumption_verified')
        self.assertEqual(report['coverage']['sampleCount'], 96)
        self.assertEqual(report['coverage']['invalidRecordCount'], 0)
        self.assertEqual(report['coverage']['duplicateRecordCount'], 0)
        self.assertEqual(before, {path: path.read_bytes() for path in self.snapshots.glob('*.json')})

    def test_missing_fields_within_or_after_day_remain_invalid(self):
        self.write(full_day())
        for observed in (START, START + dt.timedelta(days=1, minutes=30)):
            with self.subTest(observed=iso(observed)):
                row = {'observedAt': iso(observed), 'budgetState': {'day': observed.date().isoformat()},
                       'dailyTotals': {DAY: {'rowsWritten': 1000}}}
                (self.snapshots / 'invalid.json').write_text(json.dumps(row))
                report = self.report()
                self.assertEqual(report['status'], 'coverage_insufficient')
                self.assertEqual(report['coverage']['invalidRecordCount'], 1)
                self.assertFalse(report['metrics']['rowsWritten']['consumptionVerified'])

    def test_day_turn_mismatch_invalidates_the_observation(self):
        rows = full_day()
        rows[0]['observedAt'] = iso(START + dt.timedelta(days=1))
        self.write(rows)
        report = self.report()
        self.assertEqual(report['coverage']['invalidRecordCount'], 1)
        self.assertEqual(report['coverage']['sampleCount'], 95)
        self.assertEqual(report['status'], 'coverage_insufficient')

    def test_gap_and_both_utc_edges_are_required(self):
        for rows, expected_gap in [(full_day()[:45] + full_day()[47:], 2700),
                                   (full_day()[3:], None),
                                   (full_day()[:93] + full_day()[-1:], None)]:
            with self.subTest(expected_gap=expected_gap, size=len(rows)):
                for path in self.snapshots.glob('*.json'):
                    path.unlink()
                self.write(rows)
                report = self.report()
                self.assertEqual(report['status'], 'coverage_insufficient')
                self.assertFalse(report['coverage']['sufficient'])
                if expected_gap:
                    self.assertEqual(report['coverage']['maxGapSeconds'], expected_gap)
                    self.assertEqual(len(report['coverage']['gapsOver1800Seconds']), 1)

    def test_latest_unknown_never_recovers_old_value_or_confirmation(self):
        rows = full_day()
        unknown = copy.deepcopy(rows[-1])
        unknown['observedAt'] = iso(START + dt.timedelta(days=1, minutes=30))
        unknown['dailyMetricStatus'][DAY]['storageBytes'] = 'unknown'
        unknown['dailyDetails'][DAY]['projects']['uol-fixture']['storageStatus'] = 'unknown'
        rows.append(unknown)
        self.write(rows)
        report = self.report()
        storage = report['metrics']['storageBytes']
        self.assertEqual(storage['status'], 'unknown')
        self.assertIsNone(storage['value'])
        self.assertIsNone(storage['observedAt'])
        self.assertFalse(storage['consumptionVerified'])
        self.assertEqual(report['status'], 'coverage_insufficient')
        self.assertIsNone(report['projects']['items'][0]['metrics']['storageBytes']['value'])

    def test_unknown_initial_or_final_metric_prevents_coverage_claim(self):
        for index in (0, 95):
            with self.subTest(index=index):
                for path in self.snapshots.glob('*.json'):
                    path.unlink()
                rows = full_day()
                rows[index]['dailyMetricStatus'][DAY]['rowsWritten'] = 'unknown'
                self.write(rows)
                report = self.report()
                self.assertFalse(report['metrics']['rowsWritten']['coverageSufficient'])
                self.assertEqual(report['status'], 'coverage_insufficient')

    def test_identical_duplicates_deduplicate_and_conflicting_ones_invalidate(self):
        rows = full_day()
        rows.append(copy.deepcopy(rows[0]))
        self.write(rows)
        report = self.report()
        self.assertEqual(report['status'], 'consumption_verified')
        self.assertEqual(report['coverage']['sampleCount'], 96)
        self.assertEqual(report['coverage']['duplicateRecordCount'], 1)
        conflict = copy.deepcopy(rows[0])
        conflict['dailyTotals'][DAY]['rowsWritten'] = 1
        (self.snapshots / 'conflict.json').write_text(json.dumps(conflict))
        report = self.report()
        self.assertEqual(report['status'], 'coverage_insufficient')
        self.assertGreaterEqual(report['coverage']['invalidRecordCount'], 2)

    def test_malformed_and_future_records_block_success_without_exposing_data(self):
        self.write(full_day())
        cases = ['not json', '[]', '{"observedAt":"secret-path-fixture"}',
                 json.dumps(snapshot(NOW + dt.timedelta(seconds=1)))]
        for value in (True, -1, '1000', float('nan')):
            row = snapshot(START + dt.timedelta(minutes=7))
            row['dailyTotals'][DAY]['rowsWritten'] = value
            cases.append(json.dumps(row))
        non_utc = snapshot(START + dt.timedelta(minutes=7))
        non_utc['observedAt'] = '2026-10-10T00:07:00-03:00'
        cases.append(json.dumps(non_utc))
        malformed_details = snapshot(START + dt.timedelta(minutes=7))
        malformed_details['dailyDetails'] = []
        cases.append(json.dumps(malformed_details))
        for value in cases:
            with self.subTest(value=value[:40]):
                path = self.snapshots / 'malformed-private-path-fixture.json'
                path.write_text(value)
                before = path.read_bytes()
                report = self.report()
                self.assertEqual(report['status'], 'coverage_insufficient')
                self.assertGreater(report['coverage']['invalidRecordCount'], 0)
                self.assertEqual(path.read_bytes(), before)
                encoded = json.dumps(report)
                self.assertNotIn('secret-path-fixture', encoded)
                self.assertNotIn('malformed-private-path-fixture', encoded)

    def test_counter_regression_is_insufficient_but_storage_is_a_gauge(self):
        rows = full_day()
        rows[40]['dailyTotals'][DAY]['rowsWritten'] = 0
        self.write(rows)
        self.assertEqual(self.report()['status'], 'coverage_insufficient')
        for path in self.snapshots.glob('*.json'):
            path.unlink()
        rows = full_day()
        for index, row in enumerate(rows[:-1]):
            row['dailyTotals'][DAY]['storageBytes'] = 1000 - index
        rows[-1]['dailyTotals'][DAY]['storageBytes'] = 1
        self.write(rows)
        self.assertEqual(self.report()['status'], 'consumption_verified')

    def test_post_day_regression_and_legacy_storage_source_are_not_verified(self):
        rows = full_day()
        prior = copy.deepcopy(rows[-1])
        prior['dailyTotals'][DAY]['rowsWritten'] = 1200
        rows[-1]['observedAt'] = iso(START + dt.timedelta(days=1, minutes=30))
        rows.append(prior)
        self.write(rows)
        report = self.report()
        self.assertEqual(report['metrics']['rowsWritten']['value'], 1000)
        self.assertFalse(report['metrics']['rowsWritten']['consumptionVerified'])
        self.assertEqual(report['status'], 'coverage_insufficient')
        for path in self.snapshots.glob('*.json'):
            path.unlink()
        rows = full_day()
        rows[-1]['dailyDetails'][DAY]['storageMetric']['source'] = 'legacy-storage-source'
        self.write(rows)
        report = self.report()
        self.assertIsNone(report['metrics']['storageBytes']['value'])
        self.assertFalse(report['metrics']['storageBytes']['consumptionVerified'])
        self.assertIsNone(report['projects']['items'][0]['metrics']['storageBytes']['value'])

    def test_projects_expose_script_names_and_partial_storage_without_identifiers(self):
        rows = full_day()
        project = rows[-1]['dailyDetails'][DAY]['projects']['uol-fixture']
        project['storageStatus'] = 'partial'
        self.write(rows)
        report = self.report()
        item = report['projects']['items'][0]
        self.assertEqual(item['name'], 'uol-fixture')
        self.assertEqual(item['metrics']['storageBytes'], {'status': 'partial', 'value': 1024})
        encoded = json.dumps(report)
        for private in ('namespace-private-fixture', 'account-private-fixture',
                        'private-body-fixture', 'namespaceId', 'accountId', 'projectMapping'):
            self.assertNotIn(private, encoded)

    def test_unknown_do_projects_does_not_recover_counts_and_writes_verify_independently(self):
        rows = full_day()
        for row in rows:
            if DAY in row['dailyMetricStatus']:
                row['dailyMetricStatus'][DAY]['doRequests'] = 'billing_unknown'
        detail = rows[-1]['dailyDetails'][DAY]
        detail['status']['doProjects'] = 'unknown'
        detail['projects']['uol-fixture']['doRequestsRaw'] = 123
        self.write(rows)
        report = self.report()
        project = report['projects']['items'][0]['metrics']
        self.assertEqual(project['doRequests'], {'status': 'unknown', 'value': None})
        self.assertEqual(project['doRequestsRaw'], {'status': 'unknown', 'value': None})
        self.assertEqual(report['status'], 'coverage_insufficient')
        self.assertTrue(report['metrics']['rowsWritten']['consumptionVerified'])
        self.assertTrue(report['metrics']['rowsWritten']['writeTargetMet'])

    def test_cli_is_read_only_and_emits_safe_json(self):
        self.write(full_day())
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns)
                  for path in self.snapshots.glob('*.json')}
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('usage_report.py')),
                                 '--state-dir', str(self.root), '--day', DAY],
                                capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        self.assertEqual(report['day'], DAY)
        self.assertEqual(result.stderr, '')
        self.assertEqual(before, {path: (path.read_bytes(), path.stat().st_mtime_ns)
                                  for path in self.snapshots.glob('*.json')})
        self.assertEqual(set(self.root.iterdir()), {self.snapshots})


if __name__ == '__main__':
    unittest.main()
