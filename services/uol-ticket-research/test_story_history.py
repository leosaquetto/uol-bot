import copy
import datetime as dt
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from story_history import export_story_metadata, merge_story_history

NOW = dt.datetime(2026, 10, 13, 21, tzinfo=dt.timezone.utc)
PUBLISHED = '2026-10-13T20:00:00+00:00'
SEEN = '2026-10-13T20:00:12.125000+00:00'
URL = 'https://clube.uol.com.br/campanhasdeingresso/pQg-2-ingressos-teatro'


class StoryHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'monitor.sqlite'
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE outbox(key TEXT PRIMARY KEY, story_id TEXT, payload TEXT, status TEXT, receipt TEXT);
            CREATE TABLE story_coverage(story_id TEXT PRIMARY KEY, value TEXT);
        ''')
        self.path.chmod(0o600)

    def add(self, story='private-story-id', url=URL, published=PUBLISHED, first_seen=SEEN,
            status='pending', coverage=True, baseline=False, target_status='pending', image=None):
        payload = {'storyId': story, 'link': url, 'publishedAt': published,
                   'imageUrl': 'https://private-cdn.invalid/?token=private-image-token',
                   'imageBase64': 'private-image-bytes', 'token': 'private-token'}
        receipt = {'storyId': story, 'canonicalKey': 'private-hash',
                   'targets': {name: {'status': target_status, 'imageConfirmed': image,
                                     'messageId': 'private-message-id', 'body': 'private-body'}
                               for name in ('main', 'canal2', 'discord', 'beeper')}}
        self.db.execute('INSERT INTO outbox VALUES(?,?,?,?,?)',
                        ('key-' + story, story, json.dumps(payload), status, json.dumps(receipt)))
        if coverage:
            self.db.execute('INSERT INTO story_coverage VALUES(?,?)', (story, json.dumps({
                'storyId': story, 'publishedAt': published, 'firstSeenAt': first_seen,
                'collectionTrigger': 'push', 'baseline': baseline, 'excludedReason': '',
                'pushCorrelation': 'exact_story_id', 'exactSignalSequence': 999,
                'targets': {'private-hash': receipt['targets']}})))
        self.db.commit()

    def export(self):
        return export_story_metadata(self.path, NOW)

    def test_export_allowlist_private_bodies_never_selected_or_returned(self):
        self.add(target_status='confirmed', image=True, status='delivered')
        report = self.export()
        record = report['records'][0]
        self.assertEqual(report['status'], 'available')
        self.assertEqual(record['code'], 'pQg')
        self.assertEqual(record['instagramPublishedAt'], '2026-10-13T20:00:00Z')
        self.assertEqual(record['instagramFirstSeenAt'], '2026-10-13T20:00:12.125000Z')
        self.assertEqual(set(record), {'code', 'canonicalUrl', 'instagramPublishedAt',
            'instagramFirstSeenAt', 'collectionTrigger', 'baseline', 'excludedReason',
            'pushCorrelation', 'outboxStatus', 'targets'})
        self.assertEqual(record['targets']['beeper'], {'status': 'confirmed', 'imageConfirmed': True})
        text = json.dumps(report)
        for forbidden in ('private-', 'storyId', 'messageId', 'imageUrl', 'imageBase64', 'exactSignalSequence'):
            self.assertNotIn(forbidden, text)

    def test_read_only_reads_current_wal_without_changing_database_or_wal(self):
        self.add()
        wal = Path(str(self.path) + '-wal')
        before = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in (self.path, wal)}
        self.assertEqual(len(self.export()['records']), 1)
        after = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in (self.path, wal)}
        self.assertEqual(before, after)

    def test_bad_urls_and_future_or_malformed_timestamps_are_rejected(self):
        bad = [dict(url='https://clube.uol.com.br.evil.invalid/campanhasdeingresso/pQg-teatro'),
               dict(url=URL + '?token=private-secret'), dict(url=URL + '#fragment'),
               dict(url=URL + '/'), dict(url=URL.replace('https:', 'http:')),
               dict(url=URL.replace('clube.uol.com.br', 'user:private-password@clube.uol.com.br')),
               dict(url=URL.replace('clube.uol.com.br', 'clube.uol.com.br:443')),
               dict(published='2026-10-14T20:00:00Z'), dict(published='bad-private-timestamp'),
               dict(first_seen='2026-10-13T20:00:12'), dict(first_seen='2026-10-14T20:00:12Z'),
               dict(first_seen='2026-10-13T19:59:59Z'), dict(baseline='private-baseline')]
        for index, kwargs in enumerate(bad):
            self.add(story=str(index), **kwargs)
        result = self.export()
        self.assertEqual(result['records'], [])
        self.assertEqual(result['invalidRecordCount'], len(bad))
        self.assertEqual(result['status'], 'partial')
        self.assertNotIn('private-', json.dumps(result))

    def test_absent_database_does_not_report_known_zero_and_creates_nothing(self):
        missing = self.path.parent / 'missing.sqlite'
        result = export_story_metadata(missing, NOW)
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['reason'], 'database_missing')
        self.assertIsNone(result['invalidRecordCount'])
        self.assertIsNone(result['truncated'])
        self.assertFalse(missing.exists())

    def test_private_mode_owner_and_symlink_required(self):
        self.path.chmod(0o644)
        self.assertEqual(self.export()['reason'], 'private_database_required')
        self.path.chmod(0o600)
        with patch('story_history.os.getuid', return_value=os.getuid() + 1):
            self.assertEqual(self.export()['reason'], 'private_database_required')
        link = self.path.parent / 'alias.sqlite'
        link.symlink_to(self.path)
        self.assertEqual(export_story_metadata(link, NOW)['reason'], 'private_database_required')

    def test_missing_legacy_coverage_keeps_publication_and_marks_uncertainty(self):
        self.add(coverage=False)
        result = self.export()
        self.assertEqual(result['status'], 'partial')
        record = result['records'][0]
        self.assertIsNone(record['instagramFirstSeenAt'])
        self.assertIsNone(record['baseline'])
        self.assertEqual(record['excludedReason'], 'coverage_metadata_missing')
        self.assertEqual(record['targets']['main']['imageConfirmed'], None)

    def test_invalid_json_row_is_counted_without_disclosing_error(self):
        self.db.execute('INSERT INTO outbox VALUES(?,?,?,?,?)',
                        ('private-key', 'private-story', 'private-invalid-json', 'pending', '{}'))
        self.db.commit()
        result = self.export()
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['invalidRecordCount'], 1)
        self.assertNotIn('private-', json.dumps(result))

    def test_maximum_rows_marks_truncated_not_complete(self):
        self.add(story='first')
        self.add(story='second', published='2026-10-13T20:01:00Z', first_seen='2026-10-13T20:01:12Z')
        with patch('story_history.MAX_ROWS', 1):
            result = self.export()
        self.assertTrue(result['truncated'])
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(len(result['records']), 1)

    def test_merge_deduplicates_updates_and_preserves_other_observation_times(self):
        self.add()
        old = self.export()
        updated = copy.deepcopy(old)
        updated['capturedAt'] = '2026-10-13T21:01:00Z'
        updated['records'][0]['outboxStatus'] = 'delivered'
        updated['records'][0]['targets']['main'] = {'status': 'confirmed', 'imageConfirmed': True}
        merge_now = NOW + dt.timedelta(minutes=2)
        result = merge_story_history(old, updated, now=merge_now)
        self.assertEqual(len(result['records']), 1)
        self.assertEqual(result['records'][0]['targets']['main']['imageConfirmed'], True)
        # A later stale/pending receipt cannot erase confirmed media.
        later = copy.deepcopy(old)
        later['capturedAt'] = '2026-10-13T21:02:00Z'
        result = merge_story_history(result, later, now=merge_now)
        self.assertEqual(result['records'][0]['outboxStatus'], 'delivered')
        self.assertEqual(result['records'][0]['targets']['main']['status'], 'confirmed')
        self.add(story='another-private-id', published='2026-10-13T20:02:00Z', first_seen='2026-10-13T20:02:10Z')
        result = merge_story_history(result, self.export(), now=merge_now)
        self.assertEqual(len(result['records']), 2)
        self.assertNotIn('private-', json.dumps(result))

    def test_duplicate_public_times_merge_without_exporting_private_identifiers(self):
        self.add(story='first-private-id')
        self.add(story='second-private-id', target_status='confirmed', image=True)
        result = self.export()
        self.assertEqual(len(result['records']), 1)
        self.assertTrue(result['records'][0]['targets']['main']['imageConfirmed'])
        self.assertNotIn('private-', json.dumps(result))

    def test_unrecognized_metadata_is_unknown_and_baseline_gap_is_preserved(self):
        self.add(baseline=True, target_status='private-target-id')
        value = json.loads(self.db.execute('SELECT value FROM story_coverage').fetchone()[0])
        value.update(collectionTrigger='private-signal', excludedReason='coverage_gap',
                     pushCorrelation='private-story-id')
        self.db.execute('UPDATE story_coverage SET value=?', (json.dumps(value),))
        self.db.commit()
        record = self.export()['records'][0]
        self.assertTrue(record['baseline'])
        self.assertEqual(record['excludedReason'], 'coverage_gap')
        self.assertEqual(record['collectionTrigger'], 'unknown')
        self.assertEqual(record['pushCorrelation'], 'unknown')
        self.assertEqual(record['targets']['main']['status'], 'unknown')
        self.assertNotIn('private-', json.dumps(record))

    def test_merge_unavailable_keeps_history_and_sanitizes_saved_records(self):
        self.add()
        old = self.export()
        old['privateToken'] = 'private-token'
        old['records'][0]['privateId'] = 'private-id'
        unavailable = export_story_metadata(self.path.parent / 'missing', NOW)
        result = merge_story_history(old, unavailable, now=NOW)
        self.assertEqual(len(result['records']), 1)
        self.assertEqual(result['status'], 'unavailable')
        self.assertIsNone(result['invalidRecordCount'])
        self.assertNotIn('private-', json.dumps(result))

    def test_merge_rejects_future_capture_in_export_and_saved_history(self):
        self.add()
        valid = self.export()
        future = copy.deepcopy(valid)
        future['capturedAt'] = '2026-10-13T21:00:01Z'
        with self.assertRaisesRegex(ValueError, '^invalid_story_export$'):
            merge_story_history(valid, future, now=NOW)
        with self.assertRaisesRegex(ValueError, '^invalid_story_history$'):
            merge_story_history(future, valid, now=NOW)

    def test_merge_invalid_records_mark_partial_count_and_preserve_valid_history(self):
        self.add()
        existing = self.export()
        existing['records'].append({'privateId': 'private-discarded-id'})
        exported = copy.deepcopy(self.export())
        exported['records'][0]['instagramPublishedAt'] = '2026-10-13T21:00:01Z'
        result = merge_story_history(existing, exported, now=NOW)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['invalidRecordCount'], 2)
        self.assertEqual(len(result['records']), 1)
        self.assertEqual(result['records'][0]['instagramPublishedAt'], '2026-10-13T20:00:00Z')
        self.assertNotIn('private-', json.dumps(result))

    def test_merge_rejects_invalid_export_contract_and_oversized_records(self):
        self.add()
        valid = self.export()
        for field, value in (('records', {}), ('status', 'private-status'),
                             ('invalidRecordCount', -1), ('invalidRecordCount', True),
                             ('invalidRecordCount', None), ('truncated', 1)):
            with self.subTest(field=field, value=value):
                invalid = {**valid, field: value}
                with self.assertRaisesRegex(ValueError, '^invalid_story_export$'):
                    merge_story_history(None, invalid, now=NOW)
        with patch('story_history.MAX_ROWS', 1):
            oversized = {**valid, 'records': valid['records'] * 2}
            with self.assertRaisesRegex(ValueError, '^invalid_story_export$'):
                merge_story_history(None, oversized, now=NOW)

    def test_cli_only_outputs_sanitized_json(self):
        self.add()
        process = subprocess.run([sys.executable, str(Path(__file__).with_name('story_history.py')),
                                  '--database', str(self.path)], capture_output=True, text=True)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(json.loads(process.stdout)['schemaVersion'], 1)
        self.assertEqual(process.stderr, '')
        self.assertNotIn('private-', process.stdout)


if __name__ == '__main__':
    unittest.main()
