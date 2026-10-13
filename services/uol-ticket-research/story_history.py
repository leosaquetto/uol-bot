#!/usr/bin/env python3
"""Export public Story metadata from the private monitor database, without network."""

import argparse
from contextlib import closing
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from urllib.parse import urlsplit

MAX_ROWS = 5000
TARGETS = ('main', 'canal2', 'discord', 'beeper')
TARGET_STATUSES = {'confirmed', 'pending', 'unknown', 'failed_safe', 'held',
                   'in_flight', 'reconciling', 'expired', 'disabled'}
OUTBOX_STATUSES = {'delivered', 'pending', 'expired', 'unknown', 'held'}
TRIGGERS = {'poll', 'push', 'safety', 'unknown'}
CORRELATIONS = {'exact_story_id', 'without_corresponding_push',
                'generic_profile_signal_uncertain', 'awaiting_push', 'unknown'}
EXCLUSIONS = {'', 'coverage_gap', 'coverage_metadata_missing', 'unknown'} | {
    'receiver_' + value for value in ('stale', 'connected', 'disconnected', 'disabled',
                                     'starting', 'unavailable', 'paused', 'backoff',
                                     'auth_required', 'error', 'offline', 'blocked')
}
_STAMP = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
                    r'(?:\.\d{1,6})?(?:Z|\+00:00)')
_PATH = re.compile(r'/campanhasdeingresso/(p[A-Za-z0-9]{2,5})-'
                   r'[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*')


def _iso(value):
    return value.astimezone(dt.timezone.utc).isoformat().replace('+00:00', 'Z')


def _now(value):
    if value is None:
        return dt.datetime.now(dt.timezone.utc)
    if isinstance(value, dt.datetime) and value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc)
    if type(value) in (int, float) and math.isfinite(value):
        return dt.datetime.fromtimestamp(value, dt.timezone.utc)
    raise ValueError('invalid_capture_time')


def _stamp(value, captured):
    if not isinstance(value, str) or not _STAMP.fullmatch(value):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.timestamp() <= 0 or parsed > captured:
            return None
        return parsed
    except (ValueError, OverflowError, OSError):
        return None


def _campaign(value):
    if not isinstance(value, str) or len(value) > 2048:
        return None
    try:
        parsed = urlsplit(value)
        match = _PATH.fullmatch(parsed.path)
        if (parsed.scheme != 'https' or parsed.hostname != 'clube.uol.com.br'
                or parsed.username or parsed.password or parsed.port is not None
                or parsed.query or parsed.fragment or not match):
            return None
        return match[1], 'https://clube.uol.com.br' + parsed.path
    except ValueError:
        return None


def _enum(value, allowed):
    return value if isinstance(value, str) and value in allowed else 'unknown'


def _record(value, captured):
    """Allowlist again when merging a saved or external export."""
    if not isinstance(value, dict):
        return None
    campaign = _campaign(value.get('canonicalUrl'))
    published = _stamp(value.get('instagramPublishedAt'), captured)
    first_text = value.get('instagramFirstSeenAt')
    first = _stamp(first_text, captured) if first_text is not None else None
    if (not campaign or value.get('code') != campaign[0] or not published
            or (first_text is not None and first is None) or (first and first < published)
            or (value.get('baseline') is not None and type(value['baseline']) is not bool)):
        return None
    targets = value.get('targets')
    targets = targets if isinstance(targets, dict) else {}
    sanitized = {}
    for name in TARGETS:
        target = targets.get(name)
        target = target if isinstance(target, dict) else {}
        image = target.get('imageConfirmed')
        sanitized[name] = {'status': _enum(target.get('status'), TARGET_STATUSES),
                           'imageConfirmed': image if type(image) is bool else None}
    return {'code': campaign[0], 'canonicalUrl': campaign[1],
            'instagramPublishedAt': _iso(published),
            'instagramFirstSeenAt': _iso(first) if first else None,
            'collectionTrigger': _enum(value.get('collectionTrigger'), TRIGGERS),
            'baseline': value.get('baseline'),
            'excludedReason': _enum(value.get('excludedReason'), EXCLUSIONS),
            'pushCorrelation': _enum(value.get('pushCorrelation'), CORRELATIONS),
            'outboxStatus': _enum(value.get('outboxStatus'), OUTBOX_STATUSES),
            'targets': sanitized}


def _extract(document, path, kind='extract'):
    # SQL performs projection; private JSON bodies never enter Python or stdout.
    return f"CASE WHEN json_valid({document}) THEN json_{kind}({document}, '{path}') END"


_FIELDS = {
    'canonicalUrl': _extract('o.payload', '$.link'),
    'instagramPublishedAt': _extract('o.payload', '$.publishedAt'),
    'coveragePublished': _extract('c.value', '$.publishedAt'),
    'instagramFirstSeenAt': _extract('c.value', '$.firstSeenAt'),
    'collectionTrigger': _extract('c.value', '$.collectionTrigger'),
    'baseline': _extract('c.value', '$.baseline'),
    'baselineType': _extract('c.value', '$.baseline', 'type'),
    'excludedReason': _extract('c.value', '$.excludedReason'),
    'pushCorrelation': _extract('c.value', '$.pushCorrelation'),
    'outboxStatus': 'o.status',
    'hasCoverage': 'c.story_id IS NOT NULL',
}
for _target in TARGETS:
    for _field in ('status', 'imageConfirmed'):
        _FIELDS[_target + '_' + _field] = _extract('o.receipt', '$.targets.' + _target + '.' + _field)
    _FIELDS[_target + '_imageType'] = _extract('o.receipt', '$.targets.' + _target + '.imageConfirmed', 'type')
_QUERY = ('SELECT ' + ', '.join(f'{value} AS {key}' for key, value in _FIELDS.items())
          + ' FROM outbox o LEFT JOIN story_coverage c ON c.story_id=o.story_id'
          + ' ORDER BY o.rowid LIMIT ?')


def export_story_metadata(database_path, now=None):
    """Read a private, current-owner SQLite database; never fetch Instagram or send."""
    captured = _now(now)
    result = {'schemaVersion': 1, 'capturedAt': _iso(captured), 'status': 'unavailable',
              'invalidRecordCount': None, 'truncated': None, 'records': []}
    try:
        path = Path(database_path).absolute()
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()):
            result['reason'] = 'private_database_required'
            return result
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            rows = db.execute(_QUERY, (MAX_ROWS + 1,)).fetchall()
        result.update(status='available', invalidRecordCount=0, truncated=len(rows) > MAX_ROWS)
        incomplete = False
        for row in rows[:MAX_ROWS]:
            raw = {key: row[key] for key in ('canonicalUrl', 'instagramPublishedAt',
                   'instagramFirstSeenAt', 'collectionTrigger', 'excludedReason',
                   'pushCorrelation', 'outboxStatus')}
            campaign = _campaign(raw['canonicalUrl'])
            raw['code'] = campaign[0] if campaign else None
            raw['baseline'] = (bool(row['baseline']) if row['baselineType'] in ('true', 'false') else None)
            raw['targets'] = {name: {
                'status': row[name + '_status'],
                'imageConfirmed': (bool(row[name + '_imageConfirmed'])
                                   if row[name + '_imageType'] in ('true', 'false') else None)
            } for name in TARGETS}
            if not row['hasCoverage']:
                raw['excludedReason'] = 'coverage_metadata_missing'
                incomplete = True
            published = _stamp(raw['instagramPublishedAt'], captured)
            coverage_published = _stamp(row['coveragePublished'], captured)
            bad_coverage = row['hasCoverage'] and (
                row['baselineType'] not in ('true', 'false')
                or not raw['instagramFirstSeenAt'] or not coverage_published
                or published != coverage_published)
            record = None if bad_coverage else _record(raw, captured)
            if record is None:
                result['invalidRecordCount'] += 1
            else:
                result['records'].append(record)
        if incomplete or result['invalidRecordCount'] or result['truncated']:
            result['status'] = 'partial'
        result['records'] = _dedupe(result['records'])
        return result
    except FileNotFoundError:
        result['reason'] = 'database_missing'
    except (OSError, ValueError, sqlite3.Error):
        result['reason'] = 'database_unreadable'
    return result


def _key(record):
    return (record['canonicalUrl'], record['instagramPublishedAt'],
            record['instagramFirstSeenAt'] or '')


def _combine(previous, current):
    merged = dict(current)
    merged['targets'] = {}
    for name in TARGETS:
        old, new = previous['targets'][name], current['targets'][name]
        # A later pending/unknown snapshot cannot erase a recorded confirmation.
        merged['targets'][name] = {
            'status': 'confirmed' if old['status'] == 'confirmed' else new['status'],
            'imageConfirmed': True if old['imageConfirmed'] is True else (
                new['imageConfirmed'] if new['imageConfirmed'] is not None else old['imageConfirmed'])}
    if previous['outboxStatus'] == 'delivered':
        merged['outboxStatus'] = 'delivered'
    return merged


def _dedupe(records):
    by_key = {}
    for record in records:
        key = _key(record)
        by_key[key] = _combine(by_key[key], record) if key in by_key else record
    return [by_key[key] for key in sorted(by_key)]


def merge_story_history(existing, exported, now=None):
    """Keep distinct public observation times and confirmed delivery evidence."""
    wall_clock = _now(now)
    if (not isinstance(exported, dict) or type(exported.get('schemaVersion')) is not int
            or exported['schemaVersion'] != 1):
        raise ValueError('invalid_story_export')
    captured = _stamp(exported.get('capturedAt'), wall_clock)
    status = exported.get('status')
    source_records = exported.get('records')
    invalid_count = exported.get('invalidRecordCount')
    truncated = exported.get('truncated')
    if (captured is None or not isinstance(status, str)
            or status not in {'available', 'partial', 'unavailable'}
            or not isinstance(source_records, list) or len(source_records) > MAX_ROWS
            or (invalid_count is not None and (type(invalid_count) is not int or invalid_count < 0))
            or (truncated is not None and type(truncated) is not bool)
            or (status != 'unavailable' and (invalid_count is None or truncated is None))):
        raise ValueError('invalid_story_export')
    existing = existing if isinstance(existing, dict) else {}
    prior_capture = _stamp(existing.get('capturedAt'), wall_clock)
    if existing.get('capturedAt') is not None and prior_capture is None:
        raise ValueError('invalid_story_history')
    cutoff = max(captured, prior_capture) if prior_capture else captured
    prior_records = existing.get('records', [])
    discarded = 0
    if not isinstance(prior_records, list):
        prior_records, discarded = [], 1
    old = [_record(value, cutoff) for value in prior_records]
    new = [_record(value, captured) for value in source_records]
    discarded += sum(record is None for record in old + new)
    # A stale export cannot replace newer statuses; both retain distinct history.
    records = new + old if prior_capture and prior_capture > captured else old + new
    result = {'schemaVersion': 1, 'capturedAt': _iso(cutoff),
              'status': 'partial' if discarded else status,
              'invalidRecordCount': (invalid_count or 0) + discarded
              if discarded or invalid_count is not None else None,
              'truncated': truncated,
              'records': _dedupe([record for record in records if record is not None])}
    if exported.get('reason') in {'private_database_required', 'database_missing', 'database_unreadable'}:
        result['reason'] = exported['reason']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    args = parser.parse_args()
    result = export_story_metadata(args.database)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 1 if result['status'] == 'unavailable' else 0


if __name__ == '__main__':
    sys.exit(main())
