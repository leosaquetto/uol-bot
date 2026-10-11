"""Read-only, offline UTC-day quota evidence from observer snapshots."""
import argparse
import datetime as dt
import json
import math
from pathlib import Path
import re

UTC = dt.timezone.utc
MAX_GAP_SECONDS = 1800
WRITE_TARGET = 70_000
SQL_STORAGE_SOURCE = 'durableObjectsSqlStorageGroups.max.storedBytes'
LIMITS = {'rowsRead': 5_000_000, 'rowsWritten': 100_000,
          'workersRequests': 100_000, 'doRequests': 100_000,
          'duration': 13_000, 'storageBytes': 5_000_000_000}
STATUSES = {'known', 'unknown', 'stale', 'billing_unknown', 'partial'}
PROJECT_METRICS = (*LIMITS, 'doRequestsRaw', 'activeTime', 'inboundWebsocketMsgCount')


def utc_day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('invalid_day')
    return dt.date.fromisoformat(value)


def instant(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)', value):
        raise ValueError('invalid_timestamp')
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))


def iso(value):
    return value.isoformat().replace('+00:00', 'Z') if value else None


def number(value, key):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0
            or (key != 'duration' and (int(value) != value or value > 2**53 - 1))):
        raise ValueError('invalid_metric')
    return value


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def day_metric(record, day, key):
    if record is None:
        return {'status': 'unknown', 'value': None, 'observedAt': None}
    raw, observed = record
    status = raw['dailyMetricStatus'].get(day, {}).get(key, 'unknown')
    value = raw['dailyTotals'].get(day, {}).get(key)
    if key == 'storageBytes' and status == 'known':
        details = raw.get('dailyDetails', {}).get(day)
        if details is None and raw['budgetState']['day'] == day:
            details = raw.get('details')
        storage = details.get('storageMetric', {}) if isinstance(details, dict) else {}
        if not isinstance(storage, dict) or storage.get('source') != SQL_STORAGE_SOURCE:
            status = 'unknown'
    return {'status': status, 'value': value if status == 'known' else None,
            'observedAt': iso(observed) if status == 'known' else None}


def read_records(state_dir, day, now):
    records, conflicts = {}, set()
    invalid, duplicates = 0, 0
    start = dt.datetime.combine(utc_day(day), dt.time(), UTC)
    directory = Path(state_dir) / 'storage-usage-snapshots'
    try:
        paths = sorted(directory.glob('*.json'))
    except OSError:
        return [], 1, 0
    for path in paths:
        try:
            raw = json.loads(path.read_text(), object_pairs_hook=unique_object)
            observed = instant(raw['observedAt'])
            if observed > now:
                raise ValueError('future_observation')
            # A proven earlier timestamp cannot cover or confirm this day.
            # Ignore legacy schema only after validating its UTC timestamp.
            if observed < start:
                continue
            budget_day = raw['budgetState']['day']
            if utc_day(budget_day) != observed.date():
                raise ValueError('invalid_observation_day')
            for field in ('dailyTotals', 'dailyMetricStatus'):
                if not isinstance(raw[field], dict):
                    raise ValueError('invalid_daily_data')
            for date in {day, budget_day}:
                if date == budget_day and (date not in raw['dailyTotals']
                                          or date not in raw['dailyMetricStatus']):
                    raise ValueError('missing_current_day')
                totals = raw['dailyTotals'].get(date, {})
                statuses = raw['dailyMetricStatus'].get(date, {})
                if not isinstance(totals, dict) or not isinstance(statuses, dict):
                    raise ValueError('invalid_daily_data')
                for key in LIMITS:
                    status = statuses.get(key, 'unknown')
                    if status not in STATUSES:
                        raise ValueError('invalid_metric_status')
                    if key in totals:
                        number(totals[key], key)
                    if status == 'known' and key not in totals:
                        raise ValueError('known_metric_missing')
            daily_details = raw.get('dailyDetails', {})
            if not isinstance(daily_details, dict):
                raise ValueError('invalid_daily_details')
            for detail in [daily_details.get(day), daily_details.get(budget_day), raw.get('details')]:
                if detail is None:
                    continue
                if not isinstance(detail, dict) or not isinstance(detail.get('status', {}), dict):
                    raise ValueError('invalid_details')
                projects = detail.get('projects', {})
                if not isinstance(projects, dict):
                    raise ValueError('invalid_projects')
                for name, values in projects.items():
                    if (not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,255}', name)
                            or not isinstance(values, dict)):
                        raise ValueError('invalid_project')
                    for key in PROJECT_METRICS:
                        if key in values and not (key == 'doRequests' and values[key] is None):
                            number(values[key], key)
            # Identical files are one observation; contradictory duplicates
            # remove both observations rather than inventing an order.
            if observed in conflicts:
                invalid += 1
            elif observed in records:
                if raw == records[observed][0]:
                    duplicates += 1
                else:
                    records.pop(observed)
                    conflicts.add(observed)
                    invalid += 2
            else:
                records[observed] = (raw, observed)
        except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError):
            invalid += 1
    return sorted(records.values(), key=lambda item: item[1]), invalid, duplicates


def project_report(record, day):
    if record is None:
        return {'status': 'unknown', 'observedAt': None, 'items': []}
    raw, observed = record
    details = raw.get('dailyDetails', {}).get(day)
    if details is None and raw['budgetState']['day'] == day:
        details = raw.get('details')
    if not isinstance(details, dict) or not isinstance(details.get('projects'), dict):
        return {'status': 'unknown', 'observedAt': iso(observed), 'items': []}
    statuses = details.get('status', {})
    if not isinstance(statuses, dict):
        return {'status': 'unknown', 'observedAt': iso(observed), 'items': []}
    status = statuses.get('projects', 'unknown')
    status = status if status in STATUSES else 'unknown'
    items = []
    for name, values in sorted(details['projects'].items(), key=lambda item: str(item[0])):
        if (not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,255}', name)
                or not isinstance(values, dict)):
            status = 'unknown'
            continue
        metrics = {}
        for key in PROJECT_METRICS:
            metric_status = status
            if key == 'workersRequests':
                metric_status = statuses.get('workersProjects', 'unknown')
            elif key == 'storageBytes':
                metric_status = values.get('storageStatus', 'unknown')
                storage = details.get('storageMetric', {})
                if not isinstance(storage, dict) or storage.get('source') != SQL_STORAGE_SOURCE:
                    metric_status = 'unknown'
            elif key in {'doRequests', 'doRequestsRaw'}:
                metric_status = statuses.get('doProjects', 'unknown')
                if key == 'doRequests' and values.get(key) is None and metric_status in {'known', 'partial'}:
                    metric_status = 'billing_unknown'
            if metric_status not in STATUSES:
                metric_status = 'unknown'
            value = None
            if key in values and metric_status in {'known', 'partial'}:
                try:
                    value = number(values[key], key)
                except ValueError:
                    metric_status = 'unknown'
            else:
                metric_status = 'unknown' if metric_status == 'known' else metric_status
            metrics[key] = {'status': metric_status, 'value': value}
        items.append({'name': name, 'metrics': metrics})
    return {'status': status, 'observedAt': iso(observed), 'items': items}


def build_report(state_dir, day, now=None):
    date = utc_day(day)
    now = now or dt.datetime.now(UTC)
    start = dt.datetime.combine(date, dt.time(), UTC)
    end = start + dt.timedelta(days=1)
    records, invalid, duplicates = read_records(state_dir, day, now)
    samples = [row for row in records if row[0]['budgetState']['day'] == day]
    confirmations = [row for row in records if row[1] >= end
                     and (day in row[0]['dailyTotals'] or day in row[0]['dailyMetricStatus'])]
    times = [row[1] for row in samples]
    gaps = [{'after': iso(a), 'before': iso(b), 'seconds': (b - a).total_seconds()}
            for a, b in zip(times, times[1:]) if (b - a).total_seconds() > MAX_GAP_SECONDS]
    leading = (times[0] - start).total_seconds() if times else None
    trailing = (end - times[-1]).total_seconds() if times else None
    coverage = {'dayComplete': now >= end, 'sampleCount': len(samples),
                'firstObservedAt': iso(times[0]) if times else None,
                'lastObservedAt': iso(times[-1]) if times else None,
                'maxGapSeconds': max(((b - a).total_seconds() for a, b in zip(times, times[1:])), default=None),
                'gapsOver1800Seconds': gaps, 'startDistanceSeconds': leading,
                'endDistanceSeconds': trailing, 'invalidRecordCount': invalid,
                'duplicateRecordCount': duplicates}
    sufficient = bool(times and not invalid and not gaps
                      and leading <= MAX_GAP_SECONDS and trailing <= MAX_GAP_SECONDS)
    latest = samples[-1] if samples else None
    confirmation = confirmations[-1] if confirmations else None
    metrics, verified = {}, sufficient and now >= end
    for key, limit in LIMITS.items():
        first = day_metric(samples[0] if samples else None, day, key)
        last = day_metric(latest, day, key)
        closed = day_metric(confirmation, day, key)
        known = [row for row in samples if day_metric(row, day, key)['status'] == 'known']
        continuous = bool(first['status'] == last['status'] == 'known' and known
                          and all((b[1] - a[1]).total_seconds() <= MAX_GAP_SECONDS
                                  for a, b in zip(known, known[1:])))
        monotonic = key == 'storageBytes' or all(
            day_metric(b, day, key)['value'] >= day_metric(a, day, key)['value']
            for a, b in zip(known, known[1:]))
        known_confirmations = [row for row in confirmations if day_metric(row, day, key)['status'] == 'known']
        confirmation_monotonic = key == 'storageBytes' or all(
            day_metric(b, day, key)['value'] >= day_metric(a, day, key)['value']
            for a, b in zip(known_confirmations, known_confirmations[1:]))
        final = closed if confirmation else last
        confirmed = bool(confirmation and closed['status'] == 'known' and confirmation_monotonic
                         and (key == 'storageBytes' or last['value'] is None
                              or closed['value'] >= last['value']))
        metric_verified = bool(sufficient and continuous and monotonic and confirmed and now >= end)
        verified = verified and metric_verified
        item = {'status': final['status'], 'value': final['value'], 'observedAt': final['observedAt'],
                'limit': limit, 'percentOfLimit': final['value'] / limit * 100 if final['value'] is not None else None,
                'latestInDay': last, 'postDayConfirmation': closed,
                'coverageSufficient': continuous and monotonic and sufficient,
                'consumptionVerified': metric_verified}
        if key == 'storageBytes':
            item.update(source=SQL_STORAGE_SOURCE, aggregation='sum_of_daily_namespace_maxima', estimated=True)
        if key == 'rowsWritten':
            item.update(writeTarget=WRITE_TARGET,
                        percentOfWriteTarget=final['value'] / WRITE_TARGET * 100 if final['value'] is not None else None,
                        writeTargetMet=final['value'] < WRITE_TARGET if metric_verified else None)
        metrics[key] = item
    coverage['sufficient'] = bool(sufficient and all(item['coverageSufficient'] for item in metrics.values()))
    status = 'incomplete' if now < end else 'consumption_verified' if verified else 'coverage_insufficient'
    return {'day': day, 'status': status, 'coverage': coverage, 'metrics': metrics,
            'projects': project_report(confirmation or latest, day),
            'evidence': {'snapshotStage': 'before_ingestion', 'workerGuardOrDeliveryVerified': False,
                         'totalAcceptanceVerified': False,
                         'separateEvidenceRequired': ['worker_guard_ingestion', 'push', 'latency', 'duplicates']}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--day', required=True, type=lambda value: utc_day(value).isoformat())
    args = parser.parse_args()
    print(json.dumps(build_report(args.state_dir, args.day), ensure_ascii=False, allow_nan=False))


if __name__ == '__main__':
    main()
