"""Account-wide Cloudflare analytics; isolated from Instagram collection/delivery."""
import datetime as dt
import fcntl
import json
import math
import os
from pathlib import Path
import re
import stat
import time
import urllib.request

from trial import atomic_json

INTERVAL = 900
FRESH_SECONDS = 1800
ACCOUNT_LIMIT = 10
DETAIL_LIMIT = 1000
LIMITS = {'rowsRead': 5_000_000, 'rowsWritten': 100_000, 'workersRequests': 100_000,
          'doRequests': 100_000, 'duration': 13_000, 'storageBytes': 5_000_000_000}
WRITE_TARGET = 70_000
GRAPHQL_URL = 'https://api.cloudflare.com/client/v4/graphql'
WORKER_ORIGIN = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev'
QUERY = '''query($account: String!, $start: DateTime!, $end: DateTime!) {
  viewer { accounts(filter: {accountTag: $account}) {
    durableObjectsPeriodicGroups(limit: 10,
      filter: {datetime_geq: $start, datetime_lt: $end}) {
      dimensions { date } sum { rowsRead rowsWritten duration }
    }
  } }
}'''
DETAIL_QUERY = '''query($account: String!, $start: DateTime!, $end: DateTime!) {
  viewer { accounts(filter: {accountTag: $account}) {
    projectUsage: durableObjectsPeriodicGroups(limit: 1000,
      filter: {datetime_geq: $start, datetime_lt: $end}) {
      dimensions { date namespaceId name }
      sum { rowsRead rowsWritten duration activeTime inboundWebsocketMsgCount }
    }
    workersUsage: workersInvocationsAdaptive(limit: 10,
      filter: {datetime_geq: $start, datetime_lt: $end}) {
      dimensions { date } sum { requests }
    }
    workersProjects: workersInvocationsAdaptive(limit: 1000,
      filter: {datetime_geq: $start, datetime_lt: $end}) {
      dimensions { date scriptName } sum { requests }
    }
    doUsage: durableObjectsInvocationsAdaptiveGroups(limit: 1000,
      filter: {datetime_geq: $start, datetime_lt: $end}) {
      dimensions { date namespaceId scriptName type } sum { requests }
    }
    storageUsage: durableObjectsStorageGroups(limit: 10,
      filter: {datetime_geq: $start, datetime_lt: $end}) {
      dimensions { date } max { storedBytes }
    }
  } }
}'''


def private_text(path):
    path = Path(path)
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or path.is_symlink()
            or info.st_mode & 0o077 or info.st_uid != os.getuid()):
        raise ValueError('private_file_unsafe')
    return path.read_text().strip()


def request_json(url, token, payload):
    request = urllib.request.Request(url, data=json.dumps(payload).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
                 'Accept': 'application/json', 'User-Agent': 'UOLStorageObserver/1.0'})
    # A redirect must never forward an analytics or ingestion credential.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    with urllib.request.build_opener(NoRedirect).open(request, timeout=20) as response:
        body = response.read(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        raise ValueError('response_too_large')
    return json.loads(body)


def iso(epoch):
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat().replace('+00:00', 'Z')


def metric(value, integral=True):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0
            or (integral and (int(value) != value or value > 2**53 - 1))):
        raise ValueError('analytics_metric_invalid')
    return int(value) if integral else value


def aggregate(payload, day):
    for error in payload.get('errors') or []:
        path = error.get('path')
        if not isinstance(path, list) or 'durableObjectsPeriodicGroups' not in path or path[-1] != 'duration':
            raise ValueError('analytics_query_failed')
    accounts = payload.get('data', {}).get('viewer', {}).get('accounts', [])
    if len(accounts) != 1 or not isinstance(accounts[0].get('durableObjectsPeriodicGroups'), list):
        raise ValueError('analytics_account_missing')
    rows = accounts[0]['durableObjectsPeriodicGroups']
    if len(rows) >= ACCOUNT_LIMIT:
        raise ValueError('analytics_truncated')
    totals = {}
    missing_duration = set()
    for row in rows:
        date = row.get('dimensions', {}).get('date')
        if not isinstance(date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date) or date > day:
            raise ValueError('analytics_day_invalid')
        sums = row.get('sum', {})
        target = totals.setdefault(date, {'rowsRead': 0, 'rowsWritten': 0, 'duration': 0})
        for key in ('rowsRead', 'rowsWritten'):
            target[key] += metric(sums.get(key))
        try:
            target['duration'] += metric(sums.get('duration'), integral=False)
        except ValueError:
            missing_duration.add(date)
    for date in missing_duration:
        totals[date].pop('duration', None)
    # A missing current-day group is not evidence of zero usage. Keep the
    # prior sample until analytics supplies an explicit group for this day.
    if day not in totals:
        raise ValueError('analytics_current_day_missing')
    return totals[day], totals


def detail_rows(payload, alias, day, limit, through_day=None):
    # GraphQL may return valid data alongside an error in a different alias.
    for error in payload.get('errors') or []:
        path = error.get('path')
        if not isinstance(path, list) or alias in path or len(path) < 4:
            raise ValueError('analytics_query_failed')
    accounts = payload.get('data', {}).get('viewer', {}).get('accounts', [])
    if len(accounts) != 1 or not isinstance(accounts[0].get(alias), list):
        raise ValueError('analytics_account_missing')
    rows = accounts[0][alias]
    if len(rows) >= limit:
        raise ValueError('analytics_truncated')
    for row in rows:
        date = row.get('dimensions', {}).get('date')
        if not isinstance(date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date) or date > (through_day or day):
            raise ValueError('analytics_day_invalid')
    current = [row for row in rows if row['dimensions']['date'] == day]
    if not current:
        raise ValueError('analytics_current_day_missing')
    return current


def aggregate_details(payload, day, through_day=None):
    result = {'values': {}, 'status': {}, 'namespaces': None, 'doRequestTypes': None}
    for alias, key, limit in [('projectUsage', 'namespaces', DETAIL_LIMIT),
                              ('workersUsage', 'workersRequests', ACCOUNT_LIMIT),
                              ('doUsage', 'doRequests', DETAIL_LIMIT),
                              ('storageUsage', 'storageBytes', ACCOUNT_LIMIT)]:
        try:
            rows = detail_rows(payload, alias, day, limit, through_day)
            if alias == 'projectUsage':
                namespaces = {}
                for row in rows:
                    dimensions, sums = row['dimensions'], row.get('sum', {})
                    namespace, name = dimensions.get('namespaceId'), dimensions.get('name')
                    if not isinstance(namespace, str) or not namespace or len(namespace) > 128 or not isinstance(name, str) or len(name) > 256:
                        raise ValueError('analytics_namespace_invalid')
                    target = namespaces.setdefault(namespace, {'names': [], 'rowsRead': 0, 'rowsWritten': 0,
                                                               'duration': 0, 'activeTime': 0, 'inboundWebsocketMsgCount': 0})
                    if name not in target['names']:
                        target['names'].append(name)
                    for field in ('rowsRead', 'rowsWritten', 'duration', 'activeTime', 'inboundWebsocketMsgCount'):
                        target[field] += metric(sums.get(field), integral=field != 'duration')
                result['namespaces'] = namespaces
            elif alias == 'doUsage':
                types = {}
                for row in rows:
                    kind = row['dimensions'].get('type')
                    if not isinstance(kind, str) or not kind or len(kind) > 128:
                        raise ValueError('analytics_request_type_invalid')
                    types[kind] = types.get(kind, 0) + metric(row.get('sum', {}).get('requests'))
                result['doRequestTypes'] = types
                result['values']['doRequestsRaw'] = sum(types.values())
                # The observed HTTP/RPC/alarm types count as requests. Unknown
                # types, including WebSocket messages, need a verified billing
                # conversion before joining the request-budget comparison.
                if set(types) <= {'http', 'jsrpc', 'alarm'}:
                    result['values'][key] = sum(types.values())
                else:
                    result['status'][key] = 'billing_unknown'
                    continue
            elif alias == 'workersUsage':
                result['values'][key] = sum(metric(row.get('sum', {}).get('requests')) for row in rows)
            else:
                result['values'][key] = max(metric(row.get('max', {}).get('storedBytes')) for row in rows)
            result['status'][key] = 'known'
        except (ValueError, TypeError, KeyError, AttributeError):
            result['status'][key] = 'unknown'
    projects, mapping, do_rows = {}, {}, []
    try:
        do_rows = detail_rows(payload, 'doUsage', day, DETAIL_LIMIT, through_day)
        candidates = {}
        for row in do_rows:
            dimensions = row['dimensions']
            namespace, script = dimensions.get('namespaceId'), dimensions.get('scriptName')
            if not isinstance(namespace, str) or not namespace or len(namespace) > 128 or not isinstance(script, str) or not script or len(script) > 256:
                raise ValueError('analytics_project_invalid')
            candidates.setdefault(namespace, set()).add(script)
        mapping = {namespace: next(iter(scripts)) if len(scripts) == 1 else None
                   for namespace, scripts in candidates.items()}
        result['status']['projectMapping'] = 'known' if all(mapping.values()) else 'ambiguous'
    except (ValueError, TypeError, KeyError, AttributeError):
        result['status']['projectMapping'] = 'unknown'
    unassigned = []
    for namespace, usage in (result['namespaces'] or {}).items():
        script = mapping.get(namespace)
        mapping.setdefault(namespace, None)
        usage['project'] = script
        if script is None:
            unassigned.append(namespace)
            continue
        target = projects.setdefault(script, {'namespaces': []})
        target['namespaces'].append(namespace)
        for key in ('rowsRead', 'rowsWritten', 'duration', 'activeTime', 'inboundWebsocketMsgCount'):
            target[key] = target.get(key, 0) + usage[key]
    if unassigned and result['status']['projectMapping'] == 'known':
        result['status']['projectMapping'] = 'partial'
    try:
        by_script = {}
        for row in do_rows:
            script = mapping.get(row['dimensions'].get('namespaceId'))
            kind = row['dimensions'].get('type')
            if not isinstance(kind, str) or not kind or len(kind) > 128:
                raise ValueError('analytics_request_type_invalid')
            requests = metric(row.get('sum', {}).get('requests'))
            if script:
                types = by_script.setdefault(script, {})
                types[kind] = types.get(kind, 0) + requests
        for script, types in by_script.items():
            target = projects.setdefault(script, {'namespaces': []})
            target['doRequestsRaw'] = sum(types.values())
            target['doRequests'] = sum(types.values()) if set(types) <= {'http', 'jsrpc', 'alarm'} else None
    except (ValueError, TypeError, KeyError, AttributeError):
        result['status']['projectMapping'] = 'unknown'
    try:
        rows = detail_rows(payload, 'workersProjects', day, DETAIL_LIMIT, through_day)
        requests = {}
        for row in rows:
            script = row['dimensions'].get('scriptName')
            if not isinstance(script, str) or not script or len(script) > 256:
                raise ValueError('analytics_project_invalid')
            requests[script] = requests.get(script, 0) + metric(row.get('sum', {}).get('requests'))
        for script, count in requests.items():
            projects.setdefault(script, {'namespaces': []})['workersRequests'] = count
        result['status']['workersProjects'] = 'known'
    except (ValueError, TypeError, KeyError, AttributeError):
        result['status']['workersProjects'] = 'unknown'
    result.update(projectMapping=mapping, projects=projects, unassignedNamespaces=unassigned)
    result['status']['projects'] = 'known' if (not unassigned and result['status']['namespaces'] == 'known'
        and result['status']['projectMapping'] == 'known' and result['status']['workersProjects'] == 'known') else 'partial' if projects else 'unknown'
    return result


def budget_state(previous, values, now, statuses=None):
    """Local daily guard; only the boolean/reason leave this observer."""
    day = iso(now)[:10]
    previous = previous if previous.get('day') == day else {}
    statuses = statuses or {}
    history = [row for row in previous.get('history', [])
               if now - 4500 <= row['at'] <= now]
    known = dict(previous.get('lastKnown', {}))
    for key in LIMITS:
        if key in values:
            known[key] = {'value': values[key], 'at': now}
    if not history or now > history[-1]['at']:
        history.append({'at': now, 'values': {key: values[key] for key in LIMITS if key in values}})
    elif history[-1]['at'] == now:
        # Same observation must not advance hysteresis counters.
        return previous
    midnight = dt.datetime.fromtimestamp(now, dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    elapsed = now - midnight
    metrics, warnings, actual = {}, [], list(previous.get('actualLatched', []))
    forecasts = []
    for key, limit in LIMITS.items():
        last = known.get(key)
        fresh = bool(last and 0 <= now - last['at'] <= FRESH_SECONDS)
        value = last['value'] if last else None
        status = 'known' if key in values else statuses.get(key, 'unknown' if not last else 'stale')
        item = {'value': value, 'limit': limit, 'status': status,
                'observedAt': iso(last['at']) if last else None, 'forecast': None}
        if fresh:
            item['ratio'] = value / limit
            if value >= limit * .7:
                warnings.append(key)
            if value >= limit * .8 and key not in actual:
                actual.append(key)
        # Storage is a gauge, not a daily counter; no linear daily forecast.
        if key in values and key != 'storageBytes' and elapsed >= 3600:
            samples = [row for row in history if key in row['values']]
            baselines = [row for row in samples if 3600 <= now - row['at'] <= 4500]
            if baselines:
                base = baselines[-1]
                window = [row for row in samples if row['at'] >= base['at']]
                continuous = all(0 < b['at'] - a['at'] <= FRESH_SECONDS and
                                 b['values'][key] >= a['values'][key] for a, b in zip(window, window[1:]))
                if continuous:
                    rate = max((values[key] - base['values'][key]) / (now - base['at']), values[key] / elapsed)
                    item['forecast'] = values[key] + rate * (86400 - elapsed)
                    forecasts.append(item['forecast'] / limit)
        metrics[key] = item
    stale = any(key not in known or now - known[key]['at'] > FRESH_SECONDS
                for key in ('rowsRead', 'rowsWritten'))
    high = bool(forecasts and max(forecasts) > .85)
    # Missing forecast for any previously projected metric cannot release a guard.
    projected = set(previous.get('forecastRequired', [])) | {key for key, value in metrics.items() if value['forecast'] is not None}
    low = bool(forecasts and max(forecasts) < .75 and all(metrics[key]['forecast'] is not None for key in projected))
    high_count = previous.get('forecastHighSamples', 0) + 1 if high else 0
    low_count = previous.get('forecastLowSamples', 0) + 1 if low else 0
    deferred = previous.get('forecastDeferred', False)
    if high_count >= 2:
        deferred = True
    elif low_count >= 2:
        deferred = False
    reason = 'quota_actual' if actual else 'quota_metrics_stale' if stale else 'quota_forecast' if deferred else 'none'
    return {'day': day, 'observedAt': iso(now), 'writeTarget': WRITE_TARGET,
            'optionalWorkDeferred': reason != 'none', 'optionalWorkReason': reason,
            'actualLatched': actual, 'forecastDeferred': deferred,
            'forecastRequired': sorted(projected),
            'forecastHighSamples': min(2, high_count), 'forecastLowSamples': min(2, low_count),
            'warnings': warnings, 'metrics': metrics, 'lastKnown': known, 'history': history}


class StorageUsageObserver:
    def __init__(self, config_path, clock=time.time, requester=request_json):
        self.config_path = Path(config_path)
        self.directory = self.config_path.parent
        self.clock, self.requester = clock, requester
        self.state_path = self.directory / 'storage-usage-state.json'
        self.state = json.loads(private_text(self.state_path)) if self.state_path.exists() else {
            'startedAt': iso(clock()), 'nextEpoch': 0, 'attempts': 0, 'failures': 0,
            'lastOutcome': 'not_started', 'lastSample': None, 'dailyTotals': {},
        }

    def run_if_due(self):
        # A second observer/process must not overlap the reserved collection.
        fd = os.open(self.directory / 'storage-usage.lock',
                     os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'r+') as lock:
            info = os.fstat(lock.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
                raise ValueError('private_file_unsafe')
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {'outcome': 'in_progress'}
            if self.state_path.exists():
                self.state = json.loads(private_text(self.state_path))
            return self._run_if_due()

    def _run_if_due(self):
        now = self.clock()
        if now < self.state['nextEpoch']:
            return {'outcome': 'not_due'}
        # Reserve the cadence durably before either network request.
        self.state.update(nextEpoch=now + INTERVAL, attempts=self.state['attempts'] + 1,
                          lastAttemptAt=iso(now))
        atomic_json(self.state_path, self.state)
        stage = 'configuration'
        try:
            config = json.loads(private_text(self.config_path))
            if (not re.fullmatch(r'[a-f0-9]{32}', str(config.get('accountId', '')))
                    or config.get('workerOrigin') != WORKER_ORIGIN):
                raise ValueError('observer_config_invalid')
            for key in ('analyticsTokenFile', 'ingestTokenFile'):
                if not Path(config.get(key, '')).is_absolute():
                    raise ValueError('observer_token_path_invalid')
            analytics = private_text(config['analyticsTokenFile'])
            ingest = private_text(config['ingestTokenFile'])
            if not analytics or not ingest:
                raise ValueError('observer_token_missing')
            instant = dt.datetime.fromtimestamp(now, dt.timezone.utc)
            day = instant.date().isoformat()
            start = dt.datetime.combine(instant.date() - dt.timedelta(days=3),
                                        dt.time(), dt.timezone.utc)
            stage = 'analytics'
            variables = {
                'account': config['accountId'], 'start': start.isoformat().replace('+00:00', 'Z'),
                'end': iso(now),
            }
            raw = self.requester(GRAPHQL_URL, analytics, {'query': QUERY, 'variables': variables})
            current, totals = aggregate(raw, day)
            previous = self.state.get('lastSample') or {}
            if previous.get('day') == day and (current['rowsRead'] < previous['accountRowsRead'] or
                                               current['rowsWritten'] < previous['accountRowsWritten']):
                raise ValueError('analytics_counter_regressed')
            # Additional analytics never prevent publication of verified reads
            # and writes. At most two GraphQL calls per reserved 15-minute run.
            try:
                detail_raw = self.requester(GRAPHQL_URL, analytics, {'query': DETAIL_QUERY, 'variables': variables})
            except Exception:
                detail_raw = {}
            details = aggregate_details(detail_raw, day)
            daily_details = {(instant.date() - dt.timedelta(days=offset)).isoformat():
                             aggregate_details(detail_raw, (instant.date() - dt.timedelta(days=offset)).isoformat(), day)
                             for offset in range(1, 4)}
            daily_details[day] = details
            sample = {'day': day, 'observedAt': iso(self.clock()),
                      'accountRowsRead': current['rowsRead'],
                      'accountRowsWritten': current['rowsWritten']}
            # An observation spanning midnight belongs to the queried day and is
            # kept locally; ingestion waits for the next reserved collection.
            if self.clock() >= (dt.datetime.combine(instant.date() + dt.timedelta(days=1),
                                                    dt.time(), dt.timezone.utc).timestamp()):
                raise ValueError('utc_day_changed')
            values = {**current, **details['values']}
            statuses = {**details['status'], 'duration': 'known' if 'duration' in current else 'unknown'}
            budget = budget_state(self.state.get('budgetState', {}), values, self.clock(), statuses)
            for key in ('workersRequests', 'doRequests'):
                budget['metrics'][key]['estimated'] = True
            sample.update({key: budget[key] for key in ('optionalWorkDeferred', 'optionalWorkReason')})
            retained = dict(self.state['dailyTotals'])
            for date, total in totals.items():
                retained[date] = {**retained.get(date, {}), **total}
            daily_status = {}
            for date, detail in daily_details.items():
                retained.setdefault(date, {}).update(detail['values'])
                daily_status[date] = {**detail['status'],
                    'rowsRead': 'known' if date in totals else 'unknown',
                    'rowsWritten': 'known' if date in totals else 'unknown',
                    'duration': 'known' if 'duration' in totals.get(date, {}) else 'unknown'}
                detail['observedAt'] = sample['observedAt']
            self.state.update(lastSample=sample, dailyTotals=retained, budgetState=budget,
                              lastDetails={'day': day, **details},
                              dailyDetails={**self.state.get('dailyDetails', {}), **daily_details},
                              dailyMetricStatus={**self.state.get('dailyMetricStatus', {}), **daily_status})
            snapshot_dir = self.directory / 'storage-usage-snapshots'
            snapshot_dir.mkdir(mode=0o700, exist_ok=True)
            snapshot = snapshot_dir / (str(int(now * 1000)) + '.json')
            with snapshot.open('x') as output:
                os.chmod(snapshot, 0o600)
                json.dump({'observedAt': sample['observedAt'],
                           'dailyTotals': {date: retained[date] for date in daily_details},
                           'dailyMetricStatus': daily_status, 'dailyDetails': daily_details,
                           'details': details, 'budgetState': budget}, output)
                output.flush()
                os.fsync(output.fileno())
            atomic_json(self.state_path, self.state)
            stage = 'ingestion'
            receipt = self.requester(WORKER_ORIGIN + '/ingest-storage-usage', ingest, sample)
            if receipt.get('ok') is not True:
                raise ValueError('observer_ingestion_failed')
            self.state.update(lastOutcome='published', lastPublishedAt=sample['observedAt'],
                              lastError='', budget=receipt.get('budget', {}))
        except Exception:
            # Never emit HTTP bodies, tokens, account identifiers or raw errors.
            self.state.update(lastOutcome='failed', lastError=stage + '_unavailable',
                              failures=self.state['failures'] + 1)
            if stage != 'ingestion':
                self.state['budgetState'] = budget_state(self.state.get('budgetState', {}), {}, self.clock())
        atomic_json(self.state_path, self.state)
        return {'outcome': self.state['lastOutcome'], 'error': self.state.get('lastError', '')}
