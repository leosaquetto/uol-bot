"""Account-wide Cloudflare analytics; isolated from Instagram collection/delivery."""
import datetime as dt
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
    if payload.get('errors'):
        raise ValueError('analytics_query_failed')
    accounts = payload.get('data', {}).get('viewer', {}).get('accounts', [])
    if len(accounts) != 1 or not isinstance(accounts[0].get('durableObjectsPeriodicGroups'), list):
        raise ValueError('analytics_account_missing')
    totals = {}
    for row in accounts[0]['durableObjectsPeriodicGroups']:
        date = row.get('dimensions', {}).get('date')
        if not isinstance(date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date) or date > day:
            raise ValueError('analytics_day_invalid')
        sums = row.get('sum', {})
        target = totals.setdefault(date, {'rowsRead': 0, 'rowsWritten': 0, 'duration': 0})
        for key in target:
            target[key] += metric(sums.get(key), integral=key != 'duration')
    # A missing current-day group is not evidence of zero usage. Keep the
    # prior sample until analytics supplies an explicit group for this day.
    if day not in totals:
        raise ValueError('analytics_current_day_missing')
    return totals[day], totals


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
            raw = self.requester(GRAPHQL_URL, analytics, {'query': QUERY, 'variables': {
                'account': config['accountId'], 'start': start.isoformat().replace('+00:00', 'Z'),
                'end': iso(now),
            }})
            current, totals = aggregate(raw, day)
            sample = {'day': day, 'observedAt': iso(self.clock()),
                      'accountRowsRead': current['rowsRead'],
                      'accountRowsWritten': current['rowsWritten']}
            # An observation spanning midnight belongs to the queried day and is
            # kept locally; ingestion waits for the next reserved collection.
            if self.clock() >= (dt.datetime.combine(instant.date() + dt.timedelta(days=1),
                                                    dt.time(), dt.timezone.utc).timestamp()):
                raise ValueError('utc_day_changed')
            self.state.update(lastSample=sample, dailyTotals={**self.state['dailyTotals'], **totals})
            snapshot_dir = self.directory / 'storage-usage-snapshots'
            snapshot_dir.mkdir(mode=0o700, exist_ok=True)
            snapshot = snapshot_dir / (str(int(now * 1000)) + '.json')
            with snapshot.open('x') as output:
                os.chmod(snapshot, 0o600)
                json.dump({'observedAt': sample['observedAt'], 'dailyTotals': totals}, output)
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
        atomic_json(self.state_path, self.state)
        return {'outcome': self.state['lastOutcome'], 'error': self.state.get('lastError', '')}
