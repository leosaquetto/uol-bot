"""Bounded, read-only Instagram stability trial; no notification or redemption."""
import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import random
import resource
import sqlite3
import time

from instagram import InstagramClient, SessionError

PERIOD = 120
HOURS = 72
MAX_CYCLES = 2160
MAX_REQUESTS = 4320
TERMINAL = {'completed', 'auth_required', 'rate_limited', 'suspended', 'budget_exhausted'}


def timestamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def atomic_json(path, value):
    tmp = path.with_suffix('.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class Trial:
    def __init__(self, directory, clock=time.time):
        self.directory = Path(directory)
        self.clock = clock
        self.db = sqlite3.connect(self.directory / 'history.sqlite')
        self.db.executescript('''
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS observations (cycle INTEGER PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS stories (id TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, cycle INTEGER, kind TEXT, value TEXT);
        ''')
        row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone()
        self.state = json.loads(row[0]) if row else {
            'startedAt': timestamp(), 'startedEpoch': clock(), 'endEpoch': clock() + HOURS * 3600,
            'status': 'running', 'cycles': 0, 'requests': 0, 'successes': 0,
            'attempts': 0, 'requestsReserved': 0,
            'consecutiveFailures': 0, 'newStories': 0, 'changedStories': 0,
            'baselineEstablished': False, 'baselineIds': [], 'nextEpoch': 0,
            'notificationsEnabled': False, 'redemptionEnabled': False,
        }

    def save(self):
        self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)', (json.dumps(self.state),))
        self.db.commit()
        atomic_json(self.directory / 'status.json', self.state)

    def gate(self):
        s = self.state
        if s['status'] in TERMINAL:
            return s['status']
        if self.clock() >= s['endEpoch']:
            s['status'] = 'completed'
            s['completedAt'] = timestamp()
            self.save()
            return 'completed'
        if s['attempts'] >= MAX_CYCLES or s['requestsReserved'] + 2 > MAX_REQUESTS:
            s['status'] = 'budget_exhausted'
            self.save()
            return s['status']
        return 'due' if self.clock() >= s['nextEpoch'] else 'waiting'

    def reserve(self):
        # A crash keeps this reservation and the minimum interval. Restart cannot
        # bypass the network budget or immediately repeat the unfinished request.
        self.state['attempts'] += 1
        self.state['requestsReserved'] += 2
        self.state['nextEpoch'] = self.clock() + PERIOD
        self.save()

    def record(self, result):
        s = self.state
        s['cycles'] += 1
        s['requests'] += result['requests']
        s['lastObservationAt'] = result['checkedAt']
        s['lastResult'] = {k: result.get(k) for k in
                           ('status', 'reason', 'requests', 'duration_ms', 'body_bytes')}
        # ru_maxrss is KiB on Linux, bytes on macOS; keep the platform in evidence.
        s['maxRssNative'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        status = result['status']
        if status in {'found', 'empty'}:
            baseline = not s['baselineEstablished']
            observed = []
            for story in result['stories']:
                observed.append(story['storyId'])
                row = self.db.execute('SELECT value FROM stories WHERE id=?', (story['storyId'],)).fetchone()
                old = json.loads(row[0]) if row else None
                if old is None:
                    entry = {**story, 'firstObservedAt': result['checkedAt'],
                             'lastObservedAt': result['checkedAt'], 'baseline': baseline}
                    if not baseline:
                        self.event('new_story', story)
                        s['newStories'] += 1
                else:
                    entry = {**old, **story, 'lastObservedAt': result['checkedAt']}
                    if old['destinations'] != story['destinations']:
                        self.event('changed_destination', story)
                        s['changedStories'] += 1
                self.db.execute('INSERT OR REPLACE INTO stories VALUES(?,?)',
                                (story['storyId'], json.dumps(entry)))
            if baseline:
                s['baselineIds'] = observed
            s['baselineEstablished'] = True
            s['successes'] += 1
            s['consecutiveFailures'] = 0
            s['lastSuccessAt'] = result['checkedAt']
            s['currentIds'] = observed
            s['nextEpoch'] = self.clock() + PERIOD + random.uniform(0, 10)
        else:
            s['consecutiveFailures'] += 1
            if status in {'auth_required', 'rate_limited'}:
                s['status'] = status
            elif result.get('reason') == 'session_write_failed':
                s['status'] = 'suspended'
            elif s['consecutiveFailures'] >= 6:
                s['status'] = 'suspended'
            s['nextEpoch'] = self.clock() + min(3600, PERIOD * 2 ** s['consecutiveFailures'])
            self.event('collection_failure', s['lastResult'])
        self.db.execute('INSERT INTO observations VALUES(?,?)', (s['cycles'], json.dumps(result)))
        self.save()

    def event(self, kind, value):
        self.db.execute('INSERT INTO events(cycle,kind,value) VALUES(?,?,?)',
                        (self.state['cycles'], kind, json.dumps(value)))


def run(directory, session, once=False):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise ValueError('private_data_directory_required')
    with open(directory / 'trial.lock', 'a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        trial = Trial(directory)
        trial.save()
        while True:
            gate = trial.gate()
            if gate in TERMINAL or (once and gate == 'waiting'):
                return trial.state
            if gate == 'waiting':
                time.sleep(max(0, min(30, trial.state['nextEpoch'] - time.time(),
                                      trial.state['endEpoch'] - time.time())))
                continue
            trial.reserve()
            try:
                result = InstagramClient(session).collect()
            except SessionError as error:
                result = {'checkedAt': timestamp(), 'status': 'auth_required',
                          'reason': error.reason, 'requests': 0,
                          'duration_ms': 0, 'body_bytes': 0, 'stories': []}
            trial.record(result)
            if once or trial.state['status'] in TERMINAL:
                return trial.state


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data-dir', type=Path, required=True)
    p.add_argument('--session-file', type=Path, required=True)
    p.add_argument('--once', action='store_true')
    args = p.parse_args()
    try:
        state = run(args.data_dir, args.session_file, args.once)
        print(json.dumps({k: state.get(k) for k in
                          ('status', 'cycles', 'successes', 'requests', 'newStories', 'lastResult')}))
    except BlockingIOError:
        print(json.dumps({'status': 'locked'}))
    except Exception as e:
        # Never include exception messages, filenames or response bodies in logs.
        print(json.dumps({'status': 'failed', 'reason': type(e).__name__}))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
