"""Server-only Instagram complement for the UOL bot; never redeems benefits."""
import argparse
import base64
from concurrent.futures import Future
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import stat
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from instagram import InstagramClient, NoRedirect, SessionError, safe_media_url
from trial import atomic_json, timestamp
from push_signals import PushSignals, proof_key

INGEST_URL = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/ingest-instagram-story'
HEARTBEAT_URL = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/instagram-monitor-heartbeat'
MAX_DAILY_REQUESTS = 1440
PERIOD = 120
SAFETY_PERIOD = 1800
MAX_MEDIA_BYTES = 3 * 1024 * 1024
LOCAL_CHECKPOINT_PERIOD = 60
PUSH_CORRELATION_WAIT = 600


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def clock_timestamp(value):
    return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat()


def ticket_link(link):
    try:
        u = urllib.parse.urlsplit(link)
        if (u.scheme == 'https' and u.hostname == 'clube.uol.com.br'
                and not u.username and not u.password and u.port is None and not u.query and not u.fragment
                and re.fullmatch(r'/campanhasdeingresso/p[A-Za-z0-9]{2,5}-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*', u.path)):
            return 'https://clube.uol.com.br' + u.path
    except (TypeError, ValueError):
        pass
    return ''


def epoch(value):
    return dt.datetime.fromisoformat(value).timestamp()


def private_config(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd) as f:
        info = os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.getuid():
            raise ValueError('private_config_required')
        text = f.read(16385)
    if len(text) > 16384:
        raise ValueError('config_limit')
    config = json.loads(text)
    if config.get('ingestUrl') != INGEST_URL or not re.fullmatch(r'[a-f0-9]{64}', config.get('ingestToken', '')):
        raise ValueError('invalid_config')
    if config.get('pushMode', 'poll') not in ('poll', 'pilot', 'event'):
        raise ValueError('invalid_push_mode')
    return config


def ingest(config, payload):
    """Retry this idempotent intake, never a destination send API."""
    encoded = json.dumps(payload).encode()
    if len(encoded) > 5 * 1024 * 1024:
        return {'ok': False, 'status': 'held', 'error': 'payload_limit'}
    request = urllib.request.Request(INGEST_URL, data=encoded, method='POST', headers={
        'Authorization': 'Bearer ' + config['ingestToken'], 'Content-Type': 'application/json',
        'Accept': 'application/json', 'User-Agent': 'UOLInstagramMonitor/1.0',
    })
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=110) as r:
            raw = r.read(32769)
            if len(raw) > 32768:
                return {'ok': False, 'status': 'pending', 'error': 'response_limit'}
            result = json.loads(raw)
            # Keep only the known sanitized receipt contract.
            return {k: result.get(k) for k in ('ok', 'status', 'storyId', 'canonicalKey',
                                               'link', 'targets', 'updatedAt', 'error')}
    except urllib.error.HTTPError as e:
        try:
            return {'ok': False, 'status': 'held' if e.code in (400, 401, 403) else 'pending',
                    'error': 'ingest_http_' + str(e.code)}
        finally:
            e.close()
    except Exception:
        return {'ok': False, 'status': 'pending', 'error': 'ingest_response_uncertain'}


def fetch_media(payload, user_agent):
    url = safe_media_url(payload.get('imageUrl'))
    if not url:
        return None
    req = urllib.request.Request(url, method='GET', headers={
        'User-Agent': user_agent, 'Accept': 'image/jpeg,image/webp',
    })
    try:
        with urllib.request.build_opener(NoRedirect()).open(req, timeout=15) as r:
            mime = r.headers.get('Content-Type', '').split(';')[0].lower()
            if mime not in ('image/jpeg','image/webp'):
                return None
            data = r.read(MAX_MEDIA_BYTES+1)
            valid = data[:3] == bytes([255,216,255]) if mime == 'image/jpeg' else data[:4] == b'RIFF' and data[8:12] == b'WEBP'
            if not valid or len(data) > MAX_MEDIA_BYTES:
                return None
            return {'imageBase64': base64.b64encode(data).decode('ascii'), 'imageMime': mime}
    except Exception:
        return None


def heartbeat(config, payload):
    request = urllib.request.Request(HEARTBEAT_URL, data=json.dumps(payload).encode(), method='POST', headers={
        'Authorization': 'Bearer ' + config['ingestToken'], 'Content-Type': 'application/json',
        'Accept': 'application/json', 'User-Agent': 'UOLInstagramMonitor/1.0',
    })
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=35) as r:
            raw = r.read(32769)
            return len(raw) <= 32768 and json.loads(raw).get('ok') is True
    except Exception:
        return False


class Monitor:
    def __init__(self, directory, config, clock=time.time):
        self.directory = Path(directory)
        self.clock, self.config = clock, config
        self.push = PushSignals(self.directory)
        self.db = sqlite3.connect(self.directory / 'monitor.sqlite')
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1), value TEXT);
            CREATE TABLE IF NOT EXISTS polls(at REAL, reserved INTEGER, result TEXT);
            CREATE TABLE IF NOT EXISTS media_reads(at REAL);
            CREATE TABLE IF NOT EXISTS outbox(
                key TEXT PRIMARY KEY, story_id TEXT, expires REAL, payload TEXT,
                status TEXT, due REAL, attempts INTEGER, receipt TEXT);
            CREATE TABLE IF NOT EXISTS story_coverage(story_id TEXT PRIMARY KEY, value TEXT);
        ''')
        row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone()
        self.state = json.loads(row[0]) if row else {
            'startedAt': timestamp(), 'cycles': 0, 'requests': 0, 'nextPoll': 0,
            'failures': 0, 'sourceStatus': 'starting', 'lastSuccessAt': None,
            'profile': 'clubeuol', 'redemptionEnabled': False,
        }
        # Upgrade older polling state without letting a pending push bypass its backoff.
        self.state.setdefault('retryNotBefore', 0 if self.state['sourceStatus'] in ('starting','found','empty')
                              else self.state['nextPoll'])
        self._saved_state = canonical(json.loads(row[0])) if row else None
        self._committed_changes = self.db.total_changes
        self._next_checkpoint = 0
        self._next_coverage_refresh = 0
        self._signals = None
        try:
            self._saved_status = canonical(json.loads((self.directory/'status.json').read_text()))
        except (OSError, ValueError):
            self._saved_status = None

    def commit(self):
        if self.db.total_changes != self._committed_changes:
            self.db.commit()
            self._committed_changes = self.db.total_changes
        elif self.db.in_transaction:
            self.db.rollback()

    def save(self):
        self.state['outbox'] = {row[0]: row[1] for row in
                               self.db.execute('SELECT status,count(*) FROM outbox GROUP BY status')}
        self.update_outbox_progress()
        # Liveness uses its small checkpoint file, rather than a SQLite write every tick.
        persisted = {key: value for key, value in self.state.items() if key != 'loopAliveAt'}
        value = canonical(persisted)
        if value != self._saved_state:
            self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)', (value,))
        self.commit()
        self._saved_state = value
        status = canonical(self.state)
        if status != self._saved_status:
            atomic_json(self.directory / 'status.json', self.state)
            self._saved_status = status

    def loop_checkpoint(self):
        now = self.clock()
        if now < self._next_checkpoint:
            return False
        self._next_checkpoint = now + LOCAL_CHECKPOINT_PERIOD
        self.state['loopAliveAt'] = clock_timestamp(now)
        self.update_receiver(now)
        self.update_outbox_progress()
        # This is main-loop progress: blocked collection cannot keep it falsely alive.
        atomic_json(self.directory/'loop-heartbeat.json', {
            'loopAliveAt': self.state['loopAliveAt'],
            'pushReceiverStatus': self.state.get('pushReceiverStatus', 'disabled'),
            'pushReceiver': self.state.get('pushReceiver', {}),
            'outboxProgress': self.state['outboxProgress'],
        })
        return True

    def update_outbox_progress(self):
        rows = self.db.execute("SELECT story_id,payload,due FROM outbox WHERE status NOT IN ('delivered','expired','unknown')").fetchall()
        oldest = []
        for story_id, text, due in rows:
            coverage = self.db.execute('SELECT value FROM story_coverage WHERE story_id=?', (story_id,)).fetchone()
            if coverage:
                oldest.append(json.loads(coverage[0])['firstSeenEpoch'])
            else:
                try:
                    oldest.append(epoch(json.loads(text)['publishedAt']))
                except (KeyError, ValueError, TypeError):
                    pass
        self.state['outboxProgress'] = {'pending': len(rows),
            'oldestPendingEpoch': min(oldest) if oldest else None,
            'oldestPendingAt': clock_timestamp(min(oldest)) if oldest else None,
            'lastProgressAt': self.state.get('lastOutboxProgressAt'),
            'nextRetryAt': clock_timestamp(min(row[2] for row in rows)) if rows else None}

    def update_receiver(self, now):
        push = self.push.snapshot(now) if self.config.get('pushMode', 'poll') != 'poll' else {
            'sequence': 0, 'connected': False, 'status': 'disabled', 'fresh': False, 'observedAt': None}
        self.state['pushReceiverStatus'] = push['status']
        self.state['pushReceiver'] = {key: push[key] for key in ('status','connected','fresh','observedAt')}
        if not push['connected']:
            self.state['coverageWindowHasGap'] = True
        self.refresh_coverage(push)
        return push

    def write_coverage(self, story_id, value, previous=None):
        encoded = canonical(value)
        if previous != encoded:
            self.db.execute('INSERT INTO story_coverage VALUES(?,?) ON CONFLICT(story_id) DO UPDATE SET value=excluded.value',
                            (story_id, encoded))

    def refresh_coverage(self, push=None, force=False):
        now = self.clock()
        if push is None:
            push = self.push.snapshot(now) if self.config.get('pushMode', 'poll') != 'poll' else {
                'sequence': 0, 'connected': False, 'status': 'disabled', 'fresh': False}
        sequence = push['sequence']
        changed = sequence != self.state.get('coverageSignalSequence')
        if not force and not changed and now < self._next_coverage_refresh:
            return
        self._next_coverage_refresh = now + LOCAL_CHECKPOINT_PERIOD
        if force or changed or self._signals is None:
            self._signals = self.push.signals() if self.config.get('pushMode', 'poll') != 'poll' else []
        self.state['coverageSignalSequence'] = sequence
        counts = {'baselineStories': 0, 'excludedStories': 0, 'eligibleStories': 0,
                  'correspondingPushStories': 0, 'awaitingPushStories': 0,
                  'withoutCorrespondingPushStories': 0, 'genericProfileSignalStories': 0}
        exact = {signal['storyId']: signal for signal in reversed(self._signals) if signal['storyId']}
        generic = [signal for signal in self._signals if not signal['storyId'] and signal['receivedEpoch'] is not None]
        for story_id, text in self.db.execute('SELECT story_id,value FROM story_coverage').fetchall():
            entry = json.loads(text)
            # An unsettled correlation window cannot be scored across receiver/auth gaps.
            if (not entry['baseline'] and not entry.get('excludedReason')
                    and entry.get('pushCorrelation') not in ('exact_story_id','without_corresponding_push')):
                if not push['connected']:
                    entry['excludedReason'] = ('receiver_stale' if push['status'] == 'connected'
                                               else 'receiver_' + push['status'])
                elif self.state['sourceStatus'] not in ('found','empty'):
                    entry['excludedReason'] = 'coverage_gap'
            signal = exact.get(story_id)
            if signal:
                entry.update(pushCorrelation='exact_story_id', exactSignalSequence=signal['sequence'],
                             exactSignalReceivedAt=clock_timestamp(signal['receivedEpoch'])
                             if signal['receivedEpoch'] is not None else None)
            hints = [signal for signal in generic
                     if entry['publishedEpoch']-60 <= signal['receivedEpoch'] <= entry['firstSeenEpoch']+PUSH_CORRELATION_WAIT]
            if hints:
                entry['genericProfileSignalSequence'] = hints[0]['sequence']
            if entry.get('genericProfileSignalSequence'):
                counts['genericProfileSignalStories'] += 1
            if entry.get('pushCorrelation') != 'exact_story_id':
                entry['pushCorrelation'] = ('without_corresponding_push' if now-entry['firstSeenEpoch'] >= PUSH_CORRELATION_WAIT
                                            else 'generic_profile_signal_uncertain' if entry.get('genericProfileSignalSequence') else 'awaiting_push')
            self.write_coverage(story_id, entry, text)
            if entry['baseline']:
                counts['baselineStories'] += 1
            elif entry.get('excludedReason'):
                counts['excludedStories'] += 1
            else:
                counts['eligibleStories'] += 1
                name = {'exact_story_id': 'correspondingPushStories',
                        'without_corresponding_push': 'withoutCorrespondingPushStories'}.get(entry['pushCorrelation'], 'awaitingPushStories')
                counts[name] += 1
        counts['successDenominator'] = counts['correspondingPushStories']+counts['withoutCorrespondingPushStories']
        counts['pushCoveragePercent'] = (round(100*counts['correspondingPushStories']/counts['successDenominator'], 2)
                                         if counts['successDenominator'] else None)
        self.state['coverage'] = counts

    def reserve_poll(self):
        now = self.clock()
        push = self.update_receiver(now)
        pending = push['sequence'] > self.state.get('consumedSignalSequence', 0)
        event_due = pending and now >= self.state.get('pushAttemptNotBefore', 0)
        if now < self.state.get('retryNotBefore', 0):
            return False
        # Signals only bypass the normal schedule, never authentication/rate-limit backoff.
        if now < self.state['nextPoll'] and not event_due:
            return False
        used = self.db.execute('SELECT coalesce(sum(reserved),0) FROM polls WHERE at>?', (now-86400,)).fetchone()[0]
        if used + 2 > MAX_DAILY_REQUESTS:
            first = self.db.execute('SELECT min(at) FROM polls WHERE at>?', (now-86400,)).fetchone()[0]
            self.state.update(sourceStatus='budget_wait', nextPoll=first+86401, retryNotBefore=first+86401)
            self.save()
            return False
        self.db.execute('INSERT INTO polls VALUES(?,2,NULL)', (now,))
        self.state['nextPoll'] = now + PERIOD
        self.state['reservedSignalSequence'] = push['sequence'] if pending else 0
        self.state['pushAttemptNotBefore'] = now + 15
        self.state['lastPollTrigger'] = 'push' if event_due else 'safety' if self.event_enabled() else 'poll'
        self.save()
        return True

    def event_enabled(self):
        proof = self.config.get('pushProof')
        if self.config.get('pushMode') != 'event' or not self.push.proves(proof):
            return False
        # Activation needs both the durable server signal and a successful Story snapshot.
        key = proof_key(proof)
        if self.state.get('verifiedPushProof') != key:
            if proof['storyId'] not in self.state.get('currentStoryIds', []):
                return False
            self.state['verifiedPushProof'] = key
        # Once proven, a socket outage keeps the independent 30-minute safety sweep.
        return True

    def record(self, result):
        now, s = self.clock(), self.state
        s['cycles'] += 1
        s['requests'] += result.get('requests', 0)
        s['lastObservationAt'] = result['checkedAt']
        s['lastResult'] = {k: result.get(k) for k in ('status', 'reason', 'duration_ms', 'requests', 'body_bytes',
                                                    'structuralDiagnostics')}
        s['sourceStatus'] = result['status']
        self.db.execute('UPDATE polls SET result=? WHERE rowid=(SELECT max(rowid) FROM polls)', (json.dumps(s['lastResult']),))
        if result['status'] in ('found', 'empty'):
            receiver = self.update_receiver(now)
            baseline = 'coverageBaselineAt' not in s
            if baseline:
                s['coverageBaselineAt'] = result['checkedAt']
            s.update(failures=0, lastSuccessAt=result['checkedAt'], lastSuccessEpoch=now, retryNotBefore=0)
            s['currentStoryIds'] = [x['storyId'] for x in result['stories']]
            for story in result['stories']:
                if epoch(story['expiresAt']) <= now:
                    continue
                if not self.db.execute('SELECT 1 FROM story_coverage WHERE story_id=?', (story['storyId'],)).fetchone():
                    excluded = 'coverage_gap' if s.get('coverageWindowHasGap') else ''
                    if not receiver['connected']:
                        excluded = ('receiver_stale' if receiver['status'] == 'connected'
                                    else 'receiver_' + receiver['status'])
                    self.write_coverage(story['storyId'], {
                        'storyId': story['storyId'], 'publishedAt': story['publishedAt'],
                        'publishedEpoch': epoch(story['publishedAt']), 'firstSeenAt': clock_timestamp(now),
                        'firstSeenEpoch': now, 'collectionTrigger': s.get('lastPollTrigger', 'poll'),
                        'baseline': baseline, 'excludedReason': excluded,
                        'pushCorrelation': 'awaiting_push', 'targets': {},
                    })
                for candidate in story['destinations']:
                    link = ticket_link(candidate)
                    if not link:
                        continue
                    key = hashlib.sha256((story['storyId']+'\n'+link).encode()).hexdigest()
                    payload = {k: story.get(k) for k in ('storyId', 'publishedAt', 'expiresAt',
                                                       'imageUrl', 'imageWidth', 'imageHeight')}
                    payload['link'] = link
                    row = self.db.execute('SELECT status,payload FROM outbox WHERE key=?', (key,)).fetchone()
                    if row is None:
                        self.db.execute('INSERT INTO outbox VALUES(?,?,?,?,?,0,0,?)',
                                        (key, story['storyId'], epoch(story['expiresAt']), json.dumps(payload),
                                         'pending' if payload['imageUrl'] else 'held', '{}'))
                    elif row[0] not in ('delivered', 'expired'):
                        # Refresh signed URLs without resetting destination receipts.
                        previous = json.loads(row[1])
                        for name in ('imageBase64','imageMime'):
                            if name in previous:
                                payload[name] = previous[name]
                        if canonical(previous) != canonical(payload):
                            self.db.execute('UPDATE outbox SET payload=? WHERE key=?', (json.dumps(payload), key))
            # Persist snapshot, outbox and only the pre-fetch high-water mark in one transaction.
            # A push received during collection remains pending for the next collection.
            s['consumedSignalSequence'] = max(s.get('consumedSignalSequence', 0), s.get('reservedSignalSequence', 0))
            interval = SAFETY_PERIOD if self.event_enabled() else PERIOD
            s.update(nextPoll=now+interval+random.uniform(0, 10), activePollPeriodSeconds=interval)
            s['coverageWindowHasGap'] = not receiver['connected']
            self.refresh_coverage(receiver, force=True)
        else:
            s['coverageWindowHasGap'] = True
            s['failures'] += 1
            if result['status'] == 'auth_required' or result.get('reason') == 'session_write_failed':
                delay = 6*3600
            elif result['status'] == 'rate_limited':
                delay = max(3600, result.get('retryAfterSeconds', 0))
            else:
                delay = min(3600, PERIOD*2**min(s['failures'], 5))
            s['nextPoll'] = now + delay
            s['retryNotBefore'] = s['nextPoll']
        self.db.execute('DELETE FROM polls WHERE at<?', (now-7*86400,))
        self.db.execute('DELETE FROM outbox WHERE expires<?', (now-30*86400,))
        self.db.execute("DELETE FROM story_coverage WHERE CAST(json_extract(value, '$.firstSeenEpoch') AS REAL)<?", (now-30*86400,))
        self.save()

    def flush(self, send=ingest, media_fetch=fetch_media):
        now = self.clock()
        expired = self.db.execute("UPDATE outbox SET status='expired' WHERE expires<=? AND status NOT IN ('delivered','expired')", (now,)).rowcount
        if expired:
            self.state['lastOutboxProgressAt'] = clock_timestamp(now)
        self.commit()
        rows = self.db.execute("SELECT key,payload,attempts,status,receipt FROM outbox WHERE due<=? AND expires>? AND status NOT IN ('delivered','expired','unknown') ORDER BY due LIMIT 2", (now, now)).fetchall()
        for key, text, attempts, previous_status, previous_receipt in rows:
            payload = json.loads(text)
            if not payload.get('imageUrl'):
                self.db.execute('UPDATE outbox SET status=\'held\',due=? WHERE key=?', (now+PERIOD, key))
                continue
            if not payload.get('imageBase64'):
                reads = self.db.execute('SELECT count(*) FROM media_reads WHERE at>?', (now-86400,)).fetchone()[0]
                if reads >= 20:
                    self.db.execute('UPDATE outbox SET due=? WHERE key=?', (now+3600,key))
                    continue
                self.db.execute('INSERT INTO media_reads VALUES(?)', (now,))
                self.commit()
                self.state['mediaRequests'] = self.state.get('mediaRequests', 0)+1
                # CDN requests carry no Instagram cookies, authorization or password.
                try:
                    ua = json.loads((self.directory/'session.json').read_text()).get('user_agent','UOLInstagramMonitor/1.0')
                except (OSError,ValueError):
                    ua = 'UOLInstagramMonitor/1.0'
                media = media_fetch(payload, ua)
                if not media:
                    self.db.execute('UPDATE outbox SET due=? WHERE key=?', (now+PERIOD,key))
                    continue
                payload.update(media)
                self.db.execute('UPDATE outbox SET payload=? WHERE key=?', (json.dumps(payload),key))
            # The Worker intake is idempotent; source crash only repeats intake.
            self.db.execute("UPDATE outbox SET due=?,attempts=attempts+1 WHERE key=?", (now+PERIOD, key))
            self.commit()
            receipt = send(self.config, payload)
            status = receipt.get('status', 'pending')
            targets = receipt.get('targets') or {}
            if status == 'unknown' and any(v.get('status') in ('pending','failed_safe','held','in_flight','reconciling') for v in targets.values()):
                status = 'pending'
            if status not in ('delivered','expired','unknown','held'):
                status = 'pending'
            self.db.execute('UPDATE outbox SET status=?,due=?,receipt=? WHERE key=?',
                            (status, self.clock()+min(3600, PERIOD*2**min(attempts, 5)), json.dumps(receipt), key))
            previous = json.loads(previous_receipt)
            progress = lambda value: {'status': value.get('status'),
                'targets': {name: {key: target.get(key) for key in ('status','messageId','imageConfirmed')}
                            for name, target in (value.get('targets') or {}).items()}}
            if status != previous_status or progress(previous) != progress(receipt):
                self.state['lastOutboxProgressAt'] = clock_timestamp(self.clock())
            coverage = self.db.execute('SELECT value FROM story_coverage WHERE story_id=?', (payload['storyId'],)).fetchone()
            if coverage:
                entry = json.loads(coverage[0])
                entry['targets'][key] = {name: {field: target.get(field) for field in ('status','imageConfirmed')}
                                         for name, target in targets.items()}
                self.write_coverage(payload['storyId'], entry, coverage[0])
            if status == 'delivered':
                payload.pop('imageBase64',None)
                payload.pop('imageMime',None)
                self.db.execute('UPDATE outbox SET payload=? WHERE key=?', (json.dumps(payload),key))
                self.state['lastDelivered'] = {'storyId': payload['storyId'], 'link': payload['link'],
                                               'targets': targets, 'observedAt': timestamp()}
        self.db.execute('DELETE FROM media_reads WHERE at<?', (now-86400,))
        # Expired pictures are not kept in the retained metadata history.
        for key,text in self.db.execute("SELECT key,payload FROM outbox WHERE status='expired'").fetchall():
            payload = json.loads(text)
            if 'imageBase64' in payload:
                payload.pop('imageBase64',None);payload.pop('imageMime',None)
                self.db.execute('UPDATE outbox SET payload=? WHERE key=?',(json.dumps(payload),key))
        self.save()

    def publish_health(self, send=heartbeat):
        now = self.clock()
        status = self.state['sourceStatus']
        if (status == self.state.get('lastHeartbeatStatus')
                and now - self.state.get('lastHeartbeatEpoch', 0) < 900):
            return
        if now < self.state.get('healthRetryAt', 0):
            return
        payload = {'sourceStatus': status, 'observedAt': self.state.get('lastObservationAt'),
                   'lastSuccessAt': self.state.get('lastSuccessAt'),
                   'reason': (self.state.get('lastResult') or {}).get('reason', '')}
        # Current Worker accepts and ignores additive fields; its public contract stays intact.
        payload.update(coverage=self.state.get('coverage', {}), pushReceiver=self.state.get('pushReceiver', {}))
        if send(self.config, payload):
            self.state.update(lastHeartbeatStatus=status, lastHeartbeatEpoch=now,
                              healthRetryAt=0, remoteHealthPublished=True)
        else:
            self.state.update(healthRetryAt=now+300, remoteHealthPublished=False)
        self.save()


class StorageUsageWorker:
    """One background worker; the Story loop only polls completed results."""
    def __init__(self, observer, interval):
        self.observer, self.interval = observer, interval
        self.future = None
        self.next_due = 0
        self.outcome = observer.state.get('lastOutcome', 'not_started')
        self.wake, self.stopped = threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self._run, name='storage-usage', daemon=True)
        self.thread.start()

    def _run(self):
        while True:
            self.wake.wait()
            self.wake.clear()
            if self.stopped.is_set():
                return
            future = self.future
            future.set_running_or_notify_cancel()
            try:
                outcome = self.observer.run_if_due()['outcome']
            except Exception:
                outcome = 'failed'
            future.set_result(outcome)

    def tick(self):
        if self.future is not None and self.future.done():
            self.outcome = self.future.result()
            self.future = None
        now = self.observer.clock()
        if (not self.stopped.is_set() and self.future is None
                and now >= max(self.next_due, self.observer.state['nextEpoch'])):
            # Even failures before the observer's durable reservation stay bounded.
            self.next_due = now + self.interval
            self.future = Future()
            self.wake.set()
        return self.outcome

    def close(self):
        # Do not join a network call on --once, shutdown, or a Story-loop failure.
        self.stopped.set()
        self.wake.set()


def run(directory, config_path, once=False):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise ValueError('private_directory_required')
    config = private_config(config_path)
    with open(directory / 'monitor.lock', 'a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        monitor = Monitor(directory, config)
        storage_observer = None
        if (directory / 'storage-usage.json').exists():
            # Optional observer failure must not stop the independent Story pipeline.
            try:
                from storage_usage import INTERVAL, StorageUsageObserver
                storage_observer = StorageUsageWorker(
                    StorageUsageObserver(directory / 'storage-usage.json'), INTERVAL)
            except Exception:
                monitor.state['storageUsageObserver'] = 'unavailable'
        try:
            while True:
                monitor.loop_checkpoint()
                if storage_observer is not None:
                    try:
                        monitor.state['storageUsageObserver'] = storage_observer.tick()
                    except Exception:
                        monitor.state['storageUsageObserver'] = 'failed'
                if monitor.reserve_poll():
                    try:
                        result = InstagramClient(directory/'session.json').collect()
                    except SessionError as e:
                        result = {'checkedAt': timestamp(), 'status': 'auth_required', 'reason': e.reason,
                                  'requests': 0, 'duration_ms': 0, 'body_bytes': 0, 'stories': []}
                    monitor.record(result)
                monitor.flush()
                monitor.publish_health()
                monitor.loop_checkpoint()
                if once:
                    return monitor.state
                time.sleep(2 if config.get('pushMode', 'poll') != 'poll' else 15)
        finally:
            if storage_observer is not None:
                storage_observer.close()


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data-dir', type=Path, required=True)
    p.add_argument('--config-file', type=Path, required=True)
    p.add_argument('--once', action='store_true')
    a = p.parse_args()
    try:
        run(a.data_dir, a.config_file, a.once)
    except BlockingIOError:
        print(json.dumps({'status': 'locked'}))
    except Exception as e:
        print(json.dumps({'status': 'failed', 'reason': type(e).__name__}))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
