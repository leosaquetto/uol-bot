"""Server-only Instagram complement for the UOL bot; never redeems benefits."""
import argparse
import base64
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
import time
import urllib.error
import urllib.parse
import urllib.request

from instagram import InstagramClient, NoRedirect, SessionError, safe_media_url
from trial import atomic_json, timestamp

INGEST_URL = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/ingest-instagram-story'
HEARTBEAT_URL = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/instagram-monitor-heartbeat'
MAX_DAILY_REQUESTS = 1440
PERIOD = 120
MAX_MEDIA_BYTES = 3 * 1024 * 1024


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
        self.db = sqlite3.connect(self.directory / 'monitor.sqlite')
        self.db.executescript('''
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1), value TEXT);
            CREATE TABLE IF NOT EXISTS polls(at REAL, reserved INTEGER, result TEXT);
            CREATE TABLE IF NOT EXISTS media_reads(at REAL);
            CREATE TABLE IF NOT EXISTS outbox(
                key TEXT PRIMARY KEY, story_id TEXT, expires REAL, payload TEXT,
                status TEXT, due REAL, attempts INTEGER, receipt TEXT);
        ''')
        row = self.db.execute('SELECT value FROM state WHERE id=1').fetchone()
        self.state = json.loads(row[0]) if row else {
            'startedAt': timestamp(), 'cycles': 0, 'requests': 0, 'nextPoll': 0,
            'failures': 0, 'sourceStatus': 'starting', 'lastSuccessAt': None,
            'profile': 'clubeuol', 'redemptionEnabled': False,
        }

    def save(self):
        self.state['outbox'] = {row[0]: row[1] for row in
                               self.db.execute('SELECT status,count(*) FROM outbox GROUP BY status')}
        self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)', (json.dumps(self.state),))
        self.db.commit()
        atomic_json(self.directory / 'status.json', self.state)

    def reserve_poll(self):
        now = self.clock()
        if now < self.state['nextPoll']:
            return False
        used = self.db.execute('SELECT coalesce(sum(reserved),0) FROM polls WHERE at>?', (now-86400,)).fetchone()[0]
        if used + 2 > MAX_DAILY_REQUESTS:
            first = self.db.execute('SELECT min(at) FROM polls WHERE at>?', (now-86400,)).fetchone()[0]
            self.state.update(sourceStatus='budget_wait', nextPoll=first+86401)
            self.save()
            return False
        self.db.execute('INSERT INTO polls VALUES(?,2,NULL)', (now,))
        self.state['nextPoll'] = now + PERIOD
        self.save()
        return True

    def record(self, result):
        now, s = self.clock(), self.state
        s['cycles'] += 1
        s['requests'] += result.get('requests', 0)
        s['lastObservationAt'] = result['checkedAt']
        s['lastResult'] = {k: result.get(k) for k in ('status', 'reason', 'duration_ms', 'requests', 'body_bytes')}
        s['sourceStatus'] = result['status']
        self.db.execute('UPDATE polls SET result=? WHERE rowid=(SELECT max(rowid) FROM polls)', (json.dumps(s['lastResult']),))
        if result['status'] in ('found', 'empty'):
            s.update(failures=0, lastSuccessAt=result['checkedAt'], nextPoll=now+PERIOD+random.uniform(0, 10))
            s['currentStoryIds'] = [x['storyId'] for x in result['stories']]
            for story in result['stories']:
                if epoch(story['expiresAt']) <= now:
                    continue
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
                        self.db.execute('UPDATE outbox SET payload=? WHERE key=?', (json.dumps(payload), key))
        else:
            s['failures'] += 1
            if result['status'] == 'auth_required' or result.get('reason') == 'session_write_failed':
                delay = 6*3600
            elif result['status'] == 'rate_limited':
                delay = max(3600, result.get('retryAfterSeconds', 0))
            else:
                delay = min(3600, PERIOD*2**min(s['failures'], 5))
            s['nextPoll'] = now + delay
        self.db.execute('DELETE FROM polls WHERE at<?', (now-7*86400,))
        self.db.execute('DELETE FROM outbox WHERE expires<?', (now-30*86400,))
        self.save()

    def flush(self, send=ingest, media_fetch=fetch_media):
        now = self.clock()
        self.db.execute("UPDATE outbox SET status='expired' WHERE expires<=? AND status!='delivered'", (now,))
        self.db.commit()
        rows = self.db.execute("SELECT key,payload,attempts FROM outbox WHERE due<=? AND expires>? AND status NOT IN ('delivered','expired','unknown') ORDER BY due LIMIT 2", (now, now)).fetchall()
        for key, text, attempts in rows:
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
                self.db.commit()
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
            self.db.commit()
            receipt = send(self.config, payload)
            status = receipt.get('status', 'pending')
            targets = receipt.get('targets') or {}
            if status == 'unknown' and any(v.get('status') in ('pending','failed_safe','held','in_flight','reconciling') for v in targets.values()):
                status = 'pending'
            if status not in ('delivered','expired','unknown','held'):
                status = 'pending'
            self.db.execute('UPDATE outbox SET status=?,due=?,receipt=? WHERE key=?',
                            (status, self.clock()+min(3600, PERIOD*2**min(attempts, 5)), json.dumps(receipt), key))
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
        if send(self.config, payload):
            self.state.update(lastHeartbeatStatus=status, lastHeartbeatEpoch=now,
                              healthRetryAt=0, remoteHealthPublished=True)
        else:
            self.state.update(healthRetryAt=now+300, remoteHealthPublished=False)
        self.save()


def run(directory, config_path, once=False):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise ValueError('private_directory_required')
    config = private_config(config_path)
    with open(directory / 'monitor.lock', 'a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        monitor = Monitor(directory, config)
        while True:
            if monitor.reserve_poll():
                try:
                    result = InstagramClient(directory/'session.json').collect()
                except SessionError as e:
                    result = {'checkedAt': timestamp(), 'status': 'auth_required', 'reason': e.reason,
                              'requests': 0, 'duration_ms': 0, 'body_bytes': 0, 'stories': []}
                monitor.record(result)
            monitor.flush()
            monitor.publish_health()
            if once:
                return monitor.state
            time.sleep(15)


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
