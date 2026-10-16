"""Bounded loopback relay for authenticated phone notification hints.

Hints share the existing durable signal sequence, but never claim native Web Push
delivery or a specific Story. This process never contacts Instagram or a sender.
"""
import argparse
from contextlib import closing
import datetime as dt
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import uuid

MAX_BODY = 4096
MAX_HOURLY_SIGNALS = 60
MAX_SIGNALS = 100000
ORIGIN = 'external_notification'


class IngressError(Exception):
    def __init__(self, code, status=400):
        self.code, self.status = code, status
        super().__init__(code)


def private_file(path, limit=None):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise ValueError('private_file_required')
        return handle.read(limit + 1) if limit is not None else None


def relay_token(directory):
    raw = private_file(Path(directory) / 'notification-relay.json', MAX_BODY)
    if len(raw) > MAX_BODY:
        raise ValueError('relay_config_limit')
    config = json.loads(raw)
    if (not isinstance(config, dict) or set(config) != {'token'}
            or not re.fullmatch(r'[a-f0-9]{64}', config.get('token', ''))):
        raise ValueError('invalid_relay_config')
    return config['token']


def validate_hint(value):
    if (not isinstance(value, dict) or set(value) != {'eventId', 'profile', 'source', 'observedAt'}
            or value.get('profile') != 'clubeuol'
            or value.get('source') not in ('ios_shortcuts', 'scriptable')):
        raise IngressError('invalid_notification_hint')
    try:
        event_id = str(uuid.UUID(value['eventId']))
        if event_id != value['eventId'] or len(value['observedAt']) > 40:
            raise ValueError()
        observed = dt.datetime.fromisoformat(value['observedAt'].replace('Z', '+00:00'))
        epoch = observed.timestamp()
        if observed.tzinfo is None or not math.isfinite(epoch) or epoch <= 0:
            raise ValueError()
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise IngressError('invalid_notification_hint') from None
    return event_id, json.dumps({'origin': ORIGIN, 'source': value['source'],
                                 'observedAt': value['observedAt']}, sort_keys=True, separators=(',', ':')), epoch


class NotificationStore:
    def __init__(self, directory, clock=time.time):
        self.path, self.clock = Path(directory) / 'push.sqlite', clock
        with closing(self.connect()) as db:
            self.verify_schema(db)

    def connect(self):
        private_file(self.path)
        db = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=1)
        db.execute('PRAGMA synchronous=FULL')
        return db

    @staticmethod
    def verify_schema(db):
        columns = {row[1]: row for row in db.execute('PRAGMA table_info(signals)')}
        if (not {'seq', 'event_id', 'received_at', 'profile', 'story_id', 'evidence'} <= set(columns)
                or columns['seq'][5] != 1):
            raise ValueError('existing_signal_schema_required')
        unique_event_id = False
        for index in db.execute('PRAGMA index_list(signals)').fetchall():
            if index[2] and not index[4]:
                names = [row[0] for row in db.execute('SELECT name FROM pragma_index_info(?) ORDER BY seqno', (index[1],))]
                unique_event_id |= names == ['event_id']
        if not unique_event_id:
            raise ValueError('existing_signal_identity_required')

    def enqueue(self, value):
        now = self.clock()
        event_id, evidence, observed_epoch = validate_hint(value)
        key, received_at = 'ios:' + event_id, int(now * 1000)
        try:
            with closing(self.connect()) as db:
                db.execute('BEGIN IMMEDIATE')
                previous = db.execute('SELECT seq,evidence FROM signals WHERE event_id=?', (key,)).fetchone()
                if previous:
                    if previous[1] != evidence:
                        raise IngressError('notification_identity_conflict', 409)
                    db.rollback()
                    return {'ok': True, 'accepted': True, 'persisted': True, 'duplicate': True, 'eventId': event_id}
                # Client time is diagnostic. A delayed authenticated wake still
                # requests one current snapshot; server receipt time drives it.
                if observed_epoch > now + 300:
                    raise IngressError('invalid_notification_hint')
                count = db.execute('SELECT count(*) FROM signals').fetchone()[0]
                if count >= MAX_SIGNALS:
                    raise IngressError('notification_capacity_reached', 503)
                recent = db.execute("SELECT count(*) FROM signals WHERE event_id LIKE 'ios:%' AND received_at>?",
                                    (received_at - 3600000,)).fetchone()[0]
                if recent >= MAX_HOURLY_SIGNALS:
                    raise IngressError('notification_rate_limited', 429)
                db.execute('INSERT INTO signals(event_id,received_at,profile,story_id,evidence) VALUES(?,?,?,?,?)',
                           (key, received_at, 'clubeuol', '', evidence))
                db.commit()
                return {'ok': True, 'accepted': True, 'persisted': True, 'duplicate': False, 'eventId': event_id}
        except (OSError, ValueError, sqlite3.Error):
            raise IngressError('notification_storage_unavailable', 503) from None


def handler(store, token):
    expected = hashlib.sha256(('Bearer ' + token).encode()).digest()

    class Handler(BaseHTTPRequestHandler):
        server_version = 'NotificationRelay'

        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, *_):
            pass

        def reply(self, status, value):
            raw = json.dumps(value, separators=(',', ':')).encode()
            self.close_connection = True
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            self.reply(200, {'ok': True}) if self.path == '/healthz' else self.reply(404, {'error': 'not_found'})

        def do_POST(self):
            if self.path != '/instagram-wake':
                return self.reply(404, {'error': 'not_found'})
            authorization = self.headers.get_all('Authorization', [])
            supplied = hashlib.sha256(authorization[0].encode()).digest() if len(authorization) == 1 else b''
            if not hmac.compare_digest(expected, supplied):
                return self.reply(401, {'error': 'unauthorized'})
            if self.headers.get('Transfer-Encoding') is not None:
                return self.reply(411, {'error': 'content_length_required'})
            lengths = self.headers.get_all('Content-Length', [])
            if len(lengths) != 1 or not re.fullmatch(r'[0-9]{1,10}', lengths[0]):
                return self.reply(411, {'error': 'content_length_required'})
            size = int(lengths[0])
            if size > MAX_BODY:
                return self.reply(413, {'error': 'body_too_large'})
            if self.headers.get('Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
                return self.reply(415, {'error': 'json_required'})
            try:
                raw = self.rfile.read(size)
                if len(raw) != size:
                    raise IngressError('incomplete_body')
                value = json.loads(raw)
                result = store.enqueue(value)
                return self.reply(202, result)
            except IngressError as error:
                return self.reply(error.status, {'error': error.code})
            except (ValueError, UnicodeError, RecursionError):
                return self.reply(400, {'error': 'invalid_json'})
            except (OSError, sqlite3.Error):
                return self.reply(503, {'error': 'notification_storage_unavailable'})

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--port', type=int, default=8791)
    args = parser.parse_args()
    os.umask(0o077)
    store = NotificationStore(args.directory)
    with HTTPServer(('127.0.0.1', args.port), handler(store, relay_token(args.directory))) as server:
        server.serve_forever()


if __name__ == '__main__':
    main()
