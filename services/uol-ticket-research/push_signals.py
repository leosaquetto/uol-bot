"""Read durable source signals without reading private encrypted notification bodies."""
import json
from contextlib import closing
import datetime as dt
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time


class PushSignals:
    def __init__(self, directory):
        self.path = Path(directory) / 'push.sqlite'

    def _connect(self):
        info = self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()):
            raise ValueError('private_push_database_required')
        return sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=1)

    def snapshot(self, now):
        try:
            with closing(self._connect()) as db:
                sequence = db.execute("SELECT coalesce(max(seq),0) FROM signals WHERE profile='clubeuol'").fetchone()[0]
                receiver = db.execute('SELECT status,observed_at FROM receiver_state WHERE id=1').fetchone()
            fresh = bool(receiver and 0 <= now-receiver[1]/1000 < 600)
            return {'sequence': sequence, 'connected': bool(receiver and receiver[0] == 'connected' and fresh),
                    'status': receiver[0] if receiver else 'starting', 'fresh': fresh,
                    'observedAt': dt.datetime.fromtimestamp(receiver[1]/1000, dt.timezone.utc).isoformat()
                    if receiver else None}
        except (OSError, ValueError, sqlite3.Error):
            return {'sequence': 0, 'connected': False, 'status': 'unavailable',
                    'fresh': False, 'observedAt': None}

    def signals(self):
        """Bounded metadata only; an exact Story ID is distinct from a profile hint."""
        try:
            with closing(self._connect()) as db:
                columns = {row[1] for row in db.execute('PRAGMA table_info(signals)')}
                received = 'received_at' if 'received_at' in columns else 'NULL'
                rows = db.execute(f"SELECT seq,story_id,{received} FROM signals WHERE profile='clubeuol' ORDER BY seq DESC LIMIT 5000").fetchall()
            return [{'sequence': row[0], 'storyId': row[1],
                     'receivedEpoch': row[2]/1000 if row[2] is not None else None} for row in rows]
        except (OSError, ValueError, sqlite3.Error):
            return []

    def pending(self, after, through):
        """Reserve bounded source metadata; signals arriving later stay outside this batch."""
        try:
            with closing(self._connect()) as db:
                rows = db.execute("SELECT seq,story_id,received_at FROM signals WHERE profile='clubeuol' "
                                  'AND seq>? AND seq<=? ORDER BY seq LIMIT 5000', (after, through)).fetchall()
            return [{'sequence': row[0], 'storyId': row[1],
                     'receivedEpoch': row[2]/1000 if isinstance(row[2], (int, float)) else None}
                    for row in rows]
        except (OSError, ValueError, sqlite3.Error):
            return []

    def proves(self, proof, now=None):
        if (not isinstance(proof, dict) or proof.get('browserClosed') is not True
                or type(proof.get('signalSequence')) is not int or proof['signalSequence'] < 1
                or not isinstance(proof.get('storyId'), str)
                or not re.fullmatch(r'[0-9]{10,25}', proof['storyId'])
                or not isinstance(proof.get('observedAt'), str)
                or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
                                    r'(?:\.[0-9]{1,6})?(?:Z|\+00:00)', proof['observedAt'])):
            return False
        try:
            observed = dt.datetime.fromisoformat(proof['observedAt'].replace('Z', '+00:00'))
            observed_epoch = observed.timestamp()
            now = time.time() if now is None else now
            if not math.isfinite(now) or observed_epoch <= 0 or observed_epoch > now + 60:
                return False
            with closing(self._connect()) as db:
                row = db.execute("SELECT story_id,received_at FROM signals WHERE seq=? AND profile='clubeuol'",
                                 (proof['signalSequence'],)).fetchone()
            if not row or row[0] != proof['storyId'] or not isinstance(row[1], (int, float)):
                return False
            received_epoch = row[1] / 1000
            # Generic hints remain triggers; promotion requires an exact, timed Story proof.
            # Historical matching proof stays valid after the Story expires and on restart.
            return (math.isfinite(received_epoch) and 0 < received_epoch <= now + 60
                    and 0 <= observed_epoch - received_epoch <= 600)
        except (OSError, ValueError, TypeError, OverflowError, sqlite3.Error):
            return False


def proof_key(proof):
    return json.dumps(proof, sort_keys=True, separators=(',', ':'))
