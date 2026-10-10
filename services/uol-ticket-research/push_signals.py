"""Read durable source signals without reading private encrypted notification bodies."""
import json
from contextlib import closing
import datetime as dt
import os
from pathlib import Path
import re
import sqlite3
import stat


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

    def proves(self, proof):
        if (not isinstance(proof, dict) or proof.get('browserClosed') is not True
                or not isinstance(proof.get('signalSequence'), int) or proof['signalSequence'] < 1
                or not isinstance(proof.get('storyId'), str)
                or not re.fullmatch(r'[0-9]{10,25}', proof['storyId'])
                or not isinstance(proof.get('observedAt'), str)):
            return False
        try:
            observed = dt.datetime.fromisoformat(proof['observedAt'].replace('Z', '+00:00'))
            if observed.tzinfo is None:
                return False
            with closing(self._connect()) as db:
                row = db.execute("SELECT story_id FROM signals WHERE seq=? AND profile='clubeuol'",
                                 (proof['signalSequence'],)).fetchone()
            return bool(row and (row[0] is None or row[0] == proof['storyId']))
        except (OSError, ValueError, sqlite3.Error):
            return False


def proof_key(proof):
    return json.dumps(proof, sort_keys=True, separators=(',', ':'))
