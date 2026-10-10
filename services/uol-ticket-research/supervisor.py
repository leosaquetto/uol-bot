"""Bounded Oracle-only supervision; reads existing state, never collects Stories."""
import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import time
import urllib.error
import urllib.request
import uuid

from trial import atomic_json

NTFY_URL = 'https://ntfy.sh/macXntfy-8130'
SERVICES = ('uol-instagram-monitor', 'uol-instagram-push')
MAX_ATTEMPTS = 20
RESERVED_ATTEMPTS = 5
MAX_OUTBOX = 100


def instant(value):
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed.timestamp() if parsed.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def iso(now):
    return dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat().replace('+00:00', 'Z')


def private_json(path, maximum=1024 * 1024):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()):
            raise ValueError('private_file_required')
        text = stream.read(maximum + 1)
    if len(text.encode('utf-8')) > maximum:
        raise ValueError('private_file_limit')
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError('private_object_required')
    return value


def read_optional(path):
    try:
        return private_json(path)
    except (OSError, ValueError):
        return {}


def unit_state(name):
    if name not in SERVICES:
        raise ValueError('service_not_allowed')
    try:
        result = subprocess.run(['systemctl', 'show', name + '.service',
                                 '--property=ActiveState', '--value'],
                                capture_output=True, text=True, timeout=5)
        return result.stdout.strip() if result.returncode == 0 else 'unavailable'
    except (OSError, subprocess.TimeoutExpired):
        return 'unavailable'


def push_summary(directory):
    path = Path(directory) / 'push.sqlite'
    try:
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()):
            return {'status': 'unavailable'}
        with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1) as db:
            receiver = db.execute('SELECT status,observed_at FROM receiver_state WHERE id=1').fetchone()
            row = db.execute("SELECT count(*),coalesce(sum(state='pending'),0),"
                             "coalesce(sum(length(CAST(packet AS BLOB))),0) FROM push_events").fetchone()
        return {'status': receiver[0] if receiver else 'starting',
                'observedEpoch': receiver[1] / 1000 if receiver else None,
                'eventCount': row[0], 'pendingCount': row[1], 'payloadBytes': row[2]}
    except (OSError, ValueError, sqlite3.Error):
        return {'status': 'unavailable'}


def publish(payload):
    """No redirects, attachments, cookies, or credentials in messages."""
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    request = urllib.request.Request(NTFY_URL, data=payload['message'].encode('utf-8'), method='POST',
        headers={'Title': 'UOL - supervisao', 'Priority': '4' if payload['critical'] else '3',
                 'Content-Type': 'text/plain; charset=utf-8', 'User-Agent': 'UOLOperations/1.0'})
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=10) as response:
            body = response.read(8193)
            if len(body) > 8192:
                return {'outcome': 'uncertain'}
            result = json.loads(body)
            return {'outcome': 'accepted' if isinstance(result.get('id'), str) else 'uncertain'}
    except urllib.error.HTTPError as error:
        try:
            retry = error.headers.get('Retry-After', '')
            try:
                retry_seconds = min(86400, max(0, int(retry)))
            except ValueError:
                try:
                    import email.utils
                    retry_seconds = min(86400, max(0, email.utils.parsedate_to_datetime(retry).timestamp() - time.time()))
                except (ValueError, TypeError, AttributeError):
                    retry_seconds = 0
            return {'outcome': 'retry' if error.code == 429 or error.code >= 500 else 'rejected',
                    'retryAfter': retry_seconds}
        finally:
            error.close()
    except (TimeoutError, urllib.error.URLError, OSError, ValueError):
        return {'outcome': 'uncertain'}


class Supervisor:
    def __init__(self, directory, config, clock=time.time, sender=publish):
        self.directory, self.config = Path(directory), config
        self.clock, self.sender = clock, sender
        if config.get('ntfyUrl') != NTFY_URL:
            raise ValueError('notification_destination_not_allowed')
        expiries = config.get('credentialExpiries', [])
        if not isinstance(expiries, list) or len(expiries) > 20:
            raise ValueError('credential_metadata_invalid')
        for entry in expiries:
            if (not isinstance(entry, dict) or entry.get('name') != 'analytics-read'
                    or instant(entry.get('expiresAt')) is None):
                raise ValueError('credential_metadata_invalid')
        self.path = self.directory / 'supervisor-state.json'
        self.state = private_json(self.path) if self.path.exists() else {
            'startedAt': iso(clock()), 'lastRunAt': None, 'incidents': {},
            'serviceFailures': {}, 'outbox': [], 'publicationAttempts': [],
        }

    def save(self):
        atomic_json(self.path, self.state)

    def collect(self):
        return {'units': {name: unit_state(name) for name in SERVICES},
                'status': read_optional(self.directory / 'status.json'),
                'loop': read_optional(self.directory / 'loop-heartbeat.json'),
                'usage': read_optional(self.directory / 'storage-usage-state.json'),
                'push': push_summary(self.directory),
                'disk': os.statvfs(self.directory)}

    def evaluate(self, sources, now):
        issues = {}
        def add(key, message, critical=False):
            issues[key] = {'message': message, 'critical': critical}
        for name in SERVICES:
            active = sources.get('units', {}).get(name) == 'active'
            failures = 0 if active else self.state['serviceFailures'].get(name, 0) + 1
            self.state['serviceFailures'][name] = failures
            if failures >= 2:
                add('service:' + name, name + ': serviço sem atividade em duas verificações.', True)
        started = instant(self.state['startedAt']) or now
        status, loop = sources.get('status', {}), sources.get('loop', {})
        alive = instant(loop.get('loopAliveAt') or status.get('loopAliveAt'))
        if now - started >= 300 and (alive is None or alive > now + 60 or now - alive > 300):
            add('monitor_loop', 'Monitor: indicador de vida ausente ou antigo por mais de cinco minutos.', True)
        push = sources.get('push', {})
        observed = push.get('observedEpoch')
        if now - started >= 600 and (push.get('status') != 'connected' or observed is None
                                     or observed > now + 60 or now - observed > 600):
            add('push_connection', 'Instagram push: conexão sem confirmação recente há dez minutos.')
        if push.get('status') in ('registration_requires_review', 'push_storage_unavailable',
                'push_pending_limit', 'push_payload_limit', 'push_identity_limit'):
            add('push_blocked', 'Instagram push: recepção interrompida; inscrição ou armazenamento exige revisão.', True)
        if status.get('sourceStatus') == 'auth_required':
            add('instagram_auth', 'Instagram: sessão exige autenticação. Backoff preservado.', True)
        last_success = instant(status.get('lastSuccessAt'))
        if (status.get('sourceStatus') not in ('found', 'empty', 'starting', 'budget_wait', 'auth_required')
                and status.get('failures', 0) >= 3
                and (last_success is None or now - last_success > 600)):
            add('instagram_collection', 'Instagram: três coletas falharam sem confirmação recente. Estado desconhecido; ausência de Stories não foi comprovada.')
        progress = status.get('outboxProgress') or {}
        pending = progress.get('pending', 0) + progress.get('inFlight', 0)
        if pending:
            self.state.setdefault('backlogObservedAt', iso(now))
            last_progress = instant(progress.get('lastProgressAt'))
            oldest = progress.get('oldestPendingEpoch')
            if not isinstance(oldest, (int, float)) or oldest > now + 60:
                oldest = instant(progress.get('oldestPendingAt'))
            if oldest is None or oldest > now + 60:
                oldest = instant(self.state['backlogObservedAt']) or now
            if last_progress is not None and last_progress > now + 60:
                last_progress = None
            since = max(oldest, last_progress or 0)
            if now - since > 900:
                add('delivery_backlog', 'Ingressos: entrega pendente há mais de 15 minutos sem progresso; verificar backoff e recibos.', True)
        elif 'pending' in progress:
            self.state.pop('backlogObservedAt', None)
        usage = sources.get('usage', {})
        sample_at = instant((usage.get('lastSample') or {}).get('observedAt'))
        day = dt.datetime.fromtimestamp(now, dt.timezone.utc).date().isoformat()
        if now - started >= 1800 and (sample_at is None or sample_at > now + 60 or now - sample_at > 1800
                or (usage.get('lastSample') or {}).get('day') != day):
            add('metrics_stale', 'Cloudflare: métricas essenciais ausentes ou antigas; proteção conservadora permanece ativa.', True)
        if usage.get('lastOutcome') == 'failed':
            reason = usage.get('lastError')
            if reason in ('analytics_unavailable', 'ingestion_unavailable', 'configuration_unavailable'):
                add('metrics_failed', 'Cloudflare: observação ou publicação de métricas falhou; última amostra preservada.')
        budget = usage.get('budgetState') or {}
        if budget.get('optionalWorkDeferred'):
            add('quota_deferred', 'Cloudflare: pesquisas extras e enriquecimentos adiados; descoberta principal e entregas preservadas.', True)
        for dimension in budget.get('warnings') or []:
            if isinstance(dimension, str) and dimension in ('rowsWritten', 'rowsRead', 'duration',
                    'workersRequests', 'doRequests', 'storedBytes', 'storageBytes'):
                add('quota:' + dimension, 'Cloudflare: consumo de ' + dimension + ' atingiu faixa de atenção.')
        limits = {'pendingCount': 5000, 'payloadBytes': 32 * 1024 * 1024, 'eventCount': 100000}
        for key, limit in limits.items():
            if isinstance(push.get(key), (int, float)) and push[key] >= 0.8 * limit:
                add('capacity:' + key, 'Instagram push: armazenamento atingiu 80% do limite de ' + key + '.', True)
        disk = sources.get('disk')
        if disk and disk.f_blocks and disk.f_bavail / disk.f_blocks < 0.2:
            add('oracle_disk', 'Oracle: menos de 20% do volume disponível.', True)
        for entry in self.config.get('credentialExpiries', []):
            remaining = instant(entry['expiresAt']) - now
            level = 'expired' if remaining <= 0 else '48h' if remaining <= 48 * 3600 else '7d' if remaining <= 7 * 86400 else None
            if level:
                text = {'expired': 'Token Analytics vencido; renovar com escopo somente leitura.',
                        '48h': 'Token Analytics vence em até 48 horas; preparar renovação.',
                        '7d': 'Token Analytics vence em até sete dias; preparar renovação.'}[level]
                add('credential:' + entry['name'] + ':' + level, text, level != '7d')
        return issues

    def can_recover(self, key, sources, now):
        """Missing/unreadable state is never affirmative evidence of recovery."""
        status, usage, push = (sources.get(name, {}) for name in ('status', 'usage', 'push'))
        if key.startswith('service:'):
            return sources.get('units', {}).get(key.split(':', 1)[1]) == 'active'
        if key == 'monitor_loop':
            alive = instant(sources.get('loop', {}).get('loopAliveAt') or status.get('loopAliveAt'))
            return alive is not None and -60 <= now - alive <= 300
        if key in ('push_connection', 'push_blocked'):
            seen = push.get('observedEpoch')
            return push.get('status') == 'connected' and seen is not None and -60 <= now - seen <= 600
        if key in ('instagram_auth', 'instagram_collection'):
            return status.get('sourceStatus') in ('found', 'empty')
        if key == 'delivery_backlog':
            progress = status.get('outboxProgress') or {}
            if 'pending' not in progress:
                return False
            last = instant(progress.get('lastProgressAt'))
            return progress['pending'] + progress.get('inFlight', 0) == 0 or (last is not None and -60 <= now - last <= 900)
        sample = usage.get('lastSample') or {}
        seen = instant(sample.get('observedAt'))
        metrics_fresh = seen is not None and -60 <= now - seen <= 1800 and sample.get('day') == iso(now)[:10]
        if key == 'metrics_stale':
            return metrics_fresh
        if key == 'metrics_failed':
            return usage.get('lastOutcome') == 'published' and metrics_fresh
        if key == 'quota_deferred':
            return metrics_fresh and (usage.get('budgetState') or {}).get('optionalWorkDeferred') is False
        if key.startswith('quota:'):
            item = ((usage.get('budgetState') or {}).get('metrics') or {}).get(key.split(':', 1)[1], {})
            at = instant(item.get('observedAt'))
            return (item.get('status') == 'known' and at is not None and -60 <= now - at <= 1800
                    and item.get('value') is not None and item.get('limit', 0) > 0
                    and item['value'] < item['limit'] * .7)
        if key.startswith('capacity:'):
            name = key.split(':', 1)[1]
            limit = {'pendingCount': 5000, 'payloadBytes': 32 * 1024 * 1024, 'eventCount': 100000}.get(name)
            return limit is not None and isinstance(push.get(name), (int, float)) and push[name] < .8 * limit
        if key == 'oracle_disk':
            disk = sources.get('disk')
            return bool(disk and disk.f_blocks and disk.f_bavail / disk.f_blocks >= .2)
        if key.startswith('credential:'):
            return metrics_fresh and any(instant(entry['expiresAt']) - now > 7 * 86400
                                        for entry in self.config.get('credentialExpiries', []))
        return False

    def queue(self, changes, now):
        if not changes:
            return
        # One bounded message per run; each line is generated from fixed labels.
        lines = [line for line, _ in changes]
        message = ('UOL — supervisão\n' + '\n'.join(lines))[:1800]
        while len(message.encode('utf-8')) > 3500:
            message = message[:-1]
        self.state['outbox'].append({'id': uuid.uuid4().hex, 'message': message,
            'critical': any(priority for _, priority in changes), 'createdAt': iso(now),
            'due': now, 'attempts': 0, 'outcome': 'pending'})
        if len(self.state['outbox']) > MAX_OUTBOX:
            completed = [x for x in self.state['outbox'] if x['outcome'] not in ('pending', 'in_flight')]
            active = [x for x in self.state['outbox'] if x['outcome'] in ('pending', 'in_flight')]
            # Never erase an undelivered incident to make room; coalesce overflow.
            if len(active) > MAX_OUTBOX:
                kept, overflow = active[:MAX_OUTBOX-1], active[MAX_OUTBOX-1:]
                last = overflow[-1]
                last['message'] = 'UOL — supervisão\nDiversos incidentes ocorreram; revisar estado operacional no Oracle.'
                last['critical'] = any(x['critical'] for x in overflow)
                active = kept + [last]
            self.state['outbox'] = completed[-max(0, MAX_OUTBOX-len(active)):] + active if len(active) < MAX_OUTBOX else active

    def run_once(self, sources=None, deliver=True):
        now = self.clock()
        sources = sources if sources is not None else self.collect()
        issues = self.evaluate(sources, now)
        changes = []
        previous_run = instant(self.state.get('lastRunAt'))
        if previous_run is not None and now - previous_run > 600:
            changes.append(('Supervisão retomada após intervalo sem execução; causa da interrupção não determinada.', True))
        previous = self.state['incidents']
        for key, issue in issues.items():
            if not previous.get(key, {}).get('active'):
                changes.append(('ALERTA: ' + issue['message'], issue['critical']))
                previous[key] = {**issue, 'active': True, 'openedAt': iso(now)}
        for key, old in previous.items():
            if old.get('active') and key not in issues:
                # Advancing a credential deadline is escalation, not recovery.
                escalated = key.startswith('credential:') and any(k.startswith(':'.join(key.split(':')[:2]) + ':') for k in issues)
                if not escalated and not self.can_recover(key, sources, now):
                    continue
                if not escalated:
                    changes.append(('RECUPERADO: ' + key.split(':', 1)[0] + ' voltou a apresentar evidência de funcionamento.', True))
                old.update(active=False, recoveredAt=iso(now))
        # Retain only current incidents and recent recoveries; the notification
        # outbox keeps its independent bounded delivery evidence.
        self.state['incidents'] = {key: value for key, value in previous.items()
            if value.get('active') or now - (instant(value.get('recoveredAt')) or now) < 30 * 86400}
        self.queue(changes, now)
        self.state['lastRunAt'] = iso(now)
        self.state['publicationAttempts'] = [x for x in self.state['publicationAttempts'] if x > now - 86400]
        self.save()
        if deliver:
            self.flush()
        return {'activeIncidents': sum(bool(x.get('active')) for x in previous.values()),
                'pendingNotices': sum(x['outcome'] == 'pending' for x in self.state['outbox']),
                'attempts24h': len(self.state['publicationAttempts'])}

    def flush(self):
        now = self.clock()
        if now < self.state.get('publicationNotBefore', 0):
            return
        for entry in sorted(self.state['outbox'], key=lambda x: (not x['critical'], x['due'])):
            if entry['outcome'] == 'in_flight':
                # A crash after network acceptance is ambiguous; never claim sent.
                entry['outcome'] = 'uncertain'
                self.save()
            if entry['outcome'] != 'pending' or entry['due'] > now:
                continue
            count = len(self.state['publicationAttempts'])
            ceiling = MAX_ATTEMPTS if entry['critical'] else MAX_ATTEMPTS - RESERVED_ATTEMPTS
            if count >= ceiling:
                continue
            entry.update(outcome='in_flight', attempts=entry['attempts'] + 1)
            self.state['publicationAttempts'].append(now)
            self.save()  # Reserve attempt before HTTP, including uncertain outcomes.
            try:
                result = self.sender(entry)
            except Exception:
                result = {'outcome': 'uncertain'}
            outcome = result.get('outcome', 'uncertain')
            retry_after = result.get('retryAfter', 0)
            if isinstance(retry_after, (int, float)) and retry_after > 0:
                self.state['publicationNotBefore'] = now + retry_after
            if outcome == 'retry' and entry['attempts'] < 3:
                delay = 60 if entry['attempts'] == 1 else 300
                entry.update(outcome='pending', due=now + max(delay, retry_after))
            else:
                entry['outcome'] = outcome if outcome in ('accepted', 'rejected', 'uncertain') else 'failed'
            entry['lastAttemptAt'] = iso(now)
            self.save()
            break  # Grouped notifications, bounded network work per timer execution.


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', required=True)
    parser.add_argument('--config-file', required=True)
    parser.add_argument('--observe-only', action='store_true')
    args = parser.parse_args()
    directory = Path(args.data_dir)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid()):
        raise ValueError('private_directory_required')
    config = private_json(args.config_file, 16384)
    with open(directory / 'supervisor.lock', 'a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = Supervisor(directory, config).run_once(deliver=not args.observe_only)
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print(json.dumps({'ok': False, 'error': 'supervisor_failed'}))
        raise SystemExit(1)
