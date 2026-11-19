#!/usr/bin/env python3
"""UOL readiness monitor on the GCP Always Free box (replaces the 5-minute Actions schedule on the Simular runners).

Runs the repository's monitor-ready.mjs without a GitHub token (no issue updates) and, when the state or its reasons
change, sends one notice through the ntfy -> saquettoOS bridge (NotificationEnvelope JSON). A healthy first run is
silent; a later return to healthy sends a recovery notice. Light by design: one node process every 5 minutes.
"""
import hashlib, json, os, subprocess, sys, time, urllib.request

APP = os.environ.get('APP_DIR', '/opt/uol-ready-monitor')
NTFY_CONFIG = os.environ.get('NTFY_CONFIG', '/etc/uol-ready-monitor/ntfy.json')
STATE_FILE = os.environ.get('STATE_FILE', '/var/lib/uol-ready-monitor/state.json')
# Reasons that are the Worker protecting the free tier on purpose, with readyz still 200: the
# Oracle quota observer defers optional maintenance when the day's forecast passes 85%
# (17/11 09:30-10:50 BRT). Alone they only alert if they last longer than SOFT_GRACE_SECONDS.
# version_mismatch: right after a deploy /livez (stateless) already answers with the new version
# while /readyz (through the Durable Object) keeps the previous one until it restarts. It sent a
# false "UOL: outage" during the 18/11 release; it only alerts if it lasts past its grace.
SOFT_GRACE = {
    'maintenance_deferred': int(os.environ.get('SOFT_GRACE_SECONDS', str(2 * 3600))),
    'version_mismatch': int(os.environ.get('VERSION_GRACE_SECONDS', str(15 * 60))),
}
SOFT_REASONS = set(SOFT_GRACE)


def notify(identifier, title, body):
    with open(NTFY_CONFIG) as handle:
        config = json.load(handle)
    envelope = {'id': identifier, 'source': 'saquettoos', 'title': title, 'body': body[:3800], 'route': 'saquettoOS://home'}
    payload = {'topic': config['topic'], 'title': title, 'priority': 4,
               'message': json.dumps(envelope, ensure_ascii=False, separators=(',', ':'))}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    if config.get('token'):
        headers['Authorization'] = 'Bearer ' + config['token']
    request = urllib.request.Request(config['server'].rstrip('/') + '/', method='POST', headers=headers,
                                     data=json.dumps(payload, ensure_ascii=False).encode())
    with urllib.request.urlopen(request, timeout=20) as response:
        receipt = json.loads(response.read(65536))
    if receipt.get('event') != 'message' or receipt.get('topic') != config['topic']:
        raise RuntimeError('ntfy did not confirm the message')


def main():
    env = {k: v for k, v in os.environ.items() if not k.startswith('GITHUB_')}
    run = subprocess.run(['node', 'scripts/monitor-ready.mjs'], cwd=APP, env=env, capture_output=True, text=True, timeout=120)
    lines = [line for line in run.stdout.splitlines() if line.startswith('{')]
    if run.returncode != 0 or not lines:
        raise RuntimeError(f'monitor exit {run.returncode}: {run.stderr[-300:]}')
    result = json.loads(lines[-1])
    state, reasons = result.get('state', 'unknown'), sorted(set(result.get('reasons') or []))
    signature = hashlib.sha256(json.dumps({'state': state, 'reasons': reasons}).encode()).hexdigest()[:16]
    print(json.dumps({'state': state, 'reasons': reasons, 'signature': signature}))
    try:
        with open(STATE_FILE) as handle:
            previous = json.load(handle)
    except (FileNotFoundError, ValueError):
        previous = None
    soft = state != 'healthy' and bool(reasons) and set(reasons) <= SOFT_REASONS
    if (previous or {}).get('signature') == signature:
        if (previous or {}).get('softSince'):
            save({'state': previous.get('state'), 'signature': signature})
        return
    if soft:
        now = time.time()
        since = previous.get('softSince') if (previous or {}).get('softSignature') == signature else None
        since = since or now
        grace = min(SOFT_GRACE[reason] for reason in reasons)
        if now - since < grace:
            save({**{k: v for k, v in (previous or {}).items() if k in ('state', 'signature')},
                  'softSignature': signature, 'softSince': since})
            return
        cause = ('versão nova sem propagar' if 'version_mismatch' in reasons
                 else 'proteção de cota que não liberou')
        reasons_note = f"\nHá {int((now - since) // 60)} min ({cause})."
    else:
        reasons_note = ''
    if state == 'healthy':
        if previous is not None:
            notify(f'uol-ready-{signature}-{os.urandom(4).hex()}', 'UOL normalizado', 'O Worker e o gateway UOL voltaram ao normal.')
    else:
        snap = result.get('snapshot') or {}
        body = (f"Estado: {state}\nMotivos: {', '.join(reasons) or '-'}\n"
                f"Worker readyz HTTP {snap.get('readinessStatus')}, livez HTTP {snap.get('livenessStatus')}." + reasons_note)
        notify(f'uol-ready-{signature}-{os.urandom(4).hex()}', f'UOL: {state}', body)
    save({'state': state, 'signature': signature})


def save(data):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE + '.tmp', 'w') as handle:
        json.dump(data, handle)
    os.replace(STATE_FILE + '.tmp', STATE_FILE)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:  # visible in the journal; the next run retries
        print(f'uol-ready-monitor failed: {type(error).__name__}: {str(error)[:300]}', file=sys.stderr)
        sys.exit(1)
