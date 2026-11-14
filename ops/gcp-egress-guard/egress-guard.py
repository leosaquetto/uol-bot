#!/usr/bin/env python3
"""Egress guard for the GCP Always Free e2-micro (1 GB/month of free outbound traffic).

Every 15 minutes, adds the interface's transmitted bytes since the previous run (reboot-safe via boot_id) to a
per-month total, and sends one saquettoOS notice (ntfy bridge) the first time the month crosses 50%, 80% and 100% of
the free allowance, with the top processes by open remote connections as a hint. The counter is the NIC, so it is an
upper bound: traffic to some Google services is free. Created 14/11/2026 after 8.5 GB showed up on the NIC in 2 days.
"""
import datetime, json, os, subprocess, sys, urllib.request

IFACE = os.environ.get('IFACE', 'ens4')
LIMIT = float(os.environ.get('EGRESS_LIMIT_BYTES', 1e9))
THRESHOLDS = (0.5, 0.8, 1.0)
NTFY_CONFIG = os.environ.get('NTFY_CONFIG', '/etc/uol-ready-monitor/ntfy.json')
STATE_FILE = os.environ.get('STATE_FILE', '/var/lib/egress-guard/state.json')


def tx_bytes():
    for line in open('/proc/net/dev'):
        if line.strip().startswith(IFACE + ':'):
            return int(line.split(':', 1)[1].split()[8])
    raise RuntimeError(f'interface {IFACE} not found')


def notify(identifier, title, body):
    config = json.load(open(NTFY_CONFIG))
    envelope = {'id': identifier, 'source': 'saquettoos', 'title': title, 'body': body[:3800], 'route': 'saquettoOS://home'}
    payload = {'topic': config['topic'], 'title': title, 'priority': 4,
               'message': json.dumps(envelope, ensure_ascii=False, separators=(',', ':'))}
    request = urllib.request.Request(config['server'].rstrip('/') + '/', data=json.dumps(payload).encode(), method='POST',
                                     headers={'Content-Type': 'application/json'})
    urllib.request.urlopen(request, timeout=20).read()


def main():
    boot = open('/proc/sys/kernel/random/boot_id').read().strip()
    month = datetime.date.today().strftime('%Y-%m')
    now = tx_bytes()
    try:
        state = json.load(open(STATE_FILE))
    except (FileNotFoundError, ValueError):
        state = {}
    if state.get('month') != month:
        # First run ever counts everything since boot (an upper bound); a new month starts from zero.
        state = {'month': month, 'total': 0 if state else now, 'announced': [], 'boot': boot, 'last': now}
    delta = now - state['last'] if state.get('boot') == boot and now >= state['last'] else now
    state.update(total=state['total'] + delta, boot=boot, last=now)
    share = state['total'] / LIMIT
    crossed = [t for t in THRESHOLDS if share >= t and t not in state['announced']]
    print(json.dumps({'month': month, 'egressBytes': state['total'], 'share': round(share, 3), 'crossed': crossed}))
    if crossed:
        hint = subprocess.run("ss -tnpH state established | awk '{print $NF}' | grep -o '\"[^\"]*\"' | sort | uniq -c | sort -rn | head -5",
                              shell=True, capture_output=True, text=True).stdout.strip()
        level = max(crossed)
        notify(f'gcp-egress-{month}-{int(level * 100)}', f'GCP: saída em {share:.0%} do grátis',
               f"A gcp-box enviou {state['total'] / 1e9:.2f} GB em {month} (grátis: {LIMIT / 1e9:.0f} GB/mês; contador da placa, "
               f"parte pode ser tráfego grátis para o Google). Acima do grátis o Google cobra por GB.\n"
               f"Conexões abertas por processo:\n{hint or '-'}")
        state['announced'] = sorted(set(state['announced']) | set(crossed))
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    json.dump(state, open(STATE_FILE + '.tmp', 'w'))
    os.replace(STATE_FILE + '.tmp', STATE_FILE)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'egress-guard failed: {type(error).__name__}: {str(error)[:300]}', file=sys.stderr)
        sys.exit(1)
