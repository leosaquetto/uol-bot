#!/bin/bash
# Run from the repository root on the Mac: ops/gcp-ready-monitor/install.sh <path to saquettoOS ntfy.json>
set -euo pipefail
W=cloudflare-workers/uol-telegram-shadow-worker
T=$(mktemp -d)
mkdir -p "$T/app/scripts" "$T/app/src"
cp "$W/scripts/monitor-ready.mjs" "$T/app/scripts/"
cp "$W"/src/{headless-health,health-contract,beeper-gateway-health}.js "$T/app/src/"
echo '{"type":"module","private":true}' > "$T/app/package.json"
cp ops/gcp-ready-monitor/ready-alert.py "$T/app/"
cp ops/gcp-ready-monitor/uol-ready-monitor.{service,timer} "$T/"
cp "$1" "$T/ntfy.json"
ssh gcp-box 'rm -rf /tmp/uol-ready-install && mkdir -m 700 /tmp/uol-ready-install'
scp -q -r "$T"/* gcp-box:/tmp/uol-ready-install/
rm -rf "$T"
ssh gcp-box 'set -e; cd /tmp/uol-ready-install
command -v node >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nodejs >/dev/null
sudo rm -rf /opt/uol-ready-monitor && sudo cp -r app /opt/uol-ready-monitor
sudo install -d -m 700 /etc/uol-ready-monitor && sudo install -m 600 ntfy.json /etc/uol-ready-monitor/ntfy.json
sudo install -m 644 uol-ready-monitor.service uol-ready-monitor.timer /etc/systemd/system/
cd / && rm -rf /tmp/uol-ready-install
sudo systemctl daemon-reload && sudo systemctl enable --now uol-ready-monitor.timer'
