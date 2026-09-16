#!/usr/bin/env python3
"""Install the dedicated Mac relay. --tunnel also starts the Oracle SSH tunnel."""
import json
import os
import plistlib
import secrets
import shutil
import subprocess
import sys
from pathlib import Path

os.umask(0o077)
home = Path.home()
private = home / ".config/gymrats-smartfit-monitor"
private.mkdir(parents=True, exist_ok=True, mode=0o700)
oauth = private / "beeper-oauth.json"
if not oauth.exists():
    raise SystemExit("Complete Beeper OAuth before installing the relay.")
token = private / "relay-token"
if not token.exists():
    token.write_text(secrets.token_hex(32))
token.chmod(0o600)
runtime = home / ".local/share/gymrats-beeper-relay"
runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
shutil.copyfile(Path(__file__).with_name("relay.py"), runtime / "relay.py")
config = private / "relay.json"
config.write_text(json.dumps({"gateway_token": token.read_text().strip(), "beeper_oauth_path": str(oauth), "database": str(private / "relay.sqlite")}))
config.chmod(0o600)
agents = home / "Library/LaunchAgents"
agents.mkdir(parents=True, exist_ok=True)


def install(label, arguments):
    plist = agents / (label + ".plist")
    value = {"Label": label, "ProgramArguments": arguments, "RunAtLoad": True,
             "KeepAlive": True, "ThrottleInterval": 15, "ProcessType": "Background",
             "StandardOutPath": str(private / (label + ".log")),
             "StandardErrorPath": str(private / (label + ".error.log"))}
    plist.write_bytes(plistlib.dumps(value))
    domain = "gui/" + str(os.getuid())
    subprocess.run(["launchctl", "bootout", domain + "/" + label], capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True, capture_output=True)
    print(label + " installed")


install("com.leosaquetto.gymrats-beeper-relay", [sys.executable, str(runtime / "relay.py"), str(config)])
if "--tunnel" in sys.argv:
    install("com.leosaquetto.gymrats-beeper-tunnel", [
        "/usr/bin/ssh", "-NT", "-i", str(home / ".ssh/oracle-beeper-ed25519"),
        "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3", "-o", "ConnectTimeout=10",
        "-R", "127.0.0.1:18789:127.0.0.1:18788", "ubuntu@163.176.194.58",
    ])
