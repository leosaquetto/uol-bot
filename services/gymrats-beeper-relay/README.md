# GymRats iMessage relay

Worker → Oracle Caddy `/gymrats/` → loopback SSH reverse tunnel → dedicated Mac
relay → authenticated Beeper Desktop API → iMessage. iMessage is macOS-only;
the Oracle Linux Beeper profile is not the iMessage sender. The Mac and Beeper
must remain running and connected. No generic Beeper API is exposed.

The API on the Mac confirmed the user's exact `imsg##thread:...` destination on
September 16. Earlier direct SQLite and Oracle-only searches were insufficient:
the local integration is available through the authenticated Desktop API even
when it does not appear in the queried index/account list.

## Security and delivery

`relay.py` binds only 127.0.0.1:18788. The authenticated relay accepts only the
fixed iMessage destination, the exact Smart Fit notification, and numeric
`gymrats:336445:<entry_id>` keys. GET receipts must belong to its own permanent
ledger. It never returns chat history, participants, other accounts, or tokens.
The Beeper OAuth token remains on the Mac. Worker and relay share a different,
scoped token. SQLite reserves each key before POST, and duplicate/uncertain
requests never create a second send. HTTP redirects are not followed.

## Installation

Complete the official Beeper OAuth flow and save its result privately at
`~/.config/gymrats-smartfit-monitor/beeper-oauth.json` (mode 0600).
`python3 install-mac.py` installs only the dedicated local relay; add `--tunnel`
to also install the reconnecting SSH LaunchAgent after Oracle changes are
authorized. Existing UOL/WhatsApp services are not restarted by this installer.
Private configuration, ledger and sanitized logs are outside the repository.

Oracle requires one additive Caddy handler in its existing HTTPS host:

```caddyfile
handle_path /gymrats/* {
    reverse_proxy 127.0.0.1:18789
}
```

Preserve all existing routes, back up Caddyfile privately, validate before reload,
and compare existing UOL readiness before/after. This addition requires the
user's exception to their original no-Oracle-changes constraint. No public SSH
forward: `GatewayPorts no` and explicit loopback bind are retained.

Set the Worker's secret `BEEPER_API_URL` to
`https://163-176-194-58.sslip.io/gymrats` and `BEEPER_API_TOKEN` to the private
relay token through stdin. Verify unauthenticated denial and authenticated exact
destination lookup before enabling delivery. No synthetic check-in or unsolicited
test message is necessary; natural delivery requires separate live confirmation.

Rollback: disable Worker delivery, remove only the `/gymrats/` Caddy handler,
validate/reload Caddy, and boot out the two `com.leosaquetto.gymrats-beeper-*`
LaunchAgents. Preserve the relay and DO ledgers to prevent duplicate sends.

Tests: `python3 -m unittest -v test_relay.py`.
Sources: https://developers.beeper.com/desktop-api/ and
https://developers.beeper.com/desktop-api/auth/.
