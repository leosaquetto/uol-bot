# Beeper preview gateway

Private sidecar for the UOL Worker. It accepts only authenticated Clube UOL offer
links, pins delivery to the configured Beeper chat, and uses Beeper Server's
private local transport to attach the title, summary, and image to WhatsApp's
native link preview.

Required environment:

- `GATEWAY_TOKEN`
- `BEEPER_CHAT_ID`
- `BEEPER_ACCESS_TOKEN`
- `BEEPER_API_URL` (defaults to `http://127.0.0.1:23373`)
- `BEEPER_TRANSPORT_NONCE` and `BEEPER_ACCOUNT_ID` (enable the headless account bootstrap)
- `DATA_PATH` (defaults to `/var/lib/beeper-preview-gateway/deliveries.sqlite`)

The service binds to `127.0.0.1:8787`. Caddy provides public HTTPS. The raw
Beeper API must remain bound to localhost.

`deploy/beeper-profile-server.service` gives Beeper Server a persistent private
transport nonce. The gateway uses the same nonce to initialize the configured
account after every reboot; it never proxies that transport publicly.

Enable lingering for the Beeper Server user (`loginctl enable-linger ubuntu`).
Without it, systemd stops the user service when the last SSH session closes.
The gateway data directory must be traversable by that user and its `previews/`
subdirectory readable; keep the SQLite files private (`0600`).
