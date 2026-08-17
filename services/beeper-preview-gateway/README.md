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

Health endpoints:

- `GET /livez`: process liveness.
- `GET /readyz`: public readiness for the ledger, headless transport, and
  configured Beeper chat.
- `GET /v1/readyz`: the same no-send probe authenticated with
  `Authorization: Bearer <GATEWAY_TOKEN>`. It also returns aggregate ledger
  counts, so the Worker-to-gateway token and route can be checked safely.

Successful delivery means that Beeper's local transport accepted the request
and returned a `pendingMessageID`; it does not prove final WhatsApp delivery.
The idempotency ledger records that state as `accepted`. A timeout after the
transport request is stored as `unknown` and is never retried automatically,
preventing duplicate messages.

Preview images remain allowlisted. An unsupported URL or failed download is
omitted without blocking the validated offer text and link.

The service writes one-line JSON events for send decisions and transport state.
They contain a generated request ID and a short hash of the idempotency key,
never authorization values, message text, offer URLs, previews, or chat IDs.

Enable lingering for the Beeper Server user (`loginctl enable-linger ubuntu`).
Without it, systemd stops the user service when the last SSH session closes.
The gateway data directory must be traversable by that user and its `previews/`
subdirectory readable; keep the SQLite files private (`0600`).
