# Beeper preview gateway

Private sidecar for the UOL Worker. It accepts only authenticated Clube UOL offer
links, pins delivery to the configured Beeper chat, and restores the native
WhatsApp card by passing Beeper's original `links[]` preview payload through the
private local transport. The Worker only sees success after the local Beeper
index records final bridge status `SUCCESS` for that exact pending message.

Required environment:

- `GATEWAY_TOKEN`
- `BEEPER_CHAT_ID`
- `BEEPER_ACCESS_TOKEN`
- `BEEPER_API_URL` (defaults to `http://127.0.0.1:23373`)
- `BEEPER_TRANSPORT_NONCE` and `BEEPER_ACCOUNT_ID` (enable the headless account bootstrap)
- `BEEPER_INDEX_DB_PATH` (read-only Beeper index used for final bridge confirmation)
- `DATA_PATH` (defaults to `/var/lib/beeper-preview-gateway/deliveries.sqlite`)

The service binds to `127.0.0.1:8787`. Caddy provides public HTTPS. The raw
Beeper API and private transport must remain bound to localhost.

`deploy/beeper-profile-server.service` gives Beeper Server a persistent private
transport nonce. The gateway uses the same nonce to initialize the configured
account and restore the original `links[]` preview payload during delivery. The
private transport is never proxied publicly.

Health endpoints:

- `GET /livez`: process liveness.
- `GET /readyz`: public readiness for the ledger, headless transport, and
  configured Beeper chat and read-only delivery confirmation index.
- `GET /v1/readyz`: the same no-send probe authenticated with
  `Authorization: Bearer <GATEWAY_TOKEN>`. It also returns aggregate ledger
  counts, so the Worker-to-gateway token and route can be checked safely.

Successful delivery means that Beeper replaced the pending echo with a final
event whose bridge status is `SUCCESS`. When an image was supplied, the final
event must also retain the preview image in `links[]`.
The idempotency ledger records that state as `accepted`. A timeout or an
ambiguous upstream response after the request is stored as `unknown` and is
never retried automatically, preventing duplicate messages. Only a definitive
pre-dispatch rejection releases the same idempotency key for retry.

The service writes one-line JSON events for send decisions and transport state.
They contain a generated request ID and a short hash of the idempotency key,
never authorization values, message text, offer URLs, previews, or chat IDs.

Enable lingering for the Beeper Server user (`loginctl enable-linger ubuntu`).
Without it, systemd stops the user service when the last SSH session closes.
Keep the gateway SQLite files private (`0600`).
