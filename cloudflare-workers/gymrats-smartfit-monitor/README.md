# GymRats Smart Fit monitor

Dedicated Worker. Polls account 336445 every five minutes in America/Sao_Paulo.
Detects `academia.brand == smart_fit` or a Smart Fit title and groups challenge
copies by `workout_entry_id`. Only today's events occurring after the initial
monitor run qualify. No historical backfill or GymRats workout writes.

## Deployment and current boundary

`npm ci`, `npm test`, `npx wrangler deploy --dry-run`, `npm run deploy`.
After the first deploy, `node scripts/configure.mjs` copies the existing local
GymRats credential into the dedicated KV and sets an admin secret. It does not
print credentials. Run it only for initial setup or intentional credential repair:
it replaces the KV token with the locally stored one.

The committed configuration intentionally has `DELIVERY_ENABLED=false`.
Oracle's existing Beeper API was checked on September 16: accounts contain no
iMessage network, and the requested chat returns 404. Its HTTPS gateway only
accepts `/v1/send-offer` and `/v1/send-buyticket`, with fixed destinations.
No Oracle file, service, account, or existing Worker was changed for this monitor.

Follow-up: authenticated Beeper MCP and Desktop API on the Mac now confirm the
exact iMessage chat. It is a local macOS integration; the Oracle profile cannot
substitute for it. A dedicated, restricted adapter is prepared in
`services/gymrats-beeper-relay`, and the local relay passed authenticated lookup
(HTTP 200) and unauthenticated denial (HTTP 401). Oracle routing and Worker
activation still await the exception to the original no-Oracle-changes scope.

To enable delivery, first provide an authenticated HTTPS Beeper REST base URL
whose `GET /v1/chats/{encoded_chat_id}` returns the exact configured iMessage ID.
Store `BEEPER_API_URL` and `BEEPER_API_TOKEN` with `wrangler secret put`, then set
`DELIVERY_ENABLED=true` and deploy. A URL must be a base prefix, without `/v1`,
query, fragment, or embedded credentials. Never expose the raw local Beeper API
without a scoped authenticated gateway. Do not repurpose the UOL gateway.

## Delivery semantics

One SQLite Durable Object coordinates this account. It prevents overlapping
polls, rate-limits polls to one per five minutes, and flushes a permanent entry
reservation before sending the exact text `Você entrou em uma Smart Fit`.
The REST POST carries an `Idempotency-Key`, but correctness does not assume
the remote server implements that header.

At most one POST attempt is made per entry. Any POST error, timeout, missing
receipt, or interrupted attempt requires manual reconciliation; the entry is
never automatically retried. This prevents automatic duplicate sends at the cost
of possibly missing an alert during an ambiguous failure. It is not an
exactly-once delivery guarantee. Beeper returns a `pendingMessageID`; the monitor
resolves that exact receipt with GET and only marks it accepted when the bridge
reports `sendStatus.status=SUCCESS`, with matching chat, sender and text.
Pending receipts are checked again without sending another POST, for up to 24
hours; unresolved receipts then require manual review. No read-receipt guarantee.
Contract: https://developers.beeper.com/desktop-api-reference/typescript/resources/messages/

`LAST_NOTIFIED_WORKOUT_ENTRY_ID` in KV is a repairable compatibility checkpoint;
SQLite's full per-entry ledger is authoritative and is never automatically pruned.
Do not clear or roll back that ledger or create a replacement DO identity.
KV is not used as a distributed lock. A KV checkpoint failure cannot resend an
accepted entry. Disabled delivery neither reserves nor marks a workout notified.

JWT refresh occurs only when the successful workout response actually includes
a different `data.token`; plain `data: []` does not refresh a token. HTTP 401
is exposed as an error; no login, credential guessing, or undocumented refresh
endpoint is attempted. API responses and tokens are never logged.

## Status and verification

`GET /health` is public deployment liveness only. Authenticated `GET /status`
reports last collection and delivery states. Authenticated `POST /run` runs one
rate-limited collection. Admin credential is stored locally with mode 0600 under
`~/.config/gymrats-smartfit-monitor/admin-token`. No public diagnostic endpoint
exposes workouts, account contents, tokens, message receipts, or schedules.

Tests cover São Paulo midnight, baseline, copies, entry IDs, concurrent polls,
KV token rotation, durable restart dedupe and ambiguous delivery. The installed
Miniflare cannot run the production September 16 compatibility date; local
runtime tests explicitly use its supported August 6 date. Production keeps
September 16 and requires a separate deployed collection check.

At five minutes: at most 288 normal polls/day, approximately 288 DO invocations,
288 GymRats fetches and 288 JWT KV reads. A token changing on every poll would
add up to 288 KV writes/day; actual notification/checkpoint traffic is additional.
SQL and DO duration have separate account-wide quotas. September 15 UTC account
analytics before this addition: 1,015 Workers requests and 5,834 DO requests.
The new Cron occupies the fifth Free-tier slot. This is not a guarantee of all
other projects' future usage or an end-to-end iMessage validation.
