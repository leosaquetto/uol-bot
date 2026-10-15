# UOL delivery reliability — 15 October 2026

Scope: UOL and required shared delivery components. Preserve the free tiers,
all queues, ledgers and sessions. Instagram stays `push_only`: no manual GET,
login, polling or synthetic event. No schema migration is required.

## Release evidence

P1 commit `0e9ea5cf307f118d35bb32636d5492b27299e472` passed all four jobs in
[CI](https://github.com/leosaquetto/uol-bot/actions/runs/38818969180).
Worker version `d3650f63-2be6-46a7-a5ae-e76e0e81cffc` was published and completed
a natural primary cycle. Postdeploy: `published=true`, `runtimeVerified=true`,
`ready=false`. The first check observed version propagation lag; a read-only
recheck after both endpoints returned the new version passed. No second deploy
was needed. Oracle source hashes match that commit; the gateway, Drops, monitor,
push receiver and supervisor timer are active. Source backups remain private on
Oracle, outside service state.

Historical readiness debt at this release: three critical incidents, two dead
letters, four uncertain deliveries and one exhausted maintenance operation.
Those are unresolved records, not failed deployment evidence.

## Operational contracts

- Main, secondary and Discord transport calls require an atomic SQL claim.
  Outcomes match the same generation, attempt and reservation timestamp. A late
  receipt belongs in the ledger; it must not replace a subsequent reservation.
- Retention excludes uncertain, active, unfinished comment, WhatsApp and
  availability synchronization records before age/count limits.
- Drops identity is saved before dispatch. Recovery looks up the same route,
  key digest and request hash; missing receipts never authorize another POST.
- Supervisor recovery needs five minutes and two distinct fresh positive
  observations. A gap over ten minutes restarts the window. An episode can
  escalate to critical once. Interrupted outbox reservations become uncertain;
  attempted records and receipts cannot be coalesced away.
- Terminal push storage faults clean up and exit nonzero. The installed
  `Restart=on-failure` service recovers it; an administrative stop remains normal.

## Diagnostics and reconciliation

Authenticated `GET /inventory?id=<exact-id>` bypasses the recent 300-item limit.
Its response contains states, attempts, receipt IDs and a bounded timeline, with
no offer content. A missing ID returns 404. Inspect the original destination,
forward/thread and all expected chunks before using `/resolve-delivery`.
Partial comment proof preserves the uncertain remainder. Every resolution and
requeue is recorded in the ledger.

`POST /requeue-delivery` with `main_sold_out` or `canal2_sold_out` grants one more
edit of the existing message. Diagnose the stored error first. It preserves the
other destination and never creates a replacement post.

Authenticated Drops lookup is
`GET /v1/gateway/jobs/by-key/<route>/<keyHash>/<requestHash>` on loopback, using
the existing gateway token. An absent key returns 404; a hash mismatch returns
409. Do not infer a non-delivery from either response for an already uncertain
request.

The public gateway `/readyz/uol` excludes personal/BuyTicket readiness. The Worker
uses authenticated `/v1/readyz/uol`; global readiness endpoints keep their own
scope. Monitoring persists the latest state and canonical reason signature in
one issue update, including transitions back to an earlier state.

## Release gate and outstanding evidence

`npm run release` requires a clean main checkout, exact remote SHA and successful
CI for that SHA. It compares authenticated `/release-debt` snapshots against
`~/.config/uol-ticket-research/release-debt-baseline.json` (0600). New identities
block the next batch even if totals are unchanged. The baseline is not replaced
on subsequent releases. The one-time `--bootstrap-debt` initializes it only when
absent and reports `debtVerified=false`; this is separate from runtime proof.

Quota reports expose temporal sampling, metric interpretation and blocking
metrics separately. Keep Durable Object `hibernation` request billing unknown
until its meaning is proved; the WebSocket 20:1 rule cannot be applied to every
request. [Cloudflare pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/).
Three complete UTC days must also prove guard ingestion and delivery behavior;
raw cumulative counters alone do not satisfy that acceptance.

The technical UOL credential expires on 23 October. The repository's previous
investigation did not find the original acquisition/renewal procedure (see
`2026-09-08-uol-quota.md`). Do not guess an authentication endpoint or replace a
working secret without a validated renewal flow.

Final Story acceptance requires the next natural push, durable intake, valid
collection and destination receipts. Local session-file validity is shown
separately and does not prove current remote authentication or Story delivery.
