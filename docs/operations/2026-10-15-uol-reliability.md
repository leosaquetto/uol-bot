# UOL delivery reliability — 15 October 2026

Scope: UOL and required shared delivery components. Preserve the free tiers,
all queues, ledgers and sessions. Instagram stays `push_only`; current diagnostic
exceptions are recorded below. No automatic login, periodic polling, synthetic
event or schema migration is required.

## Current Instagram authorization — 16 October

The user corrected the execution target: collection must run on the existing
Oracle server, without requiring a local Chrome extension. They prioritized
receiving an actual Story push over five-minute polling and explicitly allowed
FBNS to become the main push channel of the dedicated account, including possible
interference with phone notifications. They also required caution about
checkpoint risk and private-protocol fragility. No automatic login, session
replacement, IP rotation or invented hardware attestation is authorized.

The collector remains `push_only`; periodic HTTP collection was not activated.
The controlled FBNS diagnostic normally waits for the global HTTP 429 backoff;
the user subsequently authorized the scoped registration exception recorded below.
Its account-association attempt is durable;
uncertain/rejected responses and protocol/auth/security failures stop for review.
`leotheblueblur` is a diagnostic source only, isolated from UOL destinations.
Current implementation and acceptance details: `services/uol-ticket-research/INSTAGRAM-FBNS.md`.
The prior extension instructions below are historical and no longer required.

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

P2 commit `13d41f199bdf40942e3d9f43de6752a2bd9aae55` passed all four jobs in
[CI](https://github.com/leosaquetto/uol-bot/actions/runs/38822215024).
Worker version `22966d2b-1a83-4e0e-86d4-155e917382ee` was published on
16 October UTC (15 October in São Paulo). Both version endpoints converged;
the read-only postdeploy recheck passed with `published=true`,
`runtimeVerified=true`, `debtVerified=true`, `ready=false`, no debt regression,
and a natural primary cycle at `2026-10-16T02:49:41.828Z`. Propagation exceeded
the first verification window; no second publication was performed.

Oracle source hashes for the ten P2 files match that commit. Gateway, Drops,
monitor, push receiver and supervisor timer are active. Gateway `/readyz/uol`
returns HTTP 200; the Worker confirms the Baileys acknowledgement contract.
Local session validation is valid and explicitly reports
`authenticationVerified=false`; the earlier collection `auth_required` result
is retained. The push receiver is connected; event count remains zero.

A subsequent post-reconciliation check failed the unchanged runtime gate with
`primary_estimate_too_high:517` (maximum 512), while `published=true`,
`debtVerified=true` and the debt comparison had no regressions. The next natural
cycle read 248 rows and reduced the estimate to 491. A profile at
`2026-10-16T02:58:14.101Z` then attributed 564 of 594 reads to ticket-probe query
`60e9f01`, which is unchanged from the pre-plan commit. The earlier passing
runtime check is historical evidence; this later failure must not be hidden or
the threshold relaxed. A single later check also failed at 692. At that point,
runtime acceptance had not been restored; another passing sample alone would
not explain these failures.

The follow-up correction changes only the probe tie-breaker to
`next_at, attempts, offer_id`, matching the existing `ticket_probe_due_v21`
index. Deadline priority is preserved; equal deadlines favor fewer attempts
then a deterministic ID instead of `first_seen_at`. No schema or gate limit
changes. Two native tests cover deadline priority, peer advancement after
rescheduling and real `cursor.rowsRead`: with 282 tied probes, the old query
reads at least 282 rows and the corrected production query reads at most eight
(over tenfold reduction).

Follow-up commit `afe50338f6c10649d0f0645aad63616d3ace0dfb` passed all four jobs
in [CI](https://github.com/leosaquetto/uol-bot/actions/runs/38844268888) and was
published as Worker version `8145d1f4-02f0-4c51-aed5-a05ce783ea36` on 16 October
UTC. Oracle source files are unchanged from the validated P2 deployment.
The first postdeploy window, including 36 attempts, still observed version
skew. Both version endpoints subsequently converged; a read-only recheck passed
with `published=true`, `runtimeVerified=true`, `debtVerified=true`, `ready=false`
and no debt regressions. No republish was needed. Worker/Durable Object code
updates are eventually consistent ([Cloudflare known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/#code-updates)).
The 512 estimate and 4,096 single-cycle limits are unchanged.

Three distinct natural primary cycles on the final version confirm the reduced
cost in production, with a fresh global guard, healthy read/write budgets and
30-second cadence:

| Cycle completed (UTC) | Entire cycle rows read | Probe query `6a1bfc41` rows read | Primary estimate |
| --- | ---: | ---: | ---: |
| 16 October 03:18:41.958 | 25 | 1 | 28 |
| 16 October 03:19:11.952 | 28 | 1 | 28 |
| 16 October 03:19:42.115 | 28 | 1 | 28 |

Immediate post-release checks: **two critical incidents, two dead letters, four
uncertain deliveries, zero exhausted maintenance operations**. The sold-out
incident completed its recovery window. The short runtime proof above does not
replace the three complete UTC-day acceptance below.

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

A follow-up review of the [metrics documentation](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)
did not establish the exact mapping from `dimensions.type=hibernation` and
`sum.requests` to billable units. The pricing discount applies to received
WebSocket messages, not every invocation. Ordinary WebSocket traffic has a
separate metrics dataset; its presence or volume in this account is unproven.
The report therefore keeps `billing_unknown`.

The technical UOL credential expires on 23 October. The repository's previous
investigation did not find the original acquisition/renewal procedure (see
`2026-09-08-uol-quota.md`). Do not guess an authentication endpoint or replace a
working secret without a validated renewal flow.

Final Story acceptance requires the next natural push, durable intake, valid
collection and destination receipts. Local session-file validity is shown
separately and does not prove current remote authentication or Story delivery.

## Reconciliation evidence — 16 October UTC

The aliases below are short SHA-256 references, not destination or message IDs.
Detailed receipts and lookup results remain in the private operator directory
`~/.config/uol-ticket-research/`; they are not committed.

| Record | Sources consulted and result | Remaining evidence |
| --- | --- | --- |
| Sold-out main edit, alias `10377612` | Direct inventory found ten definitive `editMessageText` 400 responses: the message had no text to edit. The deployed correction retries `editMessageCaption` on that exact structured error. One authorized maintenance requeue was accepted; the natural maintenance cycle recorded `synced` at `2026-10-16T02:51:14.021Z`, with the same message ID and confirmed `photo` kind. Secondary receipt, attempts, error and synchronization timestamp are unchanged; Discord receipt is unchanged. | Edit and incident resolved after the stability window. |
| Discord uncertainty, alias `b0fab0af` | Direct lookup reaches the September offer outside the recent-300 inventory. Worker state and delivery timeline retain uncertainty; no Discord receipt ID is stored. The original Beeper account has WhatsApp access, not Discord history. | A matching message ID and content in the original Discord destination. No resend or `not_sent` resolution. |
| Telegram comment uncertainty, alias `4147510b` | Direct lookup confirms the original discussion message, one required chunk, zero confirmed chunk IDs and the preserved ambiguous timeout. No original Telegram history is available through the configured Beeper account. | The existing chunk ID in that exact thread. No extra comment or inferred confirmation. |
| Three legacy WhatsApp facade uncertainties | Follow-up recovered the original outgoing Beeper messages and pending echo IDs. Production `normalizeOfferId` matches every preview URL to its exact offer ID; the reconstructed idempotency key, destination and exact `buildBeeperOfferText` body match. Each unique candidate is not deleted, has `SUCCESS`, a preview and distinct matching message/echo IDs. Production `inspectDeliveryRow(row, echoID, true)` returns `delivered`. The two existing Worker queues were resolved with those references; the third offer returns 404. Drops lookups remain 404. | Delivery is confirmed at the WhatsApp bridge level. Full request metadata-hash equality, direct facade-request-to-pending binding and recipient receipt remain unproved. Facade `unknown` locks remain intact; no replay or missing-offer recreation. |
| Sources and SLO | Current API succeeds with 50 offers, no API error and a fresh success timestamp; listing failure/drop/divergence streaks are zero. Queue uncertainty remains. | Stable current source observations and receipt-backed queue reconciliation. |
| Technical credential | Expiry remains `2026-10-23T04:04:28Z`. Related implementation commits, eight legacy scripts and their specific history, existing operational notes, available archived chat references and the anonymous official client were consulted. The legacy authentication found belongs to GitHub or the removed personal UOL flow. No technical issuer, renewal procedure or newer credential was found. The current secret was preserved. | A verifiable technical issuance/renewal flow, newer expiry and an authenticated primary cycle using only the replacement secret. The user confirmed they do not know the original procedure; do not ask the same question again. |
| ntfy | After the rolling budget released capacity, the published version recorded a natural HTTP 200 at `2026-10-16T08:51:44.989Z`. A timeout at 08:29 remains uncertain; a later HTTP 429 remains subject to retry/backoff. No synthetic send or uncertain replay was attempted. | Service acceptance is proved. Device delivery/read receipt remains outside this HTTP contract. |

### Follow-up WhatsApp reconciliation — 16 October, 03:35 UTC

The previous substring check was case-sensitive. Applying the Worker's actual
identity normalization proved the URL/key match instead of relying only on
timestamp proximity. Exact displayed body, outgoing state, original destination,
preview and bridge receipt checks also passed for all three messages.

Worker offers corresponding to aliases `8020afaf` and `41c30087` were still
uncertain and had no active send. Each received one `sent` resolution with a
stable `beeper-bridge:<receipt-digest>` evidence reference. Readback confirmed
`sent_at`, cleared ambiguity and a matching `resolved_sent` ledger event.
No transport send was issued. The missing Worker offer for alias `2adafa0e` was
not recreated or imported.

The confirmation level is **`confirmed_by_whatsapp_bridge`**. Original request
hashes and receipt IDs are preserved in the private proof. This does not assert
full metadata-hash equality, a retained direct binding to the facade request,
or a recipient receipt. Legacy facade locks are unchanged.

Private evidence: `uol-beeper-bridge-proof-2026-10-16.json` and the two
`uol-whatsapp-resolution-<alias>-2026-10-16.json` records in the operator
directory. Current state at `2026-10-16T03:35:06.456Z`: **two critical incidents,
zero dead letters, two uncertain deliveries, zero exhausted maintenance
operations**. The remaining uncertainty is Discord and the Telegram comment.
The same published version passes runtime validation, with a primary estimate
of 52, a 28-read last cycle, healthy guards and no release-debt regressions.

Credential investigation also checked the existing API's anonymous challenge:
HTTP 401 advertises Bearer authentication without an issuer or token endpoint.
No corresponding local technical credential file or available original archive
provided a renewal flow. The working secret remains unchanged.

### Remaining receipt access — 16 October, 01:20 São Paulo

Fresh verification confirms the same published version, valid runtime, fresh
global guard and no debt regressions. Counts remain two critical incidents,
zero dead letters, two uncertain deliveries and zero exhausted maintenance
operations. All five Oracle services/timers are active. Push is connected with
zero events/signals; local session validity still does not establish remote
authentication.

Related source and operational documentation contain no configured Telegram
history reader, Discord bot reader or documented destination export. The
available browser inventory has no authenticated Telegram/Discord tab. The
comment's 13 October timeout is still within the three-day Free Workers Logs
retention, so a bounded historical query was attempted for that exact offer and
`2026-10-13T15:44:00Z`–`15:51:00Z`, scoped to this Worker. The
[Observability query API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/)
was called with `dry=true`, without persisting a query or changing any Worker
state. Existing Cloudflare authentication returned HTTP 403, code 10000
(`Authentication error`); no log events were accessible. The query and response
are preserved privately in `uol-comment-historical-logs-2026-10-16.json`.

This failed lookup proves an access limitation, not absence of the comment.
Discord's September event is beyond that log-retention window. A matching
message in the original Discord channel and the chunk in the exact Telegram
thread remain required. No access scope was expanded and neither uncertain
delivery was retried. Existing authenticated destination history is the next
independent evidence source when available.

### Scheduled observation 1 — 16 October, 06:05 São Paulo

The deployed tag `git-afe50338f6c1` and both version endpoints still match
`8145d1f4-02f0-4c51-aed5-a05ce783ea36`. At
`2026-10-16T09:05:32.154Z`, checks report `published=true`,
`runtimeVerified=true`, `ready=false`, fresh global guard and no release-debt
identity regression. A natural primary cycle completed at
`2026-10-16T09:05:14.732Z`: 27 rows read, primary estimate 130 (limit 512).
Read/write guards are healthy; the write projection remains 63,488 with 36,512
headroom. These are current guard/projection checks, not complete-day quota
acceptance.

The ntfy acceptance criterion now has production evidence from this same
published version:

| Natural attempt (São Paulo) | HTTP/result | Recorded outcome |
| --- | --- | --- |
| 16 October 05:29:45.096 | Timeout | `uncertain`; no automatic replay |
| 16 October 05:51:44.989 | HTTP 200 | `http_accepted`; service acceptance proved |
| 16 October 06:02:45.039 | HTTP 429 | `rate_limited`; bounded retry/backoff |

Current outbox counts are 21 uncertain, one accepted and 23 pending. The original
20 uncertain attempts were not manually retried; one new natural timeout added
the twenty-first. The code selects only pending records, preserves terminal
uncertainty, and applies the greater of backoff and `Retry-After` for a 429.
The aggregate diagnostic does not expose the exact received `Retry-After` or a
device receipt, so neither is asserted here.

Direct inventory readback confirms the same two uncertainties: Telegram alias
`4147510b` retains zero confirmed chunk IDs, and Discord alias `b0fab0af` has no
message receipt. Counts remain **two critical incidents, zero dead letters,
two uncertain deliveries and zero exhausted maintenance operations**. No
resolution or transport send was issued during this observation.

All five Oracle services/timers are active. The Instagram push receiver is
connected with zero events and zero signals. The session-file check is valid;
the older `auth_required` collection result is preserved and remote
authentication remains unproved. No Instagram collection was triggered.
The technical UOL credential still expires at `2026-10-23T04:04:28Z`; current
API success is fresh. The three complete UTC days (17–19 October) have not yet
elapsed. No quota interpretation, credential, session or tier was changed.

Private evidence: `uol-heartbeat-2026-10-16T0904.json` and
`uol-oracle-heartbeat-2026-10-16T0904.json` in the operator directory.

### User-reported Instagram Story notification — 16 October, 13:29 São Paulo

The user reported a natural Story notification on the phone. The supplied
Instagram activity-feed screenshot shows verified `clubeuol` sharing a Story
"3 min" earlier, with the phone clock at 13:27. This corroborates a recent
mobile notification; it contains no Web Push receipt for the Oracle receiver.

Read-only checks at `2026-10-16T16:25:42.535554Z` and
`2026-10-16T16:29:56.731293Z` both found **zero persisted push events and zero
signals**. The final receiver heartbeat is `2026-10-16T16:29:30.075Z` and its
state is `connected`. Diagnostics include 102 successful handshakes and 506
frames, with no recorded notification, decryption, classification or storage
failure. No payload reaching this subscription is evidenced; the cause of
non-arrival remains unproved.

The only local registration is the one configured in the running service,
with mode 0600, the correct Instagram origin, `bootstrapStatus=created` and an
Instagram registration acceptance of HTTP 200 at
`2026-10-10T20:49:06.955Z`. Its saved acceptance does not identify the account
or establish the current Instagram-to-endpoint binding. A healthy Mozilla
handshake proves the push transport connection; a local session-file check
(`valid`, `authenticationVerified=false`) does not close that separate gap.

The monitor remains `push_only`, with consumed signal sequence zero. Its last
collection result is still the older 15 October 01:46 UTC
`auth_required/invalid_session_file` result, not a new validation of this
Story. The three delivered outbox entries predate this event. Therefore this
mobile notification proves neither a new bot collection nor intake/destination
receipts. No Instagram request, login, re-registration, synthetic event,
collection trigger or transport replay was issued.

Private read-only snapshots: `uol-natural-push-2026-10-16T1625.json`,
`uol-natural-push-2026-10-16-final.json` and
`uol-push-registration-diagnostic-2026-10-16.json` in the operator directory.
The natural Story acceptance criterion remains open.

### Controlled Instagram test and subscription refresh — 16 October, 14:08 São Paulo

The user followed `leotheblueblur`, enabled its Story notifications and published
two test Stories. The user confirmed receipt on the phone and supplied an iOS
notification screenshot with "Novo story". A read-only receiver watch from
`2026-10-16T16:52:34Z` through `17:03:14Z` found **zero push events and signals**,
with the receiver connected and no notification rejection diagnostics. This
proves mobile receipt and absence of an observed server packet in that window;
it does not identify the upstream cause.

The user explicitly authorized one-off notification-settings diagnostics with
the existing session. Authenticated settings responses identify the same account
as the phone. The current Instagram client VAPID key matches the stored key;
the receiver key pair is consistent. The read-only preferences response has
`mute_all_notifications=false`; none of its 44 radio groups is set to `off`.
`first_post=everyone` describes first posts/Stories, not every Story. No setting
in this response proves per-profile or endpoint delivery.

The current public client re-registers its existing PushManager subscription
when the service worker starts with notification permission granted. A bounded
repair now supports one explicit refresh of the previously accepted subscription:
it requires the exact prior acceptance timestamp and matching authenticated
account; creates an exclusive, durable intent before POST; preserves the prior
acceptance and all subscription identity; and records refresh separately. A
crash, timeout, malformed response or HTTP 5xx remains uncertain and cannot
automatically repeat. The existing receiver remains allowed to reconnect.

Code `b6e67032ad08d218c490c53cd5bec8a39cf690d9` passed five focused push tests,
syntax/diff checks and [UOL Worker CI](https://github.com/leosaquetto/uol-bot/actions/runs/38937222586).
Only `push/register.mjs` was installed on Oracle; its SHA-256 readback is
`65813b0123f837bec72748b237c7061b563707b519f737eec53420c60a0cf1f9`.
The Worker version listed above was not republished by this repair.

The single refresh was accepted with **HTTP 200** at
`2026-10-16T17:08:11.225495Z`. Readback confirms the original registration,
endpoint, keys and session file are unchanged. This is registration acceptance,
**not Story delivery**. The receiver was connected with zero events at 17:08:38Z;
a new user-published test after the refresh produced an in-app activity notice,
according to the user, without a confirmed new system notification. The receiver
still had zero events at 17:10:46Z. This comparison remains inconclusive. A DM
control was requested to distinguish general channel silence from Story routing.
Test-account
events remain excluded from production offer intake. Production Story capture,
intake and destination receipts remain unproved until a qualifying natural push.

For the explicit refresh, `register.mjs` accepts the prior `recordedAt` as its
fifth CLI argument; the private fields file must include `accountId` from the
authenticated settings response, matching the session cookie. The original
three-path invocation remains initial registration only. Do not clear the
refresh intent or refresh record to repeat an ambiguous operation.

The user sent the requested DM control. Three natural packets arrived at
`17:11:59.392Z`, `17:12:00.100Z` and `17:12:09.294Z`; all decrypted successfully
with `type=direct_v2_text` and `params.collapse_key=direct_v2_message`. Persistence
and transport ACKs are confirmed. These are the first observed packets and prove
the current subscription can receive and decrypt real Instagram Web Push.
They remain ignored by production Story intake, as required. No DM content was
published or copied into this document. The post-refresh Story comparison still
lacks a corresponding push; the in-app activity entry is a different evidence.

The official Service Worker source confirms root-level `type`/`params` and
interprets `params.ig` as a URL only for `post`. The old classifier expected a
`data` wrapper. Code `167957fae5a4e67165f4595a1694c281558cbcc1` now accepts the
native envelope while retaining exact Instagram HTTPS Story-owner checks and
rejecting native DMs. Recipient IDs, avatar URLs and notification prose are not
owner evidence. No numeric owner or new Story enum was inferred. Four focused
classification/persistence tests, syntax/diff checks and
[CI](https://github.com/leosaquetto/uol-bot/actions/runs/38938713233) passed.

Only `push/classify.mjs` was then installed on Oracle; SHA-256 readback is
`c23e810914046cdbe4e682434cf46ebc23d7c85deffd4d30f14b93e6c6bd913e`.
The first deployment preflight rejected the temporary filename extension;
explicit module syntax checking passed before installation. At
`2026-10-16T17:20:08.877248Z`, the receiver was active and had completed a new
successful handshake after restart. Five stored events remained five, with no
pending packets or production signals. Final inspection at `17:20:45.207Z`
confirmed all five were `direct_v2_text`, decrypted and transport-ACKed.
Thus code publication and natural DM transport are proved; a natural Story
packet, its owner classification, collection, intake and delivery remain open.

Private evidence: `uol-instagram-test-watch-2026-10-16.jsonl`,
`uol-instagram-push-preferences-session-2026-10-16.json`,
`uol-instagram-refresh-2026-10-16.json` and
`uol-instagram-after-refresh-watch-2026-10-16.jsonl`, plus
`uol-instagram-natural-push-types-2026-10-16.json`,
`uol-instagram-native-classifier-2026-10-16.json` and
`uol-instagram-push-verification-final-2026-10-16.json` in the operator directory.
The notification-client diagnostic fetched the Service Worker entry once with
the existing session and its observed static bundle without authentication;
source SHA-256 `829eee39e7d4e061076e86ef24617229e902ca193da4be0f89a0ca2a3462f93c`
is preserved privately. The session file was unchanged.
The remote refresh intent and receipt are retained. No Story GET, polling,
login, session replacement, synthetic event or delivery replay was performed.

## Observation still required

Recomputed reports for 12–14 October have sufficient temporal samples and
verified daily writes of 33,001, 33,527 and 34,865, all below the 70,000 target.
All three reports retain `metricInterpretationComplete=false` and blocking
`doRequests` with `billing_unknown`. Storage sampling on 12 October is also
insufficient. This is historical evidence, not post-release acceptance.

The first complete UTC days after P2 publication are **17, 18 and 19 October**.
Evaluate their final counters and post-day confirmations after 20 October UTC,
together with guard ingestion, discovery, latency and duplicate evidence.
Neither a healthy service nor passing offline crash tests proves a new natural
Story or receipt recovery in production.

A daily heartbeat is scheduled for seven runs. The first observed trigger was
16 October at 06:04 São Paulo; the earlier 09:00 local-time statement had not
been verified and is withdrawn. The schedule was not changed. It checks only
related evidence, keeps the Instagram restrictions above, and remains quiet
without a meaningful change. Publication details and private evidence paths are
recorded in `~/.config/uol-ticket-research/uol-publication-2026-10-16.json`.
The immutable release-debt baseline remains unchanged. Historical uncertainties
stay open until the missing receipts are proved.

### Browser extension collection — 16 October, implementation

The user explicitly authorized bounded periodic Story queries by our extension,
using the existing browser session and configured profiles. This supersedes the
prior push-only restriction for this extension only. The Oracle collector and
receiver remain unchanged in `push_only`; no session export, new login or
synthetic production event is part of this change.

Static inspection of the official public Vivies 1.0.1 and Anonymous Story Viewer
2.2.4 packages confirmed local `chrome.alarms` polling and batched
`feed/reels_media` GETs. Neither inspected version requests native notification
permission. Their behavior is not evidence that Instagram emits Story Web Push.
Primary links and mechanism limits are in `extensions/uol-instagram/README.md`.

The new MV3 extension uses the browser-owned session, a five-minute minimum
interval, at most three profiles and 300 Instagram requests/day in São Paulo.
Only verified `clubeuol` ownership plus a valid campaign sticker and CDN image
can enter the existing dedicated-token intake. Other profiles stay local.
Attempts are persisted before POST; lost responses reconcile by exact identity,
without blind resend. Interrupted or externally resolved receipts are revisited
by GET. UI state exposes no token or signed image URL. Oracle heartbeat state
remains separate from the extension's local diagnostics.

The Worker retention change preserves incomplete, uncertain, malformed and
active Story receipts before age-based removal. Capacity rejection remains
fail-closed. No migration, paid tier, debt-baseline relaxation or historical
resend is involved.

Focused validation: 34 extension parser/engine/MV3/package tests and 29 Worker Story tests
passed offline. Dedicated intake credential was verified by authenticated
read-only HTTP 200 with `ok=true`; no Story POST was sent by this validation.
The local install package is `~/Applications/UOL Instagram`, with private
provisioning excluded from git. The browser tool rejected `chrome://extensions`
by URL policy; no workaround was attempted. Installation and real authenticated
Story discovery remain unproved until the user loads that directory in the
existing Chrome profile. Publication evidence is recorded separately below.

The requested realtime investigation confirmed WebSocket infrastructure in the
current public Instagram JS, but no observed new-Story event. Story-named Relay
subscription helpers subscribe to local query snapshots, not a demonstrated
network topic. No speculative socket hook was added. Real-time acceleration
requires a naturally observed event with author/Story identity. The existing
seven-cycle automation was updated to preserve this distinction and the newly
authorized browser-only polling scope; no extra automation was created.

### Extension release and Worker receipt protection — 16 October, 18:01 UTC

Commit `bc9a3bab6a45dba163b4c5927c72d520619234d0` passed the exact-SHA
[UOL Worker CI](https://github.com/leosaquetto/uol-bot/actions/runs/38943681890).
The release published Worker version `5cef9a5d-5c7e-4ac0-9964-075c1be19707`
with tag `git-bc9a3bab6a45` at the existing workers.dev domain.

The first postdeploy attempt failed `version_metadata_mismatch` and
`debt_version_mismatch`; it was not accepted as runtime proof. A subsequent
bounded read found `/livez`, `/readyz` and authenticated `/release-debt` all
reporting the new version. The unchanged production verifier then passed:
`published=true`, `runtimeVerified=true`, `debtVerified=true`, `ready=false`,
no regressions. The natural successful primary cycle at
`2026-10-16T18:01:18.054Z` recorded 27 reads; guarded observed estimate was 64
against the unchanged acceptance ceiling of 512. Read/write guards were healthy;
projected writes were 63,872 with 36,128 headroom. These estimates do not replace
three complete UTC days or resolve `billing_unknown`.

The same two historical critical incidents and two uncertain deliveries remain;
zero dead letters, blocked configurations or maintenance dead letters. Their
identity gate passed against the original private baseline. No ambiguous
notification or destination delivery was replayed. Initial failure, later
passing receipt and current publication identity are preserved in private
`uol-extension-release-2026-10-16.log`,
`uol-extension-postdeploy-2026-10-16.log` and the publication manifest.

Extension source, private package checksums and 34 focused offline tests are
complete. A fresh review caught a private-packaging path check that could accept
an internal `..private` directory; it now rejects repository/ancestor paths and
symlink escapes before reading the credential, with a regression test. The
package is directory mode 0700, provisioning mode 0600. Browser installation,
actual session use, real Story discovery and destination receipts remain open.
Only loading `~/Applications/UOL Instagram` in the existing Chrome profile is
required to start the bounded collector; no new login or session transfer was
performed by this release.

### Server push correction and controlled FBNS preparation — 16 October

Read-only server inspection confirmed that `push_only` had made no new Story
GET: the retained `auth_required/invalid_session_file` result was from 15 October,
while current local session validation was valid. Three delivered local outbox
rows remain intact. The transport previously used HTML bootstrap JSON; the
extension-style JSON route was not implemented on the server.

A durably reserved, bounded JSON diagnostic made one authenticated profile
lookup on Oracle and received HTTP 429 with an empty body. No reels request,
fallback, login or delivery was issued. The response was recorded and a global
six-hour backoff persisted through restart until `2026-10-17T00:16:34.449477Z`
(16 October 21:16 São Paulo). Five Web Push events still correspond to the
previous DMs; no packet corresponds to the user's new phone Story screenshot.
The last stored packet was `2026-10-16T17:18:40.847Z`.

Server HTTP controls and JSON parsing are prepared as optional recovery paths,
with explicit backend selection, durable budgets, cached-owner identity captured
at reservation, and shared backoff. They do not activate periodic polling.
Retention no longer deletes incomplete local receipts after 30 days; uncertainty
is preserved at expiry. Diagnostic Web Push signals for `leotheblueblur` are
stored with that exact profile; every production consumer filters `clubeuol`.

Primary-source research identified FBNS as a separate mobile notification
transport with known Story categories. Realtime Direct/MQTT events and Relay
cache observers do not prove Story publication. The isolated FBNS diagnostic
uses a pinned MIT transport subset, a stable device identity, TLS verification,
one durable main-channel association, bounded connections and private journal
before ACK. It cannot publish to UOL destinations. Authorization to alter the
account's main push channel came directly from the user. Risk of challenge on
a datacenter IP remains real and unquantified; success must be an observed
natural Story, not connection/register/ping. The Worker is unchanged by this
server correction; its current published SHA remains the private manifest value.

The user subsequently requested bypassing the waiting time without affecting
other services. The six-hour deadline was our preventive floor, not a confirmed
Meta Retry-After. They explicitly authorized the narrow FBNS registration probe
while source polling stays blocked. `--registration-probe` audits that exception
before connection, requires the original `rate_limited/http_429` state and cannot
ignore a later longer backoff or an FBNS registration429. Account association
still has one durable attempt. UOL, X, gateway and Drops are outside the probe.

### Server FBNS publication and first runtime — 16 October, 18:45–18:53 UTC

Server commit `f1ae912f7a156735e68234a188452f019552c452` passed all jobs in
[exact-SHA CI](https://github.com/leosaquetto/uol-bot/actions/runs/38949624517).
Installed source hashes matched; the existing session and all three delivered
outbox receipts were unchanged. Drops and gateway remained active with their
original PIDs and zero restarts. The source monitor remained `push_only`.
No Cloudflare publication was performed by this server release.

The FBNS broker accepted the connection and returned durable device credentials.
Its PUBACK used header `0x42` with the standard two-byte packet ID, which the
initial strict parser rejected. A bounded diagnostic with the same identity
observed SUBACK, that PUBACK, a registration token and a successful PONG. The
token was saved before acknowledging its response. This proves broker/device
communication, not account association or Story receipt.

The subsequent single account association returned HTTP 400. The service stopped
with exit 78 and no restart; it did not repeat the POST. The original error body
was not captured, so the cause is unproved. The request used the existing desktop
User-Agent and cookies, while the pinned mobile client also supplies an Android
User-Agent, app headers and a Bearer representation of the same session. A
contract correction requires one bounded authenticated identity check first,
then a separately reserved corrective attempt only for the definitive legacy
400; the original attempt remains in history. Uncertain results never authorize
another POST. No new login, session replacement or device/token regeneration.


### Scoped FBNS backoff exception and mobile contract result — 16 October, 18:59–19:01 UTC

Commit `fa90ab9b88fa7eb03dc68d5849fd01c4eaba3321` passed all jobs in
[exact-SHA CI](https://github.com/leosaquetto/uol-bot/actions/runs/38951379029),
39 focused FBNS tests and an independent security review. Only `fbns_probe.py`
was atomically updated on Oracle; its installed hash matched. No service was
restarted by this correction. The original 21:16 São Paulo source backoff still
applies to Story collection; the audited exception was limited to FBNS diagnosis.

At `2026-10-16T18:59:43.713499Z`, the single mobile-contract identity GET returned
HTTP 200/status ok and matched the existing account. It reused the existing
session and persisted only the verification result/bindings, not the personal
account response. This proves that authentication worked for that identity
endpoint; it does not prove push-registration permission.

At `2026-10-16T18:59:51.432186Z`, the separately reserved corrective registration
returned HTTP 400 with the complete 45-byte JSON response, message
`Invalid request`, status `fail`. The original legacy 400 remains archived.
No further POST or GET validation is authorized by restart or timeout. The
mobile-contract hypothesis did not resolve registration; the response does not
identify whether rejection concerns token, parameters, account eligibility or
another server rule. No challenge was reported in that response.

Final read-only comparison at `2026-10-16T19:01:11.615765Z`: Drops, gateway,
monitor and Web Push receiver were active, with the same PIDs and zero restarts;
session and all three delivered outbox receipts were unchanged. `push_only`
remains configured. No Web Story signal or diagnostic FBNS Story was recorded.
FBNS remains stopped after the rejected association, without a retry loop.
Server code is published; FBNS account association, natural Story capture and
end-to-end delivery are **not runtime verified or ready**. The Cloudflare Worker
publication identity and runtime evidence remain unchanged in the private manifest.


### Redundant iPhone notification triggers — implementation, 16 October

The user supplied a public Shortcut and authorized a shareable multi-phone
trigger. Inspection found the Instagram notification trigger with Title contains
`clubeuol` and a single unconfigured POST action. Any matching notification may
wake collection; no Story-specific text filter is required.

The prepared native Shortcut sends only a UUID, fixed profile, source and observed
time to `/instagram-wake`. Each participant receives a separately revocable
scoped code; the Worker stores hashes, not raw participant codes. The optional
Scriptable client stores its code in Keychain. Neither client receives an
Instagram session or WhatsApp credential. Capture remains on the Oracle collector
coordinated by the principal Worker; the existing Story delivery receipts control
WhatsApp and the other destinations across all phones.

The Worker persists a bounded relay queue in the existing metadata table before
202. Only never-attempted pending hints coalesce. Lost relay responses reuse the
same UUID. Oracle's isolated loopback ingress commits the UUID to the existing
signals table before acknowledging it, with a separate relay credential and
no schema migration. A signal received during collection stays pending. External
notification batches finish after one successful snapshot; native push settlement
remains unchanged. Existing 429, authentication and daily-budget guards stay
authoritative. A guarded external batch survives through its retry window.

Phone-origin hints are labeled external and cannot prove native Web Push/FBNS
coverage. Client time is diagnostic; authenticated delayed hints can request a
current snapshot, preventing long offline intervals from blocking newer signals.
The exact publication-to-notification delay and iPhone locked execution still
require device evidence. No synthetic Story, destination send or source GET is
needed to validate relay transport while the source guard remains active.


### Notification bridge rollout diagnostics — 16 October

Oracle notification components were installed from
`ce2150c9ce9b88bb9ade68bb7aa1b82bec8b3053`; exact source hashes matched,
loopback health passed, and Caddy configuration was validated before reload.
Only the Instagram monitor was restarted for its own code update. Drops, gateway,
Web Push and Caddy retained their process identities/restart counts; session and
all three delivery receipts remained unchanged. The existing Story source guard
remained active throughout the controlled HTTP transport checks.

The first Worker publication (`ce2150c9`, version
`c2b305de-6f18-4bd6-83db-5572ab33e3f0`) and subsequent context/diagnostic update
(`fa9d0c79`, version `abc3e0da-e84c-4319-89a0-d19555d48517`) initially failed
version-consistency checks. Each later passed read-only verification after version
convergence, with the same two historical critical incidents/two uncertainties
and no regressions. Neither passing runtime gate alone proved notification relay.

Controlled requests with two participant codes were durably accepted by the
Worker; repeating a UUID was recognized and scoped phone keys could not read
Story status. Initial relay remained unconfirmed, and those same pending UUIDs
were preserved. Direct HTTPS to the isolated Oracle ingress succeeded. A
fetch-receiver-context hypothesis did not resolve production relay; it is not
a proven cause. The added sanitized diagnostic then identified redirect handling.

A targeted test using native Workers fetch and production redirect options
reproduced `Invalid redirect value ... follow or manual`: this runtime rejects
`redirect: error` before network. The correction uses `manual`, refuses 3xx as an
acknowledgement and never forwards the relay credential to a Location target.
Eight unit tests and five Workers-runtime tests passed after that correction;
the native-runtime test now exercises the actual queue request option and a
matching durable ACK. Final publication and real relay proof are recorded below.

Current Worker source `7cca546916d62b2857a0e5cee314e7598e80f56d`, version
`d8ea9c10-b215-446f-be26-3775dbfcd674`, passed
[exact-SHA CI](https://github.com/leosaquetto/uol-bot/actions/runs/38959843153)
and the read-only post-deploy gate after version convergence. `published`,
`runtimeVerified` and `debtVerified` are true; `ready` remains false for the same
two historical critical incidents and two unknown deliveries. No new regression
was found; the primary read-estimate gate remains 512.

The corrected relay reached Oracle but received HTTP 409 for the first controlled
UUID. The direct diagnostic had stored `observedAt` with `+00:00`; the Worker
normalizes the same instant to `Z`. Raw evidence-string comparison rejected that
replay despite equal identity/source/instant. Original records and both queue
identities were preserved. The narrow correction compares replay evidence
semantically, preserving the original stored timestamp and still rejecting a
different source or instant. It requires no migration, deletion or source GET.


### Notification relay verified — 16 October, 20:19 UTC

Oracle ingress replay compatibility was published from
`8009c7e84c9944bd102f6c4815f6c76b2010887a` after 14 ingress tests,
independent review and [exact-SHA CI](https://github.com/leosaquetto/uol-bot/actions/runs/38961025198)
passed. Only `uol-instagram-notification.service` restarted; its installed source
hash and health passed. Monitor, Drops, gateway, Web Push and Caddy retained their
process identities/restart counts. Session, outbox and original signal evidence
were unchanged by installation. No Worker redeployment was needed: its current
published SHA/version remain `7cca546916d62b2857a0e5cee314e7598e80f56d` /
`d8ea9c10-b215-446f-be26-3775dbfcd674`.

The same two controlled HTTP identities subsequently completed relay with HTTP
202 and matching persisted ACKs. Worker pending count is zero. Oracle contains
each identity exactly once, and repeated UUIDs return the original receipt.
The first identity had also been inserted by direct route diagnosis; its original
evidence was preserved during semantic replay. The second was persisted through
the Worker relay. Participant credentials could not read Story status. The session,
three pre-existing outbox receipts and protected services remained unchanged;
no Instagram GET or destination send was performed by these transport checks.
Private transport receipts and the initial failure remain archived.

Six individual scoped codes (owner plus five friends), a signed ready owner
Shortcut, a signed code-free import model and an optional Scriptable client are
prepared in the private local package `Downloads/UOL Instagram - Gatilho`.
No existing iCloud share was overwritten; importing/executing the new package
on iPhone remains necessary. Capture/session stay on Oracle, coordinated by the
principal Worker; all destination deduplication stays central.

Result: Worker publication/runtime/debt verification and notification transport
are confirmed; iPhone import, execution while locked and a natural notification
leading to Story media/link/intake/destination receipts are not yet proved.
The existing source 429 guard remains authoritative until
`2026-10-17T00:16:34.449477Z` (16 October, 21:16 São Paulo); its expiry alone
will not prove recovery. No periodic Instagram polling, native-push success,
mobile login, migration or replay of ambiguous destination delivery is implied.
Global readiness retains the two historical critical incidents/two unknowns.


### iPhone trigger observations — 16 October, 21:06 UTC

The user confirmed the first iPhone request was executed manually. Its UUID was
persisted once as external signal sequence 3 at 20:39:38.848 UTC. A subsequent
`ios_shortcuts` signal, sequence 4, was persisted at 21:05:10.417 UTC (18:05 São
Paulo). Its exact identity and original evidence remain in private receipts.
There are four signals total: two controlled transport tests, the manual iPhone
request and this one later request. Automatic/locked notification execution is
not independently proven merely by the `ios_shortcuts` source label.

Read-only checks at 21:06 UTC confirmed the current Worker version remains
`d8ea9c10-b215-446f-be26-3775dbfcd674`, relay pending is zero and the latest relay
has a matching persisted HTTP 202 ACK. Oracle has consumed/reserved sequence 0:
all four hints remain durably pending behind the existing HTTP 429 guard, until
00:16:34.449477 UTC on 17 October (21:16 on 16 October in São Paulo). Source
cycles and requests have not increased since the prior 20:40 UTC snapshot.
The three existing delivered outbox rows remain; no new capture or destination
delivery is proved. These observations performed no source GET or send.
