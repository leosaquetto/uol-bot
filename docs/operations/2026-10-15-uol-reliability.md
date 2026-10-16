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
