# UOL Ticket Sold-Out Probes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Confirm rapid unavailability only for ticket campaigns and synchronize sold-out edits without allowing the critical lane to consume the API polling reserve.

**Architecture:** Newly delivered ticket offers receive a bounded URL-probe state in `offers`. The primary 15-second alarm processes at most one due ticket probe per cycle, requires two consecutive deterministic “gone” results, and invokes the existing idempotent Telegram/Discord sold-out synchronizers for that offer. The existing HTML absence detector remains the fallback for all offers and for indeterminate probes; a daily probe counter prevents the new lane from expanding free-tier usage.

**Tech Stack:** Cloudflare Durable Object SQLite, Cloudflare Worker `fetch`, Telegram Bot API, Discord webhook, Node `node:test`, Wrangler types/bundle checks.

## Global Constraints

- Only links containing `/campanhasdeingresso/` may enter the rapid probe queue.
- A probe must classify 404/410 or a confirmed home redirect as `gone`; timeout, network error, 5xx, anti-bot, incomplete response, and ambiguous HTML are `indeterminate`.
- Two consecutive `gone` results are required by default; one response is never enough for a generic 200/home result.
- A confirmed probe may edit existing Telegram/Discord messages but may not send a replacement message.
- Ambiguous Telegram/Discord results remain unknown and are never retried as a new message automatically.
- Common offers keep the existing maintenance-only sold-out path.
- The probe lane is capped at one candidate per primary scan and 256 probes per UTC day by default.
- Telegram and Discord use the same `America/Sao_Paulo` sold-out time and the duration from `first_seen_at` to `sold_out_at`; invalid timestamps omit only the duration.
- No new dependency, secret, webhook, channel, or free-tier bypass may be introduced.

---

### Task 1: Pure probe classification and budget helpers

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/ticket-soldout-probe.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/ticket-soldout-probe.test.js`

**Interfaces:**
- Produces `classifyTicketProbeResponse({ requestedUrl, finalUrl, status, body })` returning `{ result: "gone" | "available" | "indeterminate", reason }`.
- Produces `nextTicketProbeState({ result, goneCount, attempts, now, confirmGoneCount, maxAttempts })` returning `{ action, goneCount, nextAt, lastResult }`, where `action` is `"confirm"`, `"continue"`, or `"fallback"`.
- Produces `ticketProbeBudget({ used, dailyLimit, perScanLimit })` returning `{ remaining, allowed, batchSize }`.

- [ ] **Step 1: Write failing classifier tests**

Add tests covering the exact cases below:

```js
assert.deepEqual(
  classifyTicketProbeResponse({
    requestedUrl: "https://clube.uol.com.br/campanhasdeingresso/ticket",
    finalUrl: "https://clube.uol.com.br/campanhasdeingresso/ticket",
    status: 404,
    body: "",
  }),
  { result: "gone", reason: "http_404" },
);
assert.equal(classifyTicketProbeResponse({
  requestedUrl: "https://clube.uol.com.br/campanhasdeingresso/ticket",
  finalUrl: "https://clube.uol.com.br/",
  status: 200,
  body: "Clube UOL",
}).result, "gone");
assert.equal(classifyTicketProbeResponse({
  requestedUrl: "https://clube.uol.com.br/campanhasdeingresso/ticket",
  finalUrl: "https://clube.uol.com.br/campanhasdeingresso/ticket",
  status: 200,
  body: "Detalhes da oferta",
}).result, "available");
assert.equal(classifyTicketProbeResponse({
  requestedUrl: "https://clube.uol.com.br/campanhasdeingresso/ticket",
  finalUrl: "https://clube.uol.com.br/",
  status: 503,
  body: "temporarily unavailable",
}).result, "indeterminate");
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run from `cloudflare-workers/uol-telegram-shadow-worker`:

```bash
node --test test/ticket-soldout-probe.test.js
```

Expected: FAIL because `src/ticket-soldout-probe.js` does not exist.

- [ ] **Step 3: Implement the pure classifiers**

Normalize only URL origin/path and lowercase response text. Treat 404/410 as
gone, and treat a successful response whose final URL is the UOL root/home as
gone only when the requested path was a campaign path. Treat a non-root 2xx
response as available; treat all other statuses as indeterminate. Cap body
inspection to 64 KiB and never include body content in returned diagnostics.

Implement state transitions so that `goneCount` reaches `confirmGoneCount`
only after two consecutive `gone` results, `available` clears the sequence,
and `indeterminate` clears the rapid queue for the maintenance fallback.

- [ ] **Step 4: Add budget tests and implementation**

Assert that `ticketProbeBudget({ used: 0, dailyLimit: 256, perScanLimit: 1 })`
allows one probe, that `used: 255` allows one, and that `used: 256` returns
`allowed: false` and `batchSize: 0`. Keep this helper independent from SQL so
the daily cap is testable without a Worker runtime.

- [ ] **Step 5: Run the focused tests**

```bash
node --test test/ticket-soldout-probe.test.js
```

Expected: all classifier, transition, and budget tests pass.

- [ ] **Step 6: Commit the pure unit**

```bash
git add src/ticket-soldout-probe.js test/ticket-soldout-probe.test.js
git commit -m "feat(uol): classify ticket availability probes"
```

### Task 2: Persist the ticket probe queue and schedule new deliveries

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:migrate`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:constructor/loadStorageUsage`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:processDeliveryQueue`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:processRestockSync`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:getInventory`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/migrations.test.js`

**Interfaces:**
- Adds migration v19 columns: `ticket_probe_next_at`,
  `ticket_probe_last_at`, `ticket_probe_last_result`,
  `ticket_probe_gone_count`, and `ticket_probe_attempts`.
- Adds index `offers_ticket_probe_idx` on
  `(status, ticket_probe_next_at, ticket_probe_attempts, first_seen_at)`.
- Adds `ticketProbeCount` to the persisted daily storage usage snapshot.

- [ ] **Step 1: Extend migration tests before schema changes**

Change the migration count/version assertions from 18 to 19 and add all five
probe columns to the required-column list. Add a v19 upgrade fixture containing
one recent delivered ticket and one common offer; assert that the ticket gets a
probe due time and the common offer remains unscheduled.

- [ ] **Step 2: Run migration tests and verify the expected failure**

```bash
node --test test/migrations.test.js
```

Expected: FAIL on the old migration count/columns.

- [ ] **Step 3: Add migration v19**

Add the columns with empty/zero defaults and the probe index. Schedule only
recent delivered ticket rows (last 24 hours) with `ticket_probe_next_at` empty
to `strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+60 seconds')`; do not schedule common
offers or rows already sold out. This bounded backfill covers offers delivered
just before deployment without replaying the historical inventory.

- [ ] **Step 4: Schedule probes after successful delivery**

In the successful main-message update inside `processDeliveryQueue`, set
`ticket_probe_next_at` to `sentAt + 60 seconds` only when the link is a ticket
campaign and the probe queue is empty. Preserve the existing value on retries.
When a restocked ticket returns to `delivered`, schedule the same one-time
probe if the queue is empty. Do not schedule any probe for a common offer.

- [ ] **Step 5: Expose sanitized probe state for verification**

Include only counters/timestamps/results in `getInventory`; never expose URL
response bodies, request headers, or secrets. Keep the public `/offers` payload
unchanged.

- [ ] **Step 6: Run migration and delivery tests**

```bash
node --test test/migrations.test.js test/delivery-state.test.js
```

Expected: all tests pass, including the common-offer exclusion.

- [ ] **Step 7: Commit the persisted queue**

```bash
git add src/worker.js test/migrations.test.js
git commit -m "feat(uol): persist ticket sold-out probe queue"
```

### Task 3: Unify sold-out time and duration text in Telegram and Discord

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/core.js:formatSoldOutTime/buildTelegramCaption`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/discord.js:buildDiscordPayload/editDiscordOffer`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/discord.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/telegram.test.js`

**Interfaces:**
- Produces `formatOfferTime(value)` using `America/Sao_Paulo` and `HH:mm`.
- Produces `formatOfferDuration(start, end)` returning `menos de 1 min`,
  `6 min`, `1h 12min`, or `1d 2h`, and `""` for invalid/negative timestamps.
- `buildDiscordPayload` accepts optional `publishedAt` while preserving the
  existing `offer.firstSeenAt` fallback.

- [ ] **Step 1: Write failing formatting tests**

Assert that both channel payloads include, immediately after the sold-out
status line, the same hour and duration for
`firstSeenAt: "2026-08-04T18:32:00.000Z"` and
`soldOutAt: "2026-08-04T18:38:00.000Z"`:

```js
const discordPayload = buildDiscordPayload({
  title: "Ingresso teste",
  link: "https://clube.uol.com.br/campanhasdeingresso/teste",
  firstSeenAt: "2026-08-04T18:32:00.000Z",
}, { soldOutAt: "2026-08-04T18:38:00.000Z" });
assert.match(buildTelegramCaption({
  title: "Ingresso teste",
  link: "https://clube.uol.com.br/campanhasdeingresso/teste",
  firstSeenAt: "2026-08-04T18:32:00.000Z",
}, { soldOutAt: "2026-08-04T18:38:00.000Z" }), /Oferta esgotada às 15:38/);
assert.match(discordPayload.embeds[0].description, /Ficou no ar por 6 min/);
```

Add an invalid-timestamp case that omits only the duration and still renders
the sold-out edit.

- [ ] **Step 2: Run the focused tests and verify the failure**

```bash
node --test test/core.test.js test/telegram.test.js test/discord.test.js
```

Expected: FAIL because the duration line is not yet part of either payload.

- [ ] **Step 3: Implement shared formatting and channel output**

Reuse one formatter from `core.js`. Add Telegram’s line
`⏱️ Ficou no ar por <duration>.` immediately after
`❌ Oferta esgotada às <HH:mm>.`. Add the same two logical lines to the Discord
embed description, keep the existing red sold-out color/title, and preserve
the native Discord `timestamp` field. Never let invalid timestamps throw.

- [ ] **Step 4: Run the focused tests**

```bash
node --test test/core.test.js test/telegram.test.js test/discord.test.js
```

Expected: all formatting, truncation, and edit tests pass.

- [ ] **Step 5: Commit the shared sold-out status**

```bash
git add src/core.js src/telegram.js src/discord.js test/core.test.js test/telegram.test.js test/discord.test.js
git commit -m "feat(uol): show sold-out duration in channels"
```

### Task 4: Process the bounded probe lane and synchronize confirmed sold-out offers

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:scan`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:processSoldOutSync`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:processDiscordAvailabilitySync`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js:evaluateSoldOut`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/ticket-soldout-probe.test.js`

**Interfaces:**
- Adds `probeTicketOfferUrl(url, fetchImpl, timeoutMs)` in
  `ticket-soldout-probe.js`; it sends a GET with `redirect: "follow"`,
  `Cache-Control: no-cache`, `Accept: text/html`, and a 5-second timeout, then
  delegates response classification to the pure classifier.
- Adds `async processTicketAvailabilityProbes(now)` on the Durable Object,
  returning `{ checked, gone, confirmed, available, indeterminate, failed,
  soldOutMainEdited, soldOutCanal2Edited, soldOutDiscordEdited }`.
- Adds `onlyIds` filtering to the existing sold-out synchronizers so a confirmed
  probe can edit only its offer without scanning unrelated rows.

- [ ] **Step 1: Write failing probe transport tests**

Use a fake `fetchImpl` to assert GET/redirect/cache headers and test 404,
home redirect, same-path 200, timeout, and 503 outcomes. Assert that response
bodies are not returned in the result.

- [ ] **Step 2: Run the focused tests and verify the new transport fails**

```bash
node --test test/ticket-soldout-probe.test.js
```

Expected: FAIL because `probeTicketOfferUrl` is not yet implemented.

- [ ] **Step 3: Implement the bounded transport and candidate query**

Use `ticketProbeBudget` with environment values
`TICKET_SOLD_OUT_PROBE_DAILY_LIMIT` (default 256) and
`TICKET_SOLD_OUT_PROBES_PER_SCAN` (default 1). Query only:

```sql
WHERE status IN ('delivered', 'partial_delivery')
  AND link LIKE '%/campanhasdeingresso/%'
  AND sold_out_at = ''
  AND ticket_probe_next_at <> ''
  AND ticket_probe_next_at <= ?
ORDER BY ticket_probe_next_at ASC, first_seen_at ASC
LIMIT ?
```

Increment the daily probe counter once per external probe. On `available`,
clear the rapid queue; on `indeterminate`, clear it and leave the existing
15-minute fallback in charge; on `gone`, increment the consecutive counter and
schedule the second probe after 5 seconds. Cap one candidate per primary scan
and stop when the daily counter reaches 256.

- [ ] **Step 4: Implement the idempotent sold-out transition**

For a second consecutive `gone`, update only the matching delivered/partial
ticket row with `sold_out_at`, `status_before_sold_out`, `status = 'sold_out'`,
`missing_since`, `absence_count = 2`, reset Telegram/Discord sold-out sync
attempts, and clear the probe queue. The `WHERE` clause must require
`sold_out_at = ''` and the original status so a duplicate alarm cannot repeat
the transition.

- [ ] **Step 5: Reuse existing destination editors with a one-row filter**

Call `processSoldOutSync(now, { onlyIds: [offerId] })` and
`processDiscordAvailabilitySync(now, 4, { onlyIds: [offerId] })`. The focused
queries must still honor configuration, retry limits, missing-message handling,
and the existing caption/text fallback. A failed or ambiguous edit is recorded
for maintenance retry; no replacement message is sent by this lane.

- [ ] **Step 6: Add a global false-positive guard**

If the probe lane sees a home redirect/absence result for more than one distinct
ticket in the same scan window, do not confirm either additional candidate;
record `indeterminate:global_home_redirect` and let the HTML fallback decide.
This prevents a temporary UOL-wide redirect from mass-marking tickets.

- [ ] **Step 7: Call the lane from the primary scan**

Run the probe lane after API discovery and normal fast delivery, before scan
telemetry is persisted. Add sanitized counters to the scan result/log only;
do not add response bodies or URLs beyond the existing offer ID/link fields.

- [ ] **Step 8: Add architecture assertions**

Assert that the probe lane is called from `scan`, filters
`/campanhasdeingresso/`, uses the daily/per-scan budget, requires two gone
results, and invokes both focused sold-out synchronizers. Assert that the
maintenance guard remains in place for the full maintenance tick.

- [ ] **Step 9: Run focused tests**

```bash
node --test test/ticket-soldout-probe.test.js test/architecture.test.js test/migrations.test.js
```

Expected: all tests pass.

- [ ] **Step 10: Commit the critical lane**

```bash
git add src/ticket-soldout-probe.js src/worker.js test/ticket-soldout-probe.test.js test/architecture.test.js test/migrations.test.js
git commit -m "feat(uol): fast sold-out lane for ticket campaigns"
```

### Task 5: Full validation, deployment, and live audit

**Files:**
- No additional source files; validate the commits from Tasks 1–3.

- [ ] **Step 1: Run the complete local checks**

```bash
npm run check:fast
npm run check:types
npm run check:bundle
```

Expected: all Node tests pass; Wrangler types and dry-run bundle pass. The
known macOS 12.6 workerd warning may remain; it is not a source failure.

- [ ] **Step 2: Review the final diff and commit state**

Run `git diff --check`, `git status --short --branch`, and inspect only the
named source/test/migration files. Confirm no secrets, response bodies, or
unrelated changes are staged.

- [ ] **Step 3: Push and deploy once**

Run `git push origin main`, then `npm run deploy` from
`cloudflare-workers/uol-telegram-shadow-worker` after Wrangler authentication
check. Record the Worker version ID and associated production URL.

- [ ] **Step 4: Verify production health**

Call `/livez` with no-cache and authenticated `/readyz`, `/dashboard.json`, and
`/inventory`. Confirm:

- Worker version is the deployed version;
- `deliveryConfigured` and `storageReadBudgetHealthy` are true;
- no new dead letters or unknown deliveries were created;
- common offers have no probe state;
- recent ticket rows show probe scheduling/counters only;
- existing incidents remain resolved.

- [ ] **Step 5: Exercise the operational path without synthetic sends**

Trigger one authenticated `/maintenance` only if the budget guard allows it;
do not force a probe or send a duplicate offer. Confirm the response reports no
unexpected delivery failures and that the next primary scan remains scheduled.

- [ ] **Step 6: Final completion audit**

Compare each requirement in the approved design with source, tests, deployed
version, readiness, and sanitized inventory evidence. Report separately what
was changed locally, committed, pushed, deployed, and proven in production.
