# UOL Headless Reliability Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Strengthen the UOL Worker behind the scenes so offer delivery remains fast, self-recovering, quota-aware, protected against UOL contract drift, and replayable without adding a user-facing dashboard.

**Architecture:** Keep API polling and delivery as the critical path. Add a small, testable headless-health classifier for the external monitor, a sidecar delivery-event ledger for durable audit/replay without adding columns to `offers`, queue-age/SLO policy that yields secondary work before new offers, and a fail-closed API contract canary. Preserve existing Telegram/Discord contracts, destination idempotency, and the free-tier reserve.

**Tech Stack:** Cloudflare Workers + Durable Objects + SQLite, Node.js 22, Node test runner, Vitest/Cloudflare pool, GitHub Actions monitor.

## Global Constraints

- No user-facing dashboard, new offer channel, Vercel service, Hermes dependency, or global polling acceleration.
- Never expose secrets, tokens, message contents, or private IDs in public health responses, logs, tests, or Git.
- `offers` must not receive another delivery/state column; new delivery metadata belongs in sidecar tables.
- A delivery result that is externally ambiguous must not be blindly replayed.
- New offers and the 15-second API lane retain priority over HTML, comments, image upgrades, and repairs.
- Maintenance must continue to yield before the Durable Object free-tier read reserve.
- Each behavior change requires a failing test before production code and a focused green test afterward.

---

### Task 1: Headless watchdog and state-change monitoring

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/headless-health.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/headless-health.test.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/scripts/monitor-ready.mjs`
- Modify: `.github/workflows/uol-ready-monitor.yml`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/scripts/verify-production.mjs`

**Interfaces:**
- `classifyHeadlessHealth({ liveness, readiness, now, maxScanAgeMs }) -> { state, hardFailure, reasons, snapshot }`.
- The monitor script fetches `/livez` and `/readyz`, creates or updates one deduplicated GitHub issue only on state transitions, and exits zero for degraded historical state so GitHub does not generate noisy failed-run notifications.

- [ ] **Step 1: Write failing classifier tests**

  Cover: healthy live Worker; liveness timeout; stale scan; missing alarm; readiness 503 caused only by historical incidents (`degraded`, not hard outage); stale scan plus liveness failure (`outage`); recovery from outage.

- [ ] **Step 2: Run the focused test and confirm the expected missing-module failure**

  Run from `cloudflare-workers/uol-telegram-shadow-worker`:

  ```bash
  node --test test/headless-health.test.js
  ```

- [ ] **Step 3: Implement the pure classifier**

  Keep the classifier free of fetch, GitHub, Cloudflare, and secret access. Treat `liveness.ok !== true`, a missing/mismatched Worker identity, stale `lastScanAt`, stale alarm, non-live mode, or missing delivery configuration as hard failure. Treat only `criticalIncidents`, `unknown`, dead letters, blocked configuration, maintenance dead letters, or a quota reserve warning as degraded when scan/alarm/liveness remain fresh.

- [ ] **Step 4: Implement the monitor script**

  Use only built-in `fetch`. Read `READY_URL`, `GITHUB_TOKEN`, `GITHUB_REPOSITORY`, and `GITHUB_RUN_URL` from the environment. Store a marker in one issue body, update the issue with a sanitized snapshot, close it once the state returns to healthy, and never print response bodies containing secrets.

- [ ] **Step 5: Replace inline workflow logic with the script**

  Preserve the five-minute schedule, `workflow_dispatch`, `issues: write`, and opt-in `vars.UOL_READY_MONITOR_ENABLED == 'true'`. The workflow may annotate a hard failure but must not fail the job for a degraded readiness state; this prevents repeated `Run failed` notifications while retaining an external incident record.

- [ ] **Step 6: Run focused tests and lint/syntax checks**

  ```bash
  node --test test/headless-health.test.js
  node --check scripts/monitor-ready.mjs
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add .github/workflows/uol-ready-monitor.yml \
    cloudflare-workers/uol-telegram-shadow-worker/src/headless-health.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/headless-health.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/scripts/monitor-ready.mjs \
    cloudflare-workers/uol-telegram-shadow-worker/scripts/verify-production.mjs
  git commit -m "feat(uol): add quiet headless watchdog"
  ```

### Task 2: Durable delivery-event ledger and safe automatic reconciliation

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/delivery-ledger.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/delivery-ledger.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/migrations.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

**Interfaces:**
- `deliveryEventKey({ offerId, target, operation, attempt, generation }) -> string`.
- `recordDeliveryEvent(sqlExec, event) -> void`, bounded to 240 events per offer and retaining failure/unknown/recovery events longer than healthy duplicates.
- `summarizeDeliveryTimeline(rows) -> sanitized timeline`.

- [ ] **Step 1: Write failing ledger tests**

  Assert deterministic idempotency keys, redaction/truncation of errors, ordering of an offer timeline, and retention of only the newest bounded events.

- [ ] **Step 2: Run the focused test and confirm failure**

  ```bash
  node --test test/delivery-ledger.test.js
  ```

- [ ] **Step 3: Add migration v20 without altering `offers`**

  Create `delivery_events(id, dedupe_key UNIQUE, offer_id, target, operation, state, attempt, generation, occurred_at, external_id, error)` plus indexes on `(offer_id, occurred_at DESC)` and `(state, occurred_at DESC)`. Backfill one `snapshot` event per existing destination that already has a sent, sold-out, restock, dead-letter, or unknown state.

- [ ] **Step 4: Instrument existing delivery boundaries**

  Record `attempt_started`, `sent`, `failed`, `unknown`, `resolved`, `sold_out_synced`, `restock_synced`, `image_upgraded`, and `comment_synced` from the existing begin/success/failure/reconciliation paths. Use the existing destination and generation values; do not change transport behavior.

- [ ] **Step 5: Add bounded automatic reconciliation**

  On each maintenance cycle, read at most 32 recent ledger events for offers still in `delivery_unknown`, `partial_delivery`, or maintenance-pending states. Recompute aggregate status from the existing authoritative delivery columns. Only retry explicitly retryable errors; leave ambiguous delivery for evidence-based resolution and record an operational event.

- [ ] **Step 6: Expose sanitized timeline only through authenticated diagnostics**

  Add `timeline` to `/decisions` and `/inventory` rows, capped at 20 events, without adding a new public route or dashboard UI.

- [ ] **Step 7: Run migrations and integration tests**

  ```bash
  node --test test/delivery-ledger.test.js test/migrations.test.js
  npm run test:worker -- --run test-worker/worker.integration.test.js
  ```

- [ ] **Step 8: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/delivery-ledger.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/delivery-ledger.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/migrations.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js
  git commit -m "feat(uol): add durable delivery event ledger"
  ```

### Task 3: Queue SLO, priority, and quota-aware backpressure

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/queue-policy.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/queue-policy.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/operations.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/operations.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

**Interfaces:**
- `summarizeQueueSlo(rows, now) -> { pending, oldestAgeMs, p95AgeMs, criticalPending, secondaryPending }`.
- `chooseDeliveryBudget({ storageReadBudget, queueSlo, configuredBatch, configuredConcurrency }) -> { batchSize, concurrency, deferSecondary, reason }`.

- [ ] **Step 1: Write failing policy tests**

  Cover new offers pre-empting secondary work, old critical work increasing priority, healthy queues retaining configured concurrency, and read-reserve pressure deferring only secondary work.

- [ ] **Step 2: Run focused red tests**

  ```bash
  node --test test/queue-policy.test.js
  ```

- [ ] **Step 3: Implement pure queue policy**

  Keep hard caps at existing configured maxima. Never increase polling frequency or delivery concurrency above current values. Return explicit reasons for `quota_reserve`, `critical_backlog`, and `healthy`.

- [ ] **Step 4: Integrate into primary and maintenance cycles**

  Compute queue age from bounded indexed queries, pass the selected budget to delivery queue calls, and let maintenance skip comments/image enrichment before it skips reconciliation or new delivery. Persist only changed SLO snapshots to avoid row inflation.

- [ ] **Step 5: Add operational signals**

  Alert only after three consecutive SLO breaches: critical offer older than 45 seconds, secondary queue older than 10 minutes, or remaining read budget below the configured reserve. Include counts and ages, never offer descriptions or credentials.

- [ ] **Step 6: Run focused tests**

  ```bash
  node --test test/queue-policy.test.js test/operations.test.js
  npm run check:fast
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/queue-policy.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/operations.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/queue-policy.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/operations.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js
  git commit -m "feat(uol): enforce delivery SLO and quota backpressure"
  ```

### Task 4: UOL API contract canary

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/uol-contract.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/uol-contract.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/uol-api.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/operations.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/uol-api.test.js`

**Interfaces:**
- `validateTicketApiPayload(payload) -> { ok, reason, total, valid, invalid, fields }`.
- `contractHealthSignal(result) -> incident signal or null`.

- [ ] **Step 1: Write failing contract tests**

  Assert that a valid payload passes; missing/non-array `beneficios`, malformed URLs, missing titles, and a sudden all-invalid payload fail closed; an honestly empty but correctly shaped array is accepted.

- [ ] **Step 2: Run focused red tests**

  ```bash
  node --test test/uol-contract.test.js
  ```

- [ ] **Step 3: Implement validation before mapping**

  Validate shape and parseability before `mapTicketApiPayload`. Do not reject an individual malformed card if valid cards remain; reject the whole API snapshot only when the schema is invalid or the valid-card ratio is zero after a nonempty payload.

- [ ] **Step 4: Persist sanitized contract snapshot and incident**

  Store counts, field names, and reason only. When the canary fails, preserve the last known good inventory, let HTML fallback run, and prevent destructive sold-out decisions from that API cycle.

- [ ] **Step 5: Run focused tests and full pure suite**

  ```bash
  node --test test/uol-contract.test.js test/uol-api.test.js
  npm run check:fast
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/uol-contract.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/uol-api.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/operations.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/uol-contract.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/uol-api.test.js
  git commit -m "feat(uol): fail closed on API contract drift"
  ```

### Task 5: End-to-end replay and burst regression suite

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/replay-fixtures.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/replay.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/README.md`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/IMPLEMENTATION.md`

**Interfaces:**
- `runReplayScenario(name, overrides) -> { events, finalOffers, calls }` using only local deterministic transport doubles.

- [ ] **Step 1: Write failing replay tests**

  Cover API discovery to all destinations, delayed image upgrade on the same Telegram message, two-check ticket sold-out edit with Discord duration, restock after sold-out, timeout/unknown without duplicate, deleted-message recovery, and a burst of 24 new offers where the first offer remains within the SLO.

- [ ] **Step 2: Run the focused red tests**

  ```bash
  node --test test/replay.test.js
  ```

- [ ] **Step 3: Add deterministic fixtures and transport recorder**

  Use sanitized URLs/titles only. Record method, destination, operation, offer ID, and message ID; never store secrets or full private payloads.

- [ ] **Step 4: Implement the scenarios against production helpers**

  Do not create a second delivery implementation. Exercise `scan`, `processDeliveryQueue`, `processTicketAvailabilityProbes`, sold-out/restock sync, and the ledger.

- [ ] **Step 5: Run pure and Cloudflare integration suites**

  ```bash
  node --test test/replay.test.js
  npm run check:fast
  npm run test:worker
  ```

  If local `workerd` cannot run on macOS 12.6, retain the pure evidence and run the integration suite in CI/Linux before deployment.

- [ ] **Step 6: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/test/replay-fixtures.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/replay.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/README.md \
    cloudflare-workers/uol-telegram-shadow-worker/IMPLEMENTATION.md
  git commit -m "test(uol): add headless delivery replay scenarios"
  ```

### Task 6: Full verification, production rollout, and runtime evidence

**Files:**
- Modify only files already listed above if verification discovers a concrete defect.

- [ ] **Step 1: Run the complete local checks from the Worker directory**

  ```bash
  npm run check:fast
  npm run check:types
  npm run check:bundle
  ```

- [ ] **Step 2: Run CI-grade integration**

  ```bash
  npm run test:worker
  npm run check:startup
  ```

  Record macOS 12.6 `workerd` limitations explicitly if the runtime refuses to start; do not substitute unit tests for this evidence.

- [ ] **Step 3: Review migration and secret diff**

  Confirm no secret values, `offers` column additions after v19, public diagnostic expansion, or unrelated legacy edits are present.

- [ ] **Step 4: Deploy once to the configured Cloudflare Worker**

  Capture the new Version ID and URL. Do not change delivery mode or bindings.

- [ ] **Step 5: Verify production**

  Confirm `/livez` 200 with the new Version ID, `/readyz` freshness/alarm/queue/budget checks, at least one primary scan after deployment, and no new migration/result-set/transport errors in a bounded tail window. Treat historical incidents as explicit pending state rather than hiding them.

- [ ] **Step 6: Push the verified commits and report remaining operational pendencies**

  Keep the branch clean, report every test limitation, and distinguish code, commit, push, deploy, and fresh production proof.
