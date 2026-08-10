# UOL Quota Efficiency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Lower Durable Objects `rows_read` consumption without slowing the normal 15-second API discovery or changing fast Telegram/Discord offer delivery.

**Architecture:** Add a persisted API snapshot fingerprint to bypass unchanged primary reconciliation, reuse one delivery snapshot per scan, and let fresh priority IDs keep secondary delivery access under quota reserve while old enrichment is deferred. Add maintenance alarm backoff, bounded stage counters, and explicit tests around quota and image terminal behavior without adding a new storage product or `offers` columns.

**Tech Stack:** Cloudflare Workers, Durable Objects SQLite, Node.js test runner, existing Worker helper modules and integration replay fixtures.

## Global Constraints

- Preserve the existing 15-second API interval and current image/description/Discord-first/Canal 2 delivery order under normal headroom.
- No new Durable Object, queue service, external database, dashboard, public route, dependency, secret, or `offers` column.
- Never blindly replay an ambiguous external delivery; keep the existing ledger and evidence-based resolution.
- Count new-offer priority work inside the primary read budget; never raise configured delivery batch size or concurrency.
- Maintenance must re-arm safely and yield before the primary read reserve.
- Use focused red/green tests for each behavior change and preserve pre-existing worktree changes.

---

### Task 1: Add deterministic API snapshot fingerprint and stage counters

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/core.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Interfaces:**
- `buildApiSnapshotFingerprint(cards) -> Promise<string>` returns a SHA-256 digest of sorted, normalized card fields, excluding `observedAt`.
- `storageUsage.stageReads` and `storageUsage.stageWrites` are bounded objects keyed by `primary`, `delivery`, `tickets`, `maintenanceLedger`, `html`, `comments`, `images`, and `guard`.
- `recordStorageStage(stage, rowsRead, rowsWritten) -> void` updates only the in-memory usage snapshot; `completeStorageUsageCycle` persists it with existing metadata.

- [ ] **Step 1: Write failing fingerprint and counter tests**

  Add tests for order-independent fingerprints, ignored `observedAt`, changed delivery fields producing a different digest, malformed/empty card input producing a stable digest, and stage counters accumulating without changing aggregate row accounting.

- [ ] **Step 2: Run focused tests and confirm failure**

  ```bash
  node --test test/core.test.js test/architecture.test.js
  ```

  Expected: the new fingerprint export and stage-counter assertions fail before implementation.

- [ ] **Step 3: Implement the pure fingerprint helper**

  Normalize the exact fields from the approved design, sort by `id`, serialize with `JSON.stringify`, and call the existing `sha256Hex`. Do not include timestamps or object key order from the source response.

- [ ] **Step 4: Add bounded stage accounting**

  Initialize/reset stage maps with the existing daily usage lifecycle. Add `recordStorageStage` and call it from primary, delivery, ticket, maintenance, HTML, comment, image, and guard boundaries using the already measured cycle deltas. Keep aggregate `rowsRead`/`rowsWritten` authoritative.

- [ ] **Step 5: Run focused tests and syntax checks**

  ```bash
  node --test test/core.test.js test/architecture.test.js
  node --check src/worker.js
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/core.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js
  git commit -m "feat(uol): measure staged storage usage"
  ```

### Task 2: Bypass unchanged primary reconciliation

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Interfaces:**
- Metadata key `runtime:api_snapshot_fingerprint` stores the last successful fingerprint; no migration is added.
- `scan()` computes `apiSnapshotChanged` after contract validation and before reconciliation.
- Existing full reconciliation remains the fallback when metadata is absent/corrupt, the API contract changes, or the fingerprint changes.

- [ ] **Step 1: Add integration replay tests**

  Cover: first API snapshot initializes; identical second snapshot skips full reconciliation and source-observation writes while still running pending delivery/ticket paths; changed card runs full reconciliation; corrupt fingerprint falls back once and rewrites the key; API contract failure never replaces the stored fingerprint.

- [ ] **Step 2: Run the new tests and confirm the expected failure**

  ```bash
  npm run test:worker -- --run test-worker/worker.integration.test.js
  ```

  Expected: the replay fixture currently performs the full reconciliation on the unchanged snapshot.

- [ ] **Step 3: Integrate fingerprint gating in `scan()`**

  Compute the digest only after a valid nonempty API result. On changed snapshots, preserve `resolveListingCards`, `processPending`, priority delivery, and `recordSourceCards`. On unchanged snapshots, skip those full operations, run bounded `processDeliveryQueue` for pending/unknown rows and `processTicketAvailabilityProbes`, and update runtime scan telemetry.

- [ ] **Step 4: Persist and recover the fingerprint safely**

  Write the metadata key only after a successful API snapshot and the existing decision path. Read through the metadata cache; if parsing or shape validation fails, use the full path and replace the value. Never clear the previous fingerprint after an API rejection or empty invalid result.

- [ ] **Step 5: Run focused replay and architecture tests**

  ```bash
  npm run test:worker -- --run test-worker/worker.integration.test.js
  node --test test/architecture.test.js
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js
  git commit -m "feat(uol): skip unchanged API reconciliation"
  ```

### Task 3: Preserve fresh secondary delivery under quota reserve and reuse rows

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/queue-policy.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/queue-policy.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

**Interfaces:**
- `chooseDeliveryBudget({ storageReadBudget, queueSlo, configuredBatch, configuredConcurrency, priorityCount }) -> { batchSize, concurrency, deferSecondary, allowPrioritySecondary, reason }`.
- `processDeliveryQueue(now, { priorityIds, priorityOnly, rows, waitForMainImage, targetNames })` accepts an optional selected-row snapshot; existing callers remain valid.

- [ ] **Step 1: Add policy tests**

  Assert healthy behavior is unchanged; quota reserve defers old Discord/Canal 2 work; quota reserve allows only current `priorityIds` for secondary targets; critical main delivery remains eligible; configured batch/concurrency never increase.

- [ ] **Step 2: Run the policy tests and confirm failure**

  ```bash
  node --test test/queue-policy.test.js
  ```

- [ ] **Step 3: Implement priority-aware policy**

  Add `allowPrioritySecondary` only when `priorityCount > 0` and the read reserve is active. Keep `deferSecondary` true for nonpriority rows. Do not bypass the existing hard-limit fail-closed path.

- [ ] **Step 4: Apply row-specific target eligibility**

  In `processDeliveryQueue`, classify priority rows with the requested targets and nonpriority rows with `main` only while the reserve is active. A secondary-only call with no priority IDs returns the existing deferred result. Keep the delivery ledger and unknown handling unchanged.

- [ ] **Step 5: Reuse selected rows inside one scan**

  Allow the first queue selection/classification result to be passed to the next target phase. Use a bounded indexed fallback query when a caller has no snapshot. Preserve current ordering: Discord/image acquisition, main Telegram image deadline/fallback, then Canal 2.

- [ ] **Step 6: Add integration replay coverage**

  Replay a new offer with `maintenanceAllowed: false` and assert Discord, main, and Canal 2 priority delivery remain eligible; replay an old secondary row and assert it is deferred; replay an ambiguous main send and assert no blind duplicate.

- [ ] **Step 7: Run focused tests**

  ```bash
  node --test test/queue-policy.test.js
  npm run test:worker -- --run test-worker/worker.integration.test.js
  ```

- [ ] **Step 8: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/queue-policy.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/queue-policy.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js
  git commit -m "feat(uol): prioritize fresh delivery under quota reserve"
  ```

### Task 4: Back off maintenance and suppress repetitive enrichment retries

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/core.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

**Interfaces:**
- `maintenanceRetryAt({ now, resetAt, skipped, baseMs, maxMs }) -> string` returns a bounded ISO timestamp before the UTC reset when possible.
- `runMaintenanceTick()` returns `retryAt` and `retryReason: "storage_read_budget_guard"` on a guard skip.
- The maintenance Durable Object alarm accepts the returned retry timestamp and still re-arms before invoking the primary object.

- [ ] **Step 1: Add backoff and terminal-image tests**

  Cover exponential backoff capped at 15 minutes, reset-time cap, fresh primary alarm independence, image rows at max attempts excluded, confirmed-not-modified rows excluded, and next-attempt timestamps respected.

- [ ] **Step 2: Run focused tests and confirm failure**

  ```bash
  node --test test/core.test.js
  npm run test:worker -- --run test-worker/worker.integration.test.js
  ```

- [ ] **Step 3: Implement maintenance backoff**

  Compute the retry timestamp from the persisted `maintenanceSkipped` count and the budget reset time. Return it from the guard path. In `UolTelegramMaintenance.alarm`, keep the pre-work re-arm, then replace the normal cadence with `retryAt` when returned. Suppress urgent maintenance bootstrap from the primary alarm while the current budget is in reserve mode.

- [ ] **Step 4: Make common HTML enrichment adaptive**

  When the day has experienced quota skips, multiply the configured common-listing interval to at least five minutes while keeping immediate reconciliation for API failure/empty/invalid snapshots. Do not alter ticket probe delay, probe count, or sold-out editing.

- [ ] **Step 5: Tighten terminal image selection**

  Keep the existing initial image and late-upgrade paths. Add explicit predicates for terminal strategy/attempt states so maintenance cannot select exhausted or confirmed-not-modified rows; retain source-change reset behavior.

- [ ] **Step 6: Run focused tests and syntax checks**

  ```bash
  node --test test/core.test.js
  npm run test:worker -- --run test-worker/worker.integration.test.js
  node --check src/worker.js
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/core.js \
    cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js
  git commit -m "feat(uol): back off quota-bound maintenance"
  ```

### Task 5: Diagnostics, quota regression replay, and production verification

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/README.md`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Interfaces:**
- Existing authenticated readiness/diagnostic responses expose aggregate budget plus sanitized stage counters.
- No public endpoint or dashboard is added.

- [ ] **Step 1: Add quota regression scenarios**

  Run a 24-offer burst, repeated unchanged API cycles, quota-reserve delivery, ticket sold-out probes, restock, image late upgrade, ambiguous delivery, and historical comment closure. Assert stage counters remain bounded and aggregate accounting is monotonic.

- [ ] **Step 2: Add architecture assertions**

  Assert the primary scan computes the fingerprint before full reconciliation, does not move API polling behind HTML, keeps the delivery order, and re-arms primary/maintenance alarms safely.

- [ ] **Step 3: Update operator documentation**

  Document the no-op fingerprint, fresh-priority reserve, maintenance backoff, stage counters, and the fact that a readiness quota warning defers enrichment rather than blocking fresh main delivery.

- [ ] **Step 4: Run the focused/full project validation**

  ```bash
  npm run check:fast
  npm run test:worker -- --run test-worker/worker.integration.test.js
  node --test test/core.test.js test/queue-policy.test.js test/architecture.test.js
  git diff --check
  ```

- [ ] **Step 5: Commit documentation and final tests**

  ```bash
  git add cloudflare-workers/uol-telegram-shadow-worker/src/worker.js \
    cloudflare-workers/uol-telegram-shadow-worker/README.md \
    cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js \
    cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js
  git commit -m "test(uol): verify quota-efficient delivery"
  ```

- [ ] **Step 6: Deploy once and verify fresh production state**

  Run the existing deploy script only after focused checks pass. Confirm the deployed version through `/livez?verify=...`, inspect `/readyz?verify=...` for fresh scan/alarm, zero pending critical queue, no new unknown/dead-letter states, stage counters, and a lower unchanged-cycle read estimate. Run a bounded error tail for the deployed version. Do not claim quota improvement without this runtime comparison.
