# UOL Quota-Efficient Headless Delivery Design

**Status:** approved for implementation by the user on 2026-08-05.

## Goal

Reduce Durable Objects `rows_read` risk while preserving the existing 15-second API lane, fast first delivery, descriptions/comments, images, Discord-first image acquisition, Canal 2 delivery, ticket probes, sold-out edits, and idempotent delivery safety.

## Current context

The Worker has two Durable Objects: the primary polling/delivery object and a separate maintenance-alarm object. The primary object currently discovers the API every 15 seconds, persists/reconciles the API snapshot, processes priority delivery, runs ticket probes, and records source observations. Maintenance handles HTML reconciliation, comments, image upgrades, sold-out/restock reconciliation, repairs, and operational checks.

The free-tier guard already preserves a reserve for future primary scans and defers secondary maintenance under pressure. Production observations show that repeated primary SQLite work is the dominant quota risk; maintenance is often skipped before it can consume a large read budget. Therefore the implementation must reduce repeated primary reads first and must not solve the problem by slowing the normal polling lane.

## Design

### 1. API no-op fast path

Build a deterministic SHA-256 fingerprint from the API card fields that can change persistence or delivery decisions:

`id`, `link`, `previewTitle`, `title`, `category`, `cardImageUrl`, `partnerImageUrl`, `partnerName`, `imageUrl`, `validity`, and `description`.

Sort cards by `id`, exclude `observedAt`, and reuse the existing `sha256Hex` helper. Persist the last successful fingerprint in the existing metadata table under a runtime key; do not add an `offers` column or migration.

When the fingerprint is unchanged and the Worker has a prior initialized snapshot:

- skip full `resolveListingCards`, pending-detail enrichment, and API source-observation writes;
- process only bounded pending/unknown deliveries, ticket availability probes, and recovery actions that are independently due;
- keep the existing API request cadence and runtime health snapshots;
- on cold start with no fingerprint, malformed metadata, API contract change, or an API card change, use the existing full reconciliation path.

When the fingerprint changes, the existing new-offer path remains authoritative: persist the decision before transport, prioritize current-scan IDs, send Discord first when needed for image acquisition, send Telegram with the current image deadline/fallback, and deliver Canal 2 through the existing ledger.

### 2. One-scan delivery snapshot and fresh-offer reserve

Refactor only the selection boundary of `processDeliveryQueue` so one scan can reuse the already selected offer rows and classification results for its target sequence. Replace repeated broad reads with bounded indexed projections where the caller needs only status, IDs, timestamps, or delivery fields. Keep the current target order and per-target idempotency.

Extend the pure queue policy with a distinction between:

- **fresh priority work:** IDs inserted by the current API scan;
- **critical retry work:** already-discovered main delivery or ambiguous delivery;
- **secondary maintenance work:** old Discord/Canal 2 copies, comments, image upgrades, HTML-derived edits, and repairs.

Under a read reserve, fresh priority work and critical main delivery may use the existing delivery batch/concurrency caps. Old secondary work is deferred. The reserve is bounded by the current scan's priority IDs and is counted in the primary cycle; it never increases concurrency, retries, or polling frequency. If the hard read budget cannot safely admit secondary delivery, the existing fail-closed behavior remains allowed rather than risking duplicate/lost delivery.

This guarantees that quota pressure does not make a new offer wait behind historical enrichment during normal headroom, while retaining the current ledger and ambiguous-send rules.

### 3. Maintenance backoff and adaptive enrichment

When `runMaintenanceTick` exits through the read-budget guard, return a bounded retry timestamp/reason. The maintenance Durable Object must schedule its next alarm using that backoff instead of waking every minute. The primary alarm remains independent and continues to re-arm before reading storage.

The primary alarm must not request an immediate maintenance alarm while the current storage snapshot is in quota-reserve mode. This prevents a burst of new offers from repeatedly bypassing maintenance backoff.

When maintenance is allowed:

- keep ticket API probes and new-offer priority delivery out of the maintenance backoff;
- use a longer HTML interval for unchanged common-offer listings only while the read reserve is active;
- wake immediately on API failure, empty/invalid API result, or a meaningful source transition;
- keep ticket sold-out probes and edits on their current fast path;
- process comments and image upgrades in bounded, oldest-first batches after delivery/reconciliation work.

No new Durable Object, queue service, dashboard, or external database is introduced.

### 4. Image terminal states and retry suppression

Use the existing image strategy, attempt, next-attempt, proxy, cache, and message fields. Treat a confirmed photo, a confirmed not-modified edit, a text fallback whose retry budget is exhausted, a valid Discord proxy/cache result, and an image circuit-open state as terminal for the relevant operation until an explicit source/image change occurs.

Maintenance queries must exclude terminal image states and respect existing next-attempt timestamps. The initial image acquisition and same-message late upgrade remain unchanged; only repeated old-offer retries are reduced.

### 5. Stage-level quota telemetry

Extend the existing `runtime:storage_usage` JSON snapshot rather than adding tables or columns. Persist bounded counters for primary scan, delivery selection/refresh, ticket probes, maintenance ledger, HTML, comments, image work, and guard skips. Record counts only at existing cycle completion and only when a cycle changes the snapshot.

Expose the sanitized counters through existing authenticated diagnostics and readiness data. Do not add a user-facing dashboard, message content, URLs, tokens, or private identifiers to logs.

### 6. Historical incident lifecycle

Keep delivery unknowns, dead letters, blocked configuration, and current maintenance failures active until evidence-based resolution. A resolved event may remain in the audit timeline but must not continue to count as an active readiness incident after the existing health state has recovered. No automatic closure may retry, delete, or mutate a current offer delivery.

## Interfaces and invariants

- `buildApiSnapshotFingerprint(cards) -> Promise<string>`: sorted, normalized, SHA-256 fingerprint; excludes observation timestamps.
- `chooseDeliveryBudget(...)` gains a fresh-priority allowance/reason without increasing configured batch or concurrency maxima.
- `runMaintenanceTick(...)` may return `{ retryAt, retryReason }` on quota guard; the transport result contract remains backward-compatible.
- `storageUsageSnapshot()` retains current aggregate fields and adds sanitized stage counters in the same metadata payload.
- No new public route, user-facing dashboard, `offers` column, secret, external dependency, or delivery channel.
- The 15-second API polling configuration remains the default. The existing emergency budget-based interval recommendation may still slow the alarm only when the guard predicts that continuing would exhaust the free tier.
- Primary delivery remains at-least-once with the existing ledger/unknown resolution; no optimization may blindly replay an ambiguous external send.

## Failure handling

- Missing/corrupt fingerprint: fall back to the existing full API reconciliation once and rewrite the metadata fingerprint.
- API contract failure or empty invalid snapshot: preserve the previous inventory, do not clear offers, and keep the existing fail-closed incident behavior.
- Snapshot reuse failure: discard the in-memory reuse for that cycle and run the existing bounded query path.
- Quota reserve: defer only noncritical/old secondary work; never hide a pending main delivery or discard a newly discovered offer.
- Maintenance alarm failure: re-arm the alarm before work and retain the existing headless health signal.
- Image retry exhaustion: retain text/photo already delivered and stop repeated retries until a source/image change.

## Acceptance criteria

1. A no-change API replay performs strictly fewer Durable Object row reads than the current full reconciliation replay.
2. A new-offer replay keeps the current delivery order, descriptions/comments, image behavior, Discord metadata, Canal 2 delivery, and idempotency outcomes.
3. A changed API snapshot runs full reconciliation and discovers the change without relying on HTML.
4. Under `maintenanceAllowed === false`, a fresh offer is not delayed by old comments/image work; the primary lane remains fresh and the maintenance alarm backs off.
5. Ticket probes, sold-out detection, Discord sold-out edits, restock handling, ambiguous delivery, and historical unknown closure continue to pass existing replay/integration scenarios.
6. Stage counters reconcile with aggregate `rowsRead` within the bounded cycle-accounting tolerance and never expose secrets or offer content.
7. Focused unit/integration tests cover fingerprint cache miss/hit, malformed metadata, changed cards, priority reserve, maintenance backoff, terminal image states, and quota-budget regression.
8. Production verification confirms the deployed version, fresh scan/alarm, zero pending critical queue, no new unknown/dead-letter states, and a lower no-change read estimate before any claim of quota improvement.

## Rollout

Implement in small commits: fingerprint/no-op path, priority/snapshot policy, maintenance backoff, image/telemetry cleanup, then tests and documentation. Run focused tests and type checks before one deployment. After deployment, use readiness plus a bounded tail and compare a no-change cycle against the pre-deploy `primaryMaxRowsRead`/`primaryEstimatedRowsRead`. Roll back only the new optimization behavior if delivery latency, queue age, or unknown states regress; do not disable the existing quota guard.
