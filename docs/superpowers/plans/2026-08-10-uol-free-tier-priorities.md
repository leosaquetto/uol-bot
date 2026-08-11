# UOL Free-Tier Priorities Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Neither execution skill is installed in this workspace, so the current native multi-agent workflow is the approved fallback.

**Goal:** Keep discovery and offer delivery inside Cloudflare's free limits by removing repeated Durable Object scans without slowing the normal 15-second polling lane or weakening delivery safety.

**Architecture:** Reuse the existing full-card SHA-256 fingerprint for HTML listings and perform a bounded periodic refresh, add partial SQLite indexes that match due comment and ticket predicates, and drive Discord availability work from its small synchronization table after a migration backfill. Preserve `offers` as the delivery source of truth and the existing delivery ledger; do not add a task table, external service, dependency, or paid product.

**Tech Stack:** Cloudflare Workers, Durable Objects SQLite, Wrangler 4, Node.js test runner, GitHub Actions free public-repository runners.

## Global Constraints

- Zero paid services or paid-plan assumptions; fit Workers and Durable Objects free tiers.
- Preserve the 15-second critical-source poll, current target ordering, retries, ambiguous-send handling, and alarm re-arming.
- Never skip a changed listing. Missing/corrupt fingerprints force the existing full reconciliation path.
- Reconcile an unchanged listing periodically so local state and source observations cannot drift indefinitely.
- Add only indexes whose expected read savings outweigh write amplification; do not add columns to the 97-column `offers` table.
- Keep health output sanitized and do not expose tokens, offer payloads, private destinations, or identifiers.

---

### Task 1: Prove the hot-path contracts before editing

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/migrations.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

- [ ] Add a full HTML-fingerprint test: reordering and observation timestamps are ignored; link, category, title, description, validity, or image changes invalidate the fingerprint.
- [ ] Add migration assertions for schema version 21 and the exact partial-index predicates.
- [ ] Add Workerd regressions for an identical maintenance listing, due comments, due ticket probes, and Discord sold-out/restock work.
- [ ] Record real `rowsRead` deltas for empty/no-change cycles and assert a conservative ceiling rather than a model-only estimate.
- [ ] Run focused tests and confirm the new assertions fail for the intended reason.

### Task 2: Add schema v21 free-tier indexes and safe Discord backfill

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/migrations.test.js`

**Migration contract:**
- `offers_comment_inflight_v21` covers only unresolved in-flight comments.
- `offers_comment_due_v21` matches the actionable comment predicate.
- `ticket_probe_due_v21` contains only nonempty due timestamps; replace the older broad index.
- `discord_avail_sold_due_v21` and `discord_avail_restock_due_v21` index unresolved synchronization rows.
- Backfill `discord_availability_sync` from existing sold-out/restocked offers with `INSERT OR IGNORE` before queries are driven by that table.

- [ ] Add migration 21 without `ALTER TABLE offers`.
- [ ] Validate ticket `next_at` values before relying on lexical ISO ordering; an unexpected value must remain visible to the old-safe fallback, not be deleted.
- [ ] Align comment and ticket SQL predicates literally with their partial indexes.
- [ ] Run migration and focused comment/ticket tests.

### Task 3: Skip unchanged HTML ledger reconciliation safely

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Runtime contract:**
- Metadata `runtime:html_snapshot_fingerprint` stores the last successfully reconciled complete listing.
- Metadata `runtime:html_snapshot_reconciled_at` limits the no-op window; default full refresh is 15 minutes and is configurable within a bounded range.
- Changed, incomplete, missing-fingerprint, corrupt-fingerprint, stale-refresh, and uninitialized snapshots use the current full reconciliation path.
- Unchanged fresh snapshots still update runtime source health, but skip `recordSourceCards`, `resolveListingCards`, `processPending`, sold-out evaluation, and pruning.

- [ ] Compute the fingerprint only from a complete two-listing snapshot.
- [ ] Gate ledger work only after source-health evaluation and before storage-heavy reconciliation.
- [ ] Persist the fingerprint and reconciliation timestamp only after the full path completes successfully.
- [ ] Prove changed listings and periodic refresh cannot be skipped.

### Task 4: Make Discord availability selection index-driven

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

- [ ] Replace the broad `offers LEFT JOIN ... WHERE sold-out OR restock` scan with two bounded branches starting from `discord_availability_sync`, joined to `offers` by primary key.
- [ ] Merge, sort by availability event time, deduplicate, and apply the original batch limit in JavaScript.
- [ ] Preserve `onlyIds`, attempts, retry timestamps, message selection, ledger events, and failure semantics.
- [ ] Test legacy rows from the migration backfill and simultaneous sold-out/restock candidates.

### Task 5: Add free-tier regression and release gates

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`
- Add: `cloudflare-workers/uol-telegram-shadow-worker/src/production-verification.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/scripts/verify-production.mjs`
- Add: `cloudflare-workers/uol-telegram-shadow-worker/test/production-verification.test.js`

- [ ] Assert real cursor `rowsRead` ceilings for unchanged primary, unchanged maintenance, due-comment, and due-ticket scenarios with terminal-history noise.
- [ ] Extract pure production-readiness checks and require current version, healthy identity/scan, zero critical pending queue, poll recommendation at most 15 seconds, and a bounded primary read estimate.
- [ ] Treat `maintenanceDeferred` as expected protection, not an automatic deployment failure.
- [ ] Run `npm test`, Workerd integration, types, startup, bundle dry-run, and `git diff --check`.
- [ ] Obtain fresh read-only correctness and release reviews, commit task files, push, merge after CI, deploy, and run the strengthened production check.
- [ ] Observe at least two post-deploy cycles; verify fresh scan, zero critical queue, no new unknown/dead-letter state, and lower per-cycle read growth before claiming improvement.

## Acceptance Criteria

- [ ] Changed/new offers retain the existing fast path and delivery guarantees.
- [ ] An unchanged maintenance listing performs no full offers-ledger reconciliation until the bounded refresh is due.
- [ ] Comment, ticket, and Discord maintenance queries are index-directed and bounded in noisy historical fixtures.
- [ ] Daily projected reads have enough margin below 5 million without reducing normal polling frequency.
- [ ] Daily projected writes remain below 100,000 with measured headroom.
- [ ] No paid service, new secret, external queue/database, or live-channel canary is introduced.
