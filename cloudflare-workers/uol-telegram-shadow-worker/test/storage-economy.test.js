import test from "node:test";
import assert from "node:assert/strict";
import { normalizeAccountStorageUsage, storageWriteBudget } from "../src/core.js";
import { ticketCodeCardFingerprint, recordTicketCodeResults, unresolvedTicketCodeEntries } from "../src/ticket-code-policy.js";

const now = new Date("2026-10-10T12:00:00.000Z");
const sample = { day: "2026-10-10", observedAt: now.toISOString(), accountRowsRead: 1_000,
  accountRowsWritten: 70_000, localRowsWrittenAtReceipt: 50_000 };

test("global write reserve counts local increments and never treats stale/absent metrics as zero", () => {
  assert.equal(storageWriteBudget({ sample, now, localRowsWritten: 50_100 }).accountRowsWritten, 70_100);
  assert.equal(storageWriteBudget({ sample, now, localRowsWritten: 50_100 }).writeMaintenanceAllowed, true);
  assert.equal(storageWriteBudget({ sample, now, localRowsWritten: 60_000 }).writeGuardReason, "storage_write_budget_guard");
  assert.equal(storageWriteBudget({ sample, now: new Date(now.getTime() + 30 * 60_000 + 1) }).writeGuardReason, "storage_global_metrics_stale");
  assert.equal(storageWriteBudget({ now }).writeMaintenanceAllowed, false);
});

test("UTC rollover discards prior-day aggregate and waits for fresh evidence", () => {
  // `sample` was observed at noon: not fresh at midnight, so no grace.
  const budget = storageWriteBudget({ sample, localRowsWritten: 30, now: new Date("2026-10-11T00:00:01Z") });
  assert.equal(budget.accountRowsWritten, 30);
  assert.equal(budget.globalFresh, false);
  assert.equal(budget.rolloverGrace, false);
  assert.equal(budget.writeMaintenanceAllowed, false);
});

test("UTC rollover grace bridges the first minutes of the day with a bounded estimate", () => {
  // Last sample of the day, 10 min before midnight, well below the ceiling (16/11/2026 incident shape).
  const lastOfDay = { ...sample, observedAt: "2026-10-10T23:50:00.000Z", accountRowsWritten: 43_200,
    localRowsWrittenAtReceipt: 9_000 };
  const at = iso => storageWriteBudget({ sample: lastOfDay, localRowsWritten: 100, now: new Date(iso) });
  const early = at("2026-10-11T00:05:00Z");
  assert.equal(early.globalFresh, true);
  assert.equal(early.rolloverGrace, true);
  assert.equal(early.writeMaintenanceAllowed, true);
  assert.equal(early.writeGuardReason, "");
  // 100 local + twice yesterday's average rate for 5 minutes (43,200 × 2 × 5/1440 = 300).
  assert.equal(early.accountRowsWritten, 400);
  assert.equal(at("2026-10-11T00:45:00Z").rolloverGrace, true);
  // Past the window the old fail-closed rule applies.
  const late = at("2026-10-11T00:45:01Z");
  assert.equal(late.globalFresh, false);
  assert.equal(late.writeGuardReason, "storage_global_metrics_stale");
  // Before midnight the same sample is simply fresh same-day evidence.
  assert.equal(storageWriteBudget({ sample: lastOfDay, localRowsWritten: 9_000, now: new Date("2026-10-10T23:59:00Z") }).rolloverGrace, false);
});

test("UTC rollover grace never applies to stale, near-ceiling, older or future samples", () => {
  const base = { ...sample, observedAt: "2026-10-10T23:50:00.000Z", accountRowsWritten: 43_200 };
  const now = new Date("2026-10-11T00:05:00Z");
  for (const bad of [
    { ...base, observedAt: "2026-10-10T23:29:59.000Z" },            // already stale at midnight
    { ...base, accountRowsWritten: 80_000 },                         // yesterday reached the ceiling
    { ...base, day: "2026-10-09", observedAt: "2026-10-09T23:50:00.000Z" }, // two days old
    { ...base, observedAt: "2026-10-11T00:01:00.000Z" },             // claims yesterday, observed today
    { ...base, accountRowsWritten: undefined },
    {},
  ]) {
    const budget = storageWriteBudget({ sample: bad, localRowsWritten: 10, now });
    assert.equal(budget.rolloverGrace, false, JSON.stringify(bad));
    assert.equal(budget.writeMaintenanceAllowed, false, JSON.stringify(bad));
  }
  // Grace estimate that crosses the ceiling still blocks maintenance.
  // 79,500 local + 549 estimated (79,000 × 2 × 5/1440) = 80,049 ≥ 80,000.
  const busy = storageWriteBudget({ sample: { ...base, accountRowsWritten: 79_000 }, localRowsWritten: 79_500, now });
  assert.equal(busy.rolloverGrace, true);
  assert.equal(busy.writeMaintenanceAllowed, false);
  assert.equal(busy.writeGuardReason, "storage_write_budget_guard");
});

test("usage samples are bounded nonnegative integer UTC evidence", () => {
  assert.equal(normalizeAccountStorageUsage(sample, now).accountRowsWritten, 70_000);
  for (const invalid of [{ ...sample, day: "2026-10-09" }, { ...sample, accountRowsWritten: -1 },
    { ...sample, accountRowsRead: 1.5 }, { ...sample, observedAt: "2026-10-10T12:00:00-03:00" },
    { ...sample, observedAt: "2026-10-10T11:29:59Z" }, { ...sample, observedAt: "2026-10-10T12:02:00Z" }]) {
    assert.throws(() => normalizeAccountStorageUsage(invalid, now), /storage_usage_invalid/);
  }
});

test("optional-work hint preserves old payloads and validates a bounded coherent pair", () => {
  assert.equal(normalizeAccountStorageUsage(sample, now).optionalWorkDeferred, undefined);
  for (const optionalWorkReason of ["quota_actual", "quota_forecast", "quota_metrics_stale"]) {
    const normalized = normalizeAccountStorageUsage({ ...sample, optionalWorkDeferred: true, optionalWorkReason }, now);
    assert.equal(normalized.optionalWorkDeferred, true);
    assert.equal(normalized.optionalWorkReason, optionalWorkReason);
  }
  assert.equal(normalizeAccountStorageUsage({ ...sample, optionalWorkDeferred: false, optionalWorkReason: "none" }, now).optionalWorkDeferred, false);
  for (const hint of [{ optionalWorkDeferred: true }, { optionalWorkReason: "none" },
    { optionalWorkDeferred: "false", optionalWorkReason: "none" },
    { optionalWorkDeferred: true, optionalWorkReason: "none" },
    { optionalWorkDeferred: false, optionalWorkReason: "quota_actual" },
    { optionalWorkDeferred: true, optionalWorkReason: "unbounded" }]) {
    assert.throws(() => normalizeAccountStorageUsage({ ...sample, ...hint }, now), /storage_usage_invalid/);
  }
});

test("hidden-ticket durable result survives restart without repeated enrichment; changed content and restock re-enter", async () => {
  const card = { id: "pPQ", link: "https://clube.uol.com.br/campanhasdeingresso/pPQ-show", previewTitle: "Show", apiDetail: { description: "Zayn" } };
  const fingerprint = await ticketCodeCardFingerprint(card);
  let state = { entries: { pPQ: {} }, requestsUsed: 2 };
  state = recordTicketCodeResults(state, ["pPQ"], [{ status: "found", card, fingerprint, requests: 2 }], now.getTime());
  assert.equal(unresolvedTicketCodeEntries(JSON.parse(JSON.stringify(state))).length, 1);
  Object.assign(state.entries.pPQ, { resolvedId: "pPQ", resolvedFingerprint: fingerprint, resolvedAvailabilityEpoch: 0 });
  state = recordTicketCodeResults(state, ["pPQ"], [{ status: "found", card, fingerprint, requests: 2 }], now.getTime() + 1);
  assert.equal(unresolvedTicketCodeEntries(state).length, 0);
  const changed = { ...card, apiDetail: { description: "Zayn nova validade" } };
  assert.notEqual(await ticketCodeCardFingerprint(changed), fingerprint);
  state = recordTicketCodeResults(state, ["pPQ"], [{ status: "absent", requests: 1 }], now.getTime() + 2);
  state = recordTicketCodeResults(state, ["pPQ"], [{ status: "found", card, fingerprint, requests: 2 }], now.getTime() + 3);
  assert.equal(unresolvedTicketCodeEntries(state).length, 1);
  assert.equal(state.entries.pPQ.availabilityEpoch, 1);
});
