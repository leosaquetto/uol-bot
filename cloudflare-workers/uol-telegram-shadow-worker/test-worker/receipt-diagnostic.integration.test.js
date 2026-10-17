import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

const admin = { Authorization: "Bearer vitest-admin-token-not-a-secret" };
const startedAt = "2026-10-17T03:09:54.675Z";
const unknownAt = "2026-10-17T03:10:02.675Z";

function insert(state, id, fields = {}) {
  const row = {
    id, link: `https://clube.uol.com.br/beneficios/${id}`, preview_title: "Fixture",
    first_seen_at: startedAt, last_seen_at: startedAt, status: "delivery_unknown",
    delivery_generation: 1, main_delivery_attempts: 1, main_delivery_unknown_at: unknownAt,
    ...fields,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

function event(instance, id, fields = {}) {
  instance.recordDeliveryLedgerEvent({
    dedupeKey: crypto.randomUUID(), offerId: id, target: "main", operation: "delivery",
    state: "attempt_started", generation: 1, attempt: 1, occurredAt: startedAt, ...fields,
  });
}

function pair(state, id, receivedAt) {
  state.storage.sql.exec(
    "INSERT INTO pending_discussion_forwards(origin_message_id, discussion_message_id, received_at) VALUES (?, ?, ?)",
    id, id + 1_000, receivedAt,
  );
}

describe("authenticated read-only main receipt diagnostics", () => {
  it("requires authorization and an explicit exact-ID opt-in", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor"), (instance, state) => {
      insert(state, "receipt-route");
      event(instance, "receipt-route");
      insert(state, "receipt-inactive", { main_delivery_unknown_at: "", status: "delivered" });
    });
    const url = "https://worker.test/inventory?id=receipt-route";
    expect((await exports.default.fetch(`${url}&receipts=true`)).status).toBe(401);
    expect((await exports.default.fetch(`${url}&receipts=true`, {
      headers: { Authorization: "Bearer invalid" },
    })).status).toBe(401);
    for (const suffix of ["", "&receipts=false", "&receipts=1"]) {
      const response = await exports.default.fetch(`${url}${suffix}`, { headers: admin });
      expect(response.status).toBe(200);
      expect((await response.json()).offer).not.toHaveProperty("receiptDiagnostics");
    }
    const response = await exports.default.fetch(`${url}&receipts=true`, { headers: admin });
    expect(response.status).toBe(200);
    expect((await response.json()).offer.receiptDiagnostics).toMatchObject({
      target: "main", generation: 1, attempt: 1, attemptStartedAt: startedAt,
    });
    const inactive = await exports.default.fetch("https://worker.test/inventory?id=receipt-inactive&receipts=true", { headers: admin });
    expect((await inactive.json()).offer).not.toHaveProperty("receiptDiagnostics");
    const inventory = await exports.default.fetch("https://worker.test/inventory?receipts=true", { headers: admin });
    expect((await inventory.json()).inventory.every(row => !Object.hasOwn(row, "receiptDiagnostics"))).toBe(true);
  });

  it("uses the indexed arrival-time window, includes boundaries and retains only the newest 32 pairs", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("receipt-window"), (instance, state) => {
      insert(state, "bounded");
      event(instance, "bounded");
      pair(state, 1, "2026-10-17T03:09:53.674Z");
      pair(state, 2, "2026-10-17T03:09:53.675Z");
      pair(state, 3, unknownAt);
      pair(state, 4, "2026-10-17T03:20:02.675Z");
      pair(state, 5, "2026-10-17T03:20:02.676Z");
      const queries = vi.spyOn(instance, "sqlExec");
      const diagnostics = instance.getInventoryById("bounded", { receipts: true }).receiptDiagnostics;
      expect(diagnostics.window).toEqual({
        timestamp: "receivedAt", from: "2026-10-17T03:09:53.675Z",
        through: "2026-10-17T03:20:02.675Z", limit: 32,
      });
      expect(diagnostics.pendingPairs.map(row => row.originMessageId)).toEqual([4, 3, 2]);
      expect(diagnostics.pendingPairs[0]).toEqual({
        originMessageId: 4, discussionMessageId: 1_004, receivedAt: "2026-10-17T03:20:02.675Z",
      });
      expect(diagnostics.scope).toContain("receipt arrival time");
      expect(diagnostics.scope).toContain("do not prove original post time or offer identity");
      const [query, ...bindings] = queries.mock.calls.find(([sql]) => sql.includes("FROM pending_discussion_forwards"));
      const plan = state.storage.sql.exec(`EXPLAIN QUERY PLAN ${query}`, ...bindings).toArray();
      expect(JSON.stringify(plan)).toContain("pending_discussion_received_idx");
      queries.mockRestore();
      for (let index = 0; index < 40; index++) pair(state, 100 + index, new Date(Date.parse(unknownAt) + index).toISOString());
      const limited = instance.getInventoryById("bounded", { receipts: true }).receiptDiagnostics.pendingPairs;
      expect(limited).toHaveLength(32);
      expect(limited[0].originMessageId).toBe(4);
      expect(limited[1].originMessageId).toBe(139);
      expect(limited.at(-1).originMessageId).toBe(109);
    });
  });

  it("returns 50 exact-attempt main events beyond the ordinary latest 20 and preserves the earliest start", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("receipt-exact-ledger"), (instance, state) => {
      insert(state, "ledger");
      event(instance, "ledger");
      for (let index = 0; index < 55; index++) event(instance, "ledger", {
        state: "unknown", occurredAt: new Date(Date.parse(startedAt) + index + 1).toISOString(),
      });
      for (let index = 0; index < 25; index++) event(instance, "ledger", {
        target: "discord", state: "failed", occurredAt: new Date(Date.parse(unknownAt) + index).toISOString(),
      });
      event(instance, "ledger", { generation: 2, occurredAt: unknownAt });
      event(instance, "ledger", { attempt: 2, occurredAt: unknownAt });
      event(instance, "ledger", { operation: "sold_out", occurredAt: unknownAt });
      const offer = instance.getInventoryById("ledger", { receipts: true });
      expect(offer.deliveryTimeline.every(row => row.target !== "main")).toBe(true);
      expect(offer.receiptDiagnostics.timeline).toHaveLength(50);
      expect(offer.receiptDiagnostics.timeline.every(row => row.target === "main" && row.operation === "delivery" && row.generation === 1 && row.attempt === 1)).toBe(true);
      expect(offer.receiptDiagnostics.timeline.some(row => row.state === "attempt_started")).toBe(false);
      expect(offer.receiptDiagnostics.attemptStartedAt).toBe(startedAt);
      expect(offer.receiptDiagnostics.earliestAttemptStartedAt).toBe(startedAt);
    });
  });

  it("uses the latest eligible start after a deferred attempt and prefers an active in-flight timestamp", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("receipt-latest-start"), (instance, state) => {
      insert(state, "deferred");
      event(instance, "deferred", { occurredAt: "2026-10-17T02:00:00.000Z" });
      event(instance, "deferred");
      event(instance, "deferred", { occurredAt: "2026-10-17T04:00:00.000Z" });
      pair(state, 1, "2026-10-17T02:00:01.000Z");
      pair(state, 2, unknownAt);
      const deferred = instance.getInventoryById("deferred", { receipts: true }).receiptDiagnostics;
      expect(deferred.attemptStartedAt).toBe(startedAt);
      expect(deferred.earliestAttemptStartedAt).toBe("2026-10-17T02:00:00.000Z");
      expect(deferred.pendingPairs.map(row => row.originMessageId)).toEqual([2]);
      insert(state, "active", {
        status: "delivery_pending", main_delivery_unknown_at: "", main_delivery_in_flight_at: startedAt,
      });
      event(instance, "active", { occurredAt: "2026-10-17T02:00:00.000Z" });
      const active = instance.getInventoryById("active", { receipts: true }).receiptDiagnostics;
      expect(active.attemptStartedAt).toBe(startedAt);
      expect(active.window.through).toBe("2026-10-17T03:19:54.675Z");
    });
  });

  it.each(["missing_start", "wrong_generation", "wrong_attempt", "invalid_start", "invalid_unknown", "unknown_before_start"])("does not suggest pairs for %s", async reason => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`receipt-invalid-${reason}`), (instance, state) => {
      insert(state, "invalid", {
        ...(reason === "invalid_unknown" ? { main_delivery_unknown_at: "2026-10-17" } : {}),
        ...(reason === "unknown_before_start" ? { main_delivery_unknown_at: "2026-10-17T03:00:00.000Z" } : {}),
      });
      if (reason !== "missing_start") event(instance, "invalid", {
        ...(reason === "wrong_generation" ? { generation: 2 } : {}),
        ...(reason === "wrong_attempt" ? { attempt: 2 } : {}),
        ...(reason === "invalid_start" ? { occurredAt: "2026-10-17" } : {}),
      });
      pair(state, 1, unknownAt);
      const diagnostics = instance.getInventoryById("invalid", { receipts: true }).receiptDiagnostics;
      expect(diagnostics.window).toBeNull();
      expect(diagnostics.pendingPairs).toEqual([]);
    });
  });

  it("performs zero writes and zero outbound fetches after fixture initialization", async () => {
    const outbound = vi.fn(() => { throw new Error("unexpected_outbound_fetch"); });
    vi.stubGlobal("fetch", outbound);
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("receipt-read-only"), (instance, state) => {
        insert(state, "read-only");
        event(instance, "read-only");
        pair(state, 1, unknownAt);
        const writesBefore = instance.storageUsage.rowsWritten;
        const queries = vi.spyOn(instance, "sqlExec");
        expect(instance.getInventoryById("read-only", { receipts: true }).receiptDiagnostics.pendingPairs).toHaveLength(1);
        expect(instance.storageUsage.rowsWritten).toBe(writesBefore);
        expect(queries.mock.calls.every(([query]) => /^\s*SELECT\b/.test(query))).toBe(true);
        queries.mockRestore();
      });
      expect(outbound).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
