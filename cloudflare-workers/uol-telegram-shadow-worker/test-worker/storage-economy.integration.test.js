import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

describe("storage economy preserves durable transitions", () => {
  it("reuses loaded row and only writes changed aggregate fields", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("economy-delivery-noop");
    await runInDurableObject(stub, (instance, state) => {
      state.storage.sql.exec("INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, status, main_sent_at) VALUES ('noop', 'https://clube.uol.com.br/noop', '', '', '', 'delivered', '2026-10-10T12:00:00Z')");
      const row = instance.sqlExec("SELECT * FROM offers WHERE id = 'noop'").one();
      const queries = vi.spyOn(instance, "sqlExec");
      instance.refreshDeliveryStatus(row);
      expect(queries).not.toHaveBeenCalled();
      state.storage.sql.exec("UPDATE offers SET status = 'delivery_pending' WHERE id = 'noop'");
      const previous = { ...row, status: "delivery_pending" };
      instance.refreshDeliveryStatus(previous);
      expect(queries.mock.calls.filter(([query]) => query.startsWith("UPDATE offers"))).toHaveLength(1);
      queries.mockClear();
      instance.refreshDeliveryStatus(row);
      expect(queries).not.toHaveBeenCalled();
    });
  });

  it("stable contract keeps live freshness and checkpoints after 15 minutes or real change", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("economy-contract-checkpoint");
    await runInDurableObject(stub, instance => {
      const checked = new Date();
      expect(instance.setStableRuntimeSnapshot("api_contract", { ok: true, lastCheckedAt: checked.toISOString() })).toBe(true);
      expect(instance.setStableRuntimeSnapshot("api_contract", { ok: true, lastCheckedAt: new Date(checked.getTime() + 60_000).toISOString() })).toBe(false);
      expect(instance.runtimeSnapshot("api_contract").lastCheckedAt).not.toBe(checked.toISOString());
      instance.runtimeSnapshotCache.clear();
      expect(instance.runtimeSnapshot("api_contract").lastCheckedAt).toBe(checked.toISOString());
      expect(instance.setStableRuntimeSnapshot("api_contract", { ok: true, lastCheckedAt: new Date(checked.getTime() + 15 * 60_000).toISOString() })).toBe(true);
      expect(instance.setStableRuntimeSnapshot("api_contract", { ok: false, lastCheckedAt: new Date(checked.getTime() + 15 * 60_000 + 1).toISOString() })).toBe(true);
    });
  });

  it("stable unknown and dead-letter rows retain their first timestamps without repeated writes", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("economy-delivery-terminal-noop");
    await runInDurableObject(stub, (instance, state) => {
      instance.env = { ...instance.env, TELEGRAM_TOKEN: "fixture-no-network" };
      state.storage.sql.exec("INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, status) VALUES ('terminal', 'https://clube.uol.com.br/terminal', '', '', '', 'delivery_pending')");
      const row = state.storage.sql.exec("SELECT * FROM offers WHERE id = 'terminal'").one();
      const first = "2026-10-10T10:00:00Z";
      const queries = vi.spyOn(instance, "sqlExec");
      const unknown = { ...row, status: "delivery_unknown", delivery_unknown_at: first,
        delivery_unknown_target: "main", main_delivery_unknown_at: first };
      instance.refreshDeliveryStatus(unknown);
      expect(queries).not.toHaveBeenCalled();
      const dead = { ...row, status: "delivery_dead_letter", delivery_dead_letter_at: first,
        delivery_dead_letter_reason: "delivery_attempts_exhausted", main_delivery_attempts: 10 };
      instance.refreshDeliveryStatus(dead);
      expect(queries).not.toHaveBeenCalled();
    });
  });

  it("deferred alarms do not rewrite timestamps, skip counters or storage snapshot", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("economy-maintenance-defer");
    await runInDurableObject(stub, async instance => {
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: false, rowsRead: 4_500_000, limit: 5_000_000 });
      const first = await instance.runMaintenanceTick("alarm");
      const snapshot = instance.metadataValue("runtime:maintenance");
      const skipped = instance.storageUsage.maintenanceSkipped;
      const written = instance.storageUsage.rowsWritten;
      const next = await instance.runMaintenanceTick("alarm");
      expect(first.outcome).toBe("storage_read_budget_guard");
      expect(next.outcome).toBe("maintenance_deferred");
      expect(instance.metadataValue("runtime:maintenance")).toBe(snapshot);
      expect(instance.storageUsage.maintenanceSkipped).toBe(skipped);
      expect(instance.storageUsage.rowsWritten).toBe(written);
    });
  });

  it("global samples reject regression, deduplicate and release a stale metrics guard", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("economy-account-usage");
    await runInDurableObject(stub, instance => {
      instance.env = { ...instance.env, STORAGE_USAGE_GLOBAL_GUARD_ENABLED: "true" };
      expect(instance.storageUsageSnapshot().writeGuardReason).toBe("storage_global_metrics_stale");
      const now = new Date();
      const sample = { day: now.toISOString().slice(0, 10), observedAt: now.toISOString(), accountRowsRead: 100, accountRowsWritten: 1_000 };
      expect(instance.ingestStorageUsage(sample).accepted).toBe(true);
      const written = instance.storageUsage.rowsWritten;
      expect(instance.ingestStorageUsage(sample).accepted).toBe(false);
      expect(instance.storageUsage.rowsWritten).toBe(written);
      expect(instance.storageUsageSnapshot().maintenanceAllowed).toBe(true);
      expect(() => instance.ingestStorageUsage({ ...sample, accountRowsWritten: 999 })).toThrow("storage_usage_out_of_order");
      expect(instance.ingestStorageUsage({ ...sample, observedAt: new Date(now.getTime() + 1).toISOString(), accountRowsWritten: 80_000 }).budget.writeGuardReason).toBe("storage_write_budget_guard");
      instance.runtimeSnapshotCache.clear();
      expect(instance.storageUsageSnapshot().writeGuardReason).toBe("storage_write_budget_guard");
    });
  });

  it("aggregate ingest accepts only its scoped bearer token", async () => {
    const denied = await exports.default.fetch("https://worker.test/ingest-storage-usage", {
      method: "POST", headers: { Authorization: "Bearer vitest-admin-token-not-a-secret" }, body: "{}",
    });
    expect(denied.status).toBe(401);
    const now = new Date();
    const accepted = await exports.default.fetch("https://worker.test/ingest-storage-usage", {
      method: "POST", headers: { Authorization: "Bearer vitest-storage-usage-token-not-a-secret", "Content-Type": "application/json" },
      body: JSON.stringify({ day: now.toISOString().slice(0, 10), observedAt: now.toISOString(), accountRowsRead: 100, accountRowsWritten: 100 }),
    });
    expect(accepted.status).toBe(200);
    expect((await accepted.json()).accepted).toBe(true);
    const oversized = await exports.default.fetch("https://worker.test/ingest-storage-usage", {
      method: "POST", headers: { Authorization: "Bearer vitest-storage-usage-token-not-a-secret" }, body: " ".repeat(4_097),
    });
    expect(oversized.status).toBe(400);
  });
});
