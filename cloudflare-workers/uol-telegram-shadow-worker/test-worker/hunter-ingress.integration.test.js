import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import { dedupeCards } from "../src/core.js";

const token = `h1.test.${"a".repeat(64)}`;
const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
const observation = () => ({ eventId: crypto.randomUUID(), source: "portal",
  observedAt: new Date().toISOString(), payload: {
    link: "https://clube.uol.com.br/campanhasdeingresso/pAB-teste", title: "2 INGRESSOS: Peça teste",
    imageUrl: "https://d310rdc9v8uuq.cloudfront.net/ofertas/teste.jpg",
  } });
const verifiedPortal = async link => ({ status: "found", card: dedupeCards([{
  link, title: "2 INGRESSOS: Peça comprovada", category: "campanhasdeingresso",
  cardImageUrl: "https://d310rdc9v8uuq.cloudfront.net/ofertas/teste.jpg",
}])[0] });

describe("Hunter authenticated durable ingress", () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(new Date("2026-10-18T12:00:00Z")); });
  afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

  it("rejects admin/wake credentials and bounds input before mutation", async () => {
    for (const credential of ["vitest-admin-token-not-a-secret", `w1.test.${"a".repeat(64)}`]) {
      const response = await exports.default.fetch("https://test.invalid/hunter-intake", {
        method: "POST", headers: { ...headers, Authorization: `Bearer ${credential}` }, body: "{}",
      });
      expect(response.status).toBe(401);
    }
    const oversized = await exports.default.fetch("https://test.invalid/hunter-intake", {
      method: "POST", headers, body: "x".repeat(65537),
    });
    expect(oversized.status).toBe(413);
    const extra = await exports.default.fetch("https://test.invalid/hunter-intake", {
      method: "POST", headers, body: JSON.stringify({ observations: [observation()], cookie: "forbidden" }),
    });
    expect(extra.status).toBe(400);
  });

  it("persists receipt before progress; restart uses ordinary campaign identity with no duplicate", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("hunter-restart");
    const event = observation();
    await runInDurableObject(stub, async (instance, state) => {
      const alarm = await state.storage.getAlarm();
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true });
      instance.fetchHunterPortal = verifiedPortal;
      const drain = instance.drainHunterIntake;
      instance.drainHunterIntake = async () => ({ status: "deferred" });
      const ack = await instance.recordHunterIntake({ observations: [event] }, "test");
      expect(ack.receipts[0]).toMatchObject({ eventId: event.eventId, persisted: true, status: "queued" });
      expect(state.storage.sql.exec("SELECT count(*) n FROM offers").one().n).toBe(0);
      instance.metadataCache.clear();
      expect(instance.getHunterIntakeStatus("test", event.eventId).status).toBe("queued");
      const inserted = await instance.registerHunterPortal(event.payload, event);
      expect(inserted).toEqual({ persisted: true });
      expect(state.storage.sql.exec("SELECT count(*) n FROM offers").one().n).toBe(1);
      await instance.registerHunterPortal(event.payload, event);
      expect(state.storage.sql.exec("SELECT count(*) n FROM offers").one().n).toBe(1);
      instance.drainHunterIntake = drain;
      await instance.drainHunterIntake();
      expect(instance.getHunterIntakeStatus("test", event.eventId).status).toBe("registered");
      expect(state.storage.sql.exec("SELECT status FROM offers").one().status).toBe("pending_enrichment");
      await instance.processPending(new Map(), new Date());
      expect(state.storage.sql.exec("SELECT status FROM offers").one().status).toBe("shadow_candidate");
      expect(await state.storage.getAlarm()).toBe(alarm);
      expect(instance.storageUsage.primaryEstimatedRowsRead).toBe(0);
    });
  });

  it("returns an authenticated receipt through the phone status route", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor"), async instance => {
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true });
      instance.fetchHunterPortal = verifiedPortal;
    });
    const event = observation();
    const response = await exports.default.fetch("https://test.invalid/hunter-intake", {
      method: "POST", headers, body: JSON.stringify({ observations: [event] }),
    });
    expect(response.status).toBe(202);
    expect((await response.json()).receipts[0]).toMatchObject({ eventId: event.eventId, persisted: true });
    const receipt = await exports.default.fetch(`https://test.invalid/hunter-intake-status?eventId=${event.eventId}`, { headers });
    expect(receipt.status).toBe(200);
    expect(await receipt.json()).toMatchObject({ eventId: event.eventId, persisted: true });
    const replay = await exports.default.fetch("https://test.invalid/hunter-intake", {
      method: "POST", headers, body: JSON.stringify({ observations: [event] }),
    });
    expect((await replay.json()).receipts[0].duplicate).toBe(true);
  });

  it("optional budget denial accepts nothing; known sold-out campaign cannot be revived by a phone", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("hunter-budget-existing");
    await runInDurableObject(stub, async (instance, state) => {
      const event = observation();
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: false });
      await expect(instance.recordHunterIntake({ observations: [event] }, "test")).rejects.toThrow("hunter_budget_guard");
      expect(instance.metadataValue("hunter_intake_v1")).toBe("");
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true });
      instance.fetchHunterPortal = verifiedPortal;
      await instance.registerHunterPortal(event.payload, event);
      const row = state.storage.sql.exec("SELECT id FROM offers").one();
      state.storage.sql.exec("UPDATE offers SET status='sold_out', sold_out_at=?, title='Título comprovado', image_url='https://imguol.com.br/existing.jpg' WHERE id=?", event.observedAt, row.id);
      await instance.registerHunterPortal({ ...event.payload, title: "Título antigo" }, event);
      expect(state.storage.sql.exec("SELECT status,sold_out_at,title,image_url FROM offers WHERE id=?", row.id).one()).toEqual({
        status: "sold_out", sold_out_at: event.observedAt, title: "Título comprovado", image_url: "https://imguol.com.br/existing.jpg",
      });
      expect(state.storage.sql.exec("SELECT count(*) n FROM offers").one().n).toBe(1);
    });
  });

  it("does not publish a phone assertion when public verification is inconclusive", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("hunter-public-validation");
    await runInDurableObject(stub, async (instance, state) => {
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true });
      instance.fetchHunterPortal = async () => ({ status: "unknown", reason: "invalid_structure" });
      const event = observation();
      expect(await instance.registerHunterPortal(event.payload, event)).toEqual({ persisted: false });
      expect(state.storage.sql.exec("SELECT count(*) n FROM offers").one().n).toBe(0);
    });
  });
});
