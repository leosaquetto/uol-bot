import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CATALOG_OFFERS_SQL, CATALOG_STORIES_SQL } from "../src/catalog.js";

function insertOffer(state, id, at, fields = {}) {
  const row = { id, link: `https://clube.uol.com.br/campanhasdeingresso/pABC-${id}`,
    preview_title: "Peça teste", first_seen_at: at, last_seen_at: at, status: "delivery_pending", ...fields };
  state.storage.sql.exec(`INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`, ...Object.values(row));
}

function insertStory(state, id, at, expiry) {
  state.storage.sql.exec(`INSERT INTO instagram_story_outbox(canonical_key, story_id, link,
    published_at, expires_at, first_seen_at, updated_at, image_url, image_width, image_height, targets_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 10, 10, ?)`, id, id,
  `https://clube.uol.com.br/campanhasdeingresso/pABC-${id}`, at, expiry, at, at,
  "https://scontent.cdninstagram.com/a.jpg", '{"private":"not-public"}');
}

describe("bounded public catalog in Workers SQLite", () => {
  beforeEach(() => {
    // At 00:00 UTC the primary read reserve can legitimately consume the daily
    // budget. Exercise catalog SQL at a fixed time with optional budget left.
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-17T12:00:00.000Z"));
  });
  afterEach(() => { vi.useRealTimers(); });

  it("reads at most 700 real rows cold and zero warm, with no writes, source calls or primary estimate changes", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("catalog-native-bounds");
    await runInDurableObject(stub, async (instance, state) => {
      const now = Date.now(), at = new Date(now).toISOString();
      for (let i = 0; i < 420; i++) insertOffer(state, `offer-${i}`, new Date(now - i * 1000).toISOString(), {
        ...(i === 0 ? { status: "sold_out", sold_out_at: at } : {}),
        ...(i === 1 ? { status: "discarded" } : {}),
        ...(i === 2 ? { link: "https://clube.uol.com.br/outro/beneficio" } : {}),
      });
      for (let i = 0; i < 1000; i++) insertStory(state, `expired-${i}`, at, new Date(now - 86400000).toISOString());
      for (let i = 0; i < 40; i++) insertStory(state, `active-${i}`, at, new Date(now + 86400000 + i * 1000).toISOString());
      instance.setCriticalSourceSnapshot({ api: { lastSuccessAt: at, lastError: "http_429", cookie: "private" },
        ticketListing: { lastSuccessAt: at, cards: [{ link: "https://clube.uol.com.br/campanhasdeingresso/pABC-offer-0" }] } });
      instance.metadataCache.clear(); instance.runtimeSnapshotCache.clear();
      instance.storageUsage.primaryEstimatedRowsRead = 512;
      expect(instance.storageUsageSnapshot().maintenanceAllowed).toBe(true);
      const before = { ...instance.storageUsage }, alarm = await state.storage.getAlarm();
      const sourceFetch = vi.fn(() => { throw new Error("catalog must never fetch sources"); });
      vi.stubGlobal("fetch", sourceFetch);
      try {
        const cold = instance.getPublicCatalog();
        // Native cursor includes 301 offers + 31 active Stories; index traversal
        // is not assumed to double the billed row count.
        expect(cold.rowsRead).toBeGreaterThanOrEqual(332);
        expect(cold.rowsRead).toBeLessThanOrEqual(700);
        expect(cold.payload.offers).toHaveLength(298);
        expect(cold.payload.offers[0]).toMatchObject({ availability: "sold_out", isPortal: true });
        expect(cold.payload.stories).toHaveLength(30);
        expect(cold.payload.coverage).toMatchObject({ truncated: true, scope: "recent_tracked" });
        expect(cold.payload.sources).toMatchObject({ api: { healthy: false }, ticketListing: { healthy: true } });
        expect(JSON.stringify(cold.payload)).not.toContain("private");
        const warm = instance.getPublicCatalog();
        expect(warm).toMatchObject({ cached: true, rowsRead: 0 });
        expect(warm.payload).toEqual(cold.payload);
        expect(instance.storageUsage.rowsWritten).toBe(before.rowsWritten);
        expect(instance.storageUsage.primaryEstimatedRowsRead).toBe(512);
        expect(instance.storageUsage.primaryLastCompletedAt).toBe(before.primaryLastCompletedAt);
        expect(await state.storage.getAlarm()).toBe(alarm);
        expect(sourceFetch).not.toHaveBeenCalled();
        const offerPlan = state.storage.sql.exec("EXPLAIN QUERY PLAN " + CATALOG_OFFERS_SQL, 301).toArray();
        const storyPlan = state.storage.sql.exec("EXPLAIN QUERY PLAN " + CATALOG_STORIES_SQL, at, 31).toArray();
        expect(JSON.stringify(offerPlan)).toContain("offers_first_seen_idx");
        expect(JSON.stringify(storyPlan)).toContain("instagram_story_expiry_idx");
      } finally { vi.unstubAllGlobals(); }
    });
  });

  it("denied optional budget performs no catalog SQL; expired cache must not fabricate an empty success", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("catalog-budget-denied");
    await runInDurableObject(stub, instance => {
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: false });
      instance.sqlExec = () => { throw new Error("denied budget must not query"); };
      expect(instance.getPublicCatalog()).toEqual({ error: "catalog_budget_guard", rowsRead: 0 });
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, maintenanceCeiling: 2000, rowsRead: 1301 });
      expect(instance.getPublicCatalog()).toEqual({ error: "catalog_budget_guard", rowsRead: 0 });
    });
  });

  it("refreshes changed observations after memory TTL without writes", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("catalog-cache-refresh");
    await runInDurableObject(stub, (instance, state) => {
      const at = new Date().toISOString();
      insertOffer(state, "changing", at);
      expect(instance.getPublicCatalog().payload.offers[0].availability).toBe("observed");
      state.storage.sql.exec("UPDATE offers SET status = 'sold_out', sold_out_at = ? WHERE id = 'changing'", at);
      expect(instance.getPublicCatalog().payload.offers[0].availability).toBe("observed");
      instance.catalogCache.expiresAt = Date.now() - 1;
      const before = instance.storageUsage.rowsWritten;
      expect(instance.getPublicCatalog().payload.offers[0].availability).toBe("sold_out");
      expect(instance.storageUsage.rowsWritten).toBe(before);
    });
  });

  it("GET/conditional GET and preflight use public CORS and the same normalized snapshot", async () => {
    const preflight = await exports.default.fetch("https://catalog.runtime.test/catalog", { method: "OPTIONS",
      headers: { Origin: "https://hunter.test", "Access-Control-Request-Headers": "If-None-Match" } });
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("Access-Control-Allow-Headers")).toBe("If-None-Match");
    const first = await exports.default.fetch("https://catalog.runtime.test/catalog?limit=1");
    expect(first.status).toBe(200);
    const payload = await first.json();
    expect(payload).toMatchObject({ ok: true, schemaVersion: 1, coverage: { offerLimit: 300, storyLimit: 30 } });
    const again = await exports.default.fetch("https://catalog.runtime.test/catalog?limit=99999&account=ignored", {
      headers: { "If-None-Match": first.headers.get("ETag") },
    });
    expect(again.status).toBe(304);
    expect(again.headers.get("X-Catalog-Rows-Read")).toBe("0");
    expect(again.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(await again.text()).toBe("");
  });
});
