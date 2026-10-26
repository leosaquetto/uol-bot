import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

const now = new Date("2026-10-26T13:00:00.000Z");
const seenAt = "2026-10-26T12:00:00.000Z";
const patterns = ["teatro-tuca", "morumbi-park-sp", "ecovilla", "teatro-liberdade",
  "teatro-multiplan", "teatro-das-artes", "museu-da-imaginacao-sp", "teatro-vanucci-rj", "teatro-riachuelo-rj"];

function insert(state, id, slug = id, at = seenAt) {
  state.storage.sql.exec(`INSERT INTO offers(id, link, preview_title, title, category,
    first_seen_at, last_seen_at, status, discord_sent_at)
    VALUES (?, ?, 'teatro-tuca', 'teatro-tuca', 'campanhasdeingresso', ?, ?, 'delivered', ?)`,
  id, `https://clube.uol.com.br/campanhasdeingresso/pABC-${slug}`, at, at, at);
  return state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();
}

function configure(instance) {
  instance.env = { ...instance.env, BEEPER_DELIVERY_ENABLED: "true",
    BEEPER_DESTINATION_KEY: "whatsapp-group", BEEPER_GATEWAY_URL: "https://beeper.test/v1/send-offer",
    BEEPER_GATEWAY_TOKEN: "test-exclusion-token", DELIVERY_MAX_ATTEMPTS: "10" };
  instance.setMetadata("beeper_delivery_enabled_state", "enabled");
  instance.setMetadata("beeper_delivery_activated_at", seenAt);
}

describe("WhatsApp offer exclusions", () => {
  it("filters enqueue, backfill, recovery and existing mixed batches without consuming excluded attempts", async () => {
    const calls = [];
    vi.stubGlobal("fetch", async (url, init = {}) => {
      if (String(url) === "https://beeper.test/v1/readyz/uol") {
        return Response.json({ ok: true, deliveryConfirmation: "confirmed_by_whatsapp_bridge" });
      }
      expect(String(url)).toBe("https://beeper.test/v1/send-offer");
      calls.push(JSON.parse(init.body).link);
      return Response.json({ pendingMessageID: `receipt-${calls.length}`,
        deliveryState: "confirmed_by_whatsapp_bridge" }, { status: 202 });
    });
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("whatsapp-exclusions-mixed"), async (instance, state) => {
        configure(instance);
        const excludedIds = patterns.map((pattern, index) => {
          const id = `excluded-${index}`;
          const row = insert(state, id, `${pattern.toUpperCase()}-extra`);
          expect(instance.enqueueBeeperOffer(row)).toBe(false);
          return id;
        });
        const allowedAt = "2026-10-26T12:01:00.000Z";
        expect(instance.enqueueBeeperOffer(insert(state, "direct-allowed", "direct-allowed", allowedAt))).toBe(true);
        insert(state, "backfill-allowed", "backfill-allowed", allowedAt);
        expect(instance.ensureBeeperDeliveryActivation(now).queued).toBeGreaterThan(0);
        expect(state.storage.sql.exec("SELECT offer_id FROM beeper_delivery_queue ORDER BY offer_id").toArray())
          .toEqual([{ offer_id: "backfill-allowed" }, { offer_id: "direct-allowed" }]);

        // Leave one excluded offer outside the queue to exercise recovery insertion too.
        for (const id of excludedIds.slice(1)) state.storage.sql.exec(
          "INSERT INTO beeper_delivery_queue(offer_id) VALUES (?)", id);
        state.storage.sql.exec(`UPDATE beeper_delivery_queue SET attempts = 10,
          last_error = 'ambiguous:beeper_send_response_ambiguous' WHERE offer_id = 'excluded-1'`);
        state.storage.sql.exec(`UPDATE beeper_delivery_queue SET attempts = 2,
          in_flight_at = '2026-10-26T11:00:00.000Z' WHERE offer_id = 'excluded-2'`);
        const excludedBefore = state.storage.sql.exec(
          "SELECT * FROM beeper_delivery_queue WHERE offer_id LIKE 'excluded-%' ORDER BY offer_id").toArray();
        const offersBefore = state.storage.sql.exec("SELECT * FROM offers ORDER BY id").toArray();
        const ids = [...excludedIds, "direct-allowed", "backfill-allowed"].sort();
        instance.env.BEEPER_DELIVERY_OFFER_IDS = ids.join(",");
        const recovery = instance.applyBeeperDeliveryRecoveryFilter({ offerIds: ids });
        expect(recovery).toMatchObject({ applied: true, queued: 0 });
        expect(recovery.reset).toBeGreaterThan(0);
        expect(instance.beeperQueueHasDueWork(now)).toBe(true);

        for (let i = 0; i < 2; i++) {
          expect(await instance.processBeeperDeliveryQueue(now, 1)).toMatchObject({ sent: 1, failed: 0, blocked: false });
        }
        expect(calls.sort()).toEqual(["backfill-allowed", "direct-allowed"].map(id =>
          `https://clube.uol.com.br/campanhasdeingresso/pABC-${id}`));
        expect(state.storage.sql.exec(
          "SELECT * FROM beeper_delivery_queue WHERE offer_id LIKE 'excluded-%' ORDER BY offer_id").toArray()).toEqual(excludedBefore);
        expect(state.storage.sql.exec("SELECT * FROM offers ORDER BY id").toArray()).toEqual(offersBefore);
        expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE offer_id LIKE 'excluded-%'").one().count).toBe(0);
        expect(instance.beeperQueueDiagnostics(10)).toMatchObject({ pending: 2, unknown: 1, inFlight: 1, exhausted: 1 });
        expect(instance.getReleaseDebt().checks).toMatchObject({ deadLetters: 1, unknown: 1 });
        expect(instance.beeperQueueHasDueWork(now)).toBe(false);
        expect(await instance.processCriticalBeeperDeliveryQueue(now)).toMatchObject({ sent: 0, skipped: true });
        expect(calls).toHaveLength(2);
      });
    } finally { vi.unstubAllGlobals(); }
  });

  it("does not probe the gateway for excluded-only work or report safe exclusions as pending debt", async () => {
    const fetchMock = vi.fn(() => { throw new Error("unexpected_network"); });
    vi.stubGlobal("fetch", fetchMock);
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("whatsapp-exclusions-only"), async (instance, state) => {
        configure(instance);
        for (let i = 0; i < 3; i++) {
          const id = `excluded-${i}`;
          insert(state, id, patterns[i]);
          state.storage.sql.exec("INSERT INTO beeper_delivery_queue(offer_id, attempts, last_error) VALUES (?, 10, 'terminal:beeper_http_400')", id);
        }
        expect(await instance.processBeeperDeliveryQueue(now)).toMatchObject({ sent: 0, failed: 0, blocked: false });
        expect(instance.beeperQueueHasDueWork(now)).toBe(false);
        expect(instance.beeperQueueDiagnostics(10)).toMatchObject({ pending: 0, unknown: 0, exhausted: 0, inFlight: 0 });
        expect(instance.getReleaseDebt().checks).toMatchObject({ deadLetters: 0, unknown: 0 });
        expect(fetchMock).not.toHaveBeenCalled();
      });
    } finally { vi.unstubAllGlobals(); }
  });

  it("retains uncertain and in-flight evidence while normal retention removes old safe excluded pending rows", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("whatsapp-exclusions-retention"), (instance, state) => {
      configure(instance);
      for (const id of ["safe", "uncertain", "active", "receipt", "ordinary"]) {
        insert(state, id, id === "ordinary" ? id : `ecovilla-${id}`, "2026-01-01T12:00:00.000Z");
        state.storage.sql.exec("INSERT INTO beeper_delivery_queue(offer_id) VALUES (?)", id);
      }
      state.storage.sql.exec("UPDATE beeper_delivery_queue SET attempts = 10, last_error = 'ambiguous:delivery_unknown' WHERE offer_id = 'uncertain'");
      state.storage.sql.exec("UPDATE beeper_delivery_queue SET in_flight_at = ? WHERE offer_id = 'active'", now.toISOString());
      state.storage.sql.exec("UPDATE beeper_delivery_queue SET pending_message_id = 'pending-receipt' WHERE offer_id = 'receipt'");
      instance.recordDeliveryLedgerEvent({ offerId: "uncertain", target: "beeper", operation: "delivery", state: "unknown" });
      const before = state.storage.sql.exec("SELECT * FROM beeper_delivery_queue WHERE offer_id <> 'safe' ORDER BY offer_id").toArray();
      instance.pruneOffers(now);
      expect(state.storage.sql.exec("SELECT id FROM offers ORDER BY id").toArray().map(row => row.id))
        .toEqual(["active", "ordinary", "receipt", "uncertain"]);
      expect(state.storage.sql.exec("SELECT * FROM beeper_delivery_queue ORDER BY offer_id").toArray()).toEqual(before);
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE offer_id = 'uncertain'").one().count).toBe(1);
    });
  });
});
