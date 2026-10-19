import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { decodeAvailabilityEventCursor } from "../src/catalog.js";

const at = "2026-10-19T12:00:00.000Z";

function insertOffer(state, id) {
  state.storage.sql.exec(
    `INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, status)
     VALUES (?, ?, 'Teste', ?, ?, 'delivered')`,
    id, `https://clube.uol.com.br/campanhasdeingresso/${id}`, at, at,
  );
}

describe("public availability transition history", () => {
  it("records only real future transitions once, including a second sell-out after restock", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("availability-transition-events");
    await runInDurableObject(stub, (instance, state) => {
      insertOffer(state, "event-offer");
      expect(instance.getAvailabilityEvents().events).toEqual([]);
      state.storage.sql.exec(
        "UPDATE offers SET status = 'sold_out', sold_out_at = ? WHERE id = 'event-offer'",
        "2026-10-19T13:00:00.000Z",
      );
      state.storage.sql.exec(
        "UPDATE offers SET status = 'sold_out', sold_out_at = ? WHERE id = 'event-offer'",
        "2026-10-19T13:00:00.000Z",
      );
      state.storage.sql.exec(
        `UPDATE offers SET status = 'restocked_pending_sync', sold_out_at = '',
         restocked_at = ? WHERE id = 'event-offer'`,
        "2026-10-19T14:00:00.000Z",
      );
      state.storage.sql.exec(
        `UPDATE offers SET status = 'sold_out', sold_out_at = ?, restocked_at = ''
         WHERE id = 'event-offer'`,
        "2026-10-19T15:00:00.000Z",
      );
      expect(instance.getAvailabilityEvents()).toEqual({ ok: true, schemaVersion: 1,
        events: [
          { offerId: "event-offer", kind: "sold_out", occurredAt: "2026-10-19T15:00:00.000Z" },
          { offerId: "event-offer", kind: "restocked", occurredAt: "2026-10-19T14:00:00.000Z" },
          { offerId: "event-offer", kind: "sold_out", occurredAt: "2026-10-19T13:00:00.000Z" },
        ], nextCursor: null });
    });
  });

  it("pages by timestamp and ID without duplication or offset scans", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("availability-events-pagination");
    await runInDurableObject(stub, (instance, state) => {
      for (let i = 0; i < 102; i++) state.storage.sql.exec(
        `INSERT INTO offer_availability_events(offer_id, kind, occurred_at)
         VALUES (?, 'sold_out', ?)`, `offer-${i}`, at,
      );
      const first = instance.getAvailabilityEvents();
      expect(first.events).toHaveLength(100);
      expect(first.events[0].offerId).toBe("offer-101");
      expect(first.events[99].offerId).toBe("offer-2");
      const cursor = decodeAvailabilityEventCursor(first.nextCursor);
      expect(cursor).toMatchObject({ occurredAt: at });
      const second = instance.getAvailabilityEvents(cursor);
      expect(second.events.map(event => event.offerId)).toEqual(["offer-1", "offer-0"]);
      expect(second.nextCursor).toBeNull();
      const plan = state.storage.sql.exec(
        `EXPLAIN QUERY PLAN SELECT id, offer_id, kind, occurred_at
         FROM offer_availability_events WHERE (occurred_at, id) < (?, ?)
         ORDER BY occurred_at DESC, id DESC LIMIT ?`, at, cursor.id, 101,
      ).toArray();
      expect(JSON.stringify(plan)).toContain("offer_availability_events_recent_v27");
    });
  });

  it("enforces read-only GET, public CORS, and a validated cursor", async () => {
    const url = "https://catalog.runtime.test/catalog/availability-events";
    const preflight = await exports.default.fetch(url, { method: "OPTIONS" });
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("Access-Control-Allow-Origin")).toBe("*");
    const denied = await exports.default.fetch(url, { method: "POST" });
    expect(denied.status).toBe(405);
    const invalid = await exports.default.fetch(`${url}?cursor=bad!`);
    expect(invalid.status).toBe(400);
    const result = await exports.default.fetch(url);
    expect(result.status).toBe(200);
    expect(result.headers.get("Cache-Control")).toBe("no-store");
    expect(await result.json()).toMatchObject({ ok: true, schemaVersion: 1,
      events: expect.any(Array), nextCursor: null });
  });
});
