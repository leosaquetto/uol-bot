import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

const at = new Date("2026-10-16T03:00:00.000Z");
function insert(state, id, { nextAt = at.toISOString(), attempts = 0, firstSeenAt = at.toISOString() } = {}) {
  state.storage.sql.exec(
    `INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at,
      status, missing_since, absence_count, main_sent_at, main_message_id)
     VALUES (?, ?, 'Fixture', ?, ?, 'delivered', ?, 2, ?, 1)`,
    id, `https://clube.uol.com.br/campanhasdeingresso/${id}`, firstSeenAt,
    firstSeenAt, firstSeenAt, firstSeenAt,
  );
  state.storage.sql.exec(
    `INSERT INTO ticket_probe_state(offer_id,next_at,last_result,attempts)
     VALUES (?, ?, 'listing_absence_pending', ?)`, id, nextAt, attempts,
  );
}
function configure(instance) {
  instance.setMetadata("delivery_mode_override", "live");
  instance.env = { ...instance.env, TICKET_SOLD_OUT_PROBE_DAILY_LIMIT: "256", TICKET_SOLD_OUT_PROBES_PER_SCAN: "1" };
}
const available = async () => new Response("Offer details", { status: 200 });

describe("ticket probe indexed queue selection", () => {
  it("bounds native rows read with 282 equal deadlines using the existing index", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("ticket-probe-index-cost"), async (instance, state) => {
      configure(instance);
      for (let index = 0; index < 282; index++) insert(state, `fixture-${String(index).padStart(3, "0")}`, {
        firstSeenAt: new Date(at.getTime() - (282 - index) * 1000).toISOString(),
      });
      const execute = instance.sqlExec.bind(instance);
      let proof;
      instance.sqlExec = (query, ...bindings) => {
        if (!proof && query.includes("FROM ticket_probe_state AS s INDEXED BY ticket_probe_due_v21")) {
          const previousQuery = query.replace(
            "ORDER BY s.next_at ASC, s.attempts ASC, s.offer_id ASC",
            "ORDER BY s.next_at ASC, o.first_seen_at ASC",
          );
          expect(previousQuery).not.toBe(query);
          const previous = state.storage.sql.exec(previousQuery, ...bindings);
          const previousRows = previous.toArray();
          const current = execute(query, ...bindings);
          const currentRows = current.toArray();
          proof = { previousReads: previous.rowsRead, currentReads: current.rowsRead,
            previousIds: previousRows.map(row => row.id), currentIds: currentRows.map(row => row.id) };
          return { toArray: () => currentRows };
        }
        return execute(query, ...bindings);
      };
      const result = await instance.processTicketAvailabilityProbes(at, { fetchImpl: available, controlCards: [] });
      expect(result.probed).toBe(1);
      expect(proof.previousIds).toEqual(["fixture-000"]);
      expect(proof.currentIds).toEqual(proof.previousIds);
      expect(proof.previousReads).toBeGreaterThanOrEqual(282);
      expect(proof.currentReads).toBeLessThan(proof.previousReads / 10);
      expect(proof.currentReads).toBeLessThanOrEqual(8);
    });
  });

  it("keeps deadline priority, then attempts and ID, and advances peers after rescheduling", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("ticket-probe-deadline-fairness"), async (instance, state) => {
      configure(instance);
      insert(state, "z-earlier", { nextAt: new Date(at.getTime() - 1000).toISOString(), attempts: 1 });
      insert(state, "a-one-attempt", { attempts: 1 });
      insert(state, "b-zero", { firstSeenAt: "2026-10-15T02:00:00.000Z" });
      insert(state, "c-zero", { firstSeenAt: "2026-10-14T01:00:00.000Z" });
      insert(state, "a-future", { nextAt: new Date(at.getTime() + 60_000).toISOString() });
      const selected = [];
      const fetchImpl = async url => {
        const id = new URL(url).pathname.split("/").at(-1);
        selected.push(id);
        return id === "b-zero" && selected.filter(value => value === id).length === 1
          ? new Response("", { status: 404 }) : available();
      };
      for (let index = 0; index < 4; index++) {
        const result = await instance.processTicketAvailabilityProbes(at, { fetchImpl, controlCards: [] });
        expect(result.probed).toBe(1);
      }
      expect(selected).toEqual(["z-earlier", "b-zero", "c-zero", "a-one-attempt"]);
      const rescheduled = state.storage.sql.exec("SELECT next_at,attempts,gone_count FROM ticket_probe_state WHERE offer_id='b-zero'").one();
      expect(rescheduled).toMatchObject({ next_at: "2026-10-16T03:00:05.000Z", attempts: 1, gone_count: 1 });
      const final = await instance.processTicketAvailabilityProbes(new Date(at.getTime() + 5000), { fetchImpl, controlCards: [] });
      expect(final.probed).toBe(1);
      expect(selected).toEqual(["z-earlier", "b-zero", "c-zero", "a-one-attempt", "b-zero"]);
      expect(state.storage.sql.exec("SELECT next_at FROM ticket_probe_state WHERE offer_id='b-zero'").one().next_at).toBe("");
      expect(state.storage.sql.exec("SELECT attempts FROM ticket_probe_state WHERE offer_id='a-future'").one().attempts).toBe(0);
    });
  });
});
