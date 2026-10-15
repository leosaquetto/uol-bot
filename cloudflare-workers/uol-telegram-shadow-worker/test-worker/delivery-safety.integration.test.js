import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
function configure(instance) {
  instance.env = { ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test-safety", TELEGRAM_CHAT_ID: "-100111", CANAL2_DELIVERY_ENABLED: "false", DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false" };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}
function insert(instance, state, id, fields = {}) {
  const at = new Date().toISOString();
  const row = { id, link: `https://clube.uol.com.br/beneficios/${id}`, title: "Teste", preview_title: "Teste", first_seen_at: at, last_seen_at: at, decision_at: at, status: "delivery_pending", would_send_main: 1, delivery_mode: "live", delivery_generation: instance.currentDeliveryGeneration(), ...fields };
  state.storage.sql.exec(`INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`, ...Object.values(row));
}
describe("durable delivery ownership and retention", () => {
  it("overlapping invocations with the same snapshot make only one POST", async () => {
    let calls = 0, release;
    const held = new Promise(resolve => { release = resolve; });
    vi.stubGlobal("fetch", async () => { calls++; await held; return Response.json({ ok: true, result: { message_id: 901 } }); });
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("delivery-claim-race"), async (instance, state) => {
        configure(instance); insert(instance, state, "race");
        const rows = state.storage.sql.exec("SELECT * FROM offers").toArray();
        const options = { rows, targetNames: ["main"], waitForMainImage: false };
        const first = instance.processDeliveryQueue(new Date(), options);
        await Promise.resolve();
        const second = instance.processDeliveryQueue(new Date(), options);
        await Promise.resolve(); release();
        const results = await Promise.all([first, second]);
        expect(results.reduce((count, result) => count + result.mainSent, 0)).toBe(1);
        expect(state.storage.sql.exec("SELECT main_delivery_attempts, main_message_id FROM offers").one()).toMatchObject({ main_delivery_attempts: 1, main_message_id: 901 });
      });
      expect(calls).toBe(1);
    } finally { vi.unstubAllGlobals(); }
  });
  it.each(["shadow", "generation", "discarded", "attempt_limit", "not_required"])(
    "rechecks %s after a selected row waits behind another delivery", async change => {
      let release, entered, calls = 0;
      const started = new Promise(resolve => { entered = resolve; });
      const held = new Promise(resolve => { release = resolve; });
      vi.stubGlobal("fetch", async () => {
        calls++; entered(); await held;
        return Response.json({ ok: true, result: { message_id: 903 } });
      });
      try {
        await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`delivery-eligibility-${change}`), async (instance, state) => {
          configure(instance);
          instance.env.DELIVERY_CONCURRENCY = "1";
          insert(instance, state, "first"); insert(instance, state, "second");
          const rows = state.storage.sql.exec("SELECT * FROM offers ORDER BY id").toArray();
          const pending = instance.processDeliveryQueue(new Date(), { rows, targetNames: ["main"], waitForMainImage: false });
          await started;
          if (change === "shadow") instance.setDeliveryMode("shadow");
          if (change === "generation") instance.setMetadata("delivery_mode_generation", instance.currentDeliveryGeneration() + 1);
          if (change === "discarded") state.storage.sql.exec("UPDATE offers SET status = 'discarded' WHERE id = 'second'");
          if (change === "attempt_limit") state.storage.sql.exec("UPDATE offers SET main_delivery_attempts = 10 WHERE id = 'second'");
          if (change === "not_required") state.storage.sql.exec("UPDATE offers SET would_send_main = 0 WHERE id = 'second'");
          release(); await pending;
          const row = state.storage.sql.exec("SELECT * FROM offers WHERE id = 'second'").one();
          expect(row.main_message_id).toBe(0);
          expect(row.main_delivery_in_flight_at).toBe("");
          expect(row.main_delivery_attempts).toBe(change === "attempt_limit" ? 10 : 0);
          if (change === "discarded") expect(row.status).toBe("discarded");
          if (change === "shadow") expect(row.status).toBe("delivery_quarantined");
          expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE offer_id = 'second' AND state = 'attempt_started'").one().count).toBe(0);
        });
        expect(calls).toBe(1);
      } finally { vi.unstubAllGlobals(); }
    },
  );
  it.each(["success", "failure"])("late %s cannot overwrite another claim; its receipt survives", async outcome => {
    let release, entered;
    const started = new Promise(resolve => { entered = resolve; });
    const held = new Promise(resolve => { release = resolve; });
    vi.stubGlobal("fetch", async () => { entered(); await held; if (outcome === "failure") throw new TypeError("network interrupted"); return Response.json({ ok: true, result: { message_id: 902 } }); });
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`delivery-late-${outcome}`), async (instance, state) => {
        configure(instance); insert(instance, state, "late");
        const pending = instance.processDeliveryQueue(new Date(), { targetNames: ["main"], waitForMainImage: false });
        await started;
        expect(() => instance.requeueDelivery("late", "main")).toThrow("delivery_in_flight_requires_resolution");
        state.storage.sql.exec("UPDATE offers SET main_delivery_attempts = 2, main_delivery_in_flight_at = ?, main_delivery_error = 'new owner' WHERE id = 'late'", "2026-10-16T10:00:00.000Z");
        release(); await pending;
        expect(state.storage.sql.exec("SELECT main_message_id, main_delivery_attempts, main_delivery_in_flight_at, main_delivery_error FROM offers").one()).toMatchObject({ main_message_id: 0, main_delivery_attempts: 2, main_delivery_in_flight_at: "2026-10-16T10:00:00.000Z", main_delivery_error: "new owner" });
        const event = state.storage.sql.exec("SELECT state, external_id FROM delivery_events WHERE state LIKE 'late_%'").one();
        expect(event.state).toBe(outcome === "success" ? "late_sent" : "late_failure");
        expect(event.external_id).toBe(outcome === "success" ? "902" : "");
      });
    } finally { vi.unstubAllGlobals(); }
  });
  it.each(["age", "overflow"])("%s pruning preserves unfinished terminal rows and evidence", async reason => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`retention-safe-${reason}`), async (instance, state) => {
      configure(instance);
      const now = new Date("2026-10-15T12:00:00.000Z");
      const at = reason === "age" ? "2026-09-01T12:00:00.000Z" : "2026-10-14T12:00:00.000Z";
      const done = { status: "delivered", first_seen_at: at, main_sent_at: at, main_message_id: 1 };
      insert(instance, state, "uncertain", { ...done, discord_delivery_unknown_at: at });
      insert(instance, state, "active", { ...done, main_delivery_in_flight_at: at });
      insert(instance, state, "comment", { ...done, discussion_message_id: 2 });
      insert(instance, state, "waiting-forward", { ...done, link: "https://clube.uol.com.br/campanhasdeingresso/test" });
      insert(instance, state, "whatsapp", done);
      state.storage.sql.exec("INSERT INTO beeper_delivery_queue(offer_id, pending_message_id) VALUES ('whatsapp', 'receipt-pending')");
      insert(instance, state, "sold-out", { ...done, status: "sold_out", sold_out_at: at, main_sold_out_attempts: 10 });
      insert(instance, state, "discord-sync", { ...done, status: "sold_out", sold_out_at: at, main_sold_out_synced_at: at });
      state.storage.sql.exec("INSERT INTO discord_availability_sync(offer_id) VALUES ('discord-sync')");
      insert(instance, state, "restock", { ...done, restocked_at: at });
      insert(instance, state, "dead-letter", { ...done, delivery_dead_letter_at: at });
      instance.recordDeliveryLedgerEvent({ offerId: "uncertain", target: "discord", operation: "delivery", state: "unknown" });
      for (let index = 0; index < 301; index++) insert(instance, state, `complete-${index}`, done);
      instance.pruneOffers(now);
      const kept = state.storage.sql.exec("SELECT id FROM offers").toArray().map(row => row.id);
      for (const id of ["uncertain", "active", "comment", "waiting-forward", "whatsapp", "sold-out", "discord-sync", "restock", "dead-letter"]) expect(kept).toContain(id);
      expect(kept.filter(id => id.startsWith("complete-")).length).toBe(reason === "age" ? 0 : 300);
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE offer_id = 'uncertain'").one().count).toBe(1);
      expect(state.storage.sql.exec("SELECT pending_message_id FROM beeper_delivery_queue").one().pending_message_id).toBe("receipt-pending");
    });
  });
});
