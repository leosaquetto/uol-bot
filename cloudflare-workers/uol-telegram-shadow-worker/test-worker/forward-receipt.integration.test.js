import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test-forward-receipt",
    TELEGRAM_CHAT_ID: "-100111", GRUPO_COMENTARIO_ID: "-100222",
    CANAL2_DELIVERY_ENABLED: "false", DISCORD_DELIVERY_ENABLED: "false",
    BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.setMetadata("delivery_mode_generation", 2);
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(instance, state, id, fields = {}) {
  const at = new Date().toISOString();
  const row = {
    id, link: `https://clube.uol.com.br/beneficios/${id}`,
    title: "Teste", preview_title: "Teste", first_seen_at: at, last_seen_at: at,
    decision_at: at, status: "delivery_pending", would_send_main: 1,
    delivery_mode: "live", delivery_generation: instance.currentDeliveryGeneration(),
    ...fields,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

function forward(id, date, messageId = 901) {
  return {
    message: {
      is_automatic_forward: true, chat: { id: -100222 }, message_id: 1901,
      text: `Oferta https://clube.uol.com.br/beneficios/${id}`,
      forward_origin: { type: "channel", chat: { id: -100111 }, message_id: messageId, date },
    },
  };
}

function recordAttempt(instance, id, at, fields = {}) {
  instance.recordDeliveryLedgerEvent({
    offerId: id, target: "main", operation: "delivery", state: "attempt_started",
    attempt: 1, generation: instance.currentDeliveryGeneration(), occurredAt: at,
    ...fields,
  });
}

describe("automatic forwards as main delivery receipts", () => {
  it("accepts an early forward once and preserves it after a late transport failure", async () => {
    let entered, release;
    const started = new Promise(resolve => { entered = resolve; });
    const held = new Promise(resolve => { release = resolve; });
    const send = vi.fn(async () => {
      entered();
      await held;
      throw new TypeError("network interrupted");
    });
    vi.stubGlobal("fetch", send);
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("forward-early-receipt"), async (instance, state) => {
        configure(instance);
        insert(instance, state, "early");
        const pending = instance.processDeliveryQueue(new Date(), {
          targetNames: ["main"], waitForMainImage: false,
        });
        await started;
        const claim = state.storage.sql.exec("SELECT * FROM offers WHERE id = 'early'").one();
        const receipt = forward("early", Math.floor(Date.parse(claim.main_delivery_in_flight_at) / 1_000));
        try {
          expect(await instance.handleTelegramUpdate(receipt)).toMatchObject({
            matched: true, reconciledOfferId: "early",
          });
          await instance.handleTelegramUpdate(receipt);
        } finally {
          release();
          await pending;
        }
        const row = state.storage.sql.exec("SELECT * FROM offers WHERE id = 'early'").one();
        expect(row).toMatchObject({
          main_message_id: 901, discussion_message_id: 1901, main_delivery_attempts: 1,
          main_delivery_in_flight_at: "", main_delivery_unknown_at: "",
          main_delivery_error: "", delivery_unknown_target: "", delivery_unknown_at: "",
          status: "delivered",
        });
        expect(row.main_sent_at).toBe(new Date(receipt.message.forward_origin.date * 1_000).toISOString());
        const receipts = state.storage.sql.exec("SELECT * FROM delivery_events WHERE state = 'resolved_sent'").toArray();
        expect(receipts).toHaveLength(1);
        expect(receipts[0]).toMatchObject({ target: "main", attempt: 1, generation: 2, external_id: "901" });
        expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE state = 'late_failure'").one().count).toBe(1);
        expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM pending_discussion_forwards").one().count).toBe(0);
        await instance.processDeliveryQueue(new Date(), { targetNames: ["main"], waitForMainImage: false });
      });
      expect(send).toHaveBeenCalledTimes(1);
    } finally {
      release?.();
      vi.unstubAllGlobals();
    }
  });

  it("resolves a delayed unknown receipt using its exact ledger attempt and preserves other destinations", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("forward-unknown-ledger"), async (instance, state) => {
      configure(instance);
      const startedAt = new Date(Date.now() - 60_000).toISOString();
      const unknownAt = new Date(Date.parse(startedAt) + 8_000).toISOString();
      insert(instance, state, "unknown", {
        status: "delivery_unknown", main_delivery_attempts: 2,
        main_delivery_unknown_at: unknownAt, main_delivery_error: "ambiguous:timeout",
        discord_delivery_unknown_at: unknownAt,
        delivery_unknown_target: "main,discord", delivery_unknown_at: unknownAt,
      });
      recordAttempt(instance, "unknown", startedAt, { attempt: 2 });
      const receipt = forward("unknown", Math.floor(Date.parse(startedAt) / 1_000) + 2);
      expect(await instance.handleTelegramUpdate(receipt)).toMatchObject({ reconciledOfferId: "unknown" });
      expect(state.storage.sql.exec("SELECT * FROM offers WHERE id = 'unknown'").one()).toMatchObject({
        main_message_id: 901, main_delivery_unknown_at: "", main_delivery_error: "",
        delivery_unknown_target: "discord", delivery_unknown_at: unknownAt,
        discord_delivery_unknown_at: unknownAt, status: "delivery_unknown",
      });
      expect(state.storage.sql.exec("SELECT attempt, generation FROM delivery_events WHERE state = 'resolved_sent'").one())
        .toEqual({ attempt: 2, generation: 2 });
    });
  });

  it.each(["canonical", "alias"])("finds %s identity beyond the latest 32 unknown offers", async identity => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`forward-deep-${identity}`), async (instance, state) => {
      configure(instance);
      const startedAt = new Date(Date.now() - 1_000).toISOString();
      const id = identity === "canonical" ? "wanted" : "stored-identity";
      insert(instance, state, id, {
        link: "https://clube.uol.com.br/beneficios/wanted",
        first_seen_at: "2026-01-01T00:00:00.000Z", main_delivery_attempts: 1,
        main_delivery_unknown_at: startedAt,
      });
      if (identity === "alias") instance.recordIdentityAliases(id, "wanted", "https://clube.uol.com.br/beneficios/wanted", startedAt);
      recordAttempt(instance, id, startedAt);
      for (let index = 0; index < 40; index++) {
        insert(instance, state, `newer-${index}`, { main_delivery_unknown_at: startedAt, main_delivery_attempts: 1 });
      }
      const receipt = forward("wanted", Math.floor(Date.parse(startedAt) / 1_000));
      expect(await instance.handleTelegramUpdate(receipt)).toMatchObject({ reconciledOfferId: id });
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM offers WHERE main_message_id > 0").one().count).toBe(1);
    });
  });

  it.each([
    "missing_date", "invalid_date", "old_date", "late_date", "old_generation",
    "ledger_generation", "ledger_attempt", "missing_ledger", "inactive",
  ])("rejects %s without losing the pending discussion mapping", async reason => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`forward-reject-${reason}`), async (instance, state) => {
      configure(instance);
      const startedAt = new Date(Date.now() - 5_000).toISOString();
      const seconds = Math.floor(Date.parse(startedAt) / 1_000);
      insert(instance, state, "reject", {
        main_delivery_attempts: 1,
        main_delivery_unknown_at: reason === "inactive" ? "" : startedAt,
        delivery_generation: reason === "old_generation" ? 1 : 2,
      });
      if (reason !== "missing_ledger") recordAttempt(instance, "reject", startedAt, {
        generation: reason === "ledger_generation" ? 1 : 2,
        attempt: reason === "ledger_attempt" ? 2 : 1,
      });
      const date = reason === "missing_date" ? undefined : reason === "invalid_date" ? "invalid" :
        reason === "old_date" ? seconds - 1 : reason === "late_date" ? seconds + 39 : seconds;
      expect(await instance.handleTelegramUpdate(forward("reject", date))).toMatchObject({
        matched: false, queued: true, reconciledOfferId: "",
      });
      expect(state.storage.sql.exec("SELECT main_message_id, main_sent_at FROM offers").one())
        .toEqual({ main_message_id: 0, main_sent_at: "" });
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE state = 'resolved_sent'").one().count).toBe(0);
      expect(state.storage.sql.exec("SELECT origin_message_id, discussion_message_id FROM pending_discussion_forwards").one())
        .toEqual({ origin_message_id: 901, discussion_message_id: 1901 });
    });
  });

  it("does not update a claim changed after selecting the matching receipt", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("forward-claim-changed"), async (instance, state) => {
      configure(instance);
      const startedAt = new Date().toISOString();
      insert(instance, state, "changed", { main_delivery_attempts: 1, main_delivery_in_flight_at: startedAt });
      const originalExec = instance.sqlExec.bind(instance);
      instance.sqlExec = (query, ...args) => {
        if (query.startsWith("UPDATE offers SET main_message_id = ?")) {
          state.storage.sql.exec("UPDATE offers SET main_delivery_attempts = 2 WHERE id = 'changed'");
        }
        return originalExec(query, ...args);
      };
      try {
        const receipt = forward("changed", Math.floor(Date.parse(startedAt) / 1_000));
        expect(await instance.handleTelegramUpdate(receipt)).toMatchObject({ reconciledOfferId: "" });
      } finally {
        instance.sqlExec = originalExec;
      }
      expect(state.storage.sql.exec("SELECT main_message_id, main_delivery_attempts FROM offers").one())
        .toEqual({ main_message_id: 0, main_delivery_attempts: 2 });
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM delivery_events WHERE state = 'resolved_sent'").one().count).toBe(0);
    });
  });
});
