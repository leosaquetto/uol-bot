import { env } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const START = Date.parse("2026-10-15T10:00:00.000Z");
const MINUTE = 60_000;

function at(minutes) {
  vi.setSystemTime(START + minutes * MINUTE);
  return new Date();
}

function isolateHealth(instance) {
  instance.env = { ...instance.env, UOL_API_AUTHORIZATION: "" };
  instance.failedRunStreak = () => 0;
  instance.deliveryQueueSlo = () => ({ criticalPending: 0, secondaryPending: 0, oldestAgeMs: 0 });
  instance.recentTicketDeliveryIssues = () => [];
  instance.deliveryQueueIssues = () => [];
}

function failApi(instance, severity = "warning") {
  instance.setRuntimeSnapshot("api", {
    lastError: severity === "critical" ? "uol_api_http_401" : "uol_api_http_503",
    failureStreak: severity === "critical" ? 1 : 3,
  });
}

function recoverApi(instance, now = new Date()) {
  instance.setRuntimeSnapshot("api", { lastError: "", failureStreak: 0, lastSuccessAt: now.toISOString() });
}

describe("estabilização durável dos alertas técnicos", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(START);
    vi.stubGlobal("fetch", vi.fn(async () => new Response(null, { status: 202 })));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("agrupa dez oscilações em uma abertura e preserva orçamento para crítico imediato", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-ten-flaps-preserve-critical-budget");
    await runInDurableObject(stub, async (instance, state) => {
      isolateHealth(instance);
      let episode;
      for (let index = 0; index < 10; index += 1) {
        failApi(instance);
        expect(await instance.processOperationalHealth(at(index * 2))).toEqual({
          active: 1, alerted: index === 0 ? 1 : 0, recovered: 0,
        });
        episode ||= instance.metadataValue("ops_ntfy_episode:ticket-api");
        expect(instance.metadataValue("ops_ntfy_episode:ticket-api")).toBe(episode);
        const cleanAt = at(index * 2 + 1);
        recoverApi(instance, cleanAt);
        expect(await instance.processOperationalHealth(cleanAt)).toEqual({ active: 0, alerted: 0, recovered: 0 });
      }
      expect(state.storage.sql.exec("SELECT status FROM incidents WHERE key = 'ticket-api'").one().status).toBe("active");
      expect(instance.loadOperationsOutbox().attempts).toHaveLength(1);
      instance.failedRunStreak = () => 3;
      recoverApi(instance, at(20));
      expect(await instance.processOperationalHealth()).toEqual({ active: 1, alerted: 1, recovered: 0 });
      const outbox = instance.loadOperationsOutbox();
      expect(outbox.attempts).toHaveLength(2);
      expect(outbox.events.find(event => event.kind === "failed-scans").status).toBe("accepted");
      expect(outbox.attempts.at(-1).privileged).toBe(true);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("só recupera após cinco minutos e duas observações afirmativas distintas", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-affirmative-stable-recovery");
    await runInDurableObject(stub, async (instance, state) => {
      isolateHealth(instance);
      failApi(instance, "critical");
      expect(await instance.processOperationalHealth(at(0))).toEqual({ active: 1, alerted: 1, recovered: 0 });
      const healthyAt = at(1);
      recoverApi(instance, healthyAt);
      expect((await instance.processOperationalHealth(healthyAt)).recovered).toBe(0);
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(0); // Same sample.
      recoverApi(instance);
      expect(await instance.processOperationalHealth()).toEqual({ active: 0, alerted: 0, recovered: 1 });
      expect(state.storage.sql.exec("SELECT status FROM incidents WHERE key = 'ticket-api'").one().status).toBe("resolved");
      expect(instance.loadOperationsOutbox().events.map(event => event.recovered)).toEqual([false, true]);
      expect((await instance.processOperationalHealth(at(7))).recovered).toBe(0);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("retoma janela e identidade pela persistência após evicção real", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-stable-recovery-real-eviction");
    let episode;
    await runInDurableObject(stub, async instance => {
      isolateHealth(instance);
      failApi(instance);
      await instance.processOperationalHealth(at(0));
      episode = instance.metadataValue("ops_ntfy_episode:ticket-api");
      const healthyAt = at(1);
      recoverApi(instance, healthyAt);
      await instance.processOperationalHealth(healthyAt);
    });
    await evictDurableObject(stub);
    await runInDurableObject(stub, async instance => {
      isolateHealth(instance);
      expect(instance.metadataValue("ops_ntfy_episode:ticket-api")).toBe(episode);
      expect(JSON.parse(instance.metadataValue("ops_ntfy_recovery:ticket-api")).since).toBe(START + MINUTE);
      const healthyAt = at(6);
      recoverApi(instance, healthyAt);
      expect((await instance.processOperationalHealth(healthyAt)).recovered).toBe(1);
      expect(instance.loadOperationsOutbox().attempts).toHaveLength(2);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("reinicia estabilização após lacuna ou amostra ausente e rejeita sucesso antigo", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-recovery-gap-missing-stale");
    await runInDurableObject(stub, async instance => {
      isolateHealth(instance);
      failApi(instance);
      await instance.processOperationalHealth(at(0));
      recoverApi(instance, at(-1));
      expect((await instance.processOperationalHealth(at(1))).recovered).toBe(0);
      expect(instance.metadataValue("ops_ntfy_recovery:ticket-api")).toBeFalsy();
      recoverApi(instance, at(2));
      await instance.processOperationalHealth();
      recoverApi(instance, at(13));
      expect((await instance.processOperationalHealth()).recovered).toBe(0); // Gap > 10 min.
      expect(JSON.parse(instance.metadataValue("ops_ntfy_recovery:ticket-api")).since).toBe(START + 13 * MINUTE);
      instance.setRuntimeSnapshot("api", {});
      expect((await instance.processOperationalHealth(at(14))).recovered).toBe(0);
      expect(instance.metadataValue("ops_ntfy_recovery:ticket-api")).toBeFalsy();
      recoverApi(instance, at(15));
      await instance.processOperationalHealth();
      recoverApi(instance, at(20));
      expect((await instance.processOperationalHealth()).recovered).toBe(1);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("escala warning a crítico uma vez sem repetir abertura incerta", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValueOnce(new TypeError("intercepted"))
      .mockResolvedValue(new Response(null, { status: 202 })));
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-critical-escalation-once-uncertain");
    await runInDurableObject(stub, async instance => {
      isolateHealth(instance);
      failApi(instance);
      await instance.processOperationalHealth(at(0));
      const opening = instance.loadOperationsOutbox().events[0];
      expect(opening.status).toBe("uncertain");
      failApi(instance, "critical");
      expect(await instance.processOperationalHealth(at(1))).toEqual({ active: 1, alerted: 1, recovered: 0 });
      failApi(instance);
      await instance.processOperationalHealth(at(2));
      failApi(instance, "critical");
      expect((await instance.processOperationalHealth(at(3))).alerted).toBe(0);
      const events = instance.loadOperationsOutbox().events;
      expect(events).toHaveLength(2);
      expect(events[0]).toEqual(opening);
      expect(events[1].id).toMatch(/:critical$/);
      expect(events[1].severity).toBe("critical");
      expect(events[1].status).toBe("accepted");
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("não recupera entrega incerta só porque saiu do LIMIT da lista de sinais", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-delivery-missing-is-not-recovered");
    await runInDurableObject(stub, async (instance, state) => {
      isolateHealth(instance);
      instance.deliveryQueueIssues = () => [{ id: "fixture-offer", target: "main", state: "unknown" }];
      await instance.processOperationalHealth(at(0));
      instance.deliveryQueueIssues = () => [];
      expect((await instance.processOperationalHealth(at(1))).recovered).toBe(0);
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(0);
      expect(state.storage.sql.exec("SELECT status FROM incidents WHERE key = 'delivery-queue:fixture-offer:main'").one().status).toBe("active");
      expect(instance.loadOperationsOutbox().events).toHaveLength(1);
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("não alerta outra vez um episódio que já abriu crítico ao oscilar severidade", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-initial-critical-not-repeated");
    await runInDurableObject(stub, async instance => {
      isolateHealth(instance);
      failApi(instance, "critical");
      await instance.processOperationalHealth(at(0));
      failApi(instance);
      await instance.processOperationalHealth(at(1));
      failApi(instance, "critical");
      expect((await instance.processOperationalHealth(at(2))).alerted).toBe(0);
      expect(instance.loadOperationsOutbox().events).toHaveLength(1);
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("recupera alvo genérico real só com recibos de todos os destinos exigidos e sem incerteza", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-real-generic-delivery-recovery");
    await runInDurableObject(stub, async (instance, state) => {
      isolateHealth(instance);
      delete instance.deliveryQueueIssues;
      instance.env = {
        ...instance.env, CANAL2_DELIVERY_ENABLED: "true", DISCORD_DELIVERY_ENABLED: "true",
      };
      const past = new Date(START - 2 * MINUTE).toISOString();
      state.storage.sql.exec(
        `INSERT INTO offers(id, link, preview_title, category, first_seen_at, last_seen_at,
           decision_at, status, would_send_main, would_send_canal2)
         VALUES ('generic-delivery', 'https://clube.uol.com.br/campanhasdeingresso/fixture',
           '', 'campanhasdeingresso', ?, ?, ?, 'delivery_pending', 1, 1)`, past, past, past,
      );
      expect(instance.deliveryQueueIssues(at(0))).toMatchObject([
        { id: "generic-delivery", target: "delivery", state: "main_delivery_slow" },
      ]);
      expect((await instance.processOperationalHealth()).alerted).toBe(1);
      const key = "delivery-queue:generic-delivery:delivery";
      const status = () => state.storage.sql.exec("SELECT status FROM incidents WHERE key = ?", key).one().status;
      state.storage.sql.exec("UPDATE offers SET main_sent_at = ?, status = 'partial_delivery' WHERE id = 'generic-delivery'", new Date().toISOString());
      expect(instance.deliveryQueueIssues()).toEqual([]);
      expect((await instance.processOperationalHealth(at(1))).recovered).toBe(0);
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(0);
      state.storage.sql.exec("UPDATE offers SET canal2_sent_at = ? WHERE id = 'generic-delivery'", new Date().toISOString());
      expect((await instance.processOperationalHealth(at(7))).recovered).toBe(0); // Discord still required.
      state.storage.sql.exec(
        `UPDATE offers SET discord_sent_at = ?, canal2_delivery_unknown_at = ?
         WHERE id = 'generic-delivery'`, new Date().toISOString(), new Date().toISOString(),
      );
      expect((await instance.processOperationalHealth(at(8))).recovered).toBe(0);
      expect((await instance.processOperationalHealth(at(13))).recovered).toBe(0);
      expect(status()).toBe("active");
      expect(instance.metadataValue(`ops_ntfy_recovery:${key}`)).toBeFalsy();
      state.storage.sql.exec("UPDATE offers SET canal2_delivery_unknown_at = '', status = 'delivered' WHERE id = 'generic-delivery'");
      expect((await instance.processOperationalHealth(at(14))).recovered).toBe(0);
      expect((await instance.processOperationalHealth(at(19))).recovered).toBe(1);
      expect(status()).toBe("resolved");
      expect(instance.loadOperationsOutbox().events.map(event => event.recovered)).toEqual([false, true]);
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});
