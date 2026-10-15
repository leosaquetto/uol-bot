import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { reserveNextOperationsEvent } from "../src/operations-outbox.js";
import { chooseDeliveryBudget } from "../src/queue-policy.js";

const TOPIC = "https://ntfy.sh/macXntfy-8130";

function isolateHealth(instance) {
  instance.env = { ...instance.env, UOL_API_AUTHORIZATION: "" };
  instance.runtimeSnapshot = () => ({});
  instance.failedRunStreak = () => 0;
  instance.deliveryQueueSlo = () => ({ criticalPending: 0, secondaryPending: 0, oldestAgeMs: 0 });
  instance.recentTicketDeliveryIssues = () => [];
  instance.deliveryQueueIssues = () => [];
}

function failApi(instance) {
  instance.setMetadata("api_last_error", "uol_api_http_401 private-detail secret-token");
  instance.setMetadata("api_failure_streak", 1);
}

function recoverApi(instance) {
  instance.setMetadata("api_last_error", "");
  instance.setMetadata("api_failure_streak", 0);
}

describe("avisos operacionais persistentes no runtime Cloudflare", () => {
  it("envia novo incidente só no ntfy e silencia a repetição ativa", async () => {
    const requests = [];
    vi.stubGlobal("fetch", async (url, init) => {
      requests.push({ url: String(url), init });
      return new Response(null, { status: 202 });
    });
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-new-incident-private-ntfy");
      await runInDurableObject(stub, async (instance, state) => {
        isolateHealth(instance);
        failApi(instance);
        const first = await instance.processOperationalHealth();
        expect(first).toEqual({ active: 1, alerted: 1, recovered: 0 });
        expect(await instance.processOperationalHealth()).toEqual({ active: 1, alerted: 0, recovered: 0 });
        const incident = state.storage.sql.exec("SELECT status FROM incidents WHERE key = 'ticket-api'").one();
        expect(incident.status).toBe("active");
        expect(instance.loadOperationsOutbox().events.map(event => event.status)).toEqual(["accepted"]);
      });
      expect(requests).toHaveLength(1);
      expect(requests[0].url).toBe(TOPIC);
      expect(requests[0].init.method).toBe("POST");
      expect(requests[0].init.headers["Content-Type"]).toBe("text/plain; charset=utf-8");
      expect(requests[0].init.body).toContain("API de ofertas");
      expect(JSON.stringify(requests)).not.toMatch(/private-detail|secret-token|api\.telegram|discord\.com/);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("não reenvia incidente histórico ativo nem sua recuperação sem episódio ntfy", async () => {
    const fetchSpy = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-historical-baseline");
      await runInDurableObject(stub, async (instance, state) => {
        isolateHealth(instance);
        failApi(instance);
        const at = new Date().toISOString();
        state.storage.sql.exec(
          `INSERT INTO incidents(key, status, severity, summary, details, first_detected_at, last_detected_at)
           VALUES ('ticket-api', 'active', 'critical', 'historical-private-summary', '', ?, ?)`, at, at,
        );
        expect(await instance.processOperationalHealth()).toEqual({ active: 1, alerted: 0, recovered: 0 });
        recoverApi(instance);
        expect(await instance.processOperationalHealth()).toEqual({ active: 0, alerted: 0, recovered: 1 });
        expect(state.storage.sql.exec("SELECT status FROM incidents WHERE key = 'ticket-api'").one().status).toBe("resolved");
        expect(instance.loadOperationsOutbox().events).toHaveLength(0);
      });
      expect(fetchSpy).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("mantém recuperação rejeitada na outbox depois de resolver o incidente e respeita backoff", async () => {
    const requests = [];
    vi.stubGlobal("fetch", async (url, init) => {
      requests.push({ url: String(url), body: init.body });
      return new Response(null, { status: requests.length === 2 ? 503 : 200 });
    });
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-recovery-durable-retry");
      await runInDurableObject(stub, async (instance, state) => {
        isolateHealth(instance);
        failApi(instance);
        await instance.processOperationalHealth();
        recoverApi(instance);
        await instance.processOperationalHealth();
        expect(state.storage.sql.exec("SELECT status FROM incidents WHERE key = 'ticket-api'").one().status).toBe("resolved");
        const pending = instance.loadOperationsOutbox().events.find(event => event.recovered);
        expect(pending.status).toBe("pending");
        expect(pending.attempts).toBe(1);
        expect(Date.parse(pending.nextAt) - Date.parse(pending.lastAttemptAt)).toBeGreaterThanOrEqual(60_000);
        await instance.drainOperationalAlerts(new Date(Date.parse(pending.nextAt) - 1));
        expect(requests).toHaveLength(2);
        await instance.drainOperationalAlerts(new Date(Date.parse(pending.nextAt) + 1));
        const accepted = instance.loadOperationsOutbox().events.find(event => event.recovered);
        expect(accepted.status).toBe("accepted");
        expect(accepted.attempts).toBe(2);
      });
      expect(requests.map(request => request.url)).toEqual([TOPIC, TOPIC, TOPIC]);
      expect(requests[1].body).toContain("incidente recuperado");
      expect(requests[2].body).toBe(requests[1].body);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("recupera reserva de uma instância anterior como incerta sem repetir POST", async () => {
    const fetchSpy = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-crash-reservation");
      await runInDurableObject(stub, async instance => {
        const now = new Date();
        instance.enqueueOperationalAlert({ key: "failed-scans", severity: "critical" }, false, now);
        const reserved = reserveNextOperationsEvent(instance.loadOperationsOutbox(now), now);
        expect(reserved.event.status).toBe("reserved");
        instance.setMetadata("ops_ntfy_outbox", JSON.stringify(reserved.state));
        instance.operationsOutboxLoaded = false;
        await instance.drainOperationalAlerts(now);
        const event = instance.loadOperationsOutbox(now).events[0];
        expect(event.status).toBe("uncertain");
        expect(event.attempts).toBe(1);
        expect(event.lastOutcome).toBe("uncertain");
      });
      expect(fetchSpy).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("drena trabalho persistido mesmo quando um novo incidente encontra a outbox cheia", async () => {
    const requests = [];
    vi.stubGlobal("fetch", async (url, init) => {
      requests.push({ url: String(url), method: init.method });
      return new Response(null, { status: 200 });
    });
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-capacity-still-drains");
      await runInDurableObject(stub, async (instance, state) => {
        isolateHealth(instance);
        failApi(instance);
        const now = new Date();
        const at = now.toISOString();
        instance.setMetadata("ops_ntfy_outbox", JSON.stringify({
          version: 1,
          attempts: [],
          events: Array.from({ length: 128 }, (_, index) => ({
            id: `persisted-${index}`,
            kind: "monitor",
            severity: "warning",
            recovered: false,
            createdAt: at,
            status: "pending",
            attempts: 0,
            nextAt: at,
          })),
        }));
        await expect(instance.processOperationalHealth(now)).rejects.toThrow("operations_outbox_capacity");
        const events = instance.loadOperationsOutbox().events;
        expect(events.filter(event => event.status === "accepted")).toHaveLength(1);
        expect(events.filter(event => event.status === "pending")).toHaveLength(127);
        expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM incidents WHERE key = 'ticket-api'").one().count).toBe(0);
        expect(instance.metadataValue("ops_ntfy_episode:ticket-api")).toBeFalsy();
      });
      expect(requests).toEqual([{ url: TOPIC, method: "POST" }]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("carrega os campos SQL que separam principal acionável de falhas terminais antigas", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("queue-actionable-main-sql");
    await runInDurableObject(stub, (instance, state) => {
      instance.env = { ...instance.env, TELEGRAM_TOKEN: "vitest-telegram-token", DELIVERY_MAX_ATTEMPTS: "3" };
      const now = new Date();
      const past = new Date(now.getTime() - 120_000).toISOString();
      const future = new Date(now.getTime() + 60_000).toISOString();
      const generation = instance.currentDeliveryGeneration();
      const insert = (id, status, attempts, nextAt = "", unknownAt = "", rowGeneration = generation) => {
        state.storage.sql.exec(
          `INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, decision_at,
             status, would_send_main, delivery_generation, main_delivery_attempts,
             main_delivery_next_attempt_at, main_delivery_unknown_at)
           VALUES (?, ?, '', ?, ?, ?, ?, 1, ?, ?, ?, ?)`,
          id, `https://clube.uol.com.br/beneficios/${id}`, past, past, past,
          status, rowGeneration, attempts, nextAt, unknownAt,
        );
      };
      insert("terminal-old", "delivery_dead_letter", 3);
      insert("unknown-old", "delivery_unknown", 1, "", past);
      insert("retry-future", "delivery_pending", 1, future);
      insert("old-generation", "delivery_pending", 0, "", "", generation + 1);
      let slo = instance.deliveryQueueSlo(now);
      expect(slo.criticalPending).toBe(4);
      expect(slo.actionableMainPending).toBe(0);
      expect(chooseDeliveryBudget({ queueSlo: slo }).deferSecondary).toBe(false);
      insert("ready-now", "delivery_pending", 1);
      slo = instance.deliveryQueueSlo(now);
      expect(slo.actionableMainPending).toBe(1);
      expect(slo.oldestActionableMainAgeMs).toBe(120_000);
      expect(chooseDeliveryBudget({ queueSlo: slo }).reason).toBe("critical_backlog");
    });
  });

  it("evita starvation do Canal 2 atrás de cem principais antigos unknown", async () => {
    const requests = [];
    vi.stubGlobal("fetch", async (url, init) => {
      expect(String(url)).toBe("https://api.telegram.org/botvitest-telegram-token/copyMessage");
      expect(init.method).toBe("POST");
      requests.push(JSON.parse(init.body));
      return Response.json({ ok: true, result: { message_id: 902 } });
    });
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("canal2-no-terminal-starvation");
      await runInDurableObject(stub, async (instance, state) => {
        instance.env = {
          ...instance.env,
          DELIVERY_MODE: "live",
          TELEGRAM_TOKEN: "vitest-telegram-token",
          TELEGRAM_CHAT_ID: "-100111",
          CANAL2_ID: "-100333",
          CANAL2_DELIVERY_ENABLED: "true",
          DISCORD_DELIVERY_ENABLED: "false",
          BEEPER_DELIVERY_ENABLED: "false",
        };
        instance.setMetadata("delivery_mode_override", "live");
        instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
        const now = new Date();
        const past = new Date(now.getTime() - 86_400_000).toISOString();
        const recent = new Date(now.getTime() - 5_000).toISOString();
        const generation = instance.currentDeliveryGeneration();
        for (let index = 0; index < 100; index++) {
          const id = `unknown-old-${index}`;
          state.storage.sql.exec(
            `INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, decision_at,
               status, would_send_main, would_send_canal2, delivery_mode, delivery_generation,
               main_delivery_attempts, main_delivery_unknown_at, delivery_unknown_at, delivery_unknown_target)
             VALUES (?, ?, '', ?, ?, ?, 'delivery_unknown', 1, 1, 'live', ?, 1, ?, ?, 'main')`,
            id, `https://clube.uol.com.br/beneficios/${id}`, past, past, past, generation, past, past,
          );
        }
        state.storage.sql.exec(
          `INSERT INTO offers(id, link, preview_title, first_seen_at, last_seen_at, decision_at,
             status, would_send_main, would_send_canal2, delivery_mode, delivery_generation,
             main_sent_at, main_message_id)
           VALUES ('canal2-new', 'https://clube.uol.com.br/beneficios/canal2-new', '', ?, ?, ?,
             'partial_delivery', 1, 1, 'live', ?, ?, 901)`,
          recent, recent, recent, generation, recent,
        );
        const result = await instance.processDeliveryQueue(now, { targetNames: ["canal2"] });
        expect(result).toMatchObject({ mainSent: 0, canal2Sent: 1, discordSent: 0, failed: 0 });
        expect(state.storage.sql.exec(
          "SELECT canal2_message_id, canal2_delivery_attempts FROM offers WHERE id = 'canal2-new'",
        ).one()).toMatchObject({ canal2_message_id: 902, canal2_delivery_attempts: 1 });
        expect(state.storage.sql.exec(
          `SELECT COUNT(*) AS count FROM offers WHERE id LIKE 'unknown-old-%'
             AND main_delivery_unknown_at = ? AND delivery_unknown_at = ?
             AND delivery_unknown_target = 'main' AND main_delivery_attempts = 1 AND main_sent_at = ''`,
          past, past,
        ).one().count).toBe(100);
      });
      expect(requests).toEqual([{
        chat_id: "-100333", from_chat_id: "-100111", message_id: 901, disable_notification: false,
      }]);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
