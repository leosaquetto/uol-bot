import { env, exports } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

const ADMIN_AUTHORIZATION = "Bearer vitest-admin-token-not-a-secret";

describe("UOL Worker no runtime Cloudflare", () => {
  it("inicializa o schema SQLite completo em uma instância nova", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("schema-current");

    await runInDurableObject(stub, (_instance, state) => {
      const version = Number(
        state.storage.sql
          .exec("SELECT COALESCE(MAX(id), 0) AS version FROM _sql_schema_migrations")
          .one().version,
      );
      const tables = state.storage.sql
        .exec("SELECT name FROM sqlite_schema WHERE type = 'table'")
        .toArray()
        .map((row) => row.name);
      const offersSchema = String(
        state.storage.sql
          .exec("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'offers'")
          .one().sql,
      );

      expect(version).toBeGreaterThanOrEqual(18);
      expect(tables).toEqual(expect.arrayContaining([
        "metadata",
        "offers",
        "runs",
        "incidents",
        "pending_discussion_forwards",
      ]));
      for (const column of [
        "delivery_generation",
        "delivery_dead_letter_at",
        "delivery_unknown_at",
        "main_delivery_unknown_at",
        "canal2_delivery_unknown_at",
        "discord_delivery_unknown_at",
        "delivery_quarantine_reason",
        "restocked_at",
        "main_sold_out_next_attempt_at",
        "canal2_restock_next_attempt_at",
        "main_image_upgrade_attempts",
        "main_image_upgrade_next_attempt_at",
        "main_image_upgrade_error",
        "discord_image_proxy_url",
        "discord_image_cache_message_id",
        "discord_image_cache_attempts",
        "discord_image_cache_next_attempt_at",
        "discord_image_cache_error",
        "discord_image_cache_sent_at",
        "discord_sold_out_synced_at",
        "discord_sold_out_attempts",
        "discord_sold_out_error",
        "discord_sold_out_next_attempt_at",
        "discord_restock_synced_at",
        "discord_restock_attempts",
        "discord_restock_error",
        "discord_restock_next_attempt_at",
      ]) {
        expect(offersSchema).toContain(column);
      }
    });
  });

  it("envia Discord, principal e canal 2 no ciclo rápido sem aguardar manutenção", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("api-fast-path");
    const deliveryCalls = [];

    await runInDurableObject(stub, async (instance) => {
      instance.setMetadata("initialized_at", "2026-08-03T12:00:00.000Z");
      instance.fetchAllApi = async () => [{
        id: "oferta-relampago-1",
        link: "https://clube.uol.com.br/beneficios/oferta-relampago-1",
        previewTitle: "Oferta relâmpago",
        apiDetail: { title: "Oferta relâmpago" },
      }];
      instance.recordSourceCards = () => {};
      instance.resolveListingCards = () => ({
        cards: [],
        inserted: 1,
        insertedIds: ["oferta-relampago-1"],
      });
      instance.processPending = async () => ({
        enriched: 1,
        wouldSendMain: 1,
        wouldSendCanal2: 1,
      });
      instance.processDeliveryQueue = async (_now, options) => {
        deliveryCalls.push(options);
        return options.targetNames.includes("discord")
          ? { mainSent: 0, canal2Sent: 0, discordSent: 1, failed: 0 }
          : { mainSent: 1, canal2Sent: 1, discordSent: 0, failed: 0 };
      };
      instance.runMaintenanceTick = async () => {
        throw new Error("maintenance_must_not_run_in_api_scan");
      };

      const result = await instance.scan("test");
      expect(result).toMatchObject({
        ok: true,
        outcome: "telegram_delivered",
        apiOffersSeen: 1,
        newOffers: 1,
        mainSent: 1,
      });
    });

    expect(deliveryCalls).toHaveLength(2);
    expect(deliveryCalls[0]).toMatchObject({
      priorityIds: ["oferta-relampago-1"],
      targetNames: ["discord"],
    });
    expect(deliveryCalls[1]).toMatchObject({
      priorityIds: ["oferta-relampago-1"],
      waitForMainImage: true,
      targetNames: ["main", "canal2"],
    });
  });

  it("expõe dead letter da manutenção no monitor operacional", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("maintenance-dead-letter");

    await runInDurableObject(stub, async (instance, state) => {
      state.storage.sql.exec(
        `INSERT INTO offers(
           id, link, preview_title, first_seen_at, last_seen_at, status,
           main_sold_out_attempts, main_sold_out_error
         ) VALUES (?, ?, ?, ?, ?, 'sold_out', 10, ?)`,
        "oferta-esgotada-1",
        "https://clube.uol.com.br/beneficios/oferta-esgotada-1",
        "Oferta esgotada",
        "2026-08-03T12:00:00.000Z",
        "2026-08-03T12:00:00.000Z",
        "telegram_edit_failed",
      );

      expect(instance.deliveryQueueIssues()).toEqual(expect.arrayContaining([
        expect.objectContaining({
          id: "oferta-esgotada-1",
          target: "main_sold_out",
          state: "dead_letter",
          error: "telegram_edit_failed",
        }),
      ]));
    });
  });

  it("reabre envio principal incerto depois da janela de reconciliação", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("main-unknown-auto-retry");

    await runInDurableObject(stub, async (instance, state) => {
      state.storage.sql.exec(
        `INSERT INTO offers(
           id, link, preview_title, first_seen_at, last_seen_at, status,
           decision_at, would_send_main, delivery_mode, delivery_generation,
           main_delivery_attempts, main_delivery_unknown_at,
           delivery_unknown_at, delivery_unknown_target
         ) VALUES (?, ?, ?, ?, ?, 'delivery_unknown', ?, 1, 'live', 1, 1, ?, ?, 'main')`,
        "oferta-incerta-1",
        "https://clube.uol.com.br/beneficios/oferta-incerta-1",
        "Oferta incerta",
        "2026-08-03T12:00:00.000Z",
        "2026-08-03T12:00:00.000Z",
        "2026-08-03T12:00:00.000Z",
        "2026-08-03T12:00:01.000Z",
        "2026-08-03T12:00:01.000Z",
      );

      expect(instance.releaseExpiredMainUnknowns(
        new Date("2026-08-03T12:01:00.000Z"),
      )).toBe(1);
      const row = state.storage.sql.exec(
        `SELECT status, main_delivery_unknown_at, delivery_unknown_target
         FROM offers WHERE id = ?`,
        "oferta-incerta-1",
      ).one();
      expect(row).toMatchObject({
        // O ambiente de integração não possui token. O envio é liberado da
        // janela ambígua e imediatamente classificado como configuração ausente.
        status: "delivery_blocked_configuration",
        main_delivery_unknown_at: "",
        delivery_unknown_target: "",
      });
    });
  });

  it("separa liveness, readiness e diagnóstico detalhado", async () => {
    const livenessResponse = await exports.default.fetch("https://worker.test/livez");
    expect(livenessResponse.status).toBe(200);
    expect(livenessResponse.headers.get("Cache-Control")).toBe("public, max-age=30");
    const liveness = await livenessResponse.json();
    expect(liveness).toMatchObject({
      ok: true,
      worker: "uol-telegram-shadow-pilot",
    });
    expect(typeof liveness.versionId).toBe("string");

    const readinessResponse = await exports.default.fetch("https://worker.test/readyz");
    expect(readinessResponse.status).toBe(503);
    expect(readinessResponse.headers.get("Cache-Control")).toBe("no-store");
    const readiness = await readinessResponse.json();
    expect(readiness).toMatchObject({
      ok: false,
      worker: "uol-telegram-shadow-pilot",
      mode: "shadow",
      checks: {
        alarmFresh: false,
        scanFresh: false,
        deliveryConfigured: false,
      },
    });

    const healthResponse = await exports.default.fetch("https://worker.test/health");
    expect(healthResponse.status).toBe(503);
    expect(await healthResponse.json()).toMatchObject({
      ok: false,
      worker: readiness.worker,
      mode: readiness.mode,
      checks: readiness.checks,
    });

    const dashboardUnauthorized = await exports.default.fetch(
      "https://worker.test/dashboard.json",
    );
    expect(dashboardUnauthorized.status).toBe(401);

    const dashboardResponse = await exports.default.fetch(
      "https://worker.test/dashboard.json",
      { headers: { Authorization: ADMIN_AUTHORIZATION } },
    );
    expect(dashboardResponse.status).toBe(200);
    expect(await dashboardResponse.json()).toMatchObject({
      ok: true,
      worker: "uol-telegram-shadow-pilot",
      mode: "shadow",
      alarmScheduledAt: "",
      counts: { tracked: 0 },
    });

    const globalStub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(globalStub, async (_instance, state) => {
      expect(await state.storage.getAlarm()).toBeNull();
    });

    const unauthorized = await exports.default.fetch("https://worker.test/inventory");
    expect(unauthorized.status).toBe(401);

    const authorized = await exports.default.fetch("https://worker.test/inventory", {
      headers: { Authorization: ADMIN_AUTHORIZATION },
    });
    expect(authorized.status).toBe(200);
    expect(await authorized.json()).toMatchObject({ ok: true, inventory: [] });

    const missing = await exports.default.fetch("https://worker.test/unknown");
    expect(missing.status).toBe(404);
  });

  it("expõe ofertas públicas com limite normalizado e ETag", async () => {
    const first = await exports.default.fetch(
      "https://worker.test/offers?limit=999&ignored=1",
    );
    expect(first.status).toBe(200);
    expect(first.headers.get("Cache-Control")).toContain("max-age=30");
    const etag = first.headers.get("ETag");
    expect(etag).toMatch(/^"[a-f0-9]+"$/);
    expect(await first.json()).toMatchObject({ ok: true, offers: [] });

    const cached = await exports.default.fetch(
      "https://worker.test/offers?limit=12&ignored=2",
      { headers: { "If-None-Match": etag } },
    );
    expect(cached.status).toBe(304);
    expect(cached.headers.get("ETag")).toBe(etag);
  });

  it("rejeita webhook sem secret e aceita update inofensivo validado", async () => {
    const body = JSON.stringify({ update_id: 1 });
    const unauthorized = await exports.default.fetch("https://worker.test/telegram-webhook", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await exports.default.fetch("https://worker.test/telegram-webhook", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Telegram-Bot-Api-Secret-Token": "vitest-webhook-token-not-a-secret",
      },
      body,
    });
    expect(authorized.status).toBe(200);
    expect(await authorized.json()).toEqual({ ok: true, matched: false });
  });

  it("sempre rearma o alarme quando o scan falha", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("alarm-recovery");
    const failure = new Error("falha simulada sem rede");

    await runInDurableObject(stub, async (instance, state) => {
      instance.ensureTelegramWebhook = async () => true;
      instance.scan = async () => {
        throw failure;
      };
      await state.storage.setAlarm(Date.now() + 60_000);
    });

    const before = Date.now();
    expect(await runDurableObjectAlarm(stub)).toBe(true);

    await runInDurableObject(stub, async (_instance, state) => {
      const nextAlarm = await state.storage.getAlarm();
      expect(nextAlarm).not.toBeNull();
      expect(nextAlarm).toBeGreaterThanOrEqual(before + 9_000);
    });
  });

  it("rearma polling antes de tocar o coordenador de manutenção", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("alarm-priority-order");
    const order = [];

    await runInDurableObject(stub, async (instance, state) => {
      instance.scan = async () => {
        order.push("scan");
        return { ok: true };
      };
      instance.ensureMaintenanceAlarm = async () => {
        const alarm = await state.storage.getAlarm();
        expect(alarm).not.toBeNull();
        order.push("maintenance_after_rearm");
        return new Date(alarm).toISOString();
      };
      await state.storage.setAlarm(Date.now() + 1_000);
    });
    expect(await runDurableObjectAlarm(stub)).toBe(true);
    expect(order).toEqual(["scan", "maintenance_after_rearm"]);
  });

  it("agenda o Durable Object independente de manutenção", async () => {
    const stub = env.UOL_TELEGRAM_MAINTENANCE.getByName("maintenance-scheduler");
    const scheduledAt = await stub.ensureAlarm();
    expect(Date.parse(scheduledAt)).toBeGreaterThan(Date.now());
    await expect(stub.getStatus()).resolves.toMatchObject({
      ok: true,
      intervalSeconds: 10,
    });
  });

  it("reconcilia automaticamente um envio principal ambíguo pelo forward do Telegram", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("unknown-main-reconciliation");
    await runInDurableObject(stub, async (instance, state) => {
      state.storage.sql.exec(
        `INSERT INTO offers(
           id, link, preview_title, first_seen_at, last_seen_at, status,
           decision_at, would_send_main, delivery_mode, delivery_generation,
           delivery_unknown_at, delivery_unknown_target, main_delivery_unknown_at
         ) VALUES (?, ?, ?, ?, ?, 'delivery_unknown', ?, 1, 'live', 1, ?, 'main', ?)`,
        "parceiro-p123-oferta-relampago",
        "https://clube.uol.com.br/beneficios/parceiro/p123-oferta-relampago",
        "Oferta relâmpago",
        "2026-08-02T12:00:00.000Z",
        "2026-08-02T12:00:00.000Z",
        "2026-08-02T12:00:00.000Z",
        "2026-08-02T12:00:01.000Z",
        "2026-08-02T12:00:01.000Z",
      );

      const result = await instance.handleTelegramUpdate({
        message: {
          is_automatic_forward: true,
          message_id: 901,
          chat: { id: -100222 },
          text: "Oferta https://clube.uol.com.br/beneficios/parceiro/p123-oferta-relampago",
          entities: [{ type: "url", offset: 7, length: 78 }],
          forward_origin: {
            type: "channel",
            chat: { id: -100111 },
            message_id: 701,
            date: 1785672000,
          },
        },
      });
      expect(result.reconciledOfferId).toBe("parceiro-p123-oferta-relampago");
      const row = state.storage.sql.exec(
        "SELECT main_message_id, main_sent_at, main_delivery_unknown_at FROM offers WHERE id = ?",
        "parceiro-p123-oferta-relampago",
      ).one();
      expect(Number(row.main_message_id)).toBe(701);
      expect(row.main_sent_at).not.toBe("");
      expect(row.main_delivery_unknown_at).toBe("");
    });
  });
});
