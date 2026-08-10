import { env, exports } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

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

      expect(version).toBeGreaterThanOrEqual(20);
      expect(tables).toEqual(expect.arrayContaining([
        "metadata",
        "offers",
        "runs",
        "incidents",
        "pending_discussion_forwards",
        "discord_availability_sync",
        "ticket_probe_state",
        "delivery_events",
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
      ]) {
        expect(offersSchema).toContain(column);
      }
    });
  });

  it("consulta o ciclo Discord sem exceder o limite de colunas SQLite", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("discord-availability-empty");
    await runInDurableObject(stub, async (instance) => {
      expect(await instance.processDiscordAvailabilitySync(
        new Date("2026-08-03T20:00:00.000Z"),
      )).toEqual({
        soldOutEdited: 0,
        restockEdited: 0,
        messageMissing: 0,
        failed: 0,
      });
    });
  });

  it("agenda Discord depois do principal sem bloquear o fim do scan crítico", async () => {
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
      instance.fetchTicketListing = async () => [];
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
        return {
          mainSent: 1,
          canal2Sent: 1,
          discordSent: 0,
          failed: 0,
          selectedRows: [{ id: "oferta-relampago-1" }],
        };
      };
      instance.scheduleDiscordDelivery = (options) => {
        deliveryCalls.push({ ...options, targetNames: ["discord"] });
        return new Promise(() => {});
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
      waitForMainImage: true,
      targetNames: ["main", "canal2"],
    });
    expect(deliveryCalls[1]).toMatchObject({
      priorityIds: ["oferta-relampago-1"],
      rows: [{ id: "oferta-relampago-1" }],
      targetNames: ["discord"],
    });
  });

  it("persiste ingressos do HTML crítico mesmo quando a API os omite", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ticket-critical-fallback");
    const missingIds = [
      "pcb-2-ingressos-16-08-teatro-vanucci-rj",
      "pca-2-ingressos-15-08-teatro-vanucci-rj",
      "pc9-2-ingressos-14-08-teatro-vanucci-rj",
      "pc1-2-ingressos-13-08-cultura-artistica-sp",
    ];
    const ticketHtml = `<!doctype html><html><body>
      <div class="beneficio" data-categoria="campanhasdeingresso">
        <a href="/campanhasdeingresso/pCB-2-ingressos-16-08-teatro-vanucci-rj">
          <p class="titulo">2 INGRESSOS: 16/08 Teatro Vanucci RJ</p>
        </a>
      </div>
      <div class="beneficio" data-categoria="campanhasdeingresso">
        <a href="/campanhasdeingresso/pCA-2-ingressos-15-08-teatro-vanucci-rj">
          <p class="titulo">2 INGRESSOS: 15/08 Teatro Vanucci RJ</p>
        </a>
      </div>
      <div class="beneficio" data-categoria="campanhasdeingresso">
        <a href="/campanhasdeingresso/pC9-2-ingressos-14-08-teatro-vanucci-rj">
          <p class="titulo">2 INGRESSOS: 14/08 Teatro Vanucci RJ</p>
        </a>
      </div>
      <div class="beneficio" data-categoria="campanhasdeingresso">
        <a href="/campanhasdeingresso/pC1-2-ingressos-13-08-cultura-artistica-sp">
          <p class="titulo">2 INGRESSOS: 13/08 Cultura Artística SP</p>
        </a>
      </div>
    </body></html>`;
    const deliveryCalls = [];
    let mainDeliveryCount = 0;

    await runInDurableObject(stub, async (instance, state) => {
      instance.setMetadata("initialized_at", "2026-08-10T12:00:00.000Z");
      instance.fetchAllApi = async () => [];
      const fetchTicketListing = instance.fetchTicketListing.bind(instance);
      instance.fetchTicketListing = () => fetchTicketListing(async (url) => {
        expect(String(url)).toContain("categoria=ingressosexclusivos");
        return new Response(ticketHtml, {
          headers: { "content-type": "text/html; charset=UTF-8" },
        });
      });
      instance.processDeliveryQueue = async (_now, options) => {
        deliveryCalls.push(options);
        if (options.targetNames.includes("discord")) {
          const rows = options.rows || [];
          for (const row of rows) {
            state.storage.sql.exec(
              "UPDATE offers SET discord_sent_at = ? WHERE id = ?",
              "2026-08-10T21:00:01.000Z",
              row.id,
            );
          }
          return {
            mainSent: 0,
            canal2Sent: 0,
            discordSent: rows.length,
            failed: 0,
            selectedRows: rows,
          };
        }
        const rows = state.storage.sql.exec(
          `SELECT id FROM offers
           WHERE id IN (${missingIds.map(() => "?").join(", ")})
             AND main_sent_at = ''`,
          ...missingIds,
        ).toArray();
        for (const row of rows) {
          state.storage.sql.exec(
            `UPDATE offers SET main_sent_at = ?, canal2_sent_at = ?, status = 'delivered'
             WHERE id = ?`,
            "2026-08-10T21:00:00.000Z",
            "2026-08-10T21:00:00.000Z",
            row.id,
          );
        }
        mainDeliveryCount += rows.length;
        return {
          mainSent: rows.length,
          canal2Sent: rows.length,
          discordSent: 0,
          failed: 0,
          selectedRows: rows,
        };
      };
      instance.scheduleDiscordDelivery = (options) => {
        if (!options.rows.length) return Promise.resolve({ discordSent: 0, failed: 0 });
        return instance.processDeliveryQueue(new Date(), {
          ...options,
          targetNames: ["discord"],
        });
      };
      instance.processTicketAvailabilityProbes = async () => ({
        probed: 0,
        confirmed: 0,
        fallback: 0,
        soldOutMainEdited: 0,
        soldOutCanal2Edited: 0,
        soldOutDiscordEdited: 0,
        failed: 0,
      });

      const first = await instance.scan("test");
      expect(first).toMatchObject({
        ok: true,
        outcome: "telegram_delivered",
        apiOffersSeen: 0,
        ticketListingOffersSeen: 4,
        newOffers: 4,
        mainSent: 4,
        discordSent: 0,
      });
      const second = await instance.scan("test-repeat");
      expect(second).toMatchObject({
        ok: true,
        outcome: "no_change",
        newOffers: 0,
        mainSent: 0,
        discordSent: 0,
      });

      const persisted = state.storage.sql.exec(
        `SELECT COUNT(*) AS tracked,
                SUM(CASE WHEN main_sent_at <> '' THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN discord_sent_at <> '' THEN 1 ELSE 0 END) AS discord_sent
         FROM offers WHERE id IN (${missingIds.map(() => "?").join(", ")})`,
        ...missingIds,
      ).one();
      expect(Number(persisted.tracked)).toBe(4);
      expect(Number(persisted.sent)).toBe(4);
      expect(Number(persisted.discord_sent)).toBe(4);
    });

    expect(mainDeliveryCount).toBe(4);
    expect(deliveryCalls.map((call) => call.targetNames)).toEqual([
      ["main", "canal2"],
      ["discord"],
      ["main", "canal2"],
    ]);
    expect(deliveryCalls[1].rows.map((row) => row.id).sort()).toEqual([...missingIds].sort());
  });

  it("pula reconciliação completa quando a assinatura da API não muda", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("api-unchanged-fingerprint");
    let resolveCalls = 0;
    let sourceCalls = 0;
    let pendingCalls = 0;
    let probeCalls = 0;
    const deliveryCalls = [];

    await runInDurableObject(stub, async (instance) => {
      instance.setMetadata("initialized_at", "2026-08-03T12:00:00.000Z");
      instance.fetchAllApi = async () => [{
        id: "oferta-estavel-1",
        link: "https://clube.uol.com.br/beneficios/oferta-estavel-1",
        previewTitle: "Oferta estável",
        title: "Oferta estável",
        description: "Descrição estável da oferta.",
      }];
      instance.fetchTicketListing = async () => [];
      instance.resolveListingCards = () => {
        resolveCalls += 1;
        return {
          cards: [],
          inserted: 0,
          insertedIds: [],
        };
      };
      instance.recordSourceCards = () => {
        sourceCalls += 1;
      };
      instance.processPending = async () => {
        pendingCalls += 1;
        return { enriched: 0, wouldSendMain: 0, wouldSendCanal2: 0 };
      };
      instance.processDeliveryQueue = async (_now, options) => {
        deliveryCalls.push(options);
        return {
          mainSent: 0,
          canal2Sent: 0,
          discordSent: 0,
          failed: 0,
          selectedRows: [],
        };
      };
      instance.processTicketAvailabilityProbes = async () => {
        probeCalls += 1;
        return {
          probed: 0,
          confirmed: 0,
          fallback: 0,
          soldOutMainEdited: 0,
          soldOutCanal2Edited: 0,
          soldOutDiscordEdited: 0,
          failed: 0,
        };
      };

      const first = await instance.scan("test");
      const second = await instance.scan("test");

      expect(first).toMatchObject({ ok: true, outcome: "no_change" });
      expect(second).toMatchObject({ ok: true, outcome: "no_change" });
    });

    expect(resolveCalls).toBe(1);
    expect(sourceCalls).toBe(1);
    expect(pendingCalls).toBe(1);
    expect(probeCalls).toBe(2);
    expect(deliveryCalls.length).toBeGreaterThanOrEqual(2);
  });

  it("mantém a fotografia da API fresca para saúde sem reler observações SQL", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("api-health-snapshot");

    await runInDurableObject(stub, async (instance) => {
      instance.runtimeSnapshot = (name) => name === "api"
        ? {
            lastOffersSeen: 1,
            lastError: "",
            lastSuccessAt: "2026-08-06T02:00:00.000Z",
            healthCards: [{
              id: "ingresso-fresco",
              link: "https://clube.uol.com.br/campanhasdeingresso/p-fresco",
              previewTitle: "Ingresso fresco",
              category: "campanhasdeingresso",
            }],
          }
        : {};
      instance.sqlExec = () => {
        throw new Error("source_observations_should_not_be_read");
      };

      expect(instance.recentApiCardsForHealth(new Date("2026-08-06T02:00:30.000Z"))).toEqual([
        {
          id: "ingresso-fresco",
          link: "https://clube.uol.com.br/campanhasdeingresso/p-fresco",
          previewTitle: "Ingresso fresco",
          category: "campanhasdeingresso",
          cardImageUrl: "",
          partnerImageUrl: "",
          partnerName: "",
        },
      ]);
    });
  });

  it("mantém secundários da oferta nova elegíveis sob reserva de quota", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("priority-delivery-quota");

    await runInDurableObject(stub, async (instance) => {
      instance.setMetadata("delivery_mode_override", "live");
      instance.storageUsageSnapshot = () => ({
        maintenanceAllowed: false,
        primaryAllowed: true,
      });
      instance.deliveryQueueSlo = () => ({
        pending: 0,
        criticalPending: 0,
        secondaryPending: 0,
        oldestAgeMs: 0,
        p95AgeMs: 0,
      });

      const oldSecondary = await instance.processDeliveryQueue(
        new Date("2026-08-05T12:00:00.000Z"),
        { targetNames: ["discord"] },
      );
      expect(oldSecondary).toMatchObject({
        deferred: true,
        deferredReason: "quota_reserve",
      });

      const freshSecondary = await instance.processDeliveryQueue(
        new Date("2026-08-05T12:00:00.000Z"),
        {
          priorityIds: ["oferta-nova-1"],
          targetNames: ["discord"],
        },
      );
      expect(freshSecondary.deferred).not.toBe(true);
    });
  });

  it("encaminha ao Canal 2 quando o principal sai da espera de imagem sob reserva", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("canal2-after-main-image-wait");
    const newOfferId = "ticket-new-image-wait";
    const oldOfferId = "ticket-old-secondary";
    const telegramCalls = [];

    vi.stubGlobal("fetch", async (input, init = {}) => {
      const method = new URL(String(input)).pathname.split("/").at(-1);
      if (!String(input).startsWith("https://api.telegram.org/") ||
          !["sendMessage", "copyMessage"].includes(method)) {
        throw new Error(`unexpected_fetch:${method}`);
      }
      const payload = JSON.parse(String(init.body || "{}"));
      telegramCalls.push({ method, chatId: String(payload.chat_id || "") });
      return Response.json({
        ok: true,
        result: { message_id: method === "sendMessage" ? 801 : 802 },
      });
    });

    try {
      await runInDurableObject(stub, async (instance, state) => {
        instance.env = {
          ...instance.env,
          DELIVERY_MODE: "live",
          TELEGRAM_TOKEN: "vitest-telegram-token-not-a-secret",
          TELEGRAM_CHAT_ID: "-100111",
          CANAL2_ID: "-100333",
          CANAL2_DELIVERY_ENABLED: "true",
          DISCORD_DELIVERY_ENABLED: "false",
          MAIN_IMAGE_WAIT_SECONDS: "60",
        };
        instance.setMetadata("delivery_mode_override", "live");
        instance.storageUsageSnapshot = () => ({
          maintenanceAllowed: false,
          primaryAllowed: true,
        });
        const now = new Date();
        const nowIso = now.toISOString();
        const oldIso = new Date(now.getTime() - 60 * 60_000).toISOString();
        state.storage.sql.exec(
          `INSERT INTO offers(
             id, link, preview_title, title, category, first_seen_at, last_seen_at,
             status, decision_at, would_send_main, would_send_canal2,
             delivery_mode, delivery_generation
           ) VALUES (?, ?, ?, ?, 'campanhasdeingresso', ?, ?,
                     'delivery_pending', ?, 1, 1, 'live', 1)`,
          newOfferId,
          "https://clube.uol.com.br/campanhasdeingresso/pNew-image-wait",
          "2 INGRESSOS: nova oferta",
          "2 INGRESSOS: nova oferta",
          nowIso,
          nowIso,
          nowIso,
        );
        state.storage.sql.exec(
          `INSERT INTO offers(
             id, link, preview_title, title, category, first_seen_at, last_seen_at,
             status, decision_at, would_send_main, would_send_canal2,
             delivery_mode, delivery_generation, main_sent_at, main_message_id
           ) VALUES (?, ?, ?, ?, 'campanhasdeingresso', ?, ?,
                     'partial_delivery', ?, 1, 1, 'live', 1, ?, 700)`,
          oldOfferId,
          "https://clube.uol.com.br/campanhasdeingresso/pOld-secondary",
          "2 INGRESSOS: oferta antiga",
          "2 INGRESSOS: oferta antiga",
          oldIso,
          oldIso,
          oldIso,
          oldIso,
        );

        const deferred = await instance.processDeliveryQueue(new Date(), {
          priorityIds: [newOfferId],
          waitForMainImage: true,
          targetNames: ["main", "canal2"],
        });
        expect(deferred).toMatchObject({ mainSent: 0, canal2Sent: 0, failed: 0 });
        expect(telegramCalls).toEqual([]);

        state.storage.sql.exec(
          `UPDATE offers SET first_seen_at = ?, main_delivery_next_attempt_at = ''
           WHERE id = ?`,
          new Date(Date.now() - 61_000).toISOString(),
          newOfferId,
        );
        const delivered = await instance.processDeliveryQueue(new Date(), {
          waitForMainImage: true,
          targetNames: ["main", "canal2"],
        });
        expect(delivered).toMatchObject({ mainSent: 1, canal2Sent: 1, failed: 0 });
        expect(telegramCalls).toEqual([
          { method: "sendMessage", chatId: "-100111" },
          { method: "copyMessage", chatId: "-100333" },
        ]);

        const rows = state.storage.sql.exec(
          `SELECT id, main_sent_at, canal2_sent_at, canal2_delivery_attempts
           FROM offers WHERE id IN (?, ?) ORDER BY id`,
          newOfferId,
          oldOfferId,
        ).toArray();
        expect(rows.find((row) => row.id === newOfferId)).toMatchObject({
          main_sent_at: expect.any(String),
          canal2_sent_at: expect.any(String),
          canal2_delivery_attempts: 1,
        });
        expect(rows.find((row) => row.id === oldOfferId)).toMatchObject({
          main_sent_at: oldIso,
          canal2_sent_at: "",
          canal2_delivery_attempts: 0,
        });

        const repeat = await instance.processDeliveryQueue(new Date(), {
          waitForMainImage: true,
          targetNames: ["main", "canal2"],
        });
        expect(repeat).toMatchObject({ mainSent: 0, canal2Sent: 0, failed: 0 });
        expect(telegramCalls).toHaveLength(2);
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("isola estágio e custo de ciclos concorrentes com AsyncLocalStorage", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("storage-context-isolation");

    await runInDurableObject(stub, async (instance) => {
      const [primaryRows, maintenanceRows] = await Promise.all([
        instance.withStorageCycle("primary", async () => {
          await instance.withStorageStage("delivery", async () => {
            instance.recordStorageUsage(3, 0);
            await Promise.resolve();
            instance.recordStorageUsage(2, 0);
          });
          return instance.completeStorageUsageCycle("primary", 0);
        }),
        instance.withStorageCycle("maintenance", async () => {
          await instance.withStorageStage("html", async () => {
            instance.recordStorageUsage(7, 0);
            await Promise.resolve();
            instance.recordStorageUsage(1, 0);
          });
          return instance.completeStorageUsageCycle("maintenance", 0);
        }),
      ]);

      expect(primaryRows).toBe(7);
      expect(maintenanceRows).toBe(10);
      expect(instance.storageUsage.stageReads.delivery).toBe(5);
      expect(instance.storageUsage.stageReads.html).toBe(8);
      expect(instance.storageUsage.primaryMaxRowsRead).toBe(7);
      expect(instance.storageUsage.maintenanceMaxRowsRead).toBe(10);
    });
  });

  it("devolve backoff quando a manutenção encontra a reserva de quota", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("maintenance-quota-backoff");

    await runInDurableObject(stub, async (instance) => {
      instance.storageUsageSnapshot = () => ({
        rowsRead: 4_900_000,
        limit: 5_000_000,
        criticalReserve: 1_000_000,
        resetAt: "2026-08-06T00:00:00.000Z",
        maintenanceAllowed: false,
      });
      instance.completeStorageUsageCycle = () => 0;
      const result = await instance.runMaintenanceTick("test");
      expect(result).toMatchObject({
        ok: false,
        outcome: "storage_read_budget_guard",
        retryReason: "storage_read_budget_guard",
      });
      expect(Date.parse(result.retryAt)).toBeGreaterThan(0);
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

  it("encerra unknown histórico de comentário sem reencaminhar a oferta", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("historical-comment-unknown");

    await runInDurableObject(stub, async (instance, state) => {
      instance.setMetadata("delivery_mode_override", "live");
      state.storage.sql.exec(
        `INSERT INTO offers(
           id, link, preview_title, first_seen_at, last_seen_at, status,
           discussion_message_id, comment_delivery_attempts,
           comment_delivery_unknown_at, delivery_generation
         ) VALUES (?, ?, ?, ?, ?, 'sold_out', ?, 1, ?, 1)`,
        "historical-comment-unknown-1",
        "https://clube.uol.com.br/beneficios/historical-comment-unknown-1",
        "Oferta histórica",
        "2026-08-04T18:00:00.000Z",
        "2026-08-04T18:00:00.000Z",
        6172,
        "2026-08-04T18:37:30.293Z",
      );

      const resolved = instance.resolveDeliveryUnknown(
        "historical-comment-unknown-1",
        "comment",
        "closed",
      );
      expect(resolved).toMatchObject({
        ok: true,
        target: "comment",
        outcome: "closed",
      });
      const row = state.storage.sql.exec(
        `SELECT status, comment_delivery_attempts, comment_delivery_error,
                comment_delivery_unknown_at, comment_delivery_in_flight_at,
                comment_delivery_next_attempt_at
         FROM offers WHERE id = ?`,
        "historical-comment-unknown-1",
      ).one();
      expect(row).toMatchObject({
        status: "sold_out",
        comment_delivery_attempts: 10,
        comment_delivery_error: "historical_unknown_closed",
        comment_delivery_unknown_at: "",
        comment_delivery_in_flight_at: "",
        comment_delivery_next_attempt_at: "",
      });
      expect(await instance.processDiscussionComments(2)).toEqual({ sent: 0, failed: 0 });
      expect(instance.deliveryQueueIssues().some(
        (issue) => issue.id === "historical-comment-unknown-1",
      )).toBe(false);
      expect(Number(state.storage.sql.exec(
        `SELECT COUNT(*) AS count FROM delivery_events
         WHERE offer_id = ? AND operation = 'comment' AND state = 'closed'`,
        "historical-comment-unknown-1",
      ).one().count)).toBe(1);
    });
  });

  it("reproduz duas sondas rápidas de ingresso pelo método de produção", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ticket-probe-replay");
    await runInDurableObject(stub, async (instance, state) => {
      instance.setMetadata("delivery_mode_override", "live");
      state.storage.sql.exec(
        `INSERT INTO offers(
           id, link, preview_title, first_seen_at, last_seen_at, status,
           main_sent_at, main_message_id
         ) VALUES (?, ?, ?, ?, ?, 'delivered', ?, 101)`,
        "ticket-replay-1",
        "https://clube.uol.com.br/campanhasdeingresso/pReplay-ticket",
        "2 INGRESSOS: Replay",
        "2026-08-04T15:00:00.000Z",
        "2026-08-04T15:00:00.000Z",
        "2026-08-04T15:00:00.000Z",
      );
      state.storage.sql.exec(
        `INSERT INTO ticket_probe_state(offer_id, next_at)
         VALUES (?, ?)`,
        "ticket-replay-1",
        "2026-08-04T15:00:00.000Z",
      );
      instance.processSoldOutSync = async () => ({
        mainEdited: 1,
        canal2Edited: 0,
        messageMissing: 0,
        failed: 0,
      });
      instance.processDiscordAvailabilitySync = async () => ({
        soldOutEdited: 1,
        restockEdited: 0,
        messageMissing: 0,
        failed: 0,
      });
      const fetchImpl = async () => new Response("", {
        status: 404,
        headers: { "content-type": "text/html" },
      });
      const first = await instance.processTicketAvailabilityProbes(
        new Date("2026-08-04T15:00:00.000Z"),
        { fetchImpl },
      );
      expect(first).toMatchObject({ probed: 1, confirmed: 0 });
      const second = await instance.processTicketAvailabilityProbes(
        new Date("2026-08-04T15:00:05.000Z"),
        { fetchImpl },
      );
      expect(second).toMatchObject({ probed: 1, confirmed: 1, soldOutMainEdited: 1 });
      expect(state.storage.sql.exec(
        "SELECT status, sold_out_at FROM offers WHERE id = ?",
        "ticket-replay-1",
      ).one().status).toBe("sold_out");
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
        alarmFresh: true,
        scanFresh: false,
        deliveryConfigured: false,
      },
      queueSlo: {
        pending: 0,
        criticalPending: 0,
        secondaryPending: 0,
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
      counts: { tracked: 0 },
    });

    const globalStub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(globalStub, async (_instance, state) => {
      expect(await state.storage.getAlarm()).not.toBeNull();
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

  it("cacheia o diagnóstico de readiness por 15s sem deixar de rearmar o polling", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("readiness-cache-rearm");

    await runInDurableObject(stub, async (instance, state) => {
      await state.storage.setAlarm(Date.now() + 60_000);
      await instance.getReadiness();
      const rowsReadAfterFirst = instance.storageUsage.rowsRead;
      instance.deliveryQueueSlo = () => {
        throw new Error("readiness_cache_miss");
      };

      await expect(instance.getReadiness()).resolves.toMatchObject({
        worker: "uol-telegram-shadow-pilot",
      });
      expect(instance.storageUsage.rowsRead).toBe(rowsReadAfterFirst);

      await state.storage.deleteAlarm();
      await expect(instance.getReadiness()).resolves.toMatchObject({
        checks: { alarmFresh: true },
      });
      expect(await state.storage.getAlarm()).not.toBeNull();
    });
  });

  it("não considera tentativa recente como descoberta fresca quando ambas as fontes falham", async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("readiness-source-success");

    await runInDurableObject(stub, async (instance, state) => {
      const now = new Date();
      const staleSuccess = new Date(now.getTime() - 10 * 60_000).toISOString();
      const recentAttempt = now.toISOString();
      instance.setRuntimeSnapshot("api", {
        lastCompletedAt: recentAttempt,
        lastSuccessAt: staleSuccess,
        lastError: "uol_api_http_503",
      });
      instance.setRuntimeSnapshot("ticket_listing", {
        lastCompletedAt: recentAttempt,
        lastSuccessAt: staleSuccess,
        lastError: "uol_http_503",
      });
      instance.setRuntimeSnapshot("maintenance", {
        lastCompletedAt: recentAttempt,
        lastError: "",
      });
      await state.storage.setAlarm(now.getTime() + 60_000);

      await expect(instance.getReadiness()).resolves.toMatchObject({
        checks: { scanFresh: false },
        lastScanAt: staleSuccess,
      });
    });
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
