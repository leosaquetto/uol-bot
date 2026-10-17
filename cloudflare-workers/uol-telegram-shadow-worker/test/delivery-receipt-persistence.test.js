import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { build } from "esbuild";
import { Log, LogLevel, Miniflare, Response as MiniflareResponse } from "miniflare";
import { cleanText, isTicketCampaign } from "../src/core.js";
import { classifyDeliveryRow, deliveryConfiguration, deliveryRetryAt,
  isAmbiguousDeliveryError } from "../src/delivery-state.js";
import { recordDeliveryEvent, trimDeliveryEvents } from "../src/delivery-ledger.js";
import { chooseDeliveryBudget, isRecentDeliveryDecision,
  RECENT_DELIVERY_PRIORITY_MS } from "../src/queue-policy.js";
import { deferredMainDeliveryState, mainImageDeliveryOffer } from "../src/image-deadline.js";
import { forwardToCanal2, sendMainOffer, telegramConfiguration,
  TELEGRAM_MUTATION_TIMEOUT_SECONDS } from "../src/telegram.js";
import { discordConfiguration, sendDiscordOffer } from "../src/discord.js";

const source = readFileSync(new URL("../src/worker.js", import.meta.url), "utf8");
function section(start, end) {
  const offset = source.indexOf(start);
  const limit = source.indexOf(end, offset);
  assert.ok(offset >= 0 && limit > offset, `missing source section: ${start}`);
  return source.slice(offset, limit);
}

// Execute the production queue, reconciliation and ledger methods against real
// SQLite. Only Durable Object plumbing and external HTTP are replaced.
const methods = [
  section("  async processDeliveryQueue(", "  ensureBeeperDeliveryActivation("),
  section("  refreshDeliveryStatus(", "  deliveryQueueSlo("),
  section("  recordDeliveryLedgerEvent(", "  runtimeSnapshot("),
].join("\n");
const helpers = [
  section("function envNumber(", "function ticketProbeNextAt("),
  section("function sanitizeError(", "function deliveryMode("),
  section("async function runBounded(", "function telegramOfferUrls("),
  section("function rowToOffer(", "export class UolTelegramShadow"),
  source.match(/const SQLITE_BOUND_PARAMETER_CHUNK = \d+;/)[0],
].join("\n");
const migrationSql = [...section("  migrate() {", "  metadataValue(")
  .matchAll(/this\.sqlExec\(`([\s\S]*?)`\);/g)].map((match) => match[1]);
const targets = ["main", "canal2", "discord"];
const receipt = (target) => target === "discord" ? "discord-receipt-701" : 701;

function fixture(t, target, { fault, failLedger = false, imageFailure = false } = {}) {
  const database = new DatabaseSync(":memory:");
  t.after(() => database.close());
  for (const sql of migrationSql) database.exec(sql);
  let postCount = 0;
  let accepted = false;
  const requests = [];
  const fetchImpl = async (url, init) => {
    assert.equal(init.method, "POST");
    requests.push({ url: String(url), body: JSON.parse(init.body) });
    postCount += 1;
    accepted = true;
    return Response.json(target === "discord"
      ? { id: receipt(target) }
      : { ok: true, result: { message_id: receipt(target) } });
  };
  const dependencies = {
    cleanText, isTicketCampaign, classifyDeliveryRow, deliveryConfiguration,
    deliveryRetryAt, isAmbiguousDeliveryError, chooseDeliveryBudget,
    isRecentDeliveryDecision, RECENT_DELIVERY_PRIORITY_MS,
    deferredMainDeliveryState, mainImageDeliveryOffer, telegramConfiguration,
    TELEGRAM_MUTATION_TIMEOUT_SECONDS, discordConfiguration,
    writeDeliveryEvent: recordDeliveryEvent, trimDeliveryEvents,
    logEvent: () => {},
    sendMainOffer: (env, offer) => sendMainOffer(env, offer, fetchImpl),
    forwardToCanal2: (env, messageId) => forwardToCanal2(env, messageId, fetchImpl),
    sendDiscordOffer: (env, offer) => sendDiscordOffer(env, offer, fetchImpl),
  };
  const Queue = new Function(...Object.keys(dependencies),
    `${helpers}\nreturn class {${methods}}`)(...Object.values(dependencies));
  const queue = new Queue();
  queue.env = {
    TELEGRAM_TOKEN: "synthetic-token", TELEGRAM_CHAT_ID: "synthetic-main",
    CANAL2_ID: "synthetic-canal2", CANAL2_DELIVERY_ENABLED: String(target === "canal2"),
    DISCORD_DELIVERY_ENABLED: String(target === "discord"),
    DISCORD_WEBHOOK_URL: "https://discord.invalid/api/webhooks/synthetic",
  };
  queue.currentDeliveryMode = () => "live";
  queue.currentDeliveryGeneration = () => 1;
  queue.deliveryQueueSlo = () => ({});
  queue.storageUsageSnapshot = () => ({});
  queue.telegramOfferWithImageState = (offer) => ({ offer, imageKey: "" });
  queue.recordImageDelivery = () => { if (imageFailure) throw new Error("synthetic_image_sql_failure"); };
  queue.enqueueBeeperOffer = () => false;
  queue.sqlExec = (sql, ...params) => {
    if (accepted && failLedger && sql.includes("INSERT OR IGNORE INTO delivery_events")) {
      throw new Error("synthetic_ledger_sql_failure");
    }
    const execute = () => {
      const statement = database.prepare(sql);
      if (statement.columns().length) {
        const rows = statement.all(...params);
        return { toArray: () => rows, one: () => rows[0] };
      }
      const result = statement.run(...params);
      return { toArray: () => [], rowsWritten: result.changes };
    };
    if (accepted && fault) {
      const result = fault({ sql, params, database, execute });
      if (result) return result;
    }
    return execute();
  };
  const now = new Date().toISOString();
  database.prepare(`INSERT INTO offers(id, link, preview_title, title,
    first_seen_at, last_seen_at, status, delivery_mode, delivery_generation,
    would_send_main, would_send_canal2, main_message_id, main_sent_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'live', 1, 1, ?, ?, ?)`).run(
    "synthetic-offer", "https://clube.uol.com.br/campanhasdeingresso/synthetic",
    "Synthetic receipt test", "Synthetic receipt test", now, now,
    target === "main" ? "delivery_pending" : "partial_delivery",
    Number(target === "canal2"), target === "main" ? 0 : 700, target === "main" ? "" : now,
  );
  return {
    database, queue, requests,
    row: () => database.prepare("SELECT * FROM offers").get(),
    events: () => database.prepare("SELECT * FROM delivery_events ORDER BY id").all(),
    posts: () => postCount,
    run: (now = new Date()) => queue.processDeliveryQueue(now, {
      targetNames: [target], waitForMainImage: false,
    }),
  };
}

for (const target of targets) {
  test(`${target}: HTTP receipt survives offer SQL failure without another POST`, async (t) => {
    let injected = false;
    const f = fixture(t, target, { fault: ({ sql }) => {
      if (!injected && sql.startsWith(`UPDATE offers SET ${target}_message_id = ?`)) {
        injected = true;
        throw new Error("synthetic_receipt_sql_failure");
      }
    } });
    await f.run();
    assert.equal(injected, true);
    const row = f.row();
    assert.equal(row[`${target}_delivery_attempts`], 1);
    assert.equal(row[`${target}_delivery_next_attempt_at`], "");
    assert.ok(row[`${target}_delivery_unknown_at`]);
    assert.equal(row[`${target}_sent_at`], "");
    const event = f.events().find((item) => item.state === "late_sent");
    assert.equal(event.external_id, String(receipt(target)));
    assert.equal(event.generation, 1);
    assert.equal(event.attempt, 1);
    await f.run(new Date(Date.now() + 10 * 60_000));
    assert.equal(f.posts(), 1);
  });

  test(`${target}: receipt and ledger storage failure preserve the original claim`, async (t) => {
    const f = fixture(t, target, { failLedger: true, fault: ({ sql }) => {
      if (sql.startsWith(`UPDATE offers SET ${target}_message_id = ?`) ||
          sql.startsWith(`UPDATE offers SET ${target}_delivery_error = 'ambiguous:receipt_persistence_failed'`)) {
        throw new Error("synthetic_receipt_sql_failure");
      }
    } });
    await f.run();
    const row = f.row();
    assert.ok(row[`${target}_delivery_in_flight_at`]);
    assert.equal(row[`${target}_delivery_unknown_at`], "");
    assert.equal(row[`${target}_delivery_next_attempt_at`], "");
    assert.equal(row[`${target}_delivery_attempts`], 1);
    await f.run();
    await f.run(new Date(Date.now() + 10 * 60_000));
    assert.equal(f.posts(), 1);
    assert.ok(f.row()[`${target}_delivery_unknown_at`]);
  });

  test(`${target}: committed receipt survives cursor and subsequent ledger failure`, async (t) => {
    let injected = false;
    const f = fixture(t, target, { failLedger: true, fault: ({ sql, execute }) => {
      if (!injected && sql.startsWith(`UPDATE offers SET ${target}_message_id = ?`)) {
        injected = true;
        execute();
        throw new Error("synthetic_post_commit_cursor_failure");
      }
    } });
    await f.run();
    const row = f.row();
    assert.equal(row[`${target}_message_id`], receipt(target));
    assert.ok(row[`${target}_sent_at`]);
    assert.equal(row[`${target}_delivery_in_flight_at`], "");
    assert.equal(row[`${target}_delivery_unknown_at`], "");
    assert.equal(row.delivery_unknown_at, "");
    await f.run(new Date(Date.now() + 10 * 60_000));
    assert.equal(f.posts(), 1);
  });
}

test("main: image bookkeeping failure preserves committed receipt even if ledger fails", async (t) => {
  const f = fixture(t, "main", { failLedger: true, imageFailure: true });
  const result = await f.run();
  assert.equal(result.mainSent, 1);
  assert.equal(f.row().main_message_id, receipt("main"));
  assert.ok(f.row().main_sent_at);
  assert.equal(f.row().main_delivery_unknown_at, "");
  await f.run(new Date(Date.now() + 10 * 60_000));
  assert.equal(f.posts(), 1);
});

for (const changedField of ["delivery_generation", "main_delivery_attempts", "main_delivery_in_flight_at"]) {
  test(`main: late receipt cannot quarantine a changed ${changedField}`, async (t) => {
    let injected = false;
    const replacement = changedField === "main_delivery_in_flight_at"
      ? "2099-01-01T00:00:00.000Z" : 2;
    const f = fixture(t, "main", { fault: ({ sql, database }) => {
      if (!injected && sql.startsWith("UPDATE offers SET main_message_id = ?")) {
        injected = true;
        database.prepare(`UPDATE offers SET ${changedField} = ?`).run(replacement);
        throw new Error("synthetic_late_receipt_sql_failure");
      }
    } });
    await f.run();
    const row = f.row();
    assert.equal(row[changedField], replacement);
    assert.ok(row.main_delivery_in_flight_at);
    assert.equal(row.main_delivery_unknown_at, "");
    assert.equal(f.events().find((item) => item.state === "late_sent").external_id, "701");
    assert.equal(f.posts(), 1);
  });
}

test("main: late outcome cannot replace a receipt already reconciled", async (t) => {
  let injected = false;
  const f = fixture(t, "main", { fault: ({ sql, database }) => {
    if (!injected && sql.startsWith("UPDATE offers SET main_message_id = ?")) {
      injected = true;
      database.prepare(`UPDATE offers SET main_message_id = 702, main_sent_at = ?,
        main_delivery_in_flight_at = ''`).run(new Date().toISOString());
      throw new Error("synthetic_reconciled_receipt_sql_failure");
    }
  } });
  await f.run();
  assert.equal(f.row().main_message_id, 702);
  assert.ok(f.row().main_sent_at);
  assert.equal(f.row().main_delivery_unknown_at, "");
  assert.equal(f.events().find((item) => item.state === "late_sent").external_id, "701");
  await f.run(new Date(Date.now() + 10 * 60_000));
  assert.equal(f.posts(), 1);
});

test("workerd SQLite preserves the receipt and quarantine with rowsWritten-only cursors", async (t) => {
  const bundled = await build({
    stdin: {
      resolveDir: fileURLToPath(new URL("../src/", import.meta.url)),
      contents: `
        import { DurableObject } from "cloudflare:workers";
        import { cleanText, isTicketCampaign } from "./core.js";
        import { classifyDeliveryRow, deliveryConfiguration, deliveryRetryAt,
          isAmbiguousDeliveryError } from "./delivery-state.js";
        import { recordDeliveryEvent as writeDeliveryEvent, trimDeliveryEvents } from "./delivery-ledger.js";
        import { chooseDeliveryBudget, isRecentDeliveryDecision,
          RECENT_DELIVERY_PRIORITY_MS } from "./queue-policy.js";
        import { deferredMainDeliveryState, mainImageDeliveryOffer } from "./image-deadline.js";
        import { forwardToCanal2, sendMainOffer, telegramConfiguration,
          TELEGRAM_MUTATION_TIMEOUT_SECONDS } from "./telegram.js";
        import { discordConfiguration, sendDiscordOffer } from "./discord.js";
        const logEvent = () => {};
        ${helpers}
        export class ReceiptQueue extends DurableObject {
          constructor(ctx, env) {
            super(ctx, env);
            this.rejectReceipt = true;
            for (const sql of ${JSON.stringify(migrationSql)}) ctx.storage.sql.exec(sql);
          }
          ${methods}
          currentDeliveryMode() { return "live"; }
          currentDeliveryGeneration() { return 1; }
          deliveryQueueSlo() { return {}; }
          storageUsageSnapshot() { return {}; }
          telegramOfferWithImageState(offer) { return { offer, imageKey: "" }; }
          recordImageDelivery() {}
          enqueueBeeperOffer() { return false; }
          sqlExec(sql, ...params) {
            if (this.rejectReceipt && sql.startsWith("UPDATE offers SET main_message_id = ?")) {
              this.rejectReceipt = false;
              throw new Error("synthetic_receipt_sql_failure");
            }
            const cursor = this.ctx.storage.sql.exec(sql, ...params);
            if (sql.includes("INSERT OR IGNORE INTO delivery_events")) {
              this.ledgerCursor = { rowsWritten: cursor.rowsWritten, hasChanges: "changes" in cursor };
            }
            return cursor;
          }
          async fetch() {
            const now = new Date().toISOString();
            this.sqlExec("INSERT OR IGNORE INTO offers(id, link, preview_title, first_seen_at, last_seen_at, status, delivery_mode, delivery_generation, would_send_main) VALUES ('synthetic-offer', 'https://clube.uol.com.br/synthetic', 'Synthetic receipt', ?, ?, 'delivery_pending', 'live', 1, 1)", now, now);
            await this.processDeliveryQueue(new Date(), { targetNames: ["main"], waitForMainImage: false });
            await this.processDeliveryQueue(new Date(Date.now() + 600000), { targetNames: ["main"], waitForMainImage: false });
            return Response.json({
              row: this.sqlExec("SELECT * FROM offers").toArray()[0],
              events: this.sqlExec("SELECT * FROM delivery_events ORDER BY id").toArray(),
              ledgerCursor: this.ledgerCursor,
            });
          }
        }
        export default {
          fetch(request, env) { return env.QUEUE.get(env.QUEUE.idFromName("receipt")).fetch(request); }
        };`,
    },
    bundle: true, format: "esm", platform: "browser", target: "es2022",
    external: ["cloudflare:workers"], write: false, logLevel: "silent",
  });
  let posts = 0;
  const runtime = new Miniflare({
    compatibilityDate: "2026-07-30", modules: true, cf: false,
    log: new Log(LogLevel.NONE), script: bundled.outputFiles[0].text,
    durableObjects: { QUEUE: { className: "ReceiptQueue", useSQLite: true } },
    bindings: {
      TELEGRAM_TOKEN: "synthetic-token", TELEGRAM_CHAT_ID: "synthetic-main",
      CANAL2_DELIVERY_ENABLED: "false", DISCORD_DELIVERY_ENABLED: "false",
    },
    outboundService: async (request) => {
      assert.equal(request.method, "POST");
      assert.equal(new URL(request.url).pathname, "/botsynthetic-token/sendMessage");
      posts += 1;
      return new MiniflareResponse(JSON.stringify({ ok: true, result: { message_id: 701 } }), {
        headers: { "Content-Type": "application/json" },
      });
    },
  });
  t.after(() => runtime.dispose());
  const response = await runtime.dispatchFetch("https://worker.invalid/receipt");
  assert.equal(response.status, 200);
  const result = await response.json();
  assert.equal(posts, 1);
  assert.equal(result.ledgerCursor.hasChanges, false);
  assert.ok(result.ledgerCursor.rowsWritten > 0);
  assert.equal(result.events.find((event) => event.state === "late_sent").external_id, "701");
  assert.ok(result.row.main_delivery_unknown_at);
  assert.equal(result.row.main_delivery_in_flight_at, "");
  assert.equal(result.row.main_delivery_next_attempt_at, "");
  assert.equal(result.row.main_delivery_attempts, 1);
});
