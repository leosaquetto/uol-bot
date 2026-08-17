import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

function migrationBlocks() {
  const source = readFileSync(new URL("../src/worker.js", import.meta.url), "utf8");
  const start = source.indexOf("  migrate() {");
  const end = source.indexOf("\n  metadataValue(", start);
  assert.ok(start >= 0 && end > start, "método migrate não encontrado");
  const body = source.slice(start, end);
  return [...body.matchAll(/this\.sqlExec\(`([\s\S]*?)`\);/g)]
    .map((match) => match[1]);
}

test("migrações SQLite criam do zero o schema corrente", () => {
  const blocks = migrationBlocks();
  assert.equal(blocks.length, 25);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks) {
    assert.equal(sql.includes("${"), false, "migração não pode depender de interpolação dinâmica");
    database.exec(sql);
  }
  const version = database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version;
  assert.equal(Number(version), 25);
  const columns = new Set(database.prepare("PRAGMA table_info(offers)").all()
    .map((column) => column.name));
  for (const column of [
    "delivery_generation",
    "main_delivery_unknown_at",
    "canal2_delivery_unknown_at",
    "discord_delivery_unknown_at",
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
    assert.equal(columns.has(column), true, `coluna ausente: ${column}`);
  }
  const probeColumns = new Set(database.prepare("PRAGMA table_info(ticket_probe_state)").all()
    .map((column) => column.name));
  assert.deepEqual(probeColumns, new Set([
    "offer_id", "next_at", "last_at", "last_result", "gone_count", "attempts",
  ]));
  const ledgerColumns = new Set(database.prepare("PRAGMA table_info(delivery_events)").all()
    .map((column) => column.name));
  assert.deepEqual(ledgerColumns, new Set([
    "id", "dedupe_key", "offer_id", "target", "operation", "state", "attempt",
    "generation", "occurred_at", "external_id", "error",
  ]));
  const beeperColumns = new Set(
    database.prepare("PRAGMA table_info(beeper_delivery_queue)").all()
      .map((column) => column.name),
  );
  assert.deepEqual(beeperColumns, new Set([
    "offer_id", "attempts", "next_attempt_at", "in_flight_at", "sent_at",
    "last_error", "pending_message_id",
  ]));
  const aliasColumns = new Set(
    database.prepare("PRAGMA table_info(offer_identity_aliases)").all()
      .map((column) => column.name),
  );
  assert.deepEqual(aliasColumns, new Set(["alias", "offer_id", "first_seen_at"]));
  const indexes = new Map(database.prepare(
    "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL",
  ).all().map((index) => [index.name, String(index.sql)]));
  for (const name of [
    "offers_comment_inflight_v21",
    "offers_comment_due_v21",
    "ticket_probe_due_v21",
    "discord_avail_sold_due_v21",
    "discord_avail_restock_due_v21",
    "offers_soldout_main_due_v22",
    "offers_soldout_canal2_due_v22",
    "offers_restock_main_due_v22",
    "offers_restock_canal2_due_v22",
    "offers_restock_finalize_v22",
    "beeper_delivery_due_v23",
    "beeper_delivery_inflight_v24",
    "offers_discord_image_cache_due_v25",
    "offers_main_image_upgrade_due_v25",
    "beeper_delivery_pending_v25",
  ]) {
    assert.equal(indexes.has(name), true, `índice ausente: ${name}`);
  }
  for (const name of [
    "offers_main_image_upgrade_idx",
    "offers_discord_image_cache_idx",
    "offers_discord_feed_idx",
  ]) {
    assert.equal(indexes.has(name), false, `índice substituído ainda presente: ${name}`);
  }
  assert.match(indexes.get("offers_comment_due_v21"), /WHERE discussion_message_id > 0/);
  assert.match(indexes.get("ticket_probe_due_v21"), /WHERE next_at > ''/);
  assert.match(indexes.get("discord_avail_sold_due_v21"), /WHERE sold_out_synced_at = ''/);
  assert.match(indexes.get("discord_avail_restock_due_v21"), /WHERE restock_synced_at = ''/);
  assert.match(indexes.get("offers_soldout_main_due_v22"), /status = 'sold_out'/);
  assert.match(indexes.get("offers_soldout_canal2_due_v22"), /would_send_canal2 = 1/);
  assert.match(indexes.get("offers_restock_main_due_v22"), /main_restock_synced_at = ''/);
  assert.match(indexes.get("offers_restock_canal2_due_v22"), /canal2_restock_synced_at = ''/);
  assert.match(indexes.get("offers_restock_finalize_v22"), /main_restock_synced_at <> ''/);
  database.close();
});

test("upgrade v9 preserva recibos, comentários e estado operacional", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 9)) database.exec(sql);
  database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       main_message_id, main_sent_at, canal2_message_id, canal2_sent_at,
       discussion_message_id, comment_message_ids, comment_chunks_sent,
       sold_out_at, main_sold_out_synced_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    "offer-v9",
    "https://clube.uol.com.br/beneficio/teste",
    "Oferta preservada",
    "2026-08-01T12:00:00.000Z",
    "2026-08-01T12:00:00.000Z",
    "sold_out",
    101,
    "2026-08-01T12:00:02.000Z",
    202,
    "2026-08-01T12:00:03.000Z",
    303,
    "[404]",
    1,
    "2026-08-01T13:00:00.000Z",
    "2026-08-01T13:00:01.000Z",
  );
  for (const sql of blocks.slice(9)) database.exec(sql);

  const row = database.prepare("SELECT * FROM offers WHERE id = ?").get("offer-v9");
  assert.equal(row.status, "sold_out");
  assert.equal(row.main_message_id, 101);
  assert.equal(row.canal2_message_id, 202);
  assert.equal(row.discussion_message_id, 303);
  assert.equal(row.comment_message_ids, "[404]");
  assert.equal(row.sold_out_at, "2026-08-01T13:00:00.000Z");
  assert.equal(row.delivery_generation, 1);
  assert.equal(row.main_delivery_unknown_at, "");
  assert.equal(row.main_restock_next_attempt_at, "");
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    25,
  );
  database.close();
});

test("v24 remove somente pendências Beeper que não são ingressos", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 23)) database.exec(sql);
  const insertOffer = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, category, first_seen_at, last_seen_at, status
     ) VALUES (?, ?, ?, ?, ?, ?, 'delivered')`,
  );
  const seenAt = "2026-08-17T18:14:48.754Z";
  insertOffer.run(
    "ticket-link",
    "https://clube.uol.com.br/campanhasdeingresso/ticket-link",
    "Ingresso por link",
    "",
    seenAt,
    seenAt,
  );
  insertOffer.run(
    "ticket-category",
    "https://clube.uol.com.br/beneficio/ticket-category",
    "Ingresso por categoria",
    "campanhasdeingresso",
    seenAt,
    seenAt,
  );
  insertOffer.run(
    "ordinary",
    "https://clube.uol.com.br/beneficio/ordinary",
    "Oferta comum",
    "gastronomia",
    seenAt,
    seenAt,
  );
  for (const id of ["ticket-link", "ticket-category", "ordinary"]) {
    database.prepare(
      "INSERT INTO beeper_delivery_queue(offer_id) VALUES (?)",
    ).run(id);
  }
  database.exec(blocks[23]);
  assert.deepEqual(
    database.prepare(
      "SELECT offer_id FROM beeper_delivery_queue ORDER BY offer_id",
    ).all().map((row) => row.offer_id),
    ["ticket-category", "ticket-link"],
  );
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    24,
  );
  database.close();
});

test("v19 agenda somente ingressos recentes entregues para probe", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 18)) database.exec(sql);
  const recent = new Date(Date.now() - 60 * 60_000).toISOString();
  const insert = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       main_sent_at, sold_out_at
     ) VALUES (?, ?, ?, ?, ?, 'delivered', ?, '')`,
  );
  insert.run(
    "ticket-recent",
    "https://clube.uol.com.br/campanhasdeingresso/ticket-recent",
    "Ingresso recente",
    recent,
    recent,
    recent,
  );
  insert.run(
    "common-recent",
    "https://clube.uol.com.br/beneficios/common-recent",
    "Oferta comum recente",
    recent,
    recent,
    recent,
  );
  database.exec(blocks[18]);
  const ticket = database.prepare(
    "SELECT next_at FROM ticket_probe_state WHERE offer_id = ?",
  ).get("ticket-recent");
  const common = database.prepare(
    "SELECT next_at FROM ticket_probe_state WHERE offer_id = ?",
  ).get("common-recent");
  assert.notEqual(ticket.next_at, "");
  assert.equal(common, undefined);
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    19,
  );
  database.close();
});

test("v20 cria ledger sem alterar a tabela offers e faz backfill mínimo", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 19)) database.exec(sql);
  database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       delivery_generation, main_sent_at, main_message_id,
       canal2_sent_at, canal2_message_id
     ) VALUES (?, ?, ?, ?, ?, 'delivered', 3, ?, 101, ?, 202)`,
  ).run(
    "ledger-backfill",
    "https://clube.uol.com.br/beneficios/ledger-backfill",
    "Oferta ledger",
    "2026-08-04T19:00:00.000Z",
    "2026-08-04T19:00:00.000Z",
    "2026-08-04T19:00:01.000Z",
    "2026-08-04T19:00:02.000Z",
  );

  database.exec(blocks[19]);
  const events = database.prepare(
    `SELECT target, operation, state, generation, external_id
     FROM delivery_events WHERE offer_id = ? ORDER BY target`,
  ).all("ledger-backfill").map((row) => ({ ...row }));
  assert.deepEqual(events, [
    {
      target: "canal2",
      operation: "snapshot",
      state: "sent",
      generation: 3,
      external_id: "202",
    },
    {
      target: "main",
      operation: "snapshot",
      state: "sent",
      generation: 3,
      external_id: "101",
    },
  ]);
  const offersColumns = database.prepare("PRAGMA table_info(offers)").all();
  assert.equal(offersColumns.some((column) => column.name === "delivery_event"), false);
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    20,
  );
  database.close();
});

test("v21 cria índices parciais e faz backfill da disponibilidade Discord", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 20)) database.exec(sql);
  const insert = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status, restocked_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  );
  insert.run(
    "sold-out-v21",
    "https://clube.uol.com.br/beneficios/sold-out-v21",
    "Esgotada",
    "2026-08-10T12:00:00.000Z",
    "2026-08-10T12:00:00.000Z",
    "sold_out",
    "",
  );
  insert.run(
    "restock-v21",
    "https://clube.uol.com.br/beneficios/restock-v21",
    "Reposta",
    "2026-08-10T12:00:00.000Z",
    "2026-08-10T12:00:00.000Z",
    "delivered",
    "2026-08-10T13:00:00.000Z",
  );
  database.prepare(
    `INSERT INTO ticket_probe_state(offer_id, next_at)
     VALUES ('sold-out-v21', '9999')`,
  ).run();
  database.exec(blocks[20]);

  assert.deepEqual(
    database.prepare(
      "SELECT offer_id FROM discord_availability_sync ORDER BY offer_id",
    ).all().map((row) => row.offer_id),
    ["restock-v21", "sold-out-v21"],
  );
  assert.equal(Number(database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version), 21);
  const repairedProbeAt = database.prepare(
    "SELECT next_at FROM ticket_probe_state WHERE offer_id = 'sold-out-v21'",
  ).get().next_at;
  assert.notEqual(repairedProbeAt, "9999");
  assert.equal(Number.isFinite(Date.parse(repairedProbeAt)), true);
  const commentPlan = database.prepare(
    `EXPLAIN QUERY PLAN SELECT id FROM offers
     WHERE discussion_message_id > 0 AND comment_sent_at = ''
       AND comment_delivery_in_flight_at = '' AND comment_delivery_unknown_at = ''
       AND delivery_generation = ?
       AND status NOT IN (
         'discarded', 'delivery_quarantined', 'shadow_candidate',
         'baseline', 'shadow_sold_out'
       )
       AND comment_delivery_next_attempt_at <= ?
     LIMIT 2`,
  ).all(1, "2026-08-10T14:00:00.000Z").map((row) => row.detail).join(" ");
  const ticketPlan = database.prepare(
    `EXPLAIN QUERY PLAN SELECT o.id
     FROM ticket_probe_state AS s INDEXED BY ticket_probe_due_v21
     JOIN offers AS o ON o.id = s.offer_id
     WHERE s.next_at > '' AND s.next_at <= ? AND s.attempts < ?
     LIMIT 1`,
  ).all("2026-08-10T14:00:00.000Z", 2).map((row) => row.detail).join(" ");
  const discordPlan = database.prepare(
    `EXPLAIN QUERY PLAN SELECT o.id
     FROM discord_availability_sync AS d INDEXED BY discord_avail_sold_due_v21
     JOIN offers AS o ON o.id = d.offer_id
     WHERE d.sold_out_synced_at = '' AND d.sold_out_next_attempt_at <= ?
     LIMIT 4`,
  ).all("2026-08-10T14:00:00.000Z").map((row) => row.detail).join(" ");
  assert.match(commentPlan, /offers_comment_due_v21/);
  assert.match(ticketPlan, /ticket_probe_due_v21/);
  assert.match(discordPlan, /discord_avail_sold_due_v21/);
  database.close();
});

test("v16 encerra edições impossíveis e libera retry de restock", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 15)) database.exec(sql);
  const insert = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       main_message_id, main_sent_at, sold_out_at,
       main_sold_out_attempts, main_sold_out_error,
       main_restock_attempts, main_restock_error
     ) VALUES (?, ?, ?, ?, ?, ?, 101, ?, ?, ?, ?, ?, ?)`,
  );
  insert.run(
    "sold-out-missing",
    "https://clube.uol.com.br/beneficios/sold-out-missing",
    "Esgotada",
    "2026-08-03T12:00:00.000Z",
    "2026-08-03T12:00:00.000Z",
    "sold_out",
    "2026-08-03T12:00:01.000Z",
    "2026-08-03T13:00:00.000Z",
    10,
    "telegram_editMessageText_400:Bad Request: message to edit not found",
    0,
    "",
  );
  insert.run(
    "restock-missing",
    "https://clube.uol.com.br/beneficios/restock-missing",
    "Disponível novamente",
    "2026-08-03T12:00:00.000Z",
    "2026-08-03T12:00:00.000Z",
    "restocked_pending_sync",
    "2026-08-03T12:00:01.000Z",
    "2026-08-03T13:00:00.000Z",
    0,
    "",
    10,
    "telegram_editMessageText_400:Bad Request: message to edit not found",
  );
  database.prepare(
    `INSERT INTO incidents(
       key, status, severity, summary, details, first_detected_at, last_detected_at
     ) VALUES (?, 'active', 'critical', 'Falha', 'Teste', ?, ?)`,
  ).run(
    "delivery-queue:sold-out-missing:main_sold_out",
    "2026-08-03T13:00:00.000Z",
    "2026-08-03T13:00:00.000Z",
  );

  database.exec(blocks[15]);
  const soldOut = database.prepare(
    `SELECT main_sold_out_synced_at, main_sold_out_error
     FROM offers WHERE id = 'sold-out-missing'`,
  ).get();
  assert.notEqual(soldOut.main_sold_out_synced_at, "");
  assert.equal(soldOut.main_sold_out_error, "");
  assert.equal(
    database.prepare(
      "SELECT status FROM incidents WHERE key = ?",
    ).get("delivery-queue:sold-out-missing:main_sold_out").status,
    "resolved",
  );
  const restock = database.prepare(
    `SELECT main_restock_attempts, main_restock_error
     FROM offers WHERE id = 'restock-missing'`,
  ).get();
  assert.equal(Number(restock.main_restock_attempts), 0);
  assert.equal(restock.main_restock_error, "");
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    16,
  );
  database.close();
});

test("v17 recupera Canal 2 e fotos do incidente atual sem reenviar histórico", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 16)) database.exec(sql);
  const insert = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       decision_at, would_send_main, would_send_canal2,
       main_message_id, main_message_kind, main_sent_at,
       image_url, telegram_image_strategy, main_image_upgrade_attempts,
       main_image_upgrade_error
     ) VALUES (?, ?, ?, ?, ?, 'delivered', ?, 1, 0, 101, 'text', ?, ?, ?, 10, ?)`,
  );
  insert.run(
    "ticket-atual",
    "https://clube.uol.com.br/campanhasdeingresso/ticket-atual",
    "2 ingressos Teatro Itália",
    "2026-08-03T19:31:00.000Z",
    "2026-08-03T19:31:00.000Z",
    "2026-08-03T19:31:00.000Z",
    "2026-08-03T19:31:10.000Z",
    "https://example.com/ticket.png",
    "text_timeout",
    "offer_image_http_403",
  );
  insert.run(
    "ticket-antigo",
    "https://clube.uol.com.br/campanhasdeingresso/ticket-antigo",
    "2 ingressos antigos",
    "2026-08-03T18:00:00.000Z",
    "2026-08-03T18:00:00.000Z",
    "2026-08-03T18:00:00.000Z",
    "2026-08-03T18:00:10.000Z",
    "https://example.com/antigo.png",
    "text_timeout",
    "offer_image_http_403",
  );

  database.exec(blocks[16]);
  const current = database.prepare("SELECT * FROM offers WHERE id = 'ticket-atual'").get();
  assert.equal(Number(current.would_send_canal2), 1);
  assert.equal(current.status, "partial_delivery");
  assert.equal(Number(current.main_image_upgrade_attempts), 0);
  assert.equal(current.main_image_upgrade_error, "");
  assert.equal(current.discord_image_proxy_url, "");
  const old = database.prepare("SELECT * FROM offers WHERE id = 'ticket-antigo'").get();
  assert.equal(Number(old.would_send_canal2), 0);
  assert.equal(Number(old.main_image_upgrade_attempts), 10);
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    17,
  );
  database.close();
});

test("v18 inicia o feed comum sem despejar ofertas históricas", () => {
  const blocks = migrationBlocks();
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 17)) database.exec(sql);
  database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       would_send_main, main_sent_at
     ) VALUES (?, ?, ?, ?, ?, 'delivered', 1, ?)`,
  ).run(
    "oferta-comum-antiga",
    "https://clube.uol.com.br/beneficios/oferta-comum-antiga",
    "Oferta comum antiga",
    "2026-08-03T18:00:00.000Z",
    "2026-08-03T18:00:00.000Z",
    "2026-08-03T18:00:10.000Z",
  );

  database.exec(blocks[17]);
  const row = database.prepare(
    "SELECT discord_image_cache_attempts FROM offers WHERE id = ?",
  ).get("oferta-comum-antiga");
  assert.equal(Number(row.discord_image_cache_attempts), 10);
  assert.equal(
    database.prepare(
      "SELECT COUNT(*) AS count FROM sqlite_schema WHERE type = 'table' AND name = ?",
    ).get("discord_availability_sync").count,
    1,
  );
  assert.equal(
    Number(database.prepare(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).get().version),
    18,
  );
  database.close();
});
