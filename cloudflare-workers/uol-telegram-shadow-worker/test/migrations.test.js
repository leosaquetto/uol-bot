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
  assert.equal(blocks.length, 20);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks) {
    assert.equal(sql.includes("${"), false, "migração não pode depender de interpolação dinâmica");
    database.exec(sql);
  }
  const version = database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version;
  assert.equal(Number(version), 20);
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
  const aliasColumns = new Set(
    database.prepare("PRAGMA table_info(offer_identity_aliases)").all()
      .map((column) => column.name),
  );
  assert.deepEqual(aliasColumns, new Set(["alias", "offer_id", "first_seen_at"]));
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
    20,
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
