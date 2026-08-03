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
  assert.equal(blocks.length, 16);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks) {
    assert.equal(sql.includes("${"), false, "migração não pode depender de interpolação dinâmica");
    database.exec(sql);
  }
  const version = database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version;
  assert.equal(Number(version), 16);
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
  ]) {
    assert.equal(columns.has(column), true, `coluna ausente: ${column}`);
  }
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
    16,
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
