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
  return [...body.matchAll(/this\.ctx\.storage\.sql\.exec\(`([\s\S]*?)`\);/g)]
    .map((match) => match[1]);
}

test("migrações SQLite criam do zero o schema corrente", () => {
  const blocks = migrationBlocks();
  assert.equal(blocks.length, 14);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks) {
    assert.equal(sql.includes("${"), false, "migração não pode depender de interpolação dinâmica");
    database.exec(sql);
  }
  const version = database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version;
  assert.equal(Number(version), 14);
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
    14,
  );
  database.close();
});
