import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

const NOW = "2026-08-17T21:00:00.000Z";

function currentDatabase() {
  const source = readFileSync(new URL("../src/worker.js", import.meta.url), "utf8");
  const start = source.indexOf("  migrate() {");
  const end = source.indexOf("\n  metadataValue(", start);
  const blocks = [...source.slice(start, end).matchAll(/this\.sqlExec\(`([\s\S]*?)`\);/g)]
    .map((match) => match[1]);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks) database.exec(sql);
  return database;
}

const discordImageSql = `SELECT id FROM offers INDEXED BY offers_discord_image_cache_due_v25
  WHERE would_send_main = 1
    AND link NOT LIKE '%/campanhasdeingresso/%'
    AND discord_image_cache_message_id = ''
    AND discord_image_proxy_url = ''
    AND status NOT IN ('baseline', 'discarded', 'shadow_sold_out', 'sold_out')
    AND discord_image_cache_attempts < ?
    AND discord_image_cache_next_attempt_at <= ?
  ORDER BY first_seen_at DESC LIMIT ?`;

const mainImageSql = `SELECT id FROM offers INDEXED BY offers_main_image_upgrade_due_v25
  WHERE telegram_image_strategy = 'text_timeout'
    AND main_message_kind = 'text'
    AND main_message_id > 0
    AND main_image_upgrade_attempts < ?
    AND COALESCE(NULLIF(telegram_photo_file_id, ''), NULLIF(image_url, ''),
                 NULLIF(card_image_url, ''), partner_image_url) <> ''
    AND main_image_upgrade_next_attempt_at <= ?
  ORDER BY CASE
    WHEN discord_image_proxy_url <> '' THEN 0
    WHEN discord_message_id <> '' THEN 1
    ELSE 2
  END, first_seen_at DESC LIMIT ?`;

test("v25 restringe as duas buscas recorrentes de imagem a índices parciais", () => {
  const database = currentDatabase();
  const indexNames = new Set(database.prepare(
    "SELECT name FROM sqlite_schema WHERE type = 'index'",
  ).all().map((row) => row.name));
  for (const retired of [
    "offers_main_image_upgrade_idx",
    "offers_discord_image_cache_idx",
    "offers_discord_feed_idx",
  ]) {
    assert.equal(indexNames.has(retired), false);
  }
  const plans = [
    database.prepare(`EXPLAIN QUERY PLAN ${discordImageSql}`).all(10, NOW, 2),
    database.prepare(`EXPLAIN QUERY PLAN ${mainImageSql}`).all(10, NOW, 4),
  ].map((rows) => rows.map((row) => row.detail).join(" | "));

  assert.match(plans[0], /offers_discord_image_cache_due_v25/);
  assert.match(plans[0], /SEARCH offers USING INDEX/);
  assert.match(plans[1], /offers_main_image_upgrade_due_v25/);
  assert.match(plans[1], /SEARCH offers USING INDEX/);

  const insert = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       would_send_main, discord_image_cache_next_attempt_at,
       telegram_image_strategy, main_message_kind, main_message_id,
       image_url, main_image_upgrade_next_attempt_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  insert.run(
    "due",
    "https://clube.uol.com.br/beneficios/due",
    "due",
    NOW,
    NOW,
    "delivered",
    1,
    "",
    "text_timeout",
    "text",
    123,
    "https://example.test/due.jpg",
    "",
  );
  insert.run(
    "future",
    "https://clube.uol.com.br/beneficios/future",
    "future",
    NOW,
    NOW,
    "delivered",
    1,
    "2026-08-17T22:00:00.000Z",
    "text_timeout",
    "text",
    124,
    "https://example.test/future.jpg",
    "2026-08-17T22:00:00.000Z",
  );
  insert.run(
    "terminal",
    "https://clube.uol.com.br/beneficios/terminal",
    "terminal",
    NOW,
    NOW,
    "sold_out",
    1,
    "",
    "remote_url",
    "photo",
    125,
    "https://example.test/terminal.jpg",
    "",
  );

  const discordIds = database.prepare(discordImageSql).all(10, NOW, 2)
    .map((row) => row.id);
  const mainIds = database.prepare(mainImageSql).all(10, NOW, 4)
    .map((row) => row.id);
  assert.deepEqual(discordIds, ["due"]);
  assert.deepEqual(mainIds, ["due"]);
  assert.deepEqual(
    discordIds,
    database.prepare(discordImageSql.replace(
      " INDEXED BY offers_discord_image_cache_due_v25",
      "",
    )).all(10, NOW, 2).map((row) => row.id),
  );
  assert.deepEqual(
    mainIds,
    database.prepare(mainImageSql.replace(
      " INDEXED BY offers_main_image_upgrade_due_v25",
      "",
    )).all(10, NOW, 4).map((row) => row.id),
  );
  database.close();
});
