import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

const NOW = "2026-08-12T12:00:00.000Z";
const MAX_ATTEMPTS = 10;

function migrationBlocks() {
  const source = readFileSync(new URL("../src/worker.js", import.meta.url), "utf8");
  const start = source.indexOf("  migrate() {");
  const end = source.indexOf("\n  metadataValue(", start);
  assert.ok(start >= 0 && end > start, "método migrate não encontrado");
  return [...source.slice(start, end).matchAll(/this\.sqlExec\(`([\s\S]*?)`\);/g)]
    .map((match) => match[1]);
}

function databaseAtV22() {
  const database = new DatabaseSync(":memory:");
  for (const sql of migrationBlocks()) database.exec(sql);
  return database;
}

function insertOffer(database, id, overrides) {
  const row = {
    id,
    link: `https://clube.uol.com.br/beneficios/${id}`,
    preview_title: id,
    first_seen_at: overrides.sold_out_at || overrides.restocked_at || NOW,
    last_seen_at: overrides.sold_out_at || overrides.restocked_at || NOW,
    ...overrides,
  };
  const columns = Object.keys(row);
  database.prepare(
    `INSERT INTO offers(${columns.join(", ")})
     VALUES (${columns.map(() => "?").join(", ")})`,
  ).run(...Object.values(row));
}

function plan(database, sql, ...params) {
  return database.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params)
    .map((row) => row.detail).join(" ");
}

function mergeIds(branches, timestamp) {
  const byId = new Map();
  for (const row of branches.flat().sort((left, right) =>
    String(left[timestamp] || "").localeCompare(String(right[timestamp] || ""))
  )) {
    if (!byId.has(row.id)) byId.set(row.id, row);
  }
  return [...byId.keys()].slice(0, 4);
}

const soldOutMainSql = `SELECT id, sold_out_at
  FROM offers INDEXED BY offers_soldout_main_due_v22
  WHERE status = 'sold_out'
    AND main_message_id > 0
    AND main_sold_out_synced_at = ''
    AND main_sold_out_attempts < ?
    AND (main_sold_out_next_attempt_at = '' OR main_sold_out_next_attempt_at <= ?)
  ORDER BY sold_out_at ASC LIMIT 4`;

const soldOutCanal2Sql = `SELECT id, sold_out_at
  FROM offers INDEXED BY offers_soldout_canal2_due_v22
  WHERE status = 'sold_out'
    AND main_message_id > 0
    AND would_send_canal2 = 1
    AND canal2_message_id > 0
    AND canal2_sold_out_synced_at = ''
    AND canal2_sold_out_attempts < ?
    AND (canal2_sold_out_next_attempt_at = '' OR canal2_sold_out_next_attempt_at <= ?)
  ORDER BY sold_out_at ASC LIMIT 4`;

const restockMainSql = `SELECT id, restocked_at
  FROM offers INDEXED BY offers_restock_main_due_v22
  WHERE status = 'restocked_pending_sync'
    AND main_message_id > 0
    AND main_restock_synced_at = ''
    AND main_restock_attempts < ?
    AND (main_restock_next_attempt_at = '' OR main_restock_next_attempt_at <= ?)
  ORDER BY restocked_at ASC LIMIT 4`;

const restockCanal2Sql = `SELECT id, restocked_at
  FROM offers INDEXED BY offers_restock_canal2_due_v22
  WHERE status = 'restocked_pending_sync'
    AND would_send_canal2 = 1
    AND canal2_message_id > 0
    AND canal2_restock_synced_at = ''
    AND canal2_restock_attempts < ?
    AND (canal2_restock_next_attempt_at = '' OR canal2_restock_next_attempt_at <= ?)
  ORDER BY restocked_at ASC LIMIT 4`;

function oldSoldOutIds(database, onlyIds = []) {
  const onlyIdsClause = onlyIds.length
    ? `AND id IN (${onlyIds.map(() => "?").join(", ")})`
    : "";
  return database.prepare(
    `SELECT id FROM offers
     WHERE status = 'sold_out' AND main_message_id > 0
       AND (
         (main_sold_out_synced_at = '' AND main_sold_out_attempts < ? AND
          (main_sold_out_next_attempt_at = '' OR main_sold_out_next_attempt_at <= ?))
         OR
         (would_send_canal2 = 1 AND canal2_message_id > 0 AND
          canal2_sold_out_synced_at = '' AND canal2_sold_out_attempts < ? AND
          (canal2_sold_out_next_attempt_at = '' OR
           canal2_sold_out_next_attempt_at <= ?))
       )
       ${onlyIdsClause}
     ORDER BY sold_out_at ASC LIMIT 4`,
  ).all(MAX_ATTEMPTS, NOW, MAX_ATTEMPTS, NOW, ...onlyIds).map((row) => row.id);
}

function splitSoldOutIds(database) {
  return mergeIds([
    database.prepare(soldOutMainSql).all(MAX_ATTEMPTS, NOW),
    database.prepare(soldOutCanal2Sql).all(MAX_ATTEMPTS, NOW),
  ], "sold_out_at");
}

function oldRestockIds(database, canal2Enabled) {
  return database.prepare(
    `SELECT id FROM offers
     WHERE status = 'restocked_pending_sync'
       AND (
         (main_restock_synced_at = '' AND main_message_id > 0 AND
          main_restock_attempts < ? AND
          (main_restock_next_attempt_at = '' OR main_restock_next_attempt_at <= ?))
         OR
         (would_send_canal2 = 1 AND canal2_message_id > 0 AND
          canal2_restock_synced_at = '' AND canal2_restock_attempts < ? AND
          (canal2_restock_next_attempt_at = '' OR
           canal2_restock_next_attempt_at <= ?))
         OR
         (main_restock_synced_at <> '' AND
          (? = 0 OR would_send_canal2 = 0 OR canal2_message_id <= 0 OR
           canal2_restock_synced_at <> ''))
       )
     ORDER BY restocked_at ASC LIMIT 4`,
  ).all(MAX_ATTEMPTS, NOW, MAX_ATTEMPTS, NOW, canal2Enabled ? 1 : 0)
    .map((row) => row.id);
}

function splitRestockIds(database, canal2Enabled) {
  const finalizeSql = `SELECT id, restocked_at
    FROM offers INDEXED BY offers_restock_finalize_v22
    WHERE status = 'restocked_pending_sync'
      AND main_restock_synced_at <> ''
      ${canal2Enabled
        ? `AND (would_send_canal2 = 0 OR canal2_message_id <= 0 OR
             canal2_restock_synced_at <> '')`
        : ""}
    ORDER BY restocked_at ASC LIMIT 4`;
  return mergeIds([
    database.prepare(restockMainSql).all(MAX_ATTEMPTS, NOW),
    database.prepare(restockCanal2Sql).all(MAX_ATTEMPTS, NOW),
    database.prepare(finalizeSql).all(),
  ], "restocked_at");
}

test("v22 cria cinco índices parciais e todos os seletores evitam sort temporário", () => {
  const blocks = migrationBlocks();
  assert.equal(blocks.length, 22);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks.slice(0, 21)) database.exec(sql);
  assert.equal(Number(database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version), 21);
  database.exec(blocks[21]);
  assert.equal(Number(database.prepare(
    "SELECT MAX(id) AS version FROM _sql_schema_migrations",
  ).get().version), 22);

  const plans = new Map([
    ["offers_soldout_main_due_v22", plan(database, soldOutMainSql, MAX_ATTEMPTS, NOW)],
    ["offers_soldout_canal2_due_v22", plan(
      database,
      soldOutCanal2Sql,
      MAX_ATTEMPTS,
      NOW,
    )],
    ["offers_restock_main_due_v22", plan(database, restockMainSql, MAX_ATTEMPTS, NOW)],
    ["offers_restock_canal2_due_v22", plan(
      database,
      restockCanal2Sql,
      MAX_ATTEMPTS,
      NOW,
    )],
    ["offers_restock_finalize_v22", plan(
      database,
      `SELECT id, restocked_at FROM offers INDEXED BY offers_restock_finalize_v22
       WHERE status = 'restocked_pending_sync' AND main_restock_synced_at <> ''
       ORDER BY restocked_at ASC LIMIT 4`,
    )],
  ]);
  for (const [indexName, details] of plans) {
    assert.match(details, new RegExp(indexName));
    assert.doesNotMatch(details, /USE TEMP B-TREE/);
  }
  database.close();
});

test("v22 mantém seleção, ordem, dedupe, limite e caminho PK de onlyIds", () => {
  const database = databaseAtV22();
  const soldOutBase = {
    status: "sold_out",
    main_message_id: 101,
    would_send_canal2: 1,
    canal2_message_id: 202,
  };
  const soldOutRows = [
    ["sold-both-0", "2026-08-12T10:00:00.000Z", {}],
    ["sold-main-1", "2026-08-12T10:01:00.000Z", { canal2_sold_out_synced_at: NOW }],
    ["sold-canal2-2", "2026-08-12T10:02:00.000Z", { main_sold_out_synced_at: NOW }],
    ["sold-both-3", "2026-08-12T10:03:00.000Z", {}],
    ["sold-both-4", "2026-08-12T10:04:00.000Z", {}],
    ["sold-both-5", "2026-08-12T10:05:00.000Z", {}],
    ["sold-both-6", "2026-08-12T10:06:00.000Z", {}],
    ["sold-future", "2026-08-12T09:00:00.000Z", {
      main_sold_out_next_attempt_at: "2026-08-12T13:00:00.000Z",
      canal2_sold_out_next_attempt_at: "2026-08-12T13:00:00.000Z",
    }],
    ["sold-max", "2026-08-12T09:01:00.000Z", {
      main_sold_out_attempts: 10,
      canal2_sold_out_attempts: 10,
    }],
    ["sold-no-main-message", "2026-08-12T09:02:00.000Z", { main_message_id: 0 }],
  ];
  for (const [id, soldOutAt, overrides] of soldOutRows) {
    insertOffer(database, id, { ...soldOutBase, sold_out_at: soldOutAt, ...overrides });
  }
  assert.deepEqual(splitSoldOutIds(database), oldSoldOutIds(database));
  assert.deepEqual(splitSoldOutIds(database), [
    "sold-both-0", "sold-main-1", "sold-canal2-2", "sold-both-3",
  ]);

  const onlyIds = ["sold-canal2-2", "sold-both-5", "sold-future", "sold-no-main-message"];
  assert.deepEqual(oldSoldOutIds(database, onlyIds), ["sold-canal2-2", "sold-both-5"]);
  const onlyIdsPlan = plan(
    database,
    `SELECT id FROM offers WHERE status = 'sold_out' AND main_message_id > 0
       AND ((main_sold_out_synced_at = '' AND main_sold_out_attempts < ? AND
             (main_sold_out_next_attempt_at = '' OR main_sold_out_next_attempt_at <= ?))
         OR (would_send_canal2 = 1 AND canal2_message_id > 0 AND
             canal2_sold_out_synced_at = '' AND canal2_sold_out_attempts < ? AND
             (canal2_sold_out_next_attempt_at = '' OR
              canal2_sold_out_next_attempt_at <= ?)))
       AND id IN (?) ORDER BY sold_out_at ASC LIMIT 4`,
    MAX_ATTEMPTS,
    NOW,
    MAX_ATTEMPTS,
    NOW,
    onlyIds[0],
  );
  assert.match(onlyIdsPlan, /sqlite_autoindex_offers_1/);

  const restockBase = {
    status: "restocked_pending_sync",
    main_message_id: 101,
    would_send_canal2: 1,
    canal2_message_id: 202,
  };
  const restockRows = [
    ["restock-both-0", "2026-08-12T10:00:00.000Z", {}],
    ["restock-main-1", "2026-08-12T10:01:00.000Z", { canal2_restock_synced_at: NOW }],
    ["restock-canal2-2", "2026-08-12T10:02:00.000Z", { main_restock_synced_at: NOW }],
    ["restock-disabled-25", "2026-08-12T10:02:30.000Z", {
      main_restock_synced_at: NOW,
      canal2_restock_next_attempt_at: "2026-08-12T13:00:00.000Z",
    }],
    ["restock-final-3", "2026-08-12T10:03:00.000Z", {
      main_restock_synced_at: NOW,
      would_send_canal2: 0,
    }],
    ["restock-both-4", "2026-08-12T10:04:00.000Z", {}],
    ["restock-both-5", "2026-08-12T10:05:00.000Z", {}],
    ["restock-both-6", "2026-08-12T10:06:00.000Z", {}],
    ["restock-future", "2026-08-12T09:00:00.000Z", {
      main_restock_next_attempt_at: "2026-08-12T13:00:00.000Z",
      canal2_restock_next_attempt_at: "2026-08-12T13:00:00.000Z",
    }],
    ["restock-max", "2026-08-12T09:01:00.000Z", {
      main_restock_attempts: 10,
      canal2_restock_attempts: 10,
    }],
  ];
  for (const [id, restockedAt, overrides] of restockRows) {
    insertOffer(database, id, { ...restockBase, restocked_at: restockedAt, ...overrides });
  }
  for (const canal2Enabled of [true, false]) {
    assert.deepEqual(
      splitRestockIds(database, canal2Enabled),
      oldRestockIds(database, canal2Enabled),
    );
  }
  assert.deepEqual(splitRestockIds(database, true), [
    "restock-both-0", "restock-main-1", "restock-canal2-2", "restock-final-3",
  ]);
  assert.deepEqual(splitRestockIds(database, false), [
    "restock-both-0", "restock-main-1", "restock-canal2-2", "restock-disabled-25",
  ]);
  database.close();
});
