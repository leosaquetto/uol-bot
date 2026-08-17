import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

const workerSource = readFileSync(new URL("../src/worker.js", import.meta.url), "utf8");

function currentDatabase() {
  const start = workerSource.indexOf("  migrate() {");
  const end = workerSource.indexOf("\n  metadataValue(", start);
  const blocks = [...workerSource.slice(start, end)
    .matchAll(/this\.sqlExec\(`([\s\S]*?)`\);/g)]
    .map((match) => match[1]);
  const database = new DatabaseSync(":memory:");
  for (const sql of blocks) database.exec(sql);
  return database;
}

const dueUnknownSql = `SELECT id FROM offers
  WHERE main_sent_at = ''
    AND main_delivery_unknown_at > ''
    AND main_delivery_unknown_at <= ?
    AND main_delivery_attempts < ?
  ORDER BY main_delivery_unknown_at ASC
  LIMIT 100`;

test("polling busca somente entregas principais incertas pelo intervalo do índice", () => {
  const methodStart = workerSource.indexOf("  releaseExpiredMainUnknowns(");
  const methodEnd = workerSource.indexOf("\n  deliveryQueueSlo(", methodStart);
  const method = workerSource.slice(methodStart, methodEnd);
  assert.match(method, /main_delivery_unknown_at > ''/);
  assert.doesNotMatch(method, /main_delivery_unknown_at <> ''/);

  const database = currentDatabase();
  const insert = database.prepare(
    `INSERT INTO offers(
       id, link, preview_title, first_seen_at, last_seen_at, status,
       main_delivery_unknown_at, main_delivery_attempts
     ) VALUES (?, ?, ?, ?, ?, 'baseline', ?, ?)`,
  );
  for (let index = 0; index < 500; index += 1) {
    insert.run(
      `baseline-${index}`,
      `https://clube.uol.com.br/beneficios/baseline-${index}`,
      "baseline",
      "2026-08-01T00:00:00.000Z",
      "2026-08-01T00:00:00.000Z",
      "",
      0,
    );
  }
  insert.run(
    "unknown-due",
    "https://clube.uol.com.br/beneficios/unknown-due",
    "unknown",
    "2026-08-17T20:00:00.000Z",
    "2026-08-17T20:00:00.000Z",
    "2026-08-17T20:59:00.000Z",
    1,
  );
  insert.run(
    "unknown-future",
    "https://clube.uol.com.br/beneficios/unknown-future",
    "unknown",
    "2026-08-17T20:00:00.000Z",
    "2026-08-17T20:00:00.000Z",
    "2026-08-17T22:00:00.000Z",
    1,
  );

  const plan = database.prepare(`EXPLAIN QUERY PLAN ${dueUnknownSql}`)
    .all("2026-08-17T21:00:00.000Z", 10)
    .map((row) => row.detail)
    .join(" | ");
  assert.match(plan, /offers_main_unknown_due_idx/);
  assert.match(plan, /main_delivery_unknown_at>\? AND main_delivery_unknown_at<\?/);
  assert.deepEqual(
    database.prepare(dueUnknownSql)
      .all("2026-08-17T21:00:00.000Z", 10)
      .map((row) => row.id),
    ["unknown-due"],
  );
  database.close();
});
