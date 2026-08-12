import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";

const V22_INDEXES = [
  "offers_soldout_main_due_v22",
  "offers_soldout_canal2_due_v22",
  "offers_restock_main_due_v22",
  "offers_restock_canal2_due_v22",
  "offers_restock_finalize_v22",
];

it("migra v22 e executa seletores indexados e onlyIds no Workerd", async () => {
  const stub = env.UOL_TELEGRAM_SHADOW.getByName("v22-indexed-selectors");
  await runInDurableObject(stub, async (instance, state) => {
    expect(Number(state.storage.sql.exec(
      "SELECT MAX(id) AS version FROM _sql_schema_migrations",
    ).one().version)).toBe(22);
    expect(state.storage.sql.exec(
      `SELECT name FROM sqlite_schema
       WHERE type = 'index' AND name LIKE '%_v22' ORDER BY name`,
    ).toArray().map((row) => row.name)).toEqual([...V22_INDEXES].sort());

    instance.setMetadata("delivery_mode_override", "live");
    await expect(instance.processSoldOutSync(
      new Date("2026-08-12T12:00:00.000Z"),
    )).resolves.toEqual({
      mainEdited: 0,
      canal2Edited: 0,
      messageMissing: 0,
      failed: 0,
    });
    await expect(instance.processSoldOutSync(
      new Date("2026-08-12T12:00:00.000Z"),
      { onlyIds: ["missing-offer"] },
    )).resolves.toEqual({
      mainEdited: 0,
      canal2Edited: 0,
      messageMissing: 0,
      failed: 0,
    });
    await expect(instance.processRestockSync(
      new Date("2026-08-12T12:00:00.000Z"),
    )).resolves.toEqual({
      mainEdited: 0,
      canal2Edited: 0,
      mainReposted: 0,
      canal2Reposted: 0,
      failed: 0,
    });
  });
});
