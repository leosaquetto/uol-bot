import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Miniflare, Response as MFResponse } from "miniflare";
import { candidates, localDate, beeperUrl, CHAT_ID, MESSAGE } from "../src/api.js";

test("São Paulo midnight, baseline, copies and entry identity", () => {
  assert.equal(localDate(Date.parse("2026-09-16T02:59:59Z")), "2026-09-15");
  assert.equal(localDate(Date.parse("2026-09-16T03:00:00Z")), "2026-09-16");
  const now = Date.parse("2026-09-16T12:00:00Z");
  const w = { workout_entry_id: 1, occurred_at: "2026-09-16T11:00:00Z", title: "Smart Fit" };
  assert.equal(candidates([w, { ...w, id: 222 }, { ...w, title: "Corrida", workout_entry_id: 2 }], now, now - 7_200_000).length, 1);
  assert.equal(candidates([w], now, now - 1000).length, 0);
  assert.equal(candidates([{ ...w, title: "Treino", academia: { brand: "smart_fit" } }], now, 0).length, 1);
  assert.throws(() => candidates([{ ...w, workout_entry_id: null, id: 222 }], now, 0), /missing_entry_id/);
  assert.throws(() => beeperUrl("http://example.com"), /invalid_url/);
  assert.equal(decodeURIComponent(beeperUrl("https://beeper.example", true).pathname), `/v1/chats/${CHAT_ID}/messages`);
});

async function runtime(t, { enabled = true, sendStatus = 200, rotate = false, receiptStatus = "SUCCESS" } = {}) {
  const dir = await mkdtemp(join(tmpdir(), "gymrats-test-"));
  let sends = 0;
  let reads = 0;
  let fail = false;
  let instance;
  const options = {
    name: "gymrats-test",
    modules: true,
    modulesRules: [{ type: "ESModule", include: ["**/*.js"] }],
    scriptPath: resolve("test/fixture.js"),
    modulesRoot: process.cwd(),
    // Installed simulator supports August 6. Production remains September 16;
    // these tests prove logic/storage behavior, not September compatibility changes.
    compatibilityDate: "2026-08-06", compatibilityFlags: ["nodejs_compat"],
    durableObjects: { MONITOR: { className: "TestMonitor", useSQLite: true, unsafeUniqueKey: "gymrats-test-monitor" } },
    resourcePersistencePath: dir,
    kvNamespaces: { SESSION: "gymrats-test-session" },
    bindings: { ADMIN_TOKEN: "test-admin", DELIVERY_ENABLED: String(enabled), BEEPER_API_URL: "https://beeper.example", BEEPER_API_TOKEN: "test-beeper" },
    outboundService: async request => {
      const url = new URL(request.url);
      if (url.hostname === "www.gymrats.app") {
        reads++;
        assert.equal(request.headers.get("rat-timezone"), "America/Sao_Paulo");
        assert.match(url.searchParams.get("start_date"), /T00:00:00-03:00$/);
        if (fail) return new MFResponse("upstream", { status: 401 });
        const workout = { workout_entry_id: 11, title: "Smart Fit", occurred_at: new Date(Date.now() - 1000).toISOString() };
        return MFResponse.json({ data: rotate ? { workouts: [workout, workout], token: "rotated-token" } : [workout, workout] });
      }
      assert.equal(url.hostname, "beeper.example");
      assert.equal(request.headers.get("Authorization"), "Bearer test-beeper");
      if (url.pathname.endsWith("/messages/receipt-1")) return MFResponse.json({ id: "final-1", chatID: CHAT_ID, isSender: true, text: MESSAGE, sendStatus: { status: receiptStatus } });
      assert.equal(decodeURIComponent(url.pathname), `/v1/chats/${CHAT_ID}${request.method === "POST" ? "/messages" : ""}`);
      if (request.method === "GET") return MFResponse.json({ id: CHAT_ID });
      sends++;
      assert.deepEqual(await request.json(), { text: MESSAGE });
      assert.equal(request.headers.get("Idempotency-Key"), "gymrats:336445:11");
      return MFResponse.json({ chatID: CHAT_ID, pendingMessageID: "receipt-1" }, { status: sendStatus });
    },
  };
  const start = async () => { instance = new Miniflare(options); await instance.ready; };
  t.after(async () => { if (instance) await instance.dispose(); await rm(dir, { recursive: true, force: true }); });
  await start();
  const kv = await instance.getKVNamespace("SESSION");
  await kv.put("GYMRATS_JWT", "initial-token");
  const call = async (path, method = "GET") => instance.dispatchFetch(`http://localhost${path}`, { method, headers: { Authorization: "Bearer test-admin" } });
  return {
    call, kv, sends: () => sends, reads: () => reads, fail: () => { fail = true; },
    confirm: () => { receiptStatus = "SUCCESS"; },
    prepare: () => call("/test/prepare"),
    run: async () => (await call("/run", "POST")).json(),
    restart: async () => { await instance.dispose(); await start(); },
  };
}

test("runtime: concurrent runs, rotation, persistent dedupe and checkpoint", async t => {
  const r = await runtime(t, { rotate: true });
  await r.prepare();
  await Promise.all([r.run(), r.run()]);
  assert.equal(r.sends(), 1);
  assert.equal(r.reads(), 1);
  assert.equal(await r.kv.get("GYMRATS_JWT"), "rotated-token");
  assert.equal(await r.kv.get("LAST_NOTIFIED_WORKOUT_ENTRY_ID"), "11");
  await r.restart();
  await r.prepare();
  assert.equal((await r.run()).state, "ok");
  assert.equal(r.sends(), 1);
});

test("runtime: ambiguous POST is never retried after restart", async t => {
  const r = await runtime(t, { sendStatus: 500 });
  await r.prepare();
  assert.equal((await r.run()).state, "delivery_requires_review");
  await r.restart();
  await r.prepare();
  assert.equal((await r.run()).state, "ok");
  assert.equal(r.sends(), 1);
  assert.equal(await (await r.call("/status")).json().then(s => s.deliveries[0].state), "uncertain");
});

test("runtime: first-run baseline, disabled delivery and API rejection", async t => {
  const r = await runtime(t, { enabled: false });
  const first = await r.run();
  assert.equal(first.matches, 0);
  await r.prepare();
  assert.equal((await r.run()).state, "monitoring_delivery_disabled");
  assert.equal(r.sends(), 0);
  r.fail();
  await r.prepare();
  assert.equal((await r.run()).code, "gymrats_http_401");
});

test("runtime: pending receipt never checkpoints or resends before bridge confirmation", async t => {
  const r = await runtime(t, { receiptStatus: "PENDING" });
  await r.prepare();
  assert.equal((await r.run()).state, "delivery_pending");
  assert.equal(await r.kv.get("LAST_NOTIFIED_WORKOUT_ENTRY_ID"), null);
  r.confirm();
  await r.prepare();
  assert.equal((await r.run()).accepted, 1);
  assert.equal(r.sends(), 1);
  assert.equal(await r.kv.get("LAST_NOTIFIED_WORKOUT_ENTRY_ID"), "11");
});
