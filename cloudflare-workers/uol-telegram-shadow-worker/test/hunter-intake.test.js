import assert from "node:assert/strict";
import test from "node:test";
import { authenticateHunter, HunterIntakeQueue, normalizeHunterObservation, publicHunterError, readHunterIntakeJson } from "../src/hunter-intake.js";
const NOW = Date.parse("2026-10-17T18:00:00Z");
const uuid = n => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const portal = (n = 1, fields = {}) => ({ eventId: uuid(n), source: "portal", observedAt: new Date(NOW).toISOString(),
  payload: { link: `https://clube.uol.com.br/campanhasdeingresso/pAB-campaign-${n}`, title: "Peça", ...fields } });
const story = (n = 1) => ({ eventId: uuid(n), source: "instagram", observedAt: new Date(NOW).toISOString(), payload: {
  storyId: "4004185955500703427", link: "https://clube.uol.com.br/campanhasdeingresso/pAB-campaign",
  imageUrl: "https://scontent.cdninstagram.com/story.jpg", imageWidth: 1080, imageHeight: 1920,
  publishedAt: "2026-10-17T17:00:00Z", expiresAt: "2026-10-18T17:00:00Z" } });
const batch = (...observations) => ({ observations });
function fixture(options = {}) {
  let raw = "", clock = NOW, allowed = true, writes = 0;
  const calls = [];
  const settings = { readState: () => raw, writeState: value => { raw = value; writes++; }, now: () => clock,
    budgetAllowed: () => allowed,
    processPortal: async payload => { calls.push(payload.link); return { persisted: true }; },
    processStory: async payload => { calls.push(payload.storyId); return { status: "delivered" }; }, ...options };
  return { queue: new HunterIntakeQueue(settings), settings, calls, get state() { return raw && JSON.parse(raw); },
    get raw() { return raw; }, get writes() { return writes; }, setNow: v => { clock = v; }, setBudget: v => { allowed = v; },
    setState: v => { raw = JSON.stringify(v); } };
}
async function hash(v) { return Buffer.from(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(v))).toString("hex"); }

test("h1 authentication is per-device, revocable and hashed", async () => {
  const token = `h1.test-phone.${"a".repeat(64)}`, secretHash = await hash(token);
  const req = v => new Request("https://hunter.test", { headers: { Authorization: v } });
  const env = v => ({ HUNTER_INTAKE_CLIENTS: JSON.stringify(v) });
  assert.equal(await authenticateHunter(req(`Bearer ${token}`), env({ "test-phone": { enabled: true, secretHash } })), "test-phone");
  for (const clients of [{ "test-phone": { enabled: false, secretHash } }, { other: secretHash }, { "test-phone": "b".repeat(64) }])
    assert.equal(await authenticateHunter(req(`Bearer ${token}`), env(clients)), "");
  for (const auth of [token, `bearer ${token}`, `Bearer h1.UPPER.${"a".repeat(64)}`, `Bearer ${token}x`])
    assert.equal(await authenticateHunter(req(auth), env({ "test-phone": secretHash })), "");
  assert.equal(await authenticateHunter(req(`Bearer ${token}`), { HUNTER_INTAKE_CLIENTS: "broken" }), "");
});

test("strict public fields and exact long Story ID", () => {
  assert.equal(normalizeHunterObservation(story(), NOW).payload.storyId, "4004185955500703427");
  for (const v of [{ ...portal(), cookie: "private" }, portal(1, { cookie: "private" }), portal(1, { title: "<script>x</script>" }),
    portal(1, { imageUrl: "https://evil.test/image.jpg" }), portal(1, { link: `${portal().payload.link}?sessionid=private` }),
    { ...story(), payload: { ...story().payload, imageBase64: "private" } },
    { ...story(), payload: { ...story().payload, imageUrl: "https://scontent.cdninstagram.com/story.jpg?sessionid=secret" } },
    { ...story(), payload: { ...story().payload, storyId: 4004185955500703427 } },
    { ...portal(), observedAt: "2026-10-18T00:00:00Z" }, { ...portal(), eventId: "bad" }])
    assert.throws(() => normalizeHunterObservation(v, NOW));
  assert.equal(normalizeHunterObservation(portal(1, { imageUrl: "https://d310rdc9v8uuq.cloudfront.net/poster.jpg" }), NOW).payload.imageUrl,
    "https://d310rdc9v8uuq.cloudfront.net/poster.jpg");
});

test("streaming undeclared body cancels at 64KiB before processing", async () => {
  let cancelled = false, pulls = 0;
  const stream = new ReadableStream({ pull(c) { pulls++; c.enqueue(new Uint8Array(33000)); }, cancel() { cancelled = true; } });
  await assert.rejects(readHunterIntakeJson(new Request("https://hunter.test", { method: "POST", body: stream, duplex: "half",
    headers: { "Content-Type": "application/json" } })), { message: "hunter_payload_too_large", httpStatus: 413 });
  assert.equal(cancelled, true); assert.ok(pulls <= 3);
  await assert.rejects(readHunterIntakeJson(new Request("https://hunter.test", { method: "POST", body: "{}" })), { httpStatus: 415 });
  await assert.rejects(readHunterIntakeJson(new Request("https://hunter.test", { method: "POST", body: "{", headers: { "Content-Type": "application/json" } })), { message: "hunter_invalid_json" });
  assert.deepEqual(await readHunterIntakeJson(new Request("https://hunter.test", { method: "POST", body: JSON.stringify(batch(portal())), headers: { "Content-Type": "application/json" } })), batch(portal()));
});

test("batch validation/conflict is atomic; duplicate inside batch reuses identity", async () => {
  const f = fixture();
  await assert.rejects(f.queue.accept("phone", batch(...Array.from({ length: 9 }, (_, i) => portal(i)))), { message: "hunter_batch_invalid" });
  await assert.rejects(f.queue.accept("phone", batch(portal(), portal(2, { cookie: "secret" }))), { message: "hunter_portal_payload_invalid" });
  assert.equal(f.writes, 0);
  const accepted = await f.queue.accept("phone", batch(portal(), portal()));
  assert.deepEqual(accepted.receipts.map(r => r.duplicate), [false, true]);
  assert.equal(f.state.pending.length, 1); assert.equal(f.calls.length, 0);
  const before = f.raw;
  await assert.rejects(f.queue.accept("phone", batch(portal(2), portal(1, { title: "changed" }))), { message: "hunter_identity_conflict" });
  assert.equal(f.raw, before);
});

test("restart/lost response preserves receipts and ownership even while budget blocked", async () => {
  const f = fixture(); await f.queue.accept("phone", batch(portal()));
  const restarted = new HunterIntakeQueue(f.settings); f.setBudget(false);
  assert.equal((await restarted.accept("phone", batch(portal()))).receipts[0].duplicate, true);
  await assert.rejects(restarted.accept("other", batch(portal())), { httpStatus: 409 });
  assert.throws(() => restarted.status("other", uuid(1)), { httpStatus: 404 });
  assert.equal(restarted.status("phone", uuid(1)).status, "queued");
  await assert.rejects(restarted.accept("phone", batch(portal(2))), { message: "hunter_budget_guard" });
  f.setBudget(true); await restarted.drain();
  assert.equal(restarted.status("phone", uuid(1)).status, "registered");
  assert.equal((await restarted.accept("phone", batch(portal()))).receipts[0].duplicate, true);
  await restarted.drain(); assert.equal(f.calls.length, 1);
});

test("simultaneous admission cannot exceed 32 pending", async () => {
  const f = fixture();
  await Promise.all(Array.from({ length: 4 }, (_, page) => f.queue.accept("phone", batch(...Array.from({ length: 8 }, (_, i) => portal(page * 8 + i))))));
  assert.equal(f.state.pending.length, 32); const before = f.raw;
  await assert.rejects(f.queue.accept("phone", batch(portal(40))), { message: "hunter_capacity" }); assert.equal(f.raw, before);
});

test("revocation during async preparation denies admission before any write", async () => {
  const f = fixture();
  let authorized = true, checks = 0;
  const accepting = f.queue.accept("phone", batch(portal()), () => { checks++; return authorized; });
  authorized = false;
  await assert.rejects(accepting, { message: "hunter_client_invalid", httpStatus: 401 });
  assert.equal(checks, 1);
  assert.equal(f.writes, 0);
  assert.equal(f.raw, "");
  authorized = true;
  assert.equal((await f.queue.accept("phone", batch(portal()), () => authorized)).persisted, true);
});

test("authorization is checked after serialized admission wait, including duplicate receipts", async () => {
  const f = fixture();
  await f.queue.accept("phone", batch(portal()));
  const before = f.raw, writes = f.writes;
  let release, queued, authorized = true;
  const waiting = new Promise(resolve => { release = resolve; });
  const admittedToWait = new Promise(resolve => { queued = resolve; });
  f.queue.admission = { then(callback) { queued(); return waiting.then(callback); } };
  const accepting = f.queue.accept("phone", batch(portal()), () => authorized);
  await admittedToWait;
  authorized = false;
  release();
  await assert.rejects(accepting, { message: "hunter_client_invalid", httpStatus: 401 });
  assert.equal(f.raw, before);
  assert.equal(f.writes, writes);
});

test("256KiB aggregate reserves progress bytes; input also respects 64KiB", async () => {
  const f = fixture(), rich = n => portal(n, { description: "a".repeat(8192), validity: "b".repeat(500), title: "c".repeat(280) });
  for (let page = 0; page < 3; page++) await f.queue.accept("phone", batch(...Array.from({ length: 7 }, (_, i) => rich(page * 7 + i))));
  const before = f.raw;
  await assert.rejects(f.queue.accept("phone", batch(...Array.from({ length: 7 }, (_, i) => rich(30 + i)))), { message: "hunter_capacity" });
  assert.equal(f.raw, before); assert.ok(Buffer.byteLength(f.raw) < 256 * 1024);
  await f.queue.drain(); assert.ok(Buffer.byteLength(f.raw) < 256 * 1024);
  await assert.rejects(f.queue.accept("phone", batch(...Array.from({ length: 8 }, (_, i) => rich(50 + i)))), { message: "hunter_payload_too_large" });
});

test("drain reserves before I/O, blocks overlap, merges new admissions", async () => {
  let entered, release; const started = new Promise(resolve => { entered = resolve; });
  const f = fixture({ processPortal: async () => { entered(); await new Promise(resolve => { release = resolve; }); return { persisted: true }; } });
  await f.queue.accept("phone", batch(portal())); const draining = f.queue.drain(); await started;
  assert.equal(f.state.pending[0].status, "processing"); assert.equal(f.state.pending[0].attempts, 1);
  assert.equal((await f.queue.drain()).status, "busy"); await f.queue.accept("phone", batch(portal(2)));
  f.setBudget(false); release(); await draining;
  assert.equal(f.state.recent[0].id, uuid(1)); assert.equal(f.state.pending[0].id, uuid(2));
});

test("one pass processes at most two observations and one Story", async () => {
  const f = fixture(); await f.queue.accept("phone", batch(story(1), story(2), portal(3), portal(4)));
  assert.equal((await f.queue.drain()).processed, 2);
  assert.equal(f.calls.filter(v => v === story().payload.storyId).length, 1);
  assert.deepEqual(f.state.pending.map(v => v.id), [uuid(2), uuid(4)]);
});

test("held Story remains queued and continues after phone closes", async () => {
  let resumed = 0, ingested = 0;
  const f = fixture({ resumeStory: async () => ++resumed > 1 ? { status: "delivered" } : null,
    processStory: async () => { ingested++; return { status: "held", expiresAt: story().payload.expiresAt,
      targets: { main: { status: "held", error: "bot_not_live", nextAttemptAt: new Date(NOW + 900000).toISOString() } } }; } });
  await f.queue.accept("phone", batch(story())); await f.queue.drain();
  assert.equal(f.state.pending[0].status, "queued"); assert.equal(f.state.pending[0].nextAt, NOW + 900000);
  f.setNow(NOW + 900001); await new HunterIntakeQueue(f.settings).drain();
  assert.equal(f.queue.status("phone", uuid(1)).status, "delivered"); assert.equal(ingested, 1);
});

test("expired uncertain Story retains payload and identity without fresh ingestion", async () => {
  const f = fixture({ processStory: async () => { throw new Error("must not ingest"); }, resumeStory: async () => ({ status: "unknown",
    expiresAt: story().payload.expiresAt, targets: { main: { status: "unknown" }, beeper: { status: "reconciling" } } }) });
  await f.queue.accept("phone", batch(story())); f.setNow(NOW + 2 * 86400000); await f.queue.drain();
  assert.equal(f.state.pending[0].status, "needs_reconciliation");
  assert.equal(f.state.pending[0].event.payload.storyId, story().payload.storyId);
  assert.match(f.state.pending[0].canonicalKey, /^instagram:/); assert.equal(f.state.recent.length, 0);
});

test("expired offline replay absent from inbox records expiry without dispatch", async () => {
  const f = fixture({ processStory: async () => { throw new Error("must not ingest"); } });
  f.setNow(NOW + 2 * 86400000); await f.queue.accept("phone", batch(story())); await f.queue.drain();
  assert.equal(f.queue.status("phone", uuid(1)).status, "expired");
});

test("a confirmed unavailable Portal page keeps a source-specific receipt", async () => {
  const f = fixture({ processPortal: async () => { throw Object.assign(new Error("campaign unavailable"), { httpStatus: 410 }); } });
  await f.queue.accept("phone", batch(portal())); await f.queue.drain();
  const receipt = f.queue.status("phone", uuid(1));
  assert.equal(receipt.status, "expired");
  assert.equal(receipt.error, "hunter_campaign_unavailable");
});

test("public errors survive RPC but arbitrary error messages remain private", () => {
  assert.deepEqual(publicHunterError(new Error("hunter_capacity")), { error: "hunter_capacity", httpStatus: 429 });
  assert.deepEqual(publicHunterError(new Error("instagram_story_id_invalid")), { error: "hunter_story_payload_invalid", httpStatus: 400 });
  for (const v of ["signed secret url", "hunter_super_private_value", "instagram_cookies_secret"])
    assert.deepEqual(publicHunterError(new Error(v)), { error: "hunter_unavailable", httpStatus: 503 });
});
