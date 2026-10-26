import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { sendBeeperOffer } from "../src/beeper.js";
import { HunterIntakeQueue } from "../src/hunter-intake.js";
import {
  InstagramStoryInbox,
  initializeInstagramStorySchema,
  normalizeInstagramStory,
  readInstagramStoryJson,
  validateInstagramCampaignLink,
  validateInstagramImageUrl,
  uploadInstagramDiscordPhoto,
} from "../src/instagram-story.js";

const NOW = new Date("2026-10-09T20:00:00Z");
const STORY = {
  storyId: "4004185955500703427",
  publishedAt: "2026-10-09T14:29:43Z",
  expiresAt: "2026-10-10T14:29:43Z",
  link: "https://clube.uol.com.br/campanhasdeingresso/pQg-2-ingressos-11-10-teatro-sp",
  imageUrl: "https://scontent.cdninstagram.com/story.jpg?private_signed_query=secret",
  imageWidth: 1080, imageHeight: 1920,
};

function fixture(overrides = {}) {
  const database = new DatabaseSync(":memory:");
  const sql = (query, ...bindings) => {
    if (!bindings.length) {
      if (/^\s*(CREATE|DELETE|INSERT|UPDATE)/i.test(query)) {
        database.exec(query);
        return { toArray: () => [] };
      }
    }
    const statement = database.prepare(query);
    const rows = statement.columns().length ? statement.all(...bindings) : (statement.run(...bindings), []);
    return { toArray: () => rows };
  };
  database.exec("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)");
  initializeInstagramStorySchema(sql);
  const calls = [];
  const transports = Object.fromEntries(["main", "canal2", "discord", "beeper"].map(target => [target, async row => {
    calls.push({ target, storyId: row.story_id, image: target === "beeper" ? row.discord_image_proxy_url : row.image_url });
    return { messageId: `${target}-${row.story_id}`, imageConfirmed: target === "beeper", imageProxyUrl: "https://media.discordapp.net/external/image.jpg" };
  }]));
  transports.discordProxy = async () => "https://media.discordapp.net/external/image.jpg";
  Object.assign(transports, overrides.transports);
  let instant = NOW;
  const options = {
    transports, now: () => instant,
    mode: () => "live",
    readiness: () => ({ main: true, canal2: true, discord: true, beeper: true }),
    ...overrides,
  };
  const inbox = new InstagramStoryInbox(sql, {}, options);
  return { database, sql, calls, inbox, transports, options, setNow: value => { instant = value; } };
}

function seedRetentionRow(database, key, { changeTargets, rawTargets, ...overrides } = {}) {
  const targets = Object.fromEntries(["main", "canal2", "discord", "beeper"].map(target => [target, {
    status: "confirmed", attempts: 1, messageId: `${target}-receipt`,
    nextAttemptAt: "", error: "", imageConfirmed: true,
  }]));
  changeTargets?.(targets);
  const row = {
    canonical_key: key, story_id: "4004185955500703000", link: STORY.link,
    published_at: "2026-10-05T19:00:00.000Z", expires_at: "2026-10-06T19:00:00.000Z",
    first_seen_at: "2026-10-05T20:00:00.000Z", updated_at: "2026-10-05T20:00:00.000Z",
    image_url: STORY.imageUrl, image_width: 1080, image_height: 1920,
    discord_image_proxy_url: "https://media.discordapp.net/external/image.jpg",
    targets_json: rawTargets ?? JSON.stringify(targets), ...overrides,
  };
  const columns = Object.keys(row);
  database.prepare(`INSERT INTO instagram_story_outbox (${columns.join(", ")})
    VALUES (${columns.map(() => "?").join(", ")})`).run(...Object.values(row));
}

test("accepts ticket campaign only and preserves the long Story ID as a string", () => {
  assert.equal(normalizeInstagramStory(STORY, NOW).storyId, STORY.storyId);
  assert.equal(normalizeInstagramStory({ ...STORY, publishedAt: 1791556183, expiresAt: 1791642583 }, NOW).storyId, STORY.storyId);
  assert.throws(() => validateInstagramCampaignLink("https://clube.uol.com.br/fotoregistro/pO8-presente"));
  assert.throws(() => normalizeInstagramStory({ ...STORY, storyId: Number(STORY.storyId) }, NOW));
});

test("rejects credentials, ports, query, fragments, deceptive hosts and subroutes in ticket links", () => {
  for (const link of [
    "http://clube.uol.com.br/campanhasdeingresso/pQg-ticket",
    "https://user:pass@clube.uol.com.br/campanhasdeingresso/pQg-ticket",
    "https://clube.uol.com.br:443/campanhasdeingresso/pQg-ticket",
    "https://clube.uol.com.br.evil.test/campanhasdeingresso/pQg-ticket",
    "https://clube.uol.com.br/campanhasdeingresso/pQg-ticket?fbclid=anything",
    "https://clube.uol.com.br/campanhasdeingresso/pQg-ticket#private",
    "https://clube.uol.com.br/campanhasdeingresso/pQg-ticket/redeem",
    "https://clube.uol.com.br/campanhasdeingresso/pQg-%2Fticket",
  ]) assert.throws(() => validateInstagramCampaignLink(link), link);
});

test("accepts Instagram image CDN subdomains and rejects foreign hosts and unsafe media", () => {
  for (const url of [STORY.imageUrl, "https://scontent-1.xx.fbcdn.net/path/story.webp?sig=private"]) {
    assert.equal(validateInstagramImageUrl(url), url);
  }
  for (const url of [
    "https://cdninstagram.com/story.jpg", "https://cdninstagram.com.evil.test/story.jpg",
    "https://scontent.cdninstagram.com:443/story.jpg", "https://user@scontent.fbcdn.net/story.jpg",
    "https://scontent.fbcdn.net/story.mp4", "https://127.0.0.1/story.jpg",
    "https://@scontent.fbcdn.net/story.jpg", "https://scontent.fbcdn.net/story.jpg\n?sig=value",
    "https://scontent.fbcdn.net/story.jpg#fragment",
  ]) assert.throws(() => validateInstagramImageUrl(url), url);
});

test("rejects future, expired or oversized-duration Stories and invalid image dimensions", async () => {
  assert.throws(() => normalizeInstagramStory({ ...STORY, publishedAt: "2026-10-10T00:00:00Z" }, NOW));
  assert.throws(() => normalizeInstagramStory({ ...STORY, expiresAt: "2026-10-11T00:00:00Z" }, NOW));
  assert.throws(() => normalizeInstagramStory({ ...STORY, imageWidth: 0 }, NOW));
  assert.throws(() => normalizeInstagramStory({ ...STORY, imageWidth: 1 }, NOW));
  const { inbox } = fixture();
  await assert.rejects(inbox.ingest({ ...STORY, expiresAt: "2026-10-09T19:59:59Z" }), /instagram_story_expired/);
});

test("bounded JSON reader rejects oversized undeclared streams before parsing", async () => {
  const request = new Request("https://worker.test/ingest-instagram-story", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: "x".repeat(6 * 1024 * 1024),
  });
  await assert.rejects(readInstagramStoryJson(request), /instagram_payload_too_large/);
  await assert.rejects(readInstagramStoryJson(new Request("https://worker.test/", { method: "POST", body: "{}" })), /instagram_content_type_invalid/);
});

test("all four targets confirm once; repeated ingestion and status never expose signed media", async () => {
  const { inbox, calls } = fixture();
  const first = await inbox.ingest(STORY);
  assert.equal(first.status, "delivered");
  assert.deepEqual(calls.map(call => call.target), ["discord", "main", "canal2", "beeper"]);
  assert.equal(calls.at(-1).image, "https://media.discordapp.net/external/image.jpg");
  assert.equal((await inbox.ingest({ ...STORY, imageUrl: STORY.imageUrl.replace("secret", "refreshed") })).status, "delivered");
  assert.equal(calls.length, 4);
  assert.equal(inbox.status(STORY.storyId).stories.length, 1);
  assert.equal(JSON.stringify(inbox.status()).includes("private_signed_query"), false);
  assert.equal(JSON.stringify(first).includes("discordapp"), false);
});

test("each WhatsApp exclusion keeps the other three destinations and settles across restart", async () => {
  for (const pattern of ["teatro-tuca", "morumbi-park-sp", "ecovilla", "teatro-liberdade",
    "teatro-multiplan", "teatro-das-artes", "museu-da-imaginacao-sp", "teatro-vanucci-rj", "teatro-riachuelo-rj"]) {
    const sample = fixture({ readiness: () => ({ main: true, canal2: true, discord: true, beeper: false }) });
    const story = { ...STORY, link: `${STORY.link}-${pattern.toUpperCase()}-extra` };
    const result = await sample.inbox.ingest(story);
    assert.equal(result.status, "delivered", pattern);
    assert.deepEqual(sample.calls.map(call => call.target), ["discord", "main", "canal2"]);
    assert.deepEqual(result.targets.beeper, { status: "skipped", attempts: 0, nextAttemptAt: "",
      messageId: "", error: "whatsapp_link_excluded", imageConfirmed: false });
    const stored = sample.database.prepare("SELECT * FROM instagram_story_outbox").get();
    assert.equal(stored.beeper_payload_json, "");
    assert.equal((await sample.inbox.ingest(story)).status, "delivered");
    const restarted = new InstagramStoryInbox(sample.sql, {}, sample.options);
    assert.equal((await restarted.resume(result.canonicalKey)).status, "delivered");
    assert.equal(sample.calls.length, 3);
    sample.database.close();
  }
});

test("excluded WhatsApp does not wait for a Discord proxy or consume a delivery attempt", async () => {
  let proxyCalls = 0;
  const sample = fixture();
  sample.transports.discord = async () => ({ messageId: "discord-receipt", imageProxyUrl: "" });
  sample.transports.discordProxy = async () => { proxyCalls++; return ""; };
  const result = await sample.inbox.ingest({ ...STORY, link: `${STORY.link}-ecovilla` });
  assert.equal(result.targets.beeper.status, "skipped");
  assert.equal(result.targets.beeper.attempts, 0);
  assert.equal(result.targets.main.status, "confirmed");
  assert.equal(result.targets.canal2.status, "confirmed");
  assert.equal(proxyCalls, 0);
  assert.equal(sample.calls.some(call => call.target === "beeper"), false);
  sample.database.close();
});

test("skipped WhatsApp preserves held and expired aggregate states for the remaining destinations", async () => {
  const sample = fixture({ mode: () => "shadow" });
  const result = await sample.inbox.ingest({ ...STORY, link: `${STORY.link}-ecovilla` });
  assert.equal(result.status, "held");
  assert.equal(result.targets.beeper.status, "skipped");
  assert.equal(sample.calls.length, 0);
  sample.setNow(new Date("2026-10-11T14:29:43Z"));
  const expired = await sample.inbox.resume(result.canonicalKey);
  assert.equal(expired.status, "expired");
  assert.equal(expired.targets.beeper.status, "skipped");
  assert.equal(sample.calls.length, 0);
  sample.database.close();
});

test("exclusions preserve previous confirmed and uncertain WhatsApp evidence without replay", async () => {
  for (const status of ["confirmed", "unknown", "in_flight", "reconciling"]) {
    const sample = fixture();
    const frozen = JSON.stringify({ offer: { link: `${STORY.link}-teatro-tuca` }, idempotencyKey: "original-key" });
    seedRetentionRow(sample.database, "excluded-evidence", { link: `${STORY.link}-teatro-tuca`,
      expires_at: "2026-10-10T14:29:43.000Z", beeper_payload_json: frozen,
      changeTargets: targets => Object.assign(targets.beeper, { status, attempts: 2,
        messageId: status === "confirmed" ? "previous-receipt" : "", imageConfirmed: status === "confirmed",
        error: status === "unknown" ? "upstream_http_503" : "" }),
    });
    const result = await sample.inbox.resume("excluded-evidence");
    assert.equal(result.targets.beeper.status, status === "in_flight" ? "reconciling" : status);
    assert.equal(result.targets.beeper.attempts, 2);
    assert.equal(result.targets.beeper.messageId, status === "confirmed" ? "previous-receipt" : "");
    assert.equal(sample.database.prepare("SELECT beeper_payload_json FROM instagram_story_outbox").get().beeper_payload_json, frozen);
    assert.equal(sample.calls.length, 0);
    sample.database.close();
  }
});

test("exclusions settle existing safe failures without replacing other destination receipts", async () => {
  for (const status of ["pending", "held", "failed_safe", "expired"]) {
    const sample = fixture();
    seedRetentionRow(sample.database, "excluded-pending", { link: `${STORY.link}-ecovilla`,
      expires_at: "2026-10-10T14:29:43.000Z",
      changeTargets: targets => Object.assign(targets.beeper, { status, attempts: 2,
        messageId: "", imageConfirmed: false, error: "upstream_http_429", nextAttemptAt: "2026-10-10T14:00:00Z" }),
    });
    const result = await sample.inbox.resume("excluded-pending");
    assert.equal(result.status, "delivered");
    assert.equal(result.targets.beeper.status, "skipped");
    assert.equal(result.targets.beeper.attempts, 2);
    assert.equal(result.targets.beeper.nextAttemptAt, "");
    assert.equal(result.targets.main.messageId, "main-receipt");
    assert.equal(sample.calls.length, 0);
    sample.database.close();
  }
});

test("Hunter treats skipped WhatsApp as terminal while preserving uncertainty at another destination", async () => {
  let raw = "";
  const targets = Object.fromEntries(["main", "canal2", "discord", "beeper"].map(target => [target, {
    status: target === "main" ? "unknown" : target === "beeper" ? "skipped" : "confirmed",
    imageConfirmed: ["canal2", "discord"].includes(target), error: "", nextAttemptAt: "",
  }]));
  const queue = new HunterIntakeQueue({ readState: () => raw, writeState: value => { raw = value; },
    now: () => NOW.getTime(), budgetAllowed: () => true,
    processStory: async () => ({ status: "unknown", expiresAt: STORY.expiresAt, targets }),
  });
  const eventId = "00000000-0000-4000-8000-000000000001";
  await queue.accept("phone", { observations: [{ eventId, source: "instagram", observedAt: NOW.toISOString(),
    payload: { ...STORY, link: `${STORY.link}-ecovilla` } }] });
  await queue.drain();
  assert.equal(queue.status("phone", eventId).status, "needs_reconciliation");
});

test("same benefit in another Story remains eligible independent of offers deduplication", async () => {
  const { inbox, calls, database } = fixture();
  await inbox.ingest(STORY);
  await inbox.ingest({ ...STORY, storyId: "4004185955500703428" });
  assert.equal(calls.length, 8);
  assert.equal(database.prepare("SELECT COUNT(*) AS count FROM instagram_story_outbox").get().count, 2);
  assert.equal(database.prepare("SELECT name FROM sqlite_schema WHERE name = 'offers'").get(), undefined);
});

test("429 respects upstream retry time and skips already confirmed destinations", async () => {
  let attempts = 0;
  const sample = fixture();
  sample.transports.main = async () => {
    attempts++;
    if (attempts === 1) throw Object.assign(new Error("signed URL must never leak"), { httpStatus: 429, retryAfterSeconds: 600 });
    return { messageId: "main-confirmed" };
  };
  const first = await sample.inbox.ingest(STORY);
  assert.equal(first.targets.main.status, "failed_safe");
  assert.ok(Date.parse(first.targets.main.nextAttemptAt) >= NOW.getTime() + 600_000);
  await sample.inbox.ingest(STORY);
  assert.equal(attempts, 1);
  sample.setNow(new Date(NOW.getTime() + 700_000));
  const last = await sample.inbox.ingest(STORY);
  assert.equal(last.status, "delivered");
  assert.equal(attempts, 2);
  assert.equal(sample.calls.filter(call => call.target === "discord").length, 1);
  assert.equal(sample.calls.filter(call => call.target === "beeper").length, 1);
  assert.equal(JSON.stringify(first).includes("signed URL"), false);
});

test("ambiguous mutations become unknown and never resend, even after reconstruction", async () => {
  const sample = fixture();
  let attempts = 0;
  sample.transports.main = async () => { attempts++; throw Object.assign(new Error("private-image-url"), { ambiguous: true }); };
  const first = await sample.inbox.ingest(STORY);
  assert.equal(first.targets.main.status, "unknown");
  const restarted = new InstagramStoryInbox(sample.sql, {}, sample.options);
  assert.equal((await restarted.ingest(STORY)).targets.main.status, "unknown");
  assert.equal(attempts, 1);
  assert.equal(JSON.stringify(first).includes("private-image-url"), false);
});

test("persisted in-flight reservation without live owner becomes unknown without sending", async () => {
  const sample = fixture();
  await sample.inbox.ingest(STORY);
  const row = sample.database.prepare("SELECT * FROM instagram_story_outbox").get();
  const targets = JSON.parse(row.targets_json);
  targets.main = { status: "in_flight", attempts: 1, nextAttemptAt: "", messageId: "", error: "" };
  sample.database.prepare("UPDATE instagram_story_outbox SET targets_json = ?").run(JSON.stringify(targets));
  const restarted = new InstagramStoryInbox(sample.sql, {}, sample.options);
  const result = await restarted.ingest(STORY);
  assert.equal(result.targets.main.status, "unknown");
  assert.equal(sample.calls.length, 4);
});

test("internal resume after expiry preserves ambiguous reservations and expires only unsent targets", async () => {
  const sample = fixture();
  seedRetentionRow(sample.database, "expired-recovery", {
    beeper_payload_json: JSON.stringify({ idempotencyKey: "frozen-original-key" }),
    changeTargets: targets => {
      targets.main = { status: "in_flight", attempts: 1, messageId: "", nextAttemptAt: "", error: "" };
      targets.canal2 = { status: "pending", attempts: 0, messageId: "", nextAttemptAt: "", error: "" };
      targets.beeper = { status: "in_flight", attempts: 1, messageId: "", nextAttemptAt: "", error: "" };
    },
  });
  const restarted = new InstagramStoryInbox(sample.sql, {}, sample.options);
  const result = await restarted.resume("expired-recovery");
  assert.equal(result.targets.main.status, "unknown");
  assert.equal(result.targets.canal2.status, "expired");
  assert.equal(result.targets.beeper.status, "reconciling");
  assert.equal(result.targets.beeper.attempts, 1);
  assert.equal(sample.calls.length, 0);
  const stored = sample.database.prepare("SELECT * FROM instagram_story_outbox WHERE canonical_key = ?").get("expired-recovery");
  assert.match(stored.beeper_payload_json, /frozen-original-key/);
  assert.equal(stored.story_id, "4004185955500703000");
  assert.equal(await restarted.resume("absent"), null);
});

test("two phones and Oracle converge on one durable Story and four destination messages", async () => {
  const sample = fixture();
  const oracle = await sample.inbox.ingest(STORY);
  let raw = "";
  const queue = new HunterIntakeQueue({ readState: () => raw, writeState: value => { raw = value; },
    now: () => NOW.getTime(), budgetAllowed: () => true,
    processStory: payload => sample.inbox.ingest(payload),
    resumeStory: (key, payload) => sample.inbox.resume(key, payload),
  });
  const observation = n => ({ eventId: `00000000-0000-4000-8000-00000000000${n}`, source: "instagram",
    observedAt: NOW.toISOString(), payload: STORY });
  await queue.accept("phone-one", { observations: [observation(1)] });
  await queue.accept("phone-two", { observations: [observation(2)] });
  await queue.drain();
  await queue.drain();
  assert.equal(sample.database.prepare("SELECT COUNT(*) AS count FROM instagram_story_outbox").get().count, 1);
  assert.equal(sample.calls.length, 4);
  for (const [client, n] of [["phone-one", 1], ["phone-two", 2]]) {
    const receipt = queue.status(client, observation(n).eventId);
    assert.equal(receipt.status, "delivered");
    assert.deepEqual(Object.keys(receipt.targets), Object.keys(oracle.targets));
    assert.equal(JSON.stringify(receipt).includes("private_signed_query"), false);
  }
  assert.equal((await queue.accept("phone-one", { observations: [observation(1)] })).receipts[0].duplicate, true);
  assert.equal(sample.calls.length, 4);
});

test("Hunter cannot resume a canonical Story with incompatible immutable timestamps", async () => {
  const sample = fixture({ mode: () => "shadow" });
  await sample.inbox.ingest(STORY);
  const before = sample.database.prepare("SELECT * FROM instagram_story_outbox").get();
  let raw = "";
  const queue = new HunterIntakeQueue({ readState: () => raw, writeState: value => { raw = value; },
    now: () => NOW.getTime(), budgetAllowed: () => true,
    processStory: () => { throw new Error("must not ingest"); },
    resumeStory: (key, payload) => sample.inbox.resume(key, payload),
  });
  const event = { eventId: "00000000-0000-4000-8000-000000000001", source: "instagram", observedAt: NOW.toISOString(),
    payload: { ...STORY, expiresAt: "2026-10-10T14:28:43Z" } };
  await queue.accept("phone", { observations: [event] });
  await queue.drain();
  assert.equal(queue.status("phone", event.eventId).status, "blocked");
  assert.equal(sample.calls.length, 0);
  assert.deepEqual(sample.database.prepare("SELECT * FROM instagram_story_outbox").get(), before);
});

test("concurrent ingestion observes reservation and does not duplicate the active send", async () => {
  const sample = fixture();
  let release;
  let entered;
  const ready = new Promise(resolve => { entered = resolve; });
  sample.transports.discord = async () => {
    entered();
    await new Promise(resolve => { release = resolve; });
    return { messageId: "discord-confirmed", imageProxyUrl: "https://media.discordapp.net/external/image.jpg" };
  };
  const first = sample.inbox.ingest(STORY);
  await ready;
  const second = await sample.inbox.ingest(STORY);
  assert.equal(second.targets.discord.status, "in_flight");
  release();
  assert.equal((await first).status, "delivered");
  assert.equal(sample.calls.length, 3);
});

test("missing Discord proxy holds WhatsApp instead of silently sending text", async () => {
  const sample = fixture();
  sample.transports.discord = async () => ({ messageId: "discord-confirmed", imageProxyUrl: "https://evil.test/story.jpg" });
  sample.transports.discordProxy = async () => "";
  const result = await sample.inbox.ingest(STORY);
  assert.equal(result.targets.beeper.status, "held");
  assert.equal(result.targets.beeper.error, "story_image_proxy_pending");
  assert.equal(sample.calls.some(call => call.target === "beeper"), false);
});

test("daily budget bounds new Stories and known receipt replay consumes no extra capacity", async () => {
  const sample = fixture();
  for (let i = 0; i < 20; i++) await sample.inbox.ingest({ ...STORY, storyId: String(BigInt(STORY.storyId) + BigInt(i)) });
  await sample.inbox.ingest(STORY);
  await assert.rejects(sample.inbox.ingest({ ...STORY, storyId: "4004185955500703500" }), /instagram_story_daily_limit/);
  assert.equal(sample.calls.length, 80);
});

test("retention removes only old complete receipts and preserves pending, active and malformed evidence", async () => {
  const sample = fixture();
  const protectedKeys = [];
  const preserve = (key, options) => {
    protectedKeys.push(key);
    seedRetentionRow(sample.database, key, options);
  };
  for (const status of ["unknown", "pending", "in_flight", "reconciling", "failed_safe", "held", "expired"]) {
    preserve(status, { changeTargets: targets => { targets.main.status = status; } });
  }
  for (const target of ["main", "canal2", "discord", "beeper"]) {
    preserve(`missing-receipt-${target}`, { changeTargets: targets => { targets[target].messageId = ""; } });
    preserve(`missing-image-${target}`, { changeTargets: targets => { delete targets[target].imageConfirmed; } });
  }
  for (const [label, rawTargets] of Object.entries({
    invalid: "{", null: "null", array: "[]", missing: "{}",
  })) preserve(`malformed-${label}`, { rawTargets });
  preserve("malformed-target", { changeTargets: targets => { targets.beeper = "confirmed"; } });
  preserve("malformed-receipt", { changeTargets: targets => { targets.main.messageId = 123; } });
  preserve("blank-receipt", { changeTargets: targets => { targets.canal2.messageId = " \n"; } });
  preserve("image-not-boolean", { changeTargets: targets => { targets.beeper.imageConfirmed = "true"; } });
  preserve("invalid-proxy", { discord_image_proxy_url: "https://example.com/image.jpg" });
  preserve("invalid-expiry", { expires_at: "2020" });
  preserve("active");
  sample.inbox.active.add("active");
  preserve("cutoff", { expires_at: "2026-10-07T20:00:00.000Z" });
  preserve("recent-complete", { expires_at: "2026-10-08T20:00:00.000Z" });
  seedRetentionRow(sample.database, "old-complete");
  const before = sample.database.prepare("SELECT * FROM instagram_story_outbox WHERE canonical_key != 'old-complete' ORDER BY canonical_key").all();

  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");

  assert.equal(sample.database.prepare("SELECT canonical_key FROM instagram_story_outbox WHERE canonical_key = 'old-complete'").get(), undefined);
  const after = sample.database.prepare("SELECT * FROM instagram_story_outbox WHERE story_id = '4004185955500703000' ORDER BY canonical_key").all();
  assert.equal(after.length, protectedKeys.length);
  assert.deepEqual(after, before);
  assert.equal(sample.calls.length, 4);
});

test("retention accepts a valid WhatsApp skip and preserves malformed skips or uncertain evidence", async () => {
  const sample = fixture();
  const excluded = `${STORY.link}-ecovilla`;
  const skipped = targets => Object.assign(targets.beeper, { status: "skipped", attempts: 0,
    messageId: "", imageConfirmed: false, error: "whatsapp_link_excluded" });
  seedRetentionRow(sample.database, "old-excluded-complete", { link: excluded, changeTargets: skipped });
  seedRetentionRow(sample.database, "skip-without-excluded-link", { changeTargets: skipped });
  seedRetentionRow(sample.database, "skip-without-reason", { link: excluded,
    changeTargets: targets => { skipped(targets); targets.beeper.error = ""; } });
  seedRetentionRow(sample.database, "excluded-unknown", { link: excluded,
    changeTargets: targets => { targets.beeper.status = "unknown"; } });
  seedRetentionRow(sample.database, "excluded-other-incomplete", { link: excluded,
    changeTargets: targets => { skipped(targets); targets.main.messageId = ""; } });
  const before = sample.database.prepare("SELECT * FROM instagram_story_outbox WHERE canonical_key != 'old-excluded-complete' ORDER BY canonical_key").all();
  await sample.inbox.ingest(STORY);
  assert.equal(sample.database.prepare("SELECT canonical_key FROM instagram_story_outbox WHERE canonical_key = 'old-excluded-complete'").get(), undefined);
  assert.deepEqual(sample.database.prepare("SELECT * FROM instagram_story_outbox WHERE story_id = '4004185955500703000' ORDER BY canonical_key").all(), before);
  sample.database.close();
});

test("capacity remains closed with fifty protected expired receipts and does not consume budget or send", async () => {
  const sample = fixture();
  for (let index = 0; index < 50; index++) seedRetentionRow(sample.database, `protected-${index}`, {
    changeTargets: targets => { targets.beeper.status = index % 2 ? "unknown" : "pending"; },
  });
  const before = sample.database.prepare("SELECT * FROM instagram_story_outbox ORDER BY canonical_key").all();

  await assert.rejects(sample.inbox.ingest(STORY), { message: "instagram_story_capacity_reached", httpStatus: 429 });

  assert.deepEqual(sample.database.prepare("SELECT * FROM instagram_story_outbox ORDER BY canonical_key").all(), before);
  assert.equal(sample.database.prepare("SELECT value FROM metadata WHERE key = 'instagram_story_daily_budget'").get(), undefined);
  assert.equal(sample.calls.length, 0);
});

test("old completed receipt frees one capacity slot without discarding protected rows", async () => {
  const sample = fixture();
  for (let index = 0; index < 49; index++) seedRetentionRow(sample.database, `protected-${index}`, {
    changeTargets: targets => { targets.discord.status = "in_flight"; },
  });
  seedRetentionRow(sample.database, "old-complete");

  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");

  assert.equal(sample.database.prepare("SELECT COUNT(*) AS count FROM instagram_story_outbox").get().count, 50);
  assert.equal(sample.database.prepare("SELECT canonical_key FROM instagram_story_outbox WHERE canonical_key = 'old-complete'").get(), undefined);
  assert.equal(sample.database.prepare("SELECT COUNT(*) AS count FROM instagram_story_outbox WHERE canonical_key LIKE 'protected-%'").get().count, 49);
  assert.equal(sample.calls.length, 4);
});

test("WhatsApp Story photo flag is explicit while normal offer body stays unchanged", async () => {
  const bodies = [];
  const env = {
    BEEPER_GATEWAY_URL: "https://gateway.test/v1/send-offer",
    BEEPER_GATEWAY_TOKEN: "fixture-token", BEEPER_DESTINATION_KEY: "whatsapp-main",
  };
  const send = async (_url, request) => {
    bodies.push(JSON.parse(request.body));
    return Response.json({ pendingMessageId: "receipt", deliveryState: "confirmed_by_whatsapp_bridge", ...(bodies.at(-1).deliveryFormat ? { deliveryFormat: "story_photo" } : {}) });
  };
  const offer = { title: "Story do @clubeuol", link: STORY.link, imageUrl: "https://media.discordapp.net/external/story.jpg" };
  await sendBeeperOffer(env, { ...offer, deliveryFormat: "story_photo" }, { idempotencyKey: "uol:instagram:story:hash:dest:v1" }, send);
  await sendBeeperOffer(env, offer, { idempotencyKey: "normal-offer" }, send);
  assert.equal(bodies[0].deliveryFormat, "story_photo");
  assert.equal("deliveryFormat" in bodies[1], false);
});

test("bot shadow mode holds every destination without I/O and resumes safely in live mode", async () => {
  let mode = "shadow";
  const sample = fixture({ mode: () => mode });
  const held = await sample.inbox.ingest(STORY);
  assert.equal(held.status, "held");
  for (const state of Object.values(held.targets)) {
    assert.equal(state.status, "held");
    assert.equal(state.error, "bot_not_live");
    assert.equal(state.attempts, 0);
  }
  assert.equal(sample.calls.length, 0);
  mode = "live";
  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");
  assert.equal(sample.calls.length, 4);
  mode = "shadow";
  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");
  assert.equal(sample.calls.length, 4);
});

test("mode change during an awaited send stops later destinations and preserves confirmations", async () => {
  let mode = "live";
  const sample = fixture({ mode: () => mode });
  let release;
  let entered;
  let mainAttempts = 0;
  const ready = new Promise(resolve => { entered = resolve; });
  sample.transports.main = async () => {
    mainAttempts++;
    entered();
    await new Promise(resolve => { release = resolve; });
    return { messageId: "main-confirmed" };
  };
  const pending = sample.inbox.ingest(STORY);
  await ready;
  mode = "shadow";
  release();
  const paused = await pending;
  assert.equal(paused.targets.discord.status, "confirmed");
  assert.equal(paused.targets.main.status, "confirmed");
  assert.equal(paused.targets.canal2.error, "bot_not_live");
  assert.equal(paused.targets.beeper.error, "bot_not_live");
  assert.deepEqual(sample.calls.map(call => call.target), ["discord"]);
  mode = "live";
  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");
  assert.equal(mainAttempts, 1);
  assert.deepEqual(sample.calls.map(call => call.target), ["discord", "canal2", "beeper"]);
});

test("mode is rechecked after the awaited Discord proxy lookup before WhatsApp mutation", async () => {
  let mode = "live";
  let proxyReads = 0;
  const sample = fixture({ mode: () => mode });
  sample.transports.discord = async () => ({ messageId: "discord-confirmed" });
  sample.transports.discordProxy = async () => {
    if (++proxyReads === 1) mode = "shadow";
    return "https://media.discordapp.net/external/story.jpg";
  };
  const paused = await sample.inbox.ingest(STORY);
  assert.equal(paused.targets.beeper.error, "bot_not_live");
  assert.equal(sample.calls.some(call => call.target === "beeper"), false);
  mode = "live";
  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");
  assert.equal(sample.calls.filter(call => call.target === "beeper").length, 1);
});

test("paused and resumed modes never rewrite or resend unknown reservations", async () => {
  let mode = "live";
  const sample = fixture({ mode: () => mode });
  let mainAttempts = 0;
  sample.transports.main = async () => {
    mainAttempts++;
    throw Object.assign(new Error("delivery_unconfirmed"), { ambiguous: true });
  };
  const first = await sample.inbox.ingest(STORY);
  assert.equal(first.targets.main.status, "unknown");
  mode = "shadow";
  assert.equal((await sample.inbox.ingest(STORY)).targets.main.status, "unknown");
  mode = "live";
  assert.equal((await sample.inbox.ingest(STORY)).targets.main.status, "unknown");
  assert.equal(mainAttempts, 1);
  assert.equal(sample.calls.filter(call => call.target === "discord").length, 1);
  assert.equal(sample.calls.filter(call => call.target === "beeper").length, 1);
});

test("WhatsApp reconciles its frozen payload after timeout without repeating confirmed channels", async () => {
  const sample = fixture();
  const packages = [];
  sample.transports.beeper = async row => {
    packages.push(row.beeper_payload_json);
    if (packages.length === 1) throw Object.assign(new Error("timeout"), { ambiguous: true });
    return { messageId: "whatsapp-photo-receipt", imageConfirmed: true };
  };
  const first = await sample.inbox.ingest(STORY);
  assert.equal(first.targets.beeper.status, "reconciling");
  sample.setNow(new Date(NOW.getTime() + 3_600_000));
  const last = await sample.inbox.ingest({ ...STORY, imageUrl: STORY.imageUrl.replace("secret", "new") });
  assert.equal(last.status, "delivered");
  assert.equal(packages.length, 2);
  assert.equal(packages[0], packages[1]);
  assert.equal(sample.calls.length, 3);
  assert.equal(JSON.stringify(last).includes("image.jpg"), false);
});

test("Discord media is reconciled by GET without posting its confirmed message again", async () => {
  const sample = fixture();
  let posts = 0;
  let reads = 0;
  sample.transports.discord = async () => { posts++; return { messageId: "discord-photo" }; };
  sample.transports.discordProxy = async () => ++reads === 1 ? "" : "https://media.discordapp.net/external/confirmed.jpg";
  const first = await sample.inbox.ingest(STORY);
  assert.equal(first.targets.discord.status, "confirmed");
  assert.equal(first.targets.discord.imageConfirmed, false);
  assert.notEqual(first.status, "delivered");
  sample.setNow(new Date(NOW.getTime() + 60_000));
  assert.equal((await sample.inbox.ingest(STORY)).status, "delivered");
  assert.equal(posts, 1);
  assert.equal(reads, 2);
});

test("operational auth failures and recovery are visible without media or public-channel alerts", async () => {
  const messages = [];
  const sample = fixture({ notifyOperations: async text => { messages.push(text); } });
  const payload = { sourceStatus: "auth_required", reason: "insecure_session_file",
    observedAt: NOW.toISOString(), lastSuccessAt: "" };
  await sample.inbox.recordMonitorHeartbeat(payload);
  await sample.inbox.recordMonitorHeartbeat(payload);
  assert.equal(messages.length, 1);
  assert.equal(sample.inbox.monitorStatus().healthy, false);
  sample.setNow(new Date(NOW.getTime() + 60_000));
  await sample.inbox.recordMonitorHeartbeat({ sourceStatus: "found", reason: "",
    observedAt: sample.options.now().toISOString(), lastSuccessAt: sample.options.now().toISOString() });
  assert.equal(messages.length, 2);
  assert.equal(sample.inbox.monitorStatus().healthy, true);
  assert.equal(sample.calls.length, 0);
});

test("Discord uploads exact Story bytes and repairs the existing message by PATCH", async () => {
  const bytes = new Uint8Array([255,216,255,224,0,1]);
  const calls = [];
  const send = async (url, init) => {
    calls.push({ url, init });
    if (calls.length === 1) return new Response(bytes, { headers: { "Content-Type": "image/jpeg" } });
    const uploaded = init.body.get("files[0]");
    assert.deepEqual(new Uint8Array(await uploaded.arrayBuffer()), bytes);
    const payload = JSON.parse(init.body.get("payload_json"));
    assert.equal(payload.embeds[0].image.url, "attachment://story.jpg");
    assert.equal(payload.content, `🎟️ Story do @clubeuol\n${STORY.link}`);
    assert.deepEqual(payload.attachments, [{ id: 0, filename: "story.jpg" }]);
    return Response.json({ id: "1558286627955282054", attachments: [{ proxy_url: "https://media.discordapp.net/attachments/story.jpg" }] });
  };
  const result = await uploadInstagramDiscordPhoto({ DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/fixture/token" },
    { image_url: STORY.imageUrl, link: STORY.link }, "1558286627955282054", { fetchImpl: send });
  assert.equal(calls[0].init.redirect, "error");
  assert.equal(calls[1].init.method, "PATCH");
  assert.match(calls[1].url, /\/messages\/1558286627955282054/);
  assert.equal(result.messageId, "1558286627955282054");
  assert.equal(result.imageProxyUrl, "https://media.discordapp.net/attachments/story.jpg");
});

test("media fetch and pause before mutation cannot create an ambiguous Discord post", async () => {
  const env = { DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/fixture/token" };
  const story = { image_url: STORY.imageUrl, link: STORY.link };
  let calls = 0;
  const fetchImpl = async () => {
    calls++;
    return new Response(new Uint8Array([255,216,255,224]), { headers: { "Content-Type": "image/jpeg" } });
  };
  await assert.rejects(uploadInstagramDiscordPhoto(env, story, "", { fetchImpl, stillLive: () => false }),
    error => error.beforeMutation === true);
  assert.equal(calls, 1);
  await assert.rejects(uploadInstagramDiscordPhoto(env, story, "", { fetchImpl: async () => { throw new Error("source_unavailable"); } }),
    error => error.beforeMutation === true && error.retryable === true);
});

test("an unsupported Discord external proxy cannot prevent attachment repair", async () => {
  const sample = fixture();
  await sample.inbox.ingest(STORY);
  const row = sample.database.prepare("SELECT * FROM instagram_story_outbox").get();
  const targets = JSON.parse(row.targets_json);
  targets.discord.imageConfirmed = false;
  sample.database.prepare("UPDATE instagram_story_outbox SET targets_json=?,discord_image_proxy_url=''").run(JSON.stringify(targets));
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, init = {}) => {
    requests.push({ url, method: init.method || "GET" });
    if (requests.length === 1) return Response.json({ embeds: [{ image: { proxy_url: "https://images-ext-1.discordapp.net/external/image.jpg" } }] });
    if (requests.length === 2) return new Response(new Uint8Array([255,216,255,224]), { headers: { "Content-Type": "image/jpeg" } });
    return Response.json({ id: "1558286627955282054", attachments: [{ proxy_url: "https://media.discordapp.net/attachments/image.jpg" }] });
  };
  targets.discord.messageId = "1558286627955282054";
  sample.database.prepare("UPDATE instagram_story_outbox SET targets_json=?").run(JSON.stringify(targets));
  try {
    const inbox = new InstagramStoryInbox(sample.sql, { DISCORD_WEBHOOK_URL: "https://discord.test/api/webhooks/fixture/token" },
      { now: () => NOW, mode: () => "live", readiness: () => ({}) });
    const result = await inbox.ingest(STORY);
    assert.equal(result.targets.discord.imageConfirmed, true);
    assert.equal(result.targets.discord.mediaRepairAttempts, 1);
    assert.deepEqual(requests.map(x => x.method), ["GET","GET","PATCH"]);
  } finally { globalThis.fetch = originalFetch; }
});

test("Oracle supplied media is uploaded directly without a CDN fetch or private SQL body", async () => {
  const bytes = new Uint8Array([255,216,255,224]);
  const normalized = normalizeInstagramStory({ ...STORY, imageBase64: Buffer.from(bytes).toString("base64"), imageMime: "image/jpeg" }, NOW);
  assert.deepEqual(normalized.imageBytes, bytes);
  const calls=[];
  const result=await uploadInstagramDiscordPhoto({DISCORD_WEBHOOK_URL:"https://discord.test/api/webhooks/fixture/token"},
    {image_url:STORY.imageUrl,link:STORY.link,image_bytes:normalized.imageBytes,image_mime:normalized.imageMime},"1558286627955282054",{
      fetchImpl:async (url,init)=>{
        calls.push(init.method);
        assert.deepEqual(new Uint8Array(await init.body.get("files[0]").arrayBuffer()),bytes);
        return Response.json({id:"1558286627955282054",attachments:[{proxy_url:"https://media.discordapp.net/attachments/photo.jpg"}]});
      },
    });
  assert.deepEqual(calls,["PATCH"]);
  assert.equal(result.messageId,"1558286627955282054");
  const sample=fixture();
  const receipt=await sample.inbox.ingest({...STORY,imageBase64:Buffer.from(bytes).toString("base64"),imageMime:"image/jpeg"});
  assert.equal(receipt.status,"delivered");
  assert.equal(JSON.stringify(receipt).includes("/9j/"),false);
  assert.equal(Object.keys(sample.database.prepare("SELECT * FROM instagram_story_outbox").get()).includes("image_bytes"),false);
  assert.throws(()=>normalizeInstagramStory({...STORY,imageBase64:"not base64",imageMime:"image/jpeg"},NOW));
});


test("legacy operations secret never enables Telegram heartbeat alerts", async () => {
  const sample = fixture();
  const inbox = new InstagramStoryInbox(sample.sql, {
    OPS_TELEGRAM_CHAT_ID: "fixture-legacy", TELEGRAM_TOKEN: "fixture-no-network",
  }, { now: () => NOW });
  const result = await inbox.recordMonitorHeartbeat({
    sourceStatus: "auth_required", observedAt: NOW.toISOString(),
    lastSuccessAt: "", reason: "invalid_session_file",
  });
  assert.equal(result.operationsAlert, "not_configured");
  assert.equal(inbox.notifyOperations, null);
});
