import assert from "node:assert/strict";
import test from "node:test";
import { buildCatalog, catalogBudgetAllowed, catalogCampaignUrl, catalogImageUrl,
  catalogCacheKey, serveCatalog } from "../src/catalog.js";

const now = Date.parse("2026-10-17T18:00:00Z");
const at = "2026-10-17T17:59:00.000Z";
const link = "https://clube.uol.com.br/campanhasdeingresso/pABC-teatro";
const offer = (fields = {}) => ({ id: "internal-id", link, preview_title: "Peça",
  status: "delivery_pending", first_seen_at: at, last_seen_at: at, ...fields });
const story = (fields = {}) => ({ story_id: "story-1", link,
  image_url: "https://scontent.cdninstagram.com/story.jpg?sig=public-image",
  published_at: at, first_seen_at: at, expires_at: "2026-10-18T17:59:00Z", ...fields });

test("public projection allowlists fields and bounds UTF-8 descriptions", () => {
  const result = buildCatalog([offer({ title: "<b>Peça</b>", description: "🎟".repeat(4000),
    cookie: "never", main_message_id: "private", delivery_unknown_target: "private",
    targets_json: "never", image_url: "javascript:evil", card_image_url: "https://conteudo.imguol.com.br/image.jpg" })],
  [story({ targets_json: "never", beeper_payload_json: "never", canonical_key: "private" })],
  { now, api: { lastSuccessAt: at, healthCards: [{ link }], cookie: "never" } });
  assert.deepEqual(Object.keys(result.offers[0]), ["id", "link", "title", "category", "imageUrl", "partner",
    "description", "validity", "firstSeenAt", "lastSeenAt", "soldOutAt", "restockedAt", "availability", "isPortal", "availabilityEvidence"]);
  assert.equal(result.offers[0].title, "Peça");
  assert.equal(result.offers[0].imageUrl, "https://conteudo.imguol.com.br/image.jpg");
  assert.ok(Buffer.byteLength(result.offers[0].description) <= 8192);
  assert.equal(result.offers[0].isPortal, true);
  assert.equal(result.offers[0].availability, "observed");
  assert.deepEqual(Object.keys(result.stories[0]), ["storyId", "link", "imageUrl", "publishedAt", "expiresAt", "firstSeenAt"]);
  assert.equal(JSON.stringify(result).includes("never"), false);
});

test("snapshot keeps explicit sold-out observations and advertises bounded coverage", () => {
  const rows = [offer({ status: "sold_out", sold_out_at: at }), offer({ id: "discarded", status: "discarded" }),
    offer({ id: "not-ticket", link: "https://clube.uol.com.br/beneficios/brinde" }),
    ...Array.from({ length: 298 }, (_, i) => offer({ id: String(i) }))];
  const result = buildCatalog(rows, [story(), story({ expires_at: at })], { now });
  assert.equal(result.offers.length, 298);
  assert.equal(result.offers[0].availability, "sold_out");
  assert.equal(result.offers.at(-1).id, "296");
  assert.equal(result.stories.length, 1);
  assert.deepEqual(result.coverage, { offerLimit: 300, storyLimit: 30, truncated: true, scope: "recent_tracked" });
  assert.equal(result.offers[0].isPortal, false);
});

test("healthy requires fresh success with no current failure; historical success stays historical", () => {
  for (const failure of [{ lastError: "http_429" }, { lastRunError: "failed" }, { failureStreak: 1 },
    { runFailureStreak: 1 }, { lastOutcome: "failed" }, { lastSuccessAt: "2026-10-17T16:00:00Z" },
    { lastSuccessAt: "2026-10-18T18:00:00Z" }]) {
    const result = buildCatalog([], [], { now, api: { lastSuccessAt: at, ...failure } });
    assert.equal(result.sources.api.healthy, false);
  }
  assert.deepEqual(buildCatalog([], [], { now, ticketListing: { lastSuccessAt: at } }).sources,
    { api: { lastSuccessAt: "", healthy: false }, ticketListing: { lastSuccessAt: at, healthy: true } });
});

test("public URLs reject unsafe destinations and remove campaign query credentials", () => {
  for (const value of ["javascript:alert(1)", "http://clube.uol.com.br/campanhasdeingresso/pA-test",
    "https://user:pass@clube.uol.com.br/campanhasdeingresso/pA-test", "https://clube.uol.com.br.evil/campanhasdeingresso/pA-test",
    "https://clube.uol.com.br/usuario/voucher/secret"]) assert.equal(catalogCampaignUrl(value), "");
  assert.equal(catalogCampaignUrl(`${link}?token=secret#voucher`), link);
  for (const value of ["https://example.com/image.jpg", "https://127.0.0.1/image.jpg",
    "https://cdn.discordapp.com/api/webhooks/secret", "https://scontent.cdninstagram.com/a.jpg?sessionid=secret"])
    assert.equal(catalogImageUrl(value), "");
});

test("optional budget protects the prospective read reserve", () => {
  assert.equal(catalogBudgetAllowed({ maintenanceAllowed: true, maintenanceCeiling: 5000, rowsRead: 4300 }), true);
  assert.equal(catalogBudgetAllowed({ maintenanceAllowed: true, maintenanceCeiling: 5000, rowsRead: 4301 }), false);
  assert.equal(catalogBudgetAllowed({ maintenanceAllowed: false, maintenanceCeiling: 5000, rowsRead: 0 }), false);
  assert.equal(catalogBudgetAllowed({ maintenanceAllowed: true }), false);
});

test("normalized versioned cache serves conditional GET and CORS without another catalog read", async () => {
  const entries = new Map(), pending = [];
  const cache = { async match(key) { return entries.get(key.url)?.clone(); },
    async put(key, response) { entries.set(key.url, response); } };
  let reads = 0;
  const args = { versionId: "version-a", cache, waitUntil: task => pending.push(task),
    getCatalog: async () => { reads++; return { payload: buildCatalog([offer()], [], { now }), ttlSeconds: 120, rowsRead: 4 }; } };
  const first = await serveCatalog(new Request("https://worker.test/catalog?limit=1&account=private"), args);
  await Promise.all(pending);
  assert.equal(first.status, 200);
  assert.equal(first.headers.get("X-Catalog-Rows-Read"), "4");
  const etag = first.headers.get("ETag");
  const next = await serveCatalog(new Request("https://worker.test/catalog?random=2", {
    headers: { "If-None-Match": `"other", W/${etag}` },
  }), args);
  assert.equal(next.status, 304);
  assert.equal(await next.text(), "");
  assert.equal(next.headers.get("Access-Control-Allow-Origin"), "*");
  assert.equal(next.headers.get("X-Catalog-Rows-Read"), "0");
  assert.equal(reads, 1);
  assert.notEqual(catalogCacheKey(new Request("https://worker.test/catalog"), "version-b").url,
    catalogCacheKey(new Request("https://worker.test/catalog"), "version-a").url);
  await serveCatalog(new Request("https://worker.test/catalog"), { ...args, versionId: "version-b" });
  assert.equal(reads, 2);
});

test("preflight never touches storage; denial is explicit, uncacheable and CORS-readable", async () => {
  const cache = { match: () => { throw new Error("should not read cache"); } };
  const preflight = await serveCatalog(new Request("https://worker.test/catalog", { method: "OPTIONS" }), { cache });
  assert.equal(preflight.status, 204);
  assert.equal(preflight.headers.get("Access-Control-Allow-Headers"), "If-None-Match");
  const denied = await serveCatalog(new Request("https://worker.test/catalog"), {
    versionId: "a", cache: { match: async () => null }, getCatalog: async () => ({ error: "catalog_budget_guard", rowsRead: 0 }),
    waitUntil: () => { throw new Error("must not cache refusal"); },
  });
  assert.equal(denied.status, 503);
  assert.deepEqual(await denied.json(), { ok: false, error: "catalog_budget_guard" });
  assert.equal(denied.headers.get("Cache-Control"), "no-store");
  assert.equal(denied.headers.get("Retry-After"), "120");
  assert.equal(denied.headers.get("Access-Control-Allow-Origin"), "*");
});


test("only the exact poster CDN is allowed and credentials stay rejected", () => {
  const currentPoster = "https://ddrxgn8ucibei.cloudfront.net/beneficios/poster.jpg";
  assert.equal(catalogImageUrl(currentPoster), currentPoster);
  assert.equal(buildCatalog([offer({ image_url: currentPoster })], [], { now }).offers[0].imageUrl, currentPoster);
  const image = "https://d310rdc9v8uuq.cloudfront.net/posters/image.jpg";
  assert.equal(catalogImageUrl(image), image);
  assert.equal(buildCatalog([offer({ image_url: image })], [], { now }).offers[0].imageUrl, image);
  for (const url of ["https://other.cloudfront.net/poster.jpg", "https://d310rdc9v8uuq.cloudfront.net.evil.test/poster.jpg",
    `${image}?sessionid=secret`, "https://user@d310rdc9v8uuq.cloudfront.net/poster.jpg"])
    assert.equal(catalogImageUrl(url), "");
});

test("availability evidence uses the latest explicit observation, never lastSeen or failing history", () => {
  const sold = "2026-10-17T17:30:00.000Z";
  const active = buildCatalog([offer({ status: "sold_out", sold_out_at: sold })], [],
    { now, api: { lastSuccessAt: at, healthCards: [{ link }] } }).offers[0];
  assert.deepEqual(active.availabilityEvidence, { state: "available", observedAt: at, source: "api" });
  assert.equal(active.availability, "observed");
  const stale = buildCatalog([offer({ status: "sold_out", sold_out_at: sold })], [],
    { now, api: { lastSuccessAt: at, lastError: "failed", healthCards: [{ link }] } }).offers[0];
  assert.deepEqual(stale.availabilityEvidence, { state: "sold_out", observedAt: sold, source: "worker" });
  assert.equal(stale.availability, "sold_out");
  assert.equal(buildCatalog([offer()], [], { now }).offers[0].availabilityEvidence, undefined);
  const restocked = buildCatalog([offer({ status: "sold_out", sold_out_at: sold, restocked_at: at })], [], { now }).offers[0];
  assert.deepEqual(restocked.availabilityEvidence, { state: "available", observedAt: at, source: "worker" });
  assert.equal(restocked.availability, "observed");
  const newerSold = buildCatalog([offer({ sold_out_at: at })], [],
    { now, ticketListing: { lastSuccessAt: sold, cards: [{ link }] } }).offers[0];
  assert.equal(newerSold.availabilityEvidence.state, "sold_out");
});
