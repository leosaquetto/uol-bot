import test from "node:test";
import assert from "node:assert/strict";
import { parseReels, normalizeCampaignLink, candidatesForWorker } from "../src/stories.js";

const NOW = Date.parse("2026-10-16T12:00:00.000Z");
const PROFILE = { username: "clubeuol", id: "1234567890" };
const LINK = "https://clube.uol.com.br/campanhasdeingresso/pA12-evento-uol";
const IMAGE = "https://scontent.cdninstagram.com/story.jpg?token=keep&expires=123";
const OWNER = { pk: PROFILE.id, username: "ClubeUOL" };

function item(changes = {}) {
  return { pk: "1234567890123456789", taken_at: NOW / 1_000 - 60,
    image_versions2: { candidates: [{ url: IMAGE, width: 1080, height: 1920 }] },
    story_link_stickers: [{ story_link: { url: LINK } }], ...changes };
}

function payload(items = [item()], user = OWNER) {
  return { reels: { [PROFILE.id]: { user, items } } };
}

function parse(body) { return parseReels(body, [PROFILE], NOW)[0]; }

test("both official response shapes require owner and retain only whitelisted fields", () => {
  const source = item({ secret: "discard", caption: { text: "discard" } });
  const mapped = parse(payload([source]));
  const listed = parse({ reels_media: [{ user: { id: PROFILE.id, username: "clubeuol" }, items: [source] }] });
  assert.deepEqual(mapped, listed);
  assert.equal(mapped.status, "found");
  assert.deepEqual(Object.keys(mapped.stories[0]).sort(), ["expiresAt", "imageHeight", "imageUrl", "imageWidth", "links", "publishedAt", "storyId"]);
  assert.equal(mapped.stories[0].imageUrl, IMAGE);
  assert.equal(mapped.stories[0].publishedAt, "2026-10-16T11:59:00.000Z");
  assert.equal(mapped.stories[0].expiresAt, "2026-10-17T11:59:00.000Z");
  assert.equal(JSON.stringify(mapped).includes("discard"), false);
});

test("empty requires a proven matching reel owner", () => {
  assert.equal(parse(payload([])).status, "empty");
  for (const body of [{}, { reels: {} }, { reels: { [PROFILE.id]: { items: [] } } }, payload([], { username: "clubeuol" }),
    payload([], { ...OWNER, pk: "9999999999" }), payload([], { ...OWNER, username: "other" }),
    payload([], { ...OWNER, id: "9999999999" }), { reels_media: [{ user: { pk: "9999999999", username: "other" }, items: [] }] }]) {
    assert.equal(parse(body).status, "unknown");
  }
});

test("foreign item owner and unsafe or conflicting IDs fail closed", () => {
  for (const changes of [{ pk: 1234567890123456789 }, { pk: undefined, id: 1234567890123456789 },
    { pk: "123" }, { id: "9999999999999999999_1234567890" }, { user: { ...OWNER, username: "other" } }]) {
    assert.equal(parse(payload([item(changes)])).status, "unknown");
  }
  assert.equal(parse(payload([item({ pk: undefined, id: "1234567890123456789_1234567890" })])).status, "found");
  const rounded = parse(payload([item({ pk: 1234567890123456789, id: "1234567890123456789_1234567890" })]));
  assert.equal(rounded.status, "found");
  assert.equal(rounded.stories[0].storyId, "1234567890123456789");
  assert.equal(parse(payload([], { pk: 1234567890123456789, username: "clubeuol" })).status, "unknown");
});

test("duplicates collapse; conflicting duplicate story IDs fail the profile", () => {
  assert.equal(parse(payload([item(), item()])).stories.length, 1);
  const result = parse(payload([item(), item({ taken_at: NOW / 1_000 - 120 })]));
  assert.equal(result.status, "unknown");
  assert.equal(result.reason, "conflicting_story_payload");
  assert.deepEqual(result.stories, []);
});

test("expired stories are empty; malformed, future, and overlong stories are unknown", () => {
  assert.equal(parse(payload([item({ taken_at: NOW / 1_000 - 86_400 })])).status, "empty");
  for (const changes of [{ taken_at: undefined }, { taken_at: NOW / 1_000 + 61 },
    { expiring_at: NOW / 1_000 + 86_400 }, { expiring_at: NOW / 1_000 - 61 },
    { story_link_stickers: "bad" }, { story_link_stickers: [{}] }]) {
    assert.equal(parse(payload([item(changes)])).status, "unknown");
  }
  assert.equal(parse(payload([null])).status, "unknown");
});

test("structured login and challenge payloads apply to every requested profile", () => {
  for (const body of [{ message: "login_required" }, { error_type: "challenge_required" }, { challenge: {} }, { require_login: true }]) {
    assert.ok(parseReels(body, [PROFILE, { id: "2345678901", username: "testaccount" }], NOW).every(result => result.status === "auth_required"));
  }
  assert.equal(parse({ message: "Caption says login_required" }).status, "unknown");
});

test("campaign links accept only canonical paths, named tracking and bounded Instagram redirects", () => {
  assert.equal(normalizeCampaignLink(`${LINK}?utm_source=instagram&fbclid=test&igshid=test&igsh=test`), LINK);
  const wrapped = value => `https://l.instagram.com/?u=${encodeURIComponent(value)}`;
  assert.equal(normalizeCampaignLink(wrapped(wrapped(LINK))), LINK);
  for (const value of [wrapped(wrapped(wrapped(LINK))), `${LINK}?token=private`, `${LINK}?url=https://evil.test`,
    `${LINK}#fragment`, `${LINK}#`, `${LINK}/`, LINK.replace("https:", "http:"), LINK.replace("clube.uol.com.br", "clube.uol.com.br.evil.test"),
    LINK.replace("clube.uol.com.br", "user@clube.uol.com.br"), LINK.replace("clube.uol.com.br", "clube.uol.com.br:443"),
    `${LINK} `, LINK.replace("/pA12", "\\pA12"), `${wrapped(LINK)}&u=${encodeURIComponent(LINK)}`, `${wrapped(LINK)}&fbclid=test`,
    wrapped(`https://evil.test/?u=${encodeURIComponent(LINK)}`)]) assert.equal(normalizeCampaignLink(value), null, value);
});

test("prose and unrelated sticker fields never become campaign links", () => {
  const result = parse(payload([item({ story_link_stickers: [], caption: { text: LINK }, link: LINK,
    story_mentions: [{ user: { username: "clubeuol" }, url: LINK }] })]));
  assert.equal(result.status, "found");
  assert.deepEqual(result.stories[0].links, []);
  assert.deepEqual(candidatesForWorker(result), []);
});

test("image selection prefers original aspect, otherwise portrait; no valid image is retained without queueing", () => {
  const candidates = [{ url: IMAGE.replace("story.jpg", "crop.jpg"), width: 1500, height: 1500 },
    { url: IMAGE, width: 1080, height: 1920 }, { url: IMAGE.replace("story.jpg", "small.webp"), width: 540, height: 960 }];
  assert.equal(parse(payload([item({ original_width: 1080, original_height: 1920, image_versions2: { candidates } })])).stories[0].imageUrl, IMAGE);
  assert.equal(parse(payload([item({ original_width: 1500, original_height: 1500, image_versions2: { candidates } })])).stories[0].imageWidth, 1500);
  assert.equal(parse(payload([item({ image_versions2: { candidates } })])).stories[0].imageUrl, IMAGE);
  const cropOnly = parse(payload([item({ original_width: 1080, original_height: 1920,
    image_versions2: { candidates: [candidates[0]] } })]));
  assert.equal(cropOnly.stories[0].imageUrl, "");
  assert.deepEqual(candidatesForWorker(cropOnly), []);
  for (const candidate of [{ url: "https://evil.test/story.jpg", width: 1080, height: 1920 },
    { url: IMAGE, width: 8000, height: 4000 }, { url: IMAGE, width: 1, height: 100 },
    { url: "https://cdninstagram.com/story.jpg", width: 1080, height: 1920 }]) {
    const result = parse(payload([item({ image_versions2: { candidates: [candidate] } })]));
    assert.equal(result.status, "found");
    assert.equal(result.stories[0].imageUrl, "");
    assert.deepEqual(candidatesForWorker(result), []);
  }
});

test("only clubeuol produces exact worker payloads, with one candidate per normalized link", () => {
  const parsed = parseReels(payload(), [PROFILE], NOW);
  const candidates = candidatesForWorker(parsed);
  assert.equal(candidates.length, 1);
  assert.deepEqual(Object.keys(candidates[0]).sort(), ["expiresAt", "imageHeight", "imageUrl", "imageWidth", "link", "publishedAt", "storyId"]);
  assert.equal(candidates[0].link, LINK);
  const testProfile = { username: "testaccount", id: "2345678901" };
  const testResults = parseReels({ reels_media: [{ user: { pk: testProfile.id, username: testProfile.username }, items: [item()] }] }, [testProfile], NOW);
  assert.equal(testResults[0].status, "found");
  assert.deepEqual(candidatesForWorker(testResults), []);
  assert.deepEqual(candidatesForWorker([{ ...parsed[0], status: "unknown" }]), []);
});

test("record limits fail closed", () => {
  assert.equal(parse(payload(Array.from({ length: 101 }, () => item()))).reason, "structure_limit");
  assert.equal(parse({ reels_media: Array.from({ length: 101 }, () => ({ user: OWNER, items: [] })) }).reason, "structure_limit");
  assert.throws(() => parseReels({}, [PROFILE, PROFILE, PROFILE, PROFILE], NOW), /three profiles/);
});
