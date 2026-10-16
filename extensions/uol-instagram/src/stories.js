const DAY_MS = 86_400_000;
const MAX_RECORDS = 100;
const STORY_ID = /^\d{10,24}$/;
const CAMPAIGN_PATH = /^\/campanhasdeingresso\/p[A-Za-z0-9]{2,5}-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$/;
const CDN_HOST = /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:cdninstagram\.com|fbcdn\.net)$/i;

function record(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function secureUrl(value, maxLength) {
  if (typeof value !== "string" || value.length > maxLength || /[\s\\\u0000-\u001f\u007f]/u.test(value)) return null;
  const authority = value.match(/^https:\/\/([^/?#]+)/)?.[1];
  if (!authority || /[:@]/.test(authority) || value.includes("#")) return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" && !url.username && !url.password && !url.port && !url.hash ? url : null;
  } catch { return null; }
}

export function normalizeCampaignLink(value, depth = 0) {
  const url = secureUrl(value, 2_048);
  if (!url) return null;
  if (url.hostname === "l.instagram.com") {
    const entries = [...url.searchParams];
    if (depth >= 2 || url.pathname !== "/" || entries.length !== 1 || entries[0][0] !== "u") return null;
    return normalizeCampaignLink(entries[0][1], depth + 1);
  }
  if (url.hostname !== "clube.uol.com.br" || !CAMPAIGN_PATH.test(url.pathname)) return null;
  for (const key of url.searchParams.keys()) {
    if (!["fbclid", "igshid", "igsh"].includes(key.toLowerCase()) && !/^utm_[a-z0-9_]+$/i.test(key)) return null;
  }
  url.search = "";
  return url.href;
}

function identifier(value, pattern = /^\d{1,24}$/) {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) return null;
    value = String(value);
  }
  return typeof value === "string" && pattern.test(value) ? value : null;
}

function ownerMatches(user, profile) {
  if (!record(user) || typeof user.username !== "string" || user.username.toLowerCase() !== profile.username) return false;
  const ids = [user.id, user.pk].filter(value => value !== undefined && value !== null);
  return ids.length > 0 && ids.every(value => identifier(value) === profile.id);
}

function storyIdentifier(item) {
  const pk = item.pk === undefined || item.pk === null ? null : identifier(item.pk, STORY_ID);
  const hasId = item.id !== undefined && item.id !== null;
  const id = hasId && typeof item.id === "string" ? identifier(item.id.split("_")[0], STORY_ID) : null;
  // Instagram also sends large numeric pk values. JSON has already rounded
  // those; only the independent string id can retain the exact identity.
  const roundedPk = typeof item.pk === "number" && Number.isInteger(item.pk) && !Number.isSafeInteger(item.pk);
  if ((item.pk !== undefined && item.pk !== null && !pk && !(roundedPk && id)) || (hasId && !id) || (pk && id && pk !== id)) return null;
  return id || pk;
}

function seconds(value) {
  if (typeof value === "string" && /^\d{10,13}$/.test(value)) value = Number(value);
  return Number.isSafeInteger(value) && value >= 1_000_000_000 && Number.isSafeInteger(value * 1_000) ? value * 1_000 : null;
}

function dimensions(width, height) {
  return Number.isSafeInteger(width) && Number.isSafeInteger(height) && width > 0 && height > 0 &&
    width + height <= 10_000 && Math.max(width / height, height / width) <= 20;
}

function imageUrl(value) {
  const url = secureUrl(value, 4_096);
  return url && CDN_HOST.test(url.hostname) && /\.(?:jpe?g|webp)$/i.test(url.pathname) ? url.href : null;
}

function selectImage(item) {
  const source = item.image_versions2?.candidates;
  if (!Array.isArray(source)) return { imageUrl: "", imageWidth: 0, imageHeight: 0 };
  if (source.length > MAX_RECORDS) throw new Error("structure_limit");
  const candidates = source.filter(candidate => record(candidate) && dimensions(candidate.width, candidate.height))
    .map(candidate => ({ url: imageUrl(candidate.url), width: candidate.width, height: candidate.height }))
    .filter(candidate => candidate.url);
  const originalRatio = dimensions(item.original_width, item.original_height) ? item.original_width / item.original_height : null;
  const fullAspect = originalRatio ? candidates.filter(candidate => Math.abs(candidate.width / candidate.height / originalRatio - 1) <= 0.02) : [];
  const portrait = candidates.filter(candidate => candidate.height > candidate.width);
  const preferred = originalRatio ? fullAspect : portrait.length ? portrait : candidates;
  const best = preferred.sort((a, b) => b.width * b.height - a.width * a.height)[0];
  return best ? { imageUrl: best.url, imageWidth: best.width, imageHeight: best.height } : { imageUrl: "", imageWidth: 0, imageHeight: 0 };
}

function parseStory(item, profile, nowMs) {
  if (!record(item) || (item.user !== undefined && !ownerMatches(item.user, profile))) throw new Error("malformed_story_payload");
  const storyId = storyIdentifier(item);
  const published = seconds(item.taken_at);
  const expires = item.expiring_at === undefined || item.expiring_at === null ? published + DAY_MS : seconds(item.expiring_at);
  if (!storyId || published === null || expires === null || published > nowMs + 60_000 ||
      expires <= published || expires - published > DAY_MS) throw new Error("malformed_story_payload");
  const stickers = item.story_link_stickers ?? [];
  if (!Array.isArray(stickers)) throw new Error("malformed_story_payload");
  if (stickers.length > MAX_RECORDS) throw new Error("structure_limit");
  const links = new Set();
  for (const sticker of stickers) {
    if (!record(sticker) || !record(sticker.story_link) || typeof sticker.story_link.url !== "string") throw new Error("malformed_story_payload");
    const link = normalizeCampaignLink(sticker.story_link.url);
    if (link) links.add(link);
  }
  return {
    storyId, publishedAt: new Date(published).toISOString(), expiresAt: new Date(expires).toISOString(),
    ...selectImage(item), links: [...links].sort(),
  };
}

function authRequired(payload) {
  const codes = new Set(["login_required", "challenge_required", "checkpoint_required", "not_logged_in"]);
  return record(payload) && (codes.has(payload.message) || codes.has(payload.error_type) ||
    payload.login_required === true || payload.require_login === true || payload.authenticated === false ||
    record(payload.challenge) || typeof payload.checkpoint_url === "string");
}

function findReels(payload, profile) {
  const reels = [];
  if (payload.reels !== undefined) {
    if (!record(payload.reels)) throw new Error("malformed_story_payload");
    if (Object.hasOwn(payload.reels, profile.id)) reels.push(payload.reels[profile.id]);
  }
  if (payload.reels_media !== undefined) {
    if (!Array.isArray(payload.reels_media)) throw new Error("malformed_story_payload");
    if (payload.reels_media.length > MAX_RECORDS) throw new Error("structure_limit");
    for (const reel of payload.reels_media) {
      if (!record(reel) || !record(reel.user)) continue;
      if (reel.user.username?.toLowerCase?.() === profile.username ||
          [reel.user.id, reel.user.pk].some(value => identifier(value) === profile.id)) reels.push(reel);
    }
  }
  return reels;
}

export function parseReels(payload, profiles, nowMs = Date.now()) {
  if (!Array.isArray(profiles) || profiles.length > 3) throw new TypeError("At most three profiles are supported");
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) throw new TypeError("Invalid current time");
  return profiles.map(input => {
    const profile = { username: typeof input?.username === "string" ? input.username.toLowerCase() : "", id: identifier(input?.id) || "" };
    const result = { ...profile, status: "unknown", reason: "no_validated_story_structure", stories: [] };
    if (authRequired(payload)) return { ...result, status: "auth_required", reason: "login_payload" };
    if (!profile.id || !/^[a-z0-9._]{1,30}$/.test(profile.username) || !record(payload) || payload.status === "fail") return result;
    try {
      const reels = findReels(payload, profile);
      if (!reels.length) return result;
      const stories = new Map();
      let count = 0;
      for (const reel of reels) {
        if (!record(reel) || !ownerMatches(reel.user, profile) || !Array.isArray(reel.items)) throw new Error("malformed_story_payload");
        count += reel.items.length;
        if (count > MAX_RECORDS) throw new Error("structure_limit");
        for (const item of reel.items) {
          const story = parseStory(item, profile, nowMs);
          if (stories.has(story.storyId) && JSON.stringify(stories.get(story.storyId)) !== JSON.stringify(story)) throw new Error("conflicting_story_payload");
          stories.set(story.storyId, story);
        }
      }
      result.stories = [...stories.values()].filter(story => Date.parse(story.expiresAt) > nowMs);
      result.status = result.stories.length ? "found" : "empty";
      result.reason = "";
    } catch (error) {
      result.reason = ["structure_limit", "malformed_story_payload", "conflicting_story_payload"].includes(error.message) ? error.message : "malformed_story_payload";
    }
    return result;
  });
}

export function candidatesForWorker(result) {
  const candidates = [];
  const seen = new Set();
  for (const profile of Array.isArray(result) ? result.slice(0, 3) : [result]) {
    if (profile?.username !== "clubeuol" || profile.status !== "found" || !Array.isArray(profile.stories)) continue;
    for (const story of profile.stories.slice(0, MAX_RECORDS)) {
      if (!record(story) || typeof story.storyId !== "string" || !STORY_ID.test(story.storyId) ||
          !imageUrl(story.imageUrl) || !dimensions(story.imageWidth, story.imageHeight) || !Array.isArray(story.links)) continue;
      const published = Date.parse(story.publishedAt);
      const expires = Date.parse(story.expiresAt);
      if (!Number.isFinite(published) || !Number.isFinite(expires) || expires <= published || expires - published > DAY_MS ||
          new Date(published).toISOString() !== story.publishedAt || new Date(expires).toISOString() !== story.expiresAt) continue;
      for (const value of story.links.slice(0, MAX_RECORDS)) {
        const link = normalizeCampaignLink(value);
        const key = `${story.storyId}:${link}`;
        if (!link || seen.has(key)) continue;
        seen.add(key);
        candidates.push({ storyId: story.storyId, publishedAt: story.publishedAt, expiresAt: story.expiresAt,
          imageUrl: imageUrl(story.imageUrl), imageWidth: story.imageWidth, imageHeight: story.imageHeight, link });
      }
    }
  }
  return candidates;
}
