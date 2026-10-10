import { telegramCall, forwardToCanal2, telegramConfiguration, sendOperationsAlert } from "./telegram.js";
import { getDiscordMessageImageProxy, discordConfiguration } from "./discord.js";
import { sendBeeperOffer, beeperGatewayConfiguration, beeperDestinationKey } from "./beeper.js";
import { deliveryRetryAt } from "./delivery-state.js";
import { createAmbiguousResponseTransportError } from "./transport-error.js";

const TARGETS = ["main", "canal2", "discord", "beeper"];
const MAX_INPUT_BYTES = 16 * 1024;
const DAY_MS = 24 * 60 * 60 * 1_000;
const MAX_ATTEMPTS = 5;
const MONITOR_STATUSES = new Set(["starting", "budget_wait", "found", "empty", "auth_required", "rate_limited", "unknown", "stopped"]);
const MONITOR_REASONS = new Set(["", "no_validated_story_structure", "malformed_html", "body_limit", "structure_limit",
  "malformed_story_payload", "conflicting_story_payload", "login_payload", "transport_error", "http_429",
  "http_auth_required", "redirect_limit", "redirect_not_allowed", "http_status", "network_error",
  "insecure_session_file", "session_write_failed", "session_missing", "session_invalid", "session_file_missing", "invalid_session_file"]);

function inputError(code, status = 400) {
  return Object.assign(new Error(code), { instagramInput: true, httpStatus: status });
}

function secureUrl(value, maxLength) {
  if (typeof value !== "string" || value.length > maxLength || value !== value.trim() ||
      /[\u0000-\u0020\u007f]/.test(value)) {
    throw inputError("instagram_url_invalid");
  }
  let url;
  try { url = new URL(value); } catch { throw inputError("instagram_url_invalid"); }
  const authority = value.match(/^https:\/\/([^/?#]+)/)?.[1] || "";
  if (url.protocol !== "https:" || !authority || authority.includes(":") || authority.includes("@") ||
      url.username || url.password || url.port || url.hash || value.includes("\\")) {
    throw inputError("instagram_url_invalid");
  }
  return url;
}

export function validateInstagramCampaignLink(value) {
  const url = secureUrl(value, 2_048);
  if (url.hostname !== "clube.uol.com.br" || url.search || url.href !== value ||
      !/^\/campanhasdeingresso\/p[A-Za-z0-9]{2,5}-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$/.test(url.pathname)) {
    throw inputError("instagram_campaign_link_invalid");
  }
  return url.href;
}

export function isInstagramStoryCaption(value) {
  const prefix = "🎟️ Story do @clubeuol\n";
  if (typeof value !== "string" || !value.startsWith(prefix)) return false;
  try { validateInstagramCampaignLink(value.slice(prefix.length)); return true; }
  catch { return false; }
}

export function validateInstagramImageUrl(value) {
  const url = secureUrl(value, 4_096);
  if (!/^[a-z0-9.-]+\.(?:fbcdn\.net|cdninstagram\.com)$/i.test(url.hostname) ||
      !/\.(?:jpe?g|webp)$/i.test(url.pathname)) {
    throw inputError("instagram_image_url_invalid");
  }
  return url.href;
}

function dateValue(value) {
  const ms = Number.isInteger(value) && value >= 1_000_000_000 && value < 100_000_000_000
    ? value * 1_000
    : typeof value === "string" && /T.*(?:Z|[+-]\d\d:\d\d)$/.test(value)
      ? Date.parse(value) : NaN;
  if (!Number.isFinite(ms)) throw inputError("instagram_story_date_invalid");
  return ms;
}

export function normalizeInstagramStory(payload, now = new Date()) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw inputError("instagram_story_payload_invalid");
  }
  const storyId = payload.storyId;
  if (typeof storyId !== "string" || !/^\d{10,24}$/.test(storyId)) {
    throw inputError("instagram_story_id_invalid");
  }
  const published = dateValue(payload.publishedAt);
  const expires = dateValue(payload.expiresAt);
  if (published > now.getTime() + 60_000 || expires <= published ||
      expires - published > DAY_MS || now.getTime() - published > DAY_MS) {
    throw inputError("instagram_story_date_invalid");
  }
  const width = payload.imageWidth;
  const height = payload.imageHeight;
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 1 || height < 1 ||
      width + height > 10_000 || Math.max(width / height, height / width) > 20) {
    throw inputError("instagram_story_image_dimensions_invalid");
  }
  return {
    storyId,
    link: validateInstagramCampaignLink(payload.link),
    imageUrl: validateInstagramImageUrl(payload.imageUrl),
    imageWidth: width,
    imageHeight: height,
    publishedAt: new Date(published).toISOString(),
    expiresAt: new Date(expires).toISOString(),
  };
}

export async function readInstagramStoryJson(request) {
  if (!/^application\/json(?:\s*;|$)/i.test(request.headers.get("Content-Type") || "")) {
    throw inputError("instagram_content_type_invalid", 415);
  }
  if (Number(request.headers.get("Content-Length") || 0) > MAX_INPUT_BYTES) {
    throw inputError("instagram_payload_too_large", 413);
  }
  const reader = request.body?.getReader();
  if (!reader) throw inputError("instagram_invalid_json");
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_INPUT_BYTES) {
        await reader.cancel();
        throw inputError("instagram_payload_too_large", 413);
      }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  try { return JSON.parse(new TextDecoder().decode(bytes)); }
  catch { throw inputError("instagram_invalid_json"); }
}

export function initializeInstagramStorySchema(sql) {
  sql(`CREATE TABLE IF NOT EXISTS instagram_story_outbox (
    canonical_key TEXT PRIMARY KEY,
    story_id TEXT NOT NULL,
    link TEXT NOT NULL,
    published_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    image_url TEXT NOT NULL,
    image_width INTEGER NOT NULL,
    image_height INTEGER NOT NULL,
    discord_image_proxy_url TEXT NOT NULL DEFAULT '',
    beeper_payload_json TEXT NOT NULL DEFAULT '',
    targets_json TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS instagram_story_id_idx ON instagram_story_outbox(story_id);
  CREATE INDEX IF NOT EXISTS instagram_story_expiry_idx ON instagram_story_outbox(expires_at);`);
}

async function canonicalKey(story) {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(story.link));
  const hash = Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, "0")).join("");
  return `instagram:${story.storyId}:${hash}`;
}

function discordProxyUrl(value) {
  try {
    const url = secureUrl(value, 4_096);
    return ["media.discordapp.net", "cdn.discordapp.com"].includes(url.hostname) ? url.href : "";
  } catch { return ""; }
}

function receipt(row) {
  const targets = JSON.parse(row.targets_json);
  const states = TARGETS.map(target => targets[target].status);
  const status = states.every(state => state === "confirmed") && targets.discord.imageConfirmed === true &&
    targets.beeper.imageConfirmed === true ? "delivered"
    : states.includes("unknown") ? "unknown"
      : states.includes("confirmed") ? "partial"
        : states.every(state => state === "expired") ? "expired"
          : states.every(state => state === "held") ? "held" : "pending";
  return {
    ok: true, profile: "clubeuol", status,
    storyId: row.story_id, canonicalKey: row.canonical_key, link: row.link,
    publishedAt: row.published_at, expiresAt: row.expires_at,
    updatedAt: row.updated_at, targets,
  };
}

export async function uploadInstagramDiscordPhoto(env, story, messageId = "", {
  fetchImpl = fetch, stillLive = () => true,
} = {}) {
  const imageUrl = validateInstagramImageUrl(story.image_url);
  const mediaError = code => Object.assign(new Error(code), { beforeMutation: true, retryable: true });
  let image;
  try {
    image = await fetchImpl(imageUrl, { headers: { Accept: "image/jpeg,image/webp" },
      redirect: "error", signal: AbortSignal.timeout(10_000) });
  } catch { throw mediaError("instagram_media_fetch_failed"); }
  if (!image.ok) throw mediaError("instagram_media_fetch_failed");
  const mime = String(image.headers.get("Content-Type") || "").split(";")[0].toLowerCase();
  if (!["image/jpeg", "image/webp"].includes(mime) ||
      Number(image.headers.get("Content-Length") || 0) > 5 * 1024 * 1024 || !image.body) {
    throw mediaError("instagram_media_invalid");
  }
  const reader = image.body.getReader();
  const chunks = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > 5 * 1024 * 1024) {
        await reader.cancel();
        throw mediaError("instagram_media_limit");
      }
      chunks.push(value);
    }
  } catch { throw mediaError("instagram_media_read_failed"); }
  finally { reader.releaseLock(); }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  const jpeg = bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255;
  const webp = new TextDecoder().decode(bytes.slice(0, 4)) === "RIFF" &&
    new TextDecoder().decode(bytes.slice(8, 12)) === "WEBP";
  if (!length || !(mime === "image/jpeg" ? jpeg : webp)) throw mediaError("instagram_media_invalid");
  if (!stillLive()) throw mediaError("instagram_bot_not_live");
  const filename = mime === "image/jpeg" ? "story.jpg" : "story.webp";
  const form = new FormData();
  form.set("payload_json", JSON.stringify({
    ...(!messageId ? { username: "Clube UOL" } : {}), content: `🎟️ Story do @clubeuol\n${story.link}`,
    embeds: [{ title: "🎟️ Story do @clubeuol", url: story.link,
      description: "Publicado no Instagram do @clubeuol.", image: { url: `attachment://${filename}` } }],
    attachments: [{ id: 0, filename }], allowed_mentions: { parse: [] },
  }));
  form.set("files[0]", new Blob([bytes], { type: mime }), filename);
  const url = new URL(String(env.DISCORD_WEBHOOK_URL || ""));
  if (messageId) {
    if (!/^\d{10,24}$/.test(String(messageId))) throw new Error("instagram_message_id_invalid");
    url.pathname = `${url.pathname.replace(/\/$/, "")}/messages/${messageId}`;
  }
  url.searchParams.set("wait", "true");
  let response;
  try {
    response = await fetchImpl(url.href, { method: messageId ? "PATCH" : "POST", body: form,
      signal: AbortSignal.timeout(10_000) });
  } catch {
    throw createAmbiguousResponseTransportError({ transport: "discord", operation: "storyPhoto" });
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw Object.assign(new Error("instagram_discord_upload_rejected"), {
      httpStatus: response.status, ambiguous: response.status >= 500,
      retryAfterSeconds: Number(payload?.retry_after || response.headers.get("Retry-After") || 0),
    });
  }
  if (!payload.id || messageId && String(payload.id) !== String(messageId)) {
    throw createAmbiguousResponseTransportError({ transport: "discord", operation: "storyPhoto" });
  }
  return { messageId: String(payload.id), imageProxyUrl: String(payload?.embeds?.[0]?.image?.proxy_url ||
    payload?.attachments?.[0]?.proxy_url || payload?.attachments?.[0]?.url || "") };
}

function defaultTransports(env, { stillLive, reserveRepair }) {
  return {
    async main(story) {
      const result = await telegramCall(env, "sendPhoto", {
        chat_id: String(env.TELEGRAM_CHAT_ID || "").trim(),
        photo: story.image_url,
        caption: `🎟️ Story do @clubeuol\n${story.link}`,
        disable_notification: false,
      });
      if (!Number(result?.message_id) || !Array.isArray(result?.photo) || !result.photo.length) {
        throw createAmbiguousResponseTransportError({ transport: "telegram", operation: "sendPhoto" });
      }
      return { messageId: String(result.message_id) };
    },
    async canal2(_story, targets) {
      const result = await forwardToCanal2(env, targets.main.messageId);
      if (!result?.messageId) {
        throw createAmbiguousResponseTransportError({ transport: "telegram", operation: "copyMessage" });
      }
      return { messageId: String(result.messageId) };
    },
    async discord(story) {
      return uploadInstagramDiscordPhoto(env, story, "", { stillLive });
    },
    async discordProxy(story, targets) {
      let proxy = "";
      try { proxy = await getDiscordMessageImageProxy(env, targets.discord.messageId); } catch { /* Try only a bounded repair of this exact message. */ }
      if (proxy || !stillLive() || !reserveRepair(story, targets)) return proxy;
      // Repair an existing message only. Replacing its attachment cannot post a second message.
      return (await uploadInstagramDiscordPhoto(env, story, targets.discord.messageId, { stillLive })).imageProxyUrl;
    },
    async beeper(story) {
      const frozen = JSON.parse(story.beeper_payload_json);
      const result = await sendBeeperOffer(env, frozen.offer, { idempotencyKey: frozen.idempotencyKey });
      return { messageId: result.pendingMessageId, imageConfirmed: result.deliveryFormat === "story_photo" };
    },
  };
}

export class InstagramStoryInbox {
  constructor(sql, env, { transports, now = () => new Date(), readiness, mode, notifyOperations } = {}) {
    this.sql = sql;
    this.env = env;
    this.now = now;
    this.readiness = readiness;
    this.mode = mode || (() => String(env.DELIVERY_MODE || "shadow").trim().toLowerCase());
    this.notifyOperations = notifyOperations || (String(env.OPS_TELEGRAM_CHAT_ID || "").trim()
      ? text => sendOperationsAlert(env, text) : null);
    this.transports = transports || defaultTransports(env, {
      stillLive: () => this.mode() === "live",
      reserveRepair: (row, targets) => {
        const count = Number(targets.discord.mediaRepairAttempts || 0);
        if (count >= 2) return false;
        targets.discord.mediaRepairAttempts = count + 1;
        this.save(row, targets, this.now());
        return true;
      },
    });
    this.active = new Set();
  }

  get(key) {
    return this.sql("SELECT * FROM instagram_story_outbox WHERE canonical_key = ?", key).toArray()[0];
  }

  save(row, targets, now) {
    row.targets_json = JSON.stringify(targets);
    row.updated_at = now.toISOString();
    this.sql(`UPDATE instagram_story_outbox SET targets_json = ?, updated_at = ?,
      image_url = ?, image_width = ?, image_height = ?, discord_image_proxy_url = ?, beeper_payload_json = ?
      WHERE canonical_key = ?`, row.targets_json, row.updated_at,
    row.image_url, row.image_width, row.image_height, row.discord_image_proxy_url,
    row.beeper_payload_json, row.canonical_key);
  }

  ready() {
    if (this.readiness) return this.readiness();
    const telegram = telegramConfiguration(this.env);
    return {
      main: telegram.mainReady,
      canal2: telegram.canal2Ready,
      discord: discordConfiguration(this.env).configured,
      beeper: beeperGatewayConfiguration(this.env).configured,
    };
  }

  holdUnlessLive(row, targets, target) {
    if (this.mode() === "live") return false;
    Object.assign(targets[target], { status: "held", error: "bot_not_live" });
    this.save(row, targets, this.now());
    return true;
  }

  status(storyId = "", limit = 20) {
    if (storyId && !/^\d{10,24}$/.test(storyId)) throw inputError("instagram_story_id_invalid");
    const count = Math.min(50, Math.max(1, Number.parseInt(String(limit), 10) || 20));
    const rows = storyId
      ? this.sql("SELECT * FROM instagram_story_outbox WHERE story_id = ? ORDER BY first_seen_at DESC LIMIT ?", storyId, count).toArray()
      : this.sql("SELECT * FROM instagram_story_outbox ORDER BY first_seen_at DESC LIMIT ?", count).toArray();
    return { ok: true, stories: rows.map(receipt), monitor: this.monitorStatus() };
  }

  monitorStatus() {
    const value = this.sql("SELECT value FROM metadata WHERE key = 'instagram_monitor_status'").toArray()[0]?.value;
    if (!value) return { sourceStatus: "unreported", healthy: false, stale: true, observedAt: "", lastSuccessAt: "", reason: "" };
    const monitor = JSON.parse(value);
    const stale = this.now().getTime() - Date.parse(monitor.observedAt) > 30 * 60_000;
    return { ...monitor, stale, healthy: !stale && ["found", "empty"].includes(monitor.sourceStatus) };
  }

  async recordMonitorHeartbeat(payload) {
    if (!payload || !MONITOR_STATUSES.has(payload.sourceStatus) || !MONITOR_REASONS.has(payload.reason || "")) {
      throw inputError("instagram_monitor_status_invalid");
    }
    const observed = dateValue(payload.observedAt);
    const success = payload.lastSuccessAt ? dateValue(payload.lastSuccessAt) : null;
    if (observed > this.now().getTime() + 60_000 || success !== null && success > observed) {
      throw inputError("instagram_monitor_date_invalid");
    }
    const previous = this.monitorStatus();
    if (previous.observedAt && observed < Date.parse(previous.observedAt)) {
      return { ok: true, recorded: false, monitor: previous, operationsAlert: "unchanged" };
    }
    const monitor = { sourceStatus: payload.sourceStatus, observedAt: new Date(observed).toISOString(),
      lastSuccessAt: success === null ? "" : new Date(success).toISOString(), reason: payload.reason || "" };
    this.sql(`INSERT INTO metadata(key, value) VALUES ('instagram_monitor_status', ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value`, JSON.stringify(monitor));
    const changed = previous.sourceStatus !== monitor.sourceStatus;
    const failure = ["auth_required", "rate_limited"].includes(monitor.sourceStatus);
    const recovered = ["auth_required", "rate_limited", "unknown"].includes(previous.sourceStatus) &&
      ["found", "empty"].includes(monitor.sourceStatus);
    let operationsAlert = "unchanged";
    if (changed && (failure || recovered)) {
      const event = recovered ? "recovered" : monitor.sourceStatus;
      // Reserve before awaiting the Telegram mutation, so concurrent/repeated heartbeats stay quiet.
      this.sql(`INSERT INTO metadata(key, value) VALUES ('instagram_monitor_operations_alert', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`, JSON.stringify({ event, observedAt: monitor.observedAt, state: "reserved" }));
      operationsAlert = "not_configured";
      if (this.notifyOperations) {
        try {
          await this.notifyOperations(`Instagram @clubeuol: ${event === "recovered" ? "leitura recuperada" : event === "auth_required" ? "sessão precisa de renovação" : "limite de consultas; coleta em espera"}.`);
          operationsAlert = "sent";
        } catch { operationsAlert = "unconfirmed"; }
      }
      this.sql(`INSERT INTO metadata(key, value) VALUES ('instagram_monitor_operations_alert', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`, JSON.stringify({ event, observedAt: monitor.observedAt, state: operationsAlert }));
    }
    return { ok: true, recorded: true, monitor: this.monitorStatus(), operationsAlert };
  }

  async ingest(payload) {
    const now = this.now();
    const story = normalizeInstagramStory(payload, now);
    const key = await canonicalKey(story);
    let row = this.get(key);
    if (!row) {
      if (Date.parse(story.expiresAt) <= now.getTime()) throw inputError("instagram_story_expired", 410);
      // Expired rows cannot be delivered again. Keep a bounded private receipt history.
      this.sql("DELETE FROM instagram_story_outbox WHERE expires_at < ?", new Date(now.getTime() - 2 * DAY_MS).toISOString());
      const size = Number(this.sql("SELECT COUNT(*) AS count FROM instagram_story_outbox").toArray()[0]?.count || 0);
      if (size >= 50) throw inputError("instagram_story_capacity_reached", 429);
      const day = new Intl.DateTimeFormat("en-CA", { timeZone: "America/Sao_Paulo" }).format(now);
      const budget = this.sql("SELECT value FROM metadata WHERE key = 'instagram_story_daily_budget'").toArray()[0]?.value;
      const previous = budget ? JSON.parse(budget) : {};
      const count = previous.day === day ? Number(previous.count || 0) : 0;
      if (count >= 20) throw inputError("instagram_story_daily_limit", 429);
      const targets = Object.fromEntries(TARGETS.map(target => [target, { status: "pending", attempts: 0, nextAttemptAt: "", messageId: "", error: "", imageConfirmed: false }]));
      this.sql(`INSERT INTO instagram_story_outbox(canonical_key, story_id, link,
        published_at, expires_at, first_seen_at, updated_at, image_url,
        image_width, image_height, targets_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      key, story.storyId, story.link, story.publishedAt, story.expiresAt,
      now.toISOString(), now.toISOString(), story.imageUrl, story.imageWidth,
      story.imageHeight, JSON.stringify(targets));
      this.sql(`INSERT INTO metadata(key, value) VALUES ('instagram_story_daily_budget', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`, JSON.stringify({ day, count: count + 1 }));
      row = this.get(key);
    } else if (row.published_at !== story.publishedAt || row.expires_at !== story.expiresAt) {
      throw inputError("instagram_story_identity_conflict", 409);
    }
    if (this.active.has(key)) return receipt(row);
    const targets = JSON.parse(row.targets_json);
    if (TARGETS.every(target => targets[target].status === "confirmed") &&
        targets.discord.imageConfirmed === true && targets.beeper.imageConfirmed === true) return receipt(row);
    // A persisted reservation with no live owner may already have sent its message.
    for (const target of TARGETS) if (targets[target].status === "in_flight") {
      targets[target].status = target === "beeper" && row.beeper_payload_json ? "reconciling" : "unknown";
      targets[target].error = target === "beeper" && row.beeper_payload_json
        ? "gateway_receipt_pending" : "reservation_without_live_owner";
    }
    row.image_url = story.imageUrl;
    row.image_width = story.imageWidth;
    row.image_height = story.imageHeight;
    this.save(row, targets, now);
    this.active.add(key);
    try {
      const ready = this.ready();
      // A Discord message receipt does not prove that its remote image was fetched.
      if (targets.discord.status === "confirmed" && !targets.discord.imageConfirmed && this.mode() === "live" &&
          Date.parse(row.expires_at) > this.now().getTime() &&
          (!targets.discord.nextAttemptAt || Date.parse(targets.discord.nextAttemptAt) <= this.now().getTime())) {
        try { row.discord_image_proxy_url = discordProxyUrl(await this.transports.discordProxy(row, targets)); }
        catch { /* Read-only reconciliation; never repost the webhook. */ }
        targets.discord.imageConfirmed = Boolean(row.discord_image_proxy_url);
        targets.discord.error = targets.discord.imageConfirmed ? "" : "story_image_proxy_pending";
        targets.discord.nextAttemptAt = targets.discord.imageConfirmed ? ""
          : new Date(this.now().getTime() + 30_000).toISOString();
        this.save(row, targets, this.now());
      }
      // Discord first provides an allowed image proxy for WhatsApp, without a second post.
      for (const target of ["discord", "main", "canal2", "beeper"]) {
        const state = targets[target];
        if (["confirmed", "unknown"].includes(state.status)) continue;
        if (state.status === "held" && state.error.startsWith("upstream_http_")) continue;
        const instant = this.now();
        if (Date.parse(row.expires_at) <= instant.getTime()) {
          Object.assign(state, { status: "expired", error: "story_expired" });
          this.save(row, targets, instant);
          continue;
        }
        if (this.holdUnlessLive(row, targets, target)) continue;
        if (state.nextAttemptAt && Date.parse(state.nextAttemptAt) > instant.getTime()) continue;
        if (!ready[target]) {
          Object.assign(state, { status: "held", error: "destination_not_configured" });
          this.save(row, targets, instant);
          continue;
        }
        if (target === "canal2" && targets.main.status !== "confirmed") {
          Object.assign(state, { status: "held", error: "main_photo_not_confirmed" });
          this.save(row, targets, instant);
          continue;
        }
        if (target === "beeper" && !row.discord_image_proxy_url) {
          if (targets.discord.status === "confirmed" &&
              (!targets.discord.nextAttemptAt || Date.parse(targets.discord.nextAttemptAt) <= instant.getTime())) {
            try {
              row.discord_image_proxy_url = discordProxyUrl(await this.transports.discordProxy(row, targets));
            } catch { /* A read failure never permits a media-less WhatsApp post. */ }
            if (this.holdUnlessLive(row, targets, target)) continue;
            targets.discord.imageConfirmed = Boolean(row.discord_image_proxy_url);
            if (targets.discord.imageConfirmed) {
              targets.discord.error = "";
              targets.discord.nextAttemptAt = "";
            }
          }
          if (!row.discord_image_proxy_url) {
            Object.assign(state, { status: "held", error: "story_image_proxy_pending", nextAttemptAt: new Date(instant.getTime() + 30_000).toISOString() });
            this.save(row, targets, instant);
            continue;
          }
        }
        if (state.attempts >= MAX_ATTEMPTS && target !== "beeper") {
          Object.assign(state, { status: "held", error: "retry_limit_reached" });
          this.save(row, targets, instant);
          continue;
        }
        // Mode can change while an earlier target or the proxy lookup is awaiting I/O.
        if (this.holdUnlessLive(row, targets, target)) continue;
        if (target === "beeper" && !row.beeper_payload_json) {
          row.beeper_payload_json = JSON.stringify({
            offer: { title: "🎟️ Story do @clubeuol", link: row.link,
              description: "Story do @clubeuol", imageUrl: row.discord_image_proxy_url,
              deliveryFormat: "story_photo" },
            idempotencyKey: `uol:${row.canonical_key}:${this.env.BEEPER_DESTINATION_KEY ? beeperDestinationKey(this.env) : ""}:v1`,
          });
        }
        Object.assign(state, { status: "in_flight", attempts: state.attempts + 1, error: "", nextAttemptAt: "" });
        this.save(row, targets, instant);
        try {
          const result = await this.transports[target](row, targets);
          if (!String(result?.messageId || "")) {
            throw createAmbiguousResponseTransportError({ transport: target, operation: "story" });
          }
          Object.assign(state, { status: "confirmed", messageId: String(result.messageId), error: "", nextAttemptAt: "" });
          if (target === "discord") {
            row.discord_image_proxy_url = discordProxyUrl(result.imageProxyUrl || "");
            state.imageConfirmed = Boolean(row.discord_image_proxy_url);
            if (!state.imageConfirmed) state.error = "story_image_proxy_pending";
          } else if (target === "beeper") {
            if (result.imageConfirmed !== true) {
              throw createAmbiguousResponseTransportError({ transport: "beeper", operation: "story_photo" });
            }
            state.imageConfirmed = true;
          } else state.imageConfirmed = true;
        } catch (error) {
          const status = Number(error?.httpStatus || error?.status || 0);
          // Only an explicit upstream rejection is safe to retry. Network/5xx can hide acceptance.
          const safe = error?.beforeMutation === true || error?.ambiguous !== true && status >= 400 && status < 500;
          const retryable = safe && (status === 429 || error?.retryable === true);
          const reconcile = target === "beeper" && row.beeper_payload_json &&
            (status === 0 || status === 503 || status === 429 ||
              (status === 409 && ["delivery_pending", "delivery_unknown"].includes(error?.description)) ||
              (error?.ambiguous === true && status >= 200 && status < 300));
          Object.assign(state, {
            status: reconcile ? "reconciling" : retryable ? "failed_safe" : safe ? "held" : "unknown",
            error: status ? `upstream_http_${status}` : "delivery_unconfirmed",
            nextAttemptAt: reconcile || retryable ? deliveryRetryAt(error, state.attempts, this.now()) : "",
          });
        }
        this.save(row, targets, this.now());
      }
    } finally { this.active.delete(key); }
    return receipt(this.get(key));
  }
}
