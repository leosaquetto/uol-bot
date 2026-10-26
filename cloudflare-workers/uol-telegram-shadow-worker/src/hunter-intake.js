import { catalogCampaignUrl, catalogImageUrl } from "./catalog.js";
import { normalizeInstagramStory, instagramStoryCanonicalKey } from "./instagram-story.js";

export const HUNTER_METADATA_KEY = "hunter_intake_v1";
export const HUNTER_BODY_LIMIT = 64 * 1024;
const STATE_LIMIT = 256 * 1024;
const DAY = 86400000;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CLIENT = /^[a-z0-9-]{1,32}$/;
const encoder = new TextEncoder();
const TERMINAL = new Set(["registered", "delivered", "expired", "blocked"]);

export function hunterError(code, httpStatus = 400) {
  return Object.assign(new Error(code), { httpStatus, hunterInput: true });
}
// RPC may preserve Error.message but discard custom fields. Only known codes
// are public; never return upstream URLs, signed media, or arbitrary messages.
const PUBLIC_ERRORS = new Map([
  ...["content_type_invalid"].map(code => [`hunter_${code}`, 415]),
  ...["payload_too_large"].map(code => [`hunter_${code}`, 413]),
  ...["identity_conflict"].map(code => [`hunter_${code}`, 409]),
  ...["capacity"].map(code => [`hunter_${code}`, 429]),
  ...["client_invalid"].map(code => [`hunter_${code}`, 401]),
  ...["receipt_not_found"].map(code => [`hunter_${code}`, 404]),
  ...["state_invalid", "reservation_changed", "budget_guard"].map(code => [`hunter_${code}`, 503]),
  ...["invalid_json", "metadata_invalid", "observation_invalid", "observed_at_invalid", "story_payload_invalid",
    "portal_payload_invalid", "campaign_invalid", "image_invalid", "batch_invalid", "event_id_invalid"]
    .map(code => [`hunter_${code}`, 400]),
]);
export function publicHunterError(error) {
  const code = typeof error?.message === "string" ? error.message : "";
  if (PUBLIC_ERRORS.has(code)) return { error: code, httpStatus: PUBLIC_ERRORS.get(code) };
  if (/^instagram_(?:url_invalid|campaign_link_invalid|image_url_invalid|story_(?:payload_invalid|id_invalid|date_invalid|image_dimensions_invalid))$/.test(code)) {
    return { error: "hunter_story_payload_invalid", httpStatus: 400 };
  }
  return { error: "hunter_unavailable", httpStatus: 503 };
}
function record(value) { return value && typeof value === "object" && !Array.isArray(value); }
function only(value, keys) { return record(value) && Object.keys(value).every(key => keys.includes(key)); }
async function sha(value) {
  return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(value))),
    byte => byte.toString(16).padStart(2, "0")).join("");
}
export async function authenticateHunter(request, env) {
  const match = /^Bearer (h1\.([a-z0-9-]{1,32})\.[a-f0-9]{64})$/.exec(request.headers.get("Authorization") || "");
  if (!match) return "";
  let clients;
  try { clients = JSON.parse(env.HUNTER_INTAKE_CLIENTS || "{}"); } catch { return ""; }
  const config = clients?.[match[2]];
  const expected = typeof config === "string" ? config : config?.enabled === true ? config.secretHash : "";
  if (!/^[a-f0-9]{64}$/.test(expected || "")) return "";
  const actual = await sha(match[1]);
  let diff = 0;
  for (let i = 0; i < 64; i++) diff |= actual.charCodeAt(i) ^ expected.charCodeAt(i);
  return diff === 0 ? match[2] : "";
}

export async function readHunterIntakeJson(request) {
  if (!/^application\/json(?:\s*;|$)/i.test(request.headers.get("Content-Type") || "")) {
    throw hunterError("hunter_content_type_invalid", 415);
  }
  if (Number(request.headers.get("Content-Length") || 0) > HUNTER_BODY_LIMIT) {
    await request.body?.cancel();
    throw hunterError("hunter_payload_too_large", 413);
  }
  const reader = request.body?.getReader();
  if (!reader) throw hunterError("hunter_invalid_json");
  let length = 0;
  const chunks = [];
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > HUNTER_BODY_LIMIT) {
        await reader.cancel();
        throw hunterError("hunter_payload_too_large", 413);
      }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  try { return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)); }
  catch { throw hunterError("hunter_invalid_json"); }
}

function publicText(value, limit) {
  if (typeof value !== "string" || encoder.encode(value).length > limit || /[<>\u0000-\u001f\u007f]/.test(value)) {
    throw hunterError("hunter_metadata_invalid");
  }
  return value.trim();
}
export function normalizeHunterObservation(value, now = Date.now()) {
  if (!only(value, ["eventId", "source", "observedAt", "payload"]) || !UUID.test(value.eventId || "") ||
      !["portal", "instagram"].includes(value.source) || typeof value.observedAt !== "string" ||
      !/^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:\d\d)$/.test(value.observedAt)) {
    throw hunterError("hunter_observation_invalid");
  }
  const at = Date.parse(value.observedAt);
  if (!Number.isFinite(at) || at <= 0 || at > now + 300000) throw hunterError("hunter_observed_at_invalid");
  let payload;
  if (value.source === "instagram") {
    if (!only(value.payload, ["storyId", "link", "imageUrl", "imageWidth", "imageHeight", "publishedAt", "expiresAt"])) {
      throw hunterError("hunter_story_payload_invalid");
    }
    // Validate at observation time: an offline replay may now be expired. The
    // durable Story inbox decides whether it exists or can still be inserted.
    payload = normalizeInstagramStory(value.payload, new Date(Math.min(at, now)));
    if (catalogImageUrl(payload.imageUrl) !== payload.imageUrl) throw hunterError("hunter_image_invalid");
  } else {
    if (!only(value.payload, ["link", "title", "imageUrl", "description", "category", "partner", "validity"])) {
      throw hunterError("hunter_portal_payload_invalid");
    }
    const link = catalogCampaignUrl(value.payload.link);
    if (!link || link !== value.payload.link) throw hunterError("hunter_campaign_invalid");
    payload = { link };
    for (const [key, max] of [["title", 280], ["description", 8192], ["category", 100], ["partner", 120], ["validity", 500]]) {
      if (value.payload[key] !== undefined) payload[key] = publicText(value.payload[key], max);
    }
    if (value.payload.imageUrl !== undefined) {
      const imageUrl = catalogImageUrl(value.payload.imageUrl);
      if (value.payload.imageUrl && (!imageUrl || imageUrl !== value.payload.imageUrl)) throw hunterError("hunter_image_invalid");
      payload.imageUrl = imageUrl;
    }
  }
  return { eventId: value.eventId.toLowerCase(), source: value.source,
    observedAt: new Date(at).toISOString(), payload };
}

function exposed(item, duplicate = false) {
  return { eventId: item.id, source: item.source, persisted: true, duplicate, status: item.status,
    receivedAt: new Date(item.createdAt).toISOString(), updatedAt: new Date(item.updatedAt).toISOString(),
    ...(item.error ? { error: item.error } : {}), ...(item.targets ? { targets: item.targets } : {}) };
}
function storyProgress(result, now) {
  const entries = Object.entries(result?.targets || {});
  const targets = Object.fromEntries(entries.filter(([key]) => ["main", "canal2", "discord", "beeper"].includes(key))
    .map(([key, state]) => [key, { status: state.status, imageConfirmed: state.imageConfirmed === true }]));
  if (result?.status === "delivered") return { status: "delivered", targets };
  if (!entries.length) return { status: "queued", error: "hunter_story_receipt_pending", nextAt: now + 60000 };
  const expired = Date.parse(result?.expiresAt) <= now;
  const uncertain = entries.some(([, state]) => ["unknown", "in_flight", "reconciling"].includes(state.status));
  const retryable = entries.filter(([, state]) =>
    state.status === "confirmed" ? state.imageConfirmed !== true && !expired
      : !["unknown", "expired", "skipped"].includes(state.status) && !expired &&
        !(state.status === "held" && (/^upstream_http_/.test(state.error) || state.error === "retry_limit_reached")));
  if (!retryable.length) {
    return { status: uncertain ? "needs_reconciliation" : expired ? "expired" : "blocked", targets,
      ...(uncertain ? { nextAt: now + 900000 } : {}) };
  }
  const next = retryable.map(([, state]) => Date.parse(state.nextAttemptAt)).filter(Number.isFinite);
  return { status: "queued", targets, nextAt: Math.max(now + 30000, next.length ? Math.min(...next) : now + 60000) };
}

// readState/writeState are synchronous metadata operations. Persistence is one
// atomic replacement; no destination mutation happens inside batch admission.
export class HunterIntakeQueue {
  constructor({ readState, writeState, now = Date.now, budgetAllowed = () => false,
    processPortal, processStory, resumeStory } = {}) {
    this.readState = readState; this.writeState = writeState; this.now = now;
    this.budgetAllowed = budgetAllowed; this.processPortal = processPortal;
    this.processStory = processStory; this.resumeStory = resumeStory;
    this.admission = Promise.resolve(); this.inFlight = false;
  }
  load() {
    const raw = this.readState();
    let state;
    try { state = raw ? typeof raw === "string" ? JSON.parse(raw) : structuredClone(raw)
      : { schema: 1, pending: [], recent: [] }; } catch { throw hunterError("hunter_state_invalid", 503); }
    if (state?.schema !== 1 || !Array.isArray(state.pending) || !Array.isArray(state.recent) ||
        state.pending.length > 32 || state.recent.length > 256) throw hunterError("hunter_state_invalid", 503);
    return state;
  }
  save(state, reserveProgress = false) {
    const value = JSON.stringify(state);
    // Reserve receipt/progress space at admission so a full payload queue can
    // still record safe results without exceeding the aggregate storage limit.
    if (encoder.encode(value).length + (reserveProgress ? state.pending.length * 1024 : 0) > STATE_LIMIT) throw hunterError("hunter_capacity", 429);
    this.writeState(value);
  }
  async accept(clientId, input, authorize = () => true) {
    if (!CLIENT.test(clientId || "")) throw hunterError("hunter_client_invalid", 401);
    if (!only(input, ["observations"]) || !Array.isArray(input.observations) ||
        input.observations.length < 1 || input.observations.length > 8) throw hunterError("hunter_batch_invalid");
    if (encoder.encode(JSON.stringify(input)).length > HUNTER_BODY_LIMIT) throw hunterError("hunter_payload_too_large", 413);
    const now = this.now();
    const prepared = await Promise.all(input.observations.map(async raw => {
      const event = normalizeHunterObservation(raw, now);
      return { event, digest: await sha(JSON.stringify(event)),
        ...(event.source === "instagram" ? { canonicalKey: await instagramStoryCanonicalKey(event.payload) } : {}) };
    }));
    const operation = this.admission.then(() => {
      // Preparation hashes and earlier admissions can yield. Recheck the
      // caller's durable grant at the actual admission, without another await
      // between authorization and the atomic state replacement.
      if (authorize() !== true) throw hunterError("hunter_client_invalid", 401);
      const state = this.load(), clock = this.now(), receipts = [];
      state.recent = state.recent.filter(item => item.updatedAt >= clock - DAY);
      let changed = false;
      for (const { event, digest, canonicalKey } of prepared) {
        const previous = [...state.pending, ...state.recent].find(item => item.id === event.eventId);
        if (previous) {
          if (previous.clientId !== clientId || previous.digest !== digest) throw hunterError("hunter_identity_conflict", 409);
          receipts.push(exposed(previous, true));
          continue;
        }
        if (this.budgetAllowed() !== true) throw hunterError("hunter_budget_guard", 503);
        if (state.pending.length >= 32 || state.recent.length >= 256) throw hunterError("hunter_capacity", 429);
        const item = { id: event.eventId, clientId, source: event.source, digest, event,
          ...(canonicalKey ? { canonicalKey } : {}), status: "queued", attempts: 0,
          createdAt: clock, updatedAt: clock, nextAt: clock };
        state.pending.push(item); changed = true; receipts.push(exposed(item));
      }
      if (changed) this.save(state, true);
      return { ok: true, accepted: true, persisted: true, receipts };
    });
    this.admission = operation.catch(() => {});
    return operation;
  }
  status(clientId, eventId) {
    if (!UUID.test(eventId || "")) throw hunterError("hunter_event_id_invalid");
    const state = this.load();
    const item = [...state.pending, ...state.recent].find(entry => entry.id === eventId.toLowerCase() && entry.clientId === clientId);
    if (!item) throw hunterError("hunter_receipt_not_found", 404);
    return { ok: true, ...exposed(item) };
  }
  async drain() {
    if (this.inFlight) return { ok: true, status: "busy", processed: 0 };
    this.inFlight = true;
    let processed = 0, stories = 0;
    const attempted = new Set();
    try {
      while (processed < 2 && this.budgetAllowed() === true) {
        const state = this.load(), now = this.now();
        const item = state.pending.filter(entry => entry.nextAt <= now && !attempted.has(entry.id) &&
          (entry.source !== "instagram" || stories < 1)).sort((a, b) => a.nextAt - b.nextAt || a.createdAt - b.createdAt)[0];
        if (!item) break;
        const previousStatus = item.status;
        item.attempts++; item.updatedAt = now; item.status = "processing"; item.nextAt = now + 60000;
        this.save(state); // Before an internal idempotent handoff, including restart recovery.
        attempted.add(item.id); processed++; if (item.source === "instagram") stories++;
        let update;
        try {
          if (TERMINAL.has(previousStatus)) {
            update = { status: previousStatus };
          } else if (item.source === "portal") {
            const result = await this.processPortal(item.event.payload, item.event);
            update = result?.persisted === true ? { status: "registered" }
              : { status: "queued", error: "hunter_registration_pending", nextAt: this.now() + 60000 };
          } else {
            const existing = this.resumeStory ? await this.resumeStory(item.canonicalKey, item.event.payload) : null;
            if (!existing && Date.parse(item.event.payload.expiresAt) <= this.now()) {
              update = { status: "expired", error: "hunter_story_expired" };
            } else {
              const result = existing || await this.processStory(item.event.payload, item.event);
              update = storyProgress(result, this.now());
            }
          }
        } catch (error) {
          const status = Number(error?.httpStatus || 0);
          update = { status: status === 410 ? "expired" : [400, 403, 409, 422].includes(status) ? "blocked" : "queued",
            error: status === 410 ? item.source === "instagram" ? "hunter_story_expired" : "hunter_campaign_unavailable"
              : "hunter_processing_pending",
            nextAt: this.now() + Math.min(900000, 30000 * 2 ** Math.min(item.attempts - 1, 5)) };
        }
        // Admission may have run while the internal handler awaited I/O. Merge
        // only this reserved item, never replace the captured pre-I/O state.
        const latest = this.load();
        const index = latest.pending.findIndex(entry => entry.id === item.id && entry.digest === item.digest);
        if (index < 0 || latest.pending[index].attempts !== item.attempts) throw hunterError("hunter_reservation_changed", 503);
        const current = latest.pending[index];
        Object.assign(current, { error: "", targets: undefined }, update, { updatedAt: this.now() });
        if (TERMINAL.has(current.status)) {
          // Keep only terminal receipts; pending identity and payload never compact.
          latest.recent = latest.recent.filter(entry => entry.updatedAt >= this.now() - DAY);
          if (latest.recent.length < 256) {
            latest.pending.splice(index, 1);
            const { event, canonicalKey, nextAt, ...receipt } = current;
            latest.recent.push(receipt);
          } else current.nextAt = this.now() + 900000;
        }
        this.save(latest);
      }
      return { ok: true, status: processed ? "drained" : "idle", processed };
    } finally { this.inFlight = false; }
  }
}
