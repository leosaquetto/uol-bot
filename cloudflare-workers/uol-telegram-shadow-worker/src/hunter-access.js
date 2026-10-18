// Small, durable approval registry. Only hashes enter metadata; the shared
// Instagram session is returned to approved native clients from a Worker secret.
export const HUNTER_ACCESS_KEY = "hunter_access_v1";
const ID = /^[a-z0-9-]{1,32}$/;
const HEX = /^[a-f0-9]{64}$/;
const DAY = 86400000;
const ACCOUNTS = ["leosaquetto", "gaoriel"];
const UOL_COOKIE_NAMES = new Set(["SESS", "JS_SESS", "DNA", "PHPSESSID", "AWSALB", "AWSALBCORS"]);
const UOL_COOKIE_DOMAINS = new Set(["uol.com.br", "clube.uol.com.br", "api.uol.com.br", "conta.uol.com.br"]);
const bad = (code, status = 400) => Object.assign(new Error("hunter_access_" + code), { httpStatus: status });
const plain = value => !!value && typeof value === "object" && !Array.isArray(value);
const only = (value, keys) => plain(value) && Object.keys(value).every(key => keys.includes(key));
const validAccounts = value => Array.isArray(value) && value.length <= 2 &&
  value.every(account => ACCOUNTS.includes(account)) && new Set(value).size === value.length;
export async function hunterAccessProof(request) {
  const match = /^Bearer (h1\.([a-z0-9-]{1,32})\.[a-f0-9]{64})$/.exec(request.headers.get("Authorization") || "");
  if (!match) return null;
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(match[1]));
  return { deviceId: match[2], secretHash: Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, "0")).join("") };
}
function equal(a, b) {
  if (!HEX.test(a || "") || !HEX.test(b || "")) return false;
  let diff = 0;
  for (let i = 0; i < 64; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
function proofValid(proof) { return plain(proof) && ID.test(proof.deviceId || "") && HEX.test(proof.secretHash || ""); }
function sessionConfig(raw) {
  let value;
  try { value = JSON.parse(raw || ""); } catch { throw bad("setup_unavailable", 503); }
  if (!plain(value) || typeof value.user_agent !== "string" || !value.user_agent || value.user_agent.length > 2048 || /[\r\n\0]/.test(value.user_agent)) throw bad("setup_unavailable", 503);
  const cookies = new Map();
  if (!Array.isArray(value.cookies) || value.cookies.length > 100) throw bad("setup_unavailable", 503);
  for (const cookie of value.cookies) {
    const domain = String(cookie?.domain || ".instagram.com").replace(/^\./, "");
    if (domain !== "instagram.com" && !domain.endsWith(".instagram.com")) continue;
    if (typeof cookie.name === "string" && /^[A-Za-z0-9_]+$/.test(cookie.name) && typeof cookie.value === "string" && cookie.value.length <= 16384 && !/[;\r\n\0]/.test(cookie.value)) cookies.set(cookie.name, cookie.value);
  }
  if (!cookies.get("sessionid")) throw bad("setup_unavailable", 503);
  const language = value.headers?.["accept-language"] || value.headers?.["Accept-Language"];
  const result = { user_agent: value.user_agent, cookies: [...cookies].map(([name, value]) => ({ name, value, domain: ".instagram.com", path: "/" })), headers: {} };
  if (typeof language === "string" && language.length < 1000 && !/[\r\n\0]/.test(language)) result.headers["accept-language"] = language;
  if (JSON.stringify(result).length > 48000) throw bad("setup_unavailable", 503);
  return result;
}
function personalConfig(raw, account, now) {
  let accounts;
  try { accounts = JSON.parse(raw || ""); } catch { throw bad("setup_unavailable", 503); }
  const value = accounts?.[account];
  if (!only(accounts, ACCOUNTS) || !only(value, ["identityDigest", "cookies"]) || !HEX.test(value.identityDigest || "") ||
      !Array.isArray(value.cookies) || !value.cookies.length || value.cookies.length > 100) throw bad("setup_unavailable", 503);
  const cookies = [];
  for (const cookie of value.cookies) {
    if (!plain(cookie) || !UOL_COOKIE_NAMES.has(cookie.name)) continue;
    const originalDomain = cookie.domain ?? "clube.uol.com.br";
    const domain = typeof originalDomain === "string" ? originalDomain.replace(/^\./, "").toLowerCase() : "";
    if (!UOL_COOKIE_DOMAINS.has(domain)) continue;
    const path = cookie.path ?? "/", expires = Number(cookie.expires ?? cookie.expirationDate ?? -1);
    if (typeof cookie.value !== "string" || cookie.value.length > 16384 || /[\u0000-\u0020\u007f;]/.test(cookie.value) ||
        typeof path !== "string" || !path.startsWith("/") || path.length > 2048 || /[\u0000-\u0020\u007f;]/.test(path) ||
        !Number.isFinite(expires) || (cookie.hostOnly !== undefined && typeof cookie.hostOnly !== "boolean") ||
        (cookie.secure !== undefined && typeof cookie.secure !== "boolean") ||
        (cookie.httpOnly !== undefined && typeof cookie.httpOnly !== "boolean")) throw bad("setup_unavailable", 503);
    if (expires >= 0 && expires * 1000 <= now) continue;
    cookies.push({ name: cookie.name, value: cookie.value, domain: originalDomain, path,
      hostOnly: cookie.hostOnly ?? !originalDomain.startsWith("."), secure: cookie.secure !== false, expires,
      ...(cookie.httpOnly !== undefined ? { httpOnly: cookie.httpOnly } : {}),
      ...(["Strict", "Lax", "None", "strict", "lax", "none"].includes(cookie.sameSite) ? { sameSite: cookie.sameSite } : {}) });
  }
  if (!cookies.some(cookie => ["SESS", "JS_SESS", "PHPSESSID"].includes(cookie.name)) ||
      new TextEncoder().encode(JSON.stringify(cookies)).length > 48000) throw bad("setup_unavailable", 503);
  return { identityDigest: value.identityDigest, cookies };
}
export class HunterAccessRegistry {
  constructor({ readState, writeState, budgetAllowed = () => true, env, now = Date.now }) {
    Object.assign(this, { readState, writeState, budgetAllowed, env, now });
  }
  load() {
    const raw = this.readState();
    if (!raw) return { version: 1, day: "", admissions: 0, devices: [] };
    let state;
    try { state = JSON.parse(raw); } catch { throw bad("state_invalid", 503); }
    if (state?.version !== 1 || typeof state.day !== "string" || (state.day !== "" && !/^\d{4}-\d{2}-\d{2}$/.test(state.day)) ||
        !Number.isInteger(state.admissions) || state.admissions < 0 || state.admissions > 24 ||
        !Array.isArray(state.devices) || state.devices.length > 64 ||
        state.devices.some(d => !ID.test(d?.deviceId || "") || !HEX.test(d?.secretHash || "") ||
          !["pending", "approved", "revoked"].includes(d.state) || !["member", "owner"].includes(d.role) ||
          typeof d.label !== "string" || !d.label.trim() || d.label.length > 60 || /[<>\u0000-\u001f\u007f]/.test(d.label) ||
          (d.accounts !== undefined && !validAccounts(d.accounts)) ||
          !Number.isFinite(d.requestedAt) || d.requestedAt < 0 || d.requestedAt > this.now()) ||
        new Set(state.devices.map(d => d.deviceId)).size !== state.devices.length) throw bad("state_invalid", 503);
    return state;
  }
  save(state) {
    if (!this.budgetAllowed()) throw bad("budget_guard", 503);
    const raw = JSON.stringify(state);
    if (new TextEncoder().encode(raw).length > 60000) throw bad("capacity", 429);
    this.writeState(raw);
  }
  legacy(proof) {
    let clients;
    try { clients = JSON.parse(this.env.HUNTER_INTAKE_CLIENTS || "{}"); } catch { return null; }
    const config = clients?.[proof.deviceId];
    if (!config) return null;
    const expected = typeof config === "string" ? config : config.enabled === true ? config.secretHash : "";
    if (!equal(expected, proof.secretHash)) throw bad("unauthorized", 401);
    return { deviceId: proof.deviceId, secretHash: expected, label: proof.deviceId, state: "approved", role: config?.role === "owner" ? "owner" : "member", requestedAt: 0 };
  }
  legacyDevices() {
    let clients;
    try { clients = JSON.parse(this.env.HUNTER_INTAKE_CLIENTS || "{}"); } catch { return []; }
    if (!plain(clients)) return [];
    return Object.entries(clients).filter(([id, config]) => ID.test(id) &&
      HEX.test(typeof config === "string" ? config : config?.enabled === true ? config.secretHash : ""))
      .map(([id, config]) => this.legacy({ deviceId: id, secretHash: typeof config === "string" ? config : config.secretHash }));
  }
  find(proof, state = this.load()) {
    if (!proofValid(proof)) throw bad("unauthorized", 401);
    const legacy = this.legacy(proof);
    const device = state.devices.find(d => d.deviceId === proof.deviceId);
    if (device && !equal(device.secretHash, proof.secretHash)) throw bad("unauthorized", 401);
    return device || legacy;
  }
  safe(device) {
    const expired = device.state === "pending" && this.now() - device.requestedAt >= DAY;
    return { deviceId: device.deviceId, label: device.label, code: device.secretHash.slice(0, 8).toUpperCase(),
      state: expired ? "expired" : device.state, role: device.role, requestedAt: device.requestedAt, accounts: [...(device.accounts || [])] };
  }
  status(proof) {
    const device = this.find(proof);
    if (!device) throw bad("not_found", 404);
    return { ok: true, ...this.safe(device) };
  }
  authenticate(proof) {
    const device = this.find(proof);
    return device?.state === "approved" ? device.deviceId : "";
  }
  request(proof, body) {
    if (!proofValid(proof)) throw bad("unauthorized", 401);
    if (!only(body, ["label"]) || typeof body.label !== "string" || !body.label.trim() || body.label.length > 60 || /[<>\u0000-\u001f\u007f]/.test(body.label)) throw bad("request_invalid");
    const state = this.load(), existing = this.find(proof, state);
    // A repeated/lost response uses the same identity; no admission or write.
    if (existing && this.safe(existing).state !== "expired") return { ok: true, ...this.safe(existing) };
    const day = new Date(this.now()).toISOString().slice(0, 10);
    if (state.day !== day) { state.day = day; state.admissions = 0; }
    if (state.admissions >= 24) throw bad("rate_limit", 429);
    state.devices = state.devices.filter(d => d.state !== "pending" || this.now() - d.requestedAt < DAY);
    if (state.devices.length >= 64 || state.devices.filter(d => d.state === "pending").length >= 16) throw bad("capacity", 429);
    const device = { ...proof, label: body.label.trim(), state: "pending", role: "member", requestedAt: this.now() };
    state.devices.push(device); state.admissions++;
    this.save(state);
    return { ok: true, ...this.safe(device) };
  }
  owner(proof) {
    const device = this.find(proof);
    if (device?.state !== "approved" || device.role !== "owner") throw bad("forbidden", 403);
    return device;
  }
  list(proof, admin = false) {
    if (!admin) this.owner(proof);
    const devices = this.load().devices;
    const legacy = this.legacyDevices().filter(d => !devices.some(record => record.deviceId === d.deviceId));
    return { ok: true, devices: [...devices, ...legacy].map(d => this.safe(d)) };
  }
  decide(proof, body, admin = false) {
    const owner = admin ? null : this.owner(proof);
    if (!only(body, admin ? ["deviceId", "decision", "code", "role", "accounts"] : ["deviceId", "decision", "code", "accounts"]) ||
        !ID.test(body.deviceId || "") || !["approve", "revoke"].includes(body.decision) || !/^[A-F0-9]{8}$/.test(body.code || "") ||
        (body.role !== undefined && !["owner", "member"].includes(body.role)) ||
        (body.accounts !== undefined && (body.decision !== "approve" || !validAccounts(body.accounts)))) throw bad("decision_invalid");
    const state = this.load();
    let device = state.devices.find(d => d.deviceId === body.deviceId);
    if (!device) {
      device = this.legacyDevices().find(d => d.deviceId === body.deviceId);
      if (device) { if (state.devices.length >= 64) throw bad("capacity", 429); state.devices.push(device); }
    }
    if (!device || this.safe(device).code !== body.code) throw bad("identity_conflict", 409);
    if (!admin && device.role === "owner") throw bad("forbidden", 403);
    if (body.decision === "approve" && this.safe(device).state === "expired") throw bad("expired", 409);
    const accounts = body.accounts ?? device.accounts ?? [];
    if (body.decision === "approve" && !admin && accounts.some(account => !(owner.accounts || []).includes(account))) throw bad("forbidden", 403);
    const target = body.decision === "approve" ? "approved" : "revoked";
    if (device.state !== target || (admin && body.role && device.role !== body.role) ||
        (body.accounts !== undefined && JSON.stringify(device.accounts || []) !== JSON.stringify(accounts))) {
      device.state = target;
      if (admin && body.role) device.role = body.role;
      if (body.accounts !== undefined) device.accounts = [...accounts];
      device.decidedAt = this.now();
      this.save(state);
    }
    return { ok: true, ...this.safe(device) };
  }
  config(proof) {
    const device = this.find(proof);
    if (device?.state !== "approved") throw bad("forbidden", 403);
    return { ok: true, deviceId: device.deviceId, role: device.role, instagramSession: sessionConfig(this.env.HUNTER_INSTAGRAM_SESSION) };
  }
  accountConfig(proof, account) {
    const device = this.find(proof);
    if (!ACCOUNTS.includes(account)) throw bad("request_invalid");
    if (device?.state !== "approved" || !(device.accounts || []).includes(account)) throw bad("forbidden", 403);
    return { ok: true, deviceId: device.deviceId, account,
      ...personalConfig(this.env.HUNTER_UOL_ACCOUNTS, account, this.now()) };
  }
}
export function hunterAccessFailure(error) {
  const code = String(error?.message || "");
  const statuses = { unauthorized: 401, forbidden: 403, not_found: 404, request_invalid: 400, decision_invalid: 400,
    identity_conflict: 409, expired: 409, rate_limit: 429, capacity: 429, budget_guard: 503,
    state_invalid: 503, setup_unavailable: 503 };
  const name = code.replace(/^hunter_access_/, "");
  return Object.hasOwn(statuses, name) ? { error: "hunter_access_" + name, status: statuses[name] }
    : { error: "hunter_access_unavailable", status: [400, 413].includes(Number(error?.inputStatus)) ? Number(error.inputStatus) : 503 };
}
