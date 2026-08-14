import { createHash, timingSafeEqual } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

const MAX_BODY_BYTES = 64 * 1024;
const DEFAULT_BEEPER_URL = "http://127.0.0.1:23373";
const DELIVERY_TIMEOUT_MS = 20_000;

function json(status, body, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

function secureEqual(left, right) {
  const leftBuffer = Buffer.from(String(left || ""));
  const rightBuffer = Buffer.from(String(right || ""));
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

function requestHash(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function allowedOfferUrl(value) {
  try {
    const url = new URL(String(value || ""));
    return url.protocol === "https:" && url.hostname === "clube.uol.com.br";
  } catch {
    return false;
  }
}

async function readJsonBody(request) {
  const declared = Number(request.headers.get("Content-Length") || 0);
  if (declared > MAX_BODY_BYTES) throw Object.assign(new Error("body_too_large"), { status: 413 });
  const text = await request.text();
  if (Buffer.byteLength(text) > MAX_BODY_BYTES) {
    throw Object.assign(new Error("body_too_large"), { status: 413 });
  }
  try {
    return JSON.parse(text);
  } catch {
    throw Object.assign(new Error("invalid_json"), { status: 400 });
  }
}

function openDatabase(path) {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const database = new DatabaseSync(path);
  database.exec(`
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS deliveries (
      idempotency_key TEXT PRIMARY KEY,
      request_hash TEXT NOT NULL,
      status TEXT NOT NULL,
      response_json TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    UPDATE deliveries SET status = 'unknown'
      WHERE status = 'pending';
  `);
  return database;
}

function reserveDelivery(database, key, hash, now) {
  database.exec("BEGIN IMMEDIATE");
  try {
    const existing = database.prepare(
      "SELECT * FROM deliveries WHERE idempotency_key = ?",
    ).get(key);
    if (existing) {
      database.exec("COMMIT");
      if (existing.request_hash !== hash) return { kind: "conflict" };
      if (existing.status === "sent") {
        return { kind: "replay", response: JSON.parse(existing.response_json || "{}") };
      }
      if (existing.status === "unknown") return { kind: "unknown" };
      if (existing.status === "pending") return { kind: "pending" };
      database.prepare(
        "UPDATE deliveries SET status = 'pending', updated_at = ? WHERE idempotency_key = ?",
      ).run(now, key);
      return { kind: "reserved" };
    }
    database.prepare(
      `INSERT INTO deliveries(idempotency_key, request_hash, status, created_at, updated_at)
       VALUES (?, ?, 'pending', ?, ?)`,
    ).run(key, hash, now, now);
    database.exec("COMMIT");
    return { kind: "reserved" };
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

function updateDelivery(database, key, status, response, now) {
  database.prepare(
    `UPDATE deliveries SET status = ?, response_json = ?, updated_at = ?
     WHERE idempotency_key = ?`,
  ).run(status, JSON.stringify(response || {}), now, key);
}

export function createGateway({
  token,
  chatId,
  beeperAccessToken,
  beeperApiUrl = DEFAULT_BEEPER_URL,
  databasePath,
  fetchImpl = fetch,
  now = () => new Date(),
}) {
  if (!String(token || "").trim()) throw new Error("GATEWAY_TOKEN is required");
  if (!String(chatId || "").trim()) throw new Error("BEEPER_CHAT_ID is required");
  if (!String(beeperAccessToken || "").trim()) throw new Error("BEEPER_ACCESS_TOKEN is required");
  if (!String(databasePath || "").trim()) throw new Error("DATA_PATH is required");
  const database = openDatabase(databasePath);
  const baseUrl = String(beeperApiUrl).replace(/\/+$/, "");

  return async function handler(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/livez") {
      return json(200, { ok: true });
    }
    if (request.method === "GET" && url.pathname === "/readyz") {
      try {
        const response = await fetchImpl(
          `${baseUrl}/v1/chats/${encodeURIComponent(chatId)}`,
          {
          headers: { Authorization: `Bearer ${beeperAccessToken}` },
          signal: AbortSignal.timeout(3_000),
          },
        );
        return response.ok
          ? json(200, { ok: true })
          : json(503, { ok: false, code: "beeper_not_ready" });
      } catch {
        return json(503, { ok: false, code: "beeper_not_ready" });
      }
    }
    if (request.method !== "POST" || url.pathname !== "/v1/send-offer") {
      return json(404, { code: "not_found" });
    }

    const suppliedToken = request.headers.get("Authorization")?.replace(/^Bearer\s+/i, "") || "";
    if (!secureEqual(suppliedToken, token)) return json(401, { code: "unauthorized" });
    const idempotencyKey = String(request.headers.get("Idempotency-Key") || "").trim();
    if (!/^[A-Za-z0-9:._-]{8,200}$/.test(idempotencyKey)) {
      return json(400, { code: "invalid_idempotency_key" });
    }

    let payload;
    try {
      payload = await readJsonBody(request);
    } catch (error) {
      return json(Number(error.status || 400), { code: error.message });
    }
    const link = String(payload?.link || "").trim();
    const text = String(payload?.text || "").trim();
    if (!allowedOfferUrl(link) || !text || text.length > 8_000 || !text.includes(link)) {
      return json(400, { code: "invalid_offer" });
    }

    const normalized = { link, text };
    const reserved = reserveDelivery(
      database,
      idempotencyKey,
      requestHash(normalized),
      now().toISOString(),
    );
    if (reserved.kind === "conflict") return json(409, { code: "idempotency_conflict" });
    if (reserved.kind === "unknown") return json(409, { code: "delivery_unknown" });
    if (reserved.kind === "pending") return json(409, { code: "delivery_pending" });
    if (reserved.kind === "replay") return json(200, { ...reserved.response, replayed: true });

    let response;
    try {
      response = await fetchImpl(
        `${baseUrl}/v1/chats/${encodeURIComponent(chatId)}/messages`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${beeperAccessToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ text }),
          signal: AbortSignal.timeout(DELIVERY_TIMEOUT_MS),
        },
      );
    } catch {
      updateDelivery(database, idempotencyKey, "unknown", {}, now().toISOString());
      return json(503, { code: "delivery_unknown" });
    }

    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      updateDelivery(database, idempotencyKey, "failed", {}, now().toISOString());
      return json(502, { code: "beeper_rejected", status: response.status });
    }
    const accepted = {
      accepted: true,
      pendingMessageID: String(result?.pendingMessageID || ""),
    };
    updateDelivery(database, idempotencyKey, "sent", accepted, now().toISOString());
    return json(202, accepted);
  };
}
