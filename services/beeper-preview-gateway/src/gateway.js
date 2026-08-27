import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

const MAX_BODY_BYTES = 64 * 1024;
const DEFAULT_BEEPER_URL = "http://127.0.0.1:23373";
const DELIVERY_TIMEOUT_MS = 20_000;

function auditLog(logger, level, event, fields = {}) {
  const write = logger?.[level] || logger?.log;
  if (typeof write !== "function") return;
  write.call(logger, JSON.stringify({ event, ...fields }));
}

function keyDigest(value) {
  return value
    ? createHash("sha256").update(String(value)).digest("hex").slice(0, 12)
    : undefined;
}

function safeRoute(path) {
  return ["/livez", "/readyz", "/v1/readyz", "/v1/send-offer"].includes(path)
    ? path
    : "other";
}

function json(status, body, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

function secureEqual(left, right) {
  const digest = (value) => createHash("sha256").update(String(value || "")).digest();
  return timingSafeEqual(digest(left), digest(right));
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

function allowedImageUrl(value) {
  try {
    const url = new URL(String(value || ""));
    const host = url.hostname.toLowerCase();
    return url.protocol === "https:" && [
      "media.discordapp.net",
      "cdn.discordapp.com",
      "ddrxgn8ucibei.cloudfront.net",
    ].some((allowed) => host === allowed || host.endsWith(`.${allowed}`)) ||
      url.protocol === "https:" && [
        ".uol.com.br",
        ".imguol.com",
        ".imguol.com.br",
      ].some((suffix) => host.endsWith(suffix));
  } catch {
    return false;
  }
}

// Preserve the pre-upgrade request hash so existing idempotency keys remain valid.
function normalizePreviewForHash(payload, link) {
  const preview = payload?.preview && typeof payload.preview === "object"
    ? payload.preview
    : {};
  const title = String(preview.title || payload?.title || "").trim().slice(0, 500);
  const summary = String(preview.summary || "").trim().slice(0, 2_000);
  const imageUrl = String(preview.imageUrl || "").trim();
  return {
    link,
    title: title || "Clube UOL",
    summary,
    type: "website",
    imageUrl: !imageUrl || allowedImageUrl(imageUrl) ? imageUrl : "",
  };
}

function isDefinitiveRejection(response, result) {
  const code = String(result?.code || result?.error?.code || "").trim();
  if (["not_logged_in", "account_not_connected", "chat_not_found", "chat_read_only"]
    .includes(code)) return true;
  return [400, 401, 403, 404, 405, 413, 415, 422].includes(response.status);
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
    UPDATE deliveries SET status = 'accepted'
      WHERE status = 'sent';
    UPDATE deliveries SET status = 'unknown'
      WHERE status = 'pending';
  `);
  return database;
}

function ledgerSnapshot(database) {
  database.prepare("SELECT 1 AS ok").get();
  const row = database.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
      SUM(CASE WHEN status = 'unknown' THEN 1 ELSE 0 END) AS unknown,
      SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
    FROM deliveries
  `).get();
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, Number(value || 0)]),
  );
}

function reserveDelivery(database, key, hash, now) {
  database.exec("BEGIN IMMEDIATE");
  try {
    const existing = database.prepare(
      "SELECT * FROM deliveries WHERE idempotency_key = ?",
    ).get(key);
    if (existing) {
      let result;
      if (existing.request_hash !== hash) {
        result = { kind: "conflict" };
      } else if (["accepted", "sent"].includes(existing.status)) {
        result = { kind: "replay", response: JSON.parse(existing.response_json || "{}") };
      } else if (existing.status === "unknown") {
        result = { kind: "unknown" };
      } else if (existing.status === "pending") {
        result = { kind: "pending" };
      } else {
        database.prepare(
          "UPDATE deliveries SET status = 'pending', updated_at = ? WHERE idempotency_key = ?",
        ).run(now, key);
        result = { kind: "reserved" };
      }
      database.exec("COMMIT");
      return result;
    }
    database.prepare(
      `INSERT INTO deliveries(idempotency_key, request_hash, status, created_at, updated_at)
       VALUES (?, ?, 'pending', ?, ?)`,
    ).run(key, hash, now, now);
    database.exec("COMMIT");
    return { kind: "reserved" };
  } catch (error) {
    try {
      database.exec("ROLLBACK");
    } catch {}
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
  accountId,
  beeperAccessToken,
  beeperApiUrl = DEFAULT_BEEPER_URL,
  databasePath,
  fetchImpl = fetch,
  isTransportReady = () => true,
  now = () => new Date(),
  logger = console,
}) {
  if (!String(token || "").trim()) throw new Error("GATEWAY_TOKEN is required");
  if (!String(chatId || "").trim()) throw new Error("BEEPER_CHAT_ID is required");
  if (!String(accountId || "").trim()) throw new Error("BEEPER_ACCOUNT_ID is required");
  if (!String(beeperAccessToken || "").trim()) throw new Error("BEEPER_ACCESS_TOKEN is required");
  if (!String(databasePath || "").trim()) throw new Error("DATA_PATH is required");
  const database = openDatabase(databasePath);
  const baseUrl = String(beeperApiUrl).replace(/\/+$/, "");

  async function readiness(includeLedger = false) {
    let ledger;
    try {
      ledger = ledgerSnapshot(database);
    } catch {
      return {
        status: 503,
        body: {
          ok: false,
          code: "delivery_ledger_not_ready",
          components: { transport: false, beeperApi: false, ledger: false },
        },
      };
    }
    if (!isTransportReady()) {
      return {
        status: 503,
        body: {
          ok: false,
          code: "beeper_transport_not_ready",
          components: { transport: false, beeperApi: false, ledger: true },
          ...(includeLedger ? { ledger } : {}),
        },
      };
    }
    try {
      const headers = { Authorization: `Bearer ${beeperAccessToken}` };
      const [accountsResponse, chatResponse] = await Promise.all([
        fetchImpl(`${baseUrl}/v1/accounts`, {
          headers,
          signal: AbortSignal.timeout(3_000),
        }),
        fetchImpl(`${baseUrl}/v1/chats/${encodeURIComponent(chatId)}`, {
          headers,
          signal: AbortSignal.timeout(3_000),
        }),
      ]);
      if (!accountsResponse.ok || !chatResponse.ok) {
        return {
          status: 503,
          body: {
            ok: false,
            code: "beeper_not_ready",
            components: { transport: true, beeperApi: false, ledger: true },
            ...(includeLedger ? { ledger } : {}),
          },
        };
      }
      const [accounts, chat] = await Promise.all([
        accountsResponse.json(),
        chatResponse.json(),
      ]);
      const accountReady = Array.isArray(accounts) && accounts.some((account) =>
        account?.accountID === accountId &&
        account?.network === "WhatsApp" &&
        account?.status === "connected"
      );
      const chatReady = chat?.id === chatId &&
        chat?.accountID === accountId &&
        chat?.network === "WhatsApp" &&
        chat?.isReadOnly !== true;
      if (!accountReady || !chatReady) {
        return {
          status: 503,
          body: {
            ok: false,
            code: "beeper_destination_not_ready",
            components: { transport: true, beeperApi: false, ledger: true },
            ...(includeLedger ? { ledger } : {}),
          },
        };
      }
      return {
        status: 200,
        body: {
          ok: true,
          components: { transport: true, beeperApi: true, ledger: true },
          deliveryConfirmation: "accepted_by_beeper_api",
          ...(includeLedger ? { ledger } : {}),
        },
      };
    } catch {
      return {
        status: 503,
        body: {
          ok: false,
          code: "beeper_not_ready",
          components: { transport: true, beeperApi: false, ledger: true },
          ...(includeLedger ? { ledger } : {}),
        },
      };
    }
  }

  return async function handler(request) {
    const url = new URL(request.url);
    const requestId = randomUUID();
    const startedAt = Date.now();
    let idempotencyDigest;
    let authenticated = false;
    const respond = (status, body, details = {}) => {
      const shouldLog = authenticated &&
        ["/v1/readyz", "/v1/send-offer"].includes(url.pathname);
      if (shouldLog) {
        auditLog(logger, status >= 400 ? "warn" : "info", "beeper_gateway_request", {
          requestId,
          method: request.method,
          path: safeRoute(url.pathname),
          status,
          code: String(body?.code || (status < 400 ? "ok" : "request_failed")),
          durationMs: Math.max(0, Date.now() - startedAt),
          ...(idempotencyDigest ? { idempotencyDigest } : {}),
          ...details,
        });
      }
      return json(status, body, { "X-Request-ID": requestId });
    };
    if (request.method === "GET" && url.pathname === "/livez") {
      return respond(200, { ok: true });
    }
    if (request.method === "GET" && url.pathname === "/readyz") {
      const result = await readiness();
      return respond(result.status, result.body);
    }
    if (request.method === "GET" && url.pathname === "/v1/readyz") {
      const suppliedToken = request.headers.get("Authorization")?.replace(/^Bearer\s+/i, "") || "";
      if (!secureEqual(suppliedToken, token)) return respond(401, { code: "unauthorized" });
      authenticated = true;
      const result = await readiness(true);
      return respond(result.status, result.body);
    }
    if (request.method !== "POST" || url.pathname !== "/v1/send-offer") {
      return respond(404, { code: "not_found" });
    }

    const suppliedToken = request.headers.get("Authorization")?.replace(/^Bearer\s+/i, "") || "";
    if (!secureEqual(suppliedToken, token)) return respond(401, { code: "unauthorized" });
    authenticated = true;
    const idempotencyKey = String(request.headers.get("Idempotency-Key") || "").trim();
    idempotencyDigest = keyDigest(idempotencyKey);
    if (!/^[A-Za-z0-9:._-]{8,200}$/.test(idempotencyKey)) {
      return respond(400, { code: "invalid_idempotency_key" });
    }

    let payload;
    try {
      payload = await readJsonBody(request);
    } catch (error) {
      return respond(Number(error.status || 400), { code: error.message });
    }
    const link = String(payload?.link || "").trim();
    const text = String(payload?.text || "").trim();
    if (!allowedOfferUrl(link) || !text || text.length > 8_000 || !text.includes(link)) {
      return respond(400, { code: "invalid_offer" });
    }

    const normalized = {
      link,
      text,
      preview: normalizePreviewForHash(payload, link),
    };
    const reserved = reserveDelivery(
      database,
      idempotencyKey,
      requestHash(normalized),
      now().toISOString(),
    );
    if (reserved.kind === "conflict") return respond(409, { code: "idempotency_conflict" });
    if (reserved.kind === "unknown") return respond(409, { code: "delivery_unknown" });
    if (reserved.kind === "pending") return respond(409, { code: "delivery_pending" });
    if (reserved.kind === "replay") {
      return respond(200, { ...reserved.response, replayed: true }, { replayed: true });
    }

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
      return respond(503, { code: "delivery_unknown" });
    }
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      const definitive = isDefinitiveRejection(response, result);
      updateDelivery(
        database,
        idempotencyKey,
        definitive ? "failed" : "unknown",
        {},
        now().toISOString(),
      );
      return respond(definitive ? 502 : 503, {
        code: definitive ? "beeper_rejected" : "delivery_unknown",
        status: response.status,
      });
    }
    const pendingMessageID = String(result?.pendingMessageID || "");
    if (!pendingMessageID) {
      updateDelivery(database, idempotencyKey, "unknown", {}, now().toISOString());
      return respond(503, { code: "delivery_unknown" });
    }
    const accepted = {
      accepted: true,
      pendingMessageID,
      deliveryState: "accepted_by_beeper_api",
    };
    updateDelivery(database, idempotencyKey, "accepted", accepted, now().toISOString());
    return respond(202, accepted, { deliveryState: accepted.deliveryState });
  };
}
