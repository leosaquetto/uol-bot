import { DurableObject } from "cloudflare:workers";
import { AsyncLocalStorage } from "node:async_hooks";

import {
  buildDedupeKeys,
  buildDiscussionCommentChunks,
  cleanText,
  buildApiHealthSnapshot,
  buildApiSnapshotFingerprint,
  decideShadowDelivery,
  dedupeCards,
  estimateDailyRowWrites,
  evaluateDetailQuality,
  isTicketCampaign,
  maintenanceRetryAt,
  normalizeTicketProbeAt,
  observationFreshnessMinutes,
  offerIdentityCompatible,
  offerIdentityKeys,
  offerSourceKey,
  parseRuntimeSnapshot,
  rollingReadEstimate,
  storageReadBudget,
  shouldPersistRunSummary,
  shouldReconcileHtmlSnapshot,
  shouldTouchObservation,
} from "./core.js";
import {
  cacheDiscordOfferImage,
  discordConfiguration,
  editDiscordOffer,
  getDiscordMessageImageProxy,
  sendDiscordOffer,
  sendDiscordOperationsAlert,
} from "./discord.js";
import {
  beeperDeliveryErrorCode,
  beeperDeliveryIdempotencyKey,
  beeperDestinationKey,
  beeperGatewayConfiguration,
  probeBeeperGateway,
  sendBeeperOffer,
} from "./beeper.js";
import {
  fetchOffersFromApi,
  mergeOfferCards,
  prepareImmediateApiOffer,
  ticketApiConfiguration,
} from "./uol-api.js";
import { authorizationExpiresAt } from "./uol-auth.js";
import {
  editMainOfferMedia,
  editSoldOutMessage,
  editMainOfferMessage,
  forwardToCanal2,
  sendMainOffer,
  sendDiscussionComment,
  sendOperationsAlert,
  TELEGRAM_MUTATION_TIMEOUT_SECONDS,
  telegramConfiguration,
  registerTelegramWebhook,
  getTelegramWebhookInfo,
} from "./telegram.js";
import {
  buildIncidentSignals,
  buildLatencyMetrics,
  buildOperationsAlert,
} from "./operations.js";
import {
  compareOfferSources,
  sourceSnapshotSignature,
  summarizeSourceComparison,
} from "./source-health.js";
import {
  fetchDiscordOfferDetail,
  mergeDiscordOfferDetail,
} from "./discord-detail.js";
import { renderDashboard } from "./dashboard.js";
import { nextImageCircuitState } from "./image-strategy.js";
import {
  classifyDeliveryRow,
  deliveryConfiguration,
  deliveryRetryAt,
  isAmbiguousDeliveryError,
} from "./delivery-state.js";
import {
  recordDeliveryEvent as writeDeliveryEvent,
  summarizeDeliveryTimeline,
  trimDeliveryEvents,
} from "./delivery-ledger.js";
import { isTelegramMessageMissingError } from "./transport-error.js";
import { classifyKnownMaintenanceRepair } from "./maintenance-repair.js";
import { htmlReconciliationDue } from "./scan-policy.js";
import {
  chooseDeliveryBudget,
  isRecentDeliveryDecision,
  RECENT_DELIVERY_PRIORITY_MS,
  summarizeQueueSlo,
} from "./queue-policy.js";
import {
  deferredMainDeliveryState,
  lateImageUpgradeDue,
  mainImageDeliveryOffer,
} from "./image-deadline.js";
import {
  nextTicketProbeState,
  probeTicketOfferWithControl,
  ticketProbeBudget,
} from "./ticket-soldout-probe.js";
import { readinessChecksOk } from "./health-contract.js";

const BASE_URL = "https://clube.uol.com.br";
const LIST_URL = `${BASE_URL}/?order=new`;
const TICKET_LIST_URL = `${BASE_URL}/?categoria=ingressosexclusivos&order=new`;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36";
const INSTANCE_NAME = "clube-uol-global-monitor";
const MAINTENANCE_INSTANCE_NAME = "clube-uol-maintenance";
const MAX_HTML_BYTES = 2_000_000;
const DURABLE_OBJECT_FREE_ROWS_READ_LIMIT = 5_000_000;
const DURABLE_OBJECT_CRITICAL_READ_RESERVE = 1_000_000;
const READINESS_CACHE_TTL_MS = 15_000;
const BEEPER_RECOVERY_METADATA_KEY = "beeper_delivery_offer_ids_applied_v3";
const BEEPER_RESPONSE_REPLAY_METADATA_KEY = "beeper_response_replay_policy";
const BEEPER_RESPONSE_REPLAY_POLICY = "v1";
const IDENTITY_POLICY_METADATA_KEY = "offer_identity_policy";
const IDENTITY_POLICY_VERSION = "v2";
const IDENTITY_QUERY_CHUNK_SIZE = 700;
const OPS_HEALTH_INTERVAL_SECONDS = 5 * 60;
const STORAGE_STAGE_NAMES = [
  "primary",
  "delivery",
  "tickets",
  "maintenanceLedger",
  "html",
  "comments",
  "images",
  "guard",
];

function emptyStorageStages() {
  return Object.fromEntries(STORAGE_STAGE_NAMES.map((stage) => [stage, 0]));
}

const PUBLIC_READINESS_CACHE_TTL_MS = 15_000;
let publicReadinessCache = {
  versionId: "",
  expiresAt: 0,
  payload: null,
};

function envNumber(env, name, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(env[name] || ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function ticketProbeNextAt(env, now = new Date()) {
  const base = now instanceof Date ? now : new Date(now);
  const safeBase = Number.isNaN(base.getTime()) ? new Date() : base;
  const delaySeconds = envNumber(
    env,
    "TICKET_SOLD_OUT_PROBE_DELAY_SECONDS",
    60,
    30,
    300,
  );
  return new Date(safeBase.getTime() + delaySeconds * 1_000).toISOString();
}

function sanitizeError(error) {
  return cleanText(error?.message || error).slice(0, 240);
}

function deliveryMode(env) {
  return String(env.DELIVERY_MODE || "shadow").trim().toLowerCase() === "live"
    ? "live"
    : "shadow";
}

function logEvent(level, event, data = {}) {
  const payload = {
    level,
    event,
    timestamp: new Date().toISOString(),
    ...data,
  };
  const output = JSON.stringify(payload);
  if (level === "error") console.error(output);
  else console.log(output);
}

function jsonResponse(payload, status = 200) {
  return Response.json(payload, {
    status,
    headers: {
      "Cache-Control": "no-store",
      "X-Robots-Tag": "noindex, nofollow",
    },
  });
}

async function readOperatorJson(request) {
  try {
    return await request.json();
  } catch {
    throw Object.assign(new Error("invalid_json"), { operatorInput: true });
  }
}

async function operatorActionResponse(action) {
  try {
    return jsonResponse(await action());
  } catch (error) {
    const code = sanitizeError(error);
    if (error?.operatorInput || code === "invalid_json" || code.startsWith("delivery_")) {
      return jsonResponse({ ok: false, error: code }, 400);
    }
    throw error;
  }
}

async function constantTimeEqual(left, right) {
  const encoder = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(String(left || ""))),
    crypto.subtle.digest("SHA-256", encoder.encode(String(right || ""))),
  ]);
  return crypto.subtle.timingSafeEqual(a, b);
}

async function isAuthorized(request, env) {
  const expected = String(env.ADMIN_TOKEN || "").trim();
  const authorization = request.headers.get("Authorization") || "";
  const supplied = authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
  if (!expected || !supplied || supplied.length > 4_096) return false;
  return constantTimeEqual(expected, supplied);
}

async function isDashboardAuthorized(request, env) {
  if (await isAuthorized(request, env)) return true;
  const expected = String(env.ADMIN_TOKEN || "").trim();
  const authorization = request.headers.get("Authorization") || "";
  if (!expected || !authorization.startsWith("Basic ")) return false;
  try {
    const decoded = atob(authorization.slice(6));
    const separator = decoded.indexOf(":");
    const username = separator >= 0 ? decoded.slice(0, separator) : "";
    const password = separator >= 0 ? decoded.slice(separator + 1) : "";
    return username === "admin" && password.length <= 4_096 &&
      await constantTimeEqual(expected, password);
  } catch {
    return false;
  }
}

function dashboardUnauthorized() {
  return new Response("Autenticação necessária", {
    status: 401,
    headers: {
      "WWW-Authenticate": 'Basic realm="Clube UOL Monitor", charset="UTF-8"',
      "Cache-Control": "no-store",
    },
  });
}

function freshUrl(value, marker) {
  const url = new URL(value);
  url.searchParams.set(marker, String(Date.now()));
  return url.href;
}

async function fetchHtml(url, marker, fetchImpl = fetch, timeoutMs = 20_000) {
  const response = await fetchImpl(freshUrl(url, marker), {
    headers: {
      "User-Agent": USER_AGENT,
      "Cache-Control": "no-cache, no-store, max-age=0",
      Pragma: "no-cache",
      Accept: "text/html,application/xhtml+xml",
    },
    cf: {
      cacheTtl: 0,
      cacheEverything: false,
    },
    signal: AbortSignal.timeout(Math.min(20_000, Math.max(1_000, Number(timeoutMs) || 20_000))),
  });

  if (!response.ok) throw new Error(`uol_http_${response.status}`);
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.toLowerCase().includes("text/html")) {
    throw new Error("uol_content_type_invalido");
  }
  const contentLength = Number(response.headers.get("content-length") || 0);
  if (contentLength > MAX_HTML_BYTES) throw new Error("uol_html_excede_limite");
  return response;
}

async function drainRewriter(rewriter, response) {
  const transformed = rewriter.transform(response);
  if (transformed.body) {
    await transformed.body.pipeTo(new WritableStream());
  }
}

async function parseListing(response) {
  const cards = [];
  let current = null;
  const selector = "div.beneficio";

  const rewriter = new HTMLRewriter()
    .on(selector, {
      element(element) {
        current = {
          title: "",
          link: "",
          category: cleanText(element.getAttribute("data-categoria")),
          cardImageUrl: "",
          partnerImageUrl: "",
          partnerName: "",
        };
        cards.push(current);
      },
    })
    .on(`${selector} a[href]`, {
      element(element) {
        if (!current || current.link) return;
        current.link = new URL(element.getAttribute("href") || "", BASE_URL).href;
      },
    })
    .on(`${selector} p.titulo`, {
      text(text) {
        if (current && current.title.length < 500) current.title += text.text;
      },
    })
    .on(`${selector} .thumb[data-src]`, {
      element(element) {
        if (!current || current.cardImageUrl) return;
        current.cardImageUrl = new URL(element.getAttribute("data-src") || "", BASE_URL).href;
      },
    })
    .on(`${selector} img[data-src*="/parceiros/"]`, {
      element(element) {
        if (!current || current.partnerImageUrl) return;
        current.partnerImageUrl = new URL(element.getAttribute("data-src") || "", BASE_URL).href;
        current.partnerName = cleanText(element.getAttribute("title"));
      },
    });

  await drainRewriter(rewriter, response);
  return dedupeCards(cards);
}

async function fetchListing(
  fetchImpl = fetch,
  url = LIST_URL,
  marker = "_uol_shadow_ts",
  timeoutMs = 20_000,
) {
  const response = await fetchHtml(url, marker, fetchImpl, timeoutMs);
  return parseListing(response);
}

async function timedCards(fetcher) {
  const startedAt = new Date();
  const cards = await fetcher();
  const completedAt = new Date();
  return {
    cards,
    startedAt: startedAt.toISOString(),
    completedAt: completedAt.toISOString(),
    elapsedMs: completedAt.getTime() - startedAt.getTime(),
  };
}

async function settled(promise) {
  try {
    return { status: "fulfilled", value: await promise };
  } catch (reason) {
    return { status: "rejected", reason };
  }
}

function compactTicketListingCards(cards) {
  return (cards || []).filter(isTicketCampaign).map((card) => ({
    id: String(card.id || ""),
    link: String(card.link || ""),
    previewTitle: String(card.previewTitle || ""),
    category: String(card.category || "campanhasdeingresso"),
    cardImageUrl: String(card.cardImageUrl || ""),
    partnerImageUrl: String(card.partnerImageUrl || ""),
    partnerName: String(card.partnerName || ""),
  }));
}

async function runBounded(items, limit, handler) {
  const values = Array.from(items || []);
  if (!values.length) return [];
  const results = new Array(values.length);
  let cursor = 0;
  const workers = Array.from(
    { length: Math.min(values.length, Math.max(1, Number(limit || 1))) },
    async () => {
      while (cursor < values.length) {
        const index = cursor;
        cursor += 1;
        results[index] = await handler(values[index], index);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

function telegramOfferUrls(message) {
  const urls = [];
  for (const [text, entities] of [
    [String(message?.text || ""), message?.entities],
    [String(message?.caption || ""), message?.caption_entities],
  ]) {
    for (const entity of Array.isArray(entities) ? entities : []) {
      if (entity?.type === "text_link" && entity.url) urls.push(String(entity.url));
      if (entity?.type === "url") {
        urls.push(text.slice(Number(entity.offset || 0), Number(entity.offset || 0) +
          Number(entity.length || 0)));
      }
    }
    urls.push(...text.match(/https:\/\/clube\.uol\.com\.br\/[^\s<>()]+/gi) || []);
  }
  return [...new Set(urls)].filter((value) => {
    try {
      const url = new URL(value);
      return url.protocol === "https:" && url.hostname === "clube.uol.com.br";
    } catch {
      return false;
    }
  });
}

function prepareImmediateListingOffer(card) {
  const detail = {
    title: cleanText(card?.previewTitle),
    validity: "",
    description: "",
    imageUrl: String(card?.cardImageUrl || "").trim(),
  };
  return {
    ...card,
    ...detail,
    quality: evaluateDetailQuality(detail),
    // HTML is only a degraded discovery source. A valid title + public link
    // is enough to notify now; richer detail never gates an expiring offer.
    detailOk: true,
    detailError: "",
    detailElapsedMs: 0,
  };
}


function rowToPublicDecision(row) {
  return {
    id: row.id,
    title: row.title || row.preview_title,
    path: new URL(row.link).pathname,
    category: row.category || "",
    status: row.status,
    detailQuality: row.detail_quality || "",
    descriptionLength: Number(row.description_length || 0),
    detailError: row.detail_error || "",
    detailRepairAttempts: Number(row.detail_repair_attempts || 0),
    detailRepairError: row.detail_repair_error || "",
    detailRepairedAt: row.detail_repaired_at || "",
    firstSeenAt: row.first_seen_at,
    decisionAt: row.decision_at || "",
    wouldSendMain: Boolean(row.would_send_main),
    wouldSendCanal2: Boolean(row.would_send_canal2),
    discardReason: row.discard_reason || "",
    soldOutAt: row.sold_out_at || "",
    restockedAt: row.restocked_at || "",
    mainSent: Boolean(row.main_sent_at),
    canal2Sent: Boolean(row.canal2_sent_at),
    discordSent: Boolean(row.discord_sent_at),
    discordMessageId: String(row.discord_message_id || ""),
    discordOfferMessageId: String(row.discord_image_cache_message_id || ""),
    discordOfferSent: Boolean(row.discord_image_cache_message_id),
    mainMessageId: Number(row.main_message_id || 0),
    mainMessageKind: row.main_message_kind || "",
    telegramImageStrategy: row.telegram_image_strategy || "",
    mainImageUpgradeAttempts: Number(row.main_image_upgrade_attempts || 0),
    mainImageUpgradeError: row.main_image_upgrade_error || "",
    discordImageProxyCached: Boolean(row.discord_image_proxy_url),
    canal2MessageId: Number(row.canal2_message_id || 0),
    mainDeliveryError: row.main_delivery_error || "",
    canal2DeliveryError: row.canal2_delivery_error || "",
    deliveryDeadLetterAt: row.delivery_dead_letter_at || "",
    deliveryDeadLetterReason: row.delivery_dead_letter_reason || "",
    deliveryUnknownAt: row.delivery_unknown_at || "",
    deliveryUnknownTarget: row.delivery_unknown_target || "",
    deliveryQuarantineReason: row.delivery_quarantine_reason || "",
    commentSent: Boolean(row.comment_sent_at),
    commentChunksSent: Number(row.comment_chunks_sent || 0),
    commentDeliveryError: row.comment_delivery_error || "",
    soldOutMainSynced: Boolean(row.main_sold_out_synced_at),
    soldOutCanal2Synced: Boolean(row.canal2_sold_out_synced_at),
    soldOutMainAttempts: Number(row.main_sold_out_attempts || 0),
    soldOutMainError: row.main_sold_out_error || "",
    soldOutCanal2Attempts: Number(row.canal2_sold_out_attempts || 0),
    soldOutCanal2Error: row.canal2_sold_out_error || "",
    ticketProbeNextAt: row.ticket_probe_next_at || "",
    ticketProbeLastAt: row.ticket_probe_last_at || "",
    ticketProbeLastResult: row.ticket_probe_last_result || "",
    ticketProbeGoneCount: Number(row.ticket_probe_gone_count || 0),
    ticketProbeAttempts: Number(row.ticket_probe_attempts || 0),
  };
}

function rowToOffer(row) {
  return {
    id: row.id,
    link: row.link,
    previewTitle: row.preview_title,
    title: row.title,
    category: row.category,
    cardImageUrl: row.card_image_url,
    partnerImageUrl: row.partner_image_url,
    partnerName: row.partner_name,
    imageUrl: row.image_url,
    discordImageProxyUrl: row.discord_image_proxy_url,
    validity: row.validity,
    description: row.description,
    firstSeenAt: row.first_seen_at,
  };
}

export class UolTelegramShadow extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.scanInFlight = false;
    this.maintenanceInFlight = false;
    this.beeperDeliveryInFlight = false;
    this.metadataCache = new Map();
    this.runtimeSnapshotCache = new Map();
    this.readinessCache = null;
    this.storageContext = new AsyncLocalStorage();
    this.storageUsageReady = false;
    this.storageUsage = {
      day: new Date().toISOString().slice(0, 10),
      rowsRead: 0,
      rowsWritten: 0,
      stageReads: emptyStorageStages(),
      stageWrites: emptyStorageStages(),
      primaryMaxRowsRead: 0,
      primaryEstimatedRowsRead: 0,
      primaryLastCompletedAt: "",
      primaryLastVersionId: "",
      primaryLastRowsRead: 0,
      primaryLastSuccessful: false,
      primaryLastSuccessfulAt: "",
      primaryLastSuccessfulVersionId: "",
      primaryLastSuccessfulRowsRead: 0,
      maintenanceMaxRowsRead: 0,
      maintenanceSkipped: 0,
      ticketProbeCount: 0,
      lastPersistedAt: "",
    };
    ctx.blockConcurrencyWhile(async () => {
      this.migrate();
      this.loadStorageUsage();
    });
  }

  sqlExec(query, ...bindings) {
    const cursor = this.ctx.storage.sql.exec(query, ...bindings);
    if (!this.storageUsageReady) return cursor;
    return this.trackSqlCursor(cursor, this.storageContext.getStore());
  }

  scheduleTicketAbsenceProbe(offerId, nextAt) {
    this.sqlExec(
      `INSERT INTO ticket_probe_state(
         offer_id, next_at, last_result, gone_count, attempts
       )
       SELECT id, ?, 'listing_absence_pending', 0, 0 FROM offers
       WHERE id = ? AND link LIKE '%/campanhasdeingresso/%'
       ON CONFLICT(offer_id) DO UPDATE SET
         next_at = CASE WHEN ticket_probe_state.next_at = ''
           THEN excluded.next_at ELSE ticket_probe_state.next_at END,
         last_result = CASE WHEN ticket_probe_state.next_at = ''
           THEN excluded.last_result ELSE ticket_probe_state.last_result END,
         gone_count = CASE WHEN ticket_probe_state.next_at = ''
           THEN 0 ELSE ticket_probe_state.gone_count END,
         attempts = CASE WHEN ticket_probe_state.next_at = ''
           THEN 0 ELSE ticket_probe_state.attempts END`,
      normalizeTicketProbeAt(nextAt),
      offerId,
    );
  }

  clearTicketAbsenceProbe(offerId) {
    this.sqlExec(
      `UPDATE ticket_probe_state SET next_at = '',
         last_result = 'available:listing', gone_count = 0, attempts = 0
       WHERE offer_id = ? AND (
         next_at <> '' OR last_result = 'listing_absence_pending' OR
         gone_count <> 0 OR attempts <> 0
       )`,
      offerId,
    );
  }

  trackSqlCursor(cursor, storageContext = this.storageContext.getStore()) {
    let accountedRead = 0;
    let accountedWritten = 0;
    const sync = () => {
      const rowsRead = Number(cursor.rowsRead || 0);
      const rowsWritten = Number(cursor.rowsWritten || 0);
      this.recordStorageUsage(
        Math.max(0, rowsRead - accountedRead),
        Math.max(0, rowsWritten - accountedWritten),
        storageContext,
      );
      accountedRead = rowsRead;
      accountedWritten = rowsWritten;
    };
    const wrapIterator = (iterator) => ({
      next: (...args) => {
        try {
          return iterator.next(...args);
        } finally {
          sync();
        }
      },
      return: (...args) => {
        try {
          return iterator.return ? iterator.return(...args) : { done: true };
        } finally {
          sync();
        }
      },
      throw: (...args) => {
        try {
          if (iterator.throw) return iterator.throw(...args);
          throw args[0];
        } finally {
          sync();
        }
      },
      [Symbol.iterator]() {
        return this;
      },
    });
    sync();
    return new Proxy(cursor, {
      get: (target, property) => {
        if (["toArray", "one", "next"].includes(property)) {
          return (...args) => {
            try {
              return target[property](...args);
            } finally {
              sync();
            }
          };
        }
        if (property === "raw") {
          return (...args) => wrapIterator(target.raw(...args));
        }
        if (property === Symbol.iterator) {
          return () => wrapIterator(target[Symbol.iterator]());
        }
        if (property === "rowsRead" || property === "rowsWritten") sync();
        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  }

  loadStorageUsage() {
    const cursor = this.ctx.storage.sql.exec(
      "SELECT value FROM metadata WHERE key = 'runtime:storage_usage' LIMIT 1",
    );
    const row = cursor.toArray()[0];
    let previous = {};
    try {
      previous = JSON.parse(row?.value || "{}");
    } catch {
      previous = {};
    }
    const day = new Date().toISOString().slice(0, 10);
    if (previous.day === day) {
      this.storageUsage = {
        ...this.storageUsage,
        ...previous,
        day,
        rowsRead: Number(previous.rowsRead || 0) + Number(cursor.rowsRead || 0),
        rowsWritten: Number(previous.rowsWritten || 0),
      };
    } else {
      this.storageUsage.day = day;
      this.storageUsage.rowsRead = Number(cursor.rowsRead || 0);
    }
    this.storageUsageReady = true;
  }

  rollStorageUsageDay(now = new Date()) {
    const day = now.toISOString().slice(0, 10);
    if (this.storageUsage.day === day) return;
    this.storageUsage = {
      day,
      rowsRead: 0,
      rowsWritten: 0,
      stageReads: emptyStorageStages(),
      stageWrites: emptyStorageStages(),
      primaryMaxRowsRead: 0,
      primaryEstimatedRowsRead: 0,
      primaryLastCompletedAt: "",
      primaryLastVersionId: "",
      primaryLastRowsRead: 0,
      primaryLastSuccessful: false,
      primaryLastSuccessfulAt: "",
      primaryLastSuccessfulVersionId: "",
      primaryLastSuccessfulRowsRead: 0,
      maintenanceMaxRowsRead: 0,
      maintenanceSkipped: 0,
      ticketProbeCount: 0,
      lastPersistedAt: "",
    };
  }

  recordStorageUsage(
    rowsRead = 0,
    rowsWritten = 0,
    storageContext = this.storageContext.getStore(),
  ) {
    this.rollStorageUsageDay();
    const normalizedRowsRead = Math.max(0, Number(rowsRead || 0));
    const normalizedRowsWritten = Math.max(0, Number(rowsWritten || 0));
    this.storageUsage.rowsRead += normalizedRowsRead;
    this.storageUsage.rowsWritten += normalizedRowsWritten;
    if (storageContext?.stage) {
      this.recordStorageStage(storageContext.stage, normalizedRowsRead, normalizedRowsWritten);
    }
    if (storageContext?.cycle) {
      storageContext.cycle.rowsRead += normalizedRowsRead;
      storageContext.cycle.rowsWritten += normalizedRowsWritten;
    }
  }

  recordStorageStage(stage, rowsRead = 0, rowsWritten = 0) {
    const key = STORAGE_STAGE_NAMES.includes(stage) ? stage : "primary";
    if (!this.storageUsage.stageReads || typeof this.storageUsage.stageReads !== "object") {
      this.storageUsage.stageReads = emptyStorageStages();
    }
    if (!this.storageUsage.stageWrites || typeof this.storageUsage.stageWrites !== "object") {
      this.storageUsage.stageWrites = emptyStorageStages();
    }
    this.storageUsage.stageReads[key] = Number(this.storageUsage.stageReads[key] || 0) +
      Math.max(0, Number(rowsRead || 0));
    this.storageUsage.stageWrites[key] = Number(this.storageUsage.stageWrites[key] || 0) +
      Math.max(0, Number(rowsWritten || 0));
  }

  async withStorageStage(stage, action) {
    const current = this.storageContext.getStore() || {};
    const nextStage = STORAGE_STAGE_NAMES.includes(stage) ? stage : current.stage;
    return this.storageContext.run({ ...current, stage: nextStage }, action);
  }

  async withStorageCycle(kind, action) {
    const current = this.storageContext.getStore();
    if (current?.cycle) return action();
    const stage = kind === "maintenance" ? "maintenanceLedger" : "primary";
    return this.storageContext.run({
      stage,
      cycle: { kind, rowsRead: 0, rowsWritten: 0 },
    }, action);
  }

  withDetachedStorageCycle(kind, stage, action) {
    const context = {
      stage: STORAGE_STAGE_NAMES.includes(stage) ? stage : "primary",
      cycle: { kind, rowsRead: 0, rowsWritten: 0 },
    };
    return this.storageContext.run(context, async () => {
      const startedRowsRead = Number(this.storageUsage.rowsRead || 0);
      try {
        return await action();
      } finally {
        try {
          this.completeStorageUsageCycle(kind, startedRowsRead);
        } catch (error) {
          logEvent("error", "uol_storage_usage_persist_failed", {
            cycle: kind,
            error: sanitizeError(error),
          });
        }
      }
    });
  }

  scheduleDiscordDelivery({
    priorityIds = [],
    rows = [],
    recoveryRows = [],
    source = "scan",
  } = {}) {
    const suppliedRows = [...(Array.isArray(rows) ? rows : []),
      ...(Array.isArray(recoveryRows) ? recoveryRows : [])].filter(
      (row, index, candidates) => row?.id &&
        candidates.findIndex((candidate) => candidate?.id === row.id) === index,
    );
    if (suppliedRows.length === 0) return Promise.resolve({
      mainSent: 0,
      canal2Sent: 0,
      discordSent: 0,
      failed: 0,
      selectedRows: [],
    });
    const task = this.withDetachedStorageCycle(
      "delivery",
      "delivery",
      async () => {
        const result = await this.processDeliveryQueue(new Date(), {
          priorityIds,
          rows: suppliedRows,
          targetNames: ["discord"],
        });
        if (Number(result.discordSent || 0) > 0) {
          const beeper = await this.processCriticalBeeperDeliveryQueue(new Date());
          result.beeperSent = Number(beeper.sent || 0);
          result.beeperFailed = Number(beeper.failed || 0);
        }
        return result;
      },
    ).then((result) => {
      logEvent(result.failed ? "warn" : "info", "uol_telegram_discord_delivery", {
        source,
        discordSent: Number(result.discordSent || 0),
        failed: Number(result.failed || 0),
        deferred: result.deferred === true,
        deferredReason: String(result.deferredReason || ""),
      });
      return result;
    }).catch((error) => {
      logEvent("error", "uol_telegram_discord_delivery_failed", {
        source,
        error: sanitizeError(error),
      });
      return {
        mainSent: 0,
        canal2Sent: 0,
        discordSent: 0,
        failed: 1,
        selectedRows: [],
        error: sanitizeError(error),
      };
    });
    this.ctx.waitUntil(task);
    return task;
  }

  scheduleCriticalBeeperDelivery(source = "alarm") {
    const now = new Date();
    const configuration = beeperGatewayConfiguration(this.env);
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    if (!this.beeperCriticalDeliveryDue(now, configuration, maxAttempts)) {
      return Promise.resolve({
        sent: 0,
        failed: 0,
        queued: 0,
        blocked: false,
        skipped: true,
      });
    }
    const task = this.withDetachedStorageCycle(
      "delivery",
      "delivery",
      () => this.processCriticalBeeperDeliveryQueue(now),
    ).catch((error) => {
      logEvent("error", "uol_beeper_critical_delivery_failed", {
        source,
        error: sanitizeError(error),
      });
      return { sent: 0, failed: 1, queued: 0, blocked: false };
    });
    this.ctx.waitUntil(task);
    return task;
  }

  storageUsageSnapshot(now = new Date()) {
    this.rollStorageUsageDay(now);
    const limit = envNumber(
      this.env,
      "DURABLE_OBJECT_ROWS_READ_DAILY_LIMIT",
      DURABLE_OBJECT_FREE_ROWS_READ_LIMIT,
      1_000_000,
      100_000_000_000,
    );
    const intervalSeconds = envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600);
    return {
      ...this.storageUsage,
      ...storageReadBudget({
        rowsRead: this.storageUsage.rowsRead,
        primaryEstimatedRowsRead: this.storageUsage.primaryEstimatedRowsRead,
        now,
        pollIntervalSeconds: intervalSeconds,
        limit,
        reserveFloor: DURABLE_OBJECT_CRITICAL_READ_RESERVE,
      }),
    };
  }

  completeStorageUsageCycle(kind, startedRowsRead, details = {}) {
    this.rollStorageUsageDay();
    // Reserva conservadora para getAlarm/setAlarm e para o próprio snapshot.
    this.recordStorageUsage(2, 1);
    const context = this.storageContext.getStore();
    const cycleRowsRead = context?.cycle?.kind === kind
      ? Math.max(0, Number(context.cycle.rowsRead || 0))
      : Math.max(
          0,
          Number(this.storageUsage.rowsRead || 0) - Number(startedRowsRead || 0),
        );
    if (kind === "primary") {
      const completedAt = normalizeTicketProbeAt(details.completedAt, new Date());
      const versionId = String(
        details.versionId || this.env.WORKER_VERSION?.id || "",
      ).trim();
      const previousVersionId = String(this.storageUsage.primaryLastVersionId || "").trim();
      const versionChanged = previousVersionId && versionId && previousVersionId !== versionId;
      this.storageUsage.primaryMaxRowsRead = versionChanged
        ? cycleRowsRead
        : Math.max(Number(this.storageUsage.primaryMaxRowsRead || 0), cycleRowsRead);
      this.storageUsage.primaryEstimatedRowsRead = versionChanged
        ? Math.min(512, Math.ceil(cycleRowsRead))
        : rollingReadEstimate(this.storageUsage.primaryEstimatedRowsRead, cycleRowsRead);
      this.storageUsage.primaryLastCompletedAt = completedAt;
      this.storageUsage.primaryLastVersionId = versionId;
      this.storageUsage.primaryLastRowsRead = cycleRowsRead;
      this.storageUsage.primaryLastSuccessful = details.successful === true;
      if (details.successful === true) {
        this.storageUsage.primaryLastSuccessfulAt = completedAt;
        this.storageUsage.primaryLastSuccessfulVersionId = versionId;
        this.storageUsage.primaryLastSuccessfulRowsRead = cycleRowsRead;
      }
    } else if (kind === "maintenance") {
      this.storageUsage.maintenanceMaxRowsRead = Math.max(
        Number(this.storageUsage.maintenanceMaxRowsRead || 0),
        cycleRowsRead,
      );
    }
    this.storageUsage.lastPersistedAt = new Date().toISOString();
    const serialized = JSON.stringify(this.storageUsage);
    this.ctx.storage.sql.exec(
      `INSERT INTO metadata(key, value) VALUES ('runtime:storage_usage', ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      serialized,
    );
    this.metadataCache.set("runtime:storage_usage", serialized);
    return cycleRowsRead;
  }

  migrate() {
    this.sqlExec(`
      CREATE TABLE IF NOT EXISTS _sql_schema_migrations (
        id INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS offers (
        id TEXT PRIMARY KEY,
        link TEXT NOT NULL,
        preview_title TEXT NOT NULL,
        title TEXT NOT NULL DEFAULT '',
        category TEXT NOT NULL DEFAULT '',
        card_image_url TEXT NOT NULL DEFAULT '',
        partner_image_url TEXT NOT NULL DEFAULT '',
        partner_name TEXT NOT NULL DEFAULT '',
        image_url TEXT NOT NULL DEFAULT '',
        validity TEXT NOT NULL DEFAULT '',
        description TEXT NOT NULL DEFAULT '',
        dedupe_key TEXT NOT NULL DEFAULT '',
        loose_dedupe_key TEXT NOT NULL DEFAULT '',
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        status TEXT NOT NULL,
        detail_quality TEXT NOT NULL DEFAULT '',
        detail_attempts INTEGER NOT NULL DEFAULT 0,
        detail_error TEXT NOT NULL DEFAULT '',
        decision_at TEXT NOT NULL DEFAULT '',
        would_send_main INTEGER NOT NULL DEFAULT 0,
        would_send_canal2 INTEGER NOT NULL DEFAULT 0,
        discard_reason TEXT NOT NULL DEFAULT '',
        missing_since TEXT NOT NULL DEFAULT '',
        absence_count INTEGER NOT NULL DEFAULT 0,
        sold_out_at TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS offers_first_seen_idx ON offers(first_seen_at DESC);
      CREATE INDEX IF NOT EXISTS offers_status_idx ON offers(status, first_seen_at DESC);
      CREATE INDEX IF NOT EXISTS offers_dedupe_idx ON offers(dedupe_key);
      CREATE INDEX IF NOT EXISTS offers_loose_dedupe_idx ON offers(loose_dedupe_key);
      CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT NOT NULL,
        finished_at TEXT NOT NULL,
        source TEXT NOT NULL,
        outcome TEXT NOT NULL,
        offers_seen INTEGER NOT NULL DEFAULT 0,
        new_offers INTEGER NOT NULL DEFAULT 0,
        enriched INTEGER NOT NULL DEFAULT 0,
        would_send_main INTEGER NOT NULL DEFAULT 0,
        would_send_canal2 INTEGER NOT NULL DEFAULT 0,
        sold_out_detected INTEGER NOT NULL DEFAULT 0,
        error TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS runs_started_idx ON runs(started_at DESC);
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (1);
    `);
    const currentVersion = Number(
      this.sqlExec("SELECT COALESCE(MAX(id), 0) AS version FROM _sql_schema_migrations")
        .one().version || 0,
    );
    if (currentVersion < 2) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_message_id INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN main_message_kind TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_sent_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_delivery_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN main_delivery_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_message_id INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN canal2_sent_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_delivery_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN canal2_delivery_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_sold_out_synced_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_sold_out_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN main_sold_out_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_sold_out_synced_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_sold_out_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN canal2_sold_out_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE runs ADD COLUMN main_sent INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE runs ADD COLUMN canal2_sent INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE runs ADD COLUMN delivery_failed INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE runs ADD COLUMN sold_out_main_edited INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE runs ADD COLUMN sold_out_canal2_edited INTEGER NOT NULL DEFAULT 0;
        INSERT INTO _sql_schema_migrations (id) VALUES (2);
        CREATE INDEX IF NOT EXISTS offers_delivery_idx
          ON offers(status, main_sent_at, canal2_sent_at, first_seen_at);
      `);
    }
    if (currentVersion < 3) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN title_validity_key TEXT NOT NULL DEFAULT '';
        CREATE INDEX IF NOT EXISTS offers_title_validity_idx
          ON offers(title_validity_key, main_sent_at);
        INSERT INTO _sql_schema_migrations (id) VALUES (3);
      `);
    }
    if (currentVersion < 4) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN discord_message_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_sent_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_delivery_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN discord_delivery_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discussion_message_id INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN comment_message_ids TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE offers ADD COLUMN comment_chunks_sent INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN comment_sent_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN comment_delivery_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN comment_delivery_error TEXT NOT NULL DEFAULT '';
        UPDATE offers
           SET discord_sent_at = main_sent_at
         WHERE main_sent_at <> ''
           AND link LIKE '%/campanhasdeingresso/%';
        INSERT INTO _sql_schema_migrations (id) VALUES (4);
        CREATE INDEX IF NOT EXISTS offers_comment_idx
          ON offers(discussion_message_id, comment_sent_at, first_seen_at);
        CREATE INDEX IF NOT EXISTS offers_discord_idx
          ON offers(discord_sent_at, first_seen_at);
      `);
    }
    if (currentVersion < 5) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS pending_discussion_forwards (
          origin_message_id INTEGER PRIMARY KEY,
          discussion_message_id INTEGER NOT NULL,
          received_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS pending_discussion_received_idx
          ON pending_discussion_forwards(received_at);
        INSERT INTO _sql_schema_migrations (id) VALUES (5);
      `);
    }
    if (currentVersion < 6) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS incidents (
          key TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          severity TEXT NOT NULL,
          summary TEXT NOT NULL,
          details TEXT NOT NULL DEFAULT '',
          first_detected_at TEXT NOT NULL,
          last_detected_at TEXT NOT NULL,
          last_attempted_at TEXT NOT NULL DEFAULT '',
          last_alerted_at TEXT NOT NULL DEFAULT '',
          resolved_at TEXT NOT NULL DEFAULT '',
          occurrence_count INTEGER NOT NULL DEFAULT 1,
          alert_error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS incidents_status_idx
          ON incidents(status, last_detected_at DESC);
        INSERT INTO _sql_schema_migrations (id) VALUES (6);
      `);
    }
    if (currentVersion < 7) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN telegram_photo_file_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN telegram_photo_file_unique_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN telegram_image_strategy TEXT NOT NULL DEFAULT '';
        CREATE TABLE IF NOT EXISTS source_observations (
          offer_key TEXT PRIMARY KEY,
          link TEXT NOT NULL,
          title TEXT NOT NULL DEFAULT '',
          api_first_seen_at TEXT NOT NULL DEFAULT '',
          api_last_seen_at TEXT NOT NULL DEFAULT '',
          listing_first_seen_at TEXT NOT NULL DEFAULT '',
          listing_last_seen_at TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS source_observations_recent_idx
          ON source_observations(api_last_seen_at, listing_last_seen_at);
        CREATE TABLE IF NOT EXISTS telegram_image_cache (
          image_key TEXT PRIMARY KEY,
          file_id TEXT NOT NULL,
          file_unique_id TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          last_used_at TEXT NOT NULL,
          use_count INTEGER NOT NULL DEFAULT 0,
          failure_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS telegram_image_cache_used_idx
          ON telegram_image_cache(last_used_at DESC);
        CREATE TABLE IF NOT EXISTS image_strategy_health (
          strategy TEXT PRIMARY KEY,
          state TEXT NOT NULL DEFAULT 'closed',
          consecutive_failures INTEGER NOT NULL DEFAULT 0,
          opened_until TEXT NOT NULL DEFAULT '',
          last_failure_at TEXT NOT NULL DEFAULT '',
          last_success_at TEXT NOT NULL DEFAULT '',
          last_error TEXT NOT NULL DEFAULT ''
        );
        INSERT OR IGNORE INTO image_strategy_health(strategy) VALUES
          ('file_id'), ('remote_url'), ('discord_proxy'), ('upload');
        INSERT INTO _sql_schema_migrations (id) VALUES (7);
      `);
    }
    if (currentVersion < 8) {
      this.sqlExec(`
        UPDATE offers
           SET main_sold_out_attempts = 0
         WHERE status = 'sold_out' AND main_sold_out_synced_at = '';
        UPDATE offers
           SET canal2_sold_out_attempts = 0
         WHERE status = 'sold_out' AND canal2_sold_out_synced_at = '';
        INSERT INTO _sql_schema_migrations (id) VALUES (8);
      `);
    }
    if (currentVersion < 9) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN detail_repair_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN detail_repair_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN detail_repaired_at TEXT NOT NULL DEFAULT '';
        INSERT INTO _sql_schema_migrations (id) VALUES (9);
        CREATE INDEX IF NOT EXISTS offers_detail_repair_idx
          ON offers(detail_repaired_at, detail_repair_attempts, first_seen_at);
      `);
    }
    if (currentVersion < 10) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN delivery_generation INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE offers ADD COLUMN main_delivery_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_delivery_in_flight_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_delivery_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_delivery_in_flight_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_delivery_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_delivery_in_flight_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN comment_delivery_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN comment_delivery_in_flight_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN comment_delivery_unknown_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN delivery_dead_letter_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN delivery_dead_letter_reason TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN delivery_unknown_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN delivery_unknown_target TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN delivery_quarantine_reason TEXT NOT NULL DEFAULT '';
        INSERT OR IGNORE INTO metadata(key, value) VALUES ('delivery_mode_generation', '1');
        INSERT INTO _sql_schema_migrations (id) VALUES (10);
        CREATE INDEX IF NOT EXISTS offers_delivery_retry_idx
          ON offers(status, delivery_generation, first_seen_at);
      `);
    }
    if (currentVersion < 11) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN status_before_sold_out TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN restocked_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_restock_synced_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_restock_synced_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_restock_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN canal2_restock_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN main_restock_error TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_restock_error TEXT NOT NULL DEFAULT '';
        INSERT INTO _sql_schema_migrations (id) VALUES (11);
      `);
    }
    if (currentVersion < 12) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN main_delivery_unknown_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_delivery_unknown_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_delivery_unknown_at TEXT NOT NULL DEFAULT '';
        UPDATE offers SET main_delivery_unknown_at = delivery_unknown_at
         WHERE delivery_unknown_target = 'main' AND delivery_unknown_at <> '';
        UPDATE offers SET canal2_delivery_unknown_at = delivery_unknown_at
         WHERE delivery_unknown_target = 'canal2' AND delivery_unknown_at <> '';
        UPDATE offers SET discord_delivery_unknown_at = delivery_unknown_at
         WHERE delivery_unknown_target = 'discord' AND delivery_unknown_at <> '';
        INSERT INTO _sql_schema_migrations (id) VALUES (12);
      `);
    }
    if (currentVersion < 13) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN main_sold_out_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_sold_out_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_restock_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN canal2_restock_next_attempt_at TEXT NOT NULL DEFAULT '';
        INSERT INTO _sql_schema_migrations (id) VALUES (13);
        CREATE INDEX IF NOT EXISTS offers_restock_retry_idx
          ON offers(status, restocked_at, main_restock_next_attempt_at,
                    canal2_restock_next_attempt_at);
      `);
    }
    if (currentVersion < 14) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN main_image_upgrade_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN main_image_upgrade_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN main_image_upgrade_error TEXT NOT NULL DEFAULT '';
        INSERT INTO _sql_schema_migrations (id) VALUES (14);
        CREATE INDEX IF NOT EXISTS offers_main_image_upgrade_idx
          ON offers(telegram_image_strategy, main_image_upgrade_next_attempt_at,
                    main_image_upgrade_attempts, first_seen_at);
      `);
    }
    if (currentVersion < 15) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS offer_identity_aliases (
          alias TEXT NOT NULL,
          offer_id TEXT NOT NULL,
          first_seen_at TEXT NOT NULL,
          PRIMARY KEY(alias, offer_id)
        );
        CREATE INDEX IF NOT EXISTS offer_identity_aliases_offer_idx
          ON offer_identity_aliases(offer_id);
        CREATE INDEX IF NOT EXISTS offers_main_unknown_due_idx
          ON offers(main_delivery_unknown_at, main_delivery_attempts)
          WHERE main_sent_at = '';
        INSERT OR IGNORE INTO offer_identity_aliases(alias, offer_id, first_seen_at)
          SELECT 'id:' || id, id, first_seen_at FROM offers;
        INSERT INTO _sql_schema_migrations (id) VALUES (15);
      `);
    }
    if (currentVersion < 16) {
      this.sqlExec(`
        UPDATE offers SET
          main_sold_out_synced_at = COALESCE(
            NULLIF(sold_out_at, ''), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          ),
          main_sold_out_error = '', main_sold_out_next_attempt_at = ''
        WHERE main_sold_out_synced_at = ''
          AND lower(main_sold_out_error) LIKE '%message to edit not found%';
        UPDATE offers SET
          canal2_sold_out_synced_at = COALESCE(
            NULLIF(sold_out_at, ''), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          ),
          canal2_sold_out_error = '', canal2_sold_out_next_attempt_at = ''
        WHERE canal2_sold_out_synced_at = ''
          AND lower(canal2_sold_out_error) LIKE '%message to edit not found%';
        UPDATE offers SET
          main_restock_attempts = 0, main_restock_error = '',
          main_restock_next_attempt_at = ''
        WHERE status = 'restocked_pending_sync'
          AND lower(main_restock_error) LIKE '%message to edit not found%';
        UPDATE offers SET
          canal2_restock_attempts = 0, canal2_restock_error = '',
          canal2_restock_next_attempt_at = ''
        WHERE status = 'restocked_pending_sync'
          AND lower(canal2_restock_error) LIKE '%message to edit not found%';
        UPDATE incidents SET
          status = 'resolved', resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
          last_attempted_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), alert_error = ''
        WHERE status = 'active' AND key IN (
          SELECT 'delivery-queue:' || id || ':main_sold_out'
          FROM offers WHERE main_sold_out_synced_at <> ''
          UNION
          SELECT 'delivery-queue:' || id || ':canal2_sold_out'
          FROM offers WHERE canal2_sold_out_synced_at <> ''
        );
        INSERT INTO _sql_schema_migrations (id) VALUES (16);
      `);
    }
    if (currentVersion < 17) {
      this.sqlExec(`
        ALTER TABLE offers ADD COLUMN discord_image_proxy_url TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_image_cache_message_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_image_cache_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE offers ADD COLUMN discord_image_cache_next_attempt_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE offers ADD COLUMN discord_image_cache_error TEXT NOT NULL DEFAULT '';
        UPDATE offers SET
          would_send_canal2 = 1, status = 'partial_delivery',
          canal2_delivery_attempts = 0, canal2_delivery_error = '',
          canal2_delivery_in_flight_at = '', canal2_delivery_next_attempt_at = '',
          canal2_delivery_unknown_at = '', delivery_dead_letter_at = '',
          delivery_dead_letter_reason = '', delivery_unknown_at = '',
          delivery_unknown_target = '', delivery_quarantine_reason = ''
        WHERE link LIKE '%/campanhasdeingresso/%'
          AND would_send_main = 1 AND main_sent_at <> '' AND canal2_sent_at = ''
          AND first_seen_at >= '2026-08-03T19:30:00.000Z';
        UPDATE offers SET
          telegram_image_strategy = 'text_timeout',
          main_image_upgrade_attempts = 0,
          main_image_upgrade_next_attempt_at = '', main_image_upgrade_error = ''
        WHERE main_sent_at <> '' AND main_message_kind = 'text'
          AND first_seen_at >= '2026-08-03T19:30:00.000Z'
          AND COALESCE(NULLIF(image_url, ''), NULLIF(card_image_url, ''),
                       partner_image_url) <> '';
        CREATE INDEX IF NOT EXISTS offers_discord_image_cache_idx
          ON offers(discord_image_proxy_url, discord_image_cache_next_attempt_at,
                    discord_image_cache_attempts, first_seen_at);
        INSERT INTO _sql_schema_migrations (id) VALUES (17);
      `);
    }
    if (currentVersion < 18) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS discord_availability_sync (
          offer_id TEXT PRIMARY KEY,
          sold_out_synced_at TEXT NOT NULL DEFAULT '',
          sold_out_attempts INTEGER NOT NULL DEFAULT 0,
          sold_out_error TEXT NOT NULL DEFAULT '',
          sold_out_next_attempt_at TEXT NOT NULL DEFAULT '',
          restock_synced_at TEXT NOT NULL DEFAULT '',
          restock_attempts INTEGER NOT NULL DEFAULT 0,
          restock_error TEXT NOT NULL DEFAULT '',
          restock_next_attempt_at TEXT NOT NULL DEFAULT ''
        );
        UPDATE offers SET discord_image_cache_attempts = 10
         WHERE link NOT LIKE '%/campanhasdeingresso/%'
           AND discord_image_cache_message_id = '';
        INSERT OR IGNORE INTO discord_availability_sync(
          offer_id, restock_synced_at
        ) SELECT id, restocked_at FROM offers WHERE restocked_at <> '';
        CREATE INDEX IF NOT EXISTS offers_discord_feed_idx
          ON offers(discord_image_cache_message_id, discord_image_cache_next_attempt_at,
                    discord_image_cache_attempts, first_seen_at);
        INSERT INTO _sql_schema_migrations (id) VALUES (18);
      `);
    }
    if (currentVersion < 19) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS ticket_probe_state (
          offer_id TEXT PRIMARY KEY,
          next_at TEXT NOT NULL DEFAULT '',
          last_at TEXT NOT NULL DEFAULT '',
          last_result TEXT NOT NULL DEFAULT '',
          gone_count INTEGER NOT NULL DEFAULT 0,
          attempts INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO ticket_probe_state(offer_id, next_at)
        SELECT id, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+60 seconds')
        FROM offers
        WHERE status IN ('delivered', 'partial_delivery')
          AND sold_out_at = ''
          AND main_sent_at <> ''
          AND link LIKE '%/campanhasdeingresso/%'
          AND first_seen_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 day');
        CREATE INDEX IF NOT EXISTS ticket_probe_due_idx
          ON ticket_probe_state(next_at, attempts, offer_id);
        INSERT INTO _sql_schema_migrations (id) VALUES (19);
      `);
    }
    if (currentVersion < 20) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS delivery_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          dedupe_key TEXT NOT NULL UNIQUE,
          offer_id TEXT NOT NULL,
          target TEXT NOT NULL,
          operation TEXT NOT NULL,
          state TEXT NOT NULL,
          attempt INTEGER NOT NULL DEFAULT 0,
          generation INTEGER NOT NULL DEFAULT 1,
          occurred_at TEXT NOT NULL,
          external_id TEXT NOT NULL DEFAULT '',
          error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS delivery_events_offer_idx
          ON delivery_events(offer_id, occurred_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS delivery_events_state_idx
          ON delivery_events(state, occurred_at DESC, id DESC);
        INSERT OR IGNORE INTO delivery_events(
          dedupe_key, offer_id, target, operation, state, attempt, generation,
          occurred_at, external_id, error
        )
        SELECT id || '|main|snapshot|' || main_delivery_attempts || '|' ||
               delivery_generation || '|' ||
               CASE WHEN main_sent_at <> '' THEN 'sent'
                    WHEN main_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               id, 'main', 'snapshot',
               CASE WHEN main_sent_at <> '' THEN 'sent'
                    WHEN main_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               main_delivery_attempts, delivery_generation,
               COALESCE(NULLIF(main_sent_at, ''), NULLIF(main_delivery_unknown_at, ''), first_seen_at),
               CAST(main_message_id AS TEXT), main_delivery_error
        FROM offers
        WHERE main_sent_at <> '' OR main_delivery_unknown_at <> '' OR main_delivery_error <> ''
        UNION ALL
        SELECT id || '|canal2|snapshot|' || canal2_delivery_attempts || '|' ||
               delivery_generation || '|' ||
               CASE WHEN canal2_sent_at <> '' THEN 'sent'
                    WHEN canal2_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               id, 'canal2', 'snapshot',
               CASE WHEN canal2_sent_at <> '' THEN 'sent'
                    WHEN canal2_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               canal2_delivery_attempts, delivery_generation,
               COALESCE(NULLIF(canal2_sent_at, ''), NULLIF(canal2_delivery_unknown_at, ''), first_seen_at),
               CAST(canal2_message_id AS TEXT), canal2_delivery_error
        FROM offers
        WHERE canal2_sent_at <> '' OR canal2_delivery_unknown_at <> '' OR canal2_delivery_error <> ''
        UNION ALL
        SELECT id || '|discord|snapshot|' || discord_delivery_attempts || '|' ||
               delivery_generation || '|' ||
               CASE WHEN discord_sent_at <> '' THEN 'sent'
                    WHEN discord_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               id, 'discord', 'snapshot',
               CASE WHEN discord_sent_at <> '' THEN 'sent'
                    WHEN discord_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               discord_delivery_attempts, delivery_generation,
               COALESCE(NULLIF(discord_sent_at, ''), NULLIF(discord_delivery_unknown_at, ''), first_seen_at),
               discord_message_id, discord_delivery_error
        FROM offers
        WHERE discord_sent_at <> '' OR discord_delivery_unknown_at <> '' OR discord_delivery_error <> ''
        UNION ALL
        SELECT id || '|comment|snapshot|' || comment_delivery_attempts || '|' ||
               delivery_generation || '|' ||
               CASE WHEN comment_sent_at <> '' THEN 'sent'
                    WHEN comment_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               id, 'comment', 'snapshot',
               CASE WHEN comment_sent_at <> '' THEN 'sent'
                    WHEN comment_delivery_unknown_at <> '' THEN 'unknown'
                    ELSE 'failed' END,
               comment_delivery_attempts, delivery_generation,
               COALESCE(NULLIF(comment_sent_at, ''), NULLIF(comment_delivery_unknown_at, ''), first_seen_at),
               CAST(discussion_message_id AS TEXT), comment_delivery_error
        FROM offers
        WHERE comment_sent_at <> '' OR comment_delivery_unknown_at <> '' OR comment_delivery_error <> '';
        INSERT INTO _sql_schema_migrations (id) VALUES (20);
      `);
    }
    if (currentVersion < 21) {
      this.sqlExec(`
        CREATE INDEX IF NOT EXISTS offers_comment_inflight_v21
          ON offers(delivery_generation, comment_delivery_in_flight_at)
          WHERE comment_delivery_in_flight_at <> ''
            AND comment_sent_at = ''
            AND comment_delivery_unknown_at = '';
        CREATE INDEX IF NOT EXISTS offers_comment_due_v21
          ON offers(delivery_generation, comment_delivery_next_attempt_at, first_seen_at)
          WHERE discussion_message_id > 0
            AND comment_sent_at = ''
            AND comment_delivery_in_flight_at = ''
            AND comment_delivery_unknown_at = ''
            AND status NOT IN (
              'discarded', 'delivery_quarantined', 'shadow_candidate',
              'baseline', 'shadow_sold_out'
            );
        DROP INDEX IF EXISTS ticket_probe_due_idx;
        UPDATE ticket_probe_state
          SET next_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE next_at > '' AND (
            next_at NOT GLOB '????-??-??T??:??:??*Z' OR
            length(next_at) NOT BETWEEN 20 AND 24 OR
            julianday(next_at) IS NULL
          );
        CREATE INDEX IF NOT EXISTS ticket_probe_due_v21
          ON ticket_probe_state(next_at, attempts, offer_id)
          WHERE next_at > '';
        INSERT OR IGNORE INTO discord_availability_sync(offer_id)
          SELECT id FROM offers
          WHERE status = 'sold_out' OR restocked_at <> '';
        CREATE INDEX IF NOT EXISTS discord_avail_sold_due_v21
          ON discord_availability_sync(
            sold_out_next_attempt_at, sold_out_attempts, offer_id
          ) WHERE sold_out_synced_at = '';
        CREATE INDEX IF NOT EXISTS discord_avail_restock_due_v21
          ON discord_availability_sync(
            restock_next_attempt_at, restock_attempts, offer_id
          ) WHERE restock_synced_at = '';
        INSERT INTO _sql_schema_migrations (id) VALUES (21);
      `);
    }
    if (currentVersion < 22) {
      this.sqlExec(`
        CREATE INDEX IF NOT EXISTS offers_soldout_main_due_v22
          ON offers(
            sold_out_at, main_sold_out_attempts,
            main_sold_out_next_attempt_at, id
          ) WHERE status = 'sold_out'
            AND main_message_id > 0
            AND main_sold_out_synced_at = '';
        CREATE INDEX IF NOT EXISTS offers_soldout_canal2_due_v22
          ON offers(
            sold_out_at, canal2_sold_out_attempts,
            canal2_sold_out_next_attempt_at, id
          ) WHERE status = 'sold_out'
            AND main_message_id > 0
            AND would_send_canal2 = 1
            AND canal2_message_id > 0
            AND canal2_sold_out_synced_at = '';
        CREATE INDEX IF NOT EXISTS offers_restock_main_due_v22
          ON offers(
            restocked_at, main_restock_attempts,
            main_restock_next_attempt_at, id
          ) WHERE status = 'restocked_pending_sync'
            AND main_message_id > 0
            AND main_restock_synced_at = '';
        CREATE INDEX IF NOT EXISTS offers_restock_canal2_due_v22
          ON offers(
            restocked_at, canal2_restock_attempts,
            canal2_restock_next_attempt_at, id
          ) WHERE status = 'restocked_pending_sync'
            AND would_send_canal2 = 1
            AND canal2_message_id > 0
            AND canal2_restock_synced_at = '';
        CREATE INDEX IF NOT EXISTS offers_restock_finalize_v22
          ON offers(restocked_at, id)
          WHERE status = 'restocked_pending_sync'
            AND main_restock_synced_at <> '';
        INSERT INTO _sql_schema_migrations (id) VALUES (22);
      `);
    }
    if (currentVersion < 23) {
      this.sqlExec(`
        CREATE TABLE IF NOT EXISTS beeper_delivery_queue (
          offer_id TEXT PRIMARY KEY,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at TEXT NOT NULL DEFAULT '',
          in_flight_at TEXT NOT NULL DEFAULT '',
          sent_at TEXT NOT NULL DEFAULT '',
          last_error TEXT NOT NULL DEFAULT '',
          pending_message_id TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS beeper_delivery_due_v23
          ON beeper_delivery_queue(next_attempt_at, attempts, offer_id)
          WHERE sent_at = '' AND in_flight_at = '';
        INSERT INTO _sql_schema_migrations (id) VALUES (23);
      `);
    }
    if (currentVersion < 24) {
      this.sqlExec(`
        DELETE FROM beeper_delivery_queue
         WHERE sent_at = ''
           AND NOT EXISTS (
             SELECT 1 FROM offers
              WHERE offers.id = beeper_delivery_queue.offer_id
                AND (
                  lower(offers.link) LIKE '%/campanhasdeingresso/%' OR
                  lower(offers.category) LIKE '%campanhasdeingresso%'
                )
           );
        CREATE INDEX IF NOT EXISTS beeper_delivery_inflight_v24
          ON beeper_delivery_queue(in_flight_at, offer_id)
          WHERE sent_at = '' AND in_flight_at <> '';
        INSERT INTO _sql_schema_migrations (id) VALUES (24);
      `);
    }
    if (currentVersion < 25) {
      this.sqlExec(`
        CREATE INDEX IF NOT EXISTS offers_discord_image_cache_due_v25
          ON offers(
            discord_image_cache_next_attempt_at,
            discord_image_cache_attempts,
            first_seen_at DESC,
            id
          ) WHERE would_send_main = 1
            AND link NOT LIKE '%/campanhasdeingresso/%'
            AND discord_image_cache_message_id = ''
            AND discord_image_proxy_url = ''
            AND status NOT IN (
              'baseline', 'discarded', 'shadow_sold_out', 'sold_out'
            );
        CREATE INDEX IF NOT EXISTS offers_main_image_upgrade_due_v25
          ON offers(
            main_image_upgrade_next_attempt_at,
            main_image_upgrade_attempts,
            first_seen_at DESC,
            id
          ) WHERE telegram_image_strategy = 'text_timeout'
            AND main_message_kind = 'text'
            AND main_message_id > 0
            AND COALESCE(
              NULLIF(telegram_photo_file_id, ''), NULLIF(image_url, ''),
              NULLIF(card_image_url, ''), partner_image_url
            ) <> '';
        CREATE INDEX IF NOT EXISTS beeper_delivery_pending_v25
          ON beeper_delivery_queue(offer_id)
          WHERE sent_at = '';
        DROP INDEX IF EXISTS offers_main_image_upgrade_idx;
        DROP INDEX IF EXISTS offers_discord_image_cache_idx;
        DROP INDEX IF EXISTS offers_discord_feed_idx;
        INSERT INTO _sql_schema_migrations (id) VALUES (25);
      `);
    }
  }

  metadataValue(key) {
    if (this.metadataCache.has(key)) return this.metadataCache.get(key);
    const rows = this.sqlExec("SELECT value FROM metadata WHERE key = ?", key)
      .toArray();
    const value = rows[0]?.value || "";
    this.metadataCache.set(key, value);
    return value;
  }

  setMetadata(key, value) {
    const normalized = String(value || "");
    this.sqlExec(
      `INSERT INTO metadata(key, value) VALUES (?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      key,
      normalized,
    );
    this.metadataCache.set(key, normalized);
  }

  setMetadataIfChanged(key, value) {
    const normalized = String(value || "");
    if (this.metadataValue(key) === normalized) return false;
    this.setMetadata(key, normalized);
    return true;
  }

  recordDeliveryLedgerEvent(event) {
    try {
      const recorded = writeDeliveryEvent(this.sqlExec.bind(this), event);
      if (recorded) {
        trimDeliveryEvents(this.sqlExec.bind(this), event.offerId, 240);
      }
      return recorded;
    } catch (error) {
      // O ledger is observabilidade auxiliar: uma falha nele nunca pode
      // interromper a entrega já confirmada ou transformar um resultado em
      // retry duplicado.
      logEvent("warn", "uol_delivery_ledger_write_failed", {
        error: sanitizeError(error),
      });
      return false;
    }
  }

  runtimeSnapshot(name) {
    if (this.runtimeSnapshotCache.has(name)) return this.runtimeSnapshotCache.get(name);
    if (name === "api" || name === "ticket_listing") {
      const critical = parseRuntimeSnapshot(this.metadataValue("runtime:critical_sources"));
      const nested = name === "api" ? critical.api : critical.ticketListing;
      if (nested && typeof nested === "object") {
        this.runtimeSnapshotCache.set("critical_sources", critical);
        this.runtimeSnapshotCache.set(name, nested);
        return nested;
      }
    }
    const snapshot = parseRuntimeSnapshot(this.metadataValue(`runtime:${name}`));
    this.runtimeSnapshotCache.set(name, snapshot);
    return snapshot;
  }

  setRuntimeSnapshot(name, snapshot) {
    const normalized = snapshot && typeof snapshot === "object" ? snapshot : {};
    this.setMetadata(`runtime:${name}`, JSON.stringify(normalized));
    this.runtimeSnapshotCache.set(name, normalized);
  }

  setCriticalSourceSnapshot({ api = {}, ticketListing = {} } = {}) {
    const normalized = {
      api: api && typeof api === "object" ? api : {},
      ticketListing: ticketListing && typeof ticketListing === "object"
        ? ticketListing
        : {},
    };
    this.setMetadata("runtime:critical_sources", JSON.stringify(normalized));
    this.runtimeSnapshotCache.set("critical_sources", normalized);
    this.runtimeSnapshotCache.set("api", normalized.api);
    this.runtimeSnapshotCache.set("ticket_listing", normalized.ticketListing);
  }

  freshTicketListingCards(now = new Date(), maxAgeSeconds = 45) {
    const snapshot = this.runtimeSnapshot("ticket_listing");
    const successAt = Date.parse(String(snapshot.lastSuccessAt || ""));
    const nowMs = now instanceof Date ? now.getTime() : Date.parse(String(now || ""));
    const ageMs = nowMs - successAt;
    if (
      snapshot.lastError || !Array.isArray(snapshot.cards) || !snapshot.cards.length ||
      !Number.isFinite(successAt) || !Number.isFinite(nowMs) || ageMs < 0 ||
      ageMs > Math.max(1, Number(maxAgeSeconds) || 45) * 1_000
    ) return [];
    return compactTicketListingCards(snapshot.cards);
  }

  async ticketListingForMaintenance(now = new Date(), fetchImpl = fetch) {
    const cards = this.freshTicketListingCards(now, 45);
    if (cards.length) {
      const snapshot = this.runtimeSnapshot("ticket_listing");
      const completedAt = String(snapshot.lastSuccessAt || now.toISOString());
      return {
        status: "fulfilled",
        value: {
          cards,
          startedAt: completedAt,
          completedAt,
          elapsedMs: Number(snapshot.lastElapsedMs || 0),
          reused: true,
        },
      };
    }
    return settled(timedCards(
      () => fetchListing(fetchImpl, TICKET_LIST_URL, "_uol_ticket_listing_ts"),
    ));
  }

  runtimeValue(name, key, legacyKey = "") {
    const value = this.runtimeSnapshot(name)[key];
    if (value !== undefined && value !== null) return String(value);
    return legacyKey ? this.metadataValue(legacyKey) : "";
  }

  recordSourceCards(source, cards, observedAt) {
    if (source !== "api" && source !== "listing") return;
    const firstColumn = source === "api" ? "api_first_seen_at" : "listing_first_seen_at";
    const lastColumn = source === "api" ? "api_last_seen_at" : "listing_last_seen_at";
    const touchMinutes = envNumber(this.env, "OFFER_LAST_SEEN_TOUCH_MINUTES", 15, 1, 1_440);
    for (const card of cards.filter(isTicketCampaign)) {
      const previous = this.sqlExec(
        `SELECT link, title, ${lastColumn} AS last_seen_at
         FROM source_observations WHERE offer_key = ? LIMIT 1`,
        card.id,
      ).toArray()[0];
      const contentChanged = previous && (
        String(previous.link || "") !== String(card.link || "") ||
        (card.previewTitle && String(previous.title || "") !== String(card.previewTitle))
      );
      if (
        previous && !contentChanged &&
        !shouldTouchObservation(previous.last_seen_at, observedAt, touchMinutes)
      ) continue;
      this.sqlExec(
        `INSERT INTO source_observations(
           offer_key, link, title, ${firstColumn}, ${lastColumn}
         ) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(offer_key) DO UPDATE SET
           link = excluded.link,
           title = CASE WHEN excluded.title <> '' THEN excluded.title ELSE source_observations.title END,
           ${firstColumn} = CASE
             WHEN source_observations.${firstColumn} = '' OR
                  excluded.${firstColumn} < source_observations.${firstColumn}
               THEN excluded.${firstColumn}
             ELSE source_observations.${firstColumn}
           END,
           ${lastColumn} = excluded.${lastColumn}`,
        card.id,
        card.link,
        card.previewTitle,
        observedAt,
        observedAt,
      );
    }
  }

  updateSourceHealth({
    listingResult,
    ticketListingResult,
    apiResult,
    mainListingCards,
    ticketListingCards,
    apiCards,
    now,
  }) {
    const previous = this.runtimeSnapshot("source_health");
    const mainListingSucceeded = listingResult.status === "fulfilled";
    const ticketListingSucceeded = ticketListingResult.status === "fulfilled";
    const listingSucceeded = mainListingSucceeded && ticketListingSucceeded;
    const apiSucceeded = apiResult.status === "fulfilled" && apiCards.length > 0;
    const minimum = envNumber(this.env, "MIN_HEALTHY_LISTING_OFFERS", 5, 1, 48);
    const mainListingHealthy = mainListingSucceeded && mainListingCards.length >= minimum;
    const ticketListingHealthy = ticketListingSucceeded;
    const listingHealthy = mainListingHealthy && ticketListingHealthy;
    const previousMainCount = Number(
      previous.mainListingPreviousCount ?? this.metadataValue("main_listing_previous_count") ?? 0,
    );
    const previousTicketCount = Number(
      previous.ticketListingPreviousCount ?? this.metadataValue("ticket_listing_previous_count") ?? 0,
    );
    const mainSharpDrop = mainListingSucceeded && previousMainCount >= 10 &&
      mainListingCards.length < Math.ceil(previousMainCount * 0.5);
    const ticketSharpDrop = ticketListingSucceeded && previousTicketCount >= 3 &&
      ticketListingCards.length < Math.ceil(previousTicketCount * 0.5);
    const listingFailureStreak = listingHealthy
      ? 0
      : Number(previous.listingFailureStreak ?? this.metadataValue("listing_failure_streak") ?? 0) + 1;
    const sharpDrop = mainSharpDrop || ticketSharpDrop;
    const listingDropStreak = sharpDrop
      ? Number(previous.listingDropStreak ?? this.metadataValue("listing_drop_streak") ?? 0) + 1
      : 0;
    const listingCards = mergeOfferCards(ticketListingCards, mainListingCards);
    const comparison = compareOfferSources(apiCards, listingCards);
    const divergent = listingSucceeded && apiSucceeded && comparison.apiTickets > 0 &&
      comparison.listingTickets > 0 && comparison.matchedApi === 0;
    const sourceDivergenceStreak = divergent
      ? Number(previous.sourceDivergenceStreak ?? this.metadataValue("source_divergence_streak") ?? 0) + 1
      : 0;
    const signature = sourceSnapshotSignature(listingCards);
    const previousSignature = previous.listingSnapshotSignature ??
      this.metadataValue("listing_snapshot_signature");
    this.setRuntimeSnapshot("source_health", {
      listingPreviousCount: listingCards.length,
      mainListingPreviousCount: mainListingCards.length,
      ticketListingPreviousCount: ticketListingCards.length,
      listingFailureStreak,
      listingDropStreak,
      sourceDivergenceStreak,
      sourceLastComparison: comparison,
      listingSnapshotSignature: signature,
      listingSnapshotChangedAt: signature && signature !== previousSignature
        ? now.toISOString()
        : previous.listingSnapshotChangedAt || this.metadataValue("listing_snapshot_changed_at"),
      fullSourceSuccessAt: listingHealthy && apiSucceeded
        ? now.toISOString()
        : previous.fullSourceSuccessAt || this.metadataValue("full_source_success_at"),
    });
    return {
      comparison,
      mainListingHealthy,
      ticketListingHealthy,
      mainSharpDrop,
      ticketSharpDrop,
    };
  }

  getSourceComparison() {
    const cutoff = new Date(Date.now() - 30 * 86_400_000).toISOString();
    const rows = this.sqlExec(
      `SELECT offer_key, title, api_first_seen_at, listing_first_seen_at
       FROM source_observations
       WHERE link LIKE '%/campanhasdeingresso/%'
         AND (api_last_seen_at >= ? OR listing_last_seen_at >= ?)
       ORDER BY MAX(api_first_seen_at, listing_first_seen_at) DESC
       LIMIT 200`,
      cutoff,
      cutoff,
    ).toArray();
    const sourceHealth = this.runtimeSnapshot("source_health");
    let current = sourceHealth.sourceLastComparison || {};
    if (!Object.keys(current).length) {
      current = parseRuntimeSnapshot(this.metadataValue("source_last_comparison"));
    }
    return {
      ...summarizeSourceComparison(rows),
      current,
      listingFailureStreak: Number(
        sourceHealth.listingFailureStreak ?? this.metadataValue("listing_failure_streak") ?? 0,
      ),
      listingDropStreak: Number(
        sourceHealth.listingDropStreak ?? this.metadataValue("listing_drop_streak") ?? 0,
      ),
      divergenceStreak: Number(
        sourceHealth.sourceDivergenceStreak ?? this.metadataValue("source_divergence_streak") ?? 0,
      ),
      fullSuccessAt: sourceHealth.fullSourceSuccessAt || this.metadataValue("full_source_success_at"),
      listingSnapshotChangedAt: sourceHealth.listingSnapshotChangedAt ||
        this.metadataValue("listing_snapshot_changed_at"),
    };
  }

  imageCacheKey(offer) {
    return String(
      offer?.imageUrl || offer?.cardImageUrl || offer?.partnerImageUrl || "",
    ).trim().slice(0, 1_500);
  }

  cachedTelegramPhoto(imageKey) {
    if (!imageKey) return null;
    return this.sqlExec(
      "SELECT file_id, file_unique_id FROM telegram_image_cache WHERE image_key = ? LIMIT 1",
      imageKey,
    ).toArray()[0] || null;
  }

  imageStrategyAvailability(now = new Date()) {
    const result = { file_id: true, remote_url: true, discord_proxy: true, upload: true };
    const rows = this.sqlExec("SELECT strategy, state, opened_until FROM image_strategy_health")
      .toArray();
    for (const row of rows) {
      const openedUntil = Date.parse(row.opened_until || "");
      if (row.state === "open" && Number.isFinite(openedUntil) && openedUntil > now.getTime()) {
        result[row.strategy] = false;
      } else if (row.state === "open") {
        this.sqlExec(
          "UPDATE image_strategy_health SET state = 'half_open' WHERE strategy = ?",
          row.strategy,
        );
      }
    }
    return result;
  }

  recordImageDelivery(offerId, imageKey, delivery, now = new Date()) {
    const nowIso = now.toISOString();
    const threshold = envNumber(this.env, "IMAGE_CIRCUIT_FAILURE_THRESHOLD", 3, 2, 20);
    const cooldownMinutes = envNumber(this.env, "IMAGE_CIRCUIT_COOLDOWN_MINUTES", 10, 1, 120);
    for (const attempt of delivery.imageAttempts || []) {
      const row = this.sqlExec(
        "SELECT consecutive_failures FROM image_strategy_health WHERE strategy = ?",
        attempt.strategy,
      ).toArray()[0];
      const next = nextImageCircuitState({
        consecutiveFailures: Number(row?.consecutive_failures || 0),
      }, attempt, { now, threshold, cooldownMinutes });
      if (attempt.ok) {
        this.sqlExec(
          `UPDATE image_strategy_health SET state = 'closed', consecutive_failures = 0,
             opened_until = '', last_success_at = ?, last_error = '' WHERE strategy = ?`,
          nowIso,
          attempt.strategy,
        );
        continue;
      }
      this.sqlExec(
        `UPDATE image_strategy_health SET state = ?, consecutive_failures = ?,
           opened_until = ?, last_failure_at = ?, last_error = ? WHERE strategy = ?`,
        next.state,
        next.consecutiveFailures,
        next.openedUntil,
        nowIso,
        sanitizeError(attempt.error),
        attempt.strategy,
      );
      if (attempt.strategy === "file_id" && imageKey) {
        this.sqlExec(
          "DELETE FROM telegram_image_cache WHERE image_key = ?",
          imageKey,
        );
      }
    }
    if (delivery.photoFileId && imageKey) {
      this.sqlExec(
        `INSERT INTO telegram_image_cache(
           image_key, file_id, file_unique_id, created_at, last_used_at, use_count
         ) VALUES (?, ?, ?, ?, ?, 1)
         ON CONFLICT(image_key) DO UPDATE SET
           file_id = excluded.file_id, file_unique_id = excluded.file_unique_id,
           last_used_at = excluded.last_used_at, use_count = telegram_image_cache.use_count + 1,
           failure_count = 0, last_error = ''`,
        imageKey,
        delivery.photoFileId,
        delivery.photoFileUniqueId || "",
        nowIso,
        nowIso,
      );
    }
    this.sqlExec(
      `UPDATE offers SET telegram_photo_file_id = ?, telegram_photo_file_unique_id = ?,
         telegram_image_strategy = ? WHERE id = ?`,
      delivery.photoFileId || "",
      delivery.photoFileUniqueId || "",
      delivery.imageStrategy || "",
      offerId,
    );
  }

  telegramOfferWithImageState(offer, overrides = {}) {
    const imageKey = this.imageCacheKey(offer);
    const cached = this.cachedTelegramPhoto(imageKey);
    return {
      imageKey,
      offer: {
        ...offer,
        ...overrides,
        telegramPhotoFileId: cached?.file_id || "",
        imageStrategies: this.imageStrategyAvailability(),
      },
    };
  }

  async discordImageProxyForOffer(row, offer) {
    const storedProxy = String(row?.discord_image_proxy_url || "").trim();
    if (storedProxy) return storedProxy;

    const usesImageCache = !isTicketCampaign(offer);
    let messageId = String(
      usesImageCache ? row?.discord_image_cache_message_id : row?.discord_message_id,
    ).trim();
    let webhookUrl = String(
      usesImageCache
        ? this.env.DISCORD_IMAGE_CACHE_WEBHOOK_URL
        : this.env.DISCORD_WEBHOOK_URL,
    ).trim();
    let proxyUrl = "";
    let cacheMessageId = String(row?.discord_image_cache_message_id || "").trim();

    if (usesImageCache) {
      if (!webhookUrl) return "";
      if (!messageId) {
        const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
        if (Number(row?.discord_image_cache_attempts || 0) >= maxAttempts) return "";
        const cached = await cacheDiscordOfferImage(this.env, offer);
        messageId = cached.messageId;
        cacheMessageId = cached.messageId;
        proxyUrl = cached.imageProxyUrl;
      }
    }

    if (!proxyUrl && messageId) {
      proxyUrl = await getDiscordMessageImageProxy(
        this.env,
        messageId,
        fetch,
        webhookUrl,
      );
    }
    if (!proxyUrl) throw new Error("discord_image_proxy_missing");

    this.sqlExec(
      `UPDATE offers SET discord_image_proxy_url = ?,
         discord_image_cache_message_id = CASE WHEN ? <> '' THEN ?
           ELSE discord_image_cache_message_id END,
         discord_image_cache_error = '', discord_image_cache_next_attempt_at = ''
       WHERE id = ?`,
      proxyUrl,
      cacheMessageId,
      cacheMessageId,
      row.id,
    );
    return proxyUrl;
  }

  async enrichDiscordOfferForCard(offer) {
    if (isTicketCampaign(offer)) return offer;
    if (cleanText(offer?.description) || cleanText(offer?.validity)) return offer;
    const detail = await fetchDiscordOfferDetail(offer);
    return mergeDiscordOfferDetail(offer, detail);
  }

  async primePendingDiscordImageCache(now = new Date(), limit = 2) {
    if (!discordConfiguration(this.env).imageCacheConfigured) {
      return { primed: 0, failed: 0 };
    }
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const rows = this.sqlExec(
      `SELECT * FROM offers INDEXED BY offers_discord_image_cache_due_v25
       WHERE would_send_main = 1
         AND link NOT LIKE '%/campanhasdeingresso/%'
         AND discord_image_cache_message_id = ''
         AND discord_image_proxy_url = ''
         AND status NOT IN ('baseline', 'discarded', 'shadow_sold_out', 'sold_out')
         AND discord_image_cache_attempts < ?
         AND discord_image_cache_next_attempt_at <= ?
       ORDER BY first_seen_at DESC LIMIT ?`,
      maxAttempts,
      now.toISOString(),
      Math.max(1, Math.min(16, Number(limit || 4))),
    ).toArray();
    let primed = 0;
    let failed = 0;
    for (const row of rows) {
      const attempts = Number(row.discord_image_cache_attempts || 0) + 1;
      try {
        const offer = await this.enrichDiscordOfferForCard(rowToOffer(row));
        const cached = await cacheDiscordOfferImage(this.env, offer);
        this.sqlExec(
          `UPDATE offers SET discord_image_cache_attempts = ?,
             discord_image_cache_message_id = ?,
             discord_image_proxy_url = COALESCE(NULLIF(?, ''), discord_image_proxy_url),
             discord_image_cache_error = '', discord_image_cache_next_attempt_at = ''
           WHERE id = ?`,
          attempts,
          cached.messageId,
          cached.imageProxyUrl || "",
          row.id,
        );
        primed += 1;
      } catch (error) {
        this.sqlExec(
          `UPDATE offers SET discord_image_cache_attempts = ?,
             discord_image_cache_error = ?, discord_image_cache_next_attempt_at = ?
           WHERE id = ?`,
          attempts,
          sanitizeError(error),
          deliveryRetryAt(error, attempts, now),
          row.id,
        );
        failed += 1;
      }
    }
    return { primed, failed };
  }

  async upgradeTimedOutMainImages(now = new Date(), limit = 4) {
    if (this.currentDeliveryMode() !== "live") return { upgraded: 0, failed: 0 };
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const rows = this.sqlExec(
      `SELECT * FROM offers INDEXED BY offers_main_image_upgrade_due_v25
       WHERE telegram_image_strategy = 'text_timeout'
         AND main_message_kind = 'text'
         AND main_message_id > 0
         AND main_image_upgrade_attempts < ?
         AND COALESCE(NULLIF(telegram_photo_file_id, ''), NULLIF(image_url, ''),
                      NULLIF(card_image_url, ''), partner_image_url) <> ''
         AND main_image_upgrade_next_attempt_at <= ?
       ORDER BY CASE
         WHEN discord_image_proxy_url <> '' THEN 0
         WHEN discord_message_id <> '' THEN 1
         ELSE 2
       END, first_seen_at DESC
       LIMIT ?`,
      maxAttempts,
      now.toISOString(),
      Math.max(1, Math.min(16, Number(limit || 4))),
    ).toArray().filter((row) => lateImageUpgradeDue(row, now, maxAttempts));
    let upgraded = 0;
    let failed = 0;

    for (const row of rows) {
      const attempts = Number(row.main_image_upgrade_attempts || 0) + 1;
      const originalOffer = rowToOffer(row);
      const telegramState = this.telegramOfferWithImageState(originalOffer);
      let deliveryOffer = telegramState.offer;
      let usedDiscordProxy = false;
      try {
        const proxyUrl = await this.discordImageProxyForOffer(row, originalOffer);
        if (proxyUrl) {
          deliveryOffer = {
            ...telegramState.offer,
            imageUrl: proxyUrl,
            telegramImageRemoteStrategy: "discord_proxy",
          };
          usedDiscordProxy = true;
        }
      } catch (error) {
        logEvent("warn", "uol_discord_image_proxy_unavailable", {
          offerId: row.id,
          error: sanitizeError(error),
        });
      }
      try {
        let result;
        try {
          result = await editMainOfferMedia(this.env, {
            messageId: Number(row.main_message_id),
            offer: deliveryOffer,
            telegramPhotoFileId: telegramState.offer.telegramPhotoFileId,
          });
        } catch (proxyError) {
          if (!usedDiscordProxy) throw proxyError;
          this.sqlExec(
            "UPDATE offers SET discord_image_proxy_url = '' WHERE id = ?",
            row.id,
          );
          result = await editMainOfferMedia(this.env, {
            messageId: Number(row.main_message_id),
            offer: telegramState.offer,
            telegramPhotoFileId: telegramState.offer.telegramPhotoFileId,
          });
        }
        this.recordImageDelivery(row.id, telegramState.imageKey, result, now);
        this.sqlExec(
          `UPDATE offers SET main_message_kind = 'photo',
             main_image_upgrade_attempts = ?, main_image_upgrade_next_attempt_at = '',
             main_image_upgrade_error = '' WHERE id = ?`,
          attempts,
          row.id,
        );
        upgraded += 1;
      } catch (error) {
        const message = sanitizeError(error);
        if (message.toLowerCase().includes("message is not modified")) {
          this.sqlExec(
            `UPDATE offers SET main_message_kind = 'photo',
               telegram_image_strategy = 'edit_confirmed_not_modified',
               main_image_upgrade_attempts = ?, main_image_upgrade_next_attempt_at = '',
               main_image_upgrade_error = '' WHERE id = ?`,
            attempts,
            row.id,
          );
          upgraded += 1;
          continue;
        }
        this.sqlExec(
          `UPDATE offers SET main_image_upgrade_attempts = ?,
             main_image_upgrade_next_attempt_at = ?, main_image_upgrade_error = ?
           WHERE id = ?`,
          attempts,
          deliveryRetryAt(error, attempts, now),
          `${isAmbiguousDeliveryError(error) ? "ambiguous:" : ""}${message}`.slice(0, 240),
          row.id,
        );
        failed += 1;
      }
    }
    return { upgraded, failed };
  }

  getImageDeliveryHealth() {
    const now = Date.now();
    const strategies = this.sqlExec(
      `SELECT strategy, state, consecutive_failures, opened_until,
              last_failure_at, last_success_at, last_error
       FROM image_strategy_health ORDER BY strategy`,
    ).toArray().map((row) => ({
      ...row,
      state: row.state === "open" && Date.parse(row.opened_until || "") <= now
        ? "half_open"
        : row.state,
    }));
    const cache = this.sqlExec(
      "SELECT COUNT(*) AS count, COALESCE(SUM(use_count), 0) AS uses FROM telegram_image_cache",
    ).one();
    const delivery = this.sqlExec(
      `SELECT
         SUM(CASE WHEN main_sent_at <> '' AND main_message_kind = 'photo' THEN 1 ELSE 0 END)
           AS photo_sent,
         SUM(CASE WHEN main_sent_at <> '' AND main_message_kind = 'text' THEN 1 ELSE 0 END)
           AS text_sent,
         SUM(CASE WHEN main_sent_at <> '' AND main_message_kind = 'text'
                   AND telegram_image_strategy = 'text_timeout'
                  THEN 1 ELSE 0 END) AS pending_upgrade,
         SUM(CASE WHEN discord_image_proxy_url <> '' THEN 1 ELSE 0 END) AS proxy_cached
       FROM offers`,
    ).one();
    return {
      cacheEntries: Number(cache.count || 0),
      cacheUses: Number(cache.uses || 0),
      photoSent: Number(delivery.photo_sent || 0),
      textSent: Number(delivery.text_sent || 0),
      pendingUpgrade: Number(delivery.pending_upgrade || 0),
      discordProxyCached: Number(delivery.proxy_cached || 0),
      strategies,
    };
  }

  async fetchAllApi() {
    return fetchOffersFromApi(this.env, fetch);
  }

  async fetchTicketListing(fetchImpl = fetch) {
    return fetchListing(fetchImpl, TICKET_LIST_URL, "_uol_ticket_critical_ts", 10_000);
  }

  currentDeliveryMode() {
    const override = this.metadataValue("delivery_mode_override");
    if (override === "live" || override === "shadow") return override;
    return deliveryMode(this.env);
  }

  currentDeliveryGeneration() {
    return Math.max(1, Number(this.metadataValue("delivery_mode_generation") || 1));
  }

  setDeliveryMode(mode) {
    const normalized = String(mode || "").trim().toLowerCase();
    if (normalized !== "live" && normalized !== "shadow") {
      throw new Error("delivery_mode_invalid");
    }
    const previous = this.currentDeliveryMode();
    if (previous === normalized) {
      return {
        ok: true,
        mode: normalized,
        generation: this.currentDeliveryGeneration(),
        quarantined: 0,
      };
    }
    const generation = this.currentDeliveryGeneration() + 1;
    this.setMetadata("delivery_mode_override", normalized);
    this.setMetadata("delivery_mode_generation", generation);
    let quarantined = 0;
    if (normalized === "shadow") {
      const result = this.sqlExec(
        `UPDATE offers
         SET status = 'delivery_quarantined',
             delivery_quarantine_reason = 'mode_changed_to_shadow'
         WHERE status IN (
           'delivery_pending', 'partial_delivery', 'delivery_blocked_configuration',
           'delivery_dead_letter', 'delivery_unknown'
         )
         RETURNING id`,
      ).toArray();
      quarantined = result.length;
    }
    return {
      ok: true,
      mode: normalized,
      generation,
      quarantined,
    };
  }

  async ensureAlarm() {
    const interval = envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600) * 1_000;
    const now = Date.now();
    let alarm = await this.ctx.storage.getAlarm();
    if (alarm == null || alarm < now - interval * 2) {
      const reason = alarm == null ? "missing" : "overdue";
      alarm = now + interval;
      await this.ctx.storage.setAlarm(alarm);
      this.setMetadata("alarm_last_rearmed_at", new Date(now).toISOString());
      this.setMetadata("alarm_last_rearmed_reason", reason);
    }
    return new Date(alarm).toISOString();
  }

  async ensureMaintenanceAlarm(urgent = false) {
    const namespace = this.env.UOL_TELEGRAM_MAINTENANCE;
    if (!namespace) return "";
    const stub = namespace.getByName(MAINTENANCE_INSTANCE_NAME);
    return urgent ? stub.requestImmediate() : stub.ensureAlarm();
  }

  async ensureTelegramWebhook() {
    const now = Date.now();
    const previous = this.runtimeSnapshot("webhook");
    const lastChecked = Date.parse(
      previous.checkedAt || this.metadataValue("telegram_webhook_checked_at") || "",
    );
    if (Number.isFinite(lastChecked) && now - lastChecked < 5 * 60_000) return true;

    const expectedUrl = `${String(this.env.PUBLIC_BASE_URL || "").replace(/\/+$/, "")}/telegram-webhook`;
    let info = await getTelegramWebhookInfo(this.env);
    const needsRepair = info.url !== expectedUrl || (
      info.pendingUpdateCount > 0 && Boolean(info.lastErrorMessage)
    );
    if (needsRepair) {
      await registerTelegramWebhook(this.env);
      info = await getTelegramWebhookInfo(this.env);
    }
    const checkedAt = new Date(now).toISOString();
    this.setRuntimeSnapshot("webhook", {
      checkedAt,
      registeredAt: needsRepair
        ? checkedAt
        : previous.registeredAt || this.metadataValue("telegram_webhook_registered_at") || checkedAt,
      urlMatches: info.url === expectedUrl,
      pendingUpdates: info.pendingUpdateCount,
      lastError: info.lastErrorMessage,
    });
    return true;
  }

  insertRun(run) {
    this.sqlExec(
      `INSERT INTO runs(
        started_at, finished_at, source, outcome, offers_seen, new_offers,
        enriched, would_send_main, would_send_canal2, sold_out_detected,
        main_sent, canal2_sent, delivery_failed,
        sold_out_main_edited, sold_out_canal2_edited, error
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      run.startedAt,
      run.finishedAt,
      run.source,
      run.outcome,
      run.offersSeen,
      run.newOffers,
      run.enriched,
      run.wouldSendMain,
      run.wouldSendCanal2,
      run.soldOutDetected,
      run.mainSent,
      run.canal2Sent,
      run.deliveryFailed,
      run.soldOutMainEdited,
      run.soldOutCanal2Edited,
      run.error,
    );
    this.sqlExec(
      "DELETE FROM runs WHERE id NOT IN (SELECT id FROM runs ORDER BY id DESC LIMIT 240)",
    );
  }

  failedRunStreak() {
    const snapshot = this.runtimeSnapshot("api");
    if (snapshot.runFailureStreak !== undefined) {
      return Number(snapshot.runFailureStreak || 0);
    }
    const rows = this.sqlExec(
      "SELECT outcome, error FROM runs ORDER BY id DESC LIMIT 12",
    ).toArray();
    let streak = 0;
    for (const row of rows) {
      if (row.outcome !== "failed" && !row.error) break;
      streak += 1;
    }
    return streak;
  }

  recentTicketDeliveryIssues(now) {
    let monitoringStartedAt = this.metadataValue("ops_monitor_started_at");
    if (!monitoringStartedAt) {
      monitoringStartedAt = now.toISOString();
      this.setMetadata("ops_monitor_started_at", monitoringStartedAt);
      return [];
    }
    const graceMinutes = envNumber(this.env, "OPS_TICKET_GRACE_MINUTES", 3, 1, 60);
    const graceCutoff = new Date(now.getTime() - graceMinutes * 60_000).toISOString();
    const recentCutoff = [
      new Date(now.getTime() - 24 * 60 * 60_000).toISOString(),
      monitoringStartedAt,
    ].sort().at(-1);
    return this.sqlExec(
      `SELECT id, COALESCE(NULLIF(title, ''), preview_title) AS title,
              main_message_kind, telegram_image_strategy,
              comment_sent_at, discussion_message_id
       FROM offers
       WHERE link LIKE '%/campanhasdeingresso/%'
         AND main_sent_at <> ''
         AND first_seen_at >= ?
         AND first_seen_at <= ?
         AND (
           (main_message_kind <> 'photo' AND telegram_image_strategy <> 'text_fast')
           OR comment_sent_at = ''
         )
       ORDER BY first_seen_at DESC
       LIMIT 12`,
      recentCutoff,
      graceCutoff,
    ).toArray().map((row) => ({
      id: row.id,
      title: row.title,
      missingPhoto: row.main_message_kind !== "photo" &&
        row.telegram_image_strategy !== "text_fast",
      missingComment: !row.comment_sent_at,
    }));
  }

  deliveryQueueIssues(now = new Date()) {
    const mainGraceSeconds = envNumber(
      this.env,
      "OPS_MAIN_DELIVERY_GRACE_SECONDS",
      45,
      15,
      600,
    );
    const mainGraceCutoff = new Date(now.getTime() - mainGraceSeconds * 1_000).toISOString();
    const ambiguousRetrySeconds = envNumber(
      this.env,
      "MAIN_AMBIGUOUS_RETRY_SECONDS",
      30,
      15,
      300,
    );
    const ambiguousRetryCutoff = new Date(
      now.getTime() - ambiguousRetrySeconds * 1_000,
    ).toISOString();
    const queueIssues = this.sqlExec(
      `SELECT id, COALESCE(NULLIF(title, ''), preview_title) AS title, status,
              delivery_unknown_target, delivery_dead_letter_reason,
              main_delivery_error, main_delivery_unknown_at,
              canal2_delivery_error, discord_delivery_error,
              comment_delivery_error, comment_delivery_unknown_at
       FROM offers
       WHERE status IN (
         'delivery_dead_letter', 'delivery_unknown', 'delivery_blocked_configuration'
       ) OR comment_delivery_unknown_at <> ''
          OR (
            would_send_main = 1 AND main_sent_at = '' AND decision_at <> ''
            AND decision_at <= ?
            AND status NOT IN ('discarded', 'shadow_candidate', 'shadow_sold_out')
          )
       ORDER BY first_seen_at ASC
       LIMIT 20`,
      mainGraceCutoff,
    ).toArray().map((row) => {
      const commentUnknown = Boolean(row.comment_delivery_unknown_at);
      const target = commentUnknown
        ? "comment"
        : row.delivery_unknown_target || (
            row.main_delivery_error ? "main"
              : row.canal2_delivery_error ? "canal2"
                : row.discord_delivery_error ? "discord" : "delivery"
          );
      if (
        target === "main" && row.main_delivery_unknown_at &&
        row.main_delivery_unknown_at > ambiguousRetryCutoff
      ) {
        return null;
      }
      const slowMain = !commentUnknown && ![
        "delivery_dead_letter", "delivery_unknown", "delivery_blocked_configuration",
      ].includes(row.status);
      return {
        id: row.id,
        title: row.title,
        target,
        state: slowMain
          ? "main_delivery_slow"
          : commentUnknown
          ? "unknown"
          : row.status.replace(/^delivery_/, ""),
        error: commentUnknown
          ? row.comment_delivery_error
          : row.main_delivery_error || row.canal2_delivery_error ||
            row.discord_delivery_error || row.delivery_dead_letter_reason,
      };
    }).filter(Boolean);

    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const maintenanceIssues = this.sqlExec(
      `SELECT id, COALESCE(NULLIF(title, ''), preview_title) AS title, status,
              would_send_canal2, canal2_message_id,
              main_restock_synced_at, main_restock_attempts, main_restock_error,
              canal2_restock_synced_at, canal2_restock_attempts, canal2_restock_error,
              main_sold_out_synced_at, main_sold_out_attempts, main_sold_out_error,
              canal2_sold_out_synced_at, canal2_sold_out_attempts, canal2_sold_out_error,
              comment_sent_at, discussion_message_id,
              comment_delivery_attempts, comment_delivery_error
       FROM offers
       WHERE
         (status = 'restocked_pending_sync' AND (
           (main_restock_synced_at = '' AND main_restock_attempts >= ?) OR
           (would_send_canal2 = 1 AND canal2_message_id > 0 AND
             canal2_restock_synced_at = '' AND canal2_restock_attempts >= ?)
         )) OR
         (status = 'sold_out' AND (
           (main_sold_out_synced_at = '' AND main_sold_out_attempts >= ?) OR
           (would_send_canal2 = 1 AND canal2_message_id > 0 AND
             canal2_sold_out_synced_at = '' AND canal2_sold_out_attempts >= ?)
         )) OR
         (comment_sent_at = '' AND discussion_message_id > 0 AND
           comment_delivery_attempts >= ? AND
           comment_delivery_error <> 'historical_unknown_closed')
       ORDER BY first_seen_at ASC
       LIMIT 20`,
      maxAttempts,
      maxAttempts,
      maxAttempts,
      maxAttempts,
      maxAttempts,
    ).toArray().map((row) => {
      if (!row.comment_sent_at && Number(row.discussion_message_id || 0) > 0 &&
          Number(row.comment_delivery_attempts || 0) >= maxAttempts &&
          row.comment_delivery_error !== "historical_unknown_closed") {
        return {
          id: row.id,
          title: row.title,
          target: "comment",
          state: "dead_letter",
          error: row.comment_delivery_error || "comment_delivery_attempts_exhausted",
        };
      }
      if (row.status === "restocked_pending_sync") {
        const canal2 = row.would_send_canal2 && Number(row.canal2_message_id || 0) > 0 &&
          !row.canal2_restock_synced_at && Number(row.canal2_restock_attempts || 0) >= maxAttempts;
        return {
          id: row.id,
          title: row.title,
          target: canal2 ? "canal2_restock" : "main_restock",
          state: "dead_letter",
          error: canal2
            ? row.canal2_restock_error || "canal2_restock_attempts_exhausted"
            : row.main_restock_error || "main_restock_attempts_exhausted",
        };
      }
      const canal2 = row.would_send_canal2 && Number(row.canal2_message_id || 0) > 0 &&
        !row.canal2_sold_out_synced_at && Number(row.canal2_sold_out_attempts || 0) >= maxAttempts;
      return {
        id: row.id,
        title: row.title,
        target: canal2 ? "canal2_sold_out" : "main_sold_out",
        state: "dead_letter",
        error: canal2
          ? row.canal2_sold_out_error || "canal2_sold_out_attempts_exhausted"
          : row.main_sold_out_error || "main_sold_out_attempts_exhausted",
      };
    });

    return [...queueIssues, ...maintenanceIssues]
      .filter((issue, index, all) => all.findIndex(
        (candidate) => candidate.id === issue.id && candidate.target === issue.target,
      ) === index)
      .slice(0, 20);
  }

  async sendOperationalAlert(text) {
    const transports = [{
      name: "telegram",
      promise: sendOperationsAlert(this.env, text),
    }];
    if (String(this.env.DISCORD_OPS_WEBHOOK_URL || "").trim()) {
      transports.push({
        name: "discord",
        promise: sendDiscordOperationsAlert(this.env, text),
      });
    }
    const results = await Promise.allSettled(transports.map((item) => item.promise));
    const succeeded = results
      .map((result, index) => result.status === "fulfilled" ? transports[index].name : "")
      .filter(Boolean);
    if (succeeded.length) return { succeeded };
    throw new Error(results
      .map((result, index) => result.status === "rejected"
        ? `${transports[index].name}:${sanitizeError(result.reason)}`
        : "")
      .filter(Boolean)
      .join("|") || "operations_alert_no_transport");
  }

  resolveIncidentWithoutAlert(key, now = new Date()) {
    this.sqlExec(
      `UPDATE incidents SET status = 'resolved', resolved_at = ?,
         last_attempted_at = ?, alert_error = ''
       WHERE key = ? AND status = 'active'`,
      now.toISOString(),
      now.toISOString(),
      key,
    );
  }

  operationalHealthDue(now = new Date()) {
    const intervalSeconds = envNumber(
      this.env,
      "OPS_HEALTH_INTERVAL_SECONDS",
      OPS_HEALTH_INTERVAL_SECONDS,
      60,
      3_600,
    );
    const lastCheckedAt = Date.parse(this.metadataValue("ops_health_checked_at") || "");
    return !Number.isFinite(lastCheckedAt) ||
      now.getTime() - lastCheckedAt >= intervalSeconds * 1_000;
  }

  async processOperationalHealth(now = new Date()) {
    const api = this.runtimeSnapshot("api");
    const apiContract = this.runtimeSnapshot("api_contract");
    const sourceHealth = this.runtimeSnapshot("source_health");
    const webhook = this.runtimeSnapshot("webhook");
    const apiFailureStreak = Number(
      api.failureStreak ?? this.metadataValue("api_failure_streak") ?? 0,
    );
    const fullSourceSuccessAt = Date.parse(
      sourceHealth.fullSourceSuccessAt || this.metadataValue("full_source_success_at") || "",
    );
    const secondsSinceFullSourceSuccess = Number.isFinite(fullSourceSuccessAt)
      ? Math.max(0, (now.getTime() - fullSourceSuccessAt) / 1_000)
      : 0;
    const current = sourceHealth.sourceLastComparison ||
      parseRuntimeSnapshot(this.metadataValue("source_last_comparison"));
    const sourceDetails = `API ${current.apiTickets || 0}, HTML ${current.listingTickets || 0}, ` +
      `cobertura ${current.apiCoveragePercent ?? 0}%`;
    const queueSlo = this.deliveryQueueSlo(now);
    const queueBreach = (
      queueSlo.criticalPending > 0 && queueSlo.oldestAgeMs >= 45_000
    ) || (
      queueSlo.secondaryPending > 0 && queueSlo.oldestAgeMs >= 10 * 60_000
    );
    const previousQueueBreachStreak = Number(
      this.metadataValue("delivery_slo_breach_streak") || 0,
    );
    const queueSloBreachStreak = queueBreach ? previousQueueBreachStreak + 1 : 0;
    this.setMetadataIfChanged(
      "delivery_slo_breach_streak",
      queueSloBreachStreak,
    );
    const webhookChecked = Boolean(
      webhook.checkedAt || this.metadataValue("telegram_webhook_checked_at"),
    );
    const signals = buildIncidentSignals({
      apiError: api.lastError ?? this.metadataValue("api_last_error"),
      apiFailureStreak,
      apiContract,
      apiAuthorizationExpiresAt: authorizationExpiresAt(this.env.UOL_API_AUTHORIZATION),
      webhookUrlMatches: !webhookChecked ||
        (webhook.urlMatches ?? this.metadataValue("telegram_webhook_url_matches") === "true"),
      webhookPendingUpdates: Number(
        webhook.pendingUpdates ?? this.metadataValue("telegram_webhook_pending_updates") ?? 0,
      ),
      webhookError: webhook.lastError || this.metadataValue("telegram_webhook_last_error") ||
        this.metadataValue("telegram_webhook_check_error"),
      failedRunStreak: this.failedRunStreak(),
      listingFailureStreak: Number(
        sourceHealth.listingFailureStreak ?? this.metadataValue("listing_failure_streak") ?? 0,
      ),
      listingDropStreak: Number(
        sourceHealth.listingDropStreak ?? this.metadataValue("listing_drop_streak") ?? 0,
      ),
      sourceDivergenceStreak: Number(
        sourceHealth.sourceDivergenceStreak ??
          this.metadataValue("source_divergence_streak") ?? 0,
      ),
      secondsSinceFullSourceSuccess,
      sourceDetails,
      ticketIssues: this.recentTicketDeliveryIssues(now),
      deliveryIssues: this.deliveryQueueIssues(now),
      queueSlo,
      queueSloBreachStreak,
      now,
    });
    const activeKeys = new Set(signals.map((signal) => signal.key));
    const existing = new Map(this.sqlExec(
      "SELECT * FROM incidents",
    ).toArray().map((row) => [row.key, row]));
    const nowIso = now.toISOString();
    const cooldownMinutes = envNumber(this.env, "OPS_ALERT_COOLDOWN_MINUTES", 360, 15, 1_440);
    const retrySeconds = envNumber(this.env, "OPS_ALERT_RETRY_SECONDS", 60, 15, 900);
    let alerted = 0;
    let recovered = 0;

    for (const signal of signals) {
      const row = existing.get(signal.key);
      const lastAttempt = Date.parse(row?.last_attempted_at || "");
      const lastAlert = Date.parse(row?.last_alerted_at || "");
      const newlyActive = !row || row.status !== "active";
      const previousAttemptFailed = Boolean(row?.alert_error);
      const retryElapsed = !Number.isFinite(lastAttempt) ||
        now.getTime() - lastAttempt >= retrySeconds * 1_000;
      const cooldownElapsed = !Number.isFinite(lastAlert) ||
        now.getTime() - lastAlert >= cooldownMinutes * 60_000;
      this.sqlExec(
        `INSERT INTO incidents(
           key, status, severity, summary, details, first_detected_at, last_detected_at
         ) VALUES (?, 'active', ?, ?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           status = 'active', severity = excluded.severity, summary = excluded.summary,
           details = excluded.details, last_detected_at = excluded.last_detected_at,
           resolved_at = '', occurrence_count = incidents.occurrence_count + 1`,
        signal.key, signal.severity, signal.summary, signal.details, nowIso, nowIso,
      );
      if (
        !newlyActive &&
        !(previousAttemptFailed ? retryElapsed : cooldownElapsed)
      ) continue;
      this.sqlExec(
        "UPDATE incidents SET last_attempted_at = ?, alert_error = '' WHERE key = ?",
        nowIso,
        signal.key,
      );
      try {
        await this.sendOperationalAlert(buildOperationsAlert(signal));
        this.sqlExec(
          "UPDATE incidents SET last_alerted_at = ?, alert_error = '' WHERE key = ?",
          nowIso,
          signal.key,
        );
        alerted += 1;
      } catch (error) {
        this.sqlExec(
          "UPDATE incidents SET alert_error = ? WHERE key = ?",
          sanitizeError(error),
          signal.key,
        );
      }
    }

    for (const row of existing.values()) {
      if (row.status !== "active" || activeKeys.has(row.key)) continue;
      const signal = {
        key: row.key,
        severity: row.severity,
        summary: row.summary,
        details: row.details,
      };
      this.sqlExec(
        `UPDATE incidents SET status = 'resolved', resolved_at = ?,
          last_attempted_at = ?, alert_error = '' WHERE key = ?`,
        nowIso,
        nowIso,
        row.key,
      );
      if (row.severity !== "critical") {
        recovered += 1;
        continue;
      }
      try {
        await this.sendOperationalAlert(buildOperationsAlert(signal, { recovered: true }));
        this.sqlExec(
          "UPDATE incidents SET last_alerted_at = ? WHERE key = ?",
          nowIso,
          row.key,
        );
        recovered += 1;
      } catch (error) {
        this.sqlExec(
          "UPDATE incidents SET alert_error = ? WHERE key = ?",
          sanitizeError(error),
          row.key,
        );
      }
    }
    this.setMetadata("ops_health_checked_at", nowIso);
    return { active: signals.length, alerted, recovered };
  }

  insertCard(card, nowIso, status) {
    const firstObservedAt = String(card.observedAt || nowIso);
    this.sqlExec(
      `INSERT OR IGNORE INTO offers(
        id, link, preview_title, title, category, card_image_url,
        partner_image_url, partner_name, first_seen_at, last_seen_at, status
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      card.id,
      card.link,
      card.previewTitle,
      card.previewTitle,
      card.category,
      card.cardImageUrl,
      card.partnerImageUrl,
      card.partnerName,
      firstObservedAt,
      nowIso,
      status,
    );
    this.recordIdentityAliases(card.id, card.id, card.link, firstObservedAt);
  }

  identityAliases(id, link) {
    const sourceKey = offerSourceKey(link);
    return [...new Set([
      String(id || "").trim() ? `id:${String(id).trim()}` : "",
      ...offerIdentityKeys(link),
      sourceKey ? `source:${sourceKey}` : "",
    ].filter(Boolean))];
  }

  recordIdentityAliases(offerId, sourceId, link, firstSeenAt) {
    const aliases = this.identityAliases(sourceId, link);
    if (!offerId || !aliases.length) return;
    const values = aliases.map(() => "(?, ?, ?)").join(", ");
    this.sqlExec(
      `INSERT OR IGNORE INTO offer_identity_aliases(alias, offer_id, first_seen_at)
       VALUES ${values}`,
      ...aliases.flatMap((alias) => [alias, offerId, firstSeenAt]),
    );
  }

  queryIdentityRows(candidateAliases) {
    const aliases = [...new Set(candidateAliases || [])].filter(Boolean);
    if (!aliases.length) return [];
    const rows = [];
    for (let offset = 0; offset < aliases.length; offset += IDENTITY_QUERY_CHUNK_SIZE) {
      const chunk = aliases.slice(offset, offset + IDENTITY_QUERY_CHUNK_SIZE);
      rows.push(...this.sqlExec(
        `SELECT
           o.id, o.link, o.status, o.status_before_sold_out, o.main_sent_at,
           o.canal2_sent_at, o.first_seen_at, o.last_seen_at, o.discard_reason,
           a.alias AS matched_alias,
           CASE
             WHEN a.alias LIKE 'id:%' THEN 0
             WHEN a.alias LIKE 'slug:%' THEN 1
             ELSE 2
           END AS alias_priority
         FROM offer_identity_aliases AS a
         JOIN offers AS o ON o.id = a.offer_id
         WHERE a.alias IN (${chunk.map(() => "?").join(", ")})
         ORDER BY alias_priority ASC, o.first_seen_at DESC, o.id ASC
         LIMIT 4096`,
        ...chunk,
      ).toArray());
    }
    return rows;
  }

  identityRowsForCardRows(rows, card, aliases) {
    const allowed = new Set(aliases || []);
    const byId = new Map();
    for (const row of rows || []) {
      if (!allowed.has(row.matched_alias)) continue;
      if (!offerIdentityCompatible({ id: row.id, link: row.link }, card)) continue;
      const previous = byId.get(row.id);
      if (!previous ||
          Number(row.alias_priority || 0) < Number(previous.alias_priority || 0) ||
          (Number(row.alias_priority || 0) === Number(previous.alias_priority || 0) &&
            String(row.first_seen_at).localeCompare(String(previous.first_seen_at)) > 0)) {
        byId.set(row.id, row);
      }
    }
    return [...byId.values()].sort((left, right) => (
      Number(left.alias_priority || 0) - Number(right.alias_priority || 0) ||
      String(right.first_seen_at).localeCompare(String(left.first_seen_at)) ||
      String(left.id).localeCompare(String(right.id))
    )).slice(0, 32);
  }

  findIdentityRows(card) {
    const aliases = this.identityAliases(card.id, card.link);
    if (!aliases.length) return [];
    const exactAliases = aliases.filter((alias) => !alias.startsWith("source:"));
    const exactRows = this.identityRowsForCardRows(
      this.queryIdentityRows(exactAliases),
      card,
      exactAliases,
    );
    return exactRows.length
      ? exactRows
      : this.identityRowsForCardRows(
          this.queryIdentityRows(aliases.filter((alias) => alias.startsWith("source:"))),
          card,
          aliases.filter((alias) => alias.startsWith("source:")),
        );
  }

  identityLookupKey(card) {
    return `${String(card?.id || "")}\u0000${String(card?.link || "")}`;
  }

  findIdentityRowsForCards(cards) {
    const descriptors = (cards || []).map((card) => {
      const aliases = this.identityAliases(card.id, card.link);
      return {
        card,
        aliases,
        exactAliases: aliases.filter((alias) => !alias.startsWith("source:")),
        sourceAliases: aliases.filter((alias) => alias.startsWith("source:")),
      };
    });
    const exactRows = this.queryIdentityRows(
      descriptors.flatMap((descriptor) => descriptor.exactAliases),
    );
    const result = new Map();
    const missingSource = [];
    for (const descriptor of descriptors) {
      const rows = this.identityRowsForCardRows(
        exactRows,
        descriptor.card,
        descriptor.exactAliases,
      );
      if (rows.length) result.set(this.identityLookupKey(descriptor.card), rows);
      else missingSource.push(descriptor);
    }
    if (missingSource.length) {
      const sourceRows = this.queryIdentityRows(
        missingSource.flatMap((descriptor) => descriptor.sourceAliases),
      );
      for (const descriptor of missingSource) {
        result.set(
          this.identityLookupKey(descriptor.card),
          this.identityRowsForCardRows(sourceRows, descriptor.card, descriptor.sourceAliases),
        );
      }
    }
    return result;
  }

  chooseIdentityKeeper(rows) {
    return [...rows].sort((a, b) => {
      const aSent = Boolean(a.main_sent_at || a.canal2_sent_at);
      const bSent = Boolean(b.main_sent_at || b.canal2_sent_at);
      if (aSent !== bSent) return aSent ? -1 : 1;
      const aBaseline = a.status === "baseline";
      const bBaseline = b.status === "baseline";
      if (aBaseline !== bBaseline) return aBaseline ? -1 : 1;
      return String(a.first_seen_at).localeCompare(String(b.first_seen_at));
    })[0];
  }

  reconcileIdentityAliases() {
    const rows = this.sqlExec(
      `SELECT id, link, status, first_seen_at, main_sent_at, canal2_sent_at
       FROM offers
       WHERE status <> 'discarded'`,
    ).toArray();
    const groups = new Map();
    for (const row of rows) {
      const sourceKey = offerSourceKey(row.link);
      if (!sourceKey) continue;
      const group = groups.get(sourceKey) || [];
      group.push(row);
      groups.set(sourceKey, group);
    }

    let reconciled = 0;
    for (const group of groups.values()) {
      let remaining = [...group];
      while (remaining.length) {
        const anchor = remaining.shift();
        const direct = [
          anchor,
          ...remaining.filter((candidate) => offerIdentityCompatible(anchor, candidate)),
        ];
        const keeper = this.chooseIdentityKeeper(direct);
        const component = direct.filter(
          (candidate) => offerIdentityCompatible(keeper, candidate),
        );
        const componentIds = new Set(component.map((candidate) => candidate.id));
        remaining = remaining.filter((candidate) => !componentIds.has(candidate.id));
        if (component.length < 2) continue;
        for (const duplicate of component) {
          if (duplicate.id === keeper.id) continue;
          this.sqlExec(
            `UPDATE offers SET
               status = 'discarded',
               decision_at = CASE WHEN decision_at = '' THEN datetime('now') ELSE decision_at END,
               would_send_main = 0,
               would_send_canal2 = 0,
               discard_reason = 'duplicada_identidade',
               missing_since = '',
               absence_count = 0
             WHERE id = ?`,
            duplicate.id,
          );
          reconciled += 1;
        }
      }
    }
    return reconciled;
  }

  repairConflictingOfferLinks(card, nowIso) {
    let repaired = 0;
    let currentUrl;
    try {
      currentUrl = new URL(card.link);
    } catch {
      return repaired;
    }
    const conflicts = this.sqlExec(
      `SELECT id, link FROM offers
       WHERE link = ? AND id <> ?
       ORDER BY first_seen_at ASC
       LIMIT 16`,
      card.link,
      card.id,
    ).toArray();
    for (const row of conflicts) {
      if (offerIdentityCompatible({ id: row.id, link: row.link }, card)) continue;
      const restoredId = String(row.id || "").replace(/--\d{12,14}$/u, "");
      if (!restoredId) continue;
      const restoredUrl = new URL(currentUrl.href);
      const segments = restoredUrl.pathname.split("/").filter(Boolean);
      segments[segments.length - 1] = restoredId;
      restoredUrl.pathname = `/${segments.join("/")}`;
      const cursor = this.sqlExec(
        "UPDATE offers SET link = ? WHERE id = ? AND link = ?",
        restoredUrl.href,
        row.id,
        card.link,
      );
      if (Number(cursor.rowsWritten || 0) > 0) {
        this.recordIdentityAliases(row.id, row.id, restoredUrl.href, nowIso);
        repaired += 1;
      }
    }
    return repaired;
  }

  async backfillTitleValidityKeys() {
    const rows = this.sqlExec(
      `SELECT id, preview_title, title, validity, description
       FROM offers
       WHERE title_validity_key = ''
         AND (title <> '' OR preview_title <> '')
         AND validity <> ''
       ORDER BY first_seen_at DESC
       LIMIT 300`,
    ).toArray();
    for (const row of rows) {
      const keys = await buildDedupeKeys({
        title: row.title || row.preview_title,
        validity: row.validity,
        description: row.description,
      });
      this.sqlExec(
        "UPDATE offers SET title_validity_key = ? WHERE id = ?",
        keys.titleValidityKey,
        row.id,
      );
    }
    return rows.length;
  }

  resolveListingCards(
    cards,
    nowIso,
    newStatus,
    { availabilityIdentityKeys = null, restockIdentityKeys = null } = {},
  ) {
    const lastSeenTouchMinutes = envNumber(
      this.env,
      "OFFER_LAST_SEEN_TOUCH_MINUTES",
      15,
      1,
      1_440,
    );
    const lastSeenTouchCutoff = new Date(
      Date.parse(nowIso) - lastSeenTouchMinutes * 60_000,
    ).toISOString();
    for (const card of cards) this.repairConflictingOfferLinks(card, nowIso);
    const identityRowsByCard = this.findIdentityRowsForCards(cards);
    const resolved = [];
    const resolvedIds = new Set();
    let inserted = 0;
    const insertedIds = [];
    for (const card of cards) {
      const identityRows = identityRowsByCard.get(this.identityLookupKey(card)) || [];
      const activeRows = identityRows.filter((row) => row.status !== "discarded");
      const existing = activeRows.length ? this.chooseIdentityKeeper(activeRows) : null;

      if (existing) {
        if (resolvedIds.has(existing.id)) continue;
        const authoritativePresence = availabilityIdentityKeys &&
          offerIdentityKeys(card.link).some((key) => availabilityIdentityKeys.has(key));
        const availabilitySet = authoritativePresence
          ? ", missing_since = '', absence_count = 0"
          : "";
        const availabilityWhere = authoritativePresence
          ? " OR missing_since <> '' OR absence_count <> 0"
          : "";
        this.sqlExec(
          `UPDATE offers SET
             link = ?, preview_title = ?, category = ?, card_image_url = ?,
             partner_image_url = ?, partner_name = ?,
             last_seen_at = CASE WHEN last_seen_at < ? THEN ? ELSE last_seen_at END
             ${availabilitySet}
           WHERE id = ?
             AND (
               link <> ? OR preview_title <> ? OR category <> ? OR
               card_image_url <> ? OR partner_image_url <> ? OR partner_name <> ? OR
               last_seen_at < ?${availabilityWhere}
             )`,
          card.link,
          card.previewTitle,
          card.category,
          card.cardImageUrl,
          card.partnerImageUrl,
          card.partnerName,
          lastSeenTouchCutoff,
          nowIso,
          existing.id,
          card.link,
          card.previewTitle,
          card.category,
          card.cardImageUrl,
          card.partnerImageUrl,
          card.partnerName,
          lastSeenTouchCutoff,
        );
        this.recordIdentityAliases(existing.id, card.id, card.link, nowIso);
        if (authoritativePresence && isTicketCampaign(card)) {
          this.clearTicketAbsenceProbe(existing.id);
        }
        if (
          restockIdentityKeys &&
          offerIdentityKeys(card.link).some((key) => restockIdentityKeys.has(key)) &&
          ["sold_out", "shadow_sold_out"].includes(existing.status)
        ) {
          const restockedStatus = existing.status === "shadow_sold_out"
            ? "shadow_candidate"
            : existing.main_sent_at
              ? "restocked_pending_sync"
              : existing.status_before_sold_out || "delivery_pending";
          this.sqlExec(
            `UPDATE offers SET status = ?, sold_out_at = '', restocked_at = ?,
               delivery_generation = ?,
               missing_since = '', absence_count = 0,
               main_restock_synced_at = '', canal2_restock_synced_at = '',
               main_restock_attempts = 0, canal2_restock_attempts = 0,
               main_restock_error = '', canal2_restock_error = '',
               main_restock_next_attempt_at = '', canal2_restock_next_attempt_at = ''
             WHERE id = ?`,
            restockedStatus,
            nowIso,
            this.currentDeliveryGeneration(),
            existing.id,
          );
          this.sqlExec(
            `INSERT INTO discord_availability_sync(offer_id)
             VALUES (?)
             ON CONFLICT(offer_id) DO UPDATE SET
               restock_synced_at = '', restock_attempts = 0,
               restock_error = '', restock_next_attempt_at = ''`,
            existing.id,
          );
        }
        resolved.push({ ...card, id: existing.id });
        resolvedIds.add(existing.id);
        continue;
      }

      // Um ID terminal já conhecido não volta a ser novidade só porque uma
      // fonte mais lenta ainda o mantém no payload.
      const terminal = identityRows.find((row) => row.id === card.id);
      if (terminal) {
        // Older versions discarded distinct public URLs when their visible
        // content matched another benefit. The exact page being visible again
        // is authoritative evidence that it is independently redeemable.
        if (
          terminal.status === "discarded" &&
          terminal.discard_reason === "duplicada_conteudo"
        ) {
          this.sqlExec(
            `UPDATE offers SET
               link = ?, preview_title = ?, title = ?, category = ?,
               card_image_url = ?, partner_image_url = ?, partner_name = ?,
               last_seen_at = ?, status = ?, decision_at = '',
               would_send_main = 0, would_send_canal2 = 0, discard_reason = '',
               dedupe_key = '', loose_dedupe_key = '', title_validity_key = '',
               detail_quality = '', detail_attempts = 0, detail_error = '',
               delivery_mode = '', delivery_quarantine_reason = ''
             WHERE id = ? AND status = 'discarded'
               AND discard_reason = 'duplicada_conteudo'`,
            card.link,
            card.previewTitle,
            card.previewTitle,
            card.category,
            card.cardImageUrl,
            card.partnerImageUrl,
            card.partnerName,
            nowIso,
            newStatus,
            card.id,
          );
          this.recordIdentityAliases(card.id, card.id, card.link, nowIso);
          resolved.push(card);
          resolvedIds.add(card.id);
          inserted += 1;
          insertedIds.push(card.id);
          continue;
        }
        const lastSeenAt = Date.parse(terminal?.last_seen_at || "");
        const reuseCooldownHours = envNumber(
          this.env,
          "REUSED_ID_COOLDOWN_HOURS",
          168,
          24,
          2_160,
        );
        const reusable = terminal?.status === "discarded" && Number.isFinite(lastSeenAt) &&
          Date.parse(nowIso) - lastSeenAt >= reuseCooldownHours * 3_600_000;
        this.sqlExec(
          "UPDATE offers SET last_seen_at = ? WHERE id = ? AND last_seen_at < ?",
          nowIso,
          terminal.id,
          nowIso,
        );
        if (reusable) {
          const cycle = nowIso.replace(/\D/g, "").slice(0, 12);
          const reusedCard = { ...card, id: `${card.id}--${cycle}` };
          this.insertCard(reusedCard, nowIso, newStatus);
          resolved.push(reusedCard);
          resolvedIds.add(reusedCard.id);
          inserted += 1;
          insertedIds.push(reusedCard.id);
          continue;
        }
        resolvedIds.add(card.id);
        continue;
      }

      this.insertCard(card, nowIso, newStatus);
      resolved.push(card);
      resolvedIds.add(card.id);
      inserted += 1;
      insertedIds.push(card.id);
    }
    return { cards: resolved, inserted, insertedIds };
  }

  async processPending(cardsById, now, { priorityIds = [] } = {}) {
    const priority = [...new Set(priorityIds)].slice(0, 100);
    const batchSize = priority.length ||
      envNumber(this.env, "DETAIL_BATCH_SIZE", 4, 1, 8);
    const priorityOrder = priority.length
      ? `CASE WHEN id IN (${priority.map(() => "?").join(", ")}) THEN 0 ELSE 1 END,`
      : "";
    const pendingRows = this.sqlExec(
      `SELECT id, link, preview_title, category, card_image_url,
              partner_image_url, partner_name, detail_attempts,
              main_message_id, main_message_kind
       FROM offers
       WHERE status = 'pending_enrichment'
       ORDER BY ${priorityOrder} first_seen_at ASC
       LIMIT ?`,
      ...priority,
      batchSize,
    ).toArray();

    const cards = pendingRows.map((row) => cardsById.get(row.id) || {
      id: row.id,
      link: row.link,
      previewTitle: row.preview_title,
      category: row.category,
      cardImageUrl: row.card_image_url,
      partnerImageUrl: row.partner_image_url,
      partnerName: row.partner_name,
    });
    const enriched = cards.map((card) => (
      (card.apiDetail ? prepareImmediateApiOffer(card) : null) ||
      prepareImmediateListingOffer(card)
    ));
    let wouldSendMain = 0;
    let wouldSendCanal2 = 0;

    for (let index = 0; index < enriched.length; index += 1) {
      const offer = enriched[index];
      const previousAttempts = Number(pendingRows[index]?.detail_attempts || 0);
      const attempts = previousAttempts + 1;
      if (!offer.detailOk && attempts < 3) {
        this.sqlExec(
          `UPDATE offers
           SET detail_attempts = ?, detail_error = ?, detail_quality = ?
           WHERE id = ?`,
          attempts,
          offer.detailError,
          offer.quality,
          offer.id,
        );
        continue;
      }

      const keys = await buildDedupeKeys(offer);
      const resendThreshold = new Date(
        now.getTime() -
          envNumber(this.env, "RECENT_RESEND_BLOCK_HOURS", 168, 1, 720) * 3_600_000,
      ).toISOString();
      const sourceKey = offerSourceKey(offer.link);
      const [sourcePartner, sourceCode] = sourceKey.split("|");
      const sourceClause = sourceKey ? "AND LOWER(link) LIKE ?" : "";
      const duplicateCandidates = this.sqlExec(
        `SELECT id, link FROM offers
         WHERE id <> ?
           ${sourceClause}
           AND (
             (dedupe_key <> '' AND dedupe_key IN (?, ?))
             OR (loose_dedupe_key <> '' AND loose_dedupe_key IN (?, ?))
             OR (title_validity_key <> '' AND title_validity_key = ?)
           )
           AND (
             main_sent_at >= ?
             OR status = 'baseline'
             OR (
               decision_at >= ?
               AND status IN (
                 'delivery_pending', 'partial_delivery', 'shadow_candidate',
                 'delivery_unknown', 'delivery_quarantined'
               )
             )
           )
         ORDER BY first_seen_at DESC, id ASC
         LIMIT 32`,
        offer.id,
        ...(sourceKey ? [`%/${sourcePartner}/${sourceCode}-%`] : []),
        keys.dedupeKey,
        keys.legacyDedupeKey,
        keys.looseDedupeKey,
        keys.legacyLooseDedupeKey,
        keys.titleValidityKey,
        resendThreshold,
        resendThreshold,
      ).toArray();
      const duplicate = duplicateCandidates.find((candidate) => {
        if (!sourceKey) return true;
        return offerSourceKey(candidate.link) === sourceKey;
      });

      const decision = duplicate
        ? {
            eligible: false,
            discardReason: "duplicada_conteudo",
            wouldSendMain: false,
            wouldSendCanal2: false,
          }
        : decideShadowDelivery(offer, {
            now,
            maxValidFromAgeHours: envNumber(this.env, "MAX_VALID_FROM_AGE_HOURS", 36, 1, 720),
          });
      const currentMode = this.currentDeliveryMode();
      const deliveryGeneration = this.currentDeliveryGeneration();
      const status = decision.eligible
        ? (currentMode === "live" ? "delivery_pending" : "shadow_candidate")
        : "discarded";
      const decisionAt = now.toISOString();
      this.sqlExec(
        `UPDATE offers SET
          title = ?, image_url = ?, validity = ?, description = ?,
          dedupe_key = ?, loose_dedupe_key = ?, title_validity_key = ?,
          detail_quality = ?,
          detail_attempts = ?, detail_error = ?, decision_at = ?,
          would_send_main = ?, would_send_canal2 = ?, discard_reason = ?,
          status = ?, delivery_mode = ?, delivery_generation = ?,
          delivery_quarantine_reason = ''
         WHERE id = ?`,
        offer.title,
        offer.imageUrl,
        offer.validity,
        offer.description,
        keys.dedupeKey,
        keys.looseDedupeKey,
        keys.titleValidityKey,
        offer.quality,
        attempts,
        offer.detailError,
        decisionAt,
        decision.wouldSendMain ? 1 : 0,
        decision.wouldSendCanal2 ? 1 : 0,
        decision.discardReason,
        status,
        currentMode,
        deliveryGeneration,
        offer.id,
      );
      wouldSendMain += decision.wouldSendMain ? 1 : 0;
      wouldSendCanal2 += decision.wouldSendCanal2 ? 1 : 0;
    }

    return {
      enriched: enriched.length,
      wouldSendMain,
      wouldSendCanal2,
    };
  }

  refreshDeliveryStatus(id, now = new Date()) {
    const row = this.sqlExec(
      "SELECT * FROM offers WHERE id = ? LIMIT 1",
      id,
    ).toArray()[0];
    if (!row) return null;
    const configuration = deliveryConfiguration(
      this.env,
      telegramConfiguration(this.env),
      discordConfiguration(this.env),
    );
    const classification = classifyDeliveryRow(row, configuration, {
      ticket: isTicketCampaign(rowToOffer(row)),
      maxAttempts: envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50),
      inFlightStaleSeconds: envNumber(
        this.env,
        "DELIVERY_IN_FLIGHT_STALE_SECONDS",
        60,
        30,
        600,
      ),
      now,
    });
    const unknownTargets = [
      ...(classification.unknownTargets || []),
      ...(classification.state === "unknown" ? classification.targets || [] : []),
      classification.state === "unknown" ? classification.target || "" : "",
    ].filter(Boolean).filter(
      (target, index, values) => values.indexOf(target) === index,
    );
    let reconciledStatus = row.status;
    if (unknownTargets.length) {
      reconciledStatus = "delivery_unknown";
      this.sqlExec(
        `UPDATE offers SET status = 'delivery_unknown',
           delivery_unknown_at = CASE
             WHEN delivery_unknown_at = '' THEN ? ELSE delivery_unknown_at END,
           delivery_unknown_target = ? WHERE id = ?`,
        now.toISOString(),
        unknownTargets.join(","),
        row.id,
      );
    } else if (classification.state === "complete") {
      reconciledStatus = "delivered";
      this.sqlExec(
        `UPDATE offers SET status = 'delivered', delivery_dead_letter_at = '',
           delivery_dead_letter_reason = '', delivery_unknown_at = '',
           delivery_unknown_target = '' WHERE id = ?`,
        row.id,
      );
    } else if (classification.state === "dead_letter") {
      reconciledStatus = "delivery_dead_letter";
      this.sqlExec(
        `UPDATE offers SET status = 'delivery_dead_letter',
           delivery_dead_letter_at = CASE
             WHEN delivery_dead_letter_at = '' THEN ? ELSE delivery_dead_letter_at END,
           delivery_dead_letter_reason = 'delivery_attempts_exhausted' WHERE id = ?`,
        now.toISOString(),
        row.id,
      );
    } else if (classification.state === "blocked_configuration") {
      reconciledStatus = "delivery_blocked_configuration";
      this.sqlExec(
        `UPDATE offers SET status = 'delivery_blocked_configuration',
           delivery_dead_letter_reason = 'delivery_configuration_incomplete' WHERE id = ?`,
        row.id,
      );
    } else {
      reconciledStatus = row.main_sent_at ? "partial_delivery" : "delivery_pending";
      this.sqlExec(
        `UPDATE offers SET status = ?, delivery_dead_letter_at = '',
           delivery_dead_letter_reason = '', delivery_unknown_at = '',
           delivery_unknown_target = '' WHERE id = ?`,
        reconciledStatus,
        row.id,
      );
    }
    if (reconciledStatus !== row.status) {
      this.recordDeliveryLedgerEvent({
        offerId: row.id,
        target: "aggregate",
        operation: "reconciliation",
        state: reconciledStatus,
        attempt: 0,
        generation: row.delivery_generation,
        occurredAt: now.toISOString(),
      });
    }
    return { row, classification, unknownTargets };
  }

  releaseExpiredMainUnknowns(now = new Date()) {
    const retrySeconds = envNumber(
      this.env,
      "MAIN_AMBIGUOUS_RETRY_SECONDS",
      30,
      15,
      300,
    );
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const cutoff = new Date(now.getTime() - retrySeconds * 1_000).toISOString();
    const rows = this.sqlExec(
      `SELECT id FROM offers
       WHERE main_sent_at = ''
         AND main_delivery_unknown_at > ''
         AND main_delivery_unknown_at <= ?
         AND main_delivery_attempts < ?
       ORDER BY main_delivery_unknown_at ASC
       LIMIT 100`,
      cutoff,
      maxAttempts,
    ).toArray();
    for (const row of rows) {
      // Telegram não oferece chave de idempotência. O forward da discussão
      // tem uma janela curta para reconciliar o primeiro envio. Depois dela,
      // o principal prefere duplicata rara a perder uma oferta curta.
      this.sqlExec(
        `UPDATE offers SET main_delivery_unknown_at = '',
           main_delivery_in_flight_at = '', main_delivery_next_attempt_at = '',
           delivery_unknown_at = '', delivery_unknown_target = '',
           status = 'delivery_pending'
         WHERE id = ? AND main_sent_at = ''`,
        row.id,
      );
      this.refreshDeliveryStatus(row.id, now);
    }
    return rows.length;
  }

  deliveryQueueSlo(now = new Date()) {
    const rows = this.sqlExec(
      `SELECT status, first_seen_at, decision_at, main_sent_at
       FROM offers
       WHERE status IN (
         'delivery_pending', 'partial_delivery', 'delivery_unknown',
         'delivery_dead_letter', 'delivery_blocked_configuration'
       )
       ORDER BY first_seen_at ASC
       LIMIT 128`,
    ).toArray();
    return summarizeQueueSlo(rows, now);
  }

  async processDeliveryQueue(
    now,
    {
      priorityIds = [],
      rows = null,
      waitForMainImage = true,
      targetNames = [],
    } = {},
  ) {
    if (this.currentDeliveryMode() !== "live") {
      return { mainSent: 0, canal2Sent: 0, discordSent: 0, failed: 0 };
    }
    const telegram = telegramConfiguration(this.env);
    const discord = discordConfiguration(this.env);
    const configuration = deliveryConfiguration(this.env, telegram, discord);
    const generation = this.currentDeliveryGeneration();
    const priorityList = [...new Set(priorityIds)].slice(0, 100);
    const priority = new Set(priorityList);
    const requestedTargets = [...new Set(targetNames)].filter(
      (target) => ["main", "canal2", "discord"].includes(target),
    );
    const queueSlo = this.deliveryQueueSlo(now);
    const budget = chooseDeliveryBudget({
      storageReadBudget: this.storageUsageSnapshot(now),
      queueSlo,
      configuredBatch: envNumber(this.env, "DELIVERY_BATCH_SIZE", 4, 1, 8),
      configuredConcurrency: envNumber(this.env, "DELIVERY_CONCURRENCY", 6, 1, 6),
      priorityCount: priority.size,
    });
    const hasRecentSuppliedRows = Array.isArray(rows) && rows.some(
      (row) => isRecentDeliveryDecision(row, now),
    );
    const prioritySecondaryOnly = budget.reason === "quota_reserve" && (
      (budget.allowPrioritySecondary && priority.size > 0) ||
      hasRecentSuppliedRows
    );
    if (
      budget.deferSecondary &&
      requestedTargets.length &&
      !requestedTargets.includes("main") &&
      !prioritySecondaryOnly
    ) {
      return {
        mainSent: 0,
        canal2Sent: 0,
        discordSent: 0,
        failed: 0,
        deferred: true,
        deferredReason: budget.reason,
      };
    }
    const batchSize = Math.max(
      budget.batchSize,
      Math.min(100, priority.size),
    );
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const inFlightStaleSeconds = envNumber(
      this.env,
      "DELIVERY_IN_FLIGHT_STALE_SECONDS",
      60,
      30,
      600,
    );
    const priorityOrder = priorityList.length
      ? `CASE WHEN id IN (${priorityList.map(() => "?").join(", ")}) THEN 0 ELSE 1 END,`
      : "";
    const recentPriorityOrder = budget.reason === "quota_reserve"
      ? "CASE WHEN decision_at >= ? AND decision_at <= ? THEN 0 ELSE 1 END,"
      : "";
    const recentPriorityArguments = budget.reason === "quota_reserve"
      ? [
          new Date(now.getTime() - RECENT_DELIVERY_PRIORITY_MS).toISOString(),
          now.toISOString(),
        ]
      : [];
    const candidateLimit = Math.min(256, Math.max(batchSize * 8, priorityList.length + 32));
    const candidates = rows || this.sqlExec(
      `SELECT * FROM offers
       WHERE status IN (
         'delivery_pending', 'partial_delivery', 'delivery_blocked_configuration',
         'delivery_dead_letter', 'delivery_unknown'
       )
         AND delivery_generation = ?
       ORDER BY ${priorityOrder} ${recentPriorityOrder} first_seen_at ASC
       LIMIT ?`,
      generation,
      ...priorityList,
      ...recentPriorityArguments,
      candidateLimit,
    ).toArray();
    const actionable = [];

    for (const row of candidates) {
      const rowIsPriority = priority.has(row.id);
      const rowIsRecentPriority = budget.reason === "quota_reserve" &&
        isRecentDeliveryDecision(row, now);
      const priorityMayUseSecondary = !budget.deferSecondary || (
        budget.reason === "quota_reserve" && (
          (rowIsPriority && budget.allowPrioritySecondary) ||
          rowIsRecentPriority
        )
      );
      const rowTargets = budget.deferSecondary && !priorityMayUseSecondary
        ? requestedTargets.includes("main") ? ["main"] : []
        : requestedTargets;
      if (!rowTargets.length) continue;
      const classification = classifyDeliveryRow(row, configuration, {
        ticket: isTicketCampaign(rowToOffer(row)),
        maxAttempts,
        inFlightStaleSeconds,
        targetNames: rowTargets,
        now,
      });
      for (const target of classification.staleUnknownTargets || []) {
        const columns = {
          main: ["main_delivery_in_flight_at", "main_delivery_unknown_at"],
          canal2: ["canal2_delivery_in_flight_at", "canal2_delivery_unknown_at"],
          discord: ["discord_delivery_in_flight_at", "discord_delivery_unknown_at"],
        }[target];
        if (!columns) continue;
        this.sqlExec(
          `UPDATE offers SET ${columns[0]} = '', ${columns[1]} = ?,
             status = 'delivery_unknown', delivery_unknown_at = ?,
             delivery_unknown_target = ? WHERE id = ?`,
          now.toISOString(),
          now.toISOString(),
          target,
          row.id,
        );
      }
      if (classification.state === "actionable") {
        actionable.push({
          row,
          classification,
          recentPriority: rowIsRecentPriority,
          targetNames: rowTargets,
        });
      } else {
        // Aggregate status is always computed against every required target,
        // even when this invocation owns only the critical or maintenance set.
        this.refreshDeliveryStatus(row.id, now);
      }
    }

    let mainSent = 0;
    let canal2Sent = 0;
    let discordSent = 0;
    let failed = 0;

    const beginAttempt = (rowId, target, attempts, attemptedAt) => {
      const columns = {
        main: ["main_delivery_attempts", "main_delivery_error", "main_delivery_in_flight_at",
          "main_delivery_next_attempt_at", "main_delivery_unknown_at"],
        canal2: ["canal2_delivery_attempts", "canal2_delivery_error",
          "canal2_delivery_in_flight_at", "canal2_delivery_next_attempt_at",
          "canal2_delivery_unknown_at"],
        discord: ["discord_delivery_attempts", "discord_delivery_error",
          "discord_delivery_in_flight_at", "discord_delivery_next_attempt_at",
          "discord_delivery_unknown_at"],
      }[target];
      this.sqlExec(
        `UPDATE offers SET ${columns[0]} = ?, ${columns[1]} = '',
           ${columns[2]} = ?, ${columns[3]} = '', ${columns[4]} = '' WHERE id = ?`,
        attempts,
        attemptedAt,
        rowId,
      );
      this.recordDeliveryLedgerEvent({
        offerId: rowId,
        target,
        operation: "delivery",
        state: "attempt_started",
        attempt: attempts,
        generation,
        occurredAt: attemptedAt,
      });
    };

    const recordFailure = (rowId, target, attempts, error) => {
      const columns = {
        main: ["main_delivery_error", "main_delivery_in_flight_at",
          "main_delivery_next_attempt_at", "main_delivery_unknown_at"],
        canal2: ["canal2_delivery_error", "canal2_delivery_in_flight_at",
          "canal2_delivery_next_attempt_at", "canal2_delivery_unknown_at"],
        discord: ["discord_delivery_error", "discord_delivery_in_flight_at",
          "discord_delivery_next_attempt_at", "discord_delivery_unknown_at"],
      }[target];
      const message = sanitizeError(error);
      if (isAmbiguousDeliveryError(error)) {
        const unknownAt = new Date().toISOString();
        this.sqlExec(
          `UPDATE offers SET ${columns[0]} = ?, ${columns[1]} = '',
             ${columns[2]} = '', ${columns[3]} = ?,
             status = 'delivery_unknown', delivery_unknown_at = ?,
             delivery_unknown_target = ? WHERE id = ?`,
          `ambiguous:${message}`.slice(0, 240),
          unknownAt,
          unknownAt,
          target,
          rowId,
        );
        this.recordDeliveryLedgerEvent({
          offerId: rowId,
          target,
          operation: "delivery",
          state: "unknown",
          attempt: attempts,
          generation,
          occurredAt: unknownAt,
          error: `ambiguous:${message}`,
        });
        return "unknown";
      }
      const failedAt = new Date().toISOString();
      this.sqlExec(
        `UPDATE offers SET ${columns[0]} = ?, ${columns[1]} = '', ${columns[2]} = ?
         WHERE id = ?`,
        message,
        deliveryRetryAt(error, attempts),
        rowId,
      );
      this.recordDeliveryLedgerEvent({
        offerId: rowId,
        target,
        operation: "delivery",
        state: "failed",
        attempt: attempts,
        generation,
        occurredAt: failedAt,
        error: message,
      });
      return "retry";
    };

    actionable.sort((left, right) => (
      Number(priority.has(right.row.id)) - Number(priority.has(left.row.id)) ||
      Number(right.recentPriority) - Number(left.recentPriority) ||
      String(left.row.first_seen_at).localeCompare(String(right.row.first_seen_at))
    ));
    const concurrency = budget.concurrency;
    const selected = actionable.slice(0, batchSize).map(({ row, classification, targetNames }) => {
      const offer = rowToOffer(row);
      const imageProxyUrl = String(row.discord_image_proxy_url || "").trim();
      return {
        row,
        classification,
        targetNames,
        offer,
        telegramState: this.telegramOfferWithImageState(
          offer,
          imageProxyUrl ? {
            imageUrl: imageProxyUrl,
            telegramImageRemoteStrategy: "discord_proxy",
          } : {},
        ),
      };
    });
    const stillCurrent = () => this.currentDeliveryMode() === "live" &&
      this.currentDeliveryGeneration() === generation;

    // Primeiro despacha todos os destinos principais com concorrência pequena.
    // Assim uma rajada não deixa a oferta N esperando Discord/Canal 2 da N-1.
    const allowedTargets = requestedTargets;
    if (!allowedTargets.length || allowedTargets.includes("main")) {
      await runBounded(selected, concurrency, async (entry) => {
      if (!stillCurrent()) return;
      const target = entry.classification.actionable.find((item) => item.target === "main");
      if (!target) return;
      const attempts = target.attempts + 1;
      beginAttempt(entry.row.id, "main", attempts, new Date().toISOString());
      const mainOffer = waitForMainImage
        ? mainImageDeliveryOffer(
            entry.telegramState.offer,
            new Date(),
            envNumber(this.env, "MAIN_IMAGE_WAIT_SECONDS", 60, 1, 300),
            envNumber(this.env, "MAIN_AMBIGUOUS_RETRY_SECONDS", 30, 15, 300) +
              TELEGRAM_MUTATION_TIMEOUT_SECONDS,
          )
        : { ...entry.telegramState.offer, deferTextFallback: false };
      try {
        const result = await sendMainOffer(this.env, mainOffer);
        if (result.deferred) {
          this.recordImageDelivery(entry.row.id, entry.telegramState.imageKey, result);
          const deferred = deferredMainDeliveryState(
            target.attempts,
            mainOffer.imageDeadlineAt,
            new Date(),
          );
          this.sqlExec(
            `UPDATE offers SET main_delivery_attempts = ?,
               main_delivery_error = '', main_delivery_in_flight_at = '',
               main_delivery_next_attempt_at = ? WHERE id = ?`,
            deferred.attempts,
            deferred.nextAttemptAt,
            entry.row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: entry.row.id,
            target: "main",
            operation: "image",
            state: "deferred",
            attempt: attempts,
            generation,
            occurredAt: new Date().toISOString(),
          });
          return;
        }
        if (!result.messageId) {
          throw Object.assign(new Error("telegram_main_message_id_missing"), { ambiguous: true });
        }
        const sentAt = new Date().toISOString();
        this.recordImageDelivery(entry.row.id, entry.telegramState.imageKey, result);
        this.sqlExec(
          `UPDATE offers SET main_message_id = ?, main_message_kind = ?, main_sent_at = ?,
             main_delivery_error = '', main_delivery_in_flight_at = '',
             main_delivery_next_attempt_at = '', main_delivery_unknown_at = '',
             main_image_upgrade_next_attempt_at = ?
           WHERE id = ?`,
          result.messageId,
          result.messageKind,
          sentAt,
          result.imageStrategy === "text_timeout"
            ? new Date(
                Date.now() + envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600) * 1_000,
              ).toISOString()
            : "",
          entry.row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: entry.row.id,
          target: "main",
          operation: "delivery",
          state: "sent",
          attempt: attempts,
          generation,
          occurredAt: sentAt,
          externalId: String(result.messageId),
        });
        mainSent += 1;
      } catch (error) {
        recordFailure(entry.row.id, "main", attempts, error);
        failed += 1;
      }
      });
    }

    // Cópias do Canal 2 dependem somente da confirmação do principal e também
    // são processadas antes do Discord, que nunca entra no caminho crítico.
    if (!allowedTargets.length || allowedTargets.includes("canal2")) {
      await runBounded(selected, concurrency, async (entry) => {
      if (!stillCurrent()) return;
      if (!entry.targetNames.includes("canal2")) return;
      const row = this.sqlExec(
        "SELECT * FROM offers WHERE id = ? LIMIT 1",
        entry.row.id,
      ).toArray()[0];
      if (!row) return;
      const target = classifyDeliveryRow(row, configuration, {
        ticket: isTicketCampaign(rowToOffer(row)),
        maxAttempts,
        inFlightStaleSeconds,
        targetNames: ["canal2"],
        now: new Date(),
      }).actionable.find((item) => item.target === "canal2");
      if (!target || !Number(row.main_message_id || 0) || !row.main_sent_at) return;
      const attempts = target.attempts + 1;
      beginAttempt(row.id, "canal2", attempts, new Date().toISOString());
      try {
        const result = await forwardToCanal2(this.env, Number(row.main_message_id));
        if (!result.messageId) {
          throw Object.assign(
            new Error("telegram_canal2_message_id_missing"),
            { ambiguous: true },
          );
        }
        this.sqlExec(
          `UPDATE offers SET canal2_message_id = ?, canal2_sent_at = ?,
             canal2_delivery_error = '', canal2_delivery_in_flight_at = '',
             canal2_delivery_next_attempt_at = '', canal2_delivery_unknown_at = ''
           WHERE id = ?`,
          result.messageId,
          new Date().toISOString(),
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "canal2",
          operation: "delivery",
          state: "sent",
          attempt: attempts,
          generation,
          occurredAt: new Date().toISOString(),
          externalId: String(result.messageId),
        });
        canal2Sent += 1;
      } catch (error) {
        recordFailure(row.id, "canal2", attempts, error);
        failed += 1;
      }
      });
    }

    if (!allowedTargets.length || allowedTargets.includes("discord")) {
      await runBounded(selected, concurrency, async (entry) => {
      if (!stillCurrent()) return;
      if (!entry.targetNames.includes("discord")) return;
      const row = this.sqlExec(
        "SELECT * FROM offers WHERE id = ? LIMIT 1",
        entry.row.id,
      ).toArray()[0];
      if (!row) return;
      const target = classifyDeliveryRow(row, configuration, {
        ticket: isTicketCampaign(rowToOffer(row)),
        maxAttempts,
        inFlightStaleSeconds,
        targetNames: ["discord"],
        now: new Date(),
      }).actionable.find((item) => item.target === "discord");
      if (!target) return;
      const attempts = target.attempts + 1;
      beginAttempt(row.id, "discord", attempts, new Date().toISOString());
      try {
        const result = await sendDiscordOffer(this.env, rowToOffer(row));
        if (!result.messageId) {
          throw Object.assign(new Error("discord_message_id_missing"), { ambiguous: true });
        }
        this.sqlExec(
          `UPDATE offers SET discord_message_id = ?, discord_sent_at = ?,
             discord_image_proxy_url = COALESCE(NULLIF(?, ''), discord_image_proxy_url),
             discord_delivery_error = '', discord_delivery_in_flight_at = '',
             discord_delivery_next_attempt_at = '', discord_delivery_unknown_at = ''
           WHERE id = ?`,
          result.messageId,
          new Date().toISOString(),
          result.imageProxyUrl || "",
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "discord",
          operation: "delivery",
          state: "sent",
          attempt: attempts,
          generation,
          occurredAt: new Date().toISOString(),
          externalId: String(result.messageId),
        });
        try {
          this.enqueueBeeperOffer(row);
        } catch (error) {
          try {
            this.setMetadata("beeper_delivery_backfill_completed_at", "");
          } catch {}
          logEvent("warn", "uol_beeper_enqueue_failed", {
            offerId: row.id,
            error: sanitizeError(error),
          });
        }
        discordSent += 1;
      } catch (error) {
        recordFailure(row.id, "discord", attempts, error);
        failed += 1;
      }
      });
    }

    for (const entry of selected) {
      if (!stillCurrent()) break;
      this.refreshDeliveryStatus(entry.row.id, new Date());
    }

    const selectedIds = new Set(selected.map((entry) => entry.row.id));
    const recentSecondaryRows = budget.reason === "quota_reserve" &&
      requestedTargets.includes("main")
      ? candidates.filter((row) => {
          if (selectedIds.has(row.id) || !isRecentDeliveryDecision(row, now)) return false;
          const offer = rowToOffer(row);
          if (!isTicketCampaign(offer)) return false;
          const discordState = classifyDeliveryRow(row, configuration, {
            ticket: true,
            maxAttempts,
            inFlightStaleSeconds,
            targetNames: ["discord"],
            now,
          });
          return discordState.state === "actionable" &&
            discordState.actionable.some((target) => target.target === "discord");
        }).slice(0, batchSize)
      : [];

    return {
      mainSent,
      canal2Sent,
      discordSent,
      failed,
      selectedRows: selected.map((entry) => entry.row),
      recentSecondaryRows,
    };
  }

  ensureBeeperDeliveryActivation(now = new Date()) {
    const configuration = beeperGatewayConfiguration(this.env);
    const stateKey = "beeper_delivery_enabled_state";
    const previousState = this.metadataValue(stateKey);
    if (!configuration.enabled) {
      if (previousState !== "disabled") this.setMetadata(stateKey, "disabled");
      return { enabled: false, queued: 0 };
    }
    if (previousState === "disabled") {
      this.setMetadata("beeper_delivery_activated_at", now.toISOString());
      this.setMetadata("beeper_delivery_backfill_completed_at", now.toISOString());
      this.setMetadata(stateKey, "enabled");
      return { enabled: true, queued: 0 };
    }
    if (previousState !== "enabled") this.setMetadata(stateKey, "enabled");
    let activatedAt = this.metadataValue("beeper_delivery_activated_at");
    if (!activatedAt) {
      activatedAt = now.toISOString();
      this.setMetadata("beeper_delivery_activated_at", activatedAt);
    }
    if (this.metadataValue("beeper_delivery_backfill_completed_at")) {
      return { enabled: true, queued: 0 };
    }
    const inserted = this.sqlExec(
      `INSERT OR IGNORE INTO beeper_delivery_queue(offer_id)
       SELECT id FROM offers
       WHERE first_seen_at >= ?
         AND discord_sent_at <> ''
         AND (
           lower(link) LIKE '%/campanhasdeingresso/%' OR
           lower(category) LIKE '%campanhasdeingresso%'
         )`,
      activatedAt,
    );
    this.setMetadata("beeper_delivery_backfill_completed_at", now.toISOString());
    return { enabled: true, queued: Number(inserted.rowsWritten || 0) };
  }

  enqueueBeeperOffer(row) {
    const configuration = beeperGatewayConfiguration(this.env);
    if (!configuration.enabled || !row?.id || !isTicketCampaign(rowToOffer(row))) {
      return false;
    }
    const inserted = this.sqlExec(
      "INSERT OR IGNORE INTO beeper_delivery_queue(offer_id) VALUES (?)",
      row.id,
    );
    const queued = Number(inserted.rowsWritten || 0) > 0;
    if (queued) {
      const previous = this.runtimeSnapshot("beeper");
      this.updateBeeperRuntimeSnapshot({
        enabled: true,
        configured: configuration.configured,
        pending: Number(previous.pending || 0) + 1,
      });
    }
    return queued;
  }

  updateBeeperRuntimeSnapshot(patch = {}) {
    const previous = this.runtimeSnapshot("beeper");
    const next = { ...previous, ...patch };
    const stable = (snapshot) => JSON.stringify({
      enabled: snapshot.enabled === true,
      configured: snapshot.configured === true,
      gatewayOk: snapshot.gatewayOk === true,
      gatewayStatus: Number(snapshot.gatewayStatus || 0),
      gatewayCode: String(snapshot.gatewayCode || ""),
      filterActive: snapshot.filterActive === true,
      pending: Number(snapshot.pending || 0),
      exhausted: Number(snapshot.exhausted || 0),
      unknown: Number(snapshot.unknown || 0),
      lastErrorCode: String(snapshot.lastErrorCode || ""),
    });
    const changed = stable(previous) !== stable(next);
    if (changed) {
      this.setRuntimeSnapshot("beeper", next);
      this.readinessCache = null;
    }
    else this.runtimeSnapshotCache.set("beeper", next);
    return { changed, snapshot: next };
  }

  applyBeeperDeliveryRecoveryFilter(configuration) {
    const ids = configuration.offerIds || [];
    const fingerprint = ids.join(",");
    const previous = this.metadataValue(BEEPER_RECOVERY_METADATA_KEY);
    if (!fingerprint) {
      if (previous) this.setMetadata(BEEPER_RECOVERY_METADATA_KEY, "");
      return { applied: false, queued: 0, reset: 0 };
    }
    if (previous === fingerprint) return { applied: false, queued: 0, reset: 0 };
    const placeholders = ids.map(() => "?").join(", ");
    const inserted = this.sqlExec(
      `INSERT OR IGNORE INTO beeper_delivery_queue(offer_id)
       SELECT id FROM offers
        WHERE id IN (${placeholders})
          AND (
            lower(link) LIKE '%/campanhasdeingresso/%' OR
            lower(category) LIKE '%campanhasdeingresso%'
          )`,
      ...ids,
    );
    let reset = 0;
    for (const offerId of ids) {
      const cursor = this.sqlExec(
        `UPDATE beeper_delivery_queue
            SET attempts = 0, next_attempt_at = '', in_flight_at = '', last_error = ''
          WHERE offer_id = ? AND sent_at = ''`,
        offerId,
      );
      reset += Number(cursor.rowsWritten || 0);
    }
    this.setMetadata(BEEPER_RECOVERY_METADATA_KEY, fingerprint);
    return {
      applied: true,
      queued: Number(inserted.rowsWritten || 0),
      reset,
    };
  }

  applyBeeperResponseReplayPolicy() {
    if (this.metadataValue(BEEPER_RESPONSE_REPLAY_METADATA_KEY) ===
        BEEPER_RESPONSE_REPLAY_POLICY) {
      return { applied: false, reset: 0 };
    }
    const cursor = this.sqlExec(
      `UPDATE beeper_delivery_queue
          SET attempts = 0, next_attempt_at = '', in_flight_at = ''
        WHERE sent_at = ''
          AND last_error = 'ambiguous:beeper_send_response_ambiguous'`,
    );
    const reset = Number(cursor.rowsWritten || 0);
    this.setMetadata(
      BEEPER_RESPONSE_REPLAY_METADATA_KEY,
      BEEPER_RESPONSE_REPLAY_POLICY,
    );
    return { applied: true, reset };
  }

  beeperQueueDiagnostics(maxAttempts) {
    const rows = this.sqlExec(
      `SELECT q.offer_id, q.attempts, q.next_attempt_at, q.in_flight_at,
              q.last_error,
              CASE WHEN (
                lower(COALESCE(o.link, '')) LIKE '%/campanhasdeingresso/%' OR
                lower(COALESCE(o.category, '')) LIKE '%campanhasdeingresso%'
              )
                THEN 1 ELSE 0 END AS ticket
         FROM beeper_delivery_queue AS q INDEXED BY beeper_delivery_pending_v25
         LEFT JOIN offers AS o ON o.id = q.offer_id
        WHERE q.sent_at = ''
        ORDER BY q.offer_id ASC
        LIMIT 512`,
    ).toArray();
    return {
      pending: rows.length,
      exhausted: rows.filter((row) => Number(row.attempts || 0) >= maxAttempts).length,
      unknown: rows.filter((row) => String(row.last_error || "").startsWith("ambiguous:"))
        .length,
      inFlight: rows.filter((row) => Boolean(row.in_flight_at)).length,
      nonTicket: rows.filter((row) => Number(row.ticket || 0) !== 1).length,
      truncated: rows.length >= 512,
      pendingIds: rows.map((row) => String(row.offer_id || "")),
      exhaustedIds: rows.filter((row) => Number(row.attempts || 0) >= maxAttempts)
        .map((row) => String(row.offer_id || "")),
    };
  }

  beeperQueueHasDueWork(now = new Date(), maxAttempts = 10) {
    const due = this.sqlExec(
      `SELECT offer_id
         FROM beeper_delivery_queue INDEXED BY beeper_delivery_due_v23
        WHERE sent_at = '' AND in_flight_at = ''
          AND next_attempt_at <= ? AND attempts < ?
        ORDER BY next_attempt_at ASC
        LIMIT 1`,
      now.toISOString(),
      maxAttempts,
    ).toArray()[0];
    if (due?.offer_id) return true;
    const staleSeconds = envNumber(
      this.env,
      "DELIVERY_IN_FLIGHT_STALE_SECONDS",
      30,
      15,
      3_600,
    );
    const staleCutoff = new Date(now.getTime() - staleSeconds * 1_000).toISOString();
    return Boolean(this.sqlExec(
      `SELECT offer_id
         FROM beeper_delivery_queue INDEXED BY beeper_delivery_inflight_v24
        WHERE sent_at = '' AND in_flight_at <> '' AND in_flight_at <= ?
        ORDER BY in_flight_at ASC, offer_id ASC
        LIMIT 1`,
      staleCutoff,
    ).toArray()[0]?.offer_id);
  }

  beeperCriticalDeliveryDue(
    now = new Date(),
    configuration = beeperGatewayConfiguration(this.env),
    maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50),
  ) {
    if (!configuration.enabled) return false;
    const responseReplayPending = configuration.configured &&
      this.metadataValue(BEEPER_RESPONSE_REPLAY_METADATA_KEY) !==
        BEEPER_RESPONSE_REPLAY_POLICY;
    const recoveryFingerprint = configuration.offerIds.join(",");
    const recoveryPending = configuration.configured && configuration.filterActive &&
      this.metadataValue(BEEPER_RECOVERY_METADATA_KEY) !== recoveryFingerprint;
    return responseReplayPending || recoveryPending ||
      this.beeperQueueHasDueWork(now, maxAttempts);
  }

  async processCriticalBeeperDeliveryQueue(now = new Date(), limit = 4) {
    const configuration = beeperGatewayConfiguration(this.env);
    if (!configuration.enabled) {
      return { sent: 0, failed: 0, queued: 0, blocked: false, skipped: true };
    }
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    if (!this.beeperCriticalDeliveryDue(now, configuration, maxAttempts)) {
      return { sent: 0, failed: 0, queued: 0, blocked: false, skipped: true };
    }
    if (this.beeperDeliveryInFlight) {
      return { sent: 0, failed: 0, queued: 0, blocked: false, inProgress: true };
    }
    this.beeperDeliveryInFlight = true;
    try {
      return await this.processBeeperDeliveryQueue(now, limit);
    } finally {
      this.beeperDeliveryInFlight = false;
    }
  }

  async processBeeperDeliveryQueue(now = new Date(), limit = 4) {
    const activation = this.ensureBeeperDeliveryActivation(now);
    const configuration = beeperGatewayConfiguration(this.env);
    if (!configuration.enabled) {
      this.updateBeeperRuntimeSnapshot({
        enabled: false,
        configured: true,
        gatewayOk: true,
        gatewayStatus: 0,
        gatewayCode: "disabled",
        filterActive: false,
        checkedAt: now.toISOString(),
      });
      return { sent: 0, failed: 0, queued: 0, blocked: false };
    }
    if (!configuration.configured) {
      this.updateBeeperRuntimeSnapshot({
        enabled: true,
        configured: false,
        gatewayOk: false,
        gatewayStatus: 0,
        gatewayCode: "unconfigured",
        filterActive: configuration.filterActive,
        checkedAt: now.toISOString(),
      });
      return { sent: 0, failed: 0, queued: activation.queued, blocked: true };
    }
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const responseReplay = this.applyBeeperResponseReplayPolicy();
    if (responseReplay.reset > 0) {
      logEvent("warn", "uol_beeper_response_replay_recovered", {
        reset: responseReplay.reset,
      });
    }
    const recovery = this.applyBeeperDeliveryRecoveryFilter(configuration);
    let recoveryDiagnostics = null;
    if (recovery.applied) {
      recoveryDiagnostics = this.beeperQueueDiagnostics(maxAttempts);
      logEvent("info", "uol_beeper_recovery_filter_applied", {
        offerIds: configuration.offerIds,
        queued: recovery.queued,
        reset: recovery.reset,
        pendingIds: recoveryDiagnostics.pendingIds,
        exhaustedIds: recoveryDiagnostics.exhaustedIds,
        nonTicket: recoveryDiagnostics.nonTicket,
        truncated: recoveryDiagnostics.truncated,
      });
    }

    const staleSeconds = envNumber(
      this.env,
      "DELIVERY_IN_FLIGHT_STALE_SECONDS",
      30,
      15,
      3_600,
    );
    const staleCutoff = new Date(now.getTime() - staleSeconds * 1_000).toISOString();
    const recovered = this.sqlExec(
      `UPDATE beeper_delivery_queue
          SET in_flight_at = '', next_attempt_at = '',
              last_error = CASE WHEN last_error = ''
                THEN 'beeper_in_flight_recovered' ELSE last_error END
        WHERE sent_at = '' AND in_flight_at <> '' AND in_flight_at <= ?`,
      staleCutoff,
    );
    if (Number(recovered.rowsWritten || 0) > 0) {
      logEvent("warn", "uol_beeper_in_flight_recovered", {
        count: Number(recovered.rowsWritten || 0),
      });
    }

    const filterClause = configuration.filterActive
      ? `AND q.offer_id IN (${configuration.offerIds.map(() => "?").join(", ")})`
      : "";
    const queueIndexHint = configuration.filterActive
      ? ""
      : " INDEXED BY beeper_delivery_due_v23";
    const rows = this.sqlExec(
      `SELECT o.*, q.attempts AS beeper_attempts
       FROM beeper_delivery_queue AS q${queueIndexHint}
       JOIN offers AS o ON o.id = q.offer_id
       WHERE q.sent_at = '' AND q.in_flight_at = ''
         AND q.attempts < ?
         AND q.next_attempt_at <= ?
         AND (
           lower(o.link) LIKE '%/campanhasdeingresso/%' OR
           lower(o.category) LIKE '%campanhasdeingresso%'
         )
         ${filterClause}
       ORDER BY q.next_attempt_at ASC, o.first_seen_at ASC
       LIMIT ?`,
      maxAttempts,
      now.toISOString(),
      ...configuration.offerIds,
      Math.max(1, Math.min(16, Number(limit || 4))),
    ).toArray();
    const previousGateway = this.runtimeSnapshot("beeper");
    if (rows.length) {
      const gateway = await probeBeeperGateway(this.env);
      const gatewayState = this.updateBeeperRuntimeSnapshot({
        enabled: true,
        configured: true,
        gatewayOk: gateway.ok,
        gatewayStatus: gateway.status,
        gatewayCode: gateway.code,
        filterActive: configuration.filterActive,
        pending: Math.max(Number(previousGateway.pending || 0), rows.length),
        checkedAt: gateway.checkedAt,
      });
      if (gatewayState.changed) {
        logEvent(gateway.ok ? "info" : "warn", "uol_beeper_gateway_state", {
          ok: gateway.ok,
          status: gateway.status,
          code: gateway.code,
        });
      }
      if (!gateway.ok) {
        return { sent: 0, failed: 0, queued: activation.queued, blocked: true };
      }
    } else {
      this.updateBeeperRuntimeSnapshot({
        enabled: true,
        configured: true,
        filterActive: configuration.filterActive,
      });
    }
    let sent = 0;
    let failed = 0;
    for (const row of rows) {
      const attempts = Number(row.beeper_attempts || 0) + 1;
      const attemptedAt = new Date().toISOString();
      this.sqlExec(
        `UPDATE beeper_delivery_queue SET attempts = ?, in_flight_at = ?,
           next_attempt_at = '', last_error = '' WHERE offer_id = ?`,
        attempts,
        attemptedAt,
        row.id,
      );
      logEvent("info", "uol_beeper_delivery_attempt", {
        offerId: row.id,
        attempt: attempts,
      });
      this.recordDeliveryLedgerEvent({
        offerId: row.id,
        target: "beeper",
        operation: "delivery",
        state: "attempt_started",
        attempt: attempts,
        generation: Number(row.delivery_generation || 0),
        occurredAt: attemptedAt,
      });
      try {
        const result = await sendBeeperOffer(this.env, rowToOffer(row), {
          idempotencyKey: beeperDeliveryIdempotencyKey(
            row.id,
            beeperDestinationKey(this.env),
          ),
        });
        this.sqlExec(
          `UPDATE beeper_delivery_queue SET sent_at = ?, in_flight_at = '',
             next_attempt_at = '', last_error = '', pending_message_id = ?
           WHERE offer_id = ?`,
          new Date().toISOString(),
          result.pendingMessageId,
          row.id,
        );
        logEvent("info", "uol_beeper_delivery_accepted", {
          offerId: row.id,
          attempt: attempts,
          replayed: result.replayed,
        });
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "beeper",
          operation: "delivery",
          state: "accepted",
          attempt: attempts,
          generation: Number(row.delivery_generation || 0),
          occurredAt: new Date().toISOString(),
          externalId: result.pendingMessageId,
        });
        sent += 1;
      } catch (error) {
        const deliveryUnknown = Boolean(error?.ambiguous);
        const errorCode = beeperDeliveryErrorCode(error);
        // A malformed successful response is safe to replay with the same key:
        // the gateway has already persisted its receipt. A gateway-declared
        // delivery_unknown is terminal because it deliberately locks that key.
        const replayableResponse = deliveryUnknown &&
          errorCode === "beeper_send_response_ambiguous";
        const terminalFailure = (deliveryUnknown && !replayableResponse) ||
          error?.retryable === false;
        this.sqlExec(
          `UPDATE beeper_delivery_queue SET attempts = ?, in_flight_at = '',
             next_attempt_at = ?, last_error = ? WHERE offer_id = ?`,
          terminalFailure ? maxAttempts : attempts,
          terminalFailure ? "" : deliveryRetryAt(error, attempts, now),
          `${deliveryUnknown ? "ambiguous:" : terminalFailure ? "terminal:" : ""}${errorCode}`
            .slice(0, 240),
          row.id,
        );
        logEvent("warn", "uol_beeper_delivery_failed", {
          offerId: row.id,
          attempt: attempts,
          code: errorCode,
          ambiguous: deliveryUnknown,
          exhausted: terminalFailure || attempts >= maxAttempts,
        });
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "beeper",
          operation: "delivery",
          state: deliveryUnknown ? "unknown" : "failed",
          attempt: attempts,
          generation: Number(row.delivery_generation || 0),
          occurredAt: new Date().toISOString(),
          error: errorCode,
        });
        failed += 1;
      }
    }
    if (recoveryDiagnostics || sent || failed) {
      const diagnostics = sent || failed
        ? this.beeperQueueDiagnostics(maxAttempts)
        : recoveryDiagnostics;
      this.updateBeeperRuntimeSnapshot({
        pending: diagnostics.pending,
        exhausted: diagnostics.exhausted,
        unknown: diagnostics.unknown,
        lastErrorCode: failed ? "beeper_delivery_failed" : "",
      });
    }
    return { sent, failed, queued: activation.queued, blocked: false };
  }

  async processDiscussionComments(limit = 4) {
    if (this.currentDeliveryMode() !== "live") return { sent: 0, failed: 0 };
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const generation = this.currentDeliveryGeneration();
    const now = new Date();
    this.sqlExec(
      `UPDATE offers SET comment_delivery_unknown_at = ?,
         comment_delivery_error = CASE
           WHEN comment_delivery_error = '' THEN 'ambiguous:comment_delivery_interrupted'
           ELSE comment_delivery_error
         END
       WHERE comment_delivery_in_flight_at <> ''
         AND comment_sent_at = ''
         AND comment_delivery_unknown_at = ''
         AND delivery_generation = ?
         AND status NOT IN (
           'discarded', 'delivery_quarantined', 'shadow_candidate',
           'baseline', 'shadow_sold_out'
         )`,
      now.toISOString(),
      generation,
    );
    const rows = this.sqlExec(
      `SELECT * FROM offers
       WHERE discussion_message_id > 0
         AND comment_sent_at = ''
         AND comment_delivery_attempts < ?
         AND comment_delivery_in_flight_at = ''
         AND comment_delivery_unknown_at = ''
         AND delivery_generation = ?
         AND status NOT IN (
           'discarded', 'delivery_quarantined', 'shadow_candidate',
           'baseline', 'shadow_sold_out'
         )
         AND (
           comment_delivery_next_attempt_at = '' OR comment_delivery_next_attempt_at <= ?
         )
       ORDER BY first_seen_at ASC
       LIMIT ?`,
      maxAttempts,
      generation,
      now.toISOString(),
      Math.min(8, Math.max(1, Number(limit || 4))),
    ).toArray();
    let sent = 0;
    let failed = 0;
    for (const row of rows) {
      const chunks = buildDiscussionCommentChunks(rowToOffer(row));
      let sentCount = Number(row.comment_chunks_sent || 0);
      let attempts = Number(row.comment_delivery_attempts || 0);
      let interrupted = false;
      let messageIds = [];
      try {
        messageIds = JSON.parse(row.comment_message_ids || "[]");
      } catch {
        messageIds = [];
      }
      try {
        for (let index = sentCount; index < chunks.length; index += 1) {
          if (
            this.currentDeliveryMode() !== "live" ||
            this.currentDeliveryGeneration() !== generation
          ) {
            interrupted = true;
            break;
          }
          if (attempts >= maxAttempts) {
            this.sqlExec(
              "UPDATE offers SET comment_delivery_error = ? WHERE id = ?",
              "comment_delivery_attempts_exhausted",
              row.id,
            );
            failed += 1;
            interrupted = true;
            break;
          }
          attempts += 1;
          this.sqlExec(
            `UPDATE offers SET comment_delivery_attempts = ?,
               comment_delivery_error = '', comment_delivery_in_flight_at = ?,
               comment_delivery_next_attempt_at = ''
             WHERE id = ?`,
            attempts,
            new Date().toISOString(),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "comment",
            operation: "comment",
            state: "attempt_started",
            attempt: attempts,
            generation,
            occurredAt: new Date().toISOString(),
          });
          const result = await sendDiscussionComment(
            this.env,
            chunks[index],
            Number(row.discussion_message_id),
          );
          if (!result.messageId) {
            throw Object.assign(new Error("telegram_comment_message_id_missing"), {
              ambiguous: true,
            });
          }
          messageIds.push(result.messageId);
          sentCount = index + 1;
          this.sqlExec(
            `UPDATE offers SET comment_message_ids = ?, comment_chunks_sent = ?,
               comment_delivery_in_flight_at = '' WHERE id = ?`,
            JSON.stringify(messageIds),
            sentCount,
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "comment",
            operation: "comment",
            state: "chunk_sent",
            attempt: attempts,
            generation,
            occurredAt: new Date().toISOString(),
            externalId: String(result.messageId),
          });
        }
        if (interrupted) continue;
        this.sqlExec(
          `UPDATE offers SET comment_sent_at = ?, comment_delivery_error = '',
             comment_delivery_in_flight_at = '', comment_delivery_next_attempt_at = ''
           WHERE id = ?`,
          new Date().toISOString(),
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "comment",
          operation: "comment",
          state: "sent",
          attempt: attempts,
          generation,
          occurredAt: new Date().toISOString(),
          externalId: String(messageIds.at(-1) || ""),
        });
        sent += 1;
      } catch (error) {
        const message = sanitizeError(error);
        if (isAmbiguousDeliveryError(error)) {
          const unknownAt = new Date().toISOString();
          this.sqlExec(
            `UPDATE offers SET comment_delivery_error = ?,
               comment_delivery_in_flight_at = '', comment_delivery_unknown_at = ?
             WHERE id = ?`,
            `ambiguous:${message}`.slice(0, 240),
            unknownAt,
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "comment",
            operation: "comment",
            state: "unknown",
            attempt: attempts,
            generation,
            occurredAt: unknownAt,
            error: `ambiguous:${message}`,
          });
        } else {
          const failedAt = new Date().toISOString();
          this.sqlExec(
            `UPDATE offers SET comment_delivery_error = ?,
               comment_delivery_in_flight_at = '', comment_delivery_next_attempt_at = ?
             WHERE id = ?`,
            message,
            deliveryRetryAt(error, attempts),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "comment",
            operation: "comment",
            state: "failed",
            attempt: attempts,
            generation,
            occurredAt: failedAt,
            error: message,
          });
        }
        failed += 1;
      }
    }
    return { sent, failed };
  }

  reconcileDiscussionForwards() {
    const pending = this.sqlExec(
      `SELECT origin_message_id, discussion_message_id
       FROM pending_discussion_forwards
       ORDER BY received_at ASC
       LIMIT 32`,
    ).toArray();
    let matched = 0;
    for (const forward of pending) {
      const offer = this.sqlExec(
        "SELECT id FROM offers WHERE main_message_id = ? LIMIT 1",
        Number(forward.origin_message_id),
      ).toArray()[0];
      if (!offer?.id) continue;
      this.sqlExec(
        "UPDATE offers SET discussion_message_id = ? WHERE id = ?",
        Number(forward.discussion_message_id),
        offer.id,
      );
      this.sqlExec(
        "DELETE FROM pending_discussion_forwards WHERE origin_message_id = ?",
        Number(forward.origin_message_id),
      );
      matched += 1;
    }
    this.sqlExec(
      "DELETE FROM pending_discussion_forwards WHERE received_at < datetime('now', '-7 days')",
    );
    return matched;
  }

  async processSoldOutSync(now, { onlyIds = [] } = {}) {
    if (this.currentDeliveryMode() !== "live") {
      return { mainEdited: 0, canal2Edited: 0, failed: 0 };
    }
    const telegram = telegramConfiguration(this.env);
    const configuration = deliveryConfiguration(
      this.env,
      telegram,
      discordConfiguration(this.env),
    );
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const selectedIds = [...new Set(onlyIds)].filter(Boolean).slice(0, 16);
    const onlyIdsClause = selectedIds.length
      ? `AND id IN (${selectedIds.map(() => "?").join(", ")})`
      : "";
    let selectedRowIds;
    if (selectedIds.length) {
      selectedRowIds = this.sqlExec(
        `SELECT id
         FROM offers
         WHERE status = 'sold_out'
           AND main_message_id > 0
           AND (
             (
               main_sold_out_synced_at = '' AND main_sold_out_attempts < ? AND
               (main_sold_out_next_attempt_at = '' OR main_sold_out_next_attempt_at <= ?)
             )
             OR (
               would_send_canal2 = 1
               AND canal2_message_id > 0
               AND canal2_sold_out_synced_at = ''
               AND canal2_sold_out_attempts < ?
               AND (
                 canal2_sold_out_next_attempt_at = '' OR
                 canal2_sold_out_next_attempt_at <= ?
               )
             )
           )
           ${onlyIdsClause}
         ORDER BY sold_out_at ASC
         LIMIT 4`,
        maxAttempts,
        now.toISOString(),
        maxAttempts,
        now.toISOString(),
        ...selectedIds,
      ).toArray().map((row) => row.id);
    } else {
      const mainRows = this.sqlExec(
        `SELECT id, sold_out_at
         FROM offers INDEXED BY offers_soldout_main_due_v22
         WHERE status = 'sold_out'
           AND main_message_id > 0
           AND main_sold_out_synced_at = ''
           AND main_sold_out_attempts < ?
           AND (
             main_sold_out_next_attempt_at = '' OR
             main_sold_out_next_attempt_at <= ?
           )
         ORDER BY sold_out_at ASC
         LIMIT 4`,
        maxAttempts,
        now.toISOString(),
      ).toArray();
      const canal2Rows = this.sqlExec(
        `SELECT id, sold_out_at
         FROM offers INDEXED BY offers_soldout_canal2_due_v22
         WHERE status = 'sold_out'
           AND main_message_id > 0
           AND would_send_canal2 = 1
           AND canal2_message_id > 0
           AND canal2_sold_out_synced_at = ''
           AND canal2_sold_out_attempts < ?
           AND (
             canal2_sold_out_next_attempt_at = '' OR
             canal2_sold_out_next_attempt_at <= ?
           )
         ORDER BY sold_out_at ASC
         LIMIT 4`,
        maxAttempts,
        now.toISOString(),
      ).toArray();
      selectedRowIds = [];
      const selectedRowIdSet = new Set();
      for (const row of [...mainRows, ...canal2Rows].sort((left, right) =>
        String(left.sold_out_at || "").localeCompare(String(right.sold_out_at || ""))
      )) {
        if (selectedRowIdSet.has(row.id)) continue;
        selectedRowIdSet.add(row.id);
        selectedRowIds.push(row.id);
        if (selectedRowIds.length === 4) break;
      }
    }
    const selectedRowsById = selectedRowIds.length
      ? new Map(this.sqlExec(
        `SELECT * FROM offers
         WHERE id IN (${selectedRowIds.map(() => "?").join(", ")})`,
        ...selectedRowIds,
      ).toArray().map((row) => [row.id, row]))
      : new Map();
    const rows = selectedRowIds.map((id) => selectedRowsById.get(id)).filter(Boolean);
    let mainEdited = 0;
    let canal2Edited = 0;
    let messageMissing = 0;
    let failed = 0;

    for (const row of rows) {
      const originalOffer = rowToOffer(row);
      const proxyUrl = String(row.discord_image_proxy_url || "").trim();
      const offer = proxyUrl ? { ...originalOffer, imageUrl: proxyUrl } : originalOffer;
      if (
        configuration.main.ready && !row.main_sold_out_synced_at &&
        Number(row.main_sold_out_attempts || 0) < maxAttempts
      ) {
        const attempts = Number(row.main_sold_out_attempts || 0) + 1;
        this.sqlExec(
          `UPDATE offers SET
            main_sold_out_attempts = ?, main_sold_out_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "main",
          operation: "sold_out",
          state: "attempt_started",
          attempt: attempts,
          generation: row.delivery_generation,
          occurredAt: now.toISOString(),
        });
        try {
          await editSoldOutMessage(this.env, {
            chatId: String(this.env.TELEGRAM_CHAT_ID || ""),
            messageId: row.main_message_id,
            messageKind: row.main_message_kind,
            offer,
            soldOutAt: row.sold_out_at || now.toISOString(),
          });
          this.sqlExec(
            `UPDATE offers SET
              main_sold_out_synced_at = ?, main_sold_out_error = '',
              main_sold_out_next_attempt_at = ''
             WHERE id = ?`,
            now.toISOString(),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "main",
            operation: "sold_out",
            state: "synced",
            attempt: attempts,
            generation: row.delivery_generation,
            occurredAt: now.toISOString(),
            externalId: String(row.main_message_id),
          });
          mainEdited += 1;
        } catch (error) {
          if (isTelegramMessageMissingError(error)) {
            this.sqlExec(
              `UPDATE offers SET main_sold_out_synced_at = ?,
                 main_sold_out_error = '', main_sold_out_next_attempt_at = ''
               WHERE id = ?`,
              now.toISOString(),
              row.id,
            );
            messageMissing += 1;
            logEvent("warn", "uol_telegram_sold_out_message_missing", {
              offerId: row.id,
              target: "main",
            });
            this.recordDeliveryLedgerEvent({
              offerId: row.id,
              target: "main",
              operation: "sold_out",
              state: "resolved_missing",
              attempt: attempts,
              generation: row.delivery_generation,
              occurredAt: now.toISOString(),
              externalId: String(row.main_message_id),
            });
            this.resolveIncidentWithoutAlert(
              `delivery-queue:${row.id}:main_sold_out`,
              now,
            );
          } else {
            const retryState = isAmbiguousDeliveryError(error) ? "unknown" : "failed";
            this.sqlExec(
              `UPDATE offers SET main_sold_out_error = ?,
                 main_sold_out_next_attempt_at = ? WHERE id = ?`,
              sanitizeError(error),
              deliveryRetryAt(error, attempts, now),
              row.id,
            );
            this.recordDeliveryLedgerEvent({
              offerId: row.id,
              target: "main",
              operation: "sold_out",
              state: retryState,
              attempt: attempts,
              generation: row.delivery_generation,
              occurredAt: now.toISOString(),
              externalId: String(row.main_message_id),
              error: sanitizeError(error),
            });
            failed += 1;
          }
        }
      }

      if (
        configuration.canal2.enabled && configuration.canal2.ready &&
        Boolean(row.would_send_canal2) &&
        Number(row.canal2_message_id || 0) > 0 &&
        !row.canal2_sold_out_synced_at &&
        Number(row.canal2_sold_out_attempts || 0) < maxAttempts
      ) {
        const attempts = Number(row.canal2_sold_out_attempts || 0) + 1;
        this.sqlExec(
          `UPDATE offers SET
            canal2_sold_out_attempts = ?, canal2_sold_out_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "canal2",
          operation: "sold_out",
          state: "attempt_started",
          attempt: attempts,
          generation: row.delivery_generation,
          occurredAt: now.toISOString(),
        });
        try {
          await editSoldOutMessage(this.env, {
            chatId: String(this.env.CANAL2_ID || ""),
            messageId: row.canal2_message_id,
            messageKind: row.main_message_kind,
            offer,
            soldOutAt: row.sold_out_at || now.toISOString(),
          });
          this.sqlExec(
            `UPDATE offers SET
              canal2_sold_out_synced_at = ?, canal2_sold_out_error = '',
              canal2_sold_out_next_attempt_at = ''
             WHERE id = ?`,
            now.toISOString(),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "canal2",
            operation: "sold_out",
            state: "synced",
            attempt: attempts,
            generation: row.delivery_generation,
            occurredAt: now.toISOString(),
            externalId: String(row.canal2_message_id),
          });
          canal2Edited += 1;
        } catch (error) {
          if (isTelegramMessageMissingError(error)) {
            this.sqlExec(
              `UPDATE offers SET canal2_sold_out_synced_at = ?,
                 canal2_sold_out_error = '', canal2_sold_out_next_attempt_at = ''
               WHERE id = ?`,
              now.toISOString(),
              row.id,
            );
            messageMissing += 1;
            logEvent("warn", "uol_telegram_sold_out_message_missing", {
              offerId: row.id,
              target: "canal2",
            });
            this.recordDeliveryLedgerEvent({
              offerId: row.id,
              target: "canal2",
              operation: "sold_out",
              state: "resolved_missing",
              attempt: attempts,
              generation: row.delivery_generation,
              occurredAt: now.toISOString(),
              externalId: String(row.canal2_message_id),
            });
            this.resolveIncidentWithoutAlert(
              `delivery-queue:${row.id}:canal2_sold_out`,
              now,
            );
          } else {
            // Re-editar a mesma mensagem é idempotente. Não enviamos um aviso
            // substituto sem outbox, pois uma resposta ambígua poderia duplicá-lo.
            this.sqlExec(
              `UPDATE offers SET canal2_sold_out_error = ?,
                 canal2_sold_out_next_attempt_at = ? WHERE id = ?`,
              sanitizeError(error),
              deliveryRetryAt(error, attempts, now),
              row.id,
            );
            this.recordDeliveryLedgerEvent({
              offerId: row.id,
              target: "canal2",
              operation: "sold_out",
              state: isAmbiguousDeliveryError(error) ? "unknown" : "failed",
              attempt: attempts,
              generation: row.delivery_generation,
              occurredAt: now.toISOString(),
              externalId: String(row.canal2_message_id),
              error: sanitizeError(error),
            });
            failed += 1;
          }
        }
      }
    }
    return { mainEdited, canal2Edited, messageMissing, failed };
  }

  repairKnownMaintenanceDeadLetters(now, maxAttempts) {
    const rows = this.sqlExec(
      `SELECT id, status,
              main_sold_out_synced_at, main_sold_out_attempts, main_sold_out_error,
              canal2_sold_out_synced_at, canal2_sold_out_attempts, canal2_sold_out_error
       FROM offers
       WHERE status = 'sold_out'
         AND (
           main_sold_out_attempts >= ? OR canal2_sold_out_attempts >= ?
         )
       ORDER BY sold_out_at ASC
       LIMIT 32`,
      maxAttempts,
      maxAttempts,
    ).toArray();
    let mainMarkedSynced = 0;
    let canal2Requeued = 0;
    const repairedAt = now.toISOString();
    for (const row of rows) {
      const repair = classifyKnownMaintenanceRepair(row, maxAttempts);
      if (repair.main === "mark_synced") {
        this.sqlExec(
          `UPDATE offers SET main_sold_out_synced_at = ?,
             main_sold_out_error = '', main_sold_out_next_attempt_at = ''
           WHERE id = ? AND status = 'sold_out'
             AND main_sold_out_synced_at = ''
             AND main_sold_out_attempts >= ?
             AND lower(main_sold_out_error) LIKE '%message is not modified%'`,
          repairedAt,
          row.id,
          maxAttempts,
        );
        mainMarkedSynced += 1;
      }
      if (repair.canal2 === "retry") {
        this.sqlExec(
          `UPDATE offers SET canal2_sold_out_attempts = 0,
             canal2_sold_out_error = '', canal2_sold_out_next_attempt_at = ''
           WHERE id = ? AND status = 'sold_out'
             AND canal2_sold_out_synced_at = ''
             AND canal2_sold_out_attempts >= ?
             AND lower(canal2_sold_out_error)
               LIKE '%there is no caption in the message to edit%'`,
          row.id,
          maxAttempts,
        );
        canal2Requeued += 1;
      }
    }
    if (mainMarkedSynced || canal2Requeued) {
      logEvent("warn", "uol_telegram_maintenance_repairs_applied", {
        mainMarkedSynced,
        canal2Requeued,
      });
    }
    return { mainMarkedSynced, canal2Requeued };
  }

  async processDiscordAvailabilitySync(now = new Date(), limit = 4, { onlyIds = [] } = {}) {
    if (this.currentDeliveryMode() !== "live") {
      return { soldOutEdited: 0, restockEdited: 0, messageMissing: 0, failed: 0 };
    }
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const selectedIds = [...new Set(onlyIds)].filter(Boolean).slice(0, 16);
    const onlyIdsClause = selectedIds.length
      ? `AND o.id IN (${selectedIds.map(() => "?").join(", ")})`
      : "";
    const batchLimit = Math.max(1, Math.min(16, Number(limit || 4)));
    const projection = `o.id, o.link, o.preview_title, o.title, o.category,
              o.partner_name, o.validity, o.description,
              o.card_image_url, o.partner_image_url, o.image_url,
              o.first_seen_at, o.delivery_generation,
              o.discord_image_proxy_url, o.discord_message_id,
              o.discord_image_cache_message_id, o.status, o.sold_out_at, o.restocked_at,
              d.sold_out_synced_at AS discord_sold_out_synced_at,
              d.sold_out_attempts AS discord_sold_out_attempts,
              d.sold_out_error AS discord_sold_out_error,
              d.sold_out_next_attempt_at AS discord_sold_out_next_attempt_at,
              d.restock_synced_at AS discord_restock_synced_at,
              d.restock_attempts AS discord_restock_attempts,
              d.restock_error AS discord_restock_error,
              d.restock_next_attempt_at AS discord_restock_next_attempt_at`;
    const soldOutRows = this.sqlExec(
      `SELECT ${projection}
       FROM discord_availability_sync AS d INDEXED BY discord_avail_sold_due_v21
       JOIN offers AS o ON o.id = d.offer_id
       WHERE d.sold_out_synced_at = ''
         AND d.sold_out_attempts < ?
         AND d.sold_out_next_attempt_at <= ?
         AND o.status = 'sold_out'
         AND (o.discord_message_id <> '' OR o.discord_image_cache_message_id <> '')
         ${onlyIdsClause}
       ORDER BY o.sold_out_at ASC
       LIMIT ?`,
      maxAttempts,
      now.toISOString(),
      ...selectedIds,
      batchLimit,
    ).toArray();
    const restockRows = this.sqlExec(
      `SELECT ${projection}
       FROM discord_availability_sync AS d INDEXED BY discord_avail_restock_due_v21
       JOIN offers AS o ON o.id = d.offer_id
       WHERE d.restock_synced_at = ''
         AND d.restock_attempts < ?
         AND d.restock_next_attempt_at <= ?
         AND o.restocked_at <> ''
         AND o.status <> 'sold_out'
         AND (o.discord_message_id <> '' OR o.discord_image_cache_message_id <> '')
         ${onlyIdsClause}
       ORDER BY o.restocked_at ASC
       LIMIT ?`,
      maxAttempts,
      now.toISOString(),
      ...selectedIds,
      batchLimit,
    ).toArray();
    const rowsById = new Map();
    for (const row of [...soldOutRows, ...restockRows].sort((left, right) => {
      const leftAt = left.status === "sold_out" ? left.sold_out_at : left.restocked_at;
      const rightAt = right.status === "sold_out" ? right.sold_out_at : right.restocked_at;
      return String(leftAt || "").localeCompare(String(rightAt || ""));
    })) {
      if (!rowsById.has(row.id)) rowsById.set(row.id, row);
    }
    const rows = [...rowsById.values()].slice(0, batchLimit);
    let soldOutEdited = 0;
    let restockEdited = 0;
    let messageMissing = 0;
    let failed = 0;

    for (const row of rows) {
      const baseOffer = rowToOffer(row);
      const ticket = isTicketCampaign(baseOffer);
      const messageId = String(
        ticket ? row.discord_message_id : row.discord_image_cache_message_id,
      ).trim();
      const webhookUrl = String(
        ticket
          ? this.env.DISCORD_WEBHOOK_URL
          : this.env.DISCORD_IMAGE_CACHE_WEBHOOK_URL,
      ).trim();
      if (!messageId || !webhookUrl) continue;
      const originalOffer = await this.enrichDiscordOfferForCard(baseOffer);
      const proxyUrl = String(row.discord_image_proxy_url || "").trim();
      const offer = proxyUrl ? { ...originalOffer, imageUrl: proxyUrl } : originalOffer;
      const soldOut = row.status === "sold_out";
      const prefix = soldOut ? "sold_out" : "restock";
      const attempts = Number(row[`discord_${prefix}_attempts`] || 0) + 1;
      this.sqlExec(
        `INSERT INTO discord_availability_sync(offer_id, ${prefix}_attempts)
         VALUES (?, ?)
         ON CONFLICT(offer_id) DO UPDATE SET
           ${prefix}_attempts = excluded.${prefix}_attempts, ${prefix}_error = ''`,
        row.id,
        attempts,
      );
      this.recordDeliveryLedgerEvent({
        offerId: row.id,
        target: "discord",
        operation: prefix,
        state: "attempt_started",
        attempt: attempts,
        generation: row.delivery_generation,
        occurredAt: now.toISOString(),
        externalId: messageId,
      });
      try {
        await editDiscordOffer(this.env, {
          messageId,
          offer,
          soldOutAt: soldOut ? row.sold_out_at || now.toISOString() : "",
          webhookUrl,
        });
        this.sqlExec(
          `UPDATE discord_availability_sync SET ${prefix}_synced_at = ?,
             ${prefix}_error = '', ${prefix}_next_attempt_at = '' WHERE offer_id = ?`,
          now.toISOString(),
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "discord",
          operation: prefix,
          state: "synced",
          attempt: attempts,
          generation: row.delivery_generation,
          occurredAt: now.toISOString(),
          externalId: messageId,
        });
        if (soldOut) soldOutEdited += 1;
        else restockEdited += 1;
      } catch (error) {
        const missing = Number(error?.httpStatus || error?.status || 0) === 404;
        if (missing) {
          this.sqlExec(
            `UPDATE discord_availability_sync SET ${prefix}_synced_at = ?,
               ${prefix}_error = '', ${prefix}_next_attempt_at = '' WHERE offer_id = ?`,
            now.toISOString(),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "discord",
            operation: prefix,
            state: "resolved_missing",
            attempt: attempts,
            generation: row.delivery_generation,
            occurredAt: now.toISOString(),
            externalId: messageId,
          });
          messageMissing += 1;
          continue;
        }
        this.sqlExec(
          `UPDATE discord_availability_sync SET ${prefix}_error = ?,
             ${prefix}_next_attempt_at = ? WHERE offer_id = ?`,
          sanitizeError(error),
          deliveryRetryAt(error, attempts, now),
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "discord",
          operation: prefix,
          state: isAmbiguousDeliveryError(error) ? "unknown" : "failed",
          attempt: attempts,
          generation: row.delivery_generation,
          occurredAt: now.toISOString(),
          externalId: messageId,
          error: sanitizeError(error),
        });
        failed += 1;
      }
    }
    return { soldOutEdited, restockEdited, messageMissing, failed };
  }

  async processRestockSync(now) {
    if (this.currentDeliveryMode() !== "live") {
      return { mainEdited: 0, canal2Edited: 0, failed: 0 };
    }
    const telegram = telegramConfiguration(this.env);
    const configuration = deliveryConfiguration(
      this.env,
      telegram,
      discordConfiguration(this.env),
    );
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const mainRows = this.sqlExec(
      `SELECT id, restocked_at FROM offers
       INDEXED BY offers_restock_main_due_v22
       WHERE status = 'restocked_pending_sync'
         AND main_message_id > 0
         AND main_restock_synced_at = ''
         AND main_restock_attempts < ?
         AND (
           main_restock_next_attempt_at = '' OR
           main_restock_next_attempt_at <= ?
         )
       ORDER BY restocked_at ASC
       LIMIT 4`,
      maxAttempts,
      now.toISOString(),
    ).toArray();
    const canal2Rows = this.sqlExec(
      `SELECT id, restocked_at FROM offers
       INDEXED BY offers_restock_canal2_due_v22
       WHERE status = 'restocked_pending_sync'
         AND would_send_canal2 = 1
         AND canal2_message_id > 0
         AND canal2_restock_synced_at = ''
         AND canal2_restock_attempts < ?
         AND (
           canal2_restock_next_attempt_at = '' OR
           canal2_restock_next_attempt_at <= ?
         )
       ORDER BY restocked_at ASC
       LIMIT 4`,
      maxAttempts,
      now.toISOString(),
    ).toArray();
    const finalizeCanal2Clause = configuration.canal2.enabled
      ? `AND (
           would_send_canal2 = 0 OR canal2_message_id <= 0 OR
           canal2_restock_synced_at <> ''
         )`
      : "";
    const finalizeRows = this.sqlExec(
      `SELECT id, restocked_at FROM offers
       INDEXED BY offers_restock_finalize_v22
       WHERE status = 'restocked_pending_sync'
         AND main_restock_synced_at <> ''
         ${finalizeCanal2Clause}
       ORDER BY restocked_at ASC
       LIMIT 4`,
    ).toArray();
    const selectedRowIds = [];
    const selectedRowIdSet = new Set();
    for (const row of [...mainRows, ...canal2Rows, ...finalizeRows].sort((left, right) =>
      String(left.restocked_at || "").localeCompare(String(right.restocked_at || ""))
    )) {
      if (selectedRowIdSet.has(row.id)) continue;
      selectedRowIdSet.add(row.id);
      selectedRowIds.push(row.id);
      if (selectedRowIds.length === 4) break;
    }
    const selectedRowsById = selectedRowIds.length
      ? new Map(this.sqlExec(
        `SELECT * FROM offers
         WHERE id IN (${selectedRowIds.map(() => "?").join(", ")})`,
        ...selectedRowIds,
      ).toArray().map((row) => [row.id, row]))
      : new Map();
    const rows = selectedRowIds.map((id) => selectedRowsById.get(id)).filter(Boolean);
    let mainEdited = 0;
    let canal2Edited = 0;
    let mainReposted = 0;
    let canal2Reposted = 0;
    let failed = 0;

    for (const row of rows) {
      const offer = rowToOffer(row);
      let mainSynced = Boolean(row.main_restock_synced_at);
      let canal2Synced = Boolean(row.canal2_restock_synced_at);
      let mainMessageId = Number(row.main_message_id || 0);
      let mainReplacementFailed = false;
      if (
        configuration.main.ready && Number(row.main_message_id || 0) > 0 &&
        !mainSynced && Number(row.main_restock_attempts || 0) < maxAttempts &&
        this.currentDeliveryMode() === "live"
      ) {
        const attempts = Number(row.main_restock_attempts || 0) + 1;
        this.sqlExec(
          "UPDATE offers SET main_restock_attempts = ?, main_restock_error = '' WHERE id = ?",
          attempts,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "main",
          operation: "restock",
          state: "attempt_started",
          attempt: attempts,
          generation: row.delivery_generation,
          occurredAt: now.toISOString(),
          externalId: String(row.main_message_id),
        });
        try {
          await editMainOfferMessage(this.env, {
            messageId: row.main_message_id,
            messageKind: row.main_message_kind,
            offer,
          });
          mainSynced = true;
          this.sqlExec(
            `UPDATE offers SET main_restock_synced_at = ?, main_restock_error = '',
               main_restock_next_attempt_at = ''
             WHERE id = ?`,
            now.toISOString(),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "main",
            operation: "restock",
            state: "synced",
            attempt: attempts,
            generation: row.delivery_generation,
            occurredAt: now.toISOString(),
            externalId: String(row.main_message_id),
          });
          mainEdited += 1;
        } catch (editError) {
          if (isTelegramMessageMissingError(editError)) {
            try {
              const telegramState = this.telegramOfferWithImageState(offer, {
                deferTextFallback: false,
              });
              const replacement = await sendMainOffer(this.env, telegramState.offer);
              if (!replacement.messageId) {
                throw Object.assign(
                  new Error("telegram_restock_message_id_missing"),
                  { ambiguous: true },
                );
              }
              const replacedAt = new Date().toISOString();
              mainMessageId = Number(replacement.messageId);
              this.recordImageDelivery(row.id, telegramState.imageKey, replacement, now);
              this.sqlExec(
                `UPDATE offers SET main_message_id = ?, main_message_kind = ?,
                   main_sent_at = ?, main_restock_synced_at = ?,
                   main_restock_error = '', main_restock_next_attempt_at = '',
                   discussion_message_id = 0, comment_sent_at = '',
                   comment_chunks_sent = 0, comment_message_ids = '[]',
                   comment_delivery_attempts = 0, comment_delivery_error = '',
                   comment_delivery_in_flight_at = '', comment_delivery_next_attempt_at = '',
                   comment_delivery_unknown_at = ''
                 WHERE id = ?`,
                mainMessageId,
                replacement.messageKind,
                replacedAt,
                replacedAt,
                row.id,
              );
              this.recordDeliveryLedgerEvent({
                offerId: row.id,
                target: "main",
                operation: "restock",
                state: "reposted",
                attempt: attempts,
                generation: row.delivery_generation,
                occurredAt: replacedAt,
                externalId: String(mainMessageId),
              });
              mainSynced = true;
              mainReposted += 1;
              logEvent("warn", "uol_telegram_restock_reposted", {
                offerId: row.id,
                target: "main",
                messageId: mainMessageId,
              });
            } catch (replacementError) {
              const ambiguous = isAmbiguousDeliveryError(replacementError);
              this.sqlExec(
                `UPDATE offers SET main_restock_attempts = ?, main_restock_error = ?,
                   main_restock_next_attempt_at = ? WHERE id = ?`,
                ambiguous ? maxAttempts : attempts,
                `${ambiguous ? "ambiguous:" : ""}restock_repost:${sanitizeError(replacementError)}`
                  .slice(0, 240),
                ambiguous ? "" : deliveryRetryAt(replacementError, attempts, now),
                row.id,
              );
              this.recordDeliveryLedgerEvent({
                offerId: row.id,
                target: "main",
                operation: "restock",
                state: ambiguous ? "unknown" : "failed",
                attempt: attempts,
                generation: row.delivery_generation,
                occurredAt: now.toISOString(),
                externalId: String(mainMessageId),
                error: sanitizeError(replacementError),
              });
              mainReplacementFailed = true;
              failed += 1;
            }
          } else {
            this.sqlExec(
              `UPDATE offers SET main_restock_error = ?,
                 main_restock_next_attempt_at = ? WHERE id = ?`,
              sanitizeError(editError),
              deliveryRetryAt(editError, attempts, now),
              row.id,
            );
            this.recordDeliveryLedgerEvent({
              offerId: row.id,
              target: "main",
              operation: "restock",
              state: isAmbiguousDeliveryError(editError) ? "unknown" : "failed",
              attempt: attempts,
              generation: row.delivery_generation,
              occurredAt: now.toISOString(),
              externalId: String(row.main_message_id),
              error: sanitizeError(editError),
            });
            failed += 1;
          }
        }
      }

      const canal2Required = Boolean(
        row.would_send_canal2 && configuration.canal2.enabled &&
        Number(row.canal2_message_id || 0) > 0
      );
      if (
        canal2Required && configuration.canal2.ready && !canal2Synced &&
        Number(row.canal2_restock_attempts || 0) < maxAttempts &&
        this.currentDeliveryMode() === "live" && !mainReplacementFailed
      ) {
        const attempts = Number(row.canal2_restock_attempts || 0) + 1;
        this.sqlExec(
          `UPDATE offers SET canal2_restock_attempts = ?, canal2_restock_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "canal2",
          operation: "restock",
          state: "attempt_started",
          attempt: attempts,
          generation: row.delivery_generation,
          occurredAt: now.toISOString(),
          externalId: String(row.canal2_message_id),
        });
        try {
          await editMainOfferMessage(this.env, {
            chatId: String(this.env.CANAL2_ID || ""),
            messageId: row.canal2_message_id,
            messageKind: row.main_message_kind,
            offer,
          });
          canal2Synced = true;
          this.sqlExec(
            `UPDATE offers SET canal2_restock_synced_at = ?, canal2_restock_error = '',
               canal2_restock_next_attempt_at = ''
             WHERE id = ?`,
            now.toISOString(),
            row.id,
          );
          this.recordDeliveryLedgerEvent({
            offerId: row.id,
            target: "canal2",
            operation: "restock",
            state: "synced",
            attempt: attempts,
            generation: row.delivery_generation,
            occurredAt: now.toISOString(),
            externalId: String(row.canal2_message_id),
          });
          canal2Edited += 1;
        } catch (editError) {
          if (isTelegramMessageMissingError(editError)) {
            try {
              const replacement = await forwardToCanal2(this.env, mainMessageId);
              if (!replacement.messageId) {
                throw Object.assign(
                  new Error("telegram_canal2_restock_message_id_missing"),
                  { ambiguous: true },
                );
              }
              const replacedAt = new Date().toISOString();
              this.sqlExec(
                `UPDATE offers SET canal2_message_id = ?, canal2_sent_at = ?,
                   canal2_restock_synced_at = ?, canal2_restock_error = '',
                   canal2_restock_next_attempt_at = '' WHERE id = ?`,
                Number(replacement.messageId),
                replacedAt,
                replacedAt,
                row.id,
              );
              this.recordDeliveryLedgerEvent({
                offerId: row.id,
                target: "canal2",
                operation: "restock",
                state: "reposted",
                attempt: attempts,
                generation: row.delivery_generation,
                occurredAt: replacedAt,
                externalId: String(replacement.messageId),
              });
              canal2Synced = true;
              canal2Reposted += 1;
              logEvent("warn", "uol_telegram_restock_reposted", {
                offerId: row.id,
                target: "canal2",
                messageId: Number(replacement.messageId),
              });
            } catch (replacementError) {
              const ambiguous = isAmbiguousDeliveryError(replacementError);
              this.sqlExec(
                `UPDATE offers SET canal2_restock_attempts = ?, canal2_restock_error = ?,
                   canal2_restock_next_attempt_at = ? WHERE id = ?`,
                ambiguous ? maxAttempts : attempts,
                `${ambiguous ? "ambiguous:" : ""}restock_repost:${sanitizeError(replacementError)}`
                  .slice(0, 240),
                ambiguous ? "" : deliveryRetryAt(replacementError, attempts, now),
                row.id,
              );
              this.recordDeliveryLedgerEvent({
                offerId: row.id,
                target: "canal2",
                operation: "restock",
                state: ambiguous ? "unknown" : "failed",
                attempt: attempts,
                generation: row.delivery_generation,
                occurredAt: new Date().toISOString(),
                externalId: String(row.canal2_message_id),
                error: sanitizeError(replacementError),
              });
              failed += 1;
            }
          } else {
            this.sqlExec(
              `UPDATE offers SET canal2_restock_error = ?,
                 canal2_restock_next_attempt_at = ? WHERE id = ?`,
              sanitizeError(editError),
              deliveryRetryAt(editError, attempts, now),
              row.id,
            );
            this.recordDeliveryLedgerEvent({
              offerId: row.id,
              target: "canal2",
              operation: "restock",
              state: isAmbiguousDeliveryError(editError) ? "unknown" : "failed",
              attempt: attempts,
              generation: row.delivery_generation,
              occurredAt: now.toISOString(),
              externalId: String(row.canal2_message_id),
              error: sanitizeError(editError),
            });
            failed += 1;
          }
        }
      }

      if (mainSynced && (!canal2Required || canal2Synced)) {
        const resumeStatus = row.status_before_sold_out === "partial_delivery"
          ? "partial_delivery"
          : "delivered";
        this.sqlExec(
          `UPDATE offers SET status = ?, status_before_sold_out = '',
             missing_since = '', absence_count = 0
           WHERE id = ?`,
          resumeStatus,
          row.id,
        );
      }
    }
    return { mainEdited, canal2Edited, mainReposted, canal2Reposted, failed };
  }

  async processTicketAvailabilityProbes(
    now = new Date(),
    { fetchImpl = fetch, controlCards = null } = {},
  ) {
    const empty = {
      probed: 0,
      controlProbed: 0,
      confirmed: 0,
      fallback: 0,
      soldOutMainEdited: 0,
      soldOutCanal2Edited: 0,
      soldOutDiscordEdited: 0,
      messageMissing: 0,
      failed: 0,
    };
    if (this.currentDeliveryMode() !== "live") return empty;
    const dailyLimit = envNumber(
      this.env,
      "TICKET_SOLD_OUT_PROBE_DAILY_LIMIT",
      256,
      0,
      10_000,
    );
    const budget = ticketProbeBudget({
      used: this.storageUsage.ticketProbeCount,
      dailyLimit,
      perScanLimit: envNumber(this.env, "TICKET_SOLD_OUT_PROBES_PER_SCAN", 1, 0, 4),
    });
    if (!budget.allowed) return empty;
    const maxAttempts = envNumber(this.env, "TICKET_SOLD_OUT_PROBE_MAX_ATTEMPTS", 2, 2, 2);
    const rows = this.sqlExec(
      `SELECT o.id, o.link, o.status, o.sold_out_at,
              s.next_at AS ticket_probe_next_at,
              s.last_at AS ticket_probe_last_at,
              s.last_result AS ticket_probe_last_result,
              s.gone_count AS ticket_probe_gone_count,
              s.attempts AS ticket_probe_attempts
       FROM ticket_probe_state AS s INDEXED BY ticket_probe_due_v21
       JOIN offers AS o ON o.id = s.offer_id
       WHERE o.status IN ('delivered', 'partial_delivery')
         AND o.sold_out_at = ''
         AND o.missing_since <> ''
         AND o.link LIKE '%/campanhasdeingresso/%'
         AND s.next_at > ''
         AND s.next_at <= ?
         AND s.attempts < ?
         AND (s.last_result = 'listing_absence_pending' OR s.gone_count > 0)
       ORDER BY s.next_at ASC, o.first_seen_at ASC
       LIMIT ?`,
      now.toISOString(),
      maxAttempts,
      budget.batchSize,
    ).toArray();
    const result = { ...empty };
    const activeControlCards = Array.isArray(controlCards)
      ? compactTicketListingCards(controlCards)
      : this.freshTicketListingCards(now, 45);

    for (const row of rows) {
      if (Number(this.storageUsage.ticketProbeCount || 0) >= dailyLimit) break;
      const rowIdentityKeys = new Set(offerIdentityKeys(row.link));
      const control = activeControlCards.find((card) =>
        offerIdentityKeys(card.link).every((key) => !rowIdentityKeys.has(key))
      );
      const usedBefore = Number(this.storageUsage.ticketProbeCount || 0);
      const controlUrl = usedBefore + 2 <= dailyLimit ? control?.link || "" : "";
      const probe = await probeTicketOfferWithControl(
        row.link,
        controlUrl,
        fetchImpl,
      );
      this.storageUsage.ticketProbeCount = usedBefore + Number(probe.requests || 1);
      result.probed += 1;
      if (probe.controlProbed) result.controlProbed += 1;

      const attempts = Number(row.ticket_probe_attempts || 0) + 1;
      const state = nextTicketProbeState({
        result: probe.result,
        goneCount: row.ticket_probe_gone_count,
        attempts: row.ticket_probe_attempts,
        now,
        confirmGoneCount: 2,
        maxAttempts,
        confirmDelaySeconds: 5,
      });
      const lastResult = `${probe.result}:${probe.reason}`.slice(0, 120);

      if (state.action !== "confirm") {
        this.sqlExec(
          `UPDATE ticket_probe_state SET next_at = ?, last_at = ?,
             last_result = ?, gone_count = ?, attempts = ?
           WHERE offer_id = ?
             AND EXISTS (
               SELECT 1 FROM offers
               WHERE offers.id = ticket_probe_state.offer_id
                 AND offers.status IN ('delivered', 'partial_delivery')
                 AND offers.sold_out_at = ''
             )`,
          state.nextAt,
          now.toISOString(),
          lastResult,
          state.goneCount,
          attempts,
          row.id,
        );
        if (state.action === "fallback") result.fallback += 1;
        continue;
      }

      const soldOutAt = now.toISOString();
      const updated = this.sqlExec(
        `UPDATE offers SET sold_out_at = ?, status_before_sold_out = status,
           status = 'sold_out', missing_since = ?, absence_count = 2,
           restocked_at = '', main_restock_synced_at = '', canal2_restock_synced_at = '',
           main_restock_attempts = 0, canal2_restock_attempts = 0,
           main_restock_error = '', canal2_restock_error = '',
           main_restock_next_attempt_at = '', canal2_restock_next_attempt_at = '',
           main_sold_out_synced_at = '', canal2_sold_out_synced_at = '',
           main_sold_out_attempts = 0, canal2_sold_out_attempts = 0,
           main_sold_out_error = '', canal2_sold_out_error = '',
           main_sold_out_next_attempt_at = '', canal2_sold_out_next_attempt_at = ''
        WHERE id = ? AND status IN ('delivered', 'partial_delivery')
           AND sold_out_at = ''`,
        soldOutAt,
        soldOutAt,
        row.id,
      );
      if (Number(updated?.rowsWritten || 0) < 1) continue;
      this.sqlExec(
        `UPDATE ticket_probe_state SET next_at = '', last_at = ?,
           last_result = ?, gone_count = ?, attempts = ? WHERE offer_id = ?`,
        soldOutAt,
        lastResult,
        state.goneCount,
        attempts,
        row.id,
      );
      this.sqlExec(
        `INSERT INTO discord_availability_sync(offer_id)
         VALUES (?)
         ON CONFLICT(offer_id) DO UPDATE SET
           sold_out_synced_at = '', sold_out_attempts = 0,
           sold_out_error = '', sold_out_next_attempt_at = ''`,
        row.id,
      );
      result.confirmed += 1;
      const telegram = await this.processSoldOutSync(now, { onlyIds: [row.id] });
      result.soldOutMainEdited += telegram.mainEdited;
      result.soldOutCanal2Edited += telegram.canal2Edited;
      result.messageMissing += telegram.messageMissing || 0;
      result.failed += telegram.failed;
      const discord = await this.processDiscordAvailabilitySync(now, 1, {
        onlyIds: [row.id],
      });
      result.soldOutDiscordEdited += discord.soldOutEdited;
      result.messageMissing += discord.messageMissing || 0;
      result.failed += discord.failed;
    }
    return result;
  }

  evaluateSoldOut(activeIds, now, category = "all") {
    const lookbackDays = envNumber(this.env, "SOLD_OUT_LOOKBACK_DAYS", 3, 1, 30);
    const minMisses = envNumber(this.env, "SOLD_OUT_MIN_MISSES", 2, 1, 20);
    const minAbsenceMinutes = envNumber(
      this.env,
      "SOLD_OUT_MIN_ABSENCE_MINUTES",
      15,
      1,
      1_440,
    );
    const threshold = new Date(now.getTime() - lookbackDays * 86_400_000).toISOString();
    const categoryClause = category === "ticket"
      ? "AND link LIKE '%/campanhasdeingresso/%'"
      : category === "main"
        ? "AND link NOT LIKE '%/campanhasdeingresso/%'"
        : "";
    const candidates = this.sqlExec(
      `SELECT id, link, status, missing_since, absence_count
       FROM offers
       WHERE status IN ('shadow_candidate', 'delivered', 'partial_delivery')
         AND sold_out_at = ''
         AND decision_at >= ?
         ${categoryClause}`,
      threshold,
    ).toArray();
    let soldOutDetected = 0;

    for (const candidate of candidates) {
      const ticketCandidate = isTicketCampaign(candidate);
      if (activeIds.has(candidate.id)) {
        if (ticketCandidate) this.clearTicketAbsenceProbe(candidate.id);
        if (candidate.missing_since || Number(candidate.absence_count || 0) > 0) {
          this.sqlExec(
            "UPDATE offers SET missing_since = '', absence_count = 0 WHERE id = ?",
            candidate.id,
          );
        }
        continue;
      }

      const previousCount = Number(candidate.absence_count || 0);
      if (!candidate.missing_since) {
        this.sqlExec(
          "UPDATE offers SET missing_since = ?, absence_count = 1 WHERE id = ?",
          now.toISOString(),
          candidate.id,
        );
        continue;
      }

      const missingSince = new Date(candidate.missing_since);
      const absentMinutes = Number.isNaN(missingSince.getTime())
        ? 0
        : (now.getTime() - missingSince.getTime()) / 60_000;
      const nextCount = Math.min(minMisses, previousCount + 1);
      if (nextCount !== previousCount) {
        this.sqlExec(
          "UPDATE offers SET absence_count = ? WHERE id = ?",
          nextCount,
          candidate.id,
        );
      }
      if (nextCount >= minMisses && absentMinutes >= minAbsenceMinutes) {
        if (ticketCandidate) {
          // A listagem só abre a confirmação. Para ingressos, apenas duas
          // respostas diretas "gone" podem editar os canais como esgotados.
          this.scheduleTicketAbsenceProbe(
            candidate.id,
            ticketProbeNextAt(this.env, now),
          );
          continue;
        }
        const soldOutStatus = candidate.status === "shadow_candidate"
          ? "shadow_sold_out"
          : "sold_out";
        this.sqlExec(
          `UPDATE offers SET sold_out_at = ?, status_before_sold_out = ?, status = ?,
             restocked_at = '', main_restock_synced_at = '', canal2_restock_synced_at = '',
             main_restock_attempts = 0, canal2_restock_attempts = 0,
             main_restock_error = '', canal2_restock_error = '',
             main_restock_next_attempt_at = '', canal2_restock_next_attempt_at = '',
             main_sold_out_synced_at = '', canal2_sold_out_synced_at = '',
             main_sold_out_attempts = 0, canal2_sold_out_attempts = 0,
             main_sold_out_error = '', canal2_sold_out_error = '',
             main_sold_out_next_attempt_at = '', canal2_sold_out_next_attempt_at = ''
           WHERE id = ?`,
          now.toISOString(),
          candidate.status,
          soldOutStatus,
          candidate.id,
        );
        this.sqlExec(
          `INSERT INTO discord_availability_sync(offer_id)
           VALUES (?)
           ON CONFLICT(offer_id) DO UPDATE SET
             sold_out_synced_at = '', sold_out_attempts = 0,
             sold_out_error = '', sold_out_next_attempt_at = ''`,
          candidate.id,
        );
        soldOutDetected += 1;
      }
    }
    return soldOutDetected;
  }

  pruneOffers(now, activeIds = new Set()) {
    const cleanupDay = now.toISOString().slice(0, 10);
    if (this.metadataValue("offers_cleanup_day") === cleanupDay) return;

    const retentionDays = envNumber(this.env, "OFFER_RETENTION_DAYS", 30, 7, 365);
    const maxOffers = envNumber(this.env, "MAX_STATE_OFFERS", 300, 50, 2_000);
    const cutoff = new Date(now.getTime() - retentionDays * 86_400_000).toISOString();
    const active = [...activeIds];
    const activeClause = active.length
      ? `AND id NOT IN (${active.map(() => "?").join(", ")})`
      : "";

    // Retain every currently visible card and every unfinished delivery. Old
    // terminal rows are only dedupe/history state and can be safely removed.
    this.sqlExec(
      `DELETE FROM offers
       WHERE first_seen_at < ?
         AND status IN ('baseline', 'discarded', 'delivered', 'shadow_sold_out', 'sold_out')
         ${activeClause}`,
      cutoff,
      ...active,
    );
    this.sqlExec(
      `DELETE FROM offers
       WHERE id IN (
         SELECT id FROM offers
          WHERE status IN ('baseline', 'discarded', 'delivered', 'shadow_sold_out', 'sold_out')
            ${activeClause}
          ORDER BY first_seen_at DESC
          LIMIT -1 OFFSET ?
       )`,
      ...active,
      maxOffers,
    );
    this.sqlExec(
      `DELETE FROM source_observations
       WHERE api_last_seen_at < ? AND listing_last_seen_at < ?`,
      cutoff,
      cutoff,
    );
    const imageCutoff = new Date(now.getTime() - 90 * 86_400_000).toISOString();
    this.sqlExec(
      "DELETE FROM telegram_image_cache WHERE last_used_at < ?",
      imageCutoff,
    );
    this.sqlExec(
      `DELETE FROM telegram_image_cache WHERE image_key IN (
         SELECT image_key FROM telegram_image_cache
         ORDER BY last_used_at DESC LIMIT -1 OFFSET 500
       )`,
    );
    this.sqlExec(
      `DELETE FROM offer_identity_aliases
       WHERE offer_id NOT IN (SELECT id FROM offers)`,
    );
    this.sqlExec(
      `DELETE FROM delivery_events
       WHERE offer_id NOT IN (SELECT id FROM offers)`,
    );
    this.sqlExec(
      `DELETE FROM ticket_probe_state
       WHERE offer_id NOT IN (SELECT id FROM offers)`,
    );
    this.sqlExec(
      `DELETE FROM discord_availability_sync
       WHERE offer_id NOT IN (SELECT id FROM offers)`,
    );
    this.sqlExec(
      `DELETE FROM beeper_delivery_queue
       WHERE offer_id NOT IN (SELECT id FROM offers)`,
    );
    this.setMetadata("offers_cleanup_day", cleanupDay);
  }


  recentApiCardsForHealth(now = new Date()) {
    const touchMinutes = envNumber(
      this.env,
      "OFFER_LAST_SEEN_TOUCH_MINUTES",
      15,
      1,
      1_440,
    );
    const cutoff = new Date(
      now.getTime() - observationFreshnessMinutes(touchMinutes) * 60_000,
    ).toISOString();
    const api = this.runtimeSnapshot("api");
    const apiOffers = Number(
      api.lastOffersSeen ?? this.metadataValue("api_last_offers_seen") ?? 0,
    );
    const apiError = cleanText(api.lastError ?? this.metadataValue("api_last_error"));
    const apiSuccessAt = api.lastSuccessAt || this.metadataValue("api_last_success_at");
    const healthCards = Array.isArray(api.healthCards) ? api.healthCards : [];
    const apiSuccessMs = Date.parse(apiSuccessAt);
    const cutoffMs = Date.parse(cutoff);
    if (
      apiOffers > 0 && !apiError && healthCards.length &&
      Number.isFinite(apiSuccessMs) && Number.isFinite(cutoffMs) && apiSuccessMs >= cutoffMs
    ) {
      return healthCards.map((card) => ({
        ...card,
        category: card.category || "campanhasdeingresso",
        cardImageUrl: "",
        partnerImageUrl: "",
        partnerName: "",
      }));
    }
    return this.sqlExec(
      `SELECT offer_key AS id, link, title AS preview_title
       FROM source_observations
       WHERE api_last_seen_at >= ?
       ORDER BY api_last_seen_at DESC
       LIMIT 200`,
      cutoff,
    ).toArray().map((row) => ({
      id: row.id,
      link: row.link,
      previewTitle: row.preview_title,
      category: "campanhasdeingresso",
      cardImageUrl: "",
      partnerImageUrl: "",
      partnerName: "",
    }));
  }

  async scan(source = "alarm") {
    return this.withStorageCycle("primary", () => this.scanWithStorageCycle(source));
  }

  async scanWithStorageCycle(source = "alarm") {
    if (this.scanInFlight) {
      return { ok: false, outcome: "scan_in_progress", source };
    }
    const storageReadStartedAt = Number(this.storageUsage.rowsRead || 0);
    this.scanInFlight = true;
    const startedAt = new Date();
    const previousApi = this.runtimeSnapshot("api");
    const previousTicketListing = this.runtimeSnapshot("ticket_listing");
    let apiLastSuccessAt = previousApi.lastSuccessAt || this.metadataValue("api_last_success_at");
    let apiFailureStreak = Number(
      previousApi.failureStreak ?? this.metadataValue("api_failure_streak") ?? 0,
    );
    let fastLastCompletedAt = previousApi.fastLastCompletedAt ||
      this.metadataValue("api_fast_last_completed_at");
    let fastLastElapsedMs = Number(
      previousApi.fastLastElapsedMs ?? this.metadataValue("api_fast_last_elapsed_ms") ?? 0,
    );
    let fastLastNewOffers = Number(
      previousApi.fastLastNewOffers ?? this.metadataValue("api_fast_last_new_offers") ?? 0,
    );
    let fastLastMainSent = Number(
      previousApi.fastLastMainSent ?? this.metadataValue("api_fast_last_main_sent") ?? 0,
    );
    let runFailureStreak = Number(previousApi.runFailureStreak || 0);
    let apiContract = null;
    let apiCards = [];
    let ticketListingCards = [];
    let ticketListingIdentityKeys = new Set();
    let discoveryCards = [];
    let apiHealthCards = Array.isArray(previousApi.healthCards)
      ? previousApi.healthCards
      : [];
    let discoverySnapshotFingerprint = "";
    let discoverySnapshotChanged = false;
    let ticketListingLastSuccessAt = previousTicketListing.lastSuccessAt || "";
    let ticketListingFailureStreak = Number(previousTicketListing.failureStreak || 0);
    const run = {
      startedAt: startedAt.toISOString(),
      finishedAt: "",
      source,
      outcome: "failed",
      offersSeen: 0,
      newOffers: 0,
      enriched: 0,
      wouldSendMain: 0,
      wouldSendCanal2: 0,
      soldOutDetected: 0,
      mainSent: 0,
      canal2Sent: 0,
      deliveryFailed: 0,
      soldOutMainEdited: 0,
      soldOutCanal2Edited: 0,
      soldOutDiscordEdited: 0,
      restockMainEdited: 0,
      restockCanal2Edited: 0,
      discordSent: 0,
      commentsSent: 0,
      ticketProbes: 0,
      ticketProbeConfirmed: 0,
      ticketProbeFallback: 0,
      ticketProbeMainEdited: 0,
      ticketProbeCanal2Edited: 0,
      ticketProbeDiscordEdited: 0,
      apiOffersSeen: 0,
      apiElapsedMs: 0,
      ticketListingOffersSeen: 0,
      ticketListingElapsedMs: 0,
      apiFastProcessed: 0,
      apiFastMainSent: 0,
      apiFastElapsedMs: 0,
      htmlReconciled: false,
      mainAmbiguousReleased: 0,
      apiError: "",
      ticketListingError: "",
      error: "",
    };

    try {
      const mode = this.currentDeliveryMode();
      if (mode === "live" && !this.metadataValue("live_started_at")) {
        this.setMetadata("live_started_at", startedAt.toISOString());
      }
      if (mode === "live") {
        run.mainAmbiguousReleased = this.releaseExpiredMainUnknowns(startedAt);
      }
      const [apiResult, ticketListingResult] = await Promise.all([
        settled(timedCards(() => this.fetchAllApi())),
        settled(timedCards(() => this.fetchTicketListing())),
      ]);
      run.apiElapsedMs = apiResult.status === "fulfilled" ? apiResult.value.elapsedMs : 0;
      run.ticketListingElapsedMs = ticketListingResult.status === "fulfilled"
        ? ticketListingResult.value.elapsedMs
        : 0;
      apiContract = apiResult.status === "fulfilled"
        ? apiResult.value.cards?.contract || null
        : apiResult.reason?.contract || null;
      apiCards = apiResult.status === "fulfilled"
        ? apiResult.value.cards.map((card) => ({
            ...card,
            observedAt: apiResult.value.completedAt,
          }))
        : [];
      ticketListingCards = ticketListingResult.status === "fulfilled"
        ? ticketListingResult.value.cards.map((card) => ({
            ...card,
            observedAt: ticketListingResult.value.completedAt,
          }))
        : [];
      ticketListingIdentityKeys = new Set(
        ticketListingCards.flatMap((card) => offerIdentityKeys(card.link)),
      );
      discoveryCards = mergeOfferCards(apiCards, ticketListingCards);
      run.apiOffersSeen = apiCards.length;
      run.ticketListingOffersSeen = ticketListingCards.length;
      run.offersSeen = discoveryCards.length;
      if (apiResult.status === "rejected") {
        run.apiError = sanitizeError(apiResult.reason);
      } else if (!apiCards.length) {
        run.apiError = "uol_api_sem_ofertas";
      } else if (apiContract?.degraded) {
        run.apiError = `uol_api_contract_degraded:${apiContract.reason}:` +
          `total=${apiContract.total}:valid=${apiContract.valid}`;
      }
      if (ticketListingResult.status === "rejected") {
        run.ticketListingError = sanitizeError(ticketListingResult.reason);
        ticketListingFailureStreak += 1;
      } else if (!ticketListingCards.length) {
        run.ticketListingError = "uol_ticket_listing_sem_ofertas";
        ticketListingFailureStreak += 1;
      } else {
        ticketListingLastSuccessAt = ticketListingResult.value.completedAt;
        ticketListingFailureStreak = 0;
      }
      if (apiCards.length) {
        apiLastSuccessAt = apiResult.value.completedAt;
        apiFailureStreak = apiContract?.degraded ? apiFailureStreak + 1 : 0;
        apiHealthCards = buildApiHealthSnapshot(apiCards);
      } else {
        apiFailureStreak += 1;
      }
      if (discoveryCards.length) {
        discoverySnapshotFingerprint = await buildApiSnapshotFingerprint(discoveryCards);
        const previousFingerprint = this.metadataValue("runtime:critical_snapshot_fingerprint");
        discoverySnapshotChanged = !/^[a-f0-9]{64}$/.test(previousFingerprint) ||
          previousFingerprint !== discoverySnapshotFingerprint;
      }

      const initializedAt = this.metadataValue("initialized_at");
      if (!initializedAt && discoveryCards.length) {
        this.resolveListingCards(discoveryCards, startedAt.toISOString(), "baseline", {
          availabilityIdentityKeys: ticketListingIdentityKeys,
        });
        await this.backfillTitleValidityKeys();
        this.setMetadata("initialized_at", startedAt.toISOString());
        run.outcome = "baseline_created";
      } else if (initializedAt && discoveryCards.length && discoverySnapshotChanged) {
        const fastStartedAt = Date.now();
        const fastNow = new Date();
        const resolution = this.resolveListingCards(
          discoveryCards,
          fastNow.toISOString(),
          "pending_enrichment",
          {
            availabilityIdentityKeys: ticketListingIdentityKeys,
            restockIdentityKeys: ticketListingIdentityKeys,
          },
        );
        const cardsById = new Map(resolution.cards.map((card) => [card.id, card]));
        for (let offset = 0; offset < resolution.insertedIds.length; offset += 100) {
          const processed = await this.processPending(cardsById, fastNow, {
            priorityIds: resolution.insertedIds.slice(offset, offset + 100),
          });
          run.enriched += processed.enriched;
          run.wouldSendMain += processed.wouldSendMain;
          run.wouldSendCanal2 += processed.wouldSendCanal2;
          run.apiFastProcessed += processed.enriched;
        }
        const catchUp = await this.processPending(cardsById, fastNow);
        run.enriched += catchUp.enriched;
        run.wouldSendMain += catchUp.wouldSendMain;
        run.wouldSendCanal2 += catchUp.wouldSendCanal2;
        run.apiFastProcessed += catchUp.enriched;
        run.newOffers = resolution.inserted;
        const deliveryBatches = resolution.insertedIds.length
          ? Array.from(
              { length: Math.ceil(resolution.insertedIds.length / 100) },
              (_, index) => resolution.insertedIds.slice(index * 100, index * 100 + 100),
            )
          : [[]];
        for (const priorityIds of deliveryBatches) {
          const delivered = await this.withStorageStage(
            "delivery",
            () => this.processDeliveryQueue(new Date(), {
              priorityIds,
              waitForMainImage: true,
              targetNames: ["main", "canal2"],
            }),
          );
          run.mainSent += delivered.mainSent;
          run.canal2Sent += delivered.canal2Sent;
          run.deliveryFailed += delivered.failed;
          this.scheduleDiscordDelivery({
            priorityIds,
            rows: delivered.selectedRows,
            recoveryRows: delivered.recentSecondaryRows,
            source,
          });
        }
        run.apiFastMainSent = run.mainSent;
        run.apiFastElapsedMs = Date.now() - fastStartedAt;
        fastLastCompletedAt = new Date().toISOString();
        fastLastElapsedMs = run.apiFastElapsedMs;
        fastLastNewOffers = resolution.inserted;
        fastLastMainSent = run.apiFastMainSent;
        if (run.mainSent > 0) run.outcome = "telegram_delivered";
        else if (run.deliveryFailed > 0) run.outcome = "telegram_delivery_partial";
        else if (run.newOffers > 0) {
          run.outcome = mode === "live" ? "live_decisions_recorded" : "shadow_decisions_recorded";
        } else run.outcome = "no_change";
      } else if (initializedAt && discoveryCards.length) {
        // As fontes críticas continuam em 15s, mas uma fotografia idêntica não
        // precisa reler/enriquecer toda a listagem. Entregas pendentes,
        // probes de ingressos e recuperações continuam independentes.
        const delivered = await this.withStorageStage(
          "delivery",
          () => this.processDeliveryQueue(new Date(), {
            waitForMainImage: true,
            targetNames: ["main", "canal2"],
          }),
        );
        run.mainSent = delivered.mainSent;
        run.canal2Sent = delivered.canal2Sent;
        run.deliveryFailed += delivered.failed;
        this.scheduleDiscordDelivery({
          rows: delivered.selectedRows,
          recoveryRows: delivered.recentSecondaryRows,
          source,
        });
        run.outcome = run.mainSent > 0
          ? "telegram_delivered"
          : run.deliveryFailed > 0
            ? "telegram_delivery_partial"
            : "no_change";
      } else {
        // Even with both critical discovery sources degraded, retry already-
        // persisted main deliveries. General HTML fallback remains independent.
        const catchUp = await this.processPending(new Map(), new Date());
        run.enriched += catchUp.enriched;
        run.wouldSendMain += catchUp.wouldSendMain;
        run.wouldSendCanal2 += catchUp.wouldSendCanal2;
        const delivered = await this.withStorageStage(
          "delivery",
          () => this.processDeliveryQueue(new Date(), {
            waitForMainImage: true,
            targetNames: ["main", "canal2"],
          }),
        );
        run.mainSent = delivered.mainSent;
        run.canal2Sent = delivered.canal2Sent;
        run.deliveryFailed += delivered.failed;
        this.scheduleDiscordDelivery({
          rows: delivered.selectedRows,
          recoveryRows: delivered.recentSecondaryRows,
          source,
        });
        run.outcome = run.mainSent > 0 ? "telegram_delivered" : "api_degraded";
      }
      const ticketProbes = await this.withStorageStage(
        "tickets",
        () => this.processTicketAvailabilityProbes(new Date(), {
          controlCards: ticketListingCards,
        }),
      );
      run.ticketProbes = ticketProbes.probed;
      run.ticketProbeConfirmed = ticketProbes.confirmed;
      run.ticketProbeFallback = ticketProbes.fallback;
      run.ticketProbeMainEdited = ticketProbes.soldOutMainEdited;
      run.ticketProbeCanal2Edited = ticketProbes.soldOutCanal2Edited;
      run.ticketProbeDiscordEdited = ticketProbes.soldOutDiscordEdited;
      run.soldOutMainEdited += ticketProbes.soldOutMainEdited;
      run.soldOutCanal2Edited += ticketProbes.soldOutCanal2Edited;
      run.soldOutDiscordEdited += ticketProbes.soldOutDiscordEdited;
      run.deliveryFailed += ticketProbes.failed;
      if (ticketProbes.confirmed > 0) run.outcome = "ticket_sold_out_confirmed";
      if (apiCards.length && discoverySnapshotChanged) {
        try {
          this.recordSourceCards("api", apiCards, apiLastSuccessAt);
        } catch (error) {
          logEvent("warn", "uol_source_observation_failed", {
            source: "api",
            error: sanitizeError(error),
          });
        }
      }
      if (discoverySnapshotFingerprint) {
        this.setMetadataIfChanged(
          "runtime:critical_snapshot_fingerprint",
          discoverySnapshotFingerprint,
        );
      }
    } catch (error) {
      run.error = sanitizeError(error);
      run.outcome = "failed";
    } finally {
      run.finishedAt = new Date().toISOString();
      runFailureStreak = run.error || run.outcome === "failed" ? runFailureStreak + 1 : 0;
      this.scanInFlight = false;
      try {
        const apiSnapshot = {
          lastCompletedAt: run.finishedAt,
          lastOutcome: run.outcome,
          lastRunError: run.error,
          lastOffersSeen: run.apiOffersSeen,
          lastSuccessfulOffersSeen: apiCards.length && !apiContract?.degraded
            ? run.apiOffersSeen
            : Math.max(
                run.apiOffersSeen,
                Number(
                  previousApi.lastSuccessfulOffersSeen ?? previousApi.lastOffersSeen ??
                    this.metadataValue("api_last_offers_seen") ?? 0,
                ),
              ),
          lastElapsedMs: run.apiElapsedMs,
          lastError: run.apiError,
          lastSuccessAt: apiLastSuccessAt,
          failureStreak: apiFailureStreak,
          fastLastCompletedAt,
          fastLastElapsedMs,
          fastLastNewOffers,
          fastLastMainSent,
          discoverySnapshotChanged,
          discoverySnapshotFingerprint,
          healthCards: apiHealthCards,
          runFailureStreak,
        };
        const ticketListingSnapshot = {
          lastStartedAt: startedAt.toISOString(),
          lastCompletedAt: run.finishedAt,
          lastOffersSeen: run.ticketListingOffersSeen,
          lastSuccessfulOffersSeen: ticketListingCards.length
            ? run.ticketListingOffersSeen
            : Number(
                previousTicketListing.lastSuccessfulOffersSeen ??
                  previousTicketListing.lastOffersSeen ?? 0,
              ),
          lastElapsedMs: run.ticketListingElapsedMs,
          lastError: run.ticketListingError,
          lastSuccessAt: ticketListingLastSuccessAt,
          failureStreak: ticketListingFailureStreak,
          cards: ticketListingCards.length
            ? compactTicketListingCards(ticketListingCards)
            : [],
        };
        this.setCriticalSourceSnapshot({
          api: apiSnapshot,
          ticketListing: ticketListingSnapshot,
        });
        if (apiContract) {
          this.setRuntimeSnapshot("api_contract", {
            ...apiContract,
            lastCheckedAt: run.finishedAt,
          });
        }
        const lastPersistedRun = this.sqlExec(
          "SELECT outcome, error, finished_at FROM runs ORDER BY id DESC LIMIT 1",
        ).toArray()[0] || null;
        if (shouldPersistRunSummary(run, lastPersistedRun, run.finishedAt)) {
          this.insertRun(run);
        }
      } catch (error) {
        logEvent("warn", "uol_api_poll_telemetry_failed", {
          error: sanitizeError(error),
        });
      }
      try {
        run.storageRowsRead = this.completeStorageUsageCycle(
          "primary",
          storageReadStartedAt,
          {
            completedAt: run.finishedAt,
            versionId: this.env.WORKER_VERSION?.id,
            successful: !run.error && !run.apiError && !run.ticketListingError &&
              apiCards.length > 0 && ticketListingCards.length > 0,
          },
        );
      } catch (error) {
        logEvent("error", "uol_storage_usage_persist_failed", {
          cycle: "primary",
          error: sanitizeError(error),
        });
      }
    }

    logEvent(run.error ? "error" : "info", "uol_telegram_api_poll", {
      source: run.source,
      outcome: run.outcome,
      apiOffersSeen: run.apiOffersSeen,
      apiElapsedMs: run.apiElapsedMs,
      ticketListingOffersSeen: run.ticketListingOffersSeen,
      ticketListingElapsedMs: run.ticketListingElapsedMs,
      newOffers: run.newOffers,
      apiFastProcessed: run.apiFastProcessed,
      apiFastMainSent: run.apiFastMainSent,
      apiFastElapsedMs: run.apiFastElapsedMs,
      ticketProbes: run.ticketProbes,
      ticketProbeConfirmed: run.ticketProbeConfirmed,
      mainAmbiguousReleased: run.mainAmbiguousReleased,
      deliveryFailed: run.deliveryFailed,
      apiError: run.apiError,
      ticketListingError: run.ticketListingError,
      error: run.error,
    });
    return { ok: !run.error, ...run };
  }

  async runMaintenanceTick(source = "alarm") {
    return this.withStorageCycle(
      "maintenance",
      () => this.runMaintenanceTickWithStorageCycle(source),
    );
  }

  async runMaintenanceTickWithStorageCycle(source = "alarm") {
    if (this.maintenanceInFlight) {
      return { ok: false, outcome: "maintenance_in_progress" };
    }
    const storageReadStartedAt = Number(this.storageUsage.rowsRead || 0);
    this.maintenanceInFlight = true;
    const startedAt = new Date();
    const budget = this.storageUsageSnapshot(startedAt);
    if (!budget.maintenanceAllowed) {
      const storageContext = this.storageContext.getStore();
      if (storageContext) storageContext.stage = "guard";
      this.storageUsage.maintenanceSkipped += 1;
      const hardReserveActive = Number(budget.rowsRead || 0) >=
        Number(budget.limit || 0) - DURABLE_OBJECT_CRITICAL_READ_RESERVE;
      const result = {
        ok: false,
        outcome: "storage_read_budget_guard",
        error: "durable_object_rows_read_reserve_active",
        rowsRead: budget.rowsRead,
        limit: budget.limit,
        criticalReserve: budget.criticalReserve,
        retryAt: maintenanceRetryAt({
          now: startedAt,
          resetAt: budget.resetAt,
          skipped: this.storageUsage.maintenanceSkipped,
          deferUntilReset: hardReserveActive,
        }),
        retryReason: "storage_read_budget_guard",
      };
      this.maintenanceInFlight = false;
      try {
        this.setRuntimeSnapshot("maintenance", {
          lastStartedAt: startedAt.toISOString(),
          lastCompletedAt: new Date().toISOString(),
          lastElapsedMs: Date.now() - startedAt.getTime(),
          lastError: "",
          lastOutcome: result.outcome,
          retryAt: result.retryAt,
          deferred: true,
          deferredReason: result.retryReason,
        });
        this.completeStorageUsageCycle("maintenance", storageReadStartedAt);
      } catch (error) {
        logEvent("error", "uol_storage_usage_persist_failed", {
          cycle: "maintenance_guard",
          error: sanitizeError(error),
        });
      }
      logEvent("warn", "uol_telegram_maintenance", result);
      return result;
    }
    const result = {
      ok: true,
      outcome: "no_change",
      htmlReconciled: false,
      htmlLedgerReconciled: false,
      htmlLedgerReason: "not_due",
      newOffers: 0,
      canal2Sent: 0,
      discordSent: 0,
      commentsSent: 0,
      soldOutDetected: 0,
      soldOutMainEdited: 0,
      soldOutCanal2Edited: 0,
      soldOutDiscordEdited: 0,
      restockMainEdited: 0,
      restockCanal2Edited: 0,
      restockMainReposted: 0,
      restockCanal2Reposted: 0,
      restockDiscordEdited: 0,
      soldOutMessageMissing: 0,
      maintenanceRepairs: 0,
      deliveryFailed: 0,
      beeperSent: 0,
      beeperQueued: 0,
      beeperBlocked: false,
      mainImagesUpgraded: 0,
      imageCachesPrimed: 0,
      ticketListingReused: false,
      error: "",
    };
    let activeOfferIds = new Set();
    let completeListingSnapshot = false;

    try {
      const now = new Date();
      const operationalHealthDue = this.operationalHealthDue(now);
      const initializedAt = this.metadataValue("initialized_at");
      const identityPolicyPending = Boolean(initializedAt) &&
        this.metadataValue(IDENTITY_POLICY_METADATA_KEY) !== IDENTITY_POLICY_VERSION;
      const api = this.runtimeSnapshot("api");
      const html = this.runtimeSnapshot("html");
      const apiError = api.lastError ?? this.metadataValue("api_last_error");
      const apiOffers = Number(
        api.lastOffersSeen ?? this.metadataValue("api_last_offers_seen") ?? 0,
      );
      const lastHtmlStartedAt = html.lastStartedAt ||
        this.metadataValue("html_reconciliation_last_started_at");
      const configuredHtmlIntervalSeconds = envNumber(
        this.env,
        "HTML_RECONCILIATION_INTERVAL_SECONDS",
        60,
        30,
        3_600,
      );
      const htmlIntervalSeconds = this.storageUsage.maintenanceSkipped > 0
        ? Math.max(300, configuredHtmlIntervalSeconds)
        : configuredHtmlIntervalSeconds;
      const htmlDue = identityPolicyPending || htmlReconciliationDue({
        source,
        apiStatus: apiError || apiOffers <= 0 ? "rejected" : "fulfilled",
        apiOffers,
        initialized: Boolean(initializedAt),
        lastStartedAt: lastHtmlStartedAt,
        intervalSeconds: htmlIntervalSeconds,
      });

      if (htmlDue) {
        result.htmlReconciled = true;
        const [listingResult, ticketListingResult] = await Promise.all([
          settled(timedCards(() => this.withStorageStage("html", () => fetchListing()))),
          this.ticketListingForMaintenance(now),
        ]);
        result.ticketListingReused = Boolean(ticketListingResult.value?.reused);
        const htmlCompletedAt = new Date().toISOString();
        const mainListingCards = listingResult.status === "fulfilled"
          ? listingResult.value.cards
          : [];
        const ticketListingCards = ticketListingResult.status === "fulfilled"
          ? ticketListingResult.value.cards
          : [];
        const listingCards = mergeOfferCards(ticketListingCards, mainListingCards);
        const listingSnapshotFetched = listingResult.status === "fulfilled" &&
          ticketListingResult.status === "fulfilled";
        activeOfferIds = new Set(listingCards.map((card) => card.id));
        const recentApiCards = this.recentApiCardsForHealth(now);
        const sourceHealth = this.updateSourceHealth({
          listingResult,
          ticketListingResult,
          apiResult: { status: recentApiCards.length ? "fulfilled" : "rejected" },
          mainListingCards,
          ticketListingCards,
          apiCards: recentApiCards,
          now,
        });
        this.setRuntimeSnapshot("html", {
          lastStartedAt: now.toISOString(),
          lastCompletedAt: htmlCompletedAt,
          mainLastOffersSeen: mainListingCards.length,
          mainLastSuccessfulOffersSeen: sourceHealth.mainListingHealthy &&
              !sourceHealth.mainSharpDrop
            ? mainListingCards.length
            : Number(
                html.mainLastSuccessfulOffersSeen ?? html.mainLastOffersSeen ??
                  this.metadataValue("main_listing_last_offers_seen") ?? 0,
              ),
          mainLastError: listingResult.status === "rejected"
            ? sanitizeError(listingResult.reason)
            : "",
          mainLastSuccessAt: listingResult.status === "fulfilled"
            ? listingResult.value.completedAt
            : html.mainLastSuccessAt || this.metadataValue("main_listing_last_success_at"),
          mainLastElapsedMs: listingResult.status === "fulfilled"
            ? listingResult.value.elapsedMs
            : Number(html.mainLastElapsedMs ?? this.metadataValue("main_listing_last_elapsed_ms") ?? 0),
          ticketLastOffersSeen: ticketListingCards.length,
          ticketLastSuccessfulOffersSeen: sourceHealth.ticketListingHealthy &&
              !sourceHealth.ticketSharpDrop && ticketListingCards.length > 0
            ? ticketListingCards.length
            : Number(
                html.ticketLastSuccessfulOffersSeen ?? html.ticketLastOffersSeen ??
                  this.metadataValue("ticket_listing_last_offers_seen") ?? 0,
              ),
          ticketLastError: ticketListingResult.status === "rejected"
            ? sanitizeError(ticketListingResult.reason)
            : "",
          ticketLastSuccessAt: ticketListingResult.status === "fulfilled"
            ? ticketListingResult.value.completedAt
            : html.ticketLastSuccessAt || this.metadataValue("ticket_listing_last_success_at"),
          ticketLastElapsedMs: ticketListingResult.status === "fulfilled"
            ? ticketListingResult.value.elapsedMs
            : Number(
              html.ticketLastElapsedMs ?? this.metadataValue("ticket_listing_last_elapsed_ms") ?? 0,
            ),
        });

        const listingSnapshotComplete = listingSnapshotFetched && listingCards.length > 0 &&
          sourceHealth.mainListingHealthy && sourceHealth.ticketListingHealthy &&
          !sourceHealth.mainSharpDrop && !sourceHealth.ticketSharpDrop;
        const htmlSnapshotFingerprint = listingSnapshotComplete
          ? await buildApiSnapshotFingerprint(listingCards)
          : "";
        const htmlSnapshotDecision = shouldReconcileHtmlSnapshot({
          fingerprint: htmlSnapshotFingerprint,
          previousFingerprint: identityPolicyPending
            ? ""
            : this.metadataValue("runtime:html_snapshot_fingerprint"),
          lastReconciledAt: this.metadataValue("runtime:html_snapshot_reconciled_at"),
          now,
          refreshIntervalSeconds: envNumber(
            this.env,
            "HTML_SNAPSHOT_REFRESH_INTERVAL_SECONDS",
            900,
            300,
            3_600,
          ),
          complete: listingSnapshotComplete,
          initialized: Boolean(initializedAt),
        });
        result.htmlLedgerReason = htmlSnapshotDecision.reason;

        if (listingCards.length && htmlSnapshotDecision.reconcile) {
          result.htmlLedgerReconciled = true;
          if (listingResult.status === "fulfilled") {
            this.recordSourceCards("listing", mainListingCards, listingResult.value.completedAt);
          }
          if (ticketListingResult.status === "fulfilled") {
            this.recordSourceCards(
              "listing",
              ticketListingCards,
              ticketListingResult.value.completedAt,
            );
          }
          if (!initializedAt) {
            const listingIdentityKeys = new Set(
              [
                ...mainListingCards.filter((card) => !isTicketCampaign(card)),
                ...ticketListingCards,
              ].flatMap((card) => offerIdentityKeys(card.link)),
            );
            this.resolveListingCards(listingCards, now.toISOString(), "baseline", {
              availabilityIdentityKeys: listingIdentityKeys,
            });
            this.setMetadata("initialized_at", now.toISOString());
          } else {
            const listingIdentityKeys = new Set(
              [
                ...mainListingCards.filter((card) => !isTicketCampaign(card)),
                ...ticketListingCards,
              ].flatMap((card) => offerIdentityKeys(card.link)),
            );
            const resolution = this.resolveListingCards(
              listingCards,
              now.toISOString(),
              "pending_enrichment",
              {
                availabilityIdentityKeys: listingIdentityKeys,
                restockIdentityKeys: listingIdentityKeys,
              },
            );
            result.newOffers = resolution.inserted;
            const cardsById = new Map(resolution.cards.map((card) => [card.id, card]));
            for (let offset = 0; offset < resolution.insertedIds.length; offset += 100) {
              await this.processPending(cardsById, now, {
                priorityIds: resolution.insertedIds.slice(offset, offset + 100),
              });
            }
            await this.processPending(cardsById, now);
            if (sourceHealth.mainListingHealthy && !sourceHealth.mainSharpDrop) {
              const mainIdentityKeys = new Set(
                mainListingCards.flatMap((card) => offerIdentityKeys(card.link)),
              );
              const mainResolvedIds = new Set(
                resolution.cards
                  .filter((card) => !isTicketCampaign(card))
                  .filter((card) => offerIdentityKeys(card.link).some(
                    (key) => mainIdentityKeys.has(key),
                  ))
                  .map((card) => card.id),
              );
              result.soldOutDetected += this.evaluateSoldOut(mainResolvedIds, now, "main");
            }
            if (sourceHealth.ticketListingHealthy && !sourceHealth.ticketSharpDrop) {
              const ticketIdentityKeys = new Set(
                ticketListingCards.flatMap((card) => offerIdentityKeys(card.link)),
              );
              const ticketResolvedIds = new Set(
                resolution.cards
                  .filter(isTicketCampaign)
                  .filter((card) => offerIdentityKeys(card.link).some(
                    (key) => ticketIdentityKeys.has(key),
                  ))
                  .map((card) => card.id),
              );
              result.soldOutDetected += this.evaluateSoldOut(ticketResolvedIds, now, "ticket");
            }
          }
          if (listingSnapshotComplete) {
            this.setMetadataIfChanged(
              "runtime:html_snapshot_fingerprint",
              htmlSnapshotFingerprint,
            );
            this.setMetadataIfChanged(
              "runtime:html_snapshot_reconciled_at",
              now.toISOString(),
            );
            this.setMetadataIfChanged(
              IDENTITY_POLICY_METADATA_KEY,
              IDENTITY_POLICY_VERSION,
            );
            completeListingSnapshot = true;
          }
        }
      }

      const discordDelivery = await this.withStorageStage(
        "delivery",
        () => this.processDeliveryQueue(new Date(), {
          targetNames: ["discord"],
        }),
      );
      result.discordSent = discordDelivery.discordSent;
      result.deliveryFailed += discordDelivery.failed;

      const imageCaches = await this.withStorageStage(
        "images",
        () => this.primePendingDiscordImageCache(new Date()),
      );
      result.imageCachesPrimed = imageCaches.primed;
      result.deliveryFailed += imageCaches.failed;

      const imageUpgrades = await this.withStorageStage(
        "images",
        () => this.upgradeTimedOutMainImages(new Date()),
      );
      result.mainImagesUpgraded = imageUpgrades.upgraded;
      result.deliveryFailed += imageUpgrades.failed;

      const canal2Delivery = await this.withStorageStage(
        "delivery",
        () => this.processDeliveryQueue(new Date(), {
          targetNames: ["canal2"],
        }),
      );
      result.canal2Sent = canal2Delivery.canal2Sent;
      result.deliveryFailed += canal2Delivery.failed;

      this.reconcileDiscussionForwards();
      const comments = await this.withStorageStage(
        "comments",
        () => this.processDiscussionComments(2),
      );
      result.commentsSent = comments.sent;
      result.deliveryFailed += comments.failed;
      const restock = await this.withStorageStage(
        "maintenanceLedger",
        () => this.processRestockSync(new Date()),
      );
      result.restockMainEdited = restock.mainEdited;
      result.restockCanal2Edited = restock.canal2Edited;
      result.restockMainReposted = restock.mainReposted;
      result.restockCanal2Reposted = restock.canal2Reposted;
      result.deliveryFailed += restock.failed;
      if (operationalHealthDue) {
        const maintenanceRepairs = this.repairKnownMaintenanceDeadLetters(
          new Date(),
          envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50),
        );
        result.maintenanceRepairs = maintenanceRepairs.mainMarkedSynced +
          maintenanceRepairs.canal2Requeued;
      }
      const soldOut = await this.withStorageStage(
        "maintenanceLedger",
        () => this.processSoldOutSync(new Date()),
      );
      result.soldOutMainEdited = soldOut.mainEdited;
      result.soldOutCanal2Edited = soldOut.canal2Edited;
      result.soldOutMessageMissing = soldOut.messageMissing;
      result.deliveryFailed += soldOut.failed;
      const discordAvailability = await this.withStorageStage(
        "maintenanceLedger",
        () => this.processDiscordAvailabilitySync(new Date()),
      );
      result.soldOutDiscordEdited = discordAvailability.soldOutEdited;
      result.restockDiscordEdited = discordAvailability.restockEdited;
      result.soldOutMessageMissing += discordAvailability.messageMissing;
      result.deliveryFailed += discordAvailability.failed;
      if (completeListingSnapshot) this.pruneOffers(new Date(), activeOfferIds);

      try {
        await this.ensureTelegramWebhook();
        this.setMetadataIfChanged("telegram_webhook_check_error", "");
      } catch (error) {
        this.setMetadataIfChanged("telegram_webhook_check_error", sanitizeError(error));
      }
      if (operationalHealthDue) {
        try {
          await this.processOperationalHealth(new Date());
          this.setMetadataIfChanged("ops_health_error", "");
        } catch (error) {
          this.setMetadataIfChanged("ops_health_error", sanitizeError(error));
        }
      }
      if (
        result.newOffers || result.canal2Sent || result.imageCachesPrimed ||
        result.discordSent || result.beeperSent || result.beeperQueued ||
        result.commentsSent || result.soldOutDetected ||
        result.soldOutMainEdited || result.soldOutCanal2Edited ||
        result.soldOutDiscordEdited || result.restockDiscordEdited ||
        result.restockMainEdited || result.restockCanal2Edited ||
        result.restockMainReposted || result.restockCanal2Reposted ||
        result.soldOutMessageMissing || result.maintenanceRepairs
      ) {
        result.outcome = "maintenance_applied";
      }
    } catch (error) {
      result.ok = false;
      result.outcome = "failed";
      result.error = sanitizeError(error);
    } finally {
      const completedAt = new Date();
      this.maintenanceInFlight = false;
      try {
        this.setRuntimeSnapshot("maintenance", {
          lastStartedAt: startedAt.toISOString(),
          lastCompletedAt: completedAt.toISOString(),
          lastElapsedMs: completedAt.getTime() - startedAt.getTime(),
          lastError: result.error,
          lastOutcome: result.outcome,
          retryAt: "",
          deferred: false,
          deferredReason: "",
        });
      } catch (error) {
        logEvent("warn", "uol_maintenance_telemetry_failed", {
          error: sanitizeError(error),
        });
      }
      try {
        result.storageRowsRead = this.completeStorageUsageCycle(
          "maintenance",
          storageReadStartedAt,
        );
      } catch (error) {
        logEvent("error", "uol_storage_usage_persist_failed", {
          cycle: "maintenance",
          error: sanitizeError(error),
        });
      }
    }
    logEvent(result.error ? "error" : "info", "uol_telegram_maintenance", result);
    return result;
  }

  async alarm() {
    const budget = this.storageUsageSnapshot();
    const cadenceTarget = budget.primaryAllowed
      ? Date.now() + budget.recommendedPollIntervalSeconds * 1_000
      : Date.parse(budget.resetAt) + 1_000;
    let result = null;

    // Rearmar antes de qualquer leitura SQL. Mesmo se a leitura seguinte
    // falhar por cota, o próximo disparo já existe e pode retomar após reset.
    await this.ctx.storage.setAlarm(Math.max(Date.now() + 1_000, cadenceTarget));
    if (!budget.primaryAllowed) {
      logEvent("error", "uol_primary_storage_read_budget_guard", {
        rowsRead: budget.rowsRead,
        limit: budget.limit,
        resetAt: budget.resetAt,
      });
      return;
    }
    try {
      // Este alarme possui uma única responsabilidade: descobrir na API,
      // persistir a decisão e enviar o destino principal imediatamente.
      result = await this.scan("alarm");
    } catch (error) {
      logEvent("error", "uol_telegram_shadow_alarm_unhandled", {
        error: sanitizeError(error),
      });
    }
    // WhatsApp é uma entrega crítica, mas permanece fora do tempo de resposta
    // do polling. O waitUntil mantém a tentativa viva sem atrasar a coleta.
    this.scheduleCriticalBeeperDelivery("alarm");
    if (result) {
      const lastMaintenanceBootstrap = Date.parse(
        this.metadataValue("maintenance_alarm_last_ensured_at") || "",
      );
      const maintenanceBootstrapDue = !Number.isFinite(lastMaintenanceBootstrap) ||
        Date.now() - lastMaintenanceBootstrap >= 5 * 60_000;
      const maintenanceUrgent = Boolean(
        result.apiError || result.error || result.newOffers || result.mainSent,
      );
      const beeperRecovery = beeperGatewayConfiguration(this.env);
      const beeperRecoveryFingerprint = beeperRecovery.offerIds.join(",");
      const beeperRecoveryUrgent = beeperRecovery.enabled &&
        beeperRecovery.configured && beeperRecovery.filterActive &&
        this.metadataValue(BEEPER_RECOVERY_METADATA_KEY) !==
          beeperRecoveryFingerprint;
      const maintenanceBudget = this.storageUsageSnapshot();
      if (maintenanceUrgent || beeperRecoveryUrgent || maintenanceBootstrapDue) {
        try {
          await this.ensureMaintenanceAlarm(
            (maintenanceUrgent || beeperRecoveryUrgent) &&
              maintenanceBudget.maintenanceAllowed,
          );
          this.setMetadata("maintenance_alarm_last_ensured_at", new Date().toISOString());
        } catch (error) {
          logEvent("error", "uol_telegram_maintenance_bootstrap_failed", {
            error: sanitizeError(error),
          });
        }
      }
    }
  }

  reconcileUnknownMainFromForward(message, origin) {
    const forwardedUrls = telegramOfferUrls(message);
    if (!forwardedUrls.length) return "";
    const rows = this.sqlExec(
      `SELECT id, link FROM offers
       WHERE main_sent_at = ''
         AND (
           main_delivery_unknown_at <> '' OR
           delivery_unknown_target = 'main' OR
           delivery_unknown_target LIKE 'main,%' OR
           delivery_unknown_target LIKE '%,main' OR
           delivery_unknown_target LIKE '%,main,%'
         )
       ORDER BY first_seen_at DESC LIMIT 32`,
    ).toArray();
    const row = rows.find((candidate) => forwardedUrls
      .some((url) => offerIdentityCompatible(candidate.link, url)));
    if (!row) return "";
    const originTimestamp = Number(origin?.date || 0) * 1_000;
    const sentAt = Number.isFinite(originTimestamp) && originTimestamp > 0
      ? new Date(originTimestamp).toISOString()
      : new Date().toISOString();
    this.sqlExec(
      `UPDATE offers SET main_message_id = ?, main_message_kind = ?, main_sent_at = ?,
         main_delivery_error = '', main_delivery_in_flight_at = '',
         main_delivery_next_attempt_at = '', main_delivery_unknown_at = ''
       WHERE id = ?`,
      Number(origin.message_id),
      Array.isArray(message?.photo) && message.photo.length ? "photo" : "text",
      sentAt,
      row.id,
    );
    this.refreshDeliveryStatus(row.id, new Date());
    logEvent("info", "uol_telegram_unknown_main_reconciled", {
      offerId: row.id,
      messageId: Number(origin.message_id),
    });
    return row.id;
  }

  async handleTelegramUpdate(update) {
    const message = update?.message;
    const origin = message?.forward_origin;
    const expectedGroup = String(this.env.GRUPO_COMENTARIO_ID || "").trim();
    const expectedChannel = String(this.env.TELEGRAM_CHAT_ID || "").trim();
    if (
      !message?.is_automatic_forward ||
      String(message?.chat?.id || "") !== expectedGroup ||
      origin?.type !== "channel" ||
      String(origin?.chat?.id || "") !== expectedChannel ||
      !Number(origin?.message_id) ||
      !Number(message?.message_id)
    ) {
      return { ok: true, matched: false };
    }
    const reconciledOfferId = this.reconcileUnknownMainFromForward(message, origin);
    this.sqlExec(
      `INSERT INTO pending_discussion_forwards(
         origin_message_id, discussion_message_id, received_at
       ) VALUES (?, ?, ?)
       ON CONFLICT(origin_message_id) DO UPDATE SET
         discussion_message_id = excluded.discussion_message_id,
         received_at = excluded.received_at`,
      Number(origin.message_id),
      Number(message.message_id),
      new Date().toISOString(),
    );
    const matched = this.reconcileDiscussionForwards();
    if (!matched) {
      return { ok: true, matched: false, queued: true, reconciledOfferId };
    }
    return { ok: true, matched: true, queued: true, reconciledOfferId };
  }

  async runNow() {
    const result = await this.scan("manual");
    const alarmScheduledAt = await this.ensureAlarm();
    const maintenanceAlarmScheduledAt = await this.ensureMaintenanceAlarm(true);
    return {
      ...result,
      alarmScheduledAt,
      maintenanceAlarmScheduledAt,
    };
  }

  resolveDeliveryUnknown(id, target, outcome, payload = {}) {
    const normalizedTarget = String(target || "").trim().toLowerCase();
    const normalizedOutcome = String(outcome || "").trim().toLowerCase();
    if (!["main", "canal2", "discord", "comment", "beeper"].includes(normalizedTarget)) {
      throw new Error("delivery_resolution_target_invalid");
    }
    if (!["sent", "not_sent", "closed"].includes(normalizedOutcome)) {
      throw new Error("delivery_resolution_outcome_invalid");
    }
    if (normalizedOutcome === "closed" && normalizedTarget !== "comment") {
      throw new Error("delivery_resolution_closed_comment_only");
    }
    const row = this.sqlExec(
      "SELECT * FROM offers WHERE id = ? LIMIT 1",
      String(id || "").trim(),
    ).toArray()[0];
    if (!row) throw new Error("delivery_offer_not_found");
    const resolvedAt = new Date().toISOString();

    if (normalizedTarget === "beeper") {
      const queue = this.sqlExec(
        `SELECT attempts, sent_at, last_error
           FROM beeper_delivery_queue WHERE offer_id = ? LIMIT 1`,
        row.id,
      ).toArray()[0];
      const lastError = String(queue?.last_error || "");
      const evidenceResolvable = lastError.startsWith("ambiguous:") ||
        lastError === "terminal:idempotency_conflict";
      if (
        !queue || queue.sent_at || !evidenceResolvable ||
        (normalizedOutcome === "not_sent" && !lastError.startsWith("ambiguous:"))
      ) {
        throw new Error("delivery_target_not_unknown");
      }
      const evidenceReference = String(payload?.evidenceReference || "").trim();
      if (
        normalizedOutcome === "sent" &&
        !/^[a-z0-9][a-z0-9:._-]{7,159}$/iu.test(evidenceReference)
      ) {
        throw new Error("delivery_resolution_evidence_required");
      }
      if (normalizedOutcome === "sent") {
        this.sqlExec(
          `UPDATE beeper_delivery_queue SET sent_at = ?, in_flight_at = '',
             next_attempt_at = '', last_error = ''
           WHERE offer_id = ? AND sent_at = '' AND (
             last_error LIKE 'ambiguous:%' OR
             last_error = 'terminal:idempotency_conflict'
           )`,
          resolvedAt,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "beeper",
          operation: "delivery",
          state: "resolved_sent",
          attempt: Number(queue.attempts || 0),
          generation: Number(row.delivery_generation || 0),
          occurredAt: resolvedAt,
          externalId: evidenceReference,
        });
      } else {
        this.sqlExec(
          `UPDATE beeper_delivery_queue SET attempts = 0, in_flight_at = '',
             next_attempt_at = '', last_error = ''
           WHERE offer_id = ? AND sent_at = '' AND last_error LIKE 'ambiguous:%'`,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "beeper",
          operation: "delivery",
          state: "resolved_not_sent",
          attempt: Number(queue.attempts || 0),
          generation: Number(row.delivery_generation || 0),
          occurredAt: resolvedAt,
        });
      }
      const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
      const diagnostics = this.beeperQueueDiagnostics(maxAttempts);
      this.updateBeeperRuntimeSnapshot({
        pending: diagnostics.pending,
        exhausted: diagnostics.exhausted,
        unknown: diagnostics.unknown,
        lastErrorCode: "",
      });
      return {
        ok: true,
        id: row.id,
        target: normalizedTarget,
        outcome: normalizedOutcome,
      };
    }
    const legacyUnknownTargets = String(row.delivery_unknown_target || "")
      .split(",").map((value) => value.trim()).filter(Boolean);
    const unknownAt = normalizedTarget === "comment"
      ? row.comment_delivery_unknown_at
      : row[`${normalizedTarget}_delivery_unknown_at`] ||
        (legacyUnknownTargets.includes(normalizedTarget) ? row.delivery_unknown_at : "");
    if (!unknownAt) throw new Error("delivery_target_not_unknown");
    if (normalizedTarget === "comment") {
      if (normalizedOutcome === "closed") {
        const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
        this.sqlExec(
          `UPDATE offers SET comment_delivery_attempts = ?,
             comment_delivery_error = 'historical_unknown_closed',
             comment_delivery_in_flight_at = '', comment_delivery_next_attempt_at = '',
             comment_delivery_unknown_at = '' WHERE id = ?`,
          maxAttempts,
          row.id,
        );
        this.recordDeliveryLedgerEvent({
          offerId: row.id,
          target: "comment",
          operation: "comment",
          state: "closed",
          attempt: maxAttempts,
          generation: row.delivery_generation,
          occurredAt: resolvedAt,
          error: "historical_unknown_closed",
        });
      } else if (normalizedOutcome === "sent") {
        const suppliedIds = (Array.isArray(payload?.messageIds) ? payload.messageIds : [])
          .map(Number).filter((value) => Number.isInteger(value) && value > 0);
        if (!suppliedIds.length) throw new Error("delivery_resolution_message_ids_required");
        let existingIds = [];
        try {
          existingIds = JSON.parse(row.comment_message_ids || "[]")
            .map(Number).filter((value) => Number.isInteger(value) && value > 0);
        } catch {
          existingIds = [];
        }
        const suppliedIncludesPrefix = existingIds.every(
          (messageId, index) => suppliedIds[index] === messageId,
        );
        const messageIds = suppliedIncludesPrefix
          ? suppliedIds
          : [...existingIds, ...suppliedIds];
        const expectedChunks = buildDiscussionCommentChunks(rowToOffer(row)).length;
        if (
          messageIds.length > expectedChunks ||
          new Set(messageIds).size !== messageIds.length
        ) {
          throw new Error("delivery_resolution_message_ids_invalid");
        }
        const completed = messageIds.length === expectedChunks;
        this.sqlExec(
          `UPDATE offers SET comment_message_ids = ?, comment_chunks_sent = ?,
             comment_sent_at = ?, comment_delivery_error = '',
             comment_delivery_attempts = CASE WHEN ? THEN comment_delivery_attempts ELSE 0 END,
             comment_delivery_in_flight_at = '', comment_delivery_next_attempt_at = '',
             comment_delivery_unknown_at = '' WHERE id = ?`,
          JSON.stringify(messageIds),
          messageIds.length,
          completed ? resolvedAt : "",
          completed ? 1 : 0,
          row.id,
        );
      } else {
        this.sqlExec(
          `UPDATE offers SET comment_delivery_attempts = 0, comment_delivery_error = '',
             comment_delivery_in_flight_at = '', comment_delivery_next_attempt_at = '',
             comment_delivery_unknown_at = '' WHERE id = ?`,
          row.id,
        );
      }
      return { ok: true, id: row.id, target: normalizedTarget, outcome: normalizedOutcome };
    }

    const messageId = String(payload?.messageId || "").trim();
    if (normalizedOutcome === "sent" && !messageId) {
      throw new Error("delivery_resolution_message_id_required");
    }
    if (normalizedOutcome === "sent" && normalizedTarget === "main") {
      const numericId = Number(messageId);
      if (!Number.isInteger(numericId) || numericId <= 0) {
        throw new Error("delivery_resolution_message_id_invalid");
      }
      const messageKind = ["photo", "text"].includes(payload?.messageKind)
        ? payload.messageKind
        : "text";
      this.sqlExec(
        `UPDATE offers SET main_message_id = ?, main_message_kind = ?, main_sent_at = ?,
           main_delivery_error = '', main_delivery_in_flight_at = '',
           main_delivery_next_attempt_at = '', main_delivery_unknown_at = '' WHERE id = ?`,
        numericId,
        messageKind,
        resolvedAt,
        row.id,
      );
    } else if (normalizedOutcome === "sent" && normalizedTarget === "canal2") {
      const numericId = Number(messageId);
      if (!Number.isInteger(numericId) || numericId <= 0) {
        throw new Error("delivery_resolution_message_id_invalid");
      }
      this.sqlExec(
        `UPDATE offers SET canal2_message_id = ?, canal2_sent_at = ?,
           canal2_delivery_error = '', canal2_delivery_in_flight_at = '',
           canal2_delivery_next_attempt_at = '', canal2_delivery_unknown_at = '' WHERE id = ?`,
        numericId,
        resolvedAt,
        row.id,
      );
    } else if (normalizedOutcome === "sent") {
      this.sqlExec(
        `UPDATE offers SET discord_message_id = ?, discord_sent_at = ?,
           discord_delivery_error = '', discord_delivery_in_flight_at = '',
           discord_delivery_next_attempt_at = '', discord_delivery_unknown_at = '' WHERE id = ?`,
        messageId,
        resolvedAt,
        row.id,
      );
    } else {
      const columns = {
        main: ["main_delivery_attempts", "main_delivery_error", "main_delivery_in_flight_at",
          "main_delivery_next_attempt_at", "main_delivery_unknown_at"],
        canal2: ["canal2_delivery_attempts", "canal2_delivery_error",
          "canal2_delivery_in_flight_at", "canal2_delivery_next_attempt_at",
          "canal2_delivery_unknown_at"],
        discord: ["discord_delivery_attempts", "discord_delivery_error",
          "discord_delivery_in_flight_at", "discord_delivery_next_attempt_at",
          "discord_delivery_unknown_at"],
      }[normalizedTarget];
      this.sqlExec(
        `UPDATE offers SET ${columns[0]} = 0, ${columns[1]} = '', ${columns[2]} = '',
           ${columns[3]} = '', ${columns[4]} = '' WHERE id = ?`,
        row.id,
      );
    }
    const refreshed = this.refreshDeliveryStatus(row.id, new Date());
    return {
      ok: true,
      id: row.id,
      target: normalizedTarget,
      outcome: normalizedOutcome,
      status: refreshed?.classification?.state || "",
    };
  }

  requeueDelivery(id, target = "offer") {
    if (this.currentDeliveryMode() !== "live") {
      throw new Error("delivery_requeue_requires_live_mode");
    }
    const row = this.sqlExec(
      "SELECT * FROM offers WHERE id = ? LIMIT 1",
      String(id || "").trim(),
    ).toArray()[0];
    if (!row) throw new Error("delivery_offer_not_found");
    const normalizedTarget = String(target || "offer").trim().toLowerCase();
    if (normalizedTarget === "comment") {
      if (!row.discussion_message_id || row.comment_sent_at) {
        throw new Error("delivery_comment_not_requeueable");
      }
      if (row.comment_delivery_unknown_at) {
        throw new Error("delivery_unknown_requires_resolution");
      }
      this.sqlExec(
        `UPDATE offers SET comment_delivery_attempts = 0, comment_delivery_error = '',
           comment_delivery_next_attempt_at = '', comment_delivery_in_flight_at = '',
           delivery_generation = ? WHERE id = ?`,
        this.currentDeliveryGeneration(),
        row.id,
      );
      return { ok: true, id: row.id, target: "comment", status: row.status };
    }
    if (!["offer", "main", "canal2", "discord"].includes(normalizedTarget)) {
      throw new Error("delivery_requeue_target_invalid");
    }
    if (![
      "delivery_dead_letter",
      "delivery_unknown",
      "delivery_quarantined",
      "delivery_blocked_configuration",
      "partial_delivery",
    ].includes(row.status)) {
      throw new Error("delivery_offer_not_requeueable");
    }
    const legacyUnknownTargets = String(row.delivery_unknown_target || "")
      .split(",").map((value) => value.trim()).filter(Boolean);
    const unknownTargets = ["main", "canal2", "discord"].filter(
      (name) => Boolean(row[`${name}_delivery_unknown_at`]) ||
        legacyUnknownTargets.includes(name),
    );
    if (
      (normalizedTarget === "offer" && unknownTargets.length) ||
      unknownTargets.includes(normalizedTarget)
    ) {
      throw new Error("delivery_unknown_requires_resolution");
    }
    const targetColumns = {
      main: ["main_delivery_attempts", "main_delivery_error",
        "main_delivery_in_flight_at", "main_delivery_next_attempt_at"],
      canal2: ["canal2_delivery_attempts", "canal2_delivery_error",
        "canal2_delivery_in_flight_at", "canal2_delivery_next_attempt_at"],
      discord: ["discord_delivery_attempts", "discord_delivery_error",
        "discord_delivery_in_flight_at", "discord_delivery_next_attempt_at"],
    };
    const targets = normalizedTarget === "offer"
      ? ["main", "canal2", "discord"]
      : [normalizedTarget];
    const resets = targets.flatMap((name) => {
      const [attempts, error, inFlight, nextAttempt] = targetColumns[name];
      return [`${attempts} = 0`, `${error} = ''`, `${inFlight} = ''`, `${nextAttempt} = ''`];
    });
    this.sqlExec(
      `UPDATE offers SET ${resets.join(", ")}, delivery_generation = ?,
         delivery_dead_letter_at = '', delivery_dead_letter_reason = '',
         delivery_quarantine_reason = '' WHERE id = ?`,
      this.currentDeliveryGeneration(),
      row.id,
    );
    const refreshed = this.refreshDeliveryStatus(row.id, new Date());
    return {
      ok: true,
      id: row.id,
      target: normalizedTarget,
      status: refreshed?.classification?.state || "",
    };
  }

  async getHealth() {
    const alarm = await this.ctx.storage.getAlarm();
    const alarmScheduledAt = alarm == null ? "" : new Date(alarm).toISOString();
    const alarmIntervalSeconds = envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600);
    const maintenanceIntervalSeconds = envNumber(
      this.env,
      "MAINTENANCE_INTERVAL_SECONDS",
      60,
      10,
      3_600,
    );
    const htmlIntervalSeconds = envNumber(
      this.env,
      "HTML_RECONCILIATION_INTERVAL_SECONDS",
      60,
      30,
      3_600,
    );
    const counts = this.sqlExec(
      `SELECT
        COUNT(*) AS tracked,
        SUM(CASE WHEN status = 'baseline' THEN 1 ELSE 0 END) AS baseline,
        SUM(CASE WHEN status = 'pending_enrichment' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN would_send_main = 1 THEN 1 ELSE 0 END) AS would_send_main,
        SUM(CASE WHEN would_send_canal2 = 1 THEN 1 ELSE 0 END) AS would_send_canal2,
        SUM(CASE WHEN main_sent_at <> '' THEN 1 ELSE 0 END) AS main_sent,
        SUM(CASE WHEN canal2_sent_at <> '' THEN 1 ELSE 0 END) AS canal2_sent,
        SUM(CASE WHEN discord_sent_at <> '' THEN 1 ELSE 0 END) AS discord_sent,
        SUM(CASE WHEN discord_image_cache_message_id <> '' THEN 1 ELSE 0 END)
          AS discord_offer_sent,
        SUM(CASE WHEN comment_sent_at <> '' THEN 1 ELSE 0 END) AS comments_sent,
        SUM(CASE WHEN status IN ('delivery_pending', 'partial_delivery') THEN 1 ELSE 0 END)
          AS delivery_pending,
        SUM(CASE WHEN status = 'delivery_dead_letter' THEN 1 ELSE 0 END) AS dead_letter,
        SUM(CASE WHEN status = 'delivery_unknown' THEN 1 ELSE 0 END) AS delivery_unknown,
        SUM(CASE WHEN status = 'delivery_blocked_configuration' THEN 1 ELSE 0 END)
          AS blocked_configuration,
        SUM(CASE WHEN status = 'delivery_quarantined' THEN 1 ELSE 0 END) AS quarantined,
        SUM(CASE WHEN status = 'restocked_pending_sync' THEN 1 ELSE 0 END) AS restock_pending,
        SUM(CASE WHEN status IN ('shadow_sold_out', 'sold_out') THEN 1 ELSE 0 END) AS sold_out,
        SUM(CASE WHEN main_delivery_error <> '' OR canal2_delivery_error <> ''
                  OR discord_delivery_error <> '' OR (
                    comment_delivery_error <> '' AND
                    comment_delivery_error <> 'historical_unknown_closed'
                  ) THEN 1 ELSE 0 END)
          AS delivery_errors
       FROM offers`,
    ).one();
    const lastRun = this.sqlExec(
      `SELECT started_at, finished_at, source, outcome, offers_seen, new_offers,
              enriched, would_send_main, would_send_canal2, sold_out_detected,
              main_sent, canal2_sent, delivery_failed,
              sold_out_main_edited, sold_out_canal2_edited, error
       FROM runs ORDER BY id DESC LIMIT 1`,
    ).toArray()[0] || null;
    const recentRuns = this.sqlExec(
      `SELECT started_at, finished_at, source, outcome, offers_seen, new_offers,
              enriched, would_send_main, would_send_canal2, sold_out_detected,
              main_sent, canal2_sent, delivery_failed,
              sold_out_main_edited, sold_out_canal2_edited, error
       FROM runs ORDER BY id DESC LIMIT 5`,
    ).toArray();
    const recent = this.sqlExec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              length(description) AS description_length, detail_error,
              detail_repair_attempts, detail_repair_error, detail_repaired_at,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at, main_sent_at, canal2_sent_at,
              main_message_id, main_message_kind, telegram_image_strategy,
              main_image_upgrade_attempts, main_image_upgrade_error,
              discord_message_id, discord_sent_at, discord_image_proxy_url,
              discord_image_cache_message_id,
              canal2_message_id,
              main_delivery_error, canal2_delivery_error,
              comment_sent_at, comment_chunks_sent, comment_delivery_error,
              main_sold_out_synced_at, canal2_sold_out_synced_at,
              main_sold_out_attempts, main_sold_out_error,
              canal2_sold_out_attempts, canal2_sold_out_error,
              COALESCE(tp.next_at, '') AS ticket_probe_next_at,
              COALESCE(tp.last_at, '') AS ticket_probe_last_at,
              COALESCE(tp.last_result, '') AS ticket_probe_last_result,
              COALESCE(tp.gone_count, 0) AS ticket_probe_gone_count,
              COALESCE(tp.attempts, 0) AS ticket_probe_attempts
       FROM offers AS o
       LEFT JOIN ticket_probe_state AS tp ON tp.offer_id = o.id
       WHERE status <> 'baseline'
       ORDER BY first_seen_at DESC
       LIMIT 8`,
    ).toArray().map(rowToPublicDecision);
    const latencyStartedAt = this.metadataValue("ops_monitor_started_at") ||
      new Date().toISOString();
    const latencyCutoff = [
      new Date(Date.now() - 24 * 60 * 60_000).toISOString(),
      latencyStartedAt,
    ].sort().at(-1);
    const latencyRows = this.sqlExec(
      `SELECT id, preview_title, title, first_seen_at, discord_sent_at,
              main_sent_at, canal2_sent_at, comment_sent_at
       FROM offers
       WHERE first_seen_at >= ?
         AND (main_sent_at <> '' OR discord_sent_at <> '')
       ORDER BY first_seen_at DESC
       LIMIT 100`,
      latencyCutoff,
    ).toArray();
    const incidents = this.sqlExec(
      `SELECT key, status, severity, summary, first_detected_at, last_detected_at,
              last_alerted_at, resolved_at, occurrence_count, alert_error
       FROM incidents ORDER BY last_detected_at DESC LIMIT 12`,
    ).toArray();
    const sourceComparison = this.getSourceComparison();
    const imageDelivery = this.getImageDeliveryHealth();
    const api = this.runtimeSnapshot("api");
    const apiContract = this.runtimeSnapshot("api_contract");
    const ticketListing = this.runtimeSnapshot("ticket_listing");
    const html = this.runtimeSnapshot("html");
    const webhook = this.runtimeSnapshot("webhook");
    const maintenance = this.runtimeSnapshot("maintenance");
    const beeperRuntime = this.runtimeSnapshot("beeper");
    const beeperConfiguration = beeperGatewayConfiguration(this.env);
    const beeperQueue = this.beeperQueueDiagnostics(
      envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50),
    );
    const usageBudget = estimateDailyRowWrites({
      pollIntervalSeconds: alarmIntervalSeconds,
      maintenanceIntervalSeconds,
      htmlIntervalSeconds,
      observationTouchMinutes: envNumber(
        this.env,
        "OFFER_LAST_SEEN_TOUCH_MINUTES",
        15,
        1,
        1_440,
      ),
      offerTouchMinutes: envNumber(
        this.env,
        "OFFER_LAST_SEEN_TOUCH_MINUTES",
        15,
        1,
        1_440,
      ),
      apiCards: Math.max(
        Number(api.lastOffersSeen || 0),
        Number(api.lastSuccessfulOffersSeen || 0),
      ),
      listingCards: Math.max(
        Number(html.mainLastOffersSeen || 0),
        Number(html.mainLastSuccessfulOffersSeen || 0),
      ) + Math.max(
        Number(html.ticketLastOffersSeen || 0),
        Number(html.ticketLastSuccessfulOffersSeen || 0),
      ),
    });
    const storageReadBudget = this.storageUsageSnapshot();

    return {
      ok: true,
      worker: "uol-telegram-shadow-pilot",
      version: {
        id: String(this.env.WORKER_VERSION?.id || ""),
        tag: String(this.env.WORKER_VERSION?.tag || ""),
        timestamp: String(this.env.WORKER_VERSION?.timestamp || ""),
      },
      mode: this.currentDeliveryMode(),
      telegram: telegramConfiguration(this.env),
      discord: discordConfiguration(this.env),
      beeper: {
        enabled: beeperConfiguration.enabled,
        configured: beeperConfiguration.configured,
        destinationKey: beeperConfiguration.destinationKey,
        filterActive: beeperConfiguration.filterActive,
        gatewayOk: typeof beeperRuntime.gatewayOk === "boolean"
          ? beeperRuntime.gatewayOk
          : null,
        gatewayStatus: Number(beeperRuntime.gatewayStatus || 0),
        gatewayCode: String(beeperRuntime.gatewayCode || ""),
        checkedAt: String(beeperRuntime.checkedAt || ""),
        deliveryConfirmation: "confirmed_by_whatsapp_bridge",
        queue: {
          pending: beeperQueue.pending,
          exhausted: beeperQueue.exhausted,
          unknown: beeperQueue.unknown,
          inFlight: beeperQueue.inFlight,
          nonTicket: beeperQueue.nonTicket,
          truncated: beeperQueue.truncated,
        },
      },
      ticketApi: {
        ...ticketApiConfiguration(this.env),
        publicationRole: "primary",
        htmlConfirmationRequiredBeforeSend: false,
        applicationAuthorizationExpiresAt: authorizationExpiresAt(
          this.env.UOL_API_AUTHORIZATION,
        ),
        lastOffersSeen: Number(
          api.lastOffersSeen ?? this.metadataValue("api_last_offers_seen") ?? 0,
        ),
        lastElapsedMs: Number(
          api.lastElapsedMs ?? this.metadataValue("api_last_elapsed_ms") ?? 0,
        ),
        lastError: api.lastError ?? this.metadataValue("api_last_error"),
        lastSuccessAt: api.lastSuccessAt || this.metadataValue("api_last_success_at"),
        contract: {
          ok: apiContract.ok ?? null,
          degraded: apiContract.degraded === true,
          reason: apiContract.reason || "",
          total: Number(apiContract.total || 0),
          valid: Number(apiContract.valid || 0),
          invalid: Number(apiContract.invalid || 0),
          fields: Array.isArray(apiContract.fields) ? apiContract.fields : [],
          lastCheckedAt: apiContract.lastCheckedAt || "",
        },
        fastPath: {
          lastCompletedAt: api.fastLastCompletedAt ||
            this.metadataValue("api_fast_last_completed_at"),
          lastElapsedMs: Number(
            api.fastLastElapsedMs ?? this.metadataValue("api_fast_last_elapsed_ms") ?? 0,
          ),
          lastNewOffers: Number(
            api.fastLastNewOffers ?? this.metadataValue("api_fast_last_new_offers") ?? 0,
          ),
          lastMainSent: Number(
            api.fastLastMainSent ?? this.metadataValue("api_fast_last_main_sent") ?? 0,
          ),
        },
      },
      publicTicketListing: {
        url: "/?categoria=ingressosexclusivos&order=new",
        publicationRole: "critical-fallback",
        pollingIntervalSeconds: alarmIntervalSeconds,
        reconciliationIntervalSeconds: htmlIntervalSeconds,
        lastReconciliationAt: html.lastCompletedAt ||
          this.metadataValue("html_reconciliation_last_completed_at"),
        lastOffersSeen: Number(
          ticketListing.lastOffersSeen ?? html.ticketLastOffersSeen ??
            this.metadataValue("ticket_listing_last_offers_seen") ?? 0,
        ),
        lastElapsedMs: Number(
          ticketListing.lastElapsedMs ?? html.ticketLastElapsedMs ??
            this.metadataValue("ticket_listing_last_elapsed_ms") ?? 0,
        ),
        lastSuccessAt: ticketListing.lastSuccessAt || html.ticketLastSuccessAt ||
          this.metadataValue("ticket_listing_last_success_at"),
        lastError: ticketListing.lastError ?? html.ticketLastError ??
          this.metadataValue("ticket_listing_last_error"),
        failureStreak: Number(ticketListing.failureStreak || 0),
      },
      publicMainListing: {
        url: "/?order=new",
        lastOffersSeen: Number(
          html.mainLastOffersSeen ?? this.metadataValue("main_listing_last_offers_seen") ?? 0,
        ),
        lastElapsedMs: Number(
          html.mainLastElapsedMs ?? this.metadataValue("main_listing_last_elapsed_ms") ?? 0,
        ),
        lastSuccessAt: html.mainLastSuccessAt ||
          this.metadataValue("main_listing_last_success_at"),
        lastError: html.mainLastError ?? this.metadataValue("main_listing_last_error"),
      },
      authentication: {
        mode: "application-bearer",
        personalAuthorizationRequired: false,
        passwordAutomationActive: false,
      },
      discussion: {
        configured: Boolean(String(this.env.GRUPO_COMENTARIO_ID || "").trim()),
        webhookRegistered: Boolean(
          webhook.registeredAt || this.metadataValue("telegram_webhook_registered_at"),
        ),
        webhookCheckedAt: webhook.checkedAt || this.metadataValue("telegram_webhook_checked_at"),
        webhookUrlMatches: webhook.urlMatches ??
          this.metadataValue("telegram_webhook_url_matches") === "true",
        pendingUpdates: Number(
          webhook.pendingUpdates ?? this.metadataValue("telegram_webhook_pending_updates") ?? 0,
        ),
        lastError: webhook.lastError ?? this.metadataValue("telegram_webhook_last_error"),
      },
      operations: {
        checkedAt: this.metadataValue("ops_health_checked_at"),
        error: this.metadataValue("ops_health_error"),
        apiFailureStreak: Number(
          api.failureStreak ?? this.metadataValue("api_failure_streak") ?? 0,
        ),
        failedRunStreak: this.failedRunStreak(),
        activeIncidents: incidents.filter((incident) => incident.status === "active").length,
        alertUsesMainFallback: telegramConfiguration(this.env).operationsUsesMainFallback,
        incidents,
      },
      latency: buildLatencyMetrics(latencyRows),
      sourceComparison,
      imageDelivery,
      usageEstimate: {
        alarmInvocationsPerDay: Math.ceil(86_400 / alarmIntervalSeconds),
        alarmInvocationsPer30Days: Math.ceil((86_400 * 30) / alarmIntervalSeconds),
        maintenanceInvocationsPerDay: Math.ceil(86_400 / maintenanceIntervalSeconds),
        apiRequestsPerScan: 1,
        htmlRequestsPerReconciliation: 2,
        detailAndDeliveryRequestsOnlyForNewOffers: true,
        durableObjectRowsWrittenPerDay: usageBudget,
        durableObjectRowsReadToday: storageReadBudget,
      },
      retention: {
        offerDays: envNumber(this.env, "OFFER_RETENTION_DAYS", 30, 7, 365),
        maxTerminalOffers: envNumber(this.env, "MAX_STATE_OFFERS", 300, 50, 2_000),
        recentRuns: 240,
        lastCleanupDay: this.metadataValue("offers_cleanup_day"),
      },
      schedule: {
        api: `durable-object-alarm:${alarmIntervalSeconds}s`,
        maintenance: `durable-object-alarm:${maintenanceIntervalSeconds}s`,
      },
      alarmScheduledAt,
      alarmRecovery: {
        lastRearmedAt: this.metadataValue("alarm_last_rearmed_at"),
        lastRearmedReason: this.metadataValue("alarm_last_rearmed_reason"),
      },
      maintenance: {
        lastStartedAt: maintenance.lastStartedAt ||
          this.metadataValue("maintenance_last_started_at"),
        lastCompletedAt: maintenance.lastCompletedAt ||
          this.metadataValue("maintenance_last_completed_at"),
        lastElapsedMs: Number(
          maintenance.lastElapsedMs ?? this.metadataValue("maintenance_last_elapsed_ms") ?? 0,
        ),
        lastError: maintenance.lastError ?? this.metadataValue("maintenance_last_error"),
        alarmLastEnsuredAt: this.metadataValue("maintenance_alarm_last_ensured_at"),
      },
      initializedAt: this.metadataValue("initialized_at"),
      liveStartedAt: this.metadataValue("live_started_at"),
      lastScanAt: api.lastCompletedAt || lastRun?.finished_at || "",
      counts: {
        tracked: Number(counts.tracked || 0),
        baseline: Number(counts.baseline || 0),
        pending: Number(counts.pending || 0),
        wouldSendMain: Number(counts.would_send_main || 0),
        wouldSendCanal2: Number(counts.would_send_canal2 || 0),
        mainSent: Number(counts.main_sent || 0),
        canal2Sent: Number(counts.canal2_sent || 0),
        discordSent: Number(counts.discord_sent || 0),
        discordOfferSent: Number(counts.discord_offer_sent || 0),
        commentsSent: Number(counts.comments_sent || 0),
        deliveryPending: Number(counts.delivery_pending || 0),
        deadLetter: Number(counts.dead_letter || 0),
        deliveryUnknown: Number(counts.delivery_unknown || 0),
        blockedConfiguration: Number(counts.blocked_configuration || 0),
        quarantined: Number(counts.quarantined || 0),
        restockPending: Number(counts.restock_pending || 0),
        deliveryErrors: Number(counts.delivery_errors || 0),
        soldOut: Number(counts.sold_out || 0),
        beeperPending: beeperQueue.pending,
        beeperExhausted: beeperQueue.exhausted,
        beeperUnknown: beeperQueue.unknown,
      },
      lastRun,
      recentRuns,
      recent,
    };
  }

  async getReadiness() {
    const now = Date.now();
    const intervalSeconds = envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600);
    const maintenanceIntervalSeconds = envNumber(
      this.env,
      "MAINTENANCE_INTERVAL_SECONDS",
      60,
      10,
      3_600,
    );
    const htmlIntervalSeconds = envNumber(
      this.env,
      "HTML_RECONCILIATION_INTERVAL_SECONDS",
      60,
      30,
      3_600,
    );
    // A public health check is also a safe recovery point after a deploy or a
    // transient alarm failure. It only writes when the primary alarm is
    // missing/overdue; normal checks remain read-only.
    const alarmScheduledAt = await this.ensureAlarm();
    const alarm = Date.parse(alarmScheduledAt);
    const alarmFresh = Number.isFinite(alarm) && alarm >= now - intervalSeconds * 2_000;
    if (
      this.readinessCache &&
      now - Number(this.readinessCache.cachedAt || 0) < READINESS_CACHE_TTL_MS
    ) {
      const mode = this.currentDeliveryMode();
      const checks = { ...this.readinessCache.value.checks, alarmFresh };
      return {
        ...this.readinessCache.value,
        ok: readinessChecksOk(mode, checks),
        mode,
        checks,
        checkedAt: new Date(now).toISOString(),
      };
    }
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const sourceSuccessTimes = [
      Date.parse(this.runtimeValue("api", "lastSuccessAt", "api_last_success_at")),
      Date.parse(this.runtimeValue("ticket_listing", "lastSuccessAt")),
    ].filter(Number.isFinite);
    const lastScanAt = sourceSuccessTimes.length ? Math.max(...sourceSuccessTimes) : Number.NaN;
    const scanFresh = Number.isFinite(lastScanAt) &&
      now - lastScanAt <= Math.max(120_000, intervalSeconds * 6_000);
    const lastMaintenanceAt = Date.parse(
      this.runtimeValue("maintenance", "lastCompletedAt", "maintenance_last_completed_at") || "",
    );
    const maintenanceRecentlyCompleted = Number.isFinite(lastMaintenanceAt) &&
      now - lastMaintenanceAt <= Math.max(120_000, maintenanceIntervalSeconds * 6_000);
    const incidents = this.sqlExec(
      `SELECT
         SUM(CASE WHEN status = 'active' AND severity = 'critical' THEN 1 ELSE 0 END)
           AS critical,
         SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active
       FROM incidents`,
    ).one();
    const queue = this.sqlExec(
      `SELECT
         SUM(CASE WHEN status = 'delivery_dead_letter' THEN 1 ELSE 0 END) AS dead_letter,
         SUM(CASE WHEN status = 'delivery_unknown' THEN 1 ELSE 0 END) AS unknown,
         SUM(CASE WHEN status = 'delivery_blocked_configuration' THEN 1 ELSE 0 END)
           AS blocked_configuration,
         SUM(CASE WHEN comment_delivery_unknown_at <> '' THEN 1 ELSE 0 END)
           AS comment_unknown,
         SUM(CASE WHEN
           status = 'restocked_pending_sync' AND (
             (main_restock_synced_at = '' AND main_restock_attempts >= ?) OR
             (
               would_send_canal2 = 1 AND canal2_message_id > 0 AND
               canal2_restock_synced_at = '' AND canal2_restock_attempts >= ?
             )
           ) THEN 1 ELSE 0 END) AS restock_dead_letter,
         SUM(CASE WHEN
           status = 'sold_out' AND (
             (main_sold_out_synced_at = '' AND main_sold_out_attempts >= ?) OR
             (
               would_send_canal2 = 1 AND canal2_message_id > 0 AND
               canal2_sold_out_synced_at = '' AND canal2_sold_out_attempts >= ?
             )
           ) THEN 1 ELSE 0 END) AS sold_out_dead_letter,
         SUM(CASE WHEN
           comment_sent_at = '' AND discussion_message_id > 0 AND
           comment_delivery_attempts >= ? AND
           comment_delivery_error <> 'historical_unknown_closed'
           THEN 1 ELSE 0 END) AS comment_dead_letter
       FROM offers`,
      maxAttempts,
      maxAttempts,
      maxAttempts,
      maxAttempts,
      maxAttempts,
    ).one();
    const telegram = telegramConfiguration(this.env);
    const beeperConfiguration = beeperGatewayConfiguration(this.env);
    const beeperRuntime = this.runtimeSnapshot("beeper");
    const beeperQueue = this.beeperQueueDiagnostics(maxAttempts);
    const configuration = deliveryConfiguration(
      this.env,
      telegram,
      discordConfiguration(this.env),
    );
    const mode = this.currentDeliveryMode();
    const modeLive = mode === "live";
    const beeperReady = !beeperConfiguration.enabled || (
      beeperConfiguration.configured && beeperRuntime.gatewayOk !== false
    );
    const deliveryConfigured = configuration.main.ready &&
      configuration.canal2.ready && configuration.discord.ready && beeperReady;
    const criticalIncidents = Number(incidents.critical || 0);
    const deadLetters = Number(queue.dead_letter || 0) +
      beeperQueue.exhausted;
    const unknown = Number(queue.unknown || 0) + Number(queue.comment_unknown || 0) +
      beeperQueue.unknown;
    const blockedConfiguration = Number(queue.blocked_configuration || 0) +
      Number(beeperConfiguration.enabled && !beeperConfiguration.configured);
    const maintenanceDeadLetters = Number(queue.restock_dead_letter || 0) +
      Number(queue.sold_out_dead_letter || 0) + Number(queue.comment_dead_letter || 0);
    const storageReadBudget = this.storageUsageSnapshot(new Date(now));
    const apiSnapshot = this.runtimeSnapshot("api");
    const htmlSnapshot = this.runtimeSnapshot("html");
    const observationTouchMinutes = envNumber(
      this.env,
      "OFFER_LAST_SEEN_TOUCH_MINUTES",
      15,
      1,
      1_440,
    );
    const storageWriteBudget = estimateDailyRowWrites({
      pollIntervalSeconds: intervalSeconds,
      maintenanceIntervalSeconds,
      htmlIntervalSeconds,
      observationTouchMinutes,
      offerTouchMinutes: observationTouchMinutes,
      apiCards: Math.max(
        Number(apiSnapshot.lastOffersSeen || 0),
        Number(apiSnapshot.lastSuccessfulOffersSeen || 0),
      ),
      listingCards: Math.max(
        Number(htmlSnapshot.mainLastOffersSeen || 0),
        Number(htmlSnapshot.mainLastSuccessfulOffersSeen || 0),
      ) + Math.max(
        Number(htmlSnapshot.ticketLastOffersSeen || 0),
        Number(htmlSnapshot.ticketLastSuccessfulOffersSeen || 0),
      ),
    });
    const maintenanceRetryAtTimestamp = Date.parse(
      this.runtimeValue("maintenance", "retryAt") || "",
    );
    const maintenanceDeferred = storageReadBudget.maintenanceAllowed === false &&
      Number.isFinite(maintenanceRetryAtTimestamp) && maintenanceRetryAtTimestamp > now;
    const maintenanceFresh = maintenanceRecentlyCompleted || maintenanceDeferred;
    const storageReadBudgetHealthy = storageReadBudget.withinFreeTier &&
      storageReadBudget.primaryAllowed;
    const storageWriteBudgetHealthy = storageWriteBudget.withinFreeTier &&
      Number(storageReadBudget.rowsWritten || 0) < storageWriteBudget.limit;
    const queueSlo = this.deliveryQueueSlo(new Date(now));
    const checks = {
      alarmFresh,
      scanFresh,
      maintenanceFresh,
      maintenanceDeferred,
      deliveryConfigured,
      criticalIncidents,
      deadLetters,
      unknown,
      blockedConfiguration,
      maintenanceDeadLetters,
      storageReadBudgetHealthy,
      storageWriteBudgetHealthy,
    };
    const result = {
      ok: readinessChecksOk(mode, checks),
      worker: "uol-telegram-shadow-pilot",
      versionId: String(this.env.WORKER_VERSION?.id || ""),
      mode,
      checks,
      beeper: {
        enabled: beeperConfiguration.enabled,
        configured: beeperConfiguration.configured,
        gatewayOk: typeof beeperRuntime.gatewayOk === "boolean"
          ? beeperRuntime.gatewayOk
          : null,
        gatewayStatus: Number(beeperRuntime.gatewayStatus || 0),
        gatewayCode: String(beeperRuntime.gatewayCode || ""),
        checkedAt: String(beeperRuntime.checkedAt || ""),
        filterActive: beeperConfiguration.filterActive,
        pending: beeperQueue.pending,
        exhausted: beeperQueue.exhausted,
        unknown: beeperQueue.unknown,
      },
      queueSlo,
      storageReadBudget,
      storageWriteBudget,
      lastScanAt: Number.isFinite(lastScanAt) ? new Date(lastScanAt).toISOString() : "",
      checkedAt: new Date(now).toISOString(),
    };
    this.readinessCache = { cachedAt: now, value: result };
    return result;
  }

  getPublicOffers(limit = 4) {
    const requestedLimit = Number(limit || 4);
    const boundedLimit = Number.isFinite(requestedLimit)
      ? Math.min(12, Math.max(1, Math.floor(requestedLimit)))
      : 4;
    const offers = this.sqlExec(
      `SELECT id, COALESCE(NULLIF(title, ''), preview_title) AS title, link,
              COALESCE(NULLIF(image_url, ''), NULLIF(card_image_url, ''), partner_image_url)
                AS image_url,
              partner_image_url, partner_name, first_seen_at, main_sent_at
       FROM offers
       WHERE main_sent_at <> '' AND sold_out_at = ''
       ORDER BY main_sent_at DESC
       LIMIT ?`,
      boundedLimit,
    ).toArray().map((row) => ({
      id: row.id,
      title: cleanText(row.title).slice(0, 280),
      link: String(row.link || ""),
      imageUrl: String(row.image_url || ""),
      partnerImageUrl: String(row.partner_image_url || ""),
      partner: cleanText(row.partner_name).slice(0, 120),
      observedAt: row.first_seen_at,
      sentAt: row.main_sent_at,
    }));
    return {
      ok: true,
      updatedAt: offers[0]?.sentAt || "",
      offers,
    };
  }

  deliveryTimelineForOffer(offerId, limit = 20) {
    const rows = this.sqlExec(
      `SELECT target, operation, state, attempt, generation,
              occurred_at, external_id, error, id
       FROM delivery_events
       WHERE offer_id = ?
       ORDER BY occurred_at DESC, id DESC
       LIMIT ?`,
      String(offerId || ""),
      Math.min(20, Math.max(1, Number(limit || 20))),
    ).toArray();
    return summarizeDeliveryTimeline(rows, limit);
  }

  async getDecisions(limit = 30) {
    const boundedLimit = Math.min(100, Math.max(1, Number(limit || 30)));
    return this.sqlExec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              length(description) AS description_length, detail_error,
              detail_repair_attempts, detail_repair_error, detail_repaired_at,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at, main_sent_at, canal2_sent_at,
              main_message_id, main_message_kind, telegram_image_strategy,
              main_image_upgrade_attempts, main_image_upgrade_error,
              discord_message_id, discord_sent_at, discord_image_proxy_url,
              canal2_message_id,
              main_delivery_error, canal2_delivery_error,
              comment_sent_at, comment_chunks_sent, comment_delivery_error,
              main_sold_out_synced_at, canal2_sold_out_synced_at,
              main_sold_out_attempts, main_sold_out_error,
              canal2_sold_out_attempts, canal2_sold_out_error,
              COALESCE(tp.next_at, '') AS ticket_probe_next_at,
              COALESCE(tp.last_at, '') AS ticket_probe_last_at,
              COALESCE(tp.last_result, '') AS ticket_probe_last_result,
              COALESCE(tp.gone_count, 0) AS ticket_probe_gone_count,
              COALESCE(tp.attempts, 0) AS ticket_probe_attempts
       FROM offers AS o
       LEFT JOIN ticket_probe_state AS tp ON tp.offer_id = o.id
       WHERE status <> 'baseline'
       ORDER BY first_seen_at DESC
       LIMIT ?`,
      boundedLimit,
    ).toArray().map((row) => ({
      ...rowToPublicDecision(row),
      deliveryTimeline: this.deliveryTimelineForOffer(row.id),
    }));
  }

  async getInventory(limit = 48) {
    const boundedLimit = Math.min(300, Math.max(1, Number(limit || 48)));
    return this.sqlExec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              length(description) AS description_length, detail_error,
              detail_repair_attempts, detail_repair_error, detail_repaired_at,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at, main_sent_at, canal2_sent_at,
              main_message_id, canal2_message_id,
              main_delivery_error, canal2_delivery_error,
              comment_sent_at, comment_chunks_sent, comment_delivery_error,
              main_sold_out_synced_at, canal2_sold_out_synced_at,
              main_sold_out_attempts, main_sold_out_error,
              canal2_sold_out_attempts, canal2_sold_out_error,
              COALESCE(tp.next_at, '') AS ticket_probe_next_at,
              COALESCE(tp.last_at, '') AS ticket_probe_last_at,
              COALESCE(tp.last_result, '') AS ticket_probe_last_result,
              COALESCE(tp.gone_count, 0) AS ticket_probe_gone_count,
              COALESCE(tp.attempts, 0) AS ticket_probe_attempts
       FROM offers AS o
       LEFT JOIN ticket_probe_state AS tp ON tp.offer_id = o.id
       ORDER BY first_seen_at DESC, id ASC
       LIMIT ?`,
      boundedLimit,
    ).toArray().map((row) => ({
      ...rowToPublicDecision(row),
      deliveryTimeline: this.deliveryTimelineForOffer(row.id),
    }));
  }

  getIdentityDiagnostics() {
    const rows = this.sqlExec(
      `SELECT id, link, status, main_sent_at, canal2_sent_at
       FROM offers
       WHERE status <> 'discarded'
       ORDER BY first_seen_at ASC`,
    ).toArray();
    const groups = new Map();
    for (const row of rows) {
      const sourceKey = offerSourceKey(row.link);
      if (!sourceKey) continue;
      const group = groups.get(sourceKey) || [];
      group.push({
        id: row.id,
        status: row.status,
        mainSent: Boolean(row.main_sent_at),
        canal2Sent: Boolean(row.canal2_sent_at),
      });
      groups.set(sourceKey, group);
    }
    const aliases = [...groups.entries()]
      .filter(([, group]) => group.length > 1)
      .map(([sourceKey, group]) => ({ sourceKey, offers: group }));
    return {
      tracked: rows.length,
      aliasGroups: aliases.length,
      aliases,
    };
  }

  repairIdentityAliases() {
    const reconciled = this.reconcileIdentityAliases();
    return {
      ok: true,
      reconciled,
      diagnostics: this.getIdentityDiagnostics(),
    };
  }
}

export class UolTelegramMaintenance extends DurableObject {
  maintenanceIntervalMs() {
    return envNumber(
      this.env,
      "MAINTENANCE_INTERVAL_SECONDS",
      60,
      10,
      3_600,
    ) * 1_000;
  }

  async ensureAlarm() {
    const interval = this.maintenanceIntervalMs();
    const now = Date.now();
    let alarm = await this.ctx.storage.getAlarm();
    if (alarm == null || alarm < now - interval * 2) {
      alarm = now + interval;
      await this.ctx.storage.setAlarm(alarm);
    }
    return new Date(alarm).toISOString();
  }

  async requestImmediate() {
    const target = Date.now() + 250;
    const alarm = await this.ctx.storage.getAlarm();
    if (alarm == null || alarm > target) await this.ctx.storage.setAlarm(target);
    return new Date(alarm == null || alarm > target ? target : alarm).toISOString();
  }

  async getStatus() {
    const alarm = await this.ctx.storage.getAlarm();
    return {
      ok: true,
      alarmScheduledAt: alarm == null ? "" : new Date(alarm).toISOString(),
      intervalSeconds: this.maintenanceIntervalMs() / 1_000,
    };
  }

  async alarm() {
    const cadenceTarget = Date.now() + this.maintenanceIntervalMs();
    const requestedAlarm = await this.ctx.storage.getAlarm();
    const nextAlarm = requestedAlarm == null
      ? cadenceTarget
      : Math.min(cadenceTarget, requestedAlarm);
    await this.ctx.storage.setAlarm(Math.max(Date.now() + 1_000, nextAlarm));
    try {
      const stub = this.env.UOL_TELEGRAM_SHADOW.getByName(INSTANCE_NAME);
      const result = await stub.runMaintenanceTick("alarm");
      const retryAt = Date.parse(String(result?.retryAt || ""));
      if (Number.isFinite(retryAt) && retryAt > Date.now() + 1_000) {
        await this.ctx.storage.setAlarm(retryAt);
      }
    } catch (error) {
      logEvent("error", "uol_telegram_maintenance_alarm_failed", {
        error: sanitizeError(error),
      });
    }
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    try {
      if (request.method === "GET" && url.pathname === "/livez") {
        return Response.json({
          ok: true,
          worker: "uol-telegram-shadow-pilot",
          versionId: String(env.WORKER_VERSION?.id || ""),
        }, {
          headers: {
            "Cache-Control": "public, max-age=30",
            "X-Robots-Tag": "noindex, nofollow",
          },
        });
      }
      const stub = env.UOL_TELEGRAM_SHADOW.getByName(INSTANCE_NAME);
      if (request.method === "GET" && url.pathname === "/offers") {
        const requestedLimit = Number(url.searchParams.get("limit") || 4);
        const limit = Number.isFinite(requestedLimit)
          ? Math.min(12, Math.max(1, Math.floor(requestedLimit)))
          : 4;
        const normalizedUrl = new URL("/offers", url.origin);
        normalizedUrl.searchParams.set("limit", String(limit));
        const cacheKey = new Request(normalizedUrl.href, { method: "GET" });
        const cached = await caches.default.match(cacheKey);
        if (cached) {
          if (request.headers.get("If-None-Match") === cached.headers.get("ETag")) {
            return new Response(null, {
              status: 304,
              headers: { ETag: cached.headers.get("ETag") || "" },
            });
          }
          return cached;
        }
        const payload = await stub.getPublicOffers(limit);
        const etagBytes = await crypto.subtle.digest(
          "SHA-256",
          new TextEncoder().encode(JSON.stringify(payload)),
        );
        const etag = `"${[...new Uint8Array(etagBytes)]
          .slice(0, 12)
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("")}"`;
        if (request.headers.get("If-None-Match") === etag) {
          return new Response(null, { status: 304, headers: { ETag: etag } });
        }
        const response = Response.json(payload, {
          headers: {
            "Cache-Control": "public, max-age=30, stale-while-revalidate=120",
            ETag: etag,
            "X-Robots-Tag": "noindex, nofollow",
          },
        });
        ctx.waitUntil(caches.default.put(cacheKey, response.clone()));
        return response;
      }
      if (request.method === "POST" && url.pathname === "/telegram-webhook") {
        const expected = String(env.TELEGRAM_WEBHOOK_SECRET || "").trim();
        const supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token") || "";
        if (!expected || !(await constantTimeEqual(expected, supplied))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.handleTelegramUpdate(await request.json()));
      }
      if (
        request.method === "GET" &&
        (url.pathname === "/health" || url.pathname === "/readyz")
      ) {
        await stub.ensureAlarm();
        const versionId = String(env.WORKER_VERSION?.id || "");
        const now = Date.now();
        let readiness = publicReadinessCache.versionId === versionId &&
          publicReadinessCache.expiresAt > now
          ? publicReadinessCache.payload
          : null;
        if (!readiness) {
          readiness = await stub.getReadiness();
          publicReadinessCache = {
            versionId,
            expiresAt: now + PUBLIC_READINESS_CACHE_TTL_MS,
            payload: readiness,
          };
        }
        return jsonResponse(readiness, readiness.ok ? 200 : 503);
      }
      if (request.method === "GET" && url.pathname === "/dashboard") {
        if (!(await isDashboardAuthorized(request, env))) return dashboardUnauthorized();
        return new Response(renderDashboard(await stub.getHealth()), {
          headers: {
            "Content-Type": "text/html; charset=UTF-8",
            "Cache-Control": "no-store",
            "X-Robots-Tag": "noindex, nofollow",
            "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
          },
        });
      }
      if (request.method === "GET" && url.pathname === "/dashboard.json") {
        if (!(await isDashboardAuthorized(request, env))) return dashboardUnauthorized();
        return jsonResponse(await stub.getHealth());
      }
      if (request.method === "POST" && url.pathname === "/run") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.runNow());
      }
      if (request.method === "POST" && url.pathname === "/maintenance") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.runMaintenanceTick("manual"));
      }
      if (request.method === "GET" && url.pathname === "/maintenance-status") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        const maintenance = env.UOL_TELEGRAM_MAINTENANCE
          .getByName(MAINTENANCE_INSTANCE_NAME);
        return jsonResponse(await maintenance.getStatus());
      }
      if (request.method === "POST" && url.pathname === "/mode") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return operatorActionResponse(async () => {
          const body = await readOperatorJson(request);
          return stub.setDeliveryMode(body?.mode);
        });
      }
      if (request.method === "POST" && url.pathname === "/requeue-delivery") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return operatorActionResponse(async () => {
          const body = await readOperatorJson(request);
          return stub.requeueDelivery(body?.id, body?.target);
        });
      }
      if (request.method === "POST" && url.pathname === "/resolve-delivery") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return operatorActionResponse(async () => {
          const body = await readOperatorJson(request);
          return stub.resolveDeliveryUnknown(
            body?.id,
            body?.target,
            body?.outcome,
            {
              messageId: body?.messageId,
              messageKind: body?.messageKind,
              messageIds: body?.messageIds,
              evidenceReference: body?.evidenceReference,
            },
          );
        });
      }
      if (request.method === "GET" && url.pathname === "/decisions") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse({
          ok: true,
          decisions: await stub.getDecisions(url.searchParams.get("limit")),
        });
      }
      if (request.method === "GET" && url.pathname === "/inventory") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse({
          ok: true,
          inventory: await stub.getInventory(url.searchParams.get("limit")),
        });
      }
      if (request.method === "GET" && url.pathname === "/identity-diagnostics") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse({
          ok: true,
          diagnostics: await stub.getIdentityDiagnostics(),
        });
      }
      if (request.method === "POST" && url.pathname === "/repair-identities") {
        if (!(await isAuthorized(request, env))) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.repairIdentityAliases());
      }
      return jsonResponse({ ok: false, error: "not_found" }, 404);
    } catch (error) {
      logEvent("error", "uol_telegram_shadow_request_failed", {
        path: url.pathname,
        method: request.method,
        error: sanitizeError(error),
      });
      return jsonResponse({ ok: false, error: "internal_error" }, 500);
    }
  },
};
