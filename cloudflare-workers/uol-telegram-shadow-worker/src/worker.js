import { DurableObject } from "cloudflare:workers";

import {
  buildDedupeKeys,
  cleanText,
  decideShadowDelivery,
  dedupeCards,
  evaluateDetailQuality,
  extractValidity,
} from "./core.js";

const BASE_URL = "https://clube.uol.com.br";
const LIST_URL = `${BASE_URL}/?order=new`;
const USER_AGENT = "Mozilla/5.0 (compatible; UOLTelegramShadowPilot/0.1)";
const INSTANCE_NAME = "clube-uol-global-monitor";
const MAX_HTML_BYTES = 2_000_000;

function envNumber(env, name, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(env[name] || ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function sanitizeError(error) {
  return cleanText(error?.message || error).slice(0, 240);
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

function constantTimeEqual(left, right) {
  const encoder = new TextEncoder();
  const a = encoder.encode(String(left || ""));
  const b = encoder.encode(String(right || ""));
  const length = Math.max(a.length, b.length, 1);
  let difference = a.length ^ b.length;
  for (let index = 0; index < length; index += 1) {
    difference |= (a[index % Math.max(a.length, 1)] || 0) ^
      (b[index % Math.max(b.length, 1)] || 0);
  }
  return difference === 0;
}

function isAuthorized(request, env) {
  const expected = String(env.ADMIN_TOKEN || "").trim();
  const authorization = request.headers.get("Authorization") || "";
  const supplied = authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
  return Boolean(expected && supplied && constantTimeEqual(expected, supplied));
}

function freshUrl(value, marker) {
  const url = new URL(value);
  url.searchParams.set(marker, String(Date.now()));
  return url.href;
}

async function fetchHtml(url, marker, fetchImpl = fetch) {
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
    signal: AbortSignal.timeout(20_000),
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

async function fetchListing(fetchImpl = fetch) {
  const response = await fetchHtml(LIST_URL, "_uol_shadow_ts", fetchImpl);
  return parseListing(response);
}

function safeAbsoluteImage(value) {
  try {
    const url = new URL(String(value || ""), BASE_URL);
    if (url.protocol !== "https:") return "";
    return url.href;
  } catch {
    return "";
  }
}

function isBadOfferImage(value) {
  const url = String(value || "").toLowerCase();
  if (!url) return true;
  return [
    "/parceiros/",
    "loader.gif",
    "/categorias/",
    "ingressosexclusivos-hover",
    "ingressos-hover",
    "icone",
    "icon-",
  ].some((term) => url.includes(term));
}

async function parseDetail(response, card) {
  let h1 = "";
  let h2 = "";
  let description = "";
  let bodyText = "";
  const images = [];

  const appendBounded = (current, value, max) => {
    if (current.length >= max) return current;
    return `${current} ${value}`.slice(0, max);
  };

  const rewriter = new HTMLRewriter()
    .on("h1", {
      text(text) {
        h1 = appendBounded(h1, text.text, 800);
      },
    })
    .on("h2", {
      text(text) {
        h2 = appendBounded(h2, text.text, 800);
      },
    })
    .on(".info-beneficio", {
      text(text) {
        description = appendBounded(description, text.text, 5_000);
      },
    })
    .on("body", {
      text(text) {
        bodyText = appendBounded(bodyText, text.text, 14_000);
      },
    })
    .on('meta[property="og:image:secure_url"]', {
      element(element) {
        images.unshift(element.getAttribute("content") || "");
      },
    })
    .on('meta[property="og:image"]', {
      element(element) {
        images.push(element.getAttribute("content") || "");
      },
    })
    .on('meta[name="twitter:image"]', {
      element(element) {
        images.push(element.getAttribute("content") || "");
      },
    })
    .on('[data-src*="/beneficios/"]', {
      element(element) {
        images.push(element.getAttribute("data-src") || "");
      },
    })
    .on("img", {
      element(element) {
        images.push(
          element.getAttribute("data-src") ||
          element.getAttribute("data-original") ||
          element.getAttribute("src") ||
          "",
        );
      },
    });

  await drainRewriter(rewriter, response);
  const imageUrl = images
    .map(safeAbsoluteImage)
    .find((candidate) => candidate && !isBadOfferImage(candidate)) ||
    card.cardImageUrl ||
    "";
  const validity = extractValidity(`${bodyText} ${description}`);
  const detail = {
    title: cleanText(h2) || cleanText(h1) || card.previewTitle,
    validity,
    description: cleanText(description).slice(0, 4_000),
    imageUrl,
  };
  return {
    ...detail,
    quality: evaluateDetailQuality(detail),
  };
}

async function enrichCard(card, fetchImpl = fetch) {
  const startedAt = Date.now();
  try {
    const response = await fetchHtml(card.link, "_uol_shadow_detail_ts", fetchImpl);
    const detail = await parseDetail(response, card);
    return {
      ...card,
      ...detail,
      detailOk: true,
      detailError: "",
      detailElapsedMs: Date.now() - startedAt,
    };
  } catch (error) {
    return {
      ...card,
      title: card.previewTitle,
      validity: "",
      description: "",
      imageUrl: card.cardImageUrl,
      quality: "failed",
      detailOk: false,
      detailError: sanitizeError(error),
      detailElapsedMs: Date.now() - startedAt,
    };
  }
}

function rowToPublicDecision(row) {
  return {
    id: row.id,
    title: row.title || row.preview_title,
    path: new URL(row.link).pathname,
    category: row.category || "",
    status: row.status,
    detailQuality: row.detail_quality || "",
    firstSeenAt: row.first_seen_at,
    decisionAt: row.decision_at || "",
    wouldSendMain: Boolean(row.would_send_main),
    wouldSendCanal2: Boolean(row.would_send_canal2),
    discardReason: row.discard_reason || "",
    soldOutAt: row.sold_out_at || "",
  };
}

export class UolTelegramShadow extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.scanInFlight = false;
    ctx.blockConcurrencyWhile(async () => this.migrate());
  }

  migrate() {
    this.ctx.storage.sql.exec(`
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
  }

  metadataValue(key) {
    const rows = this.ctx.storage.sql
      .exec("SELECT value FROM metadata WHERE key = ?", key)
      .toArray();
    return rows[0]?.value || "";
  }

  setMetadata(key, value) {
    this.ctx.storage.sql.exec(
      `INSERT INTO metadata(key, value) VALUES (?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      key,
      String(value || ""),
    );
  }

  async ensureAlarm() {
    let alarm = await this.ctx.storage.getAlarm();
    if (alarm == null) {
      alarm = Date.now() + envNumber(this.env, "ALARM_INTERVAL_SECONDS", 60, 30, 3_600) * 1_000;
      await this.ctx.storage.setAlarm(alarm);
    }
    return new Date(alarm).toISOString();
  }

  insertRun(run) {
    this.ctx.storage.sql.exec(
      `INSERT INTO runs(
        started_at, finished_at, source, outcome, offers_seen, new_offers,
        enriched, would_send_main, would_send_canal2, sold_out_detected, error
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      run.error,
    );
    this.ctx.storage.sql.exec(
      "DELETE FROM runs WHERE id NOT IN (SELECT id FROM runs ORDER BY id DESC LIMIT 240)",
    );
  }

  insertCard(card, nowIso, status) {
    this.ctx.storage.sql.exec(
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
      nowIso,
      nowIso,
      status,
    );
  }

  async processPending(cardsById, now) {
    const batchSize = envNumber(this.env, "DETAIL_BATCH_SIZE", 4, 1, 8);
    const pendingRows = this.ctx.storage.sql.exec(
      `SELECT id, link, preview_title, category, card_image_url,
              partner_image_url, partner_name, detail_attempts
       FROM offers
       WHERE status = 'pending_enrichment'
       ORDER BY first_seen_at ASC
       LIMIT ?`,
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
    const enriched = await Promise.all(cards.map((card) => enrichCard(card)));
    let wouldSendMain = 0;
    let wouldSendCanal2 = 0;

    for (let index = 0; index < enriched.length; index += 1) {
      const offer = enriched[index];
      const previousAttempts = Number(pendingRows[index]?.detail_attempts || 0);
      const attempts = previousAttempts + 1;
      if (!offer.detailOk && attempts < 3) {
        this.ctx.storage.sql.exec(
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
      const duplicate = this.ctx.storage.sql.exec(
        `SELECT id FROM offers
         WHERE id <> ?
           AND (
             (dedupe_key <> '' AND dedupe_key = ?)
             OR (loose_dedupe_key <> '' AND loose_dedupe_key = ?)
           )
         LIMIT 1`,
        offer.id,
        keys.dedupeKey,
        keys.looseDedupeKey,
      ).toArray()[0];

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
      const status = decision.eligible ? "shadow_candidate" : "discarded";
      const decisionAt = now.toISOString();
      this.ctx.storage.sql.exec(
        `UPDATE offers SET
          title = ?, image_url = ?, validity = ?, description = ?,
          dedupe_key = ?, loose_dedupe_key = ?, detail_quality = ?,
          detail_attempts = ?, detail_error = ?, decision_at = ?,
          would_send_main = ?, would_send_canal2 = ?, discard_reason = ?,
          status = ?
         WHERE id = ?`,
        offer.title,
        offer.imageUrl,
        offer.validity,
        offer.description,
        keys.dedupeKey,
        keys.looseDedupeKey,
        offer.quality,
        attempts,
        offer.detailError,
        decisionAt,
        decision.wouldSendMain ? 1 : 0,
        decision.wouldSendCanal2 ? 1 : 0,
        decision.discardReason,
        status,
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

  evaluateSoldOut(activeIds, now) {
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
    const candidates = this.ctx.storage.sql.exec(
      `SELECT id, missing_since, absence_count
       FROM offers
       WHERE status = 'shadow_candidate'
         AND sold_out_at = ''
         AND decision_at >= ?`,
      threshold,
    ).toArray();
    let soldOutDetected = 0;

    for (const candidate of candidates) {
      if (activeIds.has(candidate.id)) {
        if (candidate.missing_since || Number(candidate.absence_count || 0) > 0) {
          this.ctx.storage.sql.exec(
            "UPDATE offers SET missing_since = '', absence_count = 0 WHERE id = ?",
            candidate.id,
          );
        }
        continue;
      }

      const previousCount = Number(candidate.absence_count || 0);
      if (!candidate.missing_since) {
        this.ctx.storage.sql.exec(
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
        this.ctx.storage.sql.exec(
          "UPDATE offers SET absence_count = ? WHERE id = ?",
          nextCount,
          candidate.id,
        );
      }
      if (nextCount >= minMisses && absentMinutes >= minAbsenceMinutes) {
        this.ctx.storage.sql.exec(
          "UPDATE offers SET sold_out_at = ?, status = 'shadow_sold_out' WHERE id = ?",
          now.toISOString(),
          candidate.id,
        );
        soldOutDetected += 1;
      }
    }
    return soldOutDetected;
  }

  pruneOffers() {
    const maxOffers = envNumber(this.env, "MAX_STATE_OFFERS", 300, 50, 2_000);
    this.ctx.storage.sql.exec(
      `DELETE FROM offers
       WHERE id NOT IN (
         SELECT id FROM offers ORDER BY first_seen_at DESC LIMIT ?
       )`,
      maxOffers,
    );
  }

  async scan(source = "alarm") {
    if (this.scanInFlight) {
      return {
        ok: false,
        outcome: "scan_in_progress",
        source,
      };
    }
    this.scanInFlight = true;
    const startedAt = new Date();
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
      error: "",
    };

    try {
      if (String(this.env.SHADOW_MODE || "") !== "1") {
        throw new Error("shadow_mode_guard_disabled");
      }
      const cards = await fetchListing();
      const minimum = envNumber(this.env, "MIN_HEALTHY_LISTING_OFFERS", 5, 1, 48);
      if (cards.length < minimum) {
        throw new Error(`uol_listing_suspeita_${cards.length}`);
      }
      run.offersSeen = cards.length;
      const now = new Date();
      const nowIso = now.toISOString();
      const initializedAt = this.metadataValue("initialized_at");

      if (!initializedAt) {
        for (const card of cards) this.insertCard(card, nowIso, "baseline");
        this.setMetadata("initialized_at", nowIso);
        run.outcome = "baseline_created";
      } else {
        const knownIds = new Set(
          this.ctx.storage.sql.exec("SELECT id FROM offers").toArray().map((row) => row.id),
        );
        for (const card of cards) {
          if (knownIds.has(card.id)) continue;
          this.insertCard(card, nowIso, "pending_enrichment");
          run.newOffers += 1;
        }

        const cardsById = new Map(cards.map((card) => [card.id, card]));
        const processed = await this.processPending(cardsById, now);
        run.enriched = processed.enriched;
        run.wouldSendMain = processed.wouldSendMain;
        run.wouldSendCanal2 = processed.wouldSendCanal2;
        run.soldOutDetected = this.evaluateSoldOut(new Set(cardsById.keys()), now);
        run.outcome = run.newOffers > 0 || run.enriched > 0
          ? "shadow_decisions_recorded"
          : "no_change";
      }
      this.pruneOffers();
    } catch (error) {
      run.error = sanitizeError(error);
      run.outcome = "failed";
    } finally {
      run.finishedAt = new Date().toISOString();
      this.insertRun(run);
      this.setMetadata("last_scan_at", run.finishedAt);
      this.scanInFlight = false;
    }

    logEvent(run.error ? "error" : "info", "uol_telegram_shadow_scan", {
      source: run.source,
      outcome: run.outcome,
      offersSeen: run.offersSeen,
      newOffers: run.newOffers,
      enriched: run.enriched,
      wouldSendMain: run.wouldSendMain,
      wouldSendCanal2: run.wouldSendCanal2,
      soldOutDetected: run.soldOutDetected,
      error: run.error,
    });
    return {
      ok: !run.error,
      ...run,
    };
  }

  async alarm() {
    try {
      await this.scan("alarm");
    } catch (error) {
      logEvent("error", "uol_telegram_shadow_alarm_unhandled", {
        error: sanitizeError(error),
      });
    } finally {
      const interval = envNumber(this.env, "ALARM_INTERVAL_SECONDS", 60, 30, 3_600);
      await this.ctx.storage.setAlarm(Date.now() + interval * 1_000);
    }
  }

  async runNow() {
    const result = await this.scan("manual");
    const alarmScheduledAt = await this.ensureAlarm();
    return {
      ...result,
      alarmScheduledAt,
    };
  }

  async getHealth() {
    const alarmScheduledAt = await this.ensureAlarm();
    const counts = this.ctx.storage.sql.exec(
      `SELECT
        COUNT(*) AS tracked,
        SUM(CASE WHEN status = 'baseline' THEN 1 ELSE 0 END) AS baseline,
        SUM(CASE WHEN status = 'pending_enrichment' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN would_send_main = 1 THEN 1 ELSE 0 END) AS would_send_main,
        SUM(CASE WHEN would_send_canal2 = 1 THEN 1 ELSE 0 END) AS would_send_canal2,
        SUM(CASE WHEN status = 'shadow_sold_out' THEN 1 ELSE 0 END) AS sold_out
       FROM offers`,
    ).one();
    const lastRun = this.ctx.storage.sql.exec(
      `SELECT started_at, finished_at, source, outcome, offers_seen, new_offers,
              enriched, would_send_main, would_send_canal2, sold_out_detected, error
       FROM runs ORDER BY id DESC LIMIT 1`,
    ).toArray()[0] || null;
    const recentRuns = this.ctx.storage.sql.exec(
      `SELECT started_at, finished_at, source, outcome, offers_seen, new_offers,
              enriched, would_send_main, would_send_canal2, sold_out_detected, error
       FROM runs ORDER BY id DESC LIMIT 5`,
    ).toArray();
    const recent = this.ctx.storage.sql.exec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at
       FROM offers
       WHERE status <> 'baseline'
       ORDER BY first_seen_at DESC
       LIMIT 8`,
    ).toArray().map(rowToPublicDecision);

    return {
      ok: true,
      worker: "uol-telegram-shadow-pilot",
      mode: "shadow-only",
      telegramConfigured: false,
      schedule: "durable-object-alarm:1m",
      alarmScheduledAt,
      initializedAt: this.metadataValue("initialized_at"),
      lastScanAt: this.metadataValue("last_scan_at"),
      counts: {
        tracked: Number(counts.tracked || 0),
        baseline: Number(counts.baseline || 0),
        pending: Number(counts.pending || 0),
        wouldSendMain: Number(counts.would_send_main || 0),
        wouldSendCanal2: Number(counts.would_send_canal2 || 0),
        soldOut: Number(counts.sold_out || 0),
      },
      lastRun,
      recentRuns,
      recent,
    };
  }

  async getDecisions(limit = 30) {
    const boundedLimit = Math.min(100, Math.max(1, Number(limit || 30)));
    return this.ctx.storage.sql.exec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at
       FROM offers
       WHERE status <> 'baseline'
       ORDER BY first_seen_at DESC
       LIMIT ?`,
      boundedLimit,
    ).toArray().map(rowToPublicDecision);
  }

  async getInventory(limit = 48) {
    const boundedLimit = Math.min(300, Math.max(1, Number(limit || 48)));
    return this.ctx.storage.sql.exec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at
       FROM offers
       ORDER BY first_seen_at DESC, id ASC
       LIMIT ?`,
      boundedLimit,
    ).toArray().map(rowToPublicDecision);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const stub = env.UOL_TELEGRAM_SHADOW.getByName(INSTANCE_NAME);

    try {
      if (request.method === "GET" && url.pathname === "/health") {
        return jsonResponse(await stub.getHealth());
      }
      if (request.method === "POST" && url.pathname === "/run") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.runNow());
      }
      if (request.method === "GET" && url.pathname === "/decisions") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse({
          ok: true,
          decisions: await stub.getDecisions(url.searchParams.get("limit")),
        });
      }
      if (request.method === "GET" && url.pathname === "/inventory") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse({
          ok: true,
          inventory: await stub.getInventory(url.searchParams.get("limit")),
        });
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
