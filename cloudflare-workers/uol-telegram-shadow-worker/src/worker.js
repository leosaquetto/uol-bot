import { DurableObject } from "cloudflare:workers";

import {
  buildDedupeKeys,
  buildDiscussionCommentChunks,
  cleanText,
  decideShadowDelivery,
  dedupeCards,
  evaluateDetailQuality,
  extractValidity,
  isTicketCampaign,
  offerIdentityKeys,
  offerSourceKey,
} from "./core.js";
import {
  discordConfiguration,
  sendDiscordOffer,
} from "./discord.js";
import {
  editSoldOutMessage,
  editMainOfferMessage,
  forwardToCanal2,
  sendMainOffer,
  sendDiscussionComment,
  sendTransportTest,
  telegramConfiguration,
  registerTelegramWebhook,
} from "./telegram.js";

const BASE_URL = "https://clube.uol.com.br";
const LIST_URL = `${BASE_URL}/?order=new`;
const USER_AGENT = "Mozilla/5.0 (compatible; UOLTelegramCloudflare/1.0)";
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
  let validityText = "";
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
    .on(".descricao hr + p", {
      text(text) {
        validityText = appendBounded(validityText, text.text, 600);
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
  const validity = extractValidity(`${validityText} ${bodyText} ${description}`);
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
    mainSent: Boolean(row.main_sent_at),
    canal2Sent: Boolean(row.canal2_sent_at),
    mainDeliveryError: row.main_delivery_error || "",
    canal2DeliveryError: row.canal2_delivery_error || "",
    soldOutMainSynced: Boolean(row.main_sold_out_synced_at),
    soldOutCanal2Synced: Boolean(row.canal2_sold_out_synced_at),
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
    validity: row.validity,
    description: row.description,
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
    const currentVersion = Number(
      this.ctx.storage.sql
        .exec("SELECT COALESCE(MAX(id), 0) AS version FROM _sql_schema_migrations")
        .one().version || 0,
    );
    if (currentVersion < 2) {
      this.ctx.storage.sql.exec(`
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
      this.ctx.storage.sql.exec(`
        ALTER TABLE offers ADD COLUMN title_validity_key TEXT NOT NULL DEFAULT '';
        CREATE INDEX IF NOT EXISTS offers_title_validity_idx
          ON offers(title_validity_key, main_sent_at);
        INSERT INTO _sql_schema_migrations (id) VALUES (3);
      `);
    }
    if (currentVersion < 4) {
      this.ctx.storage.sql.exec(`
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

  currentDeliveryMode() {
    const override = this.metadataValue("delivery_mode_override");
    if (override === "live" || override === "shadow") return override;
    return deliveryMode(this.env);
  }

  setDeliveryMode(mode) {
    const normalized = String(mode || "").trim().toLowerCase();
    if (normalized !== "live" && normalized !== "shadow") {
      throw new Error("delivery_mode_invalid");
    }
    this.setMetadata("delivery_mode_override", normalized);
    return {
      ok: true,
      mode: normalized,
    };
  }

  async ensureAlarm() {
    let alarm = await this.ctx.storage.getAlarm();
    if (alarm == null) {
      alarm = Date.now() + envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600) * 1_000;
      await this.ctx.storage.setAlarm(alarm);
    }
    return new Date(alarm).toISOString();
  }

  async ensureTelegramWebhook() {
    if (this.metadataValue("telegram_webhook_registered_at")) return true;
    await registerTelegramWebhook(this.env);
    this.setMetadata("telegram_webhook_registered_at", new Date().toISOString());
    return true;
  }

  insertRun(run) {
    this.ctx.storage.sql.exec(
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
    const rows = this.ctx.storage.sql.exec(
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
      if (group.length < 2) continue;
      const keeper = this.chooseIdentityKeeper(group);
      for (const duplicate of group) {
        if (duplicate.id === keeper.id) continue;
        this.ctx.storage.sql.exec(
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
    return reconciled;
  }

  async backfillTitleValidityKeys() {
    const rows = this.ctx.storage.sql.exec(
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
      this.ctx.storage.sql.exec(
        "UPDATE offers SET title_validity_key = ? WHERE id = ?",
        keys.titleValidityKey,
        row.id,
      );
    }
    return rows.length;
  }

  resolveListingCards(cards, nowIso, newStatus) {
    const rows = this.ctx.storage.sql.exec(
      `SELECT id, link
       FROM offers
       WHERE status <> 'discarded'`,
    ).toArray();
    const byId = new Map(rows.map((row) => [row.id, row]));
    const byIdentity = new Map();
    for (const row of rows) {
      for (const key of offerIdentityKeys(row.link)) {
        if (!byIdentity.has(key)) byIdentity.set(key, row);
      }
    }

    const resolved = [];
    const resolvedIds = new Set();
    let inserted = 0;
    for (const card of cards) {
      let existing = byId.get(card.id);
      if (!existing) {
        for (const key of offerIdentityKeys(card.link)) {
          existing = byIdentity.get(key);
          if (existing) break;
        }
      }

      if (existing) {
        if (resolvedIds.has(existing.id)) continue;
        this.ctx.storage.sql.exec(
          `UPDATE offers SET
             link = ?, preview_title = ?, category = ?, card_image_url = ?,
             partner_image_url = ?, partner_name = ?,
             missing_since = '', absence_count = 0
           WHERE id = ?
             AND (
               link <> ? OR preview_title <> ? OR category <> ? OR
               card_image_url <> ? OR partner_image_url <> ? OR partner_name <> ? OR
               missing_since <> '' OR absence_count <> 0
             )`,
          card.link,
          card.previewTitle,
          card.category,
          card.cardImageUrl,
          card.partnerImageUrl,
          card.partnerName,
          existing.id,
          card.link,
          card.previewTitle,
          card.category,
          card.cardImageUrl,
          card.partnerImageUrl,
          card.partnerName,
        );
        resolved.push({ ...card, id: existing.id });
        resolvedIds.add(existing.id);
        continue;
      }

      this.insertCard(card, nowIso, newStatus);
      const row = { id: card.id, link: card.link };
      byId.set(row.id, row);
      for (const key of offerIdentityKeys(card.link)) byIdentity.set(key, row);
      resolved.push(card);
      resolvedIds.add(card.id);
      inserted += 1;
    }
    return { cards: resolved, inserted };
  }

  async processPending(cardsById, now, mode) {
    const batchSize = envNumber(this.env, "DETAIL_BATCH_SIZE", 4, 1, 8);
    const pendingRows = this.ctx.storage.sql.exec(
      `SELECT id, link, preview_title, category, card_image_url,
              partner_image_url, partner_name, detail_attempts,
              main_message_id, main_message_kind
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
      const resendThreshold = new Date(
        now.getTime() -
          envNumber(this.env, "RECENT_RESEND_BLOCK_HOURS", 168, 1, 720) * 3_600_000,
      ).toISOString();
      const duplicate = this.ctx.storage.sql.exec(
        `SELECT id FROM offers
         WHERE id <> ?
           AND (
             (dedupe_key <> '' AND dedupe_key = ?)
             OR (loose_dedupe_key <> '' AND loose_dedupe_key = ?)
             OR (
               title_validity_key <> ''
               AND title_validity_key = ?
               AND (
                 main_sent_at >= ?
                 OR status = 'baseline'
               )
             )
           )
         LIMIT 1`,
        offer.id,
        keys.dedupeKey,
        keys.looseDedupeKey,
        keys.titleValidityKey,
        resendThreshold,
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
      const status = decision.eligible
        ? (mode === "live" ? "delivery_pending" : "shadow_candidate")
        : "discarded";
      const decisionAt = now.toISOString();
      this.ctx.storage.sql.exec(
        `UPDATE offers SET
          title = ?, image_url = ?, validity = ?, description = ?,
          dedupe_key = ?, loose_dedupe_key = ?, title_validity_key = ?,
          detail_quality = ?,
          detail_attempts = ?, detail_error = ?, decision_at = ?,
          would_send_main = ?, would_send_canal2 = ?, discard_reason = ?,
          status = ?, delivery_mode = ?
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
        mode,
        offer.id,
      );
      if (decision.eligible && Number(pendingRows[index]?.main_message_id || 0) > 0) {
        try {
          await editMainOfferMessage(this.env, {
            messageId: Number(pendingRows[index].main_message_id),
            messageKind: pendingRows[index].main_message_kind,
            offer,
          });
        } catch (error) {
          logEvent("error", "uol_telegram_fast_post_enrichment_edit_failed", {
            offerId: offer.id,
            error: sanitizeError(error),
          });
        }
      }
      wouldSendMain += decision.wouldSendMain ? 1 : 0;
      wouldSendCanal2 += decision.wouldSendCanal2 ? 1 : 0;
    }

    return {
      enriched: enriched.length,
      wouldSendMain,
      wouldSendCanal2,
    };
  }

  async processFastTicketDeliveries(cards, now, mode) {
    if (mode !== "live") return { mainSent: 0, discordSent: 0, failed: 0 };
    const configuration = telegramConfiguration(this.env);
    if (!configuration.liveReady) throw new Error("telegram_live_configuration_incomplete");
    const discordReady = discordConfiguration(this.env).configured;
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    let mainSent = 0;
    let discordSent = 0;
    let failed = 0;
    for (const card of cards.filter((item) => isTicketCampaign(item))) {
      const row = this.ctx.storage.sql.exec(
        `SELECT * FROM offers
         WHERE id = ? AND status = 'pending_enrichment'
         LIMIT 1`,
        card.id,
      ).toArray()[0];
      if (!row) continue;
      const offer = rowToOffer(row);
      const deliveries = [];
      if (!row.main_sent_at && Number(row.main_delivery_attempts || 0) < maxAttempts) {
        this.ctx.storage.sql.exec(
          `UPDATE offers SET
            main_delivery_attempts = main_delivery_attempts + 1,
            main_delivery_error = '' WHERE id = ?`,
          row.id,
        );
        deliveries.push({ kind: "telegram", promise: sendMainOffer(this.env, offer) });
      }
      if (
        discordReady && !row.discord_sent_at &&
        Number(row.discord_delivery_attempts || 0) < maxAttempts
      ) {
        this.ctx.storage.sql.exec(
          `UPDATE offers SET
            discord_delivery_attempts = discord_delivery_attempts + 1,
            discord_delivery_error = '' WHERE id = ?`,
          row.id,
        );
        deliveries.push({ kind: "discord", promise: sendDiscordOffer(this.env, offer) });
      }
      const settled = await Promise.allSettled(deliveries.map((item) => item.promise));
      for (let index = 0; index < deliveries.length; index += 1) {
        const delivery = deliveries[index];
        const result = settled[index];
        if (delivery.kind === "telegram" && result.status === "fulfilled" && result.value.messageId) {
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              main_message_id = ?, main_message_kind = ?, main_sent_at = ?,
              main_delivery_error = '' WHERE id = ?`,
            result.value.messageId,
            result.value.messageKind,
            now.toISOString(),
            row.id,
          );
          mainSent += 1;
        } else if (delivery.kind === "discord" && result.status === "fulfilled") {
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              discord_message_id = ?, discord_sent_at = ?, discord_delivery_error = ''
             WHERE id = ?`,
            result.value.messageId,
            now.toISOString(),
            row.id,
          );
          discordSent += 1;
        } else {
          const column = delivery.kind === "telegram" ? "main_delivery_error" : "discord_delivery_error";
          const error = result.status === "rejected"
            ? sanitizeError(result.reason)
            : `${delivery.kind}_message_id_missing`;
          this.ctx.storage.sql.exec(`UPDATE offers SET ${column} = ? WHERE id = ?`, error, row.id);
          failed += 1;
        }
      }
    }
    return { mainSent, discordSent, failed };
  }

  async processDeliveries(now) {
    if (this.currentDeliveryMode() !== "live") {
      return { mainSent: 0, canal2Sent: 0, discordSent: 0, failed: 0 };
    }
    const configuration = telegramConfiguration(this.env);
    if (!configuration.liveReady) {
      throw new Error("telegram_live_configuration_incomplete");
    }
    const discordReady = discordConfiguration(this.env).configured;

    const batchSize = envNumber(this.env, "DELIVERY_BATCH_SIZE", 4, 1, 8);
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const rows = this.ctx.storage.sql.exec(
      `SELECT *
       FROM offers
       WHERE status IN ('delivery_pending', 'partial_delivery')
       ORDER BY first_seen_at ASC
       LIMIT ?`,
      batchSize,
    ).toArray();
    let mainSent = 0;
    let canal2Sent = 0;
    let discordSent = 0;
    let failed = 0;

    for (const row of rows) {
      const offer = rowToOffer(row);
      const ticket = isTicketCampaign(offer);
      let mainMessageId = Number(row.main_message_id || 0);
      let mainMessageKind = row.main_message_kind || "";
      let mainSentAt = row.main_sent_at || "";
      let canal2SentAt = row.canal2_sent_at || "";
      let discordSentAt = row.discord_sent_at || "";

      const deliveries = [];

      if (!mainSentAt && Number(row.main_delivery_attempts || 0) < maxAttempts) {
        const attempts = Number(row.main_delivery_attempts || 0) + 1;
        this.ctx.storage.sql.exec(
          `UPDATE offers
           SET main_delivery_attempts = ?, main_delivery_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        deliveries.push({ kind: "telegram", promise: sendMainOffer(this.env, offer) });
      }
      if (
        ticket && discordReady && !discordSentAt &&
        Number(row.discord_delivery_attempts || 0) < maxAttempts
      ) {
        const attempts = Number(row.discord_delivery_attempts || 0) + 1;
        this.ctx.storage.sql.exec(
          `UPDATE offers
           SET discord_delivery_attempts = ?, discord_delivery_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        deliveries.push({ kind: "discord", promise: sendDiscordOffer(this.env, offer) });
      }

      const results = await Promise.allSettled(deliveries.map((item) => item.promise));
      for (let index = 0; index < deliveries.length; index += 1) {
        const delivery = deliveries[index];
        const settled = results[index];
        if (delivery.kind === "telegram" && settled.status === "fulfilled") {
          const result = settled.value;
          if (!result.messageId) {
            this.ctx.storage.sql.exec(
              "UPDATE offers SET main_delivery_error = ? WHERE id = ?",
              "telegram_main_message_id_missing",
              row.id,
            );
            failed += 1;
            continue;
          }
          mainMessageId = result.messageId;
          mainMessageKind = result.messageKind;
          mainSentAt = now.toISOString();
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              main_message_id = ?, main_message_kind = ?, main_sent_at = ?,
              main_delivery_error = ''
             WHERE id = ?`,
            mainMessageId,
            mainMessageKind,
            mainSentAt,
            row.id,
          );
          mainSent += 1;
        } else if (delivery.kind === "telegram") {
          this.ctx.storage.sql.exec(
            "UPDATE offers SET main_delivery_error = ? WHERE id = ?",
            sanitizeError(settled.reason),
            row.id,
          );
          failed += 1;
        } else if (delivery.kind === "discord" && settled.status === "fulfilled") {
          discordSentAt = now.toISOString();
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              discord_message_id = ?, discord_sent_at = ?, discord_delivery_error = ''
             WHERE id = ?`,
            settled.value.messageId,
            discordSentAt,
            row.id,
          );
          discordSent += 1;
        } else {
          this.ctx.storage.sql.exec(
            "UPDATE offers SET discord_delivery_error = ? WHERE id = ?",
            sanitizeError(settled.reason),
            row.id,
          );
          failed += 1;
        }
      }

      if (
        mainSentAt &&
        Boolean(row.would_send_canal2) &&
        !canal2SentAt &&
        Number(row.canal2_delivery_attempts || 0) < maxAttempts
      ) {
        const attempts = Number(row.canal2_delivery_attempts || 0) + 1;
        this.ctx.storage.sql.exec(
          `UPDATE offers
           SET canal2_delivery_attempts = ?, canal2_delivery_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        try {
          const result = await forwardToCanal2(this.env, mainMessageId);
          if (!result.messageId) throw new Error("telegram_canal2_message_id_missing");
          canal2SentAt = now.toISOString();
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              canal2_message_id = ?, canal2_sent_at = ?, canal2_delivery_error = ''
             WHERE id = ?`,
            result.messageId,
            canal2SentAt,
            row.id,
          );
          canal2Sent += 1;
        } catch (error) {
          this.ctx.storage.sql.exec(
            "UPDATE offers SET canal2_delivery_error = ? WHERE id = ?",
            sanitizeError(error),
            row.id,
          );
          failed += 1;
        }
      }

      const complete = Boolean(
        mainSentAt &&
        (!Boolean(row.would_send_canal2) || canal2SentAt) &&
        (!ticket || !discordReady || discordSentAt),
      );
      this.ctx.storage.sql.exec(
        "UPDATE offers SET status = ? WHERE id = ?",
        complete ? "delivered" : "partial_delivery",
        row.id,
      );
    }

    return { mainSent, canal2Sent, discordSent, failed };
  }

  async processDiscussionComments(limit = 4) {
    if (this.currentDeliveryMode() !== "live") return { sent: 0, failed: 0 };
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const rows = this.ctx.storage.sql.exec(
      `SELECT * FROM offers
       WHERE discussion_message_id > 0
         AND comment_sent_at = ''
         AND description <> ''
         AND status <> 'discarded'
         AND comment_delivery_attempts < ?
       ORDER BY first_seen_at ASC
       LIMIT ?`,
      maxAttempts,
      Math.min(8, Math.max(1, Number(limit || 4))),
    ).toArray();
    let sent = 0;
    let failed = 0;
    for (const row of rows) {
      const chunks = buildDiscussionCommentChunks(rowToOffer(row));
      let sentCount = Number(row.comment_chunks_sent || 0);
      let messageIds = [];
      try {
        messageIds = JSON.parse(row.comment_message_ids || "[]");
      } catch {
        messageIds = [];
      }
      this.ctx.storage.sql.exec(
        `UPDATE offers SET
          comment_delivery_attempts = comment_delivery_attempts + 1,
          comment_delivery_error = ''
         WHERE id = ?`,
        row.id,
      );
      try {
        for (let index = sentCount; index < chunks.length; index += 1) {
          const result = await sendDiscussionComment(
            this.env,
            chunks[index],
            Number(row.discussion_message_id),
          );
          if (!result.messageId) throw new Error("telegram_comment_message_id_missing");
          messageIds.push(result.messageId);
          sentCount = index + 1;
          this.ctx.storage.sql.exec(
            `UPDATE offers SET comment_message_ids = ?, comment_chunks_sent = ? WHERE id = ?`,
            JSON.stringify(messageIds),
            sentCount,
            row.id,
          );
        }
        this.ctx.storage.sql.exec(
          `UPDATE offers SET comment_sent_at = ?, comment_delivery_error = '' WHERE id = ?`,
          new Date().toISOString(),
          row.id,
        );
        sent += 1;
      } catch (error) {
        this.ctx.storage.sql.exec(
          "UPDATE offers SET comment_delivery_error = ? WHERE id = ?",
          sanitizeError(error),
          row.id,
        );
        failed += 1;
      }
    }
    return { sent, failed };
  }

  async processSoldOutSync(now) {
    if (this.currentDeliveryMode() !== "live") {
      return { mainEdited: 0, canal2Edited: 0, failed: 0 };
    }
    const configuration = telegramConfiguration(this.env);
    if (!configuration.liveReady) {
      throw new Error("telegram_live_configuration_incomplete");
    }
    const maxAttempts = envNumber(this.env, "DELIVERY_MAX_ATTEMPTS", 10, 1, 50);
    const rows = this.ctx.storage.sql.exec(
      `SELECT *
       FROM offers
       WHERE status = 'sold_out'
         AND main_message_id > 0
         AND (
           main_sold_out_synced_at = ''
           OR (
             would_send_canal2 = 1
             AND canal2_message_id > 0
             AND canal2_sold_out_synced_at = ''
           )
         )
       ORDER BY sold_out_at ASC
       LIMIT 4`,
    ).toArray();
    let mainEdited = 0;
    let canal2Edited = 0;
    let failed = 0;

    for (const row of rows) {
      const offer = rowToOffer(row);
      if (
        !row.main_sold_out_synced_at &&
        Number(row.main_sold_out_attempts || 0) < maxAttempts
      ) {
        const attempts = Number(row.main_sold_out_attempts || 0) + 1;
        this.ctx.storage.sql.exec(
          `UPDATE offers SET
            main_sold_out_attempts = ?, main_sold_out_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        try {
          await editSoldOutMessage(this.env, {
            chatId: String(this.env.TELEGRAM_CHAT_ID || ""),
            messageId: row.main_message_id,
            messageKind: row.main_message_kind,
            offer,
            soldOutAt: row.sold_out_at || now.toISOString(),
          });
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              main_sold_out_synced_at = ?, main_sold_out_error = ''
             WHERE id = ?`,
            now.toISOString(),
            row.id,
          );
          mainEdited += 1;
        } catch (error) {
          this.ctx.storage.sql.exec(
            "UPDATE offers SET main_sold_out_error = ? WHERE id = ?",
            sanitizeError(error),
            row.id,
          );
          failed += 1;
        }
      }

      if (
        Boolean(row.would_send_canal2) &&
        Number(row.canal2_message_id || 0) > 0 &&
        !row.canal2_sold_out_synced_at &&
        Number(row.canal2_sold_out_attempts || 0) < maxAttempts
      ) {
        const attempts = Number(row.canal2_sold_out_attempts || 0) + 1;
        this.ctx.storage.sql.exec(
          `UPDATE offers SET
            canal2_sold_out_attempts = ?, canal2_sold_out_error = ''
           WHERE id = ?`,
          attempts,
          row.id,
        );
        try {
          await editSoldOutMessage(this.env, {
            chatId: String(this.env.CANAL2_ID || ""),
            messageId: row.canal2_message_id,
            messageKind: row.main_message_kind,
            offer,
            soldOutAt: row.sold_out_at || now.toISOString(),
          });
          this.ctx.storage.sql.exec(
            `UPDATE offers SET
              canal2_sold_out_synced_at = ?, canal2_sold_out_error = ''
             WHERE id = ?`,
            now.toISOString(),
            row.id,
          );
          canal2Edited += 1;
        } catch (error) {
          this.ctx.storage.sql.exec(
            "UPDATE offers SET canal2_sold_out_error = ? WHERE id = ?",
            sanitizeError(error),
            row.id,
          );
          failed += 1;
        }
      }
    }
    return { mainEdited, canal2Edited, failed };
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
      `SELECT id, status, missing_since, absence_count
       FROM offers
       WHERE status IN ('shadow_candidate', 'delivered', 'partial_delivery')
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
        const soldOutStatus = candidate.status === "shadow_candidate"
          ? "shadow_sold_out"
          : "sold_out";
        this.ctx.storage.sql.exec(
          "UPDATE offers SET sold_out_at = ?, status = ? WHERE id = ?",
          now.toISOString(),
          soldOutStatus,
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
    this.ctx.storage.sql.exec(
      `DELETE FROM offers
       WHERE first_seen_at < ?
         AND status IN ('baseline', 'discarded', 'delivered', 'shadow_sold_out', 'sold_out')
         ${activeClause}`,
      cutoff,
      ...active,
    );
    this.ctx.storage.sql.exec(
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
    this.setMetadata("offers_cleanup_day", cleanupDay);
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
      mainSent: 0,
      canal2Sent: 0,
      deliveryFailed: 0,
      soldOutMainEdited: 0,
      soldOutCanal2Edited: 0,
      discordSent: 0,
      commentsSent: 0,
      error: "",
    };

    try {
      const mode = this.currentDeliveryMode();
      if (mode === "live" && !this.metadataValue("live_started_at")) {
        this.setMetadata("live_started_at", startedAt.toISOString());
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
      let activeOfferIds = new Set(cards.map((card) => card.id));

      if (!initializedAt) {
        const resolution = this.resolveListingCards(cards, nowIso, "baseline");
        activeOfferIds = new Set(resolution.cards.map((card) => card.id));
        await this.backfillTitleValidityKeys();
        this.setMetadata("initialized_at", nowIso);
        run.outcome = "baseline_created";
      } else {
        const identityAliasesReconciled = this.reconcileIdentityAliases();
        await this.backfillTitleValidityKeys();
        const resolution = this.resolveListingCards(cards, nowIso, "pending_enrichment");
        activeOfferIds = new Set(resolution.cards.map((card) => card.id));
        run.newOffers = resolution.inserted;
        const cardsById = new Map(resolution.cards.map((card) => [card.id, card]));
        const fastDelivered = await this.processFastTicketDeliveries(resolution.cards, now, mode);
        run.mainSent += fastDelivered.mainSent;
        run.discordSent += fastDelivered.discordSent;
        run.deliveryFailed += fastDelivered.failed;
        const processed = await this.processPending(cardsById, now, mode);
        run.enriched = processed.enriched;
        run.wouldSendMain = processed.wouldSendMain;
        run.wouldSendCanal2 = processed.wouldSendCanal2;
        run.soldOutDetected = this.evaluateSoldOut(new Set(cardsById.keys()), now);
        const delivered = await this.processDeliveries(now);
        run.mainSent += delivered.mainSent;
        run.canal2Sent += delivered.canal2Sent;
        run.discordSent += delivered.discordSent;
        run.deliveryFailed += delivered.failed;
        const comments = await this.processDiscussionComments();
        run.commentsSent = comments.sent;
        run.deliveryFailed += comments.failed;
        const soldOutSync = await this.processSoldOutSync(now);
        run.soldOutMainEdited = soldOutSync.mainEdited;
        run.soldOutCanal2Edited = soldOutSync.canal2Edited;
        run.deliveryFailed += soldOutSync.failed;
        if (mode === "live" && (run.mainSent > 0 || run.canal2Sent > 0)) {
          run.outcome = "telegram_delivered";
        } else if (mode === "live" && run.deliveryFailed > 0) {
          run.outcome = "telegram_delivery_partial";
        } else if (run.newOffers > 0 || run.enriched > 0) {
          run.outcome = mode === "live"
            ? "live_decisions_recorded"
            : "shadow_decisions_recorded";
        } else {
          run.outcome = "no_change";
        }
        if (identityAliasesReconciled > 0 && run.outcome === "no_change") {
          run.outcome = "identity_aliases_reconciled";
        }
      }
      this.pruneOffers(now, activeOfferIds);
    } catch (error) {
      run.error = sanitizeError(error);
      run.outcome = "failed";
    } finally {
      run.finishedAt = new Date().toISOString();
      this.insertRun(run);
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
      mainSent: run.mainSent,
      canal2Sent: run.canal2Sent,
      discordSent: run.discordSent,
      commentsSent: run.commentsSent,
      deliveryFailed: run.deliveryFailed,
      soldOutMainEdited: run.soldOutMainEdited,
      soldOutCanal2Edited: run.soldOutCanal2Edited,
      error: run.error,
    });
    return {
      ok: !run.error,
      ...run,
    };
  }

  async alarm() {
    try {
      try {
        await this.ensureTelegramWebhook();
      } catch (error) {
        logEvent("error", "uol_telegram_webhook_registration_failed", {
          error: sanitizeError(error),
        });
      }
      await this.scan("alarm");
    } catch (error) {
      logEvent("error", "uol_telegram_shadow_alarm_unhandled", {
        error: sanitizeError(error),
      });
    } finally {
      const interval = envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600);
      await this.ctx.storage.setAlarm(Date.now() + interval * 1_000);
    }
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
    const rows = this.ctx.storage.sql.exec(
      "SELECT id FROM offers WHERE main_message_id = ? LIMIT 1",
      Number(origin.message_id),
    ).toArray();
    if (!rows[0]?.id) return { ok: true, matched: false };
    this.ctx.storage.sql.exec(
      `UPDATE offers SET discussion_message_id = ? WHERE id = ?`,
      Number(message.message_id),
      rows[0].id,
    );
    const comments = await this.processDiscussionComments(1);
    return { ok: true, matched: true, comments };
  }

  async runNow() {
    const result = await this.scan("manual");
    const alarmScheduledAt = await this.ensureAlarm();
    return {
      ...result,
      alarmScheduledAt,
    };
  }

  async testTransport() {
    const configuration = telegramConfiguration(this.env);
    if (!configuration.liveReady) {
      throw new Error("telegram_live_configuration_incomplete");
    }
    const result = await sendTransportTest(this.env);
    logEvent("info", "uol_telegram_transport_test", {
      mainOk: Boolean(result.mainMessageId),
      canal2Ok: Boolean(result.canal2MessageId),
    });
    return {
      ok: Boolean(result.mainMessageId && result.canal2MessageId),
      mainMessageId: result.mainMessageId,
      canal2MessageId: result.canal2MessageId,
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
        SUM(CASE WHEN main_sent_at <> '' THEN 1 ELSE 0 END) AS main_sent,
        SUM(CASE WHEN canal2_sent_at <> '' THEN 1 ELSE 0 END) AS canal2_sent,
        SUM(CASE WHEN discord_sent_at <> '' THEN 1 ELSE 0 END) AS discord_sent,
        SUM(CASE WHEN comment_sent_at <> '' THEN 1 ELSE 0 END) AS comments_sent,
        SUM(CASE WHEN status IN ('delivery_pending', 'partial_delivery') THEN 1 ELSE 0 END)
          AS delivery_pending,
        SUM(CASE WHEN status IN ('shadow_sold_out', 'sold_out') THEN 1 ELSE 0 END) AS sold_out,
        SUM(CASE WHEN main_delivery_error <> '' OR canal2_delivery_error <> ''
                  OR discord_delivery_error <> '' OR comment_delivery_error <> '' THEN 1 ELSE 0 END)
          AS delivery_errors
       FROM offers`,
    ).one();
    const lastRun = this.ctx.storage.sql.exec(
      `SELECT started_at, finished_at, source, outcome, offers_seen, new_offers,
              enriched, would_send_main, would_send_canal2, sold_out_detected,
              main_sent, canal2_sent, delivery_failed,
              sold_out_main_edited, sold_out_canal2_edited, error
       FROM runs ORDER BY id DESC LIMIT 1`,
    ).toArray()[0] || null;
    const recentRuns = this.ctx.storage.sql.exec(
      `SELECT started_at, finished_at, source, outcome, offers_seen, new_offers,
              enriched, would_send_main, would_send_canal2, sold_out_detected,
              main_sent, canal2_sent, delivery_failed,
              sold_out_main_edited, sold_out_canal2_edited, error
       FROM runs ORDER BY id DESC LIMIT 5`,
    ).toArray();
    const recent = this.ctx.storage.sql.exec(
      `SELECT id, link, preview_title, title, category, status, detail_quality,
              first_seen_at, decision_at, would_send_main, would_send_canal2,
              discard_reason, sold_out_at, main_sent_at, canal2_sent_at,
              main_delivery_error, canal2_delivery_error,
              main_sold_out_synced_at, canal2_sold_out_synced_at
       FROM offers
       WHERE status <> 'baseline'
       ORDER BY first_seen_at DESC
       LIMIT 8`,
    ).toArray().map(rowToPublicDecision);

    return {
      ok: true,
      worker: "uol-telegram-shadow-pilot",
      mode: this.currentDeliveryMode(),
      telegram: telegramConfiguration(this.env),
      discord: discordConfiguration(this.env),
      discussion: {
        configured: Boolean(String(this.env.GRUPO_COMENTARIO_ID || "").trim()),
        webhookRegistered: Boolean(this.metadataValue("telegram_webhook_registered_at")),
      },
      retention: {
        offerDays: envNumber(this.env, "OFFER_RETENTION_DAYS", 30, 7, 365),
        maxTerminalOffers: envNumber(this.env, "MAX_STATE_OFFERS", 300, 50, 2_000),
        recentRuns: 240,
        lastCleanupDay: this.metadataValue("offers_cleanup_day"),
      },
      schedule: `durable-object-alarm:${envNumber(this.env, "ALARM_INTERVAL_SECONDS", 15, 10, 3_600)}s`,
      alarmScheduledAt,
      initializedAt: this.metadataValue("initialized_at"),
      liveStartedAt: this.metadataValue("live_started_at"),
      lastScanAt: lastRun?.finished_at || "",
      counts: {
        tracked: Number(counts.tracked || 0),
        baseline: Number(counts.baseline || 0),
        pending: Number(counts.pending || 0),
        wouldSendMain: Number(counts.would_send_main || 0),
        wouldSendCanal2: Number(counts.would_send_canal2 || 0),
        mainSent: Number(counts.main_sent || 0),
        canal2Sent: Number(counts.canal2_sent || 0),
        discordSent: Number(counts.discord_sent || 0),
        commentsSent: Number(counts.comments_sent || 0),
        deliveryPending: Number(counts.delivery_pending || 0),
        deliveryErrors: Number(counts.delivery_errors || 0),
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
              discard_reason, sold_out_at, main_sent_at, canal2_sent_at,
              main_delivery_error, canal2_delivery_error,
              main_sold_out_synced_at, canal2_sold_out_synced_at
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
              discard_reason, sold_out_at, main_sent_at, canal2_sent_at,
              main_delivery_error, canal2_delivery_error,
              main_sold_out_synced_at, canal2_sold_out_synced_at
       FROM offers
       ORDER BY first_seen_at DESC, id ASC
       LIMIT ?`,
      boundedLimit,
    ).toArray().map(rowToPublicDecision);
  }

  getIdentityDiagnostics() {
    const rows = this.ctx.storage.sql.exec(
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const stub = env.UOL_TELEGRAM_SHADOW.getByName(INSTANCE_NAME);

    try {
      if (request.method === "POST" && url.pathname === "/telegram-webhook") {
        const expected = String(env.TELEGRAM_WEBHOOK_SECRET || "").trim();
        const supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token") || "";
        if (!expected || !constantTimeEqual(expected, supplied)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.handleTelegramUpdate(await request.json()));
      }
      if (request.method === "GET" && url.pathname === "/health") {
        return jsonResponse(await stub.getHealth());
      }
      if (request.method === "POST" && url.pathname === "/run") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.runNow());
      }
      if (request.method === "POST" && url.pathname === "/test") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse(await stub.testTransport());
      }
      if (request.method === "POST" && url.pathname === "/mode") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        const body = await request.json();
        return jsonResponse(await stub.setDeliveryMode(body?.mode));
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
      if (request.method === "GET" && url.pathname === "/identity-diagnostics") {
        if (!isAuthorized(request, env)) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
        return jsonResponse({
          ok: true,
          diagnostics: await stub.getIdentityDiagnostics(),
        });
      }
      if (request.method === "POST" && url.pathname === "/repair-identities") {
        if (!isAuthorized(request, env)) {
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
