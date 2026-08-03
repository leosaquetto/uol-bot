const LIST_URL = "https://clube.uol.com.br/?order=new";
const STATE_KEY = "state:v1";
const DEFAULT_MAX_STATE_OFFERS = 200;
const ALARM_INTERVAL_MS = 60 * 1000;
const USER_AGENT = "Mozilla/5.0 (compatible; UOLIngressosDiscordPilot/0.1)";

export function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

export function normalizeOfferId(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  let slug = raw;
  try {
    const url = new URL(raw, "https://clube.uol.com.br");
    slug = url.pathname.replace(/\/+$/, "").split("/").pop() || "";
  } catch {
    slug = raw.split("?")[0].split("#")[0].replace(/\/+$/, "").split("/").pop() || "";
  }

  try {
    slug = decodeURIComponent(slug);
  } catch {
    // Mantém o slug original quando houver escape inválido.
  }

  return slug
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function offerSourceKey(value) {
  try {
    const url = new URL(String(value || ""), "https://clube.uol.com.br");
    const parts = url.pathname.split("/").filter(Boolean);
    const partner = normalizeOfferId(parts.at(-2) || "");
    const slug = normalizeOfferId(parts.at(-1) || "");
    const code = slug.match(/^(p[a-z0-9]+)(?:-|$)/i)?.[1] || "";
    return partner && code ? `${partner}|${code.toLowerCase()}` : "";
  } catch {
    return "";
  }
}

export function isTicketCampaignLink(value) {
  try {
    const url = new URL(String(value || ""), "https://clube.uol.com.br");
    return url.hostname === "clube.uol.com.br" &&
      url.pathname.toLowerCase().includes("/campanhasdeingresso/");
  } catch {
    return false;
  }
}

export function normalizeOffer(raw) {
  if (!raw || !isTicketCampaignLink(raw.link)) return null;
  const link = new URL(raw.link, "https://clube.uol.com.br").href;
  const id = normalizeOfferId(link);
  if (!id) return null;

  return {
    id,
    sourceKey: offerSourceKey(link),
    title: cleanText(raw.title) || "Novo benefício de ingressos",
    link,
    category: cleanText(raw.category),
    imageUrl: String(raw.imageUrl || "").trim(),
  };
}

export function dedupeOffers(rawOffers) {
  const byId = new Map();
  for (const raw of rawOffers || []) {
    const offer = normalizeOffer(raw);
    if (!offer) continue;
    const key = offer.sourceKey || offer.id;
    if (byId.has(key)) continue;
    byId.set(key, offer);
  }
  return [...byId.values()];
}

function sourceKeyForEntry(key, entry) {
  return cleanText(entry?.sourceKey) || offerSourceKey(entry?.link) || `id|${entry?.id || key}`;
}

function statusRank(value) {
  return { sent: 4, baseline: 3, pending: 2, failed: 1 }[String(value || "")] || 0;
}

export function reconcileStateOffers(offers) {
  const groups = new Map();
  for (const [key, entry] of Object.entries(offers || {})) {
    const sourceKey = sourceKeyForEntry(key, entry);
    const current = groups.get(sourceKey);
    if (!current) {
      groups.set(sourceKey, { key, entry: { ...entry, sourceKey } });
      continue;
    }
    const currentRank = statusRank(current.entry?.status);
    const candidateRank = statusRank(entry?.status);
    const currentTime = String(current.entry?.firstSeenAt || "9999");
    const candidateTime = String(entry?.firstSeenAt || "9999");
    if (candidateRank > currentRank || (candidateRank === currentRank && candidateTime < currentTime)) {
      groups.set(sourceKey, { key, entry: { ...current.entry, ...entry, sourceKey } });
    }
  }
  return Object.fromEntries([...groups.values()].map(({ key, entry }) => [key, entry]));
}

function findStateEntry(offers, offer) {
  if (offers[offer.id]) return { key: offer.id, entry: offers[offer.id] };
  const sourceKey = offer.sourceKey || offerSourceKey(offer.link);
  if (!sourceKey) return null;
  for (const [key, entry] of Object.entries(offers)) {
    if (sourceKeyForEntry(key, entry) === sourceKey) return { key, entry };
  }
  return null;
}

export function createInitialState(nowIso) {
  return {
    version: 1,
    initializedAt: "",
    updatedAt: nowIso,
    offers: {},
  };
}

export function pruneStateOffers(offers, maxEntries = DEFAULT_MAX_STATE_OFFERS) {
  const entries = Object.entries(offers || {});
  entries.sort((a, b) => {
    const aTime = String(a[1]?.firstSeenAt || "");
    const bTime = String(b[1]?.firstSeenAt || "");
    return bTime.localeCompare(aTime);
  });
  return Object.fromEntries(entries.slice(0, Math.max(1, maxEntries)));
}

export function buildDiscordPayload(offer) {
  const embed = {
    title: offer.title,
    url: offer.link,
    color: 0xf5a623,
    description: "🎟️ Novo benefício na categoria de ingressos do Clube UOL.",
    fields: [
      {
        name: "Abrir oferta",
        value: `[Acessar agora no Clube UOL](${offer.link})`,
        inline: false,
      },
    ],
    footer: {
      text: "Clube UOL • monitor independente",
    },
    timestamp: new Date().toISOString(),
  };

  if (offer.imageUrl) embed.image = { url: offer.imageUrl };

  return {
    username: "Clube UOL • Ingressos",
    content: "🚨 **Novo benefício de ingressos disponível**",
    embeds: [embed],
    allowed_mentions: { parse: [] },
  };
}

export function deliveryMode(env) {
  return String(env.DELIVERY_MODE || "dry-run").trim().toLowerCase() === "live"
    ? "live"
    : "dry-run";
}

function cacheBustedListUrl(nowMs = Date.now()) {
  const url = new URL(LIST_URL);
  url.searchParams.set("_uol_worker_ts", String(nowMs));
  return url.href;
}

async function parseListingResponse(response) {
  const offers = [];
  let current = null;
  const ticketCard = 'div.beneficio[data-categoria="Ingressos Exclusivos"]';

  const rewriter = new HTMLRewriter()
    .on(ticketCard, {
      element(element) {
        current = {
          category: cleanText(element.getAttribute("data-categoria")),
          title: "",
          link: "",
          imageUrl: "",
        };
        offers.push(current);
      },
    })
    .on(`${ticketCard} a[href*="/campanhasdeingresso/"]`, {
      element(element) {
        if (!current || current.link) return;
        const href = element.getAttribute("href");
        if (isTicketCampaignLink(href)) {
          current.link = new URL(href, "https://clube.uol.com.br").href;
        }
      },
    })
    .on(`${ticketCard} p.titulo`, {
      text(text) {
        if (current) current.title += text.text;
      },
    })
    .on(`${ticketCard} .thumb[data-src]`, {
      element(element) {
        if (!current || current.imageUrl) return;
        current.imageUrl = String(element.getAttribute("data-src") || "").trim();
      },
    });

  await rewriter.transform(response).arrayBuffer();
  return dedupeOffers(offers);
}

export async function fetchTicketOffers(fetchImpl = fetch, nowMs = Date.now()) {
  const response = await fetchImpl(cacheBustedListUrl(nowMs), {
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
  });

  if (!response.ok) {
    throw new Error(`uol_list_http_${response.status}`);
  }

  return parseListingResponse(response);
}

async function loadState(env, nowIso) {
  const stored = await env.UOL_TICKETS_STATE.get(STATE_KEY, "json");
  if (!stored || typeof stored !== "object" || typeof stored.offers !== "object") {
    return createInitialState(nowIso);
  }
  return stored;
}

async function saveState(env, state, maxEntries) {
  const payload = {
    ...state,
    offers: pruneStateOffers(reconcileStateOffers(state.offers), maxEntries),
  };
  await env.UOL_TICKETS_STATE.put(STATE_KEY, JSON.stringify(payload));
  return payload;
}

async function sendDiscordWebhook(env, offer, fetchImpl = fetch) {
  const webhookUrl = String(env.DISCORD_WEBHOOK_URL || "").trim();
  if (!webhookUrl) throw new Error("discord_webhook_missing");

  const url = new URL(webhookUrl);
  url.searchParams.set("wait", "true");
  const response = await fetchImpl(url.href, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildDiscordPayload(offer)),
  });

  if (!response.ok) {
    const body = cleanText(await response.text()).slice(0, 240);
    throw new Error(`discord_http_${response.status}${body ? `:${body}` : ""}`);
  }

  const message = await response.json().catch(() => ({}));
  return String(message?.id || "");
}

function maxStateOffers(env) {
  const parsed = Number.parseInt(String(env.MAX_STATE_OFFERS || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_MAX_STATE_OFFERS;
}

export function collectorEnabled(env) {
  return String(env.COLLECTOR_ENABLED || "false").trim().toLowerCase() === "true";
}

export async function runCollector(env, options = {}) {
  const now = new Date();
  const nowIso = now.toISOString();
  const sendEnabled = options.sendEnabled ?? deliveryMode(env) === "live";
  const fetchImpl = options.fetchImpl || fetch;
  const offers = options.offers || await fetchTicketOffers(fetchImpl, now.getTime());
  let state = await loadState(env, nowIso);
  const reconciled = reconcileStateOffers(state.offers);
  let stateChanged = Object.keys(reconciled).length !== Object.keys(state.offers).length;
  state.offers = reconciled;

  if (!state.initializedAt) {
    for (const offer of offers) {
      state.offers[offer.id] = {
        ...offer,
        status: "baseline",
        firstSeenAt: nowIso,
        sentAt: "",
        discordMessageId: "",
        lastError: "",
      };
    }
    state.initializedAt = nowIso;
    state.updatedAt = nowIso;
    state = await saveState(env, state, maxStateOffers(env));
    const result = {
      outcome: "baseline_created",
      mode: deliveryMode(env),
      offersSeen: offers.length,
      newOffers: 0,
      sent: 0,
      failed: 0,
    };
    console.log(JSON.stringify({ event: "uol_ticket_scan", ...result }));
    return result;
  }

  const pending = [];
  for (const offer of offers) {
    const matched = findStateEntry(state.offers, offer);
    if (!matched) {
      state.offers[offer.id] = {
        ...offer,
        status: "pending",
        firstSeenAt: nowIso,
        sentAt: "",
        discordMessageId: "",
        lastError: "",
      };
      pending.push(offer);
      stateChanged = true;
      continue;
    }
    const { key, entry: previous } = matched;
    state.offers[key] = { ...previous, ...offer, id: previous.id || key };
    if (key !== offer.id || previous.link !== offer.link || previous.title !== offer.title) {
      stateChanged = true;
    }
    if (previous.status === "pending" || previous.status === "failed") {
      pending.push({ ...previous, ...offer, stateKey: key });
    }
  }

  if (stateChanged) {
    state.updatedAt = nowIso;
    state = await saveState(env, state, maxStateOffers(env));
  }

  let sent = 0;
  let failed = 0;
  if (sendEnabled) {
    for (const offer of pending) {
      const stateKey = offer.stateKey || offer.id;
      try {
        const messageId = await sendDiscordWebhook(env, offer, fetchImpl);
        state.offers[stateKey] = {
          ...state.offers[stateKey],
          status: "sent",
          sentAt: new Date().toISOString(),
          discordMessageId: messageId,
          lastError: "",
        };
        sent += 1;
      } catch (error) {
        state.offers[stateKey] = {
          ...state.offers[stateKey],
          status: "failed",
          lastError: cleanText(error?.message || error).slice(0, 240),
        };
        failed += 1;
      }
    }
  }

  if (sendEnabled && pending.length > 0) {
    state.updatedAt = new Date().toISOString();
    state = await saveState(env, state, maxStateOffers(env));
  }

  const result = {
    outcome: pending.length > 0 ? (sendEnabled ? "delivery_processed" : "new_offers_dry_run") : "no_change",
    mode: deliveryMode(env),
    offersSeen: offers.length,
    newOffers: pending.length,
    sent,
    failed,
  };
  console.log(JSON.stringify({ event: "uol_ticket_scan", ...result }));
  return result;
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function isAuthorized(request, env) {
  const expected = String(env.ADMIN_TOKEN || "").trim();
  if (!expected) return false;
  return request.headers.get("Authorization") === `Bearer ${expected}`;
}

async function healthPayload(env, alarmState) {
  const state = await loadState(env, new Date().toISOString());
  const entries = Object.values(state.offers || {});
  return {
    ok: true,
    worker: "uol-ingressos-discord-pilot",
    mode: deliveryMode(env),
    schedule: collectorEnabled(env) ? "durable-object-alarm:1m" : "disabled",
    alarmScheduledAt: alarmState.alarmScheduledAt || "",
    lastRun: alarmState.lastRun || null,
    initialized: Boolean(state.initializedAt),
    initializedAt: state.initializedAt || "",
    trackedOffers: entries.length,
    sentOffers: entries.filter((item) => item?.status === "sent").length,
    pendingOffers: entries.filter((item) => item?.status === "pending" || item?.status === "failed").length,
    webhookConfigured: Boolean(String(env.DISCORD_WEBHOOK_URL || "").trim()),
  };
}

export class TicketAlarm {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }

  async ensureAlarm() {
    if (!collectorEnabled(this.env)) {
      await this.ctx.storage.deleteAlarm();
      return "";
    }
    let scheduledAt = await this.ctx.storage.getAlarm();
    if (scheduledAt == null) {
      scheduledAt = Date.now() + ALARM_INTERVAL_MS;
      await this.ctx.storage.setAlarm(scheduledAt);
    }
    return new Date(scheduledAt).toISOString();
  }

  async alarm() {
    if (!collectorEnabled(this.env)) {
      await this.ctx.storage.deleteAlarm();
      await this.ctx.storage.put("lastRun", {
        at: new Date().toISOString(),
        ok: true,
        result: { outcome: "collector_disabled", sent: 0, failed: 0 },
      });
      return;
    }
    let record;
    try {
      const result = await runCollector(this.env);
      record = {
        at: new Date().toISOString(),
        ok: true,
        result,
      };
    } catch (error) {
      record = {
        at: new Date().toISOString(),
        ok: false,
        error: cleanText(error?.message || error).slice(0, 240),
      };
      console.error(JSON.stringify({ event: "uol_ticket_alarm_failed", ...record }));
    } finally {
      await this.ctx.storage.put("lastRun", record);
      await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health") {
      const alarm = await this.ctx.storage.getAlarm();
      const alarmScheduledAt = alarm == null ? "" : new Date(alarm).toISOString();
      const lastRun = await this.ctx.storage.get("lastRun");
      return jsonResponse(await healthPayload(this.env, { alarmScheduledAt, lastRun }));
    }

    if (request.method === "POST" && url.pathname === "/run") {
      if (!collectorEnabled(this.env)) {
        return jsonResponse({ ok: false, error: "collector_retired" }, 410);
      }
      if (!isAuthorized(request, this.env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, 401);
      }
      const result = await runCollector(this.env, { sendEnabled: url.searchParams.get("send") === "1" });
      const lastRun = { at: new Date().toISOString(), ok: true, manual: true, result };
      await this.ctx.storage.put("lastRun", lastRun);
      const alarmScheduledAt = await this.ensureAlarm();
      return jsonResponse({ ok: true, alarmScheduledAt, ...result });
    }

    if (request.method === "POST" && url.pathname === "/test") {
      if (!collectorEnabled(this.env)) {
        return jsonResponse({ ok: false, error: "collector_retired" }, 410);
      }
      if (!isAuthorized(request, this.env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, 401);
      }
      const discordMessageId = await sendDiscordWebhook(this.env, {
        title: "Teste do monitor de ingressos",
        link: LIST_URL,
        imageUrl: "",
      });
      return jsonResponse({ ok: true, discordMessageId });
    }

    return jsonResponse({ ok: false, error: "not_found" }, 404);
  }
}

async function handleFetch(request, env) {
  const url = new URL(request.url);
  if (url.pathname !== "/health" && url.pathname !== "/run" && url.pathname !== "/test") {
    return jsonResponse({ ok: false, error: "not_found" }, 404);
  }
  const id = env.TICKET_ALARM.idFromName("uol-ingressos-discord-pilot");
  return env.TICKET_ALARM.get(id).fetch(request);
}

export default {
  fetch: handleFetch,
};
