// Public, bounded projections of observations already collected by the Worker.
// Reading this catalog never discovers offers or changes a delivery/rescue state.
export const CATALOG_TTL_SECONDS = 120;
export const CATALOG_OFFER_LIMIT = 300;
export const CATALOG_STORY_LIMIT = 30;
export const CATALOG_READ_RESERVE = 700;

export const CATALOG_OFFERS_SQL = `SELECT id, link, preview_title, title, category,
  image_url, card_image_url, partner_image_url, partner_name, description, validity,
  first_seen_at, last_seen_at, sold_out_at, restocked_at, status
  FROM offers INDEXED BY offers_first_seen_idx
  ORDER BY first_seen_at DESC LIMIT ?`;
export const CATALOG_STORIES_SQL = `SELECT story_id, link, image_url,
  published_at, expires_at, first_seen_at
  FROM instagram_story_outbox INDEXED BY instagram_story_expiry_idx
  WHERE expires_at > ? ORDER BY expires_at DESC LIMIT ?`;

const encoder = new TextEncoder();
function text(value, maxBytes) {
  const plain = String(value || "").slice(0, maxBytes * 4)
    .replace(/<[^>]*>/g, "").replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ").trim();
  // Avoid cutting a multi-byte character while enforcing an actual byte limit.
  let size = 0, output = "";
  for (const char of plain) {
    size += encoder.encode(char).length;
    if (size > maxBytes) break;
    output += char;
  }
  return output;
}

function httpsUrl(value) {
  if (typeof value !== "string" || value.length > 4096 || value.includes("\\")) return null;
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return null;
    return url;
  } catch { return null; }
}

export function catalogCampaignUrl(value) {
  const url = httpsUrl(value);
  if (!url || url.hostname !== "clube.uol.com.br" ||
      !/^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9-]*\/?$/.test(url.pathname)) return "";
  // Campaign identity never includes query credentials, tracking, or fragments.
  return `${url.origin}${url.pathname.replace(/\/$/, "")}`;
}

export function catalogImageUrl(value) {
  const url = httpsUrl(value);
  if (!url) return "";
  const host = url.hostname;
  const allowed = ["uol.com.br", "imguol.com.br", "uol.com", "fbcdn.net", "cdninstagram.com"]
    .some(domain => host === domain || host.endsWith(`.${domain}`)) ||
    ["media.discordapp.net", "cdn.discordapp.com", "d310rdc9v8uuq.cloudfront.net"].includes(host);
  if (!allowed || /\/(?:api|webhooks|login|resgatar|resgate)(?:\/|$)/i.test(url.pathname) ||
      [...url.searchParams.keys()].some(key => /^(?:access_token|token|cookie|session|sessionid|authorization|password)$/i.test(key))) return "";
  url.hash = "";
  return url.href;
}

function iso(value) {
  if (typeof value !== "string" || !/^\d{4}-\d\d-\d\dT/.test(value)) return "";
  const at = Date.parse(value);
  return Number.isFinite(at) ? new Date(at).toISOString() : "";
}

function sourceStatus(snapshot, now) {
  const lastSuccessAt = iso(snapshot?.lastSuccessAt);
  const age = now - Date.parse(lastSuccessAt);
  return {
    lastSuccessAt,
    healthy: Number.isFinite(age) && age >= 0 && age <= 10 * 60_000 &&
      !snapshot?.lastError && !snapshot?.lastRunError && snapshot?.lastOutcome !== "failed" &&
      Number(snapshot?.failureStreak || 0) === 0 && Number(snapshot?.runFailureStreak || 0) === 0,
  };
}

export function catalogBudgetAllowed(budget) {
  if (budget?.maintenanceAllowed !== true) return false;
  const ceiling = Number(budget.maintenanceCeiling);
  const used = Number(budget.rowsRead);
  return Number.isFinite(ceiling) && Number.isFinite(used) && ceiling - used >= CATALOG_READ_RESERVE;
}

export function buildCatalog(offerRows, storyRows, { api = {}, ticketListing = {}, now = Date.now() } = {}) {
  const sources = { api: sourceStatus(api, now), ticketListing: sourceStatus(ticketListing, now) };
  const activeEvidence = new Map();
  for (const [source, cards] of [["api", api.healthCards], ["ticketListing", ticketListing.cards]]) {
    if (!sources[source].healthy || !Array.isArray(cards)) continue;
    for (const card of cards) {
      const link = catalogCampaignUrl(card?.link);
      const previous = activeEvidence.get(link);
      if (link && (!previous || sources[source].lastSuccessAt > previous.observedAt)) {
        activeEvidence.set(link, { state: "available", observedAt: sources[source].lastSuccessAt, source: source === "ticketListing" ? "ticket_listing" : source });
      }
    }
  }
  const portalLinks = new Set([
    ...(Array.isArray(api.healthCards) ? api.healthCards : []),
    ...(Array.isArray(ticketListing.cards) ? ticketListing.cards : []),
  ].map(card => catalogCampaignUrl(card?.link)).filter(Boolean));
  const offers = offerRows.slice(0, CATALOG_OFFER_LIMIT).flatMap(row => {
    const link = catalogCampaignUrl(row.link);
    if (!link || row.status === "discarded") return [];
    const soldOutAt = iso(row.sold_out_at), restockedAt = iso(row.restocked_at);
    const evidence = [activeEvidence.get(link),
      soldOutAt && { state: "sold_out", observedAt: soldOutAt, source: "worker" },
      restockedAt && { state: "available", observedAt: restockedAt, source: "worker" }]
      .filter(value => value && Date.parse(value.observedAt) <= now)
      .sort((a, b) => Date.parse(b.observedAt) - Date.parse(a.observedAt) || (a.state === "sold_out" ? -1 : 1))[0];
    return [{
      id: text(row.id, 160), link, title: text(row.title || row.preview_title, 280),
      category: text(row.category, 100),
      imageUrl: [row.image_url, row.card_image_url, row.partner_image_url].map(catalogImageUrl).find(Boolean) || "",
      partner: text(row.partner_name, 120), description: text(row.description, 8192),
      validity: text(row.validity, 500), firstSeenAt: iso(row.first_seen_at),
      lastSeenAt: iso(row.last_seen_at), soldOutAt, restockedAt,
      availability: evidence ? evidence.state === "sold_out" ? "sold_out" : "observed"
        : row.status === "sold_out" || row.status === "shadow_sold_out" || soldOutAt ? "sold_out" : "observed",
      isPortal: portalLinks.has(link),
      ...(evidence ? { availabilityEvidence: evidence } : {}),
    }];
  });
  const stories = storyRows.slice(0, CATALOG_STORY_LIMIT).flatMap(row => {
    const link = catalogCampaignUrl(row.link), expiresAt = iso(row.expires_at);
    if (!link || Date.parse(expiresAt) <= now || !expiresAt) return [];
    return [{ storyId: text(row.story_id, 100), link, imageUrl: catalogImageUrl(row.image_url),
      publishedAt: iso(row.published_at), expiresAt, firstSeenAt: iso(row.first_seen_at) }];
  });
  return {
    ok: true, schemaVersion: 1, offers, stories,
    coverage: { offerLimit: CATALOG_OFFER_LIMIT, storyLimit: CATALOG_STORY_LIMIT,
      truncated: offerRows.length > CATALOG_OFFER_LIMIT || storyRows.length > CATALOG_STORY_LIMIT,
      scope: "recent_tracked" },
    sources,
  };
}

function corsHeaders() {
  return { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "If-None-Match", "Access-Control-Expose-Headers": "ETag, X-Catalog-Rows-Read, X-Catalog-Cache, Retry-After",
    "X-Robots-Tag": "noindex, nofollow" };
}

function etagMatches(value, etag) {
  return String(value || "").split(",").some(part => part.trim() === "*" || part.trim().replace(/^W\//, "") === etag);
}

export function catalogCacheKey(request, versionId) {
  const url = new URL("/catalog", request.url);
  url.searchParams.set("version", String(versionId || "unversioned"));
  return new Request(url.href, { method: "GET" });
}

export async function serveCatalog(request, { versionId, getCatalog, cache, waitUntil }) {
  if (request.method === "OPTIONS") return new Response(null, { status: 204,
    headers: { ...corsHeaders(), "Access-Control-Max-Age": "86400" } });
  if (request.method !== "GET") return Response.json({ ok: false, error: "method_not_allowed" },
    { status: 405, headers: { ...corsHeaders(), Allow: "GET, OPTIONS", "Cache-Control": "no-store" } });
  const key = catalogCacheKey(request, versionId);
  let response = await cache.match(key);
  if (response) {
    response = new Response(response.body, response);
    response.headers.set("X-Catalog-Rows-Read", "0");
    response.headers.set("X-Catalog-Cache", "edge");
  } else {
    let result;
    try { result = await getCatalog(); }
    catch { result = { error: "catalog_unavailable", rowsRead: 0 }; }
    if (!result.payload) return Response.json({ ok: false, error: result.error || "catalog_unavailable" },
      { status: 503, headers: { ...corsHeaders(), "Cache-Control": "no-store", "Retry-After": "120",
        "X-Catalog-Rows-Read": String(result.rowsRead || 0) } });
    const body = JSON.stringify(result.payload);
    const digest = await crypto.subtle.digest("SHA-256", encoder.encode(body));
    const etag = `"${Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, "0")).join("")}"`;
    const maxAge = Math.max(0, Math.min(CATALOG_TTL_SECONDS, Math.floor(result.ttlSeconds)) || 0);
    response = new Response(body, { headers: { ...corsHeaders(), "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": `public, max-age=${maxAge}`, ETag: etag,
      "X-Catalog-Rows-Read": String(result.rowsRead || 0), "X-Catalog-Cache": result.cached ? "memory" : "miss" } });
    if (maxAge > 0) waitUntil(cache.put(key, response.clone()));
  }
  if (etagMatches(request.headers.get("If-None-Match"), response.headers.get("ETag"))) {
    return new Response(null, { status: 304, headers: response.headers });
  }
  return response;
}
