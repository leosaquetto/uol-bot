import { cleanText, evaluateDetailQuality, extractValidity, normalizeCard } from "./core.js";

const ORIGIN = "https://clube.uol.com.br";
const LEGACY_HOST = "clubeuol.clubeben.com.br";
const CODE_PATTERN = /^p[A-Za-z0-9]{2,5}$/;
const MAX_BYTES = 1024 * 1024;
const BANDS = ["abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789"];

function publicUrl(value, { legacy = false } = {}) {
  const raw = String(value || "").trim();
  if (!raw || /[\s\\%?#]/.test(raw)) return null;
  // URL normalisation must not silently erase an explicit port or path traversal.
  if (raw.split("/").some((segment) => segment === "." || segment === "..")) return null;
  if (/^(?:https:)?\/\/[^/]*:/.test(raw)) return null;
  try {
    const url = new URL(raw, ORIGIN);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return null;
    if (url.hostname !== "clube.uol.com.br" && !(legacy && url.hostname === LEGACY_HOST)) return null;
    return url;
  } catch {
    return null;
  }
}

/** Only public detail URLs; this function never accepts a redemption path. */
export function ticketCodeOfferUrl(value, code, { full = false, legacy = false } = {}) {
  if (!CODE_PATTERN.test(String(code || ""))) return "";
  const url = publicUrl(value, { legacy });
  if (!url) return "";
  const match = url.pathname.match(/^\/campanhasdeingresso\/(p[A-Za-z0-9]{2,5})(-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*)?$/);
  if (!match || match[1] !== code || (full && !match[2])) return "";
  return `${ORIGIN}${url.pathname}`;
}

function observedCode(card) {
  const raw = String(card?.link || "");
  // Anchors must come from explicit official HTTPS links, not inferred relative paths.
  if (!raw.startsWith(`${ORIGIN}/`)) return "";
  const url = publicUrl(raw);
  return url?.pathname.match(/^\/[A-Za-z0-9-]+\/(p[A-Za-z0-9]{2,5})-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$/)?.[1] || "";
}

/** Bounded gaps and neighbours around two recently observed, corroborated prefixes. */
export function buildTicketCodeCandidates(cards) {
  const seen = new Set();
  const groups = new Map();
  for (const card of cards || []) {
    const code = observedCode(card);
    if (!code || seen.has(code)) continue;
    seen.add(code);
    const prefix = code.slice(0, -1);
    if (!groups.has(prefix)) groups.set(prefix, []);
    groups.get(prefix).push(code.at(-1));
  }
  const selected = [...groups].filter(([, letters]) => letters.length >= 2).slice(0, 2);
  const gaps = [];
  const neighbours = [];
  for (const [prefix, letters] of selected) {
    const bands = [...new Set(letters.map((letter) => BANDS.find((band) => band.includes(letter))))];
    for (const band of bands) {
      const indices = letters.filter((letter) => band.includes(letter)).map((letter) => band.indexOf(letter));
      const ordered = [...indices].sort((a, b) => a - b);
      for (let index = 1; index < ordered.length; index += 1) {
        const lower = ordered[index - 1];
        const upper = ordered[index];
        if (upper - lower > 16) continue;
        for (let gap = lower + 1; gap < upper; gap += 1) gaps.push(`${prefix}${band[gap]}`);
      }
      for (const index of indices) {
        for (const delta of [1, 2, -1, -2]) {
          const letter = band[index + delta];
          if (letter) neighbours.push(`${prefix}${letter}`);
        }
      }
    }
  }
  return [...new Set([...gaps, ...neighbours])].filter((code) => !seen.has(code)).slice(0, 64);
}

function safeImage(value) {
  const raw = String(value || "").trim();
  if (!raw || /[\s\\]/.test(raw)) return "";
  try {
    const url = new URL(raw, ORIGIN);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return "";
    return url.href;
  } catch {
    return "";
  }
}

function expectedCta(value, code) {
  try {
    const url = new URL(String(value || ""), ORIGIN);
    if (url.origin !== ORIGIN || url.username || url.password || url.hash || url.port) return false;
    let path = url.pathname;
    if (path === "/auth/uol/login") {
      if ([...url.searchParams.keys()].some((key) => key !== "redirect_uri") || url.searchParams.getAll("redirect_uri").length !== 1) return false;
      path = url.searchParams.get("redirect_uri") || "";
    } else if (url.search) return false;
    if (!path.startsWith("/campanhasdeingresso/") || !path.endsWith("/resgatar")) return false;
    return Boolean(ticketCodeOfferUrl(path.slice(0, -"/resgatar".length), code));
  } catch {
    return false;
  }
}

async function boundedBody(response) {
  if (Number(response.headers.get("content-length") || 0) > MAX_BYTES) {
    await response.body?.cancel();
    throw new Error("body_too_large");
  }
  if (!response.body) throw new Error("empty_body");
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_BYTES) {
        await reader.cancel();
        throw new Error("body_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  if (!size) throw new Error("empty_body");
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new Response(bytes, { headers: { "content-type": "text/html; charset=utf-8" } });
}

function unknown(reason) {
  return { status: "unknown", reason };
}

/** Parses public HTML only. Its CTA is inspected as data and never fetched. */
export async function parseTicketCodePage(response, code, requestedUrl) {
  const expected = ticketCodeOfferUrl(requestedUrl, code);
  if (!expected || expected !== requestedUrl) return unknown("invalid_url");
  try {
    const body = await boundedBody(response);
    const page = { containers: 0, titles: 0, descriptions: 0, ctas: [], canonicals: [], ogUrls: [], aliases: [], images: [], title: "", description: "", ctaText: "", unavailable: false, disabled: false };
    const append = (key, limit) => ({ text(chunk) { page[key] += chunk.text.slice(0, Math.max(0, limit - page[key].length)); } });
    const alias = (value, legacy = false) => {
      const url = ticketCodeOfferUrl(value, code, { full: true, legacy });
      if (url) page.aliases.push(url);
    };
    const rewriter = new HTMLRewriter()
      .on("#beneficio", { element() { page.containers += 1; } })
      .on("#beneficio h2", { element() { page.titles += 1; }, ...append("title", 500) })
      .on("#beneficio .info-beneficio", { element() { page.descriptions += 1; }, ...append("description", 12_000) })
      .on("#beneficio .info-beneficio p, #beneficio .info-beneficio li, #beneficio .info-beneficio br", { element() { page.description += " "; } })
      .on("#beneficio #rescue_button", { element(element) {
        page.ctas.push(element.getAttribute("href") || "");
        page.disabled ||= element.getAttribute("disabled") !== null ||
          element.getAttribute("aria-disabled") === "true" || /(?:^|\s)disabled(?:\s|$)/.test(element.getAttribute("class") || "");
      }, ...append("ctaText", 300) })
      .on("#beneficio .esgotado, #beneficio .sold-out", { element() { page.unavailable = true; } })
      .on('link[rel="canonical"]', { element(element) { const value = element.getAttribute("href") || ""; page.canonicals.push(value); alias(value); } })
      .on('meta[property="og:url"]', { element(element) { const value = element.getAttribute("content") || ""; page.ogUrls.push(value); alias(value); } })
      .on('meta[property="og:image"]', { element(element) { page.images.push(safeImage(element.getAttribute("content"))); } })
      .on("#beneficio #ilustracoes img[src], #ilustracoes img[src]", { element(element) { page.images.push(safeImage(element.getAttribute("src"))); } })
      .on("#beneficio a[href]", { element(element) { alias(element.getAttribute("href")); } })
      .on(".fb-like[data-href]", { element(element) { alias(element.getAttribute("data-href"), true); } });
    const transformed = rewriter.transform(body);
    await transformed.body?.pipeTo(new WritableStream());
    if (page.containers !== 1 || page.titles !== 1 || page.descriptions !== 1 || page.canonicals.length !== 1 || page.ogUrls.length !== 1) return unknown("invalid_structure");
    if ([...page.canonicals, ...page.ogUrls].some((value) => ticketCodeOfferUrl(value, code) !== expected)) return unknown("identity_mismatch");
    const title = cleanText(page.title);
    const description = cleanText(page.description);
    if (title.length < 4 || description.length < 30) return unknown("incomplete_detail");
    if (page.unavailable || /\b(esgotad[oa]s?|indispon[ií]vel|encerrad[oa])\b/i.test(page.ctaText)) return { status: "absent", reason: "sold_out" };
    if (page.disabled) return unknown("cta_disabled");
    if (page.ctas.length !== 1 || !expectedCta(page.ctas[0], code)) return unknown("invalid_cta");
    const aliases = [...new Set(page.aliases)];
    if (aliases.length !== 1) return unknown(aliases.length ? "ambiguous_alias" : "missing_alias");
    const link = aliases[0];
    if (ticketCodeOfferUrl(expected, code, { full: true }) && link !== expected) return unknown("identity_mismatch");
    const imageUrl = page.images.find(Boolean) || "";
    const detail = { title, description: description.slice(0, 4_000), validity: extractValidity(description), imageUrl };
    const card = normalizeCard({ title, link, category: "campanhasdeingresso", cardImageUrl: imageUrl });
    if (!card) return unknown("invalid_card");
    return { status: "found", card: { ...card, apiDetail: { ...detail, quality: evaluateDetailQuality(detail) } } };
  } catch (error) {
    return unknown(["body_too_large", "empty_body"].includes(error?.message) ? error.message : "parse_failed");
  }
}

async function fetchPage(url, code, fetchImpl) {
  try {
    const response = await fetchImpl(url, {
      method: "GET",
      redirect: "manual",
      headers: { Accept: "text/html", "Cache-Control": "no-cache", "User-Agent": "UOLTelegramCloudflare/1.0" },
      cf: { cacheTtl: 0, cacheEverything: false },
      signal: AbortSignal.timeout(6_000),
    });
    if (response.status >= 300 && response.status < 400) {
      const destination = publicUrl(response.headers.get("location"));
      await response.body?.cancel();
      return destination && ["/", "/index.html"].includes(destination.pathname)
        ? { status: "absent", reason: "home_redirect" }
        : unknown("unexpected_redirect");
    }
    if (response.status === 404 || response.status === 410) {
      await response.body?.cancel();
      return { status: "absent", reason: "not_found" };
    }
    if (response.status !== 200) {
      await response.body?.cancel();
      return unknown(response.status === 429 ? "rate_limited" : response.status >= 500 ? "upstream_error" : "http_error");
    }
    if (response.redirected || (response.url && response.url !== url)) {
      await response.body?.cancel();
      return unknown("unexpected_url");
    }
    if (!/^text\/html(?:\s*;|$)/i.test(response.headers.get("content-type") || "")) {
      await response.body?.cancel();
      return unknown("non_html");
    }
    return await parseTicketCodePage(response, code, url);
  } catch {
    return unknown("network_or_timeout");
  }
}

export async function fetchTicketCodeOffer(code, fetchImpl = fetch) {
  if (!CODE_PATTERN.test(String(code || ""))) return { ...unknown("invalid_code"), requests: 0 };
  const first = await fetchPage(`${ORIGIN}/campanhasdeingresso/${code}`, code, fetchImpl);
  if (first.status !== "found") return { ...first, requests: 1 };
  const confirmed = await fetchPage(first.card.link, code, fetchImpl);
  return { ...confirmed, requests: 2 };
}
