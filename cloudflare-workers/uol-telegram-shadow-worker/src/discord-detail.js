import { cleanText, extractValidity, offerIdentityKeys } from "./core.js";

const BASE_URL = "https://clube.uol.com.br";
const DETAIL_TIMEOUT_MS = 8_000;
const MAX_DETAIL_HTML_BYTES = 2_000_000;
const MAX_DESCRIPTION_LENGTH = 4_000;
const MIN_DESCRIPTION_LENGTH = 60;

function freshDetailUrl(value) {
  try {
    const url = new URL(String(value || ""), BASE_URL);
    url.searchParams.set("_uol_discord_detail_ts", String(Date.now()));
    return url.href;
  } catch {
    return "";
  }
}

function appendBounded(current, value, maxLength) {
  if (current.length >= maxLength) return current;
  return `${current} ${value}`.slice(0, maxLength);
}

function responseStayedOnOffer(requestedUrl, responseUrl) {
  if (!responseUrl) return true;
  const requestedKeys = new Set(offerIdentityKeys(requestedUrl));
  const finalKeys = offerIdentityKeys(responseUrl);
  return finalKeys.some((key) => requestedKeys.has(key));
}

function limitedBody(response, maxBytes) {
  if (!response.body) return null;
  let bytes = 0;
  const stream = response.body.pipeThrough(new TransformStream({
    transform(chunk, controller) {
      bytes += chunk.byteLength || chunk.length || 0;
      if (bytes > maxBytes) throw new Error("uol_detail_html_excede_limite");
      controller.enqueue(chunk);
    },
  }));
  return new Response(stream, { headers: response.headers });
}

async function drainRewriter(rewriter, response) {
  const transformed = rewriter.transform(response);
  if (transformed.body) await transformed.body.pipeTo(new WritableStream());
}

/**
 * Normalizes fragments collected by HTMLRewriter. Kept pure so the Discord
 * payload path can be tested without starting workerd on unsupported hosts.
 */
export function buildDiscordDetail({
  title = "",
  description = "",
  validityText = "",
  bodyText = "",
  metaDescription = "",
  fallbackTitle = "",
} = {}) {
  const normalizedDescription = cleanText(
    description || metaDescription || bodyText,
  ).slice(0, MAX_DESCRIPTION_LENGTH);
  if (normalizedDescription.length < MIN_DESCRIPTION_LENGTH) return null;
  return {
    title: cleanText(title) || cleanText(fallbackTitle),
    validity: extractValidity(`${validityText} ${bodyText} ${description}`),
    description: normalizedDescription,
  };
}

export function mergeDiscordOfferDetail(offer, detail) {
  if (!detail || !cleanText(detail.description)) return { ...offer };
  return {
    ...offer,
    ...(cleanText(detail.title) ? { title: cleanText(detail.title) } : {}),
    ...(cleanText(detail.validity) ? { validity: cleanText(detail.validity) } : {}),
    ...(cleanText(detail.description)
      ? { description: cleanText(detail.description).slice(0, MAX_DESCRIPTION_LENGTH) }
      : {}),
  };
}

async function parseDiscordDetail(response, fallbackTitle) {
  let h1 = "";
  let h2 = "";
  let description = "";
  let bodyText = "";
  let validityText = "";
  let metaDescription = "";
  const append = (current, value, limit) => appendBounded(current, value, limit);

  const rewriter = new HTMLRewriter()
    .on("h1", {
      text(text) {
        h1 = append(h1, text.text, 800);
      },
    })
    .on("h2", {
      text(text) {
        h2 = append(h2, text.text, 800);
      },
    })
    .on(".info-beneficio", {
      text(text) {
        description = append(description, text.text, 5_000);
      },
    })
    .on(".descricao p", {
      text(text) {
        validityText = append(validityText, text.text, 800);
      },
    })
    .on("body", {
      text(text) {
        bodyText = append(bodyText, text.text, 14_000);
      },
    })
    .on('meta[name="description"]', {
      element(element) {
        metaDescription ||= element.getAttribute("content") || "";
      },
    })
    .on('meta[property="og:description"]', {
      element(element) {
        metaDescription ||= element.getAttribute("content") || "";
      },
    });

  await drainRewriter(rewriter, response);
  return buildDiscordDetail({
    title: cleanText(h2) || cleanText(h1),
    description,
    validityText,
    bodyText,
    metaDescription,
    fallbackTitle,
  });
}

export async function fetchDiscordOfferDetail(offer, fetchImpl = fetch) {
  const requestedUrl = freshDetailUrl(offer?.link);
  if (!requestedUrl) return null;
  try {
    const response = await fetchImpl(requestedUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; Clube-UOL-Discord/1.0)",
        "Cache-Control": "no-cache, no-store, max-age=0",
        Pragma: "no-cache",
        Accept: "text/html,application/xhtml+xml",
      },
      cf: { cacheTtl: 0, cacheEverything: false },
      signal: AbortSignal.timeout(DETAIL_TIMEOUT_MS),
    });
    if (!response?.ok) return null;
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.toLowerCase().includes("text/html")) return null;
    const contentLength = Number(response.headers.get("content-length") || 0);
    if (contentLength > MAX_DETAIL_HTML_BYTES) return null;
    if (!responseStayedOnOffer(offer.link, response.url || "")) return null;
    const body = limitedBody(response, MAX_DETAIL_HTML_BYTES);
    if (!body) return null;
    return await parseDiscordDetail(body, offer?.title || offer?.previewTitle || "");
  } catch {
    return null;
  }
}
