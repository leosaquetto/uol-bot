// Alternativa de imagem quando a `imageUrl` da API do Clube UOL responde 403.
// A página pública da oferta traz a arte vigente em `/beneficios/`; as outras
// imagens da página ficam em `/beneficios_categorias/` e não servem.
// Nada aqui grava no Durable Object: a URL é resolvida em memória a cada tentativa.

const PAGE_ORIGIN = "https://clube.uol.com.br";
// Sem User-Agent o Clube responde 405 (o fetch do Worker não manda nenhum por padrão).
export const OFFER_PAGE_USER_AGENT = "UOLTelegramCloudflare/1.0";
const MAX_PAGE_BYTES = 1024 * 1024;
const BENEFIT_IMAGE_PATTERN =
  /https:\/\/[a-z0-9-]+\.cloudfront\.net\/beneficios\/[A-Za-z0-9_-]+\.(?:png|jpe?g|webp|gif)(?![A-Za-z0-9_./-])/gi;
const OG_IMAGE_PATTERN =
  /<meta\b[^>]*\bproperty\s*=\s*["']og:image["'][^>]*>/gi;

/** Só páginas públicas do próprio Clube; nunca outro host, porta ou query. */
export function offerPageUrl(link) {
  const raw = String(link || "").trim();
  if (!raw) return "";
  try {
    const url = new URL(raw);
    if (url.origin !== PAGE_ORIGIN || url.username || url.password) return "";
    if (url.search || url.hash) return "";
    if (!/^\/[A-Za-z0-9-]+\/[A-Za-z0-9][A-Za-z0-9-]*\/?$/.test(url.pathname)) return "";
    return `${PAGE_ORIGIN}${url.pathname}`;
  } catch {
    return "";
  }
}

/**
 * Devolve a única imagem em `/beneficios/` da página. Havendo mais de uma,
 * só aceita a que também for a `og:image`; senão a página é ambígua.
 */
export function extractOfferPageImage(html, { excludeUrl = "" } = {}) {
  const text = String(html || "");
  const excluded = String(excludeUrl || "").trim();
  const found = [...new Set(text.match(BENEFIT_IMAGE_PATTERN) || [])]
    .filter((url) => url !== excluded);
  if (found.length === 1) return found[0];
  if (found.length === 0) return "";
  const ogImages = new Set();
  for (const tag of text.match(OG_IMAGE_PATTERN) || []) {
    for (const url of tag.match(BENEFIT_IMAGE_PATTERN) || []) ogImages.add(url);
  }
  const preferred = found.filter((url) => ogImages.has(url));
  return preferred.length === 1 ? preferred[0] : "";
}

async function boundedText(response) {
  if (Number(response.headers.get("Content-Length") || 0) > MAX_PAGE_BYTES) {
    await response.body?.cancel().catch(() => {});
    throw new Error("offer_page_too_large");
  }
  if (!response.body) throw new Error("offer_page_empty");
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_PAGE_BYTES) {
        await reader.cancel().catch(() => {});
        throw new Error("offer_page_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  // A página vem em ISO-8859-1; cada byte vira um caractere (as URLs são ASCII).
  let text = "";
  for (const chunk of chunks) {
    for (let start = 0; start < chunk.byteLength; start += 8_192) {
      text += String.fromCharCode(...chunk.subarray(start, start + 8_192));
    }
  }
  return text;
}

/** Busca a página da oferta e devolve a URL da arte, ou lança `offer_page_*`. */
export async function resolveOfferPageImage(
  link,
  { excludeUrl = "", timeoutMs = 6_000 } = {},
  fetchImpl = fetch,
) {
  const pageUrl = offerPageUrl(link);
  if (!pageUrl) throw new Error("offer_page_link_invalid");
  const response = await fetchImpl(pageUrl, {
    headers: { Accept: "text/html", "User-Agent": OFFER_PAGE_USER_AGENT },
    signal: AbortSignal.timeout(Math.max(1, timeoutMs)),
  });
  const finalUrl = String(response.url || "");
  if (finalUrl && !finalUrl.startsWith(`${PAGE_ORIGIN}/`)) {
    await response.body?.cancel().catch(() => {});
    throw new Error("offer_page_redirect_foreign");
  }
  if (!response.ok) {
    await response.body?.cancel().catch(() => {});
    throw new Error(`offer_page_http_${response.status}`);
  }
  const imageUrl = extractOfferPageImage(await boundedText(response), { excludeUrl });
  if (!imageUrl) throw new Error("offer_page_image_missing");
  return imageUrl;
}
