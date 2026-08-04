const PUBLIC_HOST = "clube.uol.com.br";
const PUBLIC_BASE = `https://${PUBLIC_HOST}`;
const FIELD_SAMPLE_LIMIT = 12;
const FIELD_LIMIT = 32;

function publicOfferUrl(value) {
  if (!String(value || "").trim()) return false;
  try {
    const url = new URL(String(value || ""), PUBLIC_BASE);
    return url.protocol === "https:" && url.hostname === PUBLIC_HOST;
  } catch {
    return false;
  }
}

function validOffer(item) {
  if (!item || typeof item !== "object" || Array.isArray(item)) return false;
  const title = String(item.titulo || "").trim();
  return Boolean(title) && (publicOfferUrl(item.url) || publicOfferUrl(item.link));
}

function safeFields(items) {
  const fields = new Set();
  for (const item of items.slice(0, FIELD_SAMPLE_LIMIT)) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    for (const key of Object.keys(item)) {
      if (/^[A-Za-z0-9_]{1,64}$/.test(key)) fields.add(key);
    }
  }
  return [...fields].sort().slice(0, FIELD_LIMIT);
}

export function validateTicketApiPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return {
      ok: false,
      reason: "payload_invalid",
      total: 0,
      valid: 0,
      invalid: 0,
      fields: [],
    };
  }
  if (!Array.isArray(payload.beneficios)) {
    return {
      ok: false,
      reason: "beneficios_missing",
      total: 0,
      valid: 0,
      invalid: 0,
      fields: safeFields(Object.values(payload).filter(Array.isArray).flat()),
    };
  }
  const items = payload.beneficios;
  const valid = items.filter(validOffer).length;
  const result = {
    ok: valid > 0 || items.length === 0,
    reason: items.length === 0 ? "empty" : valid > 0 ? "ok" : "no_parseable_offers",
    total: items.length,
    valid,
    invalid: items.length - valid,
    fields: safeFields(items),
  };
  return result;
}
