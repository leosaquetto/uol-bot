// Ofertas de teatro costumam chegar em rajada: o mesmo espetáculo em várias datas,
// cada uma como uma oferta própria. Para não gerar uma mensagem por data, elas ficam
// retidas por alguns segundos e saem juntas, em uma única mensagem em lista.
// Shows (sem "teatro" no slug) nunca são retidos: o envio imediato importa mais.

const DEFAULT_HOLD_SECONDS = 90;
const MAX_HOLD_SECONDS = 300;
const DATE_PATTERN = /\b\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\b/g;

function slugOf(link) {
  const text = String(link || "").trim().toLowerCase();
  const marker = "/campanhasdeingresso/";
  const index = text.indexOf(marker);
  if (index < 0) return "";
  return text.slice(index + marker.length).split(/[?#]/)[0];
}

export function isTheaterLink(link) {
  return slugOf(link).includes("teatro");
}

export function theaterHoldSeconds(env = {}) {
  const raw = String(env?.THEATER_GROUP_HOLD_SECONDS ?? "").trim();
  if (!raw) return DEFAULT_HOLD_SECONDS;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) return DEFAULT_HOLD_SECONDS;
  return Math.min(MAX_HOLD_SECONDS, Math.floor(value));
}

export function theaterHoldUntil(row, env = {}) {
  const seconds = theaterHoldSeconds(env);
  if (!seconds || !isTheaterLink(row?.link)) return 0;
  const seen = Date.parse(String(row?.first_seen_at || ""));
  return Number.isFinite(seen) ? seen + seconds * 1_000 : 0;
}

export function isTheaterHeld(row, now = new Date(), env = {}) {
  const until = theaterHoldUntil(row, env);
  return until > 0 && now.getTime() < until;
}

export function theaterDateLabel(title) {
  const match = String(title || "").match(/\b\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\b/);
  return match ? match[0] : "";
}

export function theaterTitleWithoutDate(title) {
  return String(title || "")
    .replace(DATE_PATTERN, " ")
    .replace(/\s+/g, " ")
    .replace(/:\s+/g, ": ")
    .trim();
}

function normalizedImage(row) {
  const value = String(
    row?.image_url || row?.card_image_url || row?.partner_image_url || "",
  ).trim().toLowerCase();
  return value.split(/[?#]/)[0];
}

function slugWithoutDate(link) {
  return slugOf(link)
    .replace(/^p[a-z0-9]+-/, "")
    .replace(/\d{1,2}-\d{1,2}(?:-\d{2,4})?/g, "")
    .replace(/-+/g, "-");
}

// Mesmo título (sem a data) e mesma imagem = mesmo evento. Títulos iguais com
// imagens diferentes (ex.: duas atrações no mesmo local) ficam separados.
// Sem imagem, o slug sem data e sem código decide.
export function theaterGroupKey(row) {
  const title = theaterTitleWithoutDate(row?.title || row?.preview_title).toLowerCase();
  if (!title) return "";
  const image = normalizedImage(row);
  return image ? `${title}|img:${image}` : `${title}|slug:${slugWithoutDate(row?.link)}`;
}

export function parseGroupedOffers(value) {
  if (!value) return null;
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    const items = Array.isArray(parsed?.items)
      ? parsed.items.filter((item) => item && item.link)
      : [];
    if (items.length < 2) return null;
    return { title: String(parsed.title || ""), items };
  } catch {
    return null;
  }
}

export function buildGroupedOffers(rows) {
  const ordered = [...rows].sort((left, right) => (
    String(left.first_seen_at).localeCompare(String(right.first_seen_at)) ||
    String(left.id).localeCompare(String(right.id))
  ));
  return {
    title: theaterTitleWithoutDate(ordered[0]?.title || ordered[0]?.preview_title),
    items: ordered.map((row) => ({
      id: row.id,
      label: theaterDateLabel(row.title || row.preview_title),
      link: String(row.link || "").trim(),
    })),
  };
}

export function groupedItemLine(item) {
  return [item.label, item.link].filter(Boolean).join(" ➜ ");
}
