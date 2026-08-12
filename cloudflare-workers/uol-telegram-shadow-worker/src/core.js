const BASE_URL = "https://clube.uol.com.br";
const DAY_SECONDS = 86_400;
const FREE_TIER_ROW_WRITES_PER_DAY = 100_000;
const FREE_TIER_ROW_READS_PER_DAY = 5_000_000;

export function parseRuntimeSnapshot(value) {
  try {
    const parsed = JSON.parse(String(value || "{}"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

export function shouldTouchObservation(lastSeenAt, observedAt, intervalMinutes = 15) {
  const previous = Date.parse(String(lastSeenAt || ""));
  const current = Date.parse(String(observedAt || ""));
  if (!Number.isFinite(previous) || !Number.isFinite(current)) return true;
  const intervalMs = Math.max(1, Number(intervalMinutes) || 15) * 60_000;
  return current - previous >= intervalMs;
}

export function observationFreshnessMinutes(touchMinutes = 15, graceMinutes = 5) {
  return Math.max(1, Number(touchMinutes) || 15) +
    Math.max(1, Number(graceMinutes) || 5);
}

export function shouldPersistRunSummary(run, lastPersistedRun, now, intervalMinutes = 15) {
  if (run?.error || String(run?.outcome || "") !== "no_change") return true;
  if (!lastPersistedRun) return true;
  if (
    lastPersistedRun.error ||
    String(lastPersistedRun.outcome || "") !== "no_change"
  ) return true;
  return shouldTouchObservation(
    lastPersistedRun.finishedAt || lastPersistedRun.finished_at,
    now,
    intervalMinutes,
  );
}

export function estimateDailyRowWrites({
  pollIntervalSeconds = 15,
  maintenanceIntervalSeconds = 60,
  htmlIntervalSeconds = 60,
  observationTouchMinutes = 15,
  offerTouchMinutes = 15,
  apiCards = 0,
  listingCards = 0,
  safetyReserve = 20_000,
} = {}) {
  const cycles = (seconds) => Math.ceil(DAY_SECONDS / Math.max(1, Number(seconds) || 1));
  const touches = (minutes) => Math.ceil(1_440 / Math.max(1, Number(minutes) || 1));
  const activeCards = Math.max(0, Number(apiCards) || 0) +
    Math.max(0, Number(listingCards) || 0);
  const components = {
    polling: cycles(pollIntervalSeconds) * 2,
    sampledRuns: cycles(15 * 60) * 2,
    maintenance: cycles(maintenanceIntervalSeconds) * 3,
    html: cycles(htmlIntervalSeconds) * 2,
    periodicChecks: cycles(300) * 2,
    sourceObservations: activeCards * touches(observationTouchMinutes),
    offerTouches: activeCards * touches(offerTouchMinutes),
    safetyReserve: Math.max(0, Number(safetyReserve) || 0),
  };
  const projected = Object.values(components).reduce((total, value) => total + value, 0);
  return {
    limit: FREE_TIER_ROW_WRITES_PER_DAY,
    projected,
    headroom: FREE_TIER_ROW_WRITES_PER_DAY - projected,
    withinFreeTier: projected < FREE_TIER_ROW_WRITES_PER_DAY,
    components,
  };
}

export function storageReadBudget({
  rowsRead = 0,
  primaryEstimatedRowsRead = 0,
  now = new Date(),
  pollIntervalSeconds = 15,
  limit = FREE_TIER_ROW_READS_PER_DAY,
  reserveFloor = 1_000_000,
} = {}) {
  const instant = now instanceof Date ? now : new Date(now);
  const nextUtcDay = Date.UTC(
    instant.getUTCFullYear(),
    instant.getUTCMonth(),
    instant.getUTCDate() + 1,
  );
  const remainingPrimaryScans = Math.ceil(
    Math.max(0, nextUtcDay - instant.getTime()) /
      (Math.max(1, Number(pollIntervalSeconds) || 15) * 1_000),
  );
  const observedPrimaryEstimate = Math.max(64, Number(primaryEstimatedRowsRead || 0));
  const criticalReserve = Math.max(
    Number(reserveFloor || 0),
    Math.ceil(observedPrimaryEstimate * remainingPrimaryScans * 1.5),
  );
  const normalizedLimit = Math.max(1, Number(limit) || FREE_TIER_ROW_READS_PER_DAY);
  const normalizedRowsRead = Math.max(0, Number(rowsRead) || 0);
  const remaining = Math.max(0, normalizedLimit - normalizedRowsRead);
  const remainingSeconds = Math.ceil(Math.max(0, nextUtcDay - instant.getTime()) / 1_000);
  const primaryRowsWithSafety = Math.max(1, Math.ceil(observedPrimaryEstimate * 1.5));
  const affordablePrimaryScans = Math.floor(remaining / primaryRowsWithSafety);
  const configuredPollIntervalSeconds = Math.max(
    1,
    Number(pollIntervalSeconds) || 15,
  );
  const recommendedPollIntervalSeconds = affordablePrimaryScans > 0
    ? Math.max(
        configuredPollIntervalSeconds,
        Math.ceil(remainingSeconds / affordablePrimaryScans),
      )
    : remainingSeconds + 1;
  const maintenanceCeiling = Math.max(0, normalizedLimit - criticalReserve);
  return {
    limit: normalizedLimit,
    rowsRead: normalizedRowsRead,
    remaining,
    resetAt: new Date(nextUtcDay).toISOString(),
    remainingPrimaryScans,
    observedPrimaryEstimate,
    primaryRowsWithSafety,
    affordablePrimaryScans,
    recommendedPollIntervalSeconds,
    primaryAllowed: affordablePrimaryScans > 0,
    criticalReserve,
    maintenanceCeiling,
    maintenanceAllowed: normalizedRowsRead < maintenanceCeiling,
    withinFreeTier: normalizedRowsRead < normalizedLimit,
  };
}

export function rollingReadEstimate(previous, observed) {
  const current = Math.max(0, Number(previous || 0));
  const sample = Math.max(0, Number(observed || 0));
  if (!current) return Math.min(512, Math.ceil(sample));
  const weight = sample > current ? 0.5 : 0.1;
  return Math.ceil(current * (1 - weight) + sample * weight);
}

export function maintenanceRetryAt({
  now = new Date(),
  resetAt = "",
  skipped = 0,
  baseMs = 60_000,
  maxMs = 15 * 60_000,
  deferUntilReset = false,
} = {}) {
  const instant = now instanceof Date ? now : new Date(now);
  const safeNow = Number.isNaN(instant.getTime()) ? new Date() : instant;
  const exponent = Math.min(4, Math.max(0, Number(skipped || 0)));
  const delayMs = Math.min(
    Math.max(1_000, Number(maxMs) || 15 * 60_000),
    Math.max(1_000, Number(baseMs) || 60_000) * (2 ** exponent),
  );
  const minimumNext = safeNow.getTime() + 1_000;
  const resetMs = Date.parse(String(resetAt || ""));
  const safeReset = Number.isFinite(resetMs) ? resetMs + 1_000 : Number.POSITIVE_INFINITY;
  const target = deferUntilReset && Number.isFinite(safeReset)
    ? safeReset
    : Math.min(safeNow.getTime() + delayMs, safeReset);
  return new Date(Math.max(minimumNext, target)).toISOString();
}

export function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function decodeRepeatedly(value) {
  let decoded = String(value || "");
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
    } catch {
      break;
    }
  }
  return decoded;
}

function repairMojibake(value) {
  const replacements = new Map([
    ["Ã¡", "á"],
    ["Ã ", "à"],
    ["Ã¢", "â"],
    ["Ã£", "ã"],
    ["Ã©", "é"],
    ["Ãª", "ê"],
    ["Ã­", "í"],
    ["Ã³", "ó"],
    ["Ã´", "ô"],
    ["Ãµ", "õ"],
    ["Ãº", "ú"],
    ["Ã§", "ç"],
    ["Ã‰", "É"],
    ["Ã‡", "Ç"],
  ]);
  let repaired = String(value || "");
  for (const [bad, good] of replacements) repaired = repaired.replaceAll(bad, good);
  return repaired;
}

function applyKnownCanonicalFixes(value) {
  let canonical = String(value || "");
  const knownFixes = new Map([
    ["ltima", "ultima"],
    ["ltimo", "ultimo"],
    ["seleo", "selecao"],
    ["graduao", "graduacao"],
    ["grtis", "gratis"],
    ["ms", "imas"],
    ["preo", "preco"],
  ]);
  for (const [bad, good] of knownFixes) {
    canonical = canonical.replace(
      new RegExp(`(^|-)${bad}(?=-|$)`, "g"),
      (_, prefix) => `${prefix}${good}`,
    );
  }
  canonical = canonical.replace(/(^|-)ps(?=-|$)/g, (_, prefix) => `${prefix}pos`);
  canonical = canonical.replaceAll("-at-", "-ate-");
  return canonical.replace(/-+/g, "-").replace(/^-+|-+$/g, "");
}

export function canonicalKey(value) {
  let raw = cleanText(value);
  if (!raw) return "";
  raw = repairMojibake(decodeRepeatedly(raw))
    .replaceAll("\u00a0", " ")
    .toLowerCase()
    .split("?")[0]
    .split("#")[0]
    .replaceAll("&", " e ")
    .replaceAll("º", "o")
    .replaceAll("ª", "a")
    .replace(/[\s_]+/g, "-")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
  return applyKnownCanonicalFixes(raw);
}

export function normalizeText(value) {
  return canonicalKey(value).replaceAll("-", " ");
}

function rawOfferTail(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  let tail = raw;
  try {
    const url = new URL(raw, BASE_URL);
    tail = url.pathname.replace(/\/+$/, "").split("/").pop() || "";
  } catch {
    tail = raw.split("?")[0].split("#")[0].replace(/\/+$/, "").split("/").pop() || "";
  }

  return decodeRepeatedly(tail);
}

export function slugTailVariants(value) {
  const base = canonicalKey(rawOfferTail(value));
  if (!base) return [];
  const variants = new Set([base, base.replaceAll("-de-", "-")]);
  if (base.includes("joo")) variants.add(base.replaceAll("joo", "joao"));
  if (base.includes("joao")) variants.add(base.replaceAll("joao", "joo"));
  return [...variants].filter(Boolean).sort();
}

export function normalizeOfferId(value) {
  return slugTailVariants(value)[0] || "";
}

export function offerSourceKey(value) {
  try {
    const url = new URL(String(value || ""), BASE_URL);
    const segments = url.pathname
      .split("/")
      .filter(Boolean)
      .map(decodeRepeatedly);
    if (segments.length < 2) return "";
    const partner = canonicalKey(segments.at(-2));
    const firstToken = String(segments.at(-1) || "").split("-")[0];
    const offerCode = canonicalKey(firstToken);
    if (!partner || !/^p[a-z0-9]{2,5}$/.test(offerCode)) return "";
    return `${partner}|${offerCode}`;
  } catch {
    return "";
  }
}

export function offerIdentityKeys(value) {
  const keys = slugTailVariants(value).map((variant) => `slug:${variant}`);
  const sourceKey = offerSourceKey(value);
  if (sourceKey) keys.push(`source:${sourceKey}`);
  return [...new Set(keys)];
}

export function normalizePublicLink(value) {
  try {
    const url = new URL(String(value || ""), BASE_URL);
    if (url.protocol !== "https:" || url.hostname !== "clube.uol.com.br") return "";
    url.hash = "";
    url.search = "";
    return url.href;
  } catch {
    return "";
  }
}

export function normalizeCard(raw) {
  const link = normalizePublicLink(raw?.link);
  const id = normalizeOfferId(link);
  const title = cleanText(raw?.title);
  if (!link || !id || !title) return null;

  return {
    id,
    link,
    previewTitle: title,
    category: cleanText(raw?.category),
    cardImageUrl: String(raw?.cardImageUrl || "").trim(),
    partnerImageUrl: String(raw?.partnerImageUrl || "").trim(),
    partnerName: cleanText(raw?.partnerName),
  };
}

export function dedupeCards(rawCards) {
  const cards = [];
  const seenKeys = new Set();
  for (const raw of rawCards || []) {
    const card = normalizeCard(raw);
    if (!card) continue;
    const identityKeys = offerIdentityKeys(card.link);
    if (identityKeys.some((key) => seenKeys.has(key))) continue;
    cards.push(card);
    for (const key of identityKeys) seenKeys.add(key);
  }
  return cards;
}

export function isTicketCampaign(offer) {
  const link = String(offer?.link || "").toLowerCase();
  const category = normalizeText(offer?.category);
  return link.includes("/campanhasdeingresso/") || category.includes("campanhasdeingresso");
}

function containsWord(text, term) {
  const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`).test(text);
}

export function shouldSendToCanal2(offer) {
  return isTicketCampaign(offer);
}

export function extractValidity(text) {
  const value = cleanText(text);
  if (!value) return "";
  const patterns = [
    /benefício válido de[^.!?\n]*[.!?]?/i,
    /válido até[^.!?\n]*[.!?]?/i,
    /\d{2}\/\d{2}\/\d{4}[\s\S]{0,80}\d{2}\/\d{2}\/\d{4}/i,
  ];
  for (const pattern of patterns) {
    const match = value.match(pattern);
    if (match?.[0]) return cleanText(match[0]);
  }
  return "";
}

const SAO_PAULO_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/Sao_Paulo",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

function saoPauloPartsAt(timestamp) {
  const result = {};
  for (const part of SAO_PAULO_DATE_TIME_FORMATTER.formatToParts(new Date(timestamp))) {
    if (part.type !== "literal") result[part.type] = Number(part.value);
  }
  return result;
}

function saoPauloDateTimeToUtc(parts) {
  const desiredAsUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
  let candidate = desiredAsUtc;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const represented = saoPauloPartsAt(candidate);
    const representedAsUtc = Date.UTC(
      represented.year,
      represented.month - 1,
      represented.day,
      represented.hour,
      represented.minute,
      represented.second,
    );
    const adjustment = desiredAsUtc - representedAsUtc;
    candidate += adjustment;
    if (!adjustment) break;
  }

  const represented = saoPauloPartsAt(candidate);
  if (
    represented.year !== parts.year ||
    represented.month !== parts.month ||
    represented.day !== parts.day ||
    represented.hour !== parts.hour ||
    represented.minute !== parts.minute ||
    represented.second !== parts.second
  ) {
    return null;
  }
  return new Date(candidate + parts.millisecond);
}

function brDateTimeToUtc(value, { endOfDay = false } = {}) {
  const match = String(value || "").match(
    /(\d{2})\/(\d{2})\/(\d{4})(?:\s+(?:às\s+)?(\d{2}):(\d{2}))?/i,
  );
  if (!match) return null;
  const [, rawDay, rawMonth, rawYear, rawHour, rawMinute] = match;
  const year = Number(rawYear);
  const month = Number(rawMonth);
  const day = Number(rawDay);
  const hasTime = rawHour !== undefined && rawMinute !== undefined;
  const hour = hasTime ? Number(rawHour) : endOfDay ? 23 : 0;
  const minute = hasTime ? Number(rawMinute) : endOfDay ? 59 : 0;
  const second = !hasTime && endOfDay ? 59 : 0;
  const millisecond = !hasTime && endOfDay ? 999 : 0;
  const daysInMonth = month >= 1 && month <= 12
    ? new Date(Date.UTC(year, month, 0)).getUTCDate()
    : 0;
  if (
    year < 1000 ||
    day < 1 ||
    day > daysInMonth ||
    hour < 0 ||
    hour > 23 ||
    minute < 0 ||
    minute > 59
  ) {
    return null;
  }
  return saoPauloDateTimeToUtc({
    year,
    month,
    day,
    hour,
    minute,
    second,
    millisecond,
  });
}

function validityDateRole(context) {
  const normalized = String(context || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
  if (/\bate\b[^0-9]*$/i.test(normalized)) return "end";
  if (/\b(?:de|desde)\b[^0-9]*$/i.test(normalized)) return "start";
  return "";
}

export function parseValidityWindow(validity) {
  const text = String(validity || "");
  const matches = [...text.matchAll(
    /(\d{2}\/\d{2}\/\d{4}(?:\s+(?:às\s+)?\d{2}:\d{2})?)/gi,
  )];
  if (!matches.length) return { start: null, end: null };

  const dates = matches.map((match, index) => {
    const previousEnd = index ? matches[index - 1].index + matches[index - 1][0].length : 0;
    return {
      value: match[1],
      role: validityDateRole(text.slice(previousEnd, match.index)),
    };
  });
  let startDate = dates.find((date) => date.role === "start") || null;
  let endDate = dates.findLast((date) => date.role === "end") || null;

  if (dates.length === 1) {
    if (!endDate) startDate = dates[0];
  } else if (!startDate && !endDate) {
    [startDate] = dates;
    endDate = dates.at(-1);
  } else {
    startDate ||= dates.find((date) => date !== endDate && date.role !== "end") || null;
    endDate ||= dates.findLast((date) => date !== startDate && date.role !== "start") || null;
  }

  return {
    start: startDate ? brDateTimeToUtc(startDate.value) : null,
    end: endDate ? brDateTimeToUtc(endDate.value, { endOfDay: true }) : null,
  };
}

export function evaluateDetailQuality(detail) {
  const hasTitle = cleanText(detail?.title).length >= 4;
  const hasValidity = cleanText(detail?.validity).length >= 8;
  const hasDescription = cleanText(detail?.description).length >= 60;
  const hasImage = Boolean(String(detail?.imageUrl || "").trim());

  if (hasTitle && hasValidity && hasDescription && hasImage) return "complete";
  if ((hasValidity && hasDescription) || (hasDescription && hasImage) || (hasValidity && hasImage)) {
    return "partial";
  }
  if (hasTitle || hasDescription || hasImage || hasValidity) return "weak";
  return "failed";
}

function descriptionAnchor(value) {
  return normalizeText(value).slice(0, 900);
}

export async function sha256Hex(value) {
  const bytes = new TextEncoder().encode(String(value || ""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

const API_SNAPSHOT_FIELDS = [
  "id",
  "link",
  "previewTitle",
  "title",
  "category",
  "cardImageUrl",
  "partnerImageUrl",
  "partnerName",
  "imageUrl",
  "validity",
  "description",
];

function snapshotFieldValue(card, field) {
  const value = card?.[field] ?? card?.apiDetail?.[field];
  if (["cardImageUrl", "partnerImageUrl", "imageUrl", "link"].includes(field)) {
    return String(value || "").trim();
  }
  return cleanText(value);
}

export async function buildApiSnapshotFingerprint(cards = []) {
  const normalized = Array.from(cards || [])
    .map((card) => Object.fromEntries(
      API_SNAPSHOT_FIELDS.map((field) => [field, snapshotFieldValue(card, field)]),
    ))
    .filter((card) => card.id || card.link)
    .sort((left, right) => {
      const byId = left.id.localeCompare(right.id);
      return byId || left.link.localeCompare(right.link);
    });
  return sha256Hex(JSON.stringify(normalized));
}

export function normalizeTicketProbeAt(value, fallback = new Date()) {
  const normalized = String(value || "").trim();
  const parsed = Date.parse(normalized);
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(normalized) &&
      Number.isFinite(parsed)) {
    return new Date(parsed).toISOString();
  }
  const fallbackDate = fallback instanceof Date ? fallback : new Date(fallback);
  const fallbackAt = fallbackDate.getTime();
  return new Date(Number.isFinite(fallbackAt) ? fallbackAt : Date.now()).toISOString();
}

export function shouldReconcileHtmlSnapshot({
  fingerprint,
  previousFingerprint,
  lastReconciledAt,
  now = new Date(),
  refreshIntervalSeconds = 900,
  complete = false,
  initialized = false,
} = {}) {
  if (!complete) return { reconcile: true, reason: "incomplete" };
  if (!initialized) return { reconcile: true, reason: "uninitialized" };
  if (!previousFingerprint) return { reconcile: true, reason: "missing_fingerprint" };
  if (!/^[a-f0-9]{64}$/.test(previousFingerprint)) {
    return { reconcile: true, reason: "invalid_fingerprint" };
  }
  if (fingerprint !== previousFingerprint) {
    return { reconcile: true, reason: "changed" };
  }
  const reconciledAt = Date.parse(lastReconciledAt || "");
  if (!Number.isFinite(reconciledAt)) {
    return { reconcile: true, reason: "missing_reconciled_at" };
  }
  const nowAt = now instanceof Date ? now.getTime() : Date.parse(now || "");
  if (Number.isFinite(nowAt) && reconciledAt > nowAt) {
    return { reconcile: true, reason: "invalid_reconciled_at" };
  }
  const refreshMs = Math.max(1, Number(refreshIntervalSeconds || 900)) * 1_000;
  if (!Number.isFinite(nowAt) || nowAt - reconciledAt >= refreshMs) {
    return { reconcile: true, reason: "periodic_refresh" };
  }
  return { reconcile: false, reason: "unchanged_fresh" };
}

export function buildApiHealthSnapshot(cards = []) {
  return Array.from(cards || [])
    .map((card) => ({
      id: cleanText(card?.id ?? card?.apiDetail?.id),
      link: String(card?.link ?? card?.apiDetail?.link ?? "").trim(),
      previewTitle: cleanText(
        card?.previewTitle ?? card?.title ?? card?.apiDetail?.previewTitle ??
          card?.apiDetail?.title,
      ),
      category: cleanText(card?.category ?? card?.apiDetail?.category),
    }))
    .filter((card) => card.id && card.link)
    .sort((left, right) => left.id.localeCompare(right.id) || left.link.localeCompare(right.link));
}

export async function buildDedupeKeys(offer) {
  const title = normalizeText(offer?.title || offer?.previewTitle);
  const validity = normalizeText(offer?.validity);
  const description = descriptionAnchor(offer?.description);
  const sourcePartner = offerSourceKey(offer?.link).split("|", 1)[0] ||
    normalizeText(offer?.partnerName);
  return {
    dedupeKey: await sha256Hex(`${sourcePartner}|${title}|${validity}|${description}`),
    looseDedupeKey: await sha256Hex(
      `${sourcePartner}|${title}|${description.slice(0, 280)}`,
    ),
    legacyDedupeKey: await sha256Hex(`${title}|${validity}|${description}`),
    legacyLooseDedupeKey: await sha256Hex(`${title}|${description.slice(0, 280)}`),
    titleValidityKey: title && validity ? await sha256Hex(`${title}|${validity}`) : "",
  };
}

export function decideShadowDelivery(offer, options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const maxValidFromAgeHours = Number(options.maxValidFromAgeHours || 36);
  const validity = parseValidityWindow(offer?.validity);

  if (validity.end && now > validity.end) {
    return {
      eligible: false,
      discardReason: "validade_expirada",
      wouldSendMain: false,
      wouldSendCanal2: false,
    };
  }

  if (validity.start && !validity.end) {
    const ageHours = (now.getTime() - validity.start.getTime()) / 3_600_000;
    if (ageHours > maxValidFromAgeHours) {
      return {
        eligible: false,
        discardReason: "inicio_validade_antigo_sem_fim",
        wouldSendMain: false,
        wouldSendCanal2: false,
      };
    }
  }

  const hasMinimumPayload = Boolean(
    cleanText(offer?.title || offer?.previewTitle) && normalizePublicLink(offer?.link),
  );
  if (!hasMinimumPayload) {
    return {
      eligible: false,
      discardReason: "incompleta",
      wouldSendMain: false,
      wouldSendCanal2: false,
    };
  }

  return {
    eligible: true,
    discardReason: "",
    wouldSendMain: true,
    wouldSendCanal2: shouldSendToCanal2(offer),
  };
}

export function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

const HASHTAG_RULES = [
  ["#ingresso", ["ingresso", "ingressos"]],
  ["#show", ["show", "festival", "musical", "turne", "apresentacao"]],
  ["#teatro", ["teatro", "musical", "espetaculo", "peca"]],
  ["#standup", ["stand up", "standup", "comediante", "humor"]],
  ["#entretenimentoviagens", ["cinema", "ingressos", "espetaculo", "evento"]],
  ["#servicos", ["terapia"]],
  ["#beleza", ["depilacao", "axilas", "beleza", "barba"]],
  ["#comerbeber", ["vinho", "cerveja", "jantar", "almoco", "sobremesa", "restaurante"]],
  ["#cursos", ["curso", "cursos", "ingles", "english"]],
  ["#compraspresentes", ["ovo de pascoa", "vivara", "presente"]],
  ["#educacao", ["graduacao", "pos", "ead", "aprender", "enem"]],
  ["#viagem", ["viagem", "viagens"]],
  ["#eletrodomesticoseletronicos", ["dell", "lg", "eletro", "geladeira", "lavadora"]],
];

const SILENT_HASHTAGS = new Set([
  "#servicos",
  "#beleza",
  "#cursos",
  "#educacao",
  "#eletrodomesticoseletronicos",
]);

export function buildSmartHashtags(offer) {
  const title = normalizeText(offer?.title || offer?.previewTitle);
  const description = normalizeText(offer?.description);
  const full = `${title} ${description}`;
  const tags = [];

  if (isTicketCampaign(offer)) tags.push("#campanhasdeingresso");
  for (const [tag, keywords] of HASHTAG_RULES) {
    const haystack = [
      "#servicos",
      "#beleza",
      "#cursos",
      "#compraspresentes",
      "#educacao",
      "#viagem",
      "#eletrodomesticoseletronicos",
    ].includes(tag) ? title : full;
    if (keywords.some((keyword) => containsWord(haystack, keyword))) tags.push(tag);
  }
  return [...new Set(tags)];
}

export function shouldSendSilent(offer) {
  if (isTicketCampaign(offer)) return false;
  return buildSmartHashtags(offer).some((tag) => SILENT_HASHTAGS.has(tag));
}

export function extractLocationSummary(description) {
  const text = cleanText(description);
  if (!text) return "";
  const explicit = text.match(
    /\blocal\s*:\s*(.{2,220}?)(?=\s+(?:importante|regras?(?:\s+de\s+resgate)?|aten[cç][aã]o|observa[cç][oõ]es?)\s*:|$)/i,
  );
  const candidate = cleanText(explicit?.[1] || "");
  if (!candidate) return "";
  if (candidate.length <= 160) return candidate.replace(/[.,;]+$/, "");
  const cityState = candidate.match(
    /([A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+?)\s*[-/]\s*([A-Za-z]{2})(?=$|[\s,;)])/i,
  );
  if (cityState) {
    return `${cleanText(cityState[1]).replace(/[.,;]+$/, "")} - ${cityState[2].toUpperCase()}`;
  }
  return candidate.slice(0, 157).replace(/\s+\S*$/, "") + "…";
}

export function formatOfferTime(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return cleanText(value);
  return new Intl.DateTimeFormat("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

export function formatOfferDuration(startValue, endValue) {
  const start = startValue instanceof Date ? startValue : new Date(startValue);
  const end = endValue instanceof Date ? endValue : new Date(endValue);
  const elapsedMs = end.getTime() - start.getTime();
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || elapsedMs < 0) return "";
  const minutes = Math.floor(elapsedMs / 60_000);
  if (minutes < 1) return "menos de 1 min";
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (hours < 24) return `${hours}h${remainingMinutes ? ` ${remainingMinutes}min` : ""}`;
  const days = Math.floor(hours / 24);
  const remainingHours = hours % 24;
  return `${days}d${remainingHours ? ` ${remainingHours}h` : ""}`;
}

export function buildTelegramCaption(offer, options = {}) {
  const title = cleanText(offer?.title || offer?.previewTitle || "Oferta").slice(0, 280);
  const link = normalizePublicLink(offer?.link);
  const tags = buildSmartHashtags(offer);
  const validity = cleanText(offer?.validity).replace(/\s+\./g, ".").slice(0, 360);
  const location = extractLocationSummary(offer?.description);
  const capturedAt = offer?.firstSeenAt || offer?.first_seen_at || "";
  const soldOutAt = options.soldOutAt || offer?.soldOutAt || offer?.sold_out_at || "";
  const ticketTitle = isTicketCampaign(offer) ? `‼️ ${title} ‼️` : title;
  const decoratedTitle = soldOutAt ? `[ESGOTADO] ${ticketTitle}` : ticketTitle;
  const titleHtml = soldOutAt
    ? `<s>${escapeHtml(decoratedTitle)}</s>`
    : escapeHtml(decoratedTitle);
  const lines = [`<b>${titleHtml}</b>`];

  if (tags.length) lines.push(escapeHtml(tags.join(" ")));
  if (location) lines.push(`📍 ${escapeHtml(location)}`);
  if (validity) lines.push(`📅 ${escapeHtml(validity.endsWith(".") ? validity : `${validity}.`)}`);
  if (soldOutAt) {
    lines.push(`📸 Oferta capturada às ${escapeHtml(formatOfferTime(capturedAt))}.`);
    lines.push(`❌ Oferta esgotada às ${escapeHtml(formatOfferTime(soldOutAt))}.`);
    const duration = formatOfferDuration(
      options.publishedAt || offer?.firstSeenAt || offer?.first_seen_at,
      soldOutAt,
    );
    if (duration) lines.push(`⏱️ Ficou no ar por ${escapeHtml(duration)}.`);
  }
  if (link) lines.push(`🔗 ${escapeHtml(link)}`);
  if (options.commentsEnabled && !soldOutAt) {
    lines.push("💬 Veja os detalhes completos nos comentários.");
  }

  return lines.join("\n\n");
}

function splitBoundedText(value, maxLength) {
  const chunks = [];
  let remaining = cleanText(value);
  while (remaining.length > maxLength) {
    let cut = remaining.lastIndexOf(" ", maxLength);
    if (cut < Math.floor(maxLength * 0.6)) cut = maxLength;
    chunks.push(remaining.slice(0, cut).trim());
    remaining = remaining.slice(cut).trim();
  }
  if (remaining) chunks.push(remaining);
  return chunks;
}

export function buildDiscussionCommentChunks(offer) {
  const title = cleanText(offer?.title || offer?.previewTitle || "Oferta");
  let description = cleanText(offer?.description);
  const validity = cleanText(offer?.validity);
  const link = normalizePublicLink(offer?.link);
  const labels = [
    "Sobre o Parceiro", "Benefício", "Regras do benefício", "Regras",
    "Como utilizar", "Como resgatar", "Passo a passo para resgate",
    "Data do Show", "Data", "Quando", "Local", "Importante",
    "REGRAS DE RESGATE", "Atenção, Assinante UOL!",
  ];
  const labelPattern = labels
    .sort((a, b) => b.length - a.length)
    .map((label) => label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .join("|");
  description = description.replace(
    new RegExp(`\\s*(${labelPattern})\\s*:?\\s*`, "gi"),
    "\n\n$1: ",
  );
  description = description
    .replace(/\s*•\s*/g, "\n• ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  const blocks = [`📋 <b>${escapeHtml(title)}</b>`];
  for (const rawBlock of description.split(/\n\n+/).map((item) => item.trim()).filter(Boolean)) {
    const labelMatch = rawBlock.match(/^([^:!]{2,60})([:!])\s*(.*)$/s);
    if (labelMatch) {
      const [, label, punctuation, rest] = labelMatch;
      const icon = /^(data|quando)/i.test(label) ? "🗓️ "
        : /^local/i.test(label) ? "📍 "
        : /^(importante|atenção)/i.test(label) ? "❗ "
        : /^regras/i.test(label) ? "📌 " : "";
      blocks.push(`${icon}<b>${escapeHtml(label)}${escapeHtml(punctuation)}</b>${rest ? ` ${escapeHtml(rest)}` : ""}`);
    } else {
      blocks.push(escapeHtml(rawBlock));
    }
  }
  if (validity) blocks.push(`📅 ${escapeHtml(validity.endsWith(".") ? validity : `${validity}.`)}`);
  if (link) blocks.push(`🔗 ${escapeHtml(link)}`);

  const chunks = [];
  let current = "";
  for (const block of blocks.flatMap((item) => splitBoundedText(item, 3_600))) {
    const candidate = current ? `${current}\n\n${block}` : block;
    if (candidate.length > 3_800 && current) {
      chunks.push(current);
      current = block;
    } else {
      current = candidate;
    }
  }
  if (current) chunks.push(current);
  return chunks;
}
