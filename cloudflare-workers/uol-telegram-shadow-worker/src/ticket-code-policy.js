// One bounded snapshot, shared by all alarms; no extra timer or table.
export const TICKET_CODE_DAILY_REQUEST_LIMIT = 6_000;
const MINUTE = 60_000;
const DAY = 86_400_000;

export async function ticketCodeCardFingerprint(card) {
  const detail = card?.apiDetail || {};
  const content = JSON.stringify([card?.id || "", card?.link || "", card?.previewTitle || "",
    card?.category || "", card?.cardImageUrl || "", card?.partnerImageUrl || "",
    card?.partnerName || "", detail.title || "", detail.description || "",
    detail.validity || "", detail.imageUrl || "", detail.quality || ""]);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(content));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

export function unresolvedTicketCodeEntries(snapshot) {
  return Object.values(snapshot.entries || {}).filter(entry => entry.card && entry.status === "found" &&
    (!entry.resolvedId || !entry.fingerprint || entry.resolvedFingerprint !== entry.fingerprint ||
      Number(entry.resolvedAvailabilityEpoch || 0) !== Number(entry.availabilityEpoch || 0)));
}

export function planTicketCodeScan(previous, candidates, now) {
  const day = new Date(now).toISOString().slice(0, 10);
  if (Number(previous.nextAt || 0) > now) return null;
  const requestsUsed = previous.day === day ? Number(previous.requestsUsed || 0) : 0;
  const batchSize = Math.min(4, Math.floor((TICKET_CODE_DAILY_REQUEST_LIMIT - requestsUsed) / 2));
  if (batchSize <= 0) return null;
  const entries = {};
  // Retain discovered pages outside the current code window for availability
  // checks. Unknown/failed responses never become evidence of absence.
  for (const [code, entry] of Object.entries(previous.entries || {})) {
    if (entry.card && now - Number(entry.foundAt || 0) < 30 * DAY) entries[code] = entry;
  }
  for (const code of candidates.slice(0, 64)) entries[code] ||= previous.entries?.[code] || {};
  for (const code of Object.keys(entries).slice(96)) delete entries[code];
  const due = Object.keys(entries).filter(code => Number(entries[code].nextAt || 0) <= now)
    .sort((a, b) => Number(entries[a].checkedAt || 0) - Number(entries[b].checkedAt || 0));
  const selected = due.slice(0, batchSize);
  if (!selected.length) return null;
  const reserved = { ...entries };
  for (const code of selected) reserved[code] = { ...entries[code], checkedAt: now, nextAt: now + 10 * MINUTE };
  return {
    selected,
    state: { ...previous, day, requestsUsed: requestsUsed + selected.length * 2,
      nextAt: now + MINUTE, entries: reserved },
  };
}

export function recordTicketCodeResults(state, selected, results, now) {
  const entries = { ...state.entries };
  let requests = 0;
  for (let i = 0; i < selected.length; i++) {
    const code = selected[i];
    const result = results[i];
    const old = entries[code];
    requests += Math.min(2, Math.max(1, Number(result.requests || 2)));
    const found = result.status === "found" && result.card;
    const misses = result.status === "absent" ? Number(old.misses || 0) + 1 : 0;
    entries[code] = { ...old, checkedAt: now, status: result.status, misses,
      nextAt: now + (found ? 5 : 10) * MINUTE,
      ...(found ? { card: result.card, foundAt: now, fingerprint: result.fingerprint || "",
        availabilityEpoch: Number(old.availabilityEpoch || 0) + (old.lastConfirmedStatus === "absent" ? 1 : 0) } : {}),
      ...(["found", "absent"].includes(result.status) ? { lastConfirmedStatus: result.status } : {}),
      ...(misses >= 2 ? { card: null, resolvedId: "", resolvedFingerprint: "" } : {}),
    };
  }
  // Hard cap retained found pages as well as unsuccessful candidates.
  const kept = Object.entries(entries).sort((a, b) =>
    Number(Boolean(b[1].card)) - Number(Boolean(a[1].card)) ||
    Number(b[1].foundAt || b[1].checkedAt || 0) - Number(a[1].foundAt || a[1].checkedAt || 0),
  ).slice(0, 96);
  return { ...state, entries: Object.fromEntries(kept),
    requestsUsed: state.requestsUsed - selected.length * 2 + requests,
    nextAt: results.some(result => result.status === "unknown") ? now + 5 * MINUTE : state.nextAt,
    lastCheckedAt: new Date(now).toISOString(), attempted: selected.length,
    found: results.filter(result => result.status === "found").length,
    lastError: results.find(result => result.status === "unknown")?.reason || "",
  };
}

export function protectedTicketCodeIds(snapshot, now = Date.now()) {
  return Object.values(snapshot.entries || {}).filter(entry => entry.card && entry.resolvedId &&
    now - Number(entry.foundAt || 0) < 30 * DAY).map(entry => entry.resolvedId);
}
