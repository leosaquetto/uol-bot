const DEFAULT_CONFIRM_GONE_COUNT = 2;
const DEFAULT_CONFIRM_DELAY_SECONDS = 5;
const UOL_HOST = "clube.uol.com.br";

function normalizedNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function safeUrl(value, base = "") {
  try {
    return new URL(String(value || ""), base || undefined);
  } catch {
    return null;
  }
}

function isTicketPath(value) {
  const url = safeUrl(value);
  return Boolean(url && url.hostname === UOL_HOST &&
    url.pathname.toLowerCase().includes("/campanhasdeingresso/"));
}

function isHomePath(value) {
  const url = safeUrl(value, "https://clube.uol.com.br/");
  if (!url || url.hostname !== UOL_HOST) return false;
  const path = url.pathname.replace(/\/+$/, "") || "/";
  return path === "/" || path.toLowerCase() === "/index.html";
}

export function classifyTicketProbeResponse({
  requestedUrl,
  finalUrl = requestedUrl,
  status = 0,
  body = "",
} = {}) {
  if (!isTicketPath(requestedUrl)) {
    return { result: "indeterminate", reason: "not_ticket_url" };
  }
  const httpStatus = Math.max(0, Math.floor(normalizedNumber(status)));
  if (httpStatus === 404 || httpStatus === 410) {
    return { result: "gone", reason: `http_${httpStatus}` };
  }
  if (httpStatus < 200 || httpStatus >= 300) {
    return { result: "indeterminate", reason: `http_${httpStatus || "unknown"}` };
  }
  if (isHomePath(finalUrl)) {
    return { result: "gone", reason: "home_redirect" };
  }
  if (!String(body || "").trim()) {
    return { result: "indeterminate", reason: "empty_body" };
  }
  return { result: "available", reason: "offer_page" };
}

export function nextTicketProbeState({
  result,
  goneCount = 0,
  attempts = 0,
  now = new Date(),
  confirmGoneCount = DEFAULT_CONFIRM_GONE_COUNT,
  maxAttempts = DEFAULT_CONFIRM_GONE_COUNT,
  confirmDelaySeconds = DEFAULT_CONFIRM_DELAY_SECONDS,
} = {}) {
  const currentGoneCount = Math.max(0, Math.floor(normalizedNumber(goneCount)));
  const currentAttempts = Math.max(0, Math.floor(normalizedNumber(attempts)));
  const confirmedAtCount = Math.max(1, Math.floor(normalizedNumber(confirmGoneCount, 2)));
  const attemptLimit = Math.max(1, Math.floor(normalizedNumber(maxAttempts, 2)));
  const base = now instanceof Date ? now : new Date(now);
  const safeNow = Number.isFinite(base.getTime()) ? base : new Date();
  const normalizedResult = String(result || "").trim().toLowerCase();

  if (normalizedResult !== "gone") {
    return {
      action: "fallback",
      goneCount: 0,
      nextAt: "",
      lastResult: normalizedResult || "indeterminate",
    };
  }

  const nextGoneCount = currentGoneCount + 1;
  if (nextGoneCount >= confirmedAtCount) {
    return {
      action: "confirm",
      goneCount: nextGoneCount,
      nextAt: "",
      lastResult: "gone",
    };
  }
  if (currentAttempts + 1 >= attemptLimit) {
    return {
      action: "fallback",
      goneCount: 0,
      nextAt: "",
      lastResult: "gone_attempt_limit",
    };
  }
  return {
    action: "continue",
    goneCount: nextGoneCount,
    nextAt: new Date(
      safeNow.getTime() + Math.max(1, Number(confirmDelaySeconds) || 5) * 1_000,
    ).toISOString(),
    lastResult: "gone",
  };
}

export function ticketProbeBudget({ used = 0, dailyLimit = 256, perScanLimit = 1 } = {}) {
  const normalizedUsed = Math.max(0, Math.floor(normalizedNumber(used)));
  const normalizedLimit = Math.max(0, Math.floor(normalizedNumber(dailyLimit)));
  const normalizedPerScan = Math.max(0, Math.floor(normalizedNumber(perScanLimit)));
  const remaining = Math.max(0, normalizedLimit - normalizedUsed);
  const batchSize = Math.min(remaining, normalizedPerScan);
  return {
    remaining,
    allowed: batchSize > 0,
    batchSize,
  };
}

export async function probeTicketOfferUrl(
  url,
  fetchImpl = fetch,
  timeoutMs = 5_000,
) {
  const requestedUrl = String(url || "").trim();
  if (!isTicketPath(requestedUrl)) {
    return { result: "indeterminate", reason: "not_ticket_url" };
  }
  try {
    const response = await fetchImpl(requestedUrl, {
      method: "GET",
      redirect: "follow",
      headers: {
        Accept: "text/html",
        "Cache-Control": "no-cache",
      },
      signal: AbortSignal.timeout(Math.max(1_000, Number(timeoutMs) || 5_000)),
    });
    const body = response.status >= 200 && response.status < 300
      ? (await response.text()).slice(0, 64 * 1024)
      : "";
    return classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: response.url || requestedUrl,
      status: response.status,
      body,
    });
  } catch {
    return { result: "indeterminate", reason: "network_or_timeout" };
  }
}

export async function probeTicketOfferWithControl(
  url,
  controlUrl,
  fetchImpl = fetch,
  timeoutMs = 5_000,
) {
  const candidate = await probeTicketOfferUrl(url, fetchImpl, timeoutMs);
  if (candidate.result !== "gone" || candidate.reason !== "home_redirect") {
    return { ...candidate, requests: 1, controlProbed: false };
  }

  const normalizedControlUrl = String(controlUrl || "").trim();
  if (
    !isTicketPath(normalizedControlUrl) ||
    normalizedControlUrl === String(url || "").trim()
  ) {
    return {
      result: "indeterminate",
      reason: "home_redirect_control_unavailable",
      requests: 1,
      controlProbed: false,
    };
  }

  const control = await probeTicketOfferUrl(normalizedControlUrl, fetchImpl, timeoutMs);
  if (control.result === "available") {
    return { ...candidate, requests: 2, controlProbed: true };
  }
  if (control.result === "gone" && control.reason === "home_redirect") {
    return {
      result: "indeterminate",
      reason: "global_home_redirect",
      requests: 2,
      controlProbed: true,
    };
  }
  return {
    result: "indeterminate",
    reason: `home_redirect_control_${control.reason}`.slice(0, 120),
    requests: 2,
    controlProbed: true,
  };
}
