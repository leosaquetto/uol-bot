export function nextImageCircuitState(
  current = {},
  attempt = {},
  { now = new Date(), threshold = 3, cooldownMinutes = 10 } = {},
) {
  if (attempt.ok) {
    return {
      state: "closed",
      consecutiveFailures: 0,
      openedUntil: "",
      lastError: "",
    };
  }
  const consecutiveFailures = Number(current.consecutiveFailures || 0) + 1;
  const shouldOpen = consecutiveFailures >= threshold;
  return {
    state: shouldOpen ? "open" : "closed",
    consecutiveFailures,
    openedUntil: shouldOpen
      ? new Date(now.getTime() + cooldownMinutes * 60_000).toISOString()
      : "",
    lastError: String(attempt.error || "").slice(0, 240),
  };
}

// remote_url depends on Telegram downloading the Clube CDN itself, which fails with
// "failed to get HTTP URL content" for days at a time (22 straight failures from 18/11).
// Every half-open probe costs a Telegram call and a few seconds on a new offer, so it
// is retried far less often than the strategies that work from our side.
export function imageCircuitCooldownMinutes(strategy, { defaultMinutes = 10, remoteUrlMinutes = 360 } = {}) {
  return strategy === "remote_url" ? Math.max(defaultMinutes, remoteUrlMinutes) : defaultMinutes;
}
