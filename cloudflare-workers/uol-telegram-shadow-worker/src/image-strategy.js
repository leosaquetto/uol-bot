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
