export function htmlReconciliationDue({
  source = "alarm",
  apiStatus = "rejected",
  apiOffers = 0,
  initialized = false,
  lastStartedAt = "",
  intervalSeconds = 60,
  nowMs = Date.now(),
} = {}) {
  if (source !== "alarm" || apiStatus !== "fulfilled" || apiOffers <= 0 || !initialized) {
    return true;
  }
  const previous = Date.parse(String(lastStartedAt || ""));
  if (!Number.isFinite(previous)) return true;
  return nowMs - previous >= Math.max(1, Number(intervalSeconds || 60)) * 1_000;
}
