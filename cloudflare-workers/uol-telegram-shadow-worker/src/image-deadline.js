export function imageDeadline(firstSeenAt, now = new Date(), waitSeconds = 60) {
  const firstSeenMs = Date.parse(String(firstSeenAt || ""));
  const nowMs = now instanceof Date ? now.getTime() : Number.NaN;
  if (!Number.isFinite(firstSeenMs) || !Number.isFinite(nowMs)) {
    return { deadlineAt: "", expired: true, remainingMs: 0 };
  }

  const seconds = Math.min(300, Math.max(1, Number(waitSeconds) || 60));
  const deadlineMs = firstSeenMs + seconds * 1_000;
  const remainingMs = Math.max(0, deadlineMs - nowMs);
  return {
    deadlineAt: new Date(deadlineMs).toISOString(),
    expired: remainingMs === 0,
    remainingMs,
  };
}
