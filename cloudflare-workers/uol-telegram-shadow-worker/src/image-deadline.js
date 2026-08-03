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

export function mainImageDeliveryOffer(
  offer,
  now = new Date(),
  waitSeconds = 60,
  mutationSafetySeconds = 38,
) {
  const state = imageDeadline(offer?.firstSeenAt, now, waitSeconds);
  const deadlineMs = Date.parse(state.deadlineAt);
  const mutationDeadlineAt = Number.isFinite(deadlineMs)
    ? new Date(
        deadlineMs - Math.max(0, Number(mutationSafetySeconds || 0)) * 1_000,
      ).toISOString()
    : "";
  return {
    ...offer,
    imageDeadlineAt: state.deadlineAt,
    imageMutationDeadlineAt: mutationDeadlineAt,
    deferTextFallback: !state.expired,
  };
}

export function lateImageUpgradeDue(row, now = new Date(), maxAttempts = 10) {
  const nextAttemptMs = Date.parse(String(row?.main_image_upgrade_next_attempt_at || ""));
  const hasImage = Boolean(String(
    row?.telegram_photo_file_id || row?.image_url || row?.card_image_url ||
      row?.partner_image_url || "",
  ).trim());
  return row?.telegram_image_strategy === "text_timeout" &&
    row?.main_message_kind === "text" &&
    Number(row?.main_message_id || 0) > 0 &&
    hasImage &&
    Number(row?.main_image_upgrade_attempts || 0) < Math.max(1, Number(maxAttempts || 10)) &&
    (!Number.isFinite(nextAttemptMs) || nextAttemptMs <= now.getTime());
}

export function deferredMainDeliveryState(attempts, deadlineAt, now = new Date()) {
  const deadlineMs = Date.parse(String(deadlineAt || ""));
  const retryMs = Number.isFinite(deadlineMs)
    ? Math.min(deadlineMs, now.getTime() + 1_000)
    : now.getTime() + 1_000;
  return {
    attempts: Math.max(0, Number(attempts || 0)),
    nextAttemptAt: new Date(retryMs).toISOString(),
  };
}
