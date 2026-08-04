function normalized(value) {
  return String(value || "").trim().toLowerCase();
}

export function classifyKnownMaintenanceRepair(row, maxAttempts = 10) {
  const attemptsLimit = Math.max(1, Number(maxAttempts || 10));
  const isSoldOut = normalized(row?.status) === "sold_out";
  const mainError = normalized(row?.main_sold_out_error);
  const canal2Error = normalized(row?.canal2_sold_out_error);
  const main = isSoldOut && !normalized(row?.main_sold_out_synced_at) &&
    Number(row?.main_sold_out_attempts || 0) >= attemptsLimit &&
    mainError.includes("message is not modified")
    ? "mark_synced"
    : "none";
  const canal2 = isSoldOut && !normalized(row?.canal2_sold_out_synced_at) &&
    Number(row?.canal2_sold_out_attempts || 0) >= attemptsLimit &&
    canal2Error.includes("there is no caption in the message to edit")
    ? "retry"
    : "none";
  return { main, canal2 };
}
