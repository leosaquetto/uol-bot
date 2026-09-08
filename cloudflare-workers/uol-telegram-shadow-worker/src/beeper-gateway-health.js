export function mergeBeeperGatewayHealth(result, gateway) {
  const status = Number(gateway?.status || 0);
  const ok = status === 200 && gateway?.body?.ok === true;
  const snapshot = {
    ...(result?.snapshot || {}),
    beeperGateway: { status, ok },
  };
  if (ok) return { ...result, snapshot };
  return {
    ...result,
    state: "outage",
    hardFailure: true,
    reasons: [...new Set([...(result?.reasons || []), "beeper_gateway_unavailable"])],
    snapshot,
  };
}

// A cached readiness result is evidence only while recent; it is not a receipt.
export function currentBeeperGatewayHealth(snapshot = {}, nowMs = Date.now()) {
  const checked = Date.parse(snapshot.checkedAt || "");
  const fresh = Number.isFinite(checked) && checked <= nowMs && nowMs - checked <= 10 * 60_000;
  return {
    gatewayOk: fresh && typeof snapshot.gatewayOk === "boolean" ? snapshot.gatewayOk : null,
    gatewayStatus: fresh ? Number(snapshot.gatewayStatus || 0) : 0,
    gatewayCode: fresh ? String(snapshot.gatewayCode || "") : "stale",
    checkedAt: String(snapshot.checkedAt || ""),
  };
}
