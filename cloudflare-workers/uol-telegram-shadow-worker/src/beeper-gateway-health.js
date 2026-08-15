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
