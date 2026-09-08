import assert from "node:assert/strict";
import test from "node:test";

import { mergeBeeperGatewayHealth } from "../src/beeper-gateway-health.js";

const workerHealthy = {
  state: "healthy",
  hardFailure: false,
  reasons: [],
  snapshot: { worker: "uol-telegram-shadow-pilot" },
};

test("mantém healthy quando o gateway Oracle está pronto", () => {
  const result = mergeBeeperGatewayHealth(workerHealthy, {
    status: 200,
    body: { ok: true, ignored: "never persisted" },
  });
  assert.equal(result.state, "healthy");
  assert.deepEqual(result.reasons, []);
  assert.deepEqual(result.snapshot.beeperGateway, { status: 200, ok: true });
});

test("transforma gateway indisponível em outage sanitizado", () => {
  const result = mergeBeeperGatewayHealth(workerHealthy, {
    status: 503,
    body: { ok: false, code: "private-detail" },
  });
  assert.equal(result.state, "outage");
  assert.equal(result.hardFailure, true);
  assert.deepEqual(result.reasons, ["beeper_gateway_unavailable"]);
  assert.deepEqual(result.snapshot.beeperGateway, { status: 503, ok: false });
});

test("não duplica motivo quando Worker e gateway estão indisponíveis", () => {
  const result = mergeBeeperGatewayHealth({
    ...workerHealthy,
    state: "outage",
    hardFailure: true,
    reasons: ["beeper_gateway_unavailable"],
  }, { status: 0, body: null });
  assert.deepEqual(result.reasons, ["beeper_gateway_unavailable"]);
});

import { currentBeeperGatewayHealth } from "../src/beeper-gateway-health.js";
test("readiness antiga ou futura não comprova disponibilidade", () => {
  const now = Date.parse("2026-09-08T05:00:00Z");
  for (const checkedAt of ["", "2026-09-05T05:00:00Z", "2026-09-08T06:00:00Z"]) {
    assert.equal(currentBeeperGatewayHealth({gatewayOk: true, checkedAt}, now).gatewayOk, null);
  }
  assert.equal(currentBeeperGatewayHealth({gatewayOk: true, checkedAt: "2026-09-08T04:59:00Z"}, now).gatewayOk, true);
  assert.equal(currentBeeperGatewayHealth({gatewayOk: false, checkedAt: "2026-09-08T04:59:00Z"}, now).gatewayOk, false);
});
