import assert from "node:assert/strict";
import test from "node:test";

import { htmlReconciliationDue } from "../src/scan-policy.js";

const base = {
  source: "alarm",
  apiStatus: "fulfilled",
  apiOffers: 48,
  initialized: true,
  lastStartedAt: "2026-08-02T12:00:00.000Z",
  intervalSeconds: 60,
  nowMs: Date.parse("2026-08-02T12:00:15.000Z"),
};

test("ciclo saudável recente consulta somente a API", () => {
  assert.equal(htmlReconciliationDue(base), false);
});

test("HTML entra por cadência, falha/vazio da API ou execução manual", () => {
  assert.equal(htmlReconciliationDue({ ...base, nowMs: Date.parse("2026-08-02T12:01:00Z") }), true);
  assert.equal(htmlReconciliationDue({ ...base, apiStatus: "rejected" }), true);
  assert.equal(htmlReconciliationDue({ ...base, apiOffers: 0 }), true);
  assert.equal(htmlReconciliationDue({ ...base, source: "manual" }), true);
});
