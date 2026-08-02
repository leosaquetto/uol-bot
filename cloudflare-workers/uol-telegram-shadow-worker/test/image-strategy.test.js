import assert from "node:assert/strict";
import test from "node:test";

import { nextImageCircuitState } from "../src/image-strategy.js";

test("abre circuito na terceira falha e fecha após sucesso", () => {
  const now = new Date("2026-08-01T12:00:00.000Z");
  const opened = nextImageCircuitState(
    { consecutiveFailures: 2 },
    { ok: false, error: "cdn 403" },
    { now, threshold: 3, cooldownMinutes: 10 },
  );
  assert.equal(opened.state, "open");
  assert.equal(opened.openedUntil, "2026-08-01T12:10:00.000Z");
  assert.deepEqual(nextImageCircuitState(opened, { ok: true }, { now }), {
    state: "closed",
    consecutiveFailures: 0,
    openedUntil: "",
    lastError: "",
  });
});
