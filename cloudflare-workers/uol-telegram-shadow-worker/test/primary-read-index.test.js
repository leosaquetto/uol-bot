import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { classifyDeliveryRow } from "../src/delivery-state.js";

const source = readFileSync(new URL("../src/worker.js", import.meta.url), "utf8");

test("scan cannot release ambiguous main mutations by elapsed time", () => {
  assert.doesNotMatch(source, /releaseExpiredMainUnknowns\s*\(/);
});

test("an old main unknown stays quarantined despite unused attempts", () => {
  const result = classifyDeliveryRow({
    main_sent_at: "", main_delivery_attempts: 1,
    main_delivery_unknown_at: "2026-08-17T20:00:00.000Z",
    delivery_unknown_at: "2026-08-17T20:00:00.000Z", delivery_unknown_target: "main",
  }, { main: { ready: true }, canal2: { enabled: false }, discord: { enabled: false } }, {
    now: new Date("2026-10-15T12:00:00.000Z"), targetNames: ["main"],
  });
  assert.equal(result.state, "unknown");
  assert.deepEqual(result.actionable, []);
});
