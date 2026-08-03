import assert from "node:assert/strict";
import test from "node:test";

import { imageDeadline } from "../src/image-deadline.js";

test("mantém prazo absoluto desde a primeira detecção", () => {
  assert.deepEqual(
    imageDeadline(
      "2026-08-03T12:00:00.000Z",
      new Date("2026-08-03T12:00:45.000Z"),
      60,
    ),
    {
      deadlineAt: "2026-08-03T12:01:00.000Z",
      expired: false,
      remainingMs: 15_000,
    },
  );
});

test("expira exatamente aos 60 segundos sem renovar prazo", () => {
  assert.deepEqual(
    imageDeadline(
      "2026-08-03T12:00:00.000Z",
      new Date("2026-08-03T12:01:00.000Z"),
      60,
    ),
    {
      deadlineAt: "2026-08-03T12:01:00.000Z",
      expired: true,
      remainingMs: 0,
    },
  );
});

test("timestamp inválido falha fechado como prazo expirado", () => {
  assert.deepEqual(
    imageDeadline("inválido", new Date("2026-08-03T12:00:00.000Z"), 60),
    { deadlineAt: "", expired: true, remainingMs: 0 },
  );
});
