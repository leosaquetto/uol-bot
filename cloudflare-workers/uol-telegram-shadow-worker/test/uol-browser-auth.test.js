import assert from "node:assert/strict";
import test from "node:test";

import {
  authorizationExpiresAt,
  shouldAttemptAuthorizationRefresh,
} from "../src/uol-browser-auth.js";

function token(exp) {
  const payload = Buffer.from(JSON.stringify({ exp })).toString("base64url");
  return `header.${payload}.signature`;
}

test("lê expiração JWT sem expor ou validar o secret", () => {
  assert.equal(
    authorizationExpiresAt(token(1_800_000_000)),
    "2027-01-15T08:00:00.000Z",
  );
  assert.equal(authorizationExpiresAt("opaque-token"), "");
});

test("renova por 401 ou proximidade da expiração respeitando cooldown", () => {
  const now = new Date("2026-08-01T12:00:00.000Z");
  assert.equal(shouldAttemptAuthorizationRefresh({
    authorization: "opaque",
    apiError: "uol_api_http_401",
    now,
  }), true);
  assert.equal(shouldAttemptAuthorizationRefresh({
    authorization: token(Math.floor(now.getTime() / 1_000) + 30 * 60),
    now,
  }), true);
  assert.equal(shouldAttemptAuthorizationRefresh({
    authorization: "opaque",
    apiError: "uol_api_http_401",
    lastAttemptAt: "2026-08-01T11:30:00.000Z",
    now,
  }), false);
});
