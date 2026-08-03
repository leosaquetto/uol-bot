import assert from "node:assert/strict";
import test from "node:test";

import { authorizationExpiresAt } from "../src/uol-auth.js";

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
