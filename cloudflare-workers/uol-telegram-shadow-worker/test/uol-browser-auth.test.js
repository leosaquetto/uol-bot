import assert from "node:assert/strict";
import test from "node:test";

import {
  authorizationDiagnostics,
  authorizationExpiresAt,
} from "../src/uol-auth.js";

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

test("diagnóstico revela somente formato e nomes dos claims", () => {
  const payload = Buffer.from(JSON.stringify({ exp: 1_800_000_000, sub: "private-user" }))
    .toString("base64url");
  const diagnostics = authorizationDiagnostics(`header.${payload}.signature`);
  assert.equal(diagnostics.format, "jwt");
  assert.deepEqual(diagnostics.claimNames, ["exp", "sub"]);
  assert.equal(JSON.stringify(diagnostics).includes("private-user"), false);
});
