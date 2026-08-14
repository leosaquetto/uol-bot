import assert from "node:assert/strict";
import test from "node:test";

import { buildInitRequest } from "../src/headless-renderer.js";

test("inicializa a conta local no transporte headless do Beeper", () => {
  const accountId = "local-whatsapp_ba_example";
  const request = buildInitRequest({
    accountId,
    bridgeId: "local-whatsapp",
    bridgeType: "whatsapp",
    bridgeProvider: "local",
  });
  assert.equal(request.routeName, "platform");
  assert.equal(request.routeData.platformName, "bridge-local-whatsapp");
  assert.equal(request.routeData.accountID, accountId);
  assert.equal(request.routeData.methodName, "init");
  assert.equal(request.routeData.args[0].remoteID, "ba_example");
  assert.deepEqual(request.routeData.args[1], {
    accountID: accountId,
    bridgeID: "local-whatsapp",
    bridgeType: "whatsapp",
    bridgeProvider: "local",
  });
});
