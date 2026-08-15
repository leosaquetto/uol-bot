import assert from "node:assert/strict";
import test from "node:test";

import {
  buildInitRequest,
  buildSendMessageRequest,
} from "../src/headless-renderer.js";

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

test("injeta o preview no envio interno do Beeper", () => {
  const request = buildSendMessageRequest({
    accountId: "local-whatsapp_ba_example",
    bridgeId: "local-whatsapp",
    chatId: "!personal:local-whatsapp.localhost",
    text: "Oferta\nhttps://clube.uol.com.br/oferta",
    pendingMessageId: "~txn:network:TEST",
    preview: {
      link: "https://clube.uol.com.br/oferta",
      title: "Oferta Clube UOL",
      summary: "Resumo",
      type: "website",
      img: "file:///var/lib/beeper-preview-gateway/previews/test.jpg",
      imgType: "image/jpeg",
    },
  });
  assert.equal(request.routeData.methodName, "sendMessage");
  assert.equal(request.routeData.args[0], "!personal:local-whatsapp.localhost");
  assert.equal(request.routeData.args[1].links[0].title, "Oferta Clube UOL");
  assert.equal(request.routeData.args[1].links[0].imgType, "image/jpeg");
  assert.equal(request.routeData.args[2].pendingMessageID, "~txn:network:TEST");
});
