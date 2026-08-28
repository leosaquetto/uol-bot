import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  buildInitRequest,
  buildSendMessageRequest,
  startHeadlessRenderer,
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

test("injeta exatamente o preview antigo no envio interno", () => {
  const request = buildSendMessageRequest({
    accountId: "local-whatsapp_ba_example",
    bridgeId: "local-whatsapp",
    chatId: "!group:local-whatsapp.localhost",
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
  assert.equal(request.routeData.args[0], "!group:local-whatsapp.localhost");
  assert.deepEqual(request.routeData.args[1].links, [{
    link: "https://clube.uol.com.br/oferta",
    title: "Oferta Clube UOL",
    summary: "Resumo",
    type: "website",
    img: "file:///var/lib/beeper-preview-gateway/previews/test.jpg",
    imgSize: undefined,
    imgType: "image/jpeg",
  }]);
  assert.deepEqual(request.routeData.args[2], {
    pendingMessageID: "~txn:network:TEST",
  });
});

test("renova a conta, envia o card e devolve só o ID pendente", async () => {
  const methods = [];
  const codec = {
    decode: (raw) => JSON.parse(Buffer.from(raw).toString("utf8")),
    encode: (value) => Buffer.from(JSON.stringify(value)),
  };
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;

    constructor() {
      super();
      this.readyState = 0;
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN;
        this.emit("open");
      });
    }

    send(raw, callback) {
      const request = codec.decode(raw);
      methods.push(request.routeData.methodName);
      callback?.();
      queueMicrotask(() => this.emit("message", codec.encode({
        type: "response",
        reqID: request.reqID,
        data: { result: {} },
      })));
    }

    close() {
      if (this.readyState !== FakeWebSocket.OPEN) return;
      this.readyState = 3;
      this.emit("close");
    }
  }

  const transport = startHeadlessRenderer({
    baseUrl: "http://127.0.0.1:23374",
    transportNonce: "test-nonce",
    accountId: "local-whatsapp_ba_example",
    WebSocketImpl: FakeWebSocket,
    codecFactory: () => codec,
    logger: {},
  });
  for (let attempt = 0; attempt < 20 && !transport.isReady(); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  assert.equal(transport.isReady(), true);

  const result = await transport.sendMessage({
    chatId: "!group:local-whatsapp.localhost",
    text: "Oferta\nhttps://clube.uol.com.br/oferta",
    preview: {
      link: "https://clube.uol.com.br/oferta",
      title: "Oferta Clube UOL",
    },
  });
  assert.deepEqual(methods, ["init", "init", "sendMessage"]);
  assert.match(result.pendingMessageID, /^~txn:network:[A-F0-9]{32}$/);
  transport.stop();
});
