import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  buildInitRequest,
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

test("inicializa a conta uma vez sem fabricar confirmação de envio", async () => {
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

  assert.deepEqual(methods, ["init"]);
  assert.equal("sendMessage" in transport, false);
  transport.stop();
});
