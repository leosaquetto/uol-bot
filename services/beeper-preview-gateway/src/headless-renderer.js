import { randomUUID } from "node:crypto";

import { Encoder } from "cbor-x";
import WebSocket from "ws";

const RECONNECT_DELAY_MS = 2_000;

function websocketUrl(baseUrl, transportNonce) {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `/${transportNonce}/cbor-records`;
  url.search = "";
  return url.toString();
}

export function buildInitRequest({ accountId, bridgeId, bridgeType, bridgeProvider }) {
  const prefix = `${bridgeId}_`;
  if (!accountId.startsWith(prefix)) throw new Error("BEEPER_ACCOUNT_ID does not match bridge");
  const remoteId = accountId.slice(prefix.length);
  const session = {
    accountID: accountId,
    remoteID: remoteId,
    bridgeID: bridgeId,
    bridgeType,
    bridgeProvider,
  };
  return {
    reqID: randomUUID(),
    routeName: "platform",
    routeData: {
      platformName: `bridge-${bridgeId}`,
      accountID: accountId,
      methodName: "init",
      args: [session, {
        accountID: accountId,
        bridgeID: bridgeId,
        bridgeType,
        bridgeProvider,
      }, {}],
    },
  };
}

export function startHeadlessRenderer({
  baseUrl,
  transportNonce,
  accountId,
  bridgeId = "local-whatsapp",
  bridgeType = "whatsapp",
  bridgeProvider = "local",
  WebSocketImpl = WebSocket,
  reconnectDelayMs = RECONNECT_DELAY_MS,
  logger = console,
}) {
  if (!transportNonce || !accountId) return { stop() {} };
  const url = websocketUrl(baseUrl, transportNonce);
  let socket;
  let reconnectTimer;
  let stopped = false;

  const connect = () => {
    if (stopped) return;
    const codec = new Encoder({ useRecords: true, bundleStrings: true });
    const request = buildInitRequest({ accountId, bridgeId, bridgeType, bridgeProvider });
    socket = new WebSocketImpl(url);
    socket.on("open", () => socket.send(codec.encode(request)));
    socket.on("message", (raw) => {
      try {
        const message = codec.decode(raw);
        if (message?.type !== "response" || message?.reqID !== request.reqID) return;
        if (message?.data?.error) {
          logger.error("Beeper headless account initialization failed");
          socket.close();
          return;
        }
        logger.log("Beeper headless account initialized");
      } catch {
        logger.error("Beeper headless transport decode failed");
        socket.close();
      }
    });
    socket.on("error", () => socket.close());
    socket.on("close", () => {
      if (!stopped) reconnectTimer = setTimeout(connect, reconnectDelayMs);
    });
  };

  connect();
  return {
    stop() {
      stopped = true;
      clearTimeout(reconnectTimer);
      socket?.close();
    },
  };
}
