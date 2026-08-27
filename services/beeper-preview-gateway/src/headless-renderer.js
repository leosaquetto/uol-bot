import { randomUUID } from "node:crypto";

import { Encoder } from "cbor-x";
import WebSocket from "ws";

const RECONNECT_DELAY_MS = 2_000;
const REQUEST_TIMEOUT_MS = 20_000;

function transportLog(logger, level, event, fields = {}) {
  const write = logger?.[level] || logger?.log;
  if (typeof write !== "function") return;
  write.call(logger, JSON.stringify({ event, ...fields }));
}

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

export function buildSendMessageRequest({
  accountId,
  bridgeId,
  chatId,
  text,
  preview,
  pendingMessageId,
}) {
  return {
    reqID: randomUUID(),
    routeName: "platform",
    routeData: {
      platformName: `bridge-${bridgeId}`,
      accountID: accountId,
      methodName: "sendMessage",
      args: [chatId, {
        text,
        links: preview ? [{
          link: preview.link,
          title: preview.title,
          summary: preview.summary,
          type: preview.type || "website",
          img: preview.img,
          imgSize: preview.imgSize,
          imgType: preview.imgType,
        }] : [],
      }, { pendingMessageID: pendingMessageId }],
    },
  };
}

function transportError(message, ambiguous = false) {
  const error = new Error(message);
  error.ambiguous = ambiguous;
  return error;
}

export function startHeadlessRenderer({
  baseUrl,
  transportNonce,
  accountId,
  bridgeId = "local-whatsapp",
  bridgeType = "whatsapp",
  bridgeProvider = "local",
  WebSocketImpl = WebSocket,
  codecFactory = () => new Encoder({ useRecords: true, bundleStrings: true }),
  reconnectDelayMs = RECONNECT_DELAY_MS,
  requestTimeoutMs = REQUEST_TIMEOUT_MS,
  logger = console,
}) {
  if (!transportNonce || !accountId) {
    return {
      isReady: () => false,
      async sendMessage() {
        throw transportError("Beeper headless transport is not configured");
      },
      stop() {},
    };
  }
  const url = websocketUrl(baseUrl, transportNonce);
  const pending = new Map();
  let socket;
  let codec;
  let reconnectTimer;
  let refreshInFlight;
  let stopped = false;
  let ready = false;

  const rejectPending = (message) => {
    for (const { reject, timer, ambiguous } of pending.values()) {
      clearTimeout(timer);
      reject(transportError(message, ambiguous));
    }
    pending.clear();
  };

  const sendRequest = (request, ambiguous = false) => new Promise((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocketImpl.OPEN) {
      reject(transportError("Beeper headless transport is not connected"));
      return;
    }
    const timer = setTimeout(() => {
      pending.delete(request.reqID);
      reject(transportError("Beeper headless transport timed out", ambiguous));
    }, requestTimeoutMs);
    pending.set(request.reqID, { resolve, reject, timer, ambiguous });
    try {
      socket.send(codec.encode(request), (error) => {
        if (!error) return;
        const waiter = pending.get(request.reqID);
        if (!waiter) return;
        pending.delete(request.reqID);
        clearTimeout(waiter.timer);
        waiter.reject(transportError("Beeper headless transport write failed", ambiguous));
      });
    } catch {
      pending.delete(request.reqID);
      clearTimeout(timer);
      reject(transportError("Beeper headless transport write failed", ambiguous));
    }
  });

  const refreshAccountSession = () => {
    if (refreshInFlight) return refreshInFlight;
    const request = buildInitRequest({ accountId, bridgeId, bridgeType, bridgeProvider });
    refreshInFlight = sendRequest(request)
      .finally(() => {
        refreshInFlight = undefined;
      });
    return refreshInFlight;
  };

  const connect = () => {
    if (stopped) return;
    ready = false;
    codec = codecFactory();
    socket = new WebSocketImpl(url);
    socket.on("open", async () => {
      try {
        await refreshAccountSession();
        ready = true;
        transportLog(logger, "info", "beeper_transport_initialized");
      } catch (error) {
        transportLog(logger, "error", "beeper_transport_initialization_failed", {
          errorType: String(error?.name || "Error"),
        });
        socket.close();
      }
    });
    socket.on("message", (raw) => {
      try {
        const message = codec.decode(raw);
        if (message?.type !== "response") return;
        const waiter = pending.get(message.reqID);
        if (!waiter) return;
        pending.delete(message.reqID);
        clearTimeout(waiter.timer);
        const result = message?.data?.result ?? message?.data;
        const error = message?.data?.error || (result?.errorName ? result : null);
        if (error) waiter.reject(transportError(error.errorMessage || "Beeper transport rejected request"));
        else waiter.resolve(result);
      } catch (error) {
        transportLog(logger, "error", "beeper_transport_decode_failed", {
          errorType: String(error?.name || "Error"),
        });
        socket.close();
      }
    });
    socket.on("error", () => socket.close());
    socket.on("close", () => {
      ready = false;
      rejectPending("Beeper headless transport disconnected");
      transportLog(logger, "warn", "beeper_transport_disconnected", { reconnecting: !stopped });
      if (!stopped) reconnectTimer = setTimeout(connect, reconnectDelayMs);
    });
  };

  connect();
  return {
    isReady() {
      return ready && socket?.readyState === WebSocketImpl.OPEN;
    },
    async sendMessage({ chatId, text, preview }) {
      if (!ready) throw transportError("Beeper headless transport is not ready");
      try {
        await refreshAccountSession();
        transportLog(logger, "info", "beeper_transport_account_refreshed");
      } catch (error) {
        ready = false;
        transportLog(logger, "error", "beeper_transport_account_refresh_failed", {
          errorType: String(error?.name || "Error"),
        });
        socket?.close();
        throw error;
      }
      const pendingMessageId = `~txn:network:${randomUUID().replaceAll("-", "").toUpperCase()}`;
      const request = buildSendMessageRequest({
        accountId,
        bridgeId,
        chatId,
        text,
        preview,
        pendingMessageId,
      });
      await sendRequest(request, true);
      transportLog(logger, "info", "beeper_transport_message_accepted", {
        confirmation: "accepted_by_beeper_transport",
      });
      return { accepted: true, pendingMessageID: pendingMessageId };
    },
    stop() {
      stopped = true;
      ready = false;
      clearTimeout(reconnectTimer);
      rejectPending("Beeper headless transport stopped");
      socket?.close();
    },
  };
}
