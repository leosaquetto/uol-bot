import { createServer } from "node:http";
import { Readable } from "node:stream";

import { createDeliveryConfirmation } from "./delivery-confirmation.js";
import { createGateway } from "./gateway.js";
import { startHeadlessRenderer } from "./headless-renderer.js";
import { createDropsTransport } from "./drops-transport.js";

const port = Number(process.env.PORT || 8787);
const host = String(process.env.HOST || "127.0.0.1");
const logger = console;
const useDrops = process.env.GATEWAY_DELIVERY_TRANSPORT === "baileys";
if (process.env.GATEWAY_DELIVERY_TRANSPORT && !["baileys","beeper"].includes(process.env.GATEWAY_DELIVERY_TRANSPORT)) throw new Error("invalid_delivery_transport");
const drops = useDrops ? createDropsTransport({token:process.env.DROPS_GATEWAY_TOKEN}) : null;
const headlessTransport = useDrops ? null : startHeadlessRenderer({
  baseUrl: process.env.BEEPER_API_URL || "http://127.0.0.1:23373",
  transportNonce: process.env.BEEPER_TRANSPORT_NONCE,
  accountId: process.env.BEEPER_ACCOUNT_ID,
  logger,
});
const deliveryConfirmation = useDrops ? null : createDeliveryConfirmation({
  databasePath: process.env.BEEPER_INDEX_DB_PATH,
  chatId: process.env.BEEPER_CHAT_ID,
});
const handler = createGateway({
  token: process.env.GATEWAY_TOKEN,
  chatId: process.env.BEEPER_CHAT_ID,
  buyticketChatId: process.env.BEEPER_BUYTICKET_CHAT_ID,
  tvgloboToken: process.env.TVGLOBO_TOKEN,
  selfChatId: process.env.BEEPER_SELF_CHAT_ID,
  accountId: process.env.BEEPER_ACCOUNT_ID,
  beeperAccessToken: process.env.BEEPER_ACCESS_TOKEN,
  beeperApiUrl: process.env.BEEPER_API_URL,
  databasePath: process.env.DATA_PATH || "/var/lib/beeper-preview-gateway/deliveries.sqlite",
  transport:useDrops?"baileys":"beeper",
  probeTransport:useDrops?()=>drops.readiness():undefined,
  sendMessageImpl: (message) => useDrops?drops.sendMessage(message):headlessTransport.sendMessage(message),
  confirmDeliveryImpl: (delivery) => useDrops?drops.confirmDelivery(delivery):deliveryConfirmation.waitForDelivery(delivery),
  isTransportReady: () => useDrops||headlessTransport.isReady(),
  isDeliveryConfirmationReady: () => useDrops||deliveryConfirmation.isReady(),
  logger,
});

createServer(async (request, response) => {
  try {
    const body = ["GET", "HEAD"].includes(request.method || "")
      ? undefined
      : Readable.toWeb(request);
    const upstream = await handler(new Request(
      `http://${request.headers.host || `${host}:${port}`}${request.url || "/"}`,
      {
        method: request.method,
        headers: request.headers,
        body,
        duplex: body ? "half" : undefined,
      },
    ));
    response.writeHead(upstream.status, Object.fromEntries(upstream.headers));
    response.end(Buffer.from(await upstream.arrayBuffer()));
  } catch (error) {
    let path = "other";
    try {
      const candidate = new URL(
        request.url || "/",
        `http://${request.headers.host || "localhost"}`,
      ).pathname;
      if (["/livez", "/readyz", "/v1/readyz", "/v1/send-offer", "/v1/send-buyticket", "/v1/send-tvglobo", "/v1/send-x-post"].includes(candidate)) {
        path = candidate;
      }
    } catch {}
    logger.error(JSON.stringify({
      event: "beeper_gateway_internal_error",
      method: request.method,
      path,
      status: 500,
      errorType: String(error?.name || "Error"),
    }));
    response.writeHead(500, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ code: "internal_error" }));
  }
}).listen(port, host);
