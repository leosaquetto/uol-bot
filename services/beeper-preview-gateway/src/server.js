import { createServer } from "node:http";
import { Readable } from "node:stream";

import { createGateway } from "./gateway.js";
import { startHeadlessRenderer } from "./headless-renderer.js";

const port = Number(process.env.PORT || 8787);
const host = String(process.env.HOST || "127.0.0.1");
const logger = console;
const headlessTransport = startHeadlessRenderer({
  baseUrl: process.env.BEEPER_API_URL || "http://127.0.0.1:23373",
  transportNonce: process.env.BEEPER_TRANSPORT_NONCE,
  accountId: process.env.BEEPER_ACCOUNT_ID,
  logger,
});
const handler = createGateway({
  token: process.env.GATEWAY_TOKEN,
  chatId: process.env.BEEPER_CHAT_ID,
  accountId: process.env.BEEPER_ACCOUNT_ID,
  beeperAccessToken: process.env.BEEPER_ACCESS_TOKEN,
  beeperApiUrl: process.env.BEEPER_API_URL,
  databasePath: process.env.DATA_PATH || "/var/lib/beeper-preview-gateway/deliveries.sqlite",
  isTransportReady: () => headlessTransport.isReady(),
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
      if (["/livez", "/readyz", "/v1/readyz", "/v1/send-offer"].includes(candidate)) {
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
