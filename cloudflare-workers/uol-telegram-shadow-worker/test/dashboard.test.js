import assert from "node:assert/strict";
import test from "node:test";

import { renderDashboard } from "../src/dashboard.js";

test("renderiza painel operacional sem permitir HTML vindo das ofertas", () => {
  const html = renderDashboard({
    mode: "live",
    operations: { activeIncidents: 0, incidents: [] },
    latency: { telegram: { p50Ms: 1_200 }, discord: { p50Ms: 500 } },
    sourceComparison: { apiWins: 1, listingWins: 0, paired: 1, deltaP50Ms: 700 },
    imageDelivery: { cacheEntries: 2, strategies: [] },
    authentication: { personalAuthorizationRequired: false },
    ticketApi: {
      lastOffersSeen: 3,
      applicationAuthorizationExpiresAt: "2026-10-23T04:04:28.000Z",
    },
    publicTicketListing: { lastOffersSeen: 1 },
    recent: [{
      title: "<script>alert(1)</script>",
      status: "delivered",
      firstSeenAt: "agora",
      detailQuality: "complete",
    }],
  });
  assert.match(html, /Clube UOL Monitor/);
  assert.match(html, /1\.2 s/);
  assert.match(html, /dispensado/);
  assert.match(html, /2026-10-23/);
  assert.doesNotMatch(html, /<script>alert/);
  assert.match(html, /&lt;script&gt;alert/);
});
