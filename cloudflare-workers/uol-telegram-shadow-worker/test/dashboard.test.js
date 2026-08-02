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
    browserAuth: { autoRefreshConfigured: true },
    ticketApi: { lastOffersSeen: 3 },
    recent: [{
      title: "<script>alert(1)</script>",
      status: "delivered",
      firstSeenAt: "agora",
      detailQuality: "complete",
    }],
  });
  assert.match(html, /Clube UOL Monitor/);
  assert.match(html, /1\.2 s/);
  assert.doesNotMatch(html, /<script>alert/);
  assert.match(html, /&lt;script&gt;alert/);
});
