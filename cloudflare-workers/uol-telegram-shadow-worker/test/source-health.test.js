import assert from "node:assert/strict";
import test from "node:test";

import {
  compareOfferSources,
  sourceSnapshotSignature,
  summarizeSourceComparison,
} from "../src/source-health.js";

const api = [{
  id: "show-a",
  link: "https://clube.uol.com.br/campanhasdeingresso/pAA-show-a",
  previewTitle: "Show A",
}];

test("mede cobertura atual entre API e HTML", () => {
  const comparison = compareOfferSources(api, [...api, {
    id: "show-b",
    link: "https://clube.uol.com.br/campanhasdeingresso/pAB-show-b",
    previewTitle: "Show B",
  }]);
  assert.equal(comparison.apiCoveragePercent, 100);
  assert.equal(comparison.listingMissingFromApi, 1);
  assert.equal(sourceSnapshotSignature(api), "show-a:show a");
});

test("resume qual fonte descobriu primeiro e a diferença", () => {
  const summary = summarizeSourceComparison([{
    offer_key: "show-a",
    title: "Show A",
    api_first_seen_at: "2026-08-01T12:00:00.000Z",
    listing_first_seen_at: "2026-08-01T12:00:15.000Z",
  }, {
    offer_key: "show-b",
    title: "Show B",
    api_first_seen_at: "",
    listing_first_seen_at: "2026-08-01T13:00:00.000Z",
  }]);
  assert.equal(summary.apiWins, 1);
  assert.equal(summary.listingOnly, 1);
  assert.equal(summary.deltaP50Ms, 15_000);
});
