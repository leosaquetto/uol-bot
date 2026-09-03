import { cleanText, isTicketCampaign, offerIdentityCompatible } from "./core.js";

export function compareOfferSources(apiCards = [], listingCards = []) {
  const apiTickets = apiCards.filter(isTicketCampaign);
  const listingTickets = listingCards.filter(isTicketCampaign);
  const apiOffersMissingFromListing = apiTickets.filter((card) =>
    !listingTickets.some((listingCard) => offerIdentityCompatible(card, listingCard)));
  const listingOffersMissingFromApi = listingTickets.filter((card) =>
    !apiTickets.some((apiCard) => offerIdentityCompatible(card, apiCard)));
  const matchedApi = apiTickets.length - apiOffersMissingFromListing.length;
  return {
    apiTickets: apiTickets.length,
    listingTickets: listingTickets.length,
    matchedApi,
    apiCoveragePercent: apiTickets.length
      ? Math.round((matchedApi / apiTickets.length) * 100)
      : 100,
    apiMissingFromListing: apiOffersMissingFromListing.length,
    listingMissingFromApi: listingOffersMissingFromApi.length,
    apiMissingTitles: apiOffersMissingFromListing
      .map((card) => cleanText(card.previewTitle).slice(0, 80)).slice(0, 5),
  };
}

export function sourceSnapshotSignature(cards = []) {
  return cards
    .map((card) => `${card.id}:${cleanText(card.previewTitle).toLowerCase()}`)
    .sort()
    .join("|")
    .slice(0, 12_000);
}

function percentile(values, ratio) {
  if (!values.length) return null;
  const ordered = [...values].sort((left, right) => left - right);
  return ordered[Math.ceil(ordered.length * ratio) - 1];
}

export function summarizeSourceComparison(rows = []) {
  const samples = rows.map((row) => {
    const apiAt = Date.parse(row.api_first_seen_at || "");
    const listingAt = Date.parse(row.listing_first_seen_at || "");
    const both = Number.isFinite(apiAt) && Number.isFinite(listingAt);
    const deltaMs = both ? Math.abs(apiAt - listingAt) : null;
    const winner = !both
      ? Number.isFinite(apiAt) ? "api_only" : "listing_only"
      : apiAt < listingAt ? "api" : listingAt < apiAt ? "listing" : "tie";
    return {
      id: row.offer_key,
      title: cleanText(row.title).slice(0, 120),
      winner,
      deltaMs,
      apiFirstSeenAt: row.api_first_seen_at || "",
      listingFirstSeenAt: row.listing_first_seen_at || "",
    };
  });
  const paired = samples.filter((sample) => Number.isFinite(sample.deltaMs));
  const deltas = paired.map((sample) => sample.deltaMs);
  return {
    samples: samples.length,
    paired: paired.length,
    apiWins: paired.filter((sample) => sample.winner === "api").length,
    listingWins: paired.filter((sample) => sample.winner === "listing").length,
    ties: paired.filter((sample) => sample.winner === "tie").length,
    apiOnly: samples.filter((sample) => sample.winner === "api_only").length,
    listingOnly: samples.filter((sample) => sample.winner === "listing_only").length,
    deltaP50Ms: percentile(deltas, 0.5),
    deltaP95Ms: percentile(deltas, 0.95),
    deltaMaxMs: deltas.length ? Math.max(...deltas) : null,
    latest: samples.slice(0, 12),
  };
}
