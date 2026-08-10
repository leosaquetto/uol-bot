import assert from "node:assert/strict";
import test from "node:test";

import { runReplayScenario } from "./replay-fixtures.js";

test("replay API-first entrega ingresso nos três destinos sem duplicata", () => {
  const result = runReplayScenario("api-to-all");
  assert.deepEqual(result.calls.map((call) => `${call.target}:${call.operation}`), [
    "main:send",
    "canal2:copy",
    "discord:send",
  ]);
  assert.equal(result.finalOffers[0].status, "delivered");
  assert.equal(result.events.filter((event) => event.state === "sent").length, 3);
  assert.equal(new Set(result.events.map((event) => event.dedupeKey)).size, result.events.length);
});

test("replay preserva a mesma mensagem quando a foto chega depois do prazo", () => {
  const result = runReplayScenario("late-image-upgrade");
  assert.deepEqual(result.calls.map((call) => call.operation), ["send_text", "edit_media"]);
  assert.equal(result.calls[0].messageId, result.calls[1].messageId);
  assert.equal(result.finalOffers[0].telegram_image_strategy, "photo_upgrade");
});

test("replay confirma esgotamento em duas sondas e edita Discord com duração", () => {
  const result = runReplayScenario("ticket-sold-out");
  assert.equal(result.finalOffers[0].status, "sold_out");
  assert.equal(result.calls.filter((call) => call.operation === "probe").length, 2);
  const discordEdit = result.calls.find((call) => call.target === "discord");
  assert.match(discordEdit.body, /Oferta esgotada às/);
  assert.match(discordEdit.body, /Ficou no ar por/);
});

test("replay reabre uma oferta esgotada e edita a mensagem Discord existente", () => {
  const result = runReplayScenario("restock");
  assert.equal(result.finalOffers[0].status, "delivered");
  assert.deepEqual(result.calls.map((call) => call.operation), ["sold_out_edit", "restock_edit"]);
  assert.equal(result.calls[0].messageId, result.calls[1].messageId);
});

test("replay não duplica depois de timeout ambíguo e resolve mensagem apagada", () => {
  const unknown = runReplayScenario("ambiguous-timeout");
  assert.equal(unknown.calls.filter((call) => call.operation === "send").length, 1);
  assert.equal(unknown.finalOffers[0].status, "delivery_unknown");
  const missing = runReplayScenario("deleted-message");
  assert.equal(missing.finalOffers[0].sold_out_synced, true);
  assert.equal(missing.events.at(-1).state, "resolved_missing");
});

test("replay de rajada mantém a primeira oferta dentro do SLO", () => {
  const result = runReplayScenario("burst-24");
  assert.equal(result.finalOffers.length, 24);
  assert.equal(result.calls[0].offerId, "burst-01");
  assert.ok(result.calls[0].elapsedMs <= 45_000);
  assert.equal(result.meta.budget.reason, "healthy");
});
