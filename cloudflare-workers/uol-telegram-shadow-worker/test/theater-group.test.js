import assert from "node:assert/strict";
import test from "node:test";
import { buildTelegramCaption } from "../src/core.js";
import { buildDiscordPayload } from "../src/discord.js";
import { buildBeeperOfferText } from "../src/beeper.js";
import {
  buildGroupedOffers, isTheaterHeld, isTheaterLink, parseGroupedOffers,
  theaterGroupKey, theaterHoldSeconds,
} from "../src/theater-group.js";

const base = "https://clube.uol.com.br/campanhasdeingresso";
const tigre = (code, date, extra = {}) => ({
  id: `tigre-${date}`, first_seen_at: `2026-11-13T13:0${date % 10}:00.000Z`,
  title: `2 INGRESSOS: ${date}/11 Comédia O Tigre - RJ`,
  link: `${base}/${code}-2-ingressos-${date}-11-comedia-o-tigre-rj-teatro-vanucci-rj`,
  image_url: "https://img.uol/tigre.jpg?v=1", ...extra,
});

test("somente links com teatro no slug são retidos", () => {
  assert.equal(isTheaterLink(tigre("pWS", 14).link), true);
  assert.equal(isTheaterLink(`${base}/pWJ-2-ingressos-14-11-tokio-marine-hall-sp`), false);
  assert.equal(isTheaterLink("https://clube.uol.com.br/beneficios/teatro-x"), false);
});

test("retenção expira após o tempo configurado e pode ser desligada", () => {
  const row = tigre("pWS", 14);
  const seen = Date.parse(row.first_seen_at);
  assert.equal(theaterHoldSeconds({}), 90);
  assert.equal(isTheaterHeld(row, new Date(seen + 89_000)), true);
  assert.equal(isTheaterHeld(row, new Date(seen + 90_000)), false);
  assert.equal(isTheaterHeld(row, new Date(seen), { THEATER_GROUP_HOLD_SECONDS: "0" }), false);
  const show = { ...row, link: `${base}/pWJ-2-ingressos-14-11-tokio-marine-hall-sp` };
  assert.equal(isTheaterHeld(show, new Date(seen)), false);
});

test("mesmo título sem data e mesma imagem agrupam, apesar do código diferente", () => {
  assert.equal(theaterGroupKey(tigre("pWS", 14)), theaterGroupKey(tigre("pWT", 15)));
});

test("mesmo título e mesmo código com imagens diferentes não agrupam", () => {
  const a = { title: "2 INGRESSOS: 15/11 Teatro SP", link: `${base}/pWJ-2-ingressos-teatro-a`, image_url: "a.jpg" };
  const b = { title: "2 INGRESSOS: 14/11 Teatro SP", link: `${base}/pWJ-2-ingressos-teatro-b`, image_url: "b.jpg" };
  assert.notEqual(theaterGroupKey(a), theaterGroupKey(b));
});

test("sem imagem, o slug sem data e sem código decide", () => {
  const a = tigre("pWS", 14, { image_url: "" });
  const b = tigre("pWT", 15, { image_url: "" });
  const c = tigre("pWU", 21, { image_url: "", link: `${base}/pWU-2-ingressos-21-11-outra-peca-teatro-x` });
  assert.equal(theaterGroupKey(a), theaterGroupKey(b));
  assert.notEqual(theaterGroupKey(a), theaterGroupKey(c));
});

test("mensagens agrupadas listam todas as datas nos três destinos", () => {
  const group = buildGroupedOffers([tigre("pWT", 15), tigre("pWS", 14)]);
  assert.equal(group.title, "2 INGRESSOS: Comédia O Tigre - RJ");
  assert.deepEqual(group.items.map((item) => item.label), ["14/11", "15/11"]);
  const offer = { ...tigre("pWS", 14), groupedOffers: parseGroupedOffers(JSON.stringify(group)) };
  const telegram = buildTelegramCaption(offer);
  assert.match(telegram, /14\/11 ➜ https/);
  assert.match(telegram, /15\/11 ➜ https/);
  const beeper = buildBeeperOfferText(offer);
  assert.equal(beeper.split("\n").length, 3);
  const discord = buildDiscordPayload(offer, { soldOutAt: "2026-11-13T14:00:00Z" });
  assert.match(discord.content, /15\/11 ➜ https/);
  assert.doesNotMatch(discord.content, /ESGOTADO/);
});

test("oferta isolada mantém o formato original", () => {
  const offer = { ...tigre("pWS", 14), groupedOffers: null };
  assert.match(buildBeeperOfferText(offer), /^🚨 2 INGRESSOS: 14\/11/);
  assert.equal(parseGroupedOffers('{"mergedInto":"x"}'), null);
});
