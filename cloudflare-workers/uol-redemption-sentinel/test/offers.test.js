import test from 'node:test';
import assert from 'node:assert/strict';
import { matchOffer, normalizeText, parseCatalog, parseHistory, parseOffer } from '../src/offers.js';

const url = 'https://clube.uol.com.br/campanhasdeingresso/pZY-2-ingressos-10-10-nubank-parque-sp';
const campaign = { artistAliases: ['zayn'], date: '2026-10-10', quantity: 2, venueAliases: ['nubank parque'], city: 'São Paulo', category: 'campanhasdeingresso' };
function detail({ title = '2 INGRESSOS 10/10 Nubank Parque SP', description = '<p>Resgate <strong>1 par de ingressos</strong> para curtir <strong>Zayn</strong> em São Paulo.</p><p>Data: 10 de outubro de 2026.</p><p>Local: Nubank Parque.</p>', action = `${url}/resgatar`, extra = '', canonical = url, button = 'Utilizar este benefício' } = {}) {
  return `<html><head><link rel="canonical" href="${canonical}"><meta property="og:url" content="${canonical}"></head><body>
  <script>const injection = '</div><h2>Zayn</h2>';</script>
  <div id="beneficio"><h2>${title}</h2><div id="ilustracoes"><div class="thumb-image"><img src="https://images.example/artwork.png"></div></div>
  <div class="detalhes"><a id="rescue_button" href="${action}">${button}</a></div>
  <div class="descricao"><div class="info-beneficio">${description}</div><p>Benefício válido de 28/09/2026 até 10/10/2026.</p></div>${extra}</div>
  <aside><h2>Zayn</h2><p>2 ingressos 10/10 Nubank Parque SP</p><a href="/other/resgatar">Utilizar este benefício</a></aside></body></html>`;
}

test('known detail scope excludes validity dates and neighbouring offers', () => {
  const parsed = parseOffer(detail(), url);
  assert.equal(parsed.available, true);
  assert.equal(parsed.redeemUrl, `${url}/resgatar`);
  assert.equal(parsed.imageUrl, 'https://images.example/artwork.png');
  assert.equal(parsed.description.includes('28/09'), false);
  assert.equal(matchOffer(parsed, campaign).ok, true);
});

test('observed Casa Natura title/description disagreement blocks even with matching artist', () => {
  const parsed = parseOffer(detail({ description: '<p>Zayn. 2 ingressos no Nubank Parque, São Paulo.</p><p>Data: 08 de setembro de 2026.</p>' }), url);
  const result = matchOffer(parsed, campaign);
  assert.equal(result.ok, false);
  assert.ok(result.reasons.includes('EVENT_DATE_CONFLICT'));
});

test('venue and date alone do not identify performer; external text cannot supply artist', () => {
  const parsed = parseOffer(detail({ description: '<p>1 par de ingressos no Nubank Parque, São Paulo. Data: 10/10/2026.</p>' }), url);
  for (const offer of [parsed, { ...parsed, text: `${parsed.text} ZAYN`, artworkText: 'ZAYN THE KONNAKOL TOUR' }]) {
    const result = matchOffer(offer, campaign, 'ZAYN THE KONNAKOL TOUR');
    assert.equal(result.ok, false);
    assert.ok(result.reasons.includes('ARTIST_MISSING'));
    assert.equal(result.evidence.artistSource, null);
  }
});

test('artist must appear literally in the scoped offer title or description', () => {
  const parsed = parseOffer(detail({ title: 'ZAYN — 2 INGRESSOS 10/10 Nubank Parque SP', description: '<p>1 par de ingressos no Nubank Parque, São Paulo. Data: 10/10/2026.</p>' }), url);
  const result = matchOffer(parsed, campaign);
  assert.equal(result.ok, true);
  assert.equal(result.evidence.artistSource, 'offer_text');
  const differentArtist = parseOffer(detail({ description: '<p>Zaynation, 1 par de ingressos no Nubank Parque, São Paulo. Data: 10/10/2026.</p>' }), url);
  assert.ok(matchOffer(differentArtist, campaign).reasons.includes('ARTIST_MISSING'));
});

test('all explicit event dates must agree, including year and multiple dates', () => {
  for (const date of ['10/10/2027', '09/10/2026', '10/09/2026', '10 de outubro de 2025', '10/10/2026 ou 11/10/2026']) {
    const parsed = parseOffer(detail({ description: `<p>Zayn, 2 ingressos, Nubank Parque, São Paulo. Data: ${date}</p>` }), url);
    assert.ok(matchOffer(parsed, campaign).reasons.includes('EVENT_DATE_CONFLICT'), date);
  }
  const noDate = parseOffer(detail({ title: '2 INGRESSOS Nubank Parque SP', description: '<p>Zayn, Nubank Parque, São Paulo.</p>' }), url);
  assert.ok(matchOffer(noDate, campaign).reasons.includes('EVENT_DATE_MISSING'));
  const range = parseOffer(detail({ description: '<p>Zayn, 2 ingressos no Nubank Parque, São Paulo, nos dias 9 e 10 de outubro de 2026.</p>' }), url);
  assert.ok(matchOffer(range, campaign).reasons.includes('EVENT_DATE_AMBIGUOUS'));
});

test('different ticket quantities and discounts cannot spend the monthly quota', () => {
  for (const title of ['1 INGRESSO 10/10 Nubank Parque SP', '3 INGRESSOS 10/10 Nubank Parque SP', '30% desconto 10/10 Nubank Parque SP', 'INGRESSOS 10/10 Nubank Parque SP']) {
    const parsed = parseOffer(detail({ title, description: '<p>Zayn no Nubank Parque, São Paulo. Data: 10/10/2026.</p>' }), url);
    assert.equal(matchOffer(parsed, campaign).ok, false, title);
  }
  const contradiction = parseOffer(detail({ description: '<p>Zayn: 1 ingresso individual no Nubank Parque, São Paulo, dia 10/10/2026.</p>' }), url);
  assert.ok(matchOffer(contradiction, campaign).reasons.includes('QUANTITY_MISMATCH'));
});

test('anonymous login redirect, disabled CTA and sold-out state are not redeemable', () => {
  const login = parseOffer(detail({ action: `/auth/uol/login?redirect_uri=${encodeURIComponent(new URL(url).pathname + '/resgatar')}` }), url);
  assert.equal(login.requiresLogin, true);
  assert.equal(login.redeemUrl, null);
  assert.equal(login.available, false);
  const soldOut = parseOffer(detail({ extra: '<p>Esgotado</p>', button: 'Benefício esgotado' }), url);
  assert.equal(soldOut.available, false);
  const disabled = parseOffer(detail().replace('id="rescue_button"', 'id="rescue_button" class="disabled"'), url);
  assert.equal(disabled.available, false);
});

test('malformed or mismatched identity fails closed', () => {
  for (const html of [detail({ canonical: `${url}-other` }), detail({ action: `${url}-other/resgatar` }), detail().replace('id="beneficio"', 'id="unknown"'), detail({ extra: '<h2>Another title</h2>' }), detail({ extra: '<div class="info-beneficio">Zayn</div>' })]) {
    assert.ok(matchOffer(parseOffer(html, url), campaign).reasons.includes('IDENTITY_UNVERIFIED'));
  }
  const wrong = parseOffer(detail({ canonical: url.replace('campanhasdeingresso', 'descontos') }), url.replace('campanhasdeingresso', 'descontos'));
  assert.ok(matchOffer(wrong, campaign).reasons.includes('WRONG_CATEGORY_OR_URL'));
});

test('scripts, comments, hidden markup and adjacent offers cannot supply artist evidence', () => {
  const html = detail({ description: '<p>2 ingressos no Nubank Parque SP, dia 10/10/2026.</p><script>"Zayn"</script><!-- Zayn --><span hidden>Zayn</span><p class="d-none">Zayn</p><img alt="text > Zayn">' });
  const parsed = parseOffer(html, url);
  assert.equal(parsed.description.toLowerCase().includes('zayn'), false);
  assert.ok(matchOffer(parsed, campaign).reasons.includes('ARTIST_MISSING'));
});

test('catalog selects each category card once and never parses navigation or redemption URLs', () => {
  const card = (target, title = '2 INGRESSOS 10/10 Nubank Parque SP') => `<div class="beneficio" data-categoria="Ingressos Exclusivos"><a href="${target}"><div class="parceiro-beneficio"><img src="https://images.example/logo.png"></div><div class="imagem-beneficio"><div data-src="https://images.example/poster.png"></div></div><p class="titulo">${title}</p><a href="${target}">Utilizar benefício</a></a></div>`;
  const html = `<a href="${url}-nav">Zayn</a><section id="beneficios">${card(url)}${card(`${url}/resgatar`)}${card('https://evil.example/campanhasdeingresso/foo')}${card(url)}</section><aside>${card(`${url}-other`)}</aside>`;
  assert.deepEqual(parseCatalog(html), [{ url, title: '2 INGRESSOS 10/10 Nubank Parque SP', imageUrl: 'https://images.example/poster.png' }]);
});

test('history keeps voucher identity distinct from offer identity and never invents redemption date', () => {
  const html = `<nav>Olá, LEONARDO!</nav><h1>Meus Resgates</h1><section id="beneficios"><div class="beneficio"><div class="thumb"><img src="https://images.example/voucher.png"></div><p class="parceiro">Campanhas de ingressos</p><p class="titulo">2 INGRESSOS 08/09 Nubank Parque SP</p><a href="/perfil/beneficios/123">Visualizar benefício</a></div></section>`;
  const result = parseHistory(html);
  assert.equal(result.authenticated, true);
  assert.equal(result.signedInName, 'LEONARDO');
  assert.equal(result.entries[0].id, '123');
  assert.equal(result.entries[0].redemptionDate, null);
  assert.equal(result.entries[0].offerUrl, null);
  assert.equal(result.entries[0].category, 'campanhasdeingresso');
  assert.equal(result.hasPagination, false);
  assert.equal(result.redemptionDatesObservable, false);
});

test('history exposes pagination without claiming the visible records are complete', () => {
  const html = '<header>Olá, LEONARDO!</header><h2>Meus Resgates</h2><div id="beneficios"></div><nav class="pagination"><a href="/perfil/beneficios?page=2">2</a></nav>';
  const result = parseHistory(html);
  assert.equal(result.authenticated, true);
  assert.equal(result.hasPagination, true);
  assert.deepEqual(result.paginationUrls, ['https://clube.uol.com.br/perfil/beneficios?page=2']);
  assert.equal(parseHistory('<h2>Meus Resgates</h2><div id="beneficios"></div>').authenticated, false);
});

test('accent normalization, phrase boundaries, and escaped HTML preserve matching', () => {
  assert.equal(normalizeText(' S&atilde;o PAULO '), 'sao paulo');
  const parsed = parseOffer(detail({ description: '<p>Zaynation e Zayn, 1 par de ingressos no Nubank Parque em S&atilde;o Paulo. 10 de outubro de 2026.</p>' }), url);
  assert.equal(matchOffer(parsed, campaign).ok, true);
  const fakeVenue = parseOffer(detail({ title: '2 INGRESSOS 10/10 Nubank Parqueamento SP', description: '<p>Zayn, 1 par de ingressos, São Paulo, 10/10/2026.</p>' }), url);
  assert.ok(matchOffer(fakeVenue, campaign).reasons.includes('VENUE_MISSING'));
});
