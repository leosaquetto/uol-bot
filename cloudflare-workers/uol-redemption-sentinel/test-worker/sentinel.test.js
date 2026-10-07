import { env, exports } from 'cloudflare:workers';
import { evictDurableObject, reset, runInDurableObject } from 'cloudflare:test';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const NOW = Date.parse('2026-10-07T12:00:00-03:00');
const CAMPAIGN = 'zayn-sp-2026-10-10';
const CLUB = 'https://clube.uol.com.br';
const OFFER_PATH = '/campanhasdeingresso/pZY-2-ingressos-10-10-nubank-parque-sp';
const OFFER_URL = `${CLUB}${OFFER_PATH}`;
const TITLE = '2 INGRESSOS 10/10 Nubank Parque SP';
const IMAGE = 'https://images.example/artwork.png';
const TOPIC = 'vitest-sentinel-no-real-delivery';
const HEADERS = { 'Content-Type': 'text/html' };

const catalog = `<section id="beneficios"><div class="beneficio" data-categoria="Ingressos Exclusivos"><a href="${OFFER_URL}"><div class="imagem-beneficio"><div data-src="${IMAGE}"></div></div><p class="titulo">${TITLE}</p></a></div></section>`;
const detail = `<html><head><link rel="canonical" href="${OFFER_URL}"><meta property="og:url" content="${OFFER_URL}"></head><body><div id="beneficio"><h2>${TITLE}</h2><div id="ilustracoes"><div class="thumb-image"><img src="${IMAGE}"></div></div><div class="detalhes"><a id="rescue_button" href="${OFFER_URL}/resgatar">Utilizar este benefício</a></div><div class="descricao"><div class="info-beneficio"><p>Resgate 1 par de ingressos para Zayn em São Paulo.</p><p>Data: 10 de outubro de 2026.</p><p>Local: Nubank Parque.</p></div></div></div></body></html>`;
function history(includeNew = false) {
  const entry = (id, title, image) => `<div class="beneficio"><div class="thumb"><img src="${image}"></div><p class="parceiro">Campanhas de ingressos</p><p class="titulo">${title}</p><a href="/perfil/beneficios/${id}">Visualizar benefício</a></div>`;
  return `<nav>Olá, LEONARDO!</nav><h1>Meus Resgates</h1><section id="beneficios">${entry('123', '2 INGRESSOS 08/09 Nubank Parque SP', 'https://images.example/old.png')}${includeNew ? entry('124', TITLE, IMAGE) : ''}</section>`;
}

const bootstrapBody = {
  identity: { verified: true, source: 'https://sac.uol.com.br/', login: 'leo@example.test' },
  quotaAttestedMonth: '2026-10',
  cookies: [{ name: 'SESS', value: 'synthetic-test-session', domain: '.uol.com.br', path: '/', secure: true, expires: -1 }],
};

let calls;
let showNewVoucher;
let redemptionTimeout;
let network;

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  calls = [];
  showNewVoucher = false;
  redemptionTimeout = false;
  // The global is shared by the main Worker and its Durable Objects. There is
  // no passthrough; config also denies all outbound network at the runtime.
  network = vi.fn(async (input, options = {}) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const method = options.method || 'GET';
    calls.push({ url: url.href, method, body: options.body });
    if (url.origin === CLUB && method === 'GET') {
      if (url.pathname === '/perfil/beneficios') return new Response(history(showNewVoucher), { headers: HEADERS });
      if (url.pathname === '/' && url.search === '?categoria=ingressosexclusivos') return new Response(catalog, { headers: HEADERS });
      if (url.pathname === OFFER_PATH) return new Response(detail, { headers: HEADERS });
      if (url.pathname === `${OFFER_PATH}/resgatar`) {
        if (redemptionTimeout) throw new DOMException('Synthetic request timeout', 'AbortError');
        showNewVoucher = true;
        return new Response('', { status: 302, headers: { Location: '/perfil/beneficios/124' } });
      }
    }
    if (url.origin === 'https://ntfy.sh' && method === 'POST') {
      return Response.json({ id: 'test-receipt', time: Math.floor(NOW / 1000), topic: TOPIC, event: 'message' });
    }
    throw new Error(`Unexpected mocked request: ${method} ${url.origin}${url.pathname}`);
  });
  vi.stubGlobal('fetch', network);
});

afterEach(async () => {
  await reset();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

const redemptions = () => calls.filter(call => call.url.endsWith('/resgatar'));
const notifications = () => calls.filter(call => call.url === 'https://ntfy.sh/');
async function prepared(name) {
  const stub = env.ACCOUNTS.getByName(name);
  expect((await stub.bootstrap('leo', bootstrapBody)).mode).toBe('prepared');
  const result = await stub.probe();
  expect(result.lastProbeResult).toBe('ready');
  return stub;
}
async function active(name) {
  const stub = await prepared(name);
  expect((await stub.activate('leo', CAMPAIGN)).mode).toBe('active');
  return stub;
}

describe('sentinel safety in the Workers Durable Object runtime', () => {
  it('bootstrap and probe cannot redeem even when every offer requirement matches', async () => {
    const stub = await prepared('probe-safety');
    expect(await stub.status()).toMatchObject({ mode: 'prepared', matchingCandidates: 1, lastResult: 'qualified_offer_dry_run', monthlyAttempt: null, nextAlarmAt: null });
    expect(redemptions()).toHaveLength(0);
    expect(notifications()).toHaveLength(0);
    expect(await stub.probeArtwork()).toMatchObject({ ok: false, reason: 'ARTWORK_URL_REJECTED' });
    expect(redemptions()).toHaveLength(0);
    expect(notifications()).toHaveLength(0);
    const encrypted = await runInDurableObject(stub, instance => instance.ledger.getState('session'));
    expect(encrypted.v).toBe(1);
    expect(JSON.stringify(encrypted)).not.toContain('synthetic-test-session');
  });

  it('incomplete paginated history blocks bootstrap without reserving or redeeming', async () => {
    const stub = env.ACCOUNTS.getByName('incomplete-history');
    const original = network.getMockImplementation();
    network.mockImplementation(async (...args) => {
      if (args[0] === `${CLUB}/perfil/beneficios`) return new Response(history() + '<a rel="next" href="/perfil/beneficios?page=2">Próxima</a>', { headers: HEADERS });
      return original(...args);
    });
    const error = await runInDurableObject(stub, async instance => {
      try { await instance.bootstrap('leo', bootstrapBody); return null; }
      catch (error) { return error.code; }
    });
    expect(error).toBe('HISTORY_INCOMPLETE');
    expect(await stub.status()).toEqual({ ready: false, mode: 'unconfigured' });
    expect(redemptions()).toHaveLength(0);
  });

  it('one active alarm reserves once, redeems once and notifies only after a new voucher', async () => {
    const stub = await active('confirmed-redemption');
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect(await stub.status()).toMatchObject({ mode: 'confirmed', monthlyAttempt: { status: 'confirmed' }, notificationPending: false, nextAlarmAt: null });
    expect(redemptions()).toHaveLength(1);
    expect(redemptions()[0].method).toBe('GET');
    expect(notifications()).toHaveLength(1);
    const sent = JSON.parse(notifications()[0].body);
    expect(sent).toMatchObject({ topic: TOPIC, title: 'Resgate confirmado — Zayn', priority: 5 });
    expect(sent.message).toContain('10/10/2026');
    expect(sent.message).not.toContain('/124');
    const redeemAt = calls.findIndex(call => call.url.endsWith('/resgatar'));
    const notifyAt = calls.findIndex(call => call.url === 'https://ntfy.sh/');
    expect(calls.slice(redeemAt + 1, notifyAt).some(call => call.url.endsWith('/perfil/beneficios'))).toBe(true);
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect(redemptions()).toHaveLength(1);
    const activationError = await runInDurableObject(stub, async instance => {
      try { await instance.activate('leo', CAMPAIGN); return null; }
      catch (error) { return error.message; }
    });
    expect(activationError).toBe('monthly_attempt_exists');
  });

  it('a timeout and object eviction preserve the attempt and never resend redemption', async () => {
    const stub = await active('timeout-persisted');
    redemptionTimeout = true;
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect(await stub.status()).toMatchObject({ mode: 'reconciling', monthlyAttempt: { status: 'reconciling' } });
    expect(redemptions()).toHaveLength(1);
    expect(notifications()).toHaveLength(0);
    await evictDurableObject(stub);
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect(redemptions()).toHaveLength(1);
    expect(notifications()).toHaveLength(0);
    // Late upstream confirmation is enough; no second final request is needed.
    showNewVoucher = true;
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect((await stub.status()).mode).toBe('confirmed');
    expect(redemptions()).toHaveLength(1);
    expect(notifications()).toHaveLength(1);
  });

  it('pause deletes the alarm and prevents further requests', async () => {
    const stub = await active('pause');
    const count = calls.length;
    expect(await stub.pause()).toMatchObject({ mode: 'paused', nextAlarmAt: null });
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect(calls).toHaveLength(count);
    expect(redemptions()).toHaveLength(0);
  });

  it.each([401, 403, 503])('rejects a final offer reread with HTTP %s even if its HTML matches', async status => {
    const stub = await active(`bad-final-http-${status}`);
    const original = network.getMockImplementation();
    let offerReads = 0;
    network.mockImplementation(async (...args) => {
      const result = await original(...args);
      if (args[0] === OFFER_URL && ++offerReads === 2) return new Response(detail, { status, headers: HEADERS });
      return result;
    });
    await runInDurableObject(stub, instance => instance.alarm());
    expect(await stub.status()).toMatchObject({ mode: 'blocked', lastResult: 'ambiguous_offer', monthlyAttempt: null });
    expect(redemptions()).toHaveLength(0);
  });

  it('three transient offer failures block without reserving or redeeming', async () => {
    const stub = await active('transient-offer-errors');
    const original = network.getMockImplementation();
    network.mockImplementation(async (...args) => {
      if (args[0] === OFFER_URL) throw new DOMException('Synthetic timeout', 'AbortError');
      return original(...args);
    });
    for (let i = 1; i <= 3; i++) {
      await runInDurableObject(stub, instance => instance.alarm());
      expect(await stub.status()).toMatchObject({ mode: i === 3 ? 'blocked' : 'active', monthlyAttempt: null });
    }
    expect(redemptions()).toHaveLength(0);
    expect(notifications()).toHaveLength(1);
  });

  it('the São Paulo cutoff expires the campaign before any request', async () => {
    const stub = await active('cutoff');
    const count = calls.length;
    vi.setSystemTime(Date.parse('2026-10-10T18:00:00-03:00'));
    expect(await runInDurableObject(stub, instance => instance.alarm())).toBeUndefined();
    expect(await stub.status()).toMatchObject({ mode: 'expired', nextAlarmAt: null, monthlyAttempt: null });
    expect(calls).toHaveLength(count);
    expect(redemptions()).toHaveLength(0);
  });

  it('the public handler never exposes account state without authorization', async () => {
    const denied = await exports.default.fetch('https://sentinel.test/admin/accounts/leo/status');
    expect(denied.status).toBe(401);
    expect(await denied.json()).toEqual({ error: 'unauthorized' });
    const health = await exports.default.fetch('https://sentinel.test/health');
    expect(await health.json()).toMatchObject({ status: 'Ready' });
    expect(calls).toHaveLength(0);
  });
});
