import { env } from 'cloudflare:workers';
import { runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, vi } from 'vitest';
import { fetchTicketCodeOffer, parseTicketCodePage } from '../src/ticket-code-discovery.js';
import { protectedTicketCodeIds } from '../src/ticket-code-policy.js';

const origin = 'https://clube.uol.com.br';
const link = `${origin}/campanhasdeingresso/pPS-2-ingressos-13-10-nubank-parque-sp`;
function html(url) {
  return `<link rel="canonical" href="${url}"><meta property="og:url" content="${url}">
    <meta property="og:image" content="https://example.com/ticket.png">
    <div id="beneficio"><h2>2 INGRESSOS: 13/10 Nubank Parque SP</h2>
    <div class="fb-like" data-href="//clubeuol.clubeben.com.br/campanhasdeingresso/pPS-2-ingressos-13-10-nubank-parque-sp"></div>
    <a id="rescue_button" href="/auth/uol/login?redirect_uri=%2Fcampanhasdeingresso%2FpPS%2Fresgatar">Utilizar este benefício</a>
    <div class="info-beneficio"><p>Robbie Williams no Nubank Parque em São Paulo. O benefício dá direito a um par de ingressos para o show, sujeito às regras da campanha.</p></div></div>`;
}
function response(url) { return new Response(html(url), { headers: { 'Content-Type': 'text/html' } }); }

describe('public hidden ticket discovery', () => {
  it('confirms short and full page without fetching the redemption CTA', async () => {
    const calls = [];
    const result = await fetchTicketCodeOffer('pPS', async (url, options) => {
      calls.push(url);
      expect(options.redirect).toBe('manual');
      expect(options.headers.Authorization).toBeUndefined();
      return response(url);
    });
    expect(result.status).toBe('found');
    expect(result.card.link).toBe(link);
    expect(result.card.apiDetail.description).toContain('Robbie Williams');
    expect(calls).toEqual([`${origin}/campanhasdeingresso/pPS`, link]);
  });
  it('rejects conflicting identity, multiple containers and generic challenge pages', async () => {
    for (const body of [html(link).replace('content="'+link+'"', 'content="'+link.replace('pPS','pPQ')+'"'),
      html(link)+'<div id="beneficio"></div>', '<h2>Verify you are human</h2>']) {
      expect((await parseTicketCodePage(new Response(body), 'pPS', link)).status).toBe('unknown');
    }
  });
  it('does not announce a disabled CTA and does not mistake stock boilerplate for sold out', async () => {
    const disabled = html(link).replace('id="rescue_button"', 'id="rescue_button" aria-disabled="true"');
    expect((await parseTicketCodePage(new Response(disabled), 'pPS', link)).status).toBe('unknown');
    const gone = html(link).replace('Utilizar este benefício', 'Benefício esgotado');
    expect((await parseTicketCodePage(new Response(gone), 'pPS', link)).status).toBe('absent');
    const boilerplate = html(link).replace('sujeito às regras', 'não pode ser usado se esgotado, sujeito às regras');
    expect((await parseTicketCodePage(new Response(boilerplate), 'pPS', link)).status).toBe('found');
  });
  it('adds an unlisted offer once, restores state, and protects against public-list absence', async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName('hidden-tickets-durable');
    await runInDurableObject(stub, async instance => {
      instance.env = { ...instance.env, TICKET_CODE_DISCOVERY_ENABLED: 'true' };
      let requests = 0;
      const parsed = await parseTicketCodePage(response(link), 'pPS', link);
      instance.fetchTicketCode = async code => {
        requests++;
        return code === 'pPS' ? { ...parsed, requests: 2 } : { status: 'absent', requests: 1 };
      };
      instance.processDeliveryQueue = async () => ({ selectedRows: [], recentSecondaryRows: [] });
      instance.scheduleDiscordDelivery = () => {};
      instance.scheduleCriticalBeeperDelivery = () => {};
      instance.setRuntimeSnapshot('api', { codeAnchors: ['pPP','pPT'].map(code => ({ link: `${origin}/retail/${code}-offer` })) });
      await instance.scheduleTicketCodeDiscovery();
      expect(requests).toBe(0); // Normal baseline must exist first.
      instance.setMetadata('initialized_at', new Date().toISOString());
      const resolutions = vi.spyOn(instance, 'resolveListingCards');
      await instance.scheduleTicketCodeDiscovery();
      const count = () => instance.sqlExec('SELECT id, status FROM offers WHERE link = ?', link).toArray();
      expect(count()).toHaveLength(1);
      expect(count()[0].status).toBe('shadow_candidate');
      expect(resolutions).toHaveBeenCalledTimes(1);
      const snapshot = instance.runtimeSnapshot('ticket_code_discovery');
      expect(protectedTicketCodeIds(snapshot)).toContain(count()[0].id);
      const before = requests;
      instance.runtimeSnapshotCache.clear();
      instance.metadataCache.clear();
      await instance.scheduleTicketCodeDiscovery();
      expect(requests).toBe(before);
      expect(count()).toHaveLength(1);
      expect(instance.evaluateSoldOut(new Set(protectedTicketCodeIds(snapshot)), new Date(), 'ticket')).toBe(0);
      // Simulate the next due interval after restoring the persisted snapshot.
      snapshot.nextAt = 0;
      snapshot.entries.pPS.nextAt = 0;
      instance.setRuntimeSnapshot('ticket_code_discovery', snapshot);
      await instance.scheduleTicketCodeDiscovery();
      expect(requests).toBeGreaterThan(before);
      expect(resolutions).toHaveBeenCalledTimes(1);
      expect(count()).toHaveLength(1);
      expect(count()[0].status).toBe('shadow_candidate');
    });
  });

  it('recovers a durable verified card even when the scan allowance is exhausted', async () => {
    const stub = env.UOL_TELEGRAM_SHADOW.getByName('hidden-ticket-result-recovery');
    await runInDurableObject(stub, async instance => {
      instance.env = { ...instance.env, TICKET_CODE_DISCOVERY_ENABLED: 'true' };
      instance.setMetadata('initialized_at', new Date().toISOString());
      const parsed = await parseTicketCodePage(response(link), 'pPS', link);
      instance.setRuntimeSnapshot('ticket_code_discovery', { day: new Date().toISOString().slice(0, 10), requestsUsed: 6_000,
        entries: { pPS: { status: 'found', card: parsed.card, fingerprint: 'durable-fingerprint', foundAt: Date.now() } } });
      instance.runtimeSnapshotCache.clear();
      instance.metadataCache.clear();
      const fetchCode = vi.spyOn(instance, 'fetchTicketCode');
      instance.processDeliveryQueue = async () => ({ selectedRows: [], recentSecondaryRows: [] });
      instance.scheduleDiscordDelivery = () => {};
      instance.scheduleCriticalBeeperDelivery = () => {};
      await instance.scheduleTicketCodeDiscovery();
      expect(fetchCode).not.toHaveBeenCalled();
      const snapshot = instance.runtimeSnapshot('ticket_code_discovery');
      expect(snapshot.entries.pPS.resolvedId).toBeTruthy();
      expect(snapshot.entries.pPS.resolvedFingerprint).toBe('durable-fingerprint');
    });
  });
});
