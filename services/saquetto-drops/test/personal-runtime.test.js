import test from 'node:test';
import assert from 'node:assert/strict';
import { maskSenderJid, personalAuthorized, personalStatus, validatePersonalToken } from '../src/personal-runtime.js';

const token = 'a'.repeat(64);

test('personal token validation is strict', () => {
  assert.equal(validatePersonalToken(token), token);
  assert.throws(() => validatePersonalToken('short'), /BAILEYS_PERSONAL_TOKEN_required/);
});

test('personal auth accepts only the configured bearer token', () => {
  assert.equal(personalAuthorized(`Bearer ${token}`, token), true);
  assert.equal(personalAuthorized(`Bearer ${'b'.repeat(64)}`, token), false);
  assert.equal(personalAuthorized('', token), false);
});

test('sender identity is masked', () => {
  assert.equal(maskSenderJid('5511999991234:4@s.whatsapp.net'), '****1234');
  assert.equal(maskSenderJid(''), null);
});

test('status exposes connection state without a jid', () => {
  const whatsapp = {
    state: () => 'connected',
    isReady: () => true,
    ownDestination: () => ({ jid: '5511999991234:4@s.whatsapp.net' }),
  };
  assert.deepEqual(personalStatus({ whatsapp, pairingRequired: false }), {
    ok: true,
    whatsapp: 'connected',
    connected: true,
    pairingRequired: false,
    sender: '****1234',
  });
});

test('personal manual config exposes only the connected self alias', async () => {
  const mod = await import('../src/personal-runtime.js');
  const ready = { isReady: () => true, ownDestination: () => ({ type:'contact', jid:'5511999991234@s.whatsapp.net', verified:true }) };
  assert.deepEqual(mod.personalManualConfig(ready), {
    operation:{paused:false,dryRun:false,minDelayMs:1000},
    destinations:{self:{type:'contact',jid:'5511999991234@s.whatsapp.net',verified:true}},
  });
  const offline = { isReady: () => false, ownDestination: () => ({ type:'contact', jid:'5511999991234@s.whatsapp.net', verified:false }) };
  assert.deepEqual(mod.personalManualConfig(offline).destinations,{});
});

test('personal HTTP surface keeps media and arbitrary routes closed', async () => {
  const mod = await import('../src/personal-runtime.js');
  assert.equal(mod.personalWhatsAppRouteAllowed('GET','/v1/whatsapp/destinations'),true);
  assert.equal(mod.personalWhatsAppRouteAllowed('GET','/v1/whatsapp/requests/11111111-1111-4111-8111-111111111111'),true);
  assert.equal(mod.personalWhatsAppRouteAllowed('POST','/v1/whatsapp/send'),true);
  for (const [method,path] of [
    ['POST','/v1/whatsapp/media'],['GET','/v1/whatsapp/media'],['POST','/v1/whatsapp/send/anything'],
    ['GET','/v1/status'],['POST','/v1/whatsapp/destinations']
  ]) assert.equal(mod.personalWhatsAppRouteAllowed(method,path),false,`${method} ${path}`);
});
