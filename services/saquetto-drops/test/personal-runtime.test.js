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
