import assert from 'node:assert/strict';
import test from 'node:test';
import { adminRequest, safeStatus } from '../scripts/manage.mjs';

test('admin CLI routes only documented commands; probe always carries readOnly', async () => {
  const calls = [];
  const fetchImpl = async (url, init) => { calls.push({ url: String(url), init }); return Response.json({ ok: true }); };
  for (const command of ['status', 'probe', 'pause']) await adminRequest({ command, account: 'leo', token: 'FAKE', fetchImpl });
  assert.equal(calls[0].init.method, 'GET');
  assert.equal(calls[0].init.body, undefined);
  assert.equal(calls[1].url, 'https://uol-redemption-sentinel.leosaquetto.workers.dev/admin/accounts/leo/probe');
  assert.deepEqual(JSON.parse(calls[1].init.body), { readOnly: true });
  assert.equal(calls[2].init.redirect, 'error');
  await assert.rejects(adminRequest({ command: 'resgatar', account: 'leo', token: 'FAKE', fetchImpl }), /invalid_command/);
  await assert.rejects(adminRequest({ command: 'probe', account: 'leo', token: 'FAKE', data: { readOnly: false }, fetchImpl }), /data_only_for_bootstrap/);
  assert.equal(calls.length, 3);
});

test('activation selects a campaign; bootstrap data is carried only in the body', async () => {
  let observed;
  const fetchImpl = async (url, init) => { observed = { url: String(url), init }; return Response.json({ ok: true }); };
  await adminRequest({ command: 'activate', account: 'leo', token: 'FAKE', campaign: 'zayn-sp-2026-10-10', fetchImpl });
  assert.deepEqual(JSON.parse(observed.init.body), { campaignId: 'zayn-sp-2026-10-10' });
  const data = { cookies: [{ value: 'FAKE-COOKIE' }], identity: { login: 'fake', verified: true }, quotaAttestedMonth: '2026-10' };
  await adminRequest({ command: 'bootstrap', account: 'leo', token: 'FAKE', data, fetchImpl });
  assert.deepEqual(JSON.parse(observed.init.body), data);
  assert.ok(!observed.url.includes('FAKE-COOKIE'));
});

test('response errors, cookies and credentials are never exposed', async () => {
  await assert.rejects(adminRequest({ command: 'status', account: 'leo', token: 'FAKE', fetchImpl: async () => new Response('PRIVATE-TOKEN', { status: 403 }) }), /^Error: admin_http_403$/);
  await assert.rejects(adminRequest({ command: 'status', account: 'leo', token: 'FAKE', fetchImpl: async () => { throw new Error('PRIVATE-TOKEN'); } }), /^Error: admin_request_failed$/);
  const result = safeStatus({ ok: true, identityVerified: true, sessionReady: true, password: 'fake', cookies: ['secret'], nested: { status: 'active', login: 'secret' } });
  assert.deepEqual(result, { ok: true, identityVerified: true, sessionReady: true, nested: { status: 'active' } });
});
