import test from 'node:test';
import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { HunterAccessRegistry, hunterAccessProof, hunterAccessFailure } from '../src/hunter-access.js';

if (!globalThis.crypto) Object.defineProperty(globalThis, 'crypto', { value: webcrypto });
const NOW = Date.parse('2026-10-18T12:00:00Z');
const SESSION_SECRET = 'synthetic-private-instagram-session';
const session = { user_agent: 'synthetic-browser', cookies: [
  { name: 'sessionid', value: SESSION_SECRET, domain: '.instagram.com' },
  { name: 'csrftoken', value: 'synthetic-csrf', domain: '.instagram.com' },
  { name: 'foreign', value: 'excluded', domain: 'example.test' },
], headers: { 'accept-language': 'pt-BR', Authorization: 'excluded-auth' }, arbitrary: 'excluded-extra' };
const personalAccounts = {
  leosaquetto: { identityDigest: '1'.repeat(64), cookies: [
    { name: 'SESS', value: 'synthetic-private-uol-leo', domain: '.uol.com.br', path: '/', hostOnly: false, secure: true, httpOnly: true, sameSite: 'Lax', expires: -1 },
    { name: 'PHPSESSID', value: 'synthetic-private-uol-session', domain: 'clube.uol.com.br', path: '/perfil', hostOnly: true, secure: false, expirationDate: NOW / 1000 + 3600 },
    { name: 'DNA', value: 'expired', domain: '.uol.com.br', expires: NOW / 1000 - 1 },
    { name: 'other', value: 'excluded', domain: '.uol.com.br' },
    { name: 'SESS', value: 'excluded', domain: 'example.test' },
  ] },
  gaoriel: { identityDigest: '2'.repeat(64), cookies: [
    { name: 'SESS', value: 'synthetic-private-uol-gab', domain: '.uol.com.br', path: '/', secure: true, expires: -1 },
  ] },
};
const token = id => 'h1.' + id + '.' + 'a'.repeat(64);
const proof = id => hunterAccessProof(new Request('https://worker.test', { headers: { Authorization: 'Bearer ' + token(id) } }));
const OWNER = await proof('owner-phone');
function fixture({ raw = '', clients = { 'owner-phone': { enabled: true, secretHash: OWNER.secretHash, role: 'owner' } }, budget = true } = {}) {
  let saved = raw, time = NOW, allowed = budget;
  const writes = [];
  const env = { HUNTER_INTAKE_CLIENTS: JSON.stringify(clients), HUNTER_INSTAGRAM_SESSION: JSON.stringify(session),
    HUNTER_UOL_ACCOUNTS: JSON.stringify(personalAccounts) };
  const options = { readState: () => saved, writeState: value => { saved = value; writes.push(value); }, env, now: () => time, budgetAllowed: () => allowed };
  return { registry: new HunterAccessRegistry(options), env, writes, raw: () => saved,
    restart: () => new HunterAccessRegistry(options), advance: ms => { time += ms; }, allow: value => { allowed = value; } };
}
function fails(fn, code, status) {
  assert.throws(fn, error => {
    assert.deepEqual(hunterAccessFailure(error), { error: 'hunter_access_' + code, status });
    return true;
  });
}
function safe(value) {
  assert.doesNotMatch(JSON.stringify(value), /h1\.|secretHash|synthetic-private-instagram-session|synthetic-private-uol|excluded-auth/);
}
function approve(registry, target, extra = {}) {
  const pending = registry.status(target);
  return registry.decide(OWNER, { deviceId: target.deviceId, decision: 'approve', code: pending.code, ...extra });
}

test('bearer proof validates h1 scope and stores only a hash, never the bearer', async () => {
  const p = await proof('test-phone');
  assert.equal(p.deviceId, 'test-phone'); assert.match(p.secretHash, /^[a-f0-9]{64}$/);
  assert.doesNotMatch(JSON.stringify(p), /h1\./);
  for (const auth of ['Bearer admin-token', token('test-phone'), 'Bearer h1.test-phone.short', 'Bearer h1.UPPER.' + 'a'.repeat(64)]) {
    assert.equal(await hunterAccessProof(new Request('https://worker.test', { headers: { Authorization: auth } })), null);
  }
});

test('registration survives a lost response and restart without another write or admission', async () => {
  const h = fixture(), p = await proof('new-phone');
  const first = h.registry.request(p, { label: 'iPhone novo' });
  assert.equal(first.state, 'pending'); assert.equal(first.role, 'member'); safe(first);
  assert.equal(h.writes.length, 1); assert.doesNotMatch(h.raw(), /h1\.|instagramSession|cookies|user_agent/);
  assert.ok(h.raw().includes(p.secretHash));
  const restarted = h.restart();
  assert.deepEqual(restarted.request(p, { label: 'Mesmo aparelho, novo texto' }), first);
  assert.deepEqual(restarted.status(p), first);
  assert.equal(h.writes.length, 1);
  const collision = { ...p, secretHash: 'b'.repeat(64) };
  fails(() => restarted.request(collision, { label: 'Outra chave' }), 'unauthorized', 401);
  assert.equal(h.writes.length, 1);
});

test('pending clients cannot authenticate, obtain private config or list/approve other devices', async () => {
  const h = fixture(), p = await proof('pending-phone'), other = await proof('other-phone');
  h.registry.request(p, { label: 'Pendente' }); h.registry.request(other, { label: 'Outro' });
  assert.equal(h.registry.authenticate(p), '');
  fails(() => h.registry.config(p), 'forbidden', 403);
  fails(() => h.registry.list(p), 'forbidden', 403);
  fails(() => h.registry.decide(p, { deviceId: other.deviceId, decision: 'approve', code: h.registry.status(other).code }), 'forbidden', 403);
});

test('owner approval and revocation persist while member access never escalates', async () => {
  const h = fixture(), p = await proof('member-phone'); h.registry.request(p, { label: 'Membro' });
  const approved = approve(h.registry, p); assert.equal(approved.state, 'approved'); assert.equal(approved.role, 'member');
  assert.equal(h.restart().authenticate(p), p.deviceId); safe(h.registry.list(OWNER));
  const before = h.writes.length;
  approve(h.registry, p); assert.equal(h.writes.length, before, 'repeating the same decision is read-only');
  fails(() => approve(h.registry, p, { role: 'owner' }), 'decision_invalid', 400);
  fails(() => h.registry.list(p), 'forbidden', 403);
  const revoked = h.registry.decide(OWNER, { deviceId: p.deviceId, code: approved.code, decision: 'revoke' });
  assert.equal(revoked.state, 'revoked'); assert.equal(h.restart().authenticate(p), '');
  fails(() => h.registry.config(p), 'forbidden', 403);
  safe(h.registry.status(p));
});

test('only admin can assign owner role and members or owners cannot change an owner record', async () => {
  const h = fixture(), p = await proof('next-owner'), member = await proof('member');
  h.registry.request(p, { label: 'Próximo dono' }); h.registry.request(member, { label: 'Membro' });
  const pending = h.registry.status(p);
  h.registry.decide(null, { deviceId: p.deviceId, decision: 'approve', code: pending.code, role: 'owner' }, true);
  assert.equal(h.registry.status(p).role, 'owner'); assert.equal(h.registry.list(p).ok, true);
  fails(() => h.registry.decide(OWNER, { deviceId: p.deviceId, decision: 'revoke', code: pending.code }), 'forbidden', 403);
  fails(() => h.registry.request(member, { label: 'Membro', role: 'owner' }), 'request_invalid', 400);
});

test('confirmation code mismatches cannot modify another identity', async () => {
  const h = fixture(), p = await proof('confirm-phone'); h.registry.request(p, { label: 'Confirmar' });
  const before = h.raw();
  fails(() => h.registry.decide(OWNER, { deviceId: p.deviceId, decision: 'approve', code: '00000000' }), 'identity_conflict', 409);
  assert.equal(h.raw(), before); assert.equal(h.registry.authenticate(p), '');
});

test('approved private config is narrow, rejects invalid sessions and never writes session data into metadata', async () => {
  const h = fixture(), p = await proof('config-phone'); h.registry.request(p, { label: 'Configurar' }); approve(h.registry, p);
  const before = h.raw(), writes = h.writes.length;
  const config = h.registry.config(p);
  assert.deepEqual(Object.keys(config).sort(), ['deviceId', 'instagramSession', 'ok', 'role']);
  assert.equal(config.instagramSession.cookies.length, 2);
  assert.ok(config.instagramSession.cookies.every(cookie => cookie.domain === '.instagram.com' && cookie.path === '/'));
  assert.deepEqual(config.instagramSession.headers, { 'accept-language': 'pt-BR' });
  assert.equal(config.instagramSession.arbitrary, undefined); assert.equal(h.raw(), before); assert.equal(h.writes.length, writes);
  for (const raw of ['', '{bad-json', JSON.stringify({ user_agent: 'x', cookies: [] }), JSON.stringify({ ...session, user_agent: 'bad\r\nagent' })]) {
    h.env.HUNTER_INSTAGRAM_SESSION = raw;
    fails(() => h.registry.config(p), 'setup_unavailable', 503);
  }
  safe(h.registry.status(p)); safe(h.registry.list(OWNER)); assert.doesNotMatch(h.raw(), /synthetic-private-instagram-session/);
});

test('pending registration is limited to sixteen devices before more admissions are rejected', async () => {
  const h = fixture();
  for (let i = 0; i < 16; i++) h.registry.request(await proof('pending-' + i), { label: 'Pendente ' + i });
  assert.equal(h.registry.list(OWNER).devices.filter(device => device.state === 'pending').length, 16);
  const before = h.raw();
  fails(() => h.registry.request({ deviceId: 'overflow', secretHash: 'd'.repeat(64) }, { label: 'Excesso' }), 'capacity', 429);
  assert.equal(h.raw(), before);
});

test('global twenty-four daily admissions remain capped even when approvals free pending slots', async () => {
  const h = fixture();
  for (let i = 0; i < 24; i++) { const p = await proof('admit-' + i); h.registry.request(p, { label: 'Dispositivo ' + i }); approve(h.registry, p); }
  const next = await proof('next-day'), before = h.raw();
  fails(() => h.registry.request(next, { label: 'Amanhã' }), 'rate_limit', 429); assert.equal(h.raw(), before);
  h.advance(86400000);
  assert.equal(h.registry.request(next, { label: 'Amanhã' }).state, 'pending');
});

test('write guard denies registration/approval without losing durable state; reads and duplicate requests stay safe', async () => {
  const h = fixture(), p = await proof('guarded'); h.registry.request(p, { label: 'Guardado' });
  const before = h.raw(), writes = h.writes.length; h.allow(false);
  fails(() => approve(h.registry, p), 'budget_guard', 503);
  fails(() => h.registry.request({ deviceId: 'new', secretHash: 'c'.repeat(64) }, { label: 'Novo' }), 'budget_guard', 503);
  assert.equal(h.raw(), before); assert.equal(h.writes.length, writes);
  assert.equal(h.registry.status(p).state, 'pending'); h.registry.request(p, { label: 'Repetida' }); assert.equal(h.writes.length, writes);
});

test('pending access expires exactly after a day without granting access or permitting delayed approval', async () => {
  const h = fixture(), p = await proof('expires'); h.registry.request(p, { label: 'Expira' });
  h.advance(86399999); assert.equal(h.registry.status(p).state, 'pending');
  h.advance(1); assert.equal(h.registry.status(p).state, 'expired'); assert.equal(h.registry.authenticate(p), '');
  fails(() => approve(h.registry, p), 'expired', 409);
  h.registry.request(await proof('fresh'), { label: 'Novo' });
  assert.ok(h.registry.list(OWNER).devices.every(item => item.deviceId !== p.deviceId));
});

test('legacy credentials retain enabled member/owner scope and disabled or changed hashes fail closed', async () => {
  const p = await proof('legacy'), member = fixture({ clients: { legacy: p.secretHash } });
  assert.equal(member.registry.authenticate(p), 'legacy'); assert.equal(member.registry.status(p).role, 'member'); assert.equal(member.writes.length, 0);
  fails(() => member.registry.list(p), 'forbidden', 403);
  const owner = fixture({ clients: { legacy: { enabled: true, secretHash: p.secretHash, role: 'owner' } } });
  assert.equal(owner.registry.list(p).ok, true); assert.equal(owner.registry.config(p).role, 'owner');
  for (const config of [{ enabled: false, secretHash: p.secretHash }, { enabled: true, secretHash: 'f'.repeat(64) }]) {
    const h = fixture({ clients: { legacy: config } }); fails(() => h.registry.authenticate(p), 'unauthorized', 401);
  }
});

test('owner can revoke a legacy static member and restart cannot restore its access', async () => {
  const p = await proof('legacy-member');
  const h = fixture({ clients: { 'owner-phone': { enabled: true, secretHash: OWNER.secretHash, role: 'owner' },
    'legacy-member': p.secretHash } });
  const listed = h.registry.list(OWNER).devices;
  assert.equal(listed.length, 2);
  const member = listed.find(device => device.deviceId === p.deviceId);
  assert.equal(member.state, 'approved'); assert.equal(member.requestedAt, 0); safe(member);
  const revoked = h.registry.decide(OWNER, { deviceId: p.deviceId, decision: 'revoke', code: member.code });
  assert.equal(revoked.state, 'revoked'); assert.equal(h.writes.length, 1);
  const durable = JSON.parse(h.raw()).devices;
  assert.equal(durable.length, 1); assert.equal(durable[0].deviceId, p.deviceId);
  const restarted = h.restart();
  assert.equal(restarted.authenticate(p), '');
  fails(() => restarted.config(p), 'forbidden', 403);
  assert.equal(restarted.request(p, { label: 'Não reativar' }).state, 'revoked');
  assert.equal(h.writes.length, 1);
  assert.equal(restarted.list(OWNER).devices.filter(device => device.deviceId === p.deviceId).length, 1);
  assert.equal(restarted.list(OWNER).devices.find(device => device.deviceId === p.deviceId).state, 'revoked');
  restarted.decide(OWNER, { deviceId: p.deviceId, decision: 'approve', code: member.code });
  assert.equal(h.restart().authenticate(p), p.deviceId);
});

test('expired pending access renews the same identity and remains pending until a fresh owner decision', async () => {
  const h = fixture(), p = await proof('renew-phone');
  const first = h.registry.request(p, { label: 'Primeiro nome' });
  h.advance(86400000);
  assert.equal(h.registry.status(p).state, 'expired');
  const renewed = h.registry.request(p, { label: 'Mesmo aparelho' });
  assert.equal(renewed.deviceId, first.deviceId); assert.equal(renewed.code, first.code);
  assert.equal(renewed.state, 'pending'); assert.equal(renewed.role, 'member');
  assert.equal(renewed.requestedAt, NOW + 86400000);
  assert.equal(h.registry.authenticate(p), '');
  assert.equal(JSON.parse(h.raw()).devices.filter(device => device.deviceId === p.deviceId).length, 1);
  assert.equal(JSON.parse(h.raw()).admissions, 1);
  assert.equal(h.restart().status(p).state, 'pending');
  assert.equal(approve(h.registry, p).state, 'approved');
});

test('missing status returns 404 without granting access, writing or exposing another device', async () => {
  const h = fixture(), p = await proof('not-registered');
  fails(() => h.registry.status(p), 'not_found', 404);
  assert.equal(h.registry.authenticate(p), '');
  fails(() => h.registry.config(p), 'forbidden', 403);
  assert.equal(h.writes.length, 0);
});

test('technical approval grants no personal account permission implicitly', async () => {
  const h = fixture(), p = await proof('technical-only');
  h.registry.request(p, { label: 'Só coleta pública' }); approve(h.registry, p);
  assert.deepEqual(h.registry.status(p).accounts, []);
  assert.equal(h.registry.config(p).ok, true);
  for (const account of ['leosaquetto', 'gaoriel']) fails(() => h.registry.accountConfig(p, account), 'forbidden', 403);
  fails(() => h.registry.accountConfig(p, 'arbitrary'), 'request_invalid', 400);
  fails(() => approve(h.registry, p, { accounts: ['leosaquetto'] }), 'forbidden', 403);
});

test('admin seeds owner permissions; owner grants only a held subset and member cannot grant', async () => {
  const h = fixture(), owner = await proof('personal-owner'), member = await proof('personal-member');
  h.registry.request(owner, { label: 'Dono autorizado' }); h.registry.request(member, { label: 'Amigo' });
  h.registry.decide(null, { deviceId: owner.deviceId, code: h.registry.status(owner).code, decision: 'approve', role: 'owner', accounts: ['leosaquetto'] }, true);
  const decision = { deviceId: member.deviceId, code: h.registry.status(member).code, decision: 'approve' };
  fails(() => h.registry.decide(owner, { ...decision, accounts: ['gaoriel'] }), 'forbidden', 403);
  fails(() => h.registry.decide(owner, { ...decision, accounts: ['leosaquetto', 'leosaquetto'] }), 'decision_invalid', 400);
  fails(() => h.registry.decide(owner, { ...decision, accounts: ['arbitrary'] }), 'decision_invalid', 400);
  assert.equal(h.registry.status(member).state, 'pending');
  const approved = h.registry.decide(owner, { ...decision, accounts: ['leosaquetto'] });
  assert.deepEqual(approved.accounts, ['leosaquetto']); safe(approved);
  fails(() => h.registry.decide(member, decision), 'forbidden', 403);
  fails(() => h.registry.decide(owner, { ...decision, decision: 'revoke', accounts: [] }), 'decision_invalid', 400);
  const writes = h.writes.length;
  h.registry.decide(owner, { ...decision, accounts: ['leosaquetto'] });
  assert.equal(h.writes.length, writes);
  h.registry.decide(owner, { ...decision, accounts: [] });
  assert.deepEqual(h.restart().status(member).accounts, []);
  fails(() => h.registry.accountConfig(member, 'leosaquetto'), 'forbidden', 403);
});

test('personal config returns only the permitted account and cookie fields while revocation denies access', async () => {
  const h = fixture(), owner = await proof('both-owner'), p = await proof('leo-only');
  h.registry.request(owner, { label: 'Dono' }); h.registry.request(p, { label: 'Aparelho' });
  h.registry.decide(null, { deviceId: owner.deviceId, code: h.registry.status(owner).code, decision: 'approve', role: 'owner', accounts: ['leosaquetto', 'gaoriel'] }, true);
  const decision = { deviceId: p.deviceId, code: h.registry.status(p).code, decision: 'approve', accounts: ['leosaquetto'] };
  h.registry.decide(owner, decision);
  const before = h.raw(), writes = h.writes.length;
  const config = h.registry.accountConfig(p, 'leosaquetto');
  assert.deepEqual(Object.keys(config).sort(), ['account', 'cookies', 'deviceId', 'identityDigest', 'ok']);
  assert.equal(config.account, 'leosaquetto'); assert.equal(config.deviceId, p.deviceId);
  assert.equal(config.identityDigest, '1'.repeat(64));
  assert.deepEqual(config.cookies, [personalAccounts.leosaquetto.cookies[0], {
    name: 'PHPSESSID', value: 'synthetic-private-uol-session', domain: 'clube.uol.com.br', path: '/perfil', hostOnly: true,
    secure: false, expires: NOW / 1000 + 3600,
  }]);
  assert.doesNotMatch(JSON.stringify(config), /synthetic-private-uol-gab|expired|excluded|password/);
  fails(() => h.registry.accountConfig(p, 'gaoriel'), 'forbidden', 403);
  assert.equal(h.raw(), before); assert.equal(h.writes.length, writes); safe(h.registry.list(owner));
  assert.doesNotMatch(h.raw(), /cookies|identityDigest|synthetic-private-uol/);
  h.registry.decide(owner, { deviceId: p.deviceId, code: decision.code, decision: 'revoke' });
  fails(() => h.restart().accountConfig(p, 'leosaquetto'), 'forbidden', 403);
});

test('personal account secret fails closed when missing, malformed or lacking valid authentication cookies', async () => {
  const h = fixture(), p = await proof('personal-malformed');
  h.registry.request(p, { label: 'Aparelho' });
  h.registry.decide(null, { deviceId: p.deviceId, code: h.registry.status(p).code, decision: 'approve', accounts: ['leosaquetto'] }, true);
  for (const raw of ['', '{bad', '{}', JSON.stringify({ leosaquetto: { ...personalAccounts.leosaquetto, password: 'must-not-return' } }),
    JSON.stringify({ leosaquetto: { identityDigest: 'bad', cookies: personalAccounts.leosaquetto.cookies } }),
    JSON.stringify({ leosaquetto: { identityDigest: '1'.repeat(64), cookies: [{ name: 'SESS', value: 'header\r\ninjection', domain: '.uol.com.br' }] } }),
    JSON.stringify({ leosaquetto: { identityDigest: '1'.repeat(64), cookies: [{ name: 'SESS', value: 'x', domain: 'example.test' }] } }),
    JSON.stringify({ leosaquetto: { identityDigest: '1'.repeat(64), cookies: [{ name: 'SESS', value: 'x', domain: '.uol.com.br', expires: NOW / 1000 - 1 }] } })]) {
    h.env.HUNTER_UOL_ACCOUNTS = raw;
    fails(() => h.registry.accountConfig(p, 'leosaquetto'), 'setup_unavailable', 503);
  }
});

test('an empty JS_SESS companion cookie preserves a valid UOL session contract', async () => {
  const h = fixture(), p = await proof('empty-companion');
  h.registry.request(p, { label: 'Aparelho' });
  h.registry.decide(null, { deviceId: p.deviceId, code: h.registry.status(p).code, decision: 'approve', accounts: ['leosaquetto'] }, true);
  h.env.HUNTER_UOL_ACCOUNTS = JSON.stringify({ leosaquetto: { ...personalAccounts.leosaquetto,
    cookies: [...personalAccounts.leosaquetto.cookies, { name: 'JS_SESS', value: '', domain: '.uol.com.br', path: '/', expires: -1 }] } });
  assert.equal(h.registry.accountConfig(p, 'leosaquetto').cookies.find(c => c.name === 'JS_SESS').value, '');
});

test('corrupt registry state fails closed before any grant, listing or admission', () => {
  const valid = { version: 1, day: '2026-10-18', admissions: 0, devices: [] };
  for (const raw of ['not-json', JSON.stringify({ ...valid, devices: [{ deviceId: 'bad', secretHash: 'bad', state: 'approved', role: 'owner', label: 'x', requestedAt: NOW }] }),
    JSON.stringify({ ...valid, admissions: 'bypass' }), JSON.stringify({ ...valid, admissions: null }),
    JSON.stringify({ ...valid, day: 42 }), JSON.stringify({ version: 1, devices: [] })]) {
    const h = fixture({ raw });
    fails(() => h.registry.status(OWNER), 'state_invalid', 503);
    fails(() => h.registry.request({ deviceId: 'new', secretHash: 'c'.repeat(64) }, { label: 'Novo' }), 'state_invalid', 503);
    assert.equal(h.writes.length, 0);
  }
  for (const accounts of [null, ['leosaquetto', 'leosaquetto'], ['arbitrary'], ['leosaquetto', 'gaoriel', 'arbitrary']]) {
    const h = fixture({ raw: JSON.stringify({ ...valid, devices: [{ ...OWNER, state: 'approved', role: 'owner', label: 'Dono', requestedAt: NOW, accounts }] }) });
    fails(() => h.registry.status(OWNER), 'state_invalid', 503);
  }
});

test('public failure mapping never exposes arbitrary error text or accepts spoofed status', () => {
  for (const error of [new Error('cookie=' + SESSION_SECRET), { message: 'hunter_access_not_real', httpStatus: 200 }, null]) {
    const result = hunterAccessFailure(error); assert.deepEqual(result, { error: 'hunter_access_unavailable', status: 503 }); safe(result);
  }
  assert.deepEqual(hunterAccessFailure({ inputStatus: 413 }), { error: 'hunter_access_unavailable', status: 413 });
});
