import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('./UOL Instagram.js', import.meta.url), 'utf8');
const CODE = 'w1.testdevice.' + 'a'.repeat(64);
const pendingPath = '/docs/UOL Instagram-pending.v1.json';
const pendingDirectory = '/docs/UOL Instagram-pending.v2';
const events = files => [...files].filter(([path]) => path.startsWith(pendingDirectory + '/') && path.endsWith('.json')).map(([, encoded]) => JSON.parse(encoded));

function harness({ responses = [], files = new Map(), configured = true, setupCode = CODE } = {}) {
  const calls = [], keys = new Map(configured ? [['uol-instagram.wake-code.v1', CODE]] : []);
  let sequence = 0;
  async function run({ automatic = true } = {}) {
    let output, completed = false, alerts = 0;
    class Request {
      constructor(url) { this.url = url; }
      async loadString() {
        const body = JSON.parse(this.body);
        assert.equal(files.has(pendingDirectory + '/' + body.eventId + '.json'), true, 'persist before network');
        assert.ok(events(files).some(event => event.eventId === body.eventId));
        calls.push(this);
        const configured = responses.shift();
        const next = typeof configured === 'function' ? await configured(this) : configured;
        if (next instanceof Error) throw next;
        const status = next?.status ?? 202;
        this.response = { statusCode: status };
        return JSON.stringify(next?.body ?? { ok: true, accepted: true, eventId: body.eventId });
      }
    }
    class Alert {
      constructor() { alerts++; }
      addSecureTextField() {} addAction() {} addCancelAction() {}
      async presentAlert() { return 0; }
      textFieldValue() { return setupCode; }
    }
    const context = vm.createContext({
      Request, Alert, args: { shortcutParameter: automatic ? 'wake' : null }, config: { runsInWidget: false, runsWithSiri: false },
      Keychain: { contains: key => keys.has(key), get: key => keys.get(key), set: (key, value) => keys.set(key, value) },
      FileManager: { local: () => ({ documentsDirectory: () => '/docs', joinPath: (a, b) => a + '/' + b, fileExists: path => files.has(path), readString: path => files.get(path), writeString: (path, value) => files.set(path, value), createDirectory: path => files.set(path, null), listContents: path => [...files.keys()].filter(name => name.startsWith(path + '/')).map(name => name.slice(path.length + 1)), remove: path => files.delete(path) }) },
      UUID: { string: () => `10000000-0000-4000-8000-${String(++sequence).padStart(12, '0')}` },
      Script: { setShortcutOutput: result => { output = result; }, complete: () => { completed = true; } },
    });
    await new vm.Script(`(async () => { ${source}\n })()`).runInContext(context);
    assert.equal(completed, true);
    return { output: JSON.parse(JSON.stringify(output)), alerts };
  }
  return { run, calls, files, keys };
}

test('fixed wake request only; no Instagram session, media fetch or WhatsApp request', async () => {
  const h = harness();
  const { output } = await h.run();
  assert.equal(output.state, 'accepted');
  assert.equal(h.calls.length, 1);
  const request = h.calls[0];
  assert.equal(request.url, 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/instagram-wake');
  assert.equal(request.method, 'POST');
  assert.equal(request.headers.Authorization, 'Bearer ' + CODE);
  assert.equal(request.onRedirect({ url: 'https://elsewhere.invalid' }), null);
  assert.deepEqual(Object.keys(JSON.parse(request.body)).sort(), ['eventId', 'observedAt', 'profile', 'source']);
  assert.equal(JSON.parse(request.body).source, 'scriptable');
  assert.equal(events(h.files).length, 0);
  assert.equal([...h.files.values()].some(value => typeof value === 'string' && value.includes(CODE)), false);
});

test('lost responses retry the same durable ID; later notification preserves a new ID', async () => {
  const h = harness({ responses: [new Error('lost'), new Error('lost')] });
  const first = await h.run();
  assert.equal(first.output.state, 'network_uncertain');
  assert.equal(h.calls[0].body, h.calls[1].body);
  const oldId = JSON.parse(h.calls[0].body).eventId;
  assert.equal(events(h.files).length, 1);
  const second = await h.run();
  assert.equal(second.output.accepted, 2);
  assert.equal(JSON.parse(h.calls[2].body).eventId, oldId);
  assert.notEqual(JSON.parse(h.calls[3].body).eventId, oldId);
});

test('wrong ACK or definitive HTTP rejection conserves pending event without immediate retry', async () => {
  for (const response of [{ status: 429 }, { status: 202, body: { ok: true, accepted: true, eventId: 'different-id' } }]) {
    const h = harness({ responses: [response] });
    const { output } = await h.run();
    assert.equal(output.state, 'server_rejected');
    assert.equal(h.calls.length, 1);
    assert.equal(events(h.files).length, 1);
  }
});

test('setup is manual only; automation never prompts or sends without an individual code', async () => {
  const h = harness({ configured: false });
  const automatic = await h.run();
  assert.equal(automatic.output.state, 'setup_required');
  assert.equal(automatic.alerts, 0);
  assert.equal(h.calls.length, 0);
  const manual = await h.run({ automatic: false });
  assert.equal(manual.output.state, 'configured');
  assert.equal(manual.alerts, 1);
  assert.equal(h.calls.length, 0);
  assert.equal(h.keys.get('uol-instagram.wake-code.v1'), CODE);
});

test('invalid pending storage fails closed without overwriting entries', async () => {
  for (const stored of ['invalid', JSON.stringify({ events: [], overflowHint: { invalid: true } })]) {
    const h = harness({ files: new Map([[pendingPath, stored]]) });
    const { output } = await h.run();
    assert.equal(output.state, 'pending_storage_invalid');
    assert.equal(h.calls.length, 0);
    assert.equal(h.files.get(pendingPath), stored);
  }
});

test('full queue coalesces while offline and admits a fresh notification when draining recovers', async () => {
  const old = Array.from({ length: 64 }, (_, i) => ({ eventId: `20000000-0000-4000-8000-${String(i).padStart(12, '0')}`, profile: 'clubeuol', source: 'scriptable', observedAt: '2026-10-15T19:00:00.000Z' }));
  const h = harness({ files: new Map([[pendingPath, JSON.stringify(old)]]), responses: [new Error('lost'), new Error('lost')] });
  const first = await h.run();
  assert.equal(first.output.state, 'network_uncertain');
  assert.deepEqual(events(h.files), old, 'no attempted identity or payload changed');
  assert.equal(first.output.coalesced, true, 'full queue already holds generic wake signals');
  const second = await h.run();
  assert.equal(second.output.accepted, 8, 'full queue is not a permanent deadlock');
  const recovered = events(h.files);
  assert.equal(recovered.length, 57);
  assert.equal(second.output.coalesced, false);
  assert.equal(recovered.slice(0, 56).every((event, i) => event.eventId === old[i + 8].eventId), true);
  assert.equal(recovered[56].eventId, '10000000-0000-4000-8000-000000000002');
  assert.equal(h.files.get(pendingPath), JSON.stringify(old), 'legacy private evidence is preserved');
});

test('overlapping runs preserve B when A acknowledges its stale snapshot', async () => {
  let entered;
  const requestStarted = new Promise(resolve => { entered = resolve; });
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const h = harness({ responses: [async () => { entered(); await gate; return undefined; }, { status: 429 }] });
  const runA = h.run();
  await requestStarted;
  const runB = await h.run();
  assert.equal(runB.output.state, 'server_rejected');
  assert.equal(events(h.files).length, 2);
  release();
  await runA;
  assert.deepEqual(events(h.files).map(event => event.eventId), ['10000000-0000-4000-8000-000000000002']);
  assert.equal(JSON.parse(h.calls[0].body).eventId, JSON.parse(h.calls[1].body).eventId, 'overlap may repeat only the same immutable wake identity');
});
