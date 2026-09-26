import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import test from 'node:test';

const source = readFileSync(new URL('../shortcuts/Saquetto Drops.js', import.meta.url), 'utf8');
const requestId = '12345678-1234-4234-8234-123456789abc';
const auth = { baseUrl: 'https://drops.example.test', token: 'test-token-not-a-real-credential' };
const options = [{ id: 'canal', name: 'Canal principal', label: 'Canal principal' }];

async function load(extra = {}) {
  const context = vm.createContext({ ...extra });
  return new vm.Script(`(async () => { ${source}\n return { validateBaseUrl, splitInput, validateText, parseImages, destinationOptions, resolveDestinations, statusMessage, runDrops }; })()`).runInContext(context);
}

function memoryStore() {
  const values = new Map();
  return {
    read: (name) => values.has(name) ? JSON.parse(values.get(name)) : null,
    write: (name, value) => values.set(name, JSON.stringify(value)),
  };
}

function network(replies) {
  const calls = [];
  class Request {
    constructor(url) { this.url = url; }
    async loadString() {
      calls.push({ url: this.url, method: this.method, body: this.body, redirect: this.onRedirect(), insecure: this.allowInsecureRequest });
      const reply = replies.shift();
      assert.ok(reply, 'unexpected HTTP request');
      if (reply.error) throw new Error('private-network-error-must-not-escape');
      this.response = { statusCode: reply.status };
      return JSON.stringify(reply.value);
    }
  }
  return { Request, calls };
}

test('Scriptable source parses; exact text, links, spaces and line breaks survive', async () => {
  const client = await load();
  const original = '  Legenda 🟢\nhttps://example.test/?a=1&b=2\n\núltima linha  ';
  assert.equal(client.validateText(original, 1), original);
  assert.throws(() => client.validateText('x'.repeat(1025), 1), /1024/);
  assert.throws(() => client.validateText('x'.repeat(8001), 0), /8000/);
  assert.throws(() => client.validateText(' \n', 0), /texto ou imagens/);
  assert.equal(client.validateText('', 1), '');
});

test('configuration requires HTTPS and cannot carry credentials, query or redirect target', async () => {
  const client = await load();
  assert.equal(client.validateBaseUrl('https://drops.example.test/'), 'https://drops.example.test');
  for (const bad of ['http://drops.test', 'https://secret@drops.test', 'https://drops.test?token=x', 'https://drops.test/#x', 'https://drops.test\n']) {
    assert.throws(() => client.validateBaseUrl(bad), /HTTPS/);
  }
});

test('destination selection maps cached display names to stable aliases and rejects unknowns', async () => {
  const client = await load();
  const parsed = client.destinationOptions({ destinations: [{ id: 'canal', name: 'Canal principal', type: 'channel' }] });
  assert.equal(parsed[0].label, options[0].label);
  assert.equal(JSON.stringify(client.resolveDestinations('Canal principal\nCanal principal', parsed)), '["canal"]');
  assert.throws(() => client.resolveDestinations('another-user', parsed), /Destino mudou/);
  assert.throws(() => client.destinationOptions({ destinations: [{ id: 'a', name: 'A' }, { id: 'a', name: 'B' }] }), /inválida/);
  const overlap = client.destinationOptions({ destinations: [{ id: 'canal', name: 'Canal principal' }, { id: 'grupo', name: 'canal' }] });
  assert.equal(JSON.stringify(client.resolveDestinations('canal', overlap)), '["grupo"]');
  assert.throws(() => client.destinationOptions({ destinations: [{ id: 'a', name: 'A' }, { id: 'b', name: 'A' }] }), /nomes iguais/);
});

test('Base64 image input accepts JPEG/PNG only and enforces 10 images / 8 MiB', async () => {
  const client = await load();
  assert.equal(client.parseImages('/9j/2Q==')[0].mime, 'image/jpeg');
  assert.equal(client.parseImages('iVBORw0KGgo=')[0].mime, 'image/png');
  assert.equal(client.parseImages('/9j/2Q==\n/9j/2Q==').length, 2);
  assert.throws(() => client.parseImages(Array(11).fill('/9j/2Q==')), /10 imagens/);
  assert.throws(() => client.parseImages('R0lGODlh'), /JPEG e PNG/);
  assert.throws(() => client.parseImages('/9j/' + 'A'.repeat(4 * Math.ceil(8 * 1024 * 1024 / 3))), /8 MiB/);
  const sixMiB = '/9j/' + 'A'.repeat(8 * 1024 * 1024 - 4);
  assert.throws(() => client.parseImages([sixMiB, sixMiB, sixMiB]), /16 MiB/);
});

test('definite rejection unlocks new requests, while idempotency conflict remains unresolved', async () => {
  for (const [status, code, uncertain] of [[400, 'destination_not_allowed', false], [409, 'sending_paused', false], [409, 'idempotency_conflict', true]]) {
    const net = network([{ status, value: { code } }]);
    const client = await load({ Request: net.Request, UUID: { string: () => requestId } });
    const store = memoryStore();
    store.write('destinations.json', { baseUrl: auth.baseUrl, options });
    await assert.rejects(client.runDrops({ action: 'send', destinations: 'Canal principal', text: 'Oi', confirmed: 'yes' }, auth, store), /recusou/);
    assert.equal(store.read(`request-${requestId}.json`).uncertain, uncertain);
    if (!uncertain) {
      const stat = network([{ status: 404, value: {} }]);
      const checking = await load({ Request: stat.Request });
      assert.match((await checking.runDrops({ action: 'retry', confirmed: 'yes' }, auth, store)).message, /antes de enfileirar/);
      assert.equal(stat.calls.length, 1);
    }
  }
});

test('uncertain send is saved before HTTP, blocks new sends, and retries exact UUID/payload', async () => {
  const queued = { requestId, jobs: [{ id: 'job1', destination: 'canal', state: 'queued', code: null }] };
  const net = network([
    { status: 201, value: { mediaId: 'image1' } },
    { error: true },
    { status: 404, value: {} },
    { status: 202, value: queued },
  ]);
  const client = await load({ Request: net.Request, UUID: { string: () => requestId }, Data: { fromBase64String: (value) => ({ binary: value }) } });
  const store = memoryStore();
  store.write('destinations.json', { baseUrl: auth.baseUrl, options });
  const input = { action: 'send', destinations: options[0].label, text: '  Link\nhttps://example.test/a  ', imagesBase64: '/9j/2Q==', confirmed: 'yes' };
  await assert.rejects(client.runDrops(input, auth, store), /sem resposta confirmada/);
  const saved = store.read(`request-${requestId}.json`);
  assert.equal(saved.uncertain, true);
  assert.equal(saved.payload.text, input.text);
  await assert.rejects(client.runDrops(input, auth, store), /Há envio/);
  assert.equal(net.calls.length, 2);
  const result = await client.runDrops({ action: 'retry', confirmed: 'yes' }, auth, store);
  assert.match(result.message, /na fila/);
  assert.equal(net.calls[1].body, net.calls[3].body);
  assert.equal(net.calls[2].method, 'GET');
  assert.equal(net.calls.filter((call) => call.url.endsWith('/media')).length, 1);
  assert.ok(net.calls.every((call) => call.redirect === null && call.insecure === false));
  assert.equal(store.read(`request-${requestId}.json`).uncertain, false);
});

test('retry of an existing server request checks status without resending', async () => {
  const status = { requestId, jobs: [{ id: 'job1', destination: 'canal', state: 'accepted', code: null }] };
  const net = network([{ status: 200, value: status }]);
  const client = await load({ Request: net.Request });
  const store = memoryStore();
  store.write('last-request.json', { requestId });
  store.write(`request-${requestId}.json`, { baseUrl: auth.baseUrl, payload: { requestId, destinations: ['canal'], text: 'Olá', mediaIds: [] }, options, uncertain: true });
  const result = await client.runDrops({ action: 'retry', confirmed: 'yes' }, auth, store);
  assert.equal(net.calls.length, 1);
  assert.equal(net.calls[0].method, 'GET');
  assert.match(result.message, /aceito pelo WhatsApp/);
  assert.match(result.message, /não confirma recebimento/);
});

test('generated shortcut uses native selection, multiline editing, preview and confirmation before headless API send', () => {
  const path = fileURLToPath(new URL('../shortcuts/Saquetto Drops.unsigned.shortcut', import.meta.url));
  const workflow = JSON.parse(execFileSync('python3', ['-c', 'import plistlib,json,sys; print(json.dumps(plistlib.load(open(sys.argv[1],"rb"))))', path], { encoding: 'utf8' }));
  const actions = workflow.WFWorkflowActions;
  const scriptable = actions.filter((action) => action.WFWorkflowActionIdentifier === 'dk.simonbs.Scriptable.ParameterizedRunScriptIntent');
  assert.equal(scriptable.length, 5);
  assert.ok(scriptable.every(({ WFWorkflowActionParameters: p }) => p.fileName === 'Saquetto Drops' && p.runInApp === false && p.parameter && !p.images && !p.texts));
  assert.ok(actions.some((action) => action.WFWorkflowActionParameters.WFChooseFromListActionSelectMultiple === true));
  assert.ok(actions.some((action) => action.WFWorkflowActionParameters.WFAllowsMultilineText === true));
  assert.ok(actions.some((action) => action.WFWorkflowActionIdentifier === 'is.workflow.actions.previewdocument'));
  const confirmation = actions.findIndex((action) => action.WFWorkflowActionParameters.WFAlertActionTitle === 'Confirmar envio');
  const sendDictionary = actions.findIndex((action) => action.WFWorkflowActionIdentifier === 'is.workflow.actions.dictionary' && action.WFWorkflowActionParameters.WFItems.Value.WFDictionaryFieldValueItems.some((item) => item.WFKey.Value.string === 'action' && item.WFValue.Value.string === 'send'));
  assert.ok(confirmation > 0 && sendDictionary > confirmation);
  assert.equal(actions[confirmation].WFWorkflowActionParameters.WFAlertActionCancelButtonShown, true);
  assert.ok(actions.some((action) => action.WFWorkflowActionParameters.WFBase64LineBreakMode === 'None'));
  assert.equal(actions.filter((action) => action.WFWorkflowActionIdentifier === 'is.workflow.actions.text.combine' && action.WFWorkflowActionParameters.WFTextSeparator === 'New Lines').length, 2);
});
