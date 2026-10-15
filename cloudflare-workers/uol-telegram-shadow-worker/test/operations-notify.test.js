import test from "node:test";
import assert from "node:assert/strict";

import {
  buildOperationalNotification,
  sendOperationalNotification,
} from "../src/operations-notify.js";

test("alerta técnico usa somente o tópico fixo e campos enumerados", async () => {
  let request;
  const result = await sendOperationalNotification({
    kind: "delivery-queue",
    severity: "critical",
    recovered: false,
    url: "https://private.invalid/secret",
    title: "secret offer title",
    key: "secret-key",
    details: "secret-details",
    summary: "secret-summary",
    error: "secret-error",
    id: "secret-id",
  }, async (url, init) => {
    request = { url, init };
    return new Response("private notification ID", { status: 200 });
  });
  assert.equal(request.url, "https://ntfy.sh/macXntfy-8130");
  assert.equal(request.init.method, "POST");
  assert.equal(request.init.redirect, "manual");
  assert.equal(request.init.headers.Title, "UOL bot - aviso tecnico");
  assert.equal(request.init.headers.Priority, "5");
  assert.equal(request.init.headers["Content-Type"], "text/plain; charset=utf-8");
  assert.match(request.init.body, /Fila de entrega/);
  assert.doesNotMatch(JSON.stringify(request), /secret|private\.invalid/);
  assert.deepEqual(result, { status: "accepted", reason: "http_accepted", httpStatus: 200 });
  assert.doesNotMatch(JSON.stringify(result), /private/);
});

test("tipo e severidade desconhecidos não passam texto livre", () => {
  const result = buildOperationalNotification({
    kind: "secret-value",
    severity: "secret-value",
    recovered: "secret-value",
  });
  assert.equal(result.body, "UOL bot: atenção necessária.\nTipo: Monitor.\nNível: warning.");
  assert.equal(result.priority, "4");
  const recovered = buildOperationalNotification({
    kind: "ticket-api",
    severity: "critical",
    recovered: true,
  });
  assert.match(recovered.body, /incidente recuperado/);
  assert.equal(recovered.priority, "3");
});

test("todos os HTTP 2xx significam aceitação, sem recibo de leitura", async () => {
  for (const status of [200, 201, 202, 204, 299]) {
    const result = await sendOperationalNotification({}, async () => new Response(null, { status }));
    assert.deepEqual(result, { status: "accepted", reason: "http_accepted", httpStatus: status });
    assert.equal(Object.hasOwn(result, "delivered"), false);
  }
});

test("rejeição de fetch é incerta e não expõe a mensagem original", async () => {
  const result = await sendOperationalNotification({}, async () => {
    throw new Error("secret network URL and token");
  });
  assert.deepEqual(result, { status: "uncertain", reason: "fetch_rejected" });
});

test("falha síncrona de invocação é separada de rejeição assíncrona", async () => {
  const result = await sendOperationalNotification({}, () => {
    throw new TypeError("private transport detail");
  });
  assert.deepEqual(result, { status: "uncertain", reason: "fetch_invocation_error" });
});

test("falha de validação local não chama transporte nem vira erro de rede", async (t) => {
  const NativeRequest = globalThis.Request;
  t.mock.method(globalThis, "Request", function () { throw new TypeError("private request detail"); });
  let calls = 0;
  const result = await sendOperationalNotification({}, () => { calls += 1; });
  assert.equal(calls, 0);
  assert.deepEqual(result, { status: "permanent", reason: "request_invalid" });
  t.mock.restoreAll();
  assert.equal(globalThis.Request, NativeRequest);
});

test("resposta inválida é incerta e não inventa HTTP", async () => {
  for (const response of [null, {}, { status: NaN }, { status: 600 },
    { get status() { throw new Error("private response detail"); } }]) {
    assert.deepEqual(await sendOperationalNotification({}, async () => response), {
      status: "uncertain", reason: "response_invalid",
    });
  }
});

test("teste autorizado tem identificação fixa, sem aceitar texto livre", () => {
  const result = buildOperationalNotification({ test: true, title: "private", body: "private" });
  assert.equal(result.title, "UOL bot - teste tecnico");
  assert.match(result.body, /TESTE técnico autorizado/);
  assert.doesNotMatch(JSON.stringify(result), /private/);
});

test("timeout é limitado, aborta e permanece incerto mesmo se fetch não rejeitar", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let signal;
  const resultPromise = sendOperationalNotification({}, async (_url, init) => {
    signal = init.signal;
    return new Promise(() => {});
  });
  await Promise.resolve();
  assert.equal(signal.aborted, false);
  t.mock.timers.tick(5_000);
  assert.deepEqual(await resultPromise, { status: "uncertain", reason: "timeout" });
  assert.equal(signal.aborted, true);
});

test("429 e 5xx preservam Retry-After representável, inclusive acima de uma hora", async () => {
  for (const [status, header, expected] of [
    [429, "17", 17_000],
    [429, "86400", 86_400_000],
    [503, "999999999", 999_999_999_000],
    [500, "0", 1_000],
    [502, "invalid-secret", 60_000],
    [503, "9007199254740991", 60_000],
    [503, "8640000000000", 60_000],
    [504, "", 60_000],
  ]) {
    const result = await sendOperationalNotification({}, async () => new Response("private-error", {
      status,
      headers: { "Retry-After": header },
    }));
    assert.deepEqual(result, {
      status: "retryable",
      reason: status === 429 ? "rate_limited" : "server_error",
      httpStatus: status,
      retryAfterMs: expected,
    });
  }
});

test("Retry-After HTTP-date acima de uma hora é respeitado", async () => {
  const result = await sendOperationalNotification({}, async () => new Response(null, {
    status: 503,
    headers: { "Retry-After": new Date(Date.now() + 86_400_000).toUTCString() },
  }));
  assert.equal(result.status, "retryable");
  assert.ok(result.retryAfterMs > 86_398_000 && result.retryAfterMs <= 86_400_000);
});

test("demais HTTP são rejeições permanentes sem expor o corpo", async () => {
  for (const status of [301, 400, 401, 403, 404, 408, 422]) {
    const result = await sendOperationalNotification({}, async () => new Response("private-error", { status }));
    assert.deepEqual(result, { status: "permanent", reason: "http_rejected", httpStatus: status });
  }
});
