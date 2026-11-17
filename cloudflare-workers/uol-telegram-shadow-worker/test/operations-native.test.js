import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Log, LogLevel, Miniflare, Response as MiniflareResponse } from "miniflare";

test("transporte operacional usa fetch nativo do workerd com saída interceptada", async (t) => {
  const source = await readFile(new URL("../src/operations-notify.js", import.meta.url), "utf8");
  assert.doesNotMatch(source, /https:\/\/ntfy\.sh/, "o tópico público antigo não pode voltar");
  const topic = "https://ntfy.example.test/";
  const requests = [];
  let responseStatus = 200;
  let responseDelayMs = 0;
  const runtime = new Miniflare({
    compatibilityDate: "2026-07-30",
    modules: true,
    cf: false,
    log: new Log(LogLevel.NONE),
    script: `${source}
      export default {
        async fetch(request) {
          const path = new URL(request.url).pathname;
          if (path === "/legacy") {
            try {
              await fetch("https://ntfy.example.test/", { method: "POST", redirect: "error", body: "native regression" });
              return Response.json({ rejected: false });
            } catch (error) {
              return Response.json({
                rejected: true,
                invalidRedirect: /Invalid redirect value.*follow.*manual/.test(String(error.message)),
              });
            }
          }
          let aborted = false;
          const result = await sendOperationalNotification(
            { kind: "delivery-queue", severity: "critical", details: "excluded-private-detail", id: "ep:opened" },
            { OPS_NTFY_SERVER: "https://ntfy.example.test", OPS_NTFY_TOKEN: "tk_native", OPS_NTFY_TOPIC: "saquettoos" },
            (url, init) => {
              init.signal.addEventListener("abort", () => { aborted = true; });
              return fetch(url, init);
            },
          );
          return Response.json({ result, aborted });
        },
      }`,
    outboundService: async (request) => {
      requests.push({
        url: request.url,
        method: request.method,
        headers: Object.fromEntries(request.headers),
        body: await request.text(),
      });
      if (responseDelayMs) await new Promise((resolve) => setTimeout(resolve, responseDelayMs));
      const ok = responseStatus >= 200 && responseStatus < 300;
      return new MiniflareResponse(ok ? JSON.stringify({ id: "x", event: "message", topic: "saquettoos" }) : null, {
        status: responseStatus,
        headers: { Location: "https://redirect.invalid/blocked" },
      });
    },
  });
  t.after(() => runtime.dispose());
  const dispatch = async (path = "/notify") => {
    const response = await runtime.dispatchFetch(`https://worker.invalid${path}`);
    assert.equal(response.status, 200);
    return response.json();
  };

  await t.test("redirect error original falha antes de qualquer saída nativa", async () => {
    assert.deepEqual(await dispatch("/legacy"), { rejected: true, invalidRedirect: true });
    assert.equal(requests.length, 0);
  });

  await t.test("POST nativo mantém destino, cabeçalhos, corpo e aceitação 2xx", async () => {
    for (const status of [200, 201, 202, 299]) {
      responseStatus = status;
      const before = requests.length;
      assert.deepEqual(await dispatch(), {
        result: { status: "accepted", reason: "http_accepted", httpStatus: status },
        aborted: false,
      });
      assert.equal(requests.length, before + 1);
      const request = requests.at(-1);
      assert.ok(request.url === topic, "o fetch nativo deve usar exatamente o destino fixo");
      assert.equal(request.method, "POST");
      assert.equal(request.headers["content-type"], "application/json");
      assert.equal(request.headers.authorization, "Bearer tk_native");
      const payload = JSON.parse(request.body);
      assert.equal(payload.topic, "saquettoos");
      assert.equal(payload.title, "UOL bot - aviso tecnico");
      assert.equal(payload.priority, 5);
      assert.deepEqual(JSON.parse(payload.message), {
        id: "uol-ops-ep:opened", source: "saquettoos", title: "UOL bot - aviso tecnico",
        body: "UOL bot: atenção necessária.\nTipo: Fila de entrega.\nNível: critical.", route: "saquettoOS://home",
      });
      assert.doesNotMatch(request.body, /excluded-private-detail/);
    }
  });

  await t.test("301 e 307 são rejeições permanentes e nunca seguem Location", async () => {
    for (const status of [301, 307]) {
      responseStatus = status;
      const before = requests.length;
      assert.deepEqual(await dispatch(), {
        result: { status: "permanent", reason: "http_rejected", httpStatus: status },
        aborted: false,
      });
      assert.equal(requests.length, before + 1);
      assert.ok(requests.at(-1).url === topic, "redirecionamento não deve gerar segunda saída");
    }
  });

  await t.test("timeout de oito segundos aborta o sinal do fetch nativo", async () => {
    responseStatus = 200;
    responseDelayMs = 8_250;
    const before = requests.length;
    assert.deepEqual(await dispatch(), {
      result: { status: "uncertain", reason: "timeout" },
      aborted: true,
    });
    assert.equal(requests.length, before + 1);
  });
});
