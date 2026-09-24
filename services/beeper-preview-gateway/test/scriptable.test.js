import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const code = readFileSync(new URL("../scriptable/Ultimo-Tweet-TVGlobo.js", import.meta.url), "utf8");
const execute = new AsyncFunction("args", "Request", "Script", "config", "Pasteboard", "Keychain", "FileManager", "console", code);
const ids = { tvglobo: "2102952373022851247", outro_perfil: "2102952373022851248" };
const profileHtml = profile => `<article><a href="/${profile}/status/${ids[profile]}">data</a></article>`;

function setup(initial = {}, pages = {}) {
  const keys = new Map(Object.entries({ "tvglobo-beeper-token-v1": "test-token", ...initial }));
  const sends = [];
  let blocked = false;
  let ambiguous = false;
  let requests = 0;
  class Request {
    constructor(url) { this.url = url; this.response = { statusCode: 200 }; requests++; }
    async loadString() {
      if (this.method === "POST") {
        assert.equal(this.url, "https://163-176-194-58.sslip.io/v1/send-x-post");
        assert.equal(this.onRedirect({ url: "https://evil.test" }), null);
        sends.push({ body: JSON.parse(this.body), key: this.headers["Idempotency-Key"] });
        if (ambiguous) { this.response.statusCode = 503; return JSON.stringify({ code: "delivery_unknown" }); }
        return JSON.stringify({ deliveryState: "confirmed_by_whatsapp_bridge" });
      }
      if (blocked) { this.response.statusCode = 429; return ""; }
      if (this.url.includes("/status/")) return pages.post ?? '<meta property="og:description" content="Legenda &amp; texto"><meta property="og:image" content="https://pbs.twimg.com/media/test?format=webp&amp;name=large">';
      const profile = this.url.split("/").at(-1);
      assert.ok(ids[profile]);
      return (pages.profileMeta || "") + profileHtml(profile);
    }
  }
  async function run(input = null) {
    let output;
    await execute({ shortcutParameter: input }, Request,
      { setShortcutOutput: value => { output = value; }, complete() {} }, { runsInApp: false },
      { copyString() { throw new Error("unexpected clipboard"); } },
      { contains: key => keys.has(key), get: key => keys.get(key), set: (key, value) => keys.set(key, value) },
      { iCloud() { throw new Error("token already exists"); } }, { log() {} });
    return output;
  }
  return { run, sends, keys, block: () => { blocked = true; }, makeAmbiguous: () => { ambiguous = true; }, requests: () => requests };
}

test("Scriptable aceita @, nome e URL do perfil, com cartão dinâmico", async () => {
  for (const input of ["@Outro_Perfil", "outro_perfil", " https://x.com/outro_perfil/ ", "https://twitter.com/outro_perfil"]) {
    const { run, sends } = setup();
    const url = await run(input);
    assert.equal(url, `https://x.com/outro_perfil/status/${ids.outro_perfil}`);
    assert.equal(sends.length, 1);
    assert.equal(sends[0].body.preview.title, "X • @outro_perfil");
    assert.equal(sends[0].body.preview.summary, "Legenda & texto");
    assert.match(sends[0].body.preview.imageUrl, /format=jpg&name=large$/);
    assert.ok(sends[0].body.text.includes(url));
    assert.equal(sends[0].key, undefined);
  }
});

test("Scriptable envia a cada execução, mesmo com histórico antigo ou mesmo perfil", async () => {
  const { run, sends } = setup({ "tvglobo-beeper-ultimo-enviado-v1": ids.tvglobo });
  await run();
  assert.equal(sends.length, 1);
  await run("@outro_perfil");
  await run("@tvglobo");
  await run("@outro_perfil");
  assert.equal(sends.length, 4);
});

test("Scriptable preserva HTML de entrada e dispensa chave idempotente", async () => {
  const { run, sends, requests } = setup();
  await run(profileHtml("tvglobo"));
  assert.equal(requests(), 2);
  assert.equal(sends[0].key, undefined);
  assert.equal(sends[0].body.preview.title, "TV Globo • @tvglobo");
});

test("Scriptable rejeita entrada inválida antes de consultar ou enviar", async () => {
  for (const input of ["a,b", "@bad-user", "https://evil.test/perfil", "https://x.com/tvglobo/status/123", "verylongusernameover15", ["tvglobo"]]) {
    const { run, sends, requests } = setup();
    await assert.rejects(run(input));
    assert.equal(sends.length, 0);
    assert.equal(requests(), 0);
  }
});

test("Scriptable para em 429 e não marca entrega ambígua como sucesso", async () => {
  const first = setup();
  first.block();
  await assert.rejects(first.run(), /HTTP 429/);
  assert.equal(first.sends.length, 0);
  assert.equal(first.requests(), 1);
  const second = setup();
  second.makeAmbiguous();
  await assert.rejects(second.run("@outro_perfil"), /delivery_unknown/);
  assert.equal(second.keys.has("x-beeper-ultimo-enviado-v1-outro_perfil"), false);
});

const avatar = "https://pbs.twimg.com/profile_images/123/avatar_200x200.jpg";
const profileMeta = `<meta property="og:image" content="${avatar}"><meta name="twitter:image" content="https://pbs.twimg.com/profile_banners/123/banner">`;

test("thumbnail prioriza imagem do post sobre avatar do perfil", async () => {
  const { run, sends } = setup({}, { profileMeta });
  await run("@outro_perfil");
  assert.match(sends[0].body.preview.imageUrl, /^https:\/\/pbs\.twimg\.com\/media\//);
});

test("post sem imagem usa avatar do perfil sem consulta adicional", async () => {
  const { run, sends, requests } = setup({}, { profileMeta, post: '<meta property="og:description" content="Post só de texto">' });
  await run("@outro_perfil");
  assert.equal(sends[0].body.preview.imageUrl, avatar);
  assert.equal(sends[0].body.preview.summary, "Post só de texto");
  assert.equal(requests(), 3);
});

test("imagem genérica do X ou avatar de outro perfil no post não substitui a foto do perfil consultado", async () => {
  const { run, sends } = setup({}, { profileMeta, post: '<meta property="og:description" content="Texto"><meta property="og:image" content="https://abs.twimg.com/logo.png"><meta name="twitter:image" content="https://pbs.twimg.com/profile_images/999/outra-pessoa.jpg">' });
  await run("@outro_perfil");
  assert.equal(sends[0].body.preview.imageUrl, avatar);
});

test("vídeo usa seu próprio frame antes do avatar", async () => {
  const frame = "https://pbs.twimg.com/amplify_video_thumb/123/img/frame?format=webp&name=large";
  const { run, sends } = setup({}, { profileMeta, post: `<meta property="og:description" content="Vídeo"><meta property="og:image" content="${avatar}"><meta name="twitter:image" content="${frame}">` });
  await run("@outro_perfil");
  assert.equal(sends[0].body.preview.imageUrl, frame.replace("format=webp", "format=jpg"));
});

test("sem mídia e sem avatar disponível, não envia imagem aleatória", async () => {
  const { run, sends } = setup({}, { post: '<meta property="og:description" content="Texto">' });
  await assert.rejects(run(), /nenhuma imagem utilizável/);
  assert.equal(sends.length, 0);
});
