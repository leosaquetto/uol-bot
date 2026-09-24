import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { Script as JavaScript } from "node:vm";

const code = readFileSync(new URL("../scriptable/Ultimo-Tweet-TVGlobo.js", import.meta.url), "utf8");
const program = new JavaScript(code);
const ids = { tvglobo: "2102952373022851247", outro_perfil: "2102952373022851248" };
const profileHtml = profile => `<article><a href="/${profile}/status/${ids[profile]}">data</a></article>`;

function setup(initial = {}, pages = {}) {
  const keys = new Map(Object.entries({ "tvglobo-beeper-token-v1": "test-token", ...initial }));
  const sends = [];
  const lifecycle = [];
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
    await program.runInNewContext({
      args: { shortcutParameter: input }, Request,
      Script: {
        setShortcutOutput(value) { output = value; lifecycle.push({ event: "output", value }); },
        complete() { lifecycle.push({ event: "complete" }); },
      },
      config: { runsInApp: false },
      Pasteboard: { copyString() { throw new Error("unexpected clipboard"); } },
      Keychain: { contains: key => keys.has(key), get: key => keys.get(key), set: (key, value) => keys.set(key, value) },
      FileManager: { iCloud() { throw new Error("token already exists"); } },
      console: { log() {}, error() { lifecycle.push({ event: "error" }); } },
    });
    return output;
  }
  return { run, sends, keys, lifecycle, block: () => { blocked = true; }, makeAmbiguous: () => { ambiguous = true; }, requests: () => requests };
}

test("Atalhos recebe a URL antes da finalização, exatamente uma vez", async () => {
  const app = setup();
  const url = await app.run();
  assert.deepEqual(app.lifecycle, [{ event: "output", value: url }, { event: "complete" }]);
});

test("Atalhos recebe saída vazia e finalização mesmo com erro, sem esconder a falha", async () => {
  for (const scenario of ["invalid", "blocked", "ambiguous"]) {
    const app = setup();
    if (scenario === "blocked") app.block();
    if (scenario === "ambiguous") app.makeAmbiguous();
    await assert.rejects(app.run(scenario === "invalid" ? "@bad-user" : null));
    assert.deepEqual(app.lifecycle, [{ event: "error" }, { event: "output", value: "" }, { event: "complete" }]);
    if (scenario !== "ambiguous") assert.equal(app.sends.length, 0);
  }
});

test("Scriptable aceita @, nome e URL do perfil, com cartão dinâmico", async () => {
  for (const input of ["@Outro_Perfil", "outro_perfil", " https://x.com/outro_perfil/ ", "https://twitter.com/outro_perfil"]) {
    const { run, sends } = setup();
    const url = await run(input);
    assert.equal(url, `https://x.com/outro_perfil/status/${ids.outro_perfil}?s=46`);
    assert.equal(sends.length, 1);
    assert.equal(sends[0].body.preview.title, "outro_perfil (@outro_perfil) no X");
    assert.equal(sends[0].body.preview.summary, "");
    assert.match(sends[0].body.preview.imageUrl, /format=jpg&name=large$/);
    assert.equal(sends[0].body.text, "> ```Legenda & texto```\n> 𝕏 ```outro_perfil (@outro_perfil) no X, 23:44```\n> ```" + url + "```");
    assert.equal(sends[0].body.text.split(url).length, 2);
    assert.equal(sends[0].key, undefined);
    assert.equal(sends[0].body.format, "whatsapp");
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
  assert.equal(sends[0].body.preview.title, "TV Globo (@tvglobo) no X");
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
  assert.equal(sends[0].body.preview.avatarUrl, avatar.replace("_200x200", "_400x400"));
});

test("post sem mídia usa avatar 400x400 do perfil sem consulta adicional", async () => {
  const { run, sends, requests } = setup({}, { profileMeta, post: '<meta property="og:description" content="Post só de texto">' });
  await run("@outro_perfil");
  assert.equal(sends[0].body.preview.imageUrl, avatar.replace("_200x200", "_400x400"));
  assert.equal(sends[0].body.preview.summary, "");
  assert.equal(sends[0].body.preview.avatarUrl, undefined);
  assert.equal(requests(), 3);
});

test("imagem genérica e avatar de outro perfil não substituem o avatar do perfil consultado", async () => {
  const { run, sends } = setup({}, { profileMeta, post: '<meta property="og:description" content="Texto"><meta property="og:image" content="https://abs.twimg.com/logo.png"><meta name="twitter:image" content="https://pbs.twimg.com/profile_images/999/outra-pessoa.jpg">' });
  await run("@outro_perfil");
  assert.equal(sends[0].body.preview.imageUrl, avatar.replace("_200x200", "_400x400"));
});

test("vídeo usa seu próprio frame antes do avatar", async () => {
  const frame = "https://pbs.twimg.com/amplify_video_thumb/123/img/frame?format=webp&name=large";
  const { run, sends } = setup({}, { profileMeta, post: `<meta property="og:description" content="Vídeo"><meta property="og:image" content="${avatar}"><meta name="twitter:image" content="${frame}">` });
  await run("@outro_perfil");
  assert.equal(sends[0].body.preview.imageUrl, frame.replace("format=webp", "format=jpg"));
  assert.equal(sends[0].body.preview.avatarUrl, avatar.replace("_200x200", "_400x400"));
});

test("sem mídia e sem avatar disponível, não envia imagem aleatória", async () => {
  const { run, sends } = setup({}, { post: '<meta property="og:description" content="Texto">' });
  await run();
  assert.equal(sends.length, 1);
  assert.equal(sends[0].body.preview.imageUrl, "");
  assert.equal(sends[0].body.preview.avatarUrl, undefined);
});

test("texto integral longo vence metadado cortado, remove linhas vazias e conserva emojis e links completos", async () => {
  const texto = "Texto longo ❤️ ".repeat(700);
  const post = '<meta property="og:description" content="Texto longo…">' +
    `<article><a href="/outro_perfil/status/${ids.outro_perfil}">data</a>` +
    `<div dir="auto" class="whitespace-pre-wrap text-body"><span>${texto}</span><br><br><span>Final &amp; íntegro </span><a href="https://example.com/endereco-inteiro">example.com/end…</a></div></article>`;
  const { run, sends } = setup({}, { profileMeta: profileMeta + '<meta property="og:title" content="Nome Real (@outro_perfil) on X">', post });
  await run("outro_perfil");
  const body = sends[0].body;
  assert.ok(body.text.startsWith("> ```" + texto + "```\n> ```Final & íntegro https://example.com/endereco-inteiro```\n> 𝕏 ```Nome Real (@outro_perfil) no X, 23:44```"));
  assert.ok(body.text.length > 8000);
  assert.equal(body.text.split("\n").some(line => /^>\s*$/.test(line)), false);
  assert.equal(body.text.includes("\n\n"), false);
  assert.equal(body.preview.title, "Nome Real (@outro_perfil) no X");
  assert.equal(body.preview.summary, "");
  assert.equal(body.text.split(body.link).length, 2);
});

test("texto é extraído apenas do artigo do post, incluindo divs aninhadas", async () => {
  const post = '<meta property="og:description" content="Resumo">' +
    '<article><a href="/outra/status/2102952373022851234">data</a><div data-testid="tweetText">Errado</div></article>' +
    `<article><a href="/outro_perfil/status/${ids.outro_perfil}">data</a><div data-testid="tweetText"><div>Primeiro</div>Segundo <img alt="😀" src="emoji.png"></div><div>Comentários</div></article>`;
  const { run, sends } = setup({}, { profileMeta, post });
  await run("outro_perfil");
  assert.ok(sends[0].body.text.includes("> ```Primeiro```\n> ```Segundo 😀```\n> 𝕏 "));
  assert.equal(sends[0].body.text.includes("Comentários"), false);
});

test("metadado aparentemente cortado sem texto integral não é enviado como mensagem completa", async () => {
  const { run, sends } = setup({}, { profileMeta, post: `<meta property="og:description" content="${"a".repeat(299)}…">` });
  await assert.rejects(run(), /texto integral/);
  assert.equal(sends.length, 0);
});

test("post sem mídia mantém texto integral e usa URL entre crases, sem espaçadores extras", async () => {
  const { run, sends } = setup({}, { post: '<meta property="og:description" content="Texto com ``` literal">' });
  await run();
  assert.ok(sends[0].body.text.includes("> ```Texto com ``` literal```\n> 𝕏 "));
  assert.ok(sends[0].body.text.endsWith("> ```https://x.com/tvglobo/status/" + ids.tvglobo + "?s=46```"));
  assert.equal(sends[0].body.text.includes("push by"), false);
  assert.equal(sends[0].body.text.includes("\n\n"), false);
  assert.equal(sends[0].body.text.startsWith("> ```"), true);
});
