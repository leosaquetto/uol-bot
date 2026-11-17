import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

// Caso real de 17/11: a imageUrl da API dá 403 e a página traz a arte em /beneficios/.
const link = "https://clube.uol.com.br/campanhasdeingresso/pYy-2-ingressos-22-11-teatro-uol-sp";
const apiImage = "https://ddrxgn8ucibei.cloudfront.net/beneficios/6afc371703617_268d9b63116b15afe2ddd2618ca702f9.png";
const pageImage = "https://ddrxgn8ucibei.cloudfront.net/beneficios/cb91a74333d3abf77a4f67291087f16f.png";
const okImage = "https://ddrxgn8ucibei.cloudfront.net/beneficios/arte-ok.png";
const pageHtml = `<meta property="og:image" content="${pageImage}">
<img src="https://d310rdc9v8uuq.cloudfront.net/beneficios_categorias/0b31e54ad4def1df288c491e48ab0ce7.png">
<div id="ilustracoes"><img src="${pageImage}"></div>`;
const png = () => new Response(new Uint8Array([0x89, 0x50, 0x4e, 0x47]), {
  headers: { "Content-Type": "image/png" },
});

function stubFetch(calls) {
  vi.stubGlobal("fetch", async (url, init = {}) => {
    const href = String(url);
    calls.push(href);
    // Discord também não conseguiu baixar a arte: o embed volta sem proxy_url.
    if (href.includes("discord.invalid/api/webhooks/1/token/messages/555")) return Response.json({ id: "555", embeds: [] });
    if (href === apiImage) return new Response("<Error/>", { status: 403 });
    if (href === link) {
      // Como o Clube em produção: sem User-Agent responde 405.
      if (!new Headers(init.headers).get("User-Agent")) return new Response("", { status: 405 });
      return new Response(pageHtml, { headers: { "Content-Type": "text/html" } });
    }
    if (href === pageImage || href === okImage) {
      // Como o CloudFront do Clube: sem User-Agent responde 403.
      if (!new Headers(init.headers).get("User-Agent")) return new Response("<Error/>", { status: 403 });
      return png();
    }
    if (href.includes("api.telegram.org")) {
      // Telegram não consegue baixar a URL sozinho; só o upload multipart passa.
      if (typeof init.body === "string") {
        return Response.json({ ok: false, description: "Bad Request: failed to get HTTP URL content" }, { status: 400 });
      }
      if (href.endsWith("/sendPhoto") || href.endsWith("/editMessageMedia")) {
        return Response.json({ ok: true, result: { message_id: 900, photo: [{ file_id: "f", file_unique_id: "u", width: 280, height: 280 }] } });
      }
    }
    return new Response("blocked", { status: 403 });
  });
}

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test", TELEGRAM_CHAT_ID: "-100111",
    GRUPO_COMENTARIO_ID: "-100222", CANAL2_DELIVERY_ENABLED: "false",
    DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.setMetadata("delivery_mode_generation", 2);
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(state, row) {
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

function textTimeoutRow(imageUrl) {
  const at = new Date(Date.now() - 120_000).toISOString();
  return {
    id: "pyy-2-ingressos-22-11-teatro-uol-sp", link,
    title: "2 INGRESSOS: Teatro UOL", preview_title: "2 INGRESSOS: Teatro UOL",
    category: "campanhasdeingresso", image_url: imageUrl,
    first_seen_at: at, last_seen_at: at, decision_at: at, status: "sent",
    would_send_main: 1, delivery_mode: "live",
    main_message_id: 500, main_message_kind: "text", telegram_image_strategy: "text_timeout",
    main_image_upgrade_attempts: 0, main_image_upgrade_next_attempt_at: "",
  };
}

async function upgradeWrites(name, imageUrl) {
  const calls = [];
  stubFetch(calls);
  try {
    return await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(name), async (instance, state) => {
      configure(instance);
      insert(state, textTimeoutRow(imageUrl));
      const before = instance.storageUsage.rowsWritten;
      const result = await instance.upgradeTimedOutMainImages(new Date());
      const writes = instance.storageUsage.rowsWritten - before;
      const saved = state.storage.sql.exec(
        "SELECT main_message_kind, main_image_upgrade_error FROM offers WHERE id = 'pyy-2-ingressos-22-11-teatro-uol-sp'",
      ).one();
      return { result, writes, saved, calls };
    });
  } finally {
    vi.unstubAllGlobals();
  }
}

describe("arte da página da oferta quando a imageUrl da API responde 403", () => {
  it("upgrade tardio troca o texto pela arte da página sem gravação extra", async () => {
    const fallback = await upgradeWrites("offer-page-image-fallback", apiImage);
    expect(fallback.result).toMatchObject({ upgraded: 1, failed: 0 });
    expect(fallback.saved).toMatchObject({ main_message_kind: "photo", main_image_upgrade_error: "" });
    expect(fallback.calls).toContain(link);
    expect(fallback.calls).toContain(pageImage);
    expect(fallback.calls.some((href) => href.includes("beneficios_categorias"))).toBe(false);

    // Mesmo upgrade com uma imageUrl que funciona: a alternativa não pode custar escrita.
    const direct = await upgradeWrites("offer-page-image-direct", okImage);
    expect(direct.result).toMatchObject({ upgraded: 1, failed: 0 });
    expect(direct.calls).not.toContain(link);
    expect(fallback.writes).toBe(direct.writes);
  });

  it("envio principal sai como foto em vez de texto", async () => {
    const calls = [];
    stubFetch(calls);
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("offer-page-image-main"), async (instance, state) => {
        configure(instance);
        instance.env.DISCORD_WEBHOOK_URL = "https://discord.invalid/api/webhooks/1/token";
        // Sem a retenção de teatro, para a oferta sair já nesta passagem.
        instance.env.THEATER_GROUP_HOLD_SECONDS = "0";
        const at = new Date().toISOString();
        insert(state, {
          id: "pyy-2-ingressos-22-11-teatro-uol-sp", link,
          title: "2 INGRESSOS: Teatro UOL", preview_title: "2 INGRESSOS: Teatro UOL",
          category: "campanhasdeingresso", image_url: apiImage,
          first_seen_at: at, last_seen_at: at, decision_at: at, status: "delivery_pending",
          would_send_main: 1, delivery_mode: "live", discord_message_id: "555", discord_sent_at: at,
          delivery_generation: instance.currentDeliveryGeneration(),
        });
        await instance.processDeliveryQueue(new Date(), { targetNames: ["main"], waitForMainImage: true });
        const saved = state.storage.sql.exec(
          "SELECT main_message_kind, main_message_id, telegram_image_strategy FROM offers WHERE id = 'pyy-2-ingressos-22-11-teatro-uol-sp'",
        ).one();
        expect(saved).toMatchObject({ main_message_kind: "photo", main_message_id: 900, telegram_image_strategy: "upload" });
        expect(calls.some((href) => href.endsWith("/sendMessage"))).toBe(false);
        expect(calls).toContain(pageImage);
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
