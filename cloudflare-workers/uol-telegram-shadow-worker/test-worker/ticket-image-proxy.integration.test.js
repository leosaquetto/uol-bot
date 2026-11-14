import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

const proxy = "https://media.discordapp.net/attachments/1/2/oferta.jpg?ex=1&is=2&hm=3";

describe("imagem do Telegram em ofertas de ingresso", () => {
  it("busca o proxy do Discord já publicado e envia foto em vez de texto", async () => {
    const calls = [];
    vi.stubGlobal("fetch", async (url, init = {}) => {
      const href = String(url);
      calls.push({ href, body: init.body ? String(init.body) : "" });
      if (href.includes("discord.invalid/api/webhooks/1/token/messages/555")) {
        return Response.json({ id: "555", embeds: [{ image: { proxy_url: proxy } }] });
      }
      if (href.includes("/sendPhoto")) return Response.json({ ok: true, result: { message_id: 900, photo: [{ file_id: "f", file_unique_id: "u", width: 1, height: 1 }] } });
      if (href.includes("/sendMessage")) return Response.json({ ok: true, result: { message_id: 901 } });
      return new Response("blocked", { status: 403 });
    });
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("ticket-image-proxy"), async (instance, state) => {
        instance.env = {
          ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test", TELEGRAM_CHAT_ID: "-100111",
          GRUPO_COMENTARIO_ID: "-100222", CANAL2_DELIVERY_ENABLED: "false",
          DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false",
          DISCORD_WEBHOOK_URL: "https://discord.invalid/api/webhooks/1/token",
        };
        instance.setMetadata("delivery_mode_override", "live");
        instance.setMetadata("delivery_mode_generation", 2);
        instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
        const at = new Date().toISOString();
        const row = {
          id: "ing", link: "https://clube.uol.com.br/campanhasdeingresso/pAA-2-ingressos-show-sp",
          title: "2 INGRESSOS: Show SP", preview_title: "2 INGRESSOS: Show SP",
          category: "campanhasdeingresso", image_url: "https://img.uol/blocked.jpg",
          first_seen_at: at, last_seen_at: at, decision_at: at, status: "delivery_pending",
          would_send_main: 1, delivery_mode: "live", discord_message_id: "555", discord_sent_at: at,
          delivery_generation: instance.currentDeliveryGeneration(),
        };
        state.storage.sql.exec(
          `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
          ...Object.values(row),
        );
        await instance.processDeliveryQueue(new Date(), { targetNames: ["main"], waitForMainImage: true });
        const saved = state.storage.sql.exec("SELECT main_message_kind, main_message_id, discord_image_proxy_url FROM offers WHERE id = 'ing'").one();
        expect(saved).toMatchObject({ main_message_kind: "photo", main_message_id: 900, discord_image_proxy_url: proxy });
        expect(calls.some(call => call.href.includes("/sendMessage"))).toBe(false);
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
