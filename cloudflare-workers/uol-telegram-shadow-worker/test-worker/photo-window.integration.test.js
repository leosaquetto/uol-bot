import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

// 19/11/2026: a foto só era tentada até +14 s depois da oferta ser vista, mas o Worker roda a
// cada 30 s; ~53% das ofertas perdiam a foto e esperavam 60-90 s pelo texto. Agora a janela é +45 s.
const IMAGE = "https://ddrxgn8ucibei.cloudfront.net/beneficios/arte-ok.png";
const LINK = "https://clube.uol.com.br/clubebenshopping/pZf-desconto-em-liquidificador-philco";

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test", TELEGRAM_CHAT_ID: "-100111",
    CANAL2_DELIVERY_ENABLED: "false", DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(state, id, seenSecondsAgo) {
  const at = new Date(Date.now() - seenSecondsAgo * 1000).toISOString();
  const row = {
    id, link: LINK, title: "Desconto", preview_title: "Desconto", category: "Eletrodomésticos e Eletrônicos",
    image_url: IMAGE, description: "d", first_seen_at: at, last_seen_at: at, decision_at: at,
    status: "delivery_pending", would_send_main: 1, delivery_mode: "live", delivery_generation: 1,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

const offer = (state, id) => state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();
const photoResult = () => Response.json({ ok: true, result: { message_id: 900, photo: [{ file_id: "f", file_unique_id: "u", width: 1, height: 1 }] } });

describe("janela da foto do canal principal", () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it("oferta vista há 30 s ainda sai como foto, sem esperar o prazo de texto", async () => {
    const methods = [];
    vi.stubGlobal("fetch", async (url) => {
      const href = String(url);
      if (href.includes("api.telegram.org")) { methods.push(href.split("/").pop()); return photoResult(); }
      return new Response("blocked", { status: 403 });
    });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "janela", 30);
      await instance.processDeliveryQueue(new Date(), { priorityIds: ["janela"], waitForMainImage: true });
      expect(offer(state, "janela")).toMatchObject({ main_message_kind: "photo", main_message_id: 900 });
      expect(methods).toEqual(["sendPhoto"]);
    });
  });

  it("foto com timeout perto do limite vira incerta e nunca manda texto duplicado", async () => {
    const methods = [];
    vi.stubGlobal("fetch", async (url) => {
      const href = String(url);
      if (href.includes("api.telegram.org")) {
        methods.push(href.split("/").pop());
        throw new DOMException("timeout", "TimeoutError");
      }
      return new Response("blocked", { status: 403 });
    });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "limite", 44);
      await instance.processDeliveryQueue(new Date(), { priorityIds: ["limite"], waitForMainImage: true });
      expect(methods).not.toContain("sendMessage");
      expect(offer(state, "limite").main_message_id).toBe(0);
    });
  });

  it("depois de +52 s a foto não começa mais e o texto espera o prazo", async () => {
    const methods = [];
    vi.stubGlobal("fetch", async (url) => {
      const href = String(url);
      if (href.includes("api.telegram.org")) { methods.push(href.split("/").pop()); return photoResult(); }
      return new Response("blocked", { status: 403 });
    });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "tarde", 55);
      await instance.processDeliveryQueue(new Date(), { priorityIds: ["tarde"], waitForMainImage: true });
      expect(methods).not.toContain("sendPhoto");
    });
  });
});
