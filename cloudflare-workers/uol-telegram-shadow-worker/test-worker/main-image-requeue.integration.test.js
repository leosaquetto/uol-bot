import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

// Oferta de 17/11 que esgotou as tentativas de foto antes da correção do 403 (#154).
const ADMIN = { Authorization: "Bearer vitest-admin-token-not-a-secret" };
const link = "https://clube.uol.com.br/campanhasdeingresso/pYy-2-ingressos-22-11-teatro-uol-sp";
const apiImage = "https://ddrxgn8ucibei.cloudfront.net/beneficios/6afc371703617_268d9b63116b15afe2ddd2618ca702f9.png";
const pageImage = "https://ddrxgn8ucibei.cloudfront.net/beneficios/cb91a74333d3abf77a4f67291087f16f.png";

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test", TELEGRAM_CHAT_ID: "-100111",
    GRUPO_COMENTARIO_ID: "-100222", CANAL2_DELIVERY_ENABLED: "false",
    DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(state, id, fields = {}) {
  const at = new Date(Date.now() - 3_600_000).toISOString();
  const row = {
    id, link, title: "2 INGRESSOS: Teatro UOL", preview_title: "2 INGRESSOS: Teatro UOL",
    category: "campanhasdeingresso", image_url: apiImage,
    first_seen_at: at, last_seen_at: at, decision_at: at, status: "sent",
    would_send_main: 1, delivery_mode: "live", delivery_generation: 1,
    main_sent_at: at, main_message_id: 500, main_message_kind: "text",
    telegram_image_strategy: "text_timeout", main_image_upgrade_attempts: 10,
    main_image_upgrade_error: "offer_image_http_403",
    main_image_upgrade_next_attempt_at: "2099-01-01T00:00:00.000Z", ...fields,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

const offer = (state, id) => state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();

describe("requeue-delivery main_image", () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it("reabre só a troca por foto e a manutenção edita a mesma mensagem", async () => {
    const calls = [];
    vi.stubGlobal("fetch", async (url, init = {}) => {
      const href = String(url);
      calls.push(href);
      if (href === apiImage) return new Response("<Error/>", { status: 403 });
      if (href === link) return new Response(`<meta property="og:image" content="${pageImage}">`, { headers: { "Content-Type": "text/html" } });
      if (href === pageImage) return new Response(new Uint8Array([0x89, 0x50, 0x4e, 0x47]), { headers: { "Content-Type": "image/png" } });
      if (href.endsWith("/editMessageMedia") && typeof init.body !== "string") {
        return Response.json({ ok: true, result: { message_id: 500, photo: [{ file_id: "f", file_unique_id: "u", width: 280, height: 280 }] } });
      }
      if (href.includes("api.telegram.org")) return Response.json({ ok: false, description: "Bad Request: failed to get HTTP URL content" }, { status: 400 });
      return new Response("blocked", { status: 403 });
    });
    const id = "pyy-2-ingressos-22-11-teatro-uol-sp";
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, (instance, state) => { configure(instance); insert(state, id); });

    const body = JSON.stringify({ id, target: "main_image" });
    const denied = await exports.default.fetch("https://worker.test/requeue-delivery", { method: "POST", body });
    expect(denied.status).toBe(401);
    const response = await exports.default.fetch("https://worker.test/requeue-delivery", { method: "POST", headers: ADMIN, body });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ ok: true, id, target: "main_image" });

    await runInDurableObject(stub, async (instance, state) => {
      expect(offer(state, id)).toMatchObject({
        main_image_upgrade_attempts: 9, main_image_upgrade_error: "", main_image_upgrade_next_attempt_at: "",
      });
      const ledger = state.storage.sql.exec(
        "SELECT target, operation, state, external_id FROM delivery_events WHERE offer_id = ?", id,
      ).toArray();
      expect(ledger).toEqual([{ target: "main", operation: "image_upgrade", state: "requeued", external_id: "500" }]);

      configure(instance);
      const result = await instance.upgradeTimedOutMainImages(new Date());
      expect(result).toMatchObject({ upgraded: 1, failed: 0 });
      expect(offer(state, id)).toMatchObject({ main_message_kind: "photo", main_message_id: 500, main_image_upgrade_attempts: 10 });
    });
    // Nunca reenvia a oferta: nada de sendMessage/sendPhoto, só a edição da mensagem 500.
    expect(calls.some((href) => href.endsWith("/sendMessage") || href.endsWith("/sendPhoto"))).toBe(false);
    expect(calls).toContain(pageImage);
  });

  it("recusa ofertas que não estão presas em texto com tentativas esgotadas", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("unexpected_http"); }));
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("main-image-requeue-refuse"), (instance, state) => {
      configure(instance);
      insert(state, "ainda-tentando", { main_image_upgrade_attempts: 4 });
      insert(state, "ja-foto", { main_message_kind: "photo" });
      insert(state, "sem-mensagem", { main_message_id: 0 });
      insert(state, "sem-imagem", { image_url: "" });
      insert(state, "outra-estrategia", { telegram_image_strategy: "upload" });
      for (const id of ["ainda-tentando", "ja-foto", "sem-mensagem", "sem-imagem", "outra-estrategia"]) {
        const before = offer(state, id);
        expect(() => instance.requeueDelivery(id, "main_image")).toThrow("delivery_main_image_not_requeueable");
        expect(offer(state, id)).toEqual(before);
      }
    });
    expect(fetch).not.toHaveBeenCalled();
  });
});
