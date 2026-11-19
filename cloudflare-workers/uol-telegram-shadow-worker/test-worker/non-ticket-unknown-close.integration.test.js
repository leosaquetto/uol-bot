import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

// 19/11/2026: duas ofertas comuns ficaram 16 h em "entrega incerta" (timeout do sendPhoto)
// e mantiveram o /readyz em 503. Oferta comum incerta agora é fechada sem reenviar nem alertar.
const AT = new Date(Date.now() - 3_600_000).toISOString();

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test", TELEGRAM_CHAT_ID: "-100111",
    CANAL2_DELIVERY_ENABLED: "false", DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(state, id, link, category) {
  const row = {
    id, link, title: "10% OFF", preview_title: "10% OFF", category, description: "d",
    first_seen_at: AT, last_seen_at: AT, decision_at: AT, status: "delivery_pending",
    would_send_main: 1, delivery_mode: "live", delivery_generation: 1,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

const offer = (state, id) => state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();

describe("entrega incerta de oferta que não é ingresso", () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it("fecha a oferta comum sem reenviar e deixa o ingresso incerto", async () => {
    const posts = [];
    vi.stubGlobal("fetch", async (url) => {
      const href = String(url);
      if (href.includes("api.telegram.org")) {
        posts.push(href);
        throw new DOMException("timeout", "TimeoutError");
      }
      return new Response("blocked", { status: 403 });
    });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "comum", "https://clube.uol.com.br/studioseniorgym/edx-10-off-em-todos-os-servicos", "studioseniorgym");
      insert(state, "ingresso", "https://clube.uol.com.br/campanhasdeingresso/pYy-2-ingressos-22-11-teatro-uol-sp", "campanhasdeingresso");
      await instance.processDeliveryQueue(new Date(), { priorityIds: ["comum", "ingresso"], waitForMainImage: false });
      expect(offer(state, "comum")).toMatchObject({
        status: "delivery_quarantined", delivery_quarantine_reason: "unknown_closed_unverified:main",
        main_sent_at: "", main_message_id: 0,
      });
      const ticket = offer(state, "ingresso");
      expect(ticket.status).toBe("delivery_unknown");
      expect(ticket.delivery_quarantine_reason).toBe("");
    });
  });
});
