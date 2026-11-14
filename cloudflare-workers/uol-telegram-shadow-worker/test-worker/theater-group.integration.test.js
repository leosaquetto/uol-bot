import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

const base = "https://clube.uol.com.br/campanhasdeingresso";

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test-theater",
    TELEGRAM_CHAT_ID: "-100111", GRUPO_COMENTARIO_ID: "-100222",
    CANAL2_DELIVERY_ENABLED: "false", DISCORD_DELIVERY_ENABLED: "false",
    BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.setMetadata("delivery_mode_generation", 2);
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(instance, state, id, link, title, at, image = "https://img.uol/a.jpg") {
  const row = {
    id, link, title, preview_title: title, category: "campanhasdeingresso",
    first_seen_at: at, last_seen_at: at, decision_at: at, status: "delivery_pending",
    would_send_main: 1, delivery_mode: "live", image_url: image,
    delivery_generation: instance.currentDeliveryGeneration(),
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

describe("agrupamento de ofertas de teatro", () => {
  it("retém, junta as datas em uma única mensagem e não envia as demais", async () => {
    const bodies = [];
    vi.stubGlobal("fetch", async (url, init = {}) => {
      bodies.push(String(init.body instanceof FormData ? [...init.body.values()].join("\n") : init.body || ""));
      return Response.json({ ok: true, result: { message_id: 700 + bodies.length } });
    });
    try {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("theater-group"), async (instance, state) => {
        configure(instance);
        const t0 = new Date("2026-11-13T13:00:00.000Z");
        insert(instance, state, "d14", `${base}/pWS-2-ingressos-14-11-comedia-o-tigre-rj-teatro-vanucci-rj`, "2 INGRESSOS: 14/11 Comédia O Tigre - RJ", t0.toISOString());
        insert(instance, state, "d15", `${base}/pWT-2-ingressos-15-11-comedia-o-tigre-rj-teatro-vanucci-rj`, "2 INGRESSOS: 15/11 Comédia O Tigre - RJ", new Date(t0.getTime() + 5_000).toISOString());
        insert(instance, state, "show", `${base}/pWJ-2-ingressos-14-11-tokio-marine-hall-sp`, "2 INGRESSOS: 14/11 Tokio Marine Hall SP", new Date(t0.getTime() + 1_000).toISOString(), "https://img.uol/show.jpg");

        // Dentro da retenção: só o show (sem teatro no slug) sai.
        await instance.processDeliveryQueue(new Date(t0.getTime() + 30_000), { targetNames: ["main"], waitForMainImage: false });
        const held = state.storage.sql.exec("SELECT id, main_sent_at FROM offers ORDER BY id").toArray();
        expect(held.find(row => row.id === "show").main_sent_at).not.toBe("");
        expect(held.find(row => row.id === "d14").main_sent_at).toBe("");
        expect(held.find(row => row.id === "d15").main_sent_at).toBe("");

        // Após a retenção: uma única mensagem com as duas datas.
        const before = bodies.length;
        await instance.processDeliveryQueue(new Date(t0.getTime() + 100_000), { targetNames: ["main"], waitForMainImage: false });
        expect(bodies.length - before).toBe(1);
        expect(bodies[bodies.length - 1]).toMatch(/14\/11 ➜ https/);
        expect(bodies[bodies.length - 1]).toMatch(/15\/11 ➜ https/);
        const rows = state.storage.sql.exec("SELECT id, status, main_message_id, group_items_json FROM offers WHERE id IN ('d14','d15')").toArray();
        expect(rows.find(row => row.id === "d14").main_message_id).toBeGreaterThan(0);
        expect(rows.find(row => row.id === "d15")).toMatchObject({ status: "delivered", main_message_id: 0 });
        expect(rows.find(row => row.id === "d15").group_items_json).toContain("mergedInto");

        // Nada mais é enviado depois.
        await instance.processDeliveryQueue(new Date(t0.getTime() + 200_000), { targetNames: ["main"], waitForMainImage: false });
        expect(bodies.length - before).toBe(1);
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
