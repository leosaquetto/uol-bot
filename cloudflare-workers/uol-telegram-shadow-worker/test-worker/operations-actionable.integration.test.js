import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

// 18/11: o celular recebia "atenção/recuperado" de ingresso que ainda estava
// ganhando foto ou comentário. Agora só a foto abandonada vira aviso, com o
// nome da oferta e a ação; o resto entra no resumo diário.
const link = (id) => `https://clube.uol.com.br/campanhasdeingresso/${id}`;

function insert(state, id, fields = {}) {
  const at = new Date(Date.now() - 3_600_000).toISOString();
  const row = {
    id, link: link(id), title: `2 INGRESSOS: ${id}`, preview_title: "",
    category: "campanhasdeingresso", image_url: "https://example.test/a.png",
    first_seen_at: at, last_seen_at: at, decision_at: at, status: "sent",
    would_send_main: 1, delivery_mode: "live", delivery_generation: 1,
    main_sent_at: at, main_message_id: 500, main_message_kind: "text",
    telegram_image_strategy: "text_timeout", main_image_upgrade_attempts: 10, ...fields,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

function isolate(instance) {
  instance.env = {
    ...instance.env, UOL_API_AUTHORIZATION: "", OPS_NTFY_SERVER: "https://ntfy.example.test/",
    OPS_NTFY_TOKEN: "tk_test", OPS_NTFY_TOPIC: "saquettoos",
  };
  instance.setMetadata("ops_monitor_started_at", "2026-01-01T00:00:00.000Z");
  instance.failedRunStreak = () => 0;
  instance.deliveryQueueSlo = () => ({ criticalPending: 0, secondaryPending: 0, oldestAgeMs: 0 });
  instance.deliveryQueueIssues = () => [];
}

const sent = (calls) => calls.map((call) => JSON.parse(JSON.parse(call).message));

describe("avisos operacionais acionáveis", () => {
  afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

  it("só a foto abandonada vira aviso, com oferta e ação; retentativa e comentário não", async () => {
    const bodies = [];
    vi.stubGlobal("fetch", vi.fn(async (_url, init = {}) => {
      bodies.push(init.body);
      return Response.json({ id: "x", event: "message", topic: "saquettoos" });
    }));
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-actionable-photo");
    await runInDurableObject(stub, async (instance, state) => {
      isolate(instance);
      insert(state, "gave-up");
      insert(state, "retrying", { main_image_upgrade_attempts: 2 });
      insert(state, "no-comment", { main_message_kind: "photo", comment_sent_at: "" });
      insert(state, "fast-text", { telegram_image_strategy: "text_fast", main_image_upgrade_attempts: 0 });
      insert(state, "no-image", { main_image_upgrade_attempts: 0, image_url: "" });
      const ids = instance.recentTicketDeliveryIssues(new Date()).map((issue) => issue.id).sort();
      expect(ids).toEqual(["gave-up", "no-image"]);

      await instance.processOperationalHealth(new Date());
      const [envelope] = sent(bodies);
      expect(envelope.body).toContain("2 INGRESSOS: ");
      expect(envelope.body).toContain("requeue-delivery main_image");
      expect(envelope.body).toContain("desistiu");
    });
  });

  it("warning que se resolve sozinho não toca o celular e entra no resumo das 09:00", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(Date.parse("2026-11-19T10:00:00.000Z"));
    const bodies = [];
    vi.stubGlobal("fetch", vi.fn(async (_url, init = {}) => {
      bodies.push(init.body);
      return Response.json({ id: "x", event: "message", topic: "saquettoos" });
    }));
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-actionable-digest");
    await runInDurableObject(stub, async (instance) => {
      isolate(instance);
      instance.recentTicketDeliveryIssues = () => [];
      instance.setRuntimeSnapshot("api", { lastError: "uol_api_http_503 private-detail", failureStreak: 3 });
      expect((await instance.processOperationalHealth(new Date())).alerted).toBe(0);
      expect(bodies).toHaveLength(0);
      expect(bodies).toHaveLength(0);

      vi.setSystemTime(Date.parse("2026-11-19T12:01:00.000Z"));
      await instance.processOperationalHealth(new Date());
      const [digest] = sent(bodies);
      expect(digest.title).toBe("UOL bot - resumo do dia");
      expect(digest.body).toContain("A API de ofertas falhou");
      expect(digest.body).not.toContain("private-detail");

      // One digest per day.
      vi.setSystemTime(Date.parse("2026-11-19T15:00:00.000Z"));
      await instance.processOperationalHealth(new Date());
      expect(bodies).toHaveLength(1);
    });
  });

  it("custo zero: a histerese e o resumo não gravam por ciclo", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ id: "x", event: "message", topic: "saquettoos" })));
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("ops-actionable-cost");
    await runInDurableObject(stub, async (instance) => {
      isolate(instance);
      instance.recentTicketDeliveryIssues = () => [];
      instance.setRuntimeSnapshot("api", { lastError: "uol_api_http_503", failureStreak: 3 });
      const pass = async (iso) => {
        vi.setSystemTime(Date.parse(iso));
        const before = instance.storageUsage.rowsWritten;
        await instance.processOperationalHealth(new Date());
        return instance.storageUsage.rowsWritten - before;
      };
      await pass("2026-11-19T10:00:00.000Z");
      const steady = await pass("2026-11-19T10:01:00.000Z");
      expect(await pass("2026-11-19T10:02:00.000Z")).toBe(steady);
      // Once a day: the day marker, the queued digest and the usual send
      // bookkeeping of any alert (reserve, complete, saquettoOS push).
      expect(await pass("2026-11-19T12:01:00.000Z")).toBeLessThanOrEqual(steady + 6);
      await pass("2026-11-19T12:02:00.000Z");
      expect(await pass("2026-11-19T12:03:00.000Z")).toBe(steady);
    });
  });
});
