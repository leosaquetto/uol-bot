import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

// 20/11/2026: o comentário de uma oferta comum (cinema) deu timeout de 8 s no Telegram, ficou
// "incerto" e o incidente crítico nunca saiu (/readyz 503 por 20 h): fechar o comentário não
// reconhecia o incidente e a recuperação espera um recibo de comentário que nunca vem.
const AT = new Date(Date.now() - 3_600_000).toISOString();

function configure(instance) {
  instance.env = {
    ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test", TELEGRAM_CHAT_ID: "-100111",
    GRUPO_COMENTARIO_ID: "-100222", CANAL2_DELIVERY_ENABLED: "false",
    DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(state, id, link, category, fields = {}) {
  const row = {
    id, link, title: "Oferta", preview_title: "Oferta", category, description: "Descrição da oferta",
    first_seen_at: AT, last_seen_at: AT, decision_at: AT, status: "delivered",
    would_send_main: 1, delivery_mode: "live", delivery_generation: 1,
    main_sent_at: AT, main_message_id: 500, main_message_kind: "photo", discussion_message_id: 700, ...fields,
  };
  state.storage.sql.exec(
    `INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`,
    ...Object.values(row),
  );
}

function incident(state, key) {
  state.storage.sql.exec(`INSERT INTO incidents(key, status, severity, summary, details,
    first_detected_at, last_detected_at, last_attempted_at, last_alerted_at, occurrence_count, alert_error)
    VALUES (?, 'active', 'critical', 'Resumo', 'Detalhe', ?, ?, ?, ?, 3, '')`, key, AT, AT, AT, AT);
}

const offer = (state, id) => state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();
const incidentStatus = (state, key) => state.storage.sql.exec("SELECT status FROM incidents WHERE key = ?", key).one().status;

describe("comentário incerto no Telegram", () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it("oferta comum: fecha sozinho, sem reenviar; ingresso fica incerto", async () => {
    const calls = [];
    vi.stubGlobal("fetch", async (url) => {
      const href = String(url);
      if (href.includes("api.telegram.org")) { calls.push(href.split("/").pop()); throw new DOMException("timeout", "TimeoutError"); }
      return new Response("blocked", { status: 403 });
    });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "comum", "https://clube.uol.com.br/ingressocom/nzi-isencao-de-taxa", "ingressocom");
      insert(state, "ingresso", "https://clube.uol.com.br/campanhasdeingresso/pYy-2-ingressos-teatro", "campanhasdeingresso");
      await instance.processDiscussionComments(8);
      expect(calls.filter((name) => name === "sendMessage")).toHaveLength(2); // uma tentativa cada, nenhuma repetida
      expect(offer(state, "comum")).toMatchObject({ comment_delivery_unknown_at: "", comment_delivery_error: "historical_unknown_closed" });
      expect(offer(state, "ingresso").comment_delivery_unknown_at).not.toBe("");
      await instance.processDiscussionComments(8);
      expect(calls.filter((name) => name === "sendMessage")).toHaveLength(2); // não tenta de novo
    });
  });

  it("fechar o comentário à mão reconhece o incidente e o /readyz não fica preso", async () => {
    vi.stubGlobal("fetch", async () => { throw new Error("unexpected_http"); });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "preso", "https://clube.uol.com.br/ingressocom/nzi-preso", "ingressocom", {
        comment_delivery_unknown_at: AT, comment_delivery_error: "ambiguous:telegram_sendMessage_timeout_ambiguous",
        comment_delivery_attempts: 1,
      });
      incident(state, "delivery-queue:preso:comment");
    });
    await runInDurableObject(stub, async (instance, state) => {
      instance.resolveDeliveryUnknown("preso", "comment", "closed", {});
      expect(incidentStatus(state, "delivery-queue:preso:comment")).toBe("administratively_acknowledged");
      expect(offer(state, "preso").comment_delivery_unknown_at).toBe("");
    });
  });

  it("incidente que ficou ativo depois de um fechamento antigo se recupera sozinho", async () => {
    vi.stubGlobal("fetch", async () => { throw new Error("unexpected_http"); });
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, async (instance, state) => {
      configure(instance);
      insert(state, "antigo", "https://clube.uol.com.br/ingressocom/nzi-antigo", "ingressocom", {
        comment_delivery_error: "historical_unknown_closed", comment_delivery_attempts: 10,
      });
      incident(state, "delivery-queue:antigo:comment");
      const row = state.storage.sql.exec("SELECT * FROM incidents WHERE key = ?", "delivery-queue:antigo:comment").one();
      const now = new Date();
      const snapshots = { api: {}, apiContract: {}, sourceHealth: {}, webhook: {}, queueSlo: {} };
      expect(instance.operationalRecoveryObservedAt(row, snapshots, now)).toBe(now.getTime());
      // e um comentário com erro comum (sem fechamento) continua sem recuperação
      insert(state, "aberto", "https://clube.uol.com.br/ingressocom/nzi-aberto", "ingressocom", { comment_delivery_error: "timeout" });
      incident(state, "delivery-queue:aberto:comment");
      const open = state.storage.sql.exec("SELECT * FROM incidents WHERE key = ?", "delivery-queue:aberto:comment").one();
      expect(instance.operationalRecoveryObservedAt(open, snapshots, now)).toBeNull();
    });
  });
});
