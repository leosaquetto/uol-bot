import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

function configure(instance, quota = false) {
  instance.env = {
    ...instance.env,
    DELIVERY_MODE: "live",
    TELEGRAM_TOKEN: "vitest-queue-budget-token",
    TELEGRAM_CHAT_ID: "-100111",
    CANAL2_ID: "-100333",
    CANAL2_DELIVERY_ENABLED: "true",
    DISCORD_DELIVERY_ENABLED: "false",
    BEEPER_DELIVERY_ENABLED: "false",
    DELIVERY_BATCH_SIZE: "4",
  };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: !quota, primaryAllowed: true });
}

function insert(instance, state, id, at, fields = {}) {
  const row = {
    id,
    link: `https://clube.uol.com.br/beneficios/${id}`,
    preview_title: "Oferta de teste",
    title: "Oferta de teste",
    first_seen_at: at,
    last_seen_at: at,
    decision_at: at,
    status: "delivery_pending",
    would_send_main: 1,
    would_send_canal2: 1,
    delivery_mode: "live",
    delivery_generation: instance.currentDeliveryGeneration(),
    ...fields,
  };
  const columns = Object.keys(row);
  state.storage.sql.exec(
    `INSERT INTO offers(${columns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`,
    ...Object.values(row),
  );
}

function secondaryBacklog(instance, state, now) {
  const old = new Date(now.getTime() - 2 * 86_400_000).toISOString();
  for (let index = 0; index < 32; index++) {
    insert(instance, state, `secondary-${index}`, old, {
      status: "partial_delivery",
      main_sent_at: old,
      main_message_id: 100 + index,
    });
  }
}

function telegramRequests() {
  const requests = [];
  vi.stubGlobal("fetch", async (url, init) => {
    const method = String(url).split("/").at(-1);
    expect(["sendMessage", "copyMessage"]).toContain(method);
    expect(String(url)).toBe(`https://api.telegram.org/botvitest-queue-budget-token/${method}`);
    expect(init.method).toBe("POST");
    requests.push({ method, ...JSON.parse(init.body) });
    return Response.json({ ok: true, result: { message_id: 900 + requests.length } });
  });
  return requests;
}

describe("delivery queue ranks targets admitted by its current budget", () => {
  it.each([{ targetNames: ["main", "canal2"] }, { targetNames: [] }])(
    "sends an overdue main ahead of 32 deferred secondaries with targets $targetNames",
    async ({ targetNames }) => {
      const requests = telegramRequests();
      try {
        const stub = env.UOL_TELEGRAM_SHADOW.getByName(`queue-critical-rank-${targetNames.join("-") || "all"}`);
        await runInDurableObject(stub, async (instance, state) => {
          configure(instance);
          const now = new Date();
          secondaryBacklog(instance, state, now);
          const dueAt = new Date(now.getTime() - 120_000).toISOString();
          insert(instance, state, "main-due", dueAt);
          expect(instance.deliveryQueueSlo(now).actionableMainPending).toBe(1);
          const result = await instance.processDeliveryQueue(now, { targetNames, waitForMainImage: false });
          expect(result).toMatchObject({ mainSent: 1, canal2Sent: 0, discordSent: 0, failed: 0 });
          expect(state.storage.sql.exec("SELECT main_sent_at FROM offers WHERE id = 'main-due'").one().main_sent_at).not.toBe("");
          expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM offers WHERE canal2_sent_at <> ''").one().count).toBe(0);
        });
        expect(requests.map(request => request.method)).toEqual(["sendMessage"]);
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it("treats omitted targets as all targets in a healthy queue", async () => {
    const requests = telegramRequests();
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("queue-healthy-default-targets");
      await runInDurableObject(stub, async (instance, state) => {
        configure(instance);
        const now = new Date();
        insert(instance, state, "fresh-all", new Date(now.getTime() - 1_000).toISOString());
        const result = await instance.processDeliveryQueue(now, { waitForMainImage: false });
        expect(result).toMatchObject({ mainSent: 1, canal2Sent: 1, discordSent: 0, failed: 0 });
      });
      expect(requests.map(request => request.method)).toEqual(["sendMessage", "copyMessage"]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it.each(["priority", "recent"])("keeps the %s secondary exception under quota pressure", async exception => {
    const requests = telegramRequests();
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName(`queue-quota-${exception}-exception`);
      await runInDurableObject(stub, async (instance, state) => {
        configure(instance, true);
        const now = new Date();
        secondaryBacklog(instance, state, now);
        const at = new Date(now.getTime() - (exception === "recent" ? 1_000 : 86_400_000)).toISOString();
        insert(instance, state, "permitted-secondary", at, {
          status: "partial_delivery",
          main_sent_at: at,
          main_message_id: 700,
        });
        const result = await instance.processDeliveryQueue(now, {
          targetNames: ["canal2"],
          priorityIds: exception === "priority"
            ? ["permitted-secondary", ...Array.from({ length: 95 }, (_, index) => `absent-priority-${index}`)]
            : [],
        });
        expect(result).toMatchObject({ mainSent: 0, canal2Sent: 1, discordSent: 0, failed: 0 });
      });
      expect(requests.map(request => request.message_id)).toEqual([700]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("does not resend unknown or future-backoff mains while ranking a due main", async () => {
    const requests = telegramRequests();
    try {
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("queue-budget-preserves-unknown-backoff");
      await runInDurableObject(stub, async (instance, state) => {
        configure(instance);
        const now = new Date();
        const old = new Date(now.getTime() - 86_400_000).toISOString();
        for (let index = 0; index < 32; index++) {
          insert(instance, state, `guarded-${index}`, old, index % 2 ? {
            main_delivery_next_attempt_at: new Date(now.getTime() + 60_000).toISOString(),
            main_delivery_attempts: 1,
          } : {
            status: "delivery_unknown",
            main_delivery_unknown_at: old,
            main_delivery_attempts: 1,
          });
        }
        insert(instance, state, "due-after-guards", new Date(now.getTime() - 120_000).toISOString());
        const result = await instance.processDeliveryQueue(now, { targetNames: ["main", "canal2"], waitForMainImage: false });
        expect(result).toMatchObject({ mainSent: 1, canal2Sent: 0, failed: 0 });
        const guarded = state.storage.sql.exec("SELECT main_delivery_attempts, main_sent_at FROM offers WHERE id LIKE 'guarded-%'").toArray();
        expect(guarded).toHaveLength(32);
        expect(guarded.every(row => row.main_delivery_attempts === 1 && !row.main_sent_at)).toBe(true);
      });
      expect(requests.map(request => request.method)).toEqual(["sendMessage"]);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
