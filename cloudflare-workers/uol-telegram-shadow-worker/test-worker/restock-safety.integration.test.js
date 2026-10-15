import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

const at = "2026-10-15T12:00:00.000Z";
const later = new Date("2026-10-16T12:00:00.000Z");
const missing = () => Response.json({ ok: false, error_code: 400, description: "Bad Request: message to edit not found" }, { status: 400 });
function configure(instance, state, target, fields = {}) {
  instance.env = { ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "restock-test", TELEGRAM_CHAT_ID: "-100111",
    CANAL2_ID: "-100333", CANAL2_DELIVERY_ENABLED: target === "canal2" ? "true" : "false", DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false" };
  instance.setMetadata("delivery_mode_override", "live");
  instance.telegramOfferWithImageState = offer => ({ offer, imageKey: "" });
  const row = { id: "restock", link: "https://clube.uol.com.br/beneficios/restock", title: "Teste", preview_title: "Teste",
    first_seen_at: at, last_seen_at: at, decision_at: at, status: "restocked_pending_sync", restocked_at: at,
    main_message_id: 100, main_message_kind: "text", main_sent_at: at, would_send_main: 1,
    would_send_canal2: target === "canal2" ? 1 : 0, canal2_message_id: target === "canal2" ? 101 : 0,
    main_restock_synced_at: target === "canal2" ? at : "", delivery_mode: "live",
    delivery_generation: instance.currentDeliveryGeneration(), ...fields };
  state.storage.sql.exec(`INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`, ...Object.values(row));
}
const row = state => state.storage.sql.exec("SELECT * FROM offers WHERE id='restock'").one();
const ledger = state => state.storage.sql.exec("SELECT state,external_id,error FROM delivery_events WHERE operation='restock'").toArray();
function gate() { let release; const promise = new Promise(resolve => { release = resolve; }); return { promise, release }; }
const actor = name => env.UOL_TELEGRAM_SHADOW.getByName(`restock-${name}`);

describe("durable restock repost ownership", () => {
  it.each(["main", "canal2"])("%s reserves before POST and concurrent calls cannot duplicate", async target => {
    let posts = 0; const started = gate(), held = gate();
    vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) return missing();
      posts++; started.release(); await held.promise; return Response.json({ ok: true, result: { message_id: 900 } });
    });
    try {
      await runInDurableObject(actor(`overlap-${target}`), async (instance, state) => {
        configure(instance, state, target);
        const pending = instance.processRestockSync(new Date(at)); await started.promise;
        const marker = row(state)[`${target}_restock_error`];
        expect(row(state)[`${target}_restock_attempts`]).toBe(10); expect(marker).toMatch(/^ambiguous:restock_repost_reserved:/);
        await instance.processRestockSync(new Date(at)); expect(row(state)[`${target}_restock_error`]).toBe(marker);
        held.release(); await pending;
        expect(row(state)[`${target}_message_id`]).toBe(900); expect(row(state)[`${target}_restock_synced_at`]).not.toBe("");
        expect(row(state)[`${target}_restock_attempts`]).toBe(1);
        expect(ledger(state).some(event => event.state === "reposted" && event.external_id === "900")).toBe(true);
      });
      expect(posts).toBe(1);
    } finally { held.release(); vi.unstubAllGlobals(); }
  });
  it.each(["main", "canal2"])("%s interrupted reservation cannot publish after restart", async target => {
    let calls = 0; vi.stubGlobal("fetch", async () => { calls++; throw new Error("must_not_send"); });
    try {
      await runInDurableObject(actor(`interrupted-${target}`), async (instance, state) => {
        configure(instance, state, target, { [`${target}_restock_attempts`]: 10, [`${target}_restock_error`]: "ambiguous:restock_repost_reserved:interrupted" });
        if (target === "main") { instance.env.CANAL2_DELIVERY_ENABLED = "true"; state.storage.sql.exec("UPDATE offers SET would_send_canal2=1,canal2_message_id=101"); }
        await instance.processRestockSync(new Date(at));
        expect(row(state)[`${target}_restock_error`]).toBe("ambiguous:restock_repost_reserved:interrupted");
        expect(row(state)[`${target}_restock_synced_at`]).toBe("");
      });
      expect(calls).toBe(0);
    } finally { vi.unstubAllGlobals(); }
  });
  it.each(["main", "canal2"])("%s lost response remains reserved without another POST", async target => {
    let posts = 0; vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) return missing(); posts++; throw new TypeError("connection lost after possible acceptance");
    });
    try {
      await runInDurableObject(actor(`response-loss-${target}`), async (instance, state) => {
        configure(instance, state, target); await instance.processRestockSync(new Date(at));
        expect(row(state)[`${target}_restock_attempts`]).toBe(10);
        expect(row(state)[`${target}_restock_error`]).toMatch(/^ambiguous:restock_repost_reserved:/);
        await instance.processRestockSync(later); expect(ledger(state).some(event => event.state === "unknown")).toBe(true);
      });
      expect(posts).toBe(1);
    } finally { vi.unstubAllGlobals(); }
  });
  it("an earlier edit cannot overwrite a later repost reservation", async () => {
    let posts = 0, edits = 0; const editStarted = gate(), editHeld = gate(), postStarted = gate(), postHeld = gate();
    vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) { if (++edits === 1) { editStarted.release(); await editHeld.promise; } return missing(); }
      posts++; postStarted.release(); await postHeld.promise; return Response.json({ ok: true, result: { message_id: 901 } });
    });
    try {
      await runInDurableObject(actor("edit-race"), async (instance, state) => {
        configure(instance, state, "main"); const first = instance.processRestockSync(new Date(at)); await editStarted.promise;
        const second = instance.processRestockSync(new Date(at)); await postStarted.promise; const marker = row(state).main_restock_error;
        editHeld.release(); await first; expect(row(state).main_restock_error).toBe(marker); expect(row(state).main_restock_attempts).toBe(10);
        postHeld.release(); await second; expect(row(state).main_message_id).toBe(901);
      });
      expect(posts).toBe(1);
    } finally { editHeld.release(); postHeld.release(); vi.unstubAllGlobals(); }
  });
  it.each(["generation", "restocked_at", "marker"])("late receipt preserves %s ownership and its actual ID", async change => {
    const started = gate(), held = gate();
    vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) return missing(); started.release(); await held.promise; return Response.json({ ok: true, result: { message_id: 902 } });
    });
    try {
      await runInDurableObject(actor(`late-${change}`), async (instance, state) => {
        configure(instance, state, "main"); const pending = instance.processRestockSync(new Date(at)); await started.promise;
        if (change === "generation") state.storage.sql.exec("UPDATE offers SET delivery_generation=delivery_generation+1");
        if (change === "restocked_at") state.storage.sql.exec("UPDATE offers SET restocked_at='2026-10-16T12:00:00.000Z'");
        if (change === "marker") state.storage.sql.exec("UPDATE offers SET main_restock_error='ambiguous:restock_repost_reserved:new-owner'");
        held.release(); await pending; expect(row(state).main_message_id).toBe(100); expect(row(state).main_restock_synced_at).toBe("");
        expect(ledger(state).some(event => event.state === "late_reposted" && event.external_id === "902")).toBe(true);
      });
    } finally { held.release(); vi.unstubAllGlobals(); }
  });
  it("SQL failure after acceptance retains reservation and actual receipt", async () => {
    let posts = 0; vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) return missing(); posts++; return Response.json({ ok: true, result: { message_id: 903 } });
    });
    try {
      await runInDurableObject(actor("sql-failure"), async (instance, state) => {
        configure(instance, state, "main"); const sql = instance.sqlExec.bind(instance); let fail = true;
        instance.sqlExec = (query, ...args) => {
          if (fail && query.includes("UPDATE offers SET main_message_id = ?")) { fail = false; throw new Error("simulated storage failure"); }
          return sql(query, ...args);
        };
        await instance.processRestockSync(new Date(at)); expect(row(state).main_restock_attempts).toBe(10);
        expect(row(state).main_restock_error).toMatch(/^ambiguous:restock_repost_reserved:/);
        expect(ledger(state).some(event => event.state === "unknown" && event.external_id === "903")).toBe(true);
        await instance.processRestockSync(later);
      });
      expect(posts).toBe(1);
    } finally { vi.unstubAllGlobals(); }
  });
  it("image bookkeeping cannot undo stored receipt or cause another POST", async () => {
    let posts = 0; vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) return missing(); posts++; return Response.json({ ok: true, result: { message_id: 904 } });
    });
    try {
      await runInDurableObject(actor("image-failure"), async (instance, state) => {
        configure(instance, state, "main"); instance.recordImageDelivery = () => { expect(row(state).main_message_id).toBe(904); throw new Error("image metadata failed"); };
        const result = await instance.processRestockSync(new Date(at)); expect(result.mainReposted).toBe(1); expect(result.failed).toBe(0);
        expect(row(state).status).toBe("delivered"); await instance.processRestockSync(later);
      });
      expect(posts).toBe(1);
    } finally { vi.unstubAllGlobals(); }
  });
  it.each(["main", "canal2"])("%s definite rejection can retry without losing ownership", async target => {
    let posts = 0;
    vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) return missing();
      if (++posts === 1) return Response.json({ ok: false, error_code: 429, description: "Too Many Requests", parameters: { retry_after: 1 } }, { status: 429 });
      return Response.json({ ok: true, result: { message_id: 905 } });
    });
    try {
      await runInDurableObject(actor(`safe-retry-${target}`), async (instance, state) => {
        configure(instance, state, target); await instance.processRestockSync(new Date(at));
        expect(row(state)[`${target}_restock_attempts`]).toBe(1);
        expect(row(state)[`${target}_restock_error`]).toMatch(/^restock_repost:/);
        expect(row(state)[`${target}_restock_next_attempt_at`] > at).toBe(true);
        await instance.processRestockSync(later);
        expect(row(state)[`${target}_message_id`]).toBe(905);
        expect(row(state)[`${target}_restock_attempts`]).toBe(2);
      });
      expect(posts).toBe(2);
    } finally { vi.unstubAllGlobals(); }
  });
  it.each(["shadow", "generation"])("%s change during edit prevents a replacement POST", async change => {
    let posts = 0; const started = gate(), held = gate();
    vi.stubGlobal("fetch", async url => {
      if (url.includes("editMessage")) { started.release(); await held.promise; return missing(); }
      posts++; return Response.json({ ok: true, result: { message_id: 906 } });
    });
    try {
      await runInDurableObject(actor(`mode-${change}`), async (instance, state) => {
        configure(instance, state, "main"); const pending = instance.processRestockSync(new Date(at)); await started.promise;
        if (change === "shadow") instance.setDeliveryMode("shadow");
        else instance.setMetadata("delivery_mode_generation", instance.currentDeliveryGeneration()+1);
        held.release(); await pending;
        expect(row(state).main_message_id).toBe(100);expect(row(state).main_restock_synced_at).toBe("");
      });
      expect(posts).toBe(0);
    } finally { held.release(); vi.unstubAllGlobals(); }
  });
  it("ordinary edits finalize without replacement and mixed-target selection respects retry time", async () => {
    const chats=[];
    vi.stubGlobal("fetch", async (url,init) => {
      expect(url.includes("editMessage")).toBe(true);chats.push(JSON.parse(init.body).chat_id);
      return Response.json({ ok: true, result: { message_id: 100 } });
    });
    try {
      await runInDurableObject(actor("ordinary-edit"), async (instance,state) => {
        configure(instance,state,"main"); const result=await instance.processRestockSync(new Date(at));
        expect(result.mainEdited).toBe(1);expect(result.mainReposted).toBe(0);expect(row(state).status).toBe("delivered");
      });
      chats.length=0;
      await runInDurableObject(actor("mixed-retry"), async (instance,state) => {
        configure(instance,state,"canal2",{main_restock_synced_at:"",main_restock_next_attempt_at:"2026-10-16T12:00:00.000Z"});
        const result=await instance.processRestockSync(new Date(at));
        expect(result.mainEdited).toBe(0);expect(result.canal2Edited).toBe(1);expect(row(state).main_restock_attempts).toBe(0);
      });
      expect(chats).toEqual(["-100333"]);
    } finally { vi.unstubAllGlobals(); }
  });

  it.each(["main", "canal2"])("%s uncertain reservation stays blocked after the attempt limit increases", async target => {
    let calls = 0;
    vi.stubGlobal("fetch", async () => { calls++; throw new Error("must_not_send"); });
    try {
      await runInDurableObject(actor(`increased-limit-${target}`), async (instance, state) => {
        const marker = "ambiguous:restock_repost_reserved:old-limit";
        configure(instance, state, target, { [`${target}_restock_attempts`]: 10,
          [`${target}_restock_error`]: marker });
        instance.env.DELIVERY_MAX_ATTEMPTS = "20";
        await instance.processRestockSync(new Date(at));
        expect(row(state)[`${target}_restock_attempts`]).toBe(10);
        expect(row(state)[`${target}_restock_error`]).toBe(marker);
        expect(row(state)[`${target}_restock_synced_at`]).toBe("");
        expect(ledger(state)).toEqual([]);
      });
      expect(calls).toBe(0);
    } finally { vi.unstubAllGlobals(); }
  });

});
