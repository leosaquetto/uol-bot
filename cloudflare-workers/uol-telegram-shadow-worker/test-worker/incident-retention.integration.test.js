import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const START = Date.parse("2026-10-17T12:00:00.000Z");
const MINUTE = 60_000;
const OLD = "2026-09-01T12:00:00.000Z";
const detectedAt = new Date(START).toISOString();
const receiptAt = new Date(START + 30_000).toISOString();
const snapshots = { api: {}, apiContract: {}, sourceHealth: {}, webhook: {}, queueSlo: {} };

function at(minutes) {
  vi.setSystemTime(START + minutes * MINUTE);
  return new Date();
}

function configure(instance) {
  instance.env = { ...instance.env, UOL_API_AUTHORIZATION: "", OFFER_RETENTION_DAYS: "30",
    MAX_STATE_OFFERS: "50", CANAL2_DELIVERY_ENABLED: "false",
    DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false" };
}

function offer(state, id, fields = {}) {
  const row = { id, link: `https://clube.uol.com.br/beneficios/${id}`, title: "Fixture",
    preview_title: "Fixture", first_seen_at: OLD, last_seen_at: OLD, decision_at: OLD,
    status: "delivered", would_send_main: 1, main_sent_at: receiptAt, main_message_id: 1,
    main_message_kind: "photo", canal2_sent_at: receiptAt, discord_sent_at: receiptAt,
    comment_sent_at: receiptAt, ...fields };
  state.storage.sql.exec(`INSERT INTO offers(${Object.keys(row).join(",")})
    VALUES (${Object.keys(row).map(() => "?").join(",")})`, ...Object.values(row));
}

function incident(state, key) {
  state.storage.sql.exec(`INSERT INTO incidents(key, status, severity, summary, details,
    first_detected_at, last_detected_at) VALUES (?, 'active', 'critical', 'Fixture', '', ?, ?)`,
  key, detectedAt, detectedAt);
}

function ledger(state, id, fields = {}) {
  const row = { dedupe_key: `fixture:${state.storage.sql.exec("SELECT COUNT(*) AS n FROM delivery_events").one().n}`,
    offer_id: id, target: "discord", operation: "delivery", state: "resolved_sent",
    generation: 2, attempt: 2, occurred_at: receiptAt, external_id: "receipt-123", ...fields };
  state.storage.sql.exec(`INSERT INTO delivery_events(${Object.keys(row).join(",")})
    VALUES (${Object.keys(row).map(() => "?").join(",")})`, ...Object.values(row));
}

function incidentRow(state, key) {
  return state.storage.sql.exec("SELECT * FROM incidents WHERE key = ?", key).one();
}

function offerIds(state) {
  return state.storage.sql.exec("SELECT id FROM offers ORDER BY id").toArray().map(row => row.id);
}

describe("incident evidence survives offer retention until stable recovery", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(START);
    vi.stubGlobal("fetch", vi.fn(async () => new Response(null, { status: 202 })));
  });
  afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

  it.each(["age", "overflow"])("%s cleanup retains only exact active per-offer incidents", async reason => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`incident-retention-${reason}`), (instance, state) => {
      configure(instance);
      const firstSeen = reason === "age" ? OLD : new Date(START - 86_400_000).toISOString();
      const fields = { first_seen_at: firstSeen };
      for (const id of ["held_%:id", "held-ticket", "zz-free", "zz_%free", "zz-prefix-decoy"]) offer(state, id, fields);
      incident(state, "delivery-queue:held_%:id:discord");
      incident(state, "ticket-delivery:held-ticket");
      incident(state, "delivery-queue:zz-prefix-decoy-extra:discord");
      incident(state, "delivery-queue:zzXabcfree:discord");
      incident(state, "delivery-slo");
      ledger(state, "held_%:id"); ledger(state, "held-ticket"); ledger(state, "zz-free");
      if (reason === "overflow") {
        for (let index = 0; index < 50; index += 1) offer(state, `complete-${index}`, fields);
      }
      instance.pruneOffers(at(1));
      const kept = offerIds(state);
      expect(kept).toContain("held_%:id");
      expect(kept).toContain("held-ticket");
      expect(kept).not.toContain("zz-free");
      expect(kept).not.toContain("zz_%free");
      expect(kept).not.toContain("zz-prefix-decoy");
      expect(kept).toHaveLength(reason === "age" ? 2 : 52);
      expect(state.storage.sql.exec("SELECT offer_id FROM delivery_events ORDER BY offer_id").toArray())
        .toEqual([{ offer_id: "held-ticket" }, { offer_id: "held_%:id" }]);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["delivery-queue", "ticket-delivery"])("retains completed offer through real %s recovery, then prunes next day", async kind => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`incident-complete-${kind}`), async (instance, state) => {
      configure(instance);
      const id = "completed";
      const key = kind === "delivery-queue" ? `${kind}:${id}:discord` : `${kind}:${id}`;
      offer(state, id); incident(state, key); ledger(state, id);
      instance.pruneOffers(at(1));
      expect(offerIds(state)).toEqual([id]);
      expect(await instance.processOperationalHealth()).toEqual({ active: 0, alerted: 0, recovered: 0 });
      expect((await instance.processOperationalHealth(at(5))).recovered).toBe(0);
      expect(incidentRow(state, key).status).toBe("active");
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(1);
      expect(incidentRow(state, key).status).toBe("resolved");
      instance.pruneOffers(at(6));
      expect(offerIds(state)).toEqual([id]); // Daily cleanup already ran.
      instance.pruneOffers(at(1_441));
      expect(offerIds(state)).toEqual([]);
      expect(state.storage.sql.exec("SELECT COUNT(*) AS n FROM delivery_events").one().n).toBe(0);
    });
    expect(fetch).not.toHaveBeenCalled(); // Historical incidents do not replay notifications.
  });

  it.each(["main", "canal2", "discord"])("recovers pruned %s from exact resolved receipt, retaining orphan evidence until resolved", async target => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`incident-orphan-${target}`), async (instance, state) => {
      configure(instance);
      const id = "pruned_%:offer", key = `delivery-queue:${id}:${target}`;
      incident(state, key); ledger(state, id, { target }); ledger(state, "unrelated-orphan");
      incident(state, "delivery-slo");
      instance.pruneOffers(at(1));
      expect(state.storage.sql.exec("SELECT offer_id FROM delivery_events").toArray()).toEqual([{ offer_id: id }]);
      expect(instance.operationalRecoveryObservedAt(incidentRow(state, key), snapshots, new Date())).toBe(Date.now());
      await instance.processOperationalHealth();
      expect(incidentRow(state, key).status).toBe("active");
      await instance.processOperationalHealth(at(5));
      expect(incidentRow(state, key).status).toBe("active");
      await instance.processOperationalHealth(at(6));
      expect(incidentRow(state, key).status).toBe("resolved");
      instance.pruneOffers(at(1_441));
      expect(state.storage.sql.exec("SELECT COUNT(*) AS n FROM delivery_events").one().n).toBe(0);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  const invalidReceipts = [
    ["missing receipt", null],
    ["unknown", { state: "unknown" }],
    ["new attempt", { state: "attempt_started", attempt: 3 }],
    ["administratively closed", { state: "closed_unverified" }],
    ["sent without explicit resolution", { state: "sent" }],
    ["empty reference", { external_id: "" }],
    ["blank reference", { external_id: "   " }],
    ["stale receipt", { occurred_at: new Date(START - 1).toISOString() }],
    ["same timestamp as incident", { occurred_at: detectedAt }],
    ["future receipt", { occurred_at: new Date(START + 7 * MINUTE).toISOString() }],
    ["wrong offer", { offer_id: "other-offer" }],
    ["wrong target", { target: "main" }],
    ["wrong operation", { operation: "restock" }],
  ];

  it.each(invalidReceipts)("keeps missing offer active with %s", async (label, fields) => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`incident-invalid-${label}`), async (instance, state) => {
      configure(instance);
      const id = "missing", key = `delivery-queue:${id}:discord`;
      incident(state, key);
      if (fields) ledger(state, id, fields);
      at(1);
      expect(instance.operationalRecoveryObservedAt(incidentRow(state, key), snapshots, new Date())).toBeNull();
      expect((await instance.processOperationalHealth()).recovered).toBe(0);
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(0);
      expect(incidentRow(state, key).status).toBe("active");
      expect(instance.metadataValue(`ops_ntfy_recovery:${key}`)).toBeFalsy();
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    ["generation", { generation: 1, attempt: 99 }, { generation: 2, attempt: 1, state: "unknown" }],
    ["attempt", { attempt: 1 }, { attempt: 2, state: "attempt_started" }],
    ["failed generation", { generation: 1 }, { generation: 2, state: "failed" }],
    ["closed attempt", { attempt: 1 }, { attempt: 2, state: "closed_unverified" }],
    ["occurred_at", {}, { occurred_at: new Date(START + 45_000).toISOString(), state: "unknown" }],
    ["id", {}, { state: "unknown" }],
  ])("latest %s evidence blocks an older valid resolved receipt", async (field, prior, latest) => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`incident-order-${field}`), async (instance, state) => {
      configure(instance);
      const id = "missing", key = `delivery-queue:${id}:discord`;
      incident(state, key);
      ledger(state, id, prior);
      ledger(state, id, latest);
      at(1);
      expect(instance.operationalRecoveryObservedAt(incidentRow(state, key), snapshots, new Date())).toBeNull();
      await instance.processOperationalHealth();
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(0);
      expect(incidentRow(state, key).status).toBe("active");
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["comment", "main_restock", "main_sold_out", "delivery", "ticket"])("never substitutes ledger for missing %s state", async target => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`incident-non-delivery-${target}`), async (instance, state) => {
      configure(instance);
      const id = "missing", key = target === "ticket" ? `ticket-delivery:${id}` : `delivery-queue:${id}:${target}`;
      incident(state, key); ledger(state, id, { target });
      at(1);
      expect(instance.operationalRecoveryObservedAt(incidentRow(state, key), snapshots, new Date())).toBeNull();
      await instance.processOperationalHealth();
      expect((await instance.processOperationalHealth(at(6))).recovered).toBe(0);
      expect(incidentRow(state, key).status).toBe("active");
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  describe("explicit operator receipt for a previously pruned Discord offer", () => {
    const proof = { expectedGeneration: 7, expectedAttempt: 3,
      expectedIncidentLastDetectedAt: detectedAt, evidenceReference: "operator:incident:fixture",
      messageId: "1361937822156541953" };
    const events = (state, id) => state.storage.sql.exec("SELECT * FROM delivery_events WHERE offer_id = ? ORDER BY id", id).toArray();
    const record = (instance, id, fields = {}) => instance.resolveDeliveryUnknown(id, "discord", "sent", { ...proof, ...fields });

    it("authenticates the route and forwards the incident CAS and operator proof", async () => {
      const id = "orphan-route", key = `delivery-queue:${id}:discord`;
      const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
      await runInDurableObject(stub, (instance, state) => { configure(instance); incident(state, key); });
      at(1);
      const body = JSON.stringify({ id, target: "discord", outcome: "sent", ...proof });
      const denied = await exports.default.fetch("https://worker.test/resolve-delivery", { method: "POST", body });
      expect(denied.status).toBe(401);
      await runInDurableObject(stub, (_, state) => { expect(events(state, id)).toEqual([]); });
      const accepted = await exports.default.fetch("https://worker.test/resolve-delivery", {
        method: "POST", headers: { Authorization: "Bearer vitest-admin-token-not-a-secret" }, body,
      });
      expect(accepted.status).toBe(200);
      expect(await accepted.json()).toMatchObject({ ok: true, id, target: "discord", outcome: "sent",
        status: "receipt_recorded", archived: true, generationSource: "operator_evidence", retryAllowed: false });
      await runInDurableObject(stub, (_, state) => {
        expect(offerIds(state)).toEqual([]);
        expect(incidentRow(state, key).status).toBe("active");
        expect(events(state, id)).toHaveLength(2);
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it("writes both proofs without recreating an offer, resolves only after five minutes, and permits identical retry", async () => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("orphan-receipt-atomic-proof"), async (instance, state) => {
        configure(instance);
        const id = "receipt_%:offer", key = `delivery-queue:${id}:discord`;
        incident(state, key);
        const before = incidentRow(state, key);
        at(1);
        expect(record(instance, id)).toMatchObject({ status: "receipt_recorded", archived: true,
          generationSource: "operator_evidence", retryAllowed: false, idempotent: false });
        expect(offerIds(state)).toEqual([]);
        expect(incidentRow(state, key)).toEqual(before);
        const recorded = events(state, id);
        expect(recorded).toHaveLength(2);
        expect(recorded[0]).toMatchObject({ offer_id: id, target: "discord", operation: "delivery",
          state: "resolved_sent", external_id: proof.messageId, generation: 7, attempt: 3,
          occurred_at: new Date().toISOString() });
        expect(recorded[1]).toMatchObject({ offer_id: id, target: "discord", operation: "reconciliation",
          state: "receipt_recorded", external_id: proof.evidenceReference, generation: 7, attempt: 3,
          occurred_at: recorded[0].occurred_at });
        expect(record(instance, id)).toMatchObject({ idempotent: true });
        expect(events(state, id)).toEqual(recorded);
        expect((await instance.processOperationalHealth()).recovered).toBe(0);
        expect((await instance.processOperationalHealth(at(5))).recovered).toBe(0);
        expect(incidentRow(state, key).status).toBe("active");
        expect((await instance.processOperationalHealth(at(6))).recovered).toBe(1);
        expect(incidentRow(state, key).status).toBe("resolved");
        expect(record(instance, id)).toMatchObject({ idempotent: true });
        expect(events(state, id)).toEqual(recorded);
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each([
      ["missing evidence", { evidenceReference: "" }, "evidence_required"],
      ["unscoped evidence", { evidenceReference: "https://example.test/proof" }, "evidence_required"],
      ["zero generation", { expectedGeneration: 0 }, "expected_generation_invalid"],
      ["string generation", { expectedGeneration: "7" }, "expected_generation_invalid"],
      ["zero attempt", { expectedAttempt: 0 }, "expected_attempt_invalid"],
      ["noncanonical timestamp", { expectedIncidentLastDetectedAt: "2026-10-17T12:00:00Z" }, "expected_incident_last_detected_at_invalid"],
      ["invalid timestamp", { expectedIncidentLastDetectedAt: "2026-02-30T12:00:00.000Z" }, "expected_incident_last_detected_at_invalid"],
      ["empty message", { messageId: "" }, "message_id_invalid"],
      ["zero message", { messageId: "0" }, "message_id_invalid"],
      ["nonnumeric message", { messageId: "123abc" }, "message_id_invalid"],
      ["oversized message", { messageId: "1".repeat(21) }, "message_id_invalid"],
      ["unsafe numeric message", { messageId: 1361937822156541953 }, "message_id_invalid"],
    ])("rejects %s without state changes", async (label, fields, error) => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`orphan-invalid-${label}`), (instance, state) => {
        configure(instance);
        const id = "invalid", key = `delivery-queue:${id}:discord`;
        incident(state, key);
        const before = incidentRow(state, key);
        at(1);
        expect(() => record(instance, id, fields)).toThrow(`delivery_resolution_${error}`);
        expect(events(state, id)).toEqual([]);
        expect(offerIds(state)).toEqual([]);
        expect(incidentRow(state, key)).toEqual(before);
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each(["missing", "resolved", "different timestamp", "different target", "same instant"])("rejects %s incident CAS", async change => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`orphan-cas-${change}`), (instance, state) => {
        configure(instance);
        const id = "cas", key = `delivery-queue:${id}:discord`;
        if (change !== "missing") incident(state, change === "different target" ? `delivery-queue:${id}:main` : key);
        if (change === "resolved") state.storage.sql.exec("UPDATE incidents SET status = 'resolved' WHERE key = ?", key);
        const before = state.storage.sql.exec("SELECT * FROM incidents").toArray();
        at(change === "same instant" ? 0 : 1);
        const fields = change === "different timestamp"
          ? { expectedIncidentLastDetectedAt: new Date(START - 1).toISOString() } : {};
        expect(() => record(instance, id, fields)).toThrow("delivery_resolution_conflict");
        expect(events(state, id)).toEqual([]);
        expect(state.storage.sql.exec("SELECT * FROM incidents").toArray()).toEqual(before);
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each([
      ["unknown", { state: "unknown" }], ["existing receipt", {}],
      ["other target", { target: "main" }], ["other operation", { operation: "restock" }],
    ])("preserves %s ledger and refuses to append orphan proof", async (label, fields) => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`orphan-existing-${label}`), (instance, state) => {
        configure(instance);
        const id = "existing", key = `delivery-queue:${id}:discord`;
        incident(state, key); ledger(state, id, fields);
        const before = events(state, id);
        at(1);
        expect(() => record(instance, id)).toThrow("delivery_resolution_conflict");
        expect(events(state, id)).toEqual(before);
        expect(incidentRow(state, key).status).toBe("active");
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each([
      ["reference", { evidenceReference: "operator:other-proof" }],
      ["message", { messageId: "1361937822156541954" }],
      ["generation", { expectedGeneration: 8 }], ["attempt", { expectedAttempt: 4 }],
    ])("rejects conflicting retry %s while preserving both original proofs", async (field, fields) => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`orphan-retry-${field}`), (instance, state) => {
        configure(instance);
        const id = "retry", key = `delivery-queue:${id}:discord`;
        incident(state, key); at(1); record(instance, id);
        const before = events(state, id);
        expect(() => record(instance, id, fields)).toThrow("delivery_resolution_conflict");
        expect(events(state, id)).toEqual(before);
        expect(incidentRow(state, key).status).toBe("active");
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each(["first insert", "second insert", "missing second insert"])("rolls back both mandatory events on %s failure", async failure => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`orphan-rollback-${failure}`), (instance, state) => {
        configure(instance);
        const id = "rollback", key = `delivery-queue:${id}:discord`;
        incident(state, key);
        const before = incidentRow(state, key), sqlExec = instance.sqlExec;
        let inserts = 0;
        instance.sqlExec = function (query, ...args) {
          if (/INSERT OR IGNORE INTO delivery_events/u.test(query)) {
            inserts += 1;
            if (failure === "missing second insert" && inserts === 2) return { changes: 0 };
            if ((failure === "first insert" && inserts === 1) || (failure === "second insert" && inserts === 2)) {
              throw new Error("injected_ledger_failure");
            }
          }
          return sqlExec.call(this, query, ...args);
        };
        at(1);
        try {
          expect(() => record(instance, id)).toThrow(failure === "missing second insert"
            ? "delivery_resolution_ledger_required" : "injected_ledger_failure");
        } finally { instance.sqlExec = sqlExec; }
        expect(events(state, id)).toEqual([]);
        expect(offerIds(state)).toEqual([]);
        expect(incidentRow(state, key)).toEqual(before);
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it("rejects the orphan helper if an offer exists inside the transaction", async () => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("orphan-existing-offer"), (instance, state) => {
        configure(instance);
        const id = "present", key = `delivery-queue:${id}:discord`;
        offer(state, id); incident(state, key); at(1);
        const before = state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();
        expect(() => instance.recordOrphanDiscordReceipt(id, proof, new Date().toISOString())).toThrow("delivery_resolution_conflict");
        expect(state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one()).toEqual(before);
        expect(events(state, id)).toEqual([]);
      });
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each([["main", "sent"], ["canal2", "sent"], ["comment", "sent"], ["beeper", "sent"],
      ["discord", "not_sent"], ["discord", "closed"]])("does not enable orphan resolution for %s/%s", async (target, outcome) => {
      await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`orphan-wrong-${target}-${outcome}`), (instance, state) => {
        configure(instance);
        const id = "narrow", key = `delivery-queue:${id}:discord`;
        incident(state, key); at(1);
        expect(() => instance.resolveDeliveryUnknown(id, target, outcome, proof)).toThrow();
        expect(events(state, id)).toEqual([]);
        expect(incidentRow(state, key).status).toBe("active");
      });
      expect(fetch).not.toHaveBeenCalled();
    });
  });
});
