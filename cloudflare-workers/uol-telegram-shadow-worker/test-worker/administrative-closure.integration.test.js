import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { classifyDeliveryRow } from "../src/delivery-state.js";

const UNKNOWN_AT = "2026-10-01T12:34:56.789Z";
const ADMIN = { Authorization: "Bearer vitest-admin-token-not-a-secret" };
const PROOF = { expectedGeneration: 1, expectedAttempt: 4, expectedUnknownAt: UNKNOWN_AT,
  evidenceReference: "operator:incident-2026-10-17" };
const configuration = { main: { ready: true }, canal2: { enabled: false, ready: true },
  discord: { enabled: false, ready: true } };

function configure(instance) {
  instance.env = { ...instance.env, DELIVERY_MODE: "live", TELEGRAM_TOKEN: "test-closure",
    TELEGRAM_CHAT_ID: "-100111", CANAL2_DELIVERY_ENABLED: "false",
    DISCORD_DELIVERY_ENABLED: "false", BEEPER_DELIVERY_ENABLED: "false" };
  instance.setMetadata("delivery_mode_override", "live");
  instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true, primaryAllowed: true });
}

function insert(instance, state, id, fields = {}) {
  const row = { id, link: `https://clube.uol.com.br/beneficios/${id}`, title: "Private title",
    preview_title: "Private title", description: "Private description", first_seen_at: UNKNOWN_AT,
    last_seen_at: UNKNOWN_AT, decision_at: UNKNOWN_AT, status: "delivery_unknown",
    would_send_main: 1, delivery_mode: "live", delivery_generation: 1,
    main_delivery_attempts: 4, main_delivery_unknown_at: UNKNOWN_AT,
    main_delivery_error: "ambiguous:network interrupted", delivery_unknown_at: UNKNOWN_AT,
    delivery_unknown_target: "main", ...fields };
  state.storage.sql.exec(`INSERT INTO offers(${Object.keys(row).join(",")}) VALUES (${Object.keys(row).map(() => "?").join(",")})`, ...Object.values(row));
}

function offer(state, id) {
  return state.storage.sql.exec("SELECT * FROM offers WHERE id = ?", id).one();
}

function events(state, id) {
  return state.storage.sql.exec("SELECT * FROM delivery_events WHERE offer_id = ? ORDER BY id", id).toArray();
}

function incident(state, key) {
  state.storage.sql.exec(`INSERT INTO incidents(key, status, severity, summary, details,
    first_detected_at, last_detected_at, last_attempted_at, last_alerted_at,
    occurrence_count, alert_error) VALUES (?, 'active', 'critical', 'Private summary',
    'Private details', ?, ?, ?, ?, 3, 'prior_error')`, key, UNKNOWN_AT, UNKNOWN_AT, UNKNOWN_AT, UNKNOWN_AT);
}

function incidents(state) {
  return state.storage.sql.exec("SELECT * FROM incidents ORDER BY key").toArray();
}

function close(instance, id, proof = PROOF) {
  return instance.resolveDeliveryUnknown(id, "main", "closed", proof);
}

describe("administrative closure without delivery proof", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("unexpected_http"); }));
  });
  afterEach(() => { vi.unstubAllGlobals(); });

  it("authenticates /resolve-delivery and forwards all comparison fields", async () => {
    const id = "closure-route";
    const stub = env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor");
    await runInDurableObject(stub, (instance, state) => {
      configure(instance); insert(instance, state, id);
    });
    const body = JSON.stringify({ id, target: "main", outcome: "closed", ...PROOF });
    const denied = await exports.default.fetch("https://worker.test/resolve-delivery", { method: "POST", body });
    expect(denied.status).toBe(401);
    await runInDurableObject(stub, (_, state) => { expect(offer(state, id).status).toBe("delivery_unknown"); });
    const response = await exports.default.fetch("https://worker.test/resolve-delivery", { method: "POST", headers: ADMIN, body });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ ok: true, id, target: "main", outcome: "closed" });
    await runInDurableObject(stub, (_, state) => {
      expect(offer(state, id)).toMatchObject({ status: "delivery_quarantined", delivery_quarantine_reason: "unknown_closed_unverified:main" });
      expect(events(state, id).filter(event => event.state === "closed_unverified")).toHaveLength(1);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("changes only closure markers, preserves evidence, and acknowledges only matching incidents", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-evidence"), (instance, state) => {
      configure(instance); insert(instance, state, "closed");
      instance.recordDeliveryLedgerEvent({ offerId: "closed", target: "main", operation: "delivery",
        state: "unknown", attempt: 4, generation: 1, occurredAt: UNKNOWN_AT, error: "ambiguous:network interrupted" });
      for (const key of ["delivery-queue:closed:main", "delivery-queue:closed:delivery", "delivery-slo", "ticket-api"]) incident(state, key);
      const before = offer(state, "closed"), priorEvents = events(state, "closed"), priorIncidents = incidents(state);
      expect(close(instance, "closed")).toMatchObject({ ok: true, id: "closed", target: "main", outcome: "closed" });
      expect(offer(state, "closed")).toEqual({ ...before, status: "delivery_quarantined",
        delivery_quarantine_reason: "unknown_closed_unverified:main" });
      const ledger = events(state, "closed");
      expect(ledger.slice(0, priorEvents.length)).toEqual(priorEvents);
      expect(ledger).toHaveLength(priorEvents.length + 1);
      expect(ledger.at(-1)).toMatchObject({ offer_id: "closed", target: "main", operation: "delivery",
        state: "closed_unverified", attempt: 4, generation: 1, external_id: PROOF.evidenceReference });
      expect(ledger.at(-1).occurred_at).toMatch(/^\d{4}-\d\d-\d\dT/);
      const inventory = instance.getInventoryById("closed");
      expect(inventory.disposition).toEqual({ state: "closed_unverified", deliveryConfirmed: false,
        retryAllowed: false, evidenceReference: PROOF.evidenceReference });
      expect(inventory.deliveries.main).toMatchObject({ messageId: 0, sentAt: "", unknownAt: UNKNOWN_AT });
      expect(JSON.stringify(inventory)).not.toContain("Private");
      expect(incidents(state)).toEqual(priorIncidents.map(row => ["delivery-queue:closed:main", "delivery-slo"].includes(row.key)
        ? { ...row, status: "administratively_acknowledged" } : row));
      const settled = { row: offer(state, "closed"), ledger: events(state, "closed"), incidents: incidents(state) };
      expect(close(instance, "closed")).toMatchObject({ ok: true });
      expect({ row: offer(state, "closed"), ledger: events(state, "closed"), incidents: incidents(state) }).toEqual(settled);
      expect(() => close(instance, "closed", { ...PROOF, evidenceReference: "operator:different" })).toThrow("delivery_resolution_conflict");
      expect(() => close(instance, "closed", { ...PROOF, expectedAttempt: 5 })).toThrow("delivery_resolution_conflict");
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    ["evidenceReference", undefined, "delivery_resolution_evidence_required"],
    ["evidenceReference", "operator:", "delivery_resolution_evidence_required"],
    ["evidenceReference", "ticket:case-1", "delivery_resolution_evidence_required"],
    ["evidenceReference", "operator:private value", "delivery_resolution_evidence_required"],
    ["evidenceReference", `operator:${"a".repeat(152)}`, "delivery_resolution_evidence_required"],
    ["expectedGeneration", undefined, "delivery_resolution_expected_generation_invalid"],
    ["expectedGeneration", "1", "delivery_resolution_expected_generation_invalid"],
    ["expectedGeneration", 0, "delivery_resolution_expected_generation_invalid"],
    ["expectedGeneration", 1.5, "delivery_resolution_expected_generation_invalid"],
    ["expectedAttempt", undefined, "delivery_resolution_expected_attempt_invalid"],
    ["expectedAttempt", "4", "delivery_resolution_expected_attempt_invalid"],
    ["expectedAttempt", -1, "delivery_resolution_expected_attempt_invalid"],
    ["expectedAttempt", 4.5, "delivery_resolution_expected_attempt_invalid"],
    ["expectedUnknownAt", undefined, "delivery_resolution_expected_unknown_at_invalid"],
    ["expectedUnknownAt", "2026-10-01", "delivery_resolution_expected_unknown_at_invalid"],
    ["expectedUnknownAt", "2026-10-01T12:34:56Z", "delivery_resolution_expected_unknown_at_invalid"],
    ["expectedUnknownAt", "2026-10-01T12:34:56.789+00:00", "delivery_resolution_expected_unknown_at_invalid"],
    ["expectedUnknownAt", "2026-02-30T12:34:56.789Z", "delivery_resolution_expected_unknown_at_invalid"],
  ])("rejects invalid %s=%s before mutation", async (field, value, error) => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`closure-input-${field}-${String(value)}`), (instance, state) => {
      configure(instance); insert(instance, state, "invalid");
      const before = offer(state, "invalid");
      expect(() => close(instance, "invalid", { ...PROOF, [field]: value })).toThrow(error);
      expect(offer(state, "invalid")).toEqual(before);
      expect(events(state, "invalid")).toEqual([]);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    ["generation", { delivery_generation: 2 }, "delivery_resolution_conflict"],
    ["attempt", { main_delivery_attempts: 5 }, "delivery_resolution_conflict"],
    ["unknown timestamp", { main_delivery_unknown_at: "2026-10-02T12:34:56.789Z", delivery_unknown_at: "2026-10-02T12:34:56.789Z" }, "delivery_resolution_conflict"],
    ["status", { status: "delivery_pending" }, "delivery_target_not_unknown"],
    ["no unknown", { main_delivery_unknown_at: "", delivery_unknown_at: "", delivery_unknown_target: "" }, "delivery_target_not_unknown"],
    ["main in flight", { main_delivery_in_flight_at: UNKNOWN_AT }, "delivery_in_flight_requires_resolution"],
    ["other in flight", { discord_delivery_in_flight_at: UNKNOWN_AT }, "delivery_in_flight_requires_resolution"],
    ["main receipt timestamp", { main_sent_at: UNKNOWN_AT }, "delivery_resolution_receipt_exists"],
    ["main receipt ID", { main_message_id: 91 }, "delivery_resolution_receipt_exists"],
  ])("rejects changed %s", async (name, fields, error) => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`closure-precondition-${name}`), (instance, state) => {
      configure(instance); insert(instance, state, "conflict", fields);
      const before = offer(state, "conflict");
      expect(() => close(instance, "conflict")).toThrow(error);
      expect(offer(state, "conflict")).toEqual(before);
      expect(events(state, "conflict")).toEqual([]);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["canal2 unknown", "discord unknown", "legacy other unknown", "comment unknown", "comment pending", "beeper pending", "canal2 pending", "canal2 pending disabled", "discord ticket pending"])("rejects other debt: %s", async kind => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`closure-debt-${kind}`), (instance, state) => {
      configure(instance);
      const fields = kind === "canal2 unknown" ? { canal2_delivery_unknown_at: UNKNOWN_AT }
        : kind === "discord unknown" ? { discord_delivery_unknown_at: UNKNOWN_AT }
        : kind === "legacy other unknown" ? { delivery_unknown_target: "main,discord" }
        : kind === "comment unknown" ? { comment_delivery_unknown_at: UNKNOWN_AT }
        : kind === "comment pending" ? { discussion_message_id: 87 }
        : kind.startsWith("canal2 pending") ? { would_send_canal2: 1 }
        : kind === "discord ticket pending" ? { link: "https://clube.uol.com.br/campanhasdeingresso/private" } : {};
      insert(instance, state, "debt", fields);
      if (kind === "canal2 pending") instance.env.CANAL2_DELIVERY_ENABLED = "true";
      if (kind === "discord ticket pending") instance.env.DISCORD_DELIVERY_ENABLED = "true";
      if (kind === "beeper pending") state.storage.sql.exec("INSERT INTO beeper_delivery_queue(offer_id, pending_message_id) VALUES ('debt', 'pending-receipt')");
      const before = offer(state, "debt");
      expect(() => close(instance, "debt")).toThrow("delivery_resolution_other_debt");
      expect(offer(state, "debt")).toEqual(before);
      expect(events(state, "debt")).toEqual([]);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["sent", "late_sent", "resolved_sent"])("preserves a main %s ledger receipt", async receiptState => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName(`closure-ledger-receipt-${receiptState}`), (instance, state) => {
      configure(instance); insert(instance, state, "receipt");
      instance.recordDeliveryLedgerEvent({ offerId: "receipt", target: "main", operation: "delivery",
        state: receiptState, attempt: 4, generation: 1, occurredAt: UNKNOWN_AT, externalId: "91" });
      const before = { row: offer(state, "receipt"), ledger: events(state, "receipt") };
      expect(() => close(instance, "receipt")).toThrow("delivery_resolution_receipt_exists");
      expect({ row: offer(state, "receipt"), ledger: events(state, "receipt") }).toEqual(before);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rolls back offer and incident changes if the mandatory ledger fails", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-ledger-rollback"), (instance, state) => {
      configure(instance); insert(instance, state, "rollback"); incident(state, "delivery-queue:rollback:main"); incident(state, "delivery-slo");
      const before = { row: offer(state, "rollback"), incidents: incidents(state), ledger: events(state, "rollback") };
      const sqlExec = instance.sqlExec;
      instance.sqlExec = function (query, ...args) {
        if (/INSERT OR IGNORE INTO delivery_events/u.test(query)) throw new Error("injected_ledger_failure");
        return sqlExec.call(this, query, ...args);
      };
      try { expect(() => close(instance, "rollback")).toThrow("injected_ledger_failure"); }
      finally { instance.sqlExec = sqlExec; }
      expect({ row: offer(state, "rollback"), incidents: incidents(state), ledger: events(state, "rollback") }).toEqual(before);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("keeps delivery-slo active when another offer remains pending", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-other-pending"), (instance, state) => {
      configure(instance); insert(instance, state, "closed");
      insert(instance, state, "pending", { status: "delivery_pending", main_delivery_unknown_at: "", delivery_unknown_at: "", delivery_unknown_target: "", main_delivery_attempts: 0 });
      incident(state, "delivery-queue:closed:main"); incident(state, "delivery-slo");
      const before = incidents(state).find(row => row.key === "delivery-slo");
      close(instance, "closed");
      expect(incidents(state).find(row => row.key === "delivery-slo")).toEqual(before);
      expect(incidents(state).find(row => row.key === "delivery-queue:closed:main").status).toBe("administratively_acknowledged");
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("removes only administratively closed debt from queue and readiness counters", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-health-counters"), async (instance, state) => {
      configure(instance); insert(instance, state, "health");
      incident(state, "delivery-queue:health:main"); incident(state, "delivery-slo");
      expect(instance.getReleaseDebt().checks).toMatchObject({ criticalIncidents: 2, unknown: 1 });
      expect(instance.deliveryQueueSlo().pending).toBe(1);
      close(instance, "health", { ...PROOF, evidenceReference: `operator:${"a".repeat(151)}` });
      expect(instance.getReleaseDebt().checks).toEqual({ criticalIncidents: 0, deadLetters: 0,
        unknown: 0, maintenanceDeadLetters: 0 });
      expect(instance.deliveryQueueSlo().pending).toBe(0);
      expect(instance.deliveryQueueIssues()).toEqual([]);
      expect((await instance.getReadiness()).checks).toMatchObject({ criticalIncidents: 0,
        deadLetters: 0, unknown: 0, blockedConfiguration: 0, maintenanceDeadLetters: 0 });
      expect(offer(state, "health")).toMatchObject({ main_sent_at: "", main_message_id: 0,
        main_delivery_unknown_at: UNKNOWN_AT, delivery_unknown_at: UNKNOWN_AT });
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("preserves closure when identity repair finds an earlier duplicate keeper", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-identity-repair"), (instance, state) => {
      configure(instance);
      const id = "pabc-private", link = "https://clube.uol.com.br/beneficios/pabc-private";
      insert(instance, state, id, { link }); close(instance, id);
      insert(instance, state, "pabc-private--202609011200", { link, status: "baseline",
        first_seen_at: "2026-09-01T12:00:00.000Z", would_send_main: 0,
        main_delivery_attempts: 0, main_delivery_unknown_at: "", main_delivery_error: "",
        delivery_unknown_at: "", delivery_unknown_target: "" });
      const before = offer(state, id), ledger = events(state, id);
      expect(instance.repairIdentityAliases()).toMatchObject({ ok: true, reconciled: 0 });
      expect(instance.refreshDeliveryStatus(id).classification).toMatchObject({ state: "closed_unverified", actionable: [] });
      expect(offer(state, id)).toEqual(before);
      expect(events(state, id)).toEqual(ledger);
      expect(offer(state, "pabc-private--202609011200").status).toBe("baseline");
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("observes a returning listing card without replacing its closure or delivery evidence", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-listing-reappearance"), (instance, state) => {
      configure(instance);
      const id = "pabc-return", link = "https://clube.uol.com.br/beneficios/pabc-return";
      insert(instance, state, id, { link });
      instance.recordIdentityAliases(id, id, link, UNKNOWN_AT);
      close(instance, id);
      const before = offer(state, id), ledger = events(state, id);
      const card = { id, link, previewTitle: "Current title", category: "beneficios",
        cardImageUrl: "", partnerImageUrl: "", partnerName: "" };
      const observedAt = "2026-10-17T12:00:00.000Z";
      expect(instance.resolveListingCards([card], observedAt, "pending_enrichment")).toMatchObject({ inserted: 0, insertedIds: [] });
      const after = offer(state, id);
      expect(after).toMatchObject({ preview_title: "Current title", last_seen_at: observedAt });
      expect({ ...after, preview_title: before.preview_title, category: before.category,
        last_seen_at: before.last_seen_at }).toEqual(before);
      expect(events(state, id)).toEqual(ledger);
      expect(state.storage.sql.exec("SELECT COUNT(*) AS count FROM offers").one().count).toBe(1);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("does not recover or replay acknowledged incidents across later operational health checks", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-no-alert-replay"), async (instance, state) => {
      configure(instance); insert(instance, state, "quiet");
      for (const key of ["delivery-queue:quiet:main", "delivery-slo", "ticket-api"]) incident(state, key);
      close(instance, "quiet");
      const before = incidents(state), ledger = events(state, "quiet");
      const enqueue = vi.fn(() => false);
      instance.enqueueOperationalAlert = enqueue;
      instance.drainOperationalAlerts = async () => {};
      instance.env.UOL_API_AUTHORIZATION = "";
      const at = Date.parse("2026-10-17T12:00:00.000Z");
      for (const minutes of [0, 1, 6]) {
        expect(await instance.processOperationalHealth(new Date(at + minutes * 60_000))).toEqual({ active: 0, alerted: 0, recovered: 0 });
      }
      expect(enqueue).not.toHaveBeenCalled();
      expect(incidents(state)).toEqual(before);
      expect(events(state, "quiet")).toEqual(ledger);
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("blocks every requeue/resolution, stale dispatch, reconciliation and retention from reopening closure", async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("closure-permanent-guard"), async (instance, state) => {
      configure(instance); insert(instance, state, "sealed");
      const stale = { ...offer(state, "sealed"), status: "delivery_pending", main_delivery_unknown_at: "", delivery_unknown_at: "", delivery_unknown_target: "" };
      close(instance, "sealed");
      const before = offer(state, "sealed"), ledger = events(state, "sealed");
      for (const target of ["offer", "main", "canal2", "discord", "comment", "main_sold_out", "canal2_sold_out"]) {
        expect(() => instance.requeueDelivery("sealed", target)).toThrow("delivery_closed_unverified");
      }
      for (const outcome of ["sent", "not_sent"]) expect(() => instance.resolveDeliveryUnknown("sealed", "main", outcome, { messageId: 91 })).toThrow("delivery_closed_unverified");
      expect(classifyDeliveryRow(before, configuration)).toMatchObject({ state: "closed_unverified", actionable: [] });
      expect(instance.refreshDeliveryStatus("sealed").classification).toMatchObject({ state: "closed_unverified", actionable: [] });
      expect(await instance.processDeliveryQueue(new Date(), { rows: [stale], waitForMainImage: false })).toMatchObject({ mainSent: 0, canal2Sent: 0, discordSent: 0 });
      expect(await instance.processDiscussionComments(1)).toEqual({ sent: 0, failed: 0 });
      instance.pruneOffers(new Date("2027-01-01T12:00:00.000Z"));
      expect(offer(state, "sealed")).toEqual(before);
      expect(events(state, "sealed")).toEqual(ledger);
    });
    expect(fetch).not.toHaveBeenCalled();
  });
});
