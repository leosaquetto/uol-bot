import { env, exports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { beforeEach, describe, expect, it } from "vitest";
import worker from "../src/worker.js";

const base = "https://worker.test";
const adminToken = "vitest-admin-token-not-a-secret";
const token = id => `h1.${id}.${"a".repeat(64)}`;
const headers = credential => ({ Authorization: `Bearer ${credential}`, "Content-Type": "application/json" });
const access = (path, credential, body) => exports.default.fetch(`${base}/hunter-access/${path}`, {
  method: body === undefined ? "GET" : "POST", headers: headers(credential),
  ...(body === undefined ? {} : { body: JSON.stringify(body) }),
});
const event = () => ({ eventId: crypto.randomUUID(), source: "portal", observedAt: new Date().toISOString(),
  payload: { link: "https://clube.uol.com.br/campanhasdeingresso/pAB-access-test", title: "Peça de teste" } });
const intake = (credential, observation) => exports.default.fetch(`${base}/hunter-intake`, {
  method: "POST", headers: headers(credential), body: JSON.stringify({ observations: [observation] }),
});
const safe = value => expect(JSON.stringify(value)).not.toMatch(/h1\.|secretHash|synthetic-hunter-private-session|synthetic-uol-|must-not-leak/);

describe("Hunter per-device approval routes", () => {
  beforeEach(async () => {
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor"), instance => {
      instance.storageUsageSnapshot = () => ({ maintenanceAllowed: true });
      // Admission is the tested contract. No public fetch or destination send.
      instance.drainHunterIntake = async () => ({ status: "deferred" });
    });
  });

  it("keeps pending phones scoped, grants owner-approved config/intake and persists revocation", async () => {
    const ownerId = `owner-${crypto.randomUUID().slice(0, 8)}`;
    const memberId = `member-${crypto.randomUUID().slice(0, 8)}`;
    const owner = token(ownerId), member = token(memberId), observation = event();

    const missing = await access("status", owner);
    expect(missing.status).toBe(404);
    expect(await missing.json()).toEqual({ ok: false, error: "hunter_access_not_found" });
    const requested = await access("request", owner, { label: "iPhone proprietário" });
    expect(requested.status).toBe(202);
    expect(requested.headers.get("Cache-Control")).toBe("no-store");
    const pendingOwner = await requested.json();
    expect(pendingOwner).toMatchObject({ ok: true, deviceId: ownerId, state: "pending", role: "member" });
    safe(pendingOwner);
    expect((await access("config", owner)).status).toBe(403);
    expect((await access("devices", owner)).status).toBe(403);
    expect((await intake(owner, observation)).status).toBe(401);

    const ownerDecision = { deviceId: ownerId, decision: "approve", code: pendingOwner.code, role: "owner" };
    expect((await access("admin-decision", owner, ownerDecision)).status).toBe(401);
    const promoted = await access("admin-decision", adminToken, ownerDecision);
    expect(promoted.status).toBe(200);
    expect(await promoted.json()).toMatchObject({ deviceId: ownerId, state: "approved", role: "owner" });

    const requestedMember = await access("request", member, { label: "iPhone amigo" });
    expect(requestedMember.status).toBe(202);
    const pendingMember = await requestedMember.json();
    const listing = await access("devices", owner);
    expect(listing.status).toBe(200);
    const devices = await listing.json();
    safe(devices);
    expect(devices.devices).toEqual(expect.arrayContaining([
      expect.objectContaining({ deviceId: ownerId, role: "owner", state: "approved" }),
      expect.objectContaining({ deviceId: memberId, role: "member", state: "pending" }),
    ]));
    const memberDecision = { deviceId: memberId, decision: "approve", code: pendingMember.code };
    expect((await access("decision", owner, { ...memberDecision, role: "owner" })).status).toBe(400);
    const approved = await access("decision", owner, memberDecision);
    expect(approved.status).toBe(200);
    expect(await approved.json()).toMatchObject({ deviceId: memberId, state: "approved", role: "member" });
    expect((await access("account-config?account=leosaquetto", member)).status).toBe(403);

    const privateConfig = await access("config", member);
    expect(privateConfig.status).toBe(200);
    expect(privateConfig.headers.get("Cache-Control")).toBe("no-store");
    expect(privateConfig.headers.get("X-Robots-Tag")).toBe("noindex, nofollow");
    const config = await privateConfig.json();
    expect(config).toEqual({ ok: true, deviceId: memberId, role: "member", instagramSession: {
      user_agent: "synthetic-hunter-browser", headers: { "accept-language": "pt-BR" }, cookies: [
        { name: "sessionid", value: "synthetic-hunter-private-session", domain: ".instagram.com", path: "/" },
        { name: "csrftoken", value: "synthetic-hunter-csrf", domain: ".instagram.com", path: "/" },
      ],
    } });
    expect((await access("devices", member)).status).toBe(403);
    const accepted = await intake(member, observation);
    expect(accepted.status).toBe(202);
    expect(await accepted.json()).toMatchObject({ accepted: true, persisted: true,
      receipts: [{ eventId: observation.eventId, persisted: true, duplicate: false, status: "queued" }] });
    const receipt = await exports.default.fetch(`${base}/hunter-intake-status?eventId=${observation.eventId}`, { headers: headers(member) });
    expect(receipt.status).toBe(200);
    expect(await receipt.json()).toMatchObject({ eventId: observation.eventId, persisted: true, status: "queued" });

    const revoked = await access("decision", owner, { ...memberDecision, decision: "revoke" });
    expect(revoked.status).toBe(200);
    expect(await revoked.json()).toMatchObject({ deviceId: memberId, state: "revoked" });
    await runInDurableObject(env.UOL_TELEGRAM_SHADOW.getByName("clube-uol-global-monitor"), instance => {
      instance.metadataCache.clear();
    });
    expect((await intake(member, event())).status).toBe(401);
    expect((await access("config", member)).status).toBe(403);
    const revokedStatus = await access("status", member);
    expect(revokedStatus.status).toBe(200);
    const result = await revokedStatus.json();
    expect(result).toMatchObject({ deviceId: memberId, state: "revoked", role: "member" }); safe(result);
    const retained = await access("devices", owner);
    expect((await retained.json()).devices.filter(device => device.deviceId === memberId)).toEqual([
      expect.objectContaining({ state: "revoked" }),
    ]);
    expect((await exports.default.fetch(`${base}/hunter-intake-status?eventId=${observation.eventId}`, { headers: headers(member) })).status).toBe(401);
  });

  it("rejects wrong method/content type, unknown routes and non-h1 credentials", async () => {
    const credential = token(`methods-${crypto.randomUUID().slice(0, 8)}`);
    for (const [path, method] of [["request", "GET"], ["config", "POST"], ["decision", "GET"]]) {
      const response = await exports.default.fetch(`${base}/hunter-access/${path}`, { method, headers: headers(credential) });
      expect(response.status).toBe(405);
    }
    const wrongType = await exports.default.fetch(`${base}/hunter-access/request`, {
      method: "POST", headers: { Authorization: `Bearer ${credential}`, "Content-Type": "text/plain" }, body: "{}",
    });
    expect(wrongType.status).toBe(415);
    expect(await wrongType.json()).toEqual({ ok: false, error: "content_type_invalid" });
    expect((await access("arbitrary", credential)).status).toBe(404);
    for (const invalid of [adminToken, `w1.test.${"a".repeat(64)}`]) {
      const response = await access("status", invalid);
      expect(response.status).toBe(401); safe(await response.json());
    }
  });

  it("shares a personal account only with an explicit owner grant and denies it after revocation", async () => {
    const ownerId = `personal-owner-${crypto.randomUUID().slice(0, 8)}`;
    const memberId = `personal-member-${crypto.randomUUID().slice(0, 8)}`;
    const owner = token(ownerId), member = token(memberId);
    const pendingOwner = await (await access("request", owner, { label: "Proprietário" })).json();
    const promoted = await access("admin-decision", adminToken, { deviceId: ownerId, decision: "approve",
      code: pendingOwner.code, role: "owner", accounts: ["leosaquetto"] });
    expect(promoted.status).toBe(200);
    expect(await promoted.json()).toMatchObject({ accounts: ["leosaquetto"] });
    const pendingMember = await (await access("request", member, { label: "Amigo autorizado" })).json();
    const decision = { deviceId: memberId, decision: "approve", code: pendingMember.code };
    expect((await access("decision", owner, { ...decision, accounts: ["gaoriel"] })).status).toBe(403);
    const approved = await access("decision", owner, { ...decision, accounts: ["leosaquetto"] });
    expect(approved.status).toBe(200);
    expect(await approved.json()).toMatchObject({ accounts: ["leosaquetto"] });
    const config = await access("account-config?account=leosaquetto", member);
    expect(config.status).toBe(200);
    expect(config.headers.get("Cache-Control")).toBe("no-store");
    const value = await config.json();
    expect(value).toEqual({ ok: true, deviceId: memberId, account: "leosaquetto", identityDigest: "1".repeat(64), cookies: [
      { name: "SESS", value: "synthetic-uol-leo-only", domain: ".uol.com.br", path: "/", secure: true, httpOnly: true, hostOnly: false, expires: -1 },
    ] });
    expect(JSON.stringify(value)).not.toMatch(/gab-only|password|instagramSession/);
    expect((await access("account-config?account=gaoriel", member)).status).toBe(403);
    expect((await access("account-config?account=arbitrary", member)).status).toBe(400);
    expect((await access("decision", member, { ...decision, accounts: ["leosaquetto"] })).status).toBe(403);
    const listed = await (await access("devices", owner)).json(); safe(listed);
    expect(listed.devices.find(device => device.deviceId === memberId).accounts).toEqual(["leosaquetto"]);
    expect((await access("decision", owner, { ...decision, decision: "revoke" })).status).toBe(200);
    expect((await access("account-config?account=leosaquetto", member)).status).toBe(403);
  });

  it("cancels an undeclared activation body above 2 KiB without registering a phone", async () => {
    const credential = token(`bounded-${crypto.randomUUID().slice(0, 8)}`);
    let cancelled = false, pulls = 0;
    const body = new ReadableStream({
      pull(controller) { pulls++; controller.enqueue(new Uint8Array(1025)); },
      cancel() { cancelled = true; },
    });
    // Keep the source stream in this isolate so cancellation is observable;
    // the exports RPC bridge transports the body through a separate pipe.
    const response = await worker.fetch(new Request(`${base}/hunter-access/request`, {
      method: "POST", headers: headers(credential), body, duplex: "half",
    }), env, {});
    expect(response.status).toBe(413);
    expect(cancelled).toBe(true);
    expect(pulls).toBeLessThanOrEqual(3);
    safe(await response.json());
    expect((await access("status", credential)).status).toBe(404);
  });
});
