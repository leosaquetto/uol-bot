# Beeper Group Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate UOL ticket delivery from the WhatsApp personal canary chat to the real group without duplicate delivery, silent gateway failure, or historical backlog.

**Architecture:** Scope every delivery idempotency key to a stable destination alias owned by the Cloudflare Worker. Extend the existing public GitHub readiness monitor to include the Oracle Beeper gateway. Keep the personal destination active until a separately approved group canary succeeds; then pause Beeper delivery, change the destination and alias together, verify, and resume.

**Tech Stack:** Cloudflare Workers, Node.js 22, Beeper Server local transport, Oracle Linux systemd, GitHub Actions.

## Global Constraints

- Do not send any Beeper message without showing the complete draft and receiving explicit approval.
- Keep the current personal destination active until the group canary is approved.
- Never expose gateway, Beeper, Cloudflare, or GitHub credentials in output or committed files.
- Preserve the existing activation cutoff so historical offers are not queued.
- Deploy only after targeted tests and the existing CI release gate pass.

---

### Task 1: Destination-scoped idempotency

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/beeper.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/beeper.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/wrangler.jsonc`

**Interfaces:**
- Consumes: `env.BEEPER_DESTINATION_KEY`.
- Produces: `beeperDestinationKey(env): string` and keys shaped as `uol:<offer-id>:<destination>:v1`.

- [ ] **Step 1: Write a failing test**

```js
assert.equal(beeperDestinationKey({ BEEPER_DESTINATION_KEY: "whatsapp-personal" }), "whatsapp-personal");
assert.equal(beeperDeliveryIdempotencyKey("offer-1", "whatsapp-group"), "uol:offer-1:whatsapp-group:v1");
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `node --test cloudflare-workers/uol-telegram-shadow-worker/test/beeper.test.js`
Expected: FAIL because the destination helpers do not exist.

- [ ] **Step 3: Implement normalization and use it in the queue**

```js
export function beeperDestinationKey(env) {
  const value = String(env.BEEPER_DESTINATION_KEY || "").trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9._-]{2,63}$/.test(value)) throw new Error("beeper_destination_key_invalid");
  return value;
}

export function beeperDeliveryIdempotencyKey(offerId, destinationKey) {
  return `uol:${offerId}:${destinationKey}:v1`;
}
```

- [ ] **Step 4: Run focused and full Worker tests**

Run: `node --test cloudflare-workers/uol-telegram-shadow-worker/test/beeper.test.js`
Expected: PASS.

Run: `npm test` from `cloudflare-workers/uol-telegram-shadow-worker`.
Expected: 223 or more tests pass.

- [ ] **Step 5: Commit**

```bash
git add cloudflare-workers/uol-telegram-shadow-worker/src/beeper.js cloudflare-workers/uol-telegram-shadow-worker/src/worker.js cloudflare-workers/uol-telegram-shadow-worker/test/beeper.test.js cloudflare-workers/uol-telegram-shadow-worker/wrangler.jsonc
git commit -m "fix: scope Beeper delivery keys by destination"
```

### Task 2: External Oracle gateway monitor

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/beeper-gateway-health.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/beeper-gateway-health.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/scripts/monitor-ready.mjs`
- Modify: `.github/workflows/uol-ready-monitor.yml`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/README.md`

**Interfaces:**
- Consumes: `{status, body}` from the public Oracle `/readyz` endpoint.
- Produces: `mergeBeeperGatewayHealth(workerResult, gatewayResponse)` with a sanitized gateway snapshot.

- [ ] **Step 1: Write failing healthy and outage tests**

```js
assert.equal(mergeBeeperGatewayHealth(workerHealthy, { status: 200, body: { ok: true } }).state, "healthy");
assert.deepEqual(
  mergeBeeperGatewayHealth(workerHealthy, { status: 503, body: null }).reasons,
  ["beeper_gateway_unavailable"],
);
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `node --test cloudflare-workers/uol-telegram-shadow-worker/test/beeper-gateway-health.test.js`
Expected: FAIL because the classifier does not exist.

- [ ] **Step 3: Implement the pure classifier and monitor fetch**

```js
const healthy = gateway?.status === 200 && gateway?.body?.ok === true;
return healthy ? result : {
  ...result,
  state: "outage",
  hardFailure: true,
  reasons: [...new Set([...result.reasons, "beeper_gateway_unavailable"])],
};
```

Set `BEEPER_READY_URL=https://163-176-194-58.sslip.io/readyz` in the existing five-minute workflow. Only persist `{status, ok}` in the incident snapshot.

- [ ] **Step 4: Run focused and full Worker tests**

Run: `node --test cloudflare-workers/uol-telegram-shadow-worker/test/beeper-gateway-health.test.js`
Expected: PASS.

Run: `npm test` from `cloudflare-workers/uol-telegram-shadow-worker`.
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/uol-ready-monitor.yml cloudflare-workers/uol-telegram-shadow-worker/src/beeper-gateway-health.js cloudflare-workers/uol-telegram-shadow-worker/test/beeper-gateway-health.test.js cloudflare-workers/uol-telegram-shadow-worker/scripts/monitor-ready.mjs cloudflare-workers/uol-telegram-shadow-worker/README.md
git commit -m "feat: monitor the Oracle Beeper gateway"
```

### Task 3: Controlled group canary and cutover

**Files:**
- Modify at runtime: `/etc/beeper-preview-gateway.env` on Oracle.
- Modify during cutover: `cloudflare-workers/uol-telegram-shadow-worker/wrangler.jsonc`.

**Interfaces:**
- Consumes: approved canary draft and group chat ID `!o8sUOrMhcN9T0FkGFkKykcoqlm8:ba_4Q1u4OVNSpRHOQ0x-5cCkpRVB1o.local-whatsapp.localhost`.
- Produces: one verified group message with `links.img`, then live delivery keyed to `whatsapp-group`.

- [ ] **Step 1: Stop and request explicit approval for the complete canary draft**

```text
🧪 TESTE — pode ignorar
Validação do robô de ofertas Clube UOL no grupo.
https://clube.uol.com.br/festaclubedos30/psP-pague-apenas-r-80-taxa-no-ingresso
```

Expected: no message is sent until the user explicitly approves this exact draft.

- [ ] **Step 2: After approval, pause only Beeper delivery and deploy**

Set `BEEPER_DELIVERY_ENABLED` to `false`, keep Telegram and Discord live, run CI/release, and verify `/readyz` reports the expected deliberate configuration state.

- [ ] **Step 3: Confirm no pending Beeper queue or ambiguous gateway delivery**

Read the Worker operational snapshot and Oracle delivery ledger. Expected: zero unknown deliveries and no actionable old Beeper row.

- [ ] **Step 4: Change Oracle target and send the approved canary**

Atomically replace only `BEEPER_CHAT_ID`, restart `beeper-preview-gateway`, verify `/readyz=200`, then send the exact approved draft with the known Clube UOL preview image.

- [ ] **Step 5: Verify group delivery before resuming**

Read the group message through Beeper and require `isSender=true`, the exact text, and a non-empty `links[0].img`.

- [ ] **Step 6: Resume with the group destination alias**

Set `BEEPER_DESTINATION_KEY` to `whatsapp-group`, restore `BEEPER_DELIVERY_ENABLED=true`, deploy, and confirm Worker readiness plus Oracle readiness.

- [ ] **Step 7: Commit final configuration**

```bash
git add cloudflare-workers/uol-telegram-shadow-worker/wrangler.jsonc
git commit -m "feat: deliver UOL ticket offers to WhatsApp group"
git push origin main
```
