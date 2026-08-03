# Telegram Image Deadline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Esperar foto por no máximo 60 segundos, publicar texto no prazo e converter a mesma mensagem em foto com legenda quando a mídia chegar depois.

**Architecture:** Política pura calcula prazo desde `first_seen_at`. Entrega principal tenta foto em cada alarme e adia somente o fallback textual. Manutenção enriquece a oferta e usa `editMessageMedia` para substituir texto por `InputMediaPhoto` com legenda, mantendo `message_id`.

**Tech Stack:** Cloudflare Workers, Durable Objects SQLite, JavaScript ESM, Telegram Bot API, Node test runner.

## Global Constraints

- `MAIN_IMAGE_WAIT_SECONDS=60` e prazo absoluto desde primeira detecção.
- Nenhum preview de link.
- Discord sem alterações.
- Nenhuma dependência, fila ou custo adicional.
- Resultado ambíguo nunca cria segunda postagem.

---

### Task 1: Política de prazo da imagem

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/image-deadline.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test/image-deadline.test.js`

**Interfaces:**
- Produces: `imageDeadline(firstSeenAt, now, waitSeconds)` retornando `{ deadlineAt, expired, remainingMs }`.

- [ ] **Step 1: Write failing policy tests**

```js
assert.deepEqual(imageDeadline("2026-08-03T12:00:00.000Z", new Date("2026-08-03T12:00:45.000Z"), 60), {
  deadlineAt: "2026-08-03T12:01:00.000Z",
  expired: false,
  remainingMs: 15_000,
});
assert.equal(imageDeadline("2026-08-03T12:00:00.000Z", new Date("2026-08-03T12:01:00.000Z"), 60).expired, true);
```

- [ ] **Step 2: Run RED**

Run: `node --test test/image-deadline.test.js`
Expected: FAIL because module/function does not exist.

- [ ] **Step 3: Implement minimal policy**

Parse `firstSeenAt`, clamp seconds to `1..300`, calculate fixed deadline, and fail closed as expired for invalid timestamps.

- [ ] **Step 4: Run GREEN**

Run: `node --test test/image-deadline.test.js`
Expected: PASS.

### Task 2: Telegram text timeout and late media edit

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/telegram.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/telegram.test.js`

**Interfaces:**
- `sendMainOffer(env, offer, fetchImpl)` returns `{ deferred: true, imageAttempts }` when photo fails and `offer.deferTextFallback === true`.
- `editMainOfferMedia(env, { messageId, offer, telegramPhotoFileId, imageStrategies }, fetchImpl)` returns photo identity and strategy.

- [ ] **Step 1: Write failing transport tests**

```js
assert.equal((await sendMainOffer(env, { ...offer, deferTextFallback: true }, fetchImpl)).deferred, true);
assert.equal(calls.some(({ method }) => method === "sendMessage"), false);

const edited = await editMainOfferMedia(env, { messageId: 77, offer }, fetchImpl);
assert.equal(edited.messageId, 77);
assert.equal(editPayload.media.type, "photo");
assert.equal(editPayload.media.caption.includes(offer.title), true);
```

- [ ] **Step 2: Run RED**

Run: `node --test --test-name-pattern='adia texto|edita texto tardio' test/telegram.test.js`
Expected: FAIL because deferral/edit API is absent.

- [ ] **Step 3: Implement minimal Telegram behavior**

Reuse caption and image ordering. `editMessageMedia` sends `InputMediaPhoto` with `caption`, `parse_mode: "HTML"`; URL/`file_id` uses JSON and upload uses multipart `attach://photo`. Clamp every image request timeout to `offer.imageDeadlineAt - Date.now()` so network work cannot cross the absolute deadline. Timeout text uses `link_preview_options: { is_disabled: true }` and strategy `text_timeout`.

- [ ] **Step 4: Run GREEN**

Run: `node --test test/telegram.test.js`
Expected: PASS.

### Task 3: Persistência e fila principal

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/migrations.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Interfaces:**
- Migration 14 adds `main_image_upgrade_attempts`, `main_image_upgrade_next_attempt_at`, and `main_image_upgrade_error`.
- `processDeliveryQueue(now, options)` derives deadline from `first_seen_at` and `MAIN_IMAGE_WAIT_SECONDS`.

- [ ] **Step 1: Write failing migration and architecture tests**

```js
assert.equal(schemaVersion, 14);
assert.equal(columns.includes("main_image_upgrade_attempts"), true);
assert.match(scan, /deferTextFallback:\s*!imageDeadlineState\.expired/);
```

- [ ] **Step 2: Run RED**

Run: `node --test test/migrations.test.js test/architecture.test.js`
Expected: FAIL on schema 13 and missing deadline behavior.

- [ ] **Step 3: Implement migration and queue gate**

For main deliveries, try full photo strategies immediately. Before deadline pass `deferTextFallback: true` plus `imageDeadlineAt`; on `{ deferred: true }`, recheck the clock, send timeout text in the same execution if the deadline expired, otherwise clear in-flight state and schedule the next attempt without recording a delivery failure. At/after deadline skip image work, permit `sendMessage`, and persist `text_timeout`.

- [ ] **Step 4: Run GREEN**

Run: `node --test test/migrations.test.js test/architecture.test.js test/delivery-state.test.js`
Expected: PASS.

### Task 4: Upgrade tardio da mesma mensagem

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/telegram.test.js`

**Interfaces:**
- `upgradeTimedOutMainImages(now)` selects only `text_timeout` offers with image and existing `main_message_id`.

- [ ] **Step 1: Write failing late-upgrade test**

```js
assert.match(scan, /telegram_image_strategy = 'text_timeout'/);
assert.match(scan, /editMainOfferMedia/);
assert.match(scan, /main_message_kind = 'photo'/);
```

- [ ] **Step 2: Run RED**

Run: `node --test --test-name-pattern='foto tardia' test/architecture.test.js test/telegram.test.js`
Expected: FAIL because upgrade path is absent.

- [ ] **Step 3: Implement late upgrade**

Run after enrichment in maintenance. On success update same row to `photo`, persist cache/strategy and clear retry fields. On definite failure increment attempts and apply existing retry backoff; on ambiguous result retain message identity and retry reconciliation without sending another message. Limit attempts to 10.

- [ ] **Step 4: Run GREEN**

Run: `node --test test/architecture.test.js test/telegram.test.js`
Expected: PASS.

### Task 5: Configuração, documentação e verificação

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/wrangler.jsonc`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/worker-configuration.d.ts`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/README.md`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/IMPLEMENTATION.md`

- [ ] **Step 1: Configure exact deadline**

Add `MAIN_IMAGE_WAIT_SECONDS = "60"`. Document photo-first deadline, timeout text without preview, late `editMessageMedia`, and unchanged Discord flow.

- [ ] **Step 2: Run full verification**

Run: `npm test && npm run check:types && npm run check:bundle`
Expected: all tests pass, generated types current, dry-run exits 0.

- [ ] **Step 3: Prove Discord unchanged**

Run in `cloudflare-workers/uol-ingressos-discord-worker`: `npm run check`
Expected: 12 tests pass and dry-run exits 0.

- [ ] **Step 4: Review scope**

Run: `git diff --check && git status --short && git diff --stat`
Expected: only Telegram Worker, its tests/docs/config, plan and spec changed.
