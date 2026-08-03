# UOL Free Fast Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preservar envio principal imediato pela API e reduzir gravações estáveis abaixo do limite gratuito do Durable Objects, sem infraestrutura nova.

**Architecture:** `UolTelegramShadow` mantém polling e envio direto. Telemetria frequente passa a snapshots JSON; observações recebem intervalo mínimo de toque. `UolTelegramMaintenance` mantém trabalho secundário em cadência de 60 segundos.

**Tech Stack:** Cloudflare Workers, Durable Objects SQLite, JavaScript ESM, Node test runner, Wrangler.

## Global Constraints

- API UOL continua fonte do envio principal.
- HTML nunca bloqueia envio principal.
- `ALARM_INTERVAL_SECONDS=15`.
- `MAINTENANCE_INTERVAL_SECONDS=60`.
- `DELIVERY_CONCURRENCY=6`.
- Custo zero; sem Queue ou dependência nova.
- Preservar mudanças preexistentes; sem deploy.

---

### Task 1: Primitivas de custo gratuito

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/core.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/core.test.js`

**Interfaces:**
- Produces: `parseRuntimeSnapshot(value)`, `shouldTouchObservation(lastSeenAt, observedAt, intervalMinutes)`, `estimateDailyRowWrites(input)`.

- [x] **Step 1: Write failing tests** para JSON inválido, janela de 15 minutos e orçamento configurado menor que 100.000.
- [x] **Step 2: Run tests and confirm RED** com exports ausentes.
- [x] **Step 3: Implement pure helpers** com limite diário `100_000` e cálculo explícito por cadência.
- [x] **Step 4: Run focused tests and confirm GREEN**.

### Task 2: Snapshots e observações limitadas

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Interfaces:**
- Consumes: helpers da Task 1.
- Produces: `runtimeSnapshot`, `setRuntimeSnapshot`, `runtimeValue`; snapshots `api`, `html`, `source_health`, `webhook`, `maintenance`.

- [x] **Step 1: Write failing architecture tests** exigindo snapshots e bloqueando gravações `api_*` por campo.
- [x] **Step 2: Run tests and confirm RED**.
- [x] **Step 3: Replace per-field telemetry writes** por um snapshot por grupo, com fallback de leitura legado.
- [x] **Step 4: Gate source observation updates** usando `OFFER_LAST_SEEN_TOUCH_MINUTES`.
- [x] **Step 5: Expose budget estimate in health** com estado `withinFreeTier` e `headroom`.
- [x] **Step 6: Run focused tests and confirm GREEN**.

### Task 3: Cadência e fail-closed legado

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/wrangler.jsonc`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-ingressos-discord-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/architecture.test.js`

**Interfaces:**
- Produces: manutenção 60s, concorrência 6, um rearmamento por handler e coletor legado desativado por padrão.

- [x] **Step 1: Write failing source-contract tests** para os quatro invariantes.
- [x] **Step 2: Run tests and confirm RED**.
- [x] **Step 3: Update config and alarm handlers** preservando antecipação urgente da manutenção.
- [x] **Step 4: Change legacy default** de `true` para `false`.
- [x] **Step 5: Run focused tests and confirm GREEN**.

### Task 4: Documentação e verificação

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/README.md`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/IMPLEMENTATION.md`
- Modify: `.github/workflows/*`

**Interfaces:**
- Produces: documentação coerente e CI cobrindo Worker principal e rollback Discord.

- [x] **Step 1: Correct cost and publication wording** sem afirmar deploy.
- [x] **Step 2: Add Discord rollback tests to CI** quando esse Worker mudar.
- [x] **Step 3: Run Worker Node tests, Discord tests, root tests, type check and bundle dry-run**.
- [x] **Step 4: Review scoped diff and requirement checklist**; record only real limits.
