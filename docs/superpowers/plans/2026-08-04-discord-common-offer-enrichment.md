# Discord Common Offer Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (recommended) to implement this plan task-by-task with review checkpoints.

**Goal:** Enriquecer somente os cards de ofertas comuns no Discord e tornar o truncamento do embed explícito e seguro.

**Architecture:** `src/discord.js` terá limites conservadores e truncamento ASCII com `...`. Um módulo HTTP isolado (`src/discord-detail.js`) fará parsing streaming limitado da página pública apenas durante a manutenção do cache/edição do Discord. O `UolTelegramShadow` usará o detalhe transitório sem gravá-lo em `offers`; falhas retornam ao payload atual e não entram no caminho API/Telegram.

**Tech Stack:** Cloudflare Worker, HTMLRewriter, Durable Objects, JavaScript ESM, Node test runner e Vitest pool Workers.

## Global Constraints

- Não alterar o caminho rápido API/listagem → decisão → Telegram/Discord.
- Não alterar legenda/comentário do Telegram, polling, deduplicação, schema SQLite ou imagens do Telegram.
- Máximo padrão de 2 buscas de detalhe por ciclo de manutenção; nenhuma busca para ingressos.
- Responder com card básico quando o detalhe não estiver disponível.
- Título do embed até 240 caracteres; resumo até 1.200; campos abaixo dos limites do Discord; truncamento termina em `...`.

---

### Task 1: Proteger limites e truncamento do payload Discord

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/discord.test.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/discord.js`

**Interfaces:**
- Consumes: `buildDiscordPayload(offer, options)` existente.
- Produces: payload com textos limitados, truncamento ASCII e soma do texto do embed abaixo de 6.000.

- [ ] **Step 1: Write the failing test**

Adicionar um caso com título de 500 caracteres e descrição de 5.000 caracteres. Exigir título com 240 caracteres terminando em `...`, resumo terminando em `...` e soma de `title`, `description`, nomes/valores dos campos e footer menor que 6.000.

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/discord.test.js`

Expected: FAIL because the current helper emits `…` and reserves only one character for truncation.

- [ ] **Step 3: Write minimal implementation**

Em `src/discord.js`, declarar limites nomeados para título/resumo/campos, trocar os helpers para reservar três caracteres e retornar `${prefix}...`, e usar os limites nomeados em `buildDiscordPayload`/`discordOfferFields`.

- [ ] **Step 4: Run focused tests**

Run: `node --test test/discord.test.js`

Expected: PASS, incluindo formato, line breaks, cache, edição e erros de transporte existentes.

- [ ] **Step 5: Commit**

```bash
git add cloudflare-workers/uol-telegram-shadow-worker/src/discord.js cloudflare-workers/uol-telegram-shadow-worker/test/discord.test.js
git commit -m "fix(uol): bound Discord embed text safely"
```

### Task 2: Criar parser HTTP limitado para detalhe comum

**Files:**
- Create: `cloudflare-workers/uol-telegram-shadow-worker/src/discord-detail.js`
- Create: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/discord-detail.integration.test.js`

**Interfaces:**
- Produces: `fetchDiscordOfferDetail(offer, fetchImpl = fetch) -> Promise<{title, validity, description} | null>`.
- Consumes: link público, `cleanText`, `extractValidity` e `offerIdentityKeys`.

- [ ] **Step 1: Write the failing test**

Cobrir no pool Workers que uma resposta HTML simulada extrai título/descrição/validade, que resposta não HTML e detalhe vazio retornam `null`, e que redirect para outra oferta retorna `null`. O teste deve verificar que o marcador de cache-bust e headers `Accept: text/html` são enviados.

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm run test:worker -- --run test-worker/discord-detail.integration.test.js`

Expected: FAIL because `discord-detail.js` and its exported fetch/parser do not exist.

- [ ] **Step 3: Write minimal implementation**

Implementar fetch com timeout de 8s, `Cache-Control: no-cache`, `cf.cacheTtl = 0`, content-type HTML e limite de 2 MB. Usar `HTMLRewriter` streaming para `.info-beneficio`, `.descricao p`, `h1`/`h2`, `body` e meta description; derivar validade com `extractValidity`, limitar descrição a 4.000 e aceitar somente conteúdo útil. Validar identidade final com `offerIdentityKeys`; qualquer falha retorna `null` sem lançar para o chamador de manutenção.

- [ ] **Step 4: Run focused tests**

Run: `npm run test:worker -- --run test-worker/discord-detail.integration.test.js`.

Expected: PASS com HTML, fallback de resposta e redirect.

- [ ] **Step 5: Commit**

```bash
git add cloudflare-workers/uol-telegram-shadow-worker/src/discord-detail.js cloudflare-workers/uol-telegram-shadow-worker/test-worker/discord-detail.integration.test.js
git commit -m "feat(uol): parse public detail for Discord cards"
```

### Task 3: Conectar enriquecimento somente à manutenção Discord

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/worker.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js`

**Interfaces:**
- Consumes: `fetchDiscordOfferDetail` e `rowToOffer`.
- Produces: cards comuns de cache/edição com título, validade e descrição best-effort; o caminho `scan()` não chama o parser.

- [ ] **Step 1: Write the failing integration test**

Adicionar teste que chama `primePendingDiscordImageCache` com uma linha comum sem descrição, intercepta o fetch público e o webhook, e exige que o payload do cache contenha o texto detalhado. Adicionar no teste do caminho rápido um contador que permanece zero para o parser. Adicionar caso de edição comum com detalhe ausente e fallback sem falhar quando o fetch retorna 500.

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm run test:worker -- --run test-worker/worker.integration.test.js`

Expected: FAIL because the maintenance path currently sends `rowToOffer(row)` sem detalhe e não há chamada ao parser.

- [ ] **Step 3: Write minimal implementation**

Importar o parser no Worker, criar método best-effort que só busca quando a oferta não é ingresso e não tem descrição/validade, limitar o batch padrão do cache a 2, e usar o resultado apenas em `cacheDiscordOfferImage` e `editDiscordOffer`. Em caso de erro, manter a oferta original. Não fazer `UPDATE` adicional para detalhe; manter somente os updates de tentativa/mensagem já existentes.

- [ ] **Step 4: Run focused and fast tests**

Run: `npm run test:worker -- --run test-worker/worker.integration.test.js` e `npm run check:fast`.

Expected: PASS; o teste do caminho rápido continua sem fetch de detalhe e os testes existentes de sold-out/restock permanecem verdes.

- [ ] **Step 5: Commit**

```bash
git add cloudflare-workers/uol-telegram-shadow-worker/src/worker.js cloudflare-workers/uol-telegram-shadow-worker/test-worker/worker.integration.test.js
git commit -m "feat(uol): enrich common Discord cards during maintenance"
```

### Task 4: Verificação de bundle e produção

**Files:**
- No source changes expected.

- [ ] **Step 1: Run repository checks**

Run: `npm run check:fast`, `npm run check:types` e `npm run check:bundle`.

Expected: PASS sem mudanças de lockfile.

- [ ] **Step 2: Review diff and deploy once**

Run: `git diff origin/main...HEAD --check`, confirmar que só os arquivos acima e os documentos desta tarefa estão incluídos, então `npm run deploy`.

- [ ] **Step 3: Verify deployment**

Run: `curl -fsS https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/livez` e `npm run postdeploy:check` com a versão retornada pelo deploy.

Expected: `/livez` 200 com a nova versão. Reportar separadamente qualquer `readyz` vermelho pré-existente por quota/incidentes, sem mascará-lo como falha desta mudança.
