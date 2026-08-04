# Discord Semantic Line Breaks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with review checkpoints.

**Goal:** Separar semanticamente a descrição dos cards de ingressos no Discord usando o mesmo espaçamento visual entre blocos já usado pelo aviso inicial.

**Architecture:** Manter a montagem do embed em `src/discord.js`. Adicionar um formatador local que preserva o texto normalizado, insere `\\n\\n` antes de títulos conhecidos e aplica o limite atual sem passar novamente por `cleanText`, que removeria as quebras. Nenhuma outra rota, fonte, entrega ou armazenamento será alterada.

**Tech Stack:** Cloudflare Worker, JavaScript ESM, Node test runner.

## Global Constraints

- Não alterar `cleanText`, o modelo de oferta, Telegram, deduplicação ou fluxo de entrega.
- Não inserir quebra por quantidade fixa de caracteres.
- Preservar status, thumbnail, campos, URL e edição de esgotado.
- Não adicionar dependências nem chamadas de rede.

---

### Task 1: Cobrir o novo espaçamento no payload do Discord

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/discord.test.js`

**Interfaces:**
- Consumes: `buildDiscordPayload(offer)` existente.
- Produces: regressão que exige blocos separados por `\\n\\n` sem alterar campos e thumbnail.

- [ ] **Step 1: Write the failing test**

Adicionar um caso ao lado do teste de formato aprovado:

```js
test("separa a descrição do ingresso em blocos semânticos", () => {
  const payload = buildDiscordPayload({
    ...offer,
    description: "Assinante UOL, resgate seu par. Data: 08 de Agosto de 2026, às 18h Local: Teatro Bradesco - SP. Importante: sujeito a estoque. REGRAS DE RESGATE: Promoção válida por tempo limitado. Atenção, Assinante UOL! A venda é proibida.",
  });
  const description = payload.embeds[0].description;

  assert.match(description, /Clube UOL\\.\\n\\nAssinante UOL/);
  assert.match(description, /par\\.\\n\\nData:/);
  assert.match(description, /2026, às 18h\\n\\nLocal:/);
  assert.match(description, /SP\\.\\n\\nImportante:/);
  assert.match(description, /estoque\\.\\n\\nREGRAS DE RESGATE:/);
  assert.match(description, /limitado\\.\\n\\nAtenção, Assinante UOL!/);
  assert.equal(payload.embeds[0].image.url, offer.cardImageUrl);
  assert.equal(payload.embeds[0].fields.at(-1).value, offer.link);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/discord.test.js`

Expected: FAIL because `buildDiscordPayload` currently compacts the description into one paragraph.

### Task 2: Implementar o formatador isolado do Discord

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/discord.js:8-62`

**Interfaces:**
- Consumes: `offer.description` já normalizada pela pipeline.
- Produces: `description` do embed com blocos semânticos separados e limite máximo de 1.200 caracteres.

- [ ] **Step 1: Write the minimal implementation**

Adicionar uma lista ordenada de títulos e duas funções privadas:

```js
const DISCORD_SECTION_LABELS = [
  "Atenção, Assinante UOL!",
  "REGRAS DE RESGATE",
  "Importante",
  "Local",
  "Data",
];

const DISCORD_SECTION_PATTERN = DISCORD_SECTION_LABELS
  .sort((a, b) => b.length - a.length)
  .map((label) => label.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"))
  .join("|");

function formatDiscordDescription(value) {
  const text = cleanText(value);
  if (!text) return "";
  return text
    .replace(
      new RegExp(`\\\\s+(${DISCORD_SECTION_PATTERN})(?=\\\\s*:|\\\\s|$)`, "gi"),
      "\\n\\n$1",
    )
    .replace(/\\n{3,}/g, "\\n\\n")
    .trim();
}

function boundedDiscordDescription(value, maxLength) {
  const text = formatDiscordDescription(value);
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1).trimEnd()}…`;
}
```

Substituir apenas `boundedDiscordText(offer?.description, 1_200)` por `boundedDiscordDescription(offer?.description, 1_200)`. Não reaplicar `cleanText` sobre o resultado formatado.

- [ ] **Step 2: Run focused tests**

Run: `node --test test/discord.test.js`

Expected: PASS, incluindo os testes de envio, cache de imagem, edição de esgotado e rate limit existentes.

### Task 3: Validar regressão mínima e revisar diff

**Files:**
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/src/discord.js`
- Modify: `cloudflare-workers/uol-telegram-shadow-worker/test/discord.test.js`

- [ ] **Step 1: Run the Worker fast suite**

Run: `npm run check:fast`

Expected: PASS with no changes outside the Discord formatter/test and the already committed design/plan documents.

- [ ] **Step 2: Review the final diff**

Run: `git diff --check && git status --short`

Expected: no whitespace errors, only the intended source and test changes remain unstaged.

- [ ] **Step 3: Commit**

```bash
git add cloudflare-workers/uol-telegram-shadow-worker/src/discord.js cloudflare-workers/uol-telegram-shadow-worker/test/discord.test.js docs/superpowers/plans/2026-08-04-discord-semantic-line-breaks.md
git commit -m "feat(uol): format Discord offer sections"
```
