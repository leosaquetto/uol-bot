# Hunter structural refactor

Installed name remains **Clube UOL - Stories Hunter**. One self-contained public
artifact remains the distribution unit. Personal data is never a build input to
the public artifact.

## Phase 1 — reference

- Reference: `f6e8f1676e186e866c7e0b4032e6b3f9f0df3bf5`, version 5.7.9.
- Public artifact: 782,163 bytes; SHA-256
  `08a114ad9e0ffa6753ad6271a4111b38048ee4f2601a85f45c58d84d05d079e8`.
- Exact public bytes preserved in `baseline/hunter-5.7.9.js.gz`; source/output
  sizes and hashes in `baseline/manifest.json`. Original source files remain in
  the reference commit.
- Original offline suite repeated: **178/178**. With baseline/equivalence checks:
  **181/181**. Synthetic states cover both accounts, every navigation theme,
  startup/failure, update first opening and pending collection.
- No functional source changes, no publication, no private account operations.
- Preexisting edit to `docs/operations/2026-10-15-uol-reliability.md` preserved.

Existing tests extract code from source strings. Moving their source readers to
assembled UI/native sources is a test plumbing change, not a contract change;
the assertions must remain intact.

## Phase 2 — mechanical extraction

- Sources: `ui/shell.html`, `ui/styles.css`, `ui/app.js`, `native/host.js`,
  `assets/icons.json`, `assemble-ui.cjs`, and `distribute.cjs`.
- `build.js --prepare` reads only repository sources; private bootstrap reads
  remain available exclusively to the existing installation modes.
- Canonical SVG strings captured from the frozen artifact eliminate Downloads
  dependencies without changing a single icon byte.
- Reassembled template: 344,338 bytes, exact original SHA-256
  `179803485e6272fdc98cbe2ce6c84a5ad491b2fd16dc5ec4b00508d353966358`.
- Generated script: **782,163 → 782,163 bytes**, exact original SHA-256
  `08a114ad9e0ffa6753ad6271a4111b38048ee4f2601a85f45c58d84d05d079e8`.
- Full suite: **184/184**. Existing assertions unchanged; source readers and
  one build placeholder changed to address the extracted files.
- No observable runtime differences and no publication at this checkpoint.

## Phase 3 — asset deduplication

- One embedded logo and one canonical SVG table; placeholders resolve before
  initial-data injection, using replacement callbacks to preserve literal bytes.
- Artifact: **782,163 → 642,655 bytes** (−139,508 bytes; −17.84%).
- SHA-256: `ad43f1fe6b549ebbae3856f47246bf9f2e26577ff492bfb4826ceefea11606f2`.
- Full suite: **184/184**. Startup, failure and dashboard HTML remain byte-identical
  across the frozen synthetic fixtures. CSS, callbacks, native domains and parser
  are unchanged. Artifact bytes differ intentionally because duplicated literals
  are replaced by one local resolver.
- Build/theme unit harnesses load the extracted resolver or isolate it when only
  testing theme persistence. Their data/escaping/privacy assertions remain intact.
- Whitespace inherited from the reference was intentionally preserved during
  extraction; diff whitespace warnings are not formatting changes.
- No publication at this checkpoint.
