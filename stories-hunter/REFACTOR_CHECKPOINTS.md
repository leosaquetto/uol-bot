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
