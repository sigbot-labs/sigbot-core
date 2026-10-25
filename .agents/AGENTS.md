# Project Conventions

## Scope and Authority

- Start documentation work from [docs/index.md](../docs/index.md) or
  [docs/index_ZH.md](../docs/index_ZH.md).
- Before changing a module, read its owning architecture document linked from
  the documentation index. Do not copy architecture contracts into agent
  instructions.
- Treat source code and tests as the authority for implementation status when
  an architecture specification has not yet been reconciled with current code.
- Use `.agents/skills/` only for reusable agent workflows. Product design,
  module architecture, API contracts, and implementation status belong in
  `docs/`.

## Code Quality

- Preserve existing behavior unless the user explicitly requests a functional
  change.
- Follow user intent; if a request risks deleting or changing unrelated
  functionality, explain the conflict and obtain confirmation first.
- Keep changes cohesive, loosely coupled, logically structured, and free of
  avoidable duplication.
- Break large changes into independently verifiable steps and surface any
  unresolved work instead of hiding it.
- Follow existing module boundaries and use workspace dependencies correctly.
- Prefer language idioms, explicit error handling, testable designs, and useful
  comments for non-obvious logic.
- Consider Tokio concurrency, database consistency, and latency-sensitive
  trading paths when modifying Rust services.
- Never log credentials, tokens, private keys, or other sensitive values.

## Language-Specific Guidance

- Rust: [docs/development/rust/coding_specification.md](../docs/development/rust/coding_specification.md)
- Solidity: [docs/development/solidity/coding_specification.md](../docs/development/solidity/coding_specification.md)
- Python strategy SDK: [docs/development/python/strategy-sdk_specification.md](../docs/development/python/strategy-sdk_specification.md)
- Use Chinese for change summaries and English for code comments unless the
  user requests otherwise.

## Build and Test

- Follow [the build and test runbook](../docs/runbooks/build-and-test.md).
- Any code change must compile and pass tests proportionate to its scope.
- Documentation-only changes require link, heading, and structure validation;
  they do not require a Rust build.

## Documentation

- Each design concept must have one canonical owner. Other documents link to
  it instead of restating it.
- Current architecture belongs under `docs/architecture/`; executable
  procedures under `docs/runbooks/`; stable engineering specifications under
  `docs/development/<technology>/`; active plans, experiments, and
  implementation notes under `docs/development/in-progress/`; completed or
  stale snapshots under `docs/development/archive/`.
- English current documents use `<name>.md`; Chinese companions use
  `<name>_ZH.md`. Keep paired documents semantically aligned.
- Language and framework rules live in a technology subdirectory under
  `docs/development/` and use a `*_specification.md` canonical filename.
- Content under `docs/development/in-progress/` is not a current product
  contract. When work finishes, merge the lasting design into its canonical
  architecture, specification, or runbook before freezing process records in
  `docs/development/archive/`.
- Historical files are frozen evidence and do not require current-document
  language pairing.
- Clearly label implemented behavior, reserved design, known limitations, and
  historical evidence.
- Do not place production source, secrets, generated reports, or build
  artifacts in `docs/`. Explicitly indexed research prototypes under
  `docs/development/in-progress/` may contain executable examples, but never
  caches or production runtime assets.

## Git

- Use concise Conventional Commit subjects when committing.
- Do not add `Co-Authored-By` trailers.
