# SigBot Core Documentation

[中文索引](index_ZH.md)

This is the canonical entry point for current architecture, development
specifications, operational runbooks, use cases, and historical records. Each
design concept has one owner; other documents link to it instead of repeating
its contract.

## Architecture

The [system overview](architecture/overview.md) is the single catalog and owner
for cross-component boundaries, the constrained typed trading Workflow,
service/runtime scope, embedded SDKs, naming, data ownership, and links to every
engine design. Detailed documents remain organized by physical component under
`architecture/engine/`.

## Use Cases

| Use case | English | 中文 | Scope |
|---|---|---|---|
| Researcher integration | [Specification](use-cases/researcher.md) | [规范](use-cases/researcher_ZH.md) | Boundary with the external `sigbot-researcher` project |

## Development Lifecycle

Stable language and framework rules live in technology-specific subdirectories
and use a `*_specification.md` canonical filename.

| Technology or topic | English | 中文 |
|---|---|---|
| Rust | [Coding specification](development/rust/coding_specification.md) | [编码规范](development/rust/coding_specification_ZH.md) |
| Solidity | [Coding specification](development/solidity/coding_specification.md) | [编码规范](development/solidity/coding_specification_ZH.md) |
| Python Strategy SDK | [SDK specification](development/python/strategy-sdk_specification.md) | [SDK 规范](development/python/strategy-sdk_specification_ZH.md) |

The current implementation/evidence/known-gap handoff for coding agents is
[HARDCOVER](development/HARDCOVER.md). It is an operational snapshot, not a
replacement for the canonical architecture documents.

High-change plans, drafts, experiments, and implementation notes live under
`development/in-progress/`; frozen engineering records live under
`development/archive/`. Neither location is authoritative for current product
behavior.

| Lifecycle | Item | Meaning |
|---|---|---|
| In progress | [Copy-trading strategy research](development/in-progress/copytrading/README_ZH.md) | Indexed research prototype; its script is not production source |
| Archive | [Historical records](development/archive/index.md) | Frozen drafts, audits, and implementation snapshots |

## Operational Runbooks

| Area | Document |
|---|---|
| Local setup | [Development prerequisites](runbooks/development-prerequisites.md) |
| Build and test | [Build and test](runbooks/build-and-test.md) |
| Deployment | [Docker](runbooks/build-and-deploy-docker.md) · [Kubernetes](runbooks/deploy-kubernetes.md) |
| Profiling | [CPU](runbooks/profile-cpu.md) · [Memory](runbooks/profile-memory.md) |

## Language Convention

- English files without a language suffix are canonical for current design and
  guidance.
- Chinese companions use `_ZH.md` beside the English source.
- Historical records may remain single-language to preserve the original
  artifact.
- A semantic change to a current paired document updates both files together.

## Maintenance Rules

- Keep one canonical owner for each design concept; link instead of copying.
- Organize engine architecture by physical component. Keep cross-component
  catalogs in the architecture overview instead of duplicating them here.
- Distinguish implemented behavior, partial behavior, reserved design, and
  known limitations.
- Keep executable configuration and production source in their owning source
  directory; documentation links to them.
- Do not add empty category trees, generated caches, secrets, or runtime
  artifacts to the documentation tree.
- Validate relative links, headings, and current-document language pairs before
  considering a documentation change complete.
