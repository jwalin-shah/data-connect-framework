# Data Connect Framework (Public Template)

This repo is a framework-only template with no personal data.
It intentionally excludes runtime databases, exported archives, logs, and private account artifacts.

## Safe By Default

- Includes contracts, schemas, and ingestion/normalization scripts.
- Includes empty scaffold folders for local runtime.
- Excludes personal data and generated outputs via `.gitignore`.

## Documentation

- [Data Workflow](./DATA_WORKFLOW.md): what the platform builds, how data is collected/processed, and how personal data is kept out of this public template.
- [Case Study](./CASE_STUDY.md): what was learned building the ingestion and processing system.

## Data Connect Platform

This repository uses `meta/` as the current workspace root.

## Workspace Products

- `meta/raw/`: immutable source artifacts and indexes
- `meta/working/`: extracted archives and temporary staging
- `meta/normalized/`: source-shaped normalized records
- `meta/canonical/`: cross-source entities, events, relationships, artifacts
- `meta/views/`: app- and SDK-facing derived datasets
- `meta/manifests/`: build manifests, source freshness, source health
- `meta/schemas/`: canonical, view, and manifest contracts
- `meta/scripts/`: platform build and compatibility scripts
- `meta/lanes/`: lane-specific outputs for `connector`, `local`, and `full-export`
- `runtime/`: ephemeral indexes, sqlite databases, state, and logs
- `personal-server/`: adjacent personal server state and exported connector data

## Compatibility

Scripts accept CLI args and environment overrides. In docs, prefer repository-relative paths.

## Contracts

- [connector-artifacts.md](./contracts/connector-artifacts.md)
- [workspace-products.md](./contracts/workspace-products.md)
- [context-sdk-api.md](./contracts/context-sdk-api.md)
- [privacy-contract.md](./contracts/privacy-contract.md)
