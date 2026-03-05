# Data Connect Framework (Public Template)

This repo is a framework-only template with no personal data.
It intentionally excludes runtime databases, exported archives, logs, and private account artifacts.

## Safe By Default

- Includes contracts, schemas, and ingestion/normalization scripts.
- Includes empty scaffold folders for local runtime.
- Excludes personal data and generated outputs via `.gitignore`.

# Data Connect Platform

`/path/to/data-connect` is the public local workspace root for personal data products.

## Workspace Products

- `raw/`: immutable source artifacts and indexes
- `working/`: extracted archives and temporary staging
- `normalized/`: source-shaped normalized records
- `canonical/`: cross-source entities, events, relationships, artifacts
- `views/`: app- and SDK-facing derived datasets
- `manifests/`: build manifests, source freshness, source health
- `schemas/`: canonical, view, and manifest contracts
- `scripts/`: platform build and compatibility scripts
- `lanes/`: lane-specific outputs for `connector`, `local`, and `full-export`
- `runtime/`: ephemeral indexes, sqlite databases, state, and logs
- `personal-server/`: adjacent personal server state and exported connector data

## Compatibility

The legacy `meta/` path remains temporarily for migration compatibility. New code and docs should target `data-connect/<product>` rather than `data-connect/meta/<product>`.

## Contracts

- [connector-artifacts.md](/path/to/data-connect/contracts/connector-artifacts.md)
- [workspace-products.md](/path/to/data-connect/contracts/workspace-products.md)
- [context-sdk-api.md](/path/to/data-connect/contracts/context-sdk-api.md)
- [privacy-contract.md](/path/to/data-connect/contracts/privacy-contract.md)
