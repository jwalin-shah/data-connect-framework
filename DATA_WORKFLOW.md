# Data Connect Framework Workflow

This document describes the framework workflow for collecting, normalizing, and modeling data without including personal artifacts in this public repository.

## Goal

Provide a repeatable data pipeline framework that turns multi-source exports into:

- normalized source-shaped records
- canonical cross-source entities/events/relationships
- derived views for downstream apps and SDKs

## Pipeline Stages

1. Intake:
   - identify and stage export/source files into a controlled workspace
2. Normalize:
   - parse source formats into consistent JSONL/NDJSON records
3. Canonicalize:
   - map normalized records to shared canonical schemas
4. Derive Views:
   - build app-facing and analysis-facing view datasets
5. Validate:
   - run schema and integrity checks
6. Publish/Internal Use:
   - consume views/canonical outputs in local tools or services

## Data Collection Approach

- The framework supports two broad inputs:
  - connector pulls (API/scraped/exported app data)
  - full/manual exports (archive files, takeout bundles, mailbox dumps, etc.)
- Collection scripts stage data first, then normalize.
- Source metadata is tracked in manifests for repeatable runs.

## Public Template Scope

Included here:

- contracts
- schemas
- pipeline scripts
- empty scaffold directories for local runtime

Excluded here:

- personal export files
- staged source artifacts
- normalized/canonical/view outputs derived from personal data
- runtime sqlite/db state and logs

## Privacy and Sanitization Rules

- Never commit raw exports or account-specific data.
- Never commit runtime DB files (`*.db`, `*.db-wal`, `*.db-shm`).
- Keep credentials in environment variables, not files.
- Replace machine-specific absolute paths with project-relative or placeholder paths before sharing.

## Suggested Local Run Pattern

1. Place local source files in ignored staging directories.
2. Run staging + normalization scripts.
3. Build canonical outputs and derived views.
4. Validate outputs against schemas.
5. Share only code/contracts/docs, not generated personal datasets.
