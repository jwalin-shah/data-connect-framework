# Connector Artifacts Contract

Connectors and local/full-export adapters emit source-shaped artifacts plus a manifest entry.

Required fields:

- `connector_id`
- `platform`
- `scope`
- `source_type`
- `run_id`
- `produced_at`
- `schema_version`
- `payload_path`
- `checksum_sha256`

Rules:

- source-shaped only
- no privacy policy
- no cross-source joins
- no task-oriented context semantics
