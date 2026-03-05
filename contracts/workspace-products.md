# Workspace Products Contract

The Context SDK may consume only declared workspace products from `data-connect`.

Required lane products:

- `canonical/artifacts.ndjson`
- `canonical/entities.ndjson`
- `canonical/events.ndjson`
- `canonical/relationships.ndjson`
- `canonical/validation_report.json`
- `views/timeline.jsonl`
- `views/source_overview.jsonl`
- `views/entity_profiles.jsonl`
- `views/interest_profiles.jsonl`
- `views/behavior_profiles.jsonl`
- `views/knowledge_profiles.jsonl`
- `views/day_contexts.jsonl`
- `views/thread_summaries.jsonl`
- `views/project_contexts.jsonl`
- `manifests/lane_build_manifest.json`
- `manifests/source_freshness.json`
- `manifests/source_health.json`

Forbidden for normal query execution:

- `raw/`
- `working/`
- arbitrary export files
- `personal-server/data/`
