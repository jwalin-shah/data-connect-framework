# Context SDK API Contract

Primary public API:

```python
get_context_bundle(task_type: str, query: str, privacy_mode: str) -> ContextBundleResult
```

Secondary low-level tools:

- `search_messages`
- `search_events`
- `get_thread`
- `get_day_context`
- `get_entity_profile`
- `get_interest_profile`
- `get_project_context`
- `get_source_health`

All responses include:

- `query`
- `tool`
- `results`
- `result_count`
- `privacy_mode`
- `redaction_applied`
- `sources_used`
- `warnings`
- `data_freshness`
