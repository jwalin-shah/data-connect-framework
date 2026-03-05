CREATE OR REPLACE VIEW meta_timeline_view AS
SELECT
  id,
  occurred_at,
  event_type,
  title,
  description,
  platform,
  source_file
FROM meta_events
ORDER BY occurred_at DESC NULLS LAST, id DESC;

CREATE OR REPLACE VIEW meta_source_overview_view AS
WITH artifact_counts AS (
  SELECT
    COALESCE(platform, 'unknown') AS source,
    COUNT(*) AS artifact_count
  FROM meta_artifacts
  GROUP BY COALESCE(platform, 'unknown')
),
event_base AS (
  SELECT
    COALESCE(platform, 'unknown') AS source,
    event_type,
    COUNT(*) AS cnt,
    MAX(occurred_at) AS max_occurred_at
  FROM meta_events
  GROUP BY COALESCE(platform, 'unknown'), event_type
),
event_counts AS (
  SELECT
    source,
    SUM(cnt)::bigint AS event_count,
    MAX(max_occurred_at) AS last_imported_at,
    jsonb_object_agg(event_type, cnt ORDER BY cnt DESC) FILTER (WHERE event_type IS NOT NULL) AS top_event_types
  FROM event_base
  GROUP BY source
),
sources AS (
  SELECT source FROM artifact_counts
  UNION
  SELECT source FROM event_counts
)
SELECT
  s.source,
  COALESCE(a.artifact_count, 0) AS artifact_count,
  COALESCE(e.event_count, 0) AS event_count,
  e.last_imported_at,
  e.top_event_types
FROM sources s
LEFT JOIN artifact_counts a ON a.source = s.source
LEFT JOIN event_counts e ON e.source = s.source
ORDER BY s.source;
