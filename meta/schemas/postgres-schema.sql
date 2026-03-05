CREATE TABLE IF NOT EXISTS meta_artifacts (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  artifact_type TEXT NOT NULL,
  source_file TEXT NOT NULL,
  platform TEXT,
  scope TEXT,
  run_id TEXT,
  raw_payload JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS meta_entities (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  canonical_name TEXT,
  platform TEXT,
  attributes JSONB DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS meta_events (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  event_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ,
  title TEXT,
  description TEXT,
  platform TEXT,
  source_file TEXT,
  raw_payload JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS meta_relationships (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  predicate TEXT NOT NULL,
  subject_entity_id TEXT NOT NULL,
  object_entity_id TEXT NOT NULL,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  raw_payload JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_meta_artifacts_platform ON meta_artifacts (platform);
CREATE INDEX IF NOT EXISTS idx_meta_entities_type ON meta_entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_meta_entities_platform ON meta_entities (platform);
CREATE INDEX IF NOT EXISTS idx_meta_events_type ON meta_events (event_type);
CREATE INDEX IF NOT EXISTS idx_meta_events_platform ON meta_events (platform);
CREATE INDEX IF NOT EXISTS idx_meta_events_occurred_at ON meta_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_meta_relationships_predicate ON meta_relationships (predicate);
CREATE INDEX IF NOT EXISTS idx_meta_relationships_subject ON meta_relationships (subject_entity_id);
CREATE INDEX IF NOT EXISTS idx_meta_relationships_object ON meta_relationships (object_entity_id);
