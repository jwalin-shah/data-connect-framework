#!/usr/bin/env python3
"""Load canonical NDJSON files into Postgres tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


TABLES = {
    "artifacts": {
        "table": "meta_artifacts",
        "columns": ["id", "type", "artifact_type", "source_file", "platform", "scope", "run_id", "raw_payload"],
    },
    "entities": {
        "table": "meta_entities",
        "columns": ["id", "type", "entity_type", "canonical_name", "platform", "attributes", "first_seen_at", "last_seen_at"],
    },
    "events": {
        "table": "meta_events",
        "columns": ["id", "type", "event_type", "occurred_at", "title", "description", "platform", "source_file", "raw_payload"],
    },
    "relationships": {
        "table": "meta_relationships",
        "columns": ["id", "type", "predicate", "subject_entity_id", "object_entity_id", "valid_from", "valid_to", "raw_payload"],
    },
}


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def row_values(kind: str, row: Dict[str, Any]) -> List[Any]:
    if kind == "artifacts":
        return [
            row.get("id"),
            row.get("type"),
            row.get("artifact_type"),
            row.get("source_file"),
            row.get("platform"),
            row.get("scope"),
            row.get("run_id"),
            json.dumps(row.get("raw_payload", {})),
        ]
    if kind == "entities":
        return [
            row.get("id"),
            row.get("type"),
            row.get("entity_type"),
            row.get("canonical_name"),
            row.get("platform"),
            json.dumps(row.get("attributes", {})),
            row.get("first_seen_at"),
            row.get("last_seen_at"),
        ]
    if kind == "events":
        return [
            row.get("id"),
            row.get("type"),
            row.get("event_type"),
            row.get("occurred_at"),
            row.get("title"),
            row.get("description"),
            row.get("platform"),
            row.get("source_file"),
            json.dumps(row.get("raw_payload", {})),
        ]
    return [
        row.get("id"),
        row.get("type"),
        row.get("predicate"),
        row.get("subject_entity_id"),
        row.get("object_entity_id"),
        row.get("valid_from"),
        row.get("valid_to"),
        json.dumps(row.get("raw_payload", {})),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load canonical NDJSON into Postgres")
    parser.add_argument("--canonical-root", default="/path/to/data-connect/meta/canonical")
    parser.add_argument("--database-url", default=None, help="Postgres connection string")
    parser.add_argument(
        "--schema-file",
        default="/path/to/data-connect/meta/schemas/postgres-schema.sql",
        help="SQL file used to initialize tables",
    )
    return parser.parse_args()


def main() -> int:
    if psycopg is None:
        print("psycopg is not installed. Install it with `python3 -m pip install psycopg[binary]`.", file=sys.stderr)
        return 1

    args = parse_args()
    if not args.database_url:
        print("--database-url is required", file=sys.stderr)
        return 1

    canonical_root = Path(args.canonical_root)
    schema_sql = Path(args.schema_file).read_text(encoding="utf-8")

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            for kind, table_meta in TABLES.items():
                rows = load_jsonl(canonical_root / f"{kind}.ndjson")
                columns = table_meta["columns"]
                placeholders = ", ".join(["%s"] * len(columns))
                column_list = ", ".join(columns)
                updates = ", ".join(f"{column}=EXCLUDED.{column}" for column in columns[1:])
                sql = f"""
                    INSERT INTO {table_meta['table']} ({column_list})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET
                    {updates}
                """
                values = [row_values(kind, row) for row in rows]
                if values:
                    cur.executemany(sql, values)
        conn.commit()

    print(json.dumps({"loaded_from": str(canonical_root), "tables": list(TABLES.keys())}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
