#!/usr/bin/env python3
"""Show a complete snapshot of personal data coverage across all sources.

Run:
    python data_profile.py
    python data_profile.py --canonical   # also show canonical event breakdown
    python data_profile.py --compare     # show connector-only vs full (with full-export merged)
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

CONNECTOR_NORMALIZED = Path("/path/to/data-connect/meta/lanes/connector/normalized")
FULL_EXPORT_NORMALIZED = Path("/path/to/data-connect/meta/lanes/full-export/normalized")
CONNECTOR_CANONICAL = Path("/path/to/data-connect/meta/lanes/connector/canonical")
VIEWS_DIR = Path("/path/to/data-connect/meta/lanes/connector/views")
SQLITE_PATH = Path("/path/to/data-connect/runtime/indexes/connector.sqlite")
WATERMARK_DIR = Path("/path/to/data-connect/meta/working/merge_watermarks")


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass


def _collection_method(rec: dict, lane: str) -> str:
    meta = rec.get("meta", {}) or {}
    if meta.get("collection_method"):
        return meta["collection_method"]
    src_version = (rec.get("source") or {}).get("version", "") or ""
    if "playwright" in src_version:
        return "playwright"
    if lane == "full_export":
        return "full_export"
    if lane == "local":
        return "local_device"
    return "connector"


def profile_lane(root: Path, lane: str) -> list[dict]:
    """Return one row per (lane, kind, platform) with counts + date range."""
    rows = []
    if not root.exists():
        return rows
    for kind_file in sorted(root.glob("*.jsonl")):
        kind = kind_file.stem
        buckets: dict[str, dict] = {}
        for rec in _iter_jsonl(kind_file):
            meta = rec.get("meta", {}) or {}
            platform = meta.get("platform") or "unknown"
            method = _collection_method(rec, lane)
            key = f"{platform}|{method}"
            if key not in buckets:
                buckets[key] = {"platform": platform, "method": method, "count": 0, "dates": []}
            buckets[key]["count"] += 1
            for df in ("created_at", "observed_at", "occurred_at"):
                v = rec.get(df)
                if v and isinstance(v, str) and len(v) >= 10:
                    buckets[key]["dates"].append(v[:10])
                    break
        for b in buckets.values():
            dates = sorted(d for d in b["dates"] if d)
            rows.append({
                "lane": lane,
                "kind": kind,
                "platform": b["platform"],
                "method": b["method"],
                "count": b["count"],
                "earliest": dates[0] if dates else "—",
                "latest": dates[-1] if dates else "—",
            })
    return rows


def print_normalized_summary(rows: list[dict]) -> None:
    # Aggregate by platform across lanes
    by_platform: dict[str, dict] = {}
    for r in rows:
        p = r["platform"]
        if p not in by_platform:
            by_platform[p] = {"count": 0, "methods": Counter(), "earliest": [], "latest": [], "kinds": Counter()}
        by_platform[p]["count"] += r["count"]
        by_platform[p]["methods"][r["method"]] += r["count"]
        by_platform[p]["kinds"][r["kind"]] += r["count"]
        if r["earliest"] != "—":
            by_platform[p]["earliest"].append(r["earliest"])
        if r["latest"] != "—":
            by_platform[p]["latest"].append(r["latest"])

    col = "{:<22} {:>10}  {:<16} {:<12} {:<12}  {}"
    print(col.format("PLATFORM", "RECORDS", "SOURCE METHOD", "EARLIEST", "LATEST", "TYPES"))
    print("─" * 100)
    for plat, d in sorted(by_platform.items(), key=lambda x: -x[1]["count"]):
        method = d["methods"].most_common(1)[0][0] if d["methods"] else "?"
        # show all methods if mixed
        if len(d["methods"]) > 1:
            method = "+".join(k for k, _ in d["methods"].most_common())
        earliest = min(d["earliest"]) if d["earliest"] else "—"
        latest = max(d["latest"]) if d["latest"] else "—"
        kinds = "  ".join(f"{k}:{v:,}" for k, v in d["kinds"].most_common(4))
        print(col.format(plat, f"{d['count']:,}", method, earliest, latest, kinds))


def print_canonical_summary(canonical_dir: Path) -> None:
    events_path = canonical_dir / "events.ndjson"
    entities_path = canonical_dir / "entities.ndjson"

    if not events_path.exists():
        print(f"  events.ndjson not found at {canonical_dir}")
        return

    event_types: Counter = Counter()
    event_platforms: Counter = Counter()
    event_methods: Counter = Counter()
    for r in _iter_jsonl(events_path):
        event_types[r.get("event_type", "?")] += 1
        event_platforms[r.get("platform", "?")] += 1
        event_methods[_collection_method(r.get("raw_payload", {}) or {}, "canonical")] += 1

    entity_types: Counter = Counter()
    for r in _iter_jsonl(entities_path):
        entity_types[r.get("entity_type", "?")] += 1

    total_events = sum(event_types.values())
    total_entities = sum(entity_types.values())
    print(f"  events.ndjson   : {total_events:>10,} events")
    print(f"  entities.ndjson : {total_entities:>10,} entities")
    print()
    print(f"  {'EVENT TYPE':<32} {'COUNT':>8}  PLATFORM")
    print(f"  {'─'*32} {'─'*8}  {'─'*12}")
    for et, cnt in event_types.most_common(20):
        top_platform = event_platforms.most_common(1)[0][0] if event_platforms else "?"
        print(f"  {et:<32} {cnt:>8,}")
    print()
    print(f"  {'ENTITY TYPE':<32} {'COUNT':>8}")
    print(f"  {'─'*32} {'─'*8}")
    for et, cnt in entity_types.most_common(10):
        print(f"  {et:<32} {cnt:>8,}")


def print_sqlite_summary() -> None:
    if not SQLITE_PATH.exists():
        print(f"  SQLite not found: {SQLITE_PATH}")
        return
    try:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        rows = conn.execute(
            "SELECT platform, COUNT(*) as n FROM events GROUP BY platform ORDER BY n DESC"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        conn.close()
        print(f"  Total events in index: {total:,}")
        print()
        print(f"  {'PLATFORM':<22} {'EVENTS':>10}")
        print(f"  {'─'*22} {'─'*10}")
        for plat, cnt in rows:
            print(f"  {(plat or '?'):<22} {cnt:>10,}")
    except Exception as e:
        print(f"  Error reading SQLite: {e}")


def print_gap_analysis(connector_rows: list[dict], full_export_rows: list[dict]) -> None:
    """Show what's in full-export that hasn't been merged into connector yet."""
    connector_platforms: set[str] = {r["platform"] for r in connector_rows}
    full_export_only: list[dict] = []

    by_platform_fe: dict[str, int] = {}
    by_platform_conn: dict[str, int] = {}

    for r in full_export_rows:
        by_platform_fe[r["platform"]] = by_platform_fe.get(r["platform"], 0) + r["count"]
    for r in connector_rows:
        by_platform_conn[r["platform"]] = by_platform_conn.get(r["platform"], 0) + r["count"]

    # Platforms where full-export has significantly more records than connector
    gaps = []
    for plat, fe_count in sorted(by_platform_fe.items(), key=lambda x: -x[1]):
        conn_count = by_platform_conn.get(plat, 0)
        if fe_count > conn_count * 1.1:  # more than 10% more in full-export
            gaps.append((plat, fe_count, conn_count, fe_count - conn_count))

    if not gaps:
        print("  No significant gaps — full-export data appears fully merged.")
        return

    print(f"  {'PLATFORM':<22} {'FULL-EXPORT':>12} {'IN-CONNECTOR':>13} {'MISSING':>10}")
    print(f"  {'─'*22} {'─'*12} {'─'*13} {'─'*10}")
    for plat, fe, conn, missing in gaps:
        print(f"  {plat:<22} {fe:>12,} {conn:>13,} {missing:>10,}")


def print_watermarks() -> None:
    if not WATERMARK_DIR.exists():
        print("  No watermarks directory found.")
        return
    print(f"  {'WATERMARK':<45} {'OFFSET':>12}  UPDATED")
    print(f"  {'─'*45} {'─'*12}  {'─'*20}")
    for wf in sorted(WATERMARK_DIR.glob("*.json")):
        try:
            data = json.loads(wf.read_text())
            offset = data.get("offset", 0)
            updated = data.get("updated_at", "?")
            print(f"  {wf.stem:<45} {offset:>12,}  {updated}")
        except Exception:
            print(f"  {wf.stem:<45} (error reading)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Personal data coverage profile")
    parser.add_argument("--canonical", action="store_true", help="Show canonical layer breakdown")
    parser.add_argument("--sqlite", action="store_true", help="Show SQLite index breakdown")
    parser.add_argument("--gaps", action="store_true", help="Show gaps between full-export and connector")
    parser.add_argument("--watermarks", action="store_true", help="Show watermark states")
    parser.add_argument("--all", dest="show_all", action="store_true", help="Show everything")
    args = parser.parse_args()

    show_all = args.show_all

    print()
    print("=" * 100)
    print("  PERSONAL DATA COVERAGE PROFILE")
    print("=" * 100)

    print("\n── CONNECTOR LANE (normalized, ready for canonical build) ──────────────────────────────────────")
    connector_rows = profile_lane(CONNECTOR_NORMALIZED, "connector")
    if connector_rows:
        print_normalized_summary(connector_rows)
    else:
        print("  (empty)")

    print("\n── FULL-EXPORT LANE (downloaded archives, merge into connector via normalize_connector_exports) ─")
    full_export_rows = profile_lane(FULL_EXPORT_NORMALIZED, "full_export")
    if full_export_rows:
        print_normalized_summary(full_export_rows)
    else:
        print("  (empty)")

    total_connector = sum(r["count"] for r in connector_rows)
    total_full_export = sum(r["count"] for r in full_export_rows)
    print(f"\n  Connector total : {total_connector:,} records")
    print(f"  Full-export total: {total_full_export:,} records (partially overlaps with connector after merge)")

    if args.gaps or show_all:
        print("\n── MERGE GAPS (full-export records not yet in connector) ────────────────────────────────────────")
        print_gap_analysis(connector_rows, full_export_rows)

    if args.canonical or show_all:
        print("\n── CANONICAL LAYER ──────────────────────────────────────────────────────────────────────────────")
        print_canonical_summary(CONNECTOR_CANONICAL)

    if args.sqlite or show_all:
        print("\n── SQLITE INDEX ─────────────────────────────────────────────────────────────────────────────────")
        print_sqlite_summary()

    if args.watermarks or show_all:
        print("\n── WATERMARKS (byte offsets tracking incremental progress) ──────────────────────────────────────")
        print_watermarks()

    print()


if __name__ == "__main__":
    main()
