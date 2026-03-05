#!/usr/bin/env python3
"""Build a manifest of connector exports that should be re-run because raw files are missing."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build re-export manifest from source index")
    parser.add_argument("--source-index", default="/path/to/data-connect/meta/raw/source_index.jsonl")
    parser.add_argument("--source-root", default="/path/to/data-connect/personal-server/data")
    parser.add_argument("--output", default="/path/to/data-connect/meta/manifests/reexport_manifest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_index = load_jsonl(Path(args.source_index))
    source_root = Path(args.source_root)

    missing_rows = []
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"source": None, "scopes": set(), "files": [], "latest_collected_at": None}
    )

    for row in source_index:
        rel = row.get("source_file")
        if not rel:
            continue
        if (source_root / rel).exists():
            continue
        missing_rows.append(row)
        platform = row.get("platform") or "unknown"
        item = grouped[platform]
        item["source"] = platform
        item["scopes"].add(row.get("scope"))
        item["files"].append(rel)
        collected_at = row.get("collected_at")
        if collected_at and (item["latest_collected_at"] is None or collected_at > item["latest_collected_at"]):
            item["latest_collected_at"] = collected_at

    manifest = {
        "generated_at": now_iso(),
        "source_root": str(source_root),
        "missing_raw_files": len(missing_rows),
        "sources": [
            {
                "source": item["source"],
                "scopes": sorted(scope for scope in item["scopes"] if scope),
                "latest_collected_at": item["latest_collected_at"],
                "file_count": len(item["files"]),
                "files": sorted(item["files"]),
            }
            for _, item in sorted(grouped.items())
        ],
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
