#!/usr/bin/env python3
"""Initialize the meta workspace folder structure for raw, working, canonical, and view layers."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("/path/to/data-connect/meta")

DIRECTORIES = [
    "raw/archives",
    "raw/source-index",
    "working/extracted",
    "working/staging",
    "normalized",
    "canonical",
    "views",
    "schemas",
    "graph",
    "manifests",
    "scripts",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_if_missing(path: Path, content: str) -> None:
    if path.exists():
        return
    path.write_text(content, encoding="utf-8")


def init_workspace(root: Path) -> dict:
    for rel_path in DIRECTORIES:
        (root / rel_path).mkdir(parents=True, exist_ok=True)

    write_if_missing(
        root / "raw" / "README.md",
        "# Raw Layer\n\nKeep original exports here. Prefer the original zip/tar as the immutable source artifact.\n",
    )
    write_if_missing(
        root / "working" / "README.md",
        "# Working Layer\n\nUse for extracted archives and temporary parse artifacts. Safe to delete and rebuild.\n",
    )
    write_if_missing(
        root / "views" / "README.md",
        "# View Layer\n\nApp-facing derived datasets for timeline, interests, people, projects, and source summaries.\n",
    )
    write_if_missing(
        root / "schemas" / "README.md",
        "# Schemas\n\nContracts for canonical records and user-facing view models.\n",
    )

    manifest = {
        "initialized_at": now_iso(),
        "root": str(root),
        "directories": DIRECTORIES,
    }
    (root / "manifests" / "workspace_layout.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize the meta workspace layout")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="Meta workspace root")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = init_workspace(Path(args.root))
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
