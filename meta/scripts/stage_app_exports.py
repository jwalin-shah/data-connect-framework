#!/usr/bin/env python3
"""Stage app exported_data runs into meta working trees for connector/local lanes."""

from __future__ import annotations

import argparse
import fcntl
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple


APP_EXPORT_ROOT = Path(
    "/Users/your-username/Library/Application Support/dev.dataconnect/exported_data"
)
STAGED_ROOT = Path("/path/to/data-connect/meta/working/staged-sources")
LOCK_PATH = STAGED_ROOT / ".stage.lock"

LOCAL_PLATFORM_MAP = {
    "claude-local": "claude",
    "gemini-local": "gemini",
    "cursor-local": "cursor",
    "codex-local": "codex",
    "opencode-local": "opencode",
    "kilo-local": "kilo",
}

PLAIN_SCOPE_MAP = {
    "github": (
        "github",
        {"profile": "profile", "repositories": "repositories", "starred": "starred"},
    ),
}


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def iter_export_files(root: Path) -> Iterable[Path]:
    for company_dir in sorted(root.iterdir()):
        if not company_dir.is_dir():
            continue
        for name_dir in sorted(company_dir.iterdir()):
            if not name_dir.is_dir():
                continue
            for run_dir in sorted(name_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                json_files = sorted(run_dir.glob("*.json"))
                if json_files:
                    yield json_files[-1]


def write_payload(
    dest_root: Path, platform: str, scope: str, stem: str, payload: Dict[str, Any]
) -> None:
    out_dir = dest_root / platform / scope
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{stem}.json"
    out_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8"
    )


def make_payload(wrapper: Dict[str, Any], data: Any) -> Dict[str, Any]:
    return {
        "data": data,
        "platform": wrapper.get("content", {}).get("platform") or wrapper.get("name"),
        "company": wrapper.get("company"),
        "version": wrapper.get("content", {}).get("version"),
        "exportedAt": wrapper.get("content", {}).get("exportedAt"),
        "timestamp": wrapper.get("content", {}).get("timestamp")
        or wrapper.get("timestamp"),
        "exportSummary": wrapper.get("content", {}).get("exportSummary"),
        "source": "app_exported_data",
        "runID": wrapper.get("runID"),
    }


def stage_local_export(
    dest_root: Path, wrapper: Dict[str, Any], source_file: Path
) -> int:
    platform_id = source_file.stem.rsplit("_", 1)[0]
    platform = LOCAL_PLATFORM_MAP.get(platform_id)
    if not platform:
        return 0

    count = 0
    content = wrapper.get("content", {})
    for key, value in content.items():
        if "." not in key:
            continue
        key_platform, scope = key.split(".", 1)
        if key_platform != platform:
            continue
        write_payload(
            dest_root, platform, scope, source_file.stem, make_payload(wrapper, value)
        )
        count += 1
    return count


def stage_connector_export(
    dest_root: Path, wrapper: Dict[str, Any], source_file: Path
) -> int:
    content = wrapper.get("content", {})
    count = 0
    found_scopes = False
    for key, value in content.items():
        if "." not in key:
            continue
        platform, scope = key.split(".", 1)
        write_payload(
            dest_root, platform, scope, source_file.stem, make_payload(wrapper, value)
        )
        count += 1
        found_scopes = True

    platform_name = (
        str(content.get("platform") or wrapper.get("name") or source_file.stem)
        .strip()
        .lower()
        .replace(" ", "-")
    )
    plain_scope_config = PLAIN_SCOPE_MAP.get(platform_name)
    if plain_scope_config:
        platform, scope_map = plain_scope_config
        for key, scope in scope_map.items():
            if key not in content:
                continue
            write_payload(
                dest_root,
                platform,
                scope,
                f"{source_file.stem}-{scope}",
                make_payload(wrapper, content[key]),
            )
            count += 1
            found_scopes = True

    # Preserve failure-only exports like LinkedIn profile scrape failures.
    if not found_scopes:
        platform = {
            "chatgpt": "chatgpt",
            "claude": "claude",
            "linkedin": "linkedin",
            "spotify": "spotify",
            "instagram": "instagram",
            "github": "github",
            "youtube": "youtube",
        }.get(platform_name)
        if platform:
            write_payload(
                dest_root,
                platform,
                "profile",
                source_file.stem,
                make_payload(wrapper, content),
            )
            count += 1
    return count


def stage(source_root: Path, staged_root: Path) -> Dict[str, int]:
    connector_root = staged_root / "connector"
    local_root = staged_root / "local"
    reset_dir(connector_root)
    reset_dir(local_root)

    staged_counts = {"connector_files": 0, "local_files": 0, "source_runs": 0}
    for src_path in iter_export_files(source_root):
        wrapper = json.loads(src_path.read_text(encoding="utf-8"))
        staged_counts["source_runs"] += 1
        if src_path.parts[-4] == "Local":
            staged_counts["local_files"] += stage_local_export(
                local_root, wrapper, src_path
            )
        else:
            staged_counts["connector_files"] += stage_connector_export(
                connector_root, wrapper, src_path
            )
    return staged_counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=APP_EXPORT_ROOT)
    parser.add_argument("--staged-root", type=Path, default=STAGED_ROOT)
    args = parser.parse_args()

    args.staged_root.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        counts = stage(args.source_root, args.staged_root)
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
