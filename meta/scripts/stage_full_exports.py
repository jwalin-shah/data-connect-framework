#!/usr/bin/env python3
"""Stage full-account exports into normalize_exports-compatible JSON inputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from zipfile import ZipFile


META_ROOT = Path("/path/to/data-connect/meta")
FULL_EXPORTS_ROOT = META_ROOT / "raw" / "full-exports"
STAGED_ROOT = META_ROOT / "working" / "staged-sources" / "full-export"


def write_payload(platform: str, scope: str, name: str, payload: dict) -> Path:
    out_dir = STAGED_ROOT / platform / scope
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return out_path


def read_csv_rows(path: Path, skip_prelude_lines: int = 0) -> list[dict[str, str]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if skip_prelude_lines:
        lines = lines[skip_prelude_lines:]
    reader = csv.DictReader(lines)
    return list(reader)


def stage_claude() -> list[Path]:
    root = META_ROOT / "working" / "staging" / "full-exports" / "claude"
    outputs: list[Path] = []
    if not root.exists():
        return outputs

    mappings = {
        "conversations": "conversations_full_export",
        "memories": "memories_full_export",
        "projects": "projects_full_export",
        "users": "users_full_export",
    }
    for stem, scope in mappings.items():
        path = root / f"{stem}.json"
        if not path.exists():
            continue
        payload = {
            "collectedAt": path.stat().st_mtime_ns,
            "version": "1.0",
            "source": "claude_full_export",
            "data": json.loads(path.read_text(encoding="utf-8")),
        }
        outputs.append(write_payload("claude", scope, "2026-03-02", payload))
    return outputs


def _find_linkedin_zip() -> Path | None:
    """Return the most recent LinkedIn export zip, preferring Downloads over raw."""
    candidates = [
        Path("/path/to/Downloads/Complete_LinkedInDataExport_03-03-2026.zip.zip"),
        FULL_EXPORTS_ROOT / "linkedin-basic-export" / "Basic_LinkedInDataExport_03-02-2026.zip.zip",
    ]
    for p in candidates:
        if p.exists():
            return p
    # Fallback: any zip in the raw linkedin dir
    raw_dir = FULL_EXPORTS_ROOT / "linkedin-basic-export"
    if raw_dir.exists():
        zips = sorted(raw_dir.glob("*.zip"))
        if zips:
            return zips[-1]
    return None


def _read_csv_from_zip(archive: ZipFile, name: str, skip_lines: int = 0) -> list[dict]:
    try:
        raw = archive.read(name).decode("utf-8-sig", errors="replace")
        lines = raw.splitlines()
        if skip_lines:
            lines = lines[skip_lines:]
        return list(csv.DictReader(lines))
    except KeyError:
        return []


def stage_linkedin() -> list[Path]:
    zip_path = _find_linkedin_zip()
    outputs: list[Path] = []
    if zip_path is None:
        return outputs

    collected_at = zip_path.stat().st_mtime_ns

    with ZipFile(zip_path) as archive:
        all_names = archive.namelist()

        # Single-file scopes
        single_scopes = {
            "Profile.csv": ("profile_basic", 0),
            "Positions.csv": ("positions_basic", 0),
            "Education.csv": ("education_basic", 0),
            "Skills.csv": ("skills_basic", 0),
            "messages.csv": ("messages_basic", 0),
            "Invitations.csv": ("invitations_basic", 0),
            "Jobs/Saved Jobs.csv": ("saved_jobs_basic", 0),
            "PhoneNumbers.csv": ("phone_numbers_basic", 0),
            "Email Addresses.csv": ("email_addresses_basic", 0),
            "Connections.csv": ("connections_basic", 2),
            "SearchQueries.csv": ("search_queries_basic", 0),
            "Reactions.csv": ("reactions_basic", 0),
            "Comments.csv": ("comments_basic", 0),
            "Learning.csv": ("learning_basic", 0),
        }
        for rel_path, (scope, skip_lines) in single_scopes.items():
            rows = _read_csv_from_zip(archive, rel_path, skip_lines)
            if not rows:
                continue
            payload = {"collectedAt": collected_at, "version": "1.0",
                       "source": "linkedin_basic_export", "table": rel_path, "data": rows}
            outputs.append(write_payload("linkedin", scope, rel_path.split("/")[-1].lower().replace(" ", "_").replace(".csv", ""), payload))

        # Paginated Job Applications — combine all pages into one payload
        job_app_files = sorted(n for n in all_names if n.startswith("Jobs/Job Applications") and n.endswith(".csv"))
        if job_app_files:
            all_job_rows: list[dict] = []
            for name in job_app_files:
                all_job_rows.extend(_read_csv_from_zip(archive, name))
            payload = {"collectedAt": collected_at, "version": "1.0",
                       "source": "linkedin_basic_export", "table": "Jobs/Job Applications (all pages)",
                       "data": all_job_rows}
            outputs.append(write_payload("linkedin", "job_applications_basic", "job_applications", payload))

        manifest = {"collectedAt": collected_at, "version": "1.0",
                    "source": "linkedin_basic_export",
                    "data": {"tables": sorted(all_names), "zip": str(zip_path)}}
        outputs.append(write_payload("linkedin", "export_manifest_basic", "manifest", manifest))

    return outputs


def stage_spotify() -> list[Path]:
    root = FULL_EXPORTS_ROOT / "spotify-full-export" / "extended-streaming-history"
    outputs: list[Path] = []
    if not root.exists():
        return outputs

    for path in sorted(root.glob("*.json")):
        payload = {
            "collectedAt": path.stat().st_mtime_ns,
            "version": "1.0",
            "source": "spotify_extended_streaming_history",
            "file": path.name,
            "data": json.loads(path.read_text(encoding="utf-8")),
        }
        outputs.append(write_payload("spotify", "extended_streaming_history", path.stem, payload))
    return outputs


def stage_google_takeout() -> list[Path]:
    root = FULL_EXPORTS_ROOT / "google-takeout-full-export-excluding-drive-photos" / "source"
    outputs: list[Path] = []
    if not root.exists():
        return outputs

    for path in sorted(root.glob("*.zip")):
        with ZipFile(path) as archive:
            file_entries = [name for name in archive.namelist() if not name.endswith("/")]
        top_products = {}
        for entry in file_entries:
            parts = [part for part in entry.split("/") if part]
            product = parts[1] if len(parts) > 1 and parts[0] == "Takeout" else parts[0]
            top_products[product] = top_products.get(product, 0) + 1
        payload = {
            "collectedAt": path.stat().st_mtime_ns,
            "version": "1.0",
            "source": "google_takeout_full_export",
            "archive_name": path.name,
            "data": {
                "file_count": len(file_entries),
                "top_products": top_products,
                "sample_entries": file_entries[:200],
            },
        }
        outputs.append(write_payload("google", "takeout_inventory", path.stem, payload))
    return outputs


def main() -> int:
    STAGED_ROOT.mkdir(parents=True, exist_ok=True)
    outputs = []
    outputs.extend(stage_claude())
    outputs.extend(stage_linkedin())
    outputs.extend(stage_spotify())
    outputs.extend(stage_google_takeout())
    print(json.dumps({"staged_files": [str(path) for path in outputs]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
