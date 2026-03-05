#!/usr/bin/env python3
"""Generate a source audit manifest for the Data Connect app."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path("/path/to/data-connect/meta")
RAW_ROOT = ROOT / "raw"
FULL_EXPORTS_ROOT = RAW_ROOT / "full-exports"
WORKING_ROOT = ROOT / "working" / "staging" / "full-exports"
MANIFESTS_ROOT = ROOT / "manifests"
LANES_ROOT = ROOT / "lanes"
SOURCE_INDEX_PATH = RAW_ROOT / "source_index.jsonl"
FAILURES_PATH = ROOT / "normalized" / "failures.jsonl"
CODEX_ROOT = Path("/path/to/codex")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def list_tree(root: Path, depth: int = 2, limit: int = 24) -> list[str]:
    if not root.exists():
        return []
    results: list[str] = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root)
        if len(rel.parts) > depth:
            continue
        suffix = "/" if path.is_dir() else ""
        results.append(f"{rel.as_posix()}{suffix}")
        if len(results) >= limit:
            break
    return results


def summarize_directory(root: Path) -> dict[str, Any]:
    summary = {
        "exists": root.exists(),
        "path": str(root),
        "file_count": 0,
        "dir_count": 0,
        "total_bytes": 0,
        "top_files": [],
        "tree_preview": list_tree(root),
    }
    if not root.exists():
        return summary

    files: list[tuple[int, str]] = []
    for path in root.rglob("*"):
        if path.is_dir():
            summary["dir_count"] += 1
            continue
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        summary["file_count"] += 1
        summary["total_bytes"] += size
        files.append((size, str(path.relative_to(root))))
    files.sort(reverse=True)
    summary["top_files"] = [
        {"path": rel, "size_bytes": size}
        for size, rel in files[:10]
    ]
    return summary


def build_connector_lane(source_index_rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_index_rows:
        grouped[row.get("platform", "unknown")].append(row)

    lane_sources = {}
    for platform, rows in grouped.items():
        scope_counts = Counter(row.get("scope", "unknown") for row in rows)
        lane_sources[platform] = {
            "artifact_count": len(rows),
            "scopes": dict(scope_counts),
            "source_files": [row.get("source_file") for row in rows[:20]],
        }
    return lane_sources


def build_failures_index(failure_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    failures: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in failure_rows:
        platform = row.get("meta", {}).get("platform", "unknown")
        failures[platform].append(
            {
                "source_file": row.get("meta", {}).get("source_file"),
                "error": row.get("error"),
            }
        )
    return failures


def load_lane_summary(lane_name: str) -> dict[str, Any]:
    lane_root = LANES_ROOT / lane_name
    manifest_path = lane_root / "manifests" / "last_run_manifest.json"
    source_index_path = lane_root / "raw" / "source_index.jsonl"
    if not manifest_path.exists():
        return {"present": False}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_rows = load_jsonl(source_index_path)
    per_platform = Counter(row.get("platform", "unknown") for row in source_rows)
    per_scope = defaultdict(Counter)
    for row in source_rows:
        per_scope[row.get("platform", "unknown")][row.get("scope", "unknown")] += 1
    return {
        "present": True,
        "run_at": manifest.get("run_at"),
        "source_files": manifest.get("source_files"),
        "record_counts": manifest.get("record_counts", {}),
        "platform_files": dict(per_platform),
        "platform_scopes": {platform: dict(counts) for platform, counts in per_scope.items()},
        "output_root": manifest.get("output_root"),
    }


def load_normalized_kind_breakdown(lane_name: str, kind: str) -> dict[str, int]:
    path = LANES_ROOT / lane_name / "normalized" / f"{kind}.jsonl"
    if not path.exists():
        return {}
    counts: Counter[str] = Counter()
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            platform = ((row.get("meta") or {}).get("platform")) or "unknown"
            counts[str(platform)] += 1
    return dict(counts)


def linkedin_export_summary() -> dict[str, Any]:
    root = WORKING_ROOT / "linkedin"
    csv_files = sorted(root.rglob("*.csv"))
    row_counts = []
    for path in csv_files:
        try:
            row_count = count_csv_rows(path)
        except Exception:
            row_count = 0
        row_counts.append(
            {
                "file": str(path.relative_to(root)),
                "rows": row_count,
            }
        )

    top_tables = sorted(row_counts, key=lambda item: item["rows"], reverse=True)[:12]
    total_rows = sum(item["rows"] for item in row_counts)
    return {
        "file_count": len(csv_files),
        "table_count": len(csv_files),
        "row_count": total_rows,
        "top_tables": top_tables,
        "tree_preview": list_tree(root, depth=2, limit=30),
    }


def claude_export_summary() -> dict[str, Any]:
    root = WORKING_ROOT / "claude"
    summary = summarize_directory(root)
    conversations_path = root / "conversations.json"
    memories_path = root / "memories.json"
    projects_path = root / "projects.json"
    counts: dict[str, int] = {}
    for name, path in {
        "conversations": conversations_path,
        "memories": memories_path,
        "projects": projects_path,
    }.items():
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            counts[name] = len(payload) if isinstance(payload, list) else len(payload.keys())
    summary["payload_counts"] = counts
    return summary


def spotify_export_summary() -> dict[str, Any]:
    root = FULL_EXPORTS_ROOT / "spotify-full-export" / "extended-streaming-history"
    json_files = sorted(root.glob("*.json"))
    file_counts = []
    for path in json_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            count = len(payload) if isinstance(payload, list) else len(payload.keys())
        except Exception:
            count = 0
        file_counts.append({"file": path.name, "entries": count})
    return {
        "file_count": len(json_files),
        "files": file_counts,
        "tree_preview": list_tree(root, depth=1, limit=20),
    }


def google_export_summary() -> dict[str, Any]:
    root = FULL_EXPORTS_ROOT / "google-takeout-full-export-excluding-drive-photos" / "source"
    zip_files = sorted(root.glob("*.zip"))
    return {
        "zip_count": len(zip_files),
        "zip_files": [path.name for path in zip_files[:20]],
        "tree_preview": list_tree(root, depth=1, limit=20),
    }


def codex_local_summary() -> dict[str, Any]:
    root = CODEX_ROOT
    summary = summarize_directory(root)
    history_path = root / "history.jsonl"
    session_root = root / "sessions"
    prompt_root = root / "prompts"
    shell_root = root / "shell_snapshots"
    payload_counts: dict[str, int] = {}
    if history_path.exists():
        payload_counts["history_events"] = sum(
            1 for line in history_path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()
        )
    if session_root.exists():
        payload_counts["session_files"] = sum(1 for path in session_root.rglob("*") if path.is_file())
    if prompt_root.exists():
        payload_counts["prompt_files"] = sum(1 for path in prompt_root.rglob("*") if path.is_file())
    if shell_root.exists():
        payload_counts["shell_snapshots"] = sum(1 for path in shell_root.rglob("*") if path.is_file())
    summary["payload_counts"] = payload_counts
    summary["tree_preview"] = list_tree(root, depth=2, limit=24)
    return summary


def build_manifest() -> dict[str, Any]:
    source_index_rows = load_jsonl(SOURCE_INDEX_PATH)
    failure_rows = load_jsonl(FAILURES_PATH)
    connector_lane = build_connector_lane(source_index_rows)
    failures = build_failures_index(failure_rows)
    full_export_lane = load_lane_summary("full-export")
    connector_lane_summary = load_lane_summary("connector")
    local_lane_summary = load_lane_summary("local")
    full_export_activity_breakdown = load_normalized_kind_breakdown("full-export", "activities")

    linkedin_basic = linkedin_export_summary()
    linkedin_connector = connector_lane.get("linkedin", {})
    linkedin_gap = {
        "summary": (
            "The LinkedIn basic export contains substantially more raw data than the current "
            "connector lane. The connector currently has a single profile JSON and recorded a failure."
        ),
        "basic_export_tables": linkedin_basic["table_count"],
        "basic_export_rows": linkedin_basic["row_count"],
        "connector_artifacts": linkedin_connector.get("artifact_count", 0),
        "connector_scopes": linkedin_connector.get("scopes", {}),
        "connector_failures": failures.get("linkedin", []),
        "missing_from_connector_examples": [
            "connections",
            "positions",
            "education",
            "skills",
            "messages",
            "job seeker preferences",
            "saved jobs",
            "certifications",
            "phone numbers",
            "email addresses",
        ],
    }

    overview = {
        "automatic_sync_sources": [
            "ChatGPT connector/API conversations and memories",
            "Claude local connector artifacts",
            "Spotify connector profile and playlists",
            "Gemini local session and config importer",
            "LinkedIn connector profile attempt",
        ],
        "manual_exports_downloaded": [
            "Claude full export zip",
            "LinkedIn basic export zip",
            "Spotify extended streaming history export",
            "Google Takeout export excluding Drive and Photos",
        ],
        "manual_exports_requested": [
            "Instagram full export",
            "Facebook full export",
            "WhatsApp full export or chat export",
            "X / Twitter full archive",
        ],
        "manual_exports_missing": [
            "ChatGPT full account export",
            "Discord export",
            "Reddit export",
            "TikTok export",
            "Snapchat export",
            "Notion export",
        ],
        "full_export_activity_breakdown": full_export_activity_breakdown,
    }

    sources = {
        "chatgpt": {
            "title": "ChatGPT",
            "explanation": (
                "Current normalized ChatGPT data came from the fast connector/API lane, not from an official "
                "full account export. It includes conversations and memories gathered earlier."
            ),
            "collection_scripts": [
                "playwright connector via app runtime",
                "future ChatGPT full export should use a separate full-export staging path",
            ],
            "preferred_collection_order": [
                "official_api",
                "authenticated_web_api_capture",
                "manual_account_export",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "limited_or_internal",
                "recommended_fast_path": "authenticated_web_api_capture",
                "notes": "Current fast path is effectively authenticated web API use after browser login, which is more robust than DOM scraping.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "present",
                    "kind": "connector",
                    "details": "Fast connector/API lane with conversations and memories from an earlier run.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "missing",
                    "kind": "full_export",
                    "details": "No official ChatGPT full export has been downloaded yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "automatic_login_sync",
            "robustness_notes": [
                "Current connector is relatively strong because it calls authenticated backend endpoints after login rather than scraping the rendered chat UI.",
                "Once a full export is downloaded, keep it separate from the fast-sync lane instead of merging assumptions.",
            ],
            "lanes": {
                "connector": connector_lane.get("chatgpt", {}),
                "full_export": {
                    "present": False,
                    "summary": "No official ChatGPT full export has been staged yet.",
                },
            },
            "normalized_status": {
                "connector": connector_lane_summary,
                "local": local_lane_summary,
                "full_export": full_export_lane,
            },
        },
        "claude": {
            "title": "Claude",
            "explanation": (
                "Claude now has both an earlier lightweight local connector lane and a newly staged full export zip "
                "with conversations, memories, projects, and users."
            ),
            "collection_scripts": [
                "npm run import:claude-local",
                "npm run request:claude-export",
                "npm run import:claude-export",
                "npm run watch:claude-export",
            ],
            "preferred_collection_order": [
                "manual_account_export",
                "authenticated_web_api_capture",
                "automatic_local_sync",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "no_public_full_history_api",
                "recommended_fast_path": "authenticated_web_api_capture",
                "notes": "The web connector currently reads visible chat-list state. Full export is still the canonical source for coverage.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "partial",
                    "kind": "local_connector",
                    "details": "Earlier local connector artifacts exist, but they are clearly incomplete versus the full export.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "present",
                    "kind": "full_export",
                    "details": "Official Claude export zip downloaded, staged, and normalized.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "Current web fast sync is still UI-derived and can under-collect if the chat list is virtualized.",
                "Full export is already materially better and should remain canonical.",
            ],
            "lanes": {
                "connector": connector_lane.get("claude", {}),
                "full_export": claude_export_summary(),
            },
            "normalized_status": {
                "full_export_platform_files": full_export_lane.get("platform_files", {}).get("claude", 0),
                "full_export_platform_scopes": full_export_lane.get("platform_scopes", {}).get("claude", {}),
            },
            "coverage_summary": {
                "connector_vs_export": "Connector/local lane is incomplete. Full export currently has 82 conversations and is the authoritative Claude dataset.",
            },
        },
        "linkedin": {
            "title": "LinkedIn",
            "explanation": (
                "LinkedIn currently has a connector lane that only attempted a profile JSON and failed, plus a "
                "separate basic export with many CSV tables. The basic export is materially richer."
            ),
            "collection_scripts": [
                "playwright connector via app runtime",
                "manual LinkedIn basic export download",
            ],
            "preferred_collection_order": [
                "manual_account_export",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "gated_limited",
                "recommended_fast_path": "authenticated_web_api_capture",
                "notes": "Official LinkedIn APIs exist but broad self-serve personal data access is limited. Export remains canonical.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "partial",
                    "kind": "connector",
                    "details": "Connector attempted a profile scrape but failed to extract meaningful profile data.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "present",
                    "kind": "full_export",
                    "details": "Basic LinkedIn export zip downloaded and staged.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "Public profile pages can look superficially similar to authenticated pages, so login detection must be strict.",
                "This source should never rely on DOM scraping alone for canonical coverage.",
            ],
            "lanes": {
                "connector": connector_lane.get("linkedin", {}),
                "full_export": linkedin_basic,
            },
            "comparison": linkedin_gap,
            "normalized_status": {
                "full_export_platform_files": full_export_lane.get("platform_files", {}).get("linkedin", 0),
                "full_export_platform_scopes": full_export_lane.get("platform_scopes", {}).get("linkedin", {}),
            },
        },
        "github": {
            "title": "GitHub",
            "explanation": "GitHub now has an API-first fast sync path for profile, repositories, and starred repositories. This should replace DOM scraping for the normal quick-access case.",
            "collection_scripts": [
                "GitHub API connector via local runtime",
                "fallback: gh auth token if available",
            ],
            "preferred_collection_order": [
                "official_api",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "preferred",
                "recommended_fast_path": "official_api",
                "notes": "Use a GitHub personal access token or gh CLI auth. API coverage is stronger and more stable than UI scraping.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "present",
                    "kind": "api_connector",
                    "details": "GitHub fast sync can use a stored PAT or gh CLI auth token through the official REST API.",
                    "manual_export": False,
                }
            ],
            "canonical_source": "official_api",
            "robustness_notes": [
                "This source should be API-first. Playwright should be kept only as a fallback for narrow UI-only data.",
            ],
            "lanes": {
                "connector": connector_lane.get("github", {}),
            },
        },
        "spotify": {
            "title": "Spotify",
            "explanation": (
                "Spotify has an existing connector lane with profile and playlists, plus a full export lane with "
                "extended streaming history JSON files across multiple years."
            ),
            "collection_scripts": [
                "playwright or connector runtime for profile/playlists",
                "manual Spotify full export download for extended streaming history",
            ],
            "preferred_collection_order": [
                "official_api",
                "manual_account_export",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "available_but_incomplete_for_history",
                "recommended_fast_path": "official_api",
                "notes": "Use official Spotify APIs for profile, playlists, and saved library. Keep the export for extended listening history.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "present",
                    "kind": "connector",
                    "details": "Connector lane has profile and playlists.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "present",
                    "kind": "full_export",
                    "details": "Extended streaming history export downloaded and normalized.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "The current connector uses internal Spotify web-player mechanisms, which are stronger than DOM scraping but still tied to internal client behavior.",
                "A supported official API path should replace the current fast sync for current-state library data.",
            ],
            "lanes": {
                "connector": connector_lane.get("spotify", {}),
                "full_export": spotify_export_summary(),
            },
            "normalized_status": {
                "full_export_platform_files": full_export_lane.get("platform_files", {}).get("spotify", 0),
                "full_export_platform_scopes": full_export_lane.get("platform_scopes", {}).get("spotify", {}),
            },
        },
        "google": {
            "title": "Google",
            "explanation": (
                "Google currently has a full Takeout export set excluding Drive and Photos, plus earlier connector "
                "or local datasets under separate lanes such as Gemini local. The Takeout zip set is staged as raw full export input."
            ),
            "collection_scripts": [
                "npm run watch:google-takeout",
                "npm run import:google-takeout",
                "manual Google Takeout download",
            ],
            "preferred_collection_order": [
                "manual_account_export",
                "official_api",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "strong_for_selected_products",
                "recommended_fast_path": "official_api",
                "notes": "Gmail, People, Calendar, YouTube, and Drive metadata should use official APIs. Takeout remains canonical for broad history.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "present",
                    "kind": "local_import_watch",
                    "details": "Takeout watch/import scripts can stage Google exports automatically after download, but this is still export-driven rather than live login sync.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "present",
                    "kind": "full_export",
                    "details": "Google Takeout export excluding Drive and Photos downloaded and staged.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "Google should be split into Takeout as canonical archive and product-specific fast sync via official APIs.",
                "Do not use Playwright as the default Google fast path when supported APIs already exist.",
            ],
            "lanes": {
                "connector": connector_lane.get("google", {}),
                "full_export": google_export_summary(),
            },
            "normalized_status": {
                "full_export_platform_files": full_export_lane.get("platform_files", {}).get("google", 0),
                "full_export_platform_scopes": full_export_lane.get("platform_scopes", {}).get("google", {}),
            },
        },
        "google-fast-sync": {
            "title": "Google Fast Sync",
            "explanation": "A quick-access Google lane for lightweight current-state data gathered from official Google APIs. This stays separate from Takeout.",
            "collection_scripts": [
                "Google Fast Sync API connector via local runtime",
            ],
            "preferred_collection_order": [
                "official_api",
                "authenticated_web_api_capture",
                "automatic_local_sync",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "preferred",
                "recommended_fast_path": "official_api",
                "notes": "Prioritize Gmail, People, Calendar, YouTube, and Drive metadata APIs before browser automation.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "present",
                    "kind": "api_connector",
                    "details": "Google Fast Sync can call Gmail, People, Calendar, Drive metadata, and YouTube APIs using a stored access token.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "present",
                    "kind": "full_export",
                    "details": "Google Takeout remains the canonical archive for Google data.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "This lane should be API-first, not Playwright-first.",
                "Authenticated capture is the fallback when a useful product surface lacks stable official API coverage.",
            ],
            "lanes": {
                "connector": connector_lane.get("google", {}),
            },
        },
        "instagram": {
            "title": "Instagram",
            "explanation": "Instagram should be tracked separately from Facebook and WhatsApp. Browser automation can capture visible profile state, while Meta account export is the canonical archive.",
            "collection_scripts": [
                "playwright connector via app runtime",
                "manual Instagram export download",
            ],
            "preferred_collection_order": [
                "manual_account_export",
                "official_api",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "limited_account_dependent",
                "recommended_fast_path": "authenticated_web_api_capture",
                "notes": "Official Meta APIs are strongest for eligible business or app-managed surfaces; personal archive coverage still comes from export.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync can capture visible profile and post state through browser automation.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "requested",
                    "kind": "full_export",
                    "details": "Full Instagram export has been requested but is not staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "If browser fast sync is used, prefer captured JSON/API responses over parsing rendered profile markup.",
            ],
            "lanes": {},
        },
        "facebook": {
            "title": "Facebook",
            "explanation": "Facebook should be treated as its own source with separate export and possible connector coverage for profile and visible activity.",
            "collection_scripts": [
                "future facebook connector",
                "manual Facebook export download",
            ],
            "preferred_collection_order": [
                "manual_account_export",
                "official_api",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "limited_account_dependent",
                "recommended_fast_path": "authenticated_web_api_capture",
                "notes": "Use official API only where the account/app permissions make sense. Export remains the broad archive path.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync could target visible profile data, groups, pages, and recent feed-adjacent state if needed.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "requested",
                    "kind": "full_export",
                    "details": "Facebook account export has been requested but is not staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "Treat Facebook as export-first unless a narrow API-backed sync use case is clearly worth maintaining.",
            ],
            "lanes": {},
        },
        "whatsapp": {
            "title": "WhatsApp",
            "explanation": "WhatsApp is distinct from the rest of Meta. Exported chat histories or local-device artifacts are likely more useful than browser automation.",
            "collection_scripts": [
                "future WhatsApp local/device importer",
                "manual WhatsApp chat export",
            ],
            "preferred_collection_order": [
                "manual_account_export",
                "automatic_local_sync",
                "authenticated_web_api_capture",
            ],
            "official_api_status": {
                "status": "not_suitable_for_personal_full_history",
                "recommended_fast_path": "automatic_local_sync",
                "notes": "The official WhatsApp API is business-oriented. Personal history is better handled through device/local artifacts and exports.",
            },
            "access_paths": [
                {
                    "mode": "automatic_local_sync",
                    "status": "planned",
                    "kind": "local_import",
                    "details": "Best quick path is likely local-device artifacts or paired desktop state rather than Playwright.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "requested",
                    "kind": "full_export",
                    "details": "WhatsApp export has been requested or initiated but is not staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "Browser automation should be avoided as the primary WhatsApp strategy.",
            ],
            "lanes": {},
        },
        "x": {
            "title": "X / Twitter",
            "explanation": "X/Twitter now has an API-first fast sync path for profile, bookmarks, likes, and following where your token grants user-context access. The requested archive remains the better history source.",
            "collection_scripts": [
                "X API connector via local runtime",
                "manual X data archive download",
            ],
            "preferred_collection_order": [
                "official_api",
                "manual_account_export",
                "authenticated_web_api_capture",
                "dom_browser_automation",
            ],
            "official_api_status": {
                "status": "available_with_tier_limits",
                "recommended_fast_path": "official_api",
                "notes": "Use the official X API for fast sync where the account tier permits. Keep the archive for full history.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "present",
                    "kind": "api_connector",
                    "details": "Fast sync can use a stored X user-context token for profile, bookmarks, likes, and following.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "requested",
                    "kind": "full_export",
                    "details": "X/Twitter archive has been requested but is not staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "robustness_notes": [
                "A supported API should replace browser scraping here wherever possible.",
            ],
            "lanes": {
                "connector": connector_lane.get("x", {}),
            },
        },
        "discord": {
            "title": "Discord",
            "explanation": "Discord is common enough to track separately. Local app data or API access can be used for quick sync, while account export should be the canonical archive when available.",
            "collection_scripts": [
                "future Discord connector or local importer",
                "manual Discord export download",
            ],
            "access_paths": [
                {
                    "mode": "automatic_local_sync",
                    "status": "planned",
                    "kind": "local_import",
                    "details": "Fast sync is likely best from local desktop state or a scoped API path rather than browser automation.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "missing",
                    "kind": "full_export",
                    "details": "Discord export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "reddit": {
            "title": "Reddit",
            "explanation": "Reddit is useful for post, comment, saved, and message history. API or browser automation can provide fast sync, while export is the better archive.",
            "collection_scripts": [
                "future Reddit API connector",
                "manual Reddit export download",
            ],
            "preferred_collection_order": [
                "official_api",
                "manual_account_export",
                "authenticated_web_api_capture",
            ],
            "official_api_status": {
                "status": "available",
                "recommended_fast_path": "official_api",
                "notes": "Profile, submissions, comments, and saved items should use Reddit API before browser automation.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync can likely target profile, submissions, comments, and saved items.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "missing",
                    "kind": "full_export",
                    "details": "Reddit export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "tiktok": {
            "title": "TikTok",
            "explanation": "TikTok should have a separate fast-sync and export story for profile, likes, watch history, and account archive data.",
            "collection_scripts": [
                "future TikTok connector",
                "manual TikTok export download",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync can target visible profile and current account state if worthwhile.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "missing",
                    "kind": "full_export",
                    "details": "TikTok export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "snapchat": {
            "title": "Snapchat",
            "explanation": "Snapchat data is mostly export-driven. A quick sync lane may be lower value than the account export.",
            "collection_scripts": [
                "future Snapchat connector",
                "manual Snapchat export download",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync is possible but likely lower-value and more fragile than export-driven access.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "missing",
                    "kind": "full_export",
                    "details": "Snapchat export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "notion": {
            "title": "Notion",
            "explanation": "Notion is a high-value personal and work knowledge source. API or export can both be useful depending on the workspace setup.",
            "collection_scripts": [
                "future Notion API connector",
                "manual Notion export",
            ],
            "preferred_collection_order": [
                "official_api",
                "manual_account_export",
                "authenticated_web_api_capture",
            ],
            "official_api_status": {
                "status": "available",
                "recommended_fast_path": "official_api",
                "notes": "Notion should be API-first whenever workspace permissions allow it.",
            },
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync should use the Notion API where possible rather than browser automation.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "missing",
                    "kind": "full_export",
                    "details": "Notion export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "slack": {
            "title": "Slack",
            "explanation": "Slack is high-value but depends heavily on workspace permissions. Exports may not be available to personal users in the same way as consumer apps.",
            "collection_scripts": [
                "future Slack connector",
                "workspace export where permitted",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync would likely use the Slack API rather than browser automation.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "planned",
                    "kind": "full_export",
                    "details": "Slack export depends on workspace/admin permissions and may not be available.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "automatic_login_sync",
            "lanes": {},
        },
        "amazon": {
            "title": "Amazon",
            "explanation": "Amazon order history is a useful personal dataset. Browser automation may be enough for quick sync, but requested reports are better when available.",
            "collection_scripts": [
                "future Amazon connector",
                "manual Amazon data request",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync could target orders, returns, and account basics through browser automation.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "planned",
                    "kind": "full_export",
                    "details": "Amazon data export or order reports are not staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "uber": {
            "title": "Uber",
            "explanation": "Uber ride history is structured and useful. App/API access may provide fast sync while reports or exports provide deeper history.",
            "collection_scripts": [
                "future Uber connector",
                "manual Uber data export",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync could target recent rides, places, and account metadata.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "planned",
                    "kind": "full_export",
                    "details": "Uber export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "lyft": {
            "title": "Lyft",
            "explanation": "Lyft is similar to Uber: ride history and account metadata are useful and structured.",
            "collection_scripts": [
                "future Lyft connector",
                "manual Lyft data export",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync could target current ride and account state plus recent history.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "planned",
                    "kind": "full_export",
                    "details": "Lyft export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "paypal": {
            "title": "PayPal",
            "explanation": "PayPal transaction history is high-value and should be tracked as a financial source separate from shopping apps.",
            "collection_scripts": [
                "future PayPal connector",
                "manual PayPal data export",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync could target recent transaction summaries and account balances where available.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "planned",
                    "kind": "full_export",
                    "details": "PayPal export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "venmo": {
            "title": "Venmo",
            "explanation": "Venmo is a useful social-finance source with payments, network interactions, and account history.",
            "collection_scripts": [
                "future Venmo connector",
                "manual Venmo data export",
            ],
            "access_paths": [
                {
                    "mode": "automatic_login_sync",
                    "status": "planned",
                    "kind": "connector",
                    "details": "Fast sync could target recent payment activity and visible social feed state.",
                    "manual_export": False,
                },
                {
                    "mode": "manual_account_export",
                    "status": "planned",
                    "kind": "full_export",
                    "details": "Venmo export has not been staged yet.",
                    "manual_export": True,
                },
            ],
            "canonical_source": "manual_account_export",
            "lanes": {},
        },
        "gemini-local": {
            "title": "Gemini Local",
            "explanation": "Gemini local data was collected from local files and normalized through the connector/local lane.",
            "collection_scripts": [
                "npm run import:gemini-local",
            ],
            "access_paths": [
                {
                    "mode": "automatic_local_sync",
                    "status": "present",
                    "kind": "local_import",
                    "details": "Gemini local importer reads local CLI/session files from ~/.zenflow and ~/.gemini.",
                    "manual_export": False,
                },
            ],
            "canonical_source": "automatic_local_sync",
            "lanes": {
                "connector": connector_lane.get("gemini", {}),
            },
        },
        "codex-local": {
            "title": "Codex Local",
            "explanation": "Codex local data lives on this machine under ~/.codex and should be treated as a local auto-sync style source, separate from ChatGPT web exports.",
            "collection_scripts": [
                "future codex local importer",
            ],
            "access_paths": [
                {
                    "mode": "automatic_local_sync",
                    "status": "present",
                    "kind": "local_import",
                    "details": "Local Codex state, prompts, shell snapshots, and history exist under ~/.codex.",
                    "manual_export": False,
                },
            ],
            "canonical_source": "automatic_local_sync",
            "lanes": {
                "local": codex_local_summary(),
            },
        },
    }

    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "workspace_root": str(ROOT.parent),
        "paths": {
            "full_exports": str(FULL_EXPORTS_ROOT),
            "working_full_exports": str(WORKING_ROOT),
            "source_index": str(SOURCE_INDEX_PATH),
        },
        "notes": [
            "Connector/API/local data and full-account exports should remain in separate lanes.",
            "A source can have more than one lane at the same time.",
            "Normalization coverage is still incomplete for several full-export formats.",
        ],
        "overview": overview,
        "lane_status": {
            "connector": connector_lane_summary,
            "local": local_lane_summary,
            "full_export": full_export_lane,
        },
        "sources": sources,
    }


def main() -> int:
    MANIFESTS_ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path = MANIFESTS_ROOT / "source_audit.json"
    manifest = build_manifest()
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
