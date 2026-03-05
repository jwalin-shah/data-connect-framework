#!/usr/bin/env python3
"""Scan downloads/email artifacts, stage known full exports, and refresh full-export lanes."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path("/path/to/data-connect/meta")
RAW_ROOT = ROOT / "raw"
FULL_EXPORTS_ROOT = RAW_ROOT / "full-exports"
EMAIL_ROOT = RAW_ROOT / "email-inbox"
WORKING_FULL_EXPORTS = ROOT / "working" / "staging" / "full-exports"
MANIFESTS_ROOT = ROOT / "manifests"
STATE_PATH = MANIFESTS_ROOT / "export_intake_state.json"
MANIFEST_PATH = MANIFESTS_ROOT / "export_intake_manifest.json"
STAGE_SCRIPT = ROOT / "scripts" / "stage_full_exports.py"
NORMALIZE_SCRIPT = ROOT / "scripts" / "normalize_full_exports.py"
AUDIT_SCRIPT = ROOT / "scripts" / "generate_source_audit.py"
SOURCE_AUDIT_PATH = ROOT / "manifests" / "source_audit.json"
FULL_EXPORT_LAST_RUN_PATH = ROOT / "lanes" / "full-export" / "manifests" / "last_run_manifest.json"
EMAIL_DOWNLOADS_ROOT = EMAIL_ROOT / "downloads"


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def safe_copy(src: Path, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        src_stat = src.stat()
        dest_stat = dest.stat()
        if src_stat.st_size == dest_stat.st_size and int(src_stat.st_mtime) == int(dest_stat.st_mtime):
            return False
    shutil.copy2(src, dest)
    return True


def unzip_to(src: Path, dest: Path) -> bool:
    marker = dest / ".extracted-from.json"
    dest.mkdir(parents=True, exist_ok=True)
    fingerprint = {
        "source": str(src),
        "size": src.stat().st_size,
        "mtime": src.stat().st_mtime_ns,
    }
    if marker.exists():
        existing = load_json(marker, {})
        if existing == fingerprint:
            return False

    for child in dest.iterdir():
        if child.name == ".extracted-from.json":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

    with zipfile.ZipFile(src, "r") as archive:
        archive.extractall(dest)

    save_json(marker, fingerprint)
    return True


def run_script(path: Path) -> None:
    subprocess.run(["python3", str(path)], check=True)


def file_key(path: Path) -> str:
    stat = path.stat()
    return f"{path}:{stat.st_size}:{stat.st_mtime_ns}"


@dataclass(frozen=True)
class ProviderRule:
    provider: str
    raw_dir: str
    classify_tokens: tuple[str, ...]
    allowed_suffixes: tuple[str, ...]
    supported: bool = True
    extract_zip: bool = False
    working_dir: str | None = None
    raw_subdir: str = "source"


RULES: tuple[ProviderRule, ...] = (
    ProviderRule(
        provider="claude",
        raw_dir="claude-full-export",
        classify_tokens=("claude data", "batch-"),
        allowed_suffixes=(".zip",),
        extract_zip=True,
        working_dir="claude",
    ),
    ProviderRule(
        provider="linkedin",
        raw_dir="linkedin-basic-export",
        classify_tokens=("basic_linkedindataexport", "linkedin data export"),
        allowed_suffixes=(".zip",),
        extract_zip=True,
        working_dir="linkedin",
        raw_subdir="",
    ),
    ProviderRule(
        provider="spotify",
        raw_dir="spotify-full-export",
        classify_tokens=("my_spotify_data", "streaming_history_audio", "extendedstreaminghistory"),
        allowed_suffixes=(".zip", ".json", ".pdf"),
        extract_zip=True,
        working_dir="spotify",
        raw_subdir="",
    ),
    ProviderRule(
        provider="google",
        raw_dir="google-takeout-full-export-excluding-drive-photos",
        classify_tokens=("takeout-",),
        allowed_suffixes=(".zip",),
        extract_zip=False,
        raw_subdir="source",
    ),
    ProviderRule(
        provider="chatgpt",
        raw_dir="chatgpt-full-export",
        classify_tokens=("chatgpt export", "chatgpt_data_export", "openai data export"),
        allowed_suffixes=(".zip",),
        supported=False,
        raw_subdir="source",
    ),
    ProviderRule(
        provider="instagram",
        raw_dir="instagram-full-export",
        classify_tokens=("instagram", "your_instagram_information"),
        allowed_suffixes=(".zip",),
        supported=False,
        raw_subdir="source",
    ),
    ProviderRule(
        provider="facebook",
        raw_dir="facebook-full-export",
        classify_tokens=("facebook", "your_facebook_information"),
        allowed_suffixes=(".zip",),
        supported=False,
        raw_subdir="source",
    ),
    ProviderRule(
        provider="whatsapp",
        raw_dir="whatsapp-full-export",
        classify_tokens=("whatsapp",),
        allowed_suffixes=(".zip",),
        supported=False,
        raw_subdir="source",
    ),
    ProviderRule(
        provider="x",
        raw_dir="x-full-export",
        classify_tokens=("twitter", "x archive", "your x data"),
        allowed_suffixes=(".zip",),
        supported=False,
        raw_subdir="source",
    ),
)


def classify(path: Path) -> ProviderRule | None:
    name = path.name.lower()
    for rule in RULES:
        if path.suffix.lower() not in rule.allowed_suffixes:
            continue
        if any(token in name for token in rule.classify_tokens):
            return rule
    return None


def destination_for(rule: ProviderRule, source: Path) -> Path:
    base = FULL_EXPORTS_ROOT / rule.raw_dir
    if rule.raw_subdir:
        base = base / rule.raw_subdir
    return base / source.name


def extract_destination(rule: ProviderRule, source: Path) -> Path:
    if rule.provider == "spotify":
        return FULL_EXPORTS_ROOT / rule.raw_dir / "extended-streaming-history"
    assert rule.working_dir
    return WORKING_FULL_EXPORTS / rule.working_dir


def iter_candidate_files(search_roots: Iterable[Path]) -> list[Path]:
    candidates: list[Path] = []
    for root in search_roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix in {".zip", ".json", ".csv", ".html", ".pdf"}:
                candidates.append(path)
    return sorted(candidates, key=lambda item: item.stat().st_mtime_ns, reverse=True)


def load_pending_links() -> list[dict]:
    links_path = EMAIL_ROOT / "links.jsonl"
    if not links_path.exists():
        return []
    pending = []
    for line in links_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except Exception:
            continue
        url = str(record.get("url", ""))
        lower = url.lower()
        provider = None
        if "anthropic" in lower or "claude" in lower:
            provider = "claude"
        elif "takeout.google.com" in lower or "google" in lower:
            provider = "google"
        elif "linkedin" in lower:
            provider = "linkedin"
        elif "spotify" in lower:
            provider = "spotify"
        elif "openai" in lower or "chatgpt" in lower:
            provider = "chatgpt"
        elif "instagram" in lower:
            provider = "instagram"
        elif "facebook" in lower:
            provider = "facebook"
        elif "twitter" in lower or "x.com" in lower:
            provider = "x"
        elif "whatsapp" in lower:
            provider = "whatsapp"
        if provider:
            pending.append(
                {
                    "provider": provider,
                    "url": url,
                    "subject": record.get("subject"),
                    "from": record.get("from"),
                    "saved_at": record.get("saved_at"),
                    "message_record": record.get("message_record"),
                }
            )
    return pending


def infer_download_filename(url: str, headers: dict[str, str], provider: str) -> str:
    content_disposition = headers.get("Content-Disposition", "")
    if "filename=" in content_disposition:
        filename = content_disposition.split("filename=", 1)[1].strip().strip('"').strip("'")
        if filename:
            return filename

    parsed = urllib.parse.urlparse(url)
    basename = Path(parsed.path).name
    if basename:
        return basename

    return f"{provider}-export-{int(datetime.now(timezone.utc).timestamp())}.zip"


def is_downloadable_response(url: str, headers: dict[str, str]) -> bool:
    content_type = headers.get("Content-Type", "").lower()
    content_disposition = headers.get("Content-Disposition", "").lower()
    lower_url = url.lower()
    downloadable_types = (
        "application/zip",
        "application/x-zip-compressed",
        "application/octet-stream",
        "application/json",
        "text/csv",
        "application/pdf",
    )
    downloadable_suffixes = (".zip", ".json", ".csv", ".pdf")
    return (
        "attachment" in content_disposition
        or any(token in content_type for token in downloadable_types)
        or any(lower_url.endswith(suffix) for suffix in downloadable_suffixes)
    )


def attempt_link_download(record: dict, dry_run: bool) -> dict:
    url = str(record.get("url", ""))
    provider = str(record.get("provider", "unknown"))
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "DataConnect Export Intake/1.0",
            "Accept": "*/*",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            final_url = response.geturl()
            headers = {key: value for key, value in response.headers.items()}
            if not is_downloadable_response(final_url, headers):
                return {
                    **record,
                    "status": "needs_browser_auth",
                    "detail": "Link resolved to a non-download page or HTML response.",
                    "final_url": final_url,
                }

            filename = infer_download_filename(final_url, headers, provider)
            target = EMAIL_DOWNLOADS_ROOT / provider / filename
            if not dry_run:
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("wb") as handle:
                    shutil.copyfileobj(response, handle)

            return {
                **record,
                "status": "downloaded",
                "detail": "Downloaded direct export link.",
                "final_url": final_url,
                "download_path": str(target),
            }
    except urllib.error.HTTPError as err:
        status = "needs_browser_auth" if err.code in {401, 403} else "download_error"
        return {
            **record,
            "status": status,
            "detail": f"HTTP {err.code}",
        }
    except Exception as err:
        return {
            **record,
            "status": "download_error",
            "detail": str(err),
        }


def provider_validation(processed_records: list[dict]) -> dict:
    lane_manifest = load_json(FULL_EXPORT_LAST_RUN_PATH, {})
    audit_manifest = load_json(SOURCE_AUDIT_PATH, {})
    platform_files = (
        (((audit_manifest.get("lane_status") or {}).get("full_export") or {}).get("platform_files"))
        or {}
    )
    providers = sorted({record["provider"] for record in processed_records if record.get("supported")})
    checks = []
    for provider in providers:
        raw_present = any(
            record.get("provider") == provider and Path(record.get("raw_destination", "")).exists()
            for record in processed_records
        )
        checks.append(
            {
                "provider": provider,
                "raw_present": raw_present,
                "normalized_platform_files": int(platform_files.get(provider, 0)),
                "normalized_present": int(platform_files.get(provider, 0)) > 0,
            }
        )

    failures = int(((lane_manifest.get("record_counts") or {}).get("failures")) or 0)
    return {
        "status": "pass" if failures == 0 else "warning",
        "full_export_last_run": lane_manifest,
        "provider_checks": checks,
        "audit_manifest_generated": SOURCE_AUDIT_PATH.exists(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Process downloaded full exports into the meta workspace")
    parser.add_argument(
        "--downloads",
        default=str(Path.home() / "Downloads"),
        help="Folder to scan for newly downloaded exports",
    )
    parser.add_argument(
        "--include-email-artifacts",
        action="store_true",
        help="Also scan meta/raw/email-inbox attachments and unpacked directories",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    search_roots = [Path(args.downloads)]
    if args.include_email_artifacts:
        search_roots.extend(
            [
                EMAIL_ROOT / "attachments",
                EMAIL_ROOT / "unpacked",
            ]
        )

    state = load_json(STATE_PATH, {"processed": {}, "updated_at": None})
    processed = state.get("processed", {})
    summary = {
        "generated_at": now_iso(),
        "search_roots": [str(path) for path in search_roots],
        "processed": [],
        "unsupported": [],
        "skipped": [],
        "pending_links": load_pending_links(),
        "link_actions": [],
        "validation": {},
        "post_process": {"stage_full_exports": False, "normalize_full_exports": False, "generate_source_audit": False},
    }

    changed_supported = False

    for link_record in summary["pending_links"]:
        link_key = f"link:{link_record['provider']}:{link_record['url']}"
        if link_key in processed:
            summary["link_actions"].append(
                {
                    **link_record,
                    "status": "already_attempted",
                    "detail": processed[link_key].get("detail"),
                }
            )
            continue

        action = attempt_link_download(link_record, args.dry_run)
        summary["link_actions"].append(action)
        if not args.dry_run:
            processed[link_key] = {
                "provider": link_record["provider"],
                "url": link_record["url"],
                "recorded_at": now_iso(),
                "detail": action.get("detail"),
                "status": action.get("status"),
            }

    search_roots.append(EMAIL_DOWNLOADS_ROOT)

    for source in iter_candidate_files(search_roots):
        rule = classify(source)
        if rule is None:
            continue

        key = file_key(source)
        if key in processed:
            summary["skipped"].append({"source": str(source), "provider": rule.provider, "reason": "already_processed"})
            continue

        raw_dest = destination_for(rule, source)
        extracted = None
        copied = False
        extracted_changed = False

        if not args.dry_run:
            copied = safe_copy(source, raw_dest)
            if rule.extract_zip and source.suffix.lower() == ".zip":
                extracted = extract_destination(rule, source)
                extracted_changed = unzip_to(source, extracted)
        else:
            copied = True
            if rule.extract_zip and source.suffix.lower() == ".zip":
                extracted = extract_destination(rule, source)
                extracted_changed = True

        record = {
            "provider": rule.provider,
            "source": str(source),
            "raw_destination": str(raw_dest),
            "supported": rule.supported,
            "copied": copied,
            "extracted_to": str(extracted) if extracted else None,
            "extracted_changed": extracted_changed,
        }

        if rule.supported:
            summary["processed"].append(record)
            changed_supported = True
        else:
            summary["unsupported"].append(record)

        if not args.dry_run:
            processed[key] = {
                "provider": rule.provider,
                "source": str(source),
                "recorded_at": now_iso(),
            }

    if changed_supported and not args.dry_run:
        run_script(STAGE_SCRIPT)
        summary["post_process"]["stage_full_exports"] = True
        run_script(NORMALIZE_SCRIPT)
        summary["post_process"]["normalize_full_exports"] = True
        run_script(AUDIT_SCRIPT)
        summary["post_process"]["generate_source_audit"] = True

    summary["validation"] = provider_validation(summary["processed"]) if not args.dry_run else {
        "status": "dry_run",
        "provider_checks": [],
    }

    if not args.dry_run:
        state["processed"] = processed
        state["updated_at"] = now_iso()
        save_json(STATE_PATH, state)

    save_json(MANIFEST_PATH, summary)
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
