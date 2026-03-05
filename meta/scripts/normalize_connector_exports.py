#!/usr/bin/env python3
"""Normalize connector/API export data into a lane-specific output root."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path("/path/to/data-connect/meta/scripts/normalize_exports.py")
STAGE_SCRIPT = Path("/path/to/data-connect/meta/scripts/stage_app_exports.py")
CONNECTOR_NORMALIZED = Path(
    "/path/to/data-connect/meta/lanes/connector/normalized"
)
FULL_EXPORT_NORMALIZED = Path(
    "/path/to/data-connect/meta/lanes/full-export/normalized"
)
LOCAL_NORMALIZED = Path("/path/to/data-connect/lanes/local/normalized")

# Watermark directory — stores byte offsets so we only scan new records on re-runs
WATERMARK_DIR = Path("/path/to/data-connect/meta/working/merge_watermarks")


def _watermark_path(label: str) -> Path:
    WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
    safe = label.replace("/", "_").replace(" ", "_")
    return WATERMARK_DIR / f"{safe}.json"


def _read_watermark(label: str) -> int:
    """Return the byte offset we left off at last time (0 = start from beginning)."""
    p = _watermark_path(label)
    if p.exists():
        try:
            return json.loads(p.read_text()).get("offset", 0)
        except Exception:
            pass
    return 0


def _write_watermark(label: str, offset: int) -> None:
    _watermark_path(label).write_text(json.dumps({"offset": offset, "updated_at": _now_iso()}))

# SDK paths
SDK_SRC = Path("/path/to/personal-context-sdk/src")
BROWSER_EXTRACTOR = SDK_SRC / "personal_data_sdk/extractors/browsers/extract_all_browsers.py"
SCREENTIME_EXTRACTOR = SDK_SRC / "personal_data_sdk/extractors/apple/extract_screentime.py"
MBOX_PATH = Path("/path/to/personal-context-sdk/data/raw/Takeout/Mail/All mail Including Spam and Trash.mbox")
GMAIL_MBOX_OUTPUT = LOCAL_NORMALIZED / "mail/messages_gmail.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_existing_ids(path: Path) -> set[str]:
    """Read all 'id' fields from a JSONL file into a set."""
    ids: set[str] = set()
    if path.exists():
        with open(path) as f:
            for line in f:
                try:
                    ids.add(json.loads(line).get("id"))
                except Exception:
                    pass
    return ids


def merge_records_from_full_export(platform: str, record_type: str) -> None:
    """Merge a platform's records of a given type from full-export normalized into connector lane.

    Each platform+type gets its own dedicated output file (e.g. linkedin_activities.jsonl)
    so normalize_exports.py truncating the primary files never destroys this data.
    Uses a byte-offset watermark so subsequent runs only scan new lines appended to the source.
    """
    if not FULL_EXPORT_NORMALIZED.exists() or not CONNECTOR_NORMALIZED.exists():
        return

    src = FULL_EXPORT_NORMALIZED / f"{record_type}.jsonl"
    dst = CONNECTOR_NORMALIZED / f"{platform}_{record_type}.jsonl"
    if not src.exists():
        return

    label = f"full_export_{platform}_{record_type}"
    offset = _read_watermark(label)
    src_size = src.stat().st_size

    # Nothing new since last run
    if offset >= src_size:
        return

    existing_ids = _load_existing_ids(dst)
    added = 0
    with open(dst, "a") as f_out, open(src, "rb") as f_in:
        f_in.seek(offset)
        for raw_line in f_in:
            try:
                line = raw_line.decode("utf-8")
                rec = json.loads(line)
                rec_platform = rec.get("meta", {}).get("platform", "")
                rec_id = rec.get("id")
                if rec_platform == platform and rec_id not in existing_ids:
                    if isinstance(rec.get("meta"), dict):
                        rec["meta"]["collection_method"] = "full_export"
                    f_out.write(json.dumps(rec, ensure_ascii=True) + "\n")
                    existing_ids.add(rec_id)
                    added += 1
            except Exception:
                pass

    _write_watermark(label, src_size)
    if added:
        print(f"  merged {added} {platform} {record_type} from full-export into connector")


def merge_activities_from_full_export(platform: str) -> None:
    """Merge a platform's activities from full-export normalized into connector lane (convenience wrapper)."""
    merge_records_from_full_export(platform, "activities")


# Mac Absolute Time epoch offset (seconds between Unix epoch and Mac epoch Jan 1 2001)
_MAC_EPOCH_OFFSET = 978307200.0


def _mac_abs_to_iso(mac_ts: float | int | None) -> str | None:
    """Convert Mac Absolute Time (seconds since Jan 1 2001) to UTC ISO string."""
    if mac_ts is None:
        return None
    try:
        unix_ts = float(mac_ts) + _MAC_EPOCH_OFFSET
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _imessage_date_to_iso(date_val: int | None) -> str | None:
    """Convert iMessage date (nanoseconds since Mac epoch) to UTC ISO string."""
    if date_val is None:
        return None
    try:
        mac_seconds = int(date_val) / 1_000_000_000
        return _mac_abs_to_iso(mac_seconds)
    except Exception:
        return None


def _unix_to_iso(unix_ts: int | float | None) -> str | None:
    """Convert Unix timestamp (seconds) to UTC ISO string."""
    if unix_ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(unix_ts), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _canonicalize_local_record(raw: dict, source_subpath: str, run_at: str) -> dict | None:
    """Wrap a raw local record in the canonical envelope expected by build_canonical_ndjson.

    Returns a new canonical dict, or None if the record should be skipped.
    """
    # Already canonical (has meta with platform) — pass through
    if raw.get("meta", {}).get("platform"):
        return raw

    if source_subpath.startswith("calendar/"):
        # Raw: {summary, start_date (Mac abs float seconds), end_date}
        summary = raw.get("summary") or ""
        start_raw = raw.get("start_date")
        occurred = _mac_abs_to_iso(start_raw)
        rec_id = "calendar:" + hashlib.sha256(f"{summary}:{start_raw}".encode()).hexdigest()[:20]
        return {
            "id": rec_id,
            "type": "calendar_event",
            "title": summary,
            "content": raw.get("location") or raw.get("description"),
            "url": raw.get("url"),
            "created_at": occurred,
            "observed_at": occurred,
            "meta": {
                "platform": "calendar",
                "record_type": "activity",
                "collection_method": "local_device",
                "run_at": run_at,
            },
        }

    if source_subpath.startswith("chat_db/"):
        # Raw: {ROWID, guid, text, handle_id, date (nanoseconds since Mac epoch)}
        guid = raw.get("guid")
        if not guid:
            return None
        occurred = _imessage_date_to_iso(raw.get("date"))
        return {
            "id": f"imessage:{guid}",
            "type": "imessage",
            "content": raw.get("text"),
            "created_at": occurred,
            "observed_at": occurred,
            "conversation_id": str(raw.get("handle_id") or ""),
            "role": "participant",
            "meta": {
                "platform": "imessage",
                "record_type": "message",
                "collection_method": "local_device",
                "run_at": run_at,
                "natural_key": guid,
            },
        }

    if source_subpath.startswith("mail/"):
        # Raw: {subject, summary, date_sent (Unix), date_received, sender}
        # (Apple Mail targeted export may have slightly different fields)
        subject = raw.get("subject") or ""
        sender = raw.get("sender") or raw.get("from") or ""
        date_sent = raw.get("date_sent") or raw.get("date_received") or raw.get("date")
        # Apple Mail dates may be Mac Absolute Time or Unix — heuristic: Mac abs < 1e9 seconds from 2001
        # Unix ts for 2026 ≈ 1.77e9; Mac abs for 2026 ≈ 7.9e8 — both < 2e9
        # The targeted export used date_sent values like 1772413057 which is clearly Unix (2026)
        # The plain messages.jsonl also uses Unix timestamps from the Envelope Index
        occurred = _unix_to_iso(date_sent)
        raw_hash = hashlib.sha256(f"{subject}:{sender}:{date_sent}".encode()).hexdigest()[:20]
        return {
            "id": f"mail:{raw_hash}",
            "type": "email",
            "title": subject,
            "content": raw.get("summary") or raw.get("text") or raw.get("body"),
            "created_at": occurred,
            "observed_at": occurred,
            "role": "participant",
            "meta": {
                "platform": "apple_mail",
                "record_type": "message",
                "collection_method": "local_device",
                "run_at": run_at,
                "sender": sender,
            },
        }

    # Unknown source — pass through as-is with a generated id for dedup
    raw_line = json.dumps(raw, sort_keys=True, ensure_ascii=True)
    raw["id"] = raw.get("id") or hashlib.sha256(raw_line.encode()).hexdigest()[:16]
    if isinstance(raw.get("meta"), dict):
        raw["meta"].setdefault("collection_method", "local_device")
    return raw


def merge_local_records(record_type: str, source_subpath: str, output_filename: str | None = None) -> None:
    """Merge records from the local lane into the connector lane.

    Each source gets its own dedicated output file so normalize_exports.py truncating
    the primary files never destroys this data.

    Args:
        record_type: canonical file type (e.g. "messages", "activities") — used only if
                     output_filename is not provided.
        source_subpath: relative path under LOCAL_NORMALIZED (e.g. "chat_db/messages.jsonl")
        output_filename: explicit output filename in connector/normalized/ (e.g. "imessage_messages.jsonl").
                         Defaults to f"{record_type}.jsonl" for backwards compat.
    """
    if not LOCAL_NORMALIZED.exists() or not CONNECTOR_NORMALIZED.exists():
        return

    src = LOCAL_NORMALIZED / source_subpath
    dst = CONNECTOR_NORMALIZED / (output_filename or f"{record_type}.jsonl")
    if not src.exists():
        print(f"  skipping local {source_subpath}: file not found")
        return

    label = f"local_{source_subpath.replace('/', '_')}"
    offset = _read_watermark(label)
    src_size = src.stat().st_size

    # Nothing new since last run
    if offset >= src_size:
        return

    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0
    with open(dst, "a") as f_out, open(src, "rb") as f_bin:
        f_bin.seek(offset)
        for raw_bytes in f_bin:
            line = raw_bytes.decode("utf-8").strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
                canonical = _canonicalize_local_record(raw, source_subpath, run_at)
                if canonical is None:
                    continue
                rec_id = str(canonical.get("id") or hashlib.sha256(line.encode()).hexdigest()[:16])
                if rec_id not in existing_ids:
                    f_out.write(json.dumps(canonical, ensure_ascii=True) + "\n")
                    existing_ids.add(rec_id)
                    added += 1
            except Exception:
                pass

    _write_watermark(label, src_size)
    if added:
        print(f"  merged {added} records from local/{source_subpath} into connector/{record_type}.jsonl")


def merge_claude_full_export() -> None:
    """Merge Claude full-export data into connector lane for complete coverage."""
    if not FULL_EXPORT_NORMALIZED.exists() or not CONNECTOR_NORMALIZED.exists():
        return

    claude_conversations_path = FULL_EXPORT_NORMALIZED / "conversations.jsonl"
    claude_messages_path = FULL_EXPORT_NORMALIZED / "messages.jsonl"

    existing_conv_ids = set()
    existing_msg_ids = set()

    conv_path = CONNECTOR_NORMALIZED / "claude_conversations.jsonl"
    msg_path = CONNECTOR_NORMALIZED / "claude_full_export_messages.jsonl"

    if conv_path.exists():
        with open(conv_path) as f:
            for line in f:
                rec = json.loads(line)
                existing_conv_ids.add(rec.get("id"))

    if msg_path.exists():
        with open(msg_path) as f:
            for line in f:
                rec = json.loads(line)
                existing_msg_ids.add(rec.get("id"))

    if claude_conversations_path.exists():
        with open(conv_path, "a") as f_out:
            with open(claude_conversations_path) as f_in:
                for line in f_in:
                    rec = json.loads(line)
                    if (
                        rec.get("meta", {}).get("platform") == "claude"
                        and rec.get("id") not in existing_conv_ids
                    ):
                        if isinstance(rec.get("meta"), dict):
                            rec["meta"]["collection_method"] = "full_export"
                        f_out.write(json.dumps(rec, ensure_ascii=True) + "\n")
                        existing_conv_ids.add(rec.get("id"))

    if claude_messages_path.exists():
        with open(msg_path, "a") as f_out:
            with open(claude_messages_path) as f_in:
                for line in f_in:
                    rec = json.loads(line)
                    if (
                        rec.get("meta", {}).get("platform") == "claude"
                        and rec.get("id") not in existing_msg_ids
                    ):
                        if isinstance(rec.get("meta"), dict):
                            rec["meta"]["collection_method"] = "full_export"
                        f_out.write(json.dumps(rec, ensure_ascii=True) + "\n")
                        existing_msg_ids.add(rec.get("id"))


def merge_browser_history() -> None:
    """Extract all browser visits from Brave/Firefox and merge into connector activities.

    Each individual visit is a separate activity record, keyed by (platform, url, visit_time_unix).
    Visits without a timestamp fall back to URL-based dedup.
    """
    dst = CONNECTOR_NORMALIZED / "browser_activities.jsonl"
    if not CONNECTOR_NORMALIZED.exists():
        return

    import importlib.util
    spec = importlib.util.spec_from_file_location("extract_all_browsers", BROWSER_EXTRACTOR)
    if spec is None or not BROWSER_EXTRACTOR.exists():
        print("  skipping browser history: extractor not found")
        return

    try:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        records = mod.get_brave_history() + mod.get_firefox_history()
    except Exception as exc:
        print(f"  skipping browser history: extractor error: {exc}")
        return

    if not records:
        print("  browser history: no records extracted")
        return

    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    with open(dst, "a") as f_out:
        for rec in records:
            url = rec.get("url", "")
            platform = rec.get("platform", "browser")
            visit_ts = rec.get("visit_time_unix")

            # Unique key per individual visit: platform + url + timestamp
            if visit_ts is not None:
                ts_str = f"{visit_ts:.3f}"
                rec_id = f"browser:{platform}:{hashlib.sha256((url + ts_str).encode()).hexdigest()[:20]}"
            else:
                rec_id = f"browser:{platform}:{hashlib.sha256(url.encode()).hexdigest()[:20]}"

            if rec_id in existing_ids:
                continue

            # Convert Unix timestamp to ISO string
            occurred_at: str | None = None
            if visit_ts is not None:
                try:
                    from datetime import datetime, timezone as _tz
                    occurred_at = datetime.fromtimestamp(visit_ts, tz=_tz.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                except Exception:
                    pass
            if not occurred_at:
                occurred_at = run_at

            activity = {
                "id": rec_id,
                "type": "web_visit",
                "url": url,
                "title": rec.get("title"),
                "visit_count": rec.get("visits"),
                "created_at": occurred_at,
                "observed_at": occurred_at,
                "meta": {
                    "platform": platform,
                    "record_type": "activity",
                    "collection_method": "local_device",
                    "run_at": run_at,
                },
            }
            f_out.write(json.dumps(activity, ensure_ascii=True) + "\n")
            existing_ids.add(rec_id)
            added += 1

    if added:
        print(f"  merged {added} browser visit records into connector browser_activities")


def merge_screentime() -> None:
    """Extract every individual app session from KnowledgeC.db (no aggregation, no limit).

    Each session is keyed by (app, start_iso) so re-runs only add new sessions.
    """
    dst = CONNECTOR_NORMALIZED / "screentime_activities.jsonl"
    if not CONNECTOR_NORMALIZED.exists():
        return

    import importlib.util
    spec = importlib.util.spec_from_file_location("extract_screentime", SCREENTIME_EXTRACTOR)
    if spec is None or not SCREENTIME_EXTRACTOR.exists():
        print("  skipping screentime: extractor not found")
        return

    try:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        records = mod.get_app_usage()
    except Exception as exc:
        print(f"  skipping screentime: extractor error: {exc}")
        return

    if not records:
        print("  screentime: no records extracted")
        return

    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    with open(dst, "a") as f_out:
        for rec in records:
            app = rec.get("app", "unknown")
            start_iso = rec.get("start_iso") or run_at
            # Unique per session: app + start time
            rec_id = f"screentime:macos:{hashlib.sha256((app + start_iso).encode()).hexdigest()[:20]}"
            if rec_id in existing_ids:
                continue
            activity = {
                "id": rec_id,
                "type": "app_usage",
                "app": app,
                "title": app,
                "duration_min": rec.get("duration_min"),
                "duration_sec": rec.get("duration_sec"),
                "created_at": start_iso,
                "observed_at": start_iso,
                "end_at": rec.get("end_iso"),
                "meta": {
                    "platform": "macos_screentime",
                    "record_type": "activity",
                    "collection_method": "local_device",
                    "run_at": run_at,
                },
            }
            f_out.write(json.dumps(activity, ensure_ascii=True) + "\n")
            existing_ids.add(rec_id)
            added += 1

    if added:
        print(f"  merged {added} Screen Time sessions into connector activities")


STAGED_SOURCES = Path("/path/to/data-connect/meta/working/staged-sources/connector")


def _iter_staged_json(platform: str, scope: str):
    """Yield parsed JSON payloads from all staged source files for a platform/scope."""
    src_dir = STAGED_SOURCES / platform / scope
    if not src_dir.exists():
        return
    for p in src_dir.glob("*.json"):
        try:
            with open(p) as f:
                yield json.load(f)
        except Exception:
            pass


def merge_github_repo_events() -> None:
    """Generate activity events from GitHub repositories (created + last push)."""
    dst = CONNECTOR_NORMALIZED / "github_activities.jsonl"
    if not CONNECTOR_NORMALIZED.exists():
        return

    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    with open(dst, "a") as f_out:
        for payload in _iter_staged_json("github", "repositories"):
            try:
                if isinstance(payload, list):
                    repos = payload
                elif isinstance(payload, dict):
                    data = payload.get("data", {})
                    repos = data.get("items", []) or data.get("repositories", [])
                    if not repos and isinstance(data, list):
                        repos = data
                else:
                    continue
            except Exception:
                continue
            for repo in repos:
                name = repo.get("name") or repo.get("full_name", "")
                language = repo.get("language") or ""
                description = repo.get("description") or ""
                topics = repo.get("topics") or []

                for event_type, ts_field in [("created_repository", "created_at"), ("pushed_to_repository", "pushed_at")]:
                    ts = repo.get(ts_field)
                    if not ts:
                        continue
                    rec_id = f"github:{event_type}:{hashlib.sha256((name + ts).encode()).hexdigest()[:20]}"
                    if rec_id in existing_ids:
                        continue
                    activity = {
                        "id": rec_id,
                        "type": event_type,
                        "title": name,
                        "repository": name,
                        "content": description,
                        "language": language,
                        "topics": topics,
                        "created_at": ts,
                        "observed_at": ts,
                        "meta": {
                            "platform": "github",
                            "record_type": "activity",
                            "run_at": run_at,
                        },
                    }
                    f_out.write(json.dumps(activity, ensure_ascii=True) + "\n")
                    existing_ids.add(rec_id)
                    added += 1

    if added:
        print(f"  merged {added} GitHub repo events into connector activities")


def merge_instagram_post_events() -> None:
    """Generate activity events from Instagram posts."""
    dst = CONNECTOR_NORMALIZED / "instagram_activities.jsonl"
    if not CONNECTOR_NORMALIZED.exists():
        return

    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    with open(dst, "a") as f_out:
        for payload in _iter_staged_json("instagram", "posts"):
            if not isinstance(payload, dict):
                continue
            data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
            posts = data.get("posts", [])
            liked = data.get("liked_posts", []) or data.get("likes", [])
            saved = data.get("saved", []) or data.get("saved_posts", [])

            for post in posts:
                shortcode = post.get("shortcode") or post.get("id") or ""
                ts = post.get("timestamp") or run_at
                rec_id = f"instagram:post:{hashlib.sha256(shortcode.encode()).hexdigest()[:20]}"
                if rec_id in existing_ids:
                    continue
                activity = {
                    "id": rec_id,
                    "type": "instagram_post",
                    "title": (post.get("caption") or "")[:120],
                    "url": post.get("permalink"),
                    "content": post.get("caption"),
                    "created_at": ts,
                    "observed_at": ts,
                    "meta": {
                        "platform": "instagram",
                        "record_type": "activity",
                        "run_at": run_at,
                    },
                }
                f_out.write(json.dumps(activity, ensure_ascii=True) + "\n")
                existing_ids.add(rec_id)
                added += 1

            for post in liked:
                url = post.get("permalink") or post.get("url") or ""
                ts = post.get("timestamp") or run_at
                rec_id = f"instagram:liked:{hashlib.sha256(url.encode()).hexdigest()[:20]}"
                if rec_id in existing_ids:
                    continue
                activity = {
                    "id": rec_id,
                    "type": "instagram_like",
                    "title": post.get("caption", "")[:120],
                    "url": url,
                    "created_at": ts,
                    "observed_at": ts,
                    "meta": {
                        "platform": "instagram",
                        "record_type": "activity",
                        "run_at": run_at,
                    },
                }
                f_out.write(json.dumps(activity, ensure_ascii=True) + "\n")
                existing_ids.add(rec_id)
                added += 1

    if added:
        print(f"  merged {added} Instagram post/like events into connector activities")


def main() -> int:
    stage_cmd = [sys.executable, str(STAGE_SCRIPT)]
    stage_rc = subprocess.call(stage_cmd)
    if stage_rc != 0:
        return stage_rc

    cmd = [
        sys.executable,
        str(SCRIPT),
        "--source-root",
        "/path/to/data-connect/meta/working/staged-sources/connector",
        "--output-root",
        "/path/to/data-connect/meta/lanes/connector",
    ]
    normalize_rc = subprocess.call(cmd)
    if normalize_rc != 0:
        return normalize_rc

    # --- Promote full-export → connector ---
    merge_claude_full_export()
    merge_activities_from_full_export("spotify")

    # LinkedIn: promote all four record types from full-export
    merge_records_from_full_export("linkedin", "activities")   # ~34,926 searches/reactions
    merge_records_from_full_export("linkedin", "documents")    # ~3,181 job apps/courses
    merge_records_from_full_export("linkedin", "people")       # ~714 connections
    merge_records_from_full_export("linkedin", "messages")     # ~424 messages

    # --- Promote local lane → connector (each source owns its own output file) ---
    merge_local_records("activities", "calendar/events.jsonl",        "calendar_activities.jsonl")
    merge_local_records("messages",   "chat_db/messages.jsonl",       "imessage_messages.jsonl")
    merge_local_records("messages",   "mail/messages.jsonl",          "mail_messages.jsonl")
    merge_local_records("messages",   "mail/messages_targeted.jsonl", "mail_targeted_messages.jsonl")
    if GMAIL_MBOX_OUTPUT.exists():
        merge_local_records("messages", "mail/messages_gmail.jsonl",  "gmail_messages.jsonl")

    # --- Device data → connector ---
    merge_browser_history()   # Brave + Firefox history (all individual visits)
    merge_screentime()        # macOS KnowledgeC.db all individual app sessions

    # --- Platform activity events (connector staged sources) ---
    merge_github_repo_events()       # created/pushed events from 13 repos
    merge_instagram_post_events()    # posts from connector staged sources

    # --- Google Takeout (Chat, Calendar, Fit, My Activity, Gmail MBOX) ---
    try:
        import importlib.util as _ilu
        _takeout_script = Path(__file__).parent / "normalize_google_takeout.py"
        _spec = _ilu.spec_from_file_location("normalize_google_takeout", _takeout_script)
        if _spec and _takeout_script.exists():
            _mod = _ilu.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
            _mod.normalize_all_takeout()
    except Exception as _e:
        print(f"  google_takeout: skipped ({_e})")

    # --- Facebook Messenger export ZIPs ---
    try:
        import importlib.util as _ilu3
        _fb_script = Path(__file__).parent / "normalize_facebook_export.py"
        _spec3 = _ilu3.spec_from_file_location("normalize_facebook_export", _fb_script)
        if _spec3 and _fb_script.exists():
            _mod3 = _ilu3.module_from_spec(_spec3)
            _spec3.loader.exec_module(_mod3)  # type: ignore[union-attr]
            _mod3.normalize_facebook_export()
    except Exception as _e3:
        print(f"  facebook_export: skipped ({_e3})")

    # --- ChatGPT full export ZIP ---
    try:
        import importlib.util as _ilu2
        _chatgpt_script = Path(__file__).parent / "normalize_chatgpt_export.py"
        _spec2 = _ilu2.spec_from_file_location("normalize_chatgpt_export", _chatgpt_script)
        if _spec2 and _chatgpt_script.exists():
            _mod2 = _ilu2.module_from_spec(_spec2)
            _spec2.loader.exec_module(_mod2)  # type: ignore[union-attr]
            _mod2.normalize_chatgpt_export()
    except Exception as _e2:
        print(f"  chatgpt_export: skipped ({_e2})")

    # --- Instagram archive (run ingest_instagram_archive.py first) ---
    IG = Path("/path/to/data-connect/lanes/local/normalized/instagram")
    merge_local_records("activities", "instagram/topics.jsonl",            "instagram_activities.jsonl")
    merge_local_records("activities", "instagram/liked_posts.jsonl",       "instagram_activities.jsonl")
    merge_local_records("activities", "instagram/saved_posts.jsonl",       "instagram_activities.jsonl")
    merge_local_records("activities", "instagram/comments.jsonl",          "instagram_activities.jsonl")
    merge_local_records("activities", "instagram/story_interactions.jsonl","instagram_activities.jsonl")
    merge_local_records("activities", "instagram/ads_interactions.jsonl",  "instagram_activities.jsonl")
    merge_local_records("activities", "instagram/content_viewed.jsonl",    "instagram_activities.jsonl")
    merge_local_records("activities", "instagram/advertisers.jsonl",       "instagram_activities.jsonl")
    merge_local_records("people",     "instagram/connections.jsonl",       "instagram_people.jsonl")
    merge_local_records("messages",   "instagram/messages.jsonl",          "instagram_messages.jsonl")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
