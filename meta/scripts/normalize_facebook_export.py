#!/usr/bin/env python3
"""Normalize Facebook Messenger export ZIPs into the connector lane.

Facebook exports two ZIP files. ZIP 1 contains photos/media only.
ZIP 2 contains JSON message files for inbox, archived_threads, e2ee_cutover,
and filtered_threads — each a thread JSON with participants + messages list.

Facebook JSON encodes strings as latin-1 interpreted UTF-8 (a known export bug).
This is fixed with: text.encode('latin-1').decode('utf-8', errors='replace')

Usage:
    python3 normalize_facebook_export.py
    python3 normalize_facebook_export.py --zip ~/Downloads/facebook-*.zip
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# Default ZIPs — zip1 is photos-only, zip2 has all JSON
FACEBOOK_ZIP_1 = Path("/path/to/Downloads/facebook-your-username785931-2026-03-02-ziukFxHk.zip")
FACEBOOK_ZIP_2 = Path("/path/to/Downloads/facebook-your-username785931-2026-03-02-CqNqkyFo.zip")

CONNECTOR_NORMALIZED = Path("/path/to/data-connect/meta/lanes/connector/normalized")
WATERMARK_DIR = Path("/path/to/data-connect/meta/working/merge_watermarks")

# Thread subfolders to import
_MSG_SUBFOLDERS = ("inbox", "archived_threads", "e2ee_cutover", "filtered_threads", "dating/messages")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _watermark_path(label: str) -> Path:
    WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", label)
    return WATERMARK_DIR / f"{safe}.json"


def _read_watermark(label: str) -> int:
    p = _watermark_path(label)
    if p.exists():
        try:
            return json.loads(p.read_text()).get("offset", 0)
        except Exception:
            pass
    return 0


def _write_watermark(label: str, offset: int) -> None:
    _watermark_path(label).write_text(json.dumps({"offset": offset, "updated_at": _now_iso()}))


def _load_existing_ids(path: Path) -> set:
    ids: set = set()
    if path.exists():
        with path.open() as f:
            for line in f:
                try:
                    ids.add(json.loads(line).get("id", ""))
                except Exception:
                    pass
    return ids


def _ms_to_iso(timestamp_ms) -> str | None:
    try:
        return (
            datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except Exception:
        return None


def _fix_encoding(text: str) -> str:
    """Fix Facebook's latin-1/UTF-8 encoding bug.

    Facebook stores UTF-8 strings but marks the JSON as latin-1, so characters
    like emojis and non-ASCII appear as mojibake. Reversing: encode back to
    latin-1 bytes then decode as UTF-8 fixes it.
    """
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text  # already valid unicode or not fixable


def _parse_thread_json(raw_bytes: bytes) -> dict | None:
    """Parse a thread JSON file, fixing encoding issues."""
    try:
        # Parse as raw bytes first, then fix encoding on string fields
        data = json.loads(raw_bytes.decode("utf-8", errors="replace"))
        return data
    except Exception:
        return None


def normalize_facebook_export(zip_paths: list[Path] | None = None) -> int:
    """Parse Facebook export ZIPs → connector messages + conversations.jsonl."""
    if zip_paths is None:
        zip_paths = [p for p in (FACEBOOK_ZIP_1, FACEBOOK_ZIP_2) if p.exists()]

    if not zip_paths:
        print("  facebook_export: no ZIP files found")
        return 0

    msg_dst = CONNECTOR_NORMALIZED / "facebook_messages.jsonl"
    conv_dst = CONNECTOR_NORMALIZED / "facebook_conversations.jsonl"
    existing_msg_ids = _load_existing_ids(msg_dst)
    existing_conv_ids = _load_existing_ids(conv_dst)
    run_at = _now_iso()

    total_msgs = 0
    total_convs = 0

    for zip_path in zip_paths:
        label = f"facebook_export_{zip_path.stat().st_size}"
        file_size = zip_path.stat().st_size
        if _read_watermark(label) >= file_size:
            continue

        print(f"  facebook: processing {zip_path.name}…")

        try:
            zf = zipfile.ZipFile(str(zip_path))
        except Exception as e:
            print(f"  facebook: failed to open {zip_path.name}: {e}")
            continue

        # Find all thread JSON files
        thread_files = [
            f for f in zf.namelist()
            if f.endswith(".json")
            and "/messages/" in f
            and any(f"/{sub}/" in f or f.endswith(f"/{sub}") for sub in _MSG_SUBFOLDERS)
        ]
        print(f"  facebook: {len(thread_files):,} thread files in {zip_path.name}")

        msgs_this_zip = 0
        convs_this_zip = 0

        with msg_dst.open("a") as f_msgs, conv_dst.open("a") as f_convs:
            for thread_file in thread_files:
                try:
                    with zf.open(thread_file) as f:
                        raw = f.read()
                    data = _parse_thread_json(raw)
                    if not data:
                        continue
                except Exception:
                    continue

                # Thread identity
                thread_path = data.get("thread_path", "")
                # e.g. "inbox/keyurhalbe_2453690078188513"
                thread_id = thread_path.split("/")[-1] if thread_path else hashlib.sha256(thread_file.encode()).hexdigest()[:20]
                conv_rec_id = f"facebook:conv:{hashlib.sha256(thread_id.encode()).hexdigest()[:24]}"

                raw_title = data.get("title", "")
                title = _fix_encoding(raw_title)

                participants = [
                    _fix_encoding(p.get("name", ""))
                    for p in (data.get("participants") or [])
                    if p.get("name")
                ]

                messages = data.get("messages") or []
                if not messages:
                    continue

                timestamps = [m.get("timestamp_ms") for m in messages if m.get("timestamp_ms")]
                first_ts = _ms_to_iso(min(timestamps)) if timestamps else None
                last_ts = _ms_to_iso(max(timestamps)) if timestamps else None

                # Write conversation record once
                if conv_rec_id not in existing_conv_ids:
                    conv_record = {
                        "id": conv_rec_id,
                        "type": "conversation",
                        "title": title,
                        "created_at": first_ts,
                        "observed_at": last_ts,
                        "meta": {
                            "platform": "facebook_messenger",
                            "record_type": "conversation",
                            "collection_method": "full_export",
                            "run_at": run_at,
                            "source_file": thread_file,
                            "participants": participants,
                            "thread_id": thread_id,
                        },
                    }
                    f_convs.write(json.dumps(conv_record, ensure_ascii=True) + "\n")
                    existing_conv_ids.add(conv_rec_id)
                    convs_this_zip += 1

                # Write each message
                for msg in messages:
                    ts_ms = msg.get("timestamp_ms")
                    raw_content = msg.get("content")
                    if not raw_content:
                        # Skip photo-only, sticker-only, reaction-only messages
                        continue

                    content = _fix_encoding(str(raw_content))
                    sender = _fix_encoding(msg.get("sender_name", ""))

                    # Dedup key: thread_id + timestamp_ms + sender (multiple messages same ms rare)
                    rec_id = f"facebook:msg:{hashlib.sha256(f'{thread_id}:{ts_ms}:{sender}'.encode()).hexdigest()[:24]}"
                    if rec_id in existing_msg_ids:
                        continue

                    occurred = _ms_to_iso(ts_ms)
                    record = {
                        "id": rec_id,
                        "type": "facebook_message",
                        "content": content[:2000],
                        "created_at": occurred,
                        "observed_at": occurred,
                        "conversation_id": thread_id,
                        "role": "participant",
                        "meta": {
                            "platform": "facebook_messenger",
                            "record_type": "message",
                            "collection_method": "full_export",
                            "run_at": run_at,
                            "sender": sender,
                            "source_file": thread_file,
                            "thread_title": title,
                        },
                    }
                    f_msgs.write(json.dumps(record, ensure_ascii=True) + "\n")
                    existing_msg_ids.add(rec_id)
                    msgs_this_zip += 1

        _write_watermark(label, file_size)
        print(f"  facebook {zip_path.name}: +{convs_this_zip:,} conversations, +{msgs_this_zip:,} messages")
        total_msgs += msgs_this_zip
        total_convs += convs_this_zip

    print(f"  facebook total: +{total_convs:,} conversations, +{total_msgs:,} messages")
    return total_msgs + total_convs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", type=Path, nargs="+", default=None)
    args = parser.parse_args()
    normalize_facebook_export(args.zip)
