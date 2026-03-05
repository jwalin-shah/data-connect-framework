#!/usr/bin/env python3
"""Normalize ChatGPT full export ZIP into the connector lane.

The OpenAI data export contains conversations-000.json through conversations-NNN.json,
each a list of conversation objects with a `mapping` dict of message nodes.

Usage:
    python3 normalize_chatgpt_export.py
    python3 normalize_chatgpt_export.py --zip ~/Downloads/chatgptexport*.zip
"""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

CHATGPT_EXPORT_ZIP = Path(
    "/path/to/Downloads/"
    "chatgptexportb3b6161e4d2479830bcd95753af2af0ac7dde45a9b753cd7888870d0f860bfaa"
    "-2026-03-02-00-42-39-1e458424e62443e6b240cd8844bf4dd5.zip"
)
CONNECTOR_NORMALIZED = Path("/path/to/data-connect/meta/lanes/connector/normalized")
WATERMARK_DIR = Path("/path/to/data-connect/meta/working/merge_watermarks")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _watermark_path(label: str) -> Path:
    WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
    return WATERMARK_DIR / f"{label}.json"


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


def _unix_to_iso(ts) -> str | None:
    if ts is None:
        return None
    try:
        return (
            datetime.fromtimestamp(float(ts), tz=timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except Exception:
        return None


def _extract_text(content: dict) -> str:
    """Get plain text from a message content object."""
    if not isinstance(content, dict):
        return ""
    ctype = content.get("content_type", "")
    if ctype == "text":
        parts = content.get("parts") or []
        return " ".join(str(p) for p in parts if isinstance(p, str)).strip()
    if ctype == "code":
        return content.get("text", "")
    if ctype in ("tether_quote", "tether_browsing_display"):
        return content.get("url", "") or content.get("title", "")
    return ""


def normalize_chatgpt_export(zip_path: Path | None = None) -> int:
    """Parse ChatGPT full export ZIP → connector messages + conversations.jsonl."""
    zip_path = zip_path or CHATGPT_EXPORT_ZIP
    if not zip_path.exists():
        print(f"  chatgpt_export: not found at {zip_path}")
        return 0

    label = f"chatgpt_full_export_{zip_path.stat().st_size}"
    file_size = zip_path.stat().st_size
    if _read_watermark(label) >= file_size:
        return 0

    msg_dst = CONNECTOR_NORMALIZED / "messages.jsonl"
    conv_dst = CONNECTOR_NORMALIZED / "conversations.jsonl"
    existing_msg_ids = _load_existing_ids(msg_dst)
    existing_conv_ids = _load_existing_ids(conv_dst)
    run_at = _now_iso()

    msgs_added = 0
    convs_added = 0

    try:
        zf = zipfile.ZipFile(str(zip_path))
    except Exception as e:
        print(f"  chatgpt_export: failed to open ZIP: {e}")
        return 0

    conv_files = sorted(n for n in zf.namelist() if n.startswith("conversations-") and n.endswith(".json"))

    with msg_dst.open("a") as f_msgs, conv_dst.open("a") as f_convs:
        for conv_filename in conv_files:
            try:
                with zf.open(conv_filename) as f:
                    conversations = json.loads(f.read())
            except Exception as e:
                print(f"  chatgpt_export: error reading {conv_filename}: {e}")
                continue

            for conv in conversations:
                conv_id = conv.get("id") or conv.get("conversation_id") or ""
                title = conv.get("title") or ""
                create_ts = conv.get("create_time")
                update_ts = conv.get("update_time")
                created_at = _unix_to_iso(create_ts)
                updated_at = _unix_to_iso(update_ts)

                # Write conversation record (deduplicated)
                conv_rec_id = f"chatgpt:conv:{conv_id}"
                if conv_rec_id not in existing_conv_ids:
                    conv_record = {
                        "id": conv_rec_id,
                        "type": "conversation",
                        "title": title,
                        "created_at": created_at,
                        "observed_at": updated_at or created_at,
                        "meta": {
                            "platform": "chatgpt",
                            "record_type": "conversation",
                            "collection_method": "full_export",
                            "run_at": run_at,
                            "source_file": conv_filename,
                            "native_id": conv_id,
                        },
                    }
                    f_convs.write(json.dumps(conv_record, ensure_ascii=True) + "\n")
                    existing_conv_ids.add(conv_rec_id)
                    convs_added += 1

                # Write each user/assistant message
                mapping = conv.get("mapping") or {}
                for node_id, node in mapping.items():
                    msg = node.get("message")
                    if not msg:
                        continue
                    role = (msg.get("author") or {}).get("role", "")
                    if role not in ("user", "assistant"):
                        continue

                    content = msg.get("content") or {}
                    text = _extract_text(content)
                    if not text:
                        continue

                    msg_id = msg.get("id") or node_id
                    rec_id = f"chatgpt:msg:{hashlib.sha256(f'{conv_id}:{msg_id}'.encode()).hexdigest()[:24]}"
                    if rec_id in existing_msg_ids:
                        continue

                    occurred = _unix_to_iso(msg.get("create_time") or create_ts)
                    record = {
                        "id": rec_id,
                        "type": "chat_message",
                        "content": text[:4000],  # cap at 4KB per message
                        "role": role,
                        "created_at": occurred,
                        "observed_at": occurred,
                        "conversation_id": conv_id,
                        "meta": {
                            "platform": "chatgpt",
                            "record_type": "message",
                            "collection_method": "full_export",
                            "run_at": run_at,
                            "source_file": conv_filename,
                            "native_id": msg_id,
                            "natural_key": msg_id,
                        },
                    }
                    f_msgs.write(json.dumps(record, ensure_ascii=True) + "\n")
                    existing_msg_ids.add(rec_id)
                    msgs_added += 1

    _write_watermark(label, file_size)
    print(f"  chatgpt_export: +{convs_added:,} conversations, +{msgs_added:,} messages")
    return msgs_added + convs_added


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", type=Path, default=None)
    args = parser.parse_args()
    normalize_chatgpt_export(args.zip)
