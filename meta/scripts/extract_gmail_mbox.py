#!/usr/bin/env python3
"""Extract Gmail messages from a Google Takeout MBOX into JSONL.

Streams the 1.7 GB MBOX file without loading it entirely into memory.
Output: lanes/local/normalized/mail/messages_gmail.jsonl

Usage:
    python3 extract_gmail_mbox.py
    python3 extract_gmail_mbox.py --mbox /path/to/custom.mbox --output /path/to/out.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MBOX_PATH = Path(
    "/path/to/personal-context-sdk/data/raw/Takeout/Mail"
    "/All mail Including Spam and Trash.mbox"
)
OUTPUT_PATH = Path(
    "/path/to/data-connect/lanes/local/normalized/mail/messages_gmail.jsonl"
)

# Add SDK to path so MailParser is importable
SDK_SRC = Path("/path/to/personal-context-sdk/src")
sys.path.insert(0, str(SDK_SRC))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Gmail MBOX → JSONL")
    parser.add_argument("--mbox", default=str(MBOX_PATH), help="Path to MBOX file")
    parser.add_argument("--output", default=str(OUTPUT_PATH), help="Output JSONL path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mbox_path = Path(args.mbox)
    output_path = Path(args.output)

    if not mbox_path.exists():
        print(f"MBOX not found: {mbox_path}")
        return 1

    try:
        from personal_data_sdk.extractors.google.mail import MailParser
    except ImportError as e:
        print(f"Cannot import MailParser: {e}")
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    parser = MailParser(str(mbox_path))

    count = 0
    with output_path.open("w", encoding="utf-8") as f_out:
        for email in parser.parse_emails():
            # Wrap in a consistent envelope with platform tag
            record = {
                "id": None,  # no stable ID in MBOX; dedup by content hash at merge time
                "platform": "gmail",
                "timestamp": email.get("timestamp"),
                "from": email.get("from"),
                "to": email.get("to"),
                "subject": email.get("subject"),
                "text": email.get("text"),
                "meta": {
                    "platform": "gmail",
                    "record_type": "message",
                    "source": str(mbox_path.name),
                },
            }
            f_out.write(json.dumps(record, ensure_ascii=True) + "\n")
            count += 1
            if count % 5000 == 0:
                print(f"  {count:,} emails written…")

    print(f"Done: {count:,} Gmail messages → {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
