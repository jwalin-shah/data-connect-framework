#!/usr/bin/env python3
"""Normalize Google Takeout data into the connector lane.

Handles: Gmail MBOX, Google Chat, Google Calendar (ICS), Google Fit, My Activity HTML.
Writes directly to connector normalized with byte-offset watermarks (same pattern as
normalize_connector_exports.py) so re-runs only process new data.

Usage:
    python3 normalize_google_takeout.py
"""

from __future__ import annotations

import email.header
import email.utils
import hashlib
import json
import mailbox
import re
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

TAKEOUT_ROOT = Path("/path/to/personal-context-sdk/data/raw/Takeout")
CONNECTOR_NORMALIZED = Path("/path/to/data-connect/meta/lanes/connector/normalized")
WATERMARK_DIR = Path("/path/to/data-connect/meta/working/merge_watermarks")


# ── Shared helpers ─────────────────────────────────────────────────────────────

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


def _nano_to_iso(nanos) -> str | None:
    try:
        return (
            datetime.fromtimestamp(int(nanos) / 1e9, tz=timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except Exception:
        return None


def _parse_dt(s: str) -> str | None:
    """Parse a wide variety of timestamp strings to ISO 8601 UTC."""
    if not s:
        return None
    s = s.strip()

    # ISO / RFC 3339
    try:
        return (
            datetime.fromisoformat(s.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except ValueError:
        pass

    # RFC 2822 email dates: "Mon, 1 Jan 2024 12:00:00 +0000"
    try:
        t = email.utils.parsedate_to_datetime(s)
        return t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # Google Chat: "Monday, December 26, 2016 at 7:22:08 AM UTC"
    try:
        # drop day-of-week prefix
        _, _, rest = s.partition(", ")
        rest = re.sub(r"\s+[A-Z]{2,4}$", "", rest).replace(" at ", " ")
        return (
            datetime.strptime(rest.strip(), "%B %d, %Y %I:%M:%S %p")
            .replace(tzinfo=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except Exception:
        pass

    # My Activity: "Feb 26, 2026, 8:03:43 AM PST"
    for fmt in ("%b %d, %Y, %I:%M:%S %p", "%b %d, %Y, %I:%M %p"):
        try:
            cleaned = re.sub(r"\s+[A-Z]{2,4}$", "", s)
            return (
                datetime.strptime(cleaned.strip(), fmt)
                .replace(tzinfo=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            pass

    # ICS datetime: 20230101T120000Z or 20230101T120000 or 20230101
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y%m%d"):
        try:
            return (
                datetime.strptime(s[: len(fmt)], fmt)
                .replace(tzinfo=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            pass

    return None


# ── Google Chat ─────────────────────────────────────────────────────────────────

def normalize_google_chat() -> int:
    """Parse Google Chat DM/group messages → connector messages.jsonl."""
    chat_root = TAKEOUT_ROOT / "Google Chat"
    if not chat_root.exists():
        return 0

    dst = CONNECTOR_NORMALIZED / "google_chat_messages.jsonl"
    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    for messages_file in sorted(chat_root.glob("**/messages.json")):
        label = f"google_chat_{messages_file.parent.name}"
        file_size = messages_file.stat().st_size
        if _read_watermark(label) >= file_size:
            continue

        try:
            data = json.loads(messages_file.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            _write_watermark(label, file_size)
            continue

        conv_id = messages_file.parent.name  # e.g. "DM_9221oAAAAE"
        with dst.open("a") as f_out:
            for msg in data.get("messages", []):
                creator = msg.get("creator", {}) or {}
                text = msg.get("text") or ""
                raw_id = msg.get("message_id") or f"{conv_id}:{msg.get('created_date')}:{text}"
                rec_id = f"google_chat:{hashlib.sha256(raw_id.encode()).hexdigest()[:24]}"
                if rec_id in existing_ids:
                    continue
                occurred = _parse_dt(msg.get("created_date", ""))
                record = {
                    "id": rec_id,
                    "type": "chat_message",
                    "content": text,
                    "created_at": occurred,
                    "observed_at": occurred,
                    "conversation_id": conv_id,
                    "role": "participant",
                    "meta": {
                        "platform": "google_chat",
                        "record_type": "message",
                        "collection_method": "full_export",
                        "run_at": run_at,
                        "sender": creator.get("email") or creator.get("name"),
                        "sender_name": creator.get("name"),
                        "source_file": str(messages_file.relative_to(TAKEOUT_ROOT)),
                    },
                }
                f_out.write(json.dumps(record, ensure_ascii=True) + "\n")
                existing_ids.add(rec_id)
                added += 1

        _write_watermark(label, file_size)

    if added:
        print(f"  google_chat: +{added:,} messages")
    return added


# ── Google Calendar (ICS) ───────────────────────────────────────────────────────

def _parse_ics(path: Path) -> list[dict]:
    """Return list of VEVENT property dicts from an ICS file."""
    events: list[dict] = []
    current: dict = {}
    in_event = False
    try:
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.rstrip("\r")
            if line == "BEGIN:VEVENT":
                in_event = True
                current = {}
            elif line == "END:VEVENT":
                if current:
                    events.append(current)
                in_event = False
            elif in_event and ":" in line:
                key, _, val = line.partition(":")
                key = key.split(";")[0].strip()  # strip TZID= params
                current[key] = val.strip()
    except Exception:
        pass
    return events


def normalize_google_calendar() -> int:
    """Parse Google Calendar ICS files → connector activities.jsonl."""
    calendar_root = TAKEOUT_ROOT / "Calendar"
    if not calendar_root.exists():
        return 0

    dst = CONNECTOR_NORMALIZED / "google_takeout_activities.jsonl"
    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    for ics_file in sorted(calendar_root.glob("*.ics")):
        label = f"google_calendar_{re.sub(r'[^a-zA-Z0-9]', '_', ics_file.stem)}"
        file_size = ics_file.stat().st_size
        if _read_watermark(label) >= file_size:
            continue

        with dst.open("a") as f_out:
            for evt in _parse_ics(ics_file):
                uid = evt.get("UID") or hashlib.sha256(json.dumps(evt, sort_keys=True).encode()).hexdigest()
                rec_id = f"google_calendar:{hashlib.sha256(uid.encode()).hexdigest()[:24]}"
                if rec_id in existing_ids:
                    continue
                start = _parse_dt(evt.get("DTSTART", ""))
                end = _parse_dt(evt.get("DTEND", ""))
                record = {
                    "id": rec_id,
                    "type": "calendar_event",
                    "title": evt.get("SUMMARY", ""),
                    "content": evt.get("DESCRIPTION") or evt.get("LOCATION"),
                    "url": evt.get("URL"),
                    "created_at": start,
                    "observed_at": start,
                    "end_at": end,
                    "location": evt.get("LOCATION"),
                    "calendar": ics_file.stem,
                    "meta": {
                        "platform": "google_calendar",
                        "record_type": "activity",
                        "collection_method": "full_export",
                        "run_at": run_at,
                        "source_file": ics_file.name,
                    },
                }
                f_out.write(json.dumps(record, ensure_ascii=True) + "\n")
                existing_ids.add(rec_id)
                added += 1

        _write_watermark(label, file_size)

    if added:
        print(f"  google_calendar: +{added:,} events")
    return added


# ── Google Fit ──────────────────────────────────────────────────────────────────

# com.google.activity.segment intVal → human name
_FIT_TYPES = {
    0: "in_vehicle", 1: "on_bicycle", 2: "on_foot",
    7: "walking", 8: "running", 9: "aerobics",
    14: "biking", 15: "mountain_biking", 16: "road_biking",
    17: "spinning", 18: "stationary_biking",
    26: "elliptical", 37: "hiit", 38: "hiking",
    43: "indoor_running", 44: "ice_skating", 45: "jump_rope",
    46: "kayaking", 47: "kettlebell", 55: "pilates",
    59: "rowing", 60: "rowing_machine", 64: "treadmill_running",
    72: "snowboarding", 74: "snowshoeing", 75: "squash",
    76: "stair_climbing", 79: "swimming", 82: "table_tennis",
    84: "tennis", 85: "treadmill", 93: "weightlifting",
    96: "yoga",
}
# Skip noise: still(3), unknown(4), tilting(5), in_vehicle(0)
_FIT_SKIP = {0, 3, 4, 5}


def normalize_google_fit() -> int:
    """Parse Google Fit activity segments → connector activities.jsonl."""
    fit_dir = TAKEOUT_ROOT / "Fit" / "All Data"
    if not fit_dir.exists():
        return 0

    dst = CONNECTOR_NORMALIZED / "google_takeout_activities.jsonl"
    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    for fit_file in sorted(fit_dir.glob("derived_com.google.activity.segment_*.json")):
        label = f"google_fit_{fit_file.stem}"
        file_size = fit_file.stat().st_size
        if _read_watermark(label) >= file_size:
            continue

        try:
            data = json.loads(fit_file.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            _write_watermark(label, file_size)
            continue

        with dst.open("a") as f_out:
            for dp in data.get("Data Points", []):
                start_ns = dp.get("startTimeNanos")
                end_ns = dp.get("endTimeNanos")
                if not start_ns:
                    continue

                act_int: int | None = None
                for fv in dp.get("fitValue", []):
                    v = fv.get("value", {})
                    if isinstance(v, dict) and "intVal" in v:
                        act_int = v["intVal"]
                        break

                if act_int in _FIT_SKIP:
                    continue

                act_type = _FIT_TYPES.get(act_int, "activity") if act_int is not None else "activity"
                start_iso = _nano_to_iso(start_ns)
                end_iso = _nano_to_iso(end_ns) if end_ns else None
                key = f"{start_ns}:{end_ns}:{act_int}"
                rec_id = f"google_fit:{hashlib.sha256(key.encode()).hexdigest()[:24]}"
                if rec_id in existing_ids:
                    continue

                record = {
                    "id": rec_id,
                    "type": act_type,
                    "title": act_type.replace("_", " ").title(),
                    "created_at": start_iso,
                    "observed_at": start_iso,
                    "end_at": end_iso,
                    "meta": {
                        "platform": "google_fit",
                        "record_type": "activity",
                        "collection_method": "full_export",
                        "run_at": run_at,
                        "source_file": fit_file.name,
                    },
                }
                f_out.write(json.dumps(record, ensure_ascii=True) + "\n")
                existing_ids.add(rec_id)
                added += 1

        _write_watermark(label, file_size)

    if added:
        print(f"  google_fit: +{added:,} activity segments")
    return added


# ── My Activity HTML ────────────────────────────────────────────────────────────

_ACTIVITY_MARKER = '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'

# service folder name → (platform tag, default event_type)
# YouTube is already handled by build_canonical_ndjson.py, skip here
_MY_ACTIVITY_SERVICES: dict[str, tuple[str, str]] = {
    "Maps": ("google_maps", "maps_activity"),
    "Search": ("google_search", "searched"),
    "Chrome": ("google_chrome", "web_visit"),
    "Discover": ("google_discover", "viewed_article"),
    "Google Play Store": ("google_play", "app_activity"),
    "Books": ("google_books", "viewed_book"),
    "Image Search": ("google_image_search", "searched"),
    "Shopping": ("google_shopping", "viewed_product"),
    "Video Search": ("google_video_search", "searched"),
    "News": ("google_news", "viewed_article"),
}


def _parse_my_activity_html(
    path: Path, platform: str, default_event_type: str, run_at: str, existing_ids: set
) -> list[dict]:
    try:
        html = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []

    records: list[dict] = []
    for idx, block in enumerate(html.split(_ACTIVITY_MARKER)[1:]):
        snippet = block.split("</div>", 1)[0]
        anchors = [
            {
                "href": unescape(m.group(1)),
                "text": re.sub(r"\s+", " ", unescape(m.group(2))).strip(),
            }
            for m in re.finditer(r'<a href="([^"]+)">([\s\S]*?)</a>', snippet)
        ]
        text = unescape(re.sub(r"<[^>]+>", "\n", snippet))
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            continue

        timestamp_text = lines[-1]
        action_verb = lines[0].lower()
        title = anchors[0]["text"] if anchors else (lines[1] if len(lines) > 1 else lines[0])
        url = anchors[0]["href"] if anchors else None

        event_type = default_event_type
        if "searched" in action_verb:
            event_type = "searched"
        elif "visited" in action_verb or "viewed" in action_verb:
            event_type = "viewed_page"
        elif "used" in action_verb:
            event_type = "used_app"

        occurred = _parse_dt(timestamp_text)
        rec_id = f"google_activity:{platform}:{hashlib.sha256(f'{idx}:{timestamp_text}:{title}'.encode()).hexdigest()[:24]}"
        if rec_id in existing_ids:
            continue

        record = {
            "id": rec_id,
            "type": event_type,
            "title": title,
            "url": url,
            "created_at": occurred,
            "observed_at": occurred,
            "meta": {
                "platform": platform,
                "record_type": "activity",
                "collection_method": "full_export",
                "run_at": run_at,
                "source_file": path.name,
            },
        }
        records.append(record)
        existing_ids.add(rec_id)

    return records


def normalize_my_activity() -> int:
    """Parse My Activity HTML files for Maps, Search, Chrome, etc. → connector activities.jsonl."""
    my_activity_root = TAKEOUT_ROOT / "My Activity"
    if not my_activity_root.exists():
        return 0

    dst = CONNECTOR_NORMALIZED / "google_takeout_activities.jsonl"
    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    total_added = 0

    for service_dir in sorted(my_activity_root.iterdir()):
        if not service_dir.is_dir() or service_dir.name not in _MY_ACTIVITY_SERVICES:
            continue
        platform, default_event_type = _MY_ACTIVITY_SERVICES[service_dir.name]
        html_file = service_dir / "MyActivity.html"
        if not html_file.exists():
            continue

        label = f"google_my_activity_{re.sub(r'[^a-zA-Z0-9]', '_', service_dir.name)}"
        file_size = html_file.stat().st_size
        if _read_watermark(label) >= file_size:
            continue

        records = _parse_my_activity_html(html_file, platform, default_event_type, run_at, existing_ids)
        if records:
            with dst.open("a") as f_out:
                for r in records:
                    f_out.write(json.dumps(r, ensure_ascii=True) + "\n")
            total_added += len(records)
            print(f"  google_my_activity/{service_dir.name}: +{len(records):,}")

        _write_watermark(label, file_size)

    return total_added


# ── Gmail MBOX ──────────────────────────────────────────────────────────────────

_MBOX_PATH = TAKEOUT_ROOT / "Mail" / "All mail Including Spam and Trash.mbox"


def _decode_header(raw: str | None) -> str:
    if not raw:
        return ""
    try:
        parts = email.header.decode_header(raw)
        decoded = []
        for part, enc in parts:
            if isinstance(part, bytes):
                decoded.append(part.decode(enc or "utf-8", errors="replace"))
            else:
                decoded.append(str(part))
        return " ".join(decoded)
    except Exception:
        return str(raw or "")


def _extract_text_body(msg) -> str:
    """Extract plain-text body from a mail.Message, handling MIME multipart."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")[:2000]
                except Exception:
                    pass
        return ""
    try:
        payload = msg.get_payload(decode=True)
        charset = msg.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace")[:2000] if payload else ""
    except Exception:
        return ""


def normalize_gmail() -> int:
    """Stream the Gmail MBOX export → connector messages.jsonl."""
    if not _MBOX_PATH.exists():
        return 0

    label = "google_gmail_mbox"
    file_size = _MBOX_PATH.stat().st_size
    if _read_watermark(label) >= file_size:
        return 0

    dst = CONNECTOR_NORMALIZED / "google_takeout_gmail_messages.jsonl"
    existing_ids = _load_existing_ids(dst)
    run_at = _now_iso()
    added = 0

    print(f"  gmail: opening {_MBOX_PATH.stat().st_size / 1e6:.0f}MB MBOX (this may take a few minutes)…")

    try:
        mbox = mailbox.mbox(str(_MBOX_PATH))
    except Exception as e:
        print(f"  gmail: failed to open MBOX: {e}")
        return 0

    with dst.open("a") as f_out:
        for i, msg in enumerate(mbox):
            if i % 10_000 == 0 and i > 0:
                print(f"  gmail: processed {i:,} messages, added {added:,}…")

            subject = _decode_header(msg.get("Subject"))
            sender = _decode_header(msg.get("From"))
            recipient = _decode_header(msg.get("To"))
            date_str = msg.get("Date", "")
            msg_id_raw = msg.get("Message-ID", "")
            occurred = _parse_dt(date_str)

            # Dedup key: prefer Message-ID, fall back to hash of sender+subject+date
            if msg_id_raw:
                rec_id = f"gmail:{hashlib.sha256(msg_id_raw.strip().encode()).hexdigest()[:24]}"
            else:
                rec_id = f"gmail:{hashlib.sha256(f'{sender}:{subject}:{date_str}'.encode()).hexdigest()[:24]}"

            if rec_id in existing_ids:
                continue

            # Grab first 500 chars of body for context (skip attachments)
            snippet = _extract_text_body(msg)[:500].strip()

            record = {
                "id": rec_id,
                "type": "email",
                "title": subject,
                "content": snippet or None,
                "created_at": occurred,
                "observed_at": occurred,
                "role": "participant",
                "meta": {
                    "platform": "gmail",
                    "record_type": "message",
                    "collection_method": "full_export",
                    "run_at": run_at,
                    "sender": sender,
                    "recipient": recipient,
                    "source_file": "Mail/All mail Including Spam and Trash.mbox",
                },
            }
            f_out.write(json.dumps(record, ensure_ascii=True) + "\n")
            existing_ids.add(rec_id)
            added += 1

    _write_watermark(label, file_size)
    print(f"  gmail: +{added:,} emails")
    return added


# ── Entry point ─────────────────────────────────────────────────────────────────

def normalize_all_takeout() -> None:
    """Run all Google Takeout normalizers. Safe to call on every sync — watermarks skip done work."""
    if not TAKEOUT_ROOT.exists():
        print(f"  google_takeout: Takeout not found at {TAKEOUT_ROOT}")
        return

    normalize_google_chat()
    normalize_google_calendar()
    normalize_google_fit()
    normalize_my_activity()
    normalize_gmail()


if __name__ == "__main__":
    normalize_all_takeout()
