#!/usr/bin/env python3
"""Watch an IMAP inbox and ingest links/attachments into meta/raw/email-inbox."""

from __future__ import annotations

import argparse
import email
import imaplib
import json
import os
import re
import time
import zipfile
from datetime import datetime, timezone
from email.header import decode_header
from email.message import Message
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

URL_RE = re.compile(r"https?://[^\s<>'\"]+")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def decode_mime_header(value: Optional[str]) -> str:
    if not value:
        return ""
    parts = []
    for chunk, charset in decode_header(value):
        if isinstance(chunk, bytes):
            parts.append(chunk.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts)


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "unknown"


def ensure_dirs(base: Path) -> Dict[str, Path]:
    paths = {
        "base": base,
        "messages": base / "messages",
        "attachments": base / "attachments",
        "unpacked": base / "unpacked",
        "state": base / "state",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def load_state(state_file: Path) -> Dict[str, object]:
    if not state_file.exists():
        return {"last_uid": 0, "processed_uids": []}
    with state_file.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state_file: Path, state: Dict[str, object]) -> None:
    with state_file.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=True)
        f.write("\n")


def match_filters(sender: str, subject: str, sender_filters: List[str], subject_filters: List[str]) -> bool:
    sender_l = sender.lower()
    subject_l = subject.lower()

    if sender_filters:
        if not any(token.lower() in sender_l for token in sender_filters):
            return False
    if subject_filters:
        if not any(token.lower() in subject_l for token in subject_filters):
            return False
    return True


def extract_text_parts(msg: Message) -> Tuple[str, str]:
    text_parts: List[str] = []
    html_parts: List[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if ctype == "text/plain":
                text_parts.append(decoded)
            elif ctype == "text/html":
                html_parts.append(decoded)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if msg.get_content_type() == "text/html":
                html_parts.append(decoded)
            else:
                text_parts.append(decoded)

    return "\n".join(text_parts), "\n".join(html_parts)


def extract_links(text: str) -> List[str]:
    links = URL_RE.findall(text or "")
    # Basic trim for trailing punctuation artifacts
    cleaned = [u.rstrip(").,;\"") for u in links]
    # Stable unique order
    seen = set()
    out = []
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def save_attachments(msg: Message, msg_dir: Path, unzip_archives: bool, unpack_dir: Path) -> List[Dict[str, object]]:
    saved: List[Dict[str, object]] = []
    for part in msg.walk():
        disp = (part.get("Content-Disposition") or "").lower()
        filename = part.get_filename()
        if not filename and "attachment" not in disp:
            continue
        raw = part.get_payload(decode=True)
        if raw is None:
            continue

        decoded_name = decode_mime_header(filename or "attachment.bin")
        clean_name = safe_name(decoded_name)
        out_path = msg_dir / clean_name
        out_path.write_bytes(raw)

        item = {
            "filename": clean_name,
            "path": str(out_path),
            "size_bytes": len(raw),
            "content_type": part.get_content_type(),
            "unpacked_paths": [],
        }

        if unzip_archives and out_path.suffix.lower() == ".zip":
            zip_target = unpack_dir / out_path.stem
            zip_target.mkdir(parents=True, exist_ok=True)
            try:
                with zipfile.ZipFile(out_path, "r") as zf:
                    zf.extractall(zip_target)
                item["unpacked_paths"] = [str(p) for p in zip_target.rglob("*") if p.is_file()]
            except zipfile.BadZipFile:
                pass

        saved.append(item)

    return saved


def fetch_new_uids(conn: imaplib.IMAP4_SSL, last_uid: int) -> List[int]:
    criteria = f"UID {last_uid + 1}:*"
    status, data = conn.uid("SEARCH", None, criteria)
    if status != "OK" or not data or not data[0]:
        return []
    return [int(x) for x in data[0].decode().split() if x.isdigit()]


def process_message(
    conn: imaplib.IMAP4_SSL,
    uid: int,
    dirs: Dict[str, Path],
    sender_filters: List[str],
    subject_filters: List[str],
    unzip_archives: bool,
    links_jsonl: Path,
) -> Optional[Dict[str, object]]:
    status, data = conn.uid("FETCH", str(uid), "(RFC822)")
    if status != "OK" or not data or data[0] is None:
        return None

    raw_bytes = data[0][1]
    msg = email.message_from_bytes(raw_bytes)

    subject = decode_mime_header(msg.get("Subject"))
    sender = decode_mime_header(msg.get("From"))
    message_id = decode_mime_header(msg.get("Message-ID")) or f"uid-{uid}"
    date_raw = decode_mime_header(msg.get("Date"))

    if not match_filters(sender, subject, sender_filters, subject_filters):
        return {
            "uid": uid,
            "message_id": message_id,
            "subject": subject,
            "from": sender,
            "date": date_raw,
            "matched": False,
            "links": [],
            "attachments": [],
            "saved_at": now_iso(),
        }

    text_part, html_part = extract_text_parts(msg)
    links = extract_links(text_part + "\n" + html_part)

    msg_key = safe_name(f"{uid}-{message_id}")
    msg_json_path = dirs["messages"] / f"{msg_key}.json"
    msg_attach_dir = dirs["attachments"] / msg_key
    msg_unpack_dir = dirs["unpacked"] / msg_key
    msg_attach_dir.mkdir(parents=True, exist_ok=True)
    msg_unpack_dir.mkdir(parents=True, exist_ok=True)

    attachments = save_attachments(msg, msg_attach_dir, unzip_archives, msg_unpack_dir)

    record = {
        "uid": uid,
        "message_id": message_id,
        "subject": subject,
        "from": sender,
        "date": date_raw,
        "matched": True,
        "links": links,
        "attachments": attachments,
        "saved_at": now_iso(),
    }

    with msg_json_path.open("w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=True)
        f.write("\n")

    with links_jsonl.open("a", encoding="utf-8") as f:
        for link in links:
            f.write(
                json.dumps(
                    {
                        "saved_at": record["saved_at"],
                        "uid": uid,
                        "message_id": message_id,
                        "from": sender,
                        "subject": subject,
                        "date": date_raw,
                        "url": link,
                        "message_record": str(msg_json_path),
                    },
                    ensure_ascii=True,
                )
                + "\n"
            )

    return record


def run_once(
    host: str,
    user: str,
    password: str,
    mailbox: str,
    state_file: Path,
    dirs: Dict[str, Path],
    sender_filters: List[str],
    subject_filters: List[str],
    unzip_archives: bool,
) -> Dict[str, int]:
    state = load_state(state_file)
    last_uid = int(state.get("last_uid", 0))
    processed_uids = set(int(x) for x in state.get("processed_uids", []) if str(x).isdigit())

    summary = {"fetched": 0, "matched": 0, "links": 0, "attachments": 0}
    links_jsonl = dirs["base"] / "links.jsonl"

    conn = imaplib.IMAP4_SSL(host)
    try:
        conn.login(user, password)
        status, _ = conn.select(mailbox)
        if status != "OK":
            raise RuntimeError(f"Unable to open mailbox: {mailbox}")

        uids = fetch_new_uids(conn, last_uid)
        summary["fetched"] = len(uids)

        for uid in uids:
            if uid in processed_uids:
                continue

            record = process_message(
                conn,
                uid,
                dirs,
                sender_filters,
                subject_filters,
                unzip_archives,
                links_jsonl,
            )
            if record is None:
                continue

            processed_uids.add(uid)
            last_uid = max(last_uid, uid)

            if record.get("matched"):
                summary["matched"] += 1
                summary["links"] += len(record.get("links", []))
                summary["attachments"] += len(record.get("attachments", []))

    finally:
        try:
            conn.logout()
        except Exception:
            pass

    state["last_uid"] = last_uid
    # Keep only a tail to avoid unbounded growth.
    state["processed_uids"] = sorted(processed_uids)[-5000:]
    state["updated_at"] = now_iso()
    save_state(state_file, state)

    return summary


def parse_csv_list(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest email links/attachments from IMAP inbox")
    p.add_argument("--imap-host", default="imap.gmail.com", help="IMAP host")
    p.add_argument("--imap-user", required=True, help="IMAP username/email")
    p.add_argument("--password-env", default="EMAIL_APP_PASSWORD", help="Environment variable with IMAP app password")
    p.add_argument("--mailbox", default="INBOX", help="Mailbox folder")
    p.add_argument("--sender-filter", default="no-reply@accounts.google.com,mail-noreply@google.com,team@anthropic.com", help="Comma-separated sender substrings")
    p.add_argument("--subject-filter", default="takeout,claude,export,data", help="Comma-separated subject substrings")
    p.add_argument("--output-root", default="/path/to/data-connect/meta/raw/email-inbox", help="Output root for email artifacts")
    p.add_argument("--state-file", default="/path/to/data-connect/meta/manifests/email_ingest_state.json", help="State file path")
    p.add_argument("--loop", action="store_true", help="Run continuously")
    p.add_argument("--poll-interval", type=int, default=120, help="Polling interval in seconds when --loop is set")
    p.add_argument("--unzip-attachments", action="store_true", help="Automatically unzip .zip attachments")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    password = os.getenv(args.password_env)
    if not password:
        raise SystemExit(f"Missing env var: {args.password_env}")

    dirs = ensure_dirs(Path(args.output_root))
    state_file = Path(args.state_file)
    state_file.parent.mkdir(parents=True, exist_ok=True)

    sender_filters = parse_csv_list(args.sender_filter)
    subject_filters = parse_csv_list(args.subject_filter)

    def do_run() -> None:
        summary = run_once(
            host=args.imap_host,
            user=args.imap_user,
            password=password,
            mailbox=args.mailbox,
            state_file=state_file,
            dirs=dirs,
            sender_filters=sender_filters,
            subject_filters=subject_filters,
            unzip_archives=args.unzip_attachments,
        )
        print(json.dumps({"ts": now_iso(), **summary}, ensure_ascii=True))

    if args.loop:
        while True:
            try:
                do_run()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                print(json.dumps({"ts": now_iso(), "error": str(exc)}, ensure_ascii=True))
            time.sleep(max(15, args.poll_interval))
    else:
        do_run()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
