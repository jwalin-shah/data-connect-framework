#!/usr/bin/env python3
"""Ingest the official Instagram data archive into the local normalized lane.

Covers everything in the unzipped archive:
  - Topics Instagram assigned you
  - Liked posts (836)
  - Posts + videos viewed
  - Ads clicked / advertisers with your data (37K — strong interest signal)
  - Followers / Following (people graph)
  - DM conversations (214 threads)
  - Saved posts
  - Story interactions
  - Searches, link history, comments

Usage:
    python3 ingest_instagram_archive.py
    python3 ingest_instagram_archive.py --archive-root ~/Downloads/instagram_export_1
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ARCHIVE_ROOT = Path.home() / "Downloads/instagram_export_1"
OUTPUT_ROOT = Path("/path/to/data-connect/lanes/local/normalized/instagram")


def _write_jsonl(path: Path, rows: list[dict]) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
    return len(rows)


def _load(path: Path) -> list | dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _unwrap_list(raw: list | dict | None, *keys: str) -> list:
    """Extract a list from Instagram's wrapped dict or return raw list."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    for k in keys:
        if k in raw and isinstance(raw[k], list):
            return raw[k]
    # Try first list value
    for v in raw.values():
        if isinstance(v, list):
            return v
    return []


def _string_value(entry: dict) -> str:
    smd = entry.get("string_map_data", {})
    for key in ("Name", "Title", "Value", "Username"):
        if key in smd:
            return smd[key].get("value", "") or ""
    if smd:
        return next(iter(smd.values())).get("value", "") or ""
    return entry.get("title", "") or entry.get("value", "") or ""


def _string_href(entry: dict) -> str:
    for sv in entry.get("string_map_data", {}).values():
        href = sv.get("href", "") or ""
        if href:
            return href
    return ""


def _string_timestamp(entry: dict) -> int | None:
    for sv in entry.get("string_map_data", {}).values():
        ts = sv.get("timestamp")
        if ts:
            return ts
    return entry.get("timestamp") or None


def _uid(prefix: str, key: str) -> str:
    return f"instagram:{prefix}:{hashlib.sha256(key.encode()).hexdigest()[:20]}"


# ── Individual ingestion functions ────────────────────────────────────────────

def ingest_topics(archive: Path, out: Path) -> int:
    raw = _load(archive / "preferences/your_topics/recommended_topics.json")
    entries = _unwrap_list(raw, "topics_your_topics")
    rows = []
    for e in entries:
        name = _string_value(e)
        if not name:
            continue
        rows.append({
            "id": _uid("topic", name),
            "type": "instagram_topic",
            "topic": name,
            "meta": {"platform": "instagram", "record_type": "activity"},
        })
    n = _write_jsonl(out / "topics.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram topics")
    return n


def ingest_liked_posts(archive: Path, out: Path) -> int:
    raw = _load(archive / "your_instagram_activity/likes/liked_posts.json")
    entries = _unwrap_list(raw, "likes_media_likes")
    rows = []
    for e in entries:
        href = _string_href(e)
        title = _string_value(e)
        ts = _string_timestamp(e)
        rows.append({
            "id": _uid("liked", href or title),
            "type": "instagram_like",
            "title": title,
            "url": href,
            "timestamp": ts,
            "meta": {"platform": "instagram", "record_type": "activity"},
        })
    n = _write_jsonl(out / "liked_posts.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram liked posts")
    return n


def ingest_saved_posts(archive: Path, out: Path) -> int:
    raw = _load(archive / "your_instagram_activity/saved/saved_posts.json")
    entries = _unwrap_list(raw, "saved_saved_media")
    rows = []
    for e in entries:
        href = _string_href(e)
        title = _string_value(e)
        ts = _string_timestamp(e)
        rows.append({
            "id": _uid("saved", href or title),
            "type": "instagram_saved",
            "title": title,
            "url": href,
            "timestamp": ts,
            "meta": {"platform": "instagram", "record_type": "activity"},
        })
    n = _write_jsonl(out / "saved_posts.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram saved posts")
    return n


def ingest_comments(archive: Path, out: Path) -> int:
    total = 0
    rows = []
    for src in [
        archive / "your_instagram_activity/comments/post_comments_1.json",
        archive / "your_instagram_activity/comments/reels_comments.json",
    ]:
        raw = _load(src)
        entries = _unwrap_list(raw, "comments_media_comments")
        for e in entries:
            text = _string_value(e)
            ts = _string_timestamp(e)
            rows.append({
                "id": _uid("comment", (text or "") + str(ts or "")),
                "type": "instagram_comment",
                "content": text,
                "timestamp": ts,
                "meta": {"platform": "instagram", "record_type": "activity"},
            })
    n = _write_jsonl(out / "comments.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram comments")
    return n


def ingest_story_interactions(archive: Path, out: Path) -> int:
    rows = []
    for fname, event_type in [
        ("story_likes.json", "instagram_story_like"),
        ("polls.json", "instagram_poll"),
        ("questions.json", "instagram_question"),
        ("quizzes.json", "instagram_quiz"),
        ("emoji_sliders.json", "instagram_emoji_slider"),
    ]:
        raw = _load(archive / "your_instagram_activity/story_interactions" / fname)
        for e in _unwrap_list(raw):
            text = _string_value(e)
            ts = _string_timestamp(e)
            rows.append({
                "id": _uid(event_type, (text or "") + str(ts or "")),
                "type": event_type,
                "content": text,
                "timestamp": ts,
                "meta": {"platform": "instagram", "record_type": "activity"},
            })
    n = _write_jsonl(out / "story_interactions.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram story interactions")
    return n


def ingest_ads_and_advertisers(archive: Path, out: Path) -> int:
    # Advertisers who used your data — 37K, strong interest signal
    raw = _load(archive / "ads_information/instagram_ads_and_businesses/advertisers_using_your_activity_or_information.json")
    entries = _unwrap_list(raw, "ig_custom_audiences_all_types")
    adv_rows = []
    for e in entries:
        name = e.get("advertiser_name") or _string_value(e)
        has_data = e.get("has_data_file_custom_audience", False)
        has_remarketing = e.get("has_remarketing_custom_audience", False)
        adv_rows.append({
            "id": _uid("advertiser", name),
            "type": "instagram_advertiser",
            "advertiser": name,
            "has_data_file": has_data,
            "has_remarketing": has_remarketing,
            "meta": {"platform": "instagram", "record_type": "activity"},
        })
    n1 = _write_jsonl(out / "advertisers.jsonl", adv_rows)
    if n1:
        print(f"  {n1:,} advertisers with your Instagram data")

    # Ads clicked + viewed
    ad_rows = []
    for fname, event_type in [
        ("ads_clicked.json", "instagram_ad_clicked"),
        ("ads_viewed.json", "instagram_ad_viewed"),
    ]:
        raw = _load(archive / "ads_information/ads_and_topics" / fname)
        for e in _unwrap_list(raw, "impressions_history_ads_seen"):
            title = _string_value(e)
            ts = _string_timestamp(e)
            ad_rows.append({
                "id": _uid(event_type, (title or "") + str(ts or "")),
                "type": event_type,
                "title": title,
                "timestamp": ts,
                "meta": {"platform": "instagram", "record_type": "activity"},
            })
    n2 = _write_jsonl(out / "ads_interactions.jsonl", ad_rows)
    if n2:
        print(f"  {n2:,} Instagram ad interactions")

    # Posts viewed + videos watched
    view_rows = []
    for fname, event_type in [
        ("posts_viewed.json", "instagram_post_viewed"),
        ("videos_watched.json", "instagram_video_watched"),
    ]:
        raw = _load(archive / "ads_information/ads_and_topics" / fname)
        for e in _unwrap_list(raw, "impressions_history_posts_seen", "impressions_history_videos_watched"):
            title = _string_value(e)
            ts = _string_timestamp(e)
            view_rows.append({
                "id": _uid(event_type, (title or "") + str(ts or "")),
                "type": event_type,
                "title": title,
                "timestamp": ts,
                "meta": {"platform": "instagram", "record_type": "activity"},
            })
    n3 = _write_jsonl(out / "content_viewed.jsonl", view_rows)
    if n3:
        print(f"  {n3:,} Instagram posts/videos viewed")

    return n1 + n2 + n3


def ingest_following(archive: Path, out: Path) -> int:
    rows = []
    for fname, rel_type in [
        ("connections/followers_and_following/following.json", "following"),
        ("connections/followers_and_following/followers_1.json", "follower"),
    ]:
        raw = _load(archive / fname)
        entries = _unwrap_list(raw, "relationships_following", "relationships_followers")
        for e in entries:
            handle = _string_value(e)
            href = _string_href(e)
            ts = _string_timestamp(e)
            rows.append({
                "id": _uid(rel_type, handle or href),
                "type": f"instagram_{rel_type}",
                "handle": handle,
                "url": href,
                "timestamp": ts,
                "meta": {"platform": "instagram", "record_type": "people"},
            })
    n = _write_jsonl(out / "connections.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram followers/following")
    return n


def ingest_messages(archive: Path, out: Path) -> int:
    inbox_root = archive / "your_instagram_activity/messages/inbox"
    if not inbox_root.exists():
        return 0
    rows = []
    thread_count = 0
    for thread_dir in inbox_root.iterdir():
        if not thread_dir.is_dir():
            continue
        thread_count += 1
        thread_name = thread_dir.name
        for msg_file in sorted(thread_dir.glob("message_*.json")):
            raw = _load(msg_file)
            if not raw:
                continue
            messages = raw.get("messages", [])
            participants = raw.get("participants", [])
            for msg in messages:
                sender = msg.get("sender_name", "")
                ts_ms = msg.get("timestamp_ms")
                ts = int(ts_ms / 1000) if ts_ms else None
                content = msg.get("content", "") or ""
                rows.append({
                    "id": _uid("dm", f"{thread_name}:{sender}:{ts}"),
                    "type": "instagram_dm",
                    "thread": thread_name,
                    "sender": sender,
                    "content": content[:500],
                    "timestamp": ts,
                    "participants": [p.get("name") for p in participants],
                    "meta": {"platform": "instagram", "record_type": "message"},
                })
    n = _write_jsonl(out / "messages.jsonl", rows)
    if n:
        print(f"  {n:,} Instagram DMs across {thread_count} conversations")
    return n


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-root", default=str(ARCHIVE_ROOT))
    args = parser.parse_args()

    archive = Path(args.archive_root)
    if not archive.exists():
        print(f"Archive not found: {archive}")
        return 1

    out = OUTPUT_ROOT
    out.mkdir(parents=True, exist_ok=True)

    print(f"Ingesting Instagram archive from {archive}")
    total = 0
    total += ingest_topics(archive, out)
    total += ingest_liked_posts(archive, out)
    total += ingest_saved_posts(archive, out)
    total += ingest_comments(archive, out)
    total += ingest_story_interactions(archive, out)
    total += ingest_ads_and_advertisers(archive, out)
    total += ingest_following(archive, out)
    total += ingest_messages(archive, out)

    print(f"\nTotal: {total:,} records → {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
