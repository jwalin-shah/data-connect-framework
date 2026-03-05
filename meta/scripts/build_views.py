#!/usr/bin/env python3
"""Build app-facing view datasets from the canonical NDJSON layer."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load all rows from a JSONL file into a list. Use sparingly for small files."""
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    """Yield rows from a JSONL file one by one to save memory."""
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")
            count += 1
    return count


def tokenize(text: str) -> List[str]:
    return [token for token in re.findall(r"[a-zA-Z0-9][a-zA-Z0-9_+-]{2,}", text.lower()) if token not in STOP_WORDS]


STOP_WORDS = {
    "the", "and", "for", "with", "that", "this", "from", "your", "you", "are", "was", "but", "have", "not",
    "just", "like", "they", "them", "their", "into", "about", "what", "when", "where", "will", "would",
    "there", "then", "than", "also", "can", "could", "should", "using", "used", "user", "assistant", "text",
}

# Generic event-type strings that should never count as interest signals
_GENERIC_EVENT_TOKENS = {
    "message", "messages", "sent_message", "participant", "participants",
    "web_visit", "app_usage", "streamed_track", "stream", "calendar_event",
    "imessage", "email", "apple_mail", "spotify", "youtube", "chatgpt",
    "claude", "linkedin", "brave", "firefox", "macos_screentime",
    "added_to_playlist", "starred_repository", "saved_job", "saved_memory",
    "invitation", "sent_invitation", "observation", "snapshot",
}

# Platforms whose event titles are private/noisy and shouldn't drive interest profiles
_SKIP_INTEREST_PLATFORMS = {"imessage", "apple_mail", "macos_screentime", "calendar"}


def build_timeline(events_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build a sorted timeline of events. Since we need to sort, we still load into memory but only minimal fields."""
    rows = []
    for event in events_iter:
        rows.append(
            {
                "id": event["id"],
                "occurred_at": event.get("occurred_at"),
                "event_type": event.get("event_type"),
                "title": event.get("title"),
                "description": event.get("description"),
                "platform": event.get("platform"),
                "source_file": event.get("source_file"),
            }
        )
    rows.sort(key=lambda row: (row.get("occurred_at") or "", row["id"]), reverse=True)
    return rows


def build_source_overview(
    artifacts: List[Dict[str, Any]],
    events_iter: Iterable[Dict[str, Any]],
    validation: Dict[str, Any],
) -> List[Dict[str, Any]]:
    by_source: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "source": None,
            "artifact_count": 0,
            "event_count": 0,
            "last_imported_at": None,
            "health": "ok",
            "known_issues": [],
            "event_types": Counter(),
        }
    )

    for artifact in artifacts:
        source = artifact.get("platform") or "unknown"
        item = by_source[source]
        item["source"] = source
        item["artifact_count"] += 1

    for event in events_iter:
        source = event.get("platform") or "unknown"
        item = by_source[source]
        item["source"] = source
        item["event_count"] += 1
        item["event_types"][event.get("event_type") or "unknown"] += 1
        occurred_at = event.get("occurred_at")
        if occurred_at and (item["last_imported_at"] is None or occurred_at > item["last_imported_at"]):
            item["last_imported_at"] = occurred_at

    for issue in validation.get("issues", []):
        platform = issue.get("example", {}).get("meta", {}).get("platform")
        if not platform:
            continue
        item = by_source[platform]
        item["source"] = platform
        item["health"] = "needs_attention"
        item["known_issues"].append({"issue": issue.get("issue"), "kind": issue.get("kind")})

    rows = []
    for _, item in sorted(by_source.items()):
        rows.append(
            {
                "source": item["source"],
                "artifact_count": item["artifact_count"],
                "event_count": item["event_count"],
                "last_imported_at": item["last_imported_at"],
                "health": item["health"],
                "known_issues": item["known_issues"],
                "top_event_types": dict(item["event_types"].most_common(8)),
            }
        )
    return rows


def build_entity_profiles(entities: List[Dict[str, Any]], relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    owns_account: Dict[str, List[str]] = defaultdict(list)
    same_as: Dict[str, List[str]] = defaultdict(list)
    for rel in relationships:
        if rel.get("predicate") == "owns_account":
            owns_account[rel["subject_entity_id"]].append(rel["object_entity_id"])
        elif rel.get("predicate") == "same_as":
            same_as[rel["subject_entity_id"]].append(rel["object_entity_id"])
            same_as[rel["object_entity_id"]].append(rel["subject_entity_id"])

    rows = []
    for entity in entities:
        if entity.get("entity_type") not in {"person", "account"}:
            continue
        attrs = entity.get("attributes", {})
        rows.append(
            {
                "entity_id": entity["id"],
                "entity_type": entity.get("entity_type"),
                "canonical_name": entity.get("canonical_name"),
                "platform": entity.get("platform"),
                "aliases": same_as.get(entity["id"], []),
                "linked_accounts": owns_account.get(entity["id"], []),
                "identity_keys": entity.get("identity_keys", {}),
                "headline": attrs.get("headline") or attrs.get("bio") or attrs.get("company"),
                "location": attrs.get("location"),
                "first_seen_at": entity.get("first_seen_at"),
                "last_seen_at": entity.get("last_seen_at"),
                "observed_in": entity.get("observed_in", []),
            }
        )
    return rows


def build_interest_profiles(entities: List[Dict[str, Any]], events_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    token_counts: Counter[str] = Counter()
    evidence: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # Entity names carry strong signal (playlist names, repo names, track names, skills)
    for entity in entities:
        if entity.get("entity_type") not in {"repository", "playlist", "document", "media", "content", "skill", "project"}:
            continue
        for token in tokenize(entity.get("canonical_name") or ""):
            if token in _GENERIC_EVENT_TOKENS:
                continue
            token_counts[token] += 2
            if len(evidence[token]) < 5:
                evidence[token].append({"type": "entity", "id": entity["id"], "label": entity.get("canonical_name")})

    # Event analysis (streaming)
    domain_counts: Counter[str] = Counter()
    for event in events_iter:
        platform = event.get("platform") or ""
        
        # Domain frequency from browsers
        if platform in {"brave", "firefox"}:
            title = event.get("title") or ""
            url = (event.get("raw_payload") or {}).get("url") or event.get("url") or ""
            try:
                domain = url.split("//")[1].split("/")[0].lstrip("www.")
                if domain and "." in domain:
                    domain_counts[domain] += 1
            except Exception:
                pass
            for token in tokenize(title):
                if token in _GENERIC_EVENT_TOKENS:
                    continue
                token_counts[token] += 1
                if len(evidence[token]) < 5:
                    evidence[token].append({"type": "event", "id": event["id"], "label": title})
        
        # General interest tokens from other platforms
        elif platform not in _SKIP_INTEREST_PLATFORMS:
            text = " ".join(str(event.get(field) or "") for field in ("title", "description"))
            for token in tokenize(text):
                if token in _GENERIC_EVENT_TOKENS:
                    continue
                token_counts[token] += 1
                if len(evidence[token]) < 5:
                    evidence[token].append({"type": "event", "id": event["id"], "label": event.get("title")})

    rows = []
    for topic, score in token_counts.most_common(100):
        rows.append({"topic": topic, "score": score, "evidence": evidence[topic]})

    rows.append({
        "topic": "_domain_frequency",
        "score": sum(domain_counts.values()),
        "evidence": [{"type": "domain", "id": d, "label": d, "visits": c} for d, c in domain_counts.most_common(50)],
    })
    return rows


def build_behavior_profiles(events_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_platform: Dict[str, Counter[str]] = defaultdict(Counter)
    by_hour: Counter[str] = Counter()
    for event in events_iter:
        by_platform[event.get("platform") or "unknown"][event.get("event_type") or "unknown"] += 1
        occurred_at = event.get("occurred_at")
        if occurred_at and "T" in occurred_at:
            by_hour[occurred_at[11:13]] += 1

    rows = []
    for platform, counts in sorted(by_platform.items()):
        rows.append(
            {
                "platform": platform,
                "top_event_types": dict(counts.most_common(10)),
                "event_count": sum(counts.values()),
            }
        )

    rows.append(
        {
            "platform": "all",
            "top_hours_utc": dict(by_hour.most_common(8)),
            "event_count": sum(by_hour.values()),
        }
    )
    return rows


_PROFESSIONAL_PLATFORMS = {"linkedin"}


def build_knowledge_profiles(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    skills = []
    companies: Counter[str] = Counter()
    roles: Counter[str] = Counter()
    education: list[dict] = []

    for entity in entities:
        attrs = entity.get("attributes", {})
        platform = entity.get("platform") or ""

        if entity.get("entity_type") == "skill":
            skills.append({"name": entity.get("canonical_name"), "source": platform})

        if platform in _PROFESSIONAL_PLATFORMS:
            if attrs.get("company"):
                companies[str(attrs["company"])] += 1
            if attrs.get("title") and entity.get("entity_type") not in {"content", "media"}:
                roles[str(attrs["title"])] += 1
            if attrs.get("school") or attrs.get("degree"):
                education.append({
                    "school": attrs.get("school"),
                    "degree": attrs.get("degree"),
                    "field": attrs.get("field_of_study"),
                })

    rows = [{"kind": "skills", "items": skills[:100]}]
    rows.append({"kind": "companies", "items": [{"name": name, "count": count} for name, count in companies.most_common(25)]})
    rows.append({"kind": "roles", "items": [{"name": name, "count": count} for name, count in roles.most_common(25)]})
    if education:
        rows.append({"kind": "education", "items": education[:20]})
    return rows


def build_day_contexts(events_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build day-by-day summaries. To save memory, we only store minimal data per event during aggregation."""
    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in events_iter:
        occurred_at = event.get("occurred_at") or ""
        date = occurred_at[:10]
        if date:
            # Only store fields needed for the final view
            by_date[date].append({
                "occurred_at": event.get("occurred_at"),
                "title": event.get("title"),
                "description": event.get("description"),
                "platform": event.get("platform"),
                "event_type": event.get("event_type"),
            })

    rows = []
    for date, day_events in sorted(by_date.items()):
        day_events_sorted = sorted(day_events, key=lambda e: e.get("occurred_at") or "")
        platforms = sorted(set(e.get("platform") or "unknown" for e in day_events))
        text = " ".join(
            str(e.get("title") or "") + " " + str(e.get("description") or "")
            for e in day_events
        )
        top_tokens = [token for token, _ in Counter(tokenize(text)).most_common(15)]
        rows.append({
            "date": date,
            "event_count": len(day_events),
            "platforms": platforms,
            "top_events": day_events_sorted[:20],
            "top_tokens": top_tokens,
        })

    rows.sort(key=lambda r: r["date"], reverse=True)
    return rows


MESSAGE_TYPES = {"sent_message", "chatgpt_message", "mail_message", "received_message", "imessage"}
MESSAGE_TITLES = {"user", "assistant"}


def build_thread_summaries(events_iter: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group messages into threads. We only keep the last few events and metadata per thread in memory."""
    by_thread: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in events_iter:
        event_type = event.get("event_type") or ""
        title = event.get("title") or ""
        platform = event.get("platform") or "unknown"
        if event_type not in MESSAGE_TYPES and title not in MESSAGE_TITLES:
            continue

        if platform == "chatgpt":
            thread_key = f"chatgpt:{event.get('source_file') or 'unknown'}"
        elif platform == "mail":
            thread_key = f"mail:{title}"
        else:
            thread_key = f"{platform}:{title or event_type}"

        by_thread[thread_key].append({
            "occurred_at": event.get("occurred_at"),
            "description": event.get("description"),
            "platform": platform,
        })

    rows = []
    for thread_id, thread_events in by_thread.items():
        thread_events_sorted = sorted(thread_events, key=lambda e: e.get("occurred_at") or "")
        platform = thread_events[0].get("platform") or "unknown"
        last_event = thread_events_sorted[-1]
        snippet = (last_event.get("description") or "")[:200]
        rows.append({
            "thread_id": thread_id,
            "platform": platform,
            "message_count": len(thread_events),
            "first_at": thread_events_sorted[0].get("occurred_at"),
            "last_at": last_event.get("occurred_at"),
            "snippet": snippet,
        })

    rows.sort(key=lambda r: r["last_at"] or "", reverse=True)
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build app-facing view datasets from canonical NDJSON")
    parser.add_argument("--canonical-root", default="/path/to/data-connect/meta/lanes/connector/canonical")
    parser.add_argument("--views-root", default="/path/to/data-connect/meta/lanes/connector/views")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    canonical_root = Path(args.canonical_root)
    views_root = Path(args.views_root)
    views_root.mkdir(parents=True, exist_ok=True)

    # Small files can be loaded fully
    artifacts = load_jsonl(canonical_root / "artifacts.ndjson")
    entities = load_jsonl(canonical_root / "entities.ndjson")
    relationships = load_jsonl(canonical_root / "relationships.ndjson")
    
    validation_path = canonical_root / "validation_report.json"
    validation = json.loads(validation_path.read_text(encoding="utf-8")) if validation_path.exists() else {}

    events_path = canonical_root / "events.ndjson"

    # Multi-pass streaming to avoid loading all 1.6M events simultaneously
    timeline = build_timeline(iter_jsonl(events_path))
    source_overview = build_source_overview(artifacts, iter_jsonl(events_path), validation)
    entity_profiles = build_entity_profiles(entities, relationships)
    interest_profiles = build_interest_profiles(entities, iter_jsonl(events_path))
    behavior_profiles = build_behavior_profiles(iter_jsonl(events_path))
    knowledge_profiles = build_knowledge_profiles(entities)
    day_contexts = build_day_contexts(iter_jsonl(events_path))
    thread_summaries = build_thread_summaries(iter_jsonl(events_path))

    counts = {
        "timeline": write_jsonl(views_root / "timeline.jsonl", timeline),
        "source_overview": write_jsonl(views_root / "source_overview.jsonl", source_overview),
        "entity_profiles": write_jsonl(views_root / "entity_profiles.jsonl", entity_profiles),
        "interest_profiles": write_jsonl(views_root / "interest_profiles.jsonl", interest_profiles),
        "behavior_profiles": write_jsonl(views_root / "behavior_profiles.jsonl", behavior_profiles),
        "knowledge_profiles": write_jsonl(views_root / "knowledge_profiles.jsonl", knowledge_profiles),
        "day_contexts": write_jsonl(views_root / "day_contexts.jsonl", day_contexts),
        "thread_summaries": write_jsonl(views_root / "thread_summaries.jsonl", thread_summaries),
    }

    manifest = {
        "generated_at": now_iso(),
        "views_root": str(views_root),
        "counts": counts,
    }
    (views_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
