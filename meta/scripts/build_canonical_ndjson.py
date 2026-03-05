#!/usr/bin/env python3
"""Build canonical NDJSON records from normalized exports and YouTube Takeout."""

from __future__ import annotations

import argparse
import gzip
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class RecordStore:
    artifacts: List[Dict[str, Any]]
    entities: List[Dict[str, Any]]
    events: List[Dict[str, Any]]
    relationships: List[Dict[str, Any]]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


WATERMARK_DIR = Path("/path/to/data-connect/meta/working/merge_watermarks")


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_ids(path: Path) -> set:
    ids: set = set()
    if not path.exists():
        return ids
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    ids.add(json.loads(line)["id"])
                except (KeyError, json.JSONDecodeError):
                    pass
    return ids


def _watermark_path(label: str) -> Path:
    WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
    safe = label.replace("/", "_").replace(" ", "_")
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
    _watermark_path(label).write_text(json.dumps({"offset": offset, "updated_at": now_iso()}))


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]], compress: bool = False) -> int:
    """Write rows as NDJSON. If compress=True, also writes a .gz companion file."""
    count = 0
    lines: List[str] = []
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            line = json.dumps(row, ensure_ascii=True) + "\n"
            handle.write(line)
            if compress:
                lines.append(line)
            count += 1
    if compress and lines:
        gz_path = Path(str(path) + ".gz")
        with gzip.open(gz_path, "wt", encoding="utf-8", compresslevel=6) as gz:
            gz.writelines(lines)
    return count


def slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:96] or "unknown"


def stable_id(prefix: str, *parts: Any) -> str:
    joined = "|".join(str(part).strip() for part in parts if part not in (None, ""))
    return f"{prefix}:{slug(joined)}"


def parse_dt(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    # Accept numeric Unix timestamps (int or float)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return None
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        formats = [
            "%b %d, %Y, %I:%M:%S %p %Z",
            "%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                parsed = datetime.strptime(value, fmt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                continue
        return None


def append_unique(rows: List[Dict[str, Any]], seen: set[str], row: Dict[str, Any]) -> None:
    row_id = row["id"]
    if row_id in seen:
        return
    seen.add(row_id)
    rows.append(row)


def normalize_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = re.sub(r"\s+", " ", value.strip().lower())
    return normalized or None


def compact_values(*values: Any) -> List[str]:
    seen: set[str] = set()
    rows: List[str] = []
    for value in values:
        if value in (None, "", []):
            continue
        if isinstance(value, list):
            for item in compact_values(*value):
                if item not in seen:
                    seen.add(item)
                    rows.append(item)
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows


def make_provenance(row: Dict[str, Any], kind: str) -> Dict[str, Any]:
    meta = row.get("meta", {})
    source = row.get("source") or {}
    if not isinstance(source, dict):
        source = {}
    return {
        "kind": kind,
        "platform": meta.get("platform"),
        "scope": meta.get("scope"),
        "source_file": meta.get("source_file"),
        "run_id": meta.get("run_id"),
        "collected_at": source.get("collected_at"),
        "observed_at": row.get("observed_at"),
        "natural_key": meta.get("natural_key"),
        "snapshot_id": meta.get("snapshot_id"),
        "content_hash": meta.get("content_hash"),
    }


def entity_id(entity_type: str, platform: Optional[str], *parts: Any) -> str:
    return stable_id(f"entity:{entity_type}", platform or "unknown", *parts)


def event_id(event_type: str, platform: Optional[str], *parts: Any) -> str:
    return stable_id(f"event:{event_type}", platform or "unknown", *parts)


def relationship_id(predicate: str, *parts: Any) -> str:
    return stable_id(f"relationship:{predicate}", *parts)


def build_account_entity(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("meta", {})
    platform = meta.get("platform")
    native_key = row.get("id") or row.get("username") or row.get("uri") or row.get("url") or meta.get("natural_key")
    return {
        "id": entity_id("account", platform, native_key),
        "type": "entity",
        "entity_type": "account",
        "canonical_name": row.get("display_name") or row.get("username") or row.get("email") or str(native_key),
        "platform": platform,
        "attributes": row,
        "identity_keys": {
            "native_id": row.get("id"),
            "username": row.get("username"),
            "email": row.get("email"),
            "url": row.get("url"),
            "uri": row.get("uri"),
            "natural_key": meta.get("natural_key"),
        },
        "first_seen_at": parse_dt(row.get("observed_at") or row.get("source", {}).get("collected_at")),
        "last_seen_at": parse_dt(row.get("observed_at") or row.get("source", {}).get("collected_at")),
        "entity_version": meta.get("snapshot_id"),
        "is_latest": True,
        "provenance": make_provenance(row, "accounts"),
        "observed_in": compact_values(meta.get("source_file")),
    }


def build_person_entity(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("meta", {})
    platform = meta.get("platform")
    native_key = row.get("id") or row.get("email") or row.get("handle") or row.get("name") or meta.get("natural_key")
    return {
        "id": entity_id("person", platform, native_key),
        "type": "entity",
        "entity_type": "person",
        "canonical_name": row.get("name") or row.get("handle") or str(native_key),
        "platform": platform,
        "attributes": row,
        "identity_keys": {
            "native_id": row.get("id"),
            "handle": row.get("handle"),
            "email": row.get("email"),
            "name_normalized": normalize_name(row.get("name")),
            "natural_key": meta.get("natural_key"),
        },
        "first_seen_at": parse_dt(row.get("observed_at") or row.get("connected_at")),
        "last_seen_at": parse_dt(row.get("observed_at") or row.get("connected_at")),
        "entity_version": meta.get("snapshot_id"),
        "is_latest": True,
        "provenance": make_provenance(row, "people"),
        "observed_in": compact_values(meta.get("source_file")),
    }


def build_content_entity(row: Dict[str, Any], kind: str) -> Dict[str, Any]:
    meta = row.get("meta", {})
    platform = meta.get("platform")
    entity_type = {
        "documents": row.get("type") if row.get("type") in {"repository", "playlist", "project", "skill", "education", "position", "saved_job"} else "document",
        "media": "media",
        "conversations": "conversation",
    }[kind]
    native_key = row.get("id") or row.get("url") or row.get("uri") or row.get("name") or row.get("title") or meta.get("natural_key")
    canonical_name = row.get("title") or row.get("name") or row.get("full_name") or row.get("caption") or row.get("content") or str(native_key)
    return {
        "id": entity_id(entity_type, platform, native_key),
        "type": "entity",
        "entity_type": entity_type,
        "canonical_name": canonical_name,
        "platform": platform,
        "attributes": row,
        "identity_keys": {
            "native_id": row.get("id"),
            "url": row.get("url"),
            "uri": row.get("uri"),
            "natural_key": meta.get("natural_key"),
        },
        "first_seen_at": parse_dt(row.get("created_at") or row.get("observed_at")),
        "last_seen_at": parse_dt(row.get("updated_at") or row.get("created_at") or row.get("observed_at")),
        "entity_version": meta.get("snapshot_id"),
        "is_latest": True,
        "provenance": make_provenance(row, kind),
        "observed_in": compact_values(meta.get("source_file")),
    }


def build_message_event(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("meta", {})
    platform = meta.get("platform")
    role = row.get("role") or "unknown"
    event_type = "sent_message" if role in {"user", "assistant"} else "message"
    return {
        "id": event_id(event_type, platform, row.get("id"), row.get("conversation_id"), row.get("created_at")),
        "type": "event",
        "event_type": event_type,
        "occurred_at": parse_dt(row.get("created_at") or row.get("observed_at")),
        "title": role,
        "description": row.get("content"),
        "platform": platform,
        "source_file": meta.get("source_file"),
        "actor_entity_id": entity_id("account", platform, role),
        "subject_entity_id": entity_id("message", platform, row.get("id") or row.get("meta", {}).get("snapshot_id")),
        "object_entity_id": entity_id("conversation", platform, row.get("conversation_id")) if row.get("conversation_id") else None,
        "context_entity_id": entity_id("conversation", platform, row.get("conversation_id")) if row.get("conversation_id") else None,
        "raw_payload": row,
        "provenance": make_provenance(row, "messages"),
    }


def build_activity_event(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("meta", {})
    platform = meta.get("platform")
    raw_type = row.get("type") or f"{platform}_activity"
    canonical_type = {
        "playlist_track": "added_to_playlist",
        "starred_repository": "starred_repository",
        "stream": "streamed_track",
        "invitation": "sent_invitation",
    }.get(raw_type, raw_type)
    subject_key = row.get("track_uri") or row.get("repository") or row.get("url") or row.get("id")
    object_key = row.get("playlist_id")
    return {
        "id": event_id(canonical_type, platform, row.get("id"), row.get("created_at"), subject_key, object_key),
        "type": "event",
        "event_type": canonical_type,
        "occurred_at": parse_dt(row.get("created_at") or row.get("observed_at")),
        "title": row.get("track_name") or row.get("repository") or row.get("type"),
        "description": row.get("album") or row.get("playlist_id") or row.get("content"),
        "platform": platform,
        "source_file": meta.get("source_file"),
        "actor_entity_id": entity_id("account", platform, "self"),
        "subject_entity_id": entity_id("track", platform, subject_key) if row.get("track_name") or row.get("track_uri") else entity_id("content", platform, subject_key),
        "object_entity_id": entity_id("playlist", platform, object_key) if object_key else None,
        "context_entity_id": entity_id("playlist", platform, object_key) if object_key else None,
        "raw_payload": row,
        "provenance": make_provenance(row, "activities"),
    }


def build_document_event(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    meta = row.get("meta", {})
    platform = meta.get("platform")
    raw_type = row.get("type")
    event_type = {
        "saved_job": "saved_job",
        "memory": "saved_memory",
        "memory_summary": "saved_memory",
    }.get(raw_type)
    if not event_type:
        return None
    return {
        "id": event_id(event_type, platform, row.get("id"), row.get("created_at"), row.get("name")),
        "type": "event",
        "event_type": event_type,
        "occurred_at": parse_dt(row.get("created_at") or row.get("saved_at") or row.get("observed_at")),
        "title": row.get("name") or raw_type,
        "description": row.get("content") or row.get("company"),
        "platform": platform,
        "source_file": meta.get("source_file"),
        "actor_entity_id": entity_id("account", platform, "self"),
        "subject_entity_id": entity_id(raw_type or "document", platform, row.get("id") or row.get("name")),
        "object_entity_id": None,
        "context_entity_id": None,
        "raw_payload": row,
        "provenance": make_provenance(row, "documents"),
    }


def merge_entity(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    existing["observed_in"] = compact_values(existing.get("observed_in", []), incoming.get("observed_in", []))
    existing["last_seen_at"] = max(filter(None, [existing.get("last_seen_at"), incoming.get("last_seen_at")]), default=None)
    existing["first_seen_at"] = min(filter(None, [existing.get("first_seen_at"), incoming.get("first_seen_at")]), default=None)
    existing["attributes"] = incoming.get("attributes") or existing.get("attributes")
    return existing


def note_issue(validation: Dict[str, Any], issue_counts: Counter[str], kind_counts: Dict[str, Counter[str]], kind: str, issue: str, example: Dict[str, Any]) -> None:
    issue_counts[issue] += 1
    kind_counts[kind][issue] += 1
    if len(validation["issues"]) < 40:
        validation["issues"].append({"kind": kind, "issue": issue, "example": example})


def _iter_all_source_files(normalized_root: Path, kind: str):
    """Yield records from ALL source files for a given kind.

    Reads the primary file ({kind}.jsonl) first, then all per-source files
    ({source}_{kind}.jsonl).  This allows each source to own its own file while
    the canonical builder transparently aggregates them all.
    """
    primary = normalized_root / f"{kind}.jsonl"
    if primary.exists():
        yield from iter_jsonl(primary)
    for path in sorted(normalized_root.glob(f"*_{kind}.jsonl")):
        yield from iter_jsonl(path)


def build_from_normalized(
    normalized_root: Path,
    output_root: Path,
    source_index_path: Optional[Path],
    source_root: Optional[Path],
) -> tuple[RecordStore, Dict[str, Any]]:
    del output_root
    files = {
        "accounts": load_jsonl(normalized_root / "accounts.jsonl"),
        "people": list(_iter_all_source_files(normalized_root, "people")),
        "conversations": list(_iter_all_source_files(normalized_root, "conversations")),
        "messages": _iter_all_source_files(normalized_root, "messages"),
        "activities": _iter_all_source_files(normalized_root, "activities"),
        "documents": _iter_all_source_files(normalized_root, "documents"),
        "media": iter_jsonl(normalized_root / "media.jsonl"),
        "failures": load_jsonl(normalized_root / "failures.jsonl"),
    }

    store = RecordStore(artifacts=[], entities=[], events=[], relationships=[])
    artifacts_by_id: Dict[str, Dict[str, Any]] = {}
    entities_by_id: Dict[str, Dict[str, Any]] = {}
    events_by_id: Dict[str, Dict[str, Any]] = {}
    relationship_seen: set[str] = set()
    source_files: set[str] = set()

    validation: Dict[str, Any] = {"summary": {}, "issues": [], "by_kind": {}}
    issue_counts: Counter[str] = Counter()
    kind_counts: Dict[str, Counter[str]] = defaultdict(Counter)

    input_counts: Dict[str, int] = {kind: 0 for kind in files}

    for kind, rows in files.items():
        for row in rows:
            input_counts[kind] += 1
            meta = row.get("meta", {})
            source_file = meta.get("source_file")
            if source_file:
                source_files.add(source_file)
                artifact_id = stable_id("artifact", meta.get("platform"), source_file, meta.get("snapshot_id"))
                artifacts_by_id.setdefault(
                    artifact_id,
                    {
                        "id": artifact_id,
                        "type": "artifact",
                        "artifact_type": "normalized_jsonl_source",
                        "source_file": source_file,
                        "platform": meta.get("platform"),
                        "scope": meta.get("scope"),
                        "run_id": meta.get("run_id"),
                        "observed_at": row.get("observed_at"),
                        "content_hash": meta.get("content_hash"),
                    },
                )

            if not row.get("id") and kind != "failures":
                note_issue(validation, issue_counts, kind_counts, kind, "missing_id", row)

            if kind == "accounts":
                if not any(row.get(field) for field in ("username", "display_name", "email", "url", "uri")):
                    note_issue(validation, issue_counts, kind_counts, kind, "account_missing_identity_fields", row)
                entity = build_account_entity(row)
                entities_by_id[entity["id"]] = merge_entity(entities_by_id.get(entity["id"], entity.copy()), entity) if entity["id"] in entities_by_id else entity
            elif kind == "people":
                if not any(row.get(field) for field in ("name", "handle", "email")):
                    note_issue(validation, issue_counts, kind_counts, kind, "person_missing_identity_fields", row)
                entity = build_person_entity(row)
                entities_by_id[entity["id"]] = merge_entity(entities_by_id.get(entity["id"], entity.copy()), entity) if entity["id"] in entities_by_id else entity
            elif kind in {"documents", "media", "conversations"}:
                entity = build_content_entity(row, kind)
                entities_by_id[entity["id"]] = merge_entity(entities_by_id.get(entity["id"], entity.copy()), entity) if entity["id"] in entities_by_id else entity
                if kind == "documents":
                    event = build_document_event(row)
                    if event:
                        events_by_id.setdefault(event["id"], event)
            elif kind == "messages":
                if not row.get("conversation_id"):
                    note_issue(validation, issue_counts, kind_counts, kind, "message_missing_conversation_id", row)
                event = build_message_event(row)
                events_by_id.setdefault(event["id"], event)
            elif kind == "activities":
                if row.get("repository") is None and row.get("url") is None and row.get("track_name") is None and row.get("content") is None:
                    note_issue(validation, issue_counts, kind_counts, kind, "activity_missing_subject", row)
                event = build_activity_event(row)
                events_by_id.setdefault(event["id"], event)
            elif kind == "failures":
                note_issue(validation, issue_counts, kind_counts, kind, "source_failure", row)

    store.artifacts = sorted(artifacts_by_id.values(), key=lambda row: row["id"])
    store.entities = sorted(entities_by_id.values(), key=lambda row: row["id"])
    store.events = sorted(events_by_id.values(), key=lambda row: row["id"])

    add_event_relationships(store, relationship_seen)
    add_identity_relationships(store, validation, issue_counts, kind_counts, relationship_seen)

    validation["summary"] = {
        "normalized_input_counts": input_counts,
        "unique_source_files": len(source_files),
        "canonical_counts_so_far": {
            "artifacts": len(store.artifacts),
            "entities": len(store.entities),
            "events": len(store.events),
            "relationships": len(store.relationships),
        },
    }

    if source_index_path and source_index_path.exists():
        indexed_rows = load_jsonl(source_index_path)
        missing_raw = []
        for item in indexed_rows:
            rel = item.get("source_file")
            if not rel or not source_root:
                continue
            if not (source_root / rel).exists():
                missing_raw.append(rel)
        validation["summary"]["source_index_rows"] = len(indexed_rows)
        validation["summary"]["missing_raw_source_files"] = len(missing_raw)
        if missing_raw:
            validation["issues"].append({"kind": "source_index", "issue": "raw_source_files_missing", "example": {"missing_examples": missing_raw[:10]}})

    validation["by_kind"] = {kind: dict(counter) for kind, counter in kind_counts.items()}
    validation["summary"]["issue_counts"] = dict(issue_counts)
    return store, validation


def add_relationship(store: RecordStore, relationship_seen: set[str], row: Dict[str, Any]) -> None:
    if row["id"] in relationship_seen:
        return
    relationship_seen.add(row["id"])
    store.relationships.append(row)


def add_event_relationships(store: RecordStore, relationship_seen: set[str], all_entity_ids: Optional[set] = None) -> None:
    entity_ids = all_entity_ids if all_entity_ids is not None else {entity["id"] for entity in store.entities}
    for event in store.events:
        actor_id = event.get("actor_entity_id")
        subject_id = event.get("subject_entity_id")
        object_id = event.get("object_entity_id")
        context_id = event.get("context_entity_id")
        event_node_id = entity_id("event_record", event.get("platform"), event["id"])
        if actor_id and actor_id in entity_ids:
            add_relationship(
                store,
                relationship_seen,
                {
                    "id": relationship_id("actor_of", actor_id, event["id"]),
                    "type": "relationship",
                    "predicate": "actor_of",
                    "subject_entity_id": actor_id,
                    "object_entity_id": event_node_id,
                    "valid_from": event.get("occurred_at"),
                    "raw_payload": {"event_id": event["id"]},
                },
            )
        if subject_id:
            add_relationship(
                store,
                relationship_seen,
                {
                    "id": relationship_id("subject_of", subject_id, event["id"]),
                    "type": "relationship",
                    "predicate": "subject_of",
                    "subject_entity_id": subject_id,
                    "object_entity_id": event_node_id,
                    "valid_from": event.get("occurred_at"),
                    "raw_payload": {"event_id": event["id"]},
                },
            )
        if object_id:
            add_relationship(
                store,
                relationship_seen,
                {
                    "id": relationship_id("object_of", object_id, event["id"]),
                    "type": "relationship",
                    "predicate": "object_of",
                    "subject_entity_id": object_id,
                    "object_entity_id": event_node_id,
                    "valid_from": event.get("occurred_at"),
                    "raw_payload": {"event_id": event["id"]},
                },
            )
        if context_id:
            add_relationship(
                store,
                relationship_seen,
                {
                    "id": relationship_id("in_context", event["id"], context_id),
                    "type": "relationship",
                    "predicate": "in_context",
                    "subject_entity_id": event_node_id,
                    "object_entity_id": context_id,
                    "valid_from": event.get("occurred_at"),
                    "raw_payload": {"event_id": event["id"]},
                },
            )


def add_identity_relationships(
    store: RecordStore,
    validation: Dict[str, Any],
    issue_counts: Counter[str],
    kind_counts: Dict[str, Counter[str]],
    relationship_seen: set[str],
) -> None:
    accounts = [entity for entity in store.entities if entity.get("entity_type") == "account"]
    people = [entity for entity in store.entities if entity.get("entity_type") == "person"]

    people_by_email: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    people_by_name: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for person in people:
        keys = person.get("identity_keys", {})
        if keys.get("email"):
            people_by_email[str(keys["email"]).strip().lower()].append(person)
        if keys.get("name_normalized"):
            people_by_name[keys["name_normalized"]].append(person)

    for account in accounts:
        keys = account.get("identity_keys", {})
        matches: List[tuple[Dict[str, Any], float, List[str]]] = []
        email = keys.get("email")
        if email:
            for person in people_by_email.get(str(email).strip().lower(), []):
                matches.append((person, 0.98, ["shared_email"]))

        account_name = normalize_name(account.get("canonical_name"))
        if account_name:
            for person in people_by_name.get(account_name, []):
                matches.append((person, 0.7, ["shared_normalized_name"]))

        seen_people: set[str] = set()
        for person, confidence, evidence in sorted(matches, key=lambda item: item[1], reverse=True):
            if person["id"] in seen_people:
                continue
            seen_people.add(person["id"])
            add_relationship(
                store,
                relationship_seen,
                {
                    "id": relationship_id("owns_account", person["id"], account["id"]),
                    "type": "relationship",
                    "predicate": "owns_account",
                    "subject_entity_id": person["id"],
                    "object_entity_id": account["id"],
                    "valid_from": min(filter(None, [person.get("first_seen_at"), account.get("first_seen_at")]), default=None),
                    "confidence": confidence,
                    "evidence": evidence,
                    "raw_payload": {"person_id": person["id"], "account_id": account["id"]},
                },
            )

    for group_name, group in [("email", people_by_email), ("name", people_by_name)]:
        for key, members in group.items():
            if len(members) < 2:
                continue
            for idx, person in enumerate(members):
                for other in members[idx + 1 :]:
                    add_relationship(
                        store,
                        relationship_seen,
                        {
                            "id": relationship_id("same_as", person["id"], other["id"], group_name, key),
                            "type": "relationship",
                            "predicate": "same_as",
                            "subject_entity_id": person["id"],
                            "object_entity_id": other["id"],
                            "confidence": 0.99 if group_name == "email" else 0.62,
                            "evidence": [f"shared_{group_name}"],
                            "raw_payload": {"match_key": key},
                        },
                    )

    if not store.relationships:
        note_issue(validation, issue_counts, kind_counts, "relationships", "no_relationships_generated", {})


def incremental_build_from_normalized(
    normalized_root: Path,
    output_root: Path,
) -> Dict[str, Any]:
    """Process only records added since the last watermark; append to canonical files."""
    existing_entity_ids = load_ids(output_root / "entities.ndjson")
    existing_event_ids = load_ids(output_root / "events.ndjson")
    existing_relationship_ids = load_ids(output_root / "relationships.ndjson")

    new_entities: List[Dict[str, Any]] = []
    new_events: List[Dict[str, Any]] = []
    new_entity_ids: set = set()
    new_event_ids: set = set()

    input_counts: Dict[str, int] = {}

    # Collect all source files per kind: primary {kind}.jsonl + per-source {source}_{kind}.jsonl
    _kind_files: list[tuple[str, Path]] = []
    for kind in ("accounts", "people", "conversations", "documents", "media", "messages", "activities"):
        primary = normalized_root / f"{kind}.jsonl"
        if primary.exists():
            _kind_files.append((kind, primary))
        for path in sorted(normalized_root.glob(f"*_{kind}.jsonl")):
            _kind_files.append((kind, path))

    for kind, src in _kind_files:
        if not src.exists():
            input_counts[kind] = 0
            continue

        label = f"canonical_{src.stem}"  # unique per file, e.g. canonical_facebook_messages
        offset = _read_watermark(label)
        src_size = src.stat().st_size

        if offset >= src_size:
            input_counts[kind] = 0
            continue

        count = 0
        with open(src, "rb") as f_in:
            f_in.seek(offset)
            for raw_line in f_in:
                try:
                    row = json.loads(raw_line.decode("utf-8"))
                    count += 1
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue

                if kind == "accounts":
                    entity = build_account_entity(row)
                    if entity["id"] not in existing_entity_ids and entity["id"] not in new_entity_ids:
                        new_entities.append(entity)
                        new_entity_ids.add(entity["id"])
                        existing_entity_ids.add(entity["id"])
                elif kind == "people":
                    entity = build_person_entity(row)
                    if entity["id"] not in existing_entity_ids and entity["id"] not in new_entity_ids:
                        new_entities.append(entity)
                        new_entity_ids.add(entity["id"])
                        existing_entity_ids.add(entity["id"])
                elif kind in {"documents", "media", "conversations"}:
                    entity = build_content_entity(row, kind)
                    if entity["id"] not in existing_entity_ids and entity["id"] not in new_entity_ids:
                        new_entities.append(entity)
                        new_entity_ids.add(entity["id"])
                        existing_entity_ids.add(entity["id"])
                    if kind == "documents":
                        event = build_document_event(row)
                        if event and event["id"] not in existing_event_ids and event["id"] not in new_event_ids:
                            new_events.append(event)
                            new_event_ids.add(event["id"])
                            existing_event_ids.add(event["id"])
                elif kind == "messages":
                    event = build_message_event(row)
                    if event["id"] not in existing_event_ids and event["id"] not in new_event_ids:
                        new_events.append(event)
                        new_event_ids.add(event["id"])
                        existing_event_ids.add(event["id"])
                elif kind == "activities":
                    event = build_activity_event(row)
                    if event["id"] not in existing_event_ids and event["id"] not in new_event_ids:
                        new_events.append(event)
                        new_event_ids.add(event["id"])
                        existing_event_ids.add(event["id"])

        input_counts[kind] = count
        _write_watermark(label, src_size)

    # Build relationships for new events/entities using full entity ID set
    new_relationships: List[Dict[str, Any]] = []
    if new_events or new_entities:
        mini_store = RecordStore(artifacts=[], entities=new_entities, events=new_events, relationships=[])
        relationship_seen = existing_relationship_ids.copy()
        add_event_relationships(mini_store, relationship_seen, all_entity_ids=existing_entity_ids)

        # Identity relationships only among newly added accounts/people
        dummy_validation: Dict[str, Any] = {"summary": {}, "issues": [], "by_kind": {}}
        dummy_counts: Counter = Counter()
        dummy_kind_counts: Dict[str, Counter] = defaultdict(Counter)
        add_identity_relationships(mini_store, dummy_validation, dummy_counts, dummy_kind_counts, relationship_seen)
        new_relationships = mini_store.relationships

    # Append to canonical files
    if new_entities:
        with (output_root / "entities.ndjson").open("a", encoding="utf-8") as f:
            for e in new_entities:
                f.write(json.dumps(e, ensure_ascii=True) + "\n")
    if new_events:
        with (output_root / "events.ndjson").open("a", encoding="utf-8") as f:
            for e in new_events:
                f.write(json.dumps(e, ensure_ascii=True) + "\n")
    if new_relationships:
        with (output_root / "relationships.ndjson").open("a", encoding="utf-8") as f:
            for r in new_relationships:
                f.write(json.dumps(r, ensure_ascii=True) + "\n")

    return {
        "mode": "incremental",
        "input_counts": input_counts,
        "appended": {
            "entities": len(new_entities),
            "events": len(new_events),
            "relationships": len(new_relationships),
        },
    }


def parse_youtube_takeout(takeout_root: Path, store: RecordStore, validation: Dict[str, Any]) -> None:
    marker = '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    entity_seen = {row["id"] for row in store.entities}
    event_seen = {row["id"] for row in store.events}
    artifact_seen = {row["id"] for row in store.artifacts}

    watch_files = list(takeout_root.glob("**/history/watch-history.html"))
    search_files = list(takeout_root.glob("**/history/search-history.html"))
    files = sorted(watch_files + search_files)
    if not files:
        validation["issues"].append({"kind": "youtube_takeout", "issue": "missing_history_files", "example": {"takeout_root": str(takeout_root)}})
        return

    counts = Counter()
    for file_path in files:
        artifact = {
            "id": stable_id("artifact", "youtube_takeout", file_path.relative_to(takeout_root)),
            "type": "artifact",
            "artifact_type": "html",
            "source_file": str(file_path.relative_to(takeout_root)),
            "platform": "google",
            "scope": "youtube.takeout.history",
            "run_id": "local-takeout",
        }
        append_unique(store.artifacts, artifact_seen, artifact)

        html = file_path.read_text(encoding="utf-8")
        for index, block in enumerate(html.split(marker)[1:]):
            snippet = block.split("</div>", 1)[0]
            anchors = [
                {"href": unescape(match.group(1)), "text": re.sub(r"\s+", " ", unescape(match.group(2))).strip()}
                for match in re.finditer(r'<a href="([^"]+)">([\s\S]*?)</a>', snippet)
            ]
            text = unescape(re.sub(r"<[^>]+>", "\n", snippet))
            lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
            if not lines:
                continue

            action_verb = lines[0]
            timestamp_text = lines[-1]
            title_line = lines[1] if len(lines) > 1 else None
            middle_lines = lines[2:-1] if len(lines) > 3 else []
            event_type = "youtube_activity"
            lowered = action_verb.lower()
            if lowered.startswith("watched"):
                event_type = "watched_video"
            elif lowered.startswith("viewed"):
                event_type = "viewed_post"
            elif lowered.startswith("searched for"):
                event_type = "searched"

            action_text = action_verb if not title_line else f"{action_verb} {title_line}"
            title = anchors[0]["text"] if anchors else title_line or action_verb
            url = anchors[0]["href"] if anchors else None
            channel = anchors[1]["text"] if len(anchors) > 1 else None
            if not channel:
                for candidate in middle_lines:
                    if not candidate.lower().startswith(("watched at ", "viewed at ", "searched at ", "visited at ")):
                        channel = candidate
                        break
            channel_url = anchors[1]["href"] if len(anchors) > 1 else None

            content_entity = {
                "id": entity_id("content", "youtube", url or title or f"{file_path.name}-{index}"),
                "type": "entity",
                "entity_type": "content",
                "canonical_name": title or action_text,
                "platform": "youtube",
                "attributes": {"title": title, "url": url, "action_text": action_text},
                "first_seen_at": parse_dt(timestamp_text),
                "last_seen_at": parse_dt(timestamp_text),
                "provenance": {"source_file": str(file_path.relative_to(takeout_root)), "platform": "youtube"},
                "observed_in": [str(file_path.relative_to(takeout_root))],
            }
            append_unique(store.entities, entity_seen, content_entity)
            if channel or channel_url:
                append_unique(
                    store.entities,
                    entity_seen,
                    {
                        "id": entity_id("channel", "youtube", channel_url or channel),
                        "type": "entity",
                        "entity_type": "channel",
                        "canonical_name": channel or channel_url,
                        "platform": "youtube",
                        "attributes": {"url": channel_url},
                        "first_seen_at": parse_dt(timestamp_text),
                        "last_seen_at": parse_dt(timestamp_text),
                        "provenance": {"source_file": str(file_path.relative_to(takeout_root)), "platform": "youtube"},
                        "observed_in": [str(file_path.relative_to(takeout_root))],
                    },
                )

            event = {
                "id": event_id(event_type, "youtube", file_path.name, index, timestamp_text, url or title),
                "type": "event",
                "event_type": event_type,
                "occurred_at": parse_dt(timestamp_text),
                "title": title or action_text,
                "description": channel,
                "platform": "youtube",
                "source_file": str(file_path.relative_to(takeout_root)),
                "actor_entity_id": entity_id("account", "youtube", "self"),
                "subject_entity_id": content_entity["id"],
                "object_entity_id": entity_id("channel", "youtube", channel_url or channel) if (channel or channel_url) else None,
                "context_entity_id": None,
                "raw_payload": {
                    "action_text": action_text,
                    "timestamp_text": timestamp_text,
                    "url": url,
                    "channel": channel,
                    "channel_url": channel_url,
                },
            }
            append_unique(store.events, event_seen, event)
            counts[event_type] += 1

    validation["summary"]["youtube_takeout_counts"] = dict(counts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical NDJSON from normalized exports")
    parser.add_argument("--normalized-root", default="/path/to/data-connect/meta/normalized")
    parser.add_argument("--output-root", default="/path/to/data-connect/meta/canonical")
    parser.add_argument("--source-index", default="/path/to/data-connect/meta/raw/source_index.jsonl")
    parser.add_argument("--source-root", default="/path/to/data-connect/personal-server/data")
    parser.add_argument("--youtube-takeout-root", default="/path/to/Downloads/YouTube and YouTube Music")
    parser.add_argument("--full-rebuild", action="store_true", help="Ignore watermarks and rebuild canonical files from scratch")
    parser.add_argument("--compress", action="store_true", help="Also write .ndjson.gz compressed copies of canonical files (for faster index builds)")
    return parser.parse_args()


def _canonical_files_exist(output_root: Path) -> bool:
    for name in ("entities.ndjson", "events.ndjson"):
        p = output_root / name
        if not p.exists() or p.stat().st_size == 0:
            return False
    return True


def main() -> int:
    args = parse_args()
    normalized_root = Path(args.normalized_root)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    if args.full_rebuild:
        # Clear ALL canonical watermarks (both primary and per-source files)
        for kind in ("accounts", "people", "conversations", "documents", "media", "messages", "activities"):
            for src in [normalized_root / f"{kind}.jsonl"] + sorted(normalized_root.glob(f"*_{kind}.jsonl")):
                wp = _watermark_path(f"canonical_{src.stem}")
                if wp.exists():
                    wp.unlink()

    if not args.full_rebuild and _canonical_files_exist(output_root):
        summary = incremental_build_from_normalized(normalized_root, output_root)
        print(json.dumps({"output_root": str(output_root), **summary}, indent=2))
        return 0

    store, validation = build_from_normalized(
        normalized_root,
        output_root,
        Path(args.source_index),
        Path(args.source_root),
    )
    takeout_root = Path(args.youtube_takeout_root)
    if takeout_root.exists():
        parse_youtube_takeout(takeout_root, store, validation)

    compress = args.compress
    counts = {
        "artifacts": write_jsonl(output_root / "artifacts.ndjson", store.artifacts, compress=compress),
        "entities": write_jsonl(output_root / "entities.ndjson", store.entities, compress=compress),
        "events": write_jsonl(output_root / "events.ndjson", store.events, compress=compress),
        "relationships": write_jsonl(output_root / "relationships.ndjson", store.relationships, compress=compress),
    }

    # Write watermarks at current EOF so next incremental run only sees new data.
    # Each source file gets its own watermark key (canonical_{stem}) so per-source
    # files are tracked independently.
    for kind in ("accounts", "people", "conversations", "documents", "media", "messages", "activities"):
        for src in [normalized_root / f"{kind}.jsonl"] + sorted(normalized_root.glob(f"*_{kind}.jsonl")):
            if src.exists():
                _write_watermark(f"canonical_{src.stem}", src.stat().st_size)

    validation["summary"]["final_canonical_counts"] = counts
    validation["generated_at"] = now_iso()

    with (output_root / "validation_report.json").open("w", encoding="utf-8") as handle:
        json.dump(validation, handle, indent=2, ensure_ascii=True)
        handle.write("\n")

    print(json.dumps({"output_root": str(output_root), "counts": counts, "issues": validation["summary"].get("issue_counts", {})}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
