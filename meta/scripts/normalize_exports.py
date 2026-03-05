#!/usr/bin/env python3
"""Normalize DataConnect exports into a shared canonical JSONL layer."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


CANONICAL_FILES = {
    "people": "people.jsonl",
    "accounts": "accounts.jsonl",
    "conversations": "conversations.jsonl",
    "messages": "messages.jsonl",
    "media": "media.jsonl",
    "activities": "activities.jsonl",
    "documents": "documents.jsonl",
    "failures": "failures.jsonl",
}


@dataclass
class Ctx:
    source_root: Path
    output_root: Path
    raw_root: Path
    normalized_root: Path
    manifests_root: Path
    copy_raw: bool
    run_id: str
    run_at: str


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
            count += 1
    return count


def source_parts(path: Path, source_root: Path) -> Tuple[str, str]:
    rel = path.relative_to(source_root)
    parts = rel.parts
    platform = parts[0] if len(parts) > 0 else "unknown"
    scope = parts[1] if len(parts) > 1 else "unknown"
    return platform, scope


def add_record(records: Dict[str, List[Dict[str, Any]]], kind: str, row: Dict[str, Any]) -> None:
    records[kind].append(row)


def json_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def snapshot_id(platform: str, scope: str, natural_key: Any, payload: Any) -> str:
    return f"{platform}:{scope}:{slug_value(natural_key)}:{json_hash(payload)[:12]}"


def slug_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch if ch.isalnum() else "-" for ch in text)
    text = "-".join(part for part in text.split("-") if part)
    return text[:96] or "unknown"


def enrich_observation(
    row: Dict[str, Any],
    *,
    record_type: str,
    natural_key: Any,
    observed_at: Optional[str],
    payload_fragment: Any,
) -> Dict[str, Any]:
    meta = row.setdefault("meta", {})
    meta["record_type"] = record_type
    meta["natural_key"] = natural_key
    meta["snapshot_id"] = snapshot_id(meta.get("platform", "unknown"), meta.get("scope", "unknown"), natural_key, payload_fragment)
    meta["content_hash"] = json_hash(payload_fragment)
    row["observed_at"] = observed_at or row.get("source", {}).get("collected_at") or meta.get("run_at")
    return row


def make_base_entity(ctx: Ctx, path: Path, payload: Any, platform: str, scope: str) -> Dict[str, Any]:
    rel = str(path.relative_to(ctx.source_root))
    return {
        "meta": {
            "run_id": ctx.run_id,
            "run_at": ctx.run_at,
            "source_file": rel,
            "platform": platform,
            "scope": scope,
        },
        "source": {
            "collected_at": payload.get("collectedAt") if isinstance(payload, dict) else None,
            "schema": payload.get("$schema") if isinstance(payload, dict) else None,
            "version": payload.get("version") if isinstance(payload, dict) else None,
        },
    }


def normalize_chatgpt_conversations(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    data = payload.get("data", {})
    conversations = data.get("conversations", [])
    for conv in conversations:
        conv_id = conv.get("id") or f"chatgpt-conv-{hashlib.md5(json.dumps(conv, sort_keys=True).encode()).hexdigest()[:12]}"
        conv_row = make_base_entity(ctx, path, payload, "chatgpt", "conversations")
        conv_row.update(
            {
                "id": conv_id,
                "title": conv.get("title"),
                "created_at": conv.get("create_time"),
                "updated_at": conv.get("update_time"),
                "message_count": len(conv.get("messages", [])),
                "status": "ok" if conv.get("success", True) else "failed",
            }
        )
        enrich_observation(conv_row, record_type="snapshot", natural_key=conv_id, observed_at=conv.get("update_time") or conv.get("create_time"), payload_fragment=conv)
        add_record(records, "conversations", conv_row)

        for msg in conv.get("messages", []):
            msg_row = make_base_entity(ctx, path, payload, "chatgpt", "conversations")
            msg_row.update(
                {
                    "id": msg.get("id") or f"{conv_id}:{msg.get('create_time')}",
                    "conversation_id": conv_id,
                    "role": msg.get("role"),
                    "created_at": msg.get("create_time"),
                    "content_type": msg.get("content_type"),
                    "content": msg.get("content"),
                }
            )
            enrich_observation(
                msg_row,
                record_type="observation",
                natural_key=msg_row["id"],
                observed_at=msg.get("create_time"),
                payload_fragment=msg,
            )
            add_record(records, "messages", msg_row)


def normalize_chatgpt_memories(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    memories = payload.get("data", {}).get("memories", [])
    for memory in memories:
        row = make_base_entity(ctx, path, payload, "chatgpt", "memories")
        row.update(
            {
                "id": memory.get("id"),
                "type": "memory",
                "content": memory.get("content"),
                "created_at": memory.get("created_at"),
            }
        )
        enrich_observation(row, record_type="observation", natural_key=row["id"], observed_at=memory.get("created_at"), payload_fragment=memory)
        add_record(records, "documents", row)


def normalize_github_profile(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    rel = str(path.relative_to(ctx.source_root))
    data = payload.get("data", {})

    if rel.startswith("github/profile/"):
        profile = data.get("profile", data)

        account = make_base_entity(ctx, path, payload, "github", "profile")
        account.update(
            {
                "id": profile.get("id") or profile.get("login"),
                "username": profile.get("login"),
                "display_name": profile.get("name"),
                "url": profile.get("html_url"),
                "email": profile.get("email"),
                "company": profile.get("company"),
                "location": profile.get("location"),
                "followers": profile.get("followers"),
                "following": profile.get("following"),
            }
        )
        enrich_observation(
            account,
            record_type="snapshot",
            natural_key=profile.get("id") or profile.get("login") or profile.get("html_url"),
            observed_at=payload.get("collectedAt"),
            payload_fragment=profile,
        )
        add_record(records, "accounts", account)

        if profile.get("name") or profile.get("email"):
            person = make_base_entity(ctx, path, payload, "github", "profile")
            person.update(
                {
                    "id": profile.get("id") or profile.get("login"),
                    "name": profile.get("name") or profile.get("login"),
                    "email": profile.get("email"),
                    "location": profile.get("location"),
                }
            )
            enrich_observation(
                person,
                record_type="snapshot",
                natural_key=profile.get("id") or profile.get("login") or profile.get("email") or profile.get("name"),
                observed_at=payload.get("collectedAt"),
                payload_fragment=profile,
            )
            add_record(records, "people", person)
        return

    if rel.startswith("github/repositories/"):
        repositories = data.get("repositories", data if isinstance(data, list) else [])
        for repo in repositories:
            doc = make_base_entity(ctx, path, payload, "github", "repositories")
            doc.update(
                {
                    "id": repo.get("id") or repo.get("full_name"),
                    "type": "repository",
                    "name": repo.get("name"),
                    "full_name": repo.get("full_name"),
                    "url": repo.get("html_url"),
                    "description": repo.get("description"),
                    "created_at": repo.get("created_at"),
                    "updated_at": repo.get("updated_at"),
                    "private": repo.get("private"),
                    "stars": repo.get("stargazers_count"),
                }
            )
            enrich_observation(
                doc,
                record_type="snapshot",
                natural_key=repo.get("id") or repo.get("node_id") or repo.get("full_name"),
                observed_at=repo.get("updated_at") or repo.get("created_at"),
                payload_fragment=repo,
            )
            add_record(records, "documents", doc)
        return

    starred_repos = data.get("starred", data if isinstance(data, list) else [])
    for starred in starred_repos:
        act = make_base_entity(ctx, path, payload, "github", "starred")
        act.update(
            {
                "id": f"star:{starred.get('id') or starred.get('full_name')}",
                "type": "starred_repository",
                "repository": starred.get("full_name"),
                "url": starred.get("html_url"),
            }
        )
        enrich_observation(
            act,
            record_type="observation",
            natural_key=starred.get("id") or starred.get("full_name"),
            observed_at=payload.get("collectedAt"),
            payload_fragment=starred,
        )
        add_record(records, "activities", act)


def normalize_github_repositories(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    normalize_github_profile(ctx, path, payload, records)


def normalize_github_starred(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    normalize_github_profile(ctx, path, payload, records)


def normalize_instagram_profile(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    data = payload.get("data", {})
    account = make_base_entity(ctx, path, payload, "instagram", "profile")
    account.update(
        {
            "id": data.get("id") or data.get("username"),
            "username": data.get("username"),
            "display_name": data.get("full_name"),
            "bio": data.get("bio"),
            "followers": data.get("follower_count"),
            "following": data.get("following_count"),
            "url": data.get("external_url"),
            "is_private": data.get("is_private"),
            "is_verified": data.get("is_verified"),
        }
    )
    enrich_observation(
        account,
        record_type="snapshot",
        natural_key=data.get("id") or data.get("username"),
        observed_at=payload.get("collectedAt"),
        payload_fragment=data,
    )
    add_record(records, "accounts", account)

    if data.get("full_name"):
        person = make_base_entity(ctx, path, payload, "instagram", "profile")
        person.update(
            {
                "id": data.get("id") or data.get("username"),
                "name": data.get("full_name"),
                "handle": data.get("username"),
            }
        )
        enrich_observation(
            person,
            record_type="snapshot",
            natural_key=data.get("id") or data.get("username"),
            observed_at=payload.get("collectedAt"),
            payload_fragment=data,
        )
        add_record(records, "people", person)


def normalize_instagram_posts(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    posts = payload.get("data", {}).get("posts", [])
    for post in posts:
        media = make_base_entity(ctx, path, payload, "instagram", "posts")
        media.update(
            {
                "id": post.get("id") or post.get("shortcode"),
                "type": post.get("media_type") or "post",
                "caption": post.get("caption"),
                "url": post.get("permalink"),
                "created_at": post.get("timestamp"),
                "likes": post.get("like_count"),
                "comments": post.get("comments_count"),
            }
        )
        enrich_observation(
            media,
            record_type="snapshot",
            natural_key=post.get("id") or post.get("shortcode") or post.get("permalink"),
            observed_at=post.get("timestamp"),
            payload_fragment=post,
        )
        add_record(records, "media", media)


def normalize_spotify_profile(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    data = payload.get("data", {})
    account = make_base_entity(ctx, path, payload, "spotify", "profile")
    account.update(
        {
            "id": data.get("id"),
            "username": data.get("id"),
            "display_name": data.get("display_name"),
            "followers": data.get("followers"),
            "following": data.get("following"),
            "uri": data.get("uri"),
        }
    )
    enrich_observation(
        account,
        record_type="snapshot",
        natural_key=data.get("id") or data.get("uri"),
        observed_at=payload.get("collectedAt"),
        payload_fragment=data,
    )
    add_record(records, "accounts", account)

    if data.get("display_name"):
        person = make_base_entity(ctx, path, payload, "spotify", "profile")
        person.update({"id": data.get("id"), "name": data.get("display_name")})
        enrich_observation(
            person,
            record_type="snapshot",
            natural_key=data.get("id") or data.get("display_name"),
            observed_at=payload.get("collectedAt"),
            payload_fragment=data,
        )
        add_record(records, "people", person)


def normalize_spotify_playlists(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    playlists = payload.get("data", {}).get("playlists", [])
    for pl in playlists:
        doc = make_base_entity(ctx, path, payload, "spotify", "playlists")
        doc.update(
            {
                "id": pl.get("id") or pl.get("uri") or pl.get("name"),
                "type": "playlist",
                "name": pl.get("name"),
                "description": pl.get("description"),
                "track_count": len(pl.get("tracks", [])),
                "owner": pl.get("owner", {}).get("display_name") if isinstance(pl.get("owner"), dict) else None,
            }
        )
        enrich_observation(
            doc,
            record_type="snapshot",
            natural_key=pl.get("id") or pl.get("uri") or pl.get("name"),
            observed_at=payload.get("collectedAt"),
            payload_fragment=pl,
        )
        add_record(records, "documents", doc)

        for track in pl.get("tracks", []):
            act = make_base_entity(ctx, path, payload, "spotify", "playlist_track")
            act.update(
                {
                    "id": f"{doc['id']}::{track.get('id') or track.get('uri') or track.get('name')}",
                    "type": "playlist_track",
                    "playlist_id": doc["id"],
                    "track_name": track.get("name"),
                    "track_uri": track.get("uri"),
                    "artists": [a.get("name") for a in track.get("artists", []) if isinstance(a, dict)],
                    "album": (track.get("album") or {}).get("name") if isinstance(track.get("album"), dict) else None,
                }
            )
            enrich_observation(
                act,
                record_type="observation",
                natural_key=track.get("id") or track.get("uri") or track.get("name"),
                observed_at=payload.get("collectedAt"),
                payload_fragment=track,
            )
            add_record(records, "activities", act)


def normalize_linkedin_profile(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    data = payload.get("data", {})
    if data.get("success") is False or data.get("error"):
        fail = make_base_entity(ctx, path, payload, "linkedin", "profile")
        fail.update(
            {
                "id": "linkedin.profile",
                "error": data.get("error") or "unknown_error",
                "success": data.get("success"),
                "platform": data.get("platform"),
            }
        )
        add_record(records, "failures", fail)
        return

    # Fallback for future successful LinkedIn shapes.
    doc = make_base_entity(ctx, path, payload, "linkedin", "profile")
    doc.update({"id": "linkedin.profile", "type": "linkedin_profile", "raw_data": data})
    enrich_observation(doc, record_type="snapshot", natural_key="linkedin.profile", observed_at=payload.get("collectedAt"), payload_fragment=data)
    add_record(records, "documents", doc)


def normalize_claude_local(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    rel = str(path.relative_to(ctx.source_root))
    data = payload.get("data", payload)
    if rel.startswith("claude/conversations/"):
        for item in data.get("items", []):
            conv = make_base_entity(ctx, path, payload, "claude", "conversations")
            conv.update(
                {
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "created_at": item.get("createdAt"),
                    "updated_at": item.get("updatedAt"),
                    "message_count": item.get("messageCount"),
                    "source": payload.get("source"),
                }
            )
            enrich_observation(conv, record_type="snapshot", natural_key=item.get("id"), observed_at=item.get("updatedAt") or item.get("createdAt"), payload_fragment=item)
            add_record(records, "conversations", conv)
    elif rel.startswith("claude/messages/"):
        for item in data.get("items", []):
            msg = make_base_entity(ctx, path, payload, "claude", "messages")
            msg.update(
                {
                    "id": item.get("id"),
                    "conversation_id": item.get("conversationId"),
                    "role": item.get("role"),
                    "content": item.get("content"),
                    "created_at": item.get("createdAt"),
                    "source": payload.get("source"),
                }
            )
            enrich_observation(msg, record_type="observation", natural_key=item.get("id"), observed_at=item.get("createdAt"), payload_fragment=item)
            add_record(records, "messages", msg)
    elif rel.startswith("claude/localSessions/"):
        for idx, item in enumerate(data.get("items", [])):
            msg = make_base_entity(ctx, path, payload, "claude", "localSessions")
            msg.update(
                {
                    "id": f"claude.local.{idx}",
                    "role": item.get("role"),
                    "content": item.get("content"),
                    "content_type": "text",
                    "source_file_path": item.get("file"),
                }
            )
            enrich_observation(msg, record_type="observation", natural_key=msg["id"], observed_at=payload.get("collectedAt"), payload_fragment=item)
            add_record(records, "messages", msg)
    elif rel.startswith("claude/localLogs/"):
        for idx, item in enumerate(data.get("files", [])):
            doc = make_base_entity(ctx, path, payload, "claude", "localLogs")
            doc.update(
                {
                    "id": f"claude.log.{idx}",
                    "type": "local_log",
                    "path": item.get("path"),
                    "created_at": item.get("mtime"),
                    "content": item.get("tail"),
                }
            )
            enrich_observation(doc, record_type="snapshot", natural_key=item.get("path") or doc["id"], observed_at=item.get("mtime"), payload_fragment=item)
            add_record(records, "documents", doc)


def normalize_claude_full_export(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    rel = str(path.relative_to(ctx.source_root))
    data = payload.get("data", [])
    if rel.startswith("claude/conversations_full_export/"):
        for conv in data:
            conv_id = conv.get("uuid")
            conv_row = make_base_entity(ctx, path, payload, "claude", "conversations_full_export")
            conv_row.update(
                {
                    "id": conv_id,
                    "title": conv.get("name"),
                    "created_at": conv.get("created_at"),
                    "updated_at": conv.get("updated_at"),
                    "message_count": len(conv.get("chat_messages", [])),
                    "status": "ok",
                    "summary": conv.get("summary"),
                }
            )
            enrich_observation(conv_row, record_type="snapshot", natural_key=conv_id, observed_at=conv.get("updated_at") or conv.get("created_at"), payload_fragment=conv)
            add_record(records, "conversations", conv_row)

            for msg in conv.get("chat_messages", []):
                parts = msg.get("content", []) or []
                rendered_parts = []
                for part in parts:
                    if part.get("type") == "text" and part.get("text"):
                        rendered_parts.append(part["text"])
                    elif part.get("type") == "thinking" and part.get("thinking"):
                        rendered_parts.append(part["thinking"])
                msg_row = make_base_entity(ctx, path, payload, "claude", "conversations_full_export")
                msg_row.update(
                    {
                        "id": msg.get("uuid"),
                        "conversation_id": conv_id,
                        "role": "assistant" if msg.get("sender") == "assistant" else "user",
                        "created_at": msg.get("created_at"),
                        "content_type": "text",
                        "content": "\n\n".join(part for part in rendered_parts if part) or msg.get("text"),
                    }
                )
                enrich_observation(msg_row, record_type="observation", natural_key=msg.get("uuid"), observed_at=msg.get("created_at"), payload_fragment=msg)
                add_record(records, "messages", msg_row)
    elif rel.startswith("claude/memories_full_export/"):
        for memory in data:
            row = make_base_entity(ctx, path, payload, "claude", "memories_full_export")
            row.update(
                {
                    "id": memory.get("account_uuid") or "claude.memory",
                    "type": "memory_summary",
                    "content": memory.get("conversations_memory"),
                    "created_at": None,
                    "raw_data": memory.get("project_memories", {}),
                }
            )
            enrich_observation(row, record_type="snapshot", natural_key=row["id"], observed_at=payload.get("collectedAt"), payload_fragment=memory)
            add_record(records, "documents", row)
    elif rel.startswith("claude/projects_full_export/"):
        for project in data:
            row = make_base_entity(ctx, path, payload, "claude", "projects_full_export")
            row.update(
                {
                    "id": project.get("uuid") or project.get("name"),
                    "type": "project",
                    "name": project.get("name"),
                    "description": project.get("description"),
                    "created_at": project.get("created_at"),
                    "updated_at": project.get("updated_at"),
                }
            )
            enrich_observation(row, record_type="snapshot", natural_key=project.get("uuid") or project.get("name"), observed_at=project.get("updated_at") or project.get("created_at"), payload_fragment=project)
            add_record(records, "documents", row)
    elif rel.startswith("claude/users_full_export/"):
        for user in data:
            row = make_base_entity(ctx, path, payload, "claude", "users_full_export")
            row.update(
                {
                    "id": user.get("uuid") or user.get("email_address"),
                    "email": user.get("email_address"),
                    "display_name": user.get("name"),
                }
            )
            enrich_observation(row, record_type="snapshot", natural_key=user.get("uuid") or user.get("email_address"), observed_at=payload.get("collectedAt"), payload_fragment=user)
            add_record(records, "accounts", row)


def normalize_linkedin_basic_export(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    rel = str(path.relative_to(ctx.source_root))
    rows = payload.get("data", [])
    if rel.startswith("linkedin/profile_basic/"):
        if not rows:
            return
        row = rows[0]
        account = make_base_entity(ctx, path, payload, "linkedin", "profile_basic")
        account.update(
            {
                "id": "linkedin.basic.profile",
                "display_name": " ".join(part for part in [row.get("First Name"), row.get("Last Name")] if part),
                "headline": row.get("Headline"),
                "industry": row.get("Industry"),
                "location": row.get("Geo Location"),
                "zip_code": row.get("Zip Code"),
                "websites": row.get("Websites"),
            }
        )
        add_record(records, "accounts", account)
        person = make_base_entity(ctx, path, payload, "linkedin", "profile_basic")
        person.update(
            {
                "id": "linkedin.basic.person",
                "name": account.get("display_name"),
                "location": row.get("Geo Location"),
            }
        )
        enrich_observation(account, record_type="snapshot", natural_key="linkedin.basic.profile", observed_at=payload.get("collectedAt"), payload_fragment=row)
        enrich_observation(person, record_type="snapshot", natural_key="linkedin.basic.person", observed_at=payload.get("collectedAt"), payload_fragment=row)
        add_record(records, "people", person)
    elif rel.startswith("linkedin/positions_basic/"):
        for idx, row in enumerate(rows):
            doc = make_base_entity(ctx, path, payload, "linkedin", "positions_basic")
            doc.update(
                {
                    "id": f"linkedin.position.{idx}",
                    "type": "position",
                    "company": row.get("Company Name"),
                    "title": row.get("Title"),
                    "description": row.get("Description"),
                    "location": row.get("Location"),
                    "start": row.get("Started On"),
                    "end": row.get("Finished On"),
                }
            )
            enrich_observation(doc, record_type="snapshot", natural_key=f"{row.get('Company Name')}|{row.get('Title')}|{row.get('Started On')}", observed_at=payload.get("collectedAt"), payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/education_basic/"):
        for idx, row in enumerate(rows):
            doc = make_base_entity(ctx, path, payload, "linkedin", "education_basic")
            doc.update(
                {
                    "id": f"linkedin.education.{idx}",
                    "type": "education",
                    "school": row.get("School Name"),
                    "degree": row.get("Degree Name"),
                    "activities": row.get("Activities"),
                    "start": row.get("Start Date"),
                    "end": row.get("End Date"),
                }
            )
            enrich_observation(doc, record_type="snapshot", natural_key=f"{row.get('School Name')}|{row.get('Degree Name')}|{row.get('Start Date')}", observed_at=payload.get("collectedAt"), payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/skills_basic/"):
        for idx, row in enumerate(rows):
            doc = make_base_entity(ctx, path, payload, "linkedin", "skills_basic")
            doc.update(
                {
                    "id": f"linkedin.skill.{idx}",
                    "type": "skill",
                    "name": row.get("Name"),
                }
            )
            enrich_observation(doc, record_type="snapshot", natural_key=row.get("Name"), observed_at=payload.get("collectedAt"), payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/connections_basic/"):
        for idx, row in enumerate(rows):
            person = make_base_entity(ctx, path, payload, "linkedin", "connections_basic")
            person.update(
                {
                    "id": row.get("URL") or f"linkedin.connection.{idx}",
                    "name": " ".join(part for part in [row.get("First Name"), row.get("Last Name")] if part),
                    "email": row.get("Email Address"),
                    "company": row.get("Company"),
                    "role": row.get("Position"),
                    "connected_at": row.get("Connected On"),
                }
            )
            enrich_observation(person, record_type="observation", natural_key=row.get("URL") or row.get("Email Address") or person["id"], observed_at=row.get("Connected On"), payload_fragment=row)
            add_record(records, "people", person)
    elif rel.startswith("linkedin/messages_basic/"):
        for idx, row in enumerate(rows):
            msg = make_base_entity(ctx, path, payload, "linkedin", "messages_basic")
            msg.update(
                {
                    "id": f"{row.get('CONVERSATION ID') or 'conv'}:{idx}",
                    "conversation_id": row.get("CONVERSATION ID"),
                    "role": "user" if row.get("FROM") == "Jwalin Shah" else "contact",
                    "created_at": row.get("DATE"),
                    "content_type": "text",
                    "content": row.get("CONTENT"),
                    "subject": row.get("SUBJECT"),
                }
            )
            enrich_observation(msg, record_type="observation", natural_key=msg["id"], observed_at=row.get("DATE"), payload_fragment=row)
            add_record(records, "messages", msg)
    elif rel.startswith("linkedin/invitations_basic/"):
        for idx, row in enumerate(rows):
            act = make_base_entity(ctx, path, payload, "linkedin", "invitations_basic")
            act.update(
                {
                    "id": f"linkedin.invitation.{idx}",
                    "type": "invitation",
                    "from": row.get("From"),
                    "to": row.get("To"),
                    "direction": row.get("Direction"),
                    "created_at": row.get("Sent At"),
                    "content": row.get("Message"),
                }
            )
            enrich_observation(act, record_type="observation", natural_key=act["id"], observed_at=row.get("Sent At"), payload_fragment=row)
            add_record(records, "activities", act)
    elif rel.startswith("linkedin/saved_jobs_basic/"):
        for idx, row in enumerate(rows):
            doc = make_base_entity(ctx, path, payload, "linkedin", "saved_jobs_basic")
            doc.update(
                {
                    "id": f"linkedin.saved_job.{idx}",
                    "type": "saved_job",
                    "name": row.get("Job Title"),
                    "company": row.get("Company Name"),
                    "url": row.get("Job Url"),
                    "saved_at": row.get("Saved Date"),
                }
            )
            enrich_observation(doc, record_type="observation", natural_key=row.get("Job Url") or doc["id"], observed_at=row.get("Saved Date"), payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/phone_numbers_basic/"):
        for idx, row in enumerate(rows):
            doc = make_base_entity(ctx, path, payload, "linkedin", "phone_numbers_basic")
            doc.update(
                {
                    "id": f"linkedin.phone.{idx}",
                    "type": "phone_number",
                    "number": row.get("Number"),
                    "label": row.get("Type"),
                }
            )
            enrich_observation(doc, record_type="snapshot", natural_key=row.get("Number") or doc["id"], observed_at=payload.get("collectedAt"), payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/email_addresses_basic/"):
        for idx, row in enumerate(rows):
            doc = make_base_entity(ctx, path, payload, "linkedin", "email_addresses_basic")
            doc.update(
                {
                    "id": row.get("Email Address") or f"linkedin.email.{idx}",
                    "type": "email_address",
                    "email": row.get("Email Address"),
                    "primary": row.get("Primary"),
                    "confirmed": row.get("Confirmed"),
                }
            )
            enrich_observation(doc, record_type="snapshot", natural_key=row.get("Email Address") or doc["id"], observed_at=payload.get("collectedAt"), payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/job_applications_basic/"):
        for idx, row in enumerate(rows):
            app_date = row.get("Application Date") or row.get("Date Applied")
            company = row.get("Company Name") or row.get("Company")
            title = row.get("Job Title") or row.get("Title")
            contact_email = row.get("Contact Email") or row.get("Email")
            doc = make_base_entity(ctx, path, payload, "linkedin", "job_applications_basic")
            doc.update(
                {
                    "id": f"linkedin.job_app.{company}.{title}.{app_date}".lower().replace(" ", "_")[:120] if company and title else f"linkedin.job_app.{idx}",
                    "type": "job_application",
                    "name": title,
                    "company": company,
                    "applied_at": app_date,
                    "contact_email": contact_email,
                    "contact_phone": row.get("Contact Phone Number"),
                    "job_url": row.get("Job Link") or row.get("Job Url"),
                    "status": row.get("Application Status") or row.get("Status"),
                }
            )
            enrich_observation(doc, record_type="observation", natural_key=doc["id"], observed_at=app_date, payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/search_queries_basic/"):
        for idx, row in enumerate(rows):
            searched_at = row.get("Time") or row.get("Date")
            query = row.get("Search Query") or row.get("Query")
            if not query:
                continue
            act = make_base_entity(ctx, path, payload, "linkedin", "search_queries_basic")
            act.update(
                {
                    "id": f"linkedin.search.{idx}",
                    "type": "searched",
                    "created_at": searched_at,
                    "content": query,
                }
            )
            enrich_observation(act, record_type="observation", natural_key=act["id"], observed_at=searched_at, payload_fragment=row)
            add_record(records, "activities", act)
    elif rel.startswith("linkedin/reactions_basic/"):
        for idx, row in enumerate(rows):
            act = make_base_entity(ctx, path, payload, "linkedin", "reactions_basic")
            act.update(
                {
                    "id": f"linkedin.reaction.{idx}",
                    "type": "reacted",
                    "created_at": row.get("Date"),
                    "content": row.get("Type"),
                    "url": row.get("Link"),
                }
            )
            enrich_observation(act, record_type="observation", natural_key=act["id"], observed_at=row.get("Date"), payload_fragment=row)
            add_record(records, "activities", act)
    elif rel.startswith("linkedin/comments_basic/"):
        for idx, row in enumerate(rows):
            act = make_base_entity(ctx, path, payload, "linkedin", "comments_basic")
            act.update(
                {
                    "id": f"linkedin.comment.{idx}",
                    "type": "commented",
                    "created_at": row.get("Date"),
                    "content": row.get("Message"),
                    "url": row.get("Link"),
                }
            )
            enrich_observation(act, record_type="observation", natural_key=act["id"], observed_at=row.get("Date"), payload_fragment=row)
            add_record(records, "activities", act)
    elif rel.startswith("linkedin/learning_basic/"):
        for idx, row in enumerate(rows):
            completed_at = row.get("Content Completed At (if completed)") or row.get("Content Last Watched Date (if viewed)")
            doc = make_base_entity(ctx, path, payload, "linkedin", "learning_basic")
            doc.update(
                {
                    "id": f"linkedin.learning.{idx}",
                    "type": "course",
                    "name": row.get("Content Title"),
                    "description": row.get("Content Description"),
                    "content_type": row.get("Content Type"),
                    "completed_at": completed_at,
                }
            )
            enrich_observation(doc, record_type="observation", natural_key=doc["id"], observed_at=completed_at, payload_fragment=row)
            add_record(records, "documents", doc)
    elif rel.startswith("linkedin/export_manifest_basic/"):
        doc = make_base_entity(ctx, path, payload, "linkedin", "export_manifest_basic")
        doc.update({"id": "linkedin.basic.export_manifest", "type": "export_manifest", "raw_data": payload.get("data", {})})
        enrich_observation(doc, record_type="snapshot", natural_key="linkedin.basic.export_manifest", observed_at=payload.get("collectedAt"), payload_fragment=payload.get("data", {}))
        add_record(records, "documents", doc)


def normalize_spotify_extended_streaming(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    plays = payload.get("data", [])
    for idx, play in enumerate(plays):
        if play.get("master_metadata_track_name") is None and play.get("episode_name") is None:
            continue
        act = make_base_entity(ctx, path, payload, "spotify", "extended_streaming_history")
        act.update(
            {
                "id": f"{path.stem}:{idx}",
                "type": "stream",
                "created_at": play.get("ts"),
                "track_name": play.get("master_metadata_track_name") or play.get("episode_name"),
                "artists": [play.get("master_metadata_album_artist_name")] if play.get("master_metadata_album_artist_name") else [],
                "album": play.get("master_metadata_album_album_name"),
                "track_uri": play.get("spotify_track_uri") or play.get("spotify_episode_uri"),
                "ms_played": play.get("ms_played"),
                "platform": play.get("platform"),
                "skipped": play.get("skipped"),
                "shuffle": play.get("shuffle"),
            }
        )
        enrich_observation(act, record_type="observation", natural_key=play.get("spotify_track_uri") or play.get("spotify_episode_uri") or act["id"], observed_at=play.get("ts"), payload_fragment=play)
        add_record(records, "activities", act)


def normalize_google_takeout_inventory(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    doc = make_base_entity(ctx, path, payload, "google", "takeout_inventory")
    doc.update(
        {
            "id": payload.get("archive_name") or path.stem,
            "type": "takeout_archive_inventory",
            "name": payload.get("archive_name"),
            "raw_data": payload.get("data", {}),
        }
    )
    enrich_observation(doc, record_type="snapshot", natural_key=payload.get("archive_name") or path.stem, observed_at=payload.get("collectedAt"), payload_fragment=payload.get("data", {}))
    add_record(records, "documents", doc)


def normalize_gemini_local(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    platform, scope = source_parts(path, ctx.source_root)
    doc = make_base_entity(ctx, path, payload, platform, scope)
    doc.update(
        {
            "id": f"{platform}.{scope}",
            "type": "local_export",
            "scope": scope,
            "raw_data": payload,
        }
    )
    enrich_observation(doc, record_type="snapshot", natural_key=f"{platform}.{scope}", observed_at=payload.get("collectedAt") if isinstance(payload, dict) else None, payload_fragment=payload)
    add_record(records, "documents", doc)


def normalize_unknown(ctx: Ctx, path: Path, payload: Dict[str, Any], records: Dict[str, List[Dict[str, Any]]]) -> None:
    platform, scope = source_parts(path, ctx.source_root)
    doc = make_base_entity(ctx, path, payload, platform, scope)
    doc.update({"id": f"{platform}.{scope}", "type": "unmapped_export", "raw_data": payload})
    enrich_observation(doc, record_type="snapshot", natural_key=f"{platform}.{scope}", observed_at=payload.get("collectedAt") if isinstance(payload, dict) else None, payload_fragment=payload)
    add_record(records, "documents", doc)


def maybe_copy_raw(ctx: Ctx, src_path: Path, platform: str, scope: str) -> Optional[str]:
    if not ctx.copy_raw:
        return None
    dst_dir = ctx.raw_root / platform / scope
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_path = dst_dir / src_path.name
    shutil.copy2(src_path, dst_path)
    return str(dst_path.relative_to(ctx.output_root))


def run(ctx: Ctx) -> Dict[str, Any]:
    ctx.raw_root.mkdir(parents=True, exist_ok=True)
    ctx.normalized_root.mkdir(parents=True, exist_ok=True)
    ctx.manifests_root.mkdir(parents=True, exist_ok=True)

    records: Dict[str, List[Dict[str, Any]]] = {k: [] for k in CANONICAL_FILES.keys()}
    source_index_rows: List[Dict[str, Any]] = []

    source_files = sorted(ctx.source_root.glob("*/*/*.json"))
    for src_path in source_files:
        platform, scope = source_parts(src_path, ctx.source_root)
        raw_copy_rel = maybe_copy_raw(ctx, src_path, platform, scope)

        sha = file_sha256(src_path)
        size = src_path.stat().st_size
        source_index_rows.append(
            {
                "run_id": ctx.run_id,
                "run_at": ctx.run_at,
                "platform": platform,
                "scope": scope,
                "source_file": str(src_path.relative_to(ctx.source_root)),
                "sha256": sha,
                "size_bytes": size,
                "raw_copy": raw_copy_rel,
            }
        )

        try:
            payload = read_json(src_path)
            rel = str(src_path.relative_to(ctx.source_root))

            if rel.startswith("chatgpt/conversations/"):
                normalize_chatgpt_conversations(ctx, src_path, payload, records)
            elif rel.startswith("chatgpt/memories/"):
                normalize_chatgpt_memories(ctx, src_path, payload, records)
            elif rel.startswith("github/profile/"):
                normalize_github_profile(ctx, src_path, payload, records)
            elif rel.startswith("github/repositories/"):
                normalize_github_repositories(ctx, src_path, payload, records)
            elif rel.startswith("github/starred/"):
                normalize_github_starred(ctx, src_path, payload, records)
            elif rel.startswith("instagram/profile/"):
                normalize_instagram_profile(ctx, src_path, payload, records)
            elif rel.startswith("instagram/posts/"):
                normalize_instagram_posts(ctx, src_path, payload, records)
            elif rel.startswith("spotify/profile/"):
                normalize_spotify_profile(ctx, src_path, payload, records)
            elif rel.startswith("spotify/playlists/"):
                normalize_spotify_playlists(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/profile/"):
                normalize_linkedin_profile(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/profile_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/positions_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/education_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/skills_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/connections_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/messages_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/invitations_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/saved_jobs_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/phone_numbers_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/email_addresses_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/export_manifest_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/job_applications_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/search_queries_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/reactions_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/comments_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("linkedin/learning_basic/"):
                normalize_linkedin_basic_export(ctx, src_path, payload, records)
            elif rel.startswith("claude/"):
                if "_full_export/" in rel:
                    normalize_claude_full_export(ctx, src_path, payload, records)
                else:
                    normalize_claude_local(ctx, src_path, payload, records)
            elif rel.startswith("spotify/extended_streaming_history/"):
                normalize_spotify_extended_streaming(ctx, src_path, payload, records)
            elif rel.startswith("gemini/"):
                normalize_gemini_local(ctx, src_path, payload, records)
            elif rel.startswith("google/takeout_inventory/"):
                normalize_google_takeout_inventory(ctx, src_path, payload, records)
            else:
                normalize_unknown(ctx, src_path, payload, records)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            fail = make_base_entity(ctx, src_path, {}, platform, scope)
            fail.update({"id": f"{platform}.{scope}", "error": str(exc), "success": False})
            add_record(records, "failures", fail)

    counts: Dict[str, int] = {}
    for kind, filename in CANONICAL_FILES.items():
        out_path = ctx.normalized_root / filename
        counts[kind] = write_jsonl(out_path, records[kind])

    source_index_count = write_jsonl(ctx.raw_root / "source_index.jsonl", source_index_rows)

    manifest = {
        "run_id": ctx.run_id,
        "run_at": ctx.run_at,
        "source_root": str(ctx.source_root),
        "output_root": str(ctx.output_root),
        "copy_raw": ctx.copy_raw,
        "source_files": len(source_files),
        "source_index_rows": source_index_count,
        "record_counts": counts,
    }

    with (ctx.manifests_root / "last_run_manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=True)
        f.write("\n")

    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize DataConnect exports into canonical JSONL files")
    parser.add_argument(
        "--source-root",
        default="/path/to/data-connect/personal-server/data",
        help="Root directory containing platform/scope/*.json exports",
    )
    parser.add_argument(
        "--output-root",
        default="/path/to/data-connect/meta",
        help="Meta workspace root (raw/ normalized/ graph/ manifests/)",
    )
    parser.add_argument(
        "--copy-raw",
        action="store_true",
        help="Copy source files into raw/<platform>/<scope>/ instead of only indexing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    ctx = Ctx(
        source_root=Path(args.source_root),
        output_root=Path(args.output_root),
        raw_root=Path(args.output_root) / "raw",
        normalized_root=Path(args.output_root) / "normalized",
        manifests_root=Path(args.output_root) / "manifests",
        copy_raw=args.copy_raw,
        run_id=run_id,
        run_at=now_iso(),
    )

    if not ctx.source_root.exists():
        raise SystemExit(f"source root does not exist: {ctx.source_root}")

    manifest = run(ctx)
    print(json.dumps(manifest, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
