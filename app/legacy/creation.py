"""Original input parsing and Telegram entity serialization."""

import json
import re
from typing import Optional


def serialize_entities(entities) -> str | None:
    """Serialize aiogram message entities to JSON for storage."""
    if not entities:
        return None
    result = []
    for e in entities:
        d = {"type": e.type, "offset": e.offset, "length": e.length}
        if e.type == "custom_emoji":
            d["custom_emoji_id"] = e.custom_emoji_id
        elif e.type == "text_link":
            d["url"] = e.url
        elif e.type == "pre":
            d["language"] = getattr(e, "language", "") or ""
        result.append(d)
    return json.dumps(result, ensure_ascii=False) if result else None


def parse_chat_link(text: str) -> str | None:
    """Extract chat identifier from a t.me link. Returns @username or None."""
    text = text.strip()
    # Legacy invite form — the send path only recognizes "t.me/+HASH", so an
    # unconverted joinchat link got "@" prepended and could never be joined.
    m = re.match(r"(?:https?://)?t\.me/joinchat/([\w-]+)/?$", text)
    if m:
        return f"https://t.me/+{m.group(1)}"
    # Match t.me/username or t.me/+invite links
    m = re.match(r"(?:https?://)?t\.me/(\+?[\w-]+)(?:$|/)", text)
    if m:
        username = m.group(1)
        # Skip special paths
        if username.lower() in ("addlist", "joinchat", "proxy", "socks"):
            return None
        return f"https://t.me/{username}" if username.startswith("+") else f"@{username}"
    return None


def _parse_topic_link(text: str) -> Optional[int]:
    """Topic id from a t.me link. The topic is the FIRST number after the
    chat — a link to a message inside a topic is t.me/chat/<topic>/<msg>
    (t.me/c/<id>/<topic>/<msg> for private chats), and taking the last
    number bound the mailing to a non-existent "topic" = message id."""
    m = re.search(r"[?&](?:thread|topic)=(\d+)", text)
    if m:
        return int(m.group(1))
    m = re.search(r"t\.me/(?:c/\d+|[A-Za-z0-9_]+)/(\d+)", text)
    if m:
        return int(m.group(1))
    return None


def _parse_topic_ranges(text: str) -> Optional[list[int]]:
    """Parses "3-6, 8, 10-20" into a sorted, deduped list of topic ids.
    Returns None if the syntax is invalid or the expanded list is too large."""
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        return None
    result: set[int] = set()
    for part in parts:
        if "-" in part:
            bounds = part.split("-")
            if len(bounds) != 2:
                return None
            try:
                start, end = int(bounds[0]), int(bounds[1])
            except ValueError:
                return None
            if start < 1 or end < start or (end - start + 1) > _MAX_TOPIC_IDS:
                return None
            result.update(range(start, end + 1))
        else:
            try:
                n = int(part)
            except ValueError:
                return None
            if n < 1:
                return None
            result.add(n)
        if len(result) > _MAX_TOPIC_IDS:
            return None
    return sorted(result)


def _format_topic_ranges(ids: list[int]) -> str:
    """Collapses a sorted id list back into "3-6, 8, 10-20" for display."""
    if not ids:
        return ""
    chunks = []
    start = prev = ids[0]
    for n in ids[1:]:
        if n == prev + 1:
            prev = n
            continue
        chunks.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = n
    chunks.append(f"{start}-{prev}" if start != prev else str(start))
    return ", ".join(chunks)


_MAX_TOPIC_IDS = 500
