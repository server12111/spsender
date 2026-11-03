"""URL buttons under mailing messages.

A user account can't attach inline buttons to its own messages — only bots
can. So a message with buttons is sent the way Telegram allows: the account
sends our bot's inline result (the text/photo/video plus the buttons), which
shows "via @bot" under it. The inline query names the message by a signed
token, so nobody but our accounts can pull a mailing's text out of the bot.
"""
import hashlib
import hmac
import json
import re
from typing import Optional

from ..config import config

MAX_BUTTONS = 3
MAX_BUTTON_TEXT = 40
STYLES = {"primary": "🔵 Синяя", "success": "🟢 Зелёная", "danger": "🔴 Красная"}

_USERNAME = re.compile(r"@?([A-Za-z][A-Za-z0-9_]{3,31})")
_URL = re.compile(r"(?i)https?://[^\s/$.?#][^\s]*")
_TME = re.compile(r"(?i)(?:https?://)?(?:t|telegram)\.me/(\S+)")


def normalize_url(raw: str) -> Optional[str]:
    """A button link from what the user typed: @username, t.me/..., or an
    http(s) URL with a real host. None if it isn't one."""
    s = (raw or "").strip()
    if not s or len(s) > 512 or any(c.isspace() for c in s):
        return None
    m = _TME.fullmatch(s)
    if m:
        return f"https://t.me/{m.group(1)}"
    if s.startswith("@") or ("." not in s and "/" not in s):
        m = _USERNAME.fullmatch(s)
        return f"https://t.me/{m.group(1)}" if m else None
    if not _URL.fullmatch(s):
        if re.fullmatch(r"(?i)[a-z0-9-]+(\.[a-z0-9-]+)+(/\S*)?", s):
            s = "https://" + s
        else:
            return None
    host = re.sub(r"(?i)^https?://", "", s).split("/", 1)[0].split("?", 1)[0].split(":", 1)[0]
    if "." not in host or host.startswith(".") or host.endswith("."):
        return None
    return s


def parse_buttons(buttons_json: Optional[str]) -> list[dict]:
    try:
        data = json.loads(buttons_json) if buttons_json else []
    except (TypeError, ValueError):
        return []
    out = []
    for b in data if isinstance(data, list) else []:
        if isinstance(b, dict) and b.get("text") and b.get("url"):
            out.append({"text": str(b["text"])[:MAX_BUTTON_TEXT], "url": str(b["url"]),
                        "style": b.get("style") if b.get("style") in STYLES else "primary"})
    return out[:MAX_BUTTONS]


def dump_buttons(buttons: list[dict]) -> Optional[str]:
    return json.dumps(buttons[:MAX_BUTTONS], ensure_ascii=False) if buttons else None


def _sig(payload: str) -> str:
    key = (config.BOT_TOKEN or "x").encode()
    return hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()[:16]


def inline_token(message_id: int, version: str) -> str:
    payload = f"m{message_id}v{version}"
    return f"{payload}s{_sig(payload)}"


def parse_inline_token(token: str) -> Optional[int]:
    m = re.fullmatch(r"(m(\d+)v([0-9a-f]{1,12}))s([0-9a-f]{16})", (token or "").strip())
    if not m or not hmac.compare_digest(_sig(m.group(1)), m.group(4)):
        return None
    return int(m.group(2))


def content_version(msg) -> str:
    """Changes whenever what the inline result shows changes — the query string
    then changes too, so Telegram can't serve a stale cached result."""
    raw = f"{msg.text}|{msg.entities_json}|{msg.buttons_json}|{msg.media_file_id}"
    return hashlib.sha1(raw.encode()).hexdigest()[:8]


def can_use_buttons(user, now) -> bool:
    """Paid tier only (and admins)."""
    if not user:
        return False
    if getattr(user, "is_admin", False):
        return True
    return bool(user.subscription_end and user.subscription_end > now)
