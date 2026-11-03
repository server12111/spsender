"""Frees channel slots when an account hits Telegram's limit (500 channels and
supergroups): leaves its OLDEST broadcast channels, a few at a time with
pauses, never touching anything the bot relies on. Sponsor channels the bot
joined itself go first, so an owner's own subscriptions are touched last.

Only broadcast channels are left — sponsor channels are channels, while
mailings, the parser and warmup groups live in groups, so a group the account
needs can never be left by mistake (even one added by an invite link that
can't be matched by name).

Never left:
  * channels where the account is the owner or an admin;
  * mailing targets of any mailing this account sends for;
  * forward sources of the owner's mailings;
  * the parser's chats of this account;
  * warmup channels.
"""
import asyncio
import logging
import random
import re
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

FREE_AT_ONCE = 20                 # channels left per cleanup
PAUSE_RANGE = (6.0, 14.0)         # seconds between two leaves
COOLDOWN = 30 * 60                # one cleanup per account per 30 min

_running: set[int] = set()
_last_run: dict[int, float] = {}
_tasks: set = set()   # strong refs, so background tasks aren't collected mid-run


def _spawn(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)

_NAME_RE = re.compile(r"(?:@|t\.me/|telegram\.me/)([A-Za-z][A-Za-z0-9_]{2,31})", re.I)
_BARE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{2,31}$")
_ID_RE = re.compile(r"-?\d{6,}")


def is_channels_too_much(e: BaseException) -> bool:
    return type(e).__name__ == "ChannelsTooMuchError" or "CHANNELS_TOO_MUCH" in str(e) \
        or "too many channels" in str(e).lower()


def _norm_id(raw: int) -> int:
    """-1001234567 / 1234567 → the bare channel id Telethon entities carry."""
    raw = abs(int(raw))
    s = str(raw)
    return int(s[3:]) if s.startswith("100") and len(s) > 12 else raw


def parse_identifiers(values) -> tuple[set[str], set[int]]:
    names: set[str] = set()
    ids: set[int] = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, int):
            ids.add(_norm_id(v))
            continue
        s = str(v).strip()
        for m in _NAME_RE.finditer(s):
            names.add(m.group(1).lower())
        for token in re.split(r"[\s,;]+", s):
            t = token.strip().lstrip("@")
            if _BARE_NAME_RE.match(t):
                names.add(t.lower())
        for m in _ID_RE.finditer(s):
            ids.add(_norm_id(int(m.group(0))))
    return names, ids


def pick_channels_to_leave(dialog_entities, names: set[str], ids: set[int], limit: int = FREE_AT_ONCE,
                           sponsors: set = frozenset()) -> list:
    """Broadcast channels that are safe to leave: sponsor channels the bot
    joined itself first, then the rest; oldest-joined first within each."""
    cands = []
    for e in dialog_entities:
        if not getattr(e, "broadcast", False):        # groups/megagroups: never
            continue
        if getattr(e, "left", False):
            continue
        if getattr(e, "creator", False) or getattr(e, "admin_rights", None):
            continue
        if e.id in ids or (getattr(e, "username", None) or "").lower() in names:
            continue
        if any((u.username or "").lower() in names for u in (getattr(e, "usernames", None) or [])):
            continue
        cands.append(e)
    # Channel.date is the date this account joined it.
    far = datetime.max.replace(tzinfo=timezone.utc)
    cands.sort(key=lambda e: (e.id not in sponsors, getattr(e, "date", None) or far))
    return cands[:limit]


def record_join(db, account_id: int, result, client=None, ref: str = None) -> None:
    """Remember the channel(s) a sponsor join (JoinChannel / ImportChatInvite
    result) put this account into. An account that was already in the channel
    gets a result without chats — then the channel is looked up by ref
    (@username) in the client's own cache."""
    ids = [c.id for c in (getattr(result, "chats", None) or []) if getattr(c, "broadcast", False)]
    if ids:
        _spawn(_safe_record(db, account_id, ids))
    elif client is not None and ref:
        _spawn(_record_by_ref(db, account_id, client, ref))


async def _record_by_ref(db, account_id: int, client, ref: str):
    try:
        entity = await asyncio.wait_for(client.get_entity(ref), timeout=15)
    except Exception as e:
        logger.debug(f"sponsor join not recorded ({ref}): {e}")
        return
    if getattr(entity, "broadcast", False):
        await _safe_record(db, account_id, [entity.id])


async def _safe_record(db, account_id: int, ids: list):
    try:
        await db.add_sponsor_joins(account_id, ids)
    except Exception as e:
        logger.debug(f"sponsor join not recorded: {e}")


def schedule(db, client, account_id: int, phone: str = "") -> bool:
    """Start a cleanup for this account in the background. False when one is
    already running or the account was cleaned less than COOLDOWN ago."""
    now = time.monotonic()
    if account_id in _running or now - _last_run.get(account_id, -1e9) < COOLDOWN:
        return False
    _running.add(account_id)
    _last_run[account_id] = now
    _spawn(_run(db, client, account_id, phone))
    return True


async def _run(db, client, account_id: int, phone: str):
    from telethon.errors import FloodWaitError
    from telethon.tl.functions.channels import LeaveChannelRequest
    left: list[str] = []
    try:
        names, ids = parse_identifiers(await db.channel_cleanup_protected(account_id))
        entities = [d.entity async for d in client.iter_dialogs()]
        sponsors = await db.get_sponsor_joins(account_id)
        left_ids: list[int] = []
        for e in pick_channels_to_leave(entities, names, ids, sponsors=sponsors):
            try:
                await client(LeaveChannelRequest(e))
                left.append(f"@{e.username}" if getattr(e, "username", None) else (getattr(e, "title", "") or str(e.id)))
                left_ids.append(e.id)
            except FloodWaitError as fw:
                logger.warning(f"Channel cleanup {phone}: FloodWait {fw.seconds}s — stopping after {len(left)}")
                break
            except Exception as ex:
                logger.info(f"Channel cleanup {phone}: couldn't leave {e.id}: {type(ex).__name__}")
                continue
            await asyncio.sleep(random.uniform(*PAUSE_RANGE))
        if left_ids:
            await db.remove_sponsor_joins(account_id, left_ids)
        logger.info(f"Channel cleanup {phone}: channel limit — left {len(left)} oldest channel(s)")
        if left:
            try:
                from ..eventlog import esc, log_event
                log_event("accounts", f"🧹 Аккаунт <code>{esc(phone)}</code> упёрся в лимит каналов — "
                                      f"вышел из {len(left)} старых: {esc(', '.join(left[:8]))}"
                                      + (" …" if len(left) > 8 else ""))
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Channel cleanup {phone} failed: {type(e).__name__}: {e}")
    finally:
        _running.discard(account_id)
