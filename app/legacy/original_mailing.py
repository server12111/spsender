"""Original mailing engine from the owner-provided Git bundle."""
import asyncio
import collections
import html
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    ChatGuestSendForbiddenError,
    ChatRestrictedError,
    ChatSendMediaForbiddenError,
    ChatWriteForbiddenError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteRequestSentError,
    MsgIdInvalidError,
    PeerFloodError,
    RightForbiddenError,
    SessionRevokedError,
    SlowModeWaitError,
    TopicDeletedError,
    UserAlreadyParticipantError,
    UserBannedInChannelError,
    UserDeactivatedBanError,
    UserDeactivatedError,
    UserNotParticipantError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from .clock import now_moscow
from .config import config
from .mailing_database import LegacyMailingDatabase as Database
from .mailing_database import is_safe_media_path
from .utils import channel_cleanup
from .utils.time_utils import is_within_active_hours


def pe(value):
    return value

def subscription_expired_keyboard():
    return None


_BAN_ERRORS = (
    UserDeactivatedBanError,
    UserDeactivatedError,
    AuthKeyUnregisteredError,
    SessionRevokedError,
    AuthKeyDuplicatedError,
)

_CHAT_BAN_ERRORS = (
    UserBannedInChannelError,
)

_SPAMBOT_CHECK_COOLDOWN = 6 * 3600

_SPAM_WINDOW = 20

_CHAT_SPREAD_MIN_GAP = 5

_CHAT_SPREAD_MAX_GAP = 120

_CHAT_SENDERS_REFRESH = 300

_SPAM_MIN_ATTEMPTS = 10

_SPAM_ERROR_SHARE = 0.5

_SPAMBOT_FREE_MARKERS = (
    "no limits", "free as a bird", "good news",
    "свободен", "нет ограничений", "не ограничен", "хорошие новости",
    "вільний", "немає обмежень", "гарні новини",
)

_SPAMBOT_TOS_MARKERS = ("blocked for violation", "заблокирован за нарушени", "заблоковано за порушен")

_SPAMBOT_LIMIT_MARKERS = (
    "limited", "limit", "sorry", "harsh response", "anti-spam",
    "ограничен", "к сожалению", "антиспам", "жалоб",
    "обмежен", "на жаль",
)

_SPAMBOT_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "янв": 1, "фев": 2, "мар": 3, "апр": 4, "мая": 5, "май": 5, "июн": 6,
    "июл": 7, "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
    "січ": 1, "лют": 2, "бер": 3, "кві": 4, "тра": 5, "чер": 6,
    "лип": 7, "сер": 8, "вер": 9, "жов": 10, "лис": 11, "гру": 12,
}

_SPAMBOT_RECHECK = {"free": 6 * 3600, "unknown": 6 * 3600, "limited": 24 * 3600, "tos": 24 * 3600}

def parse_spambot_reply(text: str) -> tuple:
    """(status, until) from @SpamBot's answer. status: "free", "tos",
    "limited" (until = when it lifts, UTC, if the answer names a date) or
    "unknown" (a greeting, a menu — not a status)."""
    low = (text or "").lower()
    if any(m in low for m in _SPAMBOT_TOS_MARKERS):
        return "tos", None
    if any(m in low for m in _SPAMBOT_FREE_MARKERS):
        return "free", None
    until = None
    m = re.search(r"(\d{1,2})\s+([a-zа-яё]{3,})\.?\s+(\d{4})(?:[^\d]{1,12}(\d{1,2}):(\d{2}))?", low)
    if m:
        month = _SPAMBOT_MONTHS.get(m.group(2)[:3])
        if month:
            try:
                until = datetime(int(m.group(3)), month, int(m.group(1)),
                                 int(m.group(4) or 0), int(m.group(5) or 0))
            except ValueError:
                until = None
    if until or any(mk in low for mk in _SPAMBOT_LIMIT_MARKERS):
        return "limited", until
    return "unknown", None

_TOS_BLOCK_PAUSE = 3 * 3600

_INVALID_TOPIC_ERRORS = (
    MsgIdInvalidError,
    TopicDeletedError,
)

_FROZEN_ACCOUNT_MARKER = "not available for frozen accounts"

_NOT_MEMBER_ERRORS = (
    UserNotParticipantError,
    ChatWriteForbiddenError,
    # "guests can't send here" — the account just isn't in the chat yet: join
    # it (this used to be parked as a ban, so the account never joined).
    ChatGuestSendForbiddenError,
)

logger = logging.getLogger(__name__)

def _build_telethon_entities(entities_json: str) -> list:
    """Convert serialized aiogram entities JSON to Telethon MessageEntity objects."""
    from telethon.tl.types import (
        MessageEntityBlockquote,
        MessageEntityBold,
        MessageEntityCode,
        MessageEntityCustomEmoji,
        MessageEntityItalic,
        MessageEntityPre,
        MessageEntitySpoiler,
        MessageEntityStrike,
        MessageEntityTextUrl,
        MessageEntityUnderline,
    )
    TYPE_MAP = {
        "bold":          MessageEntityBold,
        "italic":        MessageEntityItalic,
        "code":          MessageEntityCode,
        "underline":     MessageEntityUnderline,
        "strikethrough": MessageEntityStrike,
        "spoiler":       MessageEntitySpoiler,
    }
    result = []
    try:
        items = json.loads(entities_json)
    except Exception:
        return result
    for e in items:
        t = e.get("type", "")
        o = e.get("offset", 0)
        l = e.get("length", 0)
        if t in TYPE_MAP:
            result.append(TYPE_MAP[t](offset=o, length=l))
        elif t == "pre":
            result.append(MessageEntityPre(offset=o, length=l,
                                           language=e.get("language", "") or ""))
        elif t == "text_link":
            result.append(MessageEntityTextUrl(offset=o, length=l, url=e.get("url", "")))
        elif t == "custom_emoji":
            try:
                result.append(MessageEntityCustomEmoji(
                    offset=o, length=l,
                    document_id=int(e["custom_emoji_id"])
                ))
            except (KeyError, ValueError):
                pass
        elif t == "blockquote":
            try:
                result.append(MessageEntityBlockquote(offset=o, length=l))
            except TypeError:
                pass
        elif t == "expandable_blockquote":
            try:
                result.append(MessageEntityBlockquote(offset=o, length=l, collapsed=True))
            except TypeError:
                pass
    return result

def _utf16_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2

def _truncate_utf16(text: str, max_units: int) -> str:
    if _utf16_length(text) <= max_units:
        return text
    return text.encode("utf-16-le")[:max_units * 2].decode("utf-16-le", errors="ignore")

_HOMOGLYPH_LOWER = {
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x",
}

_HOMOGLYPH_UPPER = {
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H",
    "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X",
}

def _apply_homoglyph_swap(text: str, entities: Optional[list]) -> str:
    """Replace exactly one eligible Cyrillic letter in `text` with its Latin
    look-alike (matching case, so a capital never turns into a lowercase
    letter or vice versa). Positions covered by any entity are excluded
    outright — a text-link's underlying URL wouldn't care, but an @mention/
    #hashtag/bot_command is parsed from the literal text itself, so touching
    one there could silently break it. Works in UTF-16 code units throughout
    (not Python string indices) so it lines up exactly with entity offsets
    even when the text also contains astral characters (e.g. some emoji)
    earlier on, which would otherwise desync a naive index-based approach.
    A no-op (returns text unchanged) if nothing eligible is found."""
    if not text:
        return text
    units = text.encode("utf-16-le")
    n_units = len(units) // 2
    excluded = set()
    if entities:
        for e in entities:
            off = getattr(e, "offset", None)
            length = getattr(e, "length", None)
            if off is None or length is None:
                continue
            excluded.update(range(off, off + length))
    candidates = []
    for i in range(n_units):
        if i in excluded:
            continue
        ch = units[i * 2:i * 2 + 2].decode("utf-16-le", errors="ignore")
        if ch in _HOMOGLYPH_LOWER or ch in _HOMOGLYPH_UPPER:
            candidates.append(i)
    if not candidates:
        return text
    idx = random.choice(candidates)
    ch = units[idx * 2:idx * 2 + 2].decode("utf-16-le")
    repl = _HOMOGLYPH_LOWER.get(ch) or _HOMOGLYPH_UPPER[ch]
    repl_units = repl.encode("utf-16-le")
    new_units = units[:idx * 2] + repl_units + units[idx * 2 + 2:]
    return new_units.decode("utf-16-le")

_bot_username = "feAutoSenderBot"

def set_bot_username(username: str) -> None:
    global _bot_username
    if username:
        _bot_username = username

def get_bot_username() -> str:
    return _bot_username

def _free_tier_signatures() -> list[str]:
    return [
        f"\n━━━━━━━━━━\n🤖Отправлено через @{_bot_username}",
        f"\n━━━━━━━━━━\n🤖 Рассылай бесплатно @{_bot_username}",
        f"\n━━━━━━━━━━\n🤖 Рассылка через @{_bot_username}",
    ]

def _free_tier_signature(account_id: Optional[int]) -> str:
    signatures = _free_tier_signatures()
    if account_id is None:
        return signatures[0]
    return signatures[account_id % len(signatures)]

_RESTORED_MAILING_START_STEP_SECONDS = 0.30

_RESTORED_MAILING_MAX_START_DELAY_SECONDS = 30.0

_LAST_SENT_FLUSH_INTERVAL_SECONDS = 3.0

class ForwardSourceError(RuntimeError):
    """A forward mailing's source chat can't be resolved by this account. It's
    the source's problem, never the target's: read as "the target's username
    is gone", it emptied whole mailings (every chat removed one by one).
    `final`: it failed again on the retry 30 min later — the mailing stops."""
    final = False

_FORWARD_SOURCE_ERRORS = ("PeerIdInvalidError", "MessageIdInvalidError", "MessageIdsEmptyError")

def _is_dead_username_error(e: Exception) -> bool:
    """The target's @username isn't taken by anyone any more (freed, renamed
    or deleted chat) or can't be one at all (UsernameInvalidError: "@chatname"
    was retried every 25 cycles forever) — the target goes."""
    if type(e).__name__ in ("UsernameNotOccupiedError", "UsernameInvalidError"):
        return True
    s = str(e)
    return isinstance(e, ValueError) and s.startswith("No user has") and "as username" in s

_CHAT_BAN_SKIP_CYCLES = 25

_JOIN_MIN_GAP = 30

_FIRST_HELLO = ["Привет", "Всем привет", "Привет всем)", "Здравствуйте", "Всем привет)"]

_GATE_CHECK_DELAY = 6

class _GateEvent:
    """A history message dressed as the NewMessage event handle_sponsor_check
    expects (same attributes it reads)."""
    is_private = False

    def __init__(self, message, client):
        self.message = message
        self.client = client
        self.reply_to = getattr(message, "reply_to", None)
        self.chat_id = getattr(message, "chat_id", None)

    async def get_reply_message(self):
        return await self.message.get_reply_message()

    async def get_sender(self):
        return await self.message.get_sender()

_DEAD_INVITE_ERRORS = ("InviteHashExpiredError", "InviteHashInvalidError")

def _is_cached_private_chat(client, chat: str) -> bool:
    """The account's session already knows this @username as a person or a
    bot — a send there lands in their DMs instead of a group. Session cache
    only, no request; unknown usernames are left to the normal send."""
    c = (chat or "").strip()
    if not c.startswith("@"):
        return False
    try:
        row = client.session.get_entity_rows_by_username(c[1:].lower())
    except Exception:
        return False
    if not row:
        return False
    try:
        from telethon import utils as tl_utils
        from telethon.tl.types import PeerUser
        return tl_utils.resolve_id(row[0])[1] is PeerUser
    except Exception:
        return False

def _has_text(msg) -> bool:
    """Can go to a text-only chat: a forward (sent as is) or a message with text."""
    return bool(getattr(msg, "is_forward", False) or (getattr(msg, "text", None) or "").strip())

async def _is_private_chat_now(client, chat: str) -> bool:
    """Fresh look before removing a target the cache calls a person: a
    username can move from a person to a group, and the cache would still
    have the old owner. Any doubt (error, timeout) keeps the target."""
    try:
        from telethon.tl.types import User
        entity = await asyncio.wait_for(client.get_entity(chat), timeout=10)
        return isinstance(entity, User)
    except Exception:
        return False

def _is_malformed_target(chat: str) -> bool:
    """Not a chat at all: a link glued after "@" (@https://t.me/x), several
    links pasted as one target, a site URL — Telegram can never resolve it."""
    c = (chat or "").strip()
    return ("\n" in c or " " in c
            or re.match(r"^@(https?(:|tme|telegramme)|t\.me/|telegram\.me/|www\.)", c.lower()) is not None
            # "@" + something no username can be: "@О" (Cyrillic), "@c", "@1"
            or (c.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{4,32}", c) is None))

_BAN_BACKOFF_ERRORS = {
    "UserBannedInChannelError", "ChannelPrivateError", "ChatAdminRequiredError",
    "ChatRestrictedError", "ChatGuestSendForbiddenError", "ChatWriteForbiddenError",
    "ChatSendPlainForbiddenError",
}

_BAN_BACKOFF_HOURS = (1, 3, 12, 24, 48)

_BAN_STRIKE_MEMORY = 7 * 86400

_TOPIC_CLOSED_SKIP_CYCLES = 50

_HIDDEN_TAG_COOLDOWN_HOURS = 24

_ADMIN_IDS_CACHE_TTL_SECONDS = 3600

_PARSER_CACHE_CAP = 3000

_PARSER_FLUSH_INTERVAL_SECONDS = 5.0

_PARSER_MAX_MESSAGE_LEN = 200

_PARSER_MAX_CHATS_PER_ACCOUNT = 20

_PARSER_MAX_ACTIVE_PER_USER = 3

_PARSER_RESOLVE_RETRY_SECONDS = 300

class _BoundedSet:
    """Fixed-capacity 'seen' set — insertion-order eviction once full.
    Deliberately a hard cap, not a TTL: memory stays bounded no matter how
    much traffic a watched chat produces (see ParserService)."""

    def __init__(self, capacity: int):
        self._capacity = capacity
        self._data: dict = {}

    def __contains__(self, item) -> bool:
        return item in self._data

    def add(self, item):
        if item in self._data:
            return
        self._data[item] = None
        if len(self._data) > self._capacity:
            self._data.pop(next(iter(self._data)))

def _flood_rest() -> int:
    """Random extra rest after a mailing FloodWait (10-15 min by default)."""
    lo = max(0, config.FLOOD_REST_MIN_SECONDS)
    return random.randint(lo, max(lo, config.FLOOD_REST_MAX_SECONDS))

class MailingService:
    def __init__(self, db: Database, userbot_manager):
        self.db = db
        db.on_subscription_renewed = self._resume_after_renewal
        self.userbot_manager = userbot_manager
        self._tasks: dict[int, asyncio.Task] = {}
        self._running = False
        self._stagger_until: dict[int, dict[int, datetime]] = {}  # mailing_id -> {target_id -> first_send_allowed_at}
        self._me_cache: dict[int, object] = {}  # account_id -> me object, avoids get_me() every loop
        self._admin_ids_cache: dict[str, tuple] = {}  # target -> (fetched_at_monotonic, admin_ids)
        self._mailing_locks: dict[int, asyncio.Lock] = {}
        self._working_targets: dict[int, set[int]] = {}
        self._message_rotation: dict[int, int] = {}  # mailing_id -> next message index (round-robin, not random)
        self._topic_rotation: dict[int, int] = {}  # target_id -> next topic index for multi-topic targets (round-robin)
        self._debug_account_cache: dict[int, bool] = {}  # account_id -> matches config.DEBUG_PHONE, cached
        self._flood_notify_locks: dict[int, asyncio.Lock] = {}  # account_id -> lock, serializes _notify_flood_wait
        # account_id -> last send time, ACROSS all mailings using that account.
        # Each mailing only paces itself against its own interval, so an
        # account shared by several mailings could otherwise be sent from at
        # several times its configured rate in aggregate — Telegram's
        # FloodWait/frozen-account detection looks at the account as a whole,
        # not per-mailing.
        self._account_last_send: dict[int, datetime] = {}
        # Flood-wait expiry is persisted in accounts.flood_wait_until (DB), not
        # kept here in memory — a bot restart (which happens on every deploy)
        # must not forget an account is still in an active flood window and
        # re-notify about the very same episode on the next reconnect/send.

        # target_id -> (sent_at, account_id), flushed to the DB in one batch
        # every _LAST_SENT_FLUSH_INTERVAL_SECONDS instead of one commit per
        # send — see _flush_last_sent_loop. A crash before the next flush
        # loses at most that interval's worth of bookkeeping (a target could
        # be sent to once more than intended after a restart) — a bounded,
        # deliberate tradeoff for cutting the DB's dominant write source by
        # an order of magnitude under load.
        self._pending_last_sent: dict[int, tuple[datetime, Optional[int], Optional[int]]] = {}
        self._target_next_msg: dict[int, int] = {}  # target_id -> its text index after the send in flight
        self._flush_task: Optional[asyncio.Task] = None

        # target_id -> cycles left to skip after a chat-specific ban/
        # restriction (_apply_target_restriction) — see _CHAT_BAN_SKIP_CYCLES.
        # In-memory only: a restart just means one extra attempt sooner,
        # which is fine.
        self._chat_ban_skip: dict[int, int] = {}
        self._ban_until: dict[int, float] = {}            # target_id -> monotonic time it's skipped until
        self._ban_strikes: dict[int, tuple[int, float]] = {}  # target_id -> (strikes, wall time of the last one)
        self._no_hello: set[int] = set()  # chats that forbid plain text — no "hi" first there
        # target_id -> monotonic time: our last message there was still the
        # chat's latest, so the chat waits a round ("don't repeat yourself")
        self._unseen_until: dict[int, float] = {}
        self._gate_tasks: set = set()        # post-send sponsor-gate checks in flight
        self._gates_seen: set = set()        # (chat id, message id) gates already handled
        self._last_join: dict = {}  # account_id -> monotonic time of its last chat join attempt
        self._fwd_src_bad: dict = {}  # (account_id, forward source) -> monotonic time to retry resolving it
        self._fwd_src_strikes: dict = {}  # (account_id, forward source) -> failures in a row
        self._no_inline_chats: set = set()  # chats that forbid inline bots — button messages go plain there
        self._buttons_warned: dict = {}     # error name -> monotonic time to log it again
        # account_id -> consecutive sends that hit the 60s send timeout. A
        # client can stay is_connected() while its link silently stopped
        # relaying; after a couple of these it gets torn down and rebuilt.
        self._send_timeouts: dict[int, int] = {}
        # account_id -> reconnects for timeouts with no successful send since;
        # a link that stays dead (a broken proxy) then pauses instead of being
        # rebuilt every few minutes forever.
        self._timeout_reconnects: dict[int, int] = {}
        self._timeout_pause_until: dict[int, float] = {}
        self._reconnect_times: dict[int, list] = {}  # account_id -> monotonic times of timeout reconnects
        self._tos_blocked: set[int] = set()  # accounts @SpamBot says are blocked for ToS violations
        self._tos_pause_until: dict[int, float] = {}
        self._spambot_checked_at: dict[int, float] = {}
        # account_id -> {"status", "until" (UTC epoch or None), "checked" (epoch), "next" (epoch), "text"}
        self._spambot_state: dict[int, dict] = {}
        self._chat_last_send: dict[str, float] = {}  # chat key -> monotonic time of last send by any mailing
        self._chat_senders: dict[str, int] = {}  # chat key -> active mailings targeting it
        self._chat_senders_at: float = 0.0
        # Same, per owner: one user's several mailings in one chat take turns
        # (each waits interval / their count after the owner's last post there).
        self._owner_chat_senders: dict[tuple, int] = {}
        self._owner_chat_last_send: dict[tuple, float] = {}
        self._send_outcomes: dict[int, collections.deque] = {}  # account_id -> recent True(sent)/False(chat error)
        self._spam_ctx: dict[int, tuple] = {}  # mailing_id -> (account_id, client, owner) of the current send
        self._spambot_tasks: set = set()
        # (target_id, account_id) that joined fine but still got
        # ChatWriteForbidden (muted / new-member restriction). That error
        # also means "not a member", so without this every backoff window
        # re-ran JoinChannel + a 20-40s wait on a chat the account is
        # already in (~1400 times/day). Cleared on the next successful send.
        self._write_forbidden_after_join: set[tuple[int, Optional[int]]] = set()
        # (target_id, account_id) -> (monotonic time until no auto-join, strikes):
        # a chat that kicks/bans the account right after it joins was rejoined
        # every backoff window (~26 times a day per chat in production).
        self._rejoin_block: dict[tuple, tuple[float, int]] = {}
        # (account_id, path, mtime_ns, size) -> Photo/Document Telegram returned
        # for that file's first send from that account. Sending by path made
        # Telethon re-read and re-upload the whole file on every single send
        # (videos included) — thousands of uploads/hour and a constant churn
        # of large buffers. Media objects are per-account (access_hash/
        # file_reference), hence the account in the key.
        self._sent_media_cache: dict[tuple, object] = {}

    @staticmethod
    def _media_cache_key(account_id: Optional[int], path: str) -> Optional[tuple]:
        if account_id is None:
            return None
        try:
            st = os.stat(path)
        except OSError:
            return None
        return (account_id, path, st.st_mtime_ns, st.st_size)

    async def _send_media_cached(self, client, account_id: Optional[int], target, paths: list, **kwargs):
        keys = [self._media_cache_key(account_id, p) for p in paths]
        cached = [self._sent_media_cache.get(k) if k else None for k in keys]
        if all(c is not None for c in cached):
            try:
                return await client.send_file(target, cached if len(cached) > 1 else cached[0], **kwargs)
            except Exception as e:
                # An expired/invalid file reference only means our cached
                # copy went stale — re-upload once. Anything else (chat
                # restrictions, flood waits...) is a real send failure.
                reason = f"{type(e).__name__} {getattr(e, 'message', '')}".upper()
                if "FILEREFERENCE" not in reason and "FILE_REFERENCE" not in reason and "MEDIA_EMPTY" not in reason \
                        and "MEDIAEMPTY" not in reason:
                    raise
                for k in keys:
                    self._sent_media_cache.pop(k, None)
        result = await client.send_file(target, paths if len(paths) > 1 else paths[0], **kwargs)
        sent = result if isinstance(result, list) else [result]
        medias = [getattr(m, "photo", None) or getattr(m, "document", None) for m in sent]
        if len(medias) == len(keys) and all(medias) and all(keys):
            if len(self._sent_media_cache) > 5000:
                self._sent_media_cache.clear()
            for k, media in zip(keys, medias):
                self._sent_media_cache[k] = media
        return result

    async def _diag_enabled(self, account_id: int) -> bool:
        """Same idea as manager.py's _diag_enabled: MAILING_DEBUG's [DIAG]
        lines, plus the same lines for one account matching
        config.DEBUG_PHONE — cached per account_id so this costs at most one
        extra DB read per account, not one per send. Pure logging, no effect
        on control flow."""
        if config.MAILING_DEBUG:
            return True
        if not config.DEBUG_PHONE:
            return False
        cached = self._debug_account_cache.get(account_id)
        if cached is not None:
            return cached
        try:
            account = await self.db.get_account(account_id)
            result = bool(account and account.phone == config.DEBUG_PHONE)
        except Exception:
            result = False
        self._debug_account_cache[account_id] = result
        return result

    async def start(self):
        self._running = True
        # Without this, _account_last_send starts empty on every restart and
        # every account's very first send goes out with no throttle gap at
        # all — across 150+ active mailings that's a burst, not a restart.
        try:
            self._account_last_send.update(await self.db.get_last_send_per_account())
        except Exception:
            logger.warning("Failed to seed account send throttle from DB — starting cold", exc_info=True)
        await self._restore_state()
        mailings = await self.db.get_active_mailings()
        # The cap must scale with how many mailings there are — a fixed 30s
        # cap with a 0.3s step means everything past the 100th mailing
        # (index * step >= 30s) gets clamped to the exact same delay and
        # starts in the same instant. With 150+ active mailings (routine at
        # this fleet's size) that's most of them firing their first
        # reconnect/send burst simultaneously right after every restart —
        # the opposite of what this stagger exists to prevent. Scaling the
        # cap up for large fleets keeps every mailing's delay distinct while
        # still bounding small fleets to the original 30s.
        max_delay = max(
            _RESTORED_MAILING_MAX_START_DELAY_SECONDS,
            len(mailings) * _RESTORED_MAILING_START_STEP_SECONDS,
        )
        for index, mailing in enumerate(mailings):
            initial_delay = min(
                index * _RESTORED_MAILING_START_STEP_SECONDS,
                max_delay,
            )
            await self._start_mailing_task(mailing.id, initial_delay=initial_delay)
        self._flush_task = asyncio.create_task(self._flush_last_sent_loop())
        logger.info(
            "Mailing service started with %s active mailings "
            "(restored starts spread over up to %.0fs)",
            len(mailings),
            max_delay,
        )

    async def _flush_last_sent_loop(self):
        n = 0
        while self._running:
            await asyncio.sleep(_LAST_SENT_FLUSH_INTERVAL_SECONDS)
            await self._flush_last_sent()
            n += 1
            if n % 20 == 0:  # ~once a minute
                await self._save_state()
                await self.db.flush_mailing_stats()
            if n % 28800 == 1:  # at start, then ~once a day
                try:
                    await self.db.prune_mailing_stats()
                except Exception as e:
                    logger.warning(f"Mailing stats prune failed: {e}")

    async def _save_state(self):
        """Persist cooldowns that used to reset on every restart."""
        from .state_store import mono_to_wall, save_state
        now = time.monotonic()
        await save_state(self.db, "mailing.rejoin_block", [
            [k[0], k[1], mono_to_wall(v[0]), v[1]] for k, v in self._rejoin_block.items()
            if v[0] > now - 48 * 3600  # strikes are remembered up to 48h past the block
        ])
        await save_state(self.db, "mailing.ban_backoff", [
            [tid, mono_to_wall(self._ban_until.get(tid, 0)), st[0], round(st[1])]
            for tid, st in self._ban_strikes.items() if time.time() - st[1] <= _BAN_STRIKE_MEMORY
        ])
        await save_state(self.db, "mailing.tos", {
            "blocked": sorted(self._tos_blocked),
            "pause": {str(a): mono_to_wall(t) for a, t in self._tos_pause_until.items() if t > now},
        })
        await save_state(self.db, "mailing.spambot", {
            str(a): v for a, v in self._spambot_state.items() if time.time() - v.get("checked", 0) < 30 * 86400
        })

    async def _restore_state(self):
        from .state_store import load_state, wall_to_mono
        try:
            for tid, aid, until, strikes in await load_state(self.db, "mailing.rejoin_block", []) or []:
                self._rejoin_block[(tid, aid)] = (wall_to_mono(until), strikes)
            for tid, until, strikes, last in await load_state(self.db, "mailing.ban_backoff", []) or []:
                self._ban_strikes[tid] = (strikes, last)
                self._ban_until[tid] = wall_to_mono(until)
            tos = await load_state(self.db, "mailing.tos", {}) or {}
            self._tos_blocked.update(tos.get("blocked", []))
            for a, t in (tos.get("pause") or {}).items():
                self._tos_pause_until[int(a)] = wall_to_mono(t)
            for a, v in (await load_state(self.db, "mailing.spambot", {}) or {}).items():
                if isinstance(v, dict):
                    self._spambot_state[int(a)] = v
            if self._rejoin_block or self._tos_blocked:
                logger.info(f"Mailing state restored: {len(self._rejoin_block)} rejoin cooldowns, "
                            f"{len(self._tos_blocked)} ToS-blocked accounts, {len(self._ban_strikes)} chat ban pauses")
        except Exception as e:
            logger.warning(f"Mailing state restore failed: {e}")

    async def _flush_last_sent(self):
        if not self._pending_last_sent:
            return
        # Swap the dict out before awaiting the DB write so sends that land
        # while the flush is in flight go into a fresh dict instead of
        # racing with iteration over the one being written.
        batch = self._pending_last_sent
        self._pending_last_sent = {}
        try:
            await self.db.flush_target_last_sent(batch)
        except Exception:
            logger.error("Failed to flush batched last_sent_at updates (%d targets)", len(batch), exc_info=True)
            # Put them back so the next flush retries rather than silently
            # dropping this batch — a newer update for the same target_id
            # (if one arrived since) correctly wins since dict union below
            # keeps the newer side.
            self._pending_last_sent = {**batch, **self._pending_last_sent}

    def _record_sent(self, target_id: int, account_id: Optional[int], mailing_id: Optional[int] = None):
        """Buffer a successful send for the next batched flush instead of
        writing to the DB immediately — see _pending_last_sent."""
        now = now_moscow()
        if mailing_id is not None:
            try:
                self.db.note_mailing_sent(mailing_id, target_id)
            except Exception:
                pass  # statistics must never break a send
        if account_id in self._tos_blocked:
            self._tos_blocked.discard(account_id)  # it can post again
            self._tos_pause_until.pop(account_id, None)
        self._pending_last_sent[target_id] = (now, account_id, self._target_next_msg.pop(target_id, None))
        self._write_forbidden_after_join.discard((target_id, account_id))
        if target_id in self._ban_strikes:  # the chat lets us post again
            self._ban_strikes.pop(target_id, None)
            self._ban_until.pop(target_id, None)

    def _schedule_gate_check(self, client, target, account_id: int):
        """An account that doesn't receive updates can't see a sponsor bot's
        "subscribe first" reply — look at the chat once, shortly after the
        send. Accounts that do receive updates handle it live (no double).
        Called right after a delivered send: it must never raise, or that
        send would be counted as failed."""
        try:
            mgr = getattr(self, "userbot_manager", None)
            handler = getattr(mgr, "_sponsor_check_handler", None)
            wants = getattr(mgr, "_wants_updates", None)
            if not handler or wants is None or wants(account_id):
                return
            task = asyncio.create_task(self._gate_check(client, target, account_id, handler, time.time()))
            self._gate_tasks.add(task)
            task.add_done_callback(self._gate_tasks.discard)
        except Exception as e:
            logger.debug(f"Sponsor gate check not scheduled for account {account_id}: {e}")

    async def _gate_check(self, client, target, account_id: int, handler, sent_ts: float):
        try:
            await asyncio.sleep(_GATE_CHECK_DELAY)
            account = await self.db.get_account(account_id)
            me_id = getattr(self.userbot_manager, "_me_ids", {}).get(account_id)
            if not account or not account.auto_subscribe_sponsors or not me_id:
                return
            msgs = await asyncio.wait_for(client.get_messages(target, limit=8), timeout=15)
            for m in msgs or []:
                if not getattr(m, "mentioned", False):
                    continue
                date = getattr(m, "date", None)
                if date is not None and date.timestamp() < sent_ts - 5:
                    continue                      # an older gate, already handled
                key = (getattr(m, "chat_id", None), m.id)
                if key in self._gates_seen:
                    continue
                if len(self._gates_seen) > 20000:
                    self._gates_seen.clear()
                self._gates_seen.add(key)
                await handler(_GateEvent(m, client), account, me_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"Sponsor gate check for account {account_id} in {target} skipped: {e}")

    async def _last_message_is_ours(self, client, target, thread_id=None) -> bool:
        """True when the chat's (or topic's) latest real message — service
        lines like "X joined" don't count — was sent by one of our accounts.
        Any trouble reading the chat → False: the send goes ahead as usual."""
        try:
            kwargs = {"limit": 3}
            if thread_id:
                kwargs["reply_to"] = thread_id
            msgs = await asyncio.wait_for(client.get_messages(target, **kwargs), timeout=15)
        except Exception:
            return False
        last = next((m for m in (msgs or []) if not getattr(m, "action", None)), None)
        if last is None:
            return False
        if getattr(last, "out", False):
            return True
        ours = set(getattr(getattr(self, "userbot_manager", None), "_me_ids", {}).values())
        return getattr(last, "sender_id", None) in ours

    def _apply_pending_last_sent(self, targets: list):
        """A target just sent to but not yet flushed still has its old
        last_sent_at in the DB row get_mailing_targets() just fetched — apply
        any not-yet-flushed value on top so the per-target interval check
        below sees the real last-send time, not a stale one from before the
        flush. Without this, a target could be sent to twice within one
        flush interval."""
        if not self._pending_last_sent:
            return
        for t in targets:
            pending = self._pending_last_sent.get(t.id)
            if pending:
                t.last_sent_at, t.last_account_id, next_idx = pending
                if next_idx is not None:
                    t.msg_index = next_idx

    async def stop(self):
        self._running = False
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._working_targets.clear()
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        # Flush whatever's left on a clean shutdown — a deploy restart
        # shouldn't lose bookkeeping that a graceful stop could have saved.
        await self._flush_last_sent()
        await self._save_state()
        await self.db.flush_mailing_stats()

    async def start_mailing(self, mailing_id: int) -> bool:
        mailing = await self.db.get_mailing(mailing_id)
        if not mailing:
            return False
        account = await self.db.get_account(mailing.account_id)
        if not account or not account.is_active:
            return False
        client = await self.userbot_manager.get_client(mailing.account_id)
        if not client:
            return False
        await self.db.update_mailing_status(mailing_id, True)
        try:
            await self._start_mailing_task(mailing_id)
        except Exception:
            # The active status is already durable at this point.  Do not leave
            # a mailing marked active if its task could not be created.
            try:
                await self.db.update_mailing_status(mailing_id, False)
            except Exception as rollback_error:
                logger.error(
                    "Could not roll back active status for mailing %s: %s",
                    mailing_id,
                    rollback_error,
                    exc_info=True,
                )
            logger.error(
                "Could not start task for mailing %s; status was rolled back",
                mailing_id,
                exc_info=True,
            )
            return False
        return True

    async def stop_mailing(self, mailing_id: int):
        await self.db.update_mailing_status(mailing_id, False)
        task = self._tasks.pop(mailing_id, None)
        if task and task is not asyncio.current_task():
            task.cancel()
            try:
                # is_active is already False in the DB above, so even in the
                # worst case where the loop task is stuck on some unbounded
                # Telethon call and doesn't finish cancelling in time, giving
                # up on waiting here is safe — it'll not be found on the next
                # get_mailing() check whenever it does unwind, and the user
                # isn't left staring at a callback that never responds.
                await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=10)
            except asyncio.TimeoutError:
                logger.warning(f"Mailing {mailing_id}: task didn't finish cancelling within 10s, proceeding anyway")
        self._stagger_until.pop(mailing_id, None)
        self._working_targets.pop(mailing_id, None)
        self._message_rotation.pop(mailing_id, None)

    async def stop_user_mailings(self, user_id: int):
        """Stop all active mailings for a user (called when subscription expires)."""
        mailings = await self.db.get_user_active_mailings(user_id)
        # Remembered, so exactly these start again once the user renews.
        await self.db.mark_paused_by_expiry([m.id for m in mailings])
        for m in mailings:
            await self.stop_mailing(m.id)
        return len(mailings)

    async def _resume_after_renewal(self, user_id: int):
        """The mailings that were running when the subscription ran out start
        again right away after a renewal or a switch to the free tier."""
        try:
            pending = await self.db.get_expiry_paused_mailing_ids(user_id)
            started = 0
            # A start can fail just because the account is mid-reconnect —
            # give those a few more tries instead of leaving them off.
            for attempt in range(4):
                if attempt:
                    await asyncio.sleep(30)
                retry = []
                for mailing_id in pending:
                    mailing = await self.db.get_mailing(mailing_id)
                    account = await self.db.get_account(mailing.account_id) if mailing else None
                    if not mailing or not account or not account.is_active:
                        await self.db.clear_paused_by_expiry(mailing_id)
                        continue
                    if mailing.is_active:
                        continue  # started meanwhile (by the user)
                    if await self.start_mailing(mailing_id):
                        started += 1
                        logger.info(f"Mailing {mailing_id}: resumed after subscription renewal")
                    else:
                        retry.append(mailing_id)
                pending = retry
                if not pending:
                    break
            if pending:
                logger.warning(f"Could not resume mailings {pending} for user {user_id} after renewal")
            if not started:
                return
            notify = getattr(self.userbot_manager, '_bot_notify_callback', None)
            user = await self.db.get_user_by_id(user_id)
            if notify and user:
                await notify(
                    user.telegram_id,
                    pe(f"▶️ <b>Рассылки снова запущены</b>\n\n"
                       f"Включены рассылки, которые работали до окончания подписки: <b>{started}</b>"),
                )
        except Exception as e:
            logger.warning(f"Resume after renewal failed for user {user_id}: {e}")

    async def stop_mailings_for_account(self, account_id: int):
        """Stop every active mailing using this account — called when its
        own (not pool) proxy fails. Unlike a flood wait, a dead proxy has
        no known end time, so retrying every cycle forever would just keep
        generating failed connection attempts and re-notifying; stopping
        outright and letting the owner restart once they've fixed the
        proxy is the safer default."""
        mailings = await self.db.get_active_mailings_by_account(account_id)
        for m in mailings:
            await self.stop_mailing(m.id)
        return len(mailings)

    async def _send_with_buttons(self, client, target, msg, reply_to, text_only: bool, _diag) -> bool:
        """True if sent with its buttons; False to fall back to a plain send."""
        from .utils.msg_buttons import content_version, inline_token
        key = str(target)
        if key in self._no_inline_chats:
            return False
        query = inline_token(msg.id, content_version(msg)) + (" t" if text_only else "")
        try:
            results = await asyncio.wait_for(client.inline_query(get_bot_username(), query), timeout=20)
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            if self._buttons_warned.get(type(e).__name__, 0) < time.monotonic():
                self._buttons_warned[type(e).__name__] = time.monotonic() + 600
                logger.warning(f"Buttons: inline query for message {msg.id} failed ({type(e).__name__}: {e}) "
                               f"— sending without buttons")
            return False
        if not results or getattr(getattr(results[0], "result", None), "id", "") == "promo":
            return False
        _diag("API call — inline result (message with buttons)")
        try:
            await results[0].click(target, reply_to=reply_to)
        except Exception as e:
            if type(e).__name__ in ("ChatSendInlineForbiddenError", "BotInlineDisabledError"):
                if len(self._no_inline_chats) > 20000:
                    self._no_inline_chats.clear()
                self._no_inline_chats.add(key)
                return False  # this chat bans inline bots: the plain message goes instead
            raise  # a real problem of this chat (ban, slow mode, ...): the usual handling
        return True

    def _mark_target_working(self, mailing_id: int, target_id: int):
        self._working_targets.setdefault(mailing_id, set()).add(target_id)

    def _mark_target_not_working(self, mailing_id: int, target_id: int):
        targets = self._working_targets.get(mailing_id)
        if targets is not None:
            targets.discard(target_id)

    def _clear_working_targets(self, mailing_id: int):
        self._working_targets.pop(mailing_id, None)

    def forget_target(self, mailing_id: int, target_id: int):
        """Evict a deleted target from the in-memory working set immediately,
        instead of waiting for the next full-clear event to notice it's gone."""
        self._mark_target_not_working(mailing_id, target_id)
        self._write_forbidden_after_join = {
            k for k in self._write_forbidden_after_join if k[0] != target_id
        }

    async def get_free_tier_runtime_stats(self) -> dict:
        # `rows` (get_free_tier_target_rows) is every configured target of an
        # is_active-flagged free-tier mailing — that flag only means "the
        # owner hasn't turned it off," not "it's actually delivering": a
        # banned account, a dead proxy, or every target sitting in a
        # restriction cooldown all leave is_active=1 while nothing gets
        # sent. Filter down to targets with a genuinely recent send before
        # deriving any of the three stats, so "active" means the same thing
        # everywhere on this screen: actually working right now, not just
        # switched on.
        #
        # (An earlier version used a different live signal —
        # `_working_targets`, targets already hit in the CURRENT,
        # still-in-progress send cycle — for this. That set is cleared to
        # empty at the start of every cycle and refilled one target at a
        # time as sends succeed, so it fluctuated wildly between checks
        # with nothing actually having changed, and using it for only SOME
        # of the stats produced internally inconsistent numbers on the same
        # screen. last_sent_at doesn't have either problem: it only moves
        # forward on an actual delivery.)
        total_users = await self.db.count_free_tier_users()
        rows = await self.db.get_free_tier_target_rows()

        now = now_moscow()
        active_rows = []
        for row in rows:
            last_sent = row.get("last_sent_at")
            if not last_sent:
                continue
            # Floored at 6h so a short-interval mailing isn't flagged stale
            # over one missed cycle; scaled to 3x the target's own interval
            # so a legitimately slower mailing (or one riding out a
            # per-chat ban-skip cooldown — see _apply_target_restriction,
            # up to 25 cycles) isn't falsely called dead either.
            window = max(timedelta(hours=6), timedelta(seconds=row["delay_seconds"] * 3))
            if now - last_sent <= window:
                active_rows.append(row)

        active_user_ids = {row["user_id"] for row in active_rows}
        active_mailing_ids = {row["mailing_id"] for row in active_rows}
        chat_users: dict[str, dict] = {}

        for row in active_rows:
            chat = row["chat_identifier"]
            chat_data = chat_users.setdefault(
                chat.casefold(),
                {"chat_identifier": chat, "users": {}},
            )
            user_delays = chat_data["users"]
            user_delays.setdefault(row["user_id"], []).append(int(row["delay_seconds"]))

        chats = []
        for chat_data in chat_users.values():
            user_delays = chat_data["users"]
            average_by_user = [
                sum(delays) / len(delays)
                for delays in user_delays.values()
            ]
            chats.append({
                "chat_identifier": chat_data["chat_identifier"],
                "user_count": len(user_delays),
                "average_delay": round(sum(average_by_user) / len(average_by_user)),
            })
        chats.sort(key=lambda row: (-row["user_count"], row["chat_identifier"].lower()))

        return {
            "total_users": total_users,
            "active_users": len(active_user_ids),
            "active_mailings": len(active_mailing_ids),
            "unique_chats": len(chats),
            "chats": chats,
        }

    async def send_hidden_tag(self, mailing_id: int) -> int:
        mailing = await self.db.get_mailing(mailing_id)
        if not mailing:
            return 0
        client = await self.userbot_manager.get_client(mailing.account_id)
        if not client:
            return 0

        from telethon.tl.types import ChannelParticipantsAdmins, MessageEntityMentionName

        sent = 0
        for target_obj in await self.db.get_mailing_targets(mailing_id):
            target = target_obj.chat_identifier
            try:
                participants = await client.get_participants(target)
                try:
                    # Admins (this filter includes the creator too) are the
                    # ones who'd actually notice a mass silent-tag and can
                    # ban the account for it — leave them untagged. Only
                    # works for channels/supergroups (what get_participants
                    # itself needs a ChannelParticipantsAdmins-aware entity
                    # for); a legacy basic group raises here, in which case
                    # falling back to "no admins known" just restores the
                    # previous tag-everyone behavior for that one chat
                    # instead of failing the whole send.
                    admins = await client.get_participants(target, filter=ChannelParticipantsAdmins)
                    admin_ids = {u.id for u in admins}
                except Exception:
                    admin_ids = set()
                participants = [
                    user for user in participants
                    if getattr(user, "id", None) and not getattr(user, "bot", False)
                    and not getattr(user, "deleted", False)
                    and user.id not in admin_ids
                ]
                if not participants:
                    continue
                for start in range(0, len(participants), 4096):
                    batch = participants[start:start + 4096]
                    text = "\u2063" * len(batch)
                    entities = [
                        MessageEntityMentionName(offset=index, length=1, user_id=user.id)
                        for index, user in enumerate(batch)
                    ]
                    await client.send_message(
                        target, text, formatting_entities=entities,
                        reply_to=target_obj.thread_id,
                    )
                    sent += 1
            except Exception as error:
                logger.warning("Hidden tag failed for %s: %s", target, error)
        return sent

    async def _notify_flood_wait(self, user, account_id: int, seconds: int, rest: int = 0):
        # rest: extra pause on top of Telegram's wait so the account doesn't resume
        # the very second the wait ends (_flood_rest() for FloodWait).
        finish_at = now_moscow() + timedelta(seconds=seconds + rest)
        # Only notify once per flood-wait episode — the mailing loop checks
        # accounts.flood_wait_until itself and skips this account entirely
        # until it expires, so retrying (and re-notifying) every cycle would
        # just spam the same alert until the wait is actually over.
        # Persisted in the DB (not in-memory) — a bot restart between
        # notifications, which happens on every deploy, must not forget an
        # account is still in the SAME flood episode and re-notify about it.
        # Locked per-account: two mailings sharing one account can hit
        # FloodWaitError within the same instant, and without the lock both
        # could read "not yet flooded" before either write lands, sending a
        # duplicate alert for what's really one episode.
        # Wrapped end to end — this is called from inside the mailing loop's
        # FloodWaitError handler, and a transient DB hiccup in here (the
        # same lock-contention class of issue already fixed elsewhere
        # today) shouldn't crash that cycle and force the loop's 30s
        # fatal-error auto-restart when this is fundamentally just a
        # best-effort notification side effect.
        try:
            lock = self._flood_notify_locks.setdefault(account_id, asyncio.Lock())
            async with lock:
                already_flooded = await self.db.get_flood_wait_until(account_id)
                await self.db.set_flood_wait_until(account_id, finish_at)
                if already_flooded and already_flooded > now_moscow():
                    return

            notify = getattr(self.userbot_manager, '_bot_notify_callback', None)
            if not notify or not user or getattr(user, "mute_flood_notices", False):
                return
            from .keyboards.inline import flood_notice_keyboard
            account_obj = await self.db.get_account(account_id)
            await notify(
                user.telegram_id,
                pe(
                    "⚠️ <b>FloodWait</b>\n\n"
                    f"Аккаунт: <b>{html.escape(str(account_obj.display_name if account_obj else account_id))}</b>\n"
                    f"Длительность: <b>{seconds} сек.</b>\n"
                    + (f"Отдых аккаунта после ограничения: <b>+{round(rest / 60)} мин.</b>\n" if rest >= 60 else "")
                    + f"Рассылка возобновится (МСК): <b>{finish_at.strftime('%d.%m.%Y %H:%M:%S')}</b>\n\n"
                    "Рассылка с этого аккаунта возобновится автоматически."
                ),
                reply_markup=flood_notice_keyboard(),
            )
        except Exception:
            pass

    async def delete_mailing(self, mailing_id: int):
        await self.stop_mailing(mailing_id)
        await self.db.delete_mailing(mailing_id)

    async def _get_recent_user_ids(self, client, target: str, my_id: int) -> list:
        try:
            from telethon.tl.types import User

            messages = await client.get_messages(target, limit=100)
            result = []
            seen = set()
            for message in messages:
                sender_id = getattr(message, "sender_id", None)
                if not sender_id or sender_id == my_id or sender_id in seen:
                    continue
                sender = getattr(message, "sender", None)
                if sender is None:
                    try:
                        sender = await message.get_sender()
                    except Exception:
                        continue
                if not isinstance(sender, User) or getattr(sender, "bot", False) or getattr(sender, "deleted", False):
                    continue
                seen.add(sender_id)
                result.append(sender)
                if len(result) == 15:
                    break
            if not result:
                return result
            # Admins (this filter includes the creator too) are the ones
            # who'd actually notice a silent mass-tag and can ban the
            # account for it — leave them untagged. Cached (see
            # _ADMIN_IDS_CACHE_TTL_SECONDS) since this runs on every single
            # send to this target otherwise. Only works for channels/
            # supergroups; a legacy basic group raises here, in which case
            # cache an empty set (skip the exclusion, don't keep retrying a
            # doomed call every cycle) rather than losing the tag entirely.
            try:
                from telethon.tl.types import ChannelParticipantsAdmins
                cached = self._admin_ids_cache.get(target)
                if cached is not None and (time.monotonic() - cached[0]) < _ADMIN_IDS_CACHE_TTL_SECONDS:
                    admin_ids = cached[1]
                else:
                    try:
                        admins = await client.get_participants(target, filter=ChannelParticipantsAdmins)
                        admin_ids = {u.id for u in admins}
                    except Exception:
                        admin_ids = set()
                    # Keyed by target chat, unlike _me_cache (bounded by
                    # account count) this can grow across every distinct
                    # chat ever mailed to over the bot's lifetime — a hard
                    # cap, not just the TTL above, so it can't grow
                    # unbounded. Safe to drop entirely: it's a pure cache.
                    if len(self._admin_ids_cache) > 5000:
                        self._admin_ids_cache.clear()
                    self._admin_ids_cache[target] = (time.monotonic(), admin_ids)
                if admin_ids:
                    result = [u for u in result if u.id not in admin_ids]
            except Exception:
                pass
            if not result:
                return result
            # A chat's most active posters would otherwise get a real
            # notification on every single send to that chat, forever — skip
            # anyone already tagged recently instead of hammering the same
            # handful of people.
            try:
                recently_tagged = await self.db.get_recently_hidden_tagged(
                    {u.id for u in result}, _HIDDEN_TAG_COOLDOWN_HOURS
                )
            except Exception:
                recently_tagged = set()
            result = [u for u in result if u.id not in recently_tagged]
            if result:
                try:
                    await self.db.mark_hidden_tagged([u.id for u in result])
                except Exception:
                    pass
            return result
        except Exception:
            return []

    async def _send_msg(
        self,
        client,
        target: str,
        msg,
        pm: Optional[str],
        reply_to=None,
        add_signature: bool = False,
        hidden_tag_user_ids: Optional[list[int]] = None,
        force_text_only: bool = False,
        account_id: Optional[int] = None,
        mailing_id: Optional[int] = None,
        target_snapshot=None,
    ) -> None:
        """Send one mailing message to target (forward / text / photo)."""
        # op_id ties every [DIAG] line for this one send attempt together —
        # microsecond-precision Moscow time is unique enough even when
        # several mailings on the same account interleave in the shared
        # log stream. Pure diagnostics: no secrets (no session/api_hash/
        # message text), no effect on control flow, timing, or retries.
        op_id = now_moscow().strftime('%H%M%S%f')
        diag_on = await self._diag_enabled(account_id) if account_id is not None else config.MAILING_DEBUG

        def _diag(text: str):
            if diag_on:
                logger.info(f"[DIAG op={op_id} account={account_id} mailing={mailing_id} target={target}] {text}")

        _diag(
            f"send prep started (forward={msg.is_forward}, dice={msg.is_dice}, "
            f"has_photos={bool(msg.photo_paths)}, has_video={bool(msg.video_path)}, "
            f"has_document={bool(msg.document_path)}, "
            f"hidden_tag={bool(hidden_tag_user_ids)})"
        )
        if msg.is_forward:
            peer = int(msg.forward_peer) if msg.forward_peer.lstrip('-').isdigit() else msg.forward_peer
            src_key = (account_id, msg.forward_peer)
            if self._fwd_src_bad.get(src_key, 0) > time.monotonic():
                raise ForwardSourceError(f"forward source {msg.forward_peer} unavailable (retry later)")

            def _source_failed(e: Exception) -> ForwardSourceError:
                self._fwd_src_bad[src_key] = time.monotonic() + 1800
                strikes = self._fwd_src_strikes.get(src_key, 0) + 1
                self._fwd_src_strikes[src_key] = strikes
                err = ForwardSourceError(f"forward source {msg.forward_peer} unavailable: {type(e).__name__}: {e}")
                err.final = strikes >= 2
                return err

            try:
                # The session's own cache first: no username resolve when the
                # account already knows the source (it had the chat open).
                entity = await client.get_input_entity(peer)
            except Exception as e:
                if isinstance(e, ValueError) or type(e).__name__ in ("UsernameNotOccupiedError", "UsernameInvalidError"):
                    raise _source_failed(e) from e
                raise
            _diag("API call — forward_messages")
            try:
                await client.forward_messages(
                    target, [msg.forward_msg_id], from_peer=entity
                )
            except Exception as e:
                _diag(f"API call raised {type(e).__name__}: {e}")
                if type(e).__name__ in _FORWARD_SOURCE_ERRORS:
                    raise _source_failed(e) from e
                raise
            self._fwd_src_strikes.pop(src_key, None)
            _diag(f"API call ok, client connected={client.is_connected()}")
            return

        # URL buttons (paid tier): sent as the bot's inline result. A chat that
        # forbids inline bots, or any trouble on the bot's side, gets the
        # same message without buttons below.
        if msg.buttons and not msg.is_dice and not add_signature and len(msg.photo_paths) <= 1:
            if await self._send_with_buttons(client, target, msg, reply_to, force_text_only, _diag):
                return

        dice_emoji = msg.text if msg.is_dice else None
        sig = _free_tier_signature(account_id) if add_signature else ""
        entities = _build_telethon_entities(msg.entities_json) if msg.entities_json else None
        if not dice_emoji and msg.text:
            # One homoglyph swap per send — see _apply_homoglyph_swap. Purely
            # local to this call: reads/mutates nothing shared, so it can't
            # interfere with the mailing's own message-variant rotation above.
            msg_text_for_send = msg.text  # 2.0: preserve the owner's exact text.
        else:
            msg_text_for_send = msg.text
        photos = [
            p for p in msg.photo_paths
            if is_safe_media_path(p) and os.path.exists(p)
        ]
        video = (
            msg.video_path
            if msg.video_path
            and is_safe_media_path(msg.video_path)
            and os.path.exists(msg.video_path)
            else None
        )
        document = (
            msg.document_path
            if msg.document_path
            and is_safe_media_path(msg.document_path)
            and os.path.exists(msg.document_path)
            else None
        )

        raw_text = "" if dice_emoji else (msg_text_for_send or "")
        max_len = 1024 if (photos or video or document) else 4096
        if raw_text or sig:
            if sig and _utf16_length(raw_text) + _utf16_length(sig) > max_len:
                raw_text = _truncate_utf16(
                    raw_text,
                    max_len - _utf16_length(sig),
                )
                if entities:
                    raw_length = _utf16_length(raw_text)
                    entities = [
                        entity
                        for entity in entities
                        if entity.offset + entity.length <= raw_length
                    ]
            text = raw_text + sig
        else:
            text = None
        if hidden_tag_user_ids and (text is not None or photos or video or document or dice_emoji):
            from telethon.tl.types import InputMessageEntityMentionName
            from telethon.utils import get_input_user

            base_text = text or ""
            input_users = []
            for user in hidden_tag_user_ids:
                try:
                    # InputMessageEntityMentionName.user_id needs an InputUser,
                    # not the InputPeerUser get_input_entity() would return —
                    # different wire types despite the same fields, so the
                    # entity is silently malformed on the actual RPC. hidden_tag
                    # entries are already full User objects, so this is a pure
                    # local conversion, no client/RPC needed.
                    input_users.append(get_input_user(user))
                except Exception:
                    continue

            if input_users and not entities and base_text and pm and not dice_emoji:
                base_text, entities = await client._parse_message_text(base_text, pm)

            available = max_len - _utf16_length(base_text)
            count = max(0, min(len(input_users), available))
            if count:
                offset = _utf16_length(base_text)
                hidden_entities = [
                    InputMessageEntityMentionName(
                        offset=offset + index,
                        length=1,
                        user_id=input_user,
                    )
                    for index, input_user in enumerate(input_users[:count])
                ]
                text = base_text + ("\u2063" * len(hidden_entities))
                entities = (entities or []) + hidden_entities
        eff_pm = None if entities else ("html" if dice_emoji else pm)
        if force_text_only:
            method_desc = "send_message (text-only fallback)"
        elif dice_emoji:
            method_desc = "send_file (dice)"
        elif video:
            method_desc = "send_file (video)"
        elif document:
            method_desc = "send_file (document)"
        elif len(photos) > 1:
            method_desc = f"send_file ({len(photos)} photos)"
        elif len(photos) == 1:
            method_desc = "send_file (1 photo)"
        else:
            method_desc = "send_message (text)"
        _diag(f"API call — {method_desc}")
        try:
            if force_text_only:
                # Used as a fallback when the chat rejected this specific media
                # type (e.g. ChatSendPhotosForbiddenError) but plain text is
                # still likely allowed — send just the caption/text instead of
                # skipping the target entirely.
                if not text and not entities:
                    raise ValueError("Mailing message has no text to fall back to")
                await client.send_message(target, text or "", parse_mode=eff_pm,
                                          formatting_entities=entities, reply_to=reply_to,
                                          link_preview=False)
            elif dice_emoji:
                from telethon.tl.types import InputMediaDice

                await client.send_file(
                    target,
                    InputMediaDice(dice_emoji),
                    caption=text,
                    parse_mode=eff_pm,
                    formatting_entities=entities,
                    reply_to=reply_to,
                )
            elif video or photos:
                await self._send_media_cached(
                    client, account_id, target, [video] if video else photos,
                    caption=text, parse_mode=eff_pm,
                    formatting_entities=entities, reply_to=reply_to,
                )
            elif document:
                # force_document: a picture or video sent as a file stays a
                # file (with its name), exactly as the owner added it.
                await self._send_media_cached(
                    client, account_id, target, [document],
                    caption=text, parse_mode=eff_pm,
                    formatting_entities=entities, reply_to=reply_to,
                    force_document=True,
                )
            else:
                if not text and not entities:
                    raise ValueError("Mailing message has no text and no photos")
                # link_preview=False — same as pressing the "x" on the
                # preview card before sending in the real app. Without it,
                # any link in the mailing text grows an unrelated site's
                # title/image/description under the message (see the
                # "GARANT SYNDICATE" screenshot this was reported from),
                # which visually stands out from a normal chat message.
                await client.send_message(target, text or "", parse_mode=eff_pm,
                                          formatting_entities=entities, reply_to=reply_to,
                                          link_preview=False)
        except Exception as e:
            _diag(f"API call raised {type(e).__name__}: {e}")
            raise
        _diag(f"API call ok, client connected={client.is_connected()}")

    async def _start_mailing_task(self, mailing_id: int, initial_delay: float = 0.0):
        lock = self._mailing_locks.setdefault(mailing_id, asyncio.Lock())
        async with lock:
            if mailing_id in self._tasks and not self._tasks[mailing_id].done():
                return
            self._clear_working_targets(mailing_id)
            mailing = await self.db.get_mailing(mailing_id)
            if mailing:
                targets = await self.db.get_mailing_targets(mailing_id)
                virgin = [t for t in targets if t.last_sent_at is None]
                if virgin:
                    stagger_delay = min(30, mailing.interval_seconds // max(len(targets), 1))
                    now = now_moscow()
                    self._stagger_until[mailing_id] = {
                        t.id: now + timedelta(seconds=i * stagger_delay)
                        for i, t in enumerate(virgin)
                    }
            task = asyncio.create_task(self._mailing_loop(mailing_id, initial_delay=initial_delay))
            self._tasks[mailing_id] = task

    async def _mailing_loop(self, mailing_id: int, initial_delay: float = 0.0):
        if initial_delay:
            await asyncio.sleep(initial_delay)
        logger.info(f"Mailing {mailing_id}: loop started")
        try:
            while self._running:
                try:
                    mailing = await self.db.get_mailing(mailing_id)
                    if not mailing or not mailing.is_active:
                        self._clear_working_targets(mailing_id)
                        logger.info(
                            f"Mailing {mailing_id}: loop exiting — "
                            f"{'not found in DB' if not mailing else 'is_active set to False'}"
                        )
                        break

                    if not self._is_active_hours(mailing.active_hours_json):
                        self._clear_working_targets(mailing_id)
                        await asyncio.sleep(60)
                        continue

                    flood_until = await self.db.get_flood_wait_until(mailing.account_id)
                    if flood_until:
                        now_check = now_moscow()
                        if now_check < flood_until:
                            self._clear_working_targets(mailing_id)
                            # Don't even attempt to connect/send until the
                            # known FloodWait expires — check back
                            # periodically rather than sleeping the whole
                            # remaining duration in one shot, so the loop
                            # stays responsive to stop_mailing()/cancellation.
                            await asyncio.sleep(min((flood_until - now_check).total_seconds(), 60))
                            continue
                        await self.db.set_flood_wait_until(mailing.account_id, None)

                    pause_left = self._timeout_pause_until.get(mailing.account_id, 0) - time.monotonic()
                    if pause_left > 0:
                        self._clear_working_targets(mailing_id)
                        await asyncio.sleep(min(pause_left, 60))
                        continue

                    tos_left = self._tos_pause_until.get(mailing.account_id, 0) - time.monotonic()
                    if tos_left > 0:
                        self._clear_working_targets(mailing_id)
                        await asyncio.sleep(min(tos_left, 60))
                        continue
                    if mailing.account_id in self._tos_blocked:
                        # One probe cycle now; the next in _TOS_BLOCK_PAUSE
                        # unless a send gets through (_record_sent clears it).
                        self._tos_pause_until[mailing.account_id] = time.monotonic() + _TOS_BLOCK_PAUSE

                    messages = await self.db.get_mailing_messages(mailing_id)
                    targets = await self.db.get_mailing_targets(mailing_id)
                    self._apply_pending_last_sent(targets)

                    if not messages or not targets:
                        self._clear_working_targets(mailing_id)
                        logger.debug(
                            f"Mailing {mailing_id}: no messages ({len(messages)}) or "
                            f"targets ({len(targets)}), waiting 60s"
                        )
                        await asyncio.sleep(60)
                        continue

                    logger.debug(
                        f"Mailing {mailing_id} [{mailing.name}]: cycle — "
                        f"{len(messages)} message(s), {len(targets)} target(s), "
                        f"account_id={mailing.account_id}"
                    )

                    # Resolve main client
                    client = await self.userbot_manager.get_client(mailing.account_id)
                    if not client:
                        self._clear_working_targets(mailing_id)
                        await asyncio.sleep(60)
                        continue

                    if not client.is_connected():
                        try:
                            # Without a timeout here, a proxy that accepts the
                            # TCP connection but never completes the MTProto
                            # handshake leaves this awaiting forever — no
                            # exception, no log line, nothing — silently
                            # freezing the whole mailing until someone
                            # notices and restarts the process by hand.
                            await asyncio.wait_for(client.connect(), timeout=45)
                        except _BAN_ERRORS as e:
                            self._clear_working_targets(mailing_id)
                            await self._handle_mailing_ban(mailing_id, mailing.account_id, e)
                            return
                        except Exception as e:
                            self._clear_working_targets(mailing_id)
                            logger.error(f"Reconnect failed for mailing {mailing_id}: {e}")
                            await asyncio.sleep(60)
                            continue

                    now = now_moscow()
                    sent_any = False
                    cycle_sent = 0
                    cycle_errors = 0
                    wake_in: list = []  # seconds until a skipped chat is due — the loop wakes for the nearest
                    trace_on = await self._diag_enabled(mailing.account_id)

                    def _trace(t, why):
                        if trace_on:
                            logger.info(f"[TRACE mailing={mailing_id} target={t.chat_identifier}] {why}")
                    if mailing.account_id not in self._me_cache:
                        self._me_cache[mailing.account_id] = await asyncio.wait_for(client.get_me(), timeout=30)
                    me = self._me_cache[mailing.account_id]

                    # Free tier: check once per cycle
                    mailing_user = await self.db.get_user_by_id(mailing.user_id)
                    add_sig = Database.is_free_ad_active(mailing_user) if mailing_user else False
                    # A paid subscription that just ran out used to keep sending
                    # (without the free tier's ad) until the hourly checker's
                    # sweep — stop right away instead. The checker still sends
                    # the "subscription expired" notice.
                    if (
                        mailing_user and not mailing_user.is_admin and not add_sig
                        and (not mailing_user.subscription_end or mailing_user.subscription_end <= now_moscow())
                    ):
                        self._clear_working_targets(mailing_id)
                        await self.db.mark_paused_by_expiry([mailing_id])
                        await self.db.update_mailing_status(mailing_id, False)
                        logger.info(f"Mailing {mailing_id}: owner's subscription has ended — stopping")
                        return
                    if add_sig and not any(not message.is_forward for message in messages):
                        self._clear_working_targets(mailing_id)
                        logger.debug(
                            f"Mailing {mailing_id}: all messages are forwards, "
                            "waiting for a sendable free-tier message"
                        )
                        await asyncio.sleep(60)
                        continue

                    for target_idx, target_obj in enumerate(targets):
                        if _is_malformed_target(target_obj.chat_identifier):
                            await self._remove_target(mailing_id, target_obj, "is not a chat (malformed target)")
                            continue
                        if self._ban_until.get(target_obj.id, 0) > time.monotonic():
                            _trace(target_obj, f"ban pause {self._ban_until[target_obj.id] - time.monotonic():.0f}s left")
                            continue  # banned/closed chat — growing pause, see _BAN_BACKOFF_ERRORS
                        skip_left = self._chat_ban_skip.get(target_obj.id, 0)
                        if skip_left > 0:
                            self._chat_ban_skip[target_obj.id] = skip_left - 1
                            _trace(target_obj, f"restriction skip, {skip_left} cycles left")
                            continue
                        if self._unseen_until.get(target_obj.id, 0) > time.monotonic():
                            _trace(target_obj, "our last message is still the latest — waiting a round")
                            continue

                        # Interval check per target — jitter ±3s around the configured
                        # value so sends don't land on a perfectly regular cadence
                        # (a dead-even interval is itself a spam-pattern signal).
                        base_interval = target_obj.interval_seconds or mailing.interval_seconds
                        target_interval = max(1, base_interval + random.uniform(-3, 3))
                        # Spreading accounts over a shared chat only picks WHEN a
                        # mailing starts writing there (first send, or after a long
                        # pause); from then on it's the mailing's own interval. Applied
                        # on every send it stretched a 300s mailing to 8-12 min in a
                        # chat 49 mailings write to.
                        spread_ok = True
                        if target_obj.last_sent_at is not None:
                            elapsed = (now - target_obj.last_sent_at).total_seconds()
                            spread_ok = elapsed > 2 * target_interval
                            if elapsed < target_interval:
                                _trace(target_obj, f"interval {elapsed:.0f}/{target_interval:.0f}s")
                                wake_in.append(target_interval - elapsed)
                                if target_obj.last_account_id in (None, mailing.account_id):
                                    self._mark_target_working(mailing_id, target_obj.id)
                                logger.debug(f"Mailing {mailing_id}: {target_obj.chat_identifier} — interval not elapsed ({elapsed:.0f}/{target_interval}s), skip")
                                continue

                        # Stagger: new targets wait their offset before first send
                        if target_obj.last_sent_at is None:
                            stagger_ready = self._stagger_until.get(mailing_id, {}).get(target_obj.id, datetime.min.replace(tzinfo=now.tzinfo))
                            if now < stagger_ready:
                                continue
                        # Checked once the chat is due (not on every pass of the loop).
                        if (_is_cached_private_chat(client, target_obj.chat_identifier)
                                and await _is_private_chat_now(client, target_obj.chat_identifier)):
                            await self._remove_target(mailing_id, target_obj, "is a person or a bot, not a chat")
                            continue
                        # A chat that takes text only can't get a media-only message:
                        # with no message that has text, skip it — before the account's
                        # gap, and without the "no text to fall back to" error it raised
                        # on every pass.
                        if target_obj.media_disabled and not any(_has_text(m) for m in messages):
                            _trace(target_obj, "text-only chat and no message with text — skipped")
                            continue
                        current_account_id = mailing.account_id
                        chat_key = (target_obj.chat_identifier or "").lower()
                        await self._refresh_chat_senders()
                        senders = self._chat_senders.get(chat_key, 1)
                        if senders > 1 and spread_ok:
                            gap = min(_CHAT_SPREAD_MAX_GAP, max(_CHAT_SPREAD_MIN_GAP, base_interval / senders))
                            since = time.monotonic() - self._chat_last_send.get(chat_key, 0.0)
                            if since < gap:
                                _trace(target_obj, f"chat spread {since:.0f}/{gap:.0f}s ({senders} senders)")
                                wake_in.append(gap - since)
                                if target_obj.last_account_id in (None, mailing.account_id):
                                    self._mark_target_working(mailing_id, target_obj.id)
                                continue
                        owner_key = (mailing.user_id, chat_key)
                        own_senders = self._owner_chat_senders.get(owner_key, 1)
                        if own_senders > 1 and spread_ok:
                            own_gap = max(_CHAT_SPREAD_MIN_GAP, base_interval / own_senders)
                            own_since = time.monotonic() - self._owner_chat_last_send.get(owner_key, 0.0)
                            if own_since < own_gap:
                                wake_in.append(own_gap - own_since)
                                if target_obj.last_account_id in (None, mailing.account_id):
                                    self._mark_target_working(mailing_id, target_obj.id)
                                continue

                        # Cross-mailing account-wide throttle — see
                        # self._account_last_send above. Uses a freshly-read
                        # timestamp, NOT the cycle's `now` (which is captured
                        # once at the top of the cycle and stays frozen while
                        # this loop works through potentially many targets) —
                        # comparing against a frozen `now` would make elapsed
                        # time read as ~0 for every target after the first
                        # send in the same cycle, throttling a mailing against
                        # itself regardless of real wall-clock time. Checked
                        # and set with no `await` in between, so concurrent
                        # mailing tasks on the same account can't both pass at
                        # once.
                        # The account just sent elsewhere (another mailing on it): wait out
                        # the gap and send this chat, instead of skipping it — a skip fell
                        # straight through every remaining chat of the pass in the same
                        # instant (all inside the same 2-3s), so with two mailings on one
                        # account long passes kept ending early and the far end of a
                        # 140-chat list never got a message.
                        gate_waits = 0
                        while True:
                            send_now = now_moscow()
                            last_account_send = self._account_last_send.get(current_account_id)
                            if last_account_send is None:
                                break
                            account_elapsed = (send_now - last_account_send).total_seconds()
                            # Randomized within [2, MIN_SAFE_SECONDS_PER_TARGET] instead of a
                            # flat floor — a perfectly identical gap between every send on an
                            # account is itself an unnaturally regular pattern, same reasoning
                            # as the per-target interval jitter above.
                            account_gate = random.uniform(2.0, max(2.0, config.MIN_SAFE_SECONDS_PER_TARGET))
                            if account_elapsed >= account_gate:
                                break
                            gate_waits += 1
                            _trace(target_obj, f"account gate {account_elapsed:.1f}/{account_gate:.1f}s — waiting")
                            if gate_waits > 10:
                                break  # the other mailings keep the account busy; send anyway, spaced by the waits
                            await asyncio.sleep(account_gate - account_elapsed + random.uniform(0.2, 1.0))

                        self._account_last_send[current_account_id] = send_now
                        _trace(target_obj, "sending")
                        self._chat_last_send[chat_key] = time.monotonic()
                        self._owner_chat_last_send[owner_key] = time.monotonic()
                        target_client = client
                        target_me = me
                        target = target_obj.chat_identifier
                        if target.lstrip('-').isdigit():
                            target = int(target)
                        elif "t.me/+" in target:
                            # Keep invite links intact so _try_join_and_send()
                            # can use ImportChatInviteRequest.
                            pass
                        elif not target.startswith('@'):
                            target = f"@{target}"

                        hidden_tag_user_ids = []
                        if mailing.hidden_tag_enabled and not add_sig:
                            hidden_tag_user_ids = await self._get_recent_user_ids(
                                target_client, target, target_me.id
                            )

                        sendable = [m for m in messages if not m.is_forward] if add_sig else messages
                        # A forward of a user's message was relayed into one
                        # specific account's chat with the bot; any other
                        # account can't forward that copy.
                        sendable = [
                            m for m in sendable
                            if not (m.forward_relay_account and m.forward_relay_account != current_account_id)
                        ]
                        if not sendable:
                            logger.debug(f"Mailing {mailing_id}: all messages are forwards, skipping target for free-tier user")
                            continue
                        # Texts go in order per chat: every chat gets 1, 2, 3, 1, ...
                        # cycle by cycle (one shared counter per mailing made a
                        # chat stuck on the same text whenever the chat count
                        # divided by the text count). Stored with the send, so
                        # a restart doesn't start everyone over at text 1; a
                        # failed send keeps the same text for the next try.
                        # A forward whose source is unreachable right now is left out
                        # (a failed send keeps the chat on the same text, so it would
                        # sit on the broken one forever and never get the others).
                        now_m = time.monotonic()
                        reachable = [m for m in sendable if not (
                            m.is_forward and self._fwd_src_bad.get((current_account_id, m.forward_peer), 0) > now_m)]
                        if reachable:
                            sendable = reachable
                        if target_obj.media_disabled:
                            with_text = [m for m in sendable if _has_text(m)]
                            if not with_text:
                                continue
                            sendable = with_text
                        idx = (target_obj.msg_index or 0) % len(sendable)
                        msg = sendable[idx]
                        self._target_next_msg[target_obj.id] = idx + 1
                        pm = msg.parse_mode or 'html'
                        if pm == 'plain':
                            pm = None

                        topic_ids = target_obj.topic_ids
                        if topic_ids:
                            # Round-robin through the configured topic list
                            # (e.g. "3-6, 8, 10-20") — one topic per send
                            # cycle, same idea as _message_rotation above.
                            t_idx = self._topic_rotation.get(target_obj.id, 0) % len(topic_ids)
                            reply_to_id = topic_ids[t_idx]
                            # Hard cap, not just relying on targets being deleted —
                            # keyed by target_id across the bot's whole lifetime,
                            # so it can't grow unbounded. Safe to drop entirely:
                            # it just restarts each target's rotation at index 0.
                            if len(self._topic_rotation) > 5000:
                                self._topic_rotation.clear()
                            self._topic_rotation[target_obj.id] = t_idx + 1
                        else:
                            reply_to_id = target_obj.thread_id  # use topic thread if set
                        # "Don't repeat yourself": our previous message here is still
                        # the chat's (topic's) latest — nobody wrote after it — so this
                        # chat waits a round instead of getting a second one in a row.
                        if (mailing.skip_unseen and target_obj.last_sent_at is not None
                                and await self._last_message_is_ours(target_client, target, reply_to_id)):
                            if len(self._unseen_until) > 20000:
                                self._unseen_until.clear()
                            self._unseen_until[target_obj.id] = time.monotonic() + base_interval
                            _trace(target_obj, "our last message is still the latest — skipped")
                            continue
                        # reply_mode picks a message from the chat's general history
                        # (get_messages below isn't scoped to a topic) — applying it
                        # on a forum target would overwrite the intended thread_id
                        # with a reply into a random, possibly wrong topic.
                        if mailing.reply_mode and target_obj.thread_id is None and not topic_ids:
                            try:
                                if mailing.reply_mode == 'last':
                                    offset = 0
                                elif mailing.reply_mode == 'fixed':
                                    offset = mailing.reply_offset - 1
                                else:  # random
                                    offset = random.randint(
                                        mailing.reply_random_min - 1,
                                        mailing.reply_random_max - 1
                                    )
                                msgs_list = await target_client.get_messages(target, limit=offset + 1)
                                if msgs_list and len(msgs_list) > offset:
                                    reply_to_id = msgs_list[offset].id
                            except Exception as e:
                                logger.warning(f"Mailing {mailing_id}: failed to get reply target: {e}")

                        diag_on = await self._diag_enabled(current_account_id)
                        if diag_on:
                            logger.info(
                                f"Mailing {mailing_id}: [{target_idx + 1}/{len(targets)}] "
                                f"→ {target} (account {current_account_id})"
                            )
                        else:
                            logger.debug(
                                f"Mailing {mailing_id}: [{target_idx + 1}/{len(targets)}] "
                                f"→ {target} (account {current_account_id})"
                            )
                        if diag_on:
                            # Pre-send snapshot — local state only, no extra
                            # Telegram-facing call.
                            pre_flood = await self.db.get_flood_wait_until(current_account_id)
                            logger.info(
                                f"[DIAG account={current_account_id} mailing={mailing_id} "
                                f"target_id={target_obj.id} target={target}] pre-send state — "
                                f"connected={target_client.is_connected()}, flood_wait_until={pre_flood}, "
                                f"target_never_sent={target_obj.last_sent_at is None}, "
                                f"target_last_account={target_obj.last_account_id}"
                            )
                        self._spam_ctx[mailing_id] = (current_account_id, target_client, mailing_user)
                        try:
                            # Bounded — client.is_connected() can stay True
                            # over a proxy that's silently stopped relaying
                            # data (no clean disconnect, no exception), which
                            # left this awaiting forever with zero log output
                            # and froze the whole mailing until someone
                            # noticed and restarted the process by hand.
                            if target_obj.last_sent_at is None and target_obj.id not in self._no_hello:
                                # First time in this chat: just say hi.
                                await asyncio.wait_for(target_client.send_message(
                                    target, random.choice(_FIRST_HELLO), reply_to=reply_to_id,
                                ), timeout=60)
                                self._target_next_msg.pop(target_obj.id, None)  # keep the message rotation where it is
                            else:
                                await asyncio.wait_for(self._send_msg(
                                    target_client, target, msg, pm,
                                    reply_to=reply_to_id,
                                    add_signature=add_sig,
                                    hidden_tag_user_ids=hidden_tag_user_ids,
                                    force_text_only=target_obj.media_disabled,
                                    target_snapshot=target_obj,
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                ), timeout=60)
                            if diag_on:
                                logger.info(f"Mailing {mailing_id}: ✓ sent to {target} via account {current_account_id}")
                                # Post-op status check — local state only, no
                                # extra Telegram-facing call: is the account
                                # still connected and not (yet) flagged by
                                # anything that ran concurrently.
                                post_flood = await self.db.get_flood_wait_until(current_account_id)
                                logger.info(
                                    f"[DIAG] account={current_account_id}: post-send check — "
                                    f"connected={target_client.is_connected()}, flood_wait_until={post_flood}"
                                )
                            else:
                                logger.debug(f"Mailing {mailing_id}: ✓ sent to {target} via account {current_account_id}")
                            # The message was genuinely delivered at this point —
                            # buffered for the next batched flush (see
                            # _pending_last_sent) rather than written immediately,
                            # so this can't fail the way a DB call could.
                            self._record_sent(target_obj.id, current_account_id, mailing_id)
                            self._schedule_gate_check(target_client, target, current_account_id)
                            self._send_timeouts.pop(current_account_id, None)
                            self._timeout_reconnects.pop(current_account_id, None)
                            self._note_send_outcome(current_account_id, True)
                            self._mark_target_working(mailing_id, target_obj.id)
                            sent_any = True
                            cycle_sent += 1
                        except _BAN_ERRORS as e:
                            try:
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type=type(e).__name__,
                                    error_text=str(e)[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                )
                            except Exception:
                                pass
                            await self._handle_mailing_ban(mailing_id, current_account_id, e)
                            return
                        except _CHAT_BAN_ERRORS as e:
                            self._mark_target_not_working(mailing_id, target_obj.id)
                            cycle_errors += 1
                            try:
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type=type(e).__name__,
                                    error_text=str(e)[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                            except Exception:
                                pass
                            await self._apply_target_restriction(mailing_id, target_obj, e)
                        except _NOT_MEMBER_ERRORS as e:
                            try:
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type=type(e).__name__,
                                    error_text=str(e)[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                            except Exception:
                                pass
                            if (
                                isinstance(e, ChatWriteForbiddenError)
                                and (target_obj.id, current_account_id) in self._write_forbidden_after_join
                            ):
                                # Already a member (joined earlier) — rejoining won't help.
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                cycle_errors += 1
                                await self._apply_target_restriction(mailing_id, target_obj, e)
                            else:
                                logger.info(f"Mailing {mailing_id}: not participant/forbidden in '{target}', attempting auto-join")
                                joined = await self._try_join_and_send(
                                    target_client, target, target_obj, msg, pm,
                                    mailing_id, current_account_id,
                                    reply_to=reply_to_id,
                                    add_signature=add_sig,
                                    hidden_tag_user_ids=hidden_tag_user_ids,
                                )
                                if joined:
                                    self._mark_target_working(mailing_id, target_obj.id)
                                    sent_any = True
                                else:
                                    self._mark_target_not_working(mailing_id, target_obj.id)
                                    if await self.db.get_flood_wait_until(current_account_id):
                                        # _try_join_and_send hit a FloodWait internally —
                                        # same reasoning as the main FloodWaitError
                                        # handler: stop hammering remaining targets
                                        # this cycle, the top-of-loop gate takes over.
                                        break
                        except SlowModeWaitError as e:
                            self._mark_target_not_working(mailing_id, target_obj.id)
                            try:
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type=type(e).__name__,
                                    error_text=f"{e.seconds}s",
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                            except Exception:
                                pass
                            # Slow mode is a per-CHAT restriction — it says nothing about
                            # the other targets in this mailing, so it must not block them.
                            # Used to `sleep(min(e.seconds, 60))` here, stalling the whole
                            # cycle (all other targets) for up to a full minute on every
                            # single slow-mode hit; several such chats in one mailing could
                            # add up to minutes of dead time. Skip just this target for a
                            # while instead, same as any other per-chat restriction.
                            logger.warning(f"Mailing {mailing_id}: slow mode in '{target}', wait {e.seconds}s — skipping this target for now")
                            self._chat_ban_skip[target_obj.id] = _CHAT_BAN_SKIP_CYCLES
                        except FloodWaitError as e:
                            self._clear_working_targets(mailing_id)
                            # _notify_flood_wait sets accounts.flood_wait_until
                            # and notifies once — the gate at the top of this
                            # loop is what actually waits it out on subsequent
                            # cycles. Do NOT also sleep in place here: that used
                            # to sleep out the (capped) wait fully within this
                            # same cycle, so by the time the next cycle's gate
                            # check ran, flood_wait_until already looked expired
                            # and got cleared — right before Telegram's next
                            # FloodWaitError (with an updated, still-nonzero
                            # remaining count) was treated as a brand new episode
                            # and re-notified, causing duplicate alerts.
                            await self._notify_flood_wait(mailing_user, current_account_id, e.seconds, rest=_flood_rest())
                            cycle_errors += 1
                            logger.warning(
                                f"Mailing {mailing_id}: FloodWait {e.seconds}s on account "
                                f"{current_account_id} for {target} — deferring remaining "
                                f"targets until it clears"
                            )
                            try:
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type="FloodWait",
                                    error_text=f"{e.seconds}s",
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                            except Exception:
                                pass
                            # FloodWait is account-wide, not per-target — every
                            # remaining target in this cycle would immediately
                            # hit the same wait again. Defer them; the top-of-loop
                            # gate handles the actual waiting from here.
                            break
                        except (
                            ChatSendMediaForbiddenError, RightForbiddenError,
                            ChatAdminRequiredError, ChannelPrivateError, ChatRestrictedError, InviteHashExpiredError,
                        ) as e:
                            if isinstance(e, ChatSendMediaForbiddenError):
                                # Media specifically rejected — not a "no
                                # posting rights at all" restriction (the
                                # other three errors below are). Rather than
                                # an immediate second API call in the same
                                # cycle, persist text_only for this chat: the
                                # current attempt just ends here, and from
                                # the next cycle onward the main send path
                                # checks target_obj.media_disabled and skips
                                # straight to text, no photo/video upload
                                # attempt at all.
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                try:
                                    await self.db.update_target_media_disabled(target_obj.id, True)
                                except Exception:
                                    logger.warning(f"Mailing {mailing_id}: failed to persist media_disabled for target {target_obj.id}")
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type=type(e).__name__,
                                        error_text=str(e)[:300],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                logger.info(f"Mailing {mailing_id}: media forbidden in '{target}' — switching to text-only from next cycle")
                            else:
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type=type(e).__name__,
                                        error_text=str(e)[:300],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                # Without this, a target that will never accept
                                # this account's messages (e.g. "join the
                                # discussion group first" with nothing here
                                # that joins it) had no skip/removal tracking
                                # at all and got retried every single cycle
                                # forever — the exact "mailing looks stuck,
                                # never makes progress" pattern reported live.
                                if type(e).__name__ in _DEAD_INVITE_ERRORS:
                                    await self._remove_target(mailing_id, target_obj, "invite link expired")
                                else:
                                    await self._apply_target_restriction(
                                        mailing_id, target_obj, e,
                                    )
                        except Exception as e:
                            err_str = str(e)
                            if type(e).__name__ in _DEAD_INVITE_ERRORS:
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                await self._remove_target(mailing_id, target_obj, "invite link is invalid")
                                continue
                            if isinstance(e, ForwardSourceError):
                                # Every chat would fail the same way — the chats are
                                # fine, the forwarded post's source isn't reachable.
                                cycle_errors += 1
                                if "(retry later)" in err_str:
                                    break  # already logged; the source is retried every 30 min
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type="ForwardSourceUnavailable",
                                        error_text=err_str[:300],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                others_ok = any(
                                    not (m.is_forward and self._fwd_src_strikes.get((current_account_id, m.forward_peer), 0))
                                    for m in messages)
                                if getattr(e, "final", False) and not others_ok:
                                    logger.warning(f"Mailing {mailing_id}: {err_str[:160]} — still after 30 min, mailing stopped")
                                    await self._stop_for_forward_source(mailing, current_account_id)
                                else:
                                    logger.warning(f"Mailing {mailing_id}: {err_str[:160]} — ending this pass, chats kept, "
                                                   f"retry in 30 min")
                                break
                            if isinstance(e, ConnectionError):
                                # The account's own connection dropped ("Cannot send
                                # requests while disconnected") — nothing to do with
                                # this chat. Parking the target here parked every
                                # chat of the mailing for 25 cycles after one blip.
                                # End the pass; next cycle's get_client() reconnects.
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                logger.warning(
                                    f"Mailing {mailing_id}: account {current_account_id} disconnected "
                                    f"({err_str[:80]}) — ending this pass, reconnecting next cycle"
                                )
                                break
                            elif isinstance(e, asyncio.TimeoutError):
                                # One chat can hang on its own (seen: every send to a
                                # single chat timing out) — park it like any other
                                # per-chat failure. Several different chats in a row
                                # with no success between means the account's link is
                                # stuck instead: rebuild the client.
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                cycle_errors += 1
                                await self._apply_target_restriction(mailing_id, target_obj, e)
                                n = self._send_timeouts.get(current_account_id, 0) + 1
                                self._send_timeouts[current_account_id] = n
                                if n >= 3:
                                    self._send_timeouts.pop(current_account_id, None)
                                    r = self._timeout_reconnects.get(current_account_id, 0) + 1
                                    self._timeout_reconnects[current_account_id] = r
                                    # A send going through now and then reset the counter above,
                                    # so a half-dead link (slow own proxy) was rebuilt ~240 times
                                    # a day. 4 rebuilds within an hour count the same as 3 in a row.
                                    mono = time.monotonic()
                                    recent = [t for t in self._reconnect_times.get(current_account_id, []) if mono - t < 3600]
                                    recent.append(mono)
                                    self._reconnect_times[current_account_id] = recent
                                    if r >= 3 or len(recent) >= 4:
                                        self._reconnect_times.pop(current_account_id, None)
                                        self._timeout_reconnects.pop(current_account_id, None)
                                        self._timeout_pause_until[current_account_id] = time.monotonic() + 1800
                                        logger.warning(
                                            f"Mailing {mailing_id}: account {current_account_id} still timing out "
                                            f"after {r} reconnects — pausing its sends for 30 min"
                                        )
                                    logger.warning(
                                        f"Mailing {mailing_id}: account {current_account_id} sends keep timing out "
                                        f"— reconnecting its client"
                                    )
                                    try:
                                        await asyncio.wait_for(
                                            self.userbot_manager.stop_client(current_account_id), timeout=30,
                                        )
                                    except Exception as se:
                                        logger.warning(f"Mailing {mailing_id}: stop_client({current_account_id}) failed: {se!r}")
                                    break
                            elif isinstance(e, ValueError) and isinstance(target, str) and "t.me/+" in target and "not part of" in err_str:
                                # Telethon raises this locally (no RPC round-trip) when the
                                # invite link hasn't been joined yet — _NOT_MEMBER_ERRORS
                                # never fires here since it's not a Telegram RPCError.
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type="InviteNotJoined",
                                    error_text=err_str[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                                logger.info(f"Mailing {mailing_id}: invite link '{target}' not joined yet, attempting auto-join")
                                joined = await self._try_join_and_send(
                                    target_client, target, target_obj, msg, pm,
                                    mailing_id, current_account_id,
                                    reply_to=reply_to_id,
                                    add_signature=add_sig,
                                    hidden_tag_user_ids=hidden_tag_user_ids,
                                )
                                if joined:
                                    self._mark_target_working(mailing_id, target_obj.id)
                                    sent_any = True
                                else:
                                    self._mark_target_not_working(mailing_id, target_obj.id)
                                    if await self.db.get_flood_wait_until(current_account_id):
                                        break
                            elif type(e).__name__ == "ChatSendPlainForbiddenError":
                                # The chat takes media only — the opposite of the
                                # media restrictions below. Switching it to
                                # text-only made every cycle fail again (~2400
                                # times a day): media goes back on, and the chat
                                # gets the growing pause (a text-only message can
                                # never go there).
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                self._no_hello.add(target_obj.id)
                                if target_obj.media_disabled:
                                    try:
                                        await self.db.update_target_media_disabled(target_obj.id, False)
                                    except Exception:
                                        logger.warning(f"Mailing {mailing_id}: failed to reset media_disabled for target {target_obj.id}")
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id, error_type=type(e).__name__, error_text=err_str[:300],
                                        account_id=current_account_id, mailing_id=mailing_id, chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                await self._apply_target_restriction(mailing_id, target_obj, e)
                            elif type(e).__name__.startswith("ChatSend") and type(e).__name__.endswith("ForbiddenError"):
                                # Telegram's per-media-type send restrictions
                                # (photos/videos/docs/voices/stickers/gifs/
                                # polls/...). Telethon doesn't hardcode every
                                # one of these as an importable class (some,
                                # like ChatSendPhotosForbiddenError, only
                                # exist in newer Telethon releases than may be
                                # installed here) — matching by class name
                                # instead of isinstance() catches all of them
                                # regardless of the exact installed version,
                                # same treatment as ChatSendMediaForbiddenError
                                # above: persist text_only instead of an
                                # immediate same-cycle retry — see that
                                # branch's comment for the reasoning.
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                try:
                                    await self.db.update_target_media_disabled(target_obj.id, True)
                                except Exception:
                                    logger.warning(f"Mailing {mailing_id}: failed to persist media_disabled for target {target_obj.id}")
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type=type(e).__name__,
                                    error_text=err_str[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                                logger.info(f"Mailing {mailing_id}: {type(e).__name__} in '{target}' — switching to text-only from next cycle")
                            elif isinstance(e, ValueError) and "Could not find the input entity" in err_str:
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                cycle_errors += 1
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type="EntityUnresolvable",
                                    error_text=err_str[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                                logger.warning(
                                    f"Mailing {mailing_id}: account never saw chat '{target}' — "
                                    f"can't resolve chat_id without access_hash, account must join/see it first"
                                )
                                await self._apply_target_restriction(mailing_id, target_obj, e)
                            elif _is_dead_username_error(e):
                                # The chat's username was freed/renamed/deleted — the target
                                # can never be reached again, so remove it instead of
                                # re-resolving it every backoff window.
                                cycle_errors += 1
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type="UsernameNotOccupied",
                                    error_text=err_str[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                                await self._remove_target(mailing_id, target_obj, "username no longer exists")
                            elif "PLAIN_FORBIDDEN" in err_str or "SEND_PLAIN" in err_str:
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                cycle_errors += 1
                                await self.db.add_error_log(
                                    user_id=mailing.user_id,
                                    error_type="PlainForbidden",
                                    error_text=err_str[:300],
                                    account_id=current_account_id,
                                    mailing_id=mailing_id,
                                    chat_identifier=target,
                                )
                                logger.warning(f"Mailing {mailing_id}: '{target}' — чат разрешает только медиа (PLAIN_FORBIDDEN), skipping")
                                await self._apply_target_restriction(mailing_id, target_obj, e)
                            elif "PEER_FLOOD" in err_str or isinstance(e, PeerFloodError):
                                self._clear_working_targets(mailing_id)
                                cycle_errors += 1
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type="PeerFlood",
                                        error_text="PEER_FLOOD",
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                # PEER_FLOOD is Telegram's temporary spam
                                # restriction — unlike FloodWaitError it comes
                                # with no explicit wait duration, but retrying
                                # every ~60s (the old behavior here) just kept
                                # hammering an already-restricted account,
                                # which risks extending the restriction and
                                # never told the owner it happened. Route it
                                # through the same flood_wait_until mechanism
                                # as a real FloodWait, with a conservative
                                # fixed cooldown.
                                self._clear_working_targets(mailing_id)
                                await self._notify_flood_wait(
                                    mailing_user, current_account_id, config.PEER_FLOOD_COOLDOWN_SECONDS
                                )
                                self._schedule_spambot_check(current_account_id, target_client, mailing_user)
                                logger.warning(
                                    f"Mailing {mailing_id}: PEER_FLOOD (temporary spam restriction) on "
                                    f"account {current_account_id} — deferring targets for "
                                    f"{config.PEER_FLOOD_COOLDOWN_SECONDS}s"
                                )
                                # Account-wide, not per-target — stop hammering the
                                # rest of this cycle's targets; the top-of-loop
                                # flood_wait_until gate handles the actual wait.
                                break
                            elif _FROZEN_ACCOUNT_MARKER in err_str.lower():
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type="FrozenAccount",
                                        error_text=err_str[:300],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                await self._handle_mailing_ban(mailing_id, current_account_id, e)
                                return
                            elif isinstance(e, _INVALID_TOPIC_ERRORS) and topic_ids:
                                # One bad/deleted topic id in a multi-topic
                                # round-robin list — not a chat-wide problem,
                                # so this must NOT go through
                                # _apply_target_restriction (that would also
                                # back off every other, valid topic in the
                                # list for _CHAT_BAN_SKIP_CYCLES). Just log
                                # which topic failed and move on; rotation
                                # already advanced past it for next time.
                                cycle_errors += 1
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type=type(e).__name__,
                                        error_text=f"topic #{reply_to_id}: {str(e)[:280]}",
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                logger.warning(
                                    f"Mailing {mailing_id}: topic #{reply_to_id} in '{target}' "
                                    f"doesn't exist ({type(e).__name__}) — skipping this topic, "
                                    f"chat and other topics unaffected"
                                )
                            elif "TOPIC_CLOSED" in err_str:
                                cycle_errors += 1
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type="TopicClosed",
                                        error_text=(f"topic #{reply_to_id}: " if reply_to_id else "") + err_str[:280],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                if topic_ids:
                                    # One closed topic in a round-robin list —
                                    # the other topics stay usable, so don't
                                    # park the whole chat; rotation moves on.
                                    logger.warning(
                                        f"Mailing {mailing_id}: topic #{reply_to_id} in '{target}' is closed "
                                        f"— skipping this topic, other topics unaffected"
                                    )
                                else:
                                    self._mark_target_not_working(mailing_id, target_obj.id)
                                    # TOPIC_CLOSED proves the chat has topics (its General
                                    # is closed) — flag it so the targets list offers the
                                    # topic button, e.g. for a private chat the account
                                    # joined long ago and so never went through a join.
                                    if not target_obj.is_forum and not target_obj.thread_id:
                                        try:
                                            await self.db.update_target_is_forum(target_obj.id, True)
                                        except Exception:
                                            pass
                                    await self._apply_target_restriction(
                                        mailing_id, target_obj, e, cycles=_TOPIC_CLOSED_SKIP_CYCLES,
                                    )
                            elif "ALLOW_PAYMENT_REQUIRED" in err_str:
                                cycle_errors += 1
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type="PaidMessagesRequired",
                                        error_text=err_str[:300],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                await self._remove_paid_messages_target(mailing_id, target_obj)
                            else:
                                self._mark_target_not_working(mailing_id, target_obj.id)
                                cycle_errors += 1
                                try:
                                    await self.db.add_error_log(
                                        user_id=mailing.user_id,
                                        error_type=type(e).__name__,
                                        error_text=str(e)[:300],
                                        account_id=current_account_id,
                                        mailing_id=mailing_id,
                                        chat_identifier=target,
                                    )
                                except Exception:
                                    pass
                                logger.error(f"Error sending mailing {mailing_id} to {target}: {e}")
                                # Catch-all for error types not specifically classified above —
                                # without a backoff here, any future/unanticipated permanent
                                # per-chat error (Telegram periodically adds new RPC error
                                # strings) would silently regress to the same retry-storm
                                # pattern already fixed for the errors handled explicitly.
                                # Worst case for a genuinely transient error is a bounded
                                # 25-cycle delay before the next attempt, not indefinite loss.
                                await self._apply_target_restriction(mailing_id, target_obj, e)

                        # Delay between sends to different chats within this same
                        # pass — user-configurable (mailing.chat_delay_seconds,
                        # "Изменить КД между чатами"), floor enforced again here
                        # since this is the one place that actually governs the
                        # send pacing regardless of what got written to the DB.
                        await asyncio.sleep(max(3, mailing.chat_delay_seconds or 3))

                    logger.debug(
                        f"Mailing {mailing_id}: cycle done — sent {cycle_sent}, errors {cycle_errors}"
                    )

                    if sent_any:
                        await self.db.update_mailing_last_sent(mailing_id)

                    # Sleep until next possible send (min 30s, max 5min).
                    # Was capped at 60s regardless of the mailing's actual
                    # interval, so a mailing sending once an hour still woke
                    # up (and re-read mailing/messages/targets from the DB)
                    # every minute for nothing. Scaling the cap with the real
                    # interval only reduces idle polling for slow mailings —
                    # it doesn't touch send timing (still gated by targets'
                    # own last_sent_at) or the per-target pacing below.
                    min_interval = min(
                        (t.interval_seconds or mailing.interval_seconds for t in targets),
                        default=mailing.interval_seconds,
                    )
                    sleep_time = max(30, min(300, min_interval // 10))
                    if wake_in:
                        # Wake when the nearest chat is due instead of up to a whole
                        # poll later (a 300s mailing drifted up to +30s every send).
                        sleep_time = min(sleep_time, max(5.0, min(wake_in) + 1))
                    await asyncio.sleep(sleep_time)

                except asyncio.CancelledError:
                    me_task = asyncio.current_task()
                    if me_task is None or me_task.cancelling():
                        raise  # this mailing is being stopped (or the bot shuts down)
                    # Only an awaited request was cancelled: Telethon cancels the
                    # requests in flight when their client is disconnected (a
                    # reconnect, a relay switch). Ending the loop here left the
                    # mailing dead while still "active" in the DB, until a restart.
                    logger.warning(f"Mailing {mailing_id}: a request was cancelled by its client's reconnect — continuing")
                    await asyncio.sleep(5)
                except Exception as e:
                    # Type name too: TimeoutError (e.g. an account still
                    # connecting after a restart) has an empty str().
                    logger.error(f"Error in mailing {mailing_id} iteration: {type(e).__name__}: {e}")
                    if "database is locked" in str(e).lower():
                        await self.db._end_stuck_transaction(f"mailing {mailing_id} loop")
                    await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info(f"Mailing {mailing_id} task cancelled")
        except Exception as e:
            self._clear_working_targets(mailing_id)
            logger.error(f"Mailing {mailing_id} loop fatal error: {e}", exc_info=True)
            self._tasks.pop(mailing_id, None)
            if self._running:
                await asyncio.sleep(30)
                try:
                    mailing = await self.db.get_mailing(mailing_id)
                    if mailing and mailing.is_active:
                        logger.info(f"Mailing {mailing_id} auto-restarting after fatal error")
                        # The current task was removed from _tasks above, so the
                        # normal serialized start path is safe here.
                        await self._start_mailing_task(mailing_id)
                except Exception as restart_err:
                    logger.error(f"Mailing {mailing_id} auto-restart failed: {restart_err}")
        finally:
            current_task = self._tasks.get(mailing_id)
            if current_task is asyncio.current_task():
                self._clear_working_targets(mailing_id)

    async def _stop_for_forward_source(self, mailing, account_id: int):
        """The forwarded post can't be taken from its source any more (deleted,
        or the account lost access to that chat): every chat fails the same
        way, so the mailing stops and its owner is told what to fix."""
        await self.stop_mailing(mailing.id)
        self._fwd_src_strikes = {k: v for k, v in self._fwd_src_strikes.items() if k[0] != account_id}
        notify = getattr(self.userbot_manager, '_bot_notify_callback', None)
        if not notify:
            return
        try:
            user = await self.db.get_user_by_id(mailing.user_id)
            if not user:
                return
            name = ((mailing.name or "").strip().splitlines() or [""])[0][:40] or f"#{mailing.id}"
            await notify(
                user.telegram_id,
                pe(
                    "⚠️ <b>Рассылка остановлена</b>\n\n"
                    f"📨 Рассылка: <b>{html.escape(name)}</b>\n"
                    "❗️ Не удаётся переслать сообщение: исходный пост недоступен для аккаунта — "
                    "его удалили, или аккаунт потерял доступ к чату, откуда он пересылается.\n\n"
                    "Задайте сообщение рассылки заново и запустите её. Список чатов сохранён."
                ),
            )
        except Exception as e:
            logger.error(f"Mailing {mailing.id}: forward-source stop notice failed: {e}")

    async def _handle_mailing_ban(self, mailing_id: int, account_id: int, error: Exception):
        """Stop mailing (and any sibling mailings on the same account),
        deactivate account, notify owner."""
        error_name = type(error).__name__
        if isinstance(error, (UserDeactivatedBanError,)):
            reason = "⛔️ аккаунт заблокирован Telegram"
        elif isinstance(error, UserDeactivatedError):
            reason = "❌ аккаунт деактивирован"
        elif _FROZEN_ACCOUNT_MARKER in str(error).lower():
            reason = "🧊 аккаунт заморожен Telegram — нужна проверка в официальном приложении"
        else:
            reason = "🔑 сессия сброшена (аккаунт заморожен или вышел)"

        logger.warning(f"Mailing {mailing_id}: ban detected on account {account_id} — {error_name}")

        # A frozen/banned account is broken for ALL its mailings, not just the
        # one that happened to hit the error first — leaving siblings running
        # just means they keep failing (and writing to the DB) until their
        # own next cycle notices, so stop them all right away.
        sibling_ids = [
            m.id for m in await self.db.get_active_mailings_by_account(account_id)
            if m.id != mailing_id
        ]
        await self.stop_mailing(mailing_id)
        for sibling_id in sibling_ids:
            await self.stop_mailing(sibling_id)
        newly_deactivated = await self.db.deactivate_account(account_id)

        notify = getattr(self.userbot_manager, '_bot_notify_callback', None)
        if notify and newly_deactivated:
            try:
                account = await self.db.get_account(account_id)
                if account:
                    user = await self.db.get_user_by_id(account.user_id)
                    if user:
                        await notify(
                            user.telegram_id,
                            pe(
                                f"⚠️ <b>Проблема с аккаунтом!</b>\n\n"
                                f"📱 Аккаунт: <b>{html.escape(account.display_name)}</b>\n"
                                f"❗️ Причина: {reason}\n\n"
                                f"Рассылка остановлена. Аккаунт отключён.\n"
                                f"Добавьте аккаунт заново в разделе «Аккаунты»."
                            ),
                        )
            except Exception as e:
                logger.error(f"Failed to notify about ban for account {account_id}: {e}")

    def _is_active_hours(self, active_hours_json: Optional[str]) -> bool:
        return is_within_active_hours(active_hours_json)

    # === @SpamBot ===

    def _schedule_spambot_check(self, account_id: int, client, owner) -> None:
        """Ask @SpamBot whether this account is spam-limited — at most once per
        _SPAMBOT_CHECK_COOLDOWN per account, in the background so the mailing
        loop never waits on it."""
        now = time.monotonic()
        if now - self._spambot_checked_at.get(account_id, -1e9) < _SPAMBOT_CHECK_COOLDOWN:
            return
        # What @SpamBot said last time decides when to ask again — kept across
        # restarts (the in-memory cooldown alone reset on every one of them).
        if time.time() < self._spambot_state.get(account_id, {}).get("next", 0):
            return
        self._spambot_checked_at[account_id] = now
        task = asyncio.create_task(self._spambot_check(account_id, client, owner))
        self._spambot_tasks.add(task)
        task.add_done_callback(self._spambot_tasks.discard)

    async def _ask_spambot(self, client) -> Optional[str]:
        """Send /start to @SpamBot and return its reply text (None if no reply)."""
        try:
            sent = await asyncio.wait_for(client.send_message("SpamBot", "/start"), timeout=20)
        except Exception as e:
            if type(e).__name__ != "YouBlockedUserError":
                raise
            # The account had blocked @SpamBot — unblock it and try once more.
            from telethon.tl.functions.contacts import UnblockRequest
            await asyncio.wait_for(client(UnblockRequest(id="SpamBot")), timeout=20)
            sent = await asyncio.wait_for(client.send_message("SpamBot", "/start"), timeout=20)
        # The answer can come as several messages (the status, then a note):
        # once the first one is in, wait a moment and take all of them.
        for _ in range(10):
            await asyncio.sleep(2)
            msgs = await asyncio.wait_for(client.get_messages("SpamBot", limit=5), timeout=20)
            if any(m.id > sent.id and not m.out for m in msgs):
                await asyncio.sleep(2)
                msgs = await asyncio.wait_for(client.get_messages("SpamBot", limit=5), timeout=20)
                answer = [m for m in msgs if m.id > sent.id and not m.out]
                return "\n\n".join((m.raw_text or "") for m in sorted(answer, key=lambda m: m.id))
        return None

    @staticmethod
    def _spambot_is_free(reply: str) -> bool:
        low = reply.lower()
        return any(m in low for m in _SPAMBOT_FREE_MARKERS)

    async def _refresh_chat_senders(self):
        """How many active mailings target each chat — refreshed every few
        minutes (one cheap aggregate query), shared by all mailing loops."""
        now = time.monotonic()
        if now - self._chat_senders_at < _CHAT_SENDERS_REFRESH:
            return
        self._chat_senders_at = now
        try:
            async with self.db._conn.execute(
                "SELECT lower(t.chat_identifier), COUNT(DISTINCT t.mailing_id) "
                "FROM mailing_targets t JOIN mailings m ON m.id = t.mailing_id "
                "WHERE m.is_active = 1 GROUP BY 1 HAVING COUNT(DISTINCT t.mailing_id) > 1"
            ) as cur:
                self._chat_senders = {row[0]: row[1] for row in await cur.fetchall()}
            async with self.db._conn.execute(
                "SELECT m.user_id, lower(t.chat_identifier), COUNT(DISTINCT t.mailing_id) "
                "FROM mailing_targets t JOIN mailings m ON m.id = t.mailing_id "
                "WHERE m.is_active = 1 GROUP BY 1, 2 HAVING COUNT(DISTINCT t.mailing_id) > 1"
            ) as cur:
                self._owner_chat_senders = {(row[0], row[1]): row[2] for row in await cur.fetchall()}
            if len(self._owner_chat_last_send) > 50000:
                cutoff = now - 3600
                self._owner_chat_last_send = {k: v for k, v in self._owner_chat_last_send.items() if v > cutoff}
            if len(self._chat_last_send) > 50000:
                cutoff = now - _CHAT_SPREAD_MAX_GAP * 2
                self._chat_last_send = {k: v for k, v in self._chat_last_send.items() if v > cutoff}
        except Exception as e:
            logger.warning(f"Chat senders refresh failed: {e}")

    def _note_send_outcome(self, account_id: int, sent: bool) -> bool:
        """Record one send attempt; True when half or more of the recent
        ones failed — time to ask @SpamBot."""
        window = self._send_outcomes.get(account_id)
        if window is None:
            window = self._send_outcomes[account_id] = collections.deque(maxlen=_SPAM_WINDOW)
        window.append(sent)
        if len(window) >= _SPAM_MIN_ATTEMPTS and window.count(False) / len(window) >= _SPAM_ERROR_SHARE:
            window.clear()
            return True
        return False

    def _remember_spambot(self, account_id: int, status: str, until, text: str) -> dict:
        now = time.time()
        until_ts = until.replace(tzinfo=timezone.utc).timestamp() if until else None
        wait = _SPAMBOT_RECHECK.get(status, 6 * 3600)
        nxt = max(now + 6 * 3600, until_ts + 600) if until_ts else now + wait
        st = {"status": status, "until": until_ts, "checked": now, "next": nxt, "text": (text or "")[:600]}
        self._spambot_state[account_id] = st
        return st

    def spambot_state(self, account_id: int) -> Optional[dict]:
        return self._spambot_state.get(account_id)

    async def ask_spambot_now(self, account_id: int, client) -> dict:
        """For the admin: one /start to @SpamBot from the live client, the
        answer parsed and remembered (the automatic check then waits)."""
        reply = await self._ask_spambot(client)
        if reply is None:
            return {"status": "noreply", "text": ""}
        status, until = parse_spambot_reply(reply)
        return self._remember_spambot(account_id, status, until, reply)

    async def _spambot_check(self, account_id: int, client, owner) -> None:
        try:
            first = await self._ask_spambot(client)
            if first is None:
                logger.warning(f"SpamBot check: no reply for account {account_id}")
                return
            status, until = parse_spambot_reply(first)
            lifted = False
            if status == "unknown":
                # Not a status (a greeting, a menu) — the second /start gets it.
                second = await self._ask_spambot(client)
                if second is not None:
                    first = second
                    status, until = parse_spambot_reply(second)
            elif status == "limited":
                me = await asyncio.wait_for(client.get_me(), timeout=20)
                if getattr(me, "premium", False):
                    # Premium: a second /start can lift the limit.
                    second = await self._ask_spambot(client)
                    if second is not None and parse_spambot_reply(second)[0] == "free":
                        status, until, lifted = "free", None, True
            if status == "tos":  # first: the pause must not depend on the bookkeeping below
                self._tos_blocked.add(account_id)
                self._tos_pause_until[account_id] = time.monotonic() + _TOS_BLOCK_PAUSE
            self._remember_spambot(account_id, status, until, first)
            if status in ("free", "unknown") and not lifted:
                logger.info(f"SpamBot check: account {account_id} — {status}: {first[:200]!r}")
                return
            logger.warning(
                f"SpamBot check: account {account_id} — {status}"
                f"{' until ' + until.strftime('%Y-%m-%d %H:%M UTC') if until else ''}"
                f"{' (lifted by a second /start)' if lifted else ''}: {first[:400]!r}"
            )
        except Exception as e:
            logger.warning(f"SpamBot check failed for account {account_id}: {type(e).__name__}: {e}")

    async def _apply_target_restriction(
        self, mailing_id: int, target_obj, error: Exception,
        cycles: int = _CHAT_BAN_SKIP_CYCLES,
    ):
        """Back off retrying this target for a while — a per-chat restriction
        (join-required, write-forbidden, banned-in-channel, slow mode, ...)
        never pauses the whole account or removes the target from the
        mailing, it just gets retried less often."""
        chat = target_obj.chat_identifier
        error_name = type(error).__name__
        self._chat_ban_skip[target_obj.id] = cycles
        ctx = self._spam_ctx.get(mailing_id)
        if ctx and not isinstance(error, asyncio.TimeoutError):
            account_id, client, owner = ctx
            if self._note_send_outcome(account_id, False):
                self._schedule_spambot_check(account_id, client, owner)
        if error_name in _BAN_BACKOFF_ERRORS and cycles == _CHAT_BAN_SKIP_CYCLES:
            now_wall = time.time()
            strikes, last = self._ban_strikes.get(target_obj.id, (0, 0.0))
            if now_wall - last > _BAN_STRIKE_MEMORY:
                strikes = 0
            strikes += 1
            hours = _BAN_BACKOFF_HOURS[min(strikes, len(_BAN_BACKOFF_HOURS)) - 1]
            self._ban_strikes[target_obj.id] = (strikes, now_wall)
            self._ban_until[target_obj.id] = time.monotonic() + hours * 3600
            self._chat_ban_skip.pop(target_obj.id, None)  # the timed pause replaces the cycle skip
            if strikes == 1 or hours >= 24:
                logger.warning(
                    f"Mailing {mailing_id}: restricted in '{chat}' ({error_name}) — "
                    f"pausing this chat for {hours}h (strike {strikes}), target kept"
                )
            return
        logger.warning(
            f"Mailing {mailing_id}: restricted in '{chat}' ({error_name}) — "
            f"skipping next {cycles} cycles, target kept"
        )

    async def _remove_target(self, mailing_id: int, target_obj, reason: str):
        """Drop a target whose failure retrying can never fix (paid-only chat,
        username that no longer exists) instead of re-probing it every
        backoff window forever."""
        await self.db.delete_mailing_target(target_obj.id)
        self.forget_target(mailing_id, target_obj.id)
        self._chat_ban_skip.pop(target_obj.id, None)
        self._ban_until.pop(target_obj.id, None)
        self._ban_strikes.pop(target_obj.id, None)
        logger.warning(
            f"Mailing {mailing_id}: '{target_obj.chat_identifier}' {reason} — target removed from mailing"
        )

    async def _remove_paid_messages_target(self, mailing_id: int, target_obj):
        await self._remove_target(mailing_id, target_obj, "requires paid messages (ALLOW_PAYMENT_REQUIRED)")

    async def _try_join_and_send(
        self,
        client,
        target: str,
        target_obj,
        msg,
        pm,
        mailing_id: int,
        account_id: Optional[int] = None,
        reply_to=None,
        add_signature: bool = False,
        hidden_tag_user_ids: Optional[list[int]] = None,
    ) -> bool:
        """Try to join target channel/group, then retry sending. Returns True if message was sent."""
        block_key = (target_obj.id, account_id)
        block = self._rejoin_block.get(block_key)
        if block and block[0] > time.monotonic():
            self._chat_ban_skip[target_obj.id] = _CHAT_BAN_SKIP_CYCLES
            return False
        # Joins of one account are spaced out: this chat simply waits for the
        # next round when the account joined somewhere less than 30s ago.
        now_mono = time.monotonic()
        if now_mono - self._last_join.get(account_id, -1e9) < _JOIN_MIN_GAP:
            return False
        self._last_join[account_id] = now_mono
        mailing = await self.db.get_mailing(mailing_id)
        try:
            if isinstance(target, str) and "t.me/+" in target:
                joined = await client(ImportChatInviteRequest(target.rsplit("+", 1)[1]))
                if getattr(joined, "chats", None):
                    target = joined.chats[0]
            else:
                joined = await client(JoinChannelRequest(target))
            # A private chat added by invite link can't be recognised as a
            # forum before joining; now that we're in, record it so the
            # targets list flags it as a chat with topics.
            joined_chats = getattr(joined, "chats", None) or []
            if joined_chats and getattr(joined_chats[0], "forum", False) and not target_obj.is_forum:
                try:
                    await self.db.update_target_is_forum(target_obj.id, True)
                except Exception as e:
                    logger.warning(f"Mailing {mailing_id}: failed to mark target {target_obj.id} as forum: {e}")
            logger.info(
                f"[DIAG account={account_id} mailing={mailing_id} target_id={target_obj.id} "
                f"target={target}] auto-joined (never sent before={target_obj.last_sent_at is None})"
            )
            # A real person doesn't post seconds after joining — that gap alone
            # is a recognizable automation signal. Wider and randomized instead
            # of a flat 2s.
            await asyncio.sleep(random.uniform(20, 40))
        except UserAlreadyParticipantError:
            logger.info(
                f"[DIAG account={account_id} mailing={mailing_id} target_id={target_obj.id} "
                f"target={target}] already participant — trying to send "
                f"(never sent before={target_obj.last_sent_at is None})"
            )
        except InviteRequestSentError as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            logger.info(f"Mailing {mailing_id}: join request sent/pending for '{target}'")
            return False
        except _BAN_ERRORS as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            await self._handle_mailing_ban(mailing_id, account_id, e)
            return False
        except FloodWaitError as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type="FloodWait",
                    error_text=f"{e.seconds}s",
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
                user = await self.db.get_user_by_id(mailing.user_id)
                await self._notify_flood_wait(user, account_id, e.seconds, rest=_flood_rest())
            # No in-place sleep — _notify_flood_wait already set flood_wait_until,
            # which the mailing loop's top-of-cycle gate waits out (see the
            # comment on the main FloodWaitError handler for why sleeping
            # here too caused duplicate notifications).
            return False
        except Exception as e:
            if channel_cleanup.is_channels_too_much(e):
                # make room: the account leaves its oldest channels in the background
                channel_cleanup.schedule(self.db, client, account_id, str(account_id))
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            if _FROZEN_ACCOUNT_MARKER in str(e).lower():
                # Same as the frozen check in the main send path — without
                # this, a frozen account that isn't yet a member of the
                # target chat fails here on every join attempt forever
                # (this join step runs before any send is even attempted),
                # never reaching the send-path check that would have
                # caught it and stopped the mailing.
                await self._handle_mailing_ban(mailing_id, account_id, e)
                return False
            logger.warning(f"Mailing {mailing_id}: failed to join '{target}': {e}")
            if _is_dead_username_error(e):
                await self._remove_target(mailing_id, target_obj, "username no longer exists")
            elif type(e).__name__ in _DEAD_INVITE_ERRORS:
                await self._remove_target(mailing_id, target_obj, "invite link expired")
            elif not isinstance(e, ConnectionError):
                await self._apply_target_restriction(mailing_id, target_obj, e)
            return False

        try:
            if target_obj.last_sent_at is None and target_obj.id not in self._no_hello:
                await asyncio.wait_for(client.send_message(
                    target, random.choice(_FIRST_HELLO), reply_to=reply_to,
                ), timeout=60)  # first time in this chat: just say hi
                self._target_next_msg.pop(target_obj.id, None)
            else:
                await asyncio.wait_for(self._send_msg(
                    client, target, msg, pm,
                    reply_to=reply_to,
                    add_signature=add_signature,
                    hidden_tag_user_ids=hidden_tag_user_ids,
                    force_text_only=target_obj.media_disabled,
                    target_snapshot=target_obj,
                    account_id=account_id,
                    mailing_id=mailing_id,
                ), timeout=60)
            # Same reasoning as the main send path in _mailing_loop — the
            # message was genuinely delivered, so a bookkeeping failure
            # here must not be misrouted into the except clauses below as
            # a send failure — buffered like every other successful send.
            self._record_sent(target_obj.id, account_id, mailing_id)
            self._schedule_gate_check(client, target, account_id)
            self._rejoin_block.pop(block_key, None)
            logger.info(f"Mailing {mailing_id}: sent to '{target}' after auto-join")
            return True
        except _BAN_ERRORS as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            await self._handle_mailing_ban(mailing_id, account_id, e)
            return False
        except FloodWaitError as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type="FloodWait",
                    error_text=f"{e.seconds}s",
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
                user = await self.db.get_user_by_id(mailing.user_id)
                await self._notify_flood_wait(user, account_id, e.seconds, rest=_flood_rest())
            # No in-place sleep — _notify_flood_wait already set flood_wait_until,
            # which the mailing loop's top-of-cycle gate waits out (see the
            # comment on the main FloodWaitError handler for why sleeping
            # here too caused duplicate notifications).
            return False
        except ChatWriteForbiddenError as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            logger.warning(f"Mailing {mailing_id}: can't write to '{target}' after join — slow mode or temporary restriction, skipping")
            self._write_forbidden_after_join.add((target_obj.id, account_id))
            self._block_rejoin(block_key)
            # Same reasoning as the _CHAT_BAN_ERRORS clause below — without a
            # skip/removal decision here, a chat that joins fine but never
            # actually allows writing (a permanent restriction, not a slow
            # mode blip) gets rejoined and retried on every single cycle
            # forever, eating real wall-clock time each pass and starving
            # the rest of the target list of progress.
            await self._apply_target_restriction(
                mailing_id, target_obj, e,
            )
            return False
        except _CHAT_BAN_ERRORS as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            # Without this, a target banned specifically on the post-join retry
            # send (as opposed to the main send path, which already handles
            # this) fell into the generic except below with no skip/removal
            # tracking — retried again next cycle forever instead of backing off.
            self._block_rejoin(block_key)
            await self._apply_target_restriction(mailing_id, target_obj, e)
            return False
        except Exception as e:
            if mailing:
                await self.db.add_error_log(
                    user_id=mailing.user_id,
                    error_type=type(e).__name__,
                    error_text=str(e)[:300],
                    account_id=account_id,
                    mailing_id=mailing_id,
                    chat_identifier=str(target),
                )
            if _FROZEN_ACCOUNT_MARKER in str(e).lower():
                # Same reasoning as the join-step and main send-path checks —
                # an account can also freeze exactly during this post-join
                # retry send, not just at the join RPC itself.
                await self._handle_mailing_ban(mailing_id, account_id, e)
                return False
            logger.error(f"Mailing {mailing_id}: retry send to '{target}' failed after join: {e}")
            if "ALLOW_PAYMENT_REQUIRED" in str(e):
                await self._remove_paid_messages_target(mailing_id, target_obj)
            elif isinstance(e, ConnectionError):
                pass  # account connection dropped, not this chat's fault
            elif "TOPIC_CLOSED" in str(e):
                # Multi-topic targets: only that topic is closed, rotation
                # moves on — see the matching branch in _mailing_loop.
                if not target_obj.topic_ids:
                    await self._apply_target_restriction(
                        mailing_id, target_obj, e, cycles=_TOPIC_CLOSED_SKIP_CYCLES,
                    )
            else:
                # Without a backoff here the next cycle repeats the whole
                # send -> JoinChannel -> send sequence; production showed
                # ~840 such retries/day (up to ~80 per chat), and repeatedly
                # re-joining the same chat is a strong spam-bot signal.
                self._block_rejoin(block_key)
                await self._apply_target_restriction(mailing_id, target_obj, e)
            return False

    def _block_rejoin(self, key: tuple) -> None:
        """Joined (or was in) the chat but couldn't post: no auto-join there
        from this account for 6h, doubling on each repeat up to 48h."""
        now = time.monotonic()
        if len(self._rejoin_block) > 20000:
            self._rejoin_block = {k: v for k, v in self._rejoin_block.items() if v[0] > now - 48 * 3600}
        strikes = self._rejoin_block.get(key, (0.0, 0))[1] + 1
        self._rejoin_block[key] = (now + min(6 * 2 ** (strikes - 1), 48) * 3600, strikes)
