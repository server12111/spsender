"""Small human touches for warmup chats (DMs and warmup groups): stickers and
GIFs instead of some short replies, forwarded channel posts with a comment,
and the occasional typo fixed by editing the message a few seconds later."""
import asyncio
import logging
import random
import time

from telethon.errors import FloodWaitError, PeerFloodError

logger = logging.getLogger(__name__)

_STICKER_SHARE = 0.1       # short replies sent as a sticker / GIF instead (about 1 in 10)
_GIF_SHARE = 0.1           # of those, a GIF rather than a sticker (about 1 in 10)
_TYPO_EDIT_SHARE = 0.06    # messages sent with a typo, then edited
_TYPO_RESEND_SHARE = 0.02  # messages sent with a typo, deleted and written again
_GIF_QUERIES = ["смешно", "ахаха", "кот", "ок", "класс", "лол", "да", "привет", "спасибо", "жиза", "ну да", "вау"]
FORWARD_COMMENTS = ["глянь", "ахах", "жиза", "смотри", "😂", "вот это да", "видел?", "прикол", "интересно"]
FORWARD_REPLIES = ["ору", "ахахах", "норм", "😂", "жесть", "прикольно", "ага, видел", "хаха, жиза", "ничего себе"]

_EMOJI_OF = {
    "хах": "😂", "хаха": "😂", "ахах": "😂", "😂": "😂", "😁": "😂", "😅": "😅",
    "👍": "👍", "понял": "👍", "ок": "👍", "окей": "👍", "норм": "👍", "ага": "👍", "согласен": "👍",
    "жиза": "😅", "бывает": "😅", "ну да": "😅", "🙂": "🙂", ")": "🙂",
}

# Neighbouring keys on a ЙЦУКЕН keyboard — a mistyped letter is one of these.
_ROWS = ["йцукенгшщзхъ", "фывапролджэ", "ячсмитьбю"]
_NEIGHBOURS = {}
for _row in _ROWS:
    for _i, _ch in enumerate(_row):
        _NEIGHBOURS[_ch] = (_row[_i - 1] if _i else "") + (_row[_i + 1] if _i + 1 < len(_row) else "")


class StickerNote:
    """Script slot: a sticker (or GIF) matching `emoji` instead of a short reply."""
    __slots__ = ("emoji", "fallback")

    def __init__(self, emoji: str, fallback: str):
        self.emoji, self.fallback = emoji, fallback


class ForwardNote:
    """Script slot: forward a recent channel post with a short comment."""
    __slots__ = ()


class ForwardReply:
    """Script slot right after a ForwardNote: the other side reacts (only if it went out)."""
    __slots__ = ()


def maybe_sticker(text: str):
    """A short reply → sometimes a StickerNote for it, else the text."""
    key = text.strip().lower()
    if len(key) <= 8 and random.random() < _STICKER_SHARE:
        return StickerNote(_EMOJI_OF.get(key, random.choice(["😂", "👍", "😅", "🙂", "🔥"])), text)
    return text


def make_typo(text: str):
    """A plausible typo of `text` (swapped neighbours or a neighbouring key), or None."""
    if len(text) < 8:
        return None
    idx = [i for i, ch in enumerate(text) if ch.lower() in _NEIGHBOURS]
    if len(idx) < 4:
        return None
    i = random.choice(idx[1:-1])
    if random.random() < 0.5 and i + 1 < len(text) and text[i + 1].isalpha() and text[i] != text[i + 1]:
        return text[:i] + text[i + 1] + text[i] + text[i + 2:]
    nb = _NEIGHBOURS.get(text[i].lower())
    if not nb:
        return None
    ch = random.choice(nb)
    return text[:i] + (ch.upper() if text[i].isupper() else ch) + text[i + 1:]


async def send_text(client, peer, text: str, reply_to=None):
    """send_message, now and then with a typo: usually fixed by editing a few
    seconds later, sometimes deleted for both sides and typed again."""
    r = random.random()
    mode = "resend" if r < _TYPO_RESEND_SHARE else "edit" if r < _TYPO_RESEND_SHARE + _TYPO_EDIT_SHARE else None
    typo = make_typo(text) if mode else None
    msg = await client.send_message(peer, typo or text, reply_to=reply_to)
    if not typo:
        return msg
    await asyncio.sleep(random.uniform(2, 6))
    try:
        if mode == "edit":
            await client.edit_message(peer, msg, text)
            return msg
        await client.delete_messages(peer, [msg.id], revoke=True)
        await asyncio.sleep(random.uniform(1.5, 2.5) + len(text) * random.uniform(0.08, 0.2))  # typing it again
        return await client.send_message(peer, text, reply_to=reply_to)
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: typo fix failed: {e}")
        return msg


_sticker_cache: dict = {}  # (account_id, emoji) -> (ts, [documents])


async def send_sticker(client, peer, account_id: int, note: StickerNote, reply_to=None):
    """A sticker for the emoji (or a GIF via @gif). Returns the message or None."""
    from telethon.tl.functions.messages import GetStickersRequest
    try:
        if random.random() < _GIF_SHARE:
            try:
                results = await client.inline_query("gif", random.choice(_GIF_QUERIES))
                if results:
                    return await random.choice(list(results)[:10]).click(peer, reply_to=reply_to)
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                logger.debug(f"Warmup extras: gif failed, using a sticker: {e}")
        key = (account_id, note.emoji)
        cached = _sticker_cache.get(key)
        if not cached or time.time() - cached[0] > 6 * 3600:
            res = await client(GetStickersRequest(emoticon=note.emoji, hash=0))
            docs = list(getattr(res, "stickers", None) or [])[:40]
            if len(_sticker_cache) > 3000:
                _sticker_cache.clear()
            _sticker_cache[key] = cached = (time.time(), docs)
        if not cached[1]:
            return None
        return await client.send_file(peer, random.choice(cached[1]), reply_to=reply_to)
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: sticker failed: {e}")
        return None


async def forward_post(client, peer):
    """Forward a recent, ad-free post from one of the account's channels."""
    try:
        dialogs = await client.get_dialogs(limit=40)
        channels = [d for d in dialogs if getattr(d.entity, "broadcast", False)]
        random.shuffle(channels)
        for d in channels[:4]:
            msgs = await client.get_messages(d.entity, limit=15)
            good = [m for m in msgs if m and not m.action and not m.reply_markup and (m.message or m.media)
                    and "http" not in (m.message or "") and "t.me/" not in (m.message or "")
                    and not getattr(m, "noforwards", False)]
            if good:
                return await client.forward_messages(peer, random.choice(good[:8]))
        return None
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: forward failed: {e}")
        return None


REACTIONS = ["👍", "❤", "🔥", "😂", "👏", "😁"]


async def react(client, peer, msg_id: int) -> bool:
    """Put a reaction on msg_id; when the chat doesn't allow the picked emoji,
    fall back to the ones nearly every chat allows."""
    from telethon.tl.functions.messages import SendReactionRequest
    from telethon.tl.types import ReactionEmoji
    for emo in dict.fromkeys([random.choice(REACTIONS), "👍", "❤"]):
        try:
            await client(SendReactionRequest(peer=peer, msg_id=msg_id, reaction=[ReactionEmoji(emoticon=emo)]))
            return True
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            if "REACTION_INVALID" not in str(e).upper():
                logger.debug(f"Warmup extras: reaction failed: {e}")
                return False
    return False


def pick_imported_user(res, expected_id=None, phone: str = ""):
    """The partner account out of a contacts.importContacts result. Never a
    bot, and when we know the partner's Telegram id only that user: a bare
    users[0] once sent a whole warmup chat side into some bot's DM."""
    users = [u for u in (getattr(res, "users", None) or []) if not getattr(u, "bot", False)]
    if expected_id:
        return next((u for u in users if u.id == expected_id), None)
    imported = {c.user_id for c in (getattr(res, "imported", None) or [])}
    digits = "".join(ch for ch in phone or "" if ch.isdigit())
    return (next((u for u in users if u.id in imported), None)
            or next((u for u in users if digits and "".join(ch for ch in (getattr(u, "phone", None) or "") if ch.isdigit()) == digits), None)
            or (users[0] if len(users) == 1 else None))


class ChatGuard:
    """Notices a person typing into a warmup chat from one of the accounts:
    every message the script sends is recorded, and an outgoing message that
    isn't ours (after the baseline taken at the first check) means the owner
    stepped in — the script must stop and never quote/react to it."""

    def __init__(self):
        self._base: dict = {}      # key -> top message id at the first check (None: history unavailable)
        self._mine: dict = {}      # key -> ids of messages the script sent
        self._rebase: set = set()  # keys whose last send returned no id

    def mark(self, key, res) -> None:
        for m in (res if isinstance(res, list) else [res]):
            mid = getattr(m, "id", None)
            if isinstance(mid, int):
                self._mine.setdefault(key, set()).add(mid)
            elif m is not None:
                self._rebase.add(key)

    async def intruded(self, key, client, peer, limit: int = 8) -> bool:
        try:
            msgs = [m for m in await client.get_messages(peer, limit=limit) if m]
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            logger.debug(f"Warmup extras: guard history fetch failed: {e}")
            return False
        top = max((m.id for m in msgs), default=0)
        if key not in self._base or key in self._rebase:
            self._rebase.discard(key)
            self._base[key] = max(top, self._base.get(key) or 0)
            return False
        base = self._base[key]
        mine = self._mine.get(key, set())
        return any(m.id > base and m.out and not m.action and m.id not in mine for m in msgs)


async def last_incoming_id(client, peer, sender_id=None, limit: int = 6):
    """Id of the latest message from `sender_id` (or any incoming one) as THIS
    client sees it — in private chats and basic groups every member has its own
    message numbering, so another account's msg.id is meaningless here."""
    try:
        for m in await client.get_messages(peer, limit=limit):
            if m and not m.out and not m.action and (sender_id is None or m.sender_id == sender_id):
                return m.id
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: history fetch failed: {e}")
    return None


SAVED_NOTES = [
    "купить хлеб и молоко", "позвонить маме", "не забыть оплатить интернет", "записаться к стоматологу",
    "забрать посылку", "купить корм коту", "день рождения у Саши в субботу", "сдать отчёт до пятницы",
    "посмотреть этот фильм", "купить батарейки", "продлить подписку", "оплатить коммуналку",
    "встреча в 15:00", "отдать долг Диме", "купить подарок", "забронировать столик",
    "поменять масло в машине", "заказать воду", "почитать про это", "спросить у Ани про выходные",
    "список покупок: яйца, сыр, кофе", "пароль от вайфая на работе спросить", "сходить в спортзал",
    "заплатить за телефон", "купить лампочку", "отнести вещи в химчистку", "проверить почту",
    "напомнить про встречу", "посмотреть билеты", "сделать бэкап фоток", "забрать справку",
    "рецепт: паста с курицей", "идея на выходные — за город", "вернуть книгу", "купить зарядку",
]


async def saved_note(client) -> str:
    """Saved Messages: either a channel post kept for later or a short note to self."""
    if random.random() < 0.5 and await forward_post(client, "me") is not None:
        return "saved_post"
    await client.send_message("me", random.choice(SAVED_NOTES))
    return "saved_note"


# === dialog pacing ===

AWAY_SORRY = ["сорян, был занят", "извини, отходил", "прости, отвлекли", "сорри, только освободился",
              "я тут, отвлёкся немного", "извини, не видел", "ой, пропал, сорян"]


async def hesitate(client, peer):
    """Starts typing, stops, comes back to it a bit later — like rethinking a reply."""
    from telethon.tl.functions.messages import SetTypingRequest
    from telethon.tl.types import SendMessageCancelAction, SendMessageTypingAction
    try:
        await client(SetTypingRequest(peer, SendMessageTypingAction()))
        await asyncio.sleep(random.uniform(3, 8))
        await client(SetTypingRequest(peer, SendMessageCancelAction()))
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception:
        return
    await asyncio.sleep(random.uniform(20, 60))


# === channel activity ===

_SEARCH_QUERIES = ["новости", "рецепты", "кино", "музыка", "спорт", "путешествия", "юмор", "технологии",
                   "авто", "книги", "психология", "здоровье", "мемы", "игры", "погода", "футбол", "мода", "крипта"]


async def count_views(client, peer, ids) -> None:
    """Count the post views, as the app does while scrolling a channel."""
    from telethon.tl.functions.messages import GetMessagesViewsRequest
    if not ids:
        return
    try:
        await client(GetMessagesViewsRequest(peer=peer, id=list(ids), increment=True))
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: views not counted: {e}")


async def vote_poll(client) -> bool:
    """Vote in an open poll in one of the account's channels/groups, if any."""
    from telethon.tl.functions.messages import SendVoteRequest
    from telethon.tl.types import MessageMediaPoll
    try:
        dialogs = [d for d in await client.get_dialogs(limit=40) if not d.is_user]
        random.shuffle(dialogs)
        for d in dialogs[:6]:
            for m in await client.get_messages(d.input_entity, limit=30):
                media = getattr(m, "media", None)
                if not isinstance(media, MessageMediaPoll) or media.poll.closed:
                    continue
                # Preserve ordinary polls, never vote through a bot/human gate.
                from app.services.memberships import CHALLENGE
                if getattr(media.poll,"quiz",False):
                    continue
                question=getattr(media.poll.question,"text",str(media.poll.question))
                if CHALLENGE.search(question) or any(word in question.lower() for word in ("провер", "человек", "робот", "капч")):
                    continue
                sender=await m.get_sender()
                if sender is None or getattr(sender,"bot",False):
                    continue
                results = getattr(media.results, "results", None) or []
                if any(getattr(r, "chosen", False) for r in results):
                    continue  # already voted
                answers = media.poll.answers
                k = random.randint(1, min(2, len(answers))) if media.poll.multiple_choice else 1
                await client(SendVoteRequest(peer=d.input_entity, msg_id=m.id,
                                             options=[a.option for a in random.sample(list(answers), k)]))
                return True
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: poll vote skipped: {e}")
    return False


async def search_channels(client, may_join: bool) -> bool:
    """Search Telegram for a topic and look through the results; now and then
    join a found public channel. Returns True if it joined one."""
    from telethon.tl.functions.channels import JoinChannelRequest
    from telethon.tl.functions.contacts import SearchRequest
    try:
        res = await client(SearchRequest(q=random.choice(_SEARCH_QUERIES), limit=10))
        # public channels the account isn't in yet ("left" is set on chats it's not a member of)
        chans = [c for c in (getattr(res, "chats", None) or [])
                 if getattr(c, "broadcast", False) and getattr(c, "username", None) and getattr(c, "left", True)]
        if not chans:
            return False
        ch = random.choice(chans)
        await asyncio.sleep(random.uniform(3, 10))
        try:
            await client.get_messages(ch, limit=random.randint(3, 10))  # peeks at it
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception:
            pass
        if may_join and random.random() < 0.15:
            await client(JoinChannelRequest(ch))
            return True
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: channel search skipped: {e}")
    return False


async def tidy_chats(client, warmup_usernames: set) -> None:
    """Small housekeeping that never touches the owner's own chats: mute or
    archive one of the channels warmup itself joined, or pin / unpin Saved Messages."""
    from telethon.tl.functions.account import UpdateNotifySettingsRequest
    from telethon.tl.functions.messages import ToggleDialogPinRequest
    from telethon.tl.types import InputDialogPeer, InputNotifyPeer, InputPeerNotifySettings, InputPeerSelf
    try:
        r = random.random()
        if r < 0.3:
            pinned = any(d.pinned and getattr(d.entity, "is_self", False) for d in await client.get_dialogs(limit=15))
            await client(ToggleDialogPinRequest(peer=InputDialogPeer(InputPeerSelf()), pinned=not pinned))
            return
        ours = [d for d in await client.get_dialogs(limit=60)
                if getattr(d.entity, "broadcast", False)
                and (getattr(d.entity, "username", "") or "").lower() in warmup_usernames]
        if not ours:
            return
        d = random.choice(ours)
        if r < 0.7:
            await client(UpdateNotifySettingsRequest(
                peer=InputNotifyPeer(d.input_entity), settings=InputPeerNotifySettings(mute_until=2**31 - 1)))
        else:
            await client.edit_folder(d.input_entity, 1)
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: tidy skipped: {e}")


# === bot visits ===
# Only buttons that just show information — checked live (read-only probe).
# Nothing that buys, tops up or starts a search that may spend attempts. Our
# own bot isn't here: every account just /starts it every 10 days instead
# (WarmupService._own_bot_loop).
BOT_SAFE_BUTTONS = {
    "RareNickBot": ["menu:profile", "menu:ref"],
    "FeTgAccountbot": ["profile", "my_promos", "menurules"],
    "userinfobot": [],  # /start only — it just replies with the account's id
}


async def visit_bot(client, bot: str) -> bool:
    """/start a bot, look around and press 1-2 of its whitelisted buttons."""
    safe = BOT_SAFE_BUTTONS.get(bot, [])
    try:
        ent = await client.get_entity(bot)
        clicks = random.randint(1, 2) if safe else 0
        for i in range(max(1, clicks)):
            await client.send_message(ent, "/start")
            await asyncio.sleep(random.uniform(3, 8))
            if not safe:
                break
            msg = None
            for m in await client.get_messages(ent, limit=3):
                if m and not m.out and getattr(m, "reply_markup", None):
                    msg = m
                    break
            if not msg:
                break
            present = [getattr(b,"type",b).data.decode(errors="replace") for row in getattr(msg.reply_markup, "rows", []) or []
                       for b in row.buttons if getattr(getattr(b,"type",b), "data", None)]
            options = [d for d in safe if d in present]
            if not options:
                break
            await asyncio.sleep(random.uniform(2, 6))  # reading the menu
            await msg.click(data=random.choice(options).encode())
            await asyncio.sleep(random.uniform(5, 15))  # reading what it opened
        await client.send_read_acknowledge(ent)
        return True
    except (FloodWaitError, PeerFloodError):
        raise
    except Exception as e:
        logger.debug(f"Warmup extras: bot visit to {bot} skipped: {e}")
        return False
