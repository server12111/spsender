"""Warmup group chats: small basic groups (3-5 members) where a client's
warming accounts talk with accounts from the admin's warmup pool.

Accounts of different clients never share a group: every group belongs to
one client (owner_user_id) and holds only that client's warming accounts plus
pool accounts. A pool account may sit in any number of groups. Groups live
for many days (stored in warmup_groups / warmup_group_members); each talks
1-2 times a day. They're muted and archived for every member, and the
userbot manager ignores their messages (no autoresponder / parser / sponsor
handling) via userbot_manager._warmup_chats.
"""
import asyncio
import io
import logging
import random
import time
from datetime import timedelta

from telethon.errors import FloodWaitError, PeerFloodError

from .config import config
from .services import _WD_FILLERS, _WD_THREADS, _contact_name, now_moscow
from .warmup_voices import VOICE_REPLIES, topic_of
from .warmup_extras import (FORWARD_COMMENTS, FORWARD_REPLIES, ChatGuard, StickerNote, forward_post, last_incoming_id,
                            pick_imported_user,
                            maybe_sticker, react, send_sticker, send_text)

logger = logging.getLogger(__name__)

_GROUP_SIZE = (3, 5)                  # members, creator included
_GROUPS_PER_WARMING_ACCOUNT = 2
_GROUP_MESSAGES = (30, 60)
_GROUP_TALK_GAP_H = (8, 14)           # between two talks of one group
_GROUP_JOIN_GAP_H = 20                # a warming account gets at most one new group per this
_CREATOR_PER_DAY = 2                  # groups one account creates per 24h
_GROUP_MSG_GAP = (15, 150)            # seconds between messages
_GROUP_HOURS = (6, 23)                # MSK: start talks/creations 06-23 (a talk is ~1h); at midnight they wrap up

_TITLES = [
    "Пацаны", "Наш чатик", "Выходные", "Работа 💼", "Дача", "Друзья", "Болтушка", "Свои",
    "Кофе ☕", "Флуд", "Чатик", "Компания", "Спорт ⚽", "Кино 🎬", "Поездка", "Планы",
    "Соседи", "Общий", "Тусовка", "Наши", "Вечер", "Без названия", "Чилл", "Движ",
]
_GROUP_OPENERS = ["всем привет", "народ, привет)", "привет всем", "ку всем", "хай, народ", "всем доброго дня",
                  "народ, как вы?", "о, все тут)"]
_GROUP_GREETS = ["привет)", "ку", "здарова", "хай)", "привет-привет", "о, привет", "всем ку)", "👋"]
_GROUP_FIRST = ["закинул вас сюда, так удобнее", "сделал чат, чтобы всем вместе болтать", "вот, общий чат)",
                "решил собрать всех в одном месте"]
_GROUP_CLOSERS = ["ладно, я побежал", "ок, позже напишу", "всё, я по делам", "давайте, до связи", "ладно, мне пора"]
_GROUP_BYES = ["давай)", "пока", "👋", "до связи", "ага, давай", "удачи)"]

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS warmup_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        owner_user_id INTEGER NOT NULL,
        creator_account_id INTEGER,
        title TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        next_talk_ts REAL NOT NULL DEFAULT 0,
        created_ts REAL NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS warmup_group_members (
        group_id INTEGER NOT NULL,
        account_id INTEGER NOT NULL,
        joined_ts REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (group_id, account_id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_wgm_account ON warmup_group_members(account_id)",
)


def group_avatar() -> bytes:
    """A random abstract picture: two-colour gradient with soft circles."""
    from PIL import Image, ImageDraw, ImageFilter
    size = 512
    c1 = [random.randint(30, 230) for _ in range(3)]
    c2 = [random.randint(30, 230) for _ in range(3)]
    img = Image.new("RGB", (size, size))
    px = img.load()
    diag = random.random() < 0.5
    for y in range(size):
        for x in range(size):
            t = ((x + y) / (2 * size)) if diag else (y / size)
            px[x, y] = tuple(int(c1[i] + (c2[i] - c1[i]) * t) for i in range(3))
    layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    for _ in range(random.randint(3, 7)):
        r = random.randint(40, 170)
        cx, cy = random.randint(0, size), random.randint(0, size)
        col = tuple(random.randint(0, 255) for _ in range(3)) + (random.randint(60, 140),)
        draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=col)
    layer = layer.filter(ImageFilter.GaussianBlur(random.randint(6, 18)))
    img = Image.alpha_composite(img.convert("RGBA"), layer).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=88)
    return buf.getvalue()


def group_script(n: int) -> list[tuple]:
    """Lines for a group talk between n speakers (0 = whoever starts):
    ("msg", speaker, text, reply_to_previous), ("react", speaker),
    ("voice", speaker, topic) or ("vreply", speaker) — a text answer to the
    voice note just before it, only sent if that went out."""
    target = random.randint(*_GROUP_MESSAGES)
    lines: list[tuple] = [("msg", 0, random.choice(_GROUP_OPENERS), False)]
    for sp in random.sample(range(1, n), k=min(n - 1, random.randint(1, 2))):
        lines.append(("msg", sp, random.choice(_GROUP_GREETS), False))
    count = len(lines)
    voice_slots = 0
    forwarded = False
    for thread in random.sample(_WD_THREADS, len(_WD_THREADS)):
        if count >= target - 2:
            break
        a, b = random.sample(range(n), 2)
        others = [i for i in range(n) if i not in (a, b)]
        for i, variants in enumerate(thread):
            speaker = a if i % 2 == 0 else b
            # A reply-quote now and then, as people do in groups.
            lines.append(("msg", speaker, random.choice(variants), i > 0 and random.random() < 0.25))
            count += 1
            if others and random.random() < 0.2:
                third = random.choice(others)
                if random.random() < 0.5:
                    lines.append(("react", third))
                else:
                    t = maybe_sticker(random.choice(_WD_FILLERS))
                    lines.append(("sticker", third, t) if isinstance(t, StickerNote) else ("msg", third, t, False))
                    count += 1
        if voice_slots < 3 and random.random() < 0.2:
            # A voice note on the thread's topic, answered in text by someone else.
            sp = random.choice((a, b))
            lines.append(("voice", sp, topic_of(thread)))
            lines.append(("vreply", random.choice([i for i in range(n) if i != sp])))
            voice_slots += 1
        if not forwarded and random.random() < 0.025:  # ~1 talk in 3
            sp = random.randrange(n)
            lines.append(("forward", sp))
            lines.append(("freply", random.choice([i for i in range(n) if i != sp])))
            forwarded = True
    closer = random.randrange(n)
    lines.append(("msg", closer, random.choice(_GROUP_CLOSERS), False))
    for sp in random.sample([i for i in range(n) if i != closer], k=min(n - 1, random.randint(1, 2))):
        lines.append(("msg", sp, random.choice(_GROUP_BYES), False))
    return lines


class WarmupGroups:
    def __init__(self, service):
        self.svc = service
        self.db = service.db
        self.um = service.userbot_manager
        self._ready = False
        self._busy: set[int] = set()       # group ids talking now
        self._joining: set[int] = set()    # warming accounts a group is being created for
        self._created: dict[int, list] = {}  # creator account_id -> creation times (unix)
        self._tasks: set = set()
        self._no_create: set[int] = set()  # accounts Telegram won't let create chats
        self._retry_at: dict[int, float] = {}  # warming account -> no new creation attempt before
        self._cleaned = False

    # === state that survives restarts (see bot/state_store.py) ===

    async def save_state(self):
        from .state_store import save_state
        now = time.time()
        await save_state(self.db, "warmup.groups", {
            "no_create": sorted(self._no_create),
            "retry_at": {str(a): t for a, t in self._retry_at.items() if t > now},
            "created": {str(a): [t for t in ts if now - t < 86400] for a, ts in self._created.items() if ts},
        })

    async def restore_state(self):
        from .state_store import load_state
        st = await load_state(self.db, "warmup.groups", {}) or {}
        self._no_create.update(st.get("no_create", []))
        self._retry_at.update({int(a): t for a, t in (st.get("retry_at") or {}).items()})
        for a, ts in (st.get("created") or {}).items():
            self._created[int(a)] = list(ts)

    # === storage ===

    async def ensure(self):
        if self._ready:
            return
        if not self.db.is_pg:       # PostgreSQL: the tables come with pg_schema.sql
            async with self.db._transaction_lock:
                for stmt in _SCHEMA:
                    await self.db._conn.execute(stmt)
                await self.db._conn.commit()
        async with self.db._conn.execute("SELECT chat_id FROM warmup_groups WHERE is_active = 1") as cur:
            chats = {-row[0] for row in await cur.fetchall()}
        self.um._warmup_chats |= chats
        self._ready = True

    async def _fetch(self, sql: str, args=()) -> list:
        async with self.db._conn.execute(sql, args) as cur:
            return await cur.fetchall()

    async def _write(self, sql: str, args=()):
        async with self.db._transaction_lock:
            cur = await self.db._conn.execute(sql, args)
            await self.db._conn.commit()
            return cur.lastrowid

    async def _groups_of(self, account_id: int) -> list:
        return await self._fetch(
            "SELECT g.id, g.chat_id, g.owner_user_id, g.next_talk_ts, g.title FROM warmup_groups g "
            "JOIN warmup_group_members m ON m.group_id = g.id WHERE m.account_id = ? AND g.is_active = 1",
            (account_id,),
        )

    async def _members(self, group_id: int) -> list[int]:
        return [r[0] for r in await self._fetch(
            ("SELECT account_id FROM warmup_group_members WHERE group_id = ? ORDER BY joined_ts, account_id" if self.db.is_pg else "SELECT account_id FROM warmup_group_members WHERE group_id = ? ORDER BY joined_ts"), (group_id,))]

    async def _pool_ids(self) -> list[int]:
        return [aid for aid, _ in await self.db.get_warmup_partners()]

    # === entry point from WarmupService stage 2 ===

    async def act(self, account, client) -> bool:
        """Start a talk in one of this account's groups that's due, or create
        a new group for it. Runs in the background; True if something started."""
        if not (_GROUP_HOURS[0] <= now_moscow().hour < _GROUP_HOURS[1]):
            return False
        try:
            await self.ensure()
            if not self._cleaned:
                self._cleaned = True  # once per run, clients are connected by now
                self._spawn(self.cleanup_clientless())
                self._spawn(self.rehide_all())
            if account.id in set(await self._pool_ids()) or self.svc.is_frozen(account.id):
                return False  # groups are for clients' (working) accounts; pool accounts are their company
            now = time.time()
            groups = await self._groups_of(account.id)
            due = [g for g in groups if g[3] <= now and g[0] not in self._busy]
            if due:
                g = random.choice(due)
                self._busy.add(g[0])
                self._spawn(self._talk(g[0], g[1], g[2]))
                return True
            if len(groups) >= _GROUPS_PER_WARMING_ACCOUNT or account.id in self._joining:
                return False
            if self._retry_at.get(account.id, 0) > now:
                return False
            joined = await self._fetch(
                "SELECT MAX(joined_ts) FROM warmup_group_members WHERE account_id = ?", (account.id,))
            if joined and joined[0][0] and now - joined[0][0] < _GROUP_JOIN_GAP_H * 3600:
                return False
            self._joining.add(account.id)
            self._spawn(self._create(account, client))
            return False  # creation runs in the background; the regular action still happens
        except Exception as e:
            logger.warning(f"Warmup groups: act failed for account {account.id}: {type(e).__name__}: {e}")
            return False

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _can_create(self, account_id: int) -> bool:
        if account_id in self._no_create:
            return False
        now = time.time()
        times = [t for t in self._created.get(account_id, []) if now - t < 86400]
        self._created[account_id] = times
        return len(times) < _CREATOR_PER_DAY

    # === creation ===

    async def _create(self, account, client):
        ok = False
        retry_h = 6
        try:
            ok = await self._create_inner(account, client)
        except (FloodWaitError, PeerFloodError) as e:
            logger.warning(f"Warmup groups: create stopped for account {account.id}: {type(e).__name__}")
        except Exception as e:
            self.svc.note_error(account.id, e)
            logger.warning(f"Warmup groups: create failed for account {account.id}: {type(e).__name__}: {e}")
            if type(e).__name__ == "UserRestrictedError":
                retry_h = 0.25  # only that creator is barred (now in _no_create) — another one can try soon
        finally:
            self._joining.discard(account.id)
            if not ok:
                self._retry_at[account.id] = time.time() + retry_h * 3600

    async def _create_inner(self, account, client):
        from telethon.tl.functions.contacts import ImportContactsRequest
        from telethon.tl.functions.messages import CreateChatRequest, EditChatPhotoRequest
        from telethon.tl.types import InputChatUploadedPhoto, InputPeerChat, InputPhoneContact

        pool = [aid for aid in await self._pool_ids() if aid != account.id]
        if len(pool) < 1:
            return
        # The same client's other warming accounts may join too (never another client's).
        siblings = []
        for sib in await self.db.get_user_accounts(account.user_id):
            if (sib.id == account.id or not sib.is_active or sib.id in pool or sib.id in self._joining
                    or self.svc.is_frozen(sib.id)):
                continue
            w = await self.db.get_warmup(sib.id)
            if w and w.enabled and len(await self._groups_of(sib.id)) < _GROUPS_PER_WARMING_ACCOUNT:
                siblings.append(sib)
        size = random.randint(*_GROUP_SIZE)
        clients_part = [account] + random.sample(siblings, k=min(len(siblings), random.choice([0, 0, 1])))
        # Prefer pool accounts this account shares the fewest groups with.
        shared = {}
        for row in await self._fetch(
            "SELECT m2.account_id, COUNT(*) FROM warmup_group_members m1 JOIN warmup_group_members m2 "
            "ON m1.group_id = m2.group_id WHERE m1.account_id = ? GROUP BY m2.account_id", (account.id,)):
            shared[row[0]] = row[1]
        random.shuffle(pool)
        pool.sort(key=lambda aid: shared.get(aid, 0))
        pool_part = []
        for aid in pool:
            if len(clients_part) + len(pool_part) >= size:
                break
            if self.svc.is_frozen(aid):
                continue
            acc = await self.db.get_account(aid)
            cl = await self.um.get_client(aid) if acc and acc.is_active else None
            if cl:
                pool_part.append((acc, cl))
        members = [(a, (client if a.id == account.id else await self.um.get_client(a.id))) for a in clients_part]
        members = [(a, c) for a, c in members if c] + pool_part
        if len(members) < _GROUP_SIZE[0] or not pool_part:
            return
        for sib, _ in members:
            if sib.id != account.id and sib.user_id == account.user_id:
                self._joining.add(sib.id)

        try:
            # Creator: a pool account whenever one can — clients' warming
            # accounts are often spam-limited ("can't create chats"); the
            # warming account itself only when no pool account can.
            pool_creators = [m for m in pool_part if self._can_create(m[0].id)]
            if pool_creators:
                creator = random.choice(pool_creators)
            elif self._can_create(account.id):
                creator = members[0]
            else:
                return
            c_acc, c_cl = creator

            async def _import(cl, other, other_cl, viewer_id):
                """InputPeerUser for `other` as seen by `cl`, made a contact:
                from cl's cache (they've chatted before), by username, by
                phone — spam-limited accounts often can't be found by phone."""
                from telethon.tl.functions.contacts import AddContactRequest
                peer = None
                tg_id = self.um._me_ids.get(other.id)
                if tg_id:
                    try:
                        peer = await cl.get_input_entity(tg_id)  # session cache only, no request
                    except Exception:
                        peer = None
                if peer is None:
                    try:
                        me = await other_cl.get_me()
                        if getattr(me, "username", None):
                            peer = await cl.get_input_entity("@" + me.username)
                    except (FloodWaitError, PeerFloodError):
                        raise
                    except Exception:
                        peer = None
                if peer is not None:
                    try:
                        await cl(AddContactRequest(id=peer, first_name=_contact_name(viewer_id, other.id),
                                                   last_name="", phone=""))
                    except (FloodWaitError, PeerFloodError):
                        raise
                    except Exception:
                        pass
                    return peer
                res = await cl(ImportContactsRequest([InputPhoneContact(
                    client_id=random.randint(1, 2**31), phone=other.phone,
                    first_name=_contact_name(viewer_id, other.id), last_name="")]))
                user = pick_imported_user(res, self.um._me_ids.get(other.id), other.phone)
                if user is None:
                    logger.warning(f"Warmup groups: contact import of {other.id} on {viewer_id} gave no matching user: "
                                   f"{[(u.id, getattr(u, 'bot', None)) for u in res.users]}")
                return await cl.get_input_entity(user) if user else None

            invite = []
            added = [creator]
            by_link = []  # members that couldn't be added directly — they join by an invite link
            for m_acc, m_cl in members:
                if m_acc.id == c_acc.id:
                    continue
                user = await _import(c_cl, m_acc, m_cl, c_acc.id)
                try:
                    await _import(m_cl, c_acc, c_cl, m_acc.id)
                except (FloodWaitError, PeerFloodError):
                    raise
                except Exception:
                    pass
                if user:
                    invite.append(user)
                    added.append((m_acc, m_cl))
                else:
                    by_link.append((m_acc, m_cl))
                await asyncio.sleep(random.uniform(2, 6))
            # CreateChat needs at least one invitee; the rest may come by link.
            if not invite or len(added) + len(by_link) < _GROUP_SIZE[0]:
                logger.info(f"Warmup groups: not creating for account {account.id} — too few members "
                            f"({[m[0].id for m in added]}, by link {[m[0].id for m in by_link]})")
                return
            title = random.choice(_TITLES)
            try:
                res = await c_cl(CreateChatRequest(users=invite, title=title))
            except Exception as e:
                if type(e).__name__ == "UserRestrictedError":
                    self._no_create.add(c_acc.id)  # spamreported: can't create chats
                raise
            updates = getattr(res, "updates", res)
            chats = getattr(updates, "chats", None) or []
            if not chats:
                logger.warning(f"Warmup groups: CreateChat returned no chat for creator {c_acc.id}")
                return
            chat_id = chats[0].id
            self._created.setdefault(c_acc.id, []).append(time.time())
            missing = {getattr(mi, "user_id", None) for mi in (getattr(res, "missing_invitees", None) or [])}
            if missing:
                me_ids = self.um._me_ids
                by_link += [m for m in added if me_ids.get(m[0].id) in missing]
                added = [m for m in added if me_ids.get(m[0].id) not in missing]
            if by_link:
                added += await self._join_by_link(c_cl, chat_id, by_link)
            if not any(m[0].id == account.id for m in added) or len(added) < _GROUP_SIZE[0]:
                # Telegram didn't let the client's account in — no point in the group.
                await self._delete_chat(c_cl, chat_id)
                logger.info(f"Warmup groups: account {account.id} wasn't added to the new chat, removed it")
                return
            self.um._warmup_chats.add(-chat_id)
            now = time.time()
            group_id = await self._write(
                "INSERT INTO warmup_groups (chat_id, owner_user_id, creator_account_id, title, next_talk_ts, created_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chat_id, account.user_id, c_acc.id, title, now + random.uniform(600, 3600), now),
            )
            for m_acc, _ in added:
                await self._write(
                    "INSERT INTO warmup_group_members (group_id, account_id, joined_ts) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                    (group_id, m_acc.id, now),
                )
            logger.info(f"Warmup groups: created group {group_id} (chat {chat_id}, '{title}') by {c_acc.id} "
                        f"with {[m[0].id for m in added]}")
            peer = InputPeerChat(chat_id)
            try:
                await asyncio.sleep(random.uniform(5, 20))
                photo = await c_cl.upload_file(await asyncio.to_thread(group_avatar), file_name="photo.jpg")
                await c_cl(EditChatPhotoRequest(chat_id=chat_id, photo=InputChatUploadedPhoto(file=photo)))
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                logger.warning(f"Warmup groups: avatar failed for group {group_id}: {e}")
            await self._hide(added, peer)
            if random.random() < 0.6:
                await asyncio.sleep(random.uniform(10, 60))
                await c_cl.send_message(peer, random.choice(_GROUP_FIRST))
            return True
        finally:
            for m_acc, _ in members:
                if m_acc.id != account.id:
                    self._joining.discard(m_acc.id)

    async def _join_by_link(self, c_cl, chat_id: int, joiners: list) -> list:
        """Creator makes a one-off invite link, the joiners use it, then the
        link is revoked. Returns the members that got in."""
        from telethon.errors import UserAlreadyParticipantError
        from telethon.tl.functions.messages import (EditExportedChatInviteRequest, ExportChatInviteRequest,
                                                    ImportChatInviteRequest)
        from telethon.tl.types import InputPeerChat
        joined = []
        peer = InputPeerChat(chat_id)
        try:
            inv = await c_cl(ExportChatInviteRequest(peer=peer, usage_limit=len(joiners)))
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            logger.warning(f"Warmup groups: invite link for chat {chat_id} failed: {e}")
            return joined
        link = inv.link
        slug = link.rstrip("/").rsplit("/", 1)[-1].lstrip("+")
        for m_acc, m_cl in joiners:
            await asyncio.sleep(random.uniform(5, 20))
            try:
                await m_cl(ImportChatInviteRequest(slug))
                joined.append((m_acc, m_cl))
            except UserAlreadyParticipantError:
                joined.append((m_acc, m_cl))
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                logger.warning(f"Warmup groups: {m_acc.id} couldn't join chat {chat_id} by link: {e}")
        try:
            await c_cl(EditExportedChatInviteRequest(peer=peer, link=link, revoked=True))
        except Exception as e:
            logger.debug(f"Warmup groups: revoking invite for chat {chat_id} failed: {e}")
        if joined:
            logger.info(f"Warmup groups: joined chat {chat_id} by link: {[m[0].id for m in joined]}")
        return joined

    async def _hide(self, members, peer):
        """Muted + archived for every member (stays archived on new messages)."""
        from telethon.tl.functions.account import UpdateNotifySettingsRequest
        from telethon.tl.types import InputNotifyPeer, InputPeerNotifySettings
        for m_acc, cl in members:
            try:
                await cl(UpdateNotifySettingsRequest(
                    peer=InputNotifyPeer(peer), settings=InputPeerNotifySettings(mute_until=2**31 - 1)))
                await cl.edit_folder(peer, 1)
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                self.svc.note_error(m_acc.id, e)
                logger.warning(f"Warmup groups: mute/archive failed for {m_acc.id}: {e}")

    async def _not_hidden(self, members, peer) -> list:
        """Members whose copy of the group is still outside the archive (or
        unmuted), as Telegram reports it: (account_id, folder_id, muted)."""
        from telethon.tl.functions.messages import GetPeerDialogsRequest
        from telethon.tl.types import InputDialogPeer
        bad = []
        for m_acc, cl in members:
            try:
                res = await cl(GetPeerDialogsRequest(peers=[InputDialogPeer(peer)]))
                d = res.dialogs[0] if res.dialogs else None
                if d is None:
                    continue
                mute = getattr(d.notify_settings, "mute_until", None)
                muted = bool(mute) and (not hasattr(mute, "timestamp") or mute.timestamp() > time.time())
                if (d.folder_id or 0) != 1 or not muted:
                    bad.append((m_acc.id, d.folder_id, muted))
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                logger.debug(f"Warmup groups: hidden check failed for {m_acc.id}: {e}")
        return bad

    # === talks ===

    async def _leave(self, group_id: int, chat_id: int, acc_id: int, cl):
        from telethon.tl.functions.messages import DeleteChatUserRequest
        from telethon.tl.types import InputUserSelf
        try:
            if cl:
                await cl(DeleteChatUserRequest(chat_id=chat_id, user_id=InputUserSelf()))
        except Exception as e:
            logger.debug(f"Warmup groups: leave failed for {acc_id}: {e}")
        await self._write("DELETE FROM warmup_group_members WHERE group_id = ? AND account_id = ?", (group_id, acc_id))

    async def _delete_chat(self, cl, chat_id: int) -> bool:
        from telethon.tl.functions.messages import DeleteChatRequest
        try:
            await cl(DeleteChatRequest(chat_id=chat_id))  # creator only
            return True
        except Exception as e:
            logger.debug(f"Warmup groups: delete chat {chat_id} failed: {e}")
            return False

    async def _dissolve(self, group_id: int, chat_id: int, members: list):
        """Delete the chat (via its creator) or have everyone leave it."""
        creator = await self._fetch("SELECT creator_account_id FROM warmup_groups WHERE id = ?", (group_id,))
        creator_id = creator[0][0] if creator else None
        done = False
        for acc, cl in members:
            if acc.id == creator_id and cl:
                done = await self._delete_chat(cl, chat_id)
        if not done:
            for acc, cl in members:
                await self._leave(group_id, chat_id, acc.id, cl)
        await self._write("DELETE FROM warmup_group_members WHERE group_id = ?", (group_id,))
        await self._deactivate(group_id, chat_id)

    async def rehide_all(self, loop_every: float = 1800):
        """At start and then every 30 min: who has a group outside the
        archive (or unmuted) is logged FIRST — that's the evidence of what
        brought it out — then the group is muted + archived again for them."""
        while True:
            await self._rehide_pass()
            if not loop_every:
                return
            await asyncio.sleep(loop_every)

    async def _rehide_pass(self):
        from telethon.tl.types import InputPeerChat
        try:
            await self.ensure()
            n = still = out = 0
            for group_id, chat_id in await self._fetch("SELECT id, chat_id FROM warmup_groups WHERE is_active = 1"):
                members = []
                for aid in await self._members(group_id):
                    acc = await self.db.get_account(aid)
                    if acc and acc.is_active and not self.svc.is_frozen(aid):
                        cl = await self.um.get_client(aid)
                        if cl:
                            members.append((acc, cl))
                before = await self._not_hidden(members, InputPeerChat(chat_id))
                if before:
                    out += 1
                    logger.warning(f"Warmup groups: group {group_id} was out of the archive/unmuted for "
                                   f"(account, folder, muted): {before}")
                await self._hide(members, InputPeerChat(chat_id))
                bad = await self._not_hidden(members, InputPeerChat(chat_id)) if before else []
                if bad:
                    logger.warning(f"Warmup groups: group {group_id} still not archived/muted after hiding: {bad}")
                    still += 1
                n += 1
                await asyncio.sleep(random.uniform(2, 6))
            logger.info(f"Warmup groups: checked {n} group(s): {out} were out of the archive for someone, "
                        f"{still} still not hidden after re-hiding")
        except (FloodWaitError, PeerFloodError) as e:
            logger.warning(f"Warmup groups: re-hide stopped: {type(e).__name__}")
        except Exception as e:
            logger.warning(f"Warmup groups: re-hide failed: {type(e).__name__}: {e}")

    async def cleanup_clientless(self):
        """Remove active groups that have none of their owner's (non-pool)
        warming accounts in them."""
        try:
            await self.ensure()
            pool = set(await self._pool_ids())
            for group_id, chat_id, owner in await self._fetch(
                    "SELECT id, chat_id, owner_user_id FROM warmup_groups WHERE is_active = 1"):
                ids = await self._members(group_id)
                accs = [a for a in [await self.db.get_account(i) for i in ids] if a]
                if any(a.user_id == owner and a.id not in pool for a in accs):
                    continue
                members = [(a, await self.um.get_client(a.id) if a.is_active else None) for a in accs]
                await self._dissolve(group_id, chat_id, members)
                logger.info(f"Warmup groups: removed group {group_id} (no client account in it)")
        except Exception as e:
            logger.warning(f"Warmup groups: cleanup failed: {type(e).__name__}: {e}")

    async def _deactivate(self, group_id: int, chat_id: int):
        await self._write("UPDATE warmup_groups SET is_active = 0 WHERE id = ?", (group_id,))
        self.um._warmup_chats.discard(-chat_id)

    async def _talk(self, group_id: int, chat_id: int, owner_user_id: int):
        from telethon.tl.functions.messages import SetTypingRequest
        from telethon.tl.types import InputPeerChat, SendMessageTypingAction

        peer = InputPeerChat(chat_id)
        sent = 0
        cur_acc = None
        try:
            pool = set(await self._pool_ids())
            speakers = []
            warming_left = 0
            for aid in await self._members(group_id):
                acc = await self.db.get_account(aid)
                if not acc:
                    await self._write("DELETE FROM warmup_group_members WHERE group_id = ? AND account_id = ?",
                                      (group_id, aid))
                    continue
                cl = await self.um.get_client(aid) if acc.is_active else None
                if acc.user_id == owner_user_id:
                    w = await self.db.get_warmup(aid)
                    if acc.is_active and w and w.enabled and aid not in pool:
                        warming_left += 1
                    elif aid not in pool:
                        await self._leave(group_id, chat_id, aid, cl)  # warmup off / account gone
                        continue
                elif aid not in pool:
                    await self._leave(group_id, chat_id, aid, cl)  # left the pool, not this client's
                    continue
                if cl and not self.svc.is_frozen(aid):
                    speakers.append((acc, cl))
            if warming_left == 0:
                await self._dissolve(group_id, chat_id, speakers)
                logger.info(f"Warmup groups: group {group_id} has no client warming accounts left, removed")
                return
            if len(speakers) < 2 or not any(a.user_id == owner_user_id and a.id not in pool for a, _ in speakers):
                return  # members offline/frozen for now — pool accounts don't talk among themselves
            # Re-hide before every talk: the one-time mute+archive at creation
            # could be skipped (restart / FloodWait mid-creation, or a member who
            # unarchived it), leaving the group loud and in the main chat list.
            await self._hide(speakers, peer)
            random.shuffle(speakers)
            dead: set[int] = set()          # speaker indexes no longer able to post here
            voice_ok = False
            fwd_ok = False
            last_ids: dict[int, int] = {}   # speaker index -> id of their last message (sender's numbering)
            prev = None
            guard = ChatGuard()
            for i, (_a, _c) in enumerate(speakers):
                await guard.intruded(i, _c, peer)  # baseline before the talk

            async def _intruded(*idxs) -> bool:
                # The owner typed in the group from one of the accounts: stop.
                for i in dict.fromkeys(x for x in idxs if x is not None and x not in dead):
                    if await guard.intruded(i, speakers[i][1], peer, limit=15):
                        logger.info(f"Warmup groups: group {group_id}: a person wrote from {speakers[i][0].id}, talk stopped")
                        return True
                return False

            for item in group_script(len(speakers)):
                if now_moscow().hour < 6:
                    # Midnight: someone says goodbye, a couple answer, the chat goes quiet.
                    alive = [i for i in range(len(speakers)) if i not in dead]
                    if sent and alive:
                        first = random.choice(alive)
                        for i, txt in [(first, random.choice(_GROUP_CLOSERS))] + [
                                (j, random.choice(_GROUP_BYES)) for j in random.sample(
                                    [k for k in alive if k != first], k=min(2, len(alive) - 1))]:
                            cur_acc = speakers[i][0]
                            await asyncio.sleep(random.uniform(2, 8))
                            guard.mark(i, await send_text(speakers[i][1], peer, txt))
                            sent += 1
                    break
                idx = item[1]
                if idx in dead:
                    continue
                acc, cl = speakers[idx]
                cur_acc = acc
                if sent and await _intruded(idx, prev):
                    break
                if item[0] == "voice":
                    if prev is not None and prev != idx:
                        try:
                            await cl.send_read_acknowledge(peer)
                        except Exception:
                            pass
                    msg = await self.svc.voices.send(cl, peer, acc.id, item[2])
                    voice_ok = msg is not None
                    if voice_ok:
                        guard.mark(idx, msg)
                        last_ids[idx] = msg.id
                        sent += 1
                        prev = idx
                        await asyncio.sleep(random.uniform(*_GROUP_MSG_GAP))
                    continue
                if item[0] == "vreply":
                    if not voice_ok:
                        continue
                    voice_ok = False
                    item = ("msg", idx, random.choice(VOICE_REPLIES), random.random() < 0.5)
                if item[0] == "freply":
                    if not fwd_ok:
                        continue
                    fwd_ok = False
                    item = ("msg", idx, random.choice(FORWARD_REPLIES), False)
                if item[0] == "forward":
                    fmsg = await forward_post(cl, peer)
                    fwd_ok = fmsg is not None
                    if fwd_ok:
                        guard.mark(idx, fmsg)
                        last_ids[idx] = fmsg[0].id if isinstance(fmsg, list) else fmsg.id
                        sent += 1
                        prev = idx
                        await asyncio.sleep(random.uniform(3, 12))
                        cm = await send_text(cl, peer, random.choice(FORWARD_COMMENTS))
                        guard.mark(idx, cm)
                        last_ids[idx] = cm.id
                        sent += 1
                        await asyncio.sleep(random.uniform(*_GROUP_MSG_GAP))
                    continue
                if item[0] == "sticker":
                    await asyncio.sleep(random.uniform(2, 5))
                    sm = await send_sticker(cl, peer, acc.id, item[2])
                    if sm is not None:
                        guard.mark(idx, sm)
                        last_ids[idx] = getattr(sm, "id", last_ids.get(idx))
                        sent += 1
                        prev = idx
                        await asyncio.sleep(random.uniform(*_GROUP_MSG_GAP))
                        continue
                    item = ("msg", idx, item[2].fallback, False)
                if item[0] == "react":
                    # Basic groups number messages per member: find the target
                    # in this member's own history, by its author.
                    other = prev if prev is not None and prev != idx else next((sp for sp in last_ids if sp != idx), None)
                    if other is not None:
                        if await _intruded(idx, other):
                            break
                        target = await last_incoming_id(cl, peer, self.um._me_ids.get(speakers[other][0].id))
                        if target:
                            await react(cl, peer, target)
                            await asyncio.sleep(random.uniform(3, 15))
                    continue
                _, _, text, quote = item
                if prev is not None and prev != idx:
                    try:
                        await cl.send_read_acknowledge(peer)
                    except Exception:
                        pass
                try:
                    await cl(SetTypingRequest(peer, SendMessageTypingAction()))
                except (FloodWaitError, PeerFloodError):
                    raise
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(1.5, 2.5) + len(text) * random.uniform(0.08, 0.2))
                reply_to = None
                if quote and prev is not None and prev != idx:
                    if await _intruded(idx, prev):
                        break
                    reply_to = await last_incoming_id(cl, peer, self.um._me_ids.get(speakers[prev][0].id))
                try:
                    msg = await send_text(cl, peer, text, reply_to=reply_to)
                except (FloodWaitError, PeerFloodError):
                    raise
                except Exception as e:
                    if self.svc.note_error(acc.id, e) or isinstance(e, (ConnectionError, asyncio.TimeoutError)):
                        dead.add(idx)  # frozen / connection dropped: sits this talk out, stays a member
                        if len(speakers) - len(dead) < 2:
                            break
                        continue
                    # Kicked / left / can't write: drop just this member.
                    logger.warning(f"Warmup groups: {acc.id} can't post in group {group_id}: {type(e).__name__}: {e}")
                    dead.add(idx)
                    await self._write("DELETE FROM warmup_group_members WHERE group_id = ? AND account_id = ?",
                                      (group_id, acc.id))
                    if len(speakers) - len(dead) < 2:
                        break
                    continue
                guard.mark(idx, msg)
                last_ids[idx] = msg.id
                sent += 1
                prev = idx
                await asyncio.sleep(random.uniform(*_GROUP_MSG_GAP))
            if len(speakers) - len(dead) < 2 and len(await self._members(group_id)) < 2:
                # Nobody left to talk to: the last member leaves, the warming
                # account can get a new group.
                for i, (a, c) in enumerate(speakers):
                    if i not in dead:
                        await self._leave(group_id, chat_id, a.id, c)
                await self._deactivate(group_id, chat_id)
            logger.info(f"Warmup groups: group {group_id} talk, {len(speakers)} speakers, {sent} messages")
            # Checked after every talk, as Telegram sees it: whoever has the
            # group back in the main list (or unmuted) gets it hidden again.
            alive = [sp for i, sp in enumerate(speakers) if i not in dead]
            bad = await self._not_hidden(alive, peer)
            if bad:
                logger.warning(f"Warmup groups: group {group_id} came out of the archive after a talk for {bad} — hiding again")
                await self._hide([sp for sp in alive if sp[0].id in {b[0] for b in bad}], peer)
        except (FloodWaitError, PeerFloodError) as e:
            wait_s = getattr(e, "seconds", None) or config.PEER_FLOOD_COOLDOWN_SECONDS
            logger.warning(f"Warmup groups: group {group_id} talk stopped: {type(e).__name__} on {getattr(cur_acc, 'id', None)}")
            if cur_acc is not None:
                try:
                    await self.db.set_flood_wait_until(cur_acc.id, now_moscow() + timedelta(seconds=min(wait_s, 21600)))
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Warmup groups: group {group_id} talk stopped: {type(e).__name__}: {e}")
        finally:
            self._busy.discard(group_id)
            try:
                await self._write("UPDATE warmup_groups SET next_talk_ts = ? WHERE id = ?",
                                  (time.time() + random.uniform(*_GROUP_TALK_GAP_H) * 3600, group_id))
            except Exception:
                pass
