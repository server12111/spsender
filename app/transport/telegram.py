"""Real Telegram operations. Constructed only in explicitly configured live mode."""

import asyncio
import hashlib
import json
import logging
import random
import re
from functools import wraps
from pathlib import Path
from weakref import WeakValueDictionary

from telethon import TelegramClient, errors, events, functions, types, utils

from app.paths import media_dir
from app.services.chat_pool import parse_target
from app.transport.fingerprint import LEGACY_DEVICE, parse_profile, random_profile

from .base import OperationError, Result
from .session import BoundedStringSession

# Detect captcha / "prove you are human" prompts (RU + EN). Compiled once at import so a
# broken pattern fails loudly here instead of raising re.error on every incoming message.
CAPTCHA_GATE = re.compile(
    r"captcha|капч|я\s+не\s+робот|not\s+a\s+robot|verify.*human|"
    r"подтверд.*(?:челов|не\s*бот)|нажм.*кнопк|пройд.*(?:проверк|капч)",
    re.IGNORECASE,
)


def classify_error(exc):
    name = type(exc).__name__
    if isinstance(exc, errors.FloodWaitError):
        return OperationError("flood", "FLOOD_WAIT", max(1, exc.seconds))
    if name in {
        "AuthKeyUnregisteredError",
        "SessionRevokedError",
        "SessionExpiredError",
        "UnauthorizedError",
        "AuthKeyDuplicatedError",
    }:
        return OperationError("critical", "SESSION_EXPIRED")
    if name in {"UserDeactivatedError", "UserDeactivatedBanError", "FrozenMethodInvalidError"}:
        return OperationError("critical", "FROZEN")
    if name in {"PeerFloodError", "UserRestrictedError"}:
        return OperationError("critical", "RESTRICTED")
    if name in {
        "ChatWriteForbiddenError",
        "ChatAdminRequiredError",
        "UserBannedInChannelError",
        "ChatSendPlainForbiddenError",
        "ChatSendMediaForbiddenError",
        "ChannelPrivateError",
    }:
        return OperationError("forbidden", "PUBLICATION_FORBIDDEN")
    if name in {"InviteRequestSentError", "JoinRequestSentError"}:
        return OperationError("manual", "MODERATOR_APPROVAL_REQUIRED")
    if name == "UserNotParticipantError":
        return OperationError("manual", "MEMBERSHIP_REQUIRED")
    if name in {
        "UsernameInvalidError",
        "UsernameNotOccupiedError",
        "ChannelInvalidError",
        "InviteHashExpiredError",
        "InviteHashInvalidError",
    }:
        return OperationError("chat", "CHAT_UNAVAILABLE")
    if name in {"SlowModeWaitError"}:
        return OperationError("flood", "SLOW_MODE_WAIT", max(1, exc.seconds))
    # Timeout/connection loss may happen AFTER Telegram accepted the request.
    return OperationError("unknown", name)


def can_publish(entity, permissions, media_kind=None):
    """Telethon ParticipantPermissions exposes participant rights, not send_messages."""
    if permissions is None or getattr(permissions, "has_left", False):
        return False
    if getattr(entity, "broadcast", False):
        return bool(permissions.is_creator or permissions.post_messages)
    if permissions.is_admin:
        return True
    rights = [getattr(entity, "default_banned_rights", None), getattr(permissions.participant, "banned_rights", None)]
    flags = ["view_messages", "send_messages"]
    if media_kind:
        flags += [
            "send_media",
            {"photo": "send_photos", "video": "send_videos", "document": "send_docs"}.get(media_kind, "send_docs"),
        ]
    else:
        flags.append("send_plain")
    return not any(getattr(right, flag, False) for right in rights if right for flag in flags)


class TelegramTransport:
    def __init__(self, engine):
        if engine.config.mode != "live":
            raise RuntimeError("Живой адаптер запрещён в dry-run")
        self.engine = engine
        self.clients = {}
        self._client_sessions = {}
        self._client_locks = WeakValueDictionary()
        self._incoming = asyncio.Queue(maxsize=engine.config.incoming_queue_size)
        self._incoming_workers = []
        self._incoming_dropped = 0
        self._sync_cursor = 0

    def new_client(self, session="", updates=True, device=None):
        device = device or LEGACY_DEVICE
        return TelegramClient(
            BoundedStringSession(session, entity_limit=self.engine.config.entity_cache_limit),
            self.engine.config.api_id,
            self.engine.config.api_hash,
            receive_updates=updates,
            flood_sleep_threshold=0,
            request_retries=0,
            connection_retries=1,
            auto_reconnect=False,
            timeout=30,
            device_model=device["device_model"],
            app_version=device["app_version"],
            system_version=device["system_version"],
            entity_cache_limit=self.engine.config.entity_cache_limit,
        )

    async def client(self, account_id):
        lock = self._client_locks.setdefault(account_id, asyncio.Lock())
        async with lock:
            account = await self.engine.db.run(
                self.engine.db.one,
                "SELECT a.*,s.encrypted_session FROM accounts a LEFT JOIN account_sessions s ON s.account_id=a.id WHERE a.id=?",
                (account_id,),
            )
            if not account or account["account_health"] == "REMOVED":
                raise OperationError("cancelled", "ACCOUNT_REMOVED")
            if self.engine.clock() - account["onboarding_started_at"] < 86400:
                raise OperationError("cancelled", "SILENT_PERIOD_GUARD")
            if account["flood_until"] > self.engine.clock() or account["connection_retry_at"] > self.engine.clock():
                raise OperationError("transient", "CONNECTION_BACKOFF")
            existing = self.clients.get(account_id)
            if (
                existing
                and self._client_sessions.get(account_id) == account["encrypted_session"]
                and existing.is_connected()
            ):
                return existing
            if existing:
                self.clients.pop(account_id, None)
                self._client_sessions.pop(account_id, None)
                await asyncio.wait_for(existing.disconnect(), 10)
            if not account["encrypted_session"]:
                raise OperationError("critical", "SESSION_EXPIRED")
            client = None
            try:
                session = self.engine.secrets.decrypt(account["encrypted_session"])
                client = self.new_client(session, device=parse_profile(account["device_profile"]))
                await asyncio.wait_for(client.connect(), 30)
                if not await asyncio.wait_for(client.is_user_authorized(), 30):
                    raise OperationError("critical", "SESSION_EXPIRED")
            except BaseException as exc:
                if client:
                    try:
                        await asyncio.wait_for(client.disconnect(), 5)
                    except Exception:
                        pass
                if isinstance(exc, asyncio.CancelledError):
                    raise
                if isinstance(exc, OperationError):
                    raise
                from cryptography.fernet import InvalidToken

                if isinstance(exc, (InvalidToken, ValueError)):
                    raise OperationError("critical", "SESSION_EXPIRED") from None
                await self.engine.db.run(self.engine.connection_failed, account_id)
                classified = classify_error(exc)
                if classified.category == "unknown":
                    classified = OperationError("transient", type(exc).__name__)
                raise classified from None
            self.clients[account_id] = client
            self._client_sessions[account_id] = account["encrypted_session"]
            self._start_incoming()

            @client.on(events.NewMessage(incoming=True))
            async def on_message(event):
                try:
                    self._incoming.put_nowait((account_id, event))
                except asyncio.QueueFull:
                    self._incoming_dropped += 1
                    # Bound memory under input overload. Consumer reports the dropped
                    # count through diagnostics; it never starts unchecked replies.
                    if self._incoming_dropped == 1:
                        logging.getLogger("fe_autosender").error("INCOMING_OVERLOAD account_id=%s", account_id)

            return client

    def _start_incoming(self):
        if self._incoming_workers:
            return

        async def consume():
            while True:
                account_id, event = await self._incoming.get()
                try:
                    if self._incoming_dropped:
                        dropped = self._incoming_dropped
                        self._incoming_dropped = 0
                        await self.engine.db.run(self.engine.diagnostic, "incoming", "QUEUE_OVERLOAD", dropped=dropped)
                        await self.engine.db.run(self.engine.incoming_overload, dropped)
                    await self._handle_incoming(account_id, event)
                except Exception as exc:
                    await self.engine.db.run(self.engine.diagnostic, "incoming", exc, account_id=account_id)
                finally:
                    self._incoming.task_done()

        self._incoming_workers = [asyncio.create_task(consume()) for _ in range(2)]

    async def _handle_incoming(self, account_id, event):
        try:
            current = await self.engine.db.run(
                self.engine.db.one,
                "SELECT a.*,ca.status AS campaign_status FROM accounts a JOIN campaign_members m ON m.account_id=a.id JOIN campaigns ca ON ca.id=m.campaign_id WHERE a.id=?",
                (account_id,),
            )
            if (
                not current
                or current["campaign_status"] != "RUNNING"
                or current["paused"]
                or current["account_health"] != "OK"
                or current["flood_until"] > self.engine.clock()
            ):
                return
            gate = CAPTCHA_GATE.search(event.raw_text or "")
            if not event.is_private and not event.is_reply and not gate:
                return
            sender = await asyncio.wait_for(event.get_sender(), 30)
            if not sender:
                return
            if not event.is_private and getattr(sender, "bot", False) and gate:
                await self.engine.db.run(self.engine.require_manual_verification, account_id, event.chat_id)
                return
            reply_sender = None
            if not event.is_private and event.is_reply:
                original = await asyncio.wait_for(event.get_reply_message(), 30)
                reply_sender = original.sender_id if original else None
            reply_header = getattr(event.message, "reply_to", None)
            topic = getattr(reply_header, "reply_to_top_id", None) or (
                getattr(reply_header, "reply_to_msg_id", 0) if getattr(reply_header, "forum_topic", False) else 0
            )
            await self.engine.db.run(
                self.engine.incoming,
                account_id,
                event.chat_id,
                event.id,
                sender.id,
                event.is_private,
                bot=getattr(sender, "bot", False),
                outgoing=event.out,
                reply_to_sender_id=reply_sender,
                text=event.raw_text or "",
                topic_id=topic or 0,
            )
        except Exception as exc:
            classified = classify_error(exc)
            if classified.category == "critical":
                await self.engine.db.run(self.engine.critical, account_id, classified.code)
            else:
                await self.engine.db.run(self.engine.diagnostic, "incoming", exc, account_id=account_id)

    async def sync_listeners(self):
        eligible = await self.engine.db.run(
            self.engine.db.all,
            "SELECT a.id,s.encrypted_session FROM accounts a JOIN account_sessions s ON s.account_id=a.id JOIN campaign_members m ON m.account_id=a.id JOIN campaigns ca ON ca.id=m.campaign_id WHERE a.account_health='OK' AND a.paused=0 AND a.onboarding_started_at<=? AND a.flood_until<=? AND a.connection_retry_at<=? AND ca.status='RUNNING' ORDER BY a.id",
            (self.engine.clock() - 86400, self.engine.clock(), self.engine.clock()),
        )
        ids = {a["id"] for a in eligible}
        for account_id in list(self.clients):
            if account_id not in ids:
                client = self.clients.pop(account_id)
                self._client_sessions.pop(account_id, None)
                try:
                    await asyncio.wait_for(client.disconnect(), 5)
                except Exception as exc:
                    await self.engine.db.run(self.engine.diagnostic, "disconnect", exc, account_id=account_id)
        candidates = [
            a
            for a in eligible
            if a["id"] not in self.clients
            or not self.clients[a["id"]].is_connected()
            or self._client_sessions.get(a["id"]) != a["encrypted_session"]
        ]
        chosen = candidates[: self.engine.config.connection_batch_size]
        semaphore = asyncio.Semaphore(min(4, self.engine.config.concurrency))

        async def connect(account):
            async with semaphore:
                try:
                    await self.client(account["id"])
                except Exception as exc:
                    error = exc if isinstance(exc, OperationError) else classify_error(exc)
                    if error.category == "critical":
                        await self.engine.db.run(self.engine.critical, account["id"], error.code)
                    elif error.category == "flood":

                        def flood():
                            with self.engine.db.tx() as c:
                                c.execute(
                                    "UPDATE accounts SET flood_until=? WHERE id=?",
                                    (self.engine.clock() + error.seconds, account["id"]),
                                )

                        await self.engine.db.run(flood)
                    else:
                        await self.engine.db.run(self.engine.diagnostic, "connection", error, account_id=account["id"])

        await asyncio.gather(*(connect(a) for a in chosen))

    async def execute(self, job):
        try:
            return await asyncio.wait_for(self._execute(job), timeout=120)
        except OperationError:
            raise
        except Exception as exc:
            error = classify_error(exc)
            if error.category == "unknown" and not job.get("_request_may_have_been_sent", False):
                error = OperationError("transient", type(exc).__name__)
            raise error from None

    async def _execute(self, job):
        job["_request_may_have_been_sent"] = False
        await self.guard(job)
        client = await self.client(job["account_id"])
        await self.guard(job)
        payload = job["payload"]
        if job["kind"] == "CHECK":
            me = await client.get_me()
            if not me:
                raise OperationError("critical", "SESSION_EXPIRED")
            return Result(data={"authorized": True})
        peer = payload["peer_id"]
        if job["kind"] == "JOIN":
            # No contact manipulation or fabricated conversations are needed for preparation.
            link = payload.get("link", "")
            # Re-read the permission now: a join revoked after the job was queued must not be
            # overridden by the value captured in the payload. Without it we only verify membership.
            current = await self.engine.db.run(
                self.engine.db.one, "SELECT join_allowed FROM chat_pool WHERE id=?", (job["chat_pool_id"],)
            )
            if current and current["join_allowed"]:
                try:
                    if "t.me/+" in link:
                        await self.guard(job)
                        job["_request_may_have_been_sent"] = True
                        update = await client(functions.messages.ImportChatInviteRequest(link.rsplit("+", 1)[1]))
                        entity = update.chats[0] if getattr(update, "chats", None) else await client.get_entity(peer)
                    else:
                        entity = await client.get_entity(link if link.startswith("@") else peer)
                        if isinstance(entity, types.Channel):
                            await self.guard(job)
                            job["_request_may_have_been_sent"] = True
                            await client(functions.channels.JoinChannelRequest(entity))
                except errors.UserAlreadyParticipantError:
                    entity = await client.get_entity(peer)
            else:
                entity = await client.get_entity(peer)
            permissions = await client.get_permissions(entity, "me")
            if not can_publish(entity, permissions):
                raise OperationError("forbidden", "PUBLICATION_FORBIDDEN")
            return Result(data={"joined": True, "can_write": True})
        if job["chat_pool_id"]:
            chat = await self.engine.db.run(
                self.engine.db.one, "SELECT * FROM chat_pool WHERE id=?", (job["chat_pool_id"],)
            )
            entity = await client.get_entity(chat["link"] if chat["link"].startswith("@") else peer)
            permissions = await client.get_permissions(entity, "me")
            if not can_publish(entity, permissions, payload.get("media_kind") if payload.get("media_path") else None):
                raise OperationError("forbidden", "PUBLICATION_FORBIDDEN")
            peer = entity
        reply_to = payload.get("reply_to") or payload.get("topic_id") or None
        await self.guard(job)
        media = payload.get("media_path")
        sticker = None
        if payload.get("sticker_emoji"):
            response = await client(functions.messages.GetStickersRequest(emoticon=payload["sticker_emoji"], hash=0))
            choices = list(getattr(response, "stickers", None) or [])[:40]
            if not choices:
                raise OperationError("configuration", "STICKER_UNAVAILABLE")
            sticker = random.choice(choices)
        if media:
            path = Path(media).resolve()
            root = media_dir().resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise OperationError("configuration", "MEDIA_UNAVAILABLE")
        await self.guard(job)
        # Await the status RPC so FloodWait/auth errors cannot escape in a background task.
        # Broadcast posts have no conversation typing indicator.
        if not getattr(peer, "broadcast", False):
            await asyncio.wait_for(
                client(functions.messages.SetTypingRequest(
                    peer, types.SendMessageTypingAction(), top_msg_id=payload.get("topic_id") or None
                )),
                timeout=10,
            )
        await self.guard(job)
        if sticker is not None:
            job["_request_may_have_been_sent"] = True
            message = await client.send_file(peer, sticker, reply_to=reply_to)
        elif media:
            job["_request_may_have_been_sent"] = True
            message = await client.send_file(
                peer,
                str(path),
                caption=payload.get("text", ""),
                reply_to=reply_to,
                force_document=payload.get("media_kind") == "document",
                parse_mode=None,
            )
        else:
            job["_request_may_have_been_sent"] = True
            message = await client.send_message(
                peer, payload.get("text", ""), reply_to=reply_to, link_preview=False, parse_mode=None
            )
        return Result(message.id, {"api_accepted": True})

    async def guard(self, job):
        if not await self.engine.db.run(self.engine.execution_allowed, job):
            raise OperationError("cancelled", "STATE_CHANGED_BEFORE_REQUEST")

    async def resolve_target(self, value):
        chat_id, topic, link = parse_target(value)
        if chat_id is not None:
            return chat_id, topic, link, str(chat_id)
        account = await self.engine.db.run(
            self.engine.db.one,
            "SELECT * FROM accounts WHERE account_health='OK' AND paused=0 AND onboarding_started_at<=? ORDER BY id LIMIT 1",
            (self.engine.clock() - 86400,),
        )
        if not account:
            raise ValueError(
                "Для определения ID по ссылке нужен подключённый аккаунт после тихого периода. Можно импортировать числовой ID"
            )
        client = await self.client(account["id"])
        if "t.me/+" in link:
            invite = await client(functions.messages.CheckChatInviteRequest(link.rsplit("+", 1)[1]))
            entity = getattr(invite, "chat", None)
            if not entity:
                raise ValueError("Для закрытого чата импортируйте числовой ID; вступление при импорте не выполняется")
        else:
            entity = await client.get_entity(link)
        if not isinstance(entity, (types.Chat, types.Channel)):
            raise ValueError("В пул разрешено добавлять только группы / каналы")
        # A number in a public link (t.me/<chat>/55) is a forum topic only in forums; in an ordinary
        # group it is a message id, so keep it as a topic only when the resolved chat really is a forum.
        if topic and not getattr(entity, "forum", False):
            topic = 0
        return utils.get_peer_id(entity), topic, link, entity.title

    async def close(self):
        for task in self._incoming_workers:
            task.cancel()
        await asyncio.gather(*self._incoming_workers, return_exceptions=True)
        self._incoming_workers.clear()
        while not self._incoming.empty():
            self._incoming.get_nowait()
            self._incoming.task_done()
        clients = list(self.clients.values())
        self.clients.clear()
        self._client_sessions.clear()
        await asyncio.gather(*(asyncio.wait_for(client.disconnect(), 5) for client in clients), return_exceptions=True)


def auth_serialized(function):
    @wraps(function)
    async def guarded(self, admin_id, *args, **kwargs):
        self.engine.authorize(admin_id)
        async with self._locks[admin_id]:
            return await function(self, admin_id, *args, **kwargs)

    return guarded


class AccountAuthorizer:
    """Explicit interactive authorization only. Codes/passwords are never persisted."""

    def __init__(self, engine, transport):
        self.engine, self.transport = engine, transport
        self.pending = {}
        self._locks = {admin: asyncio.Lock() for admin in engine.config.admin_ids}

    @auth_serialized
    async def begin(self, admin_id, phone):
        self.engine.authorize(admin_id)
        await self._cancel(admin_id)
        # One device profile per login, used for the code request, the sign-in and every
        # later session connect, so Telegram sees one consistent device for this account.
        device = random_profile()
        if self.engine.config.mode == "dry-run":
            self.pending[admin_id] = {"phone": phone, "at": self.engine.clock(), "device": device}
            return
        client = self.transport.new_client(updates=False, device=device)
        try:
            await asyncio.wait_for(client.connect(), 30)
            sent = await asyncio.wait_for(client.send_code_request(phone), 30)
        except BaseException:
            await asyncio.wait_for(client.disconnect(), 10)
            raise
        self.pending[admin_id] = {
            "client": client,
            "phone": phone,
            "hash": sent.phone_code_hash,
            "at": self.engine.clock(),
            "device": device,
        }

    @auth_serialized
    async def finish(self, admin_id, code=None, password=None):
        self.engine.authorize(admin_id)
        pending = self.pending.get(admin_id)
        if not pending or self.engine.clock() - pending["at"] > 600:
            await self._cancel(admin_id)
            raise ValueError("Сессия входа истекла. Начните заново")
        if self.engine.config.mode == "dry-run":
            if code != "00000":
                raise ValueError("Тестовый код: 00000")
            digest = int(hashlib.sha256(pending["phone"].encode()).hexdigest()[:10], 16)
            device = json.dumps(pending["device"])
            self.pending.pop(admin_id)
            return pending["phone"], "dry-run-session", digest, device
        client = pending["client"]
        try:
            if password is not None:
                me = await asyncio.wait_for(client.sign_in(password=password), 30)
            else:
                me = await asyncio.wait_for(
                    client.sign_in(pending["phone"], code=code, phone_code_hash=pending["hash"]), 30
                )
        except errors.SessionPasswordNeededError:
            return None
        session = client.session.save()
        phone = pending["phone"]
        device = json.dumps(pending["device"])
        await self._cancel(admin_id)
        return phone, session, me.id, device

    @auth_serialized
    async def cancel(self, admin_id):
        await self._cancel(admin_id)

    async def _cancel(self, admin_id):
        pending = self.pending.pop(admin_id, None)
        if pending and "client" in pending:
            await asyncio.wait_for(pending["client"].disconnect(), 10)

    async def expire(self):
        for admin_id in list(self.pending):
            async with self._locks[admin_id]:
                pending = self.pending.get(admin_id)
                if pending and self.engine.clock() - pending["at"] > 600:
                    await self._cancel(admin_id)
