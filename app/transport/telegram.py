"""Real Telegram operations. Constructed only in explicitly configured live mode."""

import asyncio
import base64
import hashlib
import json
import logging
import random
import re
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse
from weakref import WeakValueDictionary

from telethon import TelegramClient, errors, events, functions, types, utils

from app.services.chat_pool import parse_target
from app.services.memberships import CHALLENGE, membership_links, membership_prompt

from .base import OperationError, Result
from .limits import RPCBudget
from .session import BoundedStringSession

MANUAL_VERIFICATION = re.compile(
    r"captcha|капч\w*|я\s+не\s+робот|verify.*human|подтверд\w*.*человек|пройд\w*.*проверк", re.I
)


def classify_error(exc):
    if isinstance(exc, OperationError):
        return exc
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
        "ChatSendPhotosForbiddenError",
        "ChatSendVideosForbiddenError",
        "ChatSendDocsForbiddenError",
        "ChatSendStickersForbiddenError",
        "ChatRestrictedError",
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
    if name in {"UserIsBlockedError", "YouBlockedUserError", "UserPrivacyRestrictedError", "UserNotMutualContactError"}:
        return OperationError("recipient", "RECIPIENT_UNAVAILABLE")
    if name in {"MessageTooLongError", "MediaCaptionTooLongError", "MessageEmptyError", "StickerInvalidError"}:
        return OperationError("configuration", name)
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
        self._closing = False
        self._retiring = set()
        self._operation_semaphore = RPCBudget(engine)
        self._operation_locks = WeakValueDictionary()
        self._chat_operation_locks = WeakValueDictionary()

    def new_client(self, session="", updates=True):
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
            device_model="FE AutoSender",
            app_version="2.0",
            system_version="Python",
            entity_cache_limit=self.engine.config.entity_cache_limit,
        )

    async def client(self, account_id):
        if getattr(self, "_closing", False):
            raise OperationError("cancelled", "TRANSPORT_CLOSING")
        lock = self._client_locks.setdefault(account_id, asyncio.Lock())
        async with lock:
            if getattr(self, "_closing", False):
                raise OperationError("cancelled", "TRANSPORT_CLOSING")
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
                and account_id not in getattr(self, "_retiring", ())
            ):
                return existing
            if existing:
                await self._disconnect_client(account_id, existing, timeout=10)
            if not account["encrypted_session"]:
                raise OperationError("critical", "SESSION_EXPIRED")
            client = None
            try:
                session = self.engine.secrets.decrypt(account["encrypted_session"])
                client = self.new_client(session)
                await asyncio.wait_for(client.connect(), 30)
                if not await asyncio.wait_for(client.is_user_authorized(), 30):
                    raise OperationError("critical", "SESSION_EXPIRED")
            except BaseException as exc:
                if client:
                    try:
                        await self._disconnect_client(account_id, client)
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
            try:
                await self.engine.db.run(self.engine.connection_succeeded, account_id)
                if getattr(self, "_closing", False):
                    raise OperationError("cancelled", "TRANSPORT_CLOSING")
            except BaseException:
                await self._disconnect_client(account_id, client)
                raise
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
        legacy=getattr(self,"legacy_runtime",None)
        if legacy and event.chat_id in legacy._warmup_chats:
            return
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
            gate = MANUAL_VERIFICATION.search(event.raw_text or "") or CHALLENGE.search(event.raw_text or "")
            links, verify = membership_links(
                event.raw_text or "", getattr(getattr(event, "message", None), "reply_markup", None)
            )
            membership = membership_prompt(
                event.raw_text or "", getattr(getattr(event, "message", None), "reply_markup", None)
            )
            if not event.is_private and not event.is_reply and not gate and not membership:
                return
            sender = await asyncio.wait_for(event.get_sender(), 30)
            if not sender:
                return
            await self._remember_peer(account_id, sender)
            if getattr(sender, "bot", False) and membership:
                parent = await self.engine.db.run(
                    self.engine.membership_context, account_id, event.chat_id, private=event.is_private
                )
                trusted = bool(parent and event.is_private)
                if parent and not event.is_private:
                    target = await self.engine.db.run(
                        self.engine.db.one, "SELECT link FROM chat_pool WHERE id=?", (parent,)
                    )
                    source = await self._peer_entity(event.client, account_id, event.chat_id, target["link"])
                    permissions = await asyncio.wait_for(event.client.get_permissions(source, sender), 30)
                    trusted = bool(permissions and (permissions.is_admin or permissions.is_creator))
                if (
                    trusted
                    and links
                    and await self.engine.db.run(
                        self.engine.register_membership_request,
                        account_id,
                        parent,
                        event.chat_id,
                        event.id,
                        sender.id,
                        links,
                        verify,
                    )
                ):
                    return
                if trusted:
                    await self.engine.db.run(self.engine.membership_manual_required, account_id, parent, event.id)
                    return
            if not event.is_private and getattr(sender, "bot", False) and gate:
                await self.engine.db.run(self.engine.preparation_manual_required, account_id, event.chat_id)
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
            elif classified.category == "flood":
                await self.engine.db.run(self.engine.flood_wait, account_id, classified.seconds)
            else:
                await self.engine.db.run(self.engine.diagnostic, "incoming", exc, account_id=account_id)

    async def sync_listeners(self):
        eligible = await self.engine.db.run(
            self.engine.db.all,
            "SELECT a.id,s.encrypted_session FROM accounts a JOIN account_sessions s ON s.account_id=a.id LEFT JOIN campaign_members m ON m.account_id=a.id LEFT JOIN campaigns ca ON ca.id=m.campaign_id LEFT JOIN legacy_warmup_settings w ON w.account_id=a.id WHERE a.account_health='OK' AND a.paused=0 AND a.onboarding_started_at<=? AND a.flood_until<=? AND a.connection_retry_at<=? AND (ca.status='RUNNING' OR (?=1 AND m.account_id IS NULL AND w.enabled=1)) ORDER BY a.id",
            (self.engine.clock() - 86400, self.engine.clock(), self.engine.clock(),bool(getattr(self.engine,"legacy_runtime_active",False))),
        )
        ids = {a["id"] for a in eligible}
        retiring = sorted(account_id for account_id in self.clients if account_id not in ids)
        cursor = getattr(self, "_sync_cursor", 0)
        retired_batch = [account_id for account_id in retiring if account_id > cursor][
            : self.engine.config.connection_batch_size
        ]
        if not retired_batch:
            retired_batch = retiring[: self.engine.config.connection_batch_size]
        retirement_slots = asyncio.Semaphore(min(4, self.engine.config.concurrency))

        async def retire(account_id):
            async with retirement_slots:
                client = self.clients.get(account_id)
                if client is None:
                    return
                try:
                    await self._disconnect_client(account_id, client)
                except Exception as exc:
                    await self.engine.db.run(self.engine.diagnostic, "disconnect", exc, account_id=account_id)

        await asyncio.gather(*(retire(account_id) for account_id in retired_batch))
        if retired_batch:
            self._sync_cursor = retired_batch[-1]
        candidates = [
            a
            for a in eligible
            if a["id"] not in self.clients
            or not self.clients[a["id"]].is_connected()
            or self._client_sessions.get(a["id"]) != a["encrypted_session"]
            or a["id"] in getattr(self, "_retiring", ())
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
                        await self.engine.db.run(self.engine.flood_wait, account["id"], error.seconds)
                    else:
                        await self.engine.db.run(self.engine.diagnostic, "connection", error, account_id=account["id"])

        await asyncio.gather(*(connect(a) for a in chosen))

    async def execute(self, job):
        if hasattr(self, "_operation_semaphore"):
            lock = self._operation_locks.setdefault(job["account_id"], asyncio.Lock())
            async with self._operation_semaphore, lock:
                if job.get("chat_pool_id") is not None:
                    chat_lock = self._chat_operation_locks.setdefault(job["chat_pool_id"], asyncio.Lock())
                    async with chat_lock:
                        return await self._execute_bounded(job)
                return await self._execute_bounded(job)
        return await self._execute_bounded(job)

    async def _execute_bounded(self, job):
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
        if job["kind"] == "MEMBERSHIP":
            return await self._execute_membership(client, job)
        peer = payload["peer_id"]
        if job["kind"] in {"JOIN", "ROTATION_JOIN", "ROTATION_CHECK"}:
            # No contact manipulation or fabricated conversations are needed for preparation.
            link = payload.get("link", "")
            if payload["join_allowed"] and job["kind"] != "ROTATION_CHECK":
                try:
                    if "t.me/+" in link:
                        await self.guard(job)
                        job["_request_may_have_been_sent"] = True
                        update = await client(functions.messages.ImportChatInviteRequest(link.rsplit("+", 1)[1]))
                        entity = (
                            update.chats[0]
                            if getattr(update, "chats", None)
                            else await self._peer_entity(client, job["account_id"], peer, link)
                        )
                    else:
                        entity = await self._peer_entity(client, job["account_id"], peer, link)
                        if isinstance(entity, types.Channel):
                            await self.guard(job)
                            job["_request_may_have_been_sent"] = True
                            await client(functions.channels.JoinChannelRequest(entity))
                except errors.UserAlreadyParticipantError:
                    entity = await self._peer_entity(client, job["account_id"], peer, link)
            else:
                entity = await self._peer_entity(client, job["account_id"], peer, link)
            await self._remember_peer(job["account_id"], entity)
            permissions = await client.get_permissions(entity, "me")
            allowed = can_publish(entity, permissions)
            if not allowed and job["kind"] == "JOIN":
                raise OperationError("forbidden", "PUBLICATION_FORBIDDEN")
            return Result(data={"joined": True, "can_write": allowed})
        if job["chat_pool_id"]:
            chat = await self.engine.db.run(
                self.engine.db.one, "SELECT * FROM chat_pool WHERE id=?", (job["chat_pool_id"],)
            )
            entity = await self._peer_entity(client, job["account_id"], peer, chat["link"])
            permissions = await client.get_permissions(entity, "me")
            if not can_publish(entity, permissions, payload.get("media_kind") if payload.get("media_path") else None):
                raise OperationError("forbidden", "PUBLICATION_FORBIDDEN")
            peer = entity
        elif isinstance(peer, int):
            peer = await self._peer_entity(client, job["account_id"], peer)
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
            root = Path("data/media").resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise OperationError("configuration", "MEDIA_UNAVAILABLE")
        await self.guard(job)
        # Await the status RPC so FloodWait/auth errors cannot escape in a background task.
        # Broadcast posts have no conversation typing indicator.
        if not getattr(peer, "broadcast", False):
            await asyncio.wait_for(
                client(
                    functions.messages.SetTypingRequest(
                        peer, types.SendMessageTypingAction(), top_msg_id=payload.get("topic_id") or None
                    )
                ),
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

    async def _execute_membership(self, client, job):
        payload = job["payload"]
        if payload["action"] == "CHECK":
            entity = await self._peer_entity(client, job["account_id"], payload["peer_id"], payload["link"])
            permissions = await client.get_permissions(entity, "me")
            if not can_publish(entity, permissions):
                raise OperationError("manual", "PUBLICATION_FORBIDDEN")
            return Result(data={"can_write": True, "joined": True})
        if payload["action"] == "VERIFY":
            source = await self._peer_entity(client, job["account_id"], payload["peer_id"])
            await self.guard(job)
            job["_request_may_have_been_sent"] = True
            try:
                await client(
                    functions.messages.GetBotCallbackAnswerRequest(
                        peer=source, msg_id=payload["message_id"], data=base64.b64decode(payload["data"])
                    )
                )
            except Exception as exc:
                if type(exc).__name__ in {"DataInvalidError", "MessageIdInvalidError", "BotResponseTimeoutError"}:
                    raise OperationError("manual", "MEMBERSHIP_CHECK_REQUIRED") from None
                raise
            return Result(data={"verified": True})
        path = urlparse(payload["link"]).path.strip("/")
        if path.startswith("+"):
            await self.guard(job)
            try:
                job["_request_may_have_been_sent"] = True
                update = await client(functions.messages.ImportChatInviteRequest(path[1:]))
                entity = update.chats[0]
            except errors.UserAlreadyParticipantError:
                invite = await client(functions.messages.CheckChatInviteRequest(path[1:]))
                entity = getattr(invite, "chat", None)
                if entity is None:
                    raise OperationError("manual", "MEMBERSHIP_CHECK_REQUIRED") from None
        else:
            entity = await client.get_entity("@" + path)
            await self._remember_peer(job["account_id"], entity)
            await self.guard(job)
            if isinstance(entity, types.User) and entity.bot:
                if payload.get("membership_request_id"):
                    await self.engine.db.run(
                        self.engine.membership_bot_resolved, job["id"], job["account_id"], entity.id
                    )
                await client(functions.messages.SetTypingRequest(entity, types.SendMessageTypingAction()))
                await self.guard(job)
                job["_request_may_have_been_sent"] = True
                await client(
                    functions.messages.StartBotRequest(entity, entity, payload["start_param"], payload["random_id"])
                )
                return Result(data={"peer_id": entity.id, "bot_started": True})
            if not isinstance(entity, types.Channel):
                raise OperationError("manual", "UNSUPPORTED_MEMBERSHIP_TARGET")
            job["_request_may_have_been_sent"] = True
            try:
                await client(functions.channels.JoinChannelRequest(entity))
            except errors.UserAlreadyParticipantError:
                pass
        permissions = await client.get_permissions(entity, "me")
        if permissions is None or getattr(permissions, "has_left", False):
            raise OperationError("manual", "MEMBERSHIP_CHECK_REQUIRED")
        return Result(data={"peer_id": utils.get_peer_id(entity), "joined": True})

    async def _remember_peer(self, account_id, entity):
        if isinstance(entity, types.Channel) and entity.access_hash is not None:
            await self.engine.db.run(
                self.engine.remember_entity,
                account_id,
                utils.get_peer_id(entity),
                {"kind": "channel", "id": entity.id, "access_hash": entity.access_hash},
            )
        elif isinstance(entity, types.Chat):
            await self.engine.db.run(
                self.engine.remember_entity, account_id, utils.get_peer_id(entity), {"kind": "chat", "id": entity.id}
            )
        elif isinstance(entity, types.User) and entity.access_hash is not None:
            await self.engine.db.run(
                self.engine.remember_entity,
                account_id,
                utils.get_peer_id(entity),
                {"kind": "user", "id": entity.id, "access_hash": entity.access_hash},
            )

    async def _peer_entity(self, client, account_id, peer, link=""):
        public_link = link if link.startswith("@") else ""
        if not public_link and link:
            try:
                _, _, normalized = parse_target(link)
                public_link = normalized if normalized.startswith("@") else ""
            except ValueError:
                pass
        reference = public_link or peer
        if not public_link:
            saved = await self.engine.db.run(
                self.engine.db.one,
                "SELECT encrypted_entity FROM account_entity_cache WHERE account_id=? AND peer_id=?",
                (account_id, peer),
            )
            if saved:
                cached = json.loads(self.engine.secrets.decrypt(saved["encrypted_entity"]))
                if cached["kind"] == "channel":
                    reference = types.InputPeerChannel(cached["id"], cached["access_hash"])
                elif cached["kind"] == "user":
                    reference = types.InputPeerUser(cached["id"], cached["access_hash"])
                else:
                    reference = types.InputPeerChat(cached["id"])
        try:
            entity = await client.get_entity(reference)
        except ValueError:
            entity = None
            if "t.me/+" in link:
                invite = await client(functions.messages.CheckChatInviteRequest(link.rsplit("+", 1)[1]))
                entity = getattr(invite, "chat", None)
            if entity is None and hasattr(client, "iter_dialogs"):
                async for dialog in client.iter_dialogs(limit=self.engine.config.entity_cache_limit):
                    if utils.get_peer_id(dialog.entity) == peer:
                        entity = dialog.entity
                        break
            if entity is None:
                raise OperationError("manual", "PEER_REFERENCE_REQUIRED") from None
        if isinstance(entity, (types.Channel, types.Chat, types.User)) and utils.get_peer_id(entity) != peer:
            # A public username can be reassigned after import. Never join or
            # publish to the new owner under an old approved chat assignment.
            raise OperationError("manual", "PEER_REFERENCE_REQUIRED")
        await self._remember_peer(account_id, entity)
        return entity

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
            invite = await asyncio.wait_for(
                client(functions.messages.CheckChatInviteRequest(link.rsplit("+", 1)[1])), 30
            )
            entity = getattr(invite, "chat", None)
            if not entity:
                raise ValueError("Для закрытого чата импортируйте числовой ID; вступление при импорте не выполняется")
        else:
            entity = await asyncio.wait_for(client.get_entity(link), 30)
        if not isinstance(entity, (types.Chat, types.Channel)):
            raise ValueError("В пул разрешено добавлять только группы / каналы")
        await self._remember_peer(account["id"], entity)
        return utils.get_peer_id(entity), topic, link, entity.title

    async def _disconnect_client(self, account_id, client, timeout=5):
        if not hasattr(self, "_retiring"):
            self._retiring = set()
        self._retiring.add(account_id)
        try:
            await asyncio.wait_for(client.disconnect(), timeout)
            if getattr(client, "is_connected", lambda: False)():
                raise ConnectionError("CLIENT_STILL_CONNECTED")
        except BaseException:
            # Also retain half-created clients when connection cleanup fails.
            self.clients.setdefault(account_id, client)
            raise
        if self.clients.get(account_id) is client:
            self.clients.pop(account_id, None)
            self._client_sessions.pop(account_id, None)
        self._retiring.discard(account_id)

    async def close(self):
        self._closing = True
        for task in self._incoming_workers:
            task.cancel()
        await asyncio.gather(*self._incoming_workers, return_exceptions=True)
        self._incoming_workers.clear()
        while not self._incoming.empty():
            self._incoming.get_nowait()
            self._incoming.task_done()

        outcomes = await asyncio.gather(
            *(self._disconnect_client(account_id, client) for account_id, client in list(self.clients.items())),
            return_exceptions=True,
        )
        if any(isinstance(outcome, BaseException) for outcome in outcomes):
            raise ConnectionError("CLIENT_DISCONNECT_FAILED")


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
    async def import_file(self, admin_id, filename, content):
        import tempfile

        from app.legacy.session_import import detect_import_kind, load_import_source, sqlite_session_kind

        if self.engine.config.mode != "live":
            raise ValueError("Импорт настоящих сессий доступен только в live")
        if len(content) > 20 * 1024 * 1024:
            raise ValueError("Файл слишком большой")
        kind = detect_import_kind(filename, content[:16])
        if not kind:
            raise ValueError("Нужен .session или ZIP с tdata")
        await self._cancel(admin_id)
        directory = tempfile.TemporaryDirectory(prefix="fe-account-import-")
        path = Path(directory.name) / ("source.zip" if kind == "tdata" else "source.session")
        path.write_bytes(content)
        self.pending[admin_id] = {"directory": directory, "at": self.engine.clock(), "import": True}
        try:
            if kind == "session":
                kind = await asyncio.to_thread(sqlite_session_kind, str(path))
            source = await asyncio.wait_for(load_import_source(kind, str(path), directory.name), 90)
            self.pending[admin_id]["source"] = source
            return await self._finish_import(admin_id)
        except BaseException:
            await self._cancel(admin_id)
            raise

    @auth_serialized
    async def finish_import(self, admin_id, password):
        pending = self.pending.get(admin_id)
        if not pending or not pending.get("import") or self.engine.clock() - pending["at"] > 600:
            await self._cancel(admin_id)
            raise ValueError("Импорт истёк. Начните заново")
        return await self._finish_import(admin_id, password)

    async def _finish_import(self, admin_id, password=None):
        from opentele.api import APIData

        from app.legacy.session_import import ImportPasswordRequired, create_new_session

        pending = self.pending[admin_id]
        try:
            client = await asyncio.wait_for(
                create_new_session(
                    pending["source"],
                    APIData(api_id=self.engine.config.api_id, api_hash=self.engine.config.api_hash),
                    password=password,
                ),
                90,
            )
        except ImportPasswordRequired:
            return None
        pending["client"] = client
        me = await asyncio.wait_for(client.get_me(), 30)
        if not me or getattr(me, "bot", False) or not getattr(me, "phone", None):
            raise ValueError("Сессия должна принадлежать вашему пользовательскому аккаунту")
        result = ("+" + me.phone.lstrip("+"), client.session.save(), me.id)
        await self._cancel(admin_id)
        return result

    @auth_serialized
    async def begin(self, admin_id, phone):
        self.engine.authorize(admin_id)
        await self._cancel(admin_id)
        if self.engine.config.mode == "dry-run":
            self.pending[admin_id] = {"phone": phone, "at": self.engine.clock()}
            return
        client = self.transport.new_client(updates=False)
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
            self.pending.pop(admin_id)
            return pending["phone"], "dry-run-session", digest
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
        await self._cancel(admin_id)
        return phone, session, me.id

    @auth_serialized
    async def cancel(self, admin_id):
        await self._cancel(admin_id)

    async def _cancel(self, admin_id):
        pending = self.pending.pop(admin_id, None)
        if pending and "client" in pending:
            await asyncio.wait_for(pending["client"].disconnect(), 10)
        if pending and "source" in pending:
            from app.legacy.session_import import cleanup_import_source

            await asyncio.wait_for(cleanup_import_source(pending["source"]), 10)
        if pending and "directory" in pending:
            import tempfile
            if not Path(pending["directory"].name).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve()):
                raise ValueError("Небезопасный путь временного импорта")
            await asyncio.to_thread(pending["directory"].cleanup)

    async def expire(self):
        for admin_id in list(self.pending):
            if self._locks[admin_id].locked():
                continue
            async with self._locks[admin_id]:
                pending = self.pending.get(admin_id)
                if pending and self.engine.clock() - pending["at"] > 600:
                    await self._cancel(admin_id)
