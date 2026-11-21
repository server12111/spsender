"""Real Telegram operations. Constructed only in explicitly configured live mode."""

import asyncio
import base64
import hashlib
import hmac
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
from app.services.core import DomainError
from app.services.memberships import CHALLENGE, membership_links, membership_prompt

from .authorization import AuthorizedSession, delivery_hint, failure_context
from .base import OperationError, Result
from .limits import RPCBudget
from .session import BoundedStringSession

MANUAL_VERIFICATION = re.compile(
    r"captcha|капч\w*|я\s+не\s+робот|verify.*human|подтверд\w*.*человек|пройд\w*.*проверк", re.I
)

# Admission queueing must stay shorter than the 180 s claim lease so a job can
# never be marked WORKER_LOST while its worker is still alive.
ADMISSION_WAIT = 150


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
    if name in {"PhoneNumberInvalidError", "PhoneNumberBannedError", "PhoneNumberUnoccupiedError"}:
        return OperationError("recipient", "PHONE_INVALID")
    if name in {"PhoneNumberFloodError"}:
        return OperationError("flood", "PHONE_FLOOD", max(60, getattr(exc, "seconds", 60)))
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

    def new_client(self, session="", updates=True, api_identity=None):
        return TelegramClient(
            session=BoundedStringSession(session, entity_limit=self.engine.config.entity_cache_limit),
            api_id=api_identity.api_id if api_identity else self.engine.config.api_id,
            api_hash=api_identity.api_hash if api_identity else self.engine.config.api_hash,
            receive_updates=updates,
            flood_sleep_threshold=0,
            request_retries=0,
            raise_last_call_error=True,
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
                identity = await self.engine.db.run(self.engine.account_api, account_id)
                client = self.new_client(session, api_identity=identity)
                await asyncio.wait_for(client.connect(), 30)

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

                # Registered before any awaited setup so updates arriving during
                # authorization and DB writes are queued, not dropped.
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
                if isinstance(exc, DomainError):
                    raise OperationError("configuration", "ACCOUNT_API_REQUIRED") from None
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
        legacy = getattr(self, "legacy_runtime", None)
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
            (
                self.engine.clock() - 86400,
                self.engine.clock(),
                self.engine.clock(),
                bool(getattr(self.engine, "legacy_runtime_active", False)),
            ),
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
                # Never disconnect a client out from under an in-flight job:
                # the per-account operation lock covers the whole execute().
                locks = getattr(self, "_operation_locks", None)
                if locks is None:
                    locks = self._operation_locks = WeakValueDictionary()
                async with locks.setdefault(account_id, asyncio.Lock()):
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
        # Publish the not-sent protocol from the very first await: cancellation
        # during queueing or admission provably never reached the network.
        job["_request_may_have_been_sent"] = False
        if hasattr(self, "_operation_semaphore"):
            lock = self._operation_locks.setdefault(job["account_id"], asyncio.Lock())
            # Queueing is legal, but the 180 s claim lease must never expire
            # while the worker is alive: cap the admission phase and refresh
            # the lease at every await boundary that can outlive it.
            try:
                budget = await asyncio.wait_for(self._operation_semaphore.acquire(), ADMISSION_WAIT)
            except asyncio.TimeoutError:
                raise OperationError("cancelled", "CANCELLED_BEFORE_REQUEST") from None
            try:
                if not await self.engine.db.run(self.engine.renew_job_lease, job):
                    raise OperationError("cancelled", "STATE_CHANGED_BEFORE_REQUEST")
                async with lock:
                    if not await self.engine.db.run(self.engine.renew_job_lease, job):
                        raise OperationError("cancelled", "STATE_CHANGED_BEFORE_REQUEST")
                    if job.get("chat_pool_id") is not None:
                        chat_lock = self._chat_operation_locks.setdefault(job["chat_pool_id"], asyncio.Lock())
                        async with chat_lock:
                            # Legacy RPCs share this lock and can outlast the
                            # remaining lease: renew again before the send.
                            if not await self.engine.db.run(self.engine.renew_job_lease, job):
                                raise OperationError("cancelled", "STATE_CHANGED_BEFORE_REQUEST")
                            return await self._execute_bounded(job)
                    return await self._execute_bounded(job)
            finally:
                try:
                    await budget.release()
                except asyncio.CancelledError:
                    # The budget releases under shield internally; a shutdown
                    # cancel must not replace the in-flight exception.
                    pass
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
            root = self.engine.config.media_dir
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

    async def resolve_target(self, value, account_id=None):
        chat_id, topic, link = parse_target(value)
        if chat_id is not None:
            return chat_id, topic, link, str(chat_id)
        account = await self.engine.db.run(self.engine.chat_target_account, account_id)
        if not account:
            raise OperationError("cancelled", "CHAT_CHECK_WAITING_ACCOUNT")
        if hasattr(self, "_operation_semaphore"):
            async with self._operation_semaphore:
                lock = self._operation_locks.setdefault(account["id"], asyncio.Lock())
                async with lock:
                    if not await self.engine.db.run(self.engine.chat_target_account, account["id"]):
                        raise OperationError("cancelled", "CHAT_CHECK_WAITING_ACCOUNT")
                    return await self._resolve_chat_reference(account, topic, link)
        return await self._resolve_chat_reference(account, topic, link)

    async def _resolve_chat_reference(self, account, topic, link):
        client = await self.client(account["id"])
        if not await self.engine.db.run(self.engine.chat_target_account, account["id"]):
            raise OperationError("cancelled", "CHAT_CHECK_WAITING_ACCOUNT")
        if "t.me/+" in link:
            invite = await asyncio.wait_for(
                client(functions.messages.CheckChatInviteRequest(link.rsplit("+", 1)[1])), 30
            )
            entity = getattr(invite, "chat", None)
            if not entity:
                raise OperationError("manual", "PRIVATE_CHAT_REQUIRES_MEMBERSHIP")
        else:
            try:
                entity = await asyncio.wait_for(client.get_entity(link), 30)
            except ValueError:
                raise OperationError("chat", "CHAT_UNAVAILABLE") from None
        if not isinstance(entity, (types.Chat, types.Channel)):
            raise OperationError("chat", "NOT_A_CHAT")
        await self._remember_peer(account["id"], entity)
        return utils.get_peer_id(entity), topic, link, entity.title

    async def resolve_pending_targets(self, owner, limit=4):
        """Bounded read-only checks. Imports need neither a session nor a socket."""
        now = self.engine.clock()
        pending = await self.engine.db.run(
            self.engine.db.scalar,
            "SELECT 1 FROM chat_targets WHERE approved=1 AND "
            "((status='PENDING' AND next_attempt_at<=?) OR (status='RESOLVING' AND lease_until<=?)) LIMIT 1",
            (now, now),
        )
        if not pending:
            return 0
        account = await self.engine.db.run(self.engine.chat_target_account)
        if not account:
            return 0
        completed = 0
        options = await self.engine.db.run(self.engine.runtime_options)
        for _ in range(min(4, options["batch_size"], max(0, limit))):
            if not await self.engine.db.run(self.engine.chat_target_account, account["id"]):
                break
            target = await self.engine.db.run(self.engine.claim_chat_target, owner)
            if not target:
                break
            value = target["link"] + (f"/{target['topic_id']}" if target["topic_id"] else "")
            try:
                # The complete admission/lock/connect/read phase is shorter
                # than the claim lease. These RPCs cannot send messages.
                result = await asyncio.wait_for(self.resolve_target(value, account["id"]), 90)
                completed += bool(await self.engine.db.run(self.engine.finish_chat_target, target, result))
            except Exception as exc:
                error = classify_error(exc)
                if error.category == "critical":
                    await self.engine.db.run(self.engine.critical, account["id"], error.code)
                elif error.category == "flood":
                    await self.engine.db.run(self.engine.flood_wait, account["id"], error.seconds)
                await self.engine.db.run(self.engine.fail_chat_target, target, error)
                await self.engine.db.run(
                    self.engine.diagnostic,
                    "chat-check",
                    error,
                    account_id=account["id"],
                    target_id=target["id"],
                    reason=error.code,
                )
                if error.category in {"critical", "flood", "cancelled"}:
                    break
        return completed

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
        finally:
            # One failed disconnect must not permanently block client reuse.
            self._retiring.discard(account_id)
        if self.clients.get(account_id) is client:
            self.clients.pop(account_id, None)
            self._client_sessions.pop(account_id, None)

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
            self._stages[admin_id] = function.__name__
            try:
                return await function(self, admin_id, *args, **kwargs)
            except Exception as exc:
                pending = self.pending.get(admin_id, {})
                if isinstance(exc, errors.FloodWaitError):
                    await self.engine.db.run(self._extend_wait, "auth:api-wait", max(1, exc.seconds))
                elif type(exc).__name__ == "PhoneNumberFloodError" and pending.get("phone"):
                    await self.engine.db.run(self._extend_wait, self._phone_key(pending["phone"]), 3600)
                stage = self._stages[admin_id]
                if isinstance(exc, ValueError):
                    exc.authorization_stage = stage
                await self.engine.db.run(
                    self.engine.diagnostic, "authorization-step", exc, **failure_context(exc, stage)
                )
                raise
            finally:
                self._stages.pop(admin_id, None)

    return guarded


class AccountAuthorizer:
    """Explicit interactive authorization only. Codes/passwords are never persisted."""

    def __init__(self, engine, transport):
        self.engine, self.transport = engine, transport
        self.pending = {}
        self._locks = {admin: asyncio.Lock() for admin in engine.config.admin_ids}
        self._stages = {}

    @staticmethod
    async def _request_code(client, phone):
        """Follow explicit wrong-DC rejections only, never ambiguous failures.

        Telethon switches DC before re-raising a migration when its retry
        budget is zero. Keeping zero retries protects message delivery too.
        """
        visited = set()
        for attempt in range(3):
            try:
                return await client.send_code_request(phone)
            except (errors.PhoneMigrateError, errors.NetworkMigrateError, errors.UserMigrateError) as exc:
                if attempt == 2 or exc.new_dc in visited or await client.is_user_authorized():
                    raise
                visited.add(exc.new_dc)

    def _phone_key(self, phone):
        digest = hmac.new(self.engine.config.session_key.encode(), phone.encode(), hashlib.sha256).hexdigest()
        return "auth:phone-wait:" + digest

    def _extend_wait(self, key, seconds):
        with self.engine.db.tx() as c:
            c.execute(
                "INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=CAST(MAX(CAST(settings.value AS REAL),CAST(excluded.value AS REAL)) AS TEXT)",
                (key, str(self.engine.clock() + seconds)),
            )

    def _reserve_code_request(self, phone):
        key, now = self._phone_key(phone), self.engine.clock()
        with self.engine.db.tx() as c:
            for item in c.execute("SELECT value FROM settings WHERE key IN (?,?)", (key, "auth:api-wait")):
                delay = float(item[0]) - now
                if delay > 0:
                    raise DomainError(
                        f"Пауза перед запросом кода: ещё {max(1, int(delay) + 1)} с. Telegram-ограничения не обходятся."
                    )
            c.execute(
                "INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(now + 60)),
            )

    def _check_api_wait(self):
        wait = float(self.engine.db.scalar("SELECT value FROM settings WHERE key='auth:api-wait'") or 0)
        if wait > self.engine.clock():
            raise DomainError(f"Telegram ограничил вход. Подождите {max(1, int(wait - self.engine.clock()) + 1)} с.")

    @auth_serialized
    async def import_file(self, admin_id, filename, content, account_id=None):
        import tempfile

        from app.legacy.session_import import detect_import_kind, load_import_source, sqlite_session_kind

        if self.engine.config.mode != "live":
            raise ValueError("Импорт настоящих сессий доступен только в live")
        identity = await self.engine.db.run(self.engine.authorization_api, account_id)
        await self.engine.db.run(self._check_api_wait)
        if len(content) > 20 * 1024 * 1024:
            raise ValueError("Файл слишком большой")
        kind = detect_import_kind(filename, content[:16])
        if not kind:
            raise ValueError("Нужен .session или ZIP с tdata")
        await self._cancel(admin_id)
        directory = tempfile.TemporaryDirectory(prefix="fe-account-import-")
        path = Path(directory.name) / ("source.zip" if kind == "tdata" else "source.session")
        path.write_bytes(content)
        pending = {"directory": directory, "at": self.engine.clock(), "import": True, "api": identity}
        self.pending[admin_id] = pending
        try:
            if kind == "session":
                kind = await asyncio.to_thread(sqlite_session_kind, str(path))
            source = await asyncio.wait_for(
                load_import_source(
                    kind,
                    str(path),
                    directory.name,
                    register_client=lambda client: pending.__setitem__("source", client),
                ),
                90,
            )
            self.pending[admin_id]["source"] = source
            return await self._finish_import(admin_id)
        except BaseException:
            await self._cancel(admin_id)
            raise

    @auth_serialized
    async def finish_import(self, admin_id, password):
        await self.engine.db.run(self._check_api_wait)
        pending = self.pending.get(admin_id)
        if not pending or not pending.get("import") or self.engine.clock() - pending["at"] > 600:
            await self._cancel(admin_id)
            raise ValueError("Импорт истёк. Начните заново")
        try:
            return await self._finish_import(admin_id, password)
        except BaseException:
            await self._cancel(admin_id)
            raise

    async def _finish_import(self, admin_id, password=None):
        from opentele.api import APIData

        from app.legacy.session_import import ImportPasswordRequired, create_new_session

        pending = self.pending[admin_id]
        identity = pending.get("api")
        if identity is None:
            identity = pending["api"] = await self.engine.db.run(self.engine.authorization_api)
        client = pending.get("client")
        if client is None:
            try:
                client = await asyncio.wait_for(
                    create_new_session(
                        pending["source"],
                        APIData(api_id=identity.api_id, api_hash=identity.api_hash),
                        password=password,
                        register_client=lambda client: pending.__setitem__("client", client),
                    ),
                    90,
                )
            except ImportPasswordRequired:
                return None
            pending["client"] = client
        me = await asyncio.wait_for(client.get_me(), 30)
        if not me or getattr(me, "bot", False) or not getattr(me, "phone", None):
            raise ValueError("Сессия должна принадлежать вашему пользовательскому аккаунту")
        result = AuthorizedSession("+" + me.phone.lstrip("+"), client.session.save(), me.id, identity.credential_id)
        await self._cancel(admin_id)
        return result

    @auth_serialized
    async def begin(self, admin_id, phone, account_id=None):
        self.engine.authorize(admin_id)
        self._stages[admin_id] = "validate_api"
        if self.engine.config.mode == "live":
            await self.engine.db.run(self._check_api_wait)
        pending = self.pending.get(admin_id)
        if (
            pending
            and not pending.get("import")
            and pending.get("hash")
            and pending.get("phone") == phone
            and pending.get("account_id") == account_id
            and self.engine.clock() - pending["at"] <= 600
        ):
            return pending.get("delivery", "Код уже запрошен. Проверьте приложение Telegram.")
        self._stages[admin_id] = "cleanup_previous"
        await self._cancel(admin_id)
        if self.engine.config.mode == "dry-run":
            identity = None
            if await self.engine.db.run(self.engine.db.scalar, "SELECT count(*) FROM api_credentials WHERE enabled=1"):
                identity = await self.engine.db.run(self.engine.authorization_api, account_id)
            self.pending[admin_id] = {
                "phone": phone,
                "at": self.engine.clock(),
                "api": identity,
                "account_id": account_id,
            }
            return
        self._stages[admin_id] = "validate_api"
        identity = await self.engine.db.run(self.engine.authorization_api, account_id)
        self._stages[admin_id] = "reserve_request"
        await self.engine.db.run(self._reserve_code_request, phone)
        self._stages[admin_id] = "create_client"
        client = self.transport.new_client(updates=False, api_identity=identity)
        self.pending[admin_id] = {
            "client": client,
            "phone": phone,
            "at": self.engine.clock(),
            "api": identity,
            "account_id": account_id,
        }
        try:
            self._stages[admin_id] = "connect"
            await asyncio.wait_for(client.connect(), 30)
            self._stages[admin_id] = "request_code"
            sent = await asyncio.wait_for(self._request_code(client, phone), 30)
            if not isinstance(sent, types.auth.SentCode):
                raise DomainError(
                    "Telegram не выдал обычный код входа. Используйте официальный клиент и импорт своей сессии; автоматического обхода нет."
                )
            if not sent.phone_code_hash:
                raise DomainError(
                    "Telegram не вернул состояние кода входа. Отмените вход и повторите после паузы; код не запрашивается автоматически."
                )
        except BaseException as exc:
            if isinstance(exc, errors.FloodWaitError):
                await self.engine.db.run(self._extend_wait, "auth:api-wait", max(1, exc.seconds))
            elif type(exc).__name__ == "PhoneNumberFloodError":
                await self.engine.db.run(self._extend_wait, self._phone_key(phone), 3600)
            try:
                await self._cancel(admin_id)
            except Exception as cleanup_error:
                await self.engine.db.run(
                    self.engine.diagnostic,
                    "authorization-cleanup",
                    cleanup_error,
                    **failure_context(cleanup_error, "disconnect_failed_login"),
                )
            raise
        self.pending[admin_id] = {
            "client": client,
            "phone": phone,
            "hash": sent.phone_code_hash,
            "at": self.engine.clock(),
            "delivery": delivery_hint(sent),
            "api": identity,
            "account_id": account_id,
        }
        return self.pending[admin_id]["delivery"]

    @auth_serialized
    async def finish(self, admin_id, code=None, password=None):
        self.engine.authorize(admin_id)
        pending = self.pending.get(admin_id)
        if not pending or self.engine.clock() - pending["at"] > 600:
            await self._cancel(admin_id)
            raise DomainError("Сессия входа истекла. Отмените вход и начните заново")
        if pending.get("import"):
            raise DomainError("Открыт импорт сессии. Продолжите импорт или отмените его перед входом по номеру.")
        if self.engine.config.mode == "dry-run":
            if code != "00000":
                raise ValueError("Тестовый код: 00000")
            digest = int(hashlib.sha256(pending["phone"].encode()).hexdigest()[:10], 16)
            self.pending.pop(admin_id)
            identity = pending.get("api")
            return AuthorizedSession(
                pending["phone"], "dry-run-session", digest, identity.credential_id if identity else None
            )
        await self.engine.db.run(self._check_api_wait)
        client = pending["client"]
        try:
            if password is not None:
                self._stages[admin_id] = "sign_in_password"
                me = await asyncio.wait_for(client.sign_in(password=password), 30)
            else:
                self._stages[admin_id] = "sign_in_code"
                me = await asyncio.wait_for(
                    client.sign_in(pending["phone"], code=code, phone_code_hash=pending["hash"]), 30
                )
        except errors.SessionPasswordNeededError:
            return None
        if not me or not getattr(me, "id", None) or getattr(me, "bot", False):
            await self._cancel(admin_id)
            raise DomainError(
                "Нужен существующий пользовательский аккаунт. Регистрация нового аккаунта или вход бота не выполняются."
            )
        session = client.session.save()
        phone = pending["phone"]
        identity = pending.get("api")
        await self._cancel(admin_id)
        return AuthorizedSession(phone, session, me.id, identity.credential_id if identity else None)

    @auth_serialized
    async def cancel(self, admin_id):
        await self._cancel(admin_id)

    async def _cancel(self, admin_id):
        pending = self.pending.get(admin_id)
        if not pending:
            return
        failures = []
        for field in ("client", "source"):
            if field not in pending:
                continue
            try:
                if field == "source":
                    from app.legacy.session_import import cleanup_import_source

                    await asyncio.wait_for(cleanup_import_source(pending[field]), 10)
                else:
                    await asyncio.wait_for(pending[field].disconnect(), 10)
            except BaseException as exc:
                failures.append(exc)
            else:
                pending.pop(field)
        if "directory" in pending and not any(field in pending for field in ("client", "source")):
            import tempfile

            if not Path(pending["directory"].name).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve()):
                raise ValueError("Небезопасный путь временного импорта")
            await asyncio.to_thread(pending["directory"].cleanup)
            pending.pop("directory")
        if failures:
            raise next((exc for exc in failures if isinstance(exc, asyncio.CancelledError)), failures[0])
        self.pending.pop(admin_id, None)

    async def expire(self):
        for admin_id in list(self.pending):
            if self._locks[admin_id].locked():
                continue
            async with self._locks[admin_id]:
                pending = self.pending.get(admin_id)
                if pending and self.engine.clock() - pending["at"] > 600:
                    await self._cancel(admin_id)
