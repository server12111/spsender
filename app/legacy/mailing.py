"""Bind the original mailing loop and delivery code to pair shifts and receipts."""

import asyncio
from contextlib import nullcontext
from contextvars import ContextVar

from app.services.pairs import shift_role
from app.transport.base import OperationError

from .mailing_database import LegacyMailingDatabase
from .original_mailing import _FROZEN_ACCOUNT_MARKER, MailingService

mailing_context = ContextVar("legacy_mailing_pair", default=None)
target_context = ContextVar("legacy_mailing_target", default=None)


class PairedMailingService(MailingService):
    async def _handle_mailing_ban(self, mailing_id, account_id, error):
        from app.transport.telegram import classify_error

        classified = classify_error(error)
        health = (
            classified.code
            if classified.category == "critical"
            else "FROZEN"
            if _FROZEN_ACCOUNT_MARKER in str(error).lower()
            else "SESSION_EXPIRED"
        )
        # A virtual mailing is one pair, while its campaign can contain many
        # healthy pairs. Moderation pauses the affected account/pair only.
        await self.runtime.engine.db.run(self.runtime.engine.critical, account_id, health)
        task = self._tasks.pop(mailing_id, None)
        if task and task is not asyncio.current_task():
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
        for mapping in (self._stagger_until, self._working_targets, self._message_rotation):
            mapping.pop(mailing_id, None)

    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(LegacyMailingDatabase(runtime.engine), runtime)
        self.db.target_observer = lambda targets: self._no_hello.update(target.id for target in targets)

    async def _mailing_loop(self, mailing_id, initial_delay=0):
        token = mailing_context.set(mailing_id)
        try:
            return await super()._mailing_loop(mailing_id, initial_delay)
        finally:
            mailing_context.reset(token)

    async def _start_mailing_task(self, mailing_id, initial_delay=0):
        if mailing_id in self._tasks and not self._tasks[mailing_id].done():
            return
        # The existing greeting subsystem owns its persisted one-time greeting.
        self._no_hello.update(target.id for target in await self.db.get_mailing_targets(mailing_id))
        return await super()._start_mailing_task(mailing_id, initial_delay)

    async def _refresh_chat_senders(self, *args, **kwargs):
        # A chat belongs to exactly one pair in 2.0, hence there is no crowd of
        # independent owners/mailings to stagger in the same chat.
        self._chat_senders.clear()
        self._owner_chat_senders.clear()

    async def _apply_target_restriction(self, mailing_id, target_obj, error, *args, **kwargs):
        from app.transport.telegram import classify_error

        classified = classify_error(error)
        if classified.category == "cancelled":
            return
        target = await self.runtime.engine.db.run(
            self.runtime.engine.db.one, "SELECT chat_pool_id FROM pair_chats WHERE id=?", (target_obj.id,)
        )
        context = self._spam_ctx.get(mailing_id)
        if target and context and classified.category in {"forbidden", "chat"}:
            await self.runtime.engine.db.run(
                self.runtime.engine.chat_failure,
                mailing_id,
                target["chat_pool_id"],
                context[0],
                classified.code,
                classified.category == "forbidden",
            )
        return await super()._apply_target_restriction(mailing_id, target_obj, error, *args, **kwargs)

    async def _send_msg(self, client, target, msg, pm, *args, **kwargs):
        snapshot = kwargs.pop("target_snapshot", None)
        pair_id = mailing_context.get()
        engine = self.runtime.engine
        selector = "pc.id=?" if snapshot is not None else "(cp.link=? OR cast(cp.chat_id AS TEXT)=?)"
        identity = (snapshot.id,) if snapshot is not None else (str(target), str(target))
        item = await engine.db.run(
            engine.db.one,
            "SELECT pc.*,cp.chat_id,cp.topic_id,cp.link,p.campaign_id,c.version,c.timezone FROM pair_chats pc "
            "JOIN chat_pool cp ON cp.id=pc.chat_pool_id JOIN pairs p ON p.id=pc.pair_id JOIN campaigns c ON c.id=p.campaign_id "
            "WHERE pc.pair_id=? AND pc.removed_at IS NULL AND " + selector + " LIMIT 1",
            (pair_id, *identity),
        )
        if not item:
            raise OperationError("cancelled", "ASSIGNMENT_CHANGED")
        item["version"] = getattr(msg, "version", item["version"])
        if snapshot is not None:
            item["delivery_seq"] = snapshot.delivery_seq
            item["message_cursor"] = snapshot.msg_index
        item["media_kind"] = (
            "photo" if msg.photo_paths else ("video" if msg.video_path else ("document" if msg.document_path else None))
        )
        item["next_index"] = self._target_next_msg.get(item["id"], getattr(msg, "position", item["message_cursor"]) + 1)
        token = target_context.set(item)
        try:
            # Keep the requested visible typing indicator around original delivery.
            from telethon import functions, types

            await client(
                functions.messages.SetTypingRequest(
                    peer=target,
                    action=types.SendMessageTypingAction(),
                    top_msg_id=item["topic_id"] if "topic_id" in item else None,
                )
            )
            return await super()._send_msg(client, target, msg, pm, *args, **kwargs)
        finally:
            target_context.reset(token)

    async def _try_join_and_send(self, client, target, target_obj, msg, pm, mailing_id, account_id=None, **kwargs):
        item = await self.runtime.engine.db.run(
            self.runtime.engine.db.one,
            "SELECT pc.*,cp.chat_id,cp.topic_id,cp.link,cp.join_allowed,cp.approved,p.campaign_id,c.version "
            "FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id JOIN pairs p ON p.id=pc.pair_id JOIN campaigns c ON c.id=p.campaign_id WHERE pc.id=? AND pc.removed_at IS NULL",
            (target_obj.id,),
        )
        if not item or not item["approved"] or not item["join_allowed"]:
            return False
        access = await self.runtime.engine.db.run(
            self.runtime.engine.db.one,
            "SELECT manual_required FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
            (account_id, item["chat_pool_id"]),
        )
        if access and access["manual_required"]:
            return False
        token = target_context.set(item)
        try:
            return await super()._try_join_and_send(
                client, target, target_obj, msg, pm, mailing_id, account_id, **kwargs
            )
        finally:
            target_context.reset(token)

    async def guard(self, account_id):
        return await self.runtime.engine.db.run(self._guard, account_id)

    def _guard(self, account_id, connection=None):
        engine = self.runtime.engine
        pair_id = mailing_context.get()
        with nullcontext(connection) if connection is not None else engine.db.read() as connection:
            pair = engine._pair(connection, pair_id)
            campaign = engine.campaign(connection, pair["campaign_id"])
            active = (
                pair["account_a_id"]
                if shift_role(engine.clock(), campaign["timezone"]) == "A"
                else pair["account_b_id"]
            )
            if campaign["status"] != "RUNNING" or active != account_id or not engine._pair_ready(connection, pair):
                raise OperationError("cancelled", "PAIR_STATE_CHANGED")
            target = target_context.get()
            if target:
                current = connection.execute(
                    "SELECT pc.*,cp.approved,cp.greeting_allowed FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id WHERE pc.id=? AND pc.removed_at IS NULL",
                    (target["id"],),
                ).fetchone()
                if (
                    not current
                    or current["status"] != "ASSIGNED"
                    or not current["approved"]
                    or campaign["version"] != target["version"]
                ):
                    raise OperationError("cancelled", "ASSIGNMENT_CHANGED")
                if engine._greeting_pending(connection, campaign, current):
                    raise OperationError("cancelled", "GREETING_PENDING")
                if connection.execute(
                    "SELECT 1 FROM chat_failures WHERE pair_id=? AND chat_pool_id=? AND account_id=?",
                    (pair_id, target["chat_pool_id"], account_id),
                ).fetchone():
                    raise OperationError("cancelled", "CHAT_REQUIRES_REVIEW")
                access = connection.execute(
                    "SELECT manual_required FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
                    (account_id, target["chat_pool_id"]),
                ).fetchone()
                if access and access["manual_required"]:
                    raise OperationError("cancelled", "MANUAL_VERIFICATION_REQUIRED")
                if current["cooldown_until"] > engine.clock() and not target.get("accepted"):
                    raise OperationError("cancelled", "CHAT_COOLDOWN")
                if connection.execute(
                    "SELECT 1 FROM dispatch_jobs WHERE chat_pool_id=? AND status IN ('INFLIGHT','UNKNOWN') AND (? IS NULL OR id<>?) LIMIT 1",
                    (target["chat_pool_id"], target.get("legacy_job_id"), target.get("legacy_job_id")),
                ).fetchone():
                    raise OperationError("cancelled", "CHAT_BUSY")
