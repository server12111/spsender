"""Run the transferred service through shared limits, guards and an RPC journal."""

import asyncio
import base64
import hashlib
import json
import pickle
import time
from contextlib import AsyncExitStack
from contextvars import ContextVar
from types import SimpleNamespace
from uuid import uuid4
from weakref import WeakValueDictionary

from telethon.errors import UserAlreadyParticipantError

from app.transport.base import OperationError
from app.transport.limits import RPCBudget
from app.transport.telegram import TelegramTransport, can_publish, classify_error

from . import services
from .clock import engine_context
from .database import LegacyDatabase
from .mailing import PairedMailingService, mailing_context, target_context

DELIVERY_METHODS = {"send_message", "send_file", "forward_messages", "SendInlineBotResultRequest"}
JOIN_METHODS = {"JoinChannelRequest", "ImportChatInviteRequest"}
group_flow = ContextVar("legacy_warmup_group_flow", default=None)


def fingerprint(value):
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    return str(value)


class SafeLog:
    """Upstream formats raw RPC exceptions; only the component is logged here."""

    def __init__(self, engine, component):
        self.engine, self.component = engine, component
        self.last_at = None

    def __getattr__(self, level):
        def emit(*args, **kwargs):
            if level in {"warning", "error", "exception"}:
                if self.last_at is not None and self.engine.clock() - self.last_at < 60:
                    return
                self.last_at = self.engine.clock()
                self.engine.diagnostic(self.component, "LEGACY_EVENT")

        return emit


class GuardedClient:
    def __init__(self, runtime, account_id, client):
        self.runtime, self.account_id, self.client = runtime, account_id, client

    def is_connected(self):
        return self.client.is_connected()

    async def __call__(self, request, *args, **kwargs):
        name = type(request).__name__
        mutation = not name.startswith(("Get", "Check", "Read", "Resolve")) and name != "SetTypingRequest"
        return await self.invoke(name, self.client, request, *args, mutation=mutation, **kwargs)

    def __getattr__(self, name):
        if name.startswith("iter_"):

            async def iterate(*args, **kwargs):
                iterator = getattr(self.client, name)(*args, **kwargs).__aiter__()
                while True:
                    try:
                        yield await self.invoke(name, iterator.__anext__)
                    except StopAsyncIteration:
                        return

            return iterate
        attribute = getattr(self.client, name)
        if not callable(attribute):
            return attribute

        async def call(*args, **kwargs):
            mutation = name.startswith(("send_", "edit_", "delete_", "upload_")) and name != "send_read_acknowledge"
            result = await self.invoke(name, attribute, *args, mutation=mutation, **kwargs)
            # Message.click must also use this guard, rather than the raw client.
            for message in result if isinstance(result, (list, tuple)) else [result]:
                if hasattr(message, "_client"):
                    message._client = self
            return result

        return call

    async def invoke(self, name, function, *args, mutation=False, **kwargs):
        runtime = self.runtime
        engine = runtime.engine
        lock = runtime.transport._operation_locks.setdefault(self.account_id, asyncio.Lock())
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(runtime.transport._operation_semaphore)
            await stack.enter_async_context(lock)
            target = target_context.get()
            if target:
                chat_lock = runtime.transport._chat_operation_locks.setdefault(target["chat_pool_id"], asyncio.Lock())
                await stack.enter_async_context(chat_lock)
            await runtime.guard(self.account_id)
            operation = None
            request_started = False
            try:
                if mutation and name in {"JoinChannelRequest", "ImportChatInviteRequest"}:
                    next_at = await engine.db.run(
                        engine.db.scalar, "SELECT next_action_at FROM accounts WHERE id=?", (self.account_id,)
                    )
                    if next_at > engine.clock():
                        raise OperationError("cancelled", "JOIN_COOLDOWN")
                if mutation and target and name in {"send_message", "send_file", "forward_messages"}:
                    peer = await TelegramTransport._peer_entity(
                        runtime.transport, self.client, self.account_id, target["chat_id"], target["link"]
                    )
                    permissions = await self.client.get_permissions(peer, "me")
                    if not can_publish(peer, permissions, target.get("media_kind")):
                        raise OperationError("forbidden", "PUBLICATION_FORBIDDEN")

                    def remember_rights():
                        with engine.db.tx() as connection:
                            connection.execute(
                                "UPDATE account_chat_access SET joined=1,can_write=1,last_checked_at=? WHERE account_id=? AND chat_pool_id=?",
                                (engine.clock(), self.account_id, target["chat_pool_id"]),
                            )

                    await engine.db.run(remember_rights)
                    args = (peer,) + args[1:]
                if mutation:
                    await runtime.persist_before_rpc(self.account_id)
                    operation, cached = await engine.db.run(
                        runtime.begin_operation, self.account_id, name, args, kwargs
                    )
                    if cached:
                        if target:
                            target["accepted"] = True
                        return pickle.loads(base64.b64decode(engine.secrets.decrypt(cached)))
                await runtime.guard(self.account_id, operation)
                if target and mutation:
                    sequence = await engine.db.run(
                        engine.db.scalar, "SELECT delivery_seq FROM pair_chats WHERE id=?", (target["id"],)
                    )
                    if sequence != target["delivery_seq"]:
                        raise OperationError("cancelled", "DELIVERY_ALREADY_COMMITTED")
                request_started = True
                result = await asyncio.wait_for(function(*args, **kwargs), 90)
            except StopAsyncIteration:
                raise
            except BaseException as exc:
                if operation and name in JOIN_METHODS and isinstance(exc, UserAlreadyParticipantError):
                    # Membership is already satisfied. This known outcome must
                    # not become UNKNOWN and block an otherwise healthy account.
                    result = SimpleNamespace()
                    try:
                        await engine.db.run(runtime.end_operation, operation, result, None)
                    except Exception as persistence_error:
                        await runtime.persistence_failure(
                            operation, self.account_id, target, persistence_error, "MEMBERSHIP_ALREADY_CONFIRMED"
                        )
                        raise
                    return result
                error = exc if isinstance(exc, OperationError) else classify_error(exc)
                if isinstance(exc, asyncio.CancelledError):
                    error = OperationError("unknown" if request_started else "cancelled", "WORKER_CANCELLED")
                if operation:
                    stored = error if request_started else OperationError("cancelled", "CANCELLED_BEFORE_REQUEST")
                    try:
                        await engine.db.run(runtime.end_operation, operation, None, stored)
                    except Exception as persistence_error:
                        await runtime.persistence_failure(
                            operation, self.account_id, target, persistence_error, error.code
                        )
                        raise
                if error.category == "critical" and not operation:
                    await engine.db.run(engine.critical, self.account_id, error.code)
                elif error.category == "flood" and not operation:
                    await engine.db.run(engine.flood_wait, self.account_id, error.seconds)
                await engine.db.run(
                    engine.diagnostic,
                    "legacy-rpc",
                    error.code,
                    account_id=self.account_id,
                    chat_pool_id=target["chat_pool_id"] if target else None,
                )
                if target and error.code == "MEMBERSHIP_REQUIRED":

                    def mark_left():
                        with engine.db.tx() as connection:
                            connection.execute(
                                "UPDATE account_chat_access SET joined=0,can_write=0,last_error='MEMBERSHIP_REQUIRED' WHERE account_id=? AND chat_pool_id=?",
                                (self.account_id, target["chat_pool_id"]),
                            )

                    await engine.db.run(mark_left)
                elif target and error.category == "manual":
                    await engine.db.run(engine.membership_manual_required, self.account_id, target["chat_pool_id"], 0)
                raise
            if operation:
                try:
                    await engine.db.run(runtime.end_operation, operation, result, None)
                except Exception as persistence_error:
                    await runtime.persistence_failure(
                        operation, self.account_id, target, persistence_error, "ACKNOWLEDGED_BUT_NOT_COMMITTED"
                    )
                    raise
                if target_context.get() is not None:
                    target_context.get()["accepted"] = True
            if hasattr(result, "users"):
                for user in result.users:
                    await runtime.transport._remember_peer(self.account_id, user)
            return result


class LegacyRuntime:
    async def persistence_failure(self, operation, account_id, target, error, reason):
        job = {
            "id": f"legacy:{operation}",
            "account_id": account_id,
            "chat_pool_id": target["chat_pool_id"] if target else None,
            "campaign_id": target["campaign_id"] if target else None,
        }
        await self.engine._persistence_failure(job, error, reason, self.sink)

    async def persist_before_rpc(self, account_id):
        from .state_store import mono_to_wall, save_state

        service = self.service
        actor = {
            "times": [mono_to_wall(t) for t in service._dialog_times.get(account_id, [])],
            "partners": {str(p): mono_to_wall(t) for p, t in service._dialog_partners.get(account_id, {}).items()},
            "frozen": service._frozen.get(account_id),
            "bot_visits": service._bot_visits.get(account_id, {}),
            "own_bot_due": service._own_bot_due.get(account_id),
        }
        await save_state(self.database, f"warmup.actor:{account_id}", actor)
        for name, progress in list(service._dialog_progress.items()):
            if account_id in (progress["a"], progress["b"]):
                await save_state(self.database, f"warmup.dialog:{name}", progress)

    def __init__(self, engine, transport, owner, sink=None):
        self.engine, self.transport, self.owner = engine, transport, owner
        self.sink = sink
        if not hasattr(transport, "engine"):
            transport.engine = engine
        self.database = LegacyDatabase(engine)
        self._clients = {}
        self._warmup_pairs = set()
        self._warmup_chats = set()
        self._me_ids = {}
        if not hasattr(transport, "_operation_semaphore"):
            transport._operation_semaphore = RPCBudget(engine)
            transport._operation_locks = WeakValueDictionary()
        if not hasattr(transport, "_chat_operation_locks"):
            transport._chat_operation_locks = WeakValueDictionary()
        self.service = services.WarmupService(self.database, self)
        # The owner explicitly excluded voice and photo content from this port.
        self.service.voices._load = lambda: {"personas": [], "by": {}}
        self.service.photos._load = lambda: {}
        original_talk = self.service.groups._talk

        async def talk(*args, **kwargs):
            token = group_flow.set({"id": uuid4().hex, "sequence": 0})
            try:
                return await original_talk(*args, **kwargs)
            finally:
                group_flow.reset(token)

        self.service.groups._talk = talk
        self.mailing = PairedMailingService(self)
        engine.legacy_runtime_active = True
        with engine.db.tx() as connection:
            connection.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE kind='MAILING' AND status='PENDING'")
        self._cursor = 0
        self._pruned_at = 0
        transport.legacy_runtime = self

    async def get_client(self, account_id):
        if not await self.engine.db.run(self.engine.legacy_account_allowed, account_id):
            return None
        client = await self.transport.client(account_id)
        wrapped = self._clients.get(account_id)
        if not wrapped or wrapped.client is not client:
            wrapped = GuardedClient(self, account_id, client)
            self._clients[account_id] = wrapped
        telegram_id = await self.engine.db.run(
            self.engine.db.scalar, "SELECT telegram_id FROM accounts WHERE id=?", (account_id,)
        )
        self._me_ids[account_id] = telegram_id
        return wrapped

    async def stop_client(self, account_id):
        lock = self.transport._operation_locks.setdefault(account_id, asyncio.Lock())
        async with lock:
            client = getattr(self.transport, "clients", {}).get(account_id)
            if client is not None:
                await self.transport._disconnect_client(account_id, client)
            wrapped = self._clients.get(account_id)
            if wrapped and (client is None or wrapped.client is client):
                self._clients.pop(account_id, None)

    async def guard(self, account_id, operation_id=None):
        busy = await self.engine.db.run(
            self.engine.db.one,
            "SELECT 1 FROM legacy_rpc_journal WHERE account_id=? AND status='INFLIGHT' AND (? IS NULL OR id<>?) LIMIT 1",
            (account_id, operation_id, operation_id),
        )
        if busy:
            raise OperationError("cancelled", "LEGACY_RESULT_PENDING")
        if mailing_context.get() is not None:
            await self.mailing.guard(account_id)
        if not await self.engine.db.run(self.engine.legacy_account_allowed, account_id):
            raise OperationError("cancelled", "LEGACY_ACCOUNT_PAUSED")
        if self.engine.config.mode == "live":
            lease = await self.engine.db.run(
                self.engine.db.one, "SELECT * FROM worker_leases WHERE name='telegram-runtime'"
            )
            if not lease or lease["owner"] != self.owner or lease["expires_at"] <= self.engine.clock():
                raise OperationError("cancelled", "RUNTIME_OWNERSHIP_LOST")

    def begin_operation(self, account_id, name, args, kwargs):
        progress = {}
        target = target_context.get()
        if target:
            fields = ("id", "pair_id", "chat_pool_id", "campaign_id", "chat_id", "version")
            if name in DELIVERY_METHODS:
                fields += ("message_cursor", "delivery_seq", "next_index")
            progress = {key: target[key] for key in fields}
        for key, item in () if target else self.service._dialog_progress.items():
            if account_id in (item["a"], item["b"]):
                progress = {"dialog": key, "script": item["script"], "pos": item["pos"]}
                break
        if not progress:
            flow = group_flow.get()
            if flow:
                progress = {"group_flow": flow["id"], "step": flow["sequence"]}
                flow["sequence"] += 1
            else:
                progress = {
                    "action_due": self.engine.db.scalar(
                        "SELECT next_action_at FROM legacy_warmup_settings WHERE account_id=?", (account_id,)
                    )
                }
        if name in JOIN_METHODS:
            # Joining is idempotent at Telegram, whereas membership can change
            # after an earlier successful join. Journal each paced attempt; an
            # UNKNOWN attempt is still blocked by the account guard.
            progress["join_attempt"] = uuid4().hex
        intent = (
            [account_id, name, progress]
            if target and name in {"send_message", "send_file", "forward_messages"}
            else [account_id, name, args, kwargs, progress]
        )
        encoded = json.dumps(intent, default=fingerprint, sort_keys=True)
        key = hashlib.sha256(encoded.encode()).hexdigest()
        with self.engine.db.tx() as connection:
            old = connection.execute("SELECT * FROM legacy_rpc_journal WHERE operation_key=?", (key,)).fetchone()
            if old and old["status"] == "CONFIRMED":
                return None, old["result"]
            if old and old["status"] in {"INFLIGHT", "UNKNOWN"}:
                raise OperationError("cancelled", "LEGACY_RESULT_UNRESOLVED")
            if target and name in DELIVERY_METHODS:
                # Close the window between the async guards and this write:
                # UI stop and ordinary queue claims use the same writer lock.
                self.mailing._guard(account_id, connection)
                if connection.execute(
                    "SELECT 1 FROM dispatch_jobs WHERE account_id=? AND status='INFLIGHT' LIMIT 1", (account_id,)
                ).fetchone():
                    raise OperationError("cancelled", "ACCOUNT_BUSY")
            now = self.engine.clock()
            if target and name in DELIVERY_METHODS:
                connection.execute(
                    "UPDATE pairs SET pair_dispatch_started_at=COALESCE(pair_dispatch_started_at,?),active_since=COALESCE(active_since,?) WHERE id=?",
                    (now, now, target["pair_id"]),
                )
            connection.execute(
                "INSERT INTO legacy_rpc_journal(operation_key,account_id,method,status,created_at,updated_at,lease_owner,lease_until,context) "
                "VALUES (?,?,?,'INFLIGHT',?,?,?,?,?) ON CONFLICT(operation_key) DO UPDATE SET status='INFLIGHT',updated_at=excluded.updated_at,"
                "lease_owner=excluded.lease_owner,lease_until=excluded.lease_until,error=NULL",
                (
                    key,
                    account_id,
                    name,
                    now,
                    now,
                    self.owner,
                    now + 180,
                    json.dumps({k: v for k, v in progress.items() if k != "script"}),
                ),
            )
            item = connection.execute("SELECT id FROM legacy_rpc_journal WHERE operation_key=?", (key,)).fetchone()[0]
            if target and name in DELIVERY_METHODS:
                job = connection.execute(
                    "SELECT id FROM dispatch_jobs WHERE idempotency_key=?", (f"legacy:{item}",)
                ).fetchone()
                if job is None:
                    job_id = connection.execute(
                        "INSERT INTO dispatch_jobs(idempotency_key,campaign_id,pair_id,account_id,chat_pool_id,kind,payload,version,planned_at,created_at,status,started_at,lease_owner,lease_until) "
                        "VALUES (?,?,?,?,?,'MAILING',?,?,?,?,'INFLIGHT',?,?,?)",
                        (
                            f"legacy:{item}",
                            target["campaign_id"],
                            target["pair_id"],
                            account_id,
                            target["chat_pool_id"],
                            json.dumps({"_legacy_operation_id": item, "peer_id": target["chat_id"]}),
                            target["version"],
                            now,
                            now,
                            now,
                            self.owner,
                            now + 180,
                        ),
                    ).lastrowid
                else:
                    job_id = job[0]
                    connection.execute(
                        "UPDATE dispatch_jobs SET status='INFLIGHT',started_at=?,lease_owner=?,lease_until=? WHERE id=?",
                        (now, self.owner, now + 180, job_id),
                    )
                attempt = connection.execute(
                    "INSERT INTO message_attempts(job_id,account_id,started_at) VALUES (?,?,?)",
                    (job_id, account_id, now),
                ).lastrowid
                connection.execute("UPDATE dispatch_jobs SET current_attempt_id=? WHERE id=?", (attempt, job_id))
                connection.execute("UPDATE legacy_rpc_journal SET dispatch_job_id=? WHERE id=?", (job_id, item))
                target["legacy_job_id"] = job_id
            return item, None

    def end_operation(self, operation, result, error):
        with self.engine.db.tx() as connection:
            item = connection.execute("SELECT * FROM legacy_rpc_journal WHERE id=?", (operation,)).fetchone()
            if not item or item["status"] not in {"INFLIGHT", "UNKNOWN"} or item["lease_owner"] != self.owner:
                return
            status = (
                "CONFIRMED"
                if error is None
                else (
                    "UNKNOWN"
                    if error.category == "unknown"
                    else ("CANCELLED" if error.category == "cancelled" else "FAILED")
                )
            )
            encoded = None
            if error is None:
                if hasattr(result, "_client"):
                    result = SimpleNamespace(id=getattr(result, "id", None))
                try:
                    encoded = pickle.dumps(result)
                except (TypeError, AttributeError, pickle.PicklingError):
                    encoded = pickle.dumps(SimpleNamespace(id=getattr(result, "id", None)))
                encoded = self.engine.secrets.encrypt(base64.b64encode(encoded).decode())
            connection.execute(
                "UPDATE legacy_rpc_journal SET status=?,result=?,error=?,updated_at=? WHERE id=?",
                (status, encoded, error.code if error else None, self.engine.clock(), operation),
            )
            if item["dispatch_job_id"]:
                connection.execute(
                    "UPDATE dispatch_jobs SET status=?,completed_at=?,last_error=? WHERE id=?",
                    (status, self.engine.clock(), error.code if error else None, item["dispatch_job_id"]),
                )
                connection.execute(
                    "UPDATE message_attempts SET outcome=?,error=? WHERE id=(SELECT current_attempt_id FROM dispatch_jobs WHERE id=?)",
                    (status, error.code if error else None, item["dispatch_job_id"]),
                )
            if error is None and item["method"] in {"JoinChannelRequest", "ImportChatInviteRequest"}:
                pause = int(connection.execute("SELECT value FROM settings WHERE key='join_pause'").fetchone()[0])
                connection.execute(
                    "UPDATE accounts SET next_action_at=MAX(next_action_at,?) WHERE id=?",
                    (self.engine.clock() + pause, item["account_id"]),
                )
            if error is not None and error.category == "critical":
                self.engine._critical(connection, item["account_id"], error.code, error.code)
            elif error is not None and error.category == "flood":
                self.engine._flood_wait(connection, item["account_id"], error.seconds)
            if status == "UNKNOWN":
                self.engine._notify(
                    connection,
                    f"legacy-unknown:{operation}",
                    "legacy_unknown",
                    {"operation_id": operation, "account_id": item["account_id"]},
                    80,
                )
            if error is None and item["method"] in DELIVERY_METHODS:
                context = json.loads(item["context"])
                if "pair_id" in context:
                    self.engine._record_legacy_delivery(connection, item, context, result)
            self.engine.audit(
                connection,
                "LEGACY_RPC_" + status,
                account_id=item["account_id"],
                operation_id=operation,
                method=item["method"],
                reason=error.code if error else None,
            )

    async def initialize(self):
        engine_context.set(self.engine)
        await self.engine.db.run(self.engine.recover_legacy_operations)
        for module in (
            services,
            __import__("app.legacy.original_mailing", fromlist=["logger"]),
            __import__("app.legacy.warmup_groups", fromlist=["logger"]),
            __import__("app.legacy.warmup_extras", fromlist=["logger"]),
            __import__("app.legacy.warmup_calls", fromlist=["logger"]),
            __import__("app.legacy.warmup_voices", fromlist=["logger"]),
            __import__("app.legacy.warmup_photos", fromlist=["logger"]),
        ):
            module.logger = SafeLog(self.engine, module.__name__)
        await self.service.groups.ensure()
        records = await self.engine.db.run(self.engine.db.all, "SELECT * FROM legacy_runtime_state")
        for record in records:
            json.loads(self.engine.secrets.decrypt(record["value"]))
        self.database._restore_snapshot = {record["key"]: record for record in records}
        self.mailing.db._restore_snapshot = self.database._restore_snapshot
        try:
            await self.service._restore_state()
            await self.mailing.start()
        finally:
            del self.database._restore_snapshot
            del self.mailing.db._restore_snapshot
        if getattr(self.engine, "bot_username", None):
            services._bot_username = self.engine.bot_username
            from .original_mailing import set_bot_username

            set_bot_username(self.engine.bot_username)
            self.service._own_bot_task = asyncio.create_task(self.service._own_bot_loop())

    async def run(self):
        try:
            await self.initialize()
            await asyncio.sleep(180)
            await self.service._resume_dialogs()
            if not self.service.groups._cleaned:
                self.service.groups._cleaned = True
                self.service.groups._spawn(self.service.groups.cleanup_clientless())
                self.service.groups._spawn(self.service.groups.rehide_all())
            while True:
                active = {item.id for item in await self.mailing.db.get_active_mailings()}
                for pair_id, task in list(self.mailing._tasks.items()):
                    if pair_id not in active:
                        busy = await self.engine.db.run(
                            self.engine.db.scalar,
                            "SELECT count(*) FROM legacy_rpc_journal WHERE status='INFLIGHT' AND account_id IN (SELECT account_a_id FROM pairs WHERE id=? UNION SELECT account_b_id FROM pairs WHERE id=?)",
                            (pair_id, pair_id),
                        )
                        if busy:
                            continue
                        if not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                        self.mailing._tasks.pop(pair_id, None)
                        self.mailing._mailing_locks.pop(pair_id, None)
                        self.mailing._stagger_until.pop(pair_id, None)
                if time.monotonic() - self._pruned_at > 600:
                    ids_live = await self.engine.db.run(
                        self.engine.db.all, "SELECT id FROM pair_chats WHERE removed_at IS NULL"
                    )
                    self.mailing._no_hello.intersection_update(row["id"] for row in ids_live)
                    self._pruned_at = time.monotonic()
                for pair_id in active:
                    await self.mailing._start_mailing_task(pair_id)
                await self.engine.db.run(self.engine.recover_legacy_operations)
                ids = await self.database.get_enabled_warmup_account_ids()
                batch = [aid for aid in ids if aid > self._cursor][:16] or ids[:16]
                for account_id in batch:
                    try:
                        await self.service._process_account(account_id)
                    except Exception as exc:
                        await self.engine.db.run(self.engine.diagnostic, "legacy-warmup", exc, account_id=account_id)
                    self._cursor = account_id
                await self.service._save_state()
                await asyncio.sleep(self.engine.config.worker_interval)
        finally:
            await self.close()

    async def close(self):
        self.service._stopping = True
        tasks = (
            set(self.service._dialog_tasks)
            | set(self.service.groups._tasks)
            | set(self.mailing._gate_tasks)
            | set(self.mailing._spambot_tasks)
        )
        for name in ("_task", "_own_bot_task"):
            task = getattr(self.service, name, None)
            if task:
                tasks.add(task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.mailing.stop()
        await self.service._save_state()
        self._clients.clear()
