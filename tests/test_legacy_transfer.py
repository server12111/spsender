import asyncio
import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import restart, setup_pair
from telethon import errors, types

from app.bot.interface import Interface
from app.legacy import original_mailing, services, session_import
from app.legacy.clock import engine_context
from app.legacy.database import LegacyDatabase
from app.legacy.mailing import mailing_context
from app.legacy.runtime import LegacyRuntime
from app.legacy.state_store import load_state, save_state
from app.transport.base import OperationError
from app.transport.telegram import AccountAuthorizer


class NativeClient:
    def __init__(self):
        self.sent = []
        self.error = None
        self.requests = []

    def is_connected(self):
        return True

    async def get_input_entity(self, peer):
        return types.InputPeerChat(100000)

    async def get_entity(self, peer):
        return types.InputPeerChat(100000)

    async def get_permissions(self, *args):
        return SimpleNamespace(has_left=False, is_admin=True)

    async def get_me(self):
        return SimpleNamespace(id=1001, premium=False)

    async def __call__(self, request):
        self.requests.append(request)
        return SimpleNamespace()

    async def send_message(self, peer, text, **kwargs):
        if self.error:
            raise self.error
        self.sent.append((peer, text, kwargs))
        return SimpleNamespace(id=len(self.sent), _client=self)

    async def get_messages(self, *args, **kwargs):
        return []


def native_runtime(e):
    engine_context.set(e)
    client = NativeClient()
    transport = SimpleNamespace(client=AsyncMock(return_value=client), _remember_peer=AsyncMock())
    runtime = LegacyRuntime(e, transport, "test-owner")
    return runtime, client


def test_upstream_warmup_components_are_copied_from_backup():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / "docs/legacy-source-manifest.json").read_text(encoding="utf-8"))
    for name, item in manifest["files"].items():
        if name.startswith("warmup_"):
            assert hashlib.sha256((root / "app/legacy" / name).read_bytes()).hexdigest() == item["sha256"]


def test_original_long_dialog_generator_retains_scenarios():
    assert services._WARMUP_DIALOG_MESSAGES == (50, 100)
    assert services._WARMUP_DIALOG_PER_DAY == 10
    assert services._WARMUP_DIALOG_MIN_GAP == 1800
    for _ in range(30):
        script = services._warmup_dialog_script()
        assert 6 <= len(script) <= 110
        assert {speaker for speaker, _ in script} == {0, 1}


async def test_original_database_api_uses_encrypted_account_store_and_auto_warmup(env):
    e, clock, _, _ = env
    campaign, pair, a, b, _ = setup_pair(e, 1)
    database = LegacyDatabase(e)
    engine_context.set(e)
    assert (await database.get_account(a)).phone == "+380111111111"
    assert (await database.get_warmup(a)).enabled
    e.set_warmup_partner(b, True)
    assert (await database.get_warmup_partners())[0][0] == b
    assert a in await database.get_enabled_warmup_account_ids()
    e.campaign_status(campaign, "STOPPED", confirmed=True)
    assert a not in await database.get_enabled_warmup_account_ids()
    assert not e.legacy_account_allowed(a)
    assert "session-a" not in Path(e.db.path).read_bytes().decode("latin1")
    assert pair
    assert clock()


async def test_original_stage_one_and_persistent_schedule(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    native = SimpleNamespace(get_dialogs=AsyncMock(return_value=[]))
    action, delay, joined = await runtime.service._run_stage1(native, await runtime.database.get_warmup(a), 1)
    assert action == "list_dialogs" and not joined and delay.total_seconds() >= 20 * 3600
    native.get_dialogs.assert_awaited_once_with(limit=50)
    await save_state(runtime.database, "warmup.test", {"counter": 7})
    fresh = LegacyDatabase(restart(e))
    assert await load_state(fresh, "warmup.test") == {"counter": 7}
    assert "counter" not in e.db.scalar("SELECT value FROM legacy_runtime_state WHERE key='warmup.test'")


async def test_original_mailing_delivery_is_used_and_ack_survives_restart(env):
    e, clock, _, _ = env
    campaign, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    messages = await runtime.mailing.db.get_mailing_messages(pair)
    snapshot = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    client = await runtime.get_client(a)
    token = mailing_context.set(pair)
    try:
        await runtime.mailing._send_msg(
            client, "-100000", messages[0], None, account_id=a, mailing_id=pair, target_snapshot=snapshot
        )
        assert len(native.sent) == 1 and native.sent[0][1] == messages[0].text
        assert e.db.scalar("SELECT count(*) FROM message_results") == 1
        assert e.statistics("all")["confirmed"] == 1
        assert e.db.scalar("SELECT pair_dispatch_started_at FROM pairs WHERE id=?", (pair,)) == clock()
        clock.advance(301)
        # A crash between ACK and cursor advancement must not replay the message.
        fresh = restart(e)
        second, other = native_runtime(fresh)
        await second.mailing._send_msg(
            await second.get_client(a),
            "-100000",
            messages[0],
            None,
            account_id=a,
            mailing_id=pair,
            target_snapshot=snapshot,
        )
        assert not other.sent
        assert fresh.db.scalar("SELECT count(*) FROM message_results") == 1
        runtime.mailing._target_next_msg[e.db.scalar("SELECT id FROM pair_chats")] = 1
        runtime.mailing._record_sent(e.db.scalar("SELECT id FROM pair_chats"), a, pair)
        assert e.db.scalar("SELECT message_cursor FROM pair_chats") == 1
        assert campaign
    finally:
        mailing_context.reset(token)


async def test_original_native_operation_unknown_blocks_every_further_send(env):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    native.error = TimeoutError()
    token = mailing_context.set(pair)
    try:
        message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
        client = await runtime.get_client(a)
        with pytest.raises(TimeoutError):
            await runtime.mailing._send_msg(client, "-100000", message, None, account_id=a)
        assert e.db.scalar("SELECT count(*) FROM legacy_rpc_journal WHERE status='UNKNOWN'") == 1
        assert not e.legacy_account_allowed(a)
        with pytest.raises(OperationError):
            await client.send_message("-100000", "must not be sent")
        assert not native.sent
    finally:
        mailing_context.reset(token)


async def test_legacy_floodwait_and_critical_errors_update_shared_state(env):
    e, clock, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    client = await runtime.get_client(a)
    native.error = errors.FloodWaitError(request=None, capture=50000)
    with pytest.raises(errors.FloodWaitError):
        await client.send_message(1002, "test")
    assert e.db.scalar("SELECT flood_until FROM accounts WHERE id=?", (a,)) == clock() + 50000
    await runtime.database.set_flood_wait_until(a, runtime.database.local(clock() + 21600))
    assert e.db.scalar("SELECT flood_until FROM accounts WHERE id=?", (a,)) >= clock() + 50000
    clock.advance(50001)
    native.error = errors.AuthKeyUnregisteredError(request=None)
    with pytest.raises(errors.AuthKeyUnregisteredError):
        await client.send_message(1002, "test")
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "SESSION_EXPIRED"
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='critical'") == 1


async def test_group_autoanswers_are_disabled_and_pm_chain_remains(env):
    e, _, _, _ = env
    campaign, _, a, _, _ = setup_pair(e, 1)
    with pytest.raises(ValueError, match="только в ЛС"):
        e.edit_campaign(campaign, group_reply_enabled=True)
    assert e.incoming(a, -100000, 9, 777, False, reply_to_sender_id=1001) == []
    e.edit_campaign(campaign, "dm_first", ["Hello"], dm_first_enabled=True)
    assert e.incoming(a, 777, 10, 777, True)
    assert e.db.scalar("SELECT count(*) FROM dm_reply_sequences") == 1
    view = Interface(e).templates_hub(123, campaign, "auto")
    assert "группе" not in view.text
    assert all(len(group) <= 2 for group in view.buttons)


async def test_pool_controls_and_channel_validation(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    ui = Interface(e)
    assert e.add_warmup_channels("@MyChannel\nhttps://t.me/MyChannel\nhttps://t.me/+abcde") == 2
    with pytest.raises(ValueError):
        e.add_warmup_channels("https://example.com/test")
    view = ui.warmup_pool_view(123)
    assert "Пул прогрева" in view.text
    assert all(len(group) <= 2 for group in view.buttons)
    e.set_warmup_partner(a, True, False)
    with pytest.raises(ValueError):
        e.set_warmup_partner(a, False, False)
    with pytest.raises(PermissionError):
        ui.warmup_pool_view(999)


async def test_original_group_and_media_helpers_initialize_without_network(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    await runtime.service.groups.ensure()
    await runtime.service.voices._ensure()
    await runtime.service.photos._ensure()
    await runtime.service.calls._ensure()
    assert e.db.scalar("SELECT count(*) FROM sqlite_master WHERE name='warmup_group_members'") == 1
    assert not runtime.service.voices.available()
    assert runtime.service.photos._load() == {}
    assert len(runtime.service.groups._tasks) == 0
    assert asyncio.current_task()


async def test_original_full_mailing_cycle_uses_its_chat_delay_and_persists_cursor(env, monkeypatch):
    e, _, _, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    delays = []

    async def sleep(seconds):
        delays.append(seconds)
        runtime.mailing._running = False

    module = SimpleNamespace(**{key: getattr(asyncio, key) for key in dir(asyncio) if not key.startswith("__")})
    module.sleep = sleep
    monkeypatch.setattr(original_mailing, "asyncio", module)
    runtime.mailing._no_hello.add(e.db.scalar("SELECT id FROM pair_chats"))
    runtime.mailing._running = True
    await asyncio.wait_for(runtime.mailing._mailing_loop(pair), 3)
    assert len(native.sent) == 1
    assert e.db.scalar("SELECT message_cursor FROM pair_chats") == 1
    assert 3 in delays
    await runtime.mailing._flush_last_sent()
    assert not runtime.mailing._pending_last_sent


async def test_original_dialog_replies_between_owned_accounts_without_autoreplies(env, monkeypatch):
    e, _, _, _ = env
    _, _, a, b, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)

    async def rpc(request):
        if type(request).__name__ == "ImportContactsRequest":
            phone = request.contacts[0].phone
            identity = 1002 if phone.endswith("222222222") else 1001
            return SimpleNamespace(
                users=[types.User(id=identity, first_name="Own", phone=phone.lstrip("+"), access_hash=identity)]
            )
        return SimpleNamespace()

    def rpc_wrapper(self, request):
        return rpc(request)

    monkeypatch.setattr(native.__class__, "__call__", rpc_wrapper)
    assert rpc_wrapper
    module = SimpleNamespace(**{key: getattr(asyncio, key) for key in dir(asyncio) if not key.startswith("__")})
    module.sleep = AsyncMock()
    monkeypatch.setattr(services, "asyncio", module)
    monkeypatch.setattr(services.random, "random", lambda: 1)
    monkeypatch.setattr(
        services,
        "_warmup_dialog_script",
        lambda **kwargs: [(0, "hello"), (1, "hello"), (0, "how are you"), (1, "fine")],
    )
    ca, cb = await runtime.get_client(a), await runtime.get_client(b)
    await runtime.service._run_dialog(
        await runtime.database.get_account(a), ca, await runtime.database.get_account(b), cb
    )
    assert len(native.sent) == 4
    assert not e.db.scalar("SELECT count(*) FROM dm_reply_sequences")
    assert runtime._warmup_pairs == {frozenset((1001, 1002))}
    runtime.service._release_pair(frozenset((1001, 1002)))
    assert not runtime._warmup_pairs


async def test_original_session_import_2fa_cleans_transient_files_and_clients(env, monkeypatch):
    e, _, _, _ = env
    e.config = replace(e.config, mode="live", api_id=123, api_hash="a" * 32)
    source = SimpleNamespace(disconnect=AsyncMock())
    client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(id=777, phone="380111111111", bot=False)),
        session=SimpleNamespace(save=lambda: "imported-session"),
        disconnect=AsyncMock(),
    )
    monkeypatch.setattr(session_import, "load_import_source", AsyncMock(return_value=source))
    monkeypatch.setattr(session_import, "sqlite_session_kind", lambda path: "telethon_session")
    factory = AsyncMock(side_effect=[session_import.ImportPasswordRequired(wrong=False), client])
    monkeypatch.setattr(session_import, "create_new_session", factory)
    auth = AccountAuthorizer(e, SimpleNamespace())
    assert await auth.import_file(123, "own.session", b"SQLite format 3\x00") is None
    path = Path(auth.pending[123]["directory"].name)
    assert path.is_dir()
    assert await auth.finish_import(123, "secret-2fa") == ("+380111111111", "imported-session", 777)
    assert not path.exists() and not auth.pending
    source.disconnect.assert_awaited_once()
    client.disconnect.assert_awaited_once()
    assert factory.await_args.kwargs["password"] == "secret-2fa"
    assert not e.db.scalar("SELECT count(*) FROM accounts")


def test_original_session_format_detection_and_invalid_database(tmp_path):
    assert session_import.detect_import_kind("own.session", b"") == "session"
    assert session_import.detect_import_kind("own.zip", b"") == "tdata"
    assert session_import.detect_import_kind("text.txt", b"hello") is None
    path = tmp_path / "bad.session"
    path.write_text("not a database", encoding="utf-8")
    assert session_import.sqlite_session_kind(str(path)) is None


async def test_more_than_two_template_cycles_keep_sending_and_have_distinct_receipts(env):
    e, clock, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    messages = await runtime.mailing.db.get_mailing_messages(pair)
    token = mailing_context.set(pair)
    try:
        for sequence in range(8):
            target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
            assert target.delivery_seq == sequence
            index = target.msg_index % len(messages)
            runtime.mailing._target_next_msg[target.id] = index + 1
            await runtime.mailing._send_msg(
                await runtime.get_client(a),
                target.chat_identifier,
                messages[index],
                None,
                account_id=a,
                target_snapshot=target,
            )
            runtime.mailing._record_sent(target.id, a, pair)
            clock.advance(301)
        assert [item[1] for item in native.sent] == [messages[index % 2].text for index in range(8)]
        assert e.db.scalar("SELECT count(*) FROM message_results") == 8
        assert e.db.scalar("SELECT delivery_seq FROM pair_chats") == 8
    finally:
        mailing_context.reset(token)


@pytest.mark.parametrize("performed", [False, True])
async def test_manual_unknown_resolution_updates_both_journals(env, performed):
    e, clock, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
    token = mailing_context.set(pair)
    try:
        native.error = TimeoutError()
        with pytest.raises(TimeoutError):
            await runtime.mailing._send_msg(await runtime.get_client(a), "-100000", message, None, account_id=a)
        record = e.db.one("SELECT * FROM legacy_rpc_journal WHERE status='UNKNOWN'")
        assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (record["dispatch_job_id"],)) == "UNKNOWN"
        e.resolve_unknown(record["dispatch_job_id"], performed, 987 if performed else None)
        assert e.db.scalar("SELECT status FROM legacy_rpc_journal WHERE id=?", (record["id"],)) == (
            "CONFIRMED" if performed else "CANCELLED"
        )
        if performed:
            assert e.db.scalar("SELECT api_message_id FROM message_results") == 987
            assert e.db.scalar("SELECT delivery_seq FROM pair_chats") == 1
        else:
            native.error = None
            clock.advance(1)
            await runtime.mailing._send_msg(await runtime.get_client(a), "-100000", message, None, account_id=a)
            assert len(native.sent) == 1
            assert e.db.scalar("SELECT count(*) FROM message_attempts") == 2
        assert e.statistics("all")["confirmed"] == 1
    finally:
        mailing_context.reset(token)


async def test_late_native_ack_cannot_restart_stopped_pair_clock(env):
    e, clock, _, _ = env
    campaign, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    entered, released = asyncio.Event(), asyncio.Event()
    original = native.send_message

    async def delayed(*args, **kwargs):
        entered.set()
        await released.wait()
        return await original(*args, **kwargs)

    native.send_message = delayed
    token = mailing_context.set(pair)
    try:
        message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
        task = asyncio.create_task(
            runtime.mailing._send_msg(await runtime.get_client(a), "-100000", message, None, account_id=a)
        )
        await entered.wait()
        clock.advance(30)
        e.campaign_status(campaign, "STOPPED", True)
        before = e.pair_card(pair)["elapsed"]
        clock.advance(30)
        released.set()
        await task
        clock.advance(3600)
        assert e.pair_card(pair)["elapsed"] == before
        assert e.db.scalar("SELECT count(*) FROM message_results") == 1
    finally:
        mailing_context.reset(token)


async def test_actor_daily_limit_survives_restart_before_global_snapshot(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    runtime.service._dialog_times[a] = [time.monotonic() - index * 1800 for index in range(10)]
    await (await runtime.get_client(a)).send_message(1002, "private test")
    fresh = LegacyRuntime(restart(e), runtime.transport, "fresh")
    await fresh.service._restore_state()
    assert len(fresh.service._dialog_times[a]) == 10
    account = await fresh.database.get_account(a)
    assert not await fresh.service._act_mutual_ping(account, await fresh.get_client(a))
    assert len(native.sent) == 1


async def test_state_cache_is_not_committed_when_sqlite_commit_fails(env, monkeypatch):
    e, _, _, _ = env
    database = LegacyDatabase(e)
    original = e.db.tx

    @contextmanager
    def broken():
        with original() as connection:
            yield connection
            raise sqlite3.OperationalError("simulated commit failure")

    monkeypatch.setattr(e.db, "tx", broken)
    with pytest.raises(sqlite3.OperationalError):
        await save_state(database, "warmup.failure", {"n": 1})
    assert "warmup.failure" not in database._saved_state
    monkeypatch.setattr(e.db, "tx", original)
    await save_state(database, "warmup.failure", {"n": 1})
    assert await load_state(database, "warmup.failure") == {"n": 1}


async def test_corrupted_encrypted_state_stops_startup_before_delivery(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    runtime, native = native_runtime(e)
    with e.db.tx() as connection:
        connection.execute("INSERT INTO legacy_runtime_state VALUES ('warmup.dialogs','invalid',0)")
    from cryptography.fernet import InvalidToken

    with pytest.raises(InvalidToken):
        await runtime.initialize()
    assert not native.sent and not runtime.mailing._running


async def test_chat_interval_control_and_original_stop_are_connected(env):
    e, _, _, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    chat_id = e.db.scalar("SELECT chat_pool_id FROM pair_chats WHERE pair_id=?", (pair,))
    ui = Interface(e)
    view = ui.card(123, "chats", chat_id)
    button = next(b for group in view.buttons for b in group if "Интервал этого" in b[0])
    await ui.callback(123, button[1])
    view = await ui.message(123, "90")
    assert "90 с" in view.text
    runtime, _ = native_runtime(e)
    assert (await runtime.mailing.db.get_mailing_targets(pair))[0].interval_seconds == 90
    e.set_chat_interval(chat_id, 0)
    assert (await runtime.mailing.db.get_mailing_targets(pair))[0].interval_seconds is None
    with pytest.raises(ValueError):
        e.set_chat_interval(chat_id, 29)
    await runtime.mailing.db.update_mailing_status(pair, False)
    assert e.db.scalar("SELECT status FROM campaigns WHERE id=?", (campaign,)) == "PAUSED"


async def test_shared_rpc_budget_limits_parallelism_and_releases_on_cancel(env):
    from app.transport.limits import RPCBudget

    e, _, _, _ = env
    e.set_setting("concurrency", 2)
    budget = RPCBudget(e)
    entered, release = asyncio.Event(), asyncio.Event()
    active = maximum = 0

    async def work():
        nonlocal active, maximum
        async with budget:
            active += 1
            maximum = max(maximum, active)
            if active == 2:
                entered.set()
            try:
                await release.wait()
            finally:
                active -= 1

    tasks = [asyncio.create_task(work()) for _ in range(12)]
    await asyncio.wait_for(entered.wait(), 5)
    tasks[-1].cancel()
    await asyncio.gather(tasks[-1], return_exceptions=True)
    release.set()
    await asyncio.wait_for(asyncio.gather(*tasks[:-1]), 5)
    assert maximum == 2 and budget.active == 0
