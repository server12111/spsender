import tempfile
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import setup_pair
from test_legacy_transfer import native_runtime

from app.legacy import original_mailing, session_import
from app.legacy.database import LegacyDatabase
from app.legacy.mailing import mailing_context, target_context
from app.legacy.state_store import load_state, save_state
from app.transport.telegram import AccountAuthorizer


async def test_rejoin_context_does_not_require_delivery_fields_or_start_clock(env):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    context = e.db.one(
        "SELECT pc.*,p.campaign_id,c.version FROM pair_chats pc JOIN pairs p ON p.id=pc.pair_id "
        "JOIN campaigns c ON c.id=p.campaign_id WHERE pc.pair_id=?",
        (pair,),
    )
    context["chat_id"] = -100000
    mt = mailing_context.set(pair)
    tt = target_context.set(context)
    try:
        from telethon import functions

        await (await runtime.get_client(a))(functions.channels.JoinChannelRequest("approved_chat"))
    finally:
        target_context.reset(tt)
        mailing_context.reset(mt)
    assert len(native.requests) == 1
    assert e.db.scalar("SELECT pair_dispatch_started_at FROM pairs WHERE id=?", (pair,)) is None
    assert e.db.scalar("SELECT active_since FROM pairs WHERE id=?", (pair,)) is None
    assert e.db.scalar("SELECT count(*) FROM message_results") == 0


async def test_private_join_entity_can_be_sent_using_original_target_snapshot(env):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    snapshot = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
    token = mailing_context.set(pair)
    try:
        await runtime.mailing._send_msg(
            await runtime.get_client(a),
            SimpleNamespace(id=100000),
            message,
            None,
            account_id=a,
            target_snapshot=snapshot,
        )
    finally:
        mailing_context.reset(token)
    assert len(native.sent) == 1
    assert e.db.scalar("SELECT count(*) FROM message_results") == 1


async def test_stale_flush_cannot_roll_back_atomic_delivery_cursor(env):
    e, clock, _, _ = env
    _, pair, a, b, _ = setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    with e.db.tx() as c:
        c.execute(
            "UPDATE pair_chats SET message_cursor=2,delivery_seq=2,last_sent_at=?,legacy_options=? WHERE id=?",
            (clock(), '{"last_account_id":%d}' % b, target.id),
        )
    await runtime.mailing.db.flush_target_last_sent({target.id: (original_mailing.now_moscow(), a, 1)})
    row = e.db.one("SELECT * FROM pair_chats WHERE id=?", (target.id,))
    assert row["message_cursor"] == 2 and row["delivery_seq"] == 2
    assert str(b) in row["legacy_options"]


async def test_failed_authorization_cleanup_retains_resources_for_retry(env):
    e, _, _, _ = env
    auth = AccountAuthorizer(e, SimpleNamespace())
    client = SimpleNamespace(disconnect=AsyncMock(side_effect=ConnectionError("offline")))
    source = SimpleNamespace(disconnect=AsyncMock())
    directory = tempfile.TemporaryDirectory(prefix="fe-audit-import-")
    path = Path(directory.name)
    (path / "secret.session").write_bytes(b"test session")
    auth.pending[123] = {"client": client, "source": source, "directory": directory, "at": e.clock()}
    try:
        with pytest.raises(ConnectionError):
            await auth.cancel(123)
        assert auth.pending[123]["client"] is client and path.exists()
        source.disconnect.assert_awaited_once()
        client.disconnect.side_effect = None
        await auth.cancel(123)
        assert not auth.pending and not path.exists()
        source.disconnect.assert_awaited_once()
    finally:
        client.disconnect.side_effect = None
        await auth.cancel(123)
        directory.cleanup()


async def test_source_cleanup_failure_is_not_silently_discarded():
    source = SimpleNamespace(disconnect=AsyncMock(side_effect=ConnectionError("offline")))
    with pytest.raises(ConnectionError):
        await session_import.cleanup_import_source(source)


async def test_new_actor_snapshot_wins_over_delayed_global_snapshot(env, monkeypatch):
    e, _, _, _ = env
    database, actor_db = LegacyDatabase(e), LegacyDatabase(e)
    original, triggered = e.db.run, False

    async def race(function, *args, **kwargs):
        nonlocal triggered
        if function.__name__ == "write" and not triggered:
            triggered = True
            await save_state(actor_db, "warmup.actor:1", {"times": [2]})
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(e.db, "run", race)
    await save_state(database, "warmup.dialogs", {"times": {"1": [1]}})
    assert (await load_state(database, "warmup.dialogs"))["times"]["1"] == [2]


async def test_progress_cleanup_preserves_concurrently_started_dialog(env, monkeypatch):
    e, _, _, _ = env
    database, other = LegacyDatabase(e), LegacyDatabase(e)
    original, triggered = e.db.run, False
    progress = {"a": 1, "b": 2, "pos": 3, "ts": time.time()}

    async def race(function, *args, **kwargs):
        nonlocal triggered
        if function.__name__ == "remove_stale" and not triggered:
            triggered = True
            await save_state(other, "warmup.dialog:1-2", progress)
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(e.db, "run", race)
    await save_state(database, "warmup.progress", [])
    assert await load_state(database, "warmup.progress") == [progress]


async def test_newer_progress_cannot_be_overwritten_by_delayed_snapshot(env, monkeypatch):
    e, _, _, _ = env
    database, other = LegacyDatabase(e), LegacyDatabase(e)
    original, triggered = e.db.run, False
    old = {"a": 1, "b": 2, "pos": 1, "ts": time.time()}
    new = {**old, "pos": 2}

    async def race(function, *args, **kwargs):
        nonlocal triggered
        if function.__name__ == "write" and not triggered:
            triggered = True
            await save_state(other, "warmup.dialog:1-2", new)
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(e.db, "run", race)
    await save_state(database, "warmup.progress", [old])
    assert await load_state(database, "warmup.progress") == [new]


async def test_import_connect_failure_closes_registered_source(env, monkeypatch):
    e, _, _, _ = env
    e.config = replace(e.config, mode="live", api_id=1, api_hash="a" * 32)
    native = SimpleNamespace(connect=AsyncMock(side_effect=ConnectionError("offline")), disconnect=AsyncMock())
    monkeypatch.setattr(session_import, "sqlite_session_kind", lambda path: "telethon_session")
    monkeypatch.setattr(session_import, "OpenteleTelegramClient", lambda *a, **kw: native)
    auth = AccountAuthorizer(e, SimpleNamespace())
    with pytest.raises(session_import.ImportRejected):
        await auth.import_file(123, "test.session", b"SQLite format 3\x00")
    native.disconnect.assert_awaited_once()
    assert not auth.pending


async def test_import_validation_failure_closes_new_client(env, monkeypatch):
    e, _, _, _ = env
    source = SimpleNamespace(disconnect=AsyncMock())
    native = SimpleNamespace(
        is_user_authorized=AsyncMock(side_effect=ConnectionError("offline")), disconnect=AsyncMock()
    )
    source.QRLoginToNewClient = AsyncMock(return_value=native)
    auth = AccountAuthorizer(e, SimpleNamespace())
    auth.pending[123] = {"source": source, "import": True, "at": e.clock()}
    with pytest.raises(ConnectionError):
        await auth.finish_import(123, "test-password")
    native.disconnect.assert_awaited_once()
    source.disconnect.assert_awaited_once()
    assert not auth.pending


async def test_phone_login_connect_and_disconnect_failure_keeps_retry_reference(env):
    e, _, _, _ = env
    e.config = replace(e.config, mode="live")
    client = SimpleNamespace(
        connect=AsyncMock(side_effect=ConnectionError("offline")),
        disconnect=AsyncMock(side_effect=ConnectionError("closing failed")),
    )
    auth = AccountAuthorizer(e, SimpleNamespace(new_client=lambda **kw: client))
    with pytest.raises(ConnectionError):
        await auth.begin(123, "+380111111111")
    assert auth.pending[123]["client"] is client
    client.disconnect.side_effect = None
    await auth.cancel(123)
    assert not auth.pending


async def test_import_retry_after_cleanup_failure_reuses_existing_client(env, monkeypatch):
    e, _, _, _ = env
    client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(id=777, phone="380111111111", bot=False)),
        session=SimpleNamespace(save=lambda: "session"),
        disconnect=AsyncMock(side_effect=ConnectionError("offline")),
    )
    source = SimpleNamespace(disconnect=AsyncMock())
    factory = AsyncMock(return_value=client)
    monkeypatch.setattr(session_import, "create_new_session", factory)
    auth = AccountAuthorizer(e, SimpleNamespace())
    auth.pending[123] = {"source": source, "import": True, "at": e.clock()}
    with pytest.raises(ConnectionError):
        await auth.finish_import(123, "password")
    assert auth.pending[123]["client"] is client
    client.disconnect.side_effect = None
    assert await auth.finish_import(123, "password") == ("+380111111111", "session", 777)
    factory.assert_awaited_once()
    assert not auth.pending


async def test_confirmed_join_does_not_suppress_a_later_membership_attempt(env):
    from telethon import functions

    e, clock, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    client = await runtime.get_client(a)
    await client(functions.channels.JoinChannelRequest("approved_chat"))
    clock.advance(101)
    await client(functions.channels.JoinChannelRequest("approved_chat"))
    assert len(native.requests) == 2
    assert e.db.scalar("SELECT count(*) FROM legacy_rpc_journal WHERE status='CONFIRMED'") == 2


@pytest.mark.parametrize("private", [False, True])
async def test_already_participant_is_a_known_join_success(env, monkeypatch, private):
    from telethon import errors, functions

    e, clock, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    monkeypatch.setattr(
        type(native), "__call__", AsyncMock(side_effect=errors.UserAlreadyParticipantError(request=None))
    )
    request = (
        functions.messages.ImportChatInviteRequest("test_hash")
        if private
        else functions.channels.JoinChannelRequest("approved_chat")
    )
    await (await runtime.get_client(a))(request)
    assert e.db.scalar("SELECT status FROM legacy_rpc_journal") == "CONFIRMED"
    assert e.db.scalar("SELECT next_action_at FROM accounts WHERE id=?", (a,)) > clock()
    assert e.legacy_account_allowed(a)
