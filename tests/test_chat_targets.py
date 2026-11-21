import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import restart, setup_pair
from telethon import errors, types
from test_bot import Harness

from app.bot.interface import Interface
from app.db import Database
from app.scheduler.worker import Worker
from app.services.core import DomainError
from app.transport.base import OperationError
from app.transport.telegram import TelegramTransport


def queue(e, value="@local_chat", **options):
    with e.db.tx() as c:
        return e._queue_chat_target(c, value, approved=True, **options)


def mock_live(e):
    e.config = replace(e.config, mode="live")
    transport = TelegramTransport(e)
    client = SimpleNamespace(
        get_entity=AsyncMock(
            return_value=types.Chat(
                id=17, title="Local", photo=types.ChatPhotoEmpty(), participants_count=0, date=None, version=1
            )
        ),
        disconnect=AsyncMock(),
    )
    transport.client = AsyncMock(return_value=client)
    transport._remember_peer = AsyncMock()
    return transport, client


async def test_live_import_227_usernames_without_accounts_or_network(env):
    e, _, _, _ = env
    e.config = replace(e.config, mode="live")
    transport = SimpleNamespace(resolve_target=AsyncMock(side_effect=AssertionError("No import RPC")))
    h = Harness(e, transport)
    try:
        h.ui.set_state(123, "import_chats")
        await h.text("\n".join(f"@group_{n:04d}" for n in range(227)))
        await h.click("Есть право")
        await h.click("Импортировать")
        assert "Обработано: 227" in h.last_text()
        assert "Ошибок: 0" in h.last_text()
        assert "Пул чатов · 227" in h.last_text()
        assert e.db.scalar("SELECT count(*) FROM chat_targets WHERE status='PENDING'") == 227
        assert not e.db.scalar("SELECT count(*) FROM chat_pool")
        transport.resolve_target.assert_not_awaited()
        assert restart(e).db.scalar("SELECT count(*) FROM chat_targets") == 227
        await h.click("@group_")
        assert "сохранён" in h.last_text()
        await h.click("Удалить из пула")
        assert "Пул чатов · 226" in h.last_text()
        assert all(len(group) <= 2 for group in h.ui.listing(123, "chats").buttons)
    finally:
        await h.bot.session.close()


def test_symbolic_aliases_topics_and_numeric_chat_imports_deduplicate(env):
    e, _, _, _ = env
    for value in ("local_chat", "@LOCAL_CHAT", "https://t.me/local_chat", "telegram.me/local_chat"):
        queue(e, value)
    queue(e, "https://t.me/local_chat/12")
    queue(e, "-100123")
    assert e.db.scalar("SELECT count(*) FROM chat_targets") == 2
    assert e.db.scalar("SELECT count(*) FROM chat_pool") == 1
    assert "Пул чатов · 3" in Interface(e).listing(123, "chats").text
    assert e.statistics()["totals"]["chat_pool"] == 3


async def test_no_account_does_not_claim_or_consume_retries(env):
    e, _, _, _ = env
    queue(e)
    transport, _ = mock_live(e)
    assert await transport.resolve_pending_targets("w") == 0
    transport.client.assert_not_awaited()
    assert e.db.one("SELECT status,attempts FROM chat_targets") == {"status": "PENDING", "attempts": 0}


@pytest.mark.parametrize("state", ["silent", "paused", "flood", "backoff", "restricted", "campaign_paused", "unknown"])
async def test_unavailable_accounts_never_check_imported_chats(env, state):
    e, clock, _, _ = env
    campaign, _, a, _, _ = setup_pair(e, 1)
    queue(e)
    with e.db.tx() as c:
        c.execute("UPDATE accounts SET paused=1 WHERE id<>?", (a,))
        if state == "silent":
            c.execute("UPDATE accounts SET onboarding_started_at=? WHERE id=?", (clock(), a))
        elif state == "paused":
            c.execute("UPDATE accounts SET paused=1 WHERE id=?", (a,))
        elif state == "flood":
            e._flood_wait(c, a, 3600)
        elif state == "backoff":
            c.execute("UPDATE accounts SET connection_retry_at=? WHERE id=?", (clock() + 3600, a))
        elif state == "restricted":
            c.execute("UPDATE accounts SET account_health='RESTRICTED' WHERE id=?", (a,))
        elif state == "campaign_paused":
            c.execute("UPDATE campaigns SET status='PAUSED' WHERE id=?", (campaign,))
        else:
            c.execute(
                "INSERT INTO legacy_rpc_journal(operation_key,account_id,method,status,created_at,updated_at) "
                "VALUES ('test-unknown',?,'send_message','UNKNOWN',?,?)",
                (a, clock(), clock()),
            )
    transport, _ = mock_live(e)
    assert await transport.resolve_pending_targets("w") == 0
    transport.client.assert_not_awaited()
    assert e.db.scalar("SELECT status FROM chat_targets") == "PENDING"


async def test_worker_resolves_restart_import_preserves_permissions_and_allocates(env):
    e, _, _, sink = env
    _, pair, _, _, _ = setup_pair(e, 1)
    # Expand the pair to leave a genuine deficit for the newly imported chat.
    with e.db.tx() as c:
        c.execute("UPDATE pairs SET chat_limit=2 WHERE id=?", (pair,))
    queue(e, "@local_chat/12", join_allowed=True, greeting_allowed=True, split_allowed=True)
    e = restart(e)
    transport, client = mock_live(e)
    worker = Worker(e, transport, sink)
    # Dispatch is intentionally disabled; this test covers only import checks.
    worker.run_batch = AsyncMock(return_value=0)
    await worker.run_once()
    target = e.db.one("SELECT * FROM chat_targets")
    chat = e.db.one("SELECT * FROM chat_pool WHERE id=?", (target["chat_pool_id"],))
    assert target["status"] == "RESOLVED"
    assert chat["chat_id"] == -17 and chat["topic_id"] == 12
    assert all(chat[k] for k in ("approved", "join_allowed", "greeting_allowed", "split_allowed"))
    assert e.db.scalar("SELECT pair_id FROM pair_chats WHERE chat_pool_id=?", (chat["id"],)) == pair
    await worker.run_once()
    client.get_entity.assert_awaited_once_with("@local_chat")
    assert not e.db.scalar("SELECT count(*) FROM message_results")
    assert "Пул чатов · 2" in Interface(e).listing(123, "chats").text


async def test_resolver_processes_at_most_four_chats_per_pass(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    for n in range(10):
        queue(e, f"@local_chat{n}")
    transport, client = mock_live(e)
    assert await transport.resolve_pending_targets("w", limit=1000) == 4
    assert client.get_entity.await_count == 4
    assert e.db.scalar("SELECT count(*) FROM chat_targets WHERE status='PENDING'") == 6
    # Different aliases resolving to the same real peer never duplicate allocation.
    assert e.db.scalar("SELECT count(*) FROM chat_pool WHERE chat_id=-17") == 1


async def test_floodwait_is_persisted_and_stops_batch(env):
    e, clock, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    queue(e)
    queue(e, "@second_chat")
    transport, client = mock_live(e)
    client.get_entity.side_effect = errors.FloodWaitError(request=None, capture=7200)
    assert await transport.resolve_pending_targets("w") == 0
    assert client.get_entity.await_count == 1
    assert e.db.scalar("SELECT flood_until FROM accounts WHERE id=?", (a,)) >= clock() + 7200
    target = e.db.one("SELECT * FROM chat_targets ORDER BY id LIMIT 1")
    assert target["next_attempt_at"] >= clock() + 7200
    assert target["attempts"] == 0 and target["status"] == "PENDING"


async def test_authorization_loss_uses_critical_account_notifications(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    queue(e)
    transport, client = mock_live(e)
    client.get_entity.side_effect = errors.AuthKeyUnregisteredError(request=None)
    await transport.resolve_pending_targets("w")
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "SESSION_EXPIRED"
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE priority>=90") >= 1


async def test_temporary_failure_has_bounded_exponential_retries(env):
    e, clock, _, _ = env
    setup_pair(e, 1)
    e.set_setting("max_retries", "2")
    e.set_setting("retry_base", "10")
    queue(e)
    transport, client = mock_live(e)
    client.get_entity.side_effect = ConnectionError("secret must not be logged")
    delays = []
    for _ in range(3):
        assert await transport.resolve_pending_targets("w") == 0
        target = e.db.one("SELECT * FROM chat_targets")
        delays.append(target["next_attempt_at"] - clock())
        assert "secret" not in str(e.db.all("SELECT * FROM service_diagnostics"))
        clock.advance(delays[-1])
    assert delays == [10, 20, 40]
    assert target["status"] == "REVIEW" and target["attempts"] == 3
    assert await transport.resolve_pending_targets("w") == 0
    assert client.get_entity.await_count == 3


async def test_user_peer_is_rejected_and_retry_is_explicit(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    target_id = queue(e)
    transport, client = mock_live(e)
    client.get_entity.return_value = types.User(id=777, first_name="User")
    await transport.resolve_pending_targets("w")
    assert e.db.scalar("SELECT status FROM chat_targets") == "REVIEW"
    assert not e.db.scalar("SELECT count(*) FROM chat_pool WHERE chat_id=-777")
    # Repeated imports do not restart the failed check or consume more RPCs.
    queue(e)
    await transport.resolve_pending_targets("w")
    client.get_entity.assert_awaited_once()
    assert "пользователь или бот" in Interface(e).chat_target_card(123, target_id).text
    e.change_chat_target(target_id, "retry")
    assert e.db.scalar("SELECT status FROM chat_targets") == "PENDING"
    with pytest.raises(DomainError):
        e.change_chat_target(target_id, "retry")
    with pytest.raises(PermissionError):
        Interface(e).chat_target_card(999, target_id)


def test_leases_prevent_duplicate_claim_and_survive_process_restart(env):
    e, clock, _, _ = env
    queue(e)
    first = e.claim_chat_target("w")
    other = restart(e)
    assert other.claim_chat_target("w2") is None
    clock.advance(181)
    second = other.claim_chat_target("w2")
    resolved = (-17, 0, "@local_chat", "Chat")
    assert not e.finish_chat_target(first, resolved)
    assert other.finish_chat_target(second, resolved)
    assert not other.finish_chat_target(second, resolved)
    assert e.db.scalar("SELECT count(*) FROM chat_pool") == 1


def test_deleted_inflight_chat_is_not_resurrected_by_late_result(env):
    e, _, _, _ = env
    target_id = queue(e)
    target = e.claim_chat_target("w")
    e.change_chat_target(target_id, "remove")
    assert not e.finish_chat_target(target, (-17, 0, "@local_chat", "Chat"))
    e.fail_chat_target(target, OperationError("unknown", "TIMEOUT"))
    assert e.db.scalar("SELECT status FROM chat_targets") == "REMOVED"
    assert not e.db.scalar("SELECT count(*) FROM chat_pool")
    queue(e, join_allowed=True)
    assert e.db.scalar("SELECT status FROM chat_targets") == "PENDING"
    assert e.db.scalar("SELECT join_allowed FROM chat_targets") == 1


async def test_cancelled_read_recovered_after_lease_without_any_send(env):
    e, clock, _, _ = env
    setup_pair(e, 1)
    queue(e)
    transport, client = mock_live(e)
    entered = asyncio.Event()

    async def hang(*args):
        entered.set()
        await asyncio.Event().wait()

    client.get_entity.side_effect = hang
    task = asyncio.create_task(transport.resolve_pending_targets("w"))
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert transport._operation_semaphore.active == 0
    assert e.db.scalar("SELECT status FROM chat_targets") == "RESOLVING"
    clock.advance(181)
    client.get_entity.side_effect = None
    assert await transport.resolve_pending_targets("next-runtime") == 1


def test_cancel_import_rechecks_revision_before_saving_reference(env):
    e, _, _, _ = env
    ui = Interface(e)
    ui.import_confirmation(123, {"lines": ["@local_chat"], "approved": True})
    revision = ui.state(123)["data"]["_revision"]
    ui.clear(123)
    ui.set_state(123, "account_phone")
    with pytest.raises(DomainError, match="отменён"):
        ui._import_chat_reference(123, revision, "@local_chat", {"approved": True})
    assert not e.db.scalar("SELECT count(*) FROM chat_targets")


def test_migration_14_preserves_existing_pool_and_sessions(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    old_chats = e.db.all("SELECT * FROM chat_pool")
    old_sessions = e.db.all("SELECT * FROM account_sessions")
    with e.db.tx() as c:
        c.execute("DROP INDEX chat_pool_link")
        c.execute("DROP TABLE chat_targets")
        c.execute("DELETE FROM schema_migrations WHERE version LIKE '014%'")
    Database(e.db.path).migrate()
    assert e.db.all("SELECT * FROM chat_pool") == old_chats
    assert e.db.all("SELECT * FROM account_sessions") == old_sessions
    assert not e.db.all("PRAGMA foreign_key_check")


async def test_pause_during_connection_prevents_chat_rpc(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    queue(e)
    transport, client = mock_live(e)

    async def paused_client(account_id):
        with e.db.tx() as c:
            c.execute("UPDATE accounts SET paused=1 WHERE id=?", (a,))
        return client

    transport.client.side_effect = paused_client
    assert await transport.resolve_pending_targets("w") == 0
    client.get_entity.assert_not_awaited()
    assert e.db.scalar("SELECT attempts FROM chat_targets") == 0


async def test_configured_batch_size_limits_checks(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    e.set_setting("batch_size", "1")
    for n in range(3):
        queue(e, f"@local_chat{n}")
    transport, client = mock_live(e)
    assert await transport.resolve_pending_targets("w") == 1
    assert client.get_entity.await_count == 1


async def test_private_invite_check_does_not_join_or_request_ids(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    target_id = queue(e, "https://t.me/+privateHash", join_allowed=True)
    transport, _ = mock_live(e)
    check_invite = AsyncMock(return_value=SimpleNamespace(chat=None))
    transport.client.return_value = check_invite
    assert await transport.resolve_pending_targets("w") == 0
    assert type(check_invite.await_args.args[0]).__name__ == "CheckChatInviteRequest"
    assert check_invite.await_count == 1
    assert "закрытому чату" in Interface(e).chat_target_card(123, target_id).text


async def test_pending_chat_search_and_status_filter_through_ui(env):
    e, _, transport, _ = env
    queue(e)
    queue(e, "@second_chat")
    with e.db.tx() as c:
        c.execute("UPDATE chat_targets SET status='REVIEW' WHERE link='@second_chat'")
    h = Harness(e, transport)
    try:
        await h.text("/start")
        await h.click("Пул чатов")
        await h.click("Статус")
        await h.click("Нужна проверка")
        assert "Пул чатов · 1" in h.last_text()
        await h.click("@second_chat")
        await h.click("Повторить проверку")
        assert e.db.scalar("SELECT status FROM chat_targets WHERE link='@second_chat'") == "PENDING"
        assert "Пул чатов · 1" in h.ui.listing(123, "chats", search="local_chat").text
    finally:
        await h.bot.session.close()


async def test_slow_chat_check_does_not_block_account_connections(env):
    e, _, transport, sink = env
    e.config = replace(e.config, worker_interval=0.05)
    checking, synced = asyncio.Event(), asyncio.Event()
    count = 0

    async def check(owner):
        checking.set()
        await asyncio.Event().wait()

    async def sync():
        nonlocal count
        count += 1
        if checking.is_set() and count >= 3:
            synced.set()

    transport.resolve_pending_targets = check
    transport.sync_listeners = sync
    worker = Worker(e, transport, sink)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(synced.wait(), 3)
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not e.db.scalar("SELECT count(*) FROM worker_leases")


def test_resolved_chat_reimport_does_not_create_pending_duplicate(env):
    e, _, _, _ = env
    target_id = queue(e)
    target = e.claim_chat_target("w")
    assert e.finish_chat_target(target, (-17, 0, "@local_chat", "Chat"))
    queue(e, "https://t.me/local_chat")
    assert e.db.scalar("SELECT count(*) FROM chat_targets") == 1
    ui = Interface(e)
    assert "Пул чатов · 1" in ui.listing(123, "chats").text
    assert "ID -17" in ui.chat_target_card(123, target_id).text
    with pytest.raises(DomainError):
        e.change_chat_target(target_id, "remove")
