"""Regressions reproduced during the repository audit; all APIs are local fakes."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from weakref import WeakValueDictionary

import pytest
from conftest import restart, setup_pair
from telethon import errors

from app.bot.interface import Interface
from app.bot.runtime import BotNotificationSink
from app.services.core import DomainError
from app.transport.base import OperationError, Result
from app.transport.telegram import TelegramTransport, classify_error


def adapter(engine, client=None):
    transport = object.__new__(TelegramTransport)
    transport.engine = engine
    if client is not None:
        transport.client = AsyncMock(return_value=client)
    return transport


def action(view, engine, name, *, index=None):
    for group in view.buttons:
        for _, callback in group:
            token = engine.db.one("SELECT * FROM ui_actions WHERE token=?", (callback[2:],))
            if token["action"] == name and (index is None or json.loads(token["data"]).get("index") == index):
                return callback
    raise AssertionError(f"Missing action: {name}")


def dm(engine, *, first=False):
    campaign, pair, a, b, chats = setup_pair(engine, 1)
    kind = "dm_first" if first else "dm_always"
    engine.edit_campaign(campaign, kind, ["Reply"], **{kind + "_enabled": True})
    active = engine.pair_card(pair)["active_account_id"]
    assert engine.incoming(active, 500, 1, 500, True)
    return campaign, pair, active


@pytest.mark.parametrize("private", [True, False])
async def test_real_incoming_adapter_reaches_domain_handler(env, private):
    e, _, _, _ = env
    campaign, pair, _, _, chats = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    telegram_id = e.db.scalar("SELECT telegram_id FROM accounts WHERE id=?", (active,))
    e.edit_campaign(campaign, "dm_always", ["Reply"], dm_always_enabled=True)
    event = SimpleNamespace(
        raw_text="Hello",
        is_private=private,
        is_reply=not private,
        get_sender=AsyncMock(return_value=SimpleNamespace(id=500, bot=False)),
        get_reply_message=AsyncMock(return_value=SimpleNamespace(sender_id=telegram_id)),
        chat_id=500 if private else e.db.scalar("SELECT chat_id FROM chat_pool WHERE id=?", (chats[0],)),
        id=1,
        out=False,
        message=SimpleNamespace(reply_to=None),
    )
    await adapter(e)._handle_incoming(active, event)
    assert event.get_sender.await_count == 1
    assert e.db.scalar("SELECT count(*) FROM incoming_events") == int(private)
    assert e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE kind IN ('DM_ALWAYS','GROUP_REPLY')") == int(private)
    assert not e.db.scalar("SELECT count(*) FROM service_diagnostics")


@pytest.mark.parametrize("text", ["Пройдите проверку", "Подтвердите, что вы человек", "captcha", "Я не робот"])
async def test_captcha_messages_only_request_manual_verification(env, text):
    e, _, _, _ = env
    _, pair, _, _, chats = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    event = SimpleNamespace(
        raw_text=text,
        is_private=False,
        is_reply=False,
        get_sender=AsyncMock(return_value=SimpleNamespace(id=500, bot=True)),
        chat_id=-100000,
    )
    await adapter(e)._handle_incoming(active, event)
    assert (
        e.db.scalar(
            "SELECT manual_required FROM account_chat_access WHERE account_id=? AND chat_pool_id=?", (active, chats[0])
        )
        == 1
    )
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='manual'") == 1
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs")


@pytest.mark.parametrize("recovery", ["incoming", "resume", "edit"])
async def test_failed_dm_recovers_without_replaying_confirmed_sticker(env, recovery):
    e, clock, api, _ = env
    campaign, _, active = dm(e, first=True)
    assert await e.run_one(api)
    clock.advance(4)
    for _ in range(e.config.max_retries + 1):
        job = e.claim_job("fail-phrase")
        assert job and job["payload"]["dm_step"] == 1
        e.fail_job(job, OperationError("transient", "NETWORK_FAILURE"))
        clock.advance(600)
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "FAILED"
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='reply_failed'") == 1
    e = restart(e)
    e.tick()
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "FAILED"
    if recovery == "incoming":
        assert e.incoming(active, 500, 2, 500, True)
    elif recovery == "resume":
        e.campaign_status(campaign, "PAUSED")
        e.campaign_status(campaign, "RUNNING", confirmed=True)
        e.tick()
    else:
        e.edit_campaign(campaign, "dm_first", ["Updated"])
        e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["payload"]["dm_step"] == 1
    assert sum(job["payload"].get("dm_step") == 0 for job in api.calls) == 1


@pytest.mark.parametrize("change", ["remove", "reassign"])
def test_detaching_account_releases_its_dm_sequence(env, change):
    e, _, _, _ = env
    campaign, pair, active = dm(e)
    if change == "remove":
        e.update_account(active, "remove")
    else:
        other = e.create_campaign("Other", ["Other"])
        e.assign_account(active, other)
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "CANCELLED"
    assert e.db.scalar("SELECT reserved_job_id FROM reply_history") is None
    new, _ = e.add_account("Replacement", "+10000000099", "session", 9999, campaign)
    with e.db.tx() as c:
        c.execute(
            "UPDATE accounts SET onboarding_stage='READY',onboarding_started_at=? WHERE id=?",
            (e.clock() - 28 * 3600, new),
        )
    next_active = e.pair_card(pair)["active_account_id"]
    assert e.incoming(next_active, 500, 2, 500, True)


def test_deleted_campaign_does_not_break_free_accounts_or_list_dead_pairs(env):
    e, _, _, _ = env
    campaign, _, a, _, _ = setup_pair(e, 1)
    e.campaign_status(campaign, "REMOVED", confirmed=True)
    ui = Interface(e)
    assert "Свободный пул" in ui.card(123, "accounts", a).text
    view = ui.listing(123, "pairs")
    assert not any(
        e.db.scalar("SELECT action FROM ui_actions WHERE token=?", (callback[2:],)) == "card"
        for group in view.buttons
        for _, callback in group
    )
    assert e.statistics()["totals"]["pairs"] == 0


def test_removed_account_can_be_added_again_with_fresh_onboarding(env):
    e, clock, _, _ = env
    a, _ = e.add_account("Old", "+10000000001", "old-session", 1001)
    e.update_account(a, "remove")
    clock.advance(600)
    new, _ = e.add_account("New", "+10000000001", "new-session", 1001)
    assert new != a
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "REMOVED"
    assert e.db.scalar("SELECT onboarding_started_at FROM accounts WHERE id=?", (new,)) == clock()
    assert e.db.scalar("SELECT onboarding_stage FROM accounts WHERE id=?", (new,)) == "SILENT_24H"
    with pytest.raises(DomainError, match="уже добавлен"):
        e.add_account("Duplicate", "+10000000001", "session", 1001)


def test_planning_batch_setting_takes_effect_immediately(env, monkeypatch):
    e, _, _, _ = env
    campaign, _, _, _, _ = setup_pair(e, 1)
    for i in range(8):
        e.add_account(str(i), "+10000000099", "session", 2000 + i, campaign)
    visited = []
    monkeypatch.setattr(e, "_onboard", lambda c, p, ca: visited.append(p["id"]))
    e.set_setting("planning_batch_size", 1)
    e.tick()
    assert len(visited) == 1
    e.set_setting("planning_batch_size", 3)
    e.tick()
    assert len(visited) == 4


async def test_pending_check_is_recreated_in_new_campaign(env):
    e, clock, api, _ = env
    old = e.create_campaign("Old", ["Old"])
    a, _ = e.add_account("A", "+10000000001", "session", 1001)
    e.campaign_status(old, "RUNNING", confirmed=True)
    clock.advance(86401)
    e.tick()
    old_job = e.db.one("SELECT * FROM dispatch_jobs")
    new = e.create_campaign("New", ["New"])
    e.campaign_status(new, "RUNNING", confirmed=True)
    e.assign_account(a, new)
    e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["campaign_id"] == new
    assert e.db.scalar("SELECT prepared_at FROM accounts WHERE id=?", (a,)) == clock()
    assert e.db.scalar("SELECT campaign_id FROM dispatch_jobs WHERE id=?", (old_job["id"],)) == old
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (old_job["id"],)) == "CANCELLED"


@pytest.mark.parametrize("claimed", [False, True])
async def test_revoked_join_permission_prevents_queued_and_claimed_requests(env, claimed):
    e, clock, _, _ = env
    _, _, _, _, chats = setup_pair(e, 1, ready=False)
    with e.db.tx() as c:
        c.execute("UPDATE accounts SET onboarding_started_at=?,prepared_at=?", (clock() - 25 * 3600 - 1, clock()))
    e.tick()
    job = e.claim_job("join") if claimed else e.db.one("SELECT * FROM dispatch_jobs ORDER BY id LIMIT 1")
    e.set_chat_permissions(chats[0], True, False)
    if claimed:
        client = AsyncMock()
        with pytest.raises(OperationError) as exc:
            await adapter(e, client).execute(job)
        assert exc.value.category == "cancelled"
        client.assert_not_awaited()
    else:
        assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "CANCELLED"
        e.tick()
        new = e.claim_job("new-permissions")
        assert new and not new["payload"]["join_allowed"]


async def test_pair_chats_button_opens_real_list(env):
    e, _, _, _ = env
    _, pair, _, _, chats = setup_pair(e, 1)
    ui = Interface(e)
    callback = ui.button(123, "Chats", "pair_chats", {"id": pair})[1]
    view = await ui.callback(123, callback)
    assert "Всего чатов: 1" in view.text
    assert any(
        json.loads(e.db.scalar("SELECT data FROM ui_actions WHERE token=?", (cb[2:],))).get("id") == chats[0]
        for group in view.buttons
        for _, cb in group
    )


async def test_old_edit_button_cannot_change_another_template(env):
    e, _, _, _ = env
    campaign = e.create_campaign("Campaign", ["A", "B", "C"])
    ui = Interface(e)
    callback = action(ui.template_list(123, campaign, "mailing"), e, "template_edit", index=1)
    e.edit_campaign(campaign, "mailing", ["B", "C"])
    with pytest.raises(DomainError, match="устарела"):
        await ui.callback(123, callback)
    assert [t["text"] for t in e.templates(campaign)] == ["B", "C"]


@pytest.mark.parametrize("failure", [errors.UserIsBlockedError, errors.YouBlockedUserError])
async def test_recipient_rejection_never_blocks_other_pair_deliveries(env, failure):
    e, _, api, _ = env
    _, _, _ = dm(e)
    job = e.claim_job("blocked-peer")
    e.fail_job(job, classify_error(failure(request=None)))
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "FAILED"
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "FAILED"
    e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["kind"] == "MAILING"
    sink = BotNotificationSink(None, Interface(e))
    payload = json.loads(e.db.scalar("SELECT payload FROM notifications_outbox WHERE kind='reply_failed'"))
    assert "не отправлен" in sink.build_view(123, "reply_failed", payload).text


async def test_wizard_save_rejects_removed_selection_without_partial_campaign(env):
    e, _, _, _ = env
    a, _ = e.add_account("A", "+10000000001", "session", 1001)
    b, _ = e.add_account("B", "+10000000002", "session", 1002)
    ui = Interface(e)
    view = ui.wizard(
        123, {"step": 9, "name": "New", "texts": [{"text": "Hello"}], "interval": 300, "limit": 1, "accounts": [a, b]}
    )
    callback = action(view, e, "wizard_save")
    e.update_account(b, "remove")
    with pytest.raises(DomainError):
        await ui.callback(123, callback)
    assert e.db.scalar("SELECT count(*) FROM campaigns") == 0
    assert e.db.scalar("SELECT count(*) FROM campaign_members") == 0


def test_campaign_and_assignments_roll_back_together_on_database_failure(env, monkeypatch):
    import sqlite3

    e, _, _, _ = env
    a, _ = e.add_account("A", "+10000000001", "session", 1001)
    b, _ = e.add_account("B", "+10000000002", "session", 1002)
    assign = e._assign_account

    def failure(c, account, campaign):
        if account == b:
            raise sqlite3.OperationalError("disk I/O error")
        assign(c, account, campaign)

    monkeypatch.setattr(e, "_assign_account", failure)
    with pytest.raises(sqlite3.OperationalError):
        e.create_campaign("New", ["Hello"], account_ids=[a, b], creation_key="draft")
    for table in ("campaigns", "campaign_members", "pairs", "campaign_creation_receipts"):
        assert e.db.scalar(f"SELECT count(*) FROM {table}") == 0


def test_campaign_creation_receipt_makes_retries_idempotent(env):
    e, _, _, _ = env
    a, _ = e.add_account("A", "+10000000001", "session", 1001)
    first = e.create_campaign("New", ["Hello"], account_ids=[a], creation_key="draft")
    second = e.create_campaign("New", ["Hello"], account_ids=[a], creation_key="draft")
    assert first == second
    assert e.db.scalar("SELECT count(*) FROM campaigns") == 1


class ConnectionClient:
    def __init__(self, *, failed=False):
        self.failed = failed

    async def connect(self):
        if self.failed:
            raise ConnectionError("offline")

    async def disconnect(self):
        pass

    async def is_user_authorized(self):
        return True

    def is_connected(self):
        return False

    def on(self, event):
        return lambda callback: callback


async def test_successful_connection_resets_only_network_backoff(env):
    e, clock, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    transport = adapter(e)
    transport.clients, transport._client_sessions = {}, {}
    transport._client_locks = WeakValueDictionary()
    transport._start_incoming = lambda: None
    transport.new_client = lambda session: ConnectionClient(failed=True)
    with pytest.raises(OperationError):
        await transport.client(a)
    assert e.db.scalar("SELECT connection_failures FROM accounts WHERE id=?", (a,)) == 1
    clock.advance(6)
    e.critical(a, "RESTRICTED")
    transport.new_client = lambda session: ConnectionClient()
    await transport.client(a)  # Explicit authorization check must not clear moderation restrictions.
    record = e.db.one("SELECT * FROM accounts WHERE id=?", (a,))
    assert record["connection_failures"] == 0 and record["connection_retry_at"] == 0
    assert record["account_health"] == "RESTRICTED" and record["paused"] == 1


async def test_incoming_metadata_flood_wait_stops_further_rpcs(env):
    e, clock, _, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    event = SimpleNamespace(
        raw_text="Hello",
        is_private=True,
        is_reply=False,
        get_sender=AsyncMock(side_effect=errors.FloodWaitError(request=None, capture=90)),
    )
    transport = adapter(e)
    await transport._handle_incoming(active, event)
    assert e.db.scalar("SELECT flood_until FROM accounts WHERE id=?", (active,)) == clock() + 90
    await transport._handle_incoming(active, event)
    assert event.get_sender.await_count == 1


def test_concurrent_flood_reports_cannot_shorten_existing_wait(env):
    e, clock, _, _ = env
    _, _, _, _, _ = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("outgoing")
    e.flood_wait(job["account_id"], 100)
    e.fail_job(job, OperationError("flood", "FLOOD_WAIT", 10))
    assert e.db.scalar("SELECT flood_until FROM accounts WHERE id=?", (job["account_id"],)) == clock() + 100
    clock.advance(11)
    assert e.claim_job("too-early") is None


async def test_old_template_error_does_not_pause_corrected_campaign(env):
    e, _, api, _ = env
    campaign, _, _, _, _ = setup_pair(e, 1)
    e.tick()
    old_job = e.claim_job("old-template")
    e.edit_campaign(campaign, "mailing", ["Fixed"])
    e.fail_job(old_job, OperationError("configuration", "MEDIA_UNAVAILABLE"))
    assert e.db.scalar("SELECT status FROM campaigns WHERE id=?", (campaign,)) == "RUNNING"
    e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["payload"]["text"] == "Fixed"


async def test_next_shift_supersedes_failed_dm_without_reviving_old_account(env):
    e, clock, api, _ = env
    campaign, pair, active = dm(e, first=True)
    assert await e.run_one(api)
    clock.advance(4)
    job = e.claim_job("failed-phrase")
    e.fail_job(job, OperationError("recipient", "RECIPIENT_UNAVAILABLE"))
    old = e.db.scalar("SELECT id FROM dm_reply_sequences")
    clock.advance(6 * 3600)
    next_active = e.pair_card(pair)["active_account_id"]
    assert next_active != active
    assert e.incoming(next_active, 500, 2, 500, True)
    e.edit_campaign(campaign, "dm_first", ["Updated"])
    e.tick()
    assert e.db.scalar("SELECT status FROM dm_reply_sequences WHERE id=?", (old,)) == "CANCELLED"
    assert e.db.scalar("SELECT count(*) FROM dm_reply_sequences WHERE status='ACTIVE'") == 1


async def test_full_queue_keeps_incoming_dm_intent_across_restart(env):
    e, clock, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_first", ["Reply"], dm_first_enabled=True, interval_seconds=1)
    e.set_setting("max_pending_jobs", 1)
    e.tick()
    assert e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'") == 1
    assert not e.incoming(e.pair_card(pair)["active_account_id"], 500, 1, 500, True)
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "ACTIVE"
    assert await e.run_one(api)
    clock.advance(2)
    e = restart(e)
    e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["kind"] == "DM_FIRST" and api.calls[-1]["payload"]["dm_step"] == 0


def test_full_queue_rotates_pairs_instead_of_refilling_first_pair(env):
    e, clock, _, _ = env
    campaign, first_pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, interval_seconds=1)
    for i in range(2):
        e.add_account(str(i), "+10000000099", "session", 2000 + i, campaign)
    e.add_chat(-100999, "Second", approved=True)
    with e.db.tx() as c:
        c.execute(
            "UPDATE accounts SET onboarding_stage='READY',onboarding_started_at=?,prepared_at=?,joined_at=?",
            (clock() - 28 * 3600, clock(), clock()),
        )
        c.execute("UPDATE account_chat_access SET joined=1,can_write=1")
    e.set_setting("max_pending_jobs", 1)
    e.tick()
    job = e.claim_job("first")
    assert job and job["pair_id"] == first_pair
    e.finish_job(job, Result(1))
    clock.advance(2)
    e.tick()
    other = e.claim_job("next-pair")
    assert other and other["pair_id"] != first_pair


async def test_rechecking_restored_chat_does_not_reuse_confirmed_join(env):
    e, clock, api, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    with e.db.tx() as c:
        c.execute("UPDATE account_chat_access SET can_write=0 WHERE account_id=?", (a,))
    e.tick()
    job = e.claim_job("first-permission-check")
    assert job["kind"] == "JOIN"
    e.finish_job(job, Result(data={"joined": True}))
    clock.advance(100)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    e.resume_chat(chats[0])
    e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["kind"] == "JOIN" and api.calls[-1]["id"] != job["id"]
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "CONFIRMED"


def test_recovery_migration_preserves_unknown_and_repairs_known_orphans(env):
    from pathlib import Path

    e, _, _, _ = env
    campaign, _, active = dm(e)
    job = e.claim_job("uncertain")
    e.fail_job(job, OperationError("unknown", "NO_ACK"))
    with e.db.tx() as c:
        c.execute("DELETE FROM schema_migrations WHERE version='007_audit_recovery'")
        c.execute("DROP TABLE campaign_creation_receipts")
        c.execute("DROP INDEX dm_campaign_state")
        c.execute("ALTER TABLE account_chat_access DROP COLUMN verification_generation")
        c.execute("UPDATE accounts SET account_health='REMOVED' WHERE id<>?", (active,))
    e.db.migrate()
    e.db.migrate()
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "UNKNOWN"
    assert e.db.scalar("SELECT status FROM dm_reply_sequences WHERE campaign_id=?", (campaign,)) == "ACTIVE"
    assert e.db.scalar("SELECT telegram_id FROM accounts WHERE account_health='REMOVED'") is None
    assert e.db.scalar("SELECT count(*) FROM schema_migrations") == len(list(Path("app/db/migrations").glob("*.sql")))
    assert e.db.scalar("PRAGMA integrity_check") == "ok"
    assert not e.db.all("PRAGMA foreign_key_check")
