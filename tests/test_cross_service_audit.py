from unittest.mock import AsyncMock

import pytest
from conftest import setup_pair
from telethon import errors
from test_legacy_transfer import native_runtime

from app.legacy.mailing import mailing_context
from app.transport.base import OperationError


def unresolved(e, account_id, status):
    with e.db.tx() as c:
        c.execute(
            "INSERT INTO legacy_rpc_journal(operation_key,account_id,method,status,created_at,updated_at) "
            "VALUES ('warmup-operation',?,'send_message',?,?,?)",
            (account_id, status, e.clock(), e.clock()),
        )


@pytest.mark.parametrize("status", ["INFLIGHT", "UNKNOWN"])
@pytest.mark.parametrize("action", ["remove", "assign", "save"])
def test_native_warmup_protects_account_mutations(env, status, action):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    other = e.create_campaign("Other", ["Other"])
    unresolved(e, a, status)
    with pytest.raises(ValueError):
        if action == "remove":
            e.update_account(a, "remove")
        elif action == "assign":
            e.assign_account(a, other)
        else:
            e.save_session(a, "replacement", 1001)
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "OK"
    assert (
        e.secrets.decrypt(e.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (a,)))
        == "session-a"
    )


@pytest.mark.parametrize("status", ["INFLIGHT", "UNKNOWN"])
@pytest.mark.parametrize("action", ["remove", "assign"])
def test_partner_native_operation_protects_pair_membership(env, status, action):
    e, _, _, _ = env
    campaign, pair, a, b, _ = setup_pair(e, 1)
    other = e.create_campaign("Other", ["Other"])
    unresolved(e, b, status)
    with pytest.raises(ValueError):
        if action == "remove":
            e.update_account(a, "remove")
        else:
            e.assign_account(a, other)
    assert e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (a,)) == campaign
    assert e.db.scalar("SELECT account_a_id FROM pairs WHERE id=?", (pair,)) == a


@pytest.mark.parametrize(
    "error_type,health",
    [
        (errors.UserDeactivatedBanError, "FROZEN"),
        (errors.AuthKeyUnregisteredError, "SESSION_EXPIRED"),
        (errors.PeerFloodError, "RESTRICTED"),
    ],
)
async def test_native_ban_handling_preserves_health_and_other_pairs(env, error_type, health):
    e, _, _, _ = env
    campaign, pair, a, _, _ = setup_pair(e, 1)
    c, _ = e.add_account("C", "+380333333333", "session-c", 1003)
    e.add_account("D", "+380444444444", "session-d", 1004)
    with e.db.tx() as connection:
        connection.execute(
            "UPDATE accounts SET onboarding_stage='READY',onboarding_started_at=?", (e.clock() - 100000,)
        )
    other_pair = e.db.scalar("SELECT id FROM pairs WHERE account_a_id=?", (c,))
    runtime, _ = native_runtime(e)
    e.critical(a, health)
    await runtime.mailing._handle_mailing_ban(pair, a, error_type(request=None))
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == health
    assert e.db.scalar("SELECT status FROM campaigns WHERE id=?", (campaign,)) == "RUNNING"
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='critical'") == 1
    assert (await runtime.mailing.db.get_mailing(other_pair)).is_active
    assert not (await runtime.mailing.db.get_mailing(pair)).is_active


@pytest.mark.parametrize("health", ["FROZEN", "RESTRICTED"])
async def test_legacy_deactivation_cannot_erase_moderation_health(env, health):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    e.critical(a, health)
    await runtime.mailing.db.deactivate_account(a)
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == health


async def test_invalid_legacy_target_enters_review_instead_of_failing_constraint(env):
    e, _, _, _ = env
    _, pair, _, _, chats = setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    await runtime.mailing.db.delete_mailing_target(target.id)
    assert e.db.scalar("SELECT status FROM pair_chats WHERE id=?", (target.id,)) == "PAUSED_NEEDS_REVIEW"
    assert e.db.scalar("SELECT count(*) FROM chat_issues WHERE chat_pool_id=? AND status='OPEN'", (chats[0],)) == 1


async def test_native_targets_wait_until_configured_greeting_finishes(env):
    e, _, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1, greetings=True)
    e.edit_campaign(campaign, "greeting", ["Greeting"])
    runtime, _ = native_runtime(e)
    e.tick()
    assert not await runtime.mailing.db.get_mailing_targets(pair)
    assert await e.run_one(api)
    assert api.calls[-1]["kind"] == "GREETING"
    assert await runtime.mailing.db.get_mailing_targets(pair)


async def test_native_account_can_be_stopped_for_original_timeout_reconnect(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    await runtime.get_client(a)
    runtime.transport.clients = {a: native}
    runtime.transport._disconnect_client = AsyncMock()
    await runtime.stop_client(a)
    runtime.transport._disconnect_client.assert_awaited_once_with(a, native)
    assert a not in runtime._clients


@pytest.mark.parametrize("health", ["FROZEN", "RESTRICTED"])
@pytest.mark.parametrize("incoming", ["SESSION_EXPIRED", "DISCONNECTED"])
def test_auth_error_cannot_clear_pending_moderation(env, health, incoming):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, health)
    e.critical(a, incoming)
    e.save_session(a, "new-session", 1001)
    account = e.db.one("SELECT * FROM accounts WHERE id=?", (a,))
    assert account["account_health"] == health and account["paused"]


def test_late_critical_error_cannot_restore_removed_account(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.update_account(a, "remove")
    e.critical(a, "FROZEN")
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "REMOVED"


async def test_control_cancellation_is_not_a_chat_failure(env):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    runtime.mailing._spam_ctx[pair] = (a, native, None)
    await runtime.mailing._apply_target_restriction(pair, target, OperationError("cancelled", "STATE_CHANGED"))
    assert target.id not in runtime.mailing._chat_ban_skip
    assert not runtime.mailing._send_outcomes
    assert not runtime.mailing._spambot_tasks


async def test_native_mailing_respects_account_specific_chat_failure(env):
    e, clock, _, _ = env
    _, pair, a, b, chats = setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    e.chat_failure(pair, chats[0], a, "CHAT_UNAVAILABLE")
    assert not await runtime.mailing.db.get_mailing_targets(pair)
    clock.advance(6 * 3600)
    assert (await runtime.mailing.db.get_mailing(pair)).account_id == b
    assert await runtime.mailing.db.get_mailing_targets(pair)


@pytest.mark.parametrize("change", ["stop", "common_job"])
async def test_last_transaction_revalidates_stop_and_account_claim(env, monkeypatch, change):
    e, _, _, _ = env
    campaign, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
    target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    original, fired = e.db.run, False

    async def race(function, *args, **kwargs):
        nonlocal fired
        if function.__name__ == "begin_operation" and not fired:
            fired = True
            if change == "stop":
                e.campaign_status(campaign, "STOPPED", True)
            else:
                with e.db.tx() as c:
                    c.execute(
                        "INSERT INTO dispatch_jobs(idempotency_key,campaign_id,pair_id,account_id,kind,payload,version,planned_at,created_at,status) "
                        "VALUES ('common-job',?,?,?,'CHECK','{}',1,?,?,'INFLIGHT')",
                        (campaign, pair, a, e.clock(), e.clock()),
                    )
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(e.db, "run", race)
    token = mailing_context.set(pair)
    try:
        with pytest.raises(OperationError):
            await runtime.mailing._send_msg(
                await runtime.get_client(a), target.chat_identifier, message, None, account_id=a, target_snapshot=target
            )
    finally:
        mailing_context.reset(token)
    assert not native.sent
    assert e.db.scalar("SELECT active_since FROM pairs WHERE id=?", (pair,)) is None
    assert e.db.scalar("SELECT pair_dispatch_started_at FROM pairs WHERE id=?", (pair,)) is None


async def test_failed_original_reconnect_keeps_client_for_cleanup_retry(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    wrapped = await runtime.get_client(a)
    runtime.transport.clients = {a: native}
    runtime.transport._disconnect_client = AsyncMock(side_effect=ConnectionError("closing failed"))
    with pytest.raises(ConnectionError):
        await runtime.stop_client(a)
    assert runtime._clients[a] is wrapped


@pytest.mark.parametrize("health", ["FROZEN", "RESTRICTED"])
def test_explicit_disconnect_does_not_acknowledge_moderation_removal(env, health):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, health)
    e.update_account(a, "disconnect")
    e.save_session(a, "new-session", 1001)
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == health
    assert e.db.scalar("SELECT paused FROM accounts WHERE id=?", (a,)) == 1


@pytest.mark.parametrize("health", ["FROZEN", "RESTRICTED"])
async def test_new_auth_failure_is_not_hidden_by_existing_moderation(env, health):
    e, _, _, sink = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, health)
    e.critical(a, "SESSION_EXPIRED")
    e.critical(a, "SESSION_EXPIRED")
    await e.deliver_notifications(sink)
    assert len(sink.messages) == 2
    assert sink.messages[-1][2]["health"] == "SESSION_EXPIRED"
    assert sink.messages[-1][2]["account_health"] == health


async def test_frozen_rpc_marker_keeps_original_classification(env):
    e, _, _, _ = env
    campaign, pair, a, _, _ = setup_pair(e, 1)
    runtime, _ = native_runtime(e)
    await runtime.mailing._handle_mailing_ban(pair, a, RuntimeError("The method is not available for frozen accounts"))
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "FROZEN"
    assert e.db.scalar("SELECT status FROM campaigns WHERE id=?", (campaign,)) == "RUNNING"


@pytest.mark.parametrize("event", ["critical", "chat"])
def test_error_history_keeps_campaign_scope_for_pair_events(env, event):
    e, _, _, _ = env
    campaign, pair, a, _, chats = setup_pair(e, 1)
    if event == "critical":
        e.critical(a, "FROZEN")
    else:
        e.chat_failure(pair, chats[0], a, "CHAT_UNAVAILABLE")
    history = e.error_history("all", "campaigns", campaign)
    assert len(history) == 1
    assert history[0]["campaign_id"] == campaign


def test_existing_error_history_gets_scope_without_replacing_events(env):
    e, _, _, _ = env
    campaign, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, "FROZEN")
    event_id = e.db.scalar("SELECT id FROM audit_events WHERE event='ACCOUNT_CRITICAL'")
    count = e.db.scalar("SELECT count(*) FROM audit_events")
    with e.db.tx() as c:
        c.execute("UPDATE audit_events SET campaign_id=NULL WHERE id=?", (event_id,))
        c.execute("DELETE FROM schema_migrations WHERE version='011_audit_scope'")
    e.db.migrate()
    e.db.migrate()
    assert e.error_history("all", "campaigns", campaign)[0]["id"] == event_id
    assert e.db.scalar("SELECT count(*) FROM audit_events") == count
