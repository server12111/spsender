from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from conftest import drain, restart, setup_pair
from telethon import errors, functions, types
from telethon.tl.custom import ParticipantPermissions

from app.bot.interface import Interface
from app.services import Engine
from app.services.core import DomainError
from app.services.statistics import period_bounds
from app.transport.base import OperationError
from app.transport.telegram import TelegramTransport, can_publish, classify_error


async def test_expired_lease_is_unknown_and_never_blindly_retried(env):
    e, clock, api, sink = env
    setup_pair(e, 1)
    e.tick()
    job = e.claim_job("crashed")
    clock.advance(181)
    e = restart(e)
    e.recover_expired()
    e.tick()
    await drain(e, api)
    assert not api.calls
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "UNKNOWN"
    assert e.statistics("all")["confirmed"] == 0
    await e.deliver_notifications(sink)
    assert sink.messages[-1][1] == "unknown"
    e.resolve_unknown(job["id"], True, 777)
    assert e.statistics("all")["confirmed"] == 1
    e.tick()
    await drain(e, api)
    assert not api.calls


async def test_network_timeout_does_not_repeat_or_count_success(env):
    e, _, api, _ = env
    _, pair, _, _, chats = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    api.errors[("MAILING", active, chats[0])] = TimeoutError("contains-sensitive-data")
    e.tick()
    await drain(e, api)
    e.tick()
    await drain(e, api)
    assert len(api.calls) == 1
    assert e.statistics("all")["confirmed"] == 0
    assert e.db.scalar("SELECT last_error FROM dispatch_jobs") == "TimeoutError"
    assert "contains-sensitive" not in str(e.db.all("SELECT * FROM audit_events"))


async def test_flood_wait_is_not_reported_as_spamblock(env):
    e, clock, api, sink = env
    _, pair, _, _, chats = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    api.errors[("MAILING", active, chats[0])] = OperationError("flood", "FLOOD_WAIT", 600)
    e.tick()
    assert await e.run_one(api)
    assert not await e.run_one(api)
    account = e.db.one("SELECT * FROM accounts WHERE id=?", (active,))
    assert account["flood_until"] == clock() + 600 and account["account_health"] == "OK"
    assert not e.db.scalar("SELECT count(*) FROM chat_issues")
    assert not e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='critical'")
    api.errors.clear()
    clock.advance(599)
    assert not await e.run_one(api)
    clock.advance(1)
    e.tick()
    await drain(e, api)
    assert e.statistics("all")["confirmed"] == 1


async def test_outbox_retries_independently_of_campaign(env):
    e, clock, _, sink = env
    ca, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, "FROZEN")
    sink.fail = True
    await e.deliver_notifications(sink)
    e.campaign_status(ca, "STOPPED")
    item = e.db.one("SELECT * FROM notifications_outbox")
    assert item["status"] == "PENDING" and item["attempts"] == 1
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "FROZEN"
    sink.fail = False
    clock.advance(10)
    await e.deliver_notifications(sink)
    assert sink.messages and e.db.scalar("SELECT status FROM notifications_outbox") == "SENT"


async def test_long_join_stage_keeps_minimum_pause_and_delays_readiness(env):
    e, clock, api, _ = env
    _, pair, a, b, chats = setup_pair(e, 60, ready=False)
    clock.advance(25 * 3600)
    e.tick()
    await drain(e, api)  # connection checks
    for index in range(60):
        e.tick()
        before = len(api.calls)
        await drain(e, api)
        assert len(api.calls) - before == 2
        if index < 59:
            e.tick()
            await drain(e, api)
            assert len(api.calls) - before == 2  # cannot shorten the 75 second interval
        if index == 49:
            assert clock() > e.db.scalar("SELECT onboarding_started_at FROM accounts LIMIT 1") + 26 * 3600
            assert not e.db.scalar("SELECT count(*) FROM accounts WHERE onboarding_stage='READY'")
        clock.advance(75)
    e.tick()
    assert not e.db.scalar("SELECT count(*) FROM accounts WHERE onboarding_stage='READY'")
    clock.advance(3600)
    e.tick()
    assert e.db.scalar("SELECT count(*) FROM accounts WHERE onboarding_stage='READY'") == 2


async def test_autoreplies_filter_bots_own_and_non_reply_group_messages(env):
    e, clock, api, _ = env
    ca, pair, a, b, chats = setup_pair(e, 1)
    e.edit_campaign(ca, "group", ["Ответ группы"], group_reply_enabled=True)
    e.edit_campaign(ca, "dm_first", ["Первый ответ"], dm_first_enabled=True)
    e.edit_campaign(ca, "dm_always", ["Каждый ответ"], dm_always_enabled=True)
    active = e.pair_card(pair)["active_account_id"]
    me = e.db.scalar("SELECT telegram_id FROM accounts WHERE id=?", (active,))
    assert not e.incoming(active, 500, 1, 500, True, bot=True)
    assert not e.incoming(active, 500, 2, 1001, True)
    assert not e.incoming(active, 500, 3, 500, True, outgoing=True)
    assert not e.incoming(active, -100000, 4, 500, False)
    assert not e.incoming(active, -100000, 5, 500, False, reply_to_sender_id=999)
    assert e.incoming(active, -100000, 6, 500, False, reply_to_sender_id=me)
    assert len(e.incoming(active, 500, 7, 500, True)) == 1
    for _ in range(4):
        await drain(e, api)
        clock.advance(4)
    assert not e.incoming(active, 500, 7, 500, True)
    assert not e.incoming(active, 500, 8, 500, True)
    clock.advance(300)
    assert len(e.incoming(active, 500, 9, 500, True)) == 1


async def test_stale_and_repeated_ui_callbacks_are_rejected(env):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    ui = Interface(e)
    token = ui.button(
        123, "Запуск", "campaign_status", {"id": ca, "status": "RUNNING", "v": 1, "expected": "RUNNING"}, True
    )
    e.edit_campaign(ca, "mailing", ["Обновлено"])
    with pytest.raises(DomainError, match="устарела"):
        await ui.callback(123, token[1])
    one = ui.button(123, "Меню", "main", once=True)
    await ui.callback(123, one[1])
    with pytest.raises(DomainError):
        await ui.callback(123, one[1])


def test_sessions_and_phone_encrypted_at_rest_and_cannot_mix_modes(env):
    e, _, _, _ = env
    a, _ = e.add_account("Личный", "+380999999999", "VERY-SECRET-SESSION", 10001)
    record = e.db.one("SELECT * FROM account_sessions WHERE account_id=?", (a,))
    assert "VERY-SECRET" not in str(record) and "+380999" not in str(record)
    assert e.secrets.decrypt(record["encrypted_session"]) == "VERY-SECRET-SESSION"
    with pytest.raises(DomainError, match="DATABASE_PATH"):
        Engine(e.db, replace(e.config, mode="live"), e.secrets, e.clock)


def test_permissions_use_real_telethon_objects():
    entity = SimpleNamespace(default_banned_rights=None, broadcast=False)
    regular = ParticipantPermissions(types.ChannelParticipant(user_id=10, date=datetime.now()), False)
    assert can_publish(entity, regular)
    entity.default_banned_rights = types.ChatBannedRights(until_date=None, send_messages=True)
    assert not can_publish(entity, regular)
    entity.default_banned_rights = types.ChatBannedRights(until_date=None, send_photos=True)
    assert can_publish(entity, regular) and not can_publish(entity, regular, "photo")
    entity.broadcast = True
    assert not can_publish(entity, regular)
    assert classify_error(errors.FloodWaitError(None, 30)).category == "flood"
    assert classify_error(errors.PeerFloodError(None)).category == "critical"


async def test_real_adapter_rechecks_stop_after_permission_rpc_without_sending(env, monkeypatch):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("adapter-test")
    # Bypass only the constructor's dry-run guard to exercise the real adapter logic
    # with a local fake client. TelegramClient is never created.
    adapter = object.__new__(TelegramTransport)
    adapter.engine = e
    sent = []

    class Client:
        async def get_entity(self, peer):
            return SimpleNamespace(default_banned_rights=None, broadcast=False)

        async def get_permissions(self, entity, user):
            e.campaign_status(ca, "STOPPED")
            return ParticipantPermissions(types.ChannelParticipant(user_id=10, date=datetime.now()), False)

        async def send_message(self, *args, **kwargs):
            sent.append(args)
            return SimpleNamespace(id=10)

    async def client(account_id):
        return Client()

    adapter.client = client
    with pytest.raises(OperationError) as error:
        await adapter.execute(job)
    assert error.value.category == "cancelled" and not sent
    e.fail_job(job, error.value)
    assert e.statistics("all")["confirmed"] == 0
    assert e.statistics("all")["errors"] == 0


def test_dst_periods_and_repeated_local_hours():
    tz = ZoneInfo("Europe/Kyiv")
    now = datetime(2026, 10, 25, 23, tzinfo=tz).timestamp()
    start, end = period_bounds("today", now, "Europe/Kyiv")
    assert datetime.fromtimestamp(start, tz).hour == 0
    assert end - start == 24 * 3600 + 1  # 23:00 on a 25-hour day


async def test_template_rotation_is_sequential_across_batch(env):
    e, _, api, _ = env
    setup_pair(e, 20)
    e.tick()
    await drain(e, api)
    assert [j["payload"]["text"] for j in api.calls] == ["Текст один", "Текст два"]


def test_delayed_replacement_keeps_decision_history(env):
    e, _, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    issue = e.db.scalar("SELECT id FROM chat_issues")
    e.decide_issue(issue, "REMOVE", 123)
    new = e.add_chat(-77777, "Поздняя замена", approved=True)
    assert e.db.scalar("SELECT replacement_chat_id FROM chat_issue_decisions WHERE issue_id=?", (issue,)) == new


def test_deletion_preserves_partner_and_history(env):
    e, _, _, _ = env
    _, pair, a, b, chats = setup_pair(e, 1)
    e.update_account(a, "remove")
    assert e.db.scalar("SELECT account_a_id FROM pairs WHERE id=?", (pair,)) == b
    assert e.db.scalar("SELECT account_b_id FROM pairs WHERE id=?", (pair,)) is None
    assert not e.db.one("SELECT * FROM account_sessions WHERE account_id=?", (a,))
    assert not e.db.all("PRAGMA foreign_key_check")


async def test_reply_history_reset_keeps_actual_delivery_journal(env):
    e, clock, api, _ = env
    ca, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(ca, "dm_first", ["Первый ответ"], dm_first_enabled=True)
    active = e.pair_card(pair)["active_account_id"]
    assert e.incoming(active, 800, 1, 800, True)
    for _ in range(3):
        await drain(e, api)
        clock.advance(4)
    assert not e.incoming(active, 800, 2, 800, True)
    e.clear_reply_history(ca)
    assert e.statistics("all")["confirmed"] == 3
    assert e.incoming(active, 800, 3, 800, True)
    for _ in range(3):
        await drain(e, api)
        clock.advance(4)
    assert e.statistics("all")["confirmed"] == 6


def test_unassigned_account_advances_passive_stage_without_network(env):
    e, clock, _, _ = env
    a, _ = e.add_account("Свободный", "+10000000000", "fake", 900)
    clock.advance(86400)
    e.tick()
    assert e.db.scalar("SELECT onboarding_stage FROM accounts WHERE id=?", (a,)) == "PREPARING"
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs")


async def test_reconnecting_after_failed_preparation_releases_check_job(env):
    e, clock, api, _ = env
    _, _, a, _, _ = setup_pair(e, 1, ready=False)
    clock.advance(86400)
    api.errors[("CHECK", a, None)] = OperationError("critical", "SESSION_EXPIRED")
    e.tick()
    await drain(e, api)
    assert e.db.scalar("SELECT prepared_at FROM accounts WHERE id=?", (a,)) is None
    api.errors.clear()
    e.save_session(a, "replacement-session", 1001)
    e.tick()
    await drain(e, api)
    assert e.db.scalar("SELECT prepared_at FROM accounts WHERE id=?", (a,)) == clock()


@pytest.mark.parametrize("kind", [None, "photo", "video", "document"])
async def test_real_adapter_payload_with_local_fake_client(env, tmp_path, monkeypatch, kind):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    monkeypatch.chdir(tmp_path)
    template = {"text": "Сообщение"}
    if kind:
        from pathlib import Path

        path = Path("data/media/test.bin")
        path.parent.mkdir(parents=True)
        path.write_bytes(b"mock-media")
        template.update(media_path=str(path.resolve()), media_kind=kind)
    e.edit_campaign(ca, "mailing", [template])
    e.tick()
    job = e.claim_job("payload")
    adapter = object.__new__(TelegramTransport)
    adapter.engine = e
    calls = []

    class Client:
        async def __call__(self, request):
            assert isinstance(request, functions.messages.SetTypingRequest)
            return True

        async def get_entity(self, peer):
            return SimpleNamespace(default_banned_rights=None, broadcast=False)

        async def get_permissions(self, *args):
            return ParticipantPermissions(types.ChannelParticipant(user_id=10, date=datetime.now()), False)

        async def send_message(self, *args, **kwargs):
            calls.append(("message", args, kwargs))
            return SimpleNamespace(id=501)

        async def send_file(self, *args, **kwargs):
            calls.append(("file", args, kwargs))
            return SimpleNamespace(id=502)

    async def client(account_id):
        return Client()

    adapter.client = client
    result = await adapter.execute(job)
    e.finish_job(job, result)
    assert len(calls) == 1 and calls[0][0] == ("file" if kind else "message")
    assert e.statistics("all")["confirmed"] == 1
    if kind:
        assert calls[0][2]["force_document"] == (kind == "document")
