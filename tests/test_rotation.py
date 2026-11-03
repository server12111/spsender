import base64
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import restart, setup_pair
from telethon import functions, types

from app.bot.interface import Interface
from app.bot.runtime import BotNotificationSink
from app.services.memberships import membership_links
from app.transport.base import OperationError, Result
from app.transport.telegram import TelegramTransport


def campaign_pairs(e, count=2, chats=2, messages=2):
    campaign, _, _, _, _ = setup_pair(e, chats)
    for index in range((count - 1) * 2):
        e.add_account(f"Extra {index}", "+10000000000", "session", 2000 + index, campaign)
    for index in range((count - 1) * chats):
        e.add_chat(-200000 - index, f"Extra {index}", approved=True, join_allowed=True)
    with e.db.tx() as c:
        c.execute(
            "UPDATE accounts SET onboarding_stage='READY',onboarding_started_at=?,prepared_at=?,joined_at=?,ready_at=?",
            (e.clock() - 28 * 3600, e.clock(), e.clock(), e.clock()),
        )
        c.execute("UPDATE account_chat_access SET joined=1,can_write=1")
    e.set_rotation(campaign, True, messages=messages)
    return campaign


def assignments(e):
    result = {}
    for record in e.db.all(
        "SELECT pair_id,chat_pool_id FROM pair_chats WHERE removed_at IS NULL ORDER BY pair_id,chat_pool_id"
    ):
        result.setdefault(record["pair_id"], set()).add(record["chat_pool_id"])
    return result


def begin(e, clock, campaign):
    due = e.db.scalar("SELECT rotation_next_at FROM campaigns WHERE id=?", (campaign,))
    clock.advance(due - 3 * 86400 - clock())
    e.tick()
    return due


def ready(e, clock, campaign):
    due = e.db.scalar("SELECT rotation_next_at FROM campaigns WHERE id=?", (campaign,))
    clock.advance(due - 3600 - clock())
    with e.db.tx() as c:
        c.execute(
            "UPDATE rotation_progress SET joined=1,can_write=1,message_index=2,verified_at=?,next_action_at=0",
            (clock(),),
        )
        c.execute("UPDATE account_chat_access SET joined=1,can_write=1")
        c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE status='PENDING' AND kind LIKE 'ROTATION_%'")
    e.tick()
    return due


async def drain(e, api, limit=400):
    for _ in range(limit):
        if not await e.run_one(api):
            return
    raise AssertionError("Unbounded dispatch")


async def test_weekly_exchange_prepares_both_accounts_preserves_text_shifts_and_restarts(env):
    e, clock, api, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    original_accounts = e.db.all("SELECT id,account_a_id,account_b_id FROM pairs ORDER BY id")
    texts = [t["text"] for t in e.templates(campaign)]
    begin(e, clock, campaign)
    for hour in range(73):
        e.tick()
        await drain(e, api)
        if hour in {12, 48}:
            e = restart(e)
        if hour < 72:
            clock.advance(3600)
    after = assignments(e)
    left, right = sorted(before)
    assert after[left] == before[right] and after[right] == before[left]
    assert e.db.all("SELECT id,account_a_id,account_b_id FROM pairs ORDER BY id") == original_accounts
    assert [t["text"] for t in e.templates(campaign)] == texts
    prep = [job for job in api.calls if job["kind"] == "ROTATION_MESSAGE"]
    assert len(prep) == 16
    assert {job["payload"]["text"] for job in prep} <= set(texts)
    assert len({job["id"] for job in api.calls}) == len(api.calls)
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "APPLIED"
    assert not e.db.all("PRAGMA foreign_key_check")


def test_odd_pair_keeps_its_original_chats(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e, 3)
    before = assignments(e)
    begin(e, clock, campaign)
    due = ready(e, clock, campaign)
    clock.advance(due - clock())
    e.tick()
    after = assignments(e)
    a, b, odd = sorted(before)
    assert after[a] == before[b] and after[b] == before[a] and after[odd] == before[odd]
    assert e.db.scalar("SELECT count(*) FROM rotation_exchanges") == 1


def test_single_pair_and_different_campaigns_never_exchange(env):
    e, clock, _, _ = env
    first = campaign_pairs(e, 1)
    second = e.create_campaign("Other", ["Other"], chat_limit=2)
    for i in range(2):
        e.add_account(str(i), "+10000000099", "session", 9990 + i, second)
        e.add_chat(-900000 - i, str(i), approved=True, join_allowed=True)
    with e.db.tx() as c:
        c.execute(
            "UPDATE accounts SET onboarding_stage='READY',onboarding_started_at=?,prepared_at=?,joined_at=?",
            (clock() - 28 * 3600, clock(), clock()),
        )
    e.set_rotation(second, True)
    before = assignments(e)
    begin(e, clock, first)
    assert not e.db.scalar("SELECT count(*) FROM rotation_exchanges")
    clock.advance(3 * 86400)
    e.tick()
    assert assignments(e) == before


def test_incomplete_preparation_skips_exchange_without_reassigning(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    due = begin(e, clock, campaign)
    clock.advance(due - clock())
    e.tick()
    assert assignments(e) == before
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "SKIPPED"
    assert e.db.scalar("SELECT rotation_next_at FROM campaigns WHERE id=?", (campaign,)) == due + 7 * 86400


def test_cutover_drains_inflight_and_carries_late_ack_cooldown(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    begin(e, clock, campaign)
    due = ready(e, clock, campaign)
    job = e.claim_job("inflight")
    assert job and job["kind"] == "MAILING"
    clock.advance(due - clock())
    e.tick()
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "DRAINING"
    assert assignments(e) == before
    assert e.claim_job("blocked-at-cutover") is None
    e.finish_job(job, Result(777))
    e.tick()
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "APPLIED"
    cooldown = e.db.scalar(
        "SELECT cooldown_until FROM pair_chats WHERE chat_pool_id=? AND removed_at IS NULL", (job["chat_pool_id"],)
    )
    assert cooldown >= clock() + 300


def test_unknown_operation_prevents_exchange_and_never_replays(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    begin(e, clock, campaign)
    due = ready(e, clock, campaign)
    job = e.claim_job("uncertain")
    e.fail_job(job, OperationError("unknown", "NO_ACK"))
    clock.advance(due - clock())
    e.tick()
    assert assignments(e) == before
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "UNKNOWN"
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "SKIPPED"


@pytest.mark.parametrize("action", ["pause", "disable", "remove"])
async def test_stop_controls_block_all_preparation(env, action):
    e, clock, api, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    if action == "disable":
        e.set_rotation(campaign, False)
    else:
        e.campaign_status(campaign, "REMOVED" if action == "remove" else "PAUSED", confirmed=True)
    e.tick()
    await drain(e, api)
    assert not any(job["kind"].startswith("ROTATION_") for job in api.calls)


def test_changed_assignment_or_account_cancels_snapshot(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    account = e.db.scalar("SELECT account_a_id FROM pairs ORDER BY id LIMIT 1")
    other = e.create_campaign("Other", ["Other"])
    e.assign_account(account, other)
    e.tick()
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "CANCELLED"


def test_old_verification_cannot_make_pair_ready_at_cutover(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    due = begin(e, clock, campaign)
    with e.db.tx() as c:
        c.execute("UPDATE rotation_progress SET joined=1,can_write=1,message_index=2,verified_at=?", (clock(),))
    clock.advance(due - clock())
    e.tick()
    assert assignments(e) == before
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "SKIPPED"


def test_global_chat_lease_prevents_old_owner_and_preparing_pair_collision(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    job = e.claim_job("prep")
    assert job["kind"] == "ROTATION_JOIN"
    with e.db.tx() as c:
        c.execute(
            "UPDATE dispatch_jobs SET planned_at=? WHERE id<>? AND chat_pool_id<>?",
            (clock() + 1000, job["id"], job["chat_pool_id"]),
        )
    other = e.claim_job("other-owner")
    assert other is None or other["chat_pool_id"] != job["chat_pool_id"]


def test_preparation_rejection_does_not_pause_current_chat_owner(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    job = e.claim_job("rejected-preparation")
    e.fail_job(job, OperationError("forbidden", "PUBLICATION_FORBIDDEN"))
    assert e.db.scalar("SELECT status FROM chat_pool WHERE id=?", (job["chat_pool_id"],)) == "ASSIGNED"
    assert not e.db.scalar("SELECT count(*) FROM chat_issues")
    assert e.db.scalar("SELECT last_error FROM rotation_progress WHERE id=?", (job["payload"]["rotation_progress_id"],))


@pytest.mark.parametrize(
    "text", ["Решите CAPTCHA, подпишитесь https://t.me/channelname", "Я не робот https://t.me/channelname"]
)
def test_membership_parser_never_automates_captcha(text):
    assert membership_links(text) == ([], None)


def test_membership_parser_accepts_only_explicit_telegram_targets():
    links, _ = membership_links(
        "Подпишитесь: https://t.me/channelname https://evil.test/a https://t.me/helper_bot?start=ok"
    )
    assert [item["action"] for item in links] == ["JOIN", "BOT_START"]
    assert not membership_links("ordinary text https://t.me/channelname")[0]


def test_membership_parser_uses_buttons_without_following_external_links():
    markup = types.ReplyInlineMarkup(
        rows=[
            types.KeyboardInlineButtonRow(
                buttons=[
                    types.KeyboardInlineButton("Подписаться", types.InlineButtonTypeUrl("https://t.me/channelname")),
                    types.KeyboardInlineButton("Другое", types.InlineButtonTypeUrl("https://evil.test")),
                    types.KeyboardInlineButton("Проверить подписку", types.InlineButtonTypeCallback(b"check")),
                ]
            )
        ]
    )
    links, verify = membership_links("Требуется подписка", markup)
    assert len(links) == 1 and base64.b64decode(verify) == b"check"


async def test_trusted_group_bot_creates_automatic_membership_jobs(env):
    e, _, _, _ = env
    _, pair, _, _, chats = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    event = SimpleNamespace(
        raw_text="Подпишитесь https://t.me/channelname",
        is_private=False,
        is_reply=False,
        get_sender=AsyncMock(return_value=SimpleNamespace(id=888, bot=True)),
        chat_id=-100000,
        id=1,
        client=SimpleNamespace(
            get_entity=AsyncMock(return_value=SimpleNamespace()),
            get_permissions=AsyncMock(return_value=SimpleNamespace(is_admin=True, is_creator=False)),
        ),
        message=SimpleNamespace(reply_markup=None),
    )
    await transport._handle_incoming(active, event)
    assert event.client.get_permissions.await_count == 1
    assert e.db.scalar("SELECT count(*) FROM membership_requests") == 1
    e.tick()
    job = e.claim_job("auto-membership")
    assert job and job["kind"] == "MEMBERSHIP"
    assert job["payload"]["link"] == "https://t.me/channelname"


async def test_untrusted_group_bot_cannot_make_accounts_join_links(env):
    e, _, _, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    event = SimpleNamespace(
        raw_text="Подпишитесь https://t.me/channelname",
        is_private=False,
        is_reply=False,
        get_sender=AsyncMock(return_value=SimpleNamespace(id=888, bot=True)),
        chat_id=-100000,
        id=1,
        client=SimpleNamespace(
            get_entity=AsyncMock(return_value=SimpleNamespace()),
            get_permissions=AsyncMock(return_value=SimpleNamespace(is_admin=False, is_creator=False)),
        ),
        message=SimpleNamespace(reply_markup=None),
        out=False,
    )
    await transport._handle_incoming(active, event)
    assert event.client.get_permissions.await_count == 1
    assert not e.db.scalar("SELECT count(*) FROM membership_requests")


async def test_subscriptions_verify_then_restore_actual_publication_rights(env):
    e, clock, api, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    links, verify = membership_links("Подпишитесь https://t.me/channelname")
    assert e.register_membership_request(a, chats[0], -100000, 1, 888, links, verify)
    e.tick()
    job = e.claim_job("subscription")
    assert job["kind"] == "MEMBERSHIP"
    e.finish_job(job, Result(data={"joined": True, "peer_id": -100999}))
    clock.advance(76)
    e.tick()
    check = e.claim_job("check-membership")
    assert check["kind"] == "MEMBERSHIP" and check["payload"]["action"] == "CHECK"
    e.finish_job(check, Result(data={"can_write": True}))
    assert e.db.scalar("SELECT status FROM chat_pool WHERE id=?", (chats[0],)) == "ASSIGNED"
    assert e.db.scalar("SELECT count(*) FROM membership_requests WHERE status='CONFIRMED'") == 2


async def test_rotation_controls_and_notifications_render_in_two_columns(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    ui = Interface(e)
    view = ui.rotation_view(123, campaign)
    assert all(len(group) <= 2 for group in view.buttons)
    detail = ui.rotation_detail(123, e.db.scalar("SELECT id FROM rotation_exchanges"))
    assert "Вступил" in detail.text
    sink = BotNotificationSink(None, ui)
    payload = {"campaign_id": campaign, "exchange_id": 1, "left": 1, "right": 2, "status": "APPLIED"}
    assert "выполнен" in sink.build_view(123, "rotation", payload).text
    with pytest.raises(PermissionError):
        ui.rotation_view(999, campaign)


class MembershipClient:
    def __init__(self, bot=False):
        self.entity = (
            types.User(id=998, access_hash=11, first_name="Bot", bot=True)
            if bot
            else types.Channel(
                id=998,
                title="Sponsor",
                photo=types.ChatPhotoEmpty(),
                date=datetime.now(timezone.utc),
                access_hash=11,
                broadcast=True,
            )
        )
        self.requests = []

    async def get_entity(self, peer):
        return self.entity

    async def get_permissions(self, *args):
        return SimpleNamespace(has_left=False)

    async def __call__(self, request):
        self.requests.append(request)
        return True


@pytest.mark.parametrize("bot", [False, True])
async def test_real_membership_adapter_joins_or_starts_bot_without_messages(env, bot):
    e, _, _, _ = env
    client = MembershipClient(bot)
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    transport.client = AsyncMock(return_value=client)
    transport.guard = AsyncMock()
    job = {
        "kind": "MEMBERSHIP",
        "account_id": 1,
        "payload": {
            "action": "BOT_START" if bot else "JOIN",
            "peer_id": -100000,
            "link": "https://t.me/helper_bot?start=test" if bot else "https://t.me/channelname",
            "start_param": "test" if bot else "",
            "random_id": 12345,
        },
    }
    result = await transport.execute(job)
    expected = functions.messages.StartBotRequest if bot else functions.channels.JoinChannelRequest
    assert any(isinstance(request, expected) for request in client.requests)
    assert result.data.get("bot_started", False) == bot


def test_preparation_starts_exactly_three_days_before_weekly_date(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    due = e.db.scalar("SELECT rotation_next_at FROM campaigns WHERE id=?", (campaign,))
    assert due == clock() + 7 * 86400
    clock.advance(4 * 86400 - 1)
    e.tick()
    assert not e.db.scalar("SELECT count(*) FROM rotation_cycles")
    clock.advance(1)
    e.tick()
    assert e.db.scalar("SELECT count(*) FROM rotation_cycles") == 1


def test_atomic_cutover_rolls_back_on_database_error(env, monkeypatch):
    import sqlite3

    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    begin(e, clock, campaign)
    due = ready(e, clock, campaign)
    clock.advance(due - clock())
    audit = e.audit

    def fail(c, event, *args, **kwargs):
        if event == "ROTATION_APPLIED":
            raise sqlite3.OperationalError("disk I/O error")
        return audit(c, event, *args, **kwargs)

    monkeypatch.setattr(e, "audit", fail)
    with pytest.raises(sqlite3.OperationalError):
        e.tick()
    assert assignments(e) == before
    assert e.db.scalar("SELECT status FROM rotation_exchanges") != "APPLIED"
    monkeypatch.setattr(e, "audit", audit)
    e.tick()
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "APPLIED"
    assert e.db.scalar("PRAGMA integrity_check") == "ok"


def test_long_downtime_does_not_replay_missed_preparation_weeks(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    clock.advance(40 * 86400)
    e.tick()
    assert assignments(e) == before
    assert e.db.scalar("SELECT rotation_next_at FROM campaigns WHERE id=?", (campaign,)) > clock()
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE kind LIKE 'ROTATION_%' AND status='PENDING'")


def test_flood_wait_never_advances_preparation_progress(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    job = e.claim_job("limited")
    e.fail_job(job, OperationError("flood", "FLOOD_WAIT", 120))
    assert e.db.scalar("SELECT flood_until FROM accounts WHERE id=?", (job["account_id"],)) == clock() + 120
    assert (
        e.db.scalar("SELECT joined FROM rotation_progress WHERE id=?", (job["payload"]["rotation_progress_id"],)) == 0
    )
    assert not e.execution_allowed(job)


def test_rotation_guards_revalidate_revoked_permission_after_claim(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    job = e.claim_job("prepared")
    assert e.execution_allowed(job)
    e.set_chat_permissions(job["chat_pool_id"], False)
    assert not e.execution_allowed(job)
    e.tick()
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "CANCELLED"


def test_captcha_arriving_during_join_ack_cannot_be_cleared_by_success(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    job = e.claim_job("joining")
    e.preparation_manual_required(job["account_id"], job["payload"]["peer_id"])
    e.finish_job(job, Result(data={"can_write": True}))
    p = e.db.one("SELECT * FROM rotation_progress WHERE id=?", (job["payload"]["rotation_progress_id"],))
    assert p["last_error"] == "MANUAL_VERIFICATION" and p["can_write"] == 0
    assert e.db.scalar("SELECT status FROM chat_pool WHERE id=?", (job["chat_pool_id"],)) == "ASSIGNED"


def test_membership_verify_waits_for_joins_and_check_waits_for_verify(env):
    e, clock, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/channelname")
    assert e.register_membership_request(a, chats[0], -100000, 1, 888, links, base64.b64encode(b"verify").decode())
    e.tick()
    job = e.claim_job("join")
    assert job["payload"]["action"] == "JOIN"
    e.finish_job(job, Result(data={"joined": True}))
    clock.advance(76)
    e.tick()
    verify = e.claim_job("verify")
    assert verify["payload"]["action"] == "VERIFY"
    e.finish_job(verify, Result(data={"verified": True}))
    clock.advance(76)
    e.tick()
    check = e.claim_job("check")
    assert check["payload"]["action"] == "CHECK"
    e.finish_job(check, Result(data={"can_write": True}))
    assert e.db.scalar("SELECT count(*) FROM membership_requests WHERE status<>'CONFIRMED'") == 0


def test_repeated_requirement_does_not_rejoin_confirmed_sponsor(env):
    e, clock, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/channelname")
    e.register_membership_request(a, chats[0], -100000, 1, 888, links)
    e.tick()
    job = e.claim_job("join-once")
    e.finish_job(job, Result(data={"joined": True}))
    e.register_membership_request(a, chats[0], -100000, 2, 888, links)
    assert e.db.scalar("SELECT count(*) FROM membership_requests WHERE action='JOIN'") == 1
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "CONFIRMED"


def test_superseded_subscription_button_does_not_block_new_verification(env):
    e, _, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/channelname")
    for message in (1, 2):
        e.register_membership_request(
            a, chats[0], -100000, message, 888, links, base64.b64encode(bytes([message])).decode()
        )
    assert (
        e.db.scalar("SELECT status FROM membership_requests WHERE action='VERIFY' AND source_message_id=1")
        == "CANCELLED"
    )
    assert (
        e.db.scalar("SELECT status FROM membership_requests WHERE action='VERIFY' AND source_message_id=2") == "WAITING"
    )


def test_failed_membership_retry_does_not_touch_unknown(env):
    e, _, _, _ = env
    campaign, _, a, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/channelname")
    e.register_membership_request(a, chats[0], -100000, 1, 888, links)
    e.tick()
    job = e.claim_job("unknown")
    e.fail_job(job, OperationError("unknown", "NO_ACK"))
    e.retry_memberships(campaign)
    assert e.db.scalar("SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],)) == "UNKNOWN"


async def test_native_membership_verification_clicks_only_recorded_callback(env):
    e, _, _, _ = env
    client = MembershipClient()
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    transport.client = AsyncMock(return_value=client)
    transport.guard = AsyncMock()
    job = {
        "kind": "MEMBERSHIP",
        "account_id": 1,
        "payload": {
            "action": "VERIFY",
            "peer_id": -1000000000998,
            "message_id": 5,
            "data": base64.b64encode(b"only-this").decode(),
        },
    }
    await transport.execute(job)
    assert len(client.requests) == 1
    request = client.requests[0]
    assert isinstance(request, functions.messages.GetBotCallbackAnswerRequest)
    assert request.data == b"only-this" and request.msg_id == 5


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.test/a",
        "https://t.me@evil.test/a",
        "https://t.me:bad/a",
        "https://t.me/helper_bot?startgroup=bad",
        "https://t.me/[broken",
    ],
)
def test_unapproved_or_malformed_membership_urls_are_not_executed(url):
    markup = SimpleNamespace(rows=[SimpleNamespace(buttons=[SimpleNamespace(text="Подписаться", url=url)])])
    links, _ = membership_links("Подпишитесь", markup)
    assert not links


@pytest.mark.parametrize("kind", ["channel", "user"])
async def test_peer_access_hash_survives_restart_encrypted_and_is_account_scoped(env, kind):
    e, _, _, _ = env
    _, _, a, b, _ = setup_pair(e, 1)
    if kind == "channel":
        entity = types.Channel(
            id=555,
            title="Private",
            photo=types.ChatPhotoEmpty(),
            date=datetime.now(timezone.utc),
            access_hash=123456789,
        )
        peer_id, expected = -1000000000555, types.InputPeerChannel
    else:
        entity = types.User(id=555, first_name="Peer", access_hash=123456789)
        peer_id, expected = 555, types.InputPeerUser
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    await transport._remember_peer(a, entity)
    ciphertext = e.db.scalar("SELECT encrypted_entity FROM account_entity_cache WHERE account_id=?", (a,))
    assert "123456789" not in ciphertext
    assert not e.db.scalar("SELECT count(*) FROM account_entity_cache WHERE account_id=?", (b,))
    e = restart(e)
    fresh = object.__new__(TelegramTransport)
    fresh.engine = e

    async def resolve(reference):
        assert isinstance(reference, expected)
        assert reference.access_hash == 123456789
        return entity

    client = SimpleNamespace(get_entity=AsyncMock(side_effect=resolve))
    assert await fresh._peer_entity(client, a, peer_id) is entity
    e.save_session(a, "reconnected-session", 1001)
    assert e.db.scalar("SELECT count(*) FROM account_entity_cache WHERE account_id=?", (a,)) == 1
    e.update_account(a, "remove")
    assert not e.db.scalar("SELECT count(*) FROM account_entity_cache WHERE account_id=?", (a,))


async def test_dm_sender_reference_is_saved_before_reply_queueing(env):
    e, _, _, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    event = SimpleNamespace(
        raw_text="Hello",
        is_private=True,
        is_reply=False,
        chat_id=555,
        id=1,
        out=False,
        message=SimpleNamespace(reply_to=None, reply_markup=None),
        get_sender=AsyncMock(return_value=types.User(id=555, access_hash=456789123, first_name="Peer")),
    )
    await transport._handle_incoming(active, event)
    assert e.db.scalar("SELECT count(*) FROM account_entity_cache WHERE account_id=? AND peer_id=555", (active,)) == 1
    assert e.db.scalar("SELECT count(*) FROM incoming_events") == 1


async def test_started_bot_can_request_followup_subscriptions_in_private(env):
    e, clock, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/helper_bot?start=test")
    e.register_membership_request(a, chats[0], -100000, 1, 888, links)
    e.tick()
    start = e.claim_job("start-bot")
    e.finish_job(start, Result(data={"bot_started": True, "peer_id": 777}))
    clock.advance(76)
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    event = SimpleNamespace(
        raw_text="Подпишитесь https://t.me/channelname",
        is_private=True,
        is_reply=False,
        chat_id=777,
        id=5,
        out=False,
        message=SimpleNamespace(reply_markup=None),
        get_sender=AsyncMock(return_value=types.User(id=777, access_hash=555, bot=True, first_name="Bot")),
    )
    await transport._handle_incoming(a, event)
    followup = e.db.one("SELECT * FROM membership_requests WHERE link='https://t.me/channelname'")
    assert followup and followup["chat_pool_id"] == chats[0] and followup["account_id"] == a


def test_different_group_bots_keep_their_own_verification_buttons(env):
    e, _, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/channelname")
    for message, sender in [(1, 888), (2, 999)]:
        e.register_membership_request(
            a, chats[0], -100000, message, sender, links, base64.b64encode(b"verify").decode()
        )
    assert e.db.scalar("SELECT count(*) FROM membership_requests WHERE action='VERIFY' AND status='WAITING'") == 2


def test_subscription_ack_cannot_clear_existing_captcha_requirement(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    join = e.claim_job("joining")
    e.finish_job(join, Result(data={"can_write": False}))
    e.preparation_manual_required(join["account_id"], join["payload"]["peer_id"])
    links, _ = membership_links("Подпишитесь https://t.me/channelname")
    e.register_membership_request(join["account_id"], join["chat_pool_id"], join["payload"]["peer_id"], 12, 888, links)
    assert (
        e.db.scalar("SELECT last_error FROM rotation_progress WHERE id=?", (join["payload"]["rotation_progress_id"],))
        == "MANUAL_VERIFICATION"
    )


def test_chunked_planning_carries_odd_candidate_and_resumes_after_restart(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e, count=19, chats=1)
    pairs = e.db.all("SELECT * FROM pairs ORDER BY id")
    e.update_account(pairs[0]["account_a_id"], "pause")
    begin(e, clock, campaign)
    assert e.db.scalar("SELECT count(*) FROM rotation_exchanges") == 7
    cycle = e.db.one("SELECT * FROM rotation_cycles")
    assert cycle["planning_done"] == 0 and cycle["pending_pair_id"] == pairs[15]["id"]
    e = restart(e)
    e.tick()
    exchanges = e.db.all("SELECT * FROM rotation_exchanges")
    assert len(exchanges) == 9
    used = [x[key] for x in exchanges for key in ("left_pair_id", "right_pair_id")]
    assert len(set(used)) == len(used) and set(used) == {p["id"] for p in pairs[1:]}
    assert e.db.scalar("SELECT planning_done FROM rotation_cycles") == 1


def test_cycle_emits_one_summary_and_exchange_list_is_paginated(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e, count=16, chats=1)
    due = begin(e, clock, campaign)
    ui = Interface(e)
    assert len(e.rotation_card(campaign)["exchanges"]) == 7
    assert len(e.rotation_card(campaign, 1)["exchanges"]) == 2
    first = ui.rotation_view(123, campaign)
    second = ui.rotation_view(123, campaign, 1)
    assert first.text.count(" · Пары ") == 6 and second.text.count(" · Пары ") == 2
    assert all(len(group) <= 2 for group in first.buttons + second.buttons)
    clock.advance(due - clock())
    e.tick()
    e.tick()
    events = e.db.all("SELECT * FROM notifications_outbox WHERE kind='rotation_summary'")
    assert len(events) == 1
    sink = BotNotificationSink(None, ui)
    view = sink.build_view(
        123, "rotation_summary", {"campaign_id": campaign, "applied": 0, "skipped": 8, "cancelled": 0}
    )
    assert "8" in view.text


def test_manual_access_flag_prevents_cutover_despite_old_ready_progress(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    before = assignments(e)
    begin(e, clock, campaign)
    due = ready(e, clock, campaign)
    target = e.db.one(
        "SELECT p.account_id,t.chat_pool_id FROM rotation_progress p "
        "JOIN rotation_targets t ON t.id=p.target_id LIMIT 1"
    )
    e.membership_manual_required(target["account_id"], target["chat_pool_id"], 99)
    # Reproduce a stale/late read overwriting progress, but not the access gate.
    with e.db.tx() as c:
        c.execute("UPDATE rotation_progress SET can_write=1,last_error=NULL")
    clock.advance(due - clock())
    e.tick()
    assert assignments(e) == before
    assert e.db.scalar("SELECT status FROM rotation_exchanges") == "SKIPPED"


def test_explicit_manual_ack_rechecks_rights_without_replaying_messages(env):
    e, clock, _, _ = env
    campaign = campaign_pairs(e)
    begin(e, clock, campaign)
    target = e.db.one(
        "SELECT p.id,p.account_id,t.chat_pool_id,t.exchange_id FROM rotation_progress p "
        "JOIN rotation_targets t ON t.id=p.target_id LIMIT 1"
    )
    with e.db.tx() as c:
        c.execute("UPDATE rotation_progress SET joined=1,message_index=1 WHERE id=?", (target["id"],))
    e.membership_manual_required(target["account_id"], target["chat_pool_id"], 99)
    e.retry_rotation(target["exchange_id"])
    access = e.db.one(
        "SELECT * FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
        (target["account_id"], target["chat_pool_id"]),
    )
    assert access["manual_required"] == 0 and access["can_write"] == 0
    p = e.db.one("SELECT * FROM rotation_progress WHERE id=?", (target["id"],))
    assert p["message_index"] == 1 and p["can_write"] == 0
    e.tick()
    assert (
        e.db.scalar(
            "SELECT count(*) FROM dispatch_jobs WHERE kind='ROTATION_CHECK' AND status='PENDING' "
            "AND json_extract(payload,'$.rotation_progress_id')=?",
            (target["id"],),
        )
        == 1
    )


@pytest.mark.parametrize("changed_owner", [False, True])
async def test_public_link_resolution_checks_imported_chat_identity(env, changed_owner):
    e, _, _, _ = env
    setup_pair(e, 1)
    client = MembershipClient()
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    client.get_entity = AsyncMock(return_value=client.entity)
    expected = -1000000000999 if changed_owner else -1000000000998
    if changed_owner:
        with pytest.raises(OperationError, match="PEER_REFERENCE_REQUIRED"):
            await transport._peer_entity(client, 1, expected, "https://t.me/SomePublicGroup")
        assert not client.requests
    else:
        assert await transport._peer_entity(client, 1, expected, "https://t.me/SomePublicGroup") is client.entity
    client.get_entity.assert_awaited_once_with("@somepublicgroup")


@pytest.mark.parametrize("start_query", ["", "?start=welcome"])
def test_started_bot_followup_can_arrive_before_rpc_ack_and_is_not_started_twice(env, start_query):
    e, _, _, _ = env
    _, _, account, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/helper_bot" + start_query)
    e.register_membership_request(account, chats[0], -100000, 1, 888, links)
    e.tick()
    job = e.claim_job("bot-start")
    assert e.membership_context(account, 777, private=True) is None
    e.membership_bot_resolved(job["id"], account, 777)
    assert e.membership_context(account, 777, private=True) == chats[0]
    assert e.membership_context(account, 999, private=True) is None
    followup, _ = membership_links("Подпишитесь https://t.me/channelname")
    assert e.register_membership_request(account, chats[0], 777, 2, 777, followup)
    e.finish_job(job, Result(data={"bot_started": True, "peer_id": 777}))
    assert e.register_membership_request(account, chats[0], -100000, 3, 888, links)
    assert e.db.scalar("SELECT count(*) FROM membership_requests WHERE link=?", (links[0]["link"],)) == 1


def test_unknown_bot_start_does_not_authorize_new_private_followups(env):
    e, _, _, _ = env
    _, _, account, _, chats = setup_pair(e, 1)
    links, _ = membership_links("Подпишитесь https://t.me/helper_bot?start=welcome")
    e.register_membership_request(account, chats[0], -100000, 1, 888, links)
    e.tick()
    job = e.claim_job("bot-start")
    e.membership_bot_resolved(job["id"], account, 777)
    e.fail_job(job, OperationError("unknown", "NO_ACK"))
    assert e.membership_context(account, 777, private=True) is None
