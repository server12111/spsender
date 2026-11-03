import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pytest
from conftest import drain, restart, setup_pair

from app.bot.interface import Interface
from app.services.greetings import greeting_pool
from app.services.pairs import active_count, shift_role
from app.transport.base import FakeTransport, Result
from app.transport.telegram import AccountAuthorizer, TelegramTransport


async def test_01_new_account_is_silent_for_24_hours(env):
    e, clock, api, sink = env
    setup_pair(e, 2, ready=False)
    for seconds in (0, 3600, 23 * 3600 - 1):
        if seconds:
            clock.advance(seconds)
        e.tick()
        await drain(e, api)
    assert not api.calls
    assert e.db.scalar("SELECT count(*) FROM accounts WHERE onboarding_stage='SILENT_24H'") == 2


async def test_02_preparation_survives_restart(env):
    e, clock, api, _ = env
    setup_pair(e, 1, ready=False)
    clock.advance(86400)
    e.tick()
    await drain(e, api)
    assert len(api.calls) == 2
    assert all(j["kind"] == "CHECK" for j in api.calls)
    e = restart(e)
    e.tick()
    await drain(e, api)
    assert len(api.calls) == 2
    clock.advance(3600)
    e.tick()
    await drain(e, api)
    assert [j["kind"] for j in api.calls].count("JOIN") == 2
    clock.advance(2 * 3600)
    e.tick()
    assert e.db.scalar("SELECT count(*) FROM accounts WHERE onboarding_stage='READY'") == 2


async def test_03_campaign_assignment_choices(env):
    e, _, api, _ = env
    account, choices = e.add_account("Свободный", "+10000000000")
    assert not choices and not e.db.one("SELECT * FROM campaign_members WHERE account_id=?", (account,))
    campaign = e.create_campaign("Первая", ["текст"])
    single, choices = e.add_account("Один", "+10000000001")
    assert (
        not choices
        and e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (single,)) == campaign
    )
    e.create_campaign("Вторая", ["другой"])
    multi, choices = e.add_account("Выбор", "+10000000002")
    assert len(choices) == 2 and not e.db.one("SELECT * FROM campaign_members WHERE account_id=?", (multi,))
    ui = Interface(e, api, AccountAuthorizer(e, api))
    ui.set_state(123, "add_account_name")
    await ui.message(123, "Третий")
    await ui.message(123, "+10000000003")
    result = await ui.message(123, "00000")
    assert "Выберите рассылку" in result.text
    assert len(result.buttons) >= 2


@pytest.mark.parametrize(
    "kind", ["MAILING", "GREETING", "GROUP_REPLY", "DM_FIRST", "DM_ALWAYS", "DM_MANUAL", "JOIN", "CHECK"]
)
async def test_04_stop_blocks_every_external_action(env, kind):
    e, _, api, _ = env
    campaign, pair_id, a, _, chats = setup_pair(e, 1)
    with e.db.tx() as c:
        pair = e._pair(c, pair_id)
        ca = e.campaign(c, campaign)
        e._enqueue(c, pair, ca, kind, chats[0], a, {"text": "test", "peer_id": -100}, "stop-" + kind)
    e.campaign_status(campaign, "STOPPED")
    e.tick()
    await drain(e, api)
    assert not api.calls
    assert e.incoming(a, 5000, 1, 5000, True) == []
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'")


def test_05_exactly_n_shared_chats(env):
    e, _, _, _ = env
    _, pair_id, a, b, _ = setup_pair(e, 20, extra=10)
    card = e.pair_card(pair_id)
    assert len(card["chats"]) == 20
    access = {
        account: {
            r["chat_pool_id"] for r in e.db.all("SELECT * FROM account_chat_access WHERE account_id=?", (account,))
        }
        for account in (a, b)
    }
    assert access[a] == access[b] and len(access[a]) == 20
    assert e.db.scalar("SELECT count(*) FROM chat_pool WHERE status='FREE'") == 10


@pytest.mark.parametrize(
    "hour,minute,role",
    [(0, 0, "A"), (5, 59, "A"), (6, 0, "B"), (11, 59, "B"), (12, 0, "A"), (17, 59, "A"), (18, 0, "B"), (23, 59, "B")],
)
def test_06_fixed_shifts(env, hour, minute, role):
    from zoneinfo import ZoneInfo

    ts = datetime(2026, 10, 20, hour, minute, tzinfo=ZoneInfo("Europe/Kyiv")).timestamp()
    assert shift_role(ts) == role


@pytest.mark.parametrize("hour", [0, 6, 12, 18])
async def test_07_no_duplicate_at_shift_boundary(env, hour):
    from zoneinfo import ZoneInfo

    e, clock, api, _ = env
    _, _, a, b, _ = setup_pair(e, 1)
    boundary = datetime(2026, 10, 21, hour, 0, tzinfo=ZoneInfo("Europe/Kyiv")).timestamp()
    clock.now = boundary - 1
    e.tick()
    job = e.claim_job("first")
    assert job
    clock.now = boundary
    e.tick()
    assert e.claim_job("second") is None
    e.finish_job(job, Result(1))
    e.tick()
    await drain(e, api)
    assert not api.calls
    assert e.db.scalar("SELECT count(*) FROM message_results") == 1


async def test_08_cooldown_and_greetings_survive_shift(env):
    from zoneinfo import ZoneInfo

    e, clock, api, _ = env
    campaign, pair, _, _, chats = setup_pair(e, 1, greetings=True)
    clock.now = datetime(2026, 10, 20, 18, 0, tzinfo=ZoneInfo("Europe/Kyiv")).timestamp() - 1
    e.edit_campaign(campaign, "greeting", ["Здравствуйте от команды"])
    e.tick()
    await drain(e, api)
    assert len(api.calls) == 1 and api.calls[0]["kind"] == "GREETING"
    first = e.db.one("SELECT * FROM pair_chats WHERE pair_id=?", (pair,))
    clock.advance(1)
    e = restart(e)
    e.tick()
    await drain(e, api)
    after = e.db.one("SELECT * FROM pair_chats WHERE pair_id=?", (pair,))
    assert after["cooldown_until"] == first["cooldown_until"]
    assert len(api.calls) == 1
    assert e.db.scalar("SELECT main_done FROM greeting_progress WHERE chat_pool_id=?", (chats[0],)) == 1


def test_09_progressive_shared_subset(env):
    assert [active_count(20, day * 86400) for day in range(4)] == [2, 6, 12, 20]
    assert active_count(3, 0) == 1 and active_count(0, 999999) == 0
    e, clock, api, _ = env
    _, pair, _, _, _ = setup_pair(e, 20)
    with e.db.tx() as c:
        c.execute("UPDATE pairs SET pair_dispatch_started_at=?,active_since=? WHERE id=?", (clock(), clock(), pair))
    for day, expected in enumerate([2, 6, 12, 20]):
        assert e.pair_card(pair)["active_count"] == expected
        clock.advance(86400)


async def test_10_pause_does_not_advance_or_reset_days(env):
    e, clock, api, _ = env
    ca, pair, _, _, _ = setup_pair(e, 20)
    e.tick()
    await drain(e, api)
    clock.advance(86400)
    e.campaign_status(ca, "PAUSED")
    assert e.pair_card(pair)["active_count"] == 6
    clock.advance(10 * 86400)
    e = restart(e)
    e.tick()
    assert e.pair_card(pair)["active_count"] == 6
    e.campaign_status(ca, "RUNNING", True)
    e.tick()
    assert e.pair_card(pair)["active_count"] == 6
    clock.advance(86400)
    assert e.pair_card(pair)["active_count"] == 12


async def test_11_campaign_wide_templates_and_versioning(env):
    e, _, api, _ = env
    ca, pair, a, b, chats = setup_pair(e, 1)
    with pytest.raises(ValueError, match="только в ЛС"):
        e.edit_campaign(ca, "group", ["Группа новая"], group_reply_enabled=True)
    e.edit_campaign(ca, "dm_always", ["ЛС новое"], dm_always_enabled=True)
    e.edit_campaign(ca, "mailing", ["Новая рассылка"])
    assert len(e.templates(ca, "mailing")) == 1
    e.tick()
    await drain(e, api)
    assert api.calls[-1]["payload"]["text"] == "Новая рассылка"
    active = e.pair_card(pair)["active_account_id"]
    jobs = e.incoming(active, 5001, 11, 5001, True)
    assert jobs
    await drain(e, api)
    assert api.calls[-1]["payload"]["text"] == "ЛС новое"
    assert (
        e.db.scalar(
            "SELECT count(*) FROM campaign_templates WHERE campaign_id=? AND kind='group' AND version=(SELECT version FROM campaigns WHERE id=?)",
            (ca, ca),
        )
        == 0
    )


async def test_12_greeting_steps_idempotent_across_restart(env):
    e, clock, api, _ = env
    ca, _, _, _, chats = setup_pair(e, 1, greetings=True)
    e.edit_campaign(ca, "greeting", ["Основной 1", "Основной 2"], greeting_split=True)
    e.set_chat_permissions(chats[0], True, True, True, True)
    pool = greeting_pool()
    assert len(pool["emoji"]) == 30 and len(pool["phrases"]) == 100
    for index in range(3):
        e.tick()
        await drain(e, api)
        e = restart(e)
    greetings = [j for j in api.calls if j["kind"] == "GREETING"]
    assert len(greetings) == 3
    assert greetings[0]["payload"]["text"] in pool["emoji"]
    assert greetings[1]["payload"]["text"] in pool["phrases"]
    assert greetings[2]["payload"]["text"] in {"Основной 1", "Основной 2"}
    e.tick()
    await drain(e, api)
    assert len([j for j in api.calls if j["kind"] == "GREETING"]) == 3


def test_13_first_chat_error_never_removes(env):
    e, _, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "CHAT_UNAVAILABLE")
    assert e.db.scalar("SELECT count(*) FROM pair_chats WHERE removed_at IS NULL") == 1
    assert e.db.scalar("SELECT count(*) FROM chat_issues") == 0


def test_14_second_account_error_pauses_and_opens_issue(env):
    e, _, _, _ = env
    _, pair, a, b, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "CHAT_UNAVAILABLE")
    e.chat_failure(pair, chats[0], b, "CHAT_UNAVAILABLE")
    assert e.db.scalar("SELECT status FROM pair_chats") == "PAUSED_NEEDS_REVIEW"
    assert e.db.scalar("SELECT count(*) FROM chat_issues WHERE status='OPEN'") == 1


async def test_15_one_hourly_numbered_summary(env):
    e, clock, _, sink = env
    _, pair, a, _, chats = setup_pair(e, 2)
    for chat in chats:
        e.chat_failure(pair, chat, a, "PUBLICATION_FORBIDDEN", True)
    assert e.hourly_summary()
    assert not e.hourly_summary()
    await e.deliver_notifications(sink)
    assert len(sink.messages) == 1 and len(sink.messages[0][2]["issues"]) == 2
    numbers = [i["id"] for i in sink.messages[0][2]["issues"]]
    clock.advance(3599)
    assert not restart(e).hourly_summary()
    clock.advance(1)
    assert e.hourly_summary()
    await e.deliver_notifications(sink)
    assert numbers == [i["id"] for i in sink.messages[-1][2]["issues"]]


def test_16_remove_assigns_first_free_chat_atomically(env):
    e, _, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 2, extra=3)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    issue = e.db.scalar("SELECT id FROM chat_issues")
    replacement = e.decide_issue(issue, "REMOVE", 123)
    assert replacement == chats[2]
    assert {r["chat_pool_id"] for r in e.pair_card(pair)["chats"]} == {chats[1], chats[2]}
    assert e.db.scalar("SELECT status FROM chat_pool WHERE id=?", (chats[0],)) == "ARCHIVED"


def test_17_empty_pool_automatically_fills_when_chat_appears(env):
    e, _, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    assert e.decide_issue(e.db.scalar("SELECT id FROM chat_issues"), "REMOVE", 123) is None
    assert e.db.scalar("SELECT status FROM pairs WHERE id=?", (pair,)) == "WAITING_REPLACEMENT"
    new = e.add_chat(-900000, "Замена", approved=True)
    assert e.pair_card(pair)["chats"][0]["chat_pool_id"] == new


async def test_18_keep_leaves_assigned_paused_without_retries(env):
    e, _, api, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    e.decide_issue(e.db.scalar("SELECT id FROM chat_issues"), "KEEP", 123)
    e.tick()
    await drain(e, api)
    assert not api.calls
    assert len(e.pair_card(pair)["chats"]) == 1
    assert e.db.scalar("SELECT status FROM chat_issues") == "KEEP"


@pytest.mark.parametrize("health", ["FROZEN", "SESSION_EXPIRED", "RESTRICTED"])
async def test_19_critical_is_immediate_and_deduplicated(env, health):
    e, _, _, sink = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, health)
    assert e.db.scalar("SELECT paused FROM accounts WHERE id=?", (a,)) == 1
    assert e.db.scalar("SELECT priority FROM notifications_outbox") == 100
    await e.deliver_notifications(sink)
    assert len(sink.messages) == 1 and sink.messages[0][1] == "critical"
    e.critical(a, health)
    await e.deliver_notifications(sink)
    assert len(sink.messages) == 1
    e.critical(a, "DISCONNECTED")
    await e.deliver_notifications(sink)
    assert len(sink.messages) == 2


def test_20_account_error_does_not_blame_chats(env):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 20)
    e.critical(a, "FROZEN")
    assert len(e.pair_card(pair)["chats"]) == 20
    assert not e.db.scalar("SELECT count(*) FROM chat_issues")
    assert e.db.scalar("SELECT count(*) FROM chat_pool WHERE status='ASSIGNED'") == 20


async def test_21_statistics_matches_confirmed_results_and_ui(env):
    e, _, api, _ = env
    _, _, _, _, _ = setup_pair(e, 20)
    e.tick()
    await drain(e, api)
    stats = e.statistics("all")
    assert stats["confirmed"] == len(api.calls) == 2
    assert stats["confirmed"] == e.db.scalar("SELECT count(*) FROM audit_events WHERE event='JOB_CONFIRMED'")
    ui = Interface(e)
    for period in ("today", "yesterday", "7", "30", "all"):
        assert ui.stats(123, period).buttons
        for section in ("accounts", "pairs", "chats", "campaigns", "errors"):
            assert ui.listing(123, section, period=period).buttons
    for section, item_id in (("accounts", 1), ("pairs", 1), ("chats", 1), ("campaigns", 1)):
        assert "Подтверждено" in ui.card(123, section, item_id).text


async def test_22_workers_and_double_clicks_do_not_duplicate(env):
    e, _, api, _ = env
    _, pair, a, _, chats = setup_pair(e, 1, extra=2)
    e.tick()
    second = restart(e)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = list(pool.map(lambda engine: engine.claim_job(str(id(engine))), (e, second)))
    assert sum(j is not None for j in jobs) == 1
    job = next(j for j in jobs if j)
    e.finish_job(job, Result(123))
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    issue = e.db.scalar("SELECT id FROM chat_issues")
    with ThreadPoolExecutor(max_workers=2) as pool:
        replacements = list(pool.map(lambda engine: engine.decide_issue(issue, "REMOVE", 123), (e, second)))
    assert replacements[0] == replacements[1] == chats[1]
    assert len(e.pair_card(pair)["chats"]) == 1


async def test_23_unauthorized_cannot_see_or_execute_callback(env):
    e, _, _, _ = env
    ui = Interface(e)
    with pytest.raises(PermissionError):
        ui.main(999)
    button = ui.button(123, "Кнопка", "main")
    with pytest.raises(PermissionError):
        await ui.callback(999, button[1])
    assert e.db.scalar("SELECT consumed_at FROM ui_actions WHERE token=?", (button[1][2:],)) is None


async def test_24_manual_verification_never_automated(env):
    e, clock, api, sink = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.require_manual_verification(a, -100000)
    e.tick()
    await drain(e, api)
    assert not api.calls
    assert e.db.scalar("SELECT status FROM chat_pool") == "WAITING_MANUAL_VERIFICATION"
    await e.deliver_notifications(sink)
    assert sink.messages[0][1] == "manual"
    e.resume_chat(chats[0])
    e.tick()
    await drain(e, api)
    assert api.calls


def test_25_live_transport_forbidden_in_test_mode(env, monkeypatch):
    e, _, _, _ = env

    def forbidden(*args, **kwargs):
        raise AssertionError("Настоящий TelegramClient был создан")

    monkeypatch.setattr("app.transport.telegram.TelegramClient", forbidden)
    with pytest.raises(RuntimeError):
        TelegramTransport(e)
    assert FakeTransport().calls == []


def test_26_excluded_commercial_modules_not_migrated(env):
    e, _, _, _ = env
    tables = {r["name"] for r in e.db.all("SELECT name FROM sqlite_master WHERE type='table'")}
    assert not tables & {
        "users",
        "payments",
        "promocodes",
        "withdrawal_requests",
        "required_channels",
        "subscriptions",
        "referrals",
    }
    files = {p.name for p in Path("app").rglob("*.py")}
    assert not files & {"subscription.py", "referral.py", "stars_payment.py", "impersonation.py"}


def test_27_migrations_and_packaged_smoke(env):
    e, _, _, _ = env
    e.db.migrate()
    assert e.db.scalar("PRAGMA integrity_check") == "ok"
    assert not e.db.all("PRAGMA foreign_key_check")
    assert e.db.scalar("SELECT count(*) FROM schema_migrations") == 11
    from app.__main__ import smoke

    asyncio.run(smoke())
