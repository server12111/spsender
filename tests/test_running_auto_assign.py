import pytest
from conftest import restart, setup_pair
from test_bot import Harness

from app.bot.interface import Interface
from app.legacy.mailing_database import LegacyMailingDatabase
from app.scheduler.worker import Worker
from app.transport.authorization import AuthorizedSession


def campaign(e, name, status="DRAFT"):
    result = e.create_campaign(name, ["Test"])
    if status != "DRAFT":
        e.campaign_status(result, status, confirmed=True)
    return result


@pytest.mark.parametrize("other_status", ["DRAFT", "PAUSED", "STOPPED"])
def test_single_running_campaign_receives_new_accounts_and_forms_pair(env, other_status):
    e, _, _, _ = env
    campaign(e, "Other", other_status)
    target = campaign(e, "Running", "RUNNING")
    a, choices = e.add_account("A", "+380111111111", "session-a", 1001)
    b, second_choices = e.add_account("B", "+380222222222", "session-b", 1002)
    assert not choices and not second_choices
    assert e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (a,)) == target
    assert e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (b,)) == target
    pair = e.db.one("SELECT * FROM pairs WHERE campaign_id=?", (target,))
    assert pair["account_a_id"] == a and pair["account_b_id"] == b


def test_multiple_running_campaigns_offer_only_running_choices(env):
    e, _, _, _ = env
    campaign(e, "Draft")
    a = campaign(e, "Running A", "RUNNING")
    b = campaign(e, "Running B", "RUNNING")
    account, choices = e.add_account("A", "+380111111111", "session-a", 1001)
    assert {item["id"] for item in choices} == {a, b}
    assert not e.db.scalar("SELECT count(*) FROM campaign_members WHERE account_id=?", (account,))
    e.assign_account(account, b)
    assert e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (account,)) == b


def test_explicit_campaign_assignment_is_preserved(env):
    e, _, _, _ = env
    target = campaign(e, "Explicit")
    campaign(e, "Running", "RUNNING")
    account, choices = e.add_account("A", "+380111111111", "session-a", 1001, campaign_id=target)
    assert not choices
    assert e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (account,)) == target


async def test_auto_assignment_preserves_api_binding_and_silent_period(env):
    e, _, api, sink = env
    campaign(e, "Draft")
    target = campaign(e, "Running", "RUNNING")
    credential = e.add_api_credential("API", 12345, "a" * 32)
    h = Harness(e, api)
    try:
        await h.ui._save_authorized_account(123, AuthorizedSession("+380111111111", "session-a", 1001, credential), {})
        view = await h.ui._save_authorized_account(
            123, AuthorizedSession("+380222222222", "session-b", 1002, credential), {}
        )
        assert "автоматически" in view.text.lower() and "Running" in view.text
        assert e.db.scalar("SELECT count(*) FROM campaign_members WHERE campaign_id=?", (target,)) == 2
        assert e.db.scalar("SELECT count(*) FROM account_api_bindings WHERE credential_id=?", (credential,)) == 2
        assert e.db.scalar("SELECT count(*) FROM accounts WHERE onboarding_stage='SILENT_24H'") == 2
        await Worker(e, api, sink).run_once()
        assert not api.calls
    finally:
        await h.bot.session.close()


def test_no_campaign_leaves_account_in_free_pool(env):
    e, _, _, _ = env
    account, choices = e.add_account("A", "+380111111111", "session-a", 1001)
    assert not choices and not e.db.scalar("SELECT count(*) FROM campaign_members WHERE account_id=?", (account,))


async def test_later_pair_waits_for_its_own_chat_and_continues_automatically(env):
    e, clock, api, sink = env
    target, first_pair, _, _, existing_chats = setup_pair(e, 1)
    a, _ = e.add_account("New A", "+380333333333", "session-c", 1003)
    b, _ = e.add_account("New B", "+380444444444", "session-d", 1004)
    pair = e.db.one("SELECT * FROM pairs WHERE account_a_id=?", (a,))
    assert (pair["campaign_id"], pair["account_b_id"]) == (target, b)
    assert not e.pair_card(pair["id"])["chats"]

    clock.advance(28 * 3600)
    worker = Worker(e, api, sink)
    await worker.run_once()
    await worker.run_once()
    assert e.db.all("SELECT onboarding_stage,joined_at FROM accounts WHERE id IN (?,?)", (a, b)) == [
        {"onboarding_stage": "PREPARING", "joined_at": None},
        {"onboarding_stage": "PREPARING", "joined_at": None},
    ]
    assert e.pair_card(pair["id"])["status"] == "WAITING_CHATS"
    ui = Interface(e, api)
    assert ui.preparation(123).text.count("Пара ждёт разрешённых чатов из пула") == 2
    assert "Пара ждёт разрешённых чатов из пула" in ui.card(123, "accounts", a).text
    assert {op["kind"] for op in api.calls if op["account_id"] in {a, b}} == {"CHECK"}

    e = restart(e)
    added = e.add_chat(-1009876, "Approved new chat", approved=True, join_allowed=True)
    worker = Worker(e, api, sink)
    await worker.run_once()
    await worker.run_once()
    assert e.pair_card(first_pair)["chats"][0]["chat_pool_id"] == existing_chats[0]
    assert e.pair_card(pair["id"])["status"] == "READY"
    assert e.pair_card(pair["id"])["chats"][0]["chat_pool_id"] == added
    joined = [op for op in api.calls if op["kind"] == "JOIN" and op["chat_pool_id"] == added]
    assert sorted(op["account_id"] for op in joined) == [a, b]
    assert len(await LegacyMailingDatabase(e).get_mailing_targets(pair["id"])) == 1
