import pytest
from test_bot import Harness

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
