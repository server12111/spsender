"""Regression tests for the six bugs reported after the fingerprint release.

Each fails on the pre-fix code and passes afterwards.
"""

from conftest import setup_pair

from app.bot.interface import Interface
from app.transport.base import OperationError


# 1. Incoming handler crashed on a corrupted captcha regex (re.error: nothing to repeat).
def test_captcha_gate_matches_without_raising():
    from app.transport.telegram import CAPTCHA_GATE

    for text in (
        "Нажмите кнопку, чтобы подтвердить",
        "я не робот",
        "Пройдите проверку: капча",
        "please verify you are human",
        "Solve the captcha",
    ):
        assert CAPTCHA_GATE.search(text), text
    for text in ("Привет, как дела?", "Куплю телефон недорого", "", "любой обычный текст"):
        # The real bug was a crash here; a plain non-match must simply return None.
        assert CAPTCHA_GATE.search(text) is None


# 6. "Pairs per cycle" (planning_batch_size) saved in the UI was ignored by the planner.
def test_planning_batch_size_setting_limits_pairs_per_tick(env):
    e, _, _, _ = env
    e.create_campaign("Одна", ["Текст"], chat_limit=1)
    for i in range(6):
        e.add_account(f"Акк {i}", f"+3801100000{i:02d}", f"session-{i}", 3000 + i)
    pairs = e.db.all("SELECT id FROM pairs WHERE account_a_id IS NOT NULL ORDER BY id")
    assert len(pairs) == 3
    e.set_setting("planning_batch_size", 1)
    e.tick()
    # With a limit of 1 the planner must stop after the first pair, not sweep them all.
    assert e.db.scalar("SELECT value FROM settings WHERE key='planner_cursor'") == str(pairs[0]["id"])


# 5. A removed Telegram account could not be added again (UNIQUE telegram_id).
def test_removed_account_can_be_added_again(env):
    e, _, _, _ = env
    a, _ = e.add_account("Первый", "+380111111111", "session-a", 5551)
    e.update_account(a, "remove")
    assert e.db.scalar("SELECT telegram_id FROM accounts WHERE id=?", (a,)) is None
    b, _ = e.add_account("Снова", "+380111111111", "session-b", 5551)  # must not raise UNIQUE
    assert b != a
    assert e.db.scalar("SELECT telegram_id FROM accounts WHERE id=?", (b,)) == 5551


# 4. After removing a campaign, opening its freed accounts' cards raised "Рассылка не найдена".
def test_account_card_opens_after_its_campaign_is_removed(env):
    e, _, _, _ = env
    ca, pair, a, b, _ = setup_pair(e, 1)
    e.campaign_status(ca, "REMOVED", confirmed=True)
    ui = Interface(e)
    card = ui.card(123, "accounts", a)  # previously raised DomainError
    assert "Свободный пул" in card.text
    assert b  # both accounts freed
    # The stale pair of a removed campaign must not surface in the pairs list either
    # (a pair row reads like "… A №x / B №y"); listing it would crash on open.
    labels = [text for row in ui.listing(123, "pairs").buttons for text, _ in row]
    assert not any("A №" in label for label in labels)


# 3. Removing an account left its unfinished DM chain ACTIVE and blocking the peer.
async def test_removing_account_releases_its_active_dm_chain(env):
    e, _, _, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_always", ["Ответ"], dm_always_enabled=True)
    active = e.pair_card(pair)["active_account_id"]
    assert e.incoming(active, 700, 1, 700, True)
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "ACTIVE"
    e.update_account(active, "remove")
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "CANCELLED"


# 2. A DM reply that exhausted its retries stayed ACTIVE and blocked future replies.
async def test_failed_dm_chain_is_cancelled_and_unblocks_peer(env):
    e, _, transport, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_always", ["Ответ"], dm_always_enabled=True)
    e.set_setting("max_retries", 0)
    active = e.pair_card(pair)["active_account_id"]
    assert e.incoming(active, 700, 1, 700, True)
    transport.errors[("DM_ALWAYS", active, None)] = OperationError("transient", "NETWORK")
    assert await e.run_one(transport)  # the only pending job fails terminally
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "CANCELLED"
    # The peer is no longer blocked: a fresh message starts a new chain.
    del transport.errors[("DM_ALWAYS", active, None)]
    assert e.incoming(active, 700, 2, 700, True)
    assert e.db.scalar("SELECT count(*) FROM dm_reply_sequences WHERE status='ACTIVE'") == 1


# 7. Moving an account between campaigns stranded its already-queued CHECK on the old pair.
def test_reassigning_account_rebinds_its_pending_check(env):
    e, clock, _, _ = env
    ca, pair, a, _, _ = setup_pair(e, 1, ready=False)
    clock.advance(86400)
    e.tick()
    before = e.db.one("SELECT campaign_id FROM dispatch_jobs WHERE idempotency_key=?", (f"prepare:{a}",))
    assert before and before["campaign_id"] == ca  # CHECK queued against the first campaign

    cb = e.create_campaign("Вторая", ["Текст"], chat_limit=1)
    e.campaign_status(cb, "RUNNING", confirmed=True)
    e.assign_account(a, cb)
    pair_b = e.db.scalar("SELECT id FROM pairs WHERE campaign_id=? AND account_a_id=?", (cb, a))
    e.tick()

    job = e.db.one("SELECT campaign_id,pair_id,status FROM dispatch_jobs WHERE idempotency_key=?", (f"prepare:{a}",))
    # After the move the revived CHECK must point at the new campaign/pair, or it never runs.
    assert job["campaign_id"] == cb
    assert job["pair_id"] == pair_b
    assert job["status"] != "CANCELLED"


# 8. A join revoked after the job was queued was ignored (stale payload permission was used).
async def test_revoked_join_permission_stops_a_queued_join(env):
    from types import SimpleNamespace

    from telethon import types
    from test_typing_and_spacing import adapter_for

    e, _, _, _ = env

    class JoinClient:
        def __init__(self):
            self.requests = []

        async def __call__(self, request):
            self.requests.append(type(request).__name__)
            return SimpleNamespace(chats=[])

        async def get_entity(self, peer):
            return types.Channel(id=4242, title="Канал", photo=None, date=None, megagroup=True)

        async def get_permissions(self, entity, who):
            return SimpleNamespace(has_left=False, is_admin=True, is_creator=True, participant=None)

    def join_job(chat_id):
        return {
            "kind": "JOIN",
            "account_id": 1,
            "chat_pool_id": chat_id,
            "payload": {"peer_id": -100042, "link": "-100042", "join_allowed": True},  # stale allow
        }

    # Permission granted -> a real join request is issued.
    chat = e.add_chat(-100042, "Канал", approved=True, join_allowed=True)
    client = JoinClient()
    result = await adapter_for(e, client, guard=False).execute(join_job(chat))
    assert "JoinChannelRequest" in client.requests
    assert result.data["joined"]

    # Permission revoked after queueing -> no join, only membership is verified.
    e.set_chat_permissions(chat, approved=True, join_allowed=False)
    revoked_client = JoinClient()
    result = await adapter_for(e, revoked_client, guard=False).execute(join_job(chat))
    assert "JoinChannelRequest" not in revoked_client.requests
    assert result.data["joined"]
