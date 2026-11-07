from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import restart, setup_pair
from telethon import errors, functions, types

from app.transport.base import OperationError
from app.transport.telegram import TelegramTransport


class LocalClient:
    def __init__(self):
        self.events = []
        self.requests = []
        self.error = None
        self.on_typing = None

    async def __call__(self, request):
        if isinstance(request, functions.messages.GetStickersRequest):
            self.events.append("resolve-sticker")
            return SimpleNamespace(stickers=getattr(self, "stickers", [SimpleNamespace(id=700)]))
        assert isinstance(request, functions.messages.SetTypingRequest)
        self.requests.append(request)
        self.events.append("typing")
        if self.error:
            raise self.error
        if self.on_typing:
            self.on_typing()
        return True

    async def get_entity(self, peer):
        return SimpleNamespace(default_banned_rights=None, broadcast=getattr(self, "broadcast", False))

    async def get_permissions(self, *args):
        return SimpleNamespace(has_left=False, is_admin=True, is_creator=True, post_messages=True)

    async def get_me(self):
        return SimpleNamespace(id=1001)

    async def send_message(self, *args, **kwargs):
        self.events.append("message")
        return SimpleNamespace(id=100)

    async def send_file(self, *args, **kwargs):
        self.events.append("file")
        return SimpleNamespace(id=101)


def adapter_for(engine, client, *, guard=True):
    adapter = object.__new__(TelegramTransport)
    adapter.engine = engine
    adapter.client = AsyncMock(return_value=client)
    if not guard:
        adapter.guard = AsyncMock()
    return adapter


@pytest.mark.parametrize("kind", ["MAILING", "GREETING", "GROUP_REPLY", "DM_FIRST", "DM_ALWAYS", "DM_MANUAL"])
async def test_typing_precedes_each_outgoing_message_kind(env, kind):
    e, _, _, _ = env
    client = LocalClient()
    adapter = adapter_for(e, client, guard=False)
    topic = 0 if kind.startswith("DM_") else 42
    job = {
        "kind": kind,
        "account_id": 1,
        "chat_pool_id": None,
        "payload": {"peer_id": 123, "text": "Test", "topic_id": topic},
    }
    result = await adapter.execute(job)
    assert result.message_id == 100
    assert client.events == ["typing", "message"]
    assert isinstance(client.requests[0].action, types.SendMessageTypingAction)
    assert client.requests[0].top_msg_id == (topic or None)


async def test_broadcast_posts_keep_working_without_conversation_typing(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    e.tick()
    job = e.claim_job("broadcast")
    client = LocalClient()
    client.broadcast = True
    adapter = adapter_for(e, client)
    result = await adapter.execute(job)
    assert result.message_id == 100 and client.events == ["message"]


async def test_typing_also_precedes_media_with_caption(env, tmp_path, monkeypatch):
    e, _, _, _ = env
    monkeypatch.chdir(tmp_path)
    path = e.config.media_dir / "test.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"fake-media")
    client = LocalClient()
    adapter = adapter_for(e, client, guard=False)
    job = {
        "kind": "DM_MANUAL",
        "account_id": 1,
        "chat_pool_id": None,
        "payload": {"peer_id": 123, "text": "Caption", "media_path": str(path)},
    }
    result = await adapter.execute(job)
    assert result.message_id == 101 and client.events == ["typing", "file"]


@pytest.mark.parametrize("kind", ["CHECK", "JOIN"])
async def test_checks_and_membership_requests_do_not_send_typing(env, kind):
    e, _, _, _ = env
    client = LocalClient()
    adapter = adapter_for(e, client, guard=False)
    job = {"kind": kind, "account_id": 1, "chat_pool_id": None, "payload": {"peer_id": 123, "join_allowed": False}}
    await adapter.execute(job)
    assert client.events == []


async def test_stop_during_typing_prevents_the_message(env):
    e, _, _, _ = env
    campaign, _, _, _, _ = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("typing-stop")
    client = LocalClient()
    client.on_typing = lambda: e.campaign_status(campaign, "STOPPED")
    adapter = adapter_for(e, client)
    with pytest.raises(OperationError) as error:
        await adapter.execute(job)
    assert error.value.category == "cancelled"
    assert client.events == ["typing"]
    assert job["_request_may_have_been_sent"] is False


@pytest.mark.parametrize("failure", ["flood", "timeout"])
async def test_typing_failure_does_not_mark_a_message_as_sent(env, failure):
    e, _, _, _ = env
    client = LocalClient()
    client.error = errors.FloodWaitError(request=None, capture=17) if failure == "flood" else TimeoutError()
    adapter = adapter_for(e, client, guard=False)
    job = {"kind": "DM_FIRST", "account_id": 1, "chat_pool_id": None, "payload": {"peer_id": 123, "text": "Test"}}
    with pytest.raises(OperationError) as error:
        await adapter.execute(job)
    assert error.value.category == ("flood" if failure == "flood" else "transient")
    assert client.events == ["typing"] and job["_request_may_have_been_sent"] is False
    if failure == "flood":
        assert error.value.seconds == 17


async def test_dm_greeting_waits_between_steps_and_keeps_deadline_after_restart(env):
    e, clock, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_first", ["Main"], dm_first_enabled=True)
    account = e.pair_card(pair)["active_account_id"]
    assert e.incoming(account, 500, 1, 500, True)
    for index in range(3):
        assert await e.run_one(api)
        assert api.calls[-1]["payload"]["dm_step"] == index
        if index == 2:
            break
        due = e.db.scalar("SELECT next_step_at FROM dm_reply_sequences")
        assert 3 <= due - clock() <= 4
        e = restart(e)
        assert not await e.run_one(api)
        clock.advance(due - clock() - 0.01)
        assert not await e.run_one(api)
        clock.advance(0.02)
    assert not await e.run_one(api)
    assert len(api.calls) == 3
    assert api.calls[0]["payload"]["sticker_emoji"] == "👋"
    assert api.calls[1]["payload"]["text"] == "Привет!"
    assert api.calls[2]["payload"]["text"] == "Main"
    assert e.db.scalar("SELECT status FROM dm_reply_sequences") == "COMPLETE"


async def test_pause_during_dm_gap_does_not_repeat_completed_parts(env):
    e, clock, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_first", ["Main"], dm_first_enabled=True)
    assert e.incoming(e.pair_card(pair)["active_account_id"], 500, 1, 500, True)
    await e.run_one(api)
    e.campaign_status(campaign, "PAUSED")
    clock.advance(4)
    e = restart(e)
    e.tick()
    assert not await e.run_one(api)
    e.campaign_status(campaign, "RUNNING", confirmed=True)
    e.tick()
    await e.run_one(api)
    assert [job["payload"]["dm_step"] for job in api.calls] == [0, 1]


async def test_sticker_is_resolved_and_sent_as_a_sticker_document(env):
    e, _, _, _ = env
    client = LocalClient()
    adapter = adapter_for(e, client, guard=False)
    result = await adapter.execute(
        {"kind": "DM_FIRST", "account_id": 1, "chat_pool_id": None, "payload": {"peer_id": 123, "sticker_emoji": "👋"}}
    )
    assert result.message_id == 101
    assert client.events == ["resolve-sticker", "typing", "file"]


async def test_missing_sticker_does_not_send_placeholder_or_advance_sequence(env):
    e, _, _, _ = env
    client = LocalClient()
    client.stickers = []
    adapter = adapter_for(e, client, guard=False)
    with pytest.raises(OperationError) as error:
        await adapter.execute(
            {
                "kind": "DM_FIRST",
                "account_id": 1,
                "chat_pool_id": None,
                "payload": {"peer_id": 123, "sticker_emoji": "👋"},
            }
        )
    assert error.value.code == "STICKER_UNAVAILABLE" and client.events == ["resolve-sticker"]


async def test_group_greetings_do_not_receive_the_dm_delay(env):
    e, _, api, _ = env
    campaign, _, _, _, chats = setup_pair(e, 1, greetings=True)
    e.edit_campaign(campaign, "greeting", ["Group main"], greeting_split=True)
    e.set_chat_permissions(chats[0], True, True, True, True)
    for _ in range(3):
        e.tick()
        assert await e.run_one(api)
    assert [job["payload"]["stage"] for job in api.calls] == ["emoji", "phrase", "main"]


async def test_unknown_dm_part_blocks_the_chain_until_manual_reconciliation(env):
    e, clock, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_first", ["Main"], dm_first_enabled=True)
    account = e.pair_card(pair)["active_account_id"]
    e.incoming(account, 500, 1, 500, True)
    sticker = e.claim_job("unknown-sticker")
    e.fail_job(sticker, OperationError("unknown", "TEST_UNCERTAIN"))
    clock.advance(10)
    e = restart(e)
    e.tick()
    assert e.db.scalar("SELECT step_index FROM dm_reply_sequences") == 0
    assert not e.incoming(account, 500, 2, 500, True)
    assert not await e.run_one(api)
    e.resolve_unknown(sticker["id"], True, 101)
    assert e.db.scalar("SELECT step_index FROM dm_reply_sequences") == 1
    clock.advance(4)
    assert await e.run_one(api)
    assert api.calls[-1]["payload"]["dm_step"] == 1


async def test_editing_pending_dm_text_keeps_confirmed_sticker_and_spacing(env):
    e, clock, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_first", ["Old"], dm_first_enabled=True)
    e.incoming(e.pair_card(pair)["active_account_id"], 500, 1, 500, True)
    await e.run_one(api)
    e.edit_campaign(campaign, "dm_first", ["New"], dm_greeting_phrase="Hello")
    e.tick()
    assert not e.db.one("SELECT 1 FROM dispatch_jobs WHERE status='PENDING' AND json_extract(payload,'$.dm_step')=0")
    clock.advance(4)
    await e.run_one(api)
    assert api.calls[-1]["payload"]["text"] == "Hello"
    clock.advance(4)
    await e.run_one(api)
    assert api.calls[-1]["payload"]["text"] == "New"
    assert sum(bool(job["payload"].get("sticker_emoji")) for job in api.calls) == 1


async def test_every_message_reply_waits_for_the_first_greeting_chain(env):
    e, clock, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(campaign, "dm_first", ["First"], dm_first_enabled=True)
    e.edit_campaign(campaign, "dm_always", ["Always"], dm_always_enabled=True)
    account = e.pair_card(pair)["active_account_id"]
    assert len(e.incoming(account, 500, 1, 500, True)) == 1
    for index in range(4):
        assert await e.run_one(api)
        assert not e.incoming(account, 500, 100 + index, 500, True)
        clock.advance(4)
    assert [job["payload"]["dm_step"] for job in api.calls] == [0, 1, 2, 3]
    assert [job["payload"].get("text", "") for job in api.calls] == ["", "Привет!", "First", "Always"]
