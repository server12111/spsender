"""Regression tests for the 2.0.6 audit fixes.

Each test fails on the pre-fix code and passes afterwards. They cover chat-link
parsing, forum topic detection, resilient chat import, the pair "common chats"
page, atomic campaign creation and the chat card's account permission list.
"""

import pytest
from conftest import setup_pair
from telethon import types
from test_bot import Harness
from test_typing_and_spacing import adapter_for

from app.bot.interface import Interface
from app.services.chat_pool import parse_target
from app.services.core import DomainError


def test_parse_target_accepts_common_link_variants():
    # Plain numeric id and id:topic keep working.
    assert parse_target("-1001234567890") == (-1001234567890, 0, "-1001234567890")
    assert parse_target("-100123:7")[1] == 7
    # Private channel message link.
    assert parse_target("https://t.me/c/123/55")[:2] == (-100123, 55)
    # Public username in every accepted shape resolves to the same @handle.
    for value in (
        "@somegroup",
        "t.me/somegroup",
        "https://t.me/somegroup",
        "https://telegram.me/somegroup",
        "https://t.me/s/somegroup",  # web-preview prefix
        "https://t.me/somegroup?start=ref",  # tracking query is stripped
        "https://www.t.me/somegroup/",
    ):
        assert parse_target(value)[0::2] == (None, "@somegroup"), value
    # Both invite link formats normalise to the modern +hash canonical form.
    assert parse_target("https://t.me/+AbC-123_xy") == (None, 0, "https://t.me/+AbC-123_xy")
    assert parse_target("https://t.me/joinchat/AbC-123_xy") == (None, 0, "https://t.me/+AbC-123_xy")
    # A message link still carries its number forward for the live forum check.
    assert parse_target("https://t.me/somegroup/55")[1] == 55


def test_parse_target_still_rejects_garbage():
    for value in ("", "not a link", "http://example.com/foo", "@ab"):
        with pytest.raises(DomainError):
            parse_target(value)


def _channel(forum):
    return types.Channel(id=4242, title="Группа", photo=None, date=None, megagroup=True, forum=forum)


@pytest.mark.parametrize("forum,expected_topic", [(True, 55), (False, 0)])
async def test_message_link_number_kept_as_topic_only_for_forums(env, forum, expected_topic):
    e, _, _, _ = env
    setup_pair(e, 1)  # provides an OK account past the silent period for resolution

    class Client:
        async def get_entity(self, link):
            return _channel(forum)

    adapter = adapter_for(e, Client())
    chat_id, topic, link, title = await adapter.resolve_target("https://t.me/somegroup/55")
    assert topic == expected_topic
    assert title == "Группа" and link == "@somegroup"


async def test_import_keeps_valid_lines_and_reports_skipped(env):
    e, _, transport, _ = env
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Пул чатов")
    await h.click("Импорт")
    # Two good lines around one unparseable line.
    await h.text("-100000\nне ссылка вовсе\n-100001")
    assert "Пропущено нераспознанных строк: 1" in h.last_text()
    await h.click("право автоматизированной")
    await h.click("Импортировать")
    assert e.db.scalar("SELECT count(*) FROM chat_pool") == 2
    assert not transport.calls
    await h.bot.session.close()


async def test_import_all_bad_lines_is_rejected(env):
    e, _, transport, _ = env
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Пул чатов")
    await h.click("Импорт")
    await h.text("мусор\nтоже мусор")
    assert "Ни одна строка не распознана" in h.last_text()
    assert e.db.scalar("SELECT count(*) FROM chat_pool") == 0
    await h.bot.session.close()


async def test_pair_common_chats_page_opens(env):
    e, _, transport, _ = env
    setup_pair(e, 2)
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Пары")
    await h.click("A №1 / B №2")  # open the pair card
    await h.click("Общие чаты")
    # Before the fix this raised TypeError: 'coroutine' object is not subscriptable.
    assert "Общая очередь пары" in h.last_text()
    await h.bot.session.close()


async def test_campaign_creation_with_accounts_is_atomic(env):
    e, _, _, _ = env
    a, _ = e.add_account("Личный A", "+380111111111", "session-a", 1001)
    b, _ = e.add_account("Личный B", "+380222222222", "session-b", 1002)
    campaign = e.create_campaign("Общая", ["Текст"], accounts=[a, b])
    members = e.db.all("SELECT account_id FROM campaign_members WHERE campaign_id=?", (campaign,))
    assert {m["account_id"] for m in members} == {a, b}


async def test_campaign_rolls_back_when_an_account_cannot_be_assigned(env):
    e, _, _, _ = env
    a, _ = e.add_account("Личный A", "+380111111111", "session-a", 1001)
    with pytest.raises(DomainError):
        # 999 does not exist, so the whole create must roll back.
        e.create_campaign("Сломанная", ["Текст"], accounts=[a, 999])
    assert e.db.scalar("SELECT count(*) FROM campaigns") == 0
    assert e.db.scalar("SELECT count(*) FROM campaign_members") == 0


async def test_wizard_save_leaves_no_half_campaign_when_account_vanishes(env):
    e, _, transport, _ = env
    a, _ = e.add_account("Личный A", "+380111111111", "session-a", 1001)
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Рассылки")
    await h.click("Создать")
    await h.text("Кампания")  # step 1 name -> step 2
    await h.text("Текст")  # step 2 adds a text
    await h.click("Далее")  # step 2 -> 3 (media)
    await h.click("Далее")  # step 3 -> 4 (interval)
    await h.text("600")  # interval -> step 5 (account picker)
    await h.click("Личный A")  # pick the account
    await h.click("Далее")  # step 5 -> 6 (N)
    await h.text("20")  # N -> step 7 (flags)
    await h.click("Далее")  # step 7 -> 8 (preview)
    await h.click("Далее")  # step 8 -> 9 (save)
    # The account disappears between the preview and the save.
    e.update_account(a, "remove")
    await h.click("Сохранить")
    # The runtime surfaces the domain error instead of leaving a half-built campaign behind.
    assert "не найден" in h.last_text().lower()
    assert e.db.scalar("SELECT count(*) FROM campaigns") == 0
    assert e.db.scalar("SELECT count(*) FROM campaign_members") == 0
    await h.bot.session.close()


def test_chat_card_hides_removed_account_permissions(env):
    e, _, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1)
    ui = Interface(e)
    before = ui.card(123, "chats", chats[0]).text
    assert "Аккаунт A" in before and "Аккаунт B" in before
    e.update_account(a, "remove")
    after = ui.card(123, "chats", chats[0]).text
    assert "Аккаунт A" not in after
    assert "Аккаунт B" in after
