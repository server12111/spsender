from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from aiogram.exceptions import TelegramNetworkError
from aiogram.methods import EditMessageText
from aiogram.types import Chat, Document, Message, Update, User
from conftest import setup_pair
from test_bot import Harness

from app.config import Config


@pytest.mark.parametrize("command", ["/start", "arbitrary input"])
@pytest.mark.parametrize("context", ["admin_group", "anonymous_channel", "wrong_private_chat"])
async def test_only_owner_in_their_private_chat_can_use_the_interface(env, command, context):
    e, _, transport, _ = env
    h = Harness(e, transport)
    chat = Chat(id=-100, type="supergroup") if context == "admin_group" else Chat(id=-200, type="channel")
    if context == "wrong_private_chat":
        chat = Chat(id=999, type="private")
    user = None if context == "anonymous_channel" else h.user()
    message = Message(message_id=1, date=datetime.now(timezone.utc), chat=chat, from_user=user, text=command)
    await h.dispatcher.feed_update(h.bot, Update(update_id=1, message=message))
    assert not e.db.scalar("SELECT count(*) FROM ui_actions")
    assert not e.db.scalar("SELECT count(*) FROM admin_update_receipts")
    assert not transport.calls
    await h.bot.session.close()


async def test_unknown_user_cannot_upload_or_gain_access_from_an_old_admin_row(env):
    e, _, transport, _ = env
    h = Harness(e, transport)
    with e.db.tx() as c:
        c.execute("INSERT INTO admins VALUES (999)")
    h.ui.set_state(123, "import_chats", {})
    h.bot.download = AsyncMock(side_effect=AssertionError("Unauthorized download"))
    message = Message(
        message_id=1, date=datetime.now(timezone.utc), chat=Chat(id=999, type="private"),
        from_user=User(id=999, is_bot=False, first_name="Unknown"),
        document=Document(file_id="fake", file_unique_id="fake", file_name="chats.txt", file_size=10),
    )
    await h.dispatcher.feed_update(h.bot, Update(update_id=1, message=message))
    assert h.last_text() == "Доступ закрыт"
    h.bot.download.assert_not_awaited()
    assert h.ui.state(123)["state"] == "import_chats"
    assert not e.db.scalar("SELECT count(*) FROM chat_pool")
    await h.bot.session.close()


async def test_failed_menu_update_does_not_repeat_a_stop_action(env):
    e, _, transport, _ = env
    campaign, _, _, _, _ = setup_pair(e, 1)
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Рассылки")
    await h.click("Основная")
    original = h.api.make_request

    async def flaky(bot, method, timeout=None):
        if isinstance(method, EditMessageText):
            raise TelegramNetworkError(method=method, message="offline")
        return await original(bot, method, timeout)

    h.api.make_request = flaky
    await h.click("Остановить рассылку")
    assert e.db.scalar("SELECT status FROM campaigns WHERE id=?", (campaign,)) == "STOPPED"
    assert e.db.scalar("SELECT count(*) FROM audit_events WHERE event='CAMPAIGN_STOPPED'") == 1
    assert e.db.scalar("SELECT count(*) FROM service_diagnostics WHERE component='UI'") == 1
    assert "обновить экран" in h.last_text()
    await h.bot.session.close()


@pytest.mark.parametrize("admin", [0, -100, True])
def test_invalid_admin_ids_are_rejected(admin):
    with pytest.raises(ValueError, match="ADMIN_IDS"):
        Config(admin_ids=(admin,))


def test_bot_cannot_start_without_an_explicit_admin():
    with pytest.raises(ValueError, match="ADMIN_IDS"):
        Config(bot_token="fake", session_key="fake", admin_ids=()).validate_bot()
