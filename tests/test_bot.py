from datetime import datetime, timezone

import pytest
from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.methods import AnswerCallbackQuery, DeleteMessage, EditMessageText, GetMe, SendMessage
from aiogram.types import CallbackQuery, Chat, Message, Update, User
from conftest import setup_pair

from app.bot.interface import Interface
from app.bot.runtime import BotNotificationSink, make_dispatcher
from app.transport.telegram import AccountAuthorizer


class MockBotAPI(BaseSession):
    def __init__(self):
        super().__init__()
        self.requests = []

    async def close(self):
        pass

    async def make_request(self, bot, method, timeout=None):
        self.requests.append(method)
        if isinstance(method, GetMe):
            return User(id=100, is_bot=True, first_name="FE mock", username="fe_mock_bot")
        if isinstance(method, (AnswerCallbackQuery, DeleteMessage)):
            return True
        if isinstance(method, (SendMessage, EditMessageText)):
            return Message(
                message_id=len(self.requests),
                date=datetime.now(timezone.utc),
                chat=Chat(id=method.chat_id, type="private"),
                text=method.text,
                reply_markup=method.reply_markup,
            )
        raise AssertionError(f"Неподменённый Bot API метод: {type(method).__name__}")

    async def stream_content(self, *args, **kwargs):
        raise AssertionError("Неожиданная загрузка из сети")
        yield b""  # async generator contract


class Harness:
    def __init__(self, engine, transport):
        self.api = MockBotAPI()
        self.bot = Bot("123456:fake_token_for_unit_tests", session=self.api)
        self.ui = Interface(engine, transport, AccountAuthorizer(engine, transport))
        self.dispatcher = make_dispatcher(self.ui)
        self.id = 0

    def user(self, user=123):
        return User(id=user, is_bot=False, first_name="Администратор")

    async def text(self, text, user=123):
        self.id += 1
        msg = Message(
            message_id=self.id,
            date=datetime.now(timezone.utc),
            chat=Chat(id=user, type="private"),
            from_user=self.user(user),
            text=text,
        )
        await self.dispatcher.feed_update(self.bot, Update(update_id=self.id, message=msg))

    async def click(self, title, user=123):
        last = next(m for m in reversed(self.api.requests) if isinstance(m, (SendMessage, EditMessageText)))
        button = next(b for r in last.reply_markup.inline_keyboard for b in r if title in b.text)
        self.id += 1
        msg = Message(
            message_id=self.id, date=datetime.now(timezone.utc), chat=Chat(id=user, type="private"), text=last.text
        )
        query = CallbackQuery(
            id=str(self.id), from_user=self.user(user), chat_instance="test", message=msg, data=button.callback_data
        )
        await self.dispatcher.feed_update(self.bot, Update(update_id=self.id, callback_query=query))

    def last_text(self):
        return next(m.text for m in reversed(self.api.requests) if isinstance(m, (SendMessage, EditMessageText)))


async def test_campaign_wizard_through_aiogram_mock_api(env):
    e, _, transport, _ = env
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Рассылки")
    await h.click("Создать")
    assert "Шаг 1 из 9" in h.last_text()
    await h.text("Новая кампания")
    await h.text("Первый текст")
    await h.text("Второй текст")
    await h.click("Далее")
    assert "Шаг 3 из 9" in h.last_text()
    await h.click("Далее")
    await h.text("600")
    await h.click("Далее")
    await h.text("20")
    assert "Шаг 7 из 9" in h.last_text()
    await h.click("Приветствия")
    await h.click("Далее")
    assert "Предпросмотр" in h.last_text()
    await h.click("Далее")
    await h.click("Сохранить")
    record = e.db.one("SELECT * FROM campaigns")
    assert record["name"] == "Новая кампания" and record["status"] == "DRAFT"
    assert record["interval_seconds"] == 600 and record["chat_limit"] == 20
    assert len(e.templates(record["id"])) == 2
    await h.click("Запустить")
    assert "Подтвердите" in h.last_text()
    await h.click("Подтвердить")
    assert e.db.scalar("SELECT status FROM campaigns") == "RUNNING"
    await h.click("Остановить рассылку")
    assert e.db.scalar("SELECT status FROM campaigns") == "STOPPED"
    assert not transport.calls
    await h.bot.session.close()


async def test_account_authentication_and_import_through_ui(env):
    e, _, transport, _ = env
    e.create_campaign("Единственная", ["Текст"])
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Аккаунты")
    await h.click("Добавить")
    await h.text("+380111111111")
    await h.text("00000")
    assert e.db.scalar("SELECT onboarding_stage FROM accounts") == "SILENT_24H"
    assert e.db.scalar("SELECT campaign_id FROM campaign_members") == 1
    assert sum(isinstance(m, DeleteMessage) for m in h.api.requests) == 2
    await h.click("Главное меню")
    await h.click("Аккаунты")
    await h.click("Добавить")
    await h.text("+380222222222")
    await h.text("00000")
    pair = e.db.one("SELECT * FROM pairs WHERE account_a_id IS NOT NULL")
    assert pair["account_a_id"] and pair["account_b_id"]
    assert "Партнёр: Аккаунт 1111" in h.last_text()
    await h.click("Открыть пару")
    assert "A · Аккаунт 1111" in h.last_text() and "B · Аккаунт 2222" in h.last_text()
    await h.click("Главное меню")
    await h.click("Пул чатов")
    await h.click("Импорт")
    await h.text("-100000\n-100001\n-100000")
    await h.click("право автоматизированной")
    await h.click("Импортировать")
    assert e.db.scalar("SELECT count(*) FROM chat_pool") == 2
    assert not transport.calls
    await h.bot.session.close()


async def test_compact_menu_updates_in_place_and_retains_all_sections(env):
    e, _, transport, _ = env
    h = Harness(e, transport)
    await h.text("/start")
    main = h.api.requests[-1]
    assert [len(row) for row in main.reply_markup.inline_keyboard] == [2, 2, 2, 2, 2]
    assert len(main.text) < 180
    await h.click("Пары")
    assert isinstance(h.api.requests[-1], EditMessageText)
    assert "автоматически" in h.last_text()
    assert "Связать" not in " ".join(b.text for row in h.api.requests[-1].reply_markup.inline_keyboard for b in row)
    await h.click("Главное меню")
    await h.click("Настройки")
    await h.click("Очередь и повторы")
    assert sum(isinstance(method, SendMessage) for method in h.api.requests) == 1
    assert all(len(row) <= 2 for row in h.api.requests[-1].reply_markup.inline_keyboard)
    await h.click("Размер пакета")
    await h.text("12")
    assert e.runtime_options()["batch_size"] == 12
    assert "Очередь и повторы" in h.last_text()
    await h.click("Главное меню")
    await h.click("Статистика")
    assert "Последние события" not in h.last_text()
    await h.click("Подробности")
    assert "Последние события" in h.last_text()
    await h.click("Кратко")
    assert "Последние события" not in h.last_text()
    await h.bot.session.close()


async def test_dm_greeting_fields_are_editable_through_the_bot(env):
    e, _, transport, _ = env
    campaign = e.create_campaign("Own campaign", ["Text"])
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Автоответы")
    await h.click("Own campaign")
    await h.click("Привет в ЛС")
    await h.text("Добрый день!")
    assert e.db.scalar("SELECT dm_greeting_phrase FROM campaigns WHERE id=?", (campaign,)) == "Добрый день!"
    assert "Автоответы" in h.last_text()
    await h.click("Стикер:")
    await h.text("🙂")
    assert e.db.scalar("SELECT dm_sticker_emoji FROM campaigns WHERE id=?", (campaign,)) == "🙂"
    assert not transport.calls
    await h.bot.session.close()


@pytest.mark.parametrize("reason", ["message is not modified", "message can't be edited", "message to edit not found"])
async def test_menu_edit_handles_old_messages_without_losing_navigation(reason):
    from unittest.mock import AsyncMock

    from aiogram.exceptions import TelegramBadRequest

    from app.bot.interface import View
    from app.bot.runtime import update_view

    bot = AsyncMock()
    bot.edit_message_text.side_effect = TelegramBadRequest(
        method=EditMessageText(text="menu", chat_id=123, message_id=10), message=reason
    )
    message = Message(
        message_id=10, date=datetime.now(timezone.utc), chat=Chat(id=123, type="private"), text="old menu"
    )
    await update_view(bot, message, View("new menu", [[("Home", "test:home")]]))
    assert bot.send_message.await_count == (0 if "not modified" in reason else 1)


async def test_unauthorized_aiogram_messages_and_callbacks(env):
    e, _, transport, _ = env
    h = Harness(e, transport)
    await h.text("/start", user=999)
    assert h.last_text() == "Доступ закрыт"
    await h.text("/start")
    await h.click("Аккаунты", user=999)
    response = h.api.requests[-1]
    assert isinstance(response, AnswerCallbackQuery) and response.text == "Доступ закрыт"
    await h.bot.session.close()


async def test_notification_buttons_make_one_replacement(env):
    e, _, transport, _ = env
    _, pair, a, _, chats = setup_pair(e, 1, extra=2)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    e.hourly_summary()
    h = Harness(e, transport)
    sink = BotNotificationSink(h.bot, h.ui)
    await e.deliver_notifications(sink)
    assert "№1" in h.last_text()
    await h.click("Убрать №1")
    assert e.pair_card(pair)["chats"][0]["chat_pool_id"] == chats[1]
    assert "Назначена замена" in h.last_text()
    await h.bot.session.close()


def test_no_external_network_connection_allowed():
    import socket

    from pytest_socket import SocketConnectBlockedError

    with socket.socket() as sock, pytest.warns(UserWarning), pytest.raises(SocketConnectBlockedError):
        sock.connect(("149.154.167.50", 443))
