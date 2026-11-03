import asyncio
import io
from pathlib import Path
from uuid import uuid4

from aiogram import Bot, Dispatcher, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.services.core import DomainError

from .interface import View
from .labels import label


def markup(view):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=callback) for text, callback in buttons]
            for buttons in view.buttons
        ]
    )


def shorten(text, limit):
    return text.encode("utf-16-le")[: limit * 2].decode("utf-16-le", errors="ignore")


async def send_view(bot, chat_id, view):
    if view.media:
        path = Path(view.media).resolve()
        if not path.is_relative_to(Path("data/media").resolve()) or not path.is_file():
            raise DomainError("Медиа недоступно")
        options = dict(chat_id=chat_id, caption=shorten(view.text, 1024), reply_markup=markup(view))
        if view.media_kind == "photo":
            return await bot.send_photo(photo=FSInputFile(view.media), **options)
        if view.media_kind == "video":
            return await bot.send_video(video=FSInputFile(view.media), **options)
        return await bot.send_document(document=FSInputFile(view.media), **options)
    return await bot.send_message(chat_id, shorten(view.text, 4096), reply_markup=markup(view))


async def update_view(bot, message, view):
    # Keep navigation in place; media previews remain separate messages.
    if getattr(message, "text", None) is not None and not view.media:
        try:
            return await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=message.message_id,
                text=shorten(view.text, 4096),
                reply_markup=markup(view),
            )
        except TelegramBadRequest as exc:
            reason = exc.message.lower()
            if "message is not modified" in reason:
                return message
            if not any(
                part in reason
                for part in (
                    "message can't be edited",
                    "message can not be edited",
                    "message to edit not found",
                )
            ):
                raise
    return await send_view(bot, message.chat.id, view)


def register_message(ui, user, message_id):
    with ui.e.db.tx() as c:
        return bool(
            c.execute(
                "INSERT OR IGNORE INTO admin_update_receipts VALUES (?,?,?)", (user, message_id, ui.e.clock())
            ).rowcount
        )


def make_dispatcher(ui):
    router = Router()

    def allowed(user_id, chat):
        return user_id in ui.e.config.admin_ids and chat.type == "private" and chat.id == user_id

    @router.message(CommandStart())
    async def start(message: Message):
        if not message.from_user or message.chat.type != "private":
            return
        if not allowed(message.from_user.id, message.chat):
            await message.answer("Доступ закрыт")
            return
        await ui.e.db.run(ui.clear, message.from_user.id)
        if ui.auth:
            await ui.auth.cancel(message.from_user.id)
        await send_view(message.bot, message.chat.id, await ui.e.db.run(ui.main, message.from_user.id))

    @router.callback_query()
    async def callback(query: CallbackQuery):
        if not query.message or not allowed(query.from_user.id, query.message.chat):
            await query.answer("Доступ закрыт", show_alert=True)
            return

        async def acknowledge():
            try:
                await query.answer()
            except Exception as exc:
                await ui.e.db.run(ui.e.diagnostic, "callback-ack", exc)

        acknowledgement = asyncio.create_task(acknowledge())
        try:
            try:
                view = await ui.callback(query.from_user.id, query.data or "")
            except (DomainError, ValueError, PermissionError) as exc:
                text = (
                    str(exc)[:180] if isinstance(exc, (DomainError, PermissionError)) else "Проверьте введённые данные"
                )
                view = View(text, await ui.e.db.run(ui.nav, query.from_user.id))
            await update_view(query.bot, query.message, view)
        except Exception as exc:
            await ui.e.db.run(ui.e.diagnostic, "UI", exc)
            try:
                await query.bot.send_message(query.message.chat.id, "Не удалось обновить экран. Откройте раздел заново")
            except Exception as report_error:
                await ui.e.db.run(ui.e.diagnostic, "UI-response", report_error)
        finally:
            if not acknowledgement.done():
                acknowledgement.cancel()
            await asyncio.gather(acknowledgement, return_exceptions=True)

    @router.message()
    async def input_message(message: Message):
        if not message.from_user or message.chat.type != "private":
            return
        user = message.from_user.id
        if not allowed(user, message.chat):
            await message.answer("Доступ закрыт")
            return
        try:
            if not await ui.e.db.run(register_message, ui, user, message.message_id):
                return
            state = await ui.e.db.run(ui.state, user)
        except Exception as exc:
            await ui.e.db.run(ui.e.diagnostic, "UI-database", exc)
            await message.answer("База временно недоступна. Повторите ввод позже")
            return
        sensitive = state and state["state"] in {"account_code", "account_password", "account_phone"}
        try:
            text = message.text if message.text is not None else message.caption or ""
            template, txt_content = None, None
            document = message.document
            if state and state["state"] == "import_chats" and document:
                if not (document.file_name or "").lower().endswith(".txt") or (document.file_size or 0) > 1024 * 1024:
                    raise DomainError("Нужен TXT UTF-8 размером до 1 МБ")
                buffer = io.BytesIO()
                await message.bot.download(document, destination=buffer)
                txt_content = buffer.getvalue().decode("utf-8-sig")
            elif message.photo or message.video or document:
                if (
                    not state
                    or state["state"] not in {"campaign_wizard", "template_edit", "manual_dm"}
                    or (state["state"] == "campaign_wizard" and state["data"].get("step") not in {2, 3})
                ):
                    raise DomainError("Сначала откройте добавление текста или медиа через кнопки")
                kind = "photo" if message.photo else "video" if message.video else "document"
                media = message.photo[-1] if message.photo else message.video or document
                if (media.file_size or 0) > 20 * 1024 * 1024:
                    raise DomainError("Медиа не должно превышать 20 МБ")
                extension = {"photo": ".jpg", "video": ".mp4", "document": ".bin"}[kind]
                path = Path("data/media") / (uuid4().hex + extension)
                path.parent.mkdir(parents=True, exist_ok=True)
                await message.bot.download(media, destination=path)
                template = {"text": text, "media_path": str(path.resolve()), "media_kind": kind}
            view = await ui.message(user, text, template, txt_content)
        except (DomainError, ValueError, UnicodeError) as exc:
            error_text = str(exc) if isinstance(exc, DomainError) else "Проверьте формат данных и повторите ввод"
            view = View(error_text, ui.cancel(user))
        except Exception as exc:
            await ui.e.db.run(ui.e.diagnostic, "UI", exc)
            view = View("Действие не выполнено. Повторите ввод или отмените мастер.", ui.cancel(user))
        finally:
            if sensitive:
                try:
                    await message.delete()
                except Exception:
                    pass
        await send_view(message.bot, message.chat.id, view)

    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    return dispatcher


class BotNotificationSink:
    def __init__(self, bot: Bot, ui):
        self.bot, self.ui = bot, ui

    async def emergency(self, admin_id, payload):
        text = f"🔴 Сбой сохранения результата\nАккаунт №{payload['account_id']} · Задание №{payload['job_id']}\nПричина: {label(payload['reason'])}\nБаза временно недоступна. Автоматический повтор сообщения запрещён; проверьте результат вручную."
        await self.bot.send_message(admin_id, text)

    def build_view(self, admin_id, kind, payload):
        ui = self.ui
        if kind == "summary":
            view = ui.issues(admin_id, records=payload["issues"])
            if payload.get("total", len(payload["issues"])) > 6:
                view.buttons.insert(-1, [ui.button(admin_id, "▶️ Следующие проблемы", "issues", {"page": 1})])
                view.text += f"\nВсего нерешённых проблем: {payload['total']}"
        elif kind == "critical":
            campaign = ui.e.db.one("SELECT name FROM campaigns WHERE id=?", (payload["campaign_id"],))
            view = View(
                f"🔴 КРИТИЧЕСКОЕ СОБЫТИЕ\nАккаунт: №{payload['account_id']} · {payload['name']}\n"
                f"Проблема: {label(payload['health'])}\nРассылка: {campaign['name'] if campaign else '—'}\n"
                f"Пара: №{payload['pair_id'] or '—'}\nВремя: {ui.time(payload['at'])}\nДействие: аккаунт приостановлен автоматически.",
                [
                    [
                        ui.button(
                            admin_id, "🔎 Подробности", "card", {"section": "accounts", "id": payload["account_id"]}
                        ),
                        ui.button(admin_id, "🔄 Проверить", "check_account", {"id": payload["account_id"]}, True),
                    ],
                    [
                        ui.button(
                            admin_id,
                            "⏸ Оставить на паузе",
                            "account_action",
                            {"id": payload["account_id"], "do": "pause"},
                            True,
                        )
                    ],
                ],
            )
        elif kind == "manual":
            view = View(
                f"🛑 Нужна ручная проверка\n{payload['title']}\n{payload['link']}\nАккаунт №{payload['account_id']}\nАвтоматическое прохождение не выполняется.",
                [
                    [
                        ui.button(
                            admin_id, "🔎 Карточка чата", "card", {"section": "chats", "id": payload["chat_pool_id"]}
                        )
                    ]
                ],
            )
        elif kind == "campaign_error":
            view = View(
                f"⚠️ Рассылка №{payload['campaign_id']} приостановлена\nПричина: {label(payload['reason'])}\nИсправьте шаблон / медиа и возобновите рассылку.",
                [[ui.button(admin_id, "📣 Рассылка", "card", {"id": payload["campaign_id"], "section": "campaigns"})]],
            )
        elif kind == "reply_failed":
            view = View(
                f"⚠️ Ответ в ЛС не отправлен\nАккаунт №{payload['account_id']} · Собеседник {payload['peer_id']}\n"
                f"Причина: {label(payload['reason'])}\n"
                + (
                    "Рассылка приостановлена. "
                    if payload.get("campaign_paused")
                    else "Ошибка этого ЛС не блокирует пару. "
                )
                + "Подтверждённые части приветствия сохранены.",
                [[ui.button(admin_id, "🔎 Аккаунт", "card", {"id": payload["account_id"], "section": "accounts"})]],
            )
        elif kind == "overload":
            view = View(
                f"⚠️ Перегрузка входящих событий\nПропущено событий: {payload['count']}\nАвтоответы на них не запускались. Проверьте входящие аккаунтов вручную.",
                [[ui.button(admin_id, "🩺 Диагностика", "diagnostics")]],
            )
        elif kind == "incoming":
            view = View(
                f"📥 Входящее ЛС · {payload['name']}\nОт: {payload['peer_id']}\n{payload['text']}",
                [[ui.button(admin_id, "↩️ Ответить", "dm_reply", payload, True, ttl=600)]],
            )
        else:
            view = View(
                f"❓ Неизвестный результат задания №{payload['job_id']}\nАккаунт №{payload['account_id']}\nПроверьте историю Telegram. Автоматический повтор заблокирован.",
                [[ui.button(admin_id, "🔎 Сверить результат", "unknown")]],
            )
        return view

    async def send(self, admin_id, kind, payload):
        view = await self.ui.e.db.run(self.build_view, admin_id, kind, payload)
        await send_view(self.bot, admin_id, view)


async def serve(engine, transport, worker_only=False):
    from app.scheduler.worker import Worker
    from app.transport.telegram import AccountAuthorizer

    from .interface import Interface

    engine.config.validate_bot()
    bot = Bot(engine.config.bot_token)
    auth = AccountAuthorizer(engine, transport)
    ui = Interface(engine, transport, auth)
    sink = BotNotificationSink(bot, ui)
    worker = Worker(engine, transport, sink)
    if not await engine.db.run(worker.runtime_lease):
        await bot.session.close()
        raise RuntimeError("Другой процесс уже обслуживает Telegram")
    tasks = [asyncio.create_task(worker.run())]

    async def notifications(urgent):
        while True:
            try:
                await engine.deliver_notifications(sink, owner="urgent" if urgent else "normal", urgent=urgent)
                await auth.expire()
            except Exception as exc:
                await engine.db.run(engine.diagnostic, "notifications", exc)
            await asyncio.sleep(0.25 if urgent else 1)

    tasks.append(asyncio.create_task(notifications(True)))
    tasks.append(asyncio.create_task(notifications(False)))
    if not worker_only:
        dispatcher = make_dispatcher(ui)
        tasks.append(
            asyncio.create_task(dispatcher.start_polling(bot, handle_signals=False, tasks_concurrency_limit=32))
        )
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for admin_id in list(auth.pending):
            await auth.cancel(admin_id)
        try:
            await asyncio.wait_for(transport.close(), 10)
        except Exception as exc:
            await engine.db.run(engine.diagnostic, "shutdown", exc)
        await bot.session.close()
