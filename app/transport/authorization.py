"""Safe, actionable authorization diagnostics. Never interpolate RPC requests."""

import asyncio
import math
import traceback
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from telethon import errors

from app.services.core import DomainError


class AuthorizedSession(tuple):
    """The existing three-field result, plus its fixed API identity."""

    def __new__(cls, phone, session, telegram_id, credential_id=None):
        result = super().__new__(cls, (phone, session, telegram_id))
        result.credential_id = credential_id
        return result

    def __repr__(self):
        return "AuthorizedSession(<private>)"


def failure_context(exc, stage):
    """Call locations and versions only: no messages, source lines or locals."""
    result = {
        "stage": stage,
        "trace": [
            f"{Path(frame.f_code.co_filename).name}:{frame.f_code.co_name}:{line}"
            for frame, line in traceback.walk_tb(exc.__traceback__)
        ][-12:],
    }
    for package, key in (("Telethon", "telethon_version"), ("opentele-ng", "opentele_version")):
        try:
            result[key] = version(package)
        except PackageNotFoundError:
            result[key] = "not-installed"
    return result


def authorization_hint(exc):
    if isinstance(exc, DomainError):
        return str(exc)
    if isinstance(exc, errors.FloodWaitError):
        return f"Telegram ограничил попытки входа. Подождите {max(1, math.ceil(exc.seconds))} с. Повтор автоматически не выполняется."
    hints = {
        "ApiIdInvalidError": "Telegram отклонил API ID / API hash. Проверьте пару TELEGRAM_API_ID и TELEGRAM_API_HASH в настройках сервера.",
        "ApiIdPublishedFloodError": "Telegram ограничил этот API ID как опубликованный. Не повторяйте запрос и не меняйте API для обхода ограничения.",
        "PhoneNumberInvalidError": "Telegram отклонил номер. Проверьте международный формат с кодом страны.",
        "PhoneNumberBannedError": "Telegram заблокировал вход для этого номера. Автоматические повторы остановлены.",
        "PhoneNumberFloodError": "Слишком много запросов кода для номера. Подождите; не повторяйте запросы подряд.",
        "PhonePasswordFloodError": "Telegram ограничил попытки пароля. Подождите; не повторяйте ввод подряд.",
        "PhoneCodeInvalidError": "Неверный код Telegram. Введите последний полученный код; новый код не запрашивается.",
        "PhoneCodeEmptyError": "Введите код Telegram.",
        "PhoneCodeExpiredError": "Срок действия кода истёк. Отмените вход и начните заново после указанной паузы.",
        "PhoneCodeHashEmptyError": "Состояние входа потеряно. Отмените вход и начните заново.",
        "PasswordHashInvalidError": "Неверный пароль двухэтапной аутентификации. Проверьте пароль и раскладку клавиатуры.",
        "AuthRestartError": "Telegram потребовал начать авторизацию заново. Отмените вход и повторите после паузы.",
        "SmsCodeCreateFailedError": "Telegram не смог создать SMS-код. Проверьте уведомления в уже открытом приложении Telegram; повторяйте запрос только после паузы.",
        "UpdateAppToLoginError": "Telegram требует другой поддерживаемый способ входа. Используйте официальный клиент и импорт своей сессии.",
        "SessionPasswordNeededError": "Для входа нужен пароль двухэтапной аутентификации.",
        "ServerError": "Временный сбой Telegram при авторизации. Повтор запроса кода автоматически не выполнялся; подождите перед следующей попыткой.",
        "RpcCallFailError": "Telegram не обработал запрос входа. Автоматического повторного запроса кода нет; повторите позже.",
        "PhoneMigrateError": "Telegram несколько раз перенаправил вход между дата-центрами либо сессия уже авторизована. Вход остановлен без бесконечных повторов.",
        "NetworkMigrateError": "Telegram несколько раз перенаправил соединение между дата-центрами. Вход остановлен; повторите позже.",
        "UserMigrateError": "Telegram несколько раз перенаправил авторизацию между дата-центрами. Вход остановлен; повторите позже.",
    }
    name = type(exc).__name__
    if name in hints:
        return hints[name]
    if isinstance(exc, (asyncio.TimeoutError, ConnectionError, OSError)):
        return "Нет ответа от Telegram. Проверьте сеть сервера. Код мог быть отправлен до обрыва связи; сначала проверьте приложение Telegram."
    if isinstance(exc, errors.RPCError):
        return f"Telegram отклонил авторизацию ({name}). Отмените вход и проверьте диагностику; частые повторы не выполняйте."
    if isinstance(exc, ValueError):
        stage = getattr(exc, "authorization_stage", "unknown")
        return f"Ошибка Telegram-клиента (ValueError), этап {stage}. В журнале нужна строка authorization-step; повторные запросы кода пока не выполняйте."
    return "Не удалось выполнить авторизацию. Проверьте диагностику и настройки сервера."


def delivery_hint(sent):
    name = type(getattr(sent, "type", None)).__name__
    return {
        "SentCodeTypeApp": "Код отправлен в служебное сообщение в уже открытом приложении Telegram, а не в SMS.",
        "SentCodeTypeSms": "Telegram выбрал доставку кода по SMS.",
        "SentCodeTypeCall": "Telegram выбрал доставку кода голосовым звонком.",
        "SentCodeTypeFlashCall": "Telegram выбрал проверку через входящий звонок.",
        "SentCodeTypeMissedCall": "Telegram выбрал проверку через пропущенный звонок.",
        "SentCodeTypeEmailCode": "Telegram выбрал доставку кода на привязанную почту.",
        "SentCodeTypeFragmentSms": "Telegram выбрал доставку кода через Fragment; проверьте свой аккаунт Fragment.",
    }.get(name, "Способ доставки кода выбрал Telegram. Проверьте уже открытое приложение и доступные уведомления.")
