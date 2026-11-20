import asyncio
import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.methods import DeleteMessage
from conftest import setup_pair
from telethon import errors, types
from test_bot import Harness
from test_legacy_transfer import native_runtime

from app.bot.interface import Interface
from app.legacy import services
from app.services.core import DomainError
from app.transport.telegram import AccountAuthorizer


def live_authorizer(e, *, sent=None):
    e.config = replace(e.config, mode="live", api_id=12345, api_hash="a" * 32)
    client = SimpleNamespace(
        connect=AsyncMock(),
        disconnect=AsyncMock(),
        send_code_request=AsyncMock(
            return_value=sent
            or types.auth.SentCode(type=types.auth.SentCodeTypeApp(5), phone_code_hash="test-secret-hash", timeout=60)
        ),
        sign_in=AsyncMock(return_value=SimpleNamespace(id=1001, bot=False, phone="380111111111")),
        session=SimpleNamespace(save=lambda: "test-secret-session"),
    )
    transport = SimpleNamespace(new_client=lambda **kwargs: client)
    return AccountAuthorizer(e, transport), client


async def test_duplicate_phone_request_reuses_pending_code_without_second_rpc(env):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    await auth.begin(123, "+380111111111")
    await auth.begin(123, "+380111111111")
    client.send_code_request.assert_awaited_once()
    assert auth.pending[123]["hash"] == "test-secret-hash"
    await auth.cancel(123)


async def test_flood_wait_survives_authorizer_restart_and_blocks_new_code_requests(env):
    e, clock, _, _ = env
    auth, client = live_authorizer(e)
    client.send_code_request.side_effect = errors.FloodWaitError(request=None, capture=300)
    with pytest.raises(Exception):
        await auth.begin(123, "+380111111111")
    auth, fresh = live_authorizer(e)
    with pytest.raises(DomainError):
        await auth.begin(123, "+380222222222")
    fresh.send_code_request.assert_not_awaited()
    clock.advance(301)
    await auth.begin(123, "+380222222222")
    fresh.send_code_request.assert_awaited_once()
    await auth.cancel(123)


async def test_cancelled_login_cannot_immediately_request_another_code(env):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    await auth.begin(123, "+380111111111")
    await auth.cancel(123)
    with pytest.raises(DomainError):
        await auth.begin(123, "+380111111111")
    client.send_code_request.assert_awaited_once()
    assert "+380111111111" not in str(e.db.all("SELECT * FROM settings"))


@pytest.mark.parametrize("api_id,api_hash", [(0, ""), (123, "bad"), (-1, "a" * 32)])
async def test_invalid_api_settings_rejected_before_network(env, api_id, api_hash):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    e.config = replace(e.config, api_id=api_id, api_hash=api_hash)
    with pytest.raises(DomainError, match="API"):
        await auth.begin(123, "+380111111111")
    client.connect.assert_not_awaited()


@pytest.mark.parametrize(
    "stage,expected", [("account_phone", "API"), ("account_code", "код"), ("account_password", "парол")]
)
async def test_authentication_errors_are_actionable_and_secrets_never_logged(env, caplog, stage, expected):
    e, _, api, _ = env
    h = Harness(e, api)
    h.ui.set_state(123, stage, {"name": "Test"})
    secret = "SECRET_PASSWORD_AND_HASH_927362"
    error_cls = {
        "account_phone": errors.ApiIdInvalidError,
        "account_code": errors.PhoneCodeInvalidError,
        "account_password": errors.PasswordHashInvalidError,
    }[stage]
    error = error_cls(request=secret)
    h.ui.auth.begin = AsyncMock(side_effect=error)
    h.ui.auth.finish = AsyncMock(side_effect=error)
    try:
        with caplog.at_level(logging.WARNING):
            await h.text("+380111111111" if stage == "account_phone" else secret)
        assert expected.lower() in h.last_text().lower()
        assert secret not in caplog.text
        assert not any(record.exc_info for record in caplog.records)
        assert any(isinstance(request, DeleteMessage) for request in h.api.requests)
    finally:
        await h.bot.session.close()


async def test_login_prompt_reports_actual_delivery_method(env):
    e, _, api, _ = env
    auth, _ = live_authorizer(e)
    h = Harness(e, api)
    h.ui.auth = auth
    h.ui.set_state(123, "account_phone", {"name": "Test"})
    try:
        await h.text("+380111111111")
        assert "приложени" in h.last_text().lower() and "SMS" in h.last_text()
    finally:
        await auth.cancel(123)
        await h.bot.session.close()


@pytest.mark.parametrize("result", ["signup", "bot"])
async def test_authentication_cannot_save_signup_required_or_bot_session(env, result):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    await auth.begin(123, "+380111111111")
    client.sign_in.return_value = (
        types.auth.AuthorizationSignUpRequired() if result == "signup" else SimpleNamespace(id=111, bot=True)
    )
    with pytest.raises(DomainError):
        await auth.finish(123, code="12345")
    assert not auth.pending


async def test_import_pending_is_not_treated_as_phone_login(env):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    auth.pending[123] = {"import": True, "client": client, "at": e.clock()}
    with pytest.raises(DomainError):
        await auth.finish(123, code="12345")
    client.sign_in.assert_not_awaited()
    await auth.cancel(123)


def test_pools_are_persistent_and_silent_accounts_cannot_warm_up(env):
    e, clock, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1, ready=False)
    e.set_warmup_partner(a, True)
    assert e.add_warmup_channels("https://t.me/mychannel @mychannel @otherchannel") == 2
    assert len(e.warmup_pool()) == 2
    assert e.db.scalar("SELECT count(*) FROM chat_pool") == len(chats)
    assert not e.legacy_account_allowed(a)
    clock.advance(86400)
    assert e.legacy_account_allowed(a)
    for health in ("FROZEN", "RESTRICTED", "SESSION_EXPIRED", "DISCONNECTED"):
        with e.db.tx() as c:
            c.execute("UPDATE accounts SET account_health=? WHERE id=?", (health, a))
        assert not e.legacy_account_allowed(a)


@pytest.mark.parametrize("offline", [True, False])
async def test_original_warmup_really_issues_telegram_status_rpc(env, monkeypatch, offline):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    monkeypatch.setattr(services.random, "choice", lambda options: offline)
    await runtime.service._act_status_toggle(await runtime.get_client(a))
    assert len(native.requests) == 1
    assert isinstance(native.requests[0], __import__("telethon").functions.account.UpdateStatusRequest)
    assert native.requests[0].offline == offline
    assert not native.sent


@pytest.mark.parametrize("connected", [True, False])
def test_account_card_displays_connection_without_connecting_or_exposing_secrets(env, connected):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.config = replace(e.config, mode="live", api_id=12345, api_hash="a" * 32)
    transport = SimpleNamespace(clients={a: SimpleNamespace(is_connected=lambda: connected)})
    ui = Interface(e, transport)
    view = ui.card(123, "accounts", a)
    assert ("Соединение Telegram: подключён" if connected else "Соединение Telegram: не подключён") in view.text
    assert "session-a" not in view.text and e.config.api_hash not in view.text
    assert "одна пара API" in ui.listing(123, "settings").text


async def test_wrong_code_and_password_preserve_login_and_do_not_resend(env):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    await auth.begin(123, "+380111111111")
    client.sign_in.side_effect = errors.PhoneCodeInvalidError(request=None)
    with pytest.raises(errors.PhoneCodeInvalidError):
        await auth.finish(123, code="12345")
    assert auth.pending[123]["hash"] == "test-secret-hash"
    client.sign_in.side_effect = errors.SessionPasswordNeededError(request=None)
    assert await auth.finish(123, code="12345") is None
    client.sign_in.side_effect = errors.PasswordHashInvalidError(request=None)
    with pytest.raises(errors.PasswordHashInvalidError):
        await auth.finish(123, password="test-secret-password")
    client.sign_in.side_effect = None
    assert await auth.finish(123, password="right-test-password") == ("+380111111111", "test-secret-session", 1001)
    client.send_code_request.assert_awaited_once()
    assert not auth.pending
    persisted = str(e.db.all("SELECT * FROM settings")) + str(e.db.all("SELECT * FROM service_diagnostics"))
    for secret in ("test-secret-hash", "test-secret-password", "test-secret-session", "+380111111111"):
        assert secret not in persisted


async def test_flood_during_signin_blocks_password_attempts_and_never_shortens_wait(env):
    e, clock, _, _ = env
    auth, client = live_authorizer(e)
    await auth.begin(123, "+380111111111")
    client.sign_in.side_effect = errors.FloodWaitError(request=None, capture=300)
    with pytest.raises(errors.FloodWaitError):
        await auth.finish(123, code="12345")
    auth._extend_wait("auth:api-wait", 10)
    with pytest.raises(DomainError):
        await auth.finish(123, password="test-password")
    assert client.sign_in.await_count == 1
    clock.advance(301)
    client.sign_in.side_effect = None
    await auth.finish(123, code="12345")


@pytest.mark.parametrize("kind", [types.auth.SentCodeTypeSms(5), types.auth.SentCodeTypeEmailCode("a***@mail.test", 5)])
async def test_actual_delivery_method_is_reported_without_echoing_email(env, kind):
    e, _, _, _ = env
    sent = types.auth.SentCode(type=kind, phone_code_hash="hash", timeout=60)
    auth, _ = live_authorizer(e, sent=sent)
    hint = await auth.begin(123, "+380111111111")
    assert "SMS" in hint if isinstance(kind, types.auth.SentCodeTypeSms) else "почт" in hint
    assert "mail.test" not in hint
    await auth.cancel(123)


async def test_two_authorizers_cannot_race_into_duplicate_code_requests(env):
    e, _, _, _ = env
    first, a = live_authorizer(e)
    second, b = live_authorizer(e)
    results = await asyncio.gather(
        first.begin(123, "+380111111111"), second.begin(123, "+380111111111"), return_exceptions=True
    )
    assert a.send_code_request.await_count + b.send_code_request.await_count == 1
    assert sum(isinstance(result, DomainError) for result in results) == 1
    await first.cancel(123)
    await second.cancel(123)


async def test_empty_code_hash_never_advances_to_unusable_code_entry(env):
    e, _, _, _ = env
    sent = types.auth.SentCode(type=types.auth.SentCodeTypeApp(5), phone_code_hash="", timeout=60)
    auth, client = live_authorizer(e, sent=sent)
    with pytest.raises(DomainError):
        await auth.begin(123, "+380111111111")
    assert not auth.pending
    client.disconnect.assert_awaited_once()
