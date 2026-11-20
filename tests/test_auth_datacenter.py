import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon import errors, functions, types
from test_authorization_audit import live_authorizer

from app.transport.telegram import TelegramTransport


class Sender:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0

    def send(self, request, **kwargs):
        self.calls += 1
        response = next(self.responses)
        future = asyncio.get_running_loop().create_future()
        if isinstance(response, Exception):
            future.set_exception(response)
        else:
            future.set_result(response)
        return future


def native_auth(e, responses):
    # Keep Telethon's actual send_code_request and _call, including the exact
    # failure path at users.py:139 from the server log. Replace only the wire.
    auth, _ = live_authorizer(e)
    client = TelegramTransport.new_client(SimpleNamespace(engine=e), updates=False)
    sender = Sender(responses)
    client._sender = sender
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=False)
    client._switch_dc = AsyncMock()
    auth.transport.new_client = lambda **kwargs: client
    return auth, client, sender


def sent():
    return types.auth.SentCode(type=types.auth.SentCodeTypeApp(5), phone_code_hash="test-hash", timeout=60)


@pytest.mark.parametrize("migration", [errors.PhoneMigrateError, errors.NetworkMigrateError, errors.UserMigrateError])
async def test_real_telethon_login_follows_rejected_wrong_datacenter_once(env, migration):
    e, _, _, _ = env
    auth, client, sender = native_auth(e, [migration(request=None, capture=2), sent()])
    try:
        await auth.begin(123, "+380111111111")
        assert sender.calls == 2
        client._switch_dc.assert_awaited_once_with(2)
        assert auth.pending[123]["hash"] == "test-hash"
    finally:
        await auth.cancel(123)


async def test_actual_server_error_is_preserved_and_code_is_not_retried(env):
    e, _, _, _ = env
    auth, client, sender = native_auth(e, [errors.ServerError(request=None, message="INTERNAL", code=500), sent()])
    with pytest.raises(errors.ServerError):
        await auth.begin(123, "+380111111111")
    assert sender.calls == 1
    client.disconnect.assert_awaited_once()
    assert not auth.pending
    assert (
        e.db.scalar("SELECT error_code FROM service_diagnostics WHERE component='authorization-step'") == "ServerError"
    )


async def test_real_floodwait_is_not_retried_or_changed_to_valueerror(env):
    e, clock, _, _ = env
    auth, _, sender = native_auth(e, [errors.FloodWaitError(request=None, capture=90), sent()])
    with pytest.raises(errors.FloodWaitError):
        await auth.begin(123, "+380111111111")
    assert sender.calls == 1
    assert float(e.db.scalar("SELECT value FROM settings WHERE key='auth:api-wait'")) >= clock() + 90


async def test_datacenter_pingpong_is_bounded(env):
    e, _, _, _ = env
    auth, _, sender = native_auth(
        e,
        [
            errors.PhoneMigrateError(request=None, capture=2),
            errors.PhoneMigrateError(request=None, capture=3),
            errors.PhoneMigrateError(request=None, capture=2),
            sent(),
        ],
    )
    with pytest.raises(errors.PhoneMigrateError):
        await auth.begin(123, "+380111111111")
    assert sender.calls == 3
    assert not auth.pending


def test_message_clients_still_have_zero_implicit_send_retries(env):
    e, _, _, _ = env
    live_authorizer(e)
    client = TelegramTransport.new_client(SimpleNamespace(engine=e))
    assert client._request_retries == 0
    assert client._raise_last_call_error


async def test_datacenter_followup_can_only_be_done_by_a_new_unauthorized_login(env):
    e, _, _, _ = env
    auth, client, sender = native_auth(e, [errors.PhoneMigrateError(request=None, capture=2), sent()])
    client.is_user_authorized = AsyncMock(return_value=True)
    with pytest.raises(errors.PhoneMigrateError):
        await auth.begin(123, "+380111111111")
    assert sender.calls == 1
    client._switch_dc.assert_not_awaited()


async def test_real_telethon_message_is_not_resent_on_server_error(env):
    e, _, _, _ = env
    _, client, sender = native_auth(
        e, [errors.ServerError(request=None, message="INTERNAL", code=500), types.UpdatesTooLong()]
    )
    with pytest.raises(errors.ServerError):
        await client(
            functions.messages.SendMessageRequest(peer=types.InputPeerChat(100000), message="Offline test", random_id=1)
        )
    assert sender.calls == 1
