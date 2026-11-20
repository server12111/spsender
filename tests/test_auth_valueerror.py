from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_authorization_audit import live_authorizer

from app.transport.authorization import authorization_hint
from app.transport.telegram import TelegramTransport


@pytest.mark.parametrize("stage", ["create_client", "connect", "request_code", "cleanup_previous"])
async def test_valueerror_reports_stage_and_stack_without_exception_message(env, stage):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    secret = "API_HASH_CODE_PASSWORD_MUST_NOT_LEAK"
    if stage == "create_client":

        def fail(**kwargs):
            raise ValueError(secret)

        auth.transport.new_client = fail
    elif stage == "cleanup_previous":
        auth.pending[123] = {"client": client, "at": e.clock()}
        client.disconnect.side_effect = ValueError(secret)
    else:
        getattr(client, "connect" if stage == "connect" else "send_code_request").side_effect = ValueError(secret)
    try:
        with pytest.raises(ValueError) as caught:
            await auth.begin(123, "+380111111111")
        row = e.db.one(
            "SELECT * FROM service_diagnostics WHERE component='authorization-step' ORDER BY id DESC LIMIT 1"
        )
        assert row and row["error_code"] == "ValueError"
        assert stage in row["detail"] and "telegram.py" in row["detail"]
        assert "telethon_version" in row["detail"] and "opentele_version" in row["detail"]
        assert secret not in row["detail"]
        assert stage in authorization_hint(caught.value)
        assert secret not in authorization_hint(caught.value)
    finally:
        client.disconnect.side_effect = None
        await auth.cancel(123)


def test_client_factory_uses_unambiguous_credentials_after_opentele_patch(env, monkeypatch):
    e, _, _, _ = env
    live_authorizer(e)
    captured = {}

    def patched_factory(session=None, api=None, api_id=0, api_hash=None, **kwargs):
        assert api is None, "a positional api_id was interpreted as opentele's API profile"
        captured.update(api_id=api_id, api_hash=api_hash)
        return SimpleNamespace()

    monkeypatch.setattr("app.transport.telegram.TelegramClient", patched_factory)
    TelegramTransport.new_client(SimpleNamespace(engine=e), updates=False)
    assert captured == {"api_id": 12345, "api_hash": "a" * 32}


async def test_cleanup_failure_does_not_hide_original_request_failure(env):
    e, _, _, _ = env
    auth, client = live_authorizer(e)
    client.send_code_request.side_effect = ValueError("original-secret")
    client.disconnect = AsyncMock(side_effect=ConnectionError("cleanup-secret"))
    try:
        with pytest.raises(ValueError):
            await auth.begin(123, "+380111111111")
        assert auth.pending[123]["client"] is client
        assert e.db.scalar("SELECT count(*) FROM service_diagnostics WHERE component='authorization-cleanup'") == 1
        rows = str(e.db.all("SELECT * FROM service_diagnostics"))
        assert "original-secret" not in rows and "cleanup-secret" not in rows
    finally:
        client.disconnect.side_effect = None
        await auth.cancel(123)
