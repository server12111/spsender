from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.methods import DeleteMessage
from conftest import restart, setup_pair
from test_authorization_audit import live_authorizer
from test_bot import Harness
from test_hardening import LocalClient

from app.bot.interface import Interface
from app.legacy import session_import
from app.services.chat_pool import parse_target
from app.services.core import DomainError
from app.transport.base import OperationError
from app.transport.telegram import AccountAuthorizer, TelegramTransport


@pytest.mark.parametrize(
    "target",
    ["my_test_chat", "@my_test_chat", "t.me/my_test_chat", "https://t.me/my_test_chat", "telegram.me/my_test_chat"],
)
def test_usernames_and_links_have_same_normal_form(target):
    assert parse_target(target) == (None, 0, "@my_test_chat")


@pytest.mark.parametrize("target", ["example.com/foo", "https://evil.test/chat", "test!name", "x" * 33])
def test_invalid_usernames_are_rejected(target):
    with pytest.raises(DomainError):
        parse_target(target)


def test_api_hash_encrypted_and_pool_survives_restart(env):
    e, _, _, _ = env
    key = e.add_api_credential("My API", 11111, "a" * 32)
    assert e.add_api_credential("duplicate", 11111, "a" * 32) == key
    stored = e.db.one("SELECT * FROM api_credentials WHERE id=?", (key,))
    assert "a" * 32 not in str(stored)
    assert e.secrets.decrypt(stored["encrypted_hash"]) == "a" * 32
    other = restart(e)
    assert other.authorization_api().credential_id == key
    assert "a" * 32 not in repr(other.authorization_api())


def test_api_selection_balances_new_accounts_and_never_changes_binding(env):
    e, _, _, _ = env
    first = e.add_api_credential("A", 11111, "a" * 32)
    second = e.add_api_credential("B", 22222, "b" * 32)
    a, _ = e.add_account("A", "+380111111111", "session-a", 1001, api_credential_id=e.authorization_api().credential_id)
    assert e.authorization_api().credential_id == second
    b, _ = e.add_account("B", "+380222222222", "session-b", 1002, api_credential_id=second)
    e.set_api_enabled(first, False)
    assert e.authorization_api().credential_id == second
    assert e.account_api(a).credential_id == first
    assert restart(e).account_api(a).api_hash == "a" * 32
    with pytest.raises(DomainError):
        e.save_session(a, "wrong-api-session", 1001, api_credential_id=second)
    assert (
        e.secrets.decrypt(e.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (a,)))
        == "session-a"
    )
    assert e.account_api(b).credential_id == second


def test_existing_sessions_are_pinned_to_legacy_env_identity_on_upgrade(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    before = e.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (a,))
    e.config = replace(e.config, api_id=11111, api_hash="a" * 32)
    other = restart(e)
    assert other.account_api(a).api_id == 11111
    other.add_api_credential("New", 22222, "b" * 32)
    other.config = replace(other.config, api_id=22222, api_hash="b" * 32)
    again = restart(other)
    assert again.account_api(a).api_id == 11111
    assert again.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (a,)) == before


async def test_live_phone_login_uses_pool_without_env_and_save_pins_identity(env):
    e, _, _, _ = env
    key = e.add_api_credential("Pool", 22222, "b" * 32)
    auth, client = live_authorizer(e)
    e.config = replace(e.config, api_id=0, api_hash="")
    seen = []

    def factory(**kwargs):
        seen.append(kwargs["api_identity"])
        return client

    auth.transport.new_client = factory
    await auth.begin(123, "+380111111111")
    # A new entry or disabling this entry must not change an in-progress login.
    e.set_api_enabled(key, False)
    e.add_api_credential("Other", 33333, "c" * 32)
    result = await auth.finish(123, code="12345")
    assert seen[0].api_id == 22222 and result.credential_id == key
    ui = Interface(e, auth.transport, auth)
    await ui._save_authorized_account(123, result, {})
    account = e.db.scalar("SELECT id FROM accounts")
    assert e.account_api(account).api_hash == "b" * 32


async def test_reconnect_uses_existing_api_even_if_disabled_for_new_logins(env):
    e, _, _, _ = env
    key = e.add_api_credential("Old", 11111, "a" * 32)
    a, _ = e.add_account("A", "+380111111111", "old-session", 1001, api_credential_id=key)
    e.set_api_enabled(key, False)
    e.add_api_credential("New", 22222, "b" * 32)
    auth, client = live_authorizer(e)
    seen = []
    auth.transport.new_client = lambda **kw: seen.append(kw["api_identity"]) or client
    await auth.begin(123, "+380111111111", account_id=a)
    result = await auth.finish(123, code="12345")
    assert seen[0].credential_id == key and result.credential_id == key


async def test_api_pool_ui_deletes_secret_message_and_denies_other_users(env, caplog):
    e, _, api, _ = env
    h = Harness(e, api)
    try:
        await h.text("/start")
        await h.click("Настройки")
        await h.click("Пул API")
        await h.click("Добавить API")
        await h.text("My key | 11111 | " + "a" * 32)
        assert "API-пара сохранена" in h.last_text()
        assert "a" * 32 not in h.last_text() and "a" * 32 not in caplog.text
        assert any(isinstance(r, DeleteMessage) for r in h.api.requests)
        assert not e.db.scalar("SELECT count(*) FROM ui_states")
        assert "a" * 32 not in str(e.db.all("SELECT * FROM ui_actions"))
        await h.click("Выключить")
        assert not e.db.scalar("SELECT enabled FROM api_credentials")
        with pytest.raises(PermissionError):
            h.ui.api_pool_view(999)
    finally:
        await h.bot.session.close()


async def test_chat_import_accepts_mixed_username_batch(env):
    e, _, api, _ = env
    h = Harness(e, api)
    try:
        h.ui.set_state(123, "import_chats")
        await h.text("my_test_chat, @second_chat; https://t.me/third_chat")
        assert "Строк: 3" in h.last_text()
        await h.click("Есть право")
        await h.click("Импортировать")
        assert e.db.scalar("SELECT count(*) FROM chat_pool") == 3
        assert all(r["link"].startswith("@") for r in e.db.all("SELECT link FROM chat_pool"))
    finally:
        await h.bot.session.close()


def test_disabled_pool_does_not_fall_back_to_env_or_other_identity(env):
    e, _, _, _ = env
    key = e.add_api_credential("A", 11111, "a" * 32)
    e.set_api_enabled(key, False)
    e.config = replace(e.config, api_id=11111, api_hash="a" * 32)
    with pytest.raises(DomainError):
        e.authorization_api()


@pytest.mark.parametrize("api_id,api_hash", [(0, "a" * 32), (2**31, "a" * 32), (123, "bad")])
def test_invalid_pool_keys_never_persist(env, api_id, api_hash):
    e, _, _, _ = env
    with pytest.raises(DomainError):
        e.add_api_credential("Invalid", api_id, api_hash)
    assert not e.api_pool()


async def test_session_import_uses_pool_identity_without_env(env, monkeypatch):
    e, _, _, _ = env
    key = e.add_api_credential("Import API", 22222, "b" * 32)
    e.config = replace(e.config, mode="live", api_id=0, api_hash="")
    source = SimpleNamespace(disconnect=AsyncMock())
    native = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(id=777, phone="380111111111", bot=False)),
        session=SimpleNamespace(save=lambda: "imported-session"),
        disconnect=AsyncMock(),
    )
    monkeypatch.setattr(session_import, "sqlite_session_kind", lambda path: "telethon_session")
    monkeypatch.setattr(session_import, "load_import_source", AsyncMock(return_value=source))
    create = AsyncMock(return_value=native)
    monkeypatch.setattr(session_import, "create_new_session", create)
    auth = AccountAuthorizer(e, SimpleNamespace())
    result = await auth.import_file(123, "my.session", b"SQLite format 3\x00")
    assert result.credential_id == key
    assert create.await_args.args[1].api_id == 22222
    assert create.await_args.args[1].api_hash == "b" * 32
    assert not auth.pending
    native.disconnect.assert_awaited_once()
    source.disconnect.assert_awaited_once()


async def test_actual_transport_uses_fixed_identity_not_changed_global_config(env, monkeypatch):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    key = e.add_api_credential("Account API", 22222, "b" * 32)
    with e.db.tx() as c:
        e._bind_api(c, a, key)
    e.config = replace(e.config, mode="live", api_id=33333, api_hash="c" * 32)
    transport = TelegramTransport(e)
    native = LocalClient()
    seen = []

    def factory(session, **kwargs):
        seen.append(kwargs["api_identity"])
        return native

    monkeypatch.setattr(transport, "new_client", factory)
    try:
        await transport.client(a)
        assert seen[0].api_id == 22222 and seen[0].api_hash == "b" * 32
        assert await transport.client(a) is native
        assert len(seen) == 1
    finally:
        await transport.close()


def test_migration_13_adds_pool_without_changing_old_sessions(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    before = e.db.all("SELECT * FROM account_sessions ORDER BY account_id")
    with e.db.tx() as c:
        c.execute("DROP TABLE account_api_bindings")
        c.execute("DROP TABLE api_credentials")
        c.execute("DELETE FROM schema_migrations WHERE version LIKE '013%'")
    e.db.migrate()
    assert e.db.all("SELECT * FROM account_sessions ORDER BY account_id") == before
    assert e.db.scalar("SELECT count(*) FROM schema_migrations") == 13
    assert not e.api_pool()


async def test_dry_run_also_pins_selected_pool_identity_without_network(env):
    e, _, api, _ = env
    key = e.add_api_credential("Test", 11111, "a" * 32)
    auth = AccountAuthorizer(e, api)
    await auth.begin(123, "+380111111111")
    result = await auth.finish(123, code="00000")
    assert result.credential_id == key


async def test_missing_legacy_api_is_configuration_failure_not_expired_session(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.config = replace(e.config, mode="live", api_id=0, api_hash="")
    transport = TelegramTransport(e)
    with pytest.raises(OperationError) as exc:
        await transport.client(a)
    assert exc.value.category == "configuration" and exc.value.code == "ACCOUNT_API_REQUIRED"
    assert e.db.scalar("SELECT account_health FROM accounts WHERE id=?", (a,)) == "OK"
    assert not transport.clients
    await transport.close()
