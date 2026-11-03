"""Per-account device fingerprints.

Every account must log in and run under its own realistic device profile instead
of a shared hand-made signature, so a set of accounts cannot be cluster-banned
as a farm by their identical device.
"""

import json

from test_bot import Harness

from app.transport.fingerprint import LEGACY_DEVICE, parse_profile, random_profile

KEYS = ("device_model", "system_version", "app_version")


def test_random_profile_shape_and_realism():
    seen = set()
    for _ in range(50):
        profile = random_profile()
        assert set(profile) == set(KEYS)
        assert all(profile[k] and isinstance(profile[k], str) for k in KEYS)
        assert profile != LEGACY_DEVICE
        seen.add(tuple(profile[k] for k in KEYS))
    assert len(seen) > 1  # draws vary across accounts


def test_parse_profile_falls_back_to_legacy_for_empty_or_invalid():
    assert parse_profile("") == LEGACY_DEVICE
    assert parse_profile("not json") == LEGACY_DEVICE
    assert parse_profile(json.dumps({"device_model": "x"})) == LEGACY_DEVICE  # missing keys
    good = {"device_model": "iPhone15,2", "system_version": "iOS 17.2", "app_version": "Telegram iOS 10.12"}
    assert parse_profile(json.dumps(good)) == good


def test_legacy_account_keeps_default_device(env):
    # Accounts created before the feature (empty device_profile) must not switch device.
    e, _, _, _ = env
    a, _ = e.add_account("Личный", "+380111111111", "session-a", 1001)
    stored = e.db.scalar("SELECT device_profile FROM accounts WHERE id=?", (a,))
    assert stored == ""
    assert parse_profile(stored) == LEGACY_DEVICE


async def test_new_account_gets_its_own_device_through_login(env):
    e, _, transport, _ = env
    e.create_campaign("Единственная", ["Текст"])
    h = Harness(e, transport)
    await h.text("/start")
    await h.click("Аккаунты")
    await h.click("Добавить")
    await h.text("Личный A")
    await h.text("+380111111111")
    await h.text("00000")
    stored = e.db.scalar("SELECT device_profile FROM accounts")
    assert stored, "login must persist a device profile"
    profile = json.loads(stored)
    assert set(profile) == set(KEYS)
    assert profile != LEGACY_DEVICE
    await h.bot.session.close()


def test_reconnect_repairs_device_with_new_session(env):
    e, _, _, _ = env
    a, _ = e.add_account("Личный", "+380111111111", "session-a", 1001)
    fresh = json.dumps(random_profile())
    e.save_session(a, "renewed-session", 1001, fresh)
    assert e.db.scalar("SELECT device_profile FROM accounts WHERE id=?", (a,)) == fresh


def test_new_client_passes_device_to_telethon(env, monkeypatch):
    # The Telethon client must be built with the given device, falling back to legacy.
    import app.transport.telegram as tg

    e, _, _, _ = env
    transport = object.__new__(tg.TelegramTransport)
    transport.engine = e
    captured = {}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            captured.clear()
            captured.update(kwargs)

    monkeypatch.setattr(tg, "TelegramClient", FakeClient)

    profile = random_profile()
    transport.new_client("", device=profile)
    assert {k: captured[k] for k in KEYS} == profile

    transport.new_client("")  # no device -> legacy default
    assert {k: captured[k] for k in KEYS} == LEGACY_DEVICE
