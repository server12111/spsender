import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from app.bot.interface import Interface


def actions(engine, view, action):
    records = []
    for group in view.buttons:
        for _, token in group:
            record = engine.db.one("SELECT action,data FROM ui_actions WHERE token=?", (token[2:],))
            if record["action"] == action:
                records.append(json.loads(record["data"]))
    return records


async def test_preparation_page_explains_waits_without_starting_jobs(env):
    e, clock, transport, _ = env
    campaign = e.create_campaign("Main", ["Test"])
    account, _ = e.add_account("Own account", "+10000000001", "fake", 1001)
    ui = Interface(e, transport)
    view = ui.listing(123, "preparation")
    assert "автоматически" in view.text and "Тихий период до" in view.text
    assert actions(e, view, "card")[0]["id"] == account
    clock.advance(86400)
    assert "Рассылка: Черновик" in ui.preparation(123).text
    e.campaign_status(campaign, "RUNNING", confirmed=True)
    assert "Ожидает проверки подключения" in ui.preparation(123).text
    e.update_account(account, "pause")
    assert "Аккаунт на паузе" in ui.preparation(123).text
    assert not transport.calls
    assert e.db.scalar("SELECT count(*) FROM dispatch_jobs") == 0


def test_account_listing_uses_current_counters_not_delivery_history(env, monkeypatch):
    e, _, _, _ = env
    e.add_account("New", "+10000000001", "fake", 1001)
    removed, _ = e.add_account("Removed", "+10000000002", "fake", 1002)
    e.update_account(removed, "remove")

    def reject_history(*args, **kwargs):
        raise AssertionError("Account list must not aggregate delivery history")

    monkeypatch.setattr(e, "statistics", reject_history)
    view = Interface(e).listing(123, "accounts")
    assert "Подготовка: 1" in view.text
    assert "Аккаунты · 1" in view.text
    assert e.account_overview()["total"] == 1


async def test_campaign_picker_only_offers_free_accounts_and_pages_in_sql(env):
    e, _, _, _ = env
    first = e.create_campaign("First", ["Test"])
    second = e.create_campaign("Second", ["Test"])
    attached, _ = e.add_account("Attached", "+10000000000", "fake", 1000, campaign_id=first)
    free = [e.add_account(f"Free {i}", f"+100000000{i + 1:02d}", "fake", 1001 + i)[0] for i in range(9)]
    ui = Interface(e)
    view = await ui.callback(123, ui.button(123, "Choose", "choose_campaign_accounts", {"id": second})[1])
    assert [data["id"] for data in actions(e, view, "assign_account")] == free[:8]
    next_page = actions(e, view, "choose_campaign_accounts")[0]
    view = await ui.callback(123, ui.button(123, "Next", "choose_campaign_accounts", next_page)[1])
    assert [data["id"] for data in actions(e, view, "assign_account")] == free[8:]
    assert e.db.scalar("SELECT campaign_id FROM campaign_members WHERE account_id=?", (attached,)) == first
    assert all(len(group) <= 2 for group in view.buttons)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows output encoding regression")
def test_windows_migrations_print_russian_in_non_unicode_shell(env, tmp_path):
    e, _, _, _ = env
    child_env = {
        **os.environ,
        "PYTHONPATH": str(Path.cwd()),
        "PYTHONIOENCODING": "cp1252",
        "DATABASE_PATH": str(tmp_path / "console.sqlite3"),
        "SESSION_KEY": e.config.session_key,
        "MODE": "dry-run",
        "BOT_TOKEN": "",
        "ADMIN_IDS": "123",
        "TELEGRAM_API_ID": "",
        "TELEGRAM_API_HASH": "",
    }
    result = subprocess.run(
        [sys.executable, "-m", "app", "migrate"], cwd=tmp_path, env=child_env, capture_output=True, timeout=20
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    assert "Миграции SQLite применены" in result.stdout.decode("utf-8")
