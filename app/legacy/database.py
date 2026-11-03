"""Bridge the original warmup database API to the application's SQLite writer."""

import asyncio
import re
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo


class Cursor:
    def __init__(self, records, lastrowid):
        self.records, self.lastrowid = records, lastrowid

    async def fetchall(self):
        return self.records

    async def fetchone(self):
        return self.records[0] if self.records else None


class Query:
    def __init__(self, database, sql, args):
        self.database, self.sql, self.args = database, sql, args

    def __await__(self):
        return self.run().__await__()

    async def __aenter__(self):
        return await self.run()

    async def __aexit__(self, *args):
        return False

    async def run(self):
        def execute():
            sql = re.sub(r"\bFROM accounts\b", "FROM legacy_account_catalog", self.sql, flags=re.I)
            manager = (
                self.database.engine.db.read
                if sql.lstrip().upper().startswith(("SELECT", "PRAGMA"))
                else self.database.engine.db.tx
            )
            with manager() as connection:
                cursor = connection.execute(sql, self.args)
                return Cursor(cursor.fetchall() if cursor.description else [], cursor.lastrowid)

        return await self.database.engine.db.run(execute)


class Connection:
    def __init__(self, database):
        self.database = database

    def execute(self, sql, args=()):
        return Query(self.database, sql, args)

    async def commit(self):
        # Every facade write already committed through the shared SQLite writer.
        return None


class LegacyDatabase:
    is_pg = False

    def __init__(self, engine):
        self.engine = engine
        if not hasattr(engine, "_legacy_state_revision"):
            engine._legacy_state_revision = max(
                time.time_ns(), engine.db.scalar("SELECT COALESCE(MAX(updated_at),0) FROM legacy_runtime_state")
            )
        self._conn = Connection(self)
        self._transaction_lock = asyncio.Lock()
        self._snapshot_lock = asyncio.Lock()
        self._snapshot_at = 0
        self._account_snapshot = []
        self._warmup_snapshot = {}
        self._warmup_objects = {}
        self._zone = ZoneInfo(engine.get_setting("timezone"))

    async def snapshot(self):
        if time.monotonic() - self._snapshot_at < 2:
            return
        async with self._snapshot_lock:
            if time.monotonic() - self._snapshot_at < 2:
                return
            accounts = await self.engine.db.run(
                self.engine.db.all,
                "SELECT a.*,s.encrypted_phone FROM accounts a LEFT JOIN account_sessions s ON s.account_id=a.id WHERE a.account_health<>'REMOVED' ORDER BY a.id",
            )
            warmups = await self.engine.db.run(self.engine.db.all, "SELECT * FROM legacy_warmup_settings")
            self._account_snapshot = []
            for account in accounts:
                account.update(
                    user_id=self.engine.config.admin_ids[0],
                    phone=self.engine.secrets.decrypt(account.pop("encrypted_phone"))
                    if account.get("encrypted_phone")
                    else "",
                    is_active=account["account_health"] == "OK" and not account["paused"],
                    created_at=self.local(account["onboarding_started_at"]),
                )
                self._account_snapshot.append(SimpleNamespace(**account))
            self._warmup_snapshot = {item["account_id"]: item for item in warmups}
            self._warmup_objects.clear()
            self._zone = ZoneInfo(self.engine.get_setting("timezone"))
            self._snapshot_at = time.monotonic()

    def local(self, timestamp):
        if timestamp is None:
            return None
        local = datetime.fromtimestamp(timestamp, self._zone)
        return local.replace(tzinfo=timezone(local.utcoffset()))

    def timestamp(self, local):
        return (
            (local if local.tzinfo else local.replace(tzinfo=ZoneInfo(self.engine.get_setting("timezone")))).timestamp()
            if local
            else None
        )

    @staticmethod
    def is_free_ad_active(user):
        return False

    async def get_user_by_id(self, user_id):
        if user_id not in self.engine.config.admin_ids:
            return None
        return SimpleNamespace(
            id=user_id, telegram_id=user_id, is_admin=True, subscription_end=datetime.max.replace(tzinfo=timezone.utc)
        )

    async def get_account(self, account_id):
        item = await self.engine.db.run(
            self.engine.db.one,
            "SELECT a.*,s.encrypted_phone FROM accounts a LEFT JOIN account_sessions s ON s.account_id=a.id WHERE a.id=?",
            (account_id,),
        )
        if not item:
            return None
        item.update(
            user_id=self.engine.config.admin_ids[0],
            phone=self.engine.secrets.decrypt(item["encrypted_phone"]) if item["encrypted_phone"] else "",
            is_active=item["account_health"] == "OK" and not item["paused"],
            created_at=self.local(item["onboarding_started_at"]),
        )
        return SimpleNamespace(**item)

    async def get_user_accounts(self, user_id):
        if user_id not in self.engine.config.admin_ids:
            return []
        await self.snapshot()
        return self._account_snapshot

    async def get_warmup(self, account_id):
        await self.snapshot()
        if account_id in self._warmup_objects:
            return self._warmup_objects[account_id]
        item = self._warmup_snapshot.get(account_id)
        if item is None:
            item = await self.engine.db.run(
                self.engine.db.one, "SELECT * FROM legacy_warmup_settings WHERE account_id=?", (account_id,)
            )
        if not item:
            return None
        item = dict(item)
        for key in ("started_at", "last_action_at", "next_action_at", "last_join_at"):
            item[key] = self.local(item[key])
        item.update(
            joined_chats_count=item.pop("chats_joined"),
            subscribed_channels_count=item.pop("channels_subscribed"),
            last_action_type=item.pop("last_action"),
        )
        result = SimpleNamespace(**item)
        self._warmup_objects[account_id] = result
        return result

    async def get_enabled_warmup_account_ids(self):
        records = await self.engine.db.run(
            self.engine.db.all,
            "SELECT w.account_id FROM legacy_warmup_settings w JOIN accounts a ON a.id=w.account_id "
            "LEFT JOIN campaign_members m ON m.account_id=a.id LEFT JOIN campaigns c ON c.id=m.campaign_id "
            "WHERE w.enabled=1 AND a.account_health='OK' AND a.paused=0 AND a.onboarding_started_at<=? "
            "AND (m.account_id IS NULL OR c.status='RUNNING') ORDER BY a.id",
            (self.engine.clock() - 86400,),
        )
        return [item["account_id"] for item in records]

    async def record_warmup_action(
        self, account_id, action_type, next_action_at, stage=None, day_index=None, joined=False, subscribed=False
    ):
        def write():
            with self.engine.db.tx() as connection:
                connection.execute(
                    "UPDATE legacy_warmup_settings SET last_action_at=?,next_action_at=?,last_action=?,"
                    "stage=COALESCE(?,stage),day_index=COALESCE(?,day_index),chats_joined=chats_joined+?,"
                    "channels_subscribed=channels_subscribed+?,last_join_at=CASE WHEN ? OR ? THEN ? ELSE last_join_at END WHERE account_id=?",
                    (
                        self.engine.clock(),
                        self.timestamp(next_action_at),
                        action_type,
                        stage,
                        day_index,
                        joined,
                        subscribed,
                        joined,
                        subscribed,
                        self.engine.clock(),
                        account_id,
                    ),
                )

        await self.engine.db.run(write)
        self._warmup_snapshot.pop(account_id, None)
        self._warmup_objects.pop(account_id, None)

    async def get_flood_wait_until(self, account_id):
        value = await self.engine.db.run(
            self.engine.db.scalar, "SELECT flood_until FROM accounts WHERE id=?", (account_id,)
        )
        return self.local(value) if value else None

    async def set_flood_wait_until(self, account_id, until):
        # Legacy code caps waits at six hours. The shared guard always retains
        # a longer FloodWait already observed by the transport.
        if until:
            await self.engine.db.run(
                self.engine.flood_wait, account_id, max(1, self.timestamp(until) - self.engine.clock())
            )

    async def get_warmup_partners(self):
        records = await self.engine.db.run(
            self.engine.db.all,
            "SELECT p.account_id FROM warmup_partners p JOIN accounts a ON a.id=p.account_id "
            "WHERE a.account_health='OK' AND a.paused=0 ORDER BY p.added_at,p.account_id",
        )
        result = []
        for item in records:
            account = await self.get_account(item["account_id"])
            result.append((account.id, account.phone))
        return result

    async def get_warmup_channels(self):
        return await self.engine.db.run(self.engine.db.all, "SELECT * FROM warmup_channels ORDER BY id")
