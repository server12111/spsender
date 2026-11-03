"""Owner-managed pools and gates for the transferred operational code."""

import json
import re

from .core import DomainError, row, rows


class LegacyFeatures:
    def set_chat_interval(self, chat_id, seconds):
        seconds = int(seconds)
        if seconds and seconds < 30:
            raise DomainError("Интервал — минимум 30 секунд; 0 — общий интервал рассылки")
        with self.db.tx() as connection:
            assignment = row(
                connection, "SELECT id FROM pair_chats WHERE chat_pool_id=? AND removed_at IS NULL", (chat_id,)
            )
            if not assignment:
                raise DomainError("Чат пока не назначен паре")
            connection.execute(
                "UPDATE pair_chats SET interval_seconds=? WHERE id=?", (seconds or None, assignment["id"])
            )
            self.audit(connection, "CHAT_INTERVAL_CHANGED", chat_pool_id=chat_id, seconds=seconds)

    def _record_legacy_delivery(self, connection, operation, context, result):
        job_id = operation["dispatch_job_id"]
        if not job_id or row(connection, "SELECT 1 FROM message_results WHERE job_id=?", (job_id,)):
            return
        campaign = row(connection, "SELECT * FROM campaigns WHERE id=?", (context["campaign_id"],))
        now = self.clock()
        connection.execute(
            "UPDATE pairs SET pair_dispatch_started_at=COALESCE(pair_dispatch_started_at,?) WHERE id=?",
            (operation["created_at"], context["pair_id"]),
        )
        interval = (
            row(connection, "SELECT interval_seconds FROM pair_chats WHERE id=?", (context["id"],))["interval_seconds"]
            or campaign["interval_seconds"]
        )
        connection.execute(
            "UPDATE pair_chats SET last_sent_at=?,cooldown_until=MAX(cooldown_until,?),last_result='CONFIRMED',delivery_seq=delivery_seq+1,message_cursor=? WHERE id=? AND delivery_seq=?",
            (now, now + interval, context["next_index"], context["id"], context["delivery_seq"]),
        )
        attempt = row(connection, "SELECT current_attempt_id FROM dispatch_jobs WHERE id=?", (job_id,))[
            "current_attempt_id"
        ]
        connection.execute(
            "INSERT OR IGNORE INTO message_results(job_id,attempt_id,api_message_id,confirmed_at,result) VALUES (?,?,?,?,?)",
            (job_id, attempt, getattr(result, "id", None), now, '{"legacy":true}'),
        )

    def warmup_pool(self, page=0):
        return self.db.all(
            "SELECT a.id,a.name,a.phone_mask,EXISTS(SELECT 1 FROM warmup_partners p WHERE p.account_id=a.id) AS selected "
            "FROM accounts a WHERE a.account_health<>'REMOVED' ORDER BY a.id LIMIT 9 OFFSET ?",
            (max(0, page) * 8,),
        )

    def set_warmup_partner(self, account_id, enabled, expected=None):
        with self.db.tx() as connection:
            if not row(connection, "SELECT id FROM accounts WHERE id=? AND account_health<>'REMOVED'", (account_id,)):
                raise DomainError("Аккаунт не найден")
            old = bool(row(connection, "SELECT 1 FROM warmup_partners WHERE account_id=?", (account_id,)))
            if expected is not None and old != bool(expected):
                raise DomainError("Пул уже изменился. Откройте его заново")
            if enabled:
                connection.execute("INSERT OR IGNORE INTO warmup_partners VALUES (?,?)", (account_id, self.clock()))
            else:
                connection.execute("DELETE FROM warmup_partners WHERE account_id=?", (account_id,))
            self.audit(connection, "WARMUP_POOL_CHANGED", account_id=account_id, enabled=enabled)

    def add_warmup_channels(self, text):
        values = list(dict.fromkeys(v.strip() for v in re.split(r"[\s,;]+", text) if v.strip()))
        if not values or len(values) > 1000:
            raise DomainError("Отправьте до 1000 ссылок или @username")
        normalized = []
        for value in values:
            value = re.sub(r"^(?:https?://)?(?:t\.me|telegram\.me)/", "", value, flags=re.I).rstrip("/")
            value = value.removeprefix("joinchat/")
            if not re.fullmatch(r"@?[A-Za-z][A-Za-z0-9_]{3,31}|\+[A-Za-z0-9_-]{5,128}", value):
                raise DomainError("Нужны ссылки Telegram на каналы/группы, @username либо приглашения +hash")
            normalized.append(value if value.startswith("+") else value.lstrip("@").lower())
        with self.db.tx() as connection:
            total = 0
            for value in normalized:
                total += connection.execute(
                    "INSERT OR IGNORE INTO warmup_channels(username,added_at) VALUES (?,?)", (value, self.clock())
                ).rowcount
            return total

    def remove_warmup_channel(self, channel_id):
        with self.db.tx() as connection:
            connection.execute("DELETE FROM warmup_channels WHERE id=?", (channel_id,))

    def legacy_account_allowed(self, account_id, connection=None):
        if connection is None:
            with self.db.read() as connection:
                return self.legacy_account_allowed(account_id, connection)
        pair = row(connection, "SELECT id FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
        if pair and self._rotation_pair_blocked(connection, pair["id"]):
            return False
        account = row(
            connection,
            "SELECT a.*,w.enabled,c.status AS campaign_status,m.account_id AS member "
            "FROM accounts a JOIN legacy_warmup_settings w ON w.account_id=a.id "
            "LEFT JOIN campaign_members m ON m.account_id=a.id LEFT JOIN campaigns c ON c.id=m.campaign_id WHERE a.id=?",
            (account_id,),
        )
        return bool(
            account
            and account["enabled"]
            and account["account_health"] == "OK"
            and not account["paused"]
            and account["flood_until"] <= self.clock()
            and account["onboarding_started_at"] <= self.clock() - 86400
            and (account["member"] is None or account["campaign_status"] == "RUNNING")
            and not row(
                connection,
                "SELECT 1 FROM legacy_rpc_journal WHERE account_id=? AND status='UNKNOWN' LIMIT 1",
                (account_id,),
            )
            and not row(
                connection, "SELECT 1 FROM dispatch_jobs WHERE account_id=? AND status='UNKNOWN' LIMIT 1", (account_id,)
            )
        )

    def recover_legacy_operations(self):
        with self.db.tx() as connection:
            for item in rows(
                connection,
                "SELECT id,account_id,dispatch_job_id FROM legacy_rpc_journal WHERE status='INFLIGHT' AND lease_until<=?",
                (self.clock(),),
            ):
                connection.execute(
                    "UPDATE legacy_rpc_journal SET status='UNKNOWN',error='WORKER_LOST' WHERE id=?", (item["id"],)
                )
                if item["dispatch_job_id"]:
                    connection.execute(
                        "UPDATE dispatch_jobs SET status='UNKNOWN',last_error='WORKER_LOST' WHERE id=?",
                        (item["dispatch_job_id"],),
                    )
                    connection.execute(
                        "UPDATE message_attempts SET outcome='UNKNOWN',error='WORKER_LOST' WHERE id=(SELECT current_attempt_id FROM dispatch_jobs WHERE id=?)",
                        (item["dispatch_job_id"],),
                    )
                self._notify(
                    connection,
                    f"legacy-unknown:{item['id']}",
                    "legacy_unknown",
                    {"operation_id": item["id"], "account_id": item["account_id"]},
                    80,
                )

    def resolve_legacy_unknown(self, operation_id, performed, api_message_id=None):
        with self.db.tx() as connection:
            item = row(connection, "SELECT * FROM legacy_rpc_journal WHERE id=? AND status='UNKNOWN'", (operation_id,))
            if not item:
                raise DomainError("Операция уже разобрана")
            # Never reconstruct or repeat a network request from a journal entry.
            # The interrupted conversation is discarded before the account resumes.
            connection.execute(
                "UPDATE legacy_rpc_journal SET status=?,updated_at=? WHERE id=?",
                ("CONFIRMED" if performed else "CANCELLED", self.clock(), operation_id),
            )
            if item["dispatch_job_id"]:
                connection.execute(
                    "UPDATE dispatch_jobs SET status=?,completed_at=? WHERE id=?",
                    ("CONFIRMED" if performed else "CANCELLED", self.clock(), item["dispatch_job_id"]),
                )
                connection.execute(
                    "UPDATE message_attempts SET outcome=? WHERE id=(SELECT current_attempt_id FROM dispatch_jobs WHERE id=?)",
                    ("CONFIRMED" if performed else "CANCELLED", item["dispatch_job_id"]),
                )
            if performed:
                import base64
                import pickle
                from types import SimpleNamespace

                result = SimpleNamespace(id=api_message_id)
                encoded = self.secrets.encrypt(base64.b64encode(pickle.dumps(result)).decode())
                connection.execute("UPDATE legacy_rpc_journal SET result=? WHERE id=?", (encoded, operation_id))
                context = json.loads(item["context"])
                if item["dispatch_job_id"]:
                    self._record_legacy_delivery(connection, item, context, result)
            for state in rows(
                connection, "SELECT key,value FROM legacy_runtime_state WHERE key LIKE 'warmup.dialog:%'"
            ):
                progress = json.loads(self.secrets.decrypt(state["value"]))
                if item["account_id"] in (progress["a"], progress["b"]):
                    connection.execute("DELETE FROM legacy_runtime_state WHERE key=?", (state["key"],))
            self.audit(
                connection,
                "LEGACY_UNKNOWN_RESOLVED",
                account_id=item["account_id"],
                operation_id=operation_id,
                performed=performed,
            )
