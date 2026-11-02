import json

from .core import dump, row, rows


class Notifications:
    def _notify(self, c, dedup, kind, payload, priority=0):
        for admin_id in self.config.admin_ids:
            c.execute(
                "INSERT OR IGNORE INTO notifications_outbox(dedup_key,admin_id,priority,kind,payload,next_attempt_at,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (f"{dedup}:{admin_id}", admin_id, priority, kind, dump(payload), self.clock(), self.clock()),
            )

    def critical(self, account_id, health, reason=None):
        with self.db.tx() as c:
            self._critical(c, account_id, health, reason or health)

    def _critical(self, c, account_id, health, reason):
        if health not in {"FROZEN", "SESSION_EXPIRED", "RESTRICTED", "DISCONNECTED"}:
            raise ValueError("Invalid critical health")
        account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
        if not account:
            return
        if account["account_health"] == health:
            if not account["paused"]:
                c.execute("UPDATE accounts SET paused=1 WHERE id=?", (account_id,))
            return
        c.execute("UPDATE accounts SET account_health=?,paused=1,last_error=? WHERE id=?", (health, reason, account_id))
        c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE account_id=? AND status='PENDING'", (account_id,))
        pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
        if pair:
            self._pause_clock(c, pair)
            c.execute("UPDATE pairs SET status='NEEDS_ATTENTION' WHERE id=?", (pair["id"],))
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE pair_id=? AND status='PENDING' AND kind IN ('MAILING','GREETING','GROUP_REPLY','DM_FIRST','DM_ALWAYS','DM_MANUAL')",
                (pair["id"],),
            )
        self.audit(
            c,
            "ACCOUNT_CRITICAL",
            account_id=account_id,
            pair_id=pair["id"] if pair else None,
            health=health,
            reason=reason,
        )
        event_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        self._notify(
            c,
            f"critical:{event_id}",
            "critical",
            {
                "account_id": account_id,
                "name": account["name"],
                "health": health,
                "reason": reason,
                "pair_id": pair["id"] if pair else None,
                "campaign_id": pair["campaign_id"] if pair else None,
                "at": self.clock(),
            },
            priority=100,
        )

    def hourly_summary(self):
        now = self.clock()
        with self.db.tx() as c:
            last = row(c, "SELECT value FROM settings WHERE key='last_hourly_summary'")
            if last and now - float(last["value"]) < 3600:
                return False
            issues = rows(
                c,
                "SELECT i.*,cp.title,cp.link FROM chat_issues i JOIN chat_pool cp ON cp.id=i.chat_pool_id "
                "WHERE i.status='OPEN' ORDER BY i.id LIMIT 6",
            )
            if not issues:
                return False
            c.execute(
                "INSERT INTO settings VALUES ('last_hourly_summary',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(now),),
            )
            total = c.execute("SELECT count(*) FROM chat_issues WHERE status='OPEN'").fetchone()[0]
            c.execute("UPDATE notifications_outbox SET status='CANCELLED' WHERE kind='summary' AND status='PENDING'")
            self._notify(c, f"hourly:{now}", "summary", {"issues": issues, "at": now, "total": total})
            self.audit(c, "HOURLY_SUMMARY", count=total)
            return True

    def _claim_notification(self, owner, urgent=None):
        from uuid import uuid4

        now = self.clock()
        with self.db.tx() as c:
            item = row(
                c,
                "SELECT * FROM notifications_outbox WHERE (status='PENDING' OR (status='SENDING' AND lease_until<=?)) AND next_attempt_at<=? AND (? IS NULL OR (?=1 AND priority>=80) OR (?=0 AND priority<80)) ORDER BY priority DESC,id LIMIT 1",
                (now, now, urgent, urgent, urgent),
            )
            if not item:
                return None
            fence = f"{owner}:{uuid4().hex}"
            c.execute(
                "UPDATE notifications_outbox SET status='SENDING',lease_owner=?,lease_until=?,attempts=attempts+1 WHERE id=?",
                (fence, now + 120, item["id"]),
            )
            return {**item, "fence": fence}

    def _finish_notification(self, item, error=None):
        with self.db.tx() as c:
            if error is None:
                c.execute(
                    "UPDATE notifications_outbox SET status='SENT',sent_at=? WHERE id=? AND status='SENDING' AND lease_owner=?",
                    (self.clock(), item["id"], item["fence"]),
                )
            else:
                delay = max(min(3600, 10 * 2 ** min(item["attempts"], 8)), float(getattr(error, "retry_after", 0)))
                c.execute(
                    "UPDATE notifications_outbox SET status='PENDING',next_attempt_at=?,last_error=? WHERE id=? AND status='SENDING' AND lease_owner=?",
                    (self.clock() + delay, type(error).__name__, item["id"], item["fence"]),
                )
                if getattr(error, "retry_after", 0):
                    c.execute(
                        "UPDATE notifications_outbox SET next_attempt_at=MAX(next_attempt_at,?) WHERE admin_id=? AND status='PENDING'",
                        (self.clock() + delay, item["admin_id"]),
                    )
                self.audit(c, "NOTIFICATION_FAILED", notification_id=item["id"], error=type(error).__name__)

    async def deliver_notifications(self, sink, owner="notifier", limit=20, urgent=None):
        import asyncio

        delivered = 0
        for _ in range(limit):
            item = await self.db.run(self._claim_notification, owner, urgent)
            if not item:
                break
            if item["admin_id"] not in self.config.admin_ids:
                # Removed admins must never receive queued confidential notifications.
                await self.db.run(self._finish_notification, item)
                continue
            try:
                await asyncio.wait_for(sink.send(item["admin_id"], item["kind"], json.loads(item["payload"])), 30)
            except Exception as exc:
                await self.db.run(self._finish_notification, item, exc)
            else:
                await self.db.run(self._finish_notification, item)
                delivered += 1
        return delivered
