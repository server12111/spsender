"""Durable chat imports; checking a username never publishes or joins anything."""

from uuid import uuid4

from .chat_pool import parse_target
from .core import DomainError, row


class ChatTargets:
    def _queue_chat_target(self, c, value, **options):
        chat_id, topic, link = parse_target(value)
        if chat_id is not None:
            return self._add_chat(c, chat_id, str(chat_id), topic, link, **options)
        existing = row(c, "SELECT id FROM chat_pool WHERE link=? AND topic_id=?", (link, topic))
        if existing:
            return existing["id"]
        flags = tuple(
            bool(options.get(key)) for key in ("approved", "join_allowed", "greeting_allowed", "split_allowed")
        )
        c.execute(
            "INSERT INTO chat_targets(link,topic_id,approved,join_allowed,greeting_allowed,split_allowed,created_at) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(link,topic_id) DO NOTHING",
            (link, topic, *flags, self.clock()),
        )
        # A fresh import can restore a deliberately removed entry. It cannot
        # silently reset a failed check or change an in-flight entry's consent.
        c.execute(
            "UPDATE chat_targets SET status='PENDING',approved=?,join_allowed=?,greeting_allowed=?,split_allowed=?,"
            "attempts=0,next_attempt_at=0,last_error=NULL WHERE link=? AND topic_id=? AND status='REMOVED'",
            (*flags, link, topic),
        )
        return c.execute("SELECT id FROM chat_targets WHERE link=? AND topic_id=?", (link, topic)).fetchone()[0]

    def chat_target_account(self, account_id=None):
        now = self.clock()
        return self.db.one(
            "SELECT a.id FROM accounts a JOIN account_sessions s ON s.account_id=a.id "
            "LEFT JOIN campaign_members m ON m.account_id=a.id LEFT JOIN campaigns ca ON ca.id=m.campaign_id "
            "LEFT JOIN legacy_warmup_settings w ON w.account_id=a.id "
            "WHERE a.account_health='OK' AND a.paused=0 AND a.onboarding_started_at<=? "
            "AND a.flood_until<=? AND a.connection_retry_at<=? AND (? IS NULL OR a.id=?) "
            "AND (ca.status='RUNNING' OR (m.account_id IS NULL AND w.enabled=1)) "
            "AND NOT EXISTS(SELECT 1 FROM dispatch_jobs j WHERE j.account_id=a.id AND j.status='UNKNOWN') "
            "AND NOT EXISTS(SELECT 1 FROM legacy_rpc_journal j WHERE j.account_id=a.id AND j.status='UNKNOWN') "
            "ORDER BY a.id LIMIT 1",
            (now - 86400, now, now, account_id, account_id),
        )

    def claim_chat_target(self, owner):
        with self.db.tx() as c:
            now = self.clock()
            target = row(
                c,
                "SELECT * FROM chat_targets WHERE approved=1 AND "
                "((status='PENDING' AND next_attempt_at<=?) OR (status='RESOLVING' AND lease_until<=?)) "
                "ORDER BY id LIMIT 1",
                (now, now),
            )
            if not target:
                return None
            token = owner + ":" + uuid4().hex
            c.execute(
                "UPDATE chat_targets SET status='RESOLVING',lease_owner=?,lease_until=? WHERE id=?",
                (token, now + 180, target["id"]),
            )
            target.update(status="RESOLVING", lease_owner=token, lease_until=now + 180)
            return target

    def finish_chat_target(self, target, resolved):
        with self.db.tx() as c:
            current = self._claimed_chat_target(c, target)
            if not current:
                return False
            chat_id, topic, link, title = resolved
            if topic != current["topic_id"]:
                raise DomainError("Тема чата изменилась при проверке")
            pool_id = self._add_chat(
                c,
                chat_id,
                title,
                topic,
                link,
                **{
                    key: bool(current[key]) for key in ("approved", "join_allowed", "greeting_allowed", "split_allowed")
                },
            )
            c.execute(
                "UPDATE chat_targets SET status='RESOLVED',chat_pool_id=?,lease_owner=NULL,lease_until=NULL,last_error=NULL "
                "WHERE id=?",
                (pool_id, current["id"]),
            )
            return True

    def _claimed_chat_target(self, c, target):
        return row(
            c,
            "SELECT * FROM chat_targets WHERE id=? AND status='RESOLVING' AND lease_owner=? AND lease_until>?",
            (target["id"], target["lease_owner"], self.clock()),
        )

    def fail_chat_target(self, target, error):
        options = self.runtime_options()
        with self.db.tx() as c:
            current = self._claimed_chat_target(c, target)
            if not current:
                return
            deferred = error.category in {"cancelled", "flood", "critical"}
            attempts = current["attempts"] + (0 if deferred else 1)
            review = error.category in {"chat", "manual", "forbidden", "configuration"} or (
                not deferred and attempts > options["max_retries"]
            )
            delay = (
                max(60, error.seconds)
                if deferred
                else min(options["retry_max"], options["retry_base"] * 2 ** min(max(0, attempts - 1), 10))
            )
            c.execute(
                "UPDATE chat_targets SET status=?,attempts=?,next_attempt_at=?,last_error=?,lease_owner=NULL,lease_until=NULL "
                "WHERE id=?",
                ("REVIEW" if review else "PENDING", attempts, self.clock() + delay, error.code, current["id"]),
            )

    def change_chat_target(self, target_id, action):
        if action not in {"retry", "remove"}:
            raise DomainError("Неизвестное действие")
        with self.db.tx() as c:
            target = row(c, "SELECT * FROM chat_targets WHERE id=?", (target_id,))
            if not target or target["status"] in {"RESOLVED", "REMOVED"}:
                raise DomainError("Откройте пул чатов заново")
            if action == "retry" and target["status"] != "REVIEW":
                raise DomainError("Чат уже ожидает проверки")
            c.execute(
                "UPDATE chat_targets SET status=?,attempts=0,next_attempt_at=0,last_error=NULL,lease_owner=NULL,lease_until=NULL "
                "WHERE id=?",
                ("PENDING" if action == "retry" else "REMOVED", target_id),
            )
