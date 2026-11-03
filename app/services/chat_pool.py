import re

from .core import DomainError, row, rows


def parse_target(value):
    value = value.strip()
    if re.fullmatch(r"-?\d+(?::\d+)?", value):
        parts = value.split(":")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, value
    if re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,}", value):
        return None, 0, "@" + value[1:].lower()
    # Normalise a web link: drop the scheme, accept the telegram.me mirror and a www. prefix,
    # then strip any ?query / #fragment (e.g. ?start=ref) and the trailing slash before matching.
    url = re.sub(r"^https?://", "", value)
    url = re.sub(r"^(?:www\.)?(?:telegram|t)\.me/", "t.me/", url, count=1)
    url = url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    match = re.fullmatch(r"t\.me/c/(\d+)/(\d+)(?:/\d+)?", url)
    if match:
        return int("-100" + match[1]), int(match[2]), value
    # Private invites: the modern +hash form and the legacy joinchat/hash form carry the same hash.
    match = re.fullmatch(r"t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)", url)
    if match:
        return None, 0, "https://t.me/+" + match[1]
    # Public @username, optionally behind the /s/ web-preview prefix, with an optional message/topic id.
    # Reserved path words (joinchat/s/c) are never usernames, so an incomplete invite like
    # t.me/joinchat/ is rejected instead of being read as @joinchat.
    match = re.fullmatch(r"t\.me/(?:s/)?([A-Za-z][A-Za-z0-9_]{3,})(?:/(\d+))?", url)
    if match and match[1].lower() not in {"joinchat", "s", "c"}:
        return None, int(match[2] or 0), "@" + match[1].lower()
    raise DomainError("Укажите ID[:тема], @username или ссылку t.me")


class ChatPool:
    def add_chat(
        self,
        chat_id,
        title,
        topic_id=0,
        link="",
        approved=False,
        greeting_allowed=False,
        split_allowed=False,
        join_allowed=False,
    ):
        if chat_id >= 0 or topic_id < 0:
            raise DomainError("Некорректный ID чата / темы")
        with self.db.tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO chat_pool(chat_id,topic_id,title,link,approved,greeting_allowed,split_allowed,join_allowed,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    chat_id,
                    topic_id,
                    title[:200],
                    link or str(chat_id),
                    approved,
                    greeting_allowed,
                    split_allowed,
                    join_allowed,
                    self.clock(),
                ),
            )
            chat = row(c, "SELECT * FROM chat_pool WHERE chat_id=? AND topic_id=?", (chat_id, topic_id))
            self._fill_waiting(c)
            self.audit(c, "CHAT_IMPORTED", chat_pool_id=chat["id"])
            return chat["id"]

    def _fill_waiting(self, c):
        # Stable FIFO for deficits, then creation order. One global active allocation per chat.
        pairs = rows(
            c,
            "SELECT p.* FROM pairs p JOIN campaigns ca ON ca.id=p.campaign_id LEFT JOIN pair_chats pc ON pc.pair_id=p.id AND pc.removed_at IS NULL WHERE ca.status<>'REMOVED' "
            "AND p.account_a_id IS NOT NULL GROUP BY p.id HAVING count(pc.id)<p.chat_limit ORDER BY COALESCE(p.waiting_since,p.created_at),p.id",
        )
        for pair in pairs:
            count = c.execute(
                "SELECT count(*) FROM pair_chats WHERE pair_id=? AND removed_at IS NULL", (pair["id"],)
            ).fetchone()[0]
            deficit = max(0, pair["chat_limit"] - count)
            free = rows(
                c,
                "SELECT cp.* FROM chat_pool cp WHERE cp.status='FREE' AND cp.approved=1 AND NOT EXISTS (SELECT 1 FROM dispatch_jobs j WHERE j.chat_pool_id=cp.id AND j.status IN ('INFLIGHT','UNKNOWN')) ORDER BY cp.id LIMIT ?",
                (deficit,),
            )
            for chat in free:
                c.execute("UPDATE chat_pool SET status='RESERVED' WHERE id=? AND status='FREE'", (chat["id"],))
                # Replacement inherits the pair's latest cooldown, never gets a free immediate extra send.
                cooldown = c.execute(
                    "SELECT COALESCE(MAX(cooldown_until),0) FROM pair_chats WHERE pair_id=? OR chat_pool_id=?",
                    (pair["id"], chat["id"]),
                ).fetchone()[0]
                c.execute(
                    "INSERT INTO pair_chats(pair_id,chat_pool_id,assigned_at,cooldown_until) VALUES (?,?,?,?)",
                    (pair["id"], chat["id"], self.clock(), cooldown if pair["pair_dispatch_started_at"] else 0),
                )
                c.execute("UPDATE chat_pool SET status='ASSIGNED' WHERE id=?", (chat["id"],))
                waiting_decision = row(
                    c,
                    "SELECT d.issue_id FROM chat_issue_decisions d JOIN chat_issues i ON i.id=d.issue_id "
                    "WHERE i.pair_id=? AND d.action='REMOVE' AND d.replacement_chat_id IS NULL ORDER BY d.decided_at,d.issue_id LIMIT 1",
                    (pair["id"],),
                )
                if waiting_decision:
                    c.execute(
                        "UPDATE chat_issue_decisions SET replacement_chat_id=? WHERE issue_id=?",
                        (chat["id"], waiting_decision["issue_id"]),
                    )
                for account_id in (pair["account_a_id"], pair["account_b_id"]):
                    if account_id:
                        c.execute(
                            "INSERT OR IGNORE INTO account_chat_access(account_id,chat_pool_id) VALUES (?,?)",
                            (account_id, chat["id"]),
                        )
                self.audit(
                    c, "CHAT_ASSIGNED", campaign_id=pair["campaign_id"], pair_id=pair["id"], chat_pool_id=chat["id"]
                )
            if len(free) < deficit:
                c.execute(
                    "UPDATE pairs SET waiting_since=COALESCE(waiting_since,?),status=? WHERE id=?",
                    (
                        self.clock(),
                        "WAITING_REPLACEMENT" if pair["status"] == "WAITING_REPLACEMENT" else "WAITING_CHATS",
                        pair["id"],
                    ),
                )
            elif deficit:
                c.execute("UPDATE pairs SET waiting_since=NULL,status='PREPARING' WHERE id=?", (pair["id"],))
            if not pair["account_b_id"]:
                c.execute("UPDATE pairs SET status='WAITING_PARTNER' WHERE id=?", (pair["id"],))

    def set_chat_permissions(self, chat_id, approved, join_allowed=False, greeting_allowed=False, split_allowed=False):
        with self.db.tx() as c:
            c.execute(
                "UPDATE chat_pool SET approved=?,join_allowed=?,greeting_allowed=?,split_allowed=? WHERE id=?",
                (approved, join_allowed, greeting_allowed, split_allowed, chat_id),
            )
            if not approved:
                c.execute(
                    "UPDATE pair_chats SET status='PAUSED_NEEDS_REVIEW' WHERE chat_pool_id=? AND removed_at IS NULL",
                    (chat_id,),
                )
            self._fill_waiting(c)
            self.audit(c, "CHAT_PERMISSION_CHANGED", chat_pool_id=chat_id, approved=approved)

    def chat_failure(self, pair_id, chat_pool_id, account_id, reason, forbidden=False):
        with self.db.tx() as c:
            self._chat_failure(c, pair_id, chat_pool_id, account_id, reason, forbidden)

    def _chat_failure(self, c, pair_id, chat_pool_id, account_id, reason, forbidden=False):
        now = self.clock()
        c.execute(
            "UPDATE account_chat_access SET last_error=?,can_write=CASE WHEN ? THEN 0 ELSE can_write END WHERE account_id=? AND chat_pool_id=?",
            (reason, forbidden, account_id, chat_pool_id),
        )
        c.execute(
            "INSERT INTO chat_failures VALUES (?,?,?,?,?,?) ON CONFLICT(pair_id,chat_pool_id,account_id) "
            "DO UPDATE SET last_at=excluded.last_at,reason=excluded.reason",
            (pair_id, chat_pool_id, account_id, now, now, reason),
        )
        count = c.execute(
            "SELECT count(*) FROM chat_failures WHERE pair_id=? AND chat_pool_id=?", (pair_id, chat_pool_id)
        ).fetchone()[0]
        c.execute(
            "UPDATE pair_chats SET last_error=? WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
            (reason, pair_id, chat_pool_id),
        )
        if count >= 2 or forbidden:
            c.execute(
                "UPDATE pair_chats SET status='PAUSED_NEEDS_REVIEW' WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
                (pair_id, chat_pool_id),
            )
            c.execute("UPDATE chat_pool SET status='PAUSED_NEEDS_REVIEW' WHERE id=?", (chat_pool_id,))
            issue = row(
                c,
                "SELECT * FROM chat_issues WHERE pair_id=? AND chat_pool_id=? AND status='OPEN'",
                (pair_id, chat_pool_id),
            )
            if issue:
                c.execute("UPDATE chat_issues SET last_at=?,reason=? WHERE id=?", (now, reason, issue["id"]))
            else:
                c.execute(
                    "INSERT INTO chat_issues(pair_id,chat_pool_id,reason,first_at,last_at) VALUES (?,?,?,?,?)",
                    (pair_id, chat_pool_id, reason, now, now),
                )
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE pair_id=? AND chat_pool_id=? AND status='PENDING'",
                (pair_id, chat_pool_id),
            )
        self.audit(c, "CHAT_ERROR", pair_id=pair_id, account_id=account_id, chat_pool_id=chat_pool_id, reason=reason)

    def decide_issue(self, issue_id, action, admin_id):
        self.authorize(admin_id)
        if action not in {"KEEP", "REMOVE"}:
            raise DomainError("Неизвестное решение")
        with self.db.tx() as c:
            previous = row(c, "SELECT * FROM chat_issue_decisions WHERE issue_id=?", (issue_id,))
            if previous:
                return previous["replacement_chat_id"]
            issue = row(c, "SELECT * FROM chat_issues WHERE id=? AND status='OPEN'", (issue_id,))
            if not issue:
                raise DomainError("Проблема уже закрыта")
            c.execute(
                "INSERT INTO chat_issue_decisions(issue_id,action,admin_id,decided_at) VALUES (?,?,?,?)",
                (issue_id, action, admin_id, self.clock()),
            )
            c.execute("UPDATE chat_issues SET status=? WHERE id=?", (action, issue_id))
            replacement = None
            if action == "REMOVE":
                c.execute(
                    "UPDATE pair_chats SET removed_at=?,status='REMOVED' WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
                    (self.clock(), issue["pair_id"], issue["chat_pool_id"]),
                )
                c.execute("UPDATE chat_pool SET status='ARCHIVED' WHERE id=?", (issue["chat_pool_id"],))
                c.execute(
                    "UPDATE pairs SET status='WAITING_REPLACEMENT',waiting_since=COALESCE(waiting_since,?) WHERE id=?",
                    (self.clock(), issue["pair_id"]),
                )
                old_ids = {
                    r["chat_pool_id"]
                    for r in rows(
                        c,
                        "SELECT chat_pool_id FROM pair_chats WHERE pair_id=? AND removed_at IS NULL",
                        (issue["pair_id"],),
                    )
                }
                self._fill_waiting(c)
                new = rows(
                    c,
                    "SELECT chat_pool_id FROM pair_chats WHERE pair_id=? AND removed_at IS NULL ORDER BY id",
                    (issue["pair_id"],),
                )
                replacement = next((r["chat_pool_id"] for r in new if r["chat_pool_id"] not in old_ids), None)
                c.execute(
                    "UPDATE chat_issue_decisions SET replacement_chat_id=? WHERE issue_id=?", (replacement, issue_id)
                )
            self.audit(
                c,
                "ISSUE_" + action,
                pair_id=issue["pair_id"],
                chat_pool_id=issue["chat_pool_id"],
                admin_id=admin_id,
                replacement=replacement,
            )
            return replacement

    def resume_chat(self, chat_id):
        # Explicit admin confirmation is required by the UI before this call.
        with self.db.tx() as c:
            chat = row(c, "SELECT * FROM chat_pool WHERE id=?", (chat_id,))
            if not chat or not chat["approved"]:
                raise DomainError("Сначала подтвердите право публикации")
            c.execute(
                "UPDATE chat_pool SET status=CASE WHEN EXISTS(SELECT 1 FROM pair_chats WHERE chat_pool_id=? AND removed_at IS NULL) "
                "THEN 'ASSIGNED' ELSE 'FREE' END WHERE id=?",
                (chat_id, chat_id),
            )
            c.execute(
                "UPDATE pair_chats SET status='ASSIGNED',last_error=NULL WHERE chat_pool_id=? AND removed_at IS NULL",
                (chat_id,),
            )
            c.execute("UPDATE account_chat_access SET manual_required=0 WHERE chat_pool_id=?", (chat_id,))
            c.execute("DELETE FROM chat_failures WHERE chat_pool_id=?", (chat_id,))
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE chat_pool_id=? AND status='FAILED' AND kind='JOIN'",
                (chat_id,),
            )
            c.execute("UPDATE chat_issues SET status='CLOSED' WHERE chat_pool_id=? AND status='OPEN'", (chat_id,))
            self.audit(c, "CHAT_RESUMED", chat_pool_id=chat_id)

    def require_manual_verification(self, account_id, telegram_chat_id):
        with self.db.tx() as c:
            pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
            if not pair:
                return
            for chat in rows(
                c,
                "SELECT cp.* FROM chat_pool cp JOIN pair_chats pc ON pc.chat_pool_id=cp.id WHERE cp.chat_id=? AND pc.pair_id=? AND pc.removed_at IS NULL",
                (telegram_chat_id, pair["id"]),
            ):
                previous = row(
                    c,
                    "SELECT manual_required FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
                    (account_id, chat["id"]),
                )
                if previous and previous["manual_required"]:
                    continue
                c.execute(
                    "INSERT INTO account_chat_access(account_id,chat_pool_id,manual_required) VALUES (?,?,1) "
                    "ON CONFLICT(account_id,chat_pool_id) DO UPDATE SET manual_required=1",
                    (account_id, chat["id"]),
                )
                c.execute("UPDATE chat_pool SET status='WAITING_MANUAL_VERIFICATION' WHERE id=?", (chat["id"],))
                c.execute(
                    "UPDATE pair_chats SET status='WAITING_MANUAL_VERIFICATION' WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
                    (pair["id"], chat["id"]),
                )
                c.execute(
                    "UPDATE dispatch_jobs SET status='CANCELLED' WHERE chat_pool_id=? AND status='PENDING'",
                    (chat["id"],),
                )
                self._notify(
                    c,
                    f"manual:{account_id}:{chat['id']}:{self.clock()}",
                    "manual",
                    {
                        "chat_pool_id": chat["id"],
                        "link": chat["link"],
                        "title": chat["title"],
                        "account_id": account_id,
                    },
                    50,
                )
                self.audit(c, "MANUAL_REQUIRED", pair_id=pair["id"], account_id=account_id, chat_pool_id=chat["id"])

    def archive_chat(self, chat_id):
        with self.db.tx() as c:
            assigned = row(c, "SELECT * FROM pair_chats WHERE chat_pool_id=? AND removed_at IS NULL", (chat_id,))
            c.execute(
                "UPDATE pair_chats SET status='REMOVED',removed_at=? WHERE chat_pool_id=? AND removed_at IS NULL",
                (self.clock(), chat_id),
            )
            c.execute("UPDATE chat_pool SET status='ARCHIVED' WHERE id=?", (chat_id,))
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE chat_pool_id=? AND status='PENDING'", (chat_id,)
            )
            if assigned:
                c.execute(
                    "UPDATE pairs SET status='WAITING_REPLACEMENT',waiting_since=? WHERE id=?",
                    (self.clock(), assigned["pair_id"]),
                )
                self._fill_waiting(c)
            self.audit(c, "CHAT_ARCHIVED", chat_pool_id=chat_id)
