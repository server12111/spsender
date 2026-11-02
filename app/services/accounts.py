from app.security import mask_phone

from .core import DomainError, row, rows


class Accounts:
    def add_account(self, name, phone, session="", telegram_id=None, campaign_id=None):
        if not name.strip():
            raise DomainError("Укажите название аккаунта")
        now = self.clock()
        with self.db.tx() as c:
            candidates = rows(c, "SELECT id,name FROM campaigns WHERE status<>'REMOVED' ORDER BY id")
            if campaign_id is None and len(candidates) == 1:
                campaign_id = candidates[0]["id"]
            account_id = c.execute(
                "INSERT INTO accounts(name,phone_mask,telegram_id,onboarding_started_at,stage_started_at,"
                "account_health,created_at) VALUES (?,?,?,?,?,?,?)",
                (
                    name.strip()[:100],
                    mask_phone(phone),
                    telegram_id,
                    now,
                    now,
                    "OK" if session else "DISCONNECTED",
                    now,
                ),
            ).lastrowid
            c.execute(
                "INSERT INTO account_sessions VALUES (?,?,?,?)",
                (account_id, self.secrets.encrypt(session), self.secrets.encrypt(phone), now),
            )
            c.execute(
                "INSERT INTO account_onboarding_events(account_id,stage,created_at) VALUES (?,?,?)",
                (account_id, "SILENT_24H", now),
            )
            if campaign_id:
                self._assign_account(c, account_id, campaign_id)
            self.audit(c, "ACCOUNT_CREATED", account_id=account_id, campaign_id=campaign_id)
            return account_id, candidates if len(candidates) > 1 and not campaign_id else []

    def _assign_account(self, c, account_id, campaign_id):
        campaign = self.campaign(c, campaign_id)
        account = row(c, "SELECT * FROM accounts WHERE id=? AND account_health<>'REMOVED'", (account_id,))
        if not account:
            raise DomainError("Аккаунт не найден")
        old = row(c, "SELECT * FROM campaign_members WHERE account_id=?", (account_id,))
        if old and old["campaign_id"] == campaign_id:
            return
        if row(c, "SELECT * FROM dispatch_jobs WHERE account_id=? AND status IN ('INFLIGHT','UNKNOWN')", (account_id,)):
            raise DomainError("Дождитесь результата текущей операции / разрешите неопределённый результат")
        self._detach_account(c, account_id)
        c.execute("INSERT INTO campaign_members VALUES (?,?)", (account_id, campaign_id))
        c.execute("UPDATE accounts SET membership_status='ATTACHED' WHERE id=?", (account_id,))
        waiting = row(
            c,
            "SELECT * FROM pairs WHERE campaign_id=? AND account_b_id IS NULL AND account_a_id IS NOT NULL ORDER BY id LIMIT 1",
            (campaign_id,),
        )
        if waiting:
            c.execute("UPDATE pairs SET account_b_id=?,status='PREPARING' WHERE id=?", (account_id, waiting["id"]))
        else:
            c.execute(
                "INSERT INTO pairs(campaign_id,account_a_id,chat_limit,created_at) VALUES (?,?,?,?)",
                (campaign_id, account_id, campaign["chat_limit"], self.clock()),
            )
        self._fill_waiting(c)
        self.audit(c, "ACCOUNT_ASSIGNED", account_id=account_id, campaign_id=campaign_id)

    def assign_account(self, account_id, campaign_id):
        with self.db.tx() as c:
            self._assign_account(c, account_id, campaign_id)

    def _detach_account(self, c, account_id):
        pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
        if pair:
            if row(
                c, "SELECT 1 FROM dispatch_jobs WHERE pair_id=? AND status IN ('INFLIGHT','UNKNOWN')", (pair["id"],)
            ):
                raise DomainError("Сначала дождитесь / сверьте текущую операцию пары")
            self._pause_clock(c, pair)
            c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE pair_id=? AND status='PENDING'", (pair["id"],))
            other = pair["account_b_id"] if pair["account_a_id"] == account_id else pair["account_a_id"]
            if other:
                # Clear first to respect UNIQUE(account_a_id)/UNIQUE(account_b_id).
                c.execute("UPDATE pairs SET account_b_id=NULL WHERE id=?", (pair["id"],))
                c.execute("UPDATE pairs SET account_a_id=?,status='WAITING_PARTNER' WHERE id=?", (other, pair["id"]))
            else:
                # Keep history and its FK references; a detached empty pair is not reused.
                c.execute("UPDATE pairs SET status='DISCONNECTED' WHERE id=?", (pair["id"],))
                for pc in rows(c, "SELECT * FROM pair_chats WHERE pair_id=? AND removed_at IS NULL", (pair["id"],)):
                    c.execute(
                        "UPDATE pair_chats SET removed_at=?,status='REMOVED' WHERE id=?", (self.clock(), pc["id"])
                    )
                    c.execute("UPDATE chat_pool SET status='FREE' WHERE id=?", (pc["chat_pool_id"],))
                c.execute("UPDATE pairs SET account_a_id=NULL WHERE id=?", (pair["id"],))
        c.execute("DELETE FROM campaign_members WHERE account_id=?", (account_id,))

    def update_account(self, account_id, action, value=None):
        with self.db.tx() as c:
            account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
            if not account:
                raise DomainError("Аккаунт не найден")
            if account["account_health"] == "REMOVED":
                raise DomainError("Аккаунт удалён")
            if action == "resume" and account["account_health"] != "OK":
                raise DomainError("Сначала восстановите авторизацию или подтвердите снятие ограничения")
            if action == "remove" and row(
                c, "SELECT 1 FROM dispatch_jobs WHERE account_id=? AND status IN ('INFLIGHT','UNKNOWN')", (account_id,)
            ):
                raise DomainError("Сначала дождитесь / сверьте результат текущей операции")
            if action == "rename":
                if not str(value).strip():
                    raise DomainError("Название не может быть пустым")
                c.execute("UPDATE accounts SET name=? WHERE id=?", (str(value)[:100], account_id))
            elif action in {"pause", "resume", "disconnect", "remove"}:
                c.execute("UPDATE accounts SET paused=? WHERE id=?", (action != "resume", account_id))
                if action in {"disconnect", "remove"}:
                    c.execute(
                        "UPDATE accounts SET account_health=? WHERE id=?",
                        ("REMOVED" if action == "remove" else "DISCONNECTED", account_id),
                    )
                    if action == "remove":
                        c.execute("DELETE FROM account_sessions WHERE account_id=?", (account_id,))
                        c.execute("UPDATE accounts SET membership_status='REMOVED' WHERE id=?", (account_id,))
                c.execute(
                    "UPDATE dispatch_jobs SET status='CANCELLED' WHERE account_id=? AND status='PENDING'", (account_id,)
                )
                pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
                if pair:
                    self._pause_clock(c, pair)
                    if action != "resume":
                        c.execute(
                            "UPDATE dispatch_jobs SET status='CANCELLED' WHERE pair_id=? AND status='PENDING' AND kind IN ('MAILING','GREETING','GROUP_REPLY','DM_FIRST','DM_ALWAYS','DM_MANUAL')",
                            (pair["id"],),
                        )
                if action == "remove":
                    self._detach_account(c, account_id)
                    self._fill_waiting(c)
            else:
                raise DomainError("Неизвестное действие")
            self.audit(c, "ACCOUNT_" + action.upper(), account_id=account_id)

    def save_session(self, account_id, session, telegram_id):
        with self.db.tx() as c:
            account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
            if not account or account["account_health"] == "REMOVED":
                raise DomainError("Аккаунт удалён")
            if account["telegram_id"] is not None and account["telegram_id"] != telegram_id:
                raise DomainError("Этот номер принадлежит другому Telegram аккаунту. Добавьте его отдельно")
            if row(
                c, "SELECT 1 FROM dispatch_jobs WHERE account_id=? AND status IN ('INFLIGHT','UNKNOWN')", (account_id,)
            ):
                raise DomainError("Сначала дождитесь / сверьте текущую операцию аккаунта")
            c.execute(
                "UPDATE account_sessions SET encrypted_session=?,updated_at=? WHERE account_id=?",
                (self.secrets.encrypt(session), self.clock(), account_id),
            )
            c.execute(
                "UPDATE accounts SET account_health=CASE WHEN account_health IN ('FROZEN','RESTRICTED') THEN account_health ELSE 'OK' END,"
                "paused=CASE WHEN account_health IN ('FROZEN','RESTRICTED') THEN 1 ELSE 0 END,telegram_id=?,last_error=NULL WHERE id=?",
                (telegram_id, account_id),
            )
            self.audit(c, "ACCOUNT_RECONNECTED", account_id=account_id)
            c.execute("UPDATE accounts SET connection_failures=0,connection_retry_at=0 WHERE id=?", (account_id,))
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE account_id=? AND kind='CHECK' AND status='FAILED'",
                (account_id,),
            )

    def acknowledge_restriction_resolved(self, account_id):
        with self.db.tx() as c:
            c.execute(
                "UPDATE accounts SET account_health='OK',paused=0,last_error=NULL WHERE id=? AND account_health IN ('FROZEN','RESTRICTED')",
                (account_id,),
            )
            self.audit(c, "ACCOUNT_RESTRICTION_RESOLVED_MANUALLY", account_id=account_id)
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE account_id=? AND kind='CHECK' AND status='FAILED'",
                (account_id,),
            )

    def connection_failed(self, account_id):
        with self.db.tx() as c:
            account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
            if not account:
                return
            failures = account["connection_failures"] + 1
            delay = min(self.config.retry_max, self.config.retry_base * 2 ** min(failures - 1, 10))
            c.execute(
                "UPDATE accounts SET connection_failures=?,connection_retry_at=? WHERE id=?",
                (failures, self.clock() + delay, account_id),
            )
            self.audit(c, "CONNECTION_RETRY", account_id=account_id, retry=failures, delay=delay)

    def connection_verified(self, account_id):
        with self.db.tx() as c:
            c.execute(
                "UPDATE accounts SET account_health='OK',connection_failures=0,connection_retry_at=0 WHERE id=? AND account_health IN ('OK','DISCONNECTED','SESSION_EXPIRED')",
                (account_id,),
            )
            self.audit(c, "ACCOUNT_CHECKED", account_id=account_id)
