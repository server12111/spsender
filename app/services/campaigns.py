from .core import DomainError, row, rows


class Campaigns:
    def create_campaign(
        self, name, texts, interval=300, chat_limit=20, *, account_ids=(), creation_key=None, **options
    ):
        if not name.strip() or not texts or interval < 1 or chat_limit < 1:
            raise DomainError("Нужны название, текст, положительные интервал и N")
        with self.db.tx() as c:
            if creation_key:
                receipt = row(
                    c, "SELECT campaign_id FROM campaign_creation_receipts WHERE creation_key=?", (creation_key,)
                )
                if receipt:
                    return receipt["campaign_id"]
            account_ids = list(dict.fromkeys(account_ids))
            for account_id in account_ids:
                if not row(c, "SELECT id FROM accounts WHERE id=? AND account_health<>'REMOVED'", (account_id,)):
                    raise DomainError("Выбранный аккаунт удалён. Откройте выбор аккаунтов заново")
                if row(c, "SELECT 1 FROM campaign_members WHERE account_id=?", (account_id,)):
                    raise DomainError("Выбранный аккаунт уже назначен другой рассылке")
            campaign_id = c.execute(
                "INSERT INTO campaigns(name,timezone,interval_seconds,chat_limit,created_at) VALUES (?,?,?,?,?)",
                (
                    name.strip()[:100],
                    self.get_setting("timezone", self.config.timezone),
                    interval,
                    chat_limit,
                    self.clock(),
                ),
            ).lastrowid
            self._write_templates(c, campaign_id, 1, "mailing", texts)
            self._campaign_options(c, campaign_id, options)
            for account_id in account_ids:
                self._assign_account(c, account_id, campaign_id)
            if creation_key:
                c.execute("INSERT INTO campaign_creation_receipts VALUES (?,?)", (creation_key, campaign_id))
            self.audit(c, "CAMPAIGN_CREATED", campaign_id=campaign_id)
            return campaign_id

    def _write_templates(self, c, campaign_id, version, kind, texts):
        if kind not in {"mailing", "greeting", "group", "dm_first", "dm_always"}:
            raise DomainError("Неизвестный тип шаблона")
        for i, value in enumerate(texts):
            value = {"text": value} if isinstance(value, str) else value
            text, path = value.get("text", ""), value.get("media_path")
            if not text.strip() and not path:
                raise DomainError("Пустой шаблон")
            maximum = (1024 if path else 4096) - (64 if kind == "greeting" else 0)
            if len(text.encode("utf-16-le")) // 2 > maximum:
                raise DomainError("Текст превышает ограничение Telegram: 1024 с медиа, 4096 без медиа")
            c.execute(
                "INSERT INTO campaign_templates(campaign_id,version,kind,position,text,media_path,media_kind) "
                "VALUES (?,?,?,?,?,?,?)",
                (campaign_id, version, kind, i, text, path, value.get("media_kind")),
            )

    def _campaign_options(self, c, campaign_id, options):
        allowed = {
            "greeting_enabled",
            "greeting_split",
            "group_reply_enabled",
            "dm_first_enabled",
            "dm_always_enabled",
            "notify_dm",
            "reply_cooldown",
            "selection",
            "interval_seconds",
            "chat_limit",
            "name",
            "dm_greeting_phrase",
            "dm_sticker_emoji",
            "auto_subscribe",
        }
        for key, value in options.items():
            if key not in allowed:
                raise DomainError("Неизвестная настройка кампании")
            if key == "name":
                if not str(value).strip():
                    raise DomainError("Название рассылки не может быть пустым")
                value = str(value).strip()[:100]
            if key in {"dm_greeting_phrase", "dm_sticker_emoji"}:
                value = str(value).strip()
                maximum = 200 if key == "dm_greeting_phrase" else 16
                if not value or len(value.encode("utf-16-le")) // 2 > maximum:
                    raise DomainError(f"Введите непустое значение не длиннее {maximum} символов")
            if key == "selection" and value not in {"sequential", "random"}:
                raise DomainError("Выберите последовательный или случайный порядок")
            if key in {"reply_cooldown", "interval_seconds", "chat_limit"} and int(value) < 1:
                raise DomainError("Значение должно быть положительным")
            c.execute(f"UPDATE campaigns SET {key}=? WHERE id=?", (value, campaign_id))

    def edit_campaign(self, campaign_id, template_kind=None, texts=None, expected_version=None, **options):
        with self.db.tx() as c:
            old = self.campaign(c, campaign_id)
            if expected_version is not None and old["version"] != expected_version:
                raise DomainError("Настройки уже изменены. Откройте карточку заново")
            version = old["version"] + 1
            c.execute("UPDATE campaigns SET version=? WHERE id=?", (version, campaign_id))
            self._campaign_options(c, campaign_id, options)
            c.execute(
                "INSERT INTO campaign_templates(campaign_id,version,kind,position,text,media_path,media_kind) "
                "SELECT campaign_id,?,kind,position,text,media_path,media_kind FROM campaign_templates "
                "WHERE campaign_id=? AND version=? AND kind<>?",
                (version, campaign_id, old["version"], template_kind or ""),
            )
            if template_kind:
                self._write_templates(c, campaign_id, version, template_kind, texts or [])
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE campaign_id=? AND status='PENDING'", (campaign_id,)
            )
            self._cancel_group_replies(c, campaign_id=campaign_id)
            if "chat_limit" in options:
                for pair in rows(c, "SELECT * FROM pairs WHERE campaign_id=?", (campaign_id,)):
                    limit = int(options["chat_limit"])
                    c.execute("UPDATE pairs SET chat_limit=? WHERE id=?", (limit, pair["id"]))
                    assigned = rows(
                        c, "SELECT * FROM pair_chats WHERE pair_id=? AND removed_at IS NULL ORDER BY id", (pair["id"],)
                    )
                    for assignment in assigned[limit:]:
                        c.execute(
                            "UPDATE pair_chats SET removed_at=?,status='REMOVED' WHERE id=?",
                            (self.clock(), assignment["id"]),
                        )
                        c.execute("UPDATE chat_pool SET status='FREE' WHERE id=?", (assignment["chat_pool_id"],))
                self._fill_waiting(c)
            self.audit(c, "CAMPAIGN_EDITED", campaign_id=campaign_id, version=version)

    def campaign_status(self, campaign_id, status, confirmed=False, expected_status=None, expected_version=None):
        if status not in {"RUNNING", "PAUSED", "STOPPED", "REMOVED"}:
            raise DomainError("Неизвестный статус")
        if status in {"RUNNING", "REMOVED"} and not confirmed:
            raise DomainError("Требуется подтверждение")
        with self.db.tx() as c:
            current = self.campaign(c, campaign_id)
            if (expected_status is not None and current["status"] != expected_status) or (
                expected_version is not None and current["version"] != expected_version
            ):
                raise DomainError("Кнопка состояния устарела. Откройте карточку заново")
            if current["status"] == status:
                return
            for pair in rows(c, "SELECT * FROM pairs WHERE campaign_id=?", (campaign_id,)):
                self._pause_clock(c, pair)
            c.execute("UPDATE campaigns SET status=? WHERE id=?", (status, campaign_id))
            if status == "RUNNING":
                # Explicit restart may retry known failures, never UNKNOWN or
                # confirmed work and never obsolete inbound replies.
                c.execute(
                    "UPDATE dispatch_jobs SET status='CANCELLED' WHERE campaign_id=? AND status='FAILED' AND kind IN ('CHECK','JOIN','GREETING')",
                    (campaign_id,),
                )
                self._resume_failed_dm_sequences(c, campaign_id)
            if status != "RUNNING":
                c.execute(
                    "UPDATE dispatch_jobs SET status='CANCELLED' WHERE campaign_id=? AND status='PENDING'",
                    (campaign_id,),
                )
                self._cancel_group_replies(c, campaign_id=campaign_id)
            if status == "REMOVED":
                for exchange in rows(
                    c,
                    "SELECT x.id FROM rotation_exchanges x JOIN rotation_cycles r ON r.id=x.cycle_id "
                    "WHERE r.campaign_id=? AND x.status IN ('PREPARING','READY','DRAINING')",
                    (campaign_id,),
                ):
                    self._end_exchange(c, exchange["id"], "CANCELLED", "CAMPAIGN_REMOVED")
                c.execute(
                    "UPDATE rotation_cycles SET status='CANCELLED',completed_at=? WHERE campaign_id=? AND status='PREPARING'",
                    (self.clock(), campaign_id),
                )
                c.execute(
                    "UPDATE dm_reply_sequences SET status='CANCELLED' WHERE campaign_id=? AND status IN ('ACTIVE','FAILED')",
                    (campaign_id,),
                )
                for assignment in rows(
                    c,
                    "SELECT pc.* FROM pair_chats pc JOIN pairs p ON p.id=pc.pair_id "
                    "WHERE p.campaign_id=? AND pc.removed_at IS NULL",
                    (campaign_id,),
                ):
                    c.execute(
                        "UPDATE pair_chats SET removed_at=?,status='REMOVED' WHERE id=?",
                        (self.clock(), assignment["id"]),
                    )
                    c.execute("UPDATE chat_pool SET status='FREE' WHERE id=?", (assignment["chat_pool_id"],))
                c.execute(
                    "UPDATE accounts SET membership_status='FREE' WHERE id IN "
                    "(SELECT account_id FROM campaign_members WHERE campaign_id=?)",
                    (campaign_id,),
                )
                c.execute("DELETE FROM campaign_members WHERE campaign_id=?", (campaign_id,))
                c.execute(
                    "UPDATE chat_issues SET status='CLOSED' WHERE pair_id IN (SELECT id FROM pairs WHERE campaign_id=?)",
                    (campaign_id,),
                )
            self.audit(c, "CAMPAIGN_" + status, campaign_id=campaign_id)
