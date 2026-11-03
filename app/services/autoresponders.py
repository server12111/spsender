import random

from .core import DomainError, row


class Autoresponders:
    def clear_reply_history(self, campaign_id):
        with self.db.tx() as c:
            self.campaign(c, campaign_id)
            c.execute(
                "UPDATE dm_reply_sequences SET status='CANCELLED' WHERE campaign_id=? AND status IN ('ACTIVE','FAILED') "
                "AND (current_job_id IS NULL OR current_job_id IN (SELECT id FROM dispatch_jobs WHERE status IN ('FAILED','CANCELLED','CONFIRMED')))",
                (campaign_id,),
            )
            c.execute(
                "UPDATE reply_history SET last_replied_at=NULL WHERE campaign_id=? AND (reserved_job_id IS NULL OR reserved_job_id IN (SELECT id FROM dispatch_jobs WHERE status NOT IN ('PENDING','INFLIGHT','UNKNOWN')))",
                (campaign_id,),
            )
            self.audit(c, "REPLY_HISTORY_RESET", campaign_id=campaign_id)

    def incoming(
        self,
        account_id,
        peer_id,
        message_id,
        sender_id,
        private,
        *,
        bot=False,
        outgoing=False,
        reply_to_sender_id=None,
        text="",
        topic_id=0,
    ):
        """Preserved triggers: inbound PM, first PM, reply to this account's group message."""
        with self.db.tx() as c:
            own = {r[0] for r in c.execute("SELECT telegram_id FROM accounts WHERE telegram_id IS NOT NULL")}
            if bot or outgoing or sender_id in own:
                return []
            account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
            pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
            if not account or not pair:
                return []
            campaign = self.campaign(c, pair["campaign_id"])
            if campaign["status"] != "RUNNING" or not self._pair_ready(c, pair):
                return []
            from .pairs import shift_role

            on_shift = (
                pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
            )
            if account_id != on_shift:
                return []
            if (
                c.execute(
                    "INSERT OR IGNORE INTO incoming_events(account_id,peer_id,message_id,sender_id,private,created_at) VALUES (?,?,?,?,?,?)",
                    (account_id, peer_id, message_id, sender_id, private, self.clock()),
                ).rowcount
                == 0
            ):
                return []
            chat = None
            if not private:
                if reply_to_sender_id != account["telegram_id"]:
                    return []
                chat = row(
                    c,
                    "SELECT pc.*,cp.chat_id,cp.topic_id FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id "
                    "WHERE pc.pair_id=? AND cp.chat_id=? AND cp.topic_id=? AND pc.removed_at IS NULL AND pc.status='ASSIGNED' AND cp.approved=1",
                    (pair["id"], peer_id, topic_id),
                )
                if not chat:
                    return []
            if private and campaign["notify_dm"]:
                self._notify(
                    c,
                    f"incoming:{account_id}:{peer_id}:{message_id}",
                    "incoming",
                    {
                        "account_id": account_id,
                        "peer_id": peer_id,
                        "message_id": message_id,
                        "text": text[:300],
                        "name": account["name"],
                        "at": self.clock(),
                    },
                )
            if private and row(
                c,
                "SELECT 1 FROM dm_reply_sequences WHERE campaign_id=? AND peer_id=? AND status='ACTIVE'",
                (campaign["id"], peer_id),
            ):
                return []
            if private:
                failed = row(
                    c,
                    "SELECT * FROM dm_reply_sequences WHERE campaign_id=? AND pair_id=? AND account_id=? "
                    "AND peer_id=? AND status='FAILED' ORDER BY id DESC LIMIT 1",
                    (campaign["id"], pair["id"], account_id, peer_id),
                )
                if failed and self._reactivate_dm_sequence(c, failed):
                    c.execute("UPDATE dm_reply_sequences SET reply_to=? WHERE id=?", (message_id, failed["id"]))
                    failed = row(c, "SELECT * FROM dm_reply_sequences WHERE id=?", (failed["id"],))
                    job_id = self._queue_dm_step(c, failed, pair, campaign)
                    return [job_id] if job_id is not None else []
            kinds = ("dm_first", "dm_always") if private else ("group",)
            jobs = []
            dm_steps = []
            for kind in kinds:
                enabled_key = {
                    "group": "group_reply_enabled",
                    "dm_first": "dm_first_enabled",
                    "dm_always": "dm_always_enabled",
                }[kind]
                if not campaign[enabled_key]:
                    continue
                templates = self.templates(campaign["id"], kind, campaign["version"], c)
                if not templates:
                    continue
                history = row(
                    c,
                    "SELECT * FROM reply_history WHERE campaign_id=? AND peer_id=? AND kind=?",
                    (campaign["id"], peer_id, kind),
                )
                if history and history["last_replied_at"] is not None:
                    if kind == "dm_first" or self.clock() - history["last_replied_at"] < campaign["reply_cooldown"]:
                        continue
                if history and history["reserved_job_id"]:
                    job = row(c, "SELECT * FROM dispatch_jobs WHERE id=?", (history["reserved_job_id"],))
                    if job and job["status"] in {"PENDING", "INFLIGHT", "UNKNOWN"}:
                        continue
                template = random.choice(templates)
                job_kind = {"group": "GROUP_REPLY", "dm_first": "DM_FIRST", "dm_always": "DM_ALWAYS"}[kind]
                payload = {
                    "text": template["text"],
                    "media_path": template["media_path"],
                    "media_kind": template["media_kind"],
                    "peer_id": peer_id,
                    "reply_to": message_id,
                    "history_kind": kind,
                    "topic_id": topic_id,
                }
                if private:
                    if kind == "dm_first":
                        dm_steps.extend(
                            [
                                {
                                    "role": "sticker",
                                    "history_kind": kind,
                                    "history_complete": False,
                                    "body": {
                                        "sticker_emoji": campaign["dm_sticker_emoji"],
                                        "text": "",
                                        "media_path": None,
                                    },
                                },
                                {
                                    "role": "phrase",
                                    "history_kind": kind,
                                    "history_complete": False,
                                    "body": {"text": campaign["dm_greeting_phrase"], "media_path": None},
                                },
                            ]
                        )
                    dm_steps.append(
                        {
                            "role": "text",
                            "history_kind": kind,
                            "history_complete": True,
                            "template_position": template["position"],
                            "body": {key: template[key] for key in ("text", "media_path", "media_kind")},
                        }
                    )
                    continue
                job_id = self._enqueue(
                    c,
                    pair,
                    campaign,
                    job_kind,
                    chat["chat_pool_id"] if chat else None,
                    account_id,
                    payload,
                    f"reply:{campaign['id']}:{account_id}:{peer_id}:{message_id}:{kind}",
                )
                if job_id:
                    c.execute(
                        "INSERT INTO reply_history(campaign_id,peer_id,kind,reserved_job_id) VALUES (?,?,?,?) "
                        "ON CONFLICT(campaign_id,peer_id,kind) DO UPDATE SET reserved_job_id=excluded.reserved_job_id",
                        (campaign["id"], peer_id, kind, job_id),
                    )
                    jobs.append(job_id)
            if dm_steps:
                job_id = self._create_dm_sequence(c, pair, campaign, account_id, peer_id, message_id, dm_steps)
                if job_id is not None:
                    jobs.append(job_id)
            return jobs

    def manual_dm_reply(self, account_id, peer_id, message_id, template):
        with self.db.tx() as c:
            incoming = row(
                c,
                "SELECT * FROM incoming_events WHERE account_id=? AND peer_id=? AND message_id=? AND private=1",
                (account_id, peer_id, message_id),
            )
            if not incoming or self.clock() - incoming["created_at"] > 600:
                raise DomainError("Окно ответа истекло (10 минут). Ответьте из самого аккаунта")
            pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
            if not pair:
                raise DomainError("Пара не найдена")
            campaign = self.campaign(c, pair["campaign_id"])
            if campaign["status"] != "RUNNING" or not self._pair_ready(c, pair):
                raise DomainError("Рассылка остановлена или пара не готова")
            from .pairs import shift_role

            active = (
                pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
            )
            if account_id != active:
                raise DomainError("Этот аккаунт вне своей смены. Ответьте из самого аккаунта")
            return self._enqueue(
                c,
                pair,
                campaign,
                "DM_MANUAL",
                None,
                account_id,
                {**template, "peer_id": peer_id, "reply_to": message_id, "incoming_at": incoming["created_at"]},
                f"manual-reply:{account_id}:{peer_id}:{message_id}:{campaign['version']}",
            )
