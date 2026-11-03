"""Original mailing records backed by the existing campaign/pair assignments."""

import json
import random
from functools import wraps
from pathlib import Path

from app.services.pairs import active_count, shift_role

from .database import LegacyDatabase
from .models import Mailing, MailingMessage, MailingTarget


def is_safe_media_path(path):
    return bool(path) and Path(path).resolve().is_relative_to(Path("data/media").resolve())


def offload(function):
    @wraps(function)
    async def call(self, *args, **kwargs):
        result = await self.engine.db.run(function, self, *args, **kwargs)
        if function.__name__ == "get_mailing_targets" and hasattr(self, "target_observer"):
            self.target_observer(result)
        return result

    return call


class LegacyMailingDatabase(LegacyDatabase):
    @offload
    def get_mailing(self, mailing_id):
        with self.engine.db.read() as connection:
            pair = connection.execute("SELECT * FROM pairs WHERE id=?", (mailing_id,)).fetchone()
            if not pair:
                return None
            pair = dict(pair)
            campaign = connection.execute("SELECT * FROM campaigns WHERE id=?", (pair["campaign_id"],)).fetchone()
            if not campaign:
                return None
            campaign = dict(campaign)
            active = self.engine._pair_ready(connection, pair) and campaign["status"] == "RUNNING"
        account = (
            pair["account_a_id"]
            if shift_role(self.engine.clock(), campaign["timezone"]) == "A"
            else pair["account_b_id"]
        )
        options = json.loads(campaign["legacy_options"])
        result = Mailing(
            id=pair["id"],
            user_id=self.engine.config.admin_ids[0],
            account_id=account or 0,
            name=campaign["name"],
            is_active=active,
            interval_seconds=campaign["interval_seconds"],
            active_hours_json=None,
            last_sent_at=None,
            created_at=self.local(campaign["created_at"]),
            chat_delay_seconds=campaign["chat_delay_seconds"],
            skip_unseen=bool(campaign["skip_unseen"]),
            **{
                key: value
                for key, value in options.items()
                if key in Mailing.__dataclass_fields__
                and key
                not in {
                    "id",
                    "user_id",
                    "account_id",
                    "name",
                    "is_active",
                    "interval_seconds",
                    "active_hours_json",
                    "last_sent_at",
                    "created_at",
                    "chat_delay_seconds",
                    "skip_unseen",
                }
            },
        )
        result.version = campaign["version"]
        return result

    async def get_active_mailings(self):
        pairs = await self.engine.db.run(
            self.engine.db.all,
            "SELECT p.id FROM pairs p JOIN campaigns c ON c.id=p.campaign_id WHERE c.status='RUNNING' ORDER BY p.id",
        )
        result = []
        for pair in pairs:
            item = await self.get_mailing(pair["id"])
            if item.is_active:
                result.append(item)
        return result

    async def get_user_active_mailings(self, user_id):
        return await self.get_active_mailings() if user_id in self.engine.config.admin_ids else []

    async def get_active_mailings_by_account(self, account_id):
        return [item for item in await self.get_active_mailings() if item.account_id == account_id]

    @offload
    def get_mailing_targets(self, mailing_id):
        with self.engine.db.read() as connection:
            pair = self.engine._pair(connection, mailing_id)
            campaign = self.engine.campaign(connection, pair["campaign_id"])
            account_id = (
                pair["account_a_id"]
                if shift_role(self.engine.clock(), campaign["timezone"]) == "A"
                else pair["account_b_id"]
            )
            failures = {
                item[0]
                for item in connection.execute(
                    "SELECT chat_pool_id FROM chat_failures WHERE pair_id=? AND account_id=?", (mailing_id, account_id)
                ).fetchall()
            }
            chats = self.engine._assigned_chats(connection, mailing_id)
            total = active_count(len(chats), self.engine.elapsed(pair))
            result = []
            for chat in chats[:total]:
                if chat["status"] != "ASSIGNED" or not chat["approved"]:
                    continue
                if chat["chat_pool_id"] in failures:
                    continue
                if self.engine._greeting_pending(connection, campaign, chat):
                    continue
                access = connection.execute(
                    "SELECT count(*) FROM account_chat_access WHERE account_id IN (?,?) AND chat_pool_id=? AND joined=1 AND can_write=1 AND manual_required=0",
                    (pair["account_a_id"], pair["account_b_id"], chat["chat_pool_id"]),
                ).fetchone()[0]
                if access != 2:
                    continue
                options = json.loads(chat["legacy_options"])
                result.append(
                    MailingTarget(
                        id=chat["id"],
                        mailing_id=mailing_id,
                        chat_identifier=chat["link"],
                        interval_seconds=chat["interval_seconds"],
                        last_sent_at=self.local(chat["last_sent_at"]),
                        thread_id=chat["topic_id"] or None,
                        is_forum=bool(chat["topic_id"]),
                        msg_index=chat["message_cursor"],
                        delivery_seq=chat["delivery_seq"],
                        **{
                            key: value
                            for key, value in options.items()
                            if key in {"last_account_id", "media_disabled", "topic_ids_json"}
                        },
                    )
                )
            return result

    async def get_mailing_messages(self, mailing_id):
        pair = await self.engine.db.run(self.engine.db.one, "SELECT campaign_id FROM pairs WHERE id=?", (mailing_id,))
        templates = await self.engine.db.run(self.engine.templates, pair["campaign_id"])
        selection = await self.engine.db.run(
            self.engine.db.scalar, "SELECT selection FROM campaigns WHERE id=?", (pair["campaign_id"],)
        )
        if selection == "random":
            random.shuffle(templates)
        result = []
        for template in templates:
            options = json.loads(template["legacy_options"])
            path = template["media_path"]
            kwargs = (
                {"photo_path": path}
                if template["media_kind"] == "photo"
                else ({"video_path": path} if template["media_kind"] == "video" else {"document_path": path})
            )
            options = {
                key: value
                for key, value in options.items()
                if key in MailingMessage.__dataclass_fields__ and key not in {"id", "mailing_id", "text"}
            }
            message = MailingMessage(
                id=template["id"], mailing_id=mailing_id, text=template["text"], **{**kwargs, **options}
            )
            message.version = template["version"]
            message.position = template["position"]
            result.append(message)
        return result

    async def get_last_send_per_account(self):
        records = await self.engine.db.run(
            self.engine.db.all,
            "SELECT j.account_id,max(r.confirmed_at) AS sent_at FROM message_results r JOIN dispatch_jobs j ON j.id=r.job_id GROUP BY j.account_id",
        )
        return {item["account_id"]: self.local(item["sent_at"]) for item in records}

    async def flush_target_last_sent(self, batch):
        # The RPC acknowledgement atomically commits these fields. An older
        # upstream flush must never roll back that cursor or attribute a later
        # delivery to the previous account.
        return None

    def note_mailing_sent(self, mailing_id, target_id):
        # Confirmations and counters are committed by the guarded RPC journal.
        return None

    async def flush_mailing_stats(self):
        return None

    async def prune_mailing_stats(self):
        # The owner explicitly retains history; no automatic history deletion.
        return None

    async def update_mailing_last_sent(self, mailing_id):
        return None  # Derived from confirmed results rather than a second counter.

    async def update_mailing_status(self, mailing_id, enabled):
        pair = await self.engine.db.run(self.engine.db.one, "SELECT campaign_id FROM pairs WHERE id=?", (mailing_id,))
        if pair:
            await self.engine.db.run(
                self.engine.campaign_status, pair["campaign_id"], "RUNNING" if enabled else "PAUSED", confirmed=True
            )

    async def _end_stuck_transaction(self):
        return None  # The bridge never keeps a transaction open across awaits.

    async def update_target_is_forum(self, target_id, is_forum):
        return None  # Forum identity comes from the approved chat/topic import.

    async def update_target_media_disabled(self, target_id, disabled):
        def write():
            with self.engine.db.tx() as connection:
                connection.execute(
                    "UPDATE pair_chats SET legacy_options=json_set(legacy_options,'$.media_disabled',?) WHERE id=?",
                    (bool(disabled), target_id),
                )

        await self.engine.db.run(write)

    async def add_error_log(self, *args, **details):
        account_id = details.get("account_id")
        await self.engine.db.run(
            self.engine.diagnostic, "legacy-mailing", str(details.get("error_type", "RPC_ERROR")), account_id=account_id
        )

    async def deactivate_account(self, account_id):
        def write():
            with self.engine.db.tx() as connection:
                account = connection.execute("SELECT account_health FROM accounts WHERE id=?", (account_id,)).fetchone()
                if not account or account[0] in {"FROZEN", "RESTRICTED", "REMOVED"}:
                    return False
                self.engine._critical(connection, account_id, "SESSION_EXPIRED", "SESSION_EXPIRED")
                return True

        return await self.engine.db.run(write)

    async def delete_mailing_target(self, target_id):
        def write():
            with self.engine.db.tx() as connection:
                target = connection.execute(
                    "SELECT pc.*,p.account_a_id,p.account_b_id,c.timezone FROM pair_chats pc JOIN pairs p ON p.id=pc.pair_id "
                    "JOIN campaigns c ON c.id=p.campaign_id WHERE pc.id=? AND pc.removed_at IS NULL",
                    (target_id,),
                ).fetchone()
                if not target:
                    return
                account_id = (
                    target["account_a_id"]
                    if shift_role(self.engine.clock(), target["timezone"]) == "A"
                    else target["account_b_id"]
                )
                account_id = account_id or target["account_a_id"] or target["account_b_id"]
                if account_id:
                    self.engine._chat_failure(
                        connection, target["pair_id"], target["chat_pool_id"], account_id, "CHAT_UNAVAILABLE", True
                    )

        await self.engine.db.run(write)
