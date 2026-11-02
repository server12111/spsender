from .core import row, rows
from .pairs import shift_role


class Onboarding:
    def _stage(self, c, account, stage):
        if account["onboarding_stage"] == stage:
            return
        c.execute(
            "UPDATE accounts SET onboarding_stage=?,stage_started_at=? WHERE id=?", (stage, self.clock(), account["id"])
        )
        c.execute(
            "INSERT OR IGNORE INTO account_onboarding_events(account_id,stage,created_at) VALUES (?,?,?)",
            (account["id"], stage, self.clock()),
        )
        self.audit(c, "ONBOARDING_STAGE", account_id=account["id"], stage=stage)

    def _assigned_chats(self, c, pair_id):
        return rows(
            c,
            "SELECT pc.*,cp.chat_id,cp.topic_id,cp.title,cp.link,cp.approved,cp.join_allowed,cp.greeting_allowed,cp.split_allowed "
            "FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id WHERE pc.pair_id=? AND pc.removed_at IS NULL ORDER BY pc.id",
            (pair_id,),
        )

    def _onboard(self, c, pair, campaign):
        now = self.clock()
        accounts = self._pair_accounts(c, pair)
        if all(a and a["onboarding_stage"] == "READY" for a in accounts):
            incomplete = row(
                c,
                "SELECT 1 FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id "
                "WHERE pc.pair_id=? AND pc.removed_at IS NULL AND pc.status='ASSIGNED' AND cp.approved=1 AND "
                "(NOT EXISTS(SELECT 1 FROM account_chat_access a WHERE a.chat_pool_id=cp.id AND a.account_id=? AND a.joined=1 AND a.can_write=1 AND a.manual_required=0) OR "
                "NOT EXISTS(SELECT 1 FROM account_chat_access a WHERE a.chat_pool_id=cp.id AND a.account_id=? AND a.joined=1 AND a.can_write=1 AND a.manual_required=0)) LIMIT 1",
                (pair["id"], pair["account_a_id"], pair["account_b_id"]),
            )
            greeting_needed = campaign["greeting_enabled"] and row(
                c,
                "SELECT 1 FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id WHERE pc.pair_id=? AND pc.removed_at IS NULL AND pc.status='ASSIGNED' AND cp.greeting_allowed=1 AND cp.approved=1 AND NOT EXISTS(SELECT 1 FROM greeting_progress g WHERE g.campaign_id=? AND g.chat_pool_id=cp.id AND g.emoji_done=1 AND g.phrase_done=1 AND g.main_done=1) LIMIT 1",
                (pair["id"], campaign["id"]),
            )
            if not incomplete and not greeting_needed:
                return
        chats = self._assigned_chats(c, pair["id"])
        access_map = {
            (a["account_id"], a["chat_pool_id"]): a
            for a in rows(
                c,
                "SELECT a.* FROM account_chat_access a JOIN pair_chats pc ON pc.chat_pool_id=a.chat_pool_id WHERE pc.pair_id=? AND pc.removed_at IS NULL AND a.account_id IN (?,?)",
                (pair["id"], pair["account_a_id"], pair["account_b_id"]),
            )
        }
        for account in accounts:
            if not account:
                continue
            age = now - account["onboarding_started_at"]
            if age < 86400:
                continue
            self._stage(c, account, "PREPARING" if not account["prepared_at"] else account["onboarding_stage"])
            if (
                campaign["status"] != "RUNNING"
                or account["paused"]
                or account["account_health"] != "OK"
                or account["flood_until"] > now
            ):
                continue
            if not account["prepared_at"]:
                self._enqueue(c, pair, campaign, "CHECK", None, account["id"], {}, f"prepare:{account['id']}")
                continue
            if age < 25 * 3600 or not pair["account_b_id"]:
                continue
            incomplete = []
            for chat in chats:
                access = access_map.get((account["id"], chat["chat_pool_id"]))
                if access is None:
                    c.execute(
                        "INSERT OR IGNORE INTO account_chat_access(account_id,chat_pool_id) VALUES (?,?)",
                        (account["id"], chat["chat_pool_id"]),
                    )
                    access = {"joined": 0, "can_write": 0, "manual_required": 0}
                    access_map[(account["id"], chat["chat_pool_id"])] = access
                if not access["joined"] or not access["can_write"]:
                    incomplete.append((chat, access))
            if incomplete:
                if account["onboarding_stage"] != "READY":
                    self._stage(c, account, "JOINING_APPROVED")
                if now < account["next_action_at"]:
                    continue
                # At most one pending joining task/account. Large N extends this stage.
                if row(
                    c,
                    "SELECT id FROM dispatch_jobs WHERE account_id=? AND kind='JOIN' AND status IN ('PENDING','INFLIGHT','UNKNOWN')",
                    (account["id"],),
                ):
                    continue
                for chat, access in incomplete:
                    if chat["status"] != "ASSIGNED" or not chat["approved"] or access["manual_required"]:
                        continue
                    if row(
                        c,
                        "SELECT 1 FROM chat_failures WHERE pair_id=? AND chat_pool_id=? AND account_id=?",
                        (pair["id"], chat["chat_pool_id"], account["id"]),
                    ):
                        continue
                    self._enqueue(
                        c,
                        pair,
                        campaign,
                        "JOIN",
                        chat["chat_pool_id"],
                        account["id"],
                        {"peer_id": chat["chat_id"], "link": chat["link"], "join_allowed": bool(chat["join_allowed"])},
                        f"join:{account['id']}:{chat['chat_pool_id']}",
                    )
                    break
                continue
            if not account["joined_at"]:
                c.execute("UPDATE accounts SET joined_at=? WHERE id=?", (now, account["id"]))
            if age >= 26 * 3600 and account["onboarding_stage"] != "READY":
                self._stage(c, account, "GREETING_READY")
        # Greetings are pair-wide and use the same persisted progress for both stages.
        accounts = self._pair_accounts(c, pair)
        if not all(
            a and a["prepared_at"] and a["joined_at"] and now - a["onboarding_started_at"] >= 26 * 3600
            for a in accounts
        ):
            return
        if campaign["status"] != "RUNNING" or not all(
            a["account_health"] == "OK" and not a["paused"] and a["flood_until"] <= now for a in accounts
        ):
            return
        role = shift_role(now, campaign["timezone"])
        account_id = pair["account_a_id"] if role == "A" else pair["account_b_id"]
        outstanding = False
        for chat in chats:
            if chat["status"] != "ASSIGNED" or not chat["approved"]:
                continue
            accesses = [
                access_map.get((aid, chat["chat_pool_id"])) for aid in (pair["account_a_id"], pair["account_b_id"])
            ]
            if not all(a and a["joined"] and a["can_write"] and not a["manual_required"] for a in accesses):
                outstanding = True
                continue
            if (
                campaign["greeting_enabled"]
                and chat["greeting_allowed"]
                and self.templates(campaign["id"], "greeting", campaign["version"], c)
            ):
                progress = self._greeting(c, pair, campaign, chat)
                if not all(progress[k + "_done"] for k in ("emoji", "phrase", "main")):
                    outstanding = True
                    self._plan_greeting(c, pair, campaign, chat, account_id)
        if not outstanding and all(now - a["onboarding_started_at"] >= 27 * 3600 for a in accounts):
            for account in accounts:
                self._stage(c, account, "READY")
                c.execute("UPDATE accounts SET ready_at=COALESCE(ready_at,?) WHERE id=?", (now, account["id"]))
