"""Weekly, prepared, pairwise exchanges of chat assignments within a campaign."""

import random

from .core import DomainError, row, rows
from .pairs import shift_role

ROTATION_KINDS = {"ROTATION_JOIN", "ROTATION_CHECK", "ROTATION_MESSAGE"}
TERMINAL_EXCHANGES = {"APPLIED", "SKIPPED", "CANCELLED"}


class Rotation:
    def set_rotation(self, campaign_id, enabled, lead_days=3, messages=2, expected_enabled=None):
        if not 1 <= int(lead_days) <= 6 or not 0 <= int(messages) <= 2:
            raise DomainError("Подготовка: 1–6 дней; сообщений на аккаунт в чат: 0–2")
        with self.db.tx() as c:
            campaign = self.campaign(c, campaign_id)
            if expected_enabled is not None and bool(campaign["rotation_enabled"]) != bool(expected_enabled):
                raise DomainError("Настройка обмена уже изменилась. Откройте карточку заново")
            if not enabled:
                for exchange in rows(
                    c,
                    "SELECT x.id FROM rotation_exchanges x JOIN rotation_cycles r ON r.id=x.cycle_id "
                    "WHERE r.campaign_id=? AND x.status IN ('PREPARING','READY','DRAINING')",
                    (campaign_id,),
                ):
                    self._end_exchange(c, exchange["id"], "CANCELLED", "ROTATION_DISABLED")
                c.execute(
                    "UPDATE rotation_cycles SET status='CANCELLED',completed_at=? WHERE campaign_id=? "
                    "AND status='PREPARING'",
                    (self.clock(), campaign_id),
                )
            next_at = campaign["rotation_next_at"]
            if enabled and (not campaign["rotation_enabled"] or not next_at):
                next_at = self.clock() + 7 * 86400
            if int(messages) != campaign["rotation_messages"]:
                c.execute(
                    "UPDATE dispatch_jobs SET status='CANCELLED' WHERE campaign_id=? AND status='PENDING' "
                    "AND kind IN ('ROTATION_JOIN','ROTATION_CHECK','ROTATION_MESSAGE')",
                    (campaign_id,),
                )
            c.execute(
                "UPDATE campaigns SET rotation_enabled=?,rotation_next_at=?,rotation_lead_days=?,rotation_messages=? "
                "WHERE id=?",
                (bool(enabled), next_at, int(lead_days), int(messages), campaign_id),
            )
            self.audit(c, "ROTATION_CONFIGURED", campaign_id=campaign_id, enabled=bool(enabled))

    def rotation_card(self, campaign_id, page=0):
        with self.db.read() as c:
            campaign = self.campaign(c, campaign_id)
            exchanges = rows(
                c,
                "SELECT x.*,r.due_at FROM rotation_exchanges x JOIN rotation_cycles r ON r.id=x.cycle_id "
                "WHERE r.campaign_id=? ORDER BY r.id DESC,x.id LIMIT 7 OFFSET ?",
                (campaign_id, max(0, page) * 6),
            )
            return {"campaign": campaign, "exchanges": exchanges}

    def _rotation_pair_blocked(self, c, pair_id):
        return bool(
            row(
                c,
                "SELECT 1 FROM rotation_exchanges WHERE status='DRAINING' "
                "AND (left_pair_id=? OR right_pair_id=?) LIMIT 1",
                (pair_id, pair_id),
            )
        )

    def _rotation_accounts_ready(self, c, pair):
        return not row(
            c, "SELECT 1 FROM dispatch_jobs WHERE pair_id=? AND status='UNKNOWN' LIMIT 1", (pair["id"],)
        ) and all(
            account
            and account["account_health"] == "OK"
            and not account["paused"]
            and account["onboarding_stage"] == "READY"
            and account["flood_until"] <= self.clock()
            for account in self._pair_accounts(c, pair)
        )

    def _exchange_valid(self, c, exchange):
        left = self._pair(c, exchange["left_pair_id"])
        right = self._pair(c, exchange["right_pair_id"])
        cycle = row(c, "SELECT * FROM rotation_cycles WHERE id=?", (exchange["cycle_id"],))
        if (left["account_a_id"], left["account_b_id"], right["account_a_id"], right["account_b_id"]) != (
            exchange["left_a"],
            exchange["left_b"],
            exchange["right_a"],
            exchange["right_b"],
        ):
            return False
        if left["campaign_id"] != cycle["campaign_id"] or right["campaign_id"] != cycle["campaign_id"]:
            return False
        for pair in (left, right):
            expected = {
                r["assignment_id"]
                for r in rows(
                    c,
                    "SELECT assignment_id FROM rotation_targets WHERE exchange_id=? AND source_pair_id=?",
                    (exchange["id"], pair["id"]),
                )
            }
            actual = rows(
                c,
                "SELECT pc.id,pc.status,cp.approved FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id "
                "WHERE pc.pair_id=? AND pc.removed_at IS NULL",
                (pair["id"],),
            )
            if (
                len(actual) != pair["chat_limit"]
                or {r["id"] for r in actual} != expected
                or not all(r["approved"] and r["status"] == "ASSIGNED" for r in actual)
            ):
                return False
        return True

    def _end_exchange(self, c, exchange_id, status, reason):
        c.execute(
            "UPDATE rotation_exchanges SET status=?,last_error=? WHERE id=? AND status<>'APPLIED'",
            (status, reason, exchange_id),
        )
        c.execute(
            "UPDATE dispatch_jobs SET status='CANCELLED' WHERE status='PENDING' AND "
            "json_extract(payload,'$.rotation_progress_id') IN (SELECT p.id FROM rotation_progress p "
            "JOIN rotation_targets t ON t.id=p.target_id WHERE t.exchange_id=?)",
            (exchange_id,),
        )
        c.execute(
            "UPDATE membership_requests SET status='CANCELLED' WHERE context_key=? AND status='WAITING'",
            (f"rotation:{exchange_id}",),
        )
        c.execute(
            "UPDATE dispatch_jobs SET status='CANCELLED' WHERE status='PENDING' AND "
            "json_extract(payload,'$.membership_request_id') IN (SELECT id FROM membership_requests "
            "WHERE context_key=? AND status='CANCELLED')",
            (f"rotation:{exchange_id}",),
        )

    def _exchange_ready(self, c, exchange_id, message_count):
        due = row(
            c,
            "SELECT r.due_at FROM rotation_exchanges x JOIN rotation_cycles r ON r.id=x.cycle_id WHERE x.id=?",
            (exchange_id,),
        )["due_at"]
        threshold = max(due - 86400, self.clock() - 86400) if self.clock() >= due - 86400 else 0
        pending = row(
            c,
            "SELECT 1 FROM rotation_progress p JOIN rotation_targets t ON t.id=p.target_id "
            "WHERE t.exchange_id=? AND (p.joined=0 OR p.can_write=0 OR p.message_index<? OR p.last_error IS NOT NULL "
            "OR p.verified_at<?) LIMIT 1",
            (exchange_id, message_count, threshold),
        )
        manual = row(
            c,
            "SELECT 1 FROM rotation_progress p JOIN rotation_targets t ON t.id=p.target_id "
            "LEFT JOIN account_chat_access a ON a.account_id=p.account_id AND a.chat_pool_id=t.chat_pool_id "
            "WHERE t.exchange_id=? AND (a.account_id IS NULL OR a.joined=0 OR a.can_write=0 OR a.manual_required=1) LIMIT 1",
            (exchange_id,),
        )
        memberships = row(
            c,
            "SELECT 1 FROM membership_requests WHERE context_key=? AND status NOT IN ('CONFIRMED','CANCELLED') LIMIT 1",
            (f"rotation:{exchange_id}",),
        )
        return not pending and not manual and not memberships

    def _create_rotation(self, c, campaign):
        cycle_id = c.execute(
            "INSERT INTO rotation_cycles(campaign_id,due_at,created_at) VALUES (?,?,?)",
            (campaign["id"], campaign["rotation_next_at"], self.clock()),
        ).lastrowid
        self.audit(c, "ROTATION_PREPARING", campaign_id=campaign["id"], cycle_id=cycle_id)

    def _extend_rotation(self, c, campaign, cycle):
        # Commit at most sixteen pairs per planner transaction; large pools do not
        # hold the SQLite writer lock for the entire weekly snapshot.
        cycle_id = cycle["id"]
        selected = rows(
            c,
            "SELECT * FROM pairs WHERE campaign_id=? AND account_a_id IS NOT NULL "
            "AND account_b_id IS NOT NULL AND id>? ORDER BY id LIMIT 16",
            (campaign["id"], cycle["plan_cursor"]),
        )
        candidates = list(selected)
        if cycle["pending_pair_id"]:
            pending_pair = row(
                c, "SELECT * FROM pairs WHERE id=? AND campaign_id=?", (cycle["pending_pair_id"], campaign["id"])
            )
            if pending_pair:
                candidates.insert(0, pending_pair)
        eligible = []
        for pair in candidates:
            if not self._rotation_accounts_ready(c, pair):
                continue
            chats = self._assigned_chats(c, pair["id"])
            if len(chats) == pair["chat_limit"] and all(
                chat["approved"] and chat["status"] == "ASSIGNED" for chat in chats
            ):
                eligible.append((pair, chats))
        # Pairwise exchange: an odd remaining pair keeps its complete current set.
        for index in range(0, len(eligible) - 1, 2):
            (left, left_chats), (right, right_chats) = eligible[index : index + 2]
            if len(left_chats) != len(right_chats):
                continue
            exchange_id = c.execute(
                "INSERT INTO rotation_exchanges(cycle_id,left_pair_id,right_pair_id,left_a,left_b,right_a,right_b) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    cycle_id,
                    left["id"],
                    right["id"],
                    left["account_a_id"],
                    left["account_b_id"],
                    right["account_a_id"],
                    right["account_b_id"],
                ),
            ).lastrowid
            for target_pair, source_pair, chats in ((left, right, right_chats), (right, left, left_chats)):
                for chat in chats:
                    target_id = c.execute(
                        "INSERT INTO rotation_targets(exchange_id,pair_id,source_pair_id,assignment_id,chat_pool_id) "
                        "VALUES (?,?,?,?,?)",
                        (exchange_id, target_pair["id"], source_pair["id"], chat["id"], chat["chat_pool_id"]),
                    ).lastrowid
                    for account_id in (target_pair["account_a_id"], target_pair["account_b_id"]):
                        c.execute(
                            "INSERT INTO rotation_progress(target_id,account_id) VALUES (?,?)", (target_id, account_id)
                        )
                        c.execute(
                            "INSERT OR IGNORE INTO account_chat_access(account_id,chat_pool_id) VALUES (?,?)",
                            (account_id, chat["chat_pool_id"]),
                        )
        cursor = selected[-1]["id"] if selected else cycle["plan_cursor"]
        more = bool(
            row(
                c,
                "SELECT 1 FROM pairs WHERE campaign_id=? AND account_a_id IS NOT NULL "
                "AND account_b_id IS NOT NULL AND id>? LIMIT 1",
                (campaign["id"], cursor),
            )
        )
        pending_id = eligible[-1][0]["id"] if more and len(eligible) % 2 else None
        c.execute(
            "UPDATE rotation_cycles SET plan_cursor=?,pending_pair_id=?,planning_done=? WHERE id=?",
            (cursor, pending_id, not more, cycle_id),
        )

    def _plan_rotations(self, c, campaign):
        if not campaign["rotation_enabled"] or campaign["status"] != "RUNNING":
            return
        cycle = row(
            c,
            "SELECT * FROM rotation_cycles WHERE campaign_id=? AND status='PREPARING' ORDER BY id LIMIT 1",
            (campaign["id"],),
        )
        if not cycle:
            if (
                not campaign["rotation_next_at"]
                or self.clock() < campaign["rotation_next_at"] - campaign["rotation_lead_days"] * 86400
            ):
                return
            self._create_rotation(c, campaign)
            cycle = row(
                c,
                "SELECT * FROM rotation_cycles WHERE campaign_id=? AND status='PREPARING' ORDER BY id LIMIT 1",
                (campaign["id"],),
            )
        if not cycle["planning_done"]:
            self._extend_rotation(c, campaign, cycle)
            cycle = row(c, "SELECT * FROM rotation_cycles WHERE id=?", (cycle["id"],))
        for exchange in rows(
            c,
            "SELECT * FROM rotation_exchanges WHERE cycle_id=? AND status IN ('PREPARING','READY','DRAINING')",
            (cycle["id"],),
        ):
            if not self._exchange_valid(c, exchange):
                self._end_exchange(c, exchange["id"], "CANCELLED", "ASSIGNMENTS_CHANGED")
                continue
            ready = self._exchange_ready(c, exchange["id"], campaign["rotation_messages"])
            if self.clock() < cycle["due_at"]:
                c.execute(
                    "UPDATE rotation_exchanges SET status=? WHERE id=?",
                    ("READY" if ready else "PREPARING", exchange["id"]),
                )
                continue
            physical = all(
                self._rotation_accounts_ready(c, self._pair(c, pair_id))
                for pair_id in (exchange["left_pair_id"], exchange["right_pair_id"])
            )
            if not ready or not physical or (exchange["draining_at"] and self.clock() > exchange["draining_at"] + 600):
                self._end_exchange(c, exchange["id"], "SKIPPED", "PREPARATION_INCOMPLETE")
                continue
            if exchange["status"] != "DRAINING":
                c.execute(
                    "UPDATE rotation_exchanges SET status='DRAINING',draining_at=? WHERE id=?",
                    (self.clock(), exchange["id"]),
                )
                for pair_id in (exchange["left_pair_id"], exchange["right_pair_id"]):
                    self._pause_clock(c, self._pair(c, pair_id))
                    c.execute(
                        "UPDATE dispatch_jobs SET status='CANCELLED' WHERE pair_id=? AND status='PENDING'", (pair_id,)
                    )
                    self._cancel_group_replies(c, pair_id=pair_id)
            if not row(
                c,
                "SELECT 1 FROM dispatch_jobs WHERE pair_id IN (?,?) AND status IN ('INFLIGHT','UNKNOWN') LIMIT 1",
                (exchange["left_pair_id"], exchange["right_pair_id"]),
            ):
                self._apply_exchange(c, exchange, campaign)
        if cycle["planning_done"] and not row(
            c,
            "SELECT 1 FROM rotation_exchanges WHERE cycle_id=? AND status IN ('PREPARING','READY','DRAINING') LIMIT 1",
            (cycle["id"],),
        ):
            c.execute(
                "UPDATE rotation_cycles SET status='COMPLETE',completed_at=? WHERE id=?", (self.clock(), cycle["id"])
            )
            next_at = cycle["due_at"] + 7 * 86400
            while next_at <= self.clock():
                next_at += 7 * 86400
            c.execute("UPDATE campaigns SET rotation_next_at=? WHERE id=?", (next_at, campaign["id"]))
            counts = {
                r["status"]: r["value"]
                for r in rows(
                    c,
                    "SELECT status,count(*) AS value FROM rotation_exchanges WHERE cycle_id=? GROUP BY status",
                    (cycle["id"],),
                )
            }
            self._notify(
                c,
                f"rotation-summary:{cycle['id']}",
                "rotation_summary",
                {
                    "campaign_id": campaign["id"],
                    "cycle_id": cycle["id"],
                    "applied": counts.get("APPLIED", 0),
                    "skipped": counts.get("SKIPPED", 0),
                    "cancelled": counts.get("CANCELLED", 0),
                },
                50,
            )

    def _apply_exchange(self, c, exchange, campaign):
        targets = rows(
            c,
            "SELECT t.*,pc.cooldown_until,pc.last_sent_at FROM rotation_targets t JOIN pair_chats pc "
            "ON pc.id=t.assignment_id WHERE t.exchange_id=? ORDER BY t.id",
            (exchange["id"],),
        )
        pair_cooldowns = {
            pair_id: c.execute(
                "SELECT COALESCE(max(cooldown_until),0) FROM pair_chats WHERE pair_id=?", (pair_id,)
            ).fetchone()[0]
            for pair_id in (exchange["left_pair_id"], exchange["right_pair_id"])
        }
        for target in targets:
            c.execute(
                "UPDATE pair_chats SET removed_at=?,status='ROTATED' WHERE id=? AND removed_at IS NULL",
                (self.clock(), target["assignment_id"]),
            )
        for target in targets:
            cooldown = max(pair_cooldowns[target["pair_id"]], target["cooldown_until"])
            c.execute(
                "INSERT INTO pair_chats(pair_id,chat_pool_id,assigned_at,last_sent_at,cooldown_until) VALUES (?,?,?,?,?)",
                (target["pair_id"], target["chat_pool_id"], self.clock(), target["last_sent_at"], cooldown),
            )
        c.execute(
            "UPDATE rotation_exchanges SET status='APPLIED',applied_at=? WHERE id=?", (self.clock(), exchange["id"])
        )
        self.audit(c, "ROTATION_APPLIED", campaign_id=campaign["id"], exchange_id=exchange["id"])

    def _rotation_context(self, c, progress_id):
        return row(
            c,
            "SELECT p.*,t.pair_id,t.chat_pool_id,t.exchange_id,x.status AS exchange_status,r.campaign_id,r.due_at,"
            "cp.chat_id,cp.topic_id,cp.link,cp.approved,cp.join_allowed FROM rotation_progress p "
            "JOIN rotation_targets t ON t.id=p.target_id JOIN rotation_exchanges x ON x.id=t.exchange_id "
            "JOIN rotation_cycles r ON r.id=x.cycle_id JOIN chat_pool cp ON cp.id=t.chat_pool_id WHERE p.id=?",
            (progress_id,),
        )

    def _rotation_job_allowed(self, c, job, account, pair, campaign):
        context = self._rotation_context(c, job["payload"].get("rotation_progress_id"))
        if (
            not context
            or not campaign["rotation_enabled"]
            or context["campaign_id"] != campaign["id"]
            or context["pair_id"] != pair["id"]
            or context["account_id"] != account["id"]
            or context["exchange_status"] not in {"PREPARING", "READY"}
            or not context["approved"]
            or self.clock() >= context["due_at"]
            or not self._pair_ready(c, pair)
            or not self._exchange_valid(
                c, row(c, "SELECT * FROM rotation_exchanges WHERE id=?", (context["exchange_id"],))
            )
        ):
            return False
        if context["next_action_at"] > self.clock():
            return False
        if job["kind"] == "ROTATION_JOIN":
            return (not job["payload"].get("join_allowed") or context["join_allowed"]) and account[
                "next_action_at"
            ] <= self.clock()
        if job["kind"] == "ROTATION_CHECK":
            return True
        access = row(
            c,
            "SELECT * FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
            (account["id"], context["chat_pool_id"]),
        )
        if not access or access["manual_required"] or not access["joined"] or not access["can_write"]:
            return False
        if row(
            c,
            "SELECT 1 FROM membership_requests WHERE account_id=? AND chat_pool_id=? AND context_key=? "
            "AND status NOT IN ('CONFIRMED','CANCELLED') LIMIT 1",
            (account["id"], context["chat_pool_id"], f"rotation:{context['exchange_id']}"),
        ):
            return False
        active = pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
        cooldown = c.execute(
            "SELECT COALESCE(max(cooldown_until),0) FROM pair_chats WHERE chat_pool_id=?", (context["chat_pool_id"],)
        ).fetchone()[0]
        return (
            active == account["id"]
            and context["joined"]
            and context["can_write"]
            and context["message_index"] == job["payload"].get("rotation_step")
            and context["message_index"] < campaign["rotation_messages"]
            and cooldown <= self.clock()
        )

    def _plan_rotation_pair(self, c, pair, campaign):
        if not campaign["rotation_enabled"] or campaign["status"] != "RUNNING" or not self._pair_ready(c, pair):
            return
        active = pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
        progress = rows(
            c,
            "SELECT p.id FROM rotation_progress p JOIN rotation_targets t ON t.id=p.target_id "
            "JOIN rotation_exchanges x ON x.id=t.exchange_id JOIN rotation_cycles r ON r.id=x.cycle_id "
            "WHERE t.pair_id=? AND x.status IN ('PREPARING','READY') AND r.due_at>? AND p.next_action_at<=? "
            "AND p.last_error IS NULL AND (p.joined=0 OR p.can_write=0 OR (p.account_id=? AND p.message_index<?) "
            "OR (r.due_at-86400<=? AND p.verified_at<r.due_at-86400)) "
            "ORDER BY p.id LIMIT 16",
            (pair["id"], self.clock(), self.clock(), active, campaign["rotation_messages"], self.clock()),
        )
        for item in progress:
            p = self._rotation_context(c, item["id"])
            if p["last_error"]:
                continue
            account = row(c, "SELECT * FROM accounts WHERE id=?", (p["account_id"],))
            if not p["joined"]:
                kind, phase = "ROTATION_JOIN", f"join:{p['generation']}"
            elif not p["can_write"]:
                kind, phase = "ROTATION_CHECK", f"check:{p['generation']}"
            elif p["message_index"] < campaign["rotation_messages"]:
                kind, phase = "ROTATION_MESSAGE", f"message:{p['message_index']}"
            elif self.clock() >= p["due_at"] - 86400 and p["verified_at"] < p["due_at"] - 86400:
                kind, phase = "ROTATION_CHECK", f"check:{p['generation']}"
            else:
                continue
            payload = {
                "rotation_progress_id": p["id"],
                "rotation_step": p["message_index"],
                "peer_id": p["chat_id"],
                "topic_id": p["topic_id"],
                "link": p["link"],
                "join_allowed": bool(p["join_allowed"]),
            }
            if kind == "ROTATION_MESSAGE":
                templates = self.templates(campaign["id"], "mailing", campaign["version"], c)
                if not templates:
                    continue
                template = (
                    random.choice(templates)
                    if campaign["selection"] == "random"
                    else templates[p["message_index"] % len(templates)]
                )
                payload.update({key: template[key] for key in ("text", "media_path", "media_kind")})
            candidate = {"kind": kind, "payload": payload}
            if self._rotation_job_allowed(c, candidate, account, pair, campaign):
                self._enqueue(
                    c, pair, campaign, kind, p["chat_pool_id"], p["account_id"], payload, f"rotation:{p['id']}:{phase}"
                )

    def _finish_rotation_job(self, c, job, result):
        p = self._rotation_context(c, job["payload"]["rotation_progress_id"])
        if not p:
            return
        now = self.clock()
        if job["kind"] in {"ROTATION_JOIN", "ROTATION_CHECK"}:
            can_write = bool(result.data.get("can_write", True))
            hard_error = p["last_error"] if p["last_error"] in {"MANUAL_VERIFICATION", "MEMBERSHIP_LIMIT"} else None
            if hard_error:
                can_write = False
            c.execute(
                "UPDATE rotation_progress SET joined=1,can_write=?,generation=generation+1,check_count=check_count+1,verified_at=?,"
                "next_action_at=?,last_error=? WHERE id=?",
                (
                    can_write,
                    now,
                    now + (5 if can_write else 60),
                    hard_error or ("PUBLICATION_FORBIDDEN" if not can_write and p["check_count"] >= 5 else None),
                    p["id"],
                ),
            )
            c.execute(
                "UPDATE account_chat_access SET joined=1,can_write=?,last_checked_at=? WHERE account_id=? AND chat_pool_id=?",
                (can_write, now, p["account_id"], p["chat_pool_id"]),
            )
            if job["kind"] == "ROTATION_JOIN":
                pause = int(row(c, "SELECT value FROM settings WHERE key='join_pause'")["value"])
                c.execute("UPDATE accounts SET next_action_at=? WHERE id=?", (now + pause, job["account_id"]))
        else:
            c.execute(
                "UPDATE rotation_progress SET message_index=message_index+1,next_action_at=?,verified_at=? WHERE id=?",
                (now + 6 * 3600, now, p["id"]),
            )
            interval = row(c, "SELECT interval_seconds FROM campaigns WHERE id=?", (job["campaign_id"],))[
                "interval_seconds"
            ]
            c.execute(
                "UPDATE pair_chats SET last_sent_at=?,cooldown_until=MAX(cooldown_until,?),last_result='CONFIRMED' "
                "WHERE chat_pool_id=? AND removed_at IS NULL",
                (now, now + interval, p["chat_pool_id"]),
            )

    def retry_rotation(self, exchange_id):
        with self.db.tx() as c:
            exchange = row(
                c, "SELECT * FROM rotation_exchanges WHERE id=? AND status IN ('PREPARING','READY')", (exchange_id,)
            )
            if not exchange:
                raise DomainError("Подготовка уже завершена или обмен пропущен")
            # This button explicitly acknowledges manual completion. Re-read
            # actual rights before allowing another message; do not clear an
            # unfinished sponsor subscription or an UNKNOWN network result.
            c.execute(
                "UPDATE account_chat_access SET manual_required=0,last_error=NULL,can_write=0 "
                "WHERE last_error IN ('MANUAL_VERIFICATION','MEMBERSHIP_LIMIT') AND EXISTS("
                "SELECT 1 FROM rotation_progress p JOIN rotation_targets t ON t.id=p.target_id "
                "WHERE t.exchange_id=? AND p.account_id=account_chat_access.account_id "
                "AND t.chat_pool_id=account_chat_access.chat_pool_id)",
                (exchange_id,),
            )
            c.execute(
                "UPDATE rotation_progress SET can_write=0,verified_at=0 WHERE last_error IN "
                "('MANUAL_VERIFICATION','MEMBERSHIP_LIMIT') AND target_id IN "
                "(SELECT id FROM rotation_targets WHERE exchange_id=?)",
                (exchange_id,),
            )
            c.execute(
                "UPDATE rotation_progress SET last_error=NULL,check_count=0,generation=generation+1,next_action_at=? "
                "WHERE target_id IN (SELECT id FROM rotation_targets WHERE exchange_id=?)",
                (self.clock(), exchange_id),
            )
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE status='FAILED' AND "
                "json_extract(payload,'$.rotation_progress_id') IN (SELECT p.id FROM rotation_progress p "
                "JOIN rotation_targets t ON t.id=p.target_id WHERE t.exchange_id=?)",
                (exchange_id,),
            )
            c.execute(
                "UPDATE membership_requests SET status='WAITING',last_error=NULL WHERE context_key=? AND status='FAILED'",
                (f"rotation:{exchange_id}",),
            )
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE status='FAILED' AND "
                "json_extract(payload,'$.membership_request_id') IN (SELECT id FROM membership_requests WHERE context_key=?)",
                (f"rotation:{exchange_id}",),
            )

    def _preparation_failed(self, c, job, reason):
        if job["kind"] == "MEMBERSHIP":
            request = row(c, "SELECT * FROM membership_requests WHERE id=?", (job["payload"]["membership_request_id"],))
            if (
                request
                and request["action"] == "VERIFY"
                and row(
                    c,
                    "SELECT 1 FROM membership_requests WHERE account_id=? "
                    "AND chat_pool_id=? AND context_key=? AND action='VERIFY' "
                    "AND source_peer_id=? AND source_sender_id=? AND source_message_id>? AND status IN ('WAITING','CONFIRMED') LIMIT 1",
                    (
                        request["account_id"],
                        request["chat_pool_id"],
                        request["context_key"],
                        request["source_peer_id"],
                        request["source_sender_id"],
                        request["source_message_id"],
                    ),
                )
            ):
                c.execute(
                    "UPDATE membership_requests SET status='CANCELLED',last_error=? WHERE id=?", (reason, request["id"])
                )
                return
            c.execute(
                "UPDATE membership_requests SET status='FAILED',last_error=? WHERE id=? AND status<>'CONFIRMED'",
                (reason, job["payload"]["membership_request_id"]),
            )
        else:
            c.execute(
                "UPDATE rotation_progress SET last_error=? WHERE id=?", (reason, job["payload"]["rotation_progress_id"])
            )
        chat = row(c, "SELECT title,link FROM chat_pool WHERE id=?", (job["chat_pool_id"],))
        exchange_id = None
        if job["kind"] == "MEMBERSHIP":
            request = row(
                c, "SELECT context_key FROM membership_requests WHERE id=?", (job["payload"]["membership_request_id"],)
            )
            if request and request["context_key"].startswith("rotation:"):
                exchange_id = int(request["context_key"].split(":")[1])
        else:
            context = self._rotation_context(c, job["payload"]["rotation_progress_id"])
            exchange_id = context["exchange_id"] if context else None
        self._notify(
            c,
            f"preparation:{job['id']}:{job['attempt_id']}",
            "preparation_error",
            {
                "account_id": job["account_id"],
                "campaign_id": job["campaign_id"],
                "reason": reason,
                "chat_pool_id": job["chat_pool_id"],
                "title": chat["title"],
                "link": chat["link"],
                "exchange_id": exchange_id,
            },
            50,
        )
