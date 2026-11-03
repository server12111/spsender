import asyncio
import json
import random

from .core import DomainError, dump, row, rows
from .pairs import active_count, shift_role


class Dispatch:
    def _enqueue(self, c, pair, campaign, kind, chat_id, account_id, payload, key, planned_at=None, budget=None):
        existing = row(c, "SELECT * FROM dispatch_jobs WHERE idempotency_key=?", (key,))
        if existing and (existing["pair_id"] != pair["id"] or existing["campaign_id"] != campaign["id"]):
            if existing["status"] in {"INFLIGHT", "UNKNOWN"} or (
                existing["status"] == "CONFIRMED" and kind not in {"CHECK", "JOIN"}
            ):
                return None
            # Keep historical attempts attached to their original pair/campaign.
            key = f"{key}:scope:{campaign['id']}:{pair['id']}"
            existing = row(c, "SELECT * FROM dispatch_jobs WHERE idempotency_key=?", (key,))
        if existing and existing["status"] == "FAILED" and existing["version"] != campaign["version"]:
            c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=?", (existing["id"],))
            existing["status"] = "CANCELLED"
        needs_slot = not existing or existing["status"] == "CANCELLED"
        if needs_slot:
            if budget is None:
                pending = c.execute("SELECT value FROM queue_counters WHERE name='pending'").fetchone()[0]
                capacity = int(row(c, "SELECT value FROM settings WHERE key='max_pending_jobs'")["value"])
                if pending >= capacity:
                    return None
            else:
                if budget["remaining"] <= 0:
                    return None
                budget["remaining"] -= 1
        if existing:
            if existing["status"] == "CANCELLED":
                c.execute(
                    "UPDATE dispatch_jobs SET status='PENDING',account_id=?,payload=?,version=?,planned_at=?,current_attempt_id=NULL,retry_count=0,lease_owner=NULL,lease_until=NULL,last_error=NULL WHERE id=?",
                    (account_id, dump(payload), campaign["version"], planned_at or self.clock(), existing["id"]),
                )
                return existing["id"]
            return existing["id"] if existing["status"] == "PENDING" else None
        job_id = c.execute(
            "INSERT INTO dispatch_jobs(idempotency_key,campaign_id,pair_id,account_id,chat_pool_id,kind,payload,version,planned_at,created_at,queue_rank) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                key,
                campaign["id"],
                pair["id"],
                account_id,
                chat_id,
                kind,
                dump(payload),
                campaign["version"],
                planned_at or self.clock(),
                self.clock(),
                {
                    "CHECK": 0,
                    "JOIN": 1,
                    "GREETING": 2,
                    "MEMBERSHIP": 1,
                    "ROTATION_JOIN": 1,
                    "ROTATION_CHECK": 1,
                    "ROTATION_MESSAGE": 2,
                }.get(kind, 3),
            ),
        ).lastrowid
        return job_id

    def _plan_dispatch(self, c, pair, campaign):
        if getattr(self,"legacy_runtime_active",False):
            return
        if campaign["status"] != "RUNNING" or not self._pair_ready(c, pair):
            return
        chats = self._assigned_chats(c, pair["id"])
        limit = active_count(len(chats), self.elapsed(pair))
        templates = self.templates(campaign["id"], "mailing", campaign["version"], c)
        if not templates:
            return
        role = shift_role(self.clock(), campaign["timezone"])
        account_id = pair["account_a_id"] if role == "A" else pair["account_b_id"]
        failures = {
            r["chat_pool_id"]
            for r in rows(
                c, "SELECT chat_pool_id FROM chat_failures WHERE pair_id=? AND account_id=?", (pair["id"], account_id)
            )
        }
        writable = {
            r["chat_pool_id"]
            for r in rows(
                c,
                "SELECT a.chat_pool_id FROM account_chat_access a JOIN pair_chats pc ON pc.chat_pool_id=a.chat_pool_id WHERE pc.pair_id=? AND pc.removed_at IS NULL AND a.account_id IN (?,?) AND a.joined=1 AND a.can_write=1 AND a.manual_required=0 GROUP BY a.chat_pool_id HAVING count(*)=2",
                (pair["id"], pair["account_a_id"], pair["account_b_id"]),
            )
        }
        busy = {
            r["chat_pool_id"]
            for r in rows(
                c,
                "SELECT chat_pool_id FROM dispatch_jobs WHERE pair_id=? AND status IN ('PENDING','INFLIGHT','UNKNOWN')",
                (pair["id"],),
            )
        }
        cursor = row(c, "SELECT template_cursor FROM campaigns WHERE id=?", (campaign["id"],))["template_cursor"]
        original_cursor = cursor
        budget = {
            "remaining": int(row(c, "SELECT value FROM settings WHERE key='max_pending_jobs'")["value"])
            - row(c, "SELECT value FROM queue_counters WHERE name='pending'")["value"]
        }
        for chat in chats[:limit]:
            if chat["status"] != "ASSIGNED" or not chat["approved"] or chat["cooldown_until"] > self.clock():
                continue
            if chat["chat_pool_id"] in failures:
                continue
            if chat["chat_pool_id"] not in writable:
                continue
            if chat["chat_pool_id"] in busy:
                continue
            template = (
                random.choice(templates) if campaign["selection"] == "random" else templates[cursor % len(templates)]
            )
            payload = {
                "text": template["text"],
                "media_path": template["media_path"],
                "media_kind": template["media_kind"],
                "topic_id": chat["topic_id"],
                "peer_id": chat["chat_id"],
            }
            slot = int(self.clock() // campaign["interval_seconds"])
            job_id = self._enqueue(
                c,
                pair,
                campaign,
                "MAILING",
                chat["chat_pool_id"],
                None,
                payload,
                f"mail:{campaign['id']}:{pair['id']}:{chat['chat_pool_id']}:{slot}:{campaign['version']}",
                budget=budget,
            )
            if job_id:
                cursor += 1
            if not job_id:
                if budget["remaining"] <= 0:
                    break
        if cursor != original_cursor:
            c.execute("UPDATE campaigns SET template_cursor=? WHERE id=?", (cursor, campaign["id"]))

    def tick(self, pair_limit=None):
        limit = pair_limit if pair_limit is not None else self.runtime_options()["planning_batch_size"]
        with self.db.tx() as c:
            for account in rows(
                c,
                "SELECT * FROM accounts WHERE onboarding_stage='SILENT_24H' AND onboarding_started_at<=? AND account_health<>'REMOVED'",
                (self.clock() - 86400,),
            ):
                self._stage(c, account, "PREPARING")
            self._fill_waiting(c)
            for rotating in rows(c, "SELECT * FROM campaigns WHERE rotation_enabled=1 AND status='RUNNING'"):
                self._plan_rotations(c, rotating)
            cursor = row(c, "SELECT value FROM settings WHERE key='planner_cursor'")
            cursor = int(cursor["value"]) if cursor else 0
            selected = rows(
                c,
                "SELECT p.id FROM pairs p JOIN campaigns ca ON ca.id=p.campaign_id WHERE ca.status<>'REMOVED' AND p.account_a_id IS NOT NULL AND p.id>? ORDER BY p.id LIMIT ?",
                (cursor, limit),
            )
            if not selected and cursor:
                selected = rows(
                    c,
                    "SELECT p.id FROM pairs p JOIN campaigns ca ON ca.id=p.campaign_id WHERE ca.status<>'REMOVED' AND p.account_a_id IS NOT NULL ORDER BY p.id LIMIT ?",
                    (limit,),
                )
        # Keep writer lock bounded to one pair, not an entire account pool.
        for selected_pair in selected:
            with self.db.tx() as c:
                pair = self._pair(c, selected_pair["id"])
                if not pair["account_a_id"]:
                    continue
                record = row(c, "SELECT status FROM campaigns WHERE id=?", (pair["campaign_id"],))
                if not record or record["status"] == "REMOVED":
                    continue
                pending_before = row(c, "SELECT value FROM queue_counters WHERE name='pending'")["value"]
                campaign = self.campaign(c, pair["campaign_id"])
                self._plan_memberships(c, pair, campaign)
                self._onboard(c, pair, campaign)
                pair = self._pair(c, pair["id"])
                self._sync_pair(c, pair)
                pair = self._pair(c, pair["id"])
                self._plan_dm_sequences(c, pair, campaign)
                self._plan_group_replies(c, pair, campaign)
                self._plan_rotation_pair(c, pair, campaign)
                self._plan_dispatch(c, pair, campaign)
                c.execute(
                    "INSERT INTO settings VALUES ('planner_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(pair["id"]),),
                )
                pending_after = row(c, "SELECT value FROM queue_counters WHERE name='pending'")["value"]
                if pending_after > pending_before and pending_after >= int(
                    row(c, "SELECT value FROM settings WHERE key='max_pending_jobs'")["value"]
                ):
                    # Resume at the next pair when capacity returns, rather than
                    # letting the first pair refill every available queue slot.
                    break
        self.hourly_summary()

    def _job_allowed(self, c, job, account, pair, campaign):
        if job["kind"] == "GROUP_REPLY":
            return False
        if account and row(
            c, "SELECT 1 FROM legacy_rpc_journal WHERE account_id=? AND status IN ('INFLIGHT','UNKNOWN') LIMIT 1", (account["id"],)
        ):
            return False
        if campaign["status"] != "RUNNING" or campaign["version"] != job["version"]:
            return False
        if self._rotation_pair_blocked(c, pair["id"]):
            return False
        payload = json.loads(job["payload"]) if isinstance(job["payload"], str) else job["payload"]
        if "dm_sequence_id" in payload:
            sequence = row(c, "SELECT * FROM dm_reply_sequences WHERE id=?", (payload["dm_sequence_id"],))
            if (
                not sequence
                or sequence["status"] != "ACTIVE"
                or sequence["next_step_at"] > self.clock()
                or sequence["step_index"] != payload["dm_step"]
                or sequence["current_job_id"] != job["id"]
            ):
                return False
        if job["kind"] == "DM_MANUAL":
            payload = json.loads(job["payload"]) if isinstance(job["payload"], str) else job["payload"]
            if self.clock() - payload.get("incoming_at", self.clock()) > 600:
                return False
        if (
            not account
            or account["account_health"] != "OK"
            or account["paused"]
            or account["flood_until"] > self.clock()
        ):
            return False
        member = row(
            c, "SELECT * FROM campaign_members WHERE account_id=? AND campaign_id=?", (account["id"], campaign["id"])
        )
        if not member or account["id"] not in {pair["account_a_id"], pair["account_b_id"]}:
            return False
        age = self.clock() - account["onboarding_started_at"]
        if age < 86400:
            return False
        if job["kind"] == "MEMBERSHIP":
            return self._membership_job_allowed(c, {**job, "payload": payload}, account, pair)
        if job["kind"] in {"ROTATION_JOIN", "ROTATION_CHECK", "ROTATION_MESSAGE"}:
            return self._rotation_job_allowed(c, {**job, "payload": payload}, account, pair, campaign)
        if job["kind"] == "CHECK":
            return True
        if job["kind"] == "JOIN":
            if age < 25 * 3600 or not pair["account_b_id"] or self.clock() < account["next_action_at"]:
                return False
        else:
            active = (
                pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
            )
            if active != account["id"]:
                return False
            if job["kind"] == "GREETING":
                if age < 26 * 3600 or not campaign["greeting_enabled"]:
                    return False
                if not all(
                    a
                    and a["account_health"] == "OK"
                    and not a["paused"]
                    and a["flood_until"] <= self.clock()
                    and a["prepared_at"]
                    and a["joined_at"]
                    and self.clock() - a["onboarding_started_at"] >= 26 * 3600
                    for a in self._pair_accounts(c, pair)
                ):
                    return False
            elif not self._pair_ready(c, pair):
                return False
        if job["chat_pool_id"] is not None:
            chat = row(
                c,
                "SELECT pc.*,cp.approved,cp.join_allowed,cp.greeting_allowed,cp.split_allowed FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id "
                "WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
                (pair["id"], job["chat_pool_id"]),
            )
            if not chat or chat["status"] != "ASSIGNED" or not chat["approved"]:
                return False
            if job["kind"] == "JOIN" and payload.get("join_allowed") and not chat["join_allowed"]:
                return False
            if job["kind"] != "JOIN":
                access = row(
                    c,
                    "SELECT * FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
                    (account["id"], job["chat_pool_id"]),
                )
                if not access or not access["can_write"] or access["manual_required"]:
                    return False
            if job["kind"] == "GREETING" and not chat["greeting_allowed"]:
                return False
            if job["kind"] == "GREETING":
                payload = json.loads(job["payload"]) if isinstance(job["payload"], str) else job["payload"]
                progress = row(
                    c,
                    "SELECT * FROM greeting_progress WHERE campaign_id=? AND chat_pool_id=?",
                    (campaign["id"], job["chat_pool_id"]),
                )
                if not progress:
                    return False
                current_mask = [int(progress[key + "_done"]) for key in ("emoji", "phrase", "main")]
                stage = payload.get("stage")
                if all(current_mask) or (stage in {"emoji", "phrase", "main"} and progress[stage + "_done"]):
                    return False
                if stage != "combined" and not (campaign["greeting_split"] and chat["split_allowed"]):
                    return False
                if "progress_mask" in payload and current_mask != payload["progress_mask"]:
                    return False
            if job["kind"] == "MAILING":
                chats = self._assigned_chats(c, pair["id"])
                active_ids = [r["chat_pool_id"] for r in chats[: active_count(len(chats), self.elapsed(pair))]]
                if chat["cooldown_until"] > self.clock() or job["chat_pool_id"] not in active_ids:
                    return False
                if row(
                    c,
                    "SELECT 1 FROM chat_failures WHERE pair_id=? AND chat_pool_id=? AND account_id=?",
                    (pair["id"], job["chat_pool_id"], account["id"]),
                ):
                    return False
        return True

    def claim_job(self, owner):
        with self.db.tx() as c:
            for _ in range(32):
                job = row(
                    c,
                    "SELECT j.* FROM dispatch_jobs j INDEXED BY job_ready_order JOIN campaigns ca ON ca.id=j.campaign_id JOIN pairs p ON p.id=j.pair_id WHERE j.status='PENDING' AND j.planned_at<=? "
                    "AND ca.status='RUNNING' AND (j.account_id IS NULL OR EXISTS(SELECT 1 FROM accounts own WHERE own.id=j.account_id AND own.account_health='OK' AND own.paused=0 AND own.flood_until<=?)) "
                    "AND (j.kind IN ('CHECK','JOIN','GREETING','MEMBERSHIP') OR (EXISTS(SELECT 1 FROM accounts a WHERE a.id=p.account_a_id AND a.onboarding_stage='READY' AND a.account_health='OK' AND a.paused=0 AND a.flood_until<=?) AND EXISTS(SELECT 1 FROM accounts b WHERE b.id=p.account_b_id AND b.onboarding_stage='READY' AND b.account_health='OK' AND b.paused=0 AND b.flood_until<=?))) "
                    "AND NOT EXISTS(SELECT 1 FROM dispatch_jobs busy WHERE busy.status IN ('INFLIGHT','UNKNOWN') "
                    "AND (busy.pair_id=j.pair_id OR (j.chat_pool_id IS NOT NULL AND busy.chat_pool_id=j.chat_pool_id))) "
                    "ORDER BY j.queue_rank,j.dispatch_order,j.id LIMIT 1",
                    (self.clock(), self.clock(), self.clock(), self.clock()),
                )
                if not job:
                    break
                pair = self._pair(c, job["pair_id"])
                campaign = self.campaign(c, job["campaign_id"])
                account_id = job["account_id"]
                if job["kind"] == "MAILING":
                    account_id = (
                        pair["account_a_id"]
                        if shift_role(self.clock(), campaign["timezone"]) == "A"
                        else pair["account_b_id"]
                    )
                account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
                if not self._job_allowed(c, job, account, pair, campaign):
                    # A shift boundary can make a greeting/reply stale. Keep greetings reusable.
                    if campaign["status"] != "RUNNING" or campaign["version"] != job["version"]:
                        c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=?", (job["id"],))
                    elif job["kind"] in {"GREETING", "GROUP_REPLY", "DM_FIRST", "DM_ALWAYS", "DM_MANUAL"}:
                        c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=?", (job["id"],))
                    else:
                        c.execute(
                            "UPDATE dispatch_jobs SET planned_at=?,dispatch_order=? WHERE id=?",
                            (self.clock() + self.config.worker_interval, self.clock(), job["id"]),
                        )
                    continue
                if row(
                    c,
                    "SELECT 1 FROM dispatch_jobs WHERE status IN ('INFLIGHT','UNKNOWN') AND "
                    "(account_id=? OR chat_pool_id=?)",
                    (account_id, job["chat_pool_id"]),
                ):
                    c.execute("UPDATE dispatch_jobs SET dispatch_order=? WHERE id=?", (self.clock(), job["id"]))
                    continue
                now = self.clock()
                c.execute(
                    "UPDATE dispatch_jobs SET account_id=?,status='INFLIGHT',lease_owner=?,lease_until=?,started_at=? WHERE id=? AND status='PENDING'",
                    (account_id, owner, now + 180, now, job["id"]),
                )
                attempt_id = c.execute(
                    "INSERT INTO message_attempts(job_id,account_id,started_at) VALUES (?,?,?)",
                    (job["id"], account_id, now),
                ).lastrowid
                c.execute("UPDATE dispatch_jobs SET current_attempt_id=? WHERE id=?", (attempt_id, job["id"]))
                if job["kind"] == "MAILING" and pair["pair_dispatch_started_at"] is None:
                    c.execute(
                        "UPDATE pairs SET pair_dispatch_started_at=?,active_since=? WHERE id=?", (now, now, pair["id"])
                    )
                self.audit(
                    c,
                    "JOB_STARTED",
                    campaign_id=campaign["id"],
                    pair_id=pair["id"],
                    account_id=account_id,
                    chat_pool_id=job["chat_pool_id"],
                    job_id=job["id"],
                    kind=job["kind"],
                )
                return {
                    **job,
                    "account_id": account_id,
                    "attempt_id": attempt_id,
                    "payload": json.loads(job["payload"]),
                    "lease_owner": owner,
                }
        return None

    def execution_allowed(self, job):
        """Revalidate immediately before a new network operation, including after awaited checks."""
        with self.db.read() as c:
            current = row(c, "SELECT * FROM dispatch_jobs WHERE id=?", (job["id"],))
            if not current or current["status"] != "INFLIGHT":
                return False
            if current["current_attempt_id"] != job.get("attempt_id") or current["lease_until"] <= self.clock():
                return False
            if current["lease_owner"] != job.get("lease_owner"):
                return False
            if self.config.mode == "live":
                lease = row(c, "SELECT * FROM worker_leases WHERE name='telegram-runtime'")
                if not lease or lease["owner"] != job.get("lease_owner") or lease["expires_at"] <= self.clock():
                    return False
            pair = self._pair(c, job["pair_id"])
            campaign = row(c, "SELECT * FROM campaigns WHERE id=?", (job["campaign_id"],))
            account = row(c, "SELECT * FROM accounts WHERE id=?", (job["account_id"],))
            return bool(campaign and self._job_allowed(c, job, account, pair, campaign))

    def finish_job(self, job, result):
        with self.db.tx() as c:
            current = row(c, "SELECT * FROM dispatch_jobs WHERE id=?", (job["id"],))
            if (
                not current
                or current["status"] not in {"INFLIGHT", "UNKNOWN"}
                or current["current_attempt_id"] != job["attempt_id"]
            ):
                return
            now = self.clock()
            c.execute("UPDATE dispatch_jobs SET status='CONFIRMED',completed_at=? WHERE id=?", (now, job["id"]))
            c.execute("UPDATE message_attempts SET outcome='CONFIRMED' WHERE id=?", (job["attempt_id"],))
            c.execute(
                "INSERT OR IGNORE INTO message_results(job_id,attempt_id,api_message_id,confirmed_at,result) VALUES (?,?,?,?,?)",
                (job["id"], job["attempt_id"], result.message_id, now, dump(result.data)),
            )
            if job["kind"] in {"ROTATION_JOIN", "ROTATION_CHECK", "ROTATION_MESSAGE"}:
                self._finish_rotation_job(c, job, result)
            elif job["kind"] == "MEMBERSHIP":
                self._finish_membership(c, job, result)
            elif job["kind"] == "CHECK":
                c.execute(
                    "UPDATE accounts SET prepared_at=COALESCE(prepared_at,?) WHERE id=?", (now, job["account_id"])
                )
            elif job["kind"] == "JOIN":
                c.execute(
                    "UPDATE account_chat_access SET joined=1,can_write=1,manual_required=0,last_checked_at=?,last_error=NULL WHERE account_id=? AND chat_pool_id=?",
                    (now, job["account_id"], job["chat_pool_id"]),
                )
                pause = int(row(c, "SELECT value FROM settings WHERE key='join_pause'")["value"])
                c.execute("UPDATE accounts SET next_action_at=? WHERE id=?", (now + pause, job["account_id"]))
            else:
                campaign = row(c, "SELECT * FROM campaigns WHERE id=?", (job["campaign_id"],))
                if job["kind"] == "GREETING":
                    stage = job["payload"]["stage"]
                    fields = ("emoji_done", "phrase_done", "main_done") if stage == "combined" else (stage + "_done",)
                    for field in fields:
                        if field not in {"emoji_done", "phrase_done", "main_done"}:
                            raise ValueError("Invalid greeting stage")
                        c.execute(
                            f"UPDATE greeting_progress SET {field}=1 WHERE campaign_id=? AND chat_pool_id=?",
                            (job["campaign_id"], job["chat_pool_id"]),
                        )
                if job["chat_pool_id"] is not None:
                    c.execute(
                        "UPDATE pair_chats SET last_sent_at=?,cooldown_until=?,last_result='CONFIRMED',last_error=NULL "
                        "WHERE id=(SELECT id FROM pair_chats WHERE pair_id=? AND chat_pool_id=? ORDER BY id DESC LIMIT 1)",
                        (now, now + campaign["interval_seconds"], job["pair_id"], job["chat_pool_id"]),
                    )
                history_kind = job["payload"].get("history_kind")
                if "dm_sequence_id" in job["payload"]:
                    self._finish_dm_step(c, job, now)
                elif history_kind:
                    c.execute(
                        "UPDATE reply_history SET last_replied_at=?,reserved_job_id=NULL WHERE campaign_id=? AND peer_id=? AND kind=?",
                        (now, job["campaign_id"], job["payload"]["peer_id"], history_kind),
                    )
            self.audit(
                c,
                "JOB_CONFIRMED",
                campaign_id=job["campaign_id"],
                pair_id=job["pair_id"],
                account_id=job["account_id"],
                chat_pool_id=job["chat_pool_id"],
                job_id=job["id"],
                kind=job["kind"],
            )

    def fail_job(self, job, error):
        from app.transport.base import OperationError

        if not isinstance(error, OperationError):
            error = OperationError("unknown", type(error).__name__)
        with self.db.tx() as c:
            current = row(c, "SELECT * FROM dispatch_jobs WHERE id=?", (job["id"],))
            if (
                not current
                or current["status"] not in {"INFLIGHT", "UNKNOWN"}
                or current["current_attempt_id"] != job["attempt_id"]
            ):
                return
            status = (
                "UNKNOWN"
                if error.category == "unknown"
                else ("CANCELLED" if error.category == "cancelled" else "FAILED")
            )
            c.execute(
                "UPDATE dispatch_jobs SET status=?,last_error=?,completed_at=? WHERE id=?",
                (status, error.code, self.clock(), job["id"]),
            )
            c.execute(
                "UPDATE message_attempts SET outcome=?,error=? WHERE id=?", (status, error.code, job["attempt_id"])
            )
            preparation_error = job["kind"] in {"ROTATION_JOIN", "ROTATION_CHECK", "ROTATION_MESSAGE", "MEMBERSHIP"}
            if preparation_error and error.category in {"manual", "chat", "forbidden", "recipient"}:
                self._preparation_failed(c, job, error.code)
            elif error.category == "cancelled" and error.code == "CANCELLED_BEFORE_REQUEST":
                pair = self._pair(c, job["pair_id"])
                campaign = row(c, "SELECT * FROM campaigns WHERE id=?", (job["campaign_id"],))
                account = row(c, "SELECT * FROM accounts WHERE id=?", (job["account_id"],))
                if campaign and self._job_allowed(c, job, account, pair, campaign):
                    c.execute(
                        "UPDATE dispatch_jobs SET status='PENDING',lease_owner=NULL,lease_until=NULL,"
                        "current_attempt_id=NULL,completed_at=NULL WHERE id=?",
                        (job["id"],),
                    )
            elif error.category == "critical":
                self._critical(c, job["account_id"], error.code, error.code)
            elif error.category == "flood":
                self._flood_wait(c, job["account_id"], error.seconds)
                c.execute(
                    "UPDATE dispatch_jobs SET status='PENDING',planned_at=?,retry_count=retry_count+1 WHERE id=?",
                    (self.clock() + max(1, error.seconds), job["id"]),
                )
                maximum = int(row(c, "SELECT value FROM settings WHERE key='max_retries'")["value"])
                if current["retry_count"] >= maximum:
                    c.execute("UPDATE dispatch_jobs SET status='FAILED' WHERE id=?", (job["id"],))
            elif error.category == "transient":
                options = {
                    r["key"]: r["value"]
                    for r in rows(
                        c, "SELECT key,value FROM settings WHERE key IN ('max_retries','retry_base','retry_max')"
                    )
                }
                retry = current["retry_count"] + 1
                if retry <= int(options["max_retries"]):
                    delay = min(float(options["retry_max"]), float(options["retry_base"]) * 2 ** (retry - 1))
                    c.execute(
                        "UPDATE dispatch_jobs SET status='PENDING',retry_count=?,planned_at=?,dispatch_order=? WHERE id=?",
                        (retry, self.clock() + delay, self.clock(), job["id"]),
                    )
                else:
                    self.audit(
                        c,
                        "RETRY_EXHAUSTED",
                        campaign_id=job["campaign_id"],
                        pair_id=job["pair_id"],
                        account_id=job["account_id"],
                        chat_pool_id=job["chat_pool_id"],
                        job_id=job["id"],
                    )
            elif error.category == "manual" and job["chat_pool_id"]:
                self._cancel_group_replies(c, chat_pool_id=job["chat_pool_id"])
                c.execute(
                    "UPDATE account_chat_access SET manual_required=1,last_error=? WHERE account_id=? AND chat_pool_id=?",
                    (error.code, job["account_id"], job["chat_pool_id"]),
                )
                c.execute(
                    "UPDATE pair_chats SET status='WAITING_MANUAL_VERIFICATION',last_error=? WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
                    (error.code, job["pair_id"], job["chat_pool_id"]),
                )
                c.execute(
                    "UPDATE chat_pool SET status='WAITING_MANUAL_VERIFICATION' WHERE id=?", (job["chat_pool_id"],)
                )
                chat = row(c, "SELECT link,title FROM chat_pool WHERE id=?", (job["chat_pool_id"],))
                self._notify(
                    c,
                    f"manual:{job['account_id']}:{job['chat_pool_id']}:{job['attempt_id']}",
                    "manual",
                    {**chat, "chat_pool_id": job["chat_pool_id"], "account_id": job["account_id"]},
                    50,
                )
            elif error.category in {"chat", "forbidden"} and job["chat_pool_id"]:
                self._chat_failure(
                    c, job["pair_id"], job["chat_pool_id"], job["account_id"], error.code, error.category == "forbidden"
                )
            elif error.category == "configuration":
                paused = c.execute(
                    "UPDATE campaigns SET status='PAUSED' WHERE id=? AND version=? AND status='RUNNING'",
                    (job["campaign_id"], job["version"]),
                ).rowcount
                if paused:
                    c.execute(
                        "UPDATE dispatch_jobs SET status='CANCELLED' WHERE campaign_id=? AND status='PENDING'",
                        (job["campaign_id"],),
                    )
                    for pair in rows(c, "SELECT * FROM pairs WHERE campaign_id=?", (job["campaign_id"],)):
                        self._pause_clock(c, pair)
                    self._notify(
                        c,
                        f"configuration:{job['campaign_id']}:{job['version']}",
                        "campaign_error",
                        {"campaign_id": job["campaign_id"], "reason": error.code},
                        80,
                    )
            if status == "UNKNOWN":
                self._notify(
                    c, f"unknown:{job['id']}", "unknown", {"job_id": job["id"], "account_id": job["account_id"]}, 80
                )
            campaign = row(c, "SELECT status,version FROM campaigns WHERE id=?", (job["campaign_id"],))
            if not campaign or campaign["status"] != "RUNNING" or campaign["version"] != job["version"]:
                c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=? AND status='PENDING'", (job["id"],))
            terminal = row(c, "SELECT status FROM dispatch_jobs WHERE id=?", (job["id"],))
            if terminal["status"] == "FAILED":
                self._fail_dm_step(c, job, error.code)
                if preparation_error and error.category not in {"manual", "chat", "forbidden", "recipient"}:
                    self._preparation_failed(c, job, error.code)
            self.audit(
                c,
                "JOB_ERROR",
                campaign_id=job["campaign_id"],
                pair_id=job["pair_id"],
                account_id=job["account_id"],
                chat_pool_id=job["chat_pool_id"],
                job_id=job["id"],
                category=error.category,
                error=error.code,
            )

    def recover_expired(self):
        with self.db.tx() as c:
            for job in rows(
                c, "SELECT * FROM dispatch_jobs WHERE status='INFLIGHT' AND lease_until<=? AND json_extract(payload,'$._legacy_operation_id') IS NULL", (self.clock(),)
            ):
                c.execute("UPDATE dispatch_jobs SET status='UNKNOWN',last_error='WORKER_LOST' WHERE id=?", (job["id"],))
                c.execute(
                    "UPDATE message_attempts SET outcome='UNKNOWN',error='WORKER_LOST' WHERE job_id=? AND outcome='STARTED'",
                    (job["id"],),
                )
                self._notify(
                    c, f"unknown:{job['id']}", "unknown", {"job_id": job["id"], "account_id": job["account_id"]}, 80
                )
                self.audit(c, "JOB_UNKNOWN", campaign_id=job["campaign_id"], job_id=job["id"])

    def resolve_unknown(self, job_id, sent, api_message_id=None):
        from app.transport.base import Result

        job = self.db.one("SELECT * FROM dispatch_jobs WHERE id=? AND status='UNKNOWN'", (job_id,))
        if not job:
            raise DomainError("Задание уже разобрано")
        legacy=json.loads(job["payload"]).get("_legacy_operation_id")
        if legacy:
            return self.resolve_legacy_unknown(legacy,sent,api_message_id)
        if sent:
            job["payload"] = json.loads(job["payload"])
            job["attempt_id"] = self.db.scalar("SELECT max(id) FROM message_attempts WHERE job_id=?", (job_id,))
            self.finish_job(job, Result(api_message_id, {"manually_reconciled": True}))
        else:
            # Explicit manual resolution, never a blind retry of a potentially sent request.
            with self.db.tx() as c:
                c.execute("UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=? AND status='UNKNOWN'", (job_id,))
                self.audit(c, "UNKNOWN_NOT_SENT", campaign_id=job["campaign_id"], job_id=job_id)

    async def run_one(self, transport, owner="worker", emergency_sink=None):
        # Retain the claim result if shutdown arrives while its transaction commits.
        claim = asyncio.create_task(self.db.run(self.claim_job, owner))
        try:
            job = await asyncio.shield(claim)
        except asyncio.CancelledError:
            from app.transport.base import OperationError

            job = await claim
            if job:
                await self.db.run(self.fail_job, job, OperationError("cancelled", "CANCELLED_BEFORE_REQUEST"))
            raise
        if not job:
            return False
        try:
            if not await self.db.run(self.execution_allowed, job):
                from app.transport.base import OperationError

                raise OperationError("cancelled", "STATE_CHANGED_BEFORE_REQUEST")
            result = await asyncio.wait_for(transport.execute(job), 120)
        except asyncio.CancelledError:
            from app.transport.base import OperationError

            await self.db.run(self.fail_job, job, OperationError("unknown", "WORKER_CANCELLED"))
            raise
        except Exception as exc:
            try:
                await self.db.run(self.fail_job, job, exc)
            except Exception as persistence_error:
                await self._persistence_failure(
                    job, persistence_error, getattr(exc, "code", type(exc).__name__), emergency_sink
                )
                raise
        else:
            # A lost COMMIT acknowledgement is never an excuse to re-send.
            try:
                await self.db.run(self.finish_job, job, result)
            except Exception as persistence_error:
                await self._persistence_failure(
                    job, persistence_error, "ACKNOWLEDGED_BUT_NOT_COMMITTED", emergency_sink
                )
                raise
        return True

    async def _persistence_failure(self, job, error, reason, sink):
        payload = {
            "job_id": job["id"],
            "account_id": job["account_id"],
            "chat_pool_id": job["chat_pool_id"],
            "campaign_id": job["campaign_id"],
            "reason": reason,
        }
        await self.db.run(self.diagnostic, "result-persistence", error, **payload)
        # An unavailable SQLite outbox must not hide an already detected critical
        # condition. This best-effort lane performs no account actions or DB reads.
        if sink and hasattr(sink, "emergency"):
            for admin in self.config.admin_ids:
                try:
                    await asyncio.wait_for(sink.emergency(admin, payload), 10)
                except Exception as delivery_error:
                    await self.db.run(self.diagnostic, "emergency-notification", delivery_error, **payload)
