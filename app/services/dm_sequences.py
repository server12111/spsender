import json
import random

from .core import dump, row, rows
from .pairs import shift_role


class DMSequences:
    def _create_dm_sequence(self, c, pair, campaign, account_id, peer_id, message_id, steps):
        sequence_id = c.execute(
            "INSERT INTO dm_reply_sequences(campaign_id,pair_id,account_id,peer_id,reply_to,version,steps,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                campaign["id"],
                pair["id"],
                account_id,
                peer_id,
                message_id,
                campaign["version"],
                dump(steps),
                self.clock(),
            ),
        ).lastrowid
        sequence = row(c, "SELECT * FROM dm_reply_sequences WHERE id=?", (sequence_id,))
        job_id = self._queue_dm_step(c, sequence, pair, campaign)
        if job_id is None:
            c.execute("DELETE FROM dm_reply_sequences WHERE id=?", (sequence_id,))
        return job_id

    def _queue_dm_step(self, c, sequence, pair, campaign):
        if campaign["status"] != "RUNNING" or not self._pair_ready(c, pair):
            return None
        active = pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
        if active != sequence["account_id"]:
            return None
        steps = json.loads(sequence["steps"])
        index = sequence["step_index"]
        if sequence["version"] != campaign["version"]:
            templates = {
                kind: self.templates(campaign["id"], kind, campaign["version"], c) for kind in ("dm_first", "dm_always")
            }
            for step in steps[index:]:
                kind = step["history_kind"]
                enabled = campaign["dm_first_enabled" if kind == "dm_first" else "dm_always_enabled"]
                step["paused"] = not enabled or not templates[kind]
                if not step["paused"] and step["role"] == "text":
                    selected = templates[kind][step["template_position"] % len(templates[kind])]
                    step["body"] = {key: selected[key] for key in ("text", "media_path", "media_kind")}
                elif step["role"] == "phrase":
                    step["body"]["text"] = campaign["dm_greeting_phrase"]
                elif step["role"] == "sticker":
                    step["body"]["sticker_emoji"] = campaign["dm_sticker_emoji"]
            c.execute(
                "UPDATE dm_reply_sequences SET steps=?,version=? WHERE id=?",
                (dump(steps), campaign["version"], sequence["id"]),
            )
        if index >= len(steps) or steps[index].get("paused"):
            return None
        step = steps[index]
        payload = {
            **step["body"],
            "peer_id": sequence["peer_id"],
            "reply_to": sequence["reply_to"],
            "topic_id": 0,
            "dm_sequence_id": sequence["id"],
            "dm_step": index,
            "history_kind": step["history_kind"],
        }
        job_id = self._enqueue(
            c,
            pair,
            campaign,
            "DM_FIRST" if step["history_kind"] == "dm_first" else "DM_ALWAYS",
            None,
            sequence["account_id"],
            payload,
            f"dm-sequence:{sequence['id']}:{index}",
            planned_at=max(self.clock(), sequence["next_step_at"]),
        )
        if job_id is not None:
            c.execute("UPDATE dispatch_jobs SET queue_rank=2 WHERE id=?", (job_id,))
            c.execute(
                "UPDATE dm_reply_sequences SET current_job_id=?,step_index=? WHERE id=?",
                (job_id, index, sequence["id"]),
            )
            for kind in {value["history_kind"] for value in steps[index:]}:
                c.execute(
                    "INSERT INTO reply_history(campaign_id,peer_id,kind,reserved_job_id) VALUES (?,?,?,?) "
                    "ON CONFLICT(campaign_id,peer_id,kind) DO UPDATE SET reserved_job_id=excluded.reserved_job_id",
                    (campaign["id"], sequence["peer_id"], kind, job_id),
                )
        return job_id

    def _finish_dm_step(self, c, job, now):
        payload = job["payload"]
        sequence = row(c, "SELECT * FROM dm_reply_sequences WHERE id=?", (payload["dm_sequence_id"],))
        if (
            not sequence
            or sequence["status"] != "ACTIVE"
            or sequence["step_index"] != payload["dm_step"]
            or sequence["current_job_id"] != job["id"]
        ):
            return
        steps = json.loads(sequence["steps"])
        step = steps[sequence["step_index"]]
        if step["history_complete"]:
            c.execute(
                "UPDATE reply_history SET last_replied_at=?,reserved_job_id=NULL WHERE campaign_id=? AND peer_id=? AND kind=?",
                (now, sequence["campaign_id"], sequence["peer_id"], step["history_kind"]),
            )
        index = sequence["step_index"] + 1
        next_at = now + random.uniform(3, 4) if index < len(steps) else 0
        c.execute(
            "UPDATE dm_reply_sequences SET step_index=?,next_step_at=?,current_job_id=NULL,status=? WHERE id=?",
            (index, next_at, "ACTIVE" if index < len(steps) else "COMPLETE", sequence["id"]),
        )
        if index < len(steps):
            pair = self._pair(c, sequence["pair_id"])
            campaign = row(c, "SELECT * FROM campaigns WHERE id=?", (sequence["campaign_id"],))
            if campaign and campaign["status"] == "RUNNING":
                sequence = row(c, "SELECT * FROM dm_reply_sequences WHERE id=?", (sequence["id"],))
                self._queue_dm_step(c, sequence, pair, campaign)

    def _plan_dm_sequences(self, c, pair, campaign):
        if not campaign["dm_first_enabled"] and not campaign["dm_always_enabled"]:
            return
        if campaign["status"] != "RUNNING" or not self._pair_ready(c, pair):
            return
        active = pair["account_a_id"] if shift_role(self.clock(), campaign["timezone"]) == "A" else pair["account_b_id"]
        candidates = rows(
            c,
            "SELECT s.* FROM dm_reply_sequences s LEFT JOIN dispatch_jobs j ON j.id=s.current_job_id "
            "WHERE s.pair_id=? AND s.account_id=? AND s.status='ACTIVE' AND (j.id IS NULL OR j.status='CANCELLED' "
            "OR (j.status='FAILED' AND j.version<>?)) "
            "AND ((json_extract(s.steps,'$['||s.step_index||'].history_kind')='dm_first' AND ?=1) "
            "OR (json_extract(s.steps,'$['||s.step_index||'].history_kind')='dm_always' AND ?=1)) "
            "AND EXISTS(SELECT 1 FROM campaign_templates t WHERE t.campaign_id=s.campaign_id AND t.version=? "
            "AND t.kind=json_extract(s.steps,'$['||s.step_index||'].history_kind')) ORDER BY s.id LIMIT 8",
            (
                pair["id"],
                active,
                campaign["version"],
                campaign["dm_first_enabled"],
                campaign["dm_always_enabled"],
                campaign["version"],
            ),
        )
        for sequence in candidates:
            self._queue_dm_step(c, sequence, pair, campaign)
