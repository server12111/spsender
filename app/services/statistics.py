from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

SEND_KINDS = "('MAILING','GREETING','GROUP_REPLY','DM_FIRST','DM_ALWAYS','DM_MANUAL')"


def period_bounds(period, now, tz):
    local = datetime.fromtimestamp(now, timezone.utc).astimezone(ZoneInfo(tz))
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "today":
        start, end = midnight, local + timedelta(seconds=1)
    elif period == "yesterday":
        start, end = midnight - timedelta(days=1), midnight
    elif period in {"7", "30"}:
        start, end = midnight - timedelta(days=int(period) - 1), local + timedelta(seconds=1)
    elif period == "all":
        return 0, now + 1
    else:
        raise ValueError("Неизвестный период")
    return start.timestamp(), end.timestamp()


class Statistics:
    def account_overview(self):
        """Current account counters without reading delivery or attempt history."""
        with self.db.read() as c:
            records = c.execute(
                "SELECT account_health AS health,onboarding_stage AS stage,paused,count(*) AS value "
                "FROM accounts WHERE account_health<>'REMOVED' GROUP BY account_health,onboarding_stage,paused"
            ).fetchall()
        result = {"total": 0, "ready": 0, "preparing": 0, "paused": 0, "health": {}, "stages": {}}
        for record in records:
            value, health, stage = record["value"], record["health"], record["stage"]
            result["total"] += value
            result["health"][health] = result["health"].get(health, 0) + value
            result["stages"][stage] = result["stages"].get(stage, 0) + value
            result["preparing"] += value if stage != "READY" else 0
            result["ready"] += value if stage == "READY" and health == "OK" and not record["paused"] else 0
            result["paused"] += value if record["paused"] else 0
        return result

    def statistics(self, period="today", entity=None, entity_id=None):
        from .core import row, rows
        from .pairs import active_count

        tz = self.get_setting("timezone", self.config.timezone)
        start, end = period_bounds(period, self.clock(), tz)
        columns = {"accounts": "account_id", "pairs": "pair_id", "chats": "chat_pool_id", "campaigns": "campaign_id"}
        condition, args = "", [start, end]
        if entity in columns and entity_id is not None:
            condition = " AND j." + columns[entity] + "=?"
            args.append(entity_id)
        result_where = (
            f" FROM message_results r JOIN dispatch_jobs j ON j.id=r.job_id WHERE r.confirmed_at>=? AND r.confirmed_at<? AND j.kind IN {SEND_KINDS}"
            + condition
        )
        attempt_where = (
            f" FROM message_attempts a JOIN dispatch_jobs j ON j.id=a.job_id WHERE a.started_at>=? AND a.started_at<? AND j.kind IN {SEND_KINDS}"
            + condition
        )
        # Snapshot reads do not acquire the SQLite writer lock. History stays in SQL.
        with self.db.read() as c:
            summary = row(c, "SELECT count(*) AS confirmed, max(r.confirmed_at) AS last_activity" + result_where, args)
            attempts = row(
                c,
                "SELECT count(*) AS attempts, COALESCE(sum(a.outcome='FAILED'),0) AS errors, COALESCE(sum(a.outcome='UNKNOWN'),0) AS unknown"
                + attempt_where,
                args,
            )
            zone = ZoneInfo(tz)
            c.create_function(
                "fe_local_day",
                1,
                lambda value: datetime.fromtimestamp(value, timezone.utc).astimezone(zone).strftime("%Y-%m-%d"),
            )
            days = {
                r["day"]: r["value"]
                for r in rows(
                    c,
                    "SELECT fe_local_day(r.confirmed_at) AS day,count(*) AS value"
                    + result_where
                    + " GROUP BY day ORDER BY day",
                    args,
                )
            }
            by = {}
            for key, column in columns.items():
                by[column] = {
                    r["entity"]: r["value"]
                    for r in rows(
                        c,
                        "SELECT j." + column + " AS entity,count(*) AS value" + result_where + " GROUP BY j." + column,
                        args,
                    )
                }
            scopes = {
                "accounts": " WHERE account_health<>'REMOVED'",
                "pairs": " WHERE account_a_id IS NOT NULL",
                "campaigns": " WHERE status<>'REMOVED'",
                "chat_pool": "",
            }
            totals, statuses = {}, {}
            for table, field in (
                ("accounts", "account_health"),
                ("pairs", "status"),
                ("chat_pool", "status"),
                ("campaigns", "status"),
            ):
                values = rows(
                    c, f"SELECT {field} AS status,count(*) AS value FROM {table}" + scopes[table] + f" GROUP BY {field}"
                )
                statuses[table] = {r["status"]: r["value"] for r in values}
                totals[table] = sum(r["value"] for r in values)
            account_summary = row(
                c,
                "SELECT COALESCE(sum(onboarding_stage<>'READY' AND account_health<>'REMOVED'),0) AS preparing, COALESCE(sum(onboarding_stage='READY' AND account_health='OK' AND paused=0),0) AS ready, COALESCE(sum(paused=1 AND account_health<>'REMOVED'),0) AS paused_accounts FROM accounts",
            )
            pairs = rows(
                c,
                "SELECT p.*,ca.status AS campaign_status,count(pc.id) AS chat_count, a.onboarding_stage AS a_stage,b.onboarding_stage AS b_stage,a.account_health AS a_health,b.account_health AS b_health,a.paused AS a_paused,b.paused AS b_paused,a.flood_until AS a_flood,b.flood_until AS b_flood, EXISTS(SELECT 1 FROM dispatch_jobs j WHERE j.pair_id=p.id AND j.status='UNKNOWN') AS uncertain FROM pairs p JOIN campaigns ca ON ca.id=p.campaign_id LEFT JOIN pair_chats pc ON pc.pair_id=p.id AND pc.removed_at IS NULL LEFT JOIN accounts a ON a.id=p.account_a_id LEFT JOIN accounts b ON b.id=p.account_b_id WHERE ca.status<>'REMOVED' AND p.account_a_id IS NOT NULL GROUP BY p.id",
            )
            now = self.clock()
            on_shift = sum(
                p["campaign_status"] == "RUNNING"
                and p["chat_count"] > 0
                and p["a_stage"] == p["b_stage"] == "READY"
                and p["a_health"] == p["b_health"] == "OK"
                and not p["a_paused"]
                and not p["b_paused"]
                and p["a_flood"] <= now
                and p["b_flood"] <= now
                and not p["uncertain"]
                for p in pairs
            )
            return {
                **summary,
                **attempts,
                **account_summary,
                "totals": totals,
                "statuses": statuses,
                "days": days,
                "on_shift": on_shift,
                "active_chats": sum(active_count(p["chat_count"], self.elapsed(p)) for p in pairs),
                "assigned_chats": c.execute("SELECT count(*) FROM pair_chats WHERE removed_at IS NULL").fetchone()[0],
                "pending_issues": c.execute("SELECT count(*) FROM chat_issues WHERE status='OPEN'").fetchone()[0],
                "last_events": rows(
                    c,
                    "SELECT * FROM audit_events WHERE created_at>=? AND created_at<? ORDER BY id DESC LIMIT 10",
                    (start, end),
                ),
                "by_account": by["account_id"],
                "by_pair": by["pair_id"],
                "by_chat": by["chat_pool_id"],
                "by_campaign": by["campaign_id"],
            }

    def period_metrics(self):
        result = {}
        for period in ("today", "yesterday", "7", "30"):
            start, end = period_bounds(period, self.clock(), self.get_setting("timezone", self.config.timezone))
            confirmed = self.db.scalar(
                f"SELECT count(*) FROM message_results r JOIN dispatch_jobs j ON j.id=r.job_id WHERE r.confirmed_at>=? AND r.confirmed_at<? AND j.kind IN {SEND_KINDS}",
                (start, end),
            )
            errors = self.db.scalar(
                f"SELECT count(*) FROM message_attempts a JOIN dispatch_jobs j ON j.id=a.job_id WHERE a.started_at>=? AND a.started_at<? AND a.outcome='FAILED' AND j.kind IN {SEND_KINDS}",
                (start, end),
            )
            result[period] = {"confirmed": confirmed, "errors": errors}
        return result

    def error_history(self, period="all", entity=None, entity_id=None, page=0):
        start, end = period_bounds(period, self.clock(), self.get_setting("timezone", self.config.timezone))
        clause, args = "", [start, end]
        mapping = {"accounts": "account_id", "pairs": "pair_id", "chats": "chat_pool_id", "campaigns": "campaign_id"}
        if entity in mapping and entity_id is not None:
            clause = " AND " + mapping[entity] + "=?"
            args.append(entity_id)
        return self.db.all(
            "SELECT * FROM audit_events WHERE created_at>=? AND created_at<? AND "
            "event IN ('JOB_ERROR','CHAT_ERROR','ACCOUNT_CRITICAL','JOB_UNKNOWN','NOTIFICATION_FAILED')"
            + clause
            + " ORDER BY id DESC LIMIT 9 OFFSET ?",
            (*args, page * 8),
        )
