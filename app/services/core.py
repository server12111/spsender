import json
import logging
import re
import time
from zoneinfo import ZoneInfo


def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


RUNTIME_LIMITS = {
    "batch_size": (int, 1, 500),
    "concurrency": (int, 1, 32),
    "batch_interval": (float, 0, 60),
    "planning_batch_size": (int, 1, 500),
    "max_pending_jobs": (int, 1, 100000),
    "max_retries": (int, 0, 10),
    "retry_base": (float, 0.1, 3600),
    "retry_max": (float, 0.1, 86400),
}


def redact(value):
    if isinstance(value, dict):
        return {
            key: (
                "[СКРЫТО]"
                if key.lower() == "code"
                or any(
                    s in key.lower()
                    for s in ("token", "password", "session", "api_hash", "phone", "secret", "auth_code")
                )
                else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r"\b\d{5,12}:[A-Za-z0-9_-]{15,}\b", "[TOKEN]", value)
        value = re.sub(r"\+\d{8,15}\b", "[PHONE]", value)
        return value[:2000]
    return value


def row(c, sql, args=()):
    result = c.execute(sql, args).fetchone()
    return dict(result) if result else None


def rows(c, sql, args=()):
    return [dict(r) for r in c.execute(sql, args).fetchall()]


class DomainError(ValueError):
    """A safe, Russian-language error which can be shown to the administrator."""


class Core:
    def __init__(self, db, config, secrets, clock=time.time):
        self.db, self.config, self.secrets, self.clock = db, config, secrets, clock
        with db.tx() as c:
            mode = row(c, "SELECT value FROM settings WHERE key='database_mode'")
            if mode and mode["value"] != config.mode and c.execute("SELECT count(*) FROM accounts").fetchone()[0]:
                raise DomainError(
                    "Для другого режима используйте отдельный DATABASE_PATH: тестовые аккаунты не переносятся в live"
                )
            c.execute(
                "INSERT INTO settings VALUES ('database_mode',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (config.mode,),
            )
            for admin in config.admin_ids:
                c.execute("INSERT OR IGNORE INTO admins VALUES (?)", (admin,))
            c.execute("INSERT OR IGNORE INTO settings VALUES ('timezone',?)", (config.timezone,))
            c.execute("INSERT OR IGNORE INTO settings VALUES ('join_pause',?)", (str(config.join_pause),))
            for key in RUNTIME_LIMITS:
                c.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (key, str(getattr(config, key))))

    def authorize(self, admin_id):
        if admin_id not in self.config.admin_ids:
            raise PermissionError("Доступ закрыт")

    def audit(self, c, event, campaign_id=None, pair_id=None, account_id=None, chat_pool_id=None, **detail):
        c.execute(
            "INSERT INTO audit_events(event,campaign_id,pair_id,account_id,chat_pool_id,detail,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (event, campaign_id, pair_id, account_id, chat_pool_id, dump(redact(detail)), self.clock()),
        )
        if event in {"JOB_ERROR", "JOB_UNKNOWN", "ACCOUNT_CRITICAL", "RETRY_EXHAUSTED", "NOTIFICATION_FAILED"}:
            logging.getLogger("fe_autosender").warning(
                "%s campaign=%s pair=%s account=%s chat=%s context=%s",
                event,
                campaign_id,
                pair_id,
                account_id,
                chat_pool_id,
                dump(redact(detail)),
            )

    def incoming_overload(self, count):
        with self.db.tx() as c:
            self.audit(c, "INCOMING_OVERLOAD", dropped=count)
            self._notify(
                c, f"incoming-overload:{int(self.clock() // 60)}", "overload", {"count": count, "at": self.clock()}, 90
            )

    def diagnostic(self, component, error, **detail):
        code = type(error).__name__ if isinstance(error, BaseException) else str(error)
        # Never format exception messages: RPC/OS exceptions can contain secrets.
        logging.getLogger("fe_autosender").warning("%s error=%s context=%s", component, code, dump(redact(detail)))
        try:
            with self.db.tx() as c:
                c.execute(
                    "INSERT INTO service_diagnostics(component,error_code,created_at,detail) VALUES (?,?,?,?)",
                    (component, code, self.clock(), dump(redact(detail))),
                )
        except Exception:
            pass  # File logger remains usable when SQLite itself is unavailable.

    def runtime_options(self):
        result = {key: getattr(self.config, key) for key in RUNTIME_LIMITS}
        for item in self.db.all(
            "SELECT key,value FROM settings WHERE key IN (" + ",".join("?" for _ in result) + ")", tuple(result)
        ):
            kind, lower, upper = RUNTIME_LIMITS[item["key"]]
            try:
                value = kind(item["value"])
                if lower <= value <= upper:
                    result[item["key"]] = value
            except (ValueError, OverflowError):
                continue
        return result

    def maintenance(self):
        now = self.clock()
        with self.db.tx() as c:
            last = row(c, "SELECT value FROM settings WHERE key='last_maintenance'")
            if last and now - float(last["value"]) < 3600:
                return
            # Operational history is retained; only expired ephemeral UI data is removed.
            c.execute(
                "DELETE FROM ui_actions WHERE token IN (SELECT token FROM ui_actions WHERE expires_at<? LIMIT 2000)",
                (now,),
            )
            c.execute("DELETE FROM ui_states WHERE expires_at<?", (now,))
            c.execute(
                "DELETE FROM admin_update_receipts WHERE rowid IN (SELECT rowid FROM admin_update_receipts WHERE created_at<? LIMIT 2000)",
                (now - 30 * 86400,),
            )
            c.execute(
                "INSERT INTO settings VALUES ('last_maintenance',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(now),),
            )

    def get_setting(self, key, default=None):
        result = self.db.one("SELECT value FROM settings WHERE key=?", (key,))
        return result["value"] if result else default

    def set_setting(self, key, value):
        if key == "timezone":
            ZoneInfo(value)
        elif key == "join_pause":
            if not 50 <= int(value) <= 100:
                raise DomainError("Пауза должна быть от 50 до 100 секунд")
        elif key in RUNTIME_LIMITS:
            kind, lower, upper = RUNTIME_LIMITS[key]
            value = kind(value)
            if not lower <= value <= upper:
                raise DomainError(f"Допустимый диапазон: {lower}–{upper}")
        else:
            raise DomainError("Неизвестная настройка")
        with self.db.tx() as c:
            c.execute(
                "INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            if key == "timezone":
                c.execute("UPDATE campaigns SET timezone=?", (value,))
            self.audit(c, "SETTING_CHANGED", key=key, value=value)

    def campaign(self, c, campaign_id):
        result = row(c, "SELECT * FROM campaigns WHERE id=? AND status<>'REMOVED'", (campaign_id,))
        if not result:
            raise DomainError("Рассылка не найдена")
        return result

    def templates(self, campaign_id, kind="mailing", version=None, c=None):
        sql = "SELECT * FROM campaign_templates WHERE campaign_id=? AND kind=? AND version=? ORDER BY position"
        if version is None:
            record = self.db.one("SELECT version FROM campaigns WHERE id=?", (campaign_id,))
            if not record:
                return []
            version = record["version"]
        return rows(c, sql, (campaign_id, kind, version)) if c else self.db.all(sql, (campaign_id, kind, version))
