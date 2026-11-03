import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from app.paths import default_database_path


@dataclass(frozen=True)
class Config:
    database_path: str = "data/fe_autosender.sqlite3"
    mode: str = "dry-run"
    bot_token: str = field(default="", repr=False)
    admin_ids: tuple[int, ...] = ()
    timezone: str = "Europe/Kyiv"
    session_key: str = field(default="", repr=False)
    api_id: int = 0
    api_hash: str = field(default="", repr=False)
    worker_interval: float = 2
    join_pause: int = 75
    batch_size: int = 32
    concurrency: int = 4
    batch_interval: float = 0.25
    planning_batch_size: int = 64
    max_pending_jobs: int = 10000
    max_retries: int = 3
    retry_base: float = 5
    retry_max: float = 300
    sqlite_busy_ms: int = 250
    connection_batch_size: int = 8
    incoming_queue_size: int = 1000
    entity_cache_limit: int = 1000

    def __post_init__(self):
        if any(not isinstance(admin, int) or isinstance(admin, bool) or admin <= 0 for admin in self.admin_ids):
            raise ValueError("ADMIN_IDS должен содержать положительные числовые Telegram ID")
        limits = {
            "worker_interval": (0.05, 60),
            "batch_size": (1, 500),
            "concurrency": (1, 32),
            "batch_interval": (0, 60),
            "planning_batch_size": (1, 500),
            "max_pending_jobs": (1, 100000),
            "max_retries": (0, 10),
            "retry_base": (0.1, 3600),
            "retry_max": (0.1, 86400),
            "sqlite_busy_ms": (10, 2000),
            "connection_batch_size": (1, 64),
            "incoming_queue_size": (1, 100000),
            "entity_cache_limit": (100, 10000),
            "join_pause": (50, 100),
        }
        for key, (lower, upper) in limits.items():
            value = getattr(self, key)
            if not math.isfinite(value) or not lower <= value <= upper:
                raise ValueError(f"Недопустимое значение {key}: диапазон {lower}–{upper}")
        if self.mode not in {"dry-run", "live"}:
            raise ValueError("MODE должен быть dry-run или live")
        ZoneInfo(self.timezone)

    @classmethod
    def load(cls):
        load_dotenv()
        config = cls(
            database_path=os.getenv("DATABASE_PATH") or default_database_path(),
            mode=os.getenv("MODE", "dry-run"),
            bot_token=os.getenv("BOT_TOKEN", ""),
            admin_ids=tuple(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()),
            timezone=os.getenv("TIMEZONE", "Europe/Kyiv"),
            session_key=os.getenv("SESSION_KEY", ""),
            api_id=int(os.getenv("TELEGRAM_API_ID") or 0),
            api_hash=os.getenv("TELEGRAM_API_HASH", ""),
            worker_interval=float(os.getenv("WORKER_INTERVAL", "2")),
            join_pause=int(os.getenv("JOIN_PAUSE_SECONDS", "75")),
            **{
                key: int(os.getenv(key.upper(), str(default)))
                for key, default in {
                    "batch_size": 32,
                    "concurrency": 4,
                    "planning_batch_size": 64,
                    "max_pending_jobs": 10000,
                    "max_retries": 3,
                    "sqlite_busy_ms": 250,
                    "connection_batch_size": 8,
                    "incoming_queue_size": 1000,
                    "entity_cache_limit": 1000,
                }.items()
            },
            **{
                key: float(os.getenv(key.upper(), str(default)))
                for key, default in {"batch_interval": 0.25, "retry_base": 5, "retry_max": 300}.items()
            },
        )
        ZoneInfo(config.timezone)
        if config.mode not in {"dry-run", "live"}:
            raise ValueError("MODE должен быть dry-run или live")
        if not 50 <= config.join_pause <= 100:
            raise ValueError("JOIN_PAUSE_SECONDS: допустимо 50–100 секунд")
        return config

    def validate_bot(self):
        if not self.bot_token or not self.admin_ids or not self.session_key:
            raise ValueError("Задайте BOT_TOKEN, ADMIN_IDS и SESSION_KEY в .env")
        if self.mode == "live" and (not self.api_id or not self.api_hash):
            raise ValueError("Для live нужны TELEGRAM_API_ID и TELEGRAM_API_HASH")
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
