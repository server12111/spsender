"""Keep the original clock API while using the application's chosen timezone."""

from contextvars import ContextVar
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

engine_context = ContextVar("legacy_engine", default=None)


def now_moscow():
    engine = engine_context.get()
    if engine is None:
        raise RuntimeError("Legacy runtime has no engine context")
    local = datetime.fromtimestamp(engine.clock(), ZoneInfo(engine.get_setting("timezone")))
    return local.replace(tzinfo=timezone(local.utcoffset()))
