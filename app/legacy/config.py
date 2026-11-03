"""Only configuration required by the original operational services."""

from .clock import engine_context


class LegacyConfig:
    PEER_FLOOD_COOLDOWN_SECONDS = 86400
    DEBUG_PHONE = ""
    MAILING_DEBUG = False
    MIN_SAFE_SECONDS_PER_TARGET = 3

    def __getattr__(self, name):
        engine = engine_context.get()
        values = {"BOT_TOKEN": "bot_token", "API_ID": "api_id", "API_HASH": "api_hash", "ADMIN_IDS": "admin_ids"}
        if engine is not None and name in values:
            return getattr(engine.config, values[name])
        raise AttributeError(name)


config = LegacyConfig()
