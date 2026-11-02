import logging
import os
import re
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from cryptography.fernet import Fernet


class SecretBox:
    def __init__(self, key: str):
        self.fernet = Fernet(key.encode())

    def encrypt(self, value: str) -> str:
        return self.fernet.encrypt(value.encode()).decode()

    def decrypt(self, value: str) -> str:
        return self.fernet.decrypt(value.encode()).decode()


def mask_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    return "+" + "*" * max(0, len(digits) - 4) + digits[-4:]


def configure_logging():
    # Library RPC errors may contain peer data. The application records only error codes.
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    logging.getLogger("aiogram.event").setLevel(logging.CRITICAL)
    folder = Path("data/logs")
    folder.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        os.chmod(folder, 0o700)
    logger = logging.getLogger("fe_autosender")
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        handler = RotatingFileHandler(
            folder / "diagnostics.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        formatter = logging.Formatter("%(asctime)s UTC %(levelname)s %(message)s")
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        logger.addHandler(handler)
