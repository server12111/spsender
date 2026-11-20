"""Owner supplied API identities, fixed for the lifetime of an account session."""

import re
from dataclasses import dataclass, field

from .core import DomainError, row


@dataclass(frozen=True)
class ApiIdentity:
    api_id: int
    api_hash: str = field(repr=False)
    credential_id: int | None = None


class ApiPool:
    @staticmethod
    def _validate_api_pair(api_id, api_hash):
        if (
            not isinstance(api_id, int)
            or isinstance(api_id, bool)
            or not 0 < api_id < 2**31
            or not isinstance(api_hash, str)
            or not re.fullmatch(r"[a-fA-F0-9]{32}", api_hash)
        ):
            raise DomainError(
                "API ID — положительное число до 2147483647; API hash — 32 hex-символа. Отправьте свою пару API."
            )

    def api_pool(self, page=0):
        return self.db.all(
            "SELECT k.id,k.name,k.api_id,k.enabled,count(a.account_id) AS accounts "
            "FROM api_credentials k LEFT JOIN account_api_bindings a ON a.credential_id=k.id "
            "GROUP BY k.id ORDER BY k.id LIMIT 9 OFFSET ?",
            (max(0, page) * 8,),
        )

    def add_api_credential(self, name, api_id, api_hash):
        self._validate_api_pair(api_id, api_hash)
        if not name.strip():
            raise DomainError("Укажите название API-пары")
        with self.db.tx() as c:
            return self._insert_api(c, name, api_id, api_hash)

    def _insert_api(self, c, name, api_id, api_hash):
        existing = row(c, "SELECT * FROM api_credentials WHERE api_id=?", (api_id,))
        if existing:
            if self.secrets.decrypt(existing["encrypted_hash"]) != api_hash:
                raise DomainError("Этот API ID уже сохранён с другим hash. Закреплённые реквизиты не заменяются.")
            return existing["id"]
        result = c.execute(
            "INSERT INTO api_credentials(name,api_id,encrypted_hash,created_at) VALUES (?,?,?,?)",
            (name.strip()[:100], api_id, self.secrets.encrypt(api_hash), self.clock()),
        ).lastrowid
        self.audit(c, "API_CREDENTIAL_ADDED", credential_id=result)
        return result

    def set_api_enabled(self, credential_id, enabled, expected=None):
        with self.db.tx() as c:
            current = row(c, "SELECT enabled FROM api_credentials WHERE id=?", (credential_id,))
            if not current:
                raise DomainError("API-пара не найдена")
            if expected is not None and bool(current["enabled"]) != bool(expected):
                raise DomainError("Настройка уже изменилась. Откройте пул заново")
            c.execute("UPDATE api_credentials SET enabled=? WHERE id=?", (bool(enabled), credential_id))
            self.audit(c, "API_CREDENTIAL_ENABLED", credential_id=credential_id, enabled=bool(enabled))

    def _initialize_api_pool(self, c):
        # On upgrade, snapshot the existing .env identity before considering
        # new pool entries. Never move an existing session to another API.
        try:
            self._validate_api_pair(self.config.api_id, self.config.api_hash)
        except DomainError:
            return
        credential_id = self._insert_api(c, "API из .env", self.config.api_id, self.config.api_hash)
        c.execute(
            "INSERT OR IGNORE INTO account_api_bindings(account_id,credential_id,created_at) "
            "SELECT id,?,? FROM accounts WHERE account_health<>'REMOVED'",
            (credential_id, self.clock()),
        )

    def _identity(self, c, credential_id):
        item = row(c, "SELECT * FROM api_credentials WHERE id=?", (credential_id,))
        if not item:
            raise DomainError("Закреплённая API-пара не найдена")
        return ApiIdentity(item["api_id"], self.secrets.decrypt(item["encrypted_hash"]), item["id"])

    def _bind_api(self, c, account_id, credential_id):
        if credential_id is None:
            return
        self._identity(c, credential_id)
        old = row(c, "SELECT credential_id FROM account_api_bindings WHERE account_id=?", (account_id,))
        if old and old["credential_id"] != credential_id:
            raise DomainError("API-пара уже закреплена за аккаунтом. Автоматическая замена запрещена.")
        c.execute(
            "INSERT OR IGNORE INTO account_api_bindings VALUES (?,?,?)", (account_id, credential_id, self.clock())
        )

    def account_api(self, account_id):
        with self.db.tx() as c:
            if not row(c, "SELECT id FROM accounts WHERE id=? AND account_health<>'REMOVED'", (account_id,)):
                raise DomainError("Аккаунт не найден")
            binding = row(c, "SELECT credential_id FROM account_api_bindings WHERE account_id=?", (account_id,))
            if binding:
                return self._identity(c, binding["credential_id"])
            try:
                self._validate_api_pair(self.config.api_id, self.config.api_hash)
            except DomainError:
                raise DomainError(
                    "У старой сессии не закреплено API. Восстановите её прежнюю пару в .env и перезапустите бота."
                ) from None
            credential_id = self._insert_api(c, "API из .env", self.config.api_id, self.config.api_hash)
            self._bind_api(c, account_id, credential_id)
            return self._identity(c, credential_id)

    def authorization_api(self, account_id=None):
        if account_id is not None:
            return self.account_api(account_id)
        with self.db.read() as c:
            selected = row(
                c,
                "SELECT k.id FROM api_credentials k LEFT JOIN account_api_bindings b ON b.credential_id=k.id "
                "WHERE k.enabled=1 GROUP BY k.id ORDER BY count(b.account_id),k.id LIMIT 1",
            )
            if selected:
                return self._identity(c, selected["id"])
            any_keys = row(c, "SELECT 1 FROM api_credentials LIMIT 1")
        if not any_keys:
            try:
                self._validate_api_pair(self.config.api_id, self.config.api_hash)
            except DomainError:
                raise DomainError(
                    "Добавьте API ID/hash: Настройки → Пул API ID/hash. Либо задайте свою пару API в .env."
                ) from None
            with self.db.tx() as c:
                credential_id = self._insert_api(c, "API из .env", self.config.api_id, self.config.api_hash)
                return self._identity(c, credential_id)
        raise DomainError("В пуле нет включённых API-пар для новых входов. Настройки → Пул API ID/hash.")
