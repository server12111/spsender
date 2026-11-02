from dataclasses import dataclass, field


@dataclass
class Result:
    message_id: int | None = None
    data: dict = field(default_factory=dict)


class OperationError(Exception):
    def __init__(self, category, code, seconds=0):
        self.category, self.code, self.seconds = category, code, seconds
        super().__init__(code)


class FakeTransport:
    """No sockets, no accounts, no external side effects. Injectable results/errors."""

    def __init__(self):
        self.calls = []
        self.errors = {}

    async def execute(self, job):
        self.calls.append(job)
        error = self.errors.get((job["kind"], job["account_id"], job["chat_pool_id"]))
        if error:
            raise error
        return Result(len(self.calls) if job["kind"] not in {"CHECK", "JOIN"} else None, {"dry_run": True})

    async def close(self):
        return None

    async def resolve_target(self, value):
        import hashlib

        from app.services.chat_pool import parse_target

        chat_id, topic, link = parse_target(value)
        if chat_id is None:
            chat_id = -int(hashlib.sha256(link.encode()).hexdigest()[:12], 16)
        return chat_id, topic, link, value.strip()


class FakeNotificationSink:
    def __init__(self):
        self.messages = []
        self.fail = False

    async def send(self, admin_id, kind, payload):
        if self.fail:
            raise ConnectionError("offline")
        self.messages.append((admin_id, kind, payload))

    async def emergency(self, admin_id, payload):
        await self.send(admin_id, "emergency", payload)
