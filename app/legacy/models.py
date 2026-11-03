import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Mailing:
    id: int

    user_id: int

    account_id: int

    name: str

    is_active: bool

    interval_seconds: int

    active_hours_json: Optional[str]

    last_sent_at: Optional[datetime]

    created_at: datetime

    reply_mode: Optional[str] = None

    reply_offset: int = 1

    reply_random_min: int = 1

    reply_random_max: int = 5

    keep_targets_on_ban: bool = False

    account_rotation_mode: str = "per_target"

    batch_size: Optional[int] = None

    batch_pause: int = 10

    hidden_tag_enabled: bool = False

    # don't write into a chat while our previous message there is still the last one

    skip_unseen: bool = True

    chat_delay_seconds: int = 3


@dataclass
class MailingMessage:
    id: int

    mailing_id: int

    text: str

    photo_path: Optional[str] = None

    video_path: Optional[str] = None

    document_path: Optional[str] = None  # a file (PDF, price list...) sent as a document

    parse_mode: str = "html"

    entities_json: Optional[str] = None

    forward_peer: Optional[str] = None

    forward_msg_id: Optional[int] = None

    forward_src_chat: Optional[int] = None

    forward_src_msg: Optional[int] = None

    forward_relay_account: Optional[int] = None

    buttons_json: Optional[str] = None

    media_file_id: Optional[str] = None

    @property
    def buttons(self) -> list:

        from .utils.msg_buttons import parse_buttons

        return parse_buttons(self.buttons_json)

    @property
    def is_relayed_forward(self) -> bool:

        return bool(self.forward_src_chat and self.forward_src_msg)

    @property
    def photo_paths(self) -> list[str]:

        if not self.photo_path:
            return []

        try:
            paths = json.loads(self.photo_path)

            if isinstance(paths, list):
                return paths

        except (json.JSONDecodeError, TypeError):
            pass

        return [self.photo_path]

    @property
    def is_forward(self) -> bool:

        return bool(self.forward_peer and self.forward_msg_id)

    @property
    def is_dice(self) -> bool:

        return self.parse_mode == "dice"


@dataclass
class MailingTarget:
    id: int

    mailing_id: int

    chat_identifier: str

    interval_seconds: Optional[int] = None

    last_sent_at: Optional[datetime] = None

    thread_id: Optional[int] = None

    is_forum: bool = False

    last_account_id: Optional[int] = None

    media_disabled: bool = False

    topic_ids_json: Optional[str] = None  # JSON array of topic ids for round-robin sending

    msg_index: int = 0  # which of the mailing's texts this chat gets next (1, 2, 3, 1, ... per chat)
    delivery_seq: int = 0  # Durable intent identity, independent of the cyclic text index.

    @property
    def topic_ids(self) -> list[int]:

        try:
            return json.loads(self.topic_ids_json) or []

        except (TypeError, ValueError):
            return []
