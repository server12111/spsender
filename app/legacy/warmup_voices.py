"""Pre-generated voice notes for warmup chats (DMs and warmup groups).

Files live in data/warmup_voices/ with an index.json made by the offline
generator: each file has a persona (one of ~12 voices), a topic, a duration
and a Telegram waveform. Every account always speaks with the same persona,
sends at most _PER_DAY voice notes a day, and goes through all of its
persona's files on a topic before repeating one (then cycles from the oldest).
"""
import asyncio
import base64
import json
import logging
import os
import random
import time

from telethon.errors import FloodWaitError, PeerFloodError

logger = logging.getLogger(__name__)

VOICE_DIR = os.path.join("data", "warmup_voices")
_PER_DAY = 2

TOPIC_KEYWORDS = {
    "coffee": ["кофе", "чай"],
    "work": ["работ", "зарплат", "аванс", "заканчиваешь"],
    "busy": ["занят", "что делаешь", "чем занят", "отдыхаю"],
    "weekend": ["выходн"],
    "weather": ["погод", "дожд", "снег", "холод", "солнц"],
    "food": ["поел", "обед", "ужин", "готовишь", "готовил", "доставк", "кафе", "пицц"],
    "news": ["новост", "слышал", "что вчера было", "обсуждают"],
    "movies": ["фильм", "сериал", "кино", "посмотреть"],
    "music": ["музык", "трек", "плейлист", "наушник", "слушаешь"],
    "sleep": ["спать", "выспал", "встаёшь", "лёг"],
    "mood": ["настроен", "как дела", "как сам", "как ты вообще", "неделя", "как день"],
    "bored": ["скучн", "заняться"],
    "sport": ["спорт", "зал", "бегаешь", "футбол", "зарядк"],
    "travel": ["отпуск", "море", "поездк", "в город", "отдыхал"],
    "games": ["игр"],
    "books": ["книг", "читаешь"],
    "car": ["машин", "водишь", "пробк", "доехал"],
    "pets": ["питомц", "кот", "собак"],
    "home": ["дома?", "где сейчас", "соседи", "живёшь"],
    "shopping": ["купил", "телефон", "покупк", "приложен"],
    "study": ["учишься", "учёб", "курсы"],
    "health": ["здоров", "болеешь", "устал", "сил нет"],
    "family": ["родител", "дома все"],
    "holidays": ["праздник", "новый год"],
    "walk": ["погулять", "на улицу", "прогул"],
    "hobby": ["хобби", "увлекаешь", "рисуешь", "творчеств"],
    "evening": ["вечером", "на вечер", "завтра"],
}

# Short text answers to a voice note.
VOICE_REPLIES = ["ахах, понял", "ага, понял тебя", "ну да)", "хаха, жиза", "понятно)", "норм", "о, круто",
                 "ясно)", "ну ты даёшь)", "ага", "👍", "😂", "согласен", "понял, понял)", "хах, бывает"]

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS warmup_voice_log (
        account_id INTEGER NOT NULL,
        file TEXT NOT NULL,
        sent_ts REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_wvl_account ON warmup_voice_log(account_id, sent_ts)",
)


def topic_of(lines) -> str:
    """Topic of a dialog thread (list of variant lists or plain strings)."""
    words = []
    for x in lines:
        words.extend(x if isinstance(x, (list, tuple)) else [x])
    text = " ".join(words).lower()
    best, hits = "generic", 0
    for topic, keys in TOPIC_KEYWORDS.items():
        n = sum(1 for k in keys if k in text)
        if n > hits:
            best, hits = topic, n
    return best


class VoiceNote:
    """A slot in a warmup chat script: the speaker sends a voice note on `topic`."""
    __slots__ = ("topic",)

    def __init__(self, topic: str):
        self.topic = topic


class VoiceReply:
    """A slot right after a VoiceNote: a short text answer, only if the voice went out."""
    __slots__ = ()


class WarmupVoices:
    def __init__(self, db):
        self.db = db
        self._index = None
        self._loaded_at = 0.0
        self._ready = False

    def _load(self):
        # Reload at most every 10 minutes so new files are picked up without a restart.
        if self._index is not None and time.time() - self._loaded_at < 600:
            return self._index
        self._loaded_at = time.time()
        path = os.path.join(VOICE_DIR, "index.json")
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            by = {}
            for item in data.get("files", []):
                if os.path.exists(os.path.join(VOICE_DIR, item["file"])):
                    by.setdefault((item["p"], item["topic"]), []).append(item)
            self._index = {"personas": data.get("personas", []), "by": by}
        except FileNotFoundError:
            self._index = {"personas": [], "by": {}}
        except Exception as e:
            logger.warning(f"Warmup voices: index load failed: {e}")
            self._index = {"personas": [], "by": {}}
        return self._index

    async def _ensure(self):
        if self._ready:
            return
        if not self.db.is_pg:       # PostgreSQL: the tables come with pg_schema.sql
            async with self.db._transaction_lock:
                for stmt in _SCHEMA:
                    await self.db._conn.execute(stmt)
                await self.db._conn.commit()
        self._ready = True

    def available(self) -> bool:
        return bool(self._load()["personas"])

    async def can_send(self, account_id: int) -> bool:
        if not self.available():
            return False
        await self._ensure()
        async with self.db._conn.execute(
                "SELECT COUNT(*) FROM warmup_voice_log WHERE account_id = ? AND sent_ts > ?",
                (account_id, time.time() - 86400)) as cur:
            return (await cur.fetchone())[0] < _PER_DAY

    async def _pick(self, account_id: int, topic: str):
        idx = self._load()
        personas = idx["personas"]
        if not personas:
            return None
        persona = account_id % len(personas)
        cands = idx["by"].get((persona, topic)) or idx["by"].get((persona, "generic")) or []
        if not cands:
            return None
        async with self.db._conn.execute(
                "SELECT file, MAX(sent_ts) FROM warmup_voice_log WHERE account_id = ? GROUP BY file",
                (account_id,)) as cur:
            last = {r[0]: r[1] for r in await cur.fetchall()}
        fresh = [c for c in cands if c["file"] not in last]
        if fresh:
            return random.choice(fresh)
        return min(cands, key=lambda c: last.get(c["file"], 0))  # all used — cycle from the oldest

    async def send(self, client, peer, account_id: int, topic: str, reply_to=None):
        """Send a voice note on `topic` (with "recording" status first).
        Returns the sent message or None when skipped (daily cap, no files)."""
        from telethon.tl.functions.messages import SetTypingRequest
        from telethon.tl.types import DocumentAttributeAudio, SendMessageRecordAudioAction
        try:
            if not await self.can_send(account_id):
                return None
            item = await self._pick(account_id, topic)
            if not item:
                return None
            path = os.path.join(VOICE_DIR, item["file"])
            dur = int(item.get("dur") or 3)
            # "Recording a voice message..." for about as long as the note is.
            left = dur + random.uniform(0.5, 2)
            while left > 0:
                try:
                    await client(SetTypingRequest(peer, SendMessageRecordAudioAction()))
                except (FloodWaitError, PeerFloodError):
                    raise
                except Exception:
                    break
                step = min(left, 4.5)
                await asyncio.sleep(step)
                left -= step
            wave = base64.b64decode(item["wave"]) if item.get("wave") else None
            msg = await client.send_file(
                peer, path, voice_note=True, reply_to=reply_to,
                attributes=[DocumentAttributeAudio(duration=dur, voice=True, waveform=wave)],
            )
            async with self.db._transaction_lock:
                await self.db._conn.execute(
                    "INSERT INTO warmup_voice_log (account_id, file, sent_ts) VALUES (?, ?, ?)",
                    (account_id, item["file"], time.time()))
                await self.db._conn.commit()
            logger.info(f"Warmup voices: account {account_id} sent {item['file']} ({topic})")
            return msg
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            logger.warning(f"Warmup voices: send failed for account {account_id}: {type(e).__name__}: {e}")
            return None
