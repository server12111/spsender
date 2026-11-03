"""Photos in warmup chats: after a thread on a matching topic (coffee, food,
pets, the weather, a walk, the road, work, sport, travel) one side now and then
sends a photo with a short caption, the other answers. Files (free Wikimedia
Commons photos) live in data/warmup_photos/<topic>/ with an index.json; every
send is a slightly different copy (crop, tilt, brightness, size, no EXIF), at
most _PER_DAY a day per account, no repeats for an account until it has used
every photo of the topic."""
import asyncio
import io
import json
import logging
import os
import random
import time

from telethon.errors import FloodWaitError, PeerFloodError

logger = logging.getLogger(__name__)

PHOTO_DIR = os.path.join("data", "warmup_photos")
_PER_DAY = 1

# dialog thread topic (see warmup_voices.topic_of) -> photo topic
TOPIC_OF_THREAD = {
    "coffee": "coffee", "food": "food", "pets": "pets", "weather": "weather", "walk": "walk",
    "weekend": "walk", "car": "car", "work": "work", "busy": "work", "sport": "sport",
    "travel": "travel", "holidays": "travel",
}
CAPTIONS = {
    "coffee": ["вот мой кофе)", "кофейку", "утро начинается так", "сижу вот", ""],
    "food": ["мой ужин)", "вот что приготовил", "обед", "заказал вот", ""],
    "pets": ["смотри кто спит", "вот он)", "кот опять на диване", "мой засранец)", ""],
    "weather": ["вот такая погода", "смотри что за окном", "льёт весь день", "ну и погодка", ""],
    "walk": ["гуляю вот", "красиво сегодня", "вот где я", "осень)", ""],
    "car": ["еду вот", "в дороге", "за рулём, напишу позже", "пробки...", ""],
    "work": ["вот так и работаю", "моё рабочее место", "сижу работаю", "завал)", ""],
    "sport": ["в зале", "вот, занимаюсь", "тренировка)", "сегодня спорт", ""],
    "travel": ["вот где я был", "скучаю по морю", "летом было", "хочу обратно", ""],
}
REPLIES = {
    "coffee": ["красиво", "о, кайф", "тоже хочу", "вкусно выглядит"],
    "food": ["выглядит вкусно", "аппетитно", "о, тоже хочу", "голодный стал)"],
    "pets": ["милота", "ахах, классный", "какой милый", "😍"],
    "weather": ["жесть", "у нас так же", "бр, холодно", "красиво хоть"],
    "walk": ["красиво", "кайф", "завидую", "класс"],
    "car": ["аккуратнее там", "ок, давай", "понял)", "удачи на дороге"],
    "work": ["держись", "уютно", "понимаю", "ну ты работяга)"],
    "sport": ["красава", "молодец", "мотивируешь", "💪"],
    "travel": ["красота", "кайф", "завидую", "тоже хочу туда"],
}

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS warmup_photo_log (
        account_id INTEGER NOT NULL,
        file TEXT NOT NULL,
        sent_ts REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_wpl_account ON warmup_photo_log(account_id, sent_ts)",
)


class PhotoNote:
    """Script slot: send a photo on `topic` (a photo topic) with a caption."""
    __slots__ = ("topic",)

    def __init__(self, topic: str):
        self.topic = topic


class PhotoReply:
    """Script slot after a PhotoNote: the other side answers, only if the photo went out."""
    __slots__ = ()


def photo_topic(thread_topic: str):
    return TOPIC_OF_THREAD.get(thread_topic)


def _variant(path: str) -> bytes:
    """A slightly different copy of the photo each time, without metadata."""
    from PIL import Image, ImageEnhance
    img = Image.open(path).convert("RGB")
    w, h = img.size
    angle = random.uniform(-1.5, 1.5)
    if abs(angle) > 0.3:
        img = img.rotate(angle, resample=Image.BICUBIC, expand=False)
    k = random.uniform(0.88, 0.97)  # crop (also hides the rotation corners)
    cw, ch = int(w * k), int(h * k)
    x, y = random.randint(0, w - cw), random.randint(0, h - ch)
    img = img.crop((x, y, x + cw, y + ch))
    img = ImageEnhance.Brightness(img).enhance(random.uniform(0.94, 1.06))
    img = ImageEnhance.Contrast(img).enhance(random.uniform(0.95, 1.05))
    side = random.randint(960, 1280)
    img.thumbnail((side, side))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=random.randint(78, 90))
    return buf.getvalue()


class WarmupPhotos:
    def __init__(self, db):
        self.db = db
        self._index = None
        self._loaded_at = 0.0
        self._ready = False

    def _load(self) -> dict:
        if self._index is not None and time.time() - self._loaded_at < 600:
            return self._index
        self._loaded_at = time.time()
        by: dict = {}
        try:
            with open(os.path.join(PHOTO_DIR, "index.json"), encoding="utf-8") as f:
                for item in json.load(f).get("photos", []):
                    if os.path.exists(os.path.join(PHOTO_DIR, item["file"])):
                        by.setdefault(item["topic"], []).append(item["file"])
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Warmup photos: index load failed: {e}")
        self._index = by
        return by

    async def _ensure(self):
        if self._ready:
            return
        if not self.db.is_pg:       # PostgreSQL: the tables come with pg_schema.sql
            async with self.db._transaction_lock:
                for stmt in _SCHEMA:
                    await self.db._conn.execute(stmt)
                await self.db._conn.commit()
        self._ready = True

    async def send(self, client, peer, account_id: int, topic: str):
        """Send a photo on `topic` with a caption. Returns the message or None
        (daily cap reached, no photos for the topic, or it failed)."""
        try:
            files = self._load().get(topic) or []
            if not files:
                return None
            await self._ensure()
            async with self.db._conn.execute(
                    "SELECT COUNT(*) FROM warmup_photo_log WHERE account_id = ? AND sent_ts > ?",
                    (account_id, time.time() - 86400)) as cur:
                if (await cur.fetchone())[0] >= _PER_DAY:
                    return None
            async with self.db._conn.execute(
                    "SELECT file, MAX(sent_ts) FROM warmup_photo_log WHERE account_id = ? GROUP BY file",
                    (account_id,)) as cur:
                last = {r[0]: r[1] for r in await cur.fetchall()}
            fresh = [f for f in files if f not in last]
            name = random.choice(fresh) if fresh else min(files, key=lambda f: last.get(f, 0))
            data = await asyncio.to_thread(_variant, os.path.join(PHOTO_DIR, name))
            file = await client.upload_file(data, file_name=f"IMG_{random.randint(1000, 9999)}.jpg")
            msg = await client.send_file(peer, file, caption=random.choice(CAPTIONS.get(topic, [""])),
                                         force_document=False)
            async with self.db._transaction_lock:
                await self.db._conn.execute(
                    "INSERT INTO warmup_photo_log (account_id, file, sent_ts) VALUES (?, ?, ?)",
                    (account_id, name, time.time()))
                await self.db._conn.commit()
            logger.info(f"Warmup photos: account {account_id} sent {name}")
            return msg
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            logger.warning(f"Warmup photos: send failed for account {account_id}: {type(e).__name__}: {e}")
            return None
