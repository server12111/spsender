"""Missed / declined calls between warmup chat partners.

One side of a warmup DM (the warming account or its partner, at random) rings
the other at the start of the chat — a couple of calls a week per warming
account, counting calls it made and got. No audio is ever set up: either the partner declines after a few
seconds (both chats show a declined call), or nobody answers and the caller
hangs up (the partner's chat shows a missed call). A short text exchange about
it follows, then the usual chat.
"""
import asyncio
import hashlib
import logging
import random
import time

from telethon.errors import FloodWaitError, PeerFloodError

logger = logging.getLogger(__name__)

_MAX_PER_WEEK = 2
_MIN_GAP = 2 * 86400

AFTER_DECLINED = (["сорян, не могу говорить", "не могу сейчас ответить, давай текстом", "занят, напиши лучше",
                   "я на созвоне, давай в чате", "не удобно говорить, пиши)"],
                  ["ок)", "понял, давай", "окей, напишу", "ага, без проблем", "ладно)"])
AFTER_MISSED = (["ой, пропустил, что хотел?", "звонил? не слышал", "сорри, не видел звонок",
                 "только увидел, что ты звонил", "ты звонил? я в душе был"],
                ["да ничего срочного, просто набрал)", "да так, поболтать хотел", "неважно уже, всё ок",
                 "да просто хотел спросить как ты", "ничего, потом расскажу"])

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS warmup_call_log (
        account_id INTEGER NOT NULL,
        partner_id INTEGER NOT NULL,
        outcome TEXT NOT NULL,
        ts REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_wcl_account ON warmup_call_log(account_id, ts)",
    "CREATE INDEX IF NOT EXISTS idx_wcl_partner ON warmup_call_log(partner_id, ts)",
)


class WarmupCalls:
    def __init__(self, db):
        self.db = db
        self._ready = False
        self._dh = None  # (g, p) cached

    async def _ensure(self):
        if self._ready:
            return
        if not self.db.is_pg:       # PostgreSQL: the tables come with pg_schema.sql
            async with self.db._transaction_lock:
                for stmt in _SCHEMA:
                    await self.db._conn.execute(stmt)
                await self.db._conn.commit()
        self._ready = True

    async def can_call(self, account_id: int) -> bool:
        """Whether the warming account may be in another call (made or received)."""
        await self._ensure()
        async with self.db._conn.execute(
                "SELECT COUNT(*), MAX(ts) FROM warmup_call_log WHERE (account_id = ? OR partner_id = ?) AND ts > ?",
                (account_id, account_id, time.time() - 7 * 86400)) as cur:
            n, last = await cur.fetchone()
        return n < _MAX_PER_WEEK and (not last or time.time() - last >= _MIN_GAP)

    async def _g_a_hash(self, client) -> bytes:
        from telethon.tl.functions.messages import GetDhConfigRequest
        if self._dh is None:
            cfg = await client(GetDhConfigRequest(version=0, random_length=256))
            self._dh = (cfg.g, int.from_bytes(cfg.p, "big"))
        g, p = self._dh
        a = int.from_bytes(random.randbytes(256), "big") % (p - 1) or 2
        g_a = pow(g, a, p).to_bytes(256, "big")
        return hashlib.sha256(g_a).digest()

    async def place(self, a_acc, a_client, b_acc, b_client, b_peer_for_a):
        """A rings B. Returns "declined", "missed" or None (not placed)."""
        from telethon import utils
        from telethon.tl.functions.phone import DiscardCallRequest, RequestCallRequest
        from telethon.tl.types import (InputPhoneCall, PhoneCallDiscardReasonBusy, PhoneCallDiscardReasonMissed,
                                       PhoneCallProtocol)
        try:
            res = await a_client(RequestCallRequest(
                user_id=utils.get_input_user(b_peer_for_a),
                random_id=random.randint(1, 2**31 - 1),
                g_a_hash=await self._g_a_hash(a_client),
                protocol=PhoneCallProtocol(min_layer=65, max_layer=92, udp_p2p=True, udp_reflector=True,
                                           library_versions=["2.4.4", "2.7.7", "9.0.0", "11.0.0"]),
            ))
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception as e:
            logger.info(f"Warmup calls: {a_acc.id} -> {b_acc.id} not placed: {type(e).__name__}: {e}")
            return None
        call = res.phone_call
        inp = InputPhoneCall(id=call.id, access_hash=call.access_hash)
        outcome = None
        if random.random() < 0.5:
            await asyncio.sleep(random.uniform(5, 15))  # rings a bit, then B declines
            try:
                await b_client(DiscardCallRequest(peer=inp, duration=0, reason=PhoneCallDiscardReasonBusy(),
                                                  connection_id=0))
                outcome = "declined"
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                logger.debug(f"Warmup calls: decline by {b_acc.id} failed, leaving it missed: {e}")
        if outcome is None:
            await asyncio.sleep(random.uniform(20, 40))  # nobody picks up
            try:
                await a_client(DiscardCallRequest(peer=inp, duration=0, reason=PhoneCallDiscardReasonMissed(),
                                                  connection_id=0))
            except (FloodWaitError, PeerFloodError):
                raise
            except Exception as e:
                logger.debug(f"Warmup calls: hang-up by {a_acc.id} failed: {e}")  # it times out on its own
            outcome = "missed"
        async with self.db._transaction_lock:
            await self.db._conn.execute(
                "INSERT INTO warmup_call_log (account_id, partner_id, outcome, ts) VALUES (?, ?, ?, ?)",
                (a_acc.id, b_acc.id, outcome, time.time()))
            await self.db._conn.commit()
        logger.info(f"Warmup calls: {a_acc.id} -> {b_acc.id}: {outcome}")
        return outcome
