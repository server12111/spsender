from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .core import DomainError, row, rows


def shift_role(timestamp, tz="Europe/Kyiv"):
    hour = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(ZoneInfo(tz)).hour
    return "A" if hour // 6 % 2 == 0 else "B"


def active_count(total, seconds):
    ratio = (0.1, 0.3, 0.6, 1.0)[min(3, max(0, int(seconds // 86400)))]
    return min(total, max(1, int(total * ratio))) if total else 0


class Pairs:
    def _pair(self, c, pair_id):
        pair = row(c, "SELECT * FROM pairs WHERE id=?", (pair_id,))
        if not pair:
            raise DomainError("Пара не найдена")
        return pair

    def _pair_accounts(self, c, pair):
        return [row(c, "SELECT * FROM accounts WHERE id=?", (pair[key],)) for key in ("account_a_id", "account_b_id")]

    def _pair_ready(self, c, pair):
        if row(c, "SELECT 1 FROM dispatch_jobs WHERE pair_id=? AND status='UNKNOWN'", (pair["id"],)):
            return False
        accounts = self._pair_accounts(c, pair)
        return all(
            a
            and a["onboarding_stage"] == "READY"
            and a["account_health"] == "OK"
            and not a["paused"]
            and a["flood_until"] <= self.clock()
            for a in accounts
        )

    def _pause_clock(self, c, pair):
        pair = self._pair(c, pair["id"])
        if pair["active_since"] is not None:
            elapsed = max(0, self.clock() - pair["active_since"])
            c.execute(
                "UPDATE pairs SET active_dispatch_seconds=active_dispatch_seconds+?,active_since=NULL WHERE id=?",
                (elapsed, pair["id"]),
            )

    def elapsed(self, pair):
        return pair["active_dispatch_seconds"] + (
            max(0, self.clock() - pair["active_since"]) if pair["active_since"] is not None else 0
        )

    def _sync_pair(self, c, pair):
        campaign = self.campaign(c, pair["campaign_id"])
        if not pair["account_a_id"]:
            return
        ready = self._pair_ready(c, pair)
        if not pair["account_b_id"]:
            state = "WAITING_PARTNER"
        elif not ready:
            accounts = self._pair_accounts(c, pair)
            if not all(a and a["account_health"] == "OK" and not a["paused"] for a in accounts) or row(
                c, "SELECT 1 FROM dispatch_jobs WHERE pair_id=? AND status='UNKNOWN'", (pair["id"],)
            ):
                state = "NEEDS_ATTENTION"
            elif any(a["onboarding_stage"] != "READY" for a in accounts):
                state = "PREPARING"
            else:
                state = "PAUSED"
        else:
            total = c.execute(
                "SELECT count(*) FROM pair_chats WHERE pair_id=? AND removed_at IS NULL", (pair["id"],)
            ).fetchone()[0]
            state = (
                "READY"
                if total >= pair["chat_limit"]
                else ("WAITING_REPLACEMENT" if pair["status"] == "WAITING_REPLACEMENT" else "WAITING_CHATS")
            )
        if campaign["status"] != "RUNNING":
            state = "PAUSED"
        if campaign["status"] != "RUNNING" or not ready:
            self._pause_clock(c, pair)
        elif pair["pair_dispatch_started_at"] is not None and pair["active_since"] is None:
            c.execute("UPDATE pairs SET active_since=? WHERE id=?", (self.clock(), pair["id"]))
        if state != pair["status"]:
            c.execute("UPDATE pairs SET status=? WHERE id=?", (state, pair["id"]))

    def pair_card(self, pair_id):
        with self.db.read() as c:
            pair = self._pair(c, pair_id)
            campaign = self.campaign(c, pair["campaign_id"])
            chats = rows(
                c,
                "SELECT pc.*,cp.title,cp.chat_id,cp.topic_id FROM pair_chats pc JOIN chat_pool cp ON cp.id=pc.chat_pool_id "
                "WHERE pc.pair_id=? AND pc.removed_at IS NULL ORDER BY pc.id",
                (pair_id,),
            )
            role = shift_role(self.clock(), campaign["timezone"])
            return {
                **pair,
                "chats": chats,
                "role": role,
                "active_account_id": pair["account_a_id"] if role == "A" else pair["account_b_id"],
                "active_count": active_count(len(chats), self.elapsed(pair)),
                "elapsed": self.elapsed(pair),
            }

    def link_pair(self, account_a, account_b):
        if account_a == account_b:
            raise DomainError("Для пары нужны разные аккаунты")
        with self.db.tx() as c:
            if row(
                c,
                "SELECT 1 FROM dispatch_jobs j JOIN pairs p ON p.id=j.pair_id WHERE j.status IN ('INFLIGHT','UNKNOWN') AND (p.account_a_id IN (?,?) OR p.account_b_id IN (?,?))",
                (account_a, account_b, account_a, account_b),
            ):
                raise DomainError("Сначала дождитесь / сверьте операции участников пары")
            members = [
                row(c, "SELECT * FROM campaign_members WHERE account_id=?", (a,)) for a in (account_a, account_b)
            ]
            if not all(members) or members[0]["campaign_id"] != members[1]["campaign_id"]:
                raise DomainError("Аккаунты должны быть назначены одной рассылке")
            for a in (account_a, account_b):
                pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (a, a))
                if pair and pair["account_b_id"]:
                    if pair["account_a_id"] == account_a and pair["account_b_id"] == account_b:
                        return pair["id"]
                    raise DomainError("Аккаунт уже состоит в полной паре. Измените назначение кампании")
            campaign_id = members[0]["campaign_id"]
            for a in (account_a, account_b):
                self._detach_account(c, a)
            c.execute("INSERT INTO campaign_members VALUES (?,?)", (account_a, campaign_id))
            c.execute("INSERT INTO campaign_members VALUES (?,?)", (account_b, campaign_id))
            campaign = self.campaign(c, campaign_id)
            pair_id = c.execute(
                "INSERT INTO pairs(campaign_id,account_a_id,account_b_id,chat_limit,status,created_at) VALUES (?,?,?,?,?,?)",
                (campaign_id, account_a, account_b, campaign["chat_limit"], "PREPARING", self.clock()),
            ).lastrowid
            self._fill_waiting(c)
            self.audit(c, "PAIR_LINKED", campaign_id=campaign_id, pair_id=pair_id)
            return pair_id
