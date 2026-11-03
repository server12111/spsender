"""Explicit Telegram membership requests from trusted group bots; never CAPTCHA solving."""

import base64
import json
import re
import secrets
from urllib.parse import parse_qs, urlparse

from .core import dump, row, rows

SUBSCRIPTION = re.compile(r"подпиш|подпис[кч]|спонсор|subscribe|join.{0,40}(?:channel|group)", re.I)
CHALLENGE = re.compile(
    r"captcha|капч|я\s+не\s+робот|not\s+(?:a\s+)?(?:robot|bot)|verify.{0,20}human|prove.{0,20}human|реши.{0,20}пример",
    re.I,
)
TELEGRAM_LINK = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/[A-Za-z0-9_+/?=&%-]+|@[A-Za-z][A-Za-z0-9_]{3,31}", re.I
)


def membership_prompt(text, markup=None):
    labels = " ".join(button.text or "" for group in getattr(markup, "rows", ()) or () for button in group.buttons)
    description = (text or "") + " " + labels
    return bool(SUBSCRIPTION.search(description) and not CHALLENGE.search(description))


def membership_links(text, markup=None):
    """Return bounded explicit links + a subscription-only verification callback."""
    labels = " ".join(button.text or "" for group in getattr(markup, "rows", ()) or () for button in group.buttons)
    description = (text or "") + " " + labels
    if CHALLENGE.search(description) or not SUBSCRIPTION.search(description):
        return [], None
    urls = list(TELEGRAM_LINK.findall(text or ""))
    verify = None
    for group in getattr(markup, "rows", ()) or ():
        for button in group.buttons:
            button_type = getattr(button, "type", button)
            url = getattr(button_type, "url", None)
            if url:
                urls.append(url)
            data = getattr(button_type, "data", None)
            if data and len(data) <= 64 and re.search(r"провер|подписал|check|subscribed", button.text or "", re.I):
                verify = base64.b64encode(data).decode()
    links = []
    for url in urls:
        if url.startswith("@"):
            url = "https://t.me/" + url[1:]
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        try:
            parsed = urlparse(url)
            valid = parsed.hostname in {"t.me", "telegram.me"} and not parsed.username and not parsed.port
        except ValueError:
            valid = False
        if not valid:
            continue
        path = parsed.path.strip("/")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}|\+[A-Za-z0-9_-]{5,128}", path):
            continue
        query = parse_qs(parsed.query)
        if query and set(query) != {"start"}:
            continue
        parameter = query.get("start", [""])[0]
        if len(parameter) > 64 or not re.fullmatch(r"[A-Za-z0-9_-]*", parameter):
            continue
        canonical = "https://t.me/" + (path if path.startswith("+") else path.lower())
        if "start" in query:
            canonical += "?start=" + parameter
        item = {"link": canonical, "action": "BOT_START" if "start" in query else "JOIN", "start_param": parameter}
        if item not in links:
            links.append(item)
    return (links, verify) if len(links) <= 10 else ([], None)


class Memberships:
    def retry_memberships(self, campaign_id):
        with self.db.tx() as c:
            self.campaign(c, campaign_id)
            for request in rows(
                c,
                "SELECT m.* FROM membership_requests m JOIN dispatch_jobs j ON j.id=m.job_id "
                "WHERE m.campaign_id=? AND m.status='FAILED' AND j.status='FAILED' ORDER BY m.id LIMIT 100",
                (campaign_id,),
            ):
                context = self._membership_context(c, request["account_id"], request["chat_pool_id"])
                if context and context["key"] == request["context_key"]:
                    c.execute(
                        "UPDATE membership_requests SET status='WAITING',last_error=NULL WHERE id=?", (request["id"],)
                    )
                    c.execute(
                        "UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=? AND status='FAILED'",
                        (request["job_id"],),
                    )

    def _membership_context(self, c, account_id, chat_pool_id):
        account = row(c, "SELECT * FROM accounts WHERE id=?", (account_id,))
        member = row(c, "SELECT * FROM campaign_members WHERE account_id=?", (account_id,))
        pair = row(c, "SELECT * FROM pairs WHERE account_a_id=? OR account_b_id=?", (account_id, account_id))
        chat = row(c, "SELECT * FROM chat_pool WHERE id=? AND approved=1 AND status<>'ARCHIVED'", (chat_pool_id,))
        if not account or not member or not pair or not chat:
            return None
        campaign = row(
            c, "SELECT * FROM campaigns WHERE id=? AND status='RUNNING' AND auto_subscribe=1", (member["campaign_id"],)
        )
        if (
            not campaign
            or pair["campaign_id"] != campaign["id"]
            or account["account_health"] != "OK"
            or account["paused"]
        ):
            return None
        future = row(
            c,
            "SELECT t.exchange_id FROM rotation_targets t JOIN rotation_progress p ON p.target_id=t.id "
            "JOIN rotation_exchanges x ON x.id=t.exchange_id WHERE t.chat_pool_id=? AND t.pair_id=? "
            "AND p.account_id=? AND x.status IN ('PREPARING','READY') LIMIT 1",
            (chat_pool_id, pair["id"], account_id),
        )
        if future:
            exchange = row(c, "SELECT * FROM rotation_exchanges WHERE id=?", (future["exchange_id"],))
            if not campaign["rotation_enabled"] or not self._exchange_valid(c, exchange):
                return None
            return {"campaign": campaign, "pair": pair, "chat": chat, "key": f"rotation:{future['exchange_id']}"}
        assignment = row(
            c,
            "SELECT id FROM pair_chats WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
            (pair["id"], chat_pool_id),
        )
        if not assignment:
            return None
        access = row(
            c,
            "SELECT verification_generation FROM account_chat_access WHERE account_id=? AND chat_pool_id=?",
            (account_id, chat_pool_id),
        )
        return {
            "campaign": campaign,
            "pair": pair,
            "chat": chat,
            "key": f"assigned:{assignment['id']}:{access['verification_generation'] if access else 0}",
        }

    def membership_context(self, account_id, source_peer_id, *, private=False):
        with self.db.read() as c:
            if private:
                source = row(
                    c,
                    "SELECT m.chat_pool_id FROM membership_requests m LEFT JOIN dispatch_jobs j ON j.id=m.job_id "
                    "WHERE m.account_id=? AND m.resolved_peer_id=? "
                    "AND (m.action='BOT_START' OR json_extract(m.payload,'$.resolved_bot')=1) "
                    "AND (m.status='CONFIRMED' OR (m.status='WAITING' AND j.status='INFLIGHT')) "
                    "ORDER BY m.id DESC LIMIT 1",
                    (account_id, source_peer_id),
                )
                candidates = [source] if source else []
            else:
                candidates = rows(
                    c,
                    "SELECT id AS chat_pool_id FROM chat_pool WHERE chat_id=? AND approved=1 ORDER BY id",
                    (source_peer_id,),
                )
            for candidate in candidates:
                context = self._membership_context(c, account_id, candidate["chat_pool_id"])
                if context:
                    return context["chat"]["id"]
        return None

    def membership_bot_resolved(self, job_id, account_id, peer_id):
        # The bot can reply before StartBot's RPC acknowledgement is committed.
        # Bind its identity before the request so that reply is not discarded.
        with self.db.tx() as c:
            c.execute(
                "UPDATE membership_requests SET resolved_peer_id=?,payload=json_set(payload,'$.resolved_bot',1) "
                "WHERE job_id=? AND account_id=? AND status='WAITING' AND action IN ('JOIN','BOT_START') "
                "AND EXISTS(SELECT 1 FROM dispatch_jobs WHERE id=? AND status='INFLIGHT')",
                (peer_id, job_id, account_id, job_id),
            )

    def register_membership_request(
        self, account_id, chat_pool_id, source_peer_id, message_id, sender_id, links, verify=None
    ):
        with self.db.tx() as c:
            context = self._membership_context(c, account_id, chat_pool_id)
            if not context:
                return False
            campaign, pair = context["campaign"], context["pair"]
            total = c.execute(
                "SELECT count(*) FROM membership_requests WHERE account_id=? AND chat_pool_id=? AND context_key=?",
                (account_id, chat_pool_id, context["key"]),
            ).fetchone()[0]
            entries = list(links[:10])
            if verify:
                stale = rows(
                    c,
                    "SELECT m.id,m.job_id FROM membership_requests m LEFT JOIN dispatch_jobs j ON j.id=m.job_id "
                    "WHERE m.account_id=? AND m.chat_pool_id=? AND m.context_key=? AND m.action='VERIFY' "
                    "AND m.source_peer_id=? AND m.source_sender_id=? AND m.source_message_id<>? AND m.status IN ('WAITING','FAILED') "
                    "AND (j.id IS NULL OR j.status NOT IN ('INFLIGHT','UNKNOWN'))",
                    (account_id, chat_pool_id, context["key"], source_peer_id, sender_id, message_id),
                )
                for obsolete in stale:
                    c.execute("UPDATE membership_requests SET status='CANCELLED' WHERE id=?", (obsolete["id"],))
                    c.execute(
                        "UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=? AND status='PENDING'",
                        (obsolete["job_id"],),
                    )
                entries.append({"link": f"verify:{source_peer_id}:{message_id}", "action": "VERIFY", "data": verify})
            accepted = False
            capacity_hit = False
            for entry in entries:
                existing = row(
                    c,
                    "SELECT id FROM membership_requests WHERE account_id=? AND chat_pool_id=? AND context_key=? "
                    "AND action=? AND link=?",
                    (account_id, chat_pool_id, context["key"], entry["action"], entry["link"]),
                )
                if existing:
                    accepted = True
                    continue
                if total >= 16:
                    capacity_hit = True
                    break
                payload = {
                    **entry,
                    "peer_id": source_peer_id,
                    "message_id": message_id,
                    "random_id": secrets.randbits(63),
                }
                c.execute(
                    "INSERT INTO membership_requests(campaign_id,pair_id,account_id,chat_pool_id,source_peer_id,"
                    "source_message_id,source_sender_id,action,link,payload,context_key,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        campaign["id"],
                        pair["id"],
                        account_id,
                        chat_pool_id,
                        source_peer_id,
                        message_id,
                        sender_id,
                        entry["action"],
                        entry["link"],
                        dump(payload),
                        context["key"],
                        self.clock(),
                    ),
                )
                accepted, total = True, total + 1
            if accepted:
                c.execute(
                    "UPDATE account_chat_access SET can_write=0,last_error=CASE WHEN manual_required=0 "
                    "OR last_error='SPONSOR_REQUIRED' THEN 'SPONSOR_REQUIRED' ELSE last_error END,manual_required=1 "
                    "WHERE account_id=? AND chat_pool_id=?",
                    (account_id, chat_pool_id),
                )
                c.execute(
                    "UPDATE rotation_progress SET can_write=0,last_error=CASE WHEN last_error IN ('MANUAL_VERIFICATION','MEMBERSHIP_LIMIT') "
                    "THEN last_error ELSE NULL END,next_action_at=? WHERE account_id=? "
                    "AND target_id IN (SELECT id FROM rotation_targets WHERE chat_pool_id=?)",
                    (self.clock() + 5, account_id, chat_pool_id),
                )
            if capacity_hit:
                c.execute(
                    "UPDATE account_chat_access SET last_error='MEMBERSHIP_LIMIT',manual_required=1 WHERE account_id=? AND chat_pool_id=?",
                    (account_id, chat_pool_id),
                )
                c.execute(
                    "UPDATE rotation_progress SET last_error='MEMBERSHIP_LIMIT',can_write=0 WHERE account_id=? "
                    "AND target_id IN (SELECT id FROM rotation_targets WHERE chat_pool_id=?)",
                    (account_id, chat_pool_id),
                )
                self._notify(
                    c,
                    f"membership-limit:{account_id}:{chat_pool_id}:{context['key']}",
                    "preparation_error",
                    {
                        "account_id": account_id,
                        "campaign_id": campaign["id"],
                        "chat_pool_id": chat_pool_id,
                        "title": context["chat"]["title"],
                        "link": context["chat"]["link"],
                        "reason": "MEMBERSHIP_LIMIT",
                    },
                    50,
                )
            return accepted

    def _plan_memberships(self, c, pair, campaign):
        if campaign["status"] != "RUNNING" or not campaign["auto_subscribe"]:
            return
        for obsolete in rows(
            c,
            "SELECT m.id,m.job_id FROM membership_requests m LEFT JOIN dispatch_jobs j ON j.id=m.job_id "
            "WHERE m.pair_id=? AND m.action='VERIFY' AND m.status='WAITING' "
            "AND (j.id IS NULL OR j.status NOT IN ('INFLIGHT','UNKNOWN')) AND EXISTS("
            "SELECT 1 FROM membership_requests n WHERE n.account_id=m.account_id AND n.chat_pool_id=m.chat_pool_id "
            "AND n.context_key=m.context_key AND n.action='VERIFY' AND n.source_peer_id=m.source_peer_id "
            "AND n.source_sender_id=m.source_sender_id AND n.source_message_id>m.source_message_id "
            "AND n.status IN ('WAITING','CONFIRMED'))",
            (pair["id"],),
        ):
            c.execute("UPDATE membership_requests SET status='CANCELLED' WHERE id=?", (obsolete["id"],))
            c.execute(
                "UPDATE dispatch_jobs SET status='CANCELLED' WHERE id=? AND status='PENDING'", (obsolete["job_id"],)
            )
        for request in rows(
            c,
            "SELECT m.* FROM membership_requests m LEFT JOIN dispatch_jobs j ON j.id=m.job_id "
            "WHERE m.pair_id=? AND m.status='WAITING' AND (j.id IS NULL OR j.status='CANCELLED') "
            "ORDER BY CASE m.action WHEN 'VERIFY' THEN 1 ELSE 0 END,m.id LIMIT 8",
            (pair["id"],),
        ):
            context = self._membership_context(c, request["account_id"], request["chat_pool_id"])
            if not context or context["key"] != request["context_key"]:
                c.execute("UPDATE membership_requests SET status='CANCELLED' WHERE id=?", (request["id"],))
                continue
            if not self._membership_dependencies_done(c, request):
                continue
            payload = json.loads(request["payload"])
            payload["membership_request_id"] = request["id"]
            job_id = self._enqueue(
                c,
                pair,
                campaign,
                "MEMBERSHIP",
                request["chat_pool_id"],
                request["account_id"],
                payload,
                f"membership:{request['id']}",
            )
            if job_id:
                c.execute("UPDATE dispatch_jobs SET queue_rank=1 WHERE id=?", (job_id,))
                c.execute("UPDATE membership_requests SET job_id=? WHERE id=?", (job_id, request["id"]))

    def _membership_job_allowed(self, c, job, account, pair):
        request = row(c, "SELECT * FROM membership_requests WHERE id=?", (job["payload"].get("membership_request_id"),))
        if not request or request["status"] != "WAITING" or request["job_id"] != job["id"]:
            return False
        context = self._membership_context(c, account["id"], request["chat_pool_id"])
        return bool(
            context
            and context["pair"]["id"] == pair["id"]
            and context["key"] == request["context_key"]
            and self._membership_dependencies_done(c, request)
            and account["next_action_at"] <= self.clock()
            and not self._rotation_pair_blocked(c, pair["id"])
        )

    def _membership_dependencies_done(self, c, request):
        if request["action"] not in {"VERIFY", "CHECK"}:
            return True
        actions = "action IN ('JOIN','BOT_START')" if request["action"] == "VERIFY" else "action<>'CHECK'"
        return not row(
            c,
            "SELECT 1 FROM membership_requests WHERE account_id=? AND chat_pool_id=? AND context_key=? "
            "AND " + actions + " AND status NOT IN ('CONFIRMED','CANCELLED') LIMIT 1",
            (request["account_id"], request["chat_pool_id"], request["context_key"]),
        )

    def _finish_membership(self, c, job, result):
        request_id = job["payload"]["membership_request_id"]
        request = row(c, "SELECT * FROM membership_requests WHERE id=?", (request_id,))
        c.execute(
            "UPDATE membership_requests SET status='CONFIRMED',resolved_peer_id=? WHERE id=?",
            (result.data.get("peer_id"), request_id),
        )
        if result.data.get("bot_started"):
            c.execute(
                "UPDATE membership_requests SET payload=json_set(payload,'$.resolved_bot',1) WHERE id=?", (request_id,)
            )
        pause = int(row(c, "SELECT value FROM settings WHERE key='join_pause'")["value"])
        c.execute("UPDATE accounts SET next_action_at=? WHERE id=?", (self.clock() + pause, job["account_id"]))
        can_write = bool(result.data.get("can_write", False))
        c.execute(
            "UPDATE rotation_progress SET last_error=CASE WHEN last_error IN ('MANUAL_VERIFICATION','MEMBERSHIP_LIMIT') "
            "THEN last_error ELSE NULL END,check_count=0,can_write=CASE WHEN last_error IN ('MANUAL_VERIFICATION','MEMBERSHIP_LIMIT') "
            "THEN 0 ELSE ? END,verified_at=?,generation=generation+1,next_action_at=? "
            "WHERE account_id=? AND target_id IN (SELECT id FROM rotation_targets WHERE chat_pool_id=?)",
            (can_write, self.clock() if can_write else 0, self.clock() + 5, job["account_id"], request["chat_pool_id"]),
        )
        if request["action"] == "CHECK":
            c.execute(
                "UPDATE account_chat_access SET joined=1,can_write=?,last_checked_at=?,"
                "manual_required=CASE WHEN last_error='SPONSOR_REQUIRED' THEN 0 ELSE manual_required END,"
                "last_error=CASE WHEN last_error='SPONSOR_REQUIRED' THEN NULL ELSE last_error END "
                "WHERE account_id=? AND chat_pool_id=?",
                (can_write, self.clock(), job["account_id"], request["chat_pool_id"]),
            )
            if can_write:
                self._membership_access_restored(c, request)
        else:
            chat = row(c, "SELECT chat_id,link FROM chat_pool WHERE id=?", (request["chat_pool_id"],))
            payload = {"action": "CHECK", "peer_id": chat["chat_id"], "link": chat["link"]}
            c.execute(
                "INSERT OR IGNORE INTO membership_requests(campaign_id,pair_id,account_id,chat_pool_id,source_peer_id,"
                "source_message_id,source_sender_id,action,link,payload,context_key,created_at) VALUES (?,?,?,?,?,?,?,'CHECK',?,?,?,?)",
                (
                    request["campaign_id"],
                    request["pair_id"],
                    job["account_id"],
                    request["chat_pool_id"],
                    request["source_peer_id"],
                    request["source_message_id"],
                    request["source_sender_id"],
                    f"check:{request['source_message_id']}",
                    dump(payload),
                    request["context_key"],
                    self.clock(),
                ),
            )

    def _membership_access_restored(self, c, request):
        pair = self._pair(c, request["pair_id"])
        access = rows(
            c,
            "SELECT * FROM account_chat_access WHERE chat_pool_id=? AND account_id IN (?,?)",
            (request["chat_pool_id"], pair["account_a_id"], pair["account_b_id"]),
        )
        if len(access) != 2 or not all(a["joined"] and a["can_write"] and not a["manual_required"] for a in access):
            return
        issue = row(
            c,
            "SELECT id FROM chat_issues WHERE pair_id=? AND chat_pool_id=? AND status='OPEN' "
            "AND reason='PUBLICATION_FORBIDDEN'",
            (pair["id"], request["chat_pool_id"]),
        )
        if issue:
            c.execute("UPDATE chat_issues SET status='CLOSED' WHERE id=?", (issue["id"],))
            c.execute(
                "DELETE FROM chat_failures WHERE pair_id=? AND chat_pool_id=?", (pair["id"], request["chat_pool_id"])
            )
            c.execute(
                "UPDATE pair_chats SET status='ASSIGNED',last_error=NULL WHERE pair_id=? AND chat_pool_id=? AND removed_at IS NULL",
                (pair["id"], request["chat_pool_id"]),
            )
            c.execute(
                "UPDATE chat_pool SET status='ASSIGNED' WHERE id=? AND status='PAUSED_NEEDS_REVIEW'",
                (request["chat_pool_id"],),
            )
            self.audit(
                c,
                "MEMBERSHIP_ACCESS_RESTORED",
                pair_id=pair["id"],
                account_id=request["account_id"],
                chat_pool_id=request["chat_pool_id"],
            )

    def preparation_manual_required(self, account_id, telegram_chat_id):
        with self.db.tx() as c:
            for progress in rows(
                c,
                "SELECT p.id,t.chat_pool_id,cp.title,cp.link FROM rotation_progress p "
                "JOIN rotation_targets t ON t.id=p.target_id JOIN rotation_exchanges x ON x.id=t.exchange_id "
                "JOIN chat_pool cp ON cp.id=t.chat_pool_id WHERE p.account_id=? AND cp.chat_id=? "
                "AND x.status IN ('PREPARING','READY')",
                (account_id, telegram_chat_id),
            ):
                c.execute(
                    "UPDATE rotation_progress SET last_error='MANUAL_VERIFICATION',can_write=0 WHERE id=?",
                    (progress["id"],),
                )
                self._notify(
                    c,
                    f"rotation-manual:{progress['id']}",
                    "manual",
                    {
                        "account_id": account_id,
                        "chat_pool_id": progress["chat_pool_id"],
                        "title": progress["title"],
                        "link": progress["link"],
                    },
                    50,
                )

    def membership_manual_required(self, account_id, chat_pool_id, message_id):
        with self.db.tx() as c:
            chat = row(c, "SELECT * FROM chat_pool WHERE id=?", (chat_pool_id,))
            c.execute(
                "UPDATE account_chat_access SET manual_required=1,last_error='MANUAL_VERIFICATION' "
                "WHERE account_id=? AND chat_pool_id=?",
                (account_id, chat_pool_id),
            )
            c.execute(
                "UPDATE rotation_progress SET last_error='MANUAL_VERIFICATION',can_write=0 WHERE account_id=? "
                "AND target_id IN (SELECT id FROM rotation_targets WHERE chat_pool_id=?)",
                (account_id, chat_pool_id),
            )
            self._notify(
                c,
                f"membership-manual:{account_id}:{chat_pool_id}:{message_id}",
                "manual",
                {"account_id": account_id, "chat_pool_id": chat_pool_id, "title": chat["title"], "link": chat["link"]},
                50,
            )
