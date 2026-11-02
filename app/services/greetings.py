import json
import random
from functools import lru_cache
from importlib.resources import files

from .core import dump, row


@lru_cache(maxsize=1)
def greeting_pool():
    return json.loads(files("app").joinpath("data/greetings.json").read_text(encoding="utf-8"))


class Greetings:
    def _greeting(self, c, pair, campaign, chat):
        progress = row(
            c,
            "SELECT * FROM greeting_progress WHERE campaign_id=? AND chat_pool_id=?",
            (campaign["id"], chat["chat_pool_id"]),
        )
        if progress:
            chosen = json.loads(progress["chosen"])
            if not progress["main_done"] and chosen["main"].get("version") != campaign["version"]:
                templates = self.templates(campaign["id"], "greeting", campaign["version"], c)
                if templates:
                    chosen["main"] = templates[chosen["main"].get("position", 0) % len(templates)]
                    progress["chosen"] = dump(chosen)
                    c.execute(
                        "UPDATE greeting_progress SET chosen=? WHERE campaign_id=? AND chat_pool_id=?",
                        (progress["chosen"], campaign["id"], chat["chat_pool_id"]),
                    )
            return progress
        pool = greeting_pool()
        templates = self.templates(campaign["id"], "greeting", campaign["version"], c)
        if not templates:
            return None
        chosen = {
            "emoji": random.choice(pool["emoji"]),
            "phrase": random.choice(pool["phrases"]),
            "main": random.choice(templates),
        }
        c.execute(
            "INSERT INTO greeting_progress(campaign_id,chat_pool_id,chosen) VALUES (?,?,?)",
            (campaign["id"], chat["chat_pool_id"], dump(chosen)),
        )
        return row(
            c,
            "SELECT * FROM greeting_progress WHERE campaign_id=? AND chat_pool_id=?",
            (campaign["id"], chat["chat_pool_id"]),
        )

    def _plan_greeting(self, c, pair, campaign, chat, account_id):
        if not campaign["greeting_enabled"] or not chat["greeting_allowed"]:
            return False
        progress = self._greeting(c, pair, campaign, chat)
        if not progress or all(progress[k + "_done"] for k in ("emoji", "phrase", "main")):
            return False
        chosen = json.loads(progress["chosen"])
        split = campaign["greeting_split"] and chat["split_allowed"]
        stage = next(k for k in ("emoji", "phrase", "main") if not progress[k + "_done"]) if split else "combined"
        template = chosen["main"]
        text = (
            "\n".join(
                value
                for key, value in (("emoji", chosen["emoji"]), ("phrase", chosen["phrase"]), ("main", template["text"]))
                if not progress[key + "_done"]
            )
            if stage == "combined"
            else (template["text"] if stage == "main" else chosen[stage])
        )
        payload = {
            "text": text,
            "stage": stage,
            "topic_id": chat["topic_id"],
            "peer_id": chat["chat_id"],
            "media_path": template["media_path"] if stage in {"main", "combined"} else None,
            "media_kind": template["media_kind"],
            "progress_mask": [int(progress[key + "_done"]) for key in ("emoji", "phrase", "main")],
        }
        return self._enqueue(
            c,
            pair,
            campaign,
            "GREETING",
            chat["chat_pool_id"],
            account_id,
            payload,
            f"greet:{campaign['id']}:{chat['chat_pool_id']}:{stage}",
        )
