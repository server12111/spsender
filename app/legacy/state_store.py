"""Original state-store API, scoped to the database and encrypted at rest."""

import json
import time


def mono_to_wall(value):
    return round(time.time() + value - time.monotonic())


def wall_to_mono(value):
    return time.monotonic() + value - time.time()


async def load_state(db, key, default=None):
    if key == "warmup.progress":
        records = (
            [item for name, item in db._restore_snapshot.items() if name.startswith("warmup.dialog:")]
            if hasattr(db, "_restore_snapshot")
            else await db.engine.db.run(
                db.engine.db.all, "SELECT value FROM legacy_runtime_state WHERE key LIKE 'warmup.dialog:%'"
            )
        )
        return [json.loads(db.engine.secrets.decrypt(item["value"])) for item in records]
    item = (
        db._restore_snapshot.get(key)
        if hasattr(db, "_restore_snapshot")
        else await db.engine.db.run(
            db.engine.db.one, "SELECT value,updated_at FROM legacy_runtime_state WHERE key=?", (key,)
        )
    )
    result = json.loads(db.engine.secrets.decrypt(item["value"])) if item else default
    if key == "warmup.dialogs":
        result = result or {}
        actors = (
            [
                record
                for name, record in db._restore_snapshot.items()
                if name.startswith("warmup.actor:") and record["updated_at"] > (item["updated_at"] if item else 0)
            ]
            if hasattr(db, "_restore_snapshot")
            else await db.engine.db.run(
                db.engine.db.all,
                "SELECT key,value FROM legacy_runtime_state WHERE key LIKE 'warmup.actor:%' AND updated_at>?",
                (item["updated_at"] if item else 0,),
            )
        )
        for actor in actors:
            account_id = actor["key"].split(":")[-1]
            data = json.loads(db.engine.secrets.decrypt(actor["value"]))
            for field, value in data.items():
                if value is None:
                    result.setdefault(field, {}).pop(account_id, None)
                else:
                    result.setdefault(field, {})[account_id] = value
    return result


async def save_state(db, key, value, *, snapshot_at=None):
    # Timestamp the observation before any await, rather than the eventual
    # COMMIT. A delayed global snapshot must not supersede a newer RPC snapshot.
    if snapshot_at is None:
        # Python 3.11 on Windows can return the same wall-clock tick for
        # consecutive calls; keep observations strictly ordered within runtime.
        observed_at = max(time.time_ns(), getattr(db.engine, "_legacy_state_revision", 0) + 1)
        db.engine._legacy_state_revision = observed_at
    else:
        observed_at = snapshot_at
    if not hasattr(db.engine, "_legacy_state_cache"):
        db.engine._legacy_state_cache = {}
        db.engine._legacy_state_versions = {}
    db._saved_state = db.engine._legacy_state_cache
    if key == "warmup.progress":
        value = json.loads(json.dumps(value))
        keys = set()
        for progress in value:
            name = f"warmup.dialog:{min(progress['a'], progress['b'])}-{max(progress['a'], progress['b'])}"
            keys.add(name)
            await save_state(db, name, progress, snapshot_at=observed_at)

        def remove_stale():
            removed = []
            with db.engine.db.tx() as connection:
                for record in connection.execute(
                    "SELECT key FROM legacy_runtime_state WHERE key LIKE 'warmup.dialog:%' AND updated_at<=?",
                    (observed_at,),
                ).fetchall():
                    if record[0] not in keys:
                        connection.execute("DELETE FROM legacy_runtime_state WHERE key=?", (record[0],))
                        removed.append(record[0])
            return removed

        removed = await db.engine.db.run(remove_stale)
        for name in removed:
            if db.engine._legacy_state_versions.get(name, 0) <= observed_at:
                db._saved_state.pop(name, None)
                db.engine._legacy_state_versions.pop(name, None)
        return
    encoded = json.dumps(value, separators=(",", ":"))

    def write():
        with db.engine.db.tx() as connection:
            old = connection.execute("SELECT value,updated_at FROM legacy_runtime_state WHERE key=?", (key,)).fetchone()
            if old and old[1] > observed_at:
                return False
            if old and db.engine.secrets.decrypt(old[0]) == encoded:
                return True
            connection.execute(
                "INSERT INTO legacy_runtime_state VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, db.engine.secrets.encrypt(encoded), observed_at),
            )
        return True

    versions = db.engine._legacy_state_versions
    if versions.get(key, 0) > observed_at or db._saved_state.get(key) == encoded:
        return
    if await db.engine.db.run(write) and versions.get(key, 0) <= observed_at:
        db._saved_state[key] = encoded
        versions[key] = observed_at
