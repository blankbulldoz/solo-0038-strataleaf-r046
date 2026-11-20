import math
from .codec import name
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import checked_revision, latest_row
from .transaction import WriteBatch


class Retention:
    def __init__(self, store):
        self.store = store

    def pin(self, pin_name, revision=None):
        name(pin_name)
        with self.store.atomic() as connection:
            revision = checked_revision(self.store, revision)
            if connection.execute("SELECT 1 FROM pins WHERE name=?", (pin_name,)).fetchone():
                raise Conflict("read pin already exists")
            connection.execute("INSERT INTO pins VALUES (?,?)", (pin_name, revision))
        return revision

    def unpin(self, pin_name):
        with self.store.atomic() as connection:
            if not connection.execute("DELETE FROM pins WHERE name=?", (pin_name,)).rowcount:
                raise NotFound("unknown read pin")

    def compact(self, target):
        with self.store.atomic(writable=True) as connection:
            target = checked_revision(self.store, target)
            blockers = [row[0] for row in connection.execute("SELECT rev FROM pins UNION ALL SELECT ack FROM consumers")]
            boundary = min([target] + blockers)
            prior = int(self.store.get("floor"))
            if boundary < prior:
                raise Conflict("retention metadata violates current floor")
            removed = connection.execute("""DELETE FROM versions WHERE rev<? AND EXISTS
              (SELECT 1 FROM versions newer WHERE newer.collection=versions.collection
               AND newer.key=versions.key AND newer.rev>versions.rev AND newer.rev<=?)""", (boundary, boundary)).rowcount
            logs = connection.execute("DELETE FROM commits WHERE rev<=?", (boundary,)).rowcount
            self.store.set("floor", boundary)
            return {"requested": target, "floor": boundary, "removed_versions": removed,
                    "removed_commits": logs, "blocked": boundary < target}

    def sweep(self, now, limit=1000):
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise InvalidDocument("expiry sweep requires a finite timestamp")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise InvalidDocument("sweep limit must be between 1 and 10000")
        with self.store.read() as connection:
            rows = connection.execute("""SELECT v.* FROM versions v WHERE v.body IS NOT NULL AND v.expires<=?
              AND NOT EXISTS (SELECT 1 FROM versions n WHERE n.collection=v.collection AND n.key=v.key
                              AND n.rev>v.rev AND n.body IS NOT NULL AND n.expires<=?)
              ORDER BY v.expires,v.collection,v.key LIMIT ?""", (now, now, limit)).fetchall()
        if not rows:
            return {"expired": 0, "revision": None}
        batch = WriteBatch(self.store)
        with self.store.read() as connection:
            head = checked_revision(self.store)
            for row in rows:
                collection, key = row["collection"], row["key"]
                current = latest_row(connection, collection, key, head)
                batch.delete(collection, key, expected=current["rev"] if current is not None else 0)
        result = batch.commit()
        return {"expired": result.count, "revision": result.revision}
