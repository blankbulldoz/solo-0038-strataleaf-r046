import math
from .codec import name
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import checked_revision
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

    def _expired_candidates(self, now, limit):
        """Current, visible documents that are expired at ``now``.

        Only the latest version of each key participates: a renewed document,
        a document whose expiry was cleared, and a tombstone are not sweep
        candidates. The snapshot revision is read inside this one read
        transaction so the selection is self-consistent, and the selected
        rows leave this method with their exact revisions for the
        conditional delete. No transaction or handle lock is held by the
        caller when this returns.
        """
        with self.store.read() as connection:
            head = checked_revision(self.store)
            return connection.execute("""SELECT v.collection,v.key,v.rev,v.expires FROM versions v
              JOIN (SELECT collection,key,MAX(rev) AS rev FROM versions WHERE rev<=?
                    GROUP BY collection,key) latest
              ON v.collection=latest.collection AND v.key=latest.key AND v.rev=latest.rev
            where v.body IS NOT NULL AND v.expires IS NOT NULL AND v.expires<=?
              ORDER BY v.expires,v.collection,v.key LIMIT ?""", (head, now, limit)).fetchall()

    def sweep(self, now, limit=1000):
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise InvalidDocument("expiry sweep requires a finite timestamp")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise InvalidDocument("sweep limit must be between 1 and 10000")
        rows = self._expired_candidates(now, limit)
        if not rows:
            return {"expired": 0, "revision": None}
        batch = WriteBatch(self.store)
        for row in rows:
            # The revision observed during selection is the precondition. A
            # rewrite between selection and commit (including a still-expired
            # rewrite or a delete-then-recreate) changes the current revision
            # and therefore conflicts the whole batch instead of silently
            # re-targeting a newer version.
            batch.delete(row["collection"], row["key"], expected=row["rev"])
        result = batch.commit()
        return {"expired": result.count, "revision": result.revision}
