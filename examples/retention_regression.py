"""Repeatable regression checks for ``Retention.sweep`` (explicit expiry cleanup).

Run from the repository root::

    python3 -m examples.retention_regression

Every check uses a real temporary SQLite database through the public
``Database`` API. The module raises ``AssertionError`` (non-zero exit) on a
regression and prints a JSON summary on success.

What is checked
---------------
* Candidates are the *current, visible* versions only. A renewed document, a
  document whose expiry was cleared, and a tombstone cannot consume the
  ``limit`` slot; old bodies stay available to historical reads.
* Only deletions that actually happen appear in the change feed, and the
  current index projection follows the deletions.
* A sweep whose selected revision was rewritten between candidate selection
  and the delete commit conflicts the whole batch: it does not retry against
  a fresh candidate set, does not advance the head, and emits no delete
  commit. This holds when the rewritten value is still expired and when a
  key was deleted and recreated with the same body/expiry.
* A write to a key that was not selected does not conflict the sweep.
* If cleanup commits first and a same-key write-back arrives afterwards, the
  old sweep request has no effect on the new version.

How ordering is forced
----------------------
The race checks wrap ``Retention._expired_candidates`` on one handle. The
wrapper runs the selection, records that the revision is still the seeded
head (no deletion applied), lets a second ``Database`` handle commit its
rewrite from another thread, and only then returns the already-selected
rows. ``sweep`` subsequently builds its conditional delete from those rows.

This observation point does not create, merge, or extend a SQLite
transaction: ``_expired_candidates`` has already left its ``BEGIN`` read
transaction (and rolled it back) before the wrapper runs the other handle,
and ``WriteBatch.commit`` has not yet issued ``BEGIN IMMEDIATE``. The wrapper
only deterministically fills the natural gap between the read transaction
and the write transaction, which is exactly the production window. No
``sleep`` is used; ``threading.Event`` handshakes and the recorded trace
establish the order, and their timeouts are deadlock guards only. The
cleanup-first case needs no seam: the two commits are sequential and their
revisions prove the order.

Interleavings not covered here
------------------------------
* A second writer blocked by the sweep's ``BEGIN IMMEDIATE`` lock
  (contention/busy-timeout behaviour) or a commit landing while
  ``WriteBatch.commit`` is validating.
* Two concurrent sweeps, or a sweep racing ``Retention.compact`` / a floor
  change between selection and commit.
* Same-handle multi-thread access, external-process writers, and process
  crashes between selection and commit.
* Rewrites that keep the same revision are impossible by construction, so
  only revision-changing writes are exercised.
* Schema/catalog changes between selection and commit (they do not touch
  selected versions and are expected not to conflict, but are untested here).
* Pin/consumer watermarks: ``sweep`` deliberately deletes current data
  regardless of retention floor; that interaction is not re-tested.
"""
import json
import threading
import tempfile
from pathlib import Path

from strataleaf import Conflict, Database


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def view(db, collection, key, revision=None):
    row = db.get(collection, key, revision)
    if row is None:
        return None
    return {"revision": row.revision, "document": row.document, "expires_at": row.expires_at}


def operations(commit):
    return [(item["collection"], item["key"], item["document"], item["expected"])
            for item in commit["operations"]]


def feed_operations(db, since):
    return [(commit["revision"], operations(commit)) for commit in db.feed.read(since)]


def expect_conflict(call, label):
    try:
        call()
    except Conflict as exc:
        return str(exc)
    raise AssertionError(label + " unexpectedly succeeded instead of conflicting")


def indexed_keys(db, collection, value):
    predicate = {"field": "tag", "op": "eq", "value": value}
    page = db.query(collection, predicate)
    scanned = db.query(collection, predicate, force_scan=True)
    require(page.plan["access"] == "equality_index", "equality index was not used for " + value)
    require([row.key for row in page.records] == [row.key for row in scanned.records],
            "index and version scan disagree for " + value)
    return [row.key for row in page.records]


def selection_and_ordering(directory):
    """The long acceptance sequence: superseded versions must not be candidates."""
    path = directory / "selection.db"
    with Database(path) as db:
        db.catalog.create("orders")
        db.catalog.create("hold")
        r_orders = db.batch("seed_orders").put(
            "orders", "a", {"tag": "a1", "state": "old"}, expires_at=10).put(
            "orders", "b", {"tag": "b1", "state": "old"}, expires_at=20).put(
            "orders", "c", {"tag": "c1", "state": "old"}, expires_at=30).put(
            "orders", "d", {"tag": "d1", "state": "old"}, expires_at=100).commit().revision
        r_hold = db.batch("seed_hold").put(
            "hold", "a", {"tag": "h1", "state": "old"}, expires_at=100).commit().revision
        r_life = db.batch("lifecycle").put(
            "orders", "a", {"tag": "a2", "state": "renewed"}, expected=r_orders, expires_at=200).put(
            "orders", "b", {"tag": "b2", "state": "cleared"}, expected=r_orders, expires_at=None).delete(
            "orders", "c", expected=r_orders).commit().revision

        db.indexes.create("orders", "by_tag", "tag")
        db.indexes.create("hold", "by_tag", "tag")

        tracked = (("orders", "a"), ("orders", "b"), ("orders", "c"), ("orders", "d"), ("hold", "a"))
        before = {(collection, key): view(db, collection, key) for collection, key in tracked}
        require(before[("orders", "a")] == {"revision": r_life, "document": {"tag": "a2", "state": "renewed"},
                                            "expires_at": 200.0}, "renewed value was not current before sweep")
        require(before[("orders", "b")]["expires_at"] is None, "cleared expiry was not current")
        require(before[("orders", "c")] is None, "explicit deletion was not current")
        require(indexed_keys(db, "orders", "a1") == [], "historical body leaked into the current index")
        require(indexed_keys(db, "orders", "a2") == ["a"], "current index lost a live document")
        require(indexed_keys(db, "hold", "h1") == ["a"], "current index lost the hold document")

        # Time alone never changes reads, and a sweep below the boundary is idle.
        head = db.revision
        idle = db.retention.sweep(99)
        require(idle == {"expired": 0, "revision": None}, "sweep below the expiry boundary was not idle: " + repr(idle))
        require(db.revision == head, "idle sweep advanced the head")
        require(all(view(db, collection, key) == before[(collection, key)] for collection, key in tracked),
                "idle sweep changed current reads")

        # Ordering is (expires, collection, key): hold/a beats orders/d at the same time.
        first = db.retention.sweep(100, limit=1)
        require(first == {"expired": 1, "revision": r_life + 1}, "first sweep result is wrong: " + repr(first))
        require(view(db, "hold", "a") is None, "first sweep did not delete hold/a")
        require(view(db, "orders", "a") == before[("orders", "a")], "first sweep touched a renewed document")
        require(view(db, "orders", "b") == before[("orders", "b")], "first sweep touched a document with cleared expiry")
        require(view(db, "orders", "c") is None and view(db, "orders", "c", r_orders)["revision"] == r_orders,
                "first sweep disturbed an existing tombstone")
        require(view(db, "orders", "d") == before[("orders", "d")], "first sweep deleted d before hold/a")
        first_feed = feed_operations(db, r_life)
        require(first_feed == [(r_life + 1, [("hold", "a", None, r_hold)])],
                "first sweep feed is wrong: " + repr(first_feed))
        require(indexed_keys(db, "hold", "h1") == [], "current index kept the deleted hold document")

        second = db.retention.sweep(100, limit=1)
        require(second == {"expired": 1, "revision": r_life + 2}, "second sweep result is wrong: " + repr(second))
        require(view(db, "orders", "d") is None, "second sweep did not delete orders/d")
        require(view(db, "orders", "d", r_orders) == before[("orders", "d")], "historical read of a swept document changed")
        second_feed = feed_operations(db, r_life)
        require(second_feed[1] == (r_life + 2, [("orders", "d", None, r_orders)]),
                "second sweep feed is wrong: " + repr(second_feed))

        head, feed_length = db.revision, len(second_feed)
        third = db.retention.sweep(100, limit=1)
        require(third == {"expired": 0, "revision": None}, "third sweep was not a no-op: " + repr(third))
        require(db.revision == head and len(feed_operations(db, r_life)) == feed_length,
                "third sweep advanced the head or emitted a commit")

        # Historical reads retain body, revision, and expiry exactly as written.
        history = {
            ("orders", "a"): {"revision": r_orders, "document": {"tag": "a1", "state": "old"}, "expires_at": 10.0},
            ("orders", "b"): {"revision": r_orders, "document": {"tag": "b1", "state": "old"}, "expires_at": 20.0},
            ("orders", "c"): {"revision": r_orders, "document": {"tag": "c1", "state": "old"}, "expires_at": 30.0},
            ("orders", "d"): {"revision": r_orders, "document": {"tag": "d1", "state": "old"}, "expires_at": 100.0},
            ("hold", "a"): {"revision": r_hold, "document": {"tag": "h1", "state": "old"}, "expires_at": 100.0},
        }
        for (collection, key), expected in history.items():
            require(view(db, collection, key, expected["revision"]) == expected,
                    f"historical read changed for {collection}/{key}@{expected['revision']}")
        require(view(db, "orders", "a")["document"] == {"tag": "a2", "state": "renewed"}, "renewed value disappeared")
        require(view(db, "orders", "b")["expires_at"] is None, "cleared expiry changed")
        require(indexed_keys(db, "orders", "a2") == ["a"] and indexed_keys(db, "orders", "b2") == ["b"],
                "current index lost live documents")
        require(indexed_keys(db, "orders", "d1") == [], "current index kept a swept document")
        inspected = db.diagnostics.inspect(deep=True)
        require(inspected["ok"], "deep inspection failed after sweep: " + repr(inspected["issues"]))

        return {
            "revisions": {"orders": r_orders, "hold": r_hold, "lifecycle": r_life},
            "sweeps": [first, second, third],
            "feed": feed_operations(db, r_life),
            "current": {f"{collection}/{key}": view(db, collection, key) for collection, key in tracked},
            "history_preserved": len(history),
            "index": {"a2": indexed_keys(db, "orders", "a2"), "d1": indexed_keys(db, "orders", "d1"),
                      "h1": indexed_keys(db, "hold", "h1")},
            "inspection_ok": inspected["ok"],
        }


class Seam:
    """Insert one second-handle write between selection and the delete batch."""

    def __init__(self, handle, expected_keys, expected_head):
        self.handle = handle
        self.expected_keys = expected_keys
        self.expected_head = expected_head
        self.original = handle.retention._expired_candidates
        self.selected = threading.Event()
        self.finished = threading.Event()
        self.trace = {}
        self.outcome = {}

    def _seam(self, now, limit):
        rows = self.original(now, limit)
        selected = [(row["collection"], row["key"]) for row in rows]
        self.trace["candidates"] = [(row["collection"], row["key"], row["rev"]) for row in rows]
        self.trace["head_at_selection"] = self.handle.revision
        self.trace["feed_at_selection"] = len(self.handle.feed.read(self.expected_head))
        require(selected == [("t", key) for key in self.expected_keys],
                "unexpected candidate set: " + repr(selected))
        require(all(row["rev"] == self.expected_head for row in rows),
                "candidates were not selected at the seeded head")
        self.selected.set()
        if not self.finished.wait(5):
            raise AssertionError("rewrite thread did not finish; deadlock guard expired")
        self.trace["head_at_delete"] = self.handle.revision
        return rows

    def _writer(self, writing_handle, rewrite):
        try:
            if not self.selected.wait(5):
                raise AssertionError("selection event was not observed; deadlock guard expired")
            head = writing_handle.revision
            self.trace["pre_rewrite"] = {
                "head": head,
                "current": {key: (view(writing_handle, "t", key) or {}).get("revision")
                            for key in self.expected_keys},
                "feed": len(writing_handle.feed.read(self.expected_head)),
            }
            self.outcome["revisions"] = rewrite(writing_handle, head)
        except BaseException as exc:  # surfaced after the sweep returns
            self.outcome["error"] = exc
        finally:
            self.finished.set()

    def run(self, writing_handle, rewrite, limit):
        self.handle.retention._expired_candidates = self._seam
        thread = threading.Thread(target=self._writer, args=(writing_handle, rewrite), daemon=True)
        thread.start()
        try:
            try:
                result, conflict = self.handle.retention.sweep(1, limit=limit), None
            except Conflict as exc:
                result, conflict = None, str(exc)
        finally:
            thread.join(5)
            self.handle.retention._expired_candidates = self.original
        if "error" in self.outcome:
            raise self.outcome["error"]
        require(not thread.is_alive(), "rewrite thread did not terminate")
        return result, conflict, self.trace


def race_case(directory, name, keys, limit, rewrite, conflicts):
    path = directory / (name + ".db")
    with Database(path) as handle, Database(path) as writer:
        handle.catalog.create("t")
        seed = handle.batch()
        for index in range(keys):
            seed.put("t", f"k{index + 1}", {"state": f"old{index + 1}"}, expires_at=1)
        head = seed.commit().revision
        expected_keys = [f"k{index + 1}" for index in range(min(limit, keys))]
        seam = Seam(handle, expected_keys, head)
        result, conflict, trace = seam.run(writer, rewrite, limit)
        revisions = seam.outcome["revisions"]
        last_writer = revisions[-1]

        require(trace["head_at_selection"] == head, "selection was not made at the seeded head")
        require(trace["feed_at_selection"] == 0, "a commit existed before candidate selection")
        require(trace["pre_rewrite"]["head"] == head,
                "rewrite thread ran after cleanup had already advanced the head")
        require(all(revision == head for revision in trace["pre_rewrite"]["current"].values()),
                "a selected key was already rewritten or deleted before the injected rewrite")
        require(trace["pre_rewrite"]["feed"] == 0,
                "a delete commit was already visible before the injected rewrite")
        require(trace["head_at_delete"] == last_writer,
                "delete batch was prepared before the rewrite commit landed")
        feed = feed_operations(handle, head)
        require([revision for revision, _ in feed] == revisions + ([] if conflicts else [result["revision"]]),
                "change feed contains commits other than the rewrite and the sweep: " + repr(feed))

        if conflicts:
            require(conflict is not None, "sweep did not conflict after its selected revision changed: " + repr(result))
            require(handle.revision == last_writer,
                    "conflicted sweep advanced the head beyond the rewrite revision")
            require(view(handle, "t", expected_keys[1])["revision"] == head,
                    "an unchanged selected key was deleted by the conflicted sweep")
        else:
            require(conflict is None and result is not None,
                    "sweep conflicted although only an unselected key changed: " + repr(conflict))
            require(result["expired"] == limit, "sweep did not delete every selected candidate: " + repr(result))
            require(result["revision"] == last_writer + 1, "sweep revision is not the next commit")
            require(all(view(handle, "t", key) is None for key in expected_keys),
                    "selected keys survived a successful sweep")

        if name == "renew_still_expired":
            require(view(handle, "t", "k1") == {"revision": last_writer, "document": {"state": "new1"},
                                                "expires_at": 1.0},
                    "a still-expired rewrite was not protected by the revision conflict")
            require(view(handle, "t", "k1", head)["document"] == {"state": "old1"},
                    "historical value of the rewritten key changed")
            fresh = handle.retention.sweep(1)
            require(fresh == {"expired": 2, "revision": last_writer + 1},
                    "a fresh sweep did not handle both still-expired keys: " + repr(fresh))
            require(view(handle, "t", "k1") is None and view(handle, "t", "k2") is None,
                    "fresh sweep did not delete both still-expired keys")
        elif name == "renew_extended":
            require(view(handle, "t", "k1") == {"revision": last_writer, "document": {"state": "new1"},
                                                "expires_at": 999.0},
                    "extended expiry was not protected")
            fresh = handle.retention.sweep(1)
            require(fresh == {"expired": 1, "revision": last_writer + 1},
                    "fresh sweep did not delete only the still-expired key: " + repr(fresh))
            require(view(handle, "t", "k1")["revision"] == last_writer, "fresh sweep deleted the extended key")
        elif name == "delete_recreate":
            require(view(handle, "t", "k1") == {"revision": last_writer, "document": {"state": "old1"},
                                                "expires_at": 1.0},
                    "recreated value was not current")
            require([revision for revision, _ in feed] == revisions,
                    "recreated value did not produce the expected commit sequence")
            fresh = handle.retention.sweep(1)
            require(fresh == {"expired": 2, "revision": last_writer + 1},
                    "fresh sweep did not treat the recreated key as a new candidate: " + repr(fresh))
        elif name == "unselected_rewrite":
            require(view(handle, "t", "k3") == {"revision": last_writer, "document": {"state": "new3"},
                                                "expires_at": 999.0},
                    "unselected rewrite was lost")
            require(view(handle, "t", "k3", head)["document"] == {"state": "old3"},
                    "historical value of the unselected key changed")
            fresh = handle.retention.sweep(1)
            require(fresh == {"expired": 0, "revision": None},
                    "unexpected fresh candidate after an unselected rewrite: " + repr(fresh))

        return {"case": name, "head": head, "trace": trace, "conflict": conflict, "sweep": result,
                "feed": feed,
                "current": {f"k{index + 1}": view(handle, "t", f"k{index + 1}") for index in range(keys)}}


def cleanup_then_recreate(directory):
    """Cleanup wins first; a same-key write-back afterwards is a fresh version.

    No seam is used. The first write batch is prepared from the pre-cleanup
    revision, cleanup commits, and only then is that batch submitted; the
    revisions prove cleanup happened first.
    """
    path = directory / "cleanup_first.db"
    with Database(path) as handle, Database(path) as writer:
        handle.catalog.create("t")
        head = handle.batch().put("t", "k1", {"state": "old1"}, expires_at=1).put(
            "t", "k2", {"state": "old2"}, expires_at=1).commit().revision
        pending = writer.batch().put("t", "k1", {"state": "old1"}, expected=head, expires_at=1)

        swept = handle.retention.sweep(1)
        require(swept == {"expired": 2, "revision": head + 1},
                "cleanup-first sweep did not delete both expired keys: " + repr(swept))
        require(handle.revision == head + 1, "cleanup did not commit before the write-back was submitted")
        stale = expect_conflict(pending.commit, "write-back prepared before cleanup")

        recreated = writer.batch().put("t", "k1", {"state": "old1"}, expected=head + 1, expires_at=1).commit()
        require(recreated.revision == head + 2 and recreated.count == 1,
                "same-key write-back did not become a fresh version")
        require(view(handle, "t", "k1") == {"revision": head + 2, "document": {"state": "old1"},
                                            "expires_at": 1.0},
                "old cleanup request affected the recreated value")
        require(view(handle, "t", "k2") is None, "cleanup did not delete the other expired key")
        feed = feed_operations(handle, head)
        require(feed == [(head + 1, [("t", "k1", None, head), ("t", "k2", None, head)]),
                         (head + 2, [("t", "k1", {"state": "old1"}, head + 1)])],
                "cleanup-first feed is wrong: " + repr(feed))
        inspected = handle.diagnostics.inspect(deep=True)
        require(inspected["ok"], "deep inspection failed: " + repr(inspected["issues"]))
        return {"case": "cleanup_then_recreate", "head": head, "sweep": swept, "stale_write": stale,
                "recreate": recreated.__dict__, "feed": feed,
                "current": {"k1": view(handle, "t", "k1"), "k2": view(handle, "t", "k2")}}


def _delete_recreate(db, head):
    deleted = db.batch().delete("t", "k1", expected=head).commit().revision
    recreated = db.batch().put("t", "k1", {"state": "old1"}, expected=deleted, expires_at=1).commit().revision
    return [deleted, recreated]


def run():
    with tempfile.TemporaryDirectory(prefix="strataleaf-retention-regression-") as directory:
        folder = Path(directory)
        output = {
            "selection_and_ordering": selection_and_ordering(folder),
            "races": [
                race_case(folder, "renew_still_expired", keys=2, limit=2,
                          rewrite=lambda db, head: [db.batch().put(
                              "t", "k1", {"state": "new1"}, expected=head, expires_at=1).commit().revision],
                          conflicts=True),
                race_case(folder, "renew_extended", keys=2, limit=2,
                          rewrite=lambda db, head: [db.batch().put(
                              "t", "k1", {"state": "new1"}, expected=head, expires_at=999).commit().revision],
                          conflicts=True),
                race_case(folder, "delete_recreate", keys=2, limit=2,
                          rewrite=_delete_recreate, conflicts=True),
                race_case(folder, "unselected_rewrite", keys=3, limit=2,
                          rewrite=lambda db, head: [db.batch().put(
                              "t", "k3", {"state": "new3"}, expected=head, expires_at=999).commit().revision],
                          conflicts=False),
            ],
            "cleanup_then_recreate": cleanup_then_recreate(folder),
        }
        print(json.dumps(output, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    run()
