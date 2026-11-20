from dataclasses import dataclass
from .codec import digest
from .cursors import decode_cursor, encode_cursor
from .errors import InvalidCursor, InvalidDocument, NotFound
from .indexes import equality_candidates
from .mvcc import checked_revision, record, visible_rows, latest_row
from .predicates import equality_terms, matches, validate_predicate


@dataclass(frozen=True)
class Page:
    records: list
    revision: int
    cursor: str | None
    plan: dict


class QueryEngine:
    def __init__(self, store):
        self.store = store

    def select(self, collection, predicate=None, limit=100, cursor=None, revision=None, force_scan=False):
        validate_predicate(predicate)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("page size must be between 1 and 1000")
        with self.store.read() as connection:
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection")
            fingerprint = digest({"collection": collection, "predicate": predicate})
            after = None
            if cursor is not None:
                payload = decode_cursor(self.store, cursor)
                if payload.get("query") != fingerprint or (revision is not None and revision != payload.get("revision")):
                    raise InvalidCursor("cursor does not belong to this query/read revision")
                revision, after = payload.get("revision"), payload.get("after")
                if not isinstance(after, str):
                    raise InvalidCursor("cursor has no key boundary")
            revision = checked_revision(self.store, revision)
            plan = {"access": "version_scan", "index": None}
            candidates = None
            if not force_scan and revision == int(self.store.get("head")):
                for path, value in equality_terms(predicate):
                    indexed = equality_candidates(connection, collection, path, value)
                    if indexed is not None:
                        index_name, candidates = indexed
                        plan = {"access": "equality_index", "index": index_name}
                        break
            if candidates is None:
                rows = visible_rows(connection, collection, revision)
            else:
                rows = [latest_row(connection, collection, key, revision) for key in sorted(candidates)]
            output = []
            examined = 0
            for row in rows:
                if row is None or row["body"] is None or (after is not None and row["key"] <= after):
                    continue
                examined += 1
                item = record(row)
                if matches(item.document, predicate):
                    output.append(item)
                    if len(output) > limit:
                        break
            continuation = None
            if len(output) > limit:
                output = output[:limit]
                continuation = encode_cursor(self.store, {"database": self.store.get("database_id"),
                    "query": fingerprint, "revision": revision, "after": output[-1].key})
            return Page(output, revision, continuation, {**plan, "examined": examined})
