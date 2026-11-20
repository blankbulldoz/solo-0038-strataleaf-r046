from .codec import dumps, loads
from .errors import InvalidDocument, NotFound
from .mvcc import checked_revision, record, visible_rows
from .transaction import WriteBatch


class Transfer:
    def __init__(self, store):
        self.store = store

    def export_lines(self, collection, revision=None):
        with self.store.read() as connection:
            revision = checked_revision(self.store, revision)
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection")
            output = []
            for row in visible_rows(connection, collection, revision):
                item = record(row)
                output.append(dumps({"key": item.key, "document": item.document, "expires_at": item.expires_at}))
            return {"revision": revision, "lines": output}

    def import_lines(self, collection, lines, replace=False, request_key=None):
        if not isinstance(replace, bool):
            raise InvalidDocument("replace must be a boolean")
        batch = WriteBatch(self.store, request_key)
        count = 0
        for line in lines:
            count += 1
            if count > 10000 or not isinstance(line, str) or len(line.encode()) > 2 * 1024 * 1024:
                raise InvalidDocument("import exceeds bounded batch/line size")
            try:
                item = loads(line)
            except (ValueError, TypeError) as exc:
                raise InvalidDocument("invalid JSON at input line " + str(count)) from exc
            if not isinstance(item, dict) or set(item) - {"key", "document", "expires_at"} or not {"key", "document"}.issubset(item):
                raise InvalidDocument("line requires key and document, optional expires_at")
            batch.put(collection, item["key"], item["document"], expected=None if replace else 0,
                      expires_at=item.get("expires_at"))
        return batch.commit()
