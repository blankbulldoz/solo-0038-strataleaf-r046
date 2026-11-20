from .codec import digest, field, loads, scalar_token
from .mvcc import visible_rows


class Diagnostics:
    def __init__(self, store):
        self.store = store

    def inspect(self, deep=False):
        with self.store.read() as connection:
            head, floor = int(self.store.get("head")), int(self.store.get("floor"))
            integrity = [row[0] for row in connection.execute("PRAGMA integrity_check" if deep else "PRAGMA quick_check")]
            references = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
            counts = {table: connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                      for table in ("collections", "versions", "commits", "index_defs", "index_values", "pins", "consumers")}
            revisions = [row[0] for row in connection.execute("SELECT rev FROM commits WHERE rev>? ORDER BY rev", (floor,))]
            issues = []
            if revisions != list(range(floor + 1, head + 1)):
                issues.append("retained commit range is not contiguous")
            if references:
                issues.append("broken foreign keys")
            if integrity != ["ok"]:
                issues.append("SQLite physical integrity failure")
            if not 0 <= floor <= head:
                issues.append("invalid revision watermarks")
            for pin in connection.execute("SELECT name,rev FROM pins"):
                if not floor <= pin["rev"] <= head:
                    issues.append("read pin outside retained history: " + pin["name"])
            if deep:
                expected = set()
                for definition in connection.execute("SELECT * FROM index_defs"):
                    tokens = set()
                    for row in visible_rows(connection, definition["collection"], head):
                        token = scalar_token(field(loads(row["body"]), definition["field"]))
                        if token is None:
                            continue
                        if definition["unique_flag"] and token in tokens:
                            issues.append("unique index has duplicate source values: " + definition["name"])
                        tokens.add(token)
                        expected.add((definition["collection"], definition["name"], token, row["key"]))
                actual = {tuple(row) for row in connection.execute("SELECT collection,name,token,key FROM index_values")}
                if expected != actual:
                    issues.append("index entries differ from current visible documents")
            return {"ok": not issues, "issues": issues, "head": head, "floor": floor,
                    "database_id": self.store.get("database_id"), "mode": self.store.get("mode"),
                    "counts": counts, "sqlite": integrity, "foreign_keys": references}

    def fingerprint(self, revision=None):
        from .mvcc import checked_revision
        with self.store.read() as connection:
            revision = checked_revision(self.store, revision)
            content = []
            for collection in connection.execute("SELECT name FROM collections ORDER BY name"):
                content.extend({"collection": collection[0], "key": row["key"], "revision": row["rev"],
                                "document": loads(row["body"]), "expires_at": row["expires"]}
                               for row in visible_rows(connection, collection[0], revision))
            return {"revision": revision, "documents": len(content), "sha256": digest(content)}
