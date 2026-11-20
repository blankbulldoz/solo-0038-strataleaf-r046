from .codec import loads, name
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import checked_revision


class ChangeFeed:
    def __init__(self, store):
        self.store = store

    def read(self, since, limit=100):
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("feed limit must be between 1 and 1000 commits")
        with self.store.read() as connection:
            checked_revision(self.store, since)
            return [loads(row[0]) for row in connection.execute("SELECT payload FROM commits WHERE rev>? ORDER BY rev LIMIT ?", (since, limit))]

    def register(self, consumer, since=None):
        name(consumer)
        with self.store.atomic() as connection:
            since = checked_revision(self.store, since)
            if connection.execute("SELECT 1 FROM consumers WHERE name=?", (consumer,)).fetchone():
                raise Conflict("consumer already registered")
            connection.execute("INSERT INTO consumers VALUES (?,?)", (consumer, since))
        return since

    def position(self, consumer):
        with self.store.read() as connection:
            row = connection.execute("SELECT ack FROM consumers WHERE name=?", (consumer,)).fetchone()
            if row is None:
                raise NotFound("unknown consumer")
            return row[0]

    def acknowledge(self, consumer, revision):
        with self.store.atomic() as connection:
            revision = checked_revision(self.store, revision)
            row = connection.execute("SELECT ack FROM consumers WHERE name=?", (consumer,)).fetchone()
            if row is None:
                raise NotFound("unknown consumer")
            if revision < row[0]:
                raise Conflict("consumer acknowledgement cannot move backwards")
            connection.execute("UPDATE consumers SET ack=? WHERE name=?", (revision, consumer))
        return revision

    def release(self, consumer):
        with self.store.atomic() as connection:
            if not connection.execute("DELETE FROM consumers WHERE name=?", (consumer,)).rowcount:
                raise NotFound("unknown consumer")
