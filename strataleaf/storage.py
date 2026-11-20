import contextlib
import secrets
import sqlite3
import threading
import uuid
from pathlib import Path
from .errors import DatabaseError, ReadOnly

DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS collections (name TEXT PRIMARY KEY, schema TEXT NOT NULL, generation INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS commits (rev INTEGER PRIMARY KEY, payload TEXT NOT NULL, request_key TEXT UNIQUE, request_hash TEXT);
CREATE TABLE IF NOT EXISTS receipts (request_key TEXT PRIMARY KEY, rev INTEGER NOT NULL, request_hash TEXT NOT NULL, count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS versions (
 collection TEXT NOT NULL, key TEXT NOT NULL, rev INTEGER NOT NULL, body TEXT,
 expires REAL, PRIMARY KEY(collection,key,rev), FOREIGN KEY(collection) REFERENCES collections(name));
CREATE INDEX IF NOT EXISTS version_revision ON versions(rev);
CREATE TABLE IF NOT EXISTS index_defs (
 collection TEXT NOT NULL, name TEXT NOT NULL, field TEXT NOT NULL, unique_flag INTEGER NOT NULL,
 PRIMARY KEY(collection,name), FOREIGN KEY(collection) REFERENCES collections(name));
CREATE TABLE IF NOT EXISTS index_values (
 collection TEXT NOT NULL, name TEXT NOT NULL, token TEXT NOT NULL, key TEXT NOT NULL,
 PRIMARY KEY(collection,name,token,key), FOREIGN KEY(collection,name) REFERENCES index_defs(collection,name) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS index_document ON index_values(collection,key);
CREATE TABLE IF NOT EXISTS pins (name TEXT PRIMARY KEY, rev INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS consumers (name TEXT PRIMARY KEY, ack INTEGER NOT NULL);
"""


class Storage:
    def __init__(self, path, timeout=5.0):
        self.path = str(path)
        self.lock = threading.RLock()
        self.closed = False
        self.connection = sqlite3.connect(self.path, timeout=timeout, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(DDL)
        with self.atomic():
            initial = {"format": "1", "head": "0", "floor": "0", "database_id": str(uuid.uuid4()),
                       "cursor_secret": secrets.token_hex(32), "mode": "primary", "upstream": "", "catalog_epoch": "0"}
            for key, value in initial.items():
                self.connection.execute("INSERT OR IGNORE INTO meta VALUES (?,?)", (key, value))
            if self.get("format") != "1":
                raise DatabaseError("unsupported on-disk format")

    def get(self, key):
        row = self.connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            raise DatabaseError("missing storage metadata: " + key)
        return row[0]

    def set(self, key, value):
        self.connection.execute("UPDATE meta SET value=? WHERE key=?", (str(value), key))

    @contextlib.contextmanager
    def atomic(self, writable=False):
        with self.lock:
            if self.closed:
                raise DatabaseError("database handle is closed")
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                if writable and self.get("mode") != "primary":
                    raise ReadOnly("replica rejects primary writes")
                yield self.connection
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise

    @contextlib.contextmanager
    def read(self):
        with self.lock:
            if self.closed:
                raise DatabaseError("database handle is closed")
            self.connection.execute("BEGIN")
            try:
                yield self.connection
            finally:
                self.connection.rollback()

    def checkpoint(self):
        with self.lock:
            if self.closed:
                raise DatabaseError("database handle is closed")
            return tuple(self.connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone())

    def close(self):
        with self.lock:
            if not self.closed:
                self.connection.close()
                self.closed = True
