"""Reproduce operational boundaries through real temporary database operations."""
import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from strataleaf import Database
from .workflows import seed


def contention(folder):
    with Database(folder / 'observe.db', timeout=0.03) as db:
        seed(db)
        holder = sqlite3.connect(folder / 'observe.db', isolation_level=None)
        try:
            holder.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            try:
                db.batch().delete('segments', 'seg_00').commit()
            except sqlite3.OperationalError as exc:
                blocked = {'exception': type(exc).__name__, 'message': str(exc),
                           'elapsed_ms': round((time.monotonic() - started) * 1000, 2)}
            else:
                raise RuntimeError('expected SQLite writer contention was not observed')
        finally:
            holder.rollback()
            holder.close()
        result = db.batch().delete('segments', 'seg_00').commit()
        return {'blocked_write': blocked, 'after_release': result.__dict__, 'inspection': db.diagnostics.inspect(True)}


def retention_pressure(folder):
    with Database(folder / 'observe.db') as db:
        first = seed(db)
        db.retention.pin('long_reader', first)
        db.feed.register('offline_replica', first)
        for value in range(3):
            db.batch().put('segments', 'seg_00', {'region': 'east', 'sequence': value + 10}).commit()
        both = db.retention.compact(db.revision)
        db.retention.unpin('long_reader')
        one = db.retention.compact(db.revision)
        db.feed.acknowledge('offline_replica', db.revision)
        none = db.retention.compact(db.revision)
        return {'both_holds': both, 'one_hold': one, 'released': none, 'inspection': db.diagnostics.inspect(True)}


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='strataleaf-observation-') as directory:
        operation = {'contention': contention, 'retention_pressure': retention_pressure}[sys.argv[1]]
        print(json.dumps(operation(Path(directory)), ensure_ascii=False, sort_keys=True))
