"""
Exercises the delta-sync SQL (sync_delta_queue table, version triggers, queue queries and migration 14)
against a real SQLite engine. Run with:  python3 data/src/test/sql/test_delta_sync_sql.py
"""
import re
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from schema_loader import fresh_db, migration_sql, queries, schema_sql  # noqa: E402

Q = queries("sync_delta_queue")


def add_manga(c, url="/m", source=1, favorite=0, version=0, description="d"):
    c.execute(
        "INSERT INTO mangas(source,url,title,status,favorite,initialized,viewer,chapter_flags,"
        "cover_last_modified,date_added,version,description) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (source, url, "t", 0, favorite, 1, 0, 0, 0, 0, version, description),
    )
    return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def add_chapter(c, manga_id, url="/c", version=0):
    c.execute(
        "INSERT INTO chapters(manga_id,url,name,read,bookmark,last_page_read,chapter_number,source_order,"
        "date_fetch,date_upload,version) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (manga_id, url, "c", 0, 0, 0, 1.0, 0, 0, 0, version),
    )
    return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def queue(c):
    return c.execute("SELECT table_name, record_id, status, attempt_count FROM sync_delta_queue ORDER BY _id").fetchall()


def run(c, name, **params):
    return c.execute(Q[name], params)


class TriggerTests(unittest.TestCase):
    def setUp(self):
        self.c = fresh_db()
        self.m = add_manga(self.c)
        self.ch = add_chapter(self.c, self.m)

    def test_inserts_do_not_enqueue(self):
        self.assertEqual(queue(self.c), [])

    def test_manga_change_bumps_version_and_enqueues_once(self):
        self.c.execute("UPDATE mangas SET favorite = 1 WHERE _id = ?", (self.m,))
        self.c.execute("UPDATE mangas SET description = 'x' WHERE _id = ?", (self.m,))
        self.assertEqual(self.c.execute("SELECT version FROM mangas").fetchone()[0], 2)
        self.assertEqual(queue(self.c), [("mangas", self.m, "PENDING", 0)])

    def test_null_description_change_is_detected(self):
        m = add_manga(self.c, url="/n", description=None)
        self.c.execute("UPDATE mangas SET description = 'new' WHERE _id = ?", (m,))
        self.assertEqual(self.c.execute("SELECT version FROM mangas WHERE _id=?", (m,)).fetchone()[0], 1)
        self.assertIn(("mangas", m, "PENDING", 0), queue(self.c))

    def test_untracked_column_does_not_enqueue(self):
        self.c.execute("UPDATE mangas SET title = 'other' WHERE _id = ?", (self.m,))
        self.assertEqual(queue(self.c), [])

    def test_chapter_change_enqueues_chapter_and_manga(self):
        self.c.execute("UPDATE chapters SET read = 1 WHERE _id = ?", (self.ch,))
        self.assertEqual(
            sorted(queue(self.c)),
            sorted([("chapters", self.ch, "PENDING", 0), ("mangas", self.m, "PENDING", 0)]),
        )
        self.assertEqual(self.c.execute("SELECT version FROM chapters").fetchone()[0], 1)
        self.assertEqual(self.c.execute("SELECT version FROM mangas").fetchone()[0], 1)

    def test_is_syncing_suppresses_everything(self):
        """The pull job's write pattern: raise the flag, write, lower the flag."""
        self.c.execute("BEGIN")
        self.c.execute("UPDATE mangas SET is_syncing = 1 WHERE _id = ?", (self.m,))
        self.c.execute("UPDATE mangas SET favorite = 1, description = 'remote', url = '/moved', version = 9 WHERE _id = ?", (self.m,))
        self.c.execute("INSERT INTO categories(name,sort,flags) VALUES('Reading',1,0)")
        cat = self.c.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.c.execute("INSERT INTO mangas_categories(manga_id,category_id) VALUES(?,?)", (self.m, cat))
        self.c.execute("DELETE FROM mangas_categories WHERE manga_id = ?", (self.m,))
        self.c.execute("UPDATE chapters SET is_syncing = 1 WHERE _id = ?", (self.ch,))
        self.c.execute("UPDATE chapters SET read = 1, bookmark = 1, last_page_read = 5, version = 4 WHERE _id = ?", (self.ch,))
        self.c.execute("UPDATE chapters SET is_syncing = 0 WHERE _id = ?", (self.ch,))
        self.c.execute("UPDATE mangas SET is_syncing = 0 WHERE _id = ?", (self.m,))
        self.c.execute("COMMIT")
        self.assertEqual(queue(self.c), [])
        self.assertEqual(self.c.execute("SELECT version FROM mangas").fetchone()[0], 9)
        self.assertEqual(self.c.execute("SELECT version FROM chapters").fetchone()[0], 4)
        self.assertEqual(self.c.execute("SELECT is_syncing FROM mangas").fetchone()[0], 0)

    def test_without_flag_the_same_writes_would_loop(self):
        """Sanity check for the test above: proves the flag (not the data) is what suppresses."""
        self.c.execute("UPDATE mangas SET favorite = 1, version = 9 WHERE _id = ?", (self.m,))
        self.assertEqual(self.c.execute("SELECT version FROM mangas").fetchone()[0], 10)
        self.assertEqual(len(queue(self.c)), 1)

    def test_category_changes_enqueue(self):
        self.c.execute("INSERT INTO categories(name,sort,flags) VALUES('A',1,0)")
        cat = self.c.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.c.execute("INSERT INTO mangas_categories(manga_id,category_id) VALUES(?,?)", (self.m, cat))
        self.assertEqual(queue(self.c), [("mangas", self.m, "PENDING", 0)])
        self.c.execute("DELETE FROM sync_delta_queue")
        self.c.execute("DELETE FROM mangas_categories WHERE manga_id = ?", (self.m,))
        self.assertEqual(queue(self.c), [("mangas", self.m, "PENDING", 0)])

    def test_deleting_manga_does_not_enqueue_cascaded_category_rows(self):
        self.c.execute("INSERT INTO categories(name,sort,flags) VALUES('A',1,0)")
        cat = self.c.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.c.execute("INSERT INTO mangas_categories(manga_id,category_id) VALUES(?,?)", (self.m, cat))
        self.c.execute("DELETE FROM sync_delta_queue")
        self.c.execute("DELETE FROM mangas WHERE _id = ?", (self.m,))
        self.assertEqual(queue(self.c), [])


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.c = fresh_db()
        self.m = add_manga(self.c)
        self.ch = add_chapter(self.c, self.m)
        self.c.execute("UPDATE chapters SET read = 1 WHERE _id = ?", (self.ch,))  # queues chapter + manga

    def claim(self, batch="b1", limit=100):
        self.c.execute("BEGIN")
        run(self.c, "claimBatch", batchId=batch, limit=limit)
        mangas = run(self.c, "getClaimedMangas", batchId=batch).fetchall()
        chapters = run(self.c, "getClaimedChapters", batchId=batch).fetchall()
        self.c.execute("COMMIT")
        return mangas, chapters

    def test_claim_loads_only_queued_rows(self):
        other = add_manga(self.c, url="/untouched")
        add_chapter(self.c, other, url="/untouched-c")
        mangas, chapters = self.claim()
        self.assertEqual([r[0] for r in mangas], [self.m])
        self.assertEqual(len(chapters), 1)
        self.assertEqual(chapters[0][-2:], (1, "/m"))
        self.assertTrue(all(r[2] == "IN_FLIGHT" for r in queue(self.c)))

    def test_claim_respects_limit_and_order(self):
        self.claim(limit=1)
        self.assertEqual([r[2] for r in queue(self.c)], ["IN_FLIGHT", "PENDING"])

    def test_complete_removes_entries(self):
        self.claim()
        run(self.c, "completeBatch", batchId="b1")
        self.assertEqual(queue(self.c), [])

    def test_failed_upload_returns_entries_for_retry(self):
        self.claim()
        run(self.c, "releaseBatch", permanent=0, maxAttempts=5, error="timeout", batchId="b1")
        run(self.c, "completeBatch", batchId="b1")
        self.assertEqual(
            sorted(queue(self.c)),
            sorted([("mangas", self.m, "PENDING", 1), ("chapters", self.ch, "PENDING", 1)]),
        )
        # and the retry can be claimed again
        mangas, chapters = self.claim("b2")
        self.assertEqual((len(mangas), len(chapters)), (1, 1))

    def test_change_during_flight_survives_failure_and_success(self):
        self.claim()
        self.c.execute("UPDATE chapters SET last_page_read = 7 WHERE _id = ?", (self.ch,))  # new PENDING twin
        self.assertEqual(len(queue(self.c)), 4)
        # failure: the flying entry clashes with its twin and is superseded
        run(self.c, "releaseBatch", permanent=0, maxAttempts=5, error="503", batchId="b1")
        run(self.c, "completeBatch", batchId="b1")
        self.assertEqual(sorted(r[:3] for r in queue(self.c)),
                         sorted([("chapters", self.ch, "PENDING"), ("mangas", self.m, "PENDING")]))

    def test_change_during_flight_is_kept_on_success(self):
        self.claim()
        self.c.execute("UPDATE chapters SET last_page_read = 7 WHERE _id = ?", (self.ch,))
        run(self.c, "completeBatch", batchId="b1")
        self.assertEqual(sorted(r[:3] for r in queue(self.c)),
                         sorted([("chapters", self.ch, "PENDING"), ("mangas", self.m, "PENDING")]))

    def test_attempt_limit_and_permanent_failure_park_entries(self):
        self.claim()
        run(self.c, "releaseBatch", permanent=0, maxAttempts=1, error="x", batchId="b1")
        self.assertEqual({r[2] for r in queue(self.c)}, {"FAILED"})
        run(self.c, "requeueFailed", olderThan=2**40)
        run(self.c, "dropRequeuedLeftovers", olderThan=2**40)
        self.assertEqual({r[2] for r in queue(self.c)}, {"PENDING"})
        self.claim("b2")
        run(self.c, "releaseBatch", permanent=1, maxAttempts=99, error="400", batchId="b2")
        self.assertEqual({r[2] for r in queue(self.c)}, {"FAILED"})

    def test_requeue_failed_merges_with_newer_pending(self):
        self.claim()
        run(self.c, "releaseBatch", permanent=1, maxAttempts=99, error="400", batchId="b1")
        self.c.execute("UPDATE chapters SET bookmark = 1 WHERE _id = ?", (self.ch,))
        self.assertEqual(len(queue(self.c)), 4)  # 2 FAILED + 2 PENDING
        run(self.c, "requeueFailed", olderThan=2**40)
        run(self.c, "dropRequeuedLeftovers", olderThan=2**40)
        self.assertEqual(sorted(r[2] for r in queue(self.c)), ["PENDING", "PENDING"])

    def test_recover_in_flight_after_crash(self):
        self.claim()
        self.c.execute("UPDATE chapters SET last_page_read = 3 WHERE _id = ?", (self.ch,))
        run(self.c, "recoverInFlight")
        run(self.c, "dropRecoveredLeftovers")
        self.assertEqual({r[2] for r in queue(self.c)}, {"PENDING"})
        self.assertEqual(len(queue(self.c)), 2)

    def test_rolled_back_claim_leaves_queue_untouched(self):
        self.c.execute("BEGIN")
        run(self.c, "claimBatch", batchId="bx", limit=10)
        self.c.execute("ROLLBACK")
        self.assertEqual({r[2] for r in queue(self.c)}, {"PENDING"})

    def test_deleted_record_is_cleared_with_batch(self):
        self.claim()
        self.c.execute("DELETE FROM mangas WHERE _id = ?", (self.m,))
        self.assertEqual(run(self.c, "getClaimedMangas", batchId="b1").fetchall(), [])
        run(self.c, "completeBatch", batchId="b1")
        self.assertEqual(queue(self.c), [])

    def test_state_roundtrip(self):
        self.assertIsNone(run(self.c, "getState", key="cursor").fetchone())
        run(self.c, "setState", key="cursor", value="7")
        run(self.c, "setState", key="cursor", value="8")
        self.assertEqual(run(self.c, "getState", key="cursor").fetchone()[0], "8")


OLD_TRIGGERS = """
CREATE TRIGGER update_manga_version AFTER UPDATE ON mangas
BEGIN
    UPDATE mangas SET version = version + 1
    WHERE _id = new._id AND new.is_syncing = 0 AND (
        new.url != old.url OR new.description != old.description OR new.favorite != old.favorite
    );
END;
CREATE TRIGGER update_chapter_and_manga_version AFTER UPDATE ON chapters
WHEN new.is_syncing = 0 AND (new.read != old.read OR new.bookmark != old.bookmark OR new.last_page_read != old.last_page_read)
BEGIN
    UPDATE chapters SET version = version + 1 WHERE _id = new._id;
    UPDATE mangas SET version = version + 1
    WHERE _id = new.manga_id AND (SELECT is_syncing FROM mangas WHERE _id = new.manga_id) = 0;
END;
CREATE TRIGGER insert_manga_category_update_version AFTER INSERT ON mangas_categories
BEGIN
    UPDATE mangas SET version = version + 1
    WHERE _id = new.manga_id AND (SELECT is_syncing FROM mangas WHERE _id = new.manga_id) = 0;
END;
"""


def norm(sql):
    return re.sub(r"\s+", " ", re.sub(r"\bIF NOT EXISTS\b", "", sql or "", flags=re.I)).strip().rstrip(";")


class MigrationTests(unittest.TestCase):
    def old_db(self):
        c = fresh_db()
        for t in ("update_manga_version", "update_chapter_and_manga_version",
                  "insert_manga_category_update_version", "delete_manga_category_update_version"):
            c.execute(f"DROP TRIGGER {t}")
        c.execute("DROP TABLE sync_delta_queue")
        c.execute("DROP TABLE sync_state")
        c.executescript(OLD_TRIGGERS)
        return c

    def test_migrated_schema_matches_fresh_schema(self):
        old = self.old_db()
        old.executescript(migration_sql(14))
        fresh = fresh_db()
        q = "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        a = [(t, n, norm(s)) for t, n, s in old.execute(q)]
        b = [(t, n, norm(s)) for t, n, s in fresh.execute(q)]
        self.assertEqual(a, b)

    def test_backfill_queues_only_modified_rows_and_clears_stuck_flags(self):
        c = self.old_db()
        dirty = add_manga(c, url="/dirty", version=3)
        clean = add_manga(c, url="/clean")
        stuck = add_manga(c, url="/stuck")
        c.execute("UPDATE mangas SET is_syncing = 1 WHERE _id = ?", (stuck,))
        dc = add_chapter(c, dirty, url="/dc", version=2)
        add_chapter(c, clean, url="/cc")
        c.executescript(migration_sql(14))
        self.assertEqual(sorted(queue(c)), sorted([("mangas", dirty, "PENDING", 0), ("chapters", dc, "PENDING", 0)]))
        self.assertEqual(c.execute("SELECT count(*) FROM mangas WHERE is_syncing = 1").fetchone()[0], 0)
        # new triggers are live after migrating
        c.execute("UPDATE mangas SET favorite = 1 WHERE _id = ?", (clean,))
        self.assertIn(("mangas", clean, "PENDING", 0), queue(c))

    def test_only_sync_tables_accept_bad_values(self):
        c = fresh_db()
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO sync_delta_queue(table_name, record_id) VALUES ('history', 1)")
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO sync_delta_queue(table_name, record_id, status) VALUES ('mangas', 1, 'DONE')")


if __name__ == "__main__":
    unittest.main(verbosity=2)
