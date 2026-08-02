import gc
import hashlib
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x_context as context_x


V2_SCHEMA = """CREATE TABLE context_meta (
    key TEXT PRIMARY KEY,value TEXT NOT NULL
);
INSERT INTO context_meta VALUES('schema_version','2');
INSERT INTO context_meta VALUES('target_user_id','1');
INSERT INTO context_meta VALUES('canonical_handle','alice');
CREATE TABLE targets (
    post_id TEXT PRIMARY KEY,
    conversation_id TEXT,
    depth_min INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    lease_started_at REAL,
    discovered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_error_class TEXT,
    last_error_detail TEXT,
    unavailable_at TEXT,
    author_id TEXT,
    media_state TEXT NOT NULL DEFAULT 'none',
    media_attempts INTEGER NOT NULL DEFAULT 0,
    media_next_attempt_at REAL NOT NULL DEFAULT 0
);
CREATE TABLE reply_edges (
    child_id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL REFERENCES targets(post_id),
    conversation_id TEXT,
    depth INTEGER NOT NULL,
    discovered_run_id TEXT,
    discovered_at TEXT NOT NULL,
    cycle_detected INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX reply_edges_parent ON reply_edges(parent_id);
CREATE INDEX reply_edges_conversation ON reply_edges(conversation_id);
CREATE TABLE observations (
    post_id TEXT PRIMARY KEY REFERENCES targets(post_id),
    captured_at TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    capture_count INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE seed_sources (
    relative_path TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    run_id TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    edge_count INTEGER NOT NULL
);
CREATE TABLE local_posts (
    post_id TEXT PRIMARY KEY,
    raw_json TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    run_id TEXT NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE INDEX local_posts_source ON local_posts(relative_path);
CREATE TABLE pacing (
    singleton INTEGER PRIMARY KEY,
    next_request_at REAL NOT NULL DEFAULT 0,
    last_request_at REAL,
    last_rate_limit_at REAL,
    last_progress_at TEXT
);
INSERT INTO pacing VALUES(1,55,44,66,'2026-01-01T00:00:00Z');
"""


def make_v2(path: Path, rows: int = 100) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(V2_SCHEMA)
    values = []
    for post_id in range(1, rows + 1):
        if post_id == 2:
            state, media_state, lease = "captured", "leased", 12.5
        elif post_id == 3:
            state, media_state, lease = "manual_review", "manual_review", None
        elif post_id == 4:
            state, media_state, lease = "leased", "none", 13.5
        else:
            state, media_state, lease = (
                "retryable" if post_id % 2 else "pending",
                "none",
                None,
            )
        values.append(
            (
                str(post_id),
                "1",
                post_id % 20,
                state,
                1 if state == "retryable" else 0,
                0,
                lease,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
                "fixture" if state == "manual_review" else None,
                None,
                None,
                "1",
                media_state,
                1 if media_state in {"leased", "manual_review"} else 0,
                0,
            )
        )
    connection.executemany(
        "INSERT INTO targets VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values
    )
    connection.execute(
        "INSERT INTO observations VALUES(?,?,?,?,?,?)",
        ("2", "2026-01-01T00:00:00Z", "x:focal", "{}", "a" * 64, 1),
    )
    connection.executemany(
        "INSERT INTO reply_edges VALUES(?,?,?,?,?,?,?)",
        (
            (
                f"child-{index}",
                "1",
                "1",
                0,
                "run-a",
                "2026-01-01T00:00:00Z",
                0,
            )
            for index in range(rows // 2)
        ),
    )
    connection.execute(
        "INSERT INTO seed_sources VALUES(?,?,?,?,?,?,?)",
        (
            "runs/run-a/raw/timeline.jsonl",
            "b" * 64,
            "modern",
            "run-a",
            "2026-01-01T00:00:00Z",
            rows,
            rows // 2,
        ),
    )
    connection.execute(
        "INSERT INTO local_posts VALUES(?,?,?,?,?,?,?)",
        (
            "2",
            "{}",
            "c" * 64,
            "runs/run-a/raw/timeline.jsonl",
            "modern",
            "run-a",
            "2026-01-01T00:00:00Z",
        ),
    )
    connection.commit()
    connection.close()
    os.chmod(path, 0o600)


def plan(connection: sqlite3.Connection, sql: str, parameters) -> str:
    return "\n".join(
        str(row[3])
        for row in connection.execute("EXPLAIN QUERY PLAN " + sql, parameters)
    )


class V3MigrationTests(unittest.TestCase):
    def test_large_v2_migrates_in_place_and_preserves_compatibility_truth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "_state" / "context.sqlite3"
            make_v2(path, rows=20_000)
            original_inode = path.stat().st_ino
            original_size = path.stat().st_size

            with mock.patch.object(
                context_x.archive_x,
                "sha256_file",
                side_effect=AssertionError("v2 migration must not hash payloads"),
            ), mock.patch.object(
                context_x.archive_x,
                "iter_jsonl",
                side_effect=AssertionError("v2 migration must not parse payloads"),
            ):
                with context_x.ContextDB(path, create=False) as database:
                    version = database.connection.execute(
                        "SELECT value FROM context_meta WHERE key='schema_version'"
                    ).fetchone()[0]
                    self.assertEqual(version, "3")
                    self.assertEqual(
                        database.connection.execute(
                            "SELECT COUNT(*) FROM targets"
                        ).fetchone()[0],
                        20_000,
                    )
                    self.assertEqual(
                        database.connection.execute(
                            "SELECT parent_demand FROM targets WHERE post_id='1'"
                        ).fetchone()[0],
                        10_000,
                    )
                    leased = database.connection.execute(
                        """SELECT state,media_state,lease_started_at,
                                  media_lease_started_at,lease_token,
                                  media_lease_token
                             FROM targets WHERE post_id='2'"""
                    ).fetchone()
                    self.assertEqual(tuple(leased)[:4], ("captured", "leased", None, 12.5))
                    self.assertIsNone(leased[4])
                    self.assertTrue(str(leased[5]).startswith("migration-"))
                    metadata_lease = database.connection.execute(
                        """SELECT state,lease_started_at,lease_token
                             FROM targets WHERE post_id='4'"""
                    ).fetchone()
                    self.assertEqual(tuple(metadata_lease)[:2], ("leased", 13.5))
                    self.assertTrue(str(metadata_lease[2]).startswith("migration-"))
                    review = database.connection.execute(
                        "SELECT state,media_state FROM targets WHERE post_id='3'"
                    ).fetchone()
                    self.assertEqual(tuple(review), ("manual_review", "manual_review"))
                    pacing = database.connection.execute(
                        """SELECT next_request_at,last_request_at,last_rate_limit_at
                             FROM pacing WHERE singleton=1"""
                    ).fetchone()
                    self.assertEqual(tuple(pacing), (55.0, 44.0, 66.0))
                    source = database.connection.execute(
                        """SELECT expected_sha256,status,record_count,edge_count
                             FROM archive_sources"""
                    ).fetchone()
                    self.assertEqual(tuple(source), ("b" * 64, "committed", 20_000, 10_000))
                    account = database.connection.execute(
                        "SELECT user_id,canonical_handle FROM archive_account"
                    ).fetchone()
                    self.assertEqual(tuple(account), ("1", "alice"))
                    export = database.connection.execute(
                        "SELECT status,durable_generation,exported_generation FROM export_views"
                    ).fetchall()
                    self.assertTrue(export)
                    self.assertTrue(all(tuple(row) == ("unknown", 0, 0) for row in export))
                    counters = dict(
                        database.connection.execute(
                            "SELECT counter_name,value FROM progress_counters"
                        )
                    )
                    self.assertEqual(counters["targets_total"], 20_000)
                    self.assertEqual(counters["reply_edges_total"], 10_000)
                    self.assertEqual(counters["observations_total"], 1)

            self.assertEqual(path.stat().st_ino, original_inode)
            self.assertFalse((root / "_state" / "backups").exists())
            self.assertEqual(list(root.rglob("*.sqlite3")), [path])
            self.assertLess(path.stat().st_size, original_size * 5)
            if os.name == "posix":
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with context_x.ContextDB(path, create=False) as reopened:
                self.assertEqual(reopened.integrity_errors(), [])
                self.assertIsNone(reopened.migration_backup)

    def test_v3_reopen_is_read_only_and_incomplete_v3_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            with context_x.ContextDB(path):
                pass
            before = path.read_bytes()
            before_inode = path.stat().st_ino
            with context_x.ContextDB(path, create=False):
                pass
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(path.stat().st_ino, before_inode)

            connection = sqlite3.connect(path)
            connection.execute("DROP INDEX targets_media_priority")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(
                context_x.ContextError, "context schema v3 is incomplete"
            ):
                context_x.ContextDB(path, create=False)

    def test_existing_v3_addendum_is_atomic_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            with context_x.ContextDB(path):
                pass
            connection = sqlite3.connect(path)
            connection.execute(
                "DELETE FROM schema_migrations WHERE version=?",
                (context_x.V3_LOCAL_ADDENDUM_VERSION,),
            )
            connection.execute("DROP TABLE archive_media")
            connection.execute("DROP TABLE conversation_rollups")
            connection.commit()
            connection.close()

            original = context_x.ContextDB._create_v3_objects

            def fault(database):
                original(database)
                raise RuntimeError("injected addendum fault")

            with mock.patch.object(context_x.ContextDB, "_create_v3_objects", fault):
                with self.assertRaisesRegex(RuntimeError, "addendum fault"):
                    context_x.ContextDB(path, create=False)
            connection = sqlite3.connect(path)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            marker = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version=?",
                (context_x.V3_LOCAL_ADDENDUM_VERSION,),
            ).fetchone()
            connection.close()
            self.assertNotIn("archive_media", tables)
            self.assertIsNone(marker)

            with context_x.ContextDB(path, create=False) as database:
                self.assertIsNotNone(
                    database.connection.execute(
                        "SELECT 1 FROM schema_migrations WHERE version=?",
                        (context_x.V3_LOCAL_ADDENDUM_VERSION,),
                    ).fetchone()
                )
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM archive_media"
                    ).fetchone()[0],
                    0,
                )

    def test_write_capable_open_tightens_database_permissions(self):
        if os.name != "posix":
            self.skipTest("POSIX mode test")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            with context_x.ContextDB(path):
                pass
            os.chmod(path, 0o644)
            with context_x.ContextDB(path, create=False):
                pass
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_v2_migration_rolls_back_completely_and_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            make_v2(path, rows=20)
            original = context_x.ContextDB._create_v3_objects

            def fault(database):
                original(database)
                raise RuntimeError("fault before schema version commit")

            with mock.patch.object(
                context_x.ContextDB, "_create_v3_objects", fault
            ):
                with self.assertRaisesRegex(RuntimeError, "fault before"):
                    context_x.ContextDB(path, create=False)
            gc.collect()

            connection = sqlite3.connect(path)
            version = connection.execute(
                "SELECT value FROM context_meta WHERE key='schema_version'"
            ).fetchone()[0]
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(targets)")
            }
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            connection.close()
            self.assertEqual(version, "2")
            self.assertNotIn("parent_demand", columns)
            self.assertNotIn("descriptor_generations", tables)

            with context_x.ContextDB(path, create=False) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT value FROM context_meta WHERE key='schema_version'"
                    ).fetchone()[0],
                    "3",
                )


class V3ConstraintAndPlanTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "context.sqlite3"
        self.database = context_x.ContextDB(self.path)
        self.database.bind_identity("1", "alice")

    def tearDown(self):
        self.database.close()
        self.directory.cleanup()

    def descriptor(self, post_id: str, ordinal: int = 1, generation: int = 1):
        digest = hashlib.sha256(
            f"{post_id}:{ordinal}:{generation}".encode()
        ).hexdigest()
        cursor = self.database.connection.execute(
            """INSERT INTO descriptor_generations(
                   owner_kind,owner_id,media_ordinal,generation,
                   source_operation,media_type,extension,private_url,
                   url_sha256,url_host,descriptor_sha256,filename,
                   relative_directory,state,captured_at
               ) VALUES ('post',?,?,?,?,?,?,?,?,?,?,?,?,'active',?)""",
            (
                post_id,
                ordinal,
                generation,
                "context",
                "photo",
                "jpg",
                f"https://pbs.twimg.com/media/{digest}",
                digest,
                "pbs.twimg.com",
                digest,
                f"{post_id}_{ordinal}.jpg",
                "media/context/2026/01",
                "2026-01-01T00:00:00Z",
            ),
        )
        return cursor.lastrowid

    def test_constraints_reject_cross_owner_stale_lease_and_export_state(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO targets(
                       post_id,state,discovered_at,updated_at
                   ) VALUES ('90','leased','now','now')"""
            )
        self.database.upsert_target(
            "91", conversation_id="91", depth=0, observed_at="now"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                "UPDATE targets SET state='leased' WHERE post_id='91'"
            )
        self.database.connection.execute(
            """UPDATE targets SET state='leased',lease_started_at=1,
                   lease_token='owner-a' WHERE post_id='91'"""
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                "UPDATE targets SET state='pending' WHERE post_id='91'"
            )
        self.database.connection.execute(
            """UPDATE targets SET state='pending',lease_started_at=NULL,
                   lease_token=NULL WHERE post_id='91'"""
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO targets(
                       post_id,state,discovered_at,updated_at
                   ) VALUES ('92','captured','now','now')"""
            )

        descriptor = self.descriptor("100")
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,descriptor_id,state,
                       created_at,updated_at
                   ) VALUES ('post','101',1,?,'pending',?,?)""",
                (descriptor, "now", "now"),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,descriptor_id,state,
                       created_at,updated_at
                   ) VALUES ('post','100',1,?,'leased',?,?)""",
                (descriptor, "now", "now"),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,descriptor_id,state,
                       created_at,updated_at
                   ) VALUES ('post','100',1,?,'captured',?,?)""",
                (descriptor, "now", "now"),
            )
        self.database.connection.execute(
            "UPDATE descriptor_generations SET state='superseded' WHERE descriptor_id=?",
            (descriptor,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,descriptor_id,state,
                       created_at,updated_at
                   ) VALUES ('post','100',1,?,'pending',?,?)""",
                (descriptor, "now", "now"),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                "UPDATE descriptor_generations SET state='active' WHERE descriptor_id=?",
                (descriptor,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """UPDATE export_views SET durable_generation=1,
                       exported_generation=2 WHERE view_name='posts'"""
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """UPDATE export_views SET durable_generation=1,
                       exported_generation=1,status='current'
                     WHERE view_name='posts'"""
            )

    def test_source_provenance_and_generations_are_unique(self):
        self.database.connection.execute(
            """INSERT INTO archive_sources(
                   relative_path,source_kind,run_id,expected_sha256,status,
                   registered_at
               ) VALUES (?,?,?,?,?,?)""",
            ("runs/a/raw.jsonl", "modern", "run-a", "a" * 64, "committed", "now"),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.database.connection.execute(
                """INSERT INTO archive_sources(
                       relative_path,source_kind,run_id,status,registered_at
                   ) VALUES (?,?,?,?,?)""",
                ("runs/a/raw.jsonl", "modern", "run-b", "registered", "now"),
            )
        self.descriptor("100", generation=1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.descriptor("100", generation=1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.descriptor("100", generation=0)

    def test_exact_hot_queries_use_partial_priority_and_expiry_indexes(self):
        observed = "2026-01-01T00:00:00Z"
        for child in ("500", "501", "502"):
            self.database.add_edge(
                child,
                "100",
                conversation_id="1",
                depth=5,
                run_id="run",
                observed_at=observed,
                max_depth=10,
            )
        self.database.add_edge(
            "600",
            "200",
            conversation_id="1",
            depth=1,
            run_id="run",
            observed_at=observed,
            max_depth=10,
        )
        row = self.database.claim(now=1, lease_seconds=10, fairness_quantum=5)
        self.assertEqual(row["post_id"], "100")

        metadata = plan(
            self.database.connection, context_x.METADATA_CLAIM_SQL, (1,)
        )
        media = plan(self.database.connection, context_x.MEDIA_CLAIM_SQL, (1,))
        asset = plan(self.database.connection, context_x.ASSET_CLAIM_SQL, (1,))
        refresh = plan(
            self.database.connection, context_x.REFRESH_CLAIM_SQL, (1,)
        )
        metadata_expiry = plan(
            self.database.connection,
            context_x.METADATA_RECLAIM_SQL,
            (1, "now", 0),
        )
        media_expiry = plan(
            self.database.connection,
            context_x.MEDIA_RECLAIM_SQL,
            (1, "now", 0),
        )
        self.assertIn("targets_metadata_priority", metadata)
        self.assertNotIn("TEMP B-TREE", metadata)
        self.assertNotIn("CORRELATED", metadata)
        self.assertIn("targets_media_priority", media)
        self.assertNotIn("TEMP B-TREE", media)
        self.assertIn("asset_jobs_ready", asset)
        self.assertNotIn("TEMP B-TREE", asset)
        self.assertIn("refresh_jobs_ready", refresh)
        self.assertNotIn("TEMP B-TREE", refresh)
        self.assertIn("targets_metadata_lease_expiry", metadata_expiry)
        self.assertIn("targets_media_lease_expiry", media_expiry)

    def test_counter_triggers_track_targets_edges_and_observations(self):
        before = dict(
            self.database.connection.execute(
                "SELECT counter_name,value FROM progress_counters"
            )
        )
        self.database.add_edge(
            "300",
            "200",
            conversation_id="100",
            depth=0,
            run_id="run",
            observed_at="now",
            max_depth=10,
        )
        after = dict(
            self.database.connection.execute(
                "SELECT counter_name,value FROM progress_counters"
            )
        )
        self.assertEqual(after["targets_total"], before["targets_total"] + 1)
        self.assertEqual(after["reply_edges_total"], before["reply_edges_total"] + 1)
        self.assertEqual(after["targets_state_pending"], before["targets_state_pending"] + 1)

    def test_conversation_rollups_and_reason_cycle_counters_track_mutations(self):
        for child, parent in (("300", "200"), ("301", "201")):
            self.database.add_edge(
                child,
                parent,
                conversation_id="100",
                depth=0,
                run_id="run",
                observed_at="now",
                max_depth=10,
            )
        rollup = self.database.connection.execute(
            "SELECT * FROM conversation_rollups WHERE chain_id='100'"
        ).fetchone()
        self.assertEqual((rollup["state"], rollup["pending_count"], rollup["edge_count"]),
                         ("pending", 2, 2))

        self.database.connection.execute(
            """UPDATE targets SET state='unavailable',last_error_class='deleted',
                       unavailable_at='now',updated_at='now' WHERE post_id='200'"""
        )
        self.database.connection.execute(
            """UPDATE targets SET state='unavailable',last_error_class='protected',
                       unavailable_at='now',updated_at='now' WHERE post_id='201'"""
        )
        rollup = self.database.connection.execute(
            "SELECT * FROM conversation_rollups WHERE chain_id='100'"
        ).fetchone()
        counters = dict(
            self.database.connection.execute(
                "SELECT counter_name,value FROM progress_counters"
            )
        )
        self.assertEqual(
            (rollup["state"], rollup["unavailable_count"], rollup["pending_count"]),
            ("unavailable_boundary", 2, 0),
        )
        self.assertEqual(counters["conversations_state_unavailable_boundary"], 1)
        self.assertEqual(counters["targets_unavailable_deleted"], 1)
        self.assertEqual(counters["targets_unavailable_private"], 1)

        self.database.add_edge(
            "10", "20", conversation_id="cycle", depth=0,
            run_id="run", observed_at="now", max_depth=10,
        )
        self.database.add_edge(
            "20", "10", conversation_id="cycle", depth=1,
            run_id="run", observed_at="now", max_depth=10,
        )
        counters = dict(
            self.database.connection.execute(
                "SELECT counter_name,value FROM progress_counters"
            )
        )
        self.assertEqual(counters["reply_edges_cycles"], 1)
        self.assertEqual(self.database.status()["cycles"], 1)


if __name__ == "__main__":
    unittest.main()
