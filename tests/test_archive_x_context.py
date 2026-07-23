import importlib.util
import json
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

SPEC = importlib.util.spec_from_file_location(
    "archive_x_context", SCRIPTS / "archive_x_context.py"
)
context_x = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = context_x
assert SPEC.loader is not None
SPEC.loader.exec_module(context_x)


def post(
    post_id,
    *,
    author_id="1",
    author="alice",
    user_id="999",
    reply_id=0,
    conversation_id=None,
    count=0,
):
    return {
        "tweet_id": int(post_id),
        "reply_id": int(reply_id) if reply_id else 0,
        "retweet_id": 0,
        "quote_id": 0,
        "conversation_id": int(conversation_id or post_id),
        "author": {"id": int(author_id), "name": author, "nick": author},
        # Deliberately unreliable: stable authorship must not use this object.
        "user": {"id": int(user_id), "name": "extractor-user"},
        "content": f"post {post_id}",
        "date": "2026-01-01 00:00:00",
        "archived_at": "2026-01-02T00:00:00Z",
        "count": count,
    }


def make_archive(root: Path, records=()):
    records = tuple(records)
    user_dir = root / "users" / "alice"
    state_dir = user_dir / "_state"
    raw_dir = user_dir / "runs" / "run-a" / "raw"
    state_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    (state_dir / "state.json").write_text(
        json.dumps(
            {
                "requested_user_id": "1",
                "requested_handle": "old-alice",
                "canonical_handle": "alice",
            }
        ),
        encoding="utf-8",
    )
    raw = raw_dir / "timeline.posts.incomplete.jsonl"
    raw.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )
    (raw_dir.parent / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-a",
                "status": "success",
                "completed_at": "2026-01-02T00:00:00Z",
                "post_dataset": {"dataset_posts": len(records)},
                "endpoints": [
                    {
                        "endpoint": "timeline",
                        "raw_path": str(raw.relative_to(user_dir)),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return user_dir, state_dir / "context.sqlite3"


def add_legacy_run(user_dir: Path, records=()):
    records = tuple(records)
    run_dir = user_dir / "runs" / "run-legacy"
    canonical = run_dir / "raw" / "legacy-window.posts.jsonl"
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )
    walk = run_dir / "raw" / "legacy-window-walk-1.posts.jsonl"
    walk.write_text(canonical.read_text(encoding="utf-8"), encoding="utf-8")
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-legacy",
                "mode": "legacy_backfill",
                "status": "limited",
                "completed_at": "2026-01-03T00:00:00Z",
                "windows": [
                    {
                        "status": "success",
                        "metadata_confirmed": True,
                        "state_committed": True,
                        "canonical_raw_path": str(canonical.relative_to(user_dir)),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return canonical, walk


class ContextStateTests(unittest.TestCase):
    def test_schema_is_private_reopenable_and_rejects_unknown_version(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state" / "context.sqlite3"
            with context_x.ContextDB(path) as database:
                self.assertEqual(database.integrity_errors(), [])
            if os.name == "posix":
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with context_x.ContextDB(path, create=False):
                pass
            connection = sqlite3.connect(path)
            connection.execute(
                "UPDATE context_meta SET value='999' WHERE key='schema_version'"
            )
            connection.commit()
            connection.close()
            with self.assertRaises(context_x.ContextError):
                context_x.ContextDB(path, create=False)

    def test_v1_migration_is_private_exact_and_preserves_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "_state" / "context.sqlite3"
            with context_x.ContextDB(path) as database:
                database.bind_identity("1", "alice")
                database.add_edge(
                    "300",
                    "200",
                    conversation_id="100",
                    depth=0,
                    run_id="run-a",
                    observed_at="2026-01-01T00:00:00Z",
                    max_depth=10,
                )
            connection = sqlite3.connect(path)
            connection.execute("DROP TABLE local_posts")
            connection.execute("DROP TABLE seed_sources")
            connection.execute(
                "UPDATE context_meta SET value='1' WHERE key='schema_version'"
            )
            connection.commit()
            connection.close()
            before = context_x.archive_x.sha256_file(path)

            with context_x.ContextDB(path, create=False) as migrated:
                backup = migrated.migration_backup
                self.assertIsNotNone(backup)
                self.assertEqual(migrated.status()["edges"], 1)
                tables = {
                    row[0]
                    for row in migrated.connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                self.assertIn("seed_sources", tables)
                self.assertIn("local_posts", tables)

            self.assertEqual(context_x.archive_x.sha256_file(backup), before)
            if os.name == "posix":
                self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
            with context_x.ContextDB(path, create=False) as reopened:
                self.assertIsNone(reopened.migration_backup)

    def test_captured_requires_observation_and_transaction_rolls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            with context_x.ContextDB(path) as database:
                database.upsert_target(
                    "100", conversation_id="100", depth=0, observed_at="now"
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    database.connection.execute(
                        "UPDATE targets SET state='captured' WHERE post_id='100'"
                    )
                with self.assertRaises(RuntimeError):
                    with context_x.transaction(database.connection):
                        database.upsert_target(
                            "200", conversation_id="200", depth=0, observed_at="now"
                        )
                        raise RuntimeError("fault after insert")
                self.assertIsNone(
                    database.connection.execute(
                        "SELECT 1 FROM targets WHERE post_id='200'"
                    ).fetchone()
                )

    def test_database_binds_stable_target_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                database.bind_identity("1", "alice")
                database.bind_identity("1", "renamed-alice")
                with self.assertRaises(context_x.ContextError):
                    database.bind_identity("2", "alice")

    def test_edge_idempotence_conflict_cycle_and_max_depth(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                args = dict(
                    conversation_id="10",
                    run_id="run",
                    observed_at="now",
                    max_depth=3,
                )
                self.assertTrue(database.add_edge("30", "20", depth=0, **args))
                self.assertFalse(database.add_edge("30", "20", depth=0, **args))
                with self.assertRaises(context_x.ContextError):
                    database.add_edge("30", "21", depth=0, **args)
                self.assertIsNone(
                    database.connection.execute(
                        "SELECT 1 FROM targets WHERE post_id='21'"
                    ).fetchone()
                )
                database.add_edge("20", "10", depth=1, **args)
                database.add_edge("10", "30", depth=2, **args)
                cycle = database.connection.execute(
                    "SELECT cycle_detected FROM reply_edges WHERE child_id='10'"
                ).fetchone()[0]
                self.assertEqual(cycle, 1)
                database.add_edge("99", "98", depth=4, **args)
                reason = database.connection.execute(
                    "SELECT last_error_class FROM targets WHERE post_id='98'"
                ).fetchone()[0]
                self.assertEqual(reason, "max_depth")


class DiscoveryTests(unittest.TestCase):
    def test_default_seed_includes_committed_legacy_but_excludes_walk_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, db_path = make_archive(
                Path(directory), [post("300", reply_id="200")]
            )
            _canonical, walk = add_legacy_run(
                user_dir, [post("600", reply_id="500")]
            )

            result = context_x.seed_context(
                user_dir, db_path, dry_run=False, max_depth=10
            )

            self.assertEqual(result["files_processed"], 2)
            with context_x.ContextDB(db_path, create=False) as database:
                edges = {
                    tuple(row)
                    for row in database.connection.execute(
                        "SELECT child_id,parent_id FROM reply_edges"
                    )
                }
                kinds = {
                    row[0]
                    for row in database.connection.execute(
                        "SELECT source_kind FROM seed_sources"
                    )
                }
            self.assertEqual(edges, {("300", "200"), ("600", "500")})
            self.assertEqual(kinds, {"modern", "legacy"})
            with self.assertRaisesRegex(
                context_x.ContextError, "not a committed canonical source"
            ):
                context_x.seed_context(
                    user_dir,
                    db_path,
                    dry_run=True,
                    max_depth=10,
                    raw_paths=[walk],
                )

    def test_seed_ledger_rejects_changed_source_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, db_path = make_archive(
                Path(directory), [post("300", reply_id="200")]
            )
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=10)
            raw = next((user_dir / "runs").glob("*/raw/timeline.posts*.jsonl"))
            with raw.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(post("301", reply_id="201")) + "\n")

            with self.assertRaisesRegex(
                context_x.ContextError, "previously seeded canonical source changed"
            ):
                context_x.seed_context(
                    user_dir, db_path, dry_run=False, max_depth=10
                )

    def test_new_edge_captures_parent_from_previously_seeded_local_index(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, db_path = make_archive(Path(directory), [post("200")])
            first = context_x.seed_context(
                user_dir, db_path, dry_run=False, max_depth=10
            )
            self.assertEqual(first["local_parents"], 0)
            add_legacy_run(user_dir, [post("300", reply_id="200")])

            second = context_x.seed_context(
                user_dir, db_path, dry_run=False, max_depth=10
            )

            self.assertEqual(second["files_skipped"], 1)
            self.assertEqual(second["files_processed"], 1)
            self.assertEqual(second["local_parents"], 1)
            with context_x.ContextDB(db_path, create=False) as database:
                state = database.connection.execute(
                    "SELECT state FROM targets WHERE post_id='200'"
                ).fetchone()[0]
            self.assertEqual(state, "captured")

    def test_failed_source_transaction_does_not_write_seed_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, db_path = make_archive(
                Path(directory), [post("300", reply_id="200")]
            )
            with mock.patch.object(
                context_x.ContextDB,
                "add_edge",
                side_effect=context_x.ContextError("injected seed failure"),
            ):
                with self.assertRaisesRegex(
                    context_x.ContextError, "injected seed failure"
                ):
                    context_x.seed_context(
                        user_dir, db_path, dry_run=False, max_depth=10
                    )
            with context_x.ContextDB(db_path, create=False) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM seed_sources"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM local_posts"
                    ).fetchone()[0],
                    0,
                )

            recovered = context_x.seed_context(
                user_dir, db_path, dry_run=False, max_depth=10
            )
            self.assertEqual(recovered["files_processed"], 1)

    def test_seed_is_idempotent_deduplicates_and_captures_local_parent(self):
        records = [
            post("300", reply_id="200", conversation_id="100"),
            post("200", reply_id="100", conversation_id="100"),
            post("400", author_id="2", author="bob", reply_id="100"),
            post("500"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            user_dir, db_path = make_archive(Path(directory), records)
            first = context_x.seed_context(
                user_dir, db_path, dry_run=False, max_depth=10
            )
            second = context_x.seed_context(
                user_dir, db_path, dry_run=False, max_depth=10
            )
            self.assertEqual(first["reply_edges"], 2)
            self.assertEqual(first["unique_parents"], 2)
            self.assertEqual(first["local_parent_candidates"], 1)
            self.assertEqual(first["local_parents"], 1)
            self.assertEqual(second["local_parents"], 0)
            with context_x.ContextDB(db_path, create=False) as database:
                self.assertEqual(database.status()["edges"], 2)
                states = {
                    row[0]: row[1]
                    for row in database.connection.execute(
                        "SELECT post_id,state FROM targets"
                    )
                }
                self.assertEqual(states, {"100": "pending", "200": "captured"})

    def test_dry_run_writes_nothing_and_ignores_malformed_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir, db_path = make_archive(
                Path(directory), [post("300", reply_id="200")]
            )
            raw = next((user_dir / "runs").glob("*/raw/*.jsonl"))
            with raw.open("a", encoding="utf-8") as output:
                malformed = post("301", reply_id="200")
                malformed["reply_id"] = "not-a-number"
                output.write(json.dumps(malformed) + "\n")
                output.write("{partial")
            result = context_x.seed_context(
                user_dir, db_path, dry_run=True, max_depth=10
            )
            self.assertEqual(result["reply_edges"], 1)
            self.assertEqual(result["malformed"], 1)
            self.assertFalse(db_path.exists())

    def test_incremental_seed_accepts_only_raw_files_inside_user_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, db_path = make_archive(
                root, [post("300", reply_id="200")]
            )
            raw = next((user_dir / "runs").glob("*/raw/*.jsonl"))
            result = context_x.seed_context(
                user_dir,
                db_path,
                dry_run=False,
                max_depth=10,
                raw_paths=[raw],
            )
            self.assertEqual(result["reply_edges"], 1)
            outside = root / "outside.jsonl"
            outside.write_text(json.dumps(post("1")), encoding="utf-8")
            with self.assertRaises(context_x.ContextError):
                context_x.seed_context(
                    user_dir,
                    db_path,
                    dry_run=True,
                    max_depth=10,
                    raw_paths=[outside],
                )


class ResolverConfigTests(unittest.TestCase):
    def test_config_is_focal_ancestor_only_and_metadata_first(self):
        config, raw = context_x.build_context_config(
            handle="alice",
            post_id="123",
            archive_root=Path("/archive"),
            user_dir=Path("/archive/users/alice"),
            cookie_file=Path("/cookies/x.txt"),
            work_dir=Path("/work"),
            media=False,
        )
        twitter = config["extractor"]["twitter"]
        self.assertNotIn("timeline", twitter)
        self.assertEqual(twitter["tweet-endpoint"], "rest")
        self.assertEqual(twitter["post-filter"], "tweet_id == 123")
        for key in ("conversations", "expand", "showreplies", "quoted", "pinned"):
            self.assertFalse(twitter[key])
        self.assertEqual(
            twitter["directory"][3:6],
            ["context", "{date:%Y}", "{date:%m}"],
        )
        self.assertEqual([p["event"] for p in twitter["postprocessors"]], ["post"])
        self.assertEqual(raw, Path("/work/current.posts.jsonl.partial"))

    def test_fetch_rejects_non_focal_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, _ = make_archive(root)

            def fake_run(_command, log_path, _label):
                work = user_dir / "_state" / "context-work"
                (work / "current.posts.jsonl.partial").write_text(
                    json.dumps(post("999")) + "\n", encoding="utf-8"
                )
                log_path.write_text("ok\n", encoding="utf-8")
                return 0, None, 0, False, [], 0, False, 0

            with mock.patch.object(context_x.archive_x, "run_gallery_dl", fake_run):
                with self.assertRaises(context_x.ContextError):
                    context_x.fetch_post(
                        repo_dir=REPO,
                        archive_root=root,
                        user_dir=user_dir,
                        handle="alice",
                        post_id="123",
                        cookie_file=Path("/cookies"),
                        media=False,
                    )


class SchedulerAndRecoveryTests(unittest.TestCase):
    def add(self, database, child, parent, depth=0):
        database.add_edge(
            child,
            parent,
            conversation_id=child,
            depth=depth,
            run_id="run",
            observed_at="now",
            max_depth=20,
        )

    def test_chain_first_then_fairness_yields_to_shallower_backlog(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                self.add(database, "500", "400")
                self.add(database, "300", "200")
                first = database.claim(
                    now=10, lease_seconds=10, fairness_quantum=1
                )
                self.assertEqual(first["post_id"], "400")
                parent = database.capture(
                    "400",
                    post("400", author_id="2", reply_id="390"),
                    source_kind="test",
                    target_user_id="1",
                    max_depth=20,
                )
                database.continue_chain(parent, fairness_quantum=1)
                second = database.claim(
                    now=11, lease_seconds=10, fairness_quantum=1
                )
                self.assertEqual(second["post_id"], "200")

    def test_chain_first_continues_within_quantum(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                self.add(database, "500", "400")
                self.add(database, "300", "200")
                first = database.claim(now=10, lease_seconds=10, fairness_quantum=5)
                parent = database.capture(
                    first["post_id"],
                    post(first["post_id"], author_id="2", reply_id="390"),
                    source_kind="test",
                    target_user_id="1",
                    max_depth=20,
                )
                database.continue_chain(parent, fairness_quantum=5)
                second = database.claim(now=11, lease_seconds=10, fairness_quantum=5)
                self.assertEqual(second["post_id"], "390")

    def test_stale_metadata_and_media_leases_are_independently_recovered(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                self.add(database, "200", "100")
                database.claim(now=10, lease_seconds=5, fairness_quantum=5)
                self.assertEqual(database.reclaim_stale(20, 5), 1)
                database.capture(
                    "100",
                    post("100", author_id="2", count=1),
                    source_kind="x:focal",
                    target_user_id="1",
                    max_depth=20,
                )
                database.claim(
                    now=30, lease_seconds=5, fairness_quantum=5, media=True
                )
                self.assertEqual(database.reclaim_stale(40, 5, media=True), 1)
                row = database.connection.execute(
                    "SELECT state,media_state FROM targets WHERE post_id='100'"
                ).fetchone()
                self.assertEqual(tuple(row), ("captured", "retryable"))

    def test_metadata_capture_succeeds_while_media_remains_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                self.add(database, "200", "100")
                database.capture(
                    "100",
                    post("100", author_id="2", count=2),
                    source_kind="x:focal",
                    target_user_id="1",
                    max_depth=20,
                )
                row = database.connection.execute(
                    "SELECT state,media_state FROM targets WHERE post_id='100'"
                ).fetchone()
                self.assertEqual(tuple(row), ("captured", "pending"))

    def test_media_completion_requires_asset_sidecar_and_matching_sha256(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "users" / "alice"
            media = user_dir / "media" / "context" / "2026" / "01"
            media.mkdir(parents=True)
            asset = media / "date_100_1_bob.jpg"
            sidecar = Path(str(asset) + ".json")
            asset.write_bytes(b"image")
            sidecar.write_text(
                json.dumps({"sha256": context_x.archive_x.sha256_file(asset)}),
                encoding="utf-8",
            )
            self.assertTrue(context_x.context_media_complete(user_dir, "100"))
            asset.write_bytes(b"changed")
            self.assertFalse(context_x.context_media_complete(user_dir, "100"))


class PacingAndFailureTests(unittest.TestCase):
    def result(self, log):
        return context_x.FetchResult(1, None, log, False, [], None)

    def test_failure_classification_is_conservative(self):
        cases = {
            "Tweet unavailable ('Deleted')": ("deleted", True, False),
            "Tweets are protected": ("private", True, False),
            "User has been suspended": ("suspended", True, False),
            "withheld in your country": ("withheld", True, False),
            "Could not authenticate you": ("authentication", False, True),
            "Dependency: Unspecified": ("transient", False, False),
            "surprising failure": ("unknown", False, False),
        }
        for log, expected in cases.items():
            with self.subTest(log=log):
                self.assertEqual(context_x.classify_failure(self.result(log)), expected)

    def test_pacing_not_before_survives_reopen_and_rate_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            sleeps = []
            with context_x.ContextDB(path) as database:
                context_x.reserve_request(
                    database, "5", now=lambda: 100, sleep=sleeps.append
                )
                context_x.persist_rate_reset(database, 120)
            with context_x.ContextDB(path, create=False) as database:
                context_x.reserve_request(
                    database, "5", now=lambda: 110, sleep=sleeps.append
                )
                reset = database.connection.execute(
                    "SELECT last_rate_limit_at FROM pacing"
                ).fetchone()[0]
            self.assertEqual(sleeps, [5, 15])
            self.assertEqual(reset, 120)

    def test_retry_attempts_are_bounded_and_sensitive_lines_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            with context_x.ContextDB(Path(directory) / "context.sqlite3") as database:
                database.upsert_target(
                    "100", conversation_id="100", depth=0, observed_at="now"
                )
                database.claim(now=10, lease_seconds=5, fairness_quantum=5)
                state = database.fail(
                    "100",
                    error_class="transient",
                    detail="Cookie: auth_token=secret\nordinary error",
                    now=10,
                    max_attempts=1,
                    retry_delay=30,
                )
                row = database.connection.execute(
                    "SELECT last_error_detail FROM targets WHERE post_id='100'"
                ).fetchone()[0]
                self.assertEqual(state, "manual_review")
                self.assertNotIn("secret", row)
                self.assertIn("ordinary error", row)

    def test_shared_exclusive_lock_rejects_a_second_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "archive-x.lock"
            with context_x.archive_x.exclusive_lock(lock):
                with self.assertRaises(context_x.archive_x.ArchiveError):
                    with context_x.archive_x.exclusive_lock(lock):
                        self.fail("second worker acquired the shared lock")


class WorkerAndDatasetTests(unittest.TestCase):
    def worker_args(self, root, user_dir, db_path, fetcher):
        return dict(
            repo_dir=REPO,
            archive_root=root,
            user_dir=user_dir,
            db_path=db_path,
            handle="alice",
            cookie_file=Path("unused"),
            max_posts=1,
            request_delay="0",
            retry_delay=1,
            max_attempts=3,
            lease_seconds=10,
            fairness_quantum=5,
            max_depth=10,
            media=False,
            fetcher=fetcher,
        )

    def test_operator_interrupt_immediately_releases_current_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, db_path = make_archive(
                root, [post("300", reply_id="200")]
            )
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=10)

            def interrupted(**_kwargs):
                raise KeyboardInterrupt

            with self.assertRaises(KeyboardInterrupt):
                context_x.run_worker(
                    **self.worker_args(root, user_dir, db_path, interrupted)
                )
            with context_x.ContextDB(db_path, create=False) as database:
                row = database.connection.execute(
                    "SELECT state,lease_started_at,last_error_class "
                    "FROM targets WHERE post_id='200'"
                ).fetchone()
            self.assertEqual(tuple(row), ("retryable", None, "interrupted"))

    def test_authentication_evidence_stops_whole_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, db_path = make_archive(
                root, [post("300", reply_id="200")]
            )
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=10)

            def authentication(**_kwargs):
                return context_x.FetchResult(
                    1, None, "Could not authenticate you", False, [], None
                )

            with self.assertRaises(context_x.ContextError):
                context_x.run_worker(
                    **self.worker_args(root, user_dir, db_path, authentication)
                )
            with context_x.ContextDB(db_path, create=False) as database:
                row = database.connection.execute(
                    "SELECT state,last_error_class FROM targets WHERE post_id='200'"
                ).fetchone()
            self.assertEqual(tuple(row), ("retryable", "authentication"))

    def test_no_budget_worker_closes_an_ancestor_chain_one_post_at_a_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, db_path = make_archive(
                root, [post("300", reply_id="200", conversation_id="100")]
            )
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=10)
            seen = []

            def fetcher(**kwargs):
                post_id = kwargs["post_id"]
                seen.append(post_id)
                parent = "100" if post_id == "200" else 0
                return context_x.FetchResult(
                    0,
                    post(post_id, author_id="2", author="bob", reply_id=parent),
                    "ok",
                    False,
                    [],
                    None,
                )

            counts = context_x.run_worker(
                repo_dir=REPO,
                archive_root=root,
                user_dir=user_dir,
                db_path=db_path,
                handle="alice",
                cookie_file=Path("unused"),
                max_posts=None,
                request_delay="0",
                retry_delay=1,
                max_attempts=3,
                lease_seconds=10,
                fairness_quantum=5,
                max_depth=10,
                media=False,
                fetcher=fetcher,
            )
            self.assertEqual(seen, ["200", "100"])
            self.assertEqual(counts["captured"], 2)

    def test_standalone_worker_limits_are_optional(self):
        parser = context_x.build_parser(REPO)
        run = parser.parse_args(["--user", "alice", "run"])
        media = parser.parse_args(["--user", "alice", "media"])
        self.assertIsNone(run.max_posts)
        self.assertIsNone(media.max_posts)

    def test_no_budget_worker_waits_for_bounded_retry_then_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, db_path = make_archive(
                root, [post("300", reply_id="200")]
            )
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=10)
            current = [100.0]
            calls = []
            sleeps = []

            def fetcher(**kwargs):
                calls.append(kwargs["post_id"])
                if len(calls) == 1:
                    return context_x.FetchResult(
                        1, None, "RemoteDisconnected", False, [], None
                    )
                return context_x.FetchResult(
                    0,
                    post("200", author_id="2", author="bob"),
                    "ok",
                    False,
                    [],
                    None,
                )

            def idle_sleep(seconds):
                sleeps.append(seconds)
                current[0] += seconds

            counts = context_x.run_worker(
                repo_dir=REPO,
                archive_root=root,
                user_dir=user_dir,
                db_path=db_path,
                handle="alice",
                cookie_file=Path("unused"),
                max_posts=None,
                request_delay="0",
                retry_delay=10,
                max_attempts=3,
                lease_seconds=10,
                fairness_quantum=5,
                max_depth=10,
                media=False,
                fetcher=fetcher,
                clock=lambda: current[0],
                idle_sleep=idle_sleep,
            )

            self.assertEqual(calls, ["200", "200"])
            self.assertTrue(sleeps)
            self.assertEqual(counts["captured"], 1)

    def test_export_is_deterministic_and_uses_stable_id_authorship(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, db_path = make_archive(root)
            with context_x.ContextDB(db_path) as database:
                database.bind_identity("1", "alice")
                database.add_edge(
                    "300", "200", conversation_id="100", depth=0,
                    run_id="run", observed_at="now", max_depth=10,
                )
                database.capture(
                    "200", post("200", author_id="2", author="bob"),
                    source_kind="test", target_user_id="1", max_depth=10,
                )
                database.add_edge(
                    "400", "100", conversation_id="100", depth=0,
                    run_id="run", observed_at="now", max_depth=10,
                )
                database.capture(
                    "100", post("100", author_id="1", user_id="2"),
                    source_kind="test", target_user_id="1", max_depth=10,
                )
            first = context_x.export_datasets(user_dir, db_path)
            paths = [
                user_dir / "dataset" / "context-posts.jsonl",
                user_dir / "dataset" / "reply-edges.jsonl",
                user_dir / "dataset" / "context-status.json",
            ]
            before = [path.read_bytes() for path in paths]
            second = context_x.export_datasets(user_dir, db_path)
            after = [path.read_bytes() for path in paths]
            self.assertEqual(first, second)
            self.assertEqual(before, after)
            posts = [json.loads(line) for line in before[0].decode().splitlines()]
            by_id = {row["post_id"]: row for row in posts}
            self.assertEqual(by_id["200"]["relationship"], "context")
            self.assertFalse(by_id["200"]["is_authored_by_requested_user"])
            self.assertEqual(by_id["200"]["canonical_requested_handle"], "alice")
            self.assertTrue(by_id["100"]["is_authored_by_requested_user"])


if __name__ == "__main__":
    unittest.main()
