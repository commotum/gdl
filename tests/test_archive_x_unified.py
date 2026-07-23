import importlib
import io
import json
import sys
import tempfile
import unittest
from argparse import Namespace
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

archive_x = importlib.import_module("archive_x")
context_x = importlib.import_module("archive_x_context")
unified_x = importlib.import_module("archive_x_unified")


def unified_args(**overrides):
    values = {
        "cookies": Path("unused"),
        "request_delay": "0",
        "retries": 1,
        "http_timeout": 60,
        "stalled_rate_limit_cycles": 3,
        "modern_max_posts": None,
        "legacy_max_windows": None,
        "context_max_posts": None,
        "context_media_max_posts": None,
        "post_limit": None,
        "since": None,
        "retry_failed_only": False,
        "full_rescan": False,
        "overlap_hours": 48.0,
    }
    values.update(overrides)
    return Namespace(**values)


def empty_archive(root: Path):
    user_dir = root / "users" / "alice"
    state = user_dir / "_state" / "state.json"
    archive_x.atomic_write_json(
        state,
        {
            "schema": archive_x.SCHEMA_NAME,
            "schema_version": archive_x.SCHEMA_VERSION,
            "requested_handle": "alice",
            "canonical_handle": "alice",
            "requested_user_id": "1",
            "resume": None,
            "pending_media": [],
        },
    )
    raw = user_dir / "runs" / "run-modern" / "raw" / "timeline.posts.jsonl"
    archive_x.atomic_write_jsonl(raw, [])
    archive_x.atomic_write_json(
        raw.parents[1] / "manifest.json",
        {
            "run_id": "run-modern",
            "status": "success",
            "completed_at": "2026-07-22T12:00:00Z",
            "post_dataset": {"dataset_posts": 0},
            "endpoints": [
                {
                    "endpoint": "timeline",
                    "raw_path": str(raw.relative_to(user_dir)),
                }
            ],
        },
    )
    return user_dir


def reply_archive(root: Path):
    user_dir = empty_archive(root)
    raw = user_dir / "runs" / "run-modern" / "raw" / "timeline.posts.jsonl"
    archive_x.atomic_write_jsonl(
        raw,
        [
            {
                "tweet_id": "300",
                "date": "2026-07-20 12:00:00",
                "archived_at": "2026-07-22T12:00:00Z",
                "author": {"id": "1", "name": "alice"},
                "user": {"id": "1", "name": "alice"},
                "reply_id": "200",
                "conversation_id": "100",
                "retweet_id": None,
                "count": 0,
            }
        ],
    )
    return user_dir


def transition_archive(root: Path):
    user_dir = root / "users" / "alice"
    state_path = user_dir / "_state" / "state.json"
    archive_x.atomic_write_json(
        state_path,
        {
            "schema": archive_x.SCHEMA_NAME,
            "schema_version": archive_x.SCHEMA_VERSION,
            "requested_handle": "alice",
            "canonical_handle": "alice",
            "requested_user_id": "1",
            "resume": {
                "cursor": "3_29116490825/",
                "started_at": "2026-07-20T02:39:18Z",
                "date_after": None,
                "saved_at": "2026-07-21T01:04:43Z",
            },
            "pending_media": [],
        },
    )
    archive_x.atomic_write_json(
        user_dir / "dataset" / "profile.json",
        {"profile": {"id": 1, "name": "alice", "date": "2008-10-21 12:01:00"}},
    )
    archive_x.atomic_write_jsonl(
        user_dir / "dataset" / "posts.jsonl",
        [{"post_id": "29116490825", "posted_at": "2010-10-29 19:30:34"}],
    )
    run_id = "run-transition"
    raw = user_dir / "runs" / run_id / "raw" / "timeline.posts.incomplete.jsonl"
    archive_x.atomic_write_jsonl(
        raw,
        [
            {
                "tweet_id": "29116490825",
                "date": "2010-10-29 19:30:34",
                "archived_at": "2026-07-20T12:00:00Z",
                "author": {"id": "1", "name": "alice"},
                "user": {"id": "1", "name": "alice"},
                "reply_id": None,
                "retweet_id": None,
                "count": 0,
            }
        ],
    )
    archive_x.atomic_write_json(
        raw.parents[1] / "manifest.json",
        {
            "run_id": run_id,
            "started_at": "2026-07-20T02:39:18Z",
            "completed_at": "2026-07-21T01:04:43Z",
            "status": "stalled",
            "failure_stage": "timeline_no_progress_watchdog",
            "reposts_included": True,
            "limited_run": False,
            "retry_failed_only": False,
            "date_after": None,
            "post_dataset": {"dataset_posts": 1},
            "endpoints": [
                {
                    "endpoint": "timeline",
                    "status": "stalled",
                    "exit_code": 1,
                    "interrupted": False,
                    "stalled": True,
                    "stalled_rate_limit_cycles": 3,
                    "resume_cursor": "3_29116490825/",
                    "metadata_complete": False,
                    "other_error_count": 0,
                    "raw_has_record": True,
                    "raw_path": str(raw.relative_to(user_dir)),
                }
            ],
        },
    )
    return user_dir, state_path


class UnifiedOrchestrationTests(unittest.TestCase):
    def test_unavailable_media_is_a_successful_warning_status(self):
        self.assertEqual(
            unified_x.overall_status(
                {
                    "modern": {"status": "complete_with_unavailable_media"},
                    "legacy": {"status": "complete"},
                    "shared_media": {
                        "status": "complete_with_unavailable_media"
                    },
                    "context_media": {"status": "complete"},
                }
            ),
            "complete_with_unavailable_media",
        )

    def test_overall_status_accepts_only_the_exact_initialized_boundary_stall(self):
        self.assertEqual(
            unified_x.overall_status(
                {
                    "modern": {"status": "stalled"},
                    "transition": {"status": "initialized"},
                    "legacy": {"status": "complete"},
                }
            ),
            "success",
        )
        for transition in ("already_initialized", "ambiguous", "not_applicable"):
            with self.subTest(transition=transition):
                self.assertEqual(
                    unified_x.overall_status(
                        {
                            "modern": {"status": "stalled"},
                            "transition": {"status": transition},
                        }
                    ),
                    "failed",
                )

    def test_normal_parser_requires_no_phase_limits(self):
        args = archive_x.build_parser(REPO).parse_args(["--user", "alice"])
        self.assertIsNone(args.legacy_max_windows)
        self.assertIsNone(args.modern_max_posts)
        self.assertIsNone(args.context_max_posts)
        self.assertIsNone(args.context_media_max_posts)

    def test_diagnostic_modes_never_launch_backlog_phases(self):
        for args in (
            unified_args(post_limit=5),
            unified_args(since=archive_x.parse_datetime("2026-01-01")),
        ):
            with self.subTest(args=args), mock.patch.object(
                unified_x,
                "accept_transition",
                side_effect=AssertionError("transition"),
            ), mock.patch.object(
                unified_x,
                "run_legacy_scheduler",
                side_effect=AssertionError("legacy"),
            ), mock.patch.object(
                context_x,
                "seed_context",
                side_effect=AssertionError("context"),
            ):
                result = unified_x.run_unified_followups(
                    args,
                    REPO,
                    Path("/archive"),
                    "1.32.4",
                    {"alice": {"run_id": "modern", "status": "limited"}},
                )
            self.assertEqual(result["alice"]["status"], "limited")
            self.assertEqual(
                result["alice"]["transition"]["status"], "skipped_diagnostic"
            )

    def test_bounded_modern_rollout_continues_only_for_initialized_legacy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, _state_path = transition_archive(root)
            unified_x.legacy_x.automatic_initialize_legacy(
                user_dir,
                initialized_at="2026-07-22T12:00:00Z",
                expected_run_id="run-transition",
            )
            args = unified_args(
                modern_max_posts=5,
                legacy_max_windows=1,
                context_max_posts=1,
                context_media_max_posts=1,
            )
            with mock.patch.object(
                unified_x,
                "run_legacy_scheduler",
                return_value={"alice": {"status": "limited", "runs": []}},
            ) as legacy:
                result = unified_x.run_unified_followups(
                    args,
                    REPO,
                    root,
                    "1.32.4",
                    {"alice": {"run_id": "bounded-head", "status": "limited"}},
                )

            legacy.assert_called_once()
            self.assertEqual(result["alice"]["status"], "limited")
            self.assertEqual(
                result["alice"]["transition"]["status"], "already_initialized"
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty_archive(root)
            with mock.patch.object(
                unified_x,
                "run_legacy_scheduler",
                side_effect=AssertionError("fresh bounded handoff"),
            ):
                result = unified_x.run_unified_followups(
                    unified_args(modern_max_posts=5),
                    REPO,
                    root,
                    "1.32.4",
                    {"alice": {"run_id": "bounded", "status": "limited"}},
                )
            self.assertEqual(result["alice"]["status"], "limited")

    def test_existing_legacy_never_overrides_a_modern_auth_or_identity_failure(self):
        with mock.patch.object(
            unified_x,
            "accept_transition",
            return_value={"status": "already_initialized"},
        ), mock.patch.object(
            unified_x,
            "run_legacy_scheduler",
            side_effect=AssertionError("unsafe legacy after modern failure"),
        ), mock.patch.object(
            context_x,
            "seed_context",
            side_effect=AssertionError("unsafe context after modern failure"),
        ):
            result = unified_x.run_unified_followups(
                unified_args(),
                REPO,
                Path("/archive"),
                "1.32.4",
                {"alice": {"run_id": "modern", "status": "failed"}},
            )

        self.assertEqual(result["alice"]["status"], "failed")
        self.assertEqual(
            result["alice"]["transition"]["status"], "already_initialized"
        )

    def test_retry_only_skips_metadata_and_runs_context_media_readout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = empty_archive(root)
            db_path = user_dir / "_state" / "context.sqlite3"
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=1000)
            calls = []

            def worker(args, repo_dir, archive_root, handle, *, media, max_posts):
                calls.append(media)
                return {
                    "status": "complete",
                    "counts": {"attempted": 0},
                    "availability": {
                        "total": 0,
                        "ready": 0,
                        "manual_review": 0,
                        "next_eligible_at": None,
                    },
                }

            with mock.patch.object(
                context_x,
                "seed_context",
                side_effect=AssertionError("retry-only seed"),
            ), mock.patch.object(
                unified_x, "run_context_worker", side_effect=worker
            ):
                result = unified_x.run_unified_followups(
                    unified_args(retry_failed_only=True),
                    REPO,
                    root,
                    "1.32.4",
                    {
                        "alice": {
                            "run_id": "modern",
                            "status": "success",
                            "media_recovery": {
                                "pending_before": 1,
                                "pending_after": 0,
                            },
                        }
                    },
                )

            self.assertEqual(calls, [True])
            self.assertNotIn("context_seed", result["alice"])
            self.assertNotIn("context_metadata", result["alice"])
            self.assertEqual(result["alice"]["context_media"]["status"], "complete")

    def test_retry_only_identity_failure_never_launches_context_media(self):
        with mock.patch.object(
            unified_x,
            "run_context_scheduler",
            side_effect=AssertionError("unsafe media after identity failure"),
        ):
            result = unified_x.run_unified_followups(
                unified_args(retry_failed_only=True),
                REPO,
                Path("/archive"),
                "1.32.4",
                {"alice": {"run_id": "modern", "status": "failed"}},
            )

        self.assertEqual(result["alice"]["status"], "failed")

    def test_dry_run_previews_all_phases_without_writes_or_locks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "archive-does-not-exist"
            output = io.StringIO()
            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x,
                "atomic_write_json",
                side_effect=AssertionError("dry-run write"),
            ), mock.patch.object(
                archive_x,
                "exclusive_lock",
                side_effect=AssertionError("dry-run lock"),
            ), redirect_stdout(output):
                status = archive_x.main(
                    [
                        "--user",
                        "alice",
                        "--output-root",
                        str(root),
                        "--dry-run",
                    ]
                )

            self.assertEqual(status, 0)
            self.assertFalse(root.exists())
            text = output.getvalue()
            self.assertIn("phase 1: modern", text)
            self.assertIn("phase 2: guarded automatic legacy", text)
            self.assertIn("phase 3: seed and drain ancestor-only", text)
            self.assertIn("context: bootstrap from committed sources", text)
            self.assertIn("archive filesystem in this process: read-write", text)

    def test_real_run_refuses_read_only_filesystem_before_any_archive_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            errors = io.StringIO()
            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x, "filesystem_is_read_only", return_value=True
            ), mock.patch.object(
                archive_x,
                "atomic_write_json",
                side_effect=AssertionError("read-only preflight write"),
            ), mock.patch.object(
                archive_x,
                "exclusive_lock",
                side_effect=AssertionError("read-only preflight lock"),
            ), redirect_stderr(errors), self.assertRaises(SystemExit) as raised:
                archive_x.main(
                    ["--user", "alice", "--output-root", str(root)]
                )

            self.assertEqual(raised.exception.code, 2)
            self.assertIn("mounted read-only", errors.getvalue())

    def test_dry_run_reads_context_queue_without_migration_or_byte_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = reply_archive(root)
            db_path = user_dir / "_state" / "context.sqlite3"
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=1000)
            before = archive_x.sha256_file(db_path)
            output = io.StringIO()
            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x,
                "atomic_write_json",
                side_effect=AssertionError("dry-run write"),
            ), mock.patch.object(
                context_x,
                "backup_context_before_v2",
                side_effect=AssertionError("dry-run migration"),
            ), redirect_stdout(output):
                status = archive_x.main(
                    [
                        "--user",
                        "alice",
                        "--output-root",
                        str(root),
                        "--legacy-max-windows",
                        "1",
                        "--context-max-posts",
                        "2",
                        "--context-media-max-posts",
                        "3",
                        "--dry-run",
                    ]
                )

            self.assertEqual(status, 0)
            self.assertEqual(archive_x.sha256_file(db_path), before)
            text = output.getvalue()
            self.assertIn("advanced legacy bound: 1", text)
            self.assertIn("advanced context metadata bound: 2", text)
            self.assertIn("advanced context media bound: 3", text)
            self.assertIn("1 metadata pending", text)
            self.assertIn("integrity=ok", text)

    def test_dry_run_reports_completed_legacy_without_exposing_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _user_dir, state_path = transition_archive(root)
            unified_x.legacy_x.automatic_initialize_legacy(
                root / "users" / "alice",
                initialized_at="2026-07-22T12:00:00Z",
                expected_run_id="run-transition",
            )
            state = archive_x.load_json(state_path, {})
            legacy = state["legacy_backfill"]
            legacy["status"] = "complete"
            legacy["next_until"] = legacy["floor_since"]
            legacy["active_window"] = None
            legacy["manual_review"] = None
            legacy["coverage_conclusion"] = "source_visible_to_account_creation"
            archive_x.atomic_write_json(state_path, state)
            output = io.StringIO()
            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x,
                "atomic_write_json",
                side_effect=AssertionError("dry-run write"),
            ), redirect_stdout(output):
                status = archive_x.main(
                    [
                        "--user",
                        "alice",
                        "--output-root",
                        str(root),
                        "--dry-run",
                    ]
                )

            self.assertEqual(status, 0)
            text = output.getvalue()
            self.assertIn("legacy: complete", text)
            self.assertIn("modern: incremental head update", text)
            self.assertNotIn("3_29116490825", text)

    def test_retry_only_dry_run_does_not_preview_metadata_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty_archive(root)
            args = archive_x.build_parser(REPO).parse_args(
                ["--user", "alice", "--retry-failed-only"]
            )
            output = io.StringIO()
            with redirect_stdout(output):
                archive_x.dry_run_summary(args, root, ["alice"], "1.32.4")

            text = output.getvalue()
            self.assertIn(
                "modern/legacy/context metadata: skipped by retry-only mode", text
            )
            self.assertIn("context media: skipped; context database absent", text)
            self.assertNotIn("initial source-visible historical crawl", text)

    def test_no_backlog_fixture_runs_seed_workers_and_export_without_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = empty_archive(root)
            with mock.patch("subprocess.run", side_effect=AssertionError("subprocess")):
                result = unified_x.run_unified_followups(
                    unified_args(),
                    REPO,
                    root,
                    "1.32.4",
                    {"alice": {"run_id": "run-modern", "status": "success"}},
                )

            self.assertEqual(result["alice"]["status"], "success")
            self.assertEqual(
                result["alice"]["transition"]["status"], "not_applicable"
            )
            self.assertEqual(result["alice"]["legacy"]["status"], "not_applicable")
            self.assertEqual(
                result["alice"]["context_metadata"]["status"], "complete"
            )
            self.assertEqual(result["alice"]["context_media"]["status"], "complete")
            self.assertTrue((user_dir / "_state" / "context.sqlite3").is_file())
            self.assertTrue((user_dir / "dataset" / "context-status.json").is_file())

    def test_unified_context_recurses_and_keeps_other_author_parents(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = reply_archive(root)
            original_worker = context_x.run_worker

            def fetcher(**kwargs):
                post_id = kwargs["post_id"]
                parent_id = "100" if post_id == "200" else None
                author_id = "2" if post_id == "200" else "3"
                author_name = "bob" if post_id == "200" else "carol"
                return context_x.FetchResult(
                    status=0,
                    metadata={
                        "tweet_id": post_id,
                        "date": "2026-07-19 12:00:00",
                        "archived_at": "2026-07-22T12:01:00Z",
                        "author": {"id": author_id, "name": author_name},
                        "user": {"id": author_id, "name": author_name},
                        "reply_id": parent_id,
                        "conversation_id": "100",
                        "retweet_id": None,
                        "count": 0,
                    },
                    log="",
                    interrupted=False,
                    failed_downloads=[],
                    rate_reset=None,
                )

            def worker_with_fixture(**kwargs):
                return original_worker(**kwargs, fetcher=fetcher)

            with mock.patch.object(
                context_x, "run_worker", side_effect=worker_with_fixture
            ), mock.patch("subprocess.run", side_effect=AssertionError("subprocess")):
                result = unified_x.run_unified_followups(
                    unified_args(),
                    REPO,
                    root,
                    "1.32.4",
                    {"alice": {"run_id": "run-modern", "status": "success"}},
                )

            self.assertEqual(result["alice"]["status"], "success")
            posts = list(
                archive_x.iter_jsonl(user_dir / "dataset" / "context-posts.jsonl")
            )
            self.assertEqual({post["post_id"] for post in posts}, {"100", "200"})
            self.assertTrue(
                all(not post["is_authored_by_requested_user"] for post in posts)
            )
            edges = list(
                archive_x.iter_jsonl(user_dir / "dataset" / "reply-edges.jsonl")
            )
            self.assertEqual(
                {
                    (edge["child_post_id"], edge["parent_post_id"])
                    for edge in edges
                },
                {("300", "200"), ("200", "100")},
            )

    def test_exact_stall_initializes_then_rechecks_head_and_hands_off(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir, state_path = transition_archive(root)
            head = {"run_id": "head-run", "status": "success"}
            with mock.patch.object(
                unified_x.archive_x, "archive_user", return_value=head
            ) as modern_head, mock.patch.object(
                unified_x,
                "run_legacy_scheduler",
                return_value={"alice": {"status": "complete", "runs": []}},
            ):
                result = unified_x.run_unified_followups(
                    unified_args(),
                    REPO,
                    root,
                    "1.32.4",
                    {"alice": {"run_id": "run-transition", "status": "stalled"}},
                )

            state = archive_x.load_json(state_path, {})
            self.assertEqual(result["alice"]["status"], "success")
            self.assertEqual(result["alice"]["transition"]["status"], "initialized")
            self.assertEqual(
                result["alice"]["modern_head_after_transition"], head
            )
            modern_head.assert_called_once()
            self.assertEqual(state["resume"]["cursor"], "3_29116490825/")
            self.assertEqual(state["legacy_backfill"]["status"], "pending")
            self.assertEqual(state["modern_head"]["active"], None)
            backup = user_dir / result["alice"]["transition"]["backup_path"]
            self.assertTrue(backup.is_file())

    def test_multi_user_legacy_scheduler_round_robins_internal_windows(self):
        args = unified_args(legacy_max_windows=2)
        statuses = {"alice": "pending", "bob": "pending"}
        calls = []

        def state_status(user_dir):
            return statuses[user_dir.name]

        def fake_run(options, repo_dir, archive_root, handle, version):
            calls.append(handle)
            return {
                "run_id": f"run-{handle}-{calls.count(handle)}",
                "status": "limited",
                "windows": [{"state_committed": True}],
            }

        with mock.patch.object(
            unified_x, "legacy_state_status", side_effect=state_status
        ), mock.patch.object(
            unified_x.legacy_x, "verify_legacy_runner", return_value=None
        ), mock.patch.object(
            unified_x.legacy_x, "run_legacy_archive", side_effect=fake_run
        ), mock.patch.object(
            unified_x.archive_x, "sleep_random", return_value=0
        ):
            result = unified_x.run_legacy_scheduler(
                args, REPO, Path("/archive"), "1.32.4", ["alice", "bob"]
            )

        self.assertEqual(calls, ["alice", "bob", "alice", "bob"])
        self.assertEqual(result["alice"]["status"], "limited")
        self.assertEqual(result["bob"]["status"], "limited")

    def test_legacy_failure_isolated_to_one_user(self):
        args = unified_args(legacy_max_windows=1)
        calls = []

        def fake_run(options, repo_dir, archive_root, handle, version):
            calls.append(handle)
            if handle == "alice":
                raise archive_x.ArchiveError("alice legacy fault")
            return {
                "run_id": "run-bob",
                "status": "limited",
                "windows": [{"state_committed": True}],
            }

        with mock.patch.object(
            unified_x, "legacy_state_status", return_value="pending"
        ), mock.patch.object(
            unified_x.legacy_x, "verify_legacy_runner", return_value=None
        ), mock.patch.object(
            unified_x.legacy_x, "run_legacy_archive", side_effect=fake_run
        ):
            result = unified_x.run_legacy_scheduler(
                args, REPO, Path("/archive"), "1.32.4", ["alice", "bob"]
            )

        self.assertEqual(calls, ["alice", "bob"])
        self.assertEqual(result["alice"]["status"], "failed")
        self.assertEqual(result["bob"]["status"], "limited")

    def test_legacy_runner_preflight_failure_is_a_phase_result(self):
        with mock.patch.object(
            unified_x, "legacy_state_status", return_value="pending"
        ), mock.patch.object(
            unified_x.legacy_x,
            "verify_legacy_runner",
            side_effect=archive_x.ArchiveError("legacy runner mismatch"),
        ), mock.patch.object(
            unified_x.legacy_x,
            "run_legacy_archive",
            side_effect=AssertionError("legacy run"),
        ):
            result = unified_x.run_legacy_scheduler(
                unified_args(),
                REPO,
                Path("/archive"),
                "1.32.4",
                ["alice", "bob"],
            )

        self.assertEqual(result["alice"]["status"], "failed")
        self.assertEqual(result["bob"]["status"], "failed")

    def test_multi_user_context_scheduler_round_robins_internal_quanta(self):
        calls = []
        rounds = {"alice": 0, "bob": 0}

        def fake_worker(args, repo_dir, archive_root, handle, *, media, max_posts):
            calls.append(handle)
            rounds[handle] += 1
            complete = rounds[handle] == 2
            return {
                "status": "complete" if complete else "limited",
                "counts": {"attempted": 1},
                "availability": {
                    "total": 0 if complete else 1,
                    "ready": 0 if complete else 1,
                    "manual_review": 0,
                    "next_eligible_at": None,
                },
            }

        with mock.patch.object(
            unified_x, "run_context_worker", side_effect=fake_worker
        ):
            result = unified_x.run_context_scheduler(
                unified_args(),
                REPO,
                Path("/archive"),
                ["alice", "bob"],
                media=False,
            )

        self.assertEqual(calls, ["alice", "bob", "alice", "bob"])
        self.assertEqual(result["alice"]["status"], "complete")
        self.assertEqual(result["bob"]["status"], "complete")

    def test_context_authentication_error_is_a_global_stop(self):
        with mock.patch.object(
            unified_x,
            "run_context_worker",
            side_effect=context_x.ContextAuthenticationError("auth"),
        ), self.assertRaises(context_x.ContextAuthenticationError):
            unified_x.run_context_scheduler(
                unified_args(),
                REPO,
                Path("/archive"),
                ["alice", "bob"],
                media=False,
            )

    def test_seed_failure_does_not_starve_an_independent_user(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty_archive(root)
            bob_dir = root / "users" / "alice"
            bob_target = root / "users" / "bob"
            bob_target.parent.mkdir(parents=True, exist_ok=True)
            bob_dir.rename(bob_target)
            alice_dir = empty_archive(root)
            original_seed = context_x.seed_context

            def seed(user_dir, db_path, **kwargs):
                if user_dir == alice_dir:
                    raise context_x.ContextError("alice seed fault")
                return original_seed(user_dir, db_path, **kwargs)

            with mock.patch.object(context_x, "seed_context", side_effect=seed):
                result = unified_x.run_unified_followups(
                    unified_args(),
                    REPO,
                    root,
                    "1.32.4",
                    {
                        "alice": {"run_id": "run-modern", "status": "success"},
                        "bob": {"run_id": "run-modern", "status": "success"},
                    },
                )

            self.assertEqual(result["alice"]["status"], "failed")
            self.assertEqual(result["alice"]["context_seed"]["status"], "failed")
            self.assertEqual(result["bob"]["status"], "success")
            self.assertTrue(
                (bob_target / "dataset" / "context-status.json").is_file()
            )

    def test_export_failure_preserves_seed_commit_for_ordinary_rerun(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty_archive(root)
            modern = {"alice": {"run_id": "run-modern", "status": "success"}}
            with mock.patch.object(
                context_x,
                "export_datasets",
                side_effect=context_x.ContextError("export fault"),
            ):
                first = unified_x.run_unified_followups(
                    unified_args(), REPO, root, "1.32.4", modern
                )

            second = unified_x.run_unified_followups(
                unified_args(), REPO, root, "1.32.4", modern
            )

            self.assertEqual(first["alice"]["status"], "failed")
            self.assertEqual(first["alice"]["context_seed"]["files_processed"], 1)
            self.assertEqual(second["alice"]["status"], "success")
            self.assertEqual(second["alice"]["context_seed"]["files_processed"], 0)
            self.assertEqual(second["alice"]["context_seed"]["files_skipped"], 1)

    def test_context_manual_review_is_visible_and_never_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = reply_archive(root)
            db_path = user_dir / "_state" / "context.sqlite3"
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=1000)
            with context_x.ContextDB(db_path, create=False) as database:
                database.connection.execute(
                    "UPDATE targets SET state='manual_review' WHERE post_id='200'"
                )
                database.connection.commit()

            result = unified_x.run_unified_followups(
                unified_args(),
                REPO,
                root,
                "1.32.4",
                {"alice": {"run_id": "run-modern", "status": "success"}},
            )

            self.assertEqual(result["alice"]["status"], "manual_review")
            self.assertEqual(
                result["alice"]["context_metadata"]["status"], "manual_review"
            )
            with context_x.ContextDB(db_path, create=False) as database:
                row = database.connection.execute(
                    "SELECT state,attempts FROM targets WHERE post_id='200'"
                ).fetchone()
            self.assertEqual((row["state"], row["attempts"]), ("manual_review", 0))

    def test_manual_review_does_not_hide_other_actionable_context_work(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = reply_archive(root)
            db_path = user_dir / "_state" / "context.sqlite3"
            context_x.seed_context(user_dir, db_path, dry_run=False, max_depth=1000)
            with context_x.ContextDB(db_path, create=False) as database:
                database.connection.execute(
                    "UPDATE targets SET state='manual_review' WHERE post_id='200'"
                )
                database.upsert_target(
                    "201",
                    conversation_id="101",
                    depth=0,
                    observed_at="2026-07-22T12:00:00Z",
                )
                database.connection.commit()

            status = unified_x.context_phase_status(db_path, media=False)

            self.assertEqual(status["status"], "pending")
            self.assertEqual(status["availability"]["manual_review"], 1)
            self.assertEqual(status["availability"]["ready"], 1)

    def test_legacy_preparation_failure_creates_no_modern_run_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "alice"
            archive_x.atomic_write_json(
                user_dir / "_state" / "state.json",
                {"legacy_backfill": {"status": "pending"}},
            )
            legacy_module = mock.Mock()
            legacy_module.automatic_initialize_legacy.side_effect = (
                archive_x.ArchiveError("migration fault")
            )

            with mock.patch.object(
                archive_x.importlib, "import_module", return_value=legacy_module
            ), self.assertRaises(archive_x.ArchiveError):
                archive_x.archive_user(
                    unified_args(), REPO, root, "alice", "1.32.4"
                )

            self.assertFalse((user_dir / "runs").exists())

    def test_main_holds_two_outer_locks_once_and_calls_unified_phases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            locks = []

            @contextmanager
            def fake_lock(path):
                locks.append(Path(path))
                yield

            modern = {"run_id": "modern-run", "status": "success"}
            combined = {
                "alice": {"modern": modern, "status": "success"}
            }
            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x, "exclusive_lock", side_effect=fake_lock
            ), mock.patch.object(
                archive_x, "archive_user", return_value=modern
            ), mock.patch.object(
                unified_x, "run_unified_followups", return_value=combined
            ) as followups:
                status = archive_x.main(
                    ["--user", "alice", "--output-root", str(root)]
                )

            self.assertEqual(status, 0)
            self.assertEqual(len(locks), 2)
            followups.assert_called_once()
            invocation = list((root / "runs").glob("*.json"))
            self.assertEqual(len(invocation), 1)
            saved = json.loads(invocation[0].read_text(encoding="utf-8"))
            self.assertEqual(saved["results"][0]["status"], "success")
            self.assertEqual(saved["status"], "success")

    def test_main_completes_every_modern_target_before_backlog_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = []

            @contextmanager
            def fake_lock(path):
                yield

            def modern(args, repo_dir, archive_root, handle, version):
                calls.append(f"modern:{handle}")
                return {"run_id": f"modern-{handle}", "status": "success"}

            def followups(args, repo_dir, archive_root, version, modern_results, **kwargs):
                calls.append("backlogs")
                self.assertEqual(list(modern_results), ["alice", "bob"])
                return {
                    handle: {"modern": value, "status": "success"}
                    for handle, value in modern_results.items()
                }

            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x, "exclusive_lock", side_effect=fake_lock
            ), mock.patch.object(
                archive_x, "sleep_random", return_value=0
            ), mock.patch.object(
                archive_x, "archive_user", side_effect=modern
            ), mock.patch.object(
                unified_x, "run_unified_followups", side_effect=followups
            ):
                status = archive_x.main(
                    [
                        "--user",
                        "alice",
                        "--user",
                        "bob",
                        "--output-root",
                        str(root),
                    ]
                )

            self.assertEqual(status, 0)
            self.assertEqual(calls, ["modern:alice", "modern:bob", "backlogs"])

    def test_main_interrupt_during_modern_finalizes_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            @contextmanager
            def fake_lock(path):
                yield

            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x, "exclusive_lock", side_effect=fake_lock
            ), mock.patch.object(
                archive_x, "archive_user", side_effect=KeyboardInterrupt
            ):
                status = archive_x.main(
                    ["--user", "alice", "--output-root", str(root)]
                )

            self.assertEqual(status, 130)
            invocation = list((root / "runs").glob("*.json"))
            self.assertEqual(len(invocation), 1)
            saved = json.loads(invocation[0].read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "interrupted")
            self.assertEqual(saved["results"][0]["status"], "interrupted")

    def test_main_interrupt_in_followups_keeps_last_phase_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            @contextmanager
            def fake_lock(path):
                yield

            modern = {"run_id": "modern-run", "status": "success"}

            def interrupt_followups(*args, checkpoint, **kwargs):
                checkpoint(
                    {
                        "alice": {
                            "modern": modern,
                            "transition": {"status": "already_initialized"},
                            "legacy": {"status": "complete"},
                        }
                    }
                )
                raise KeyboardInterrupt

            with mock.patch.object(
                archive_x, "validate_cookie_file", return_value={"x.com"}
            ), mock.patch.object(
                archive_x, "gallery_dl_version", return_value="1.32.4"
            ), mock.patch.object(
                archive_x, "verify_gallery_dl_x_runner", return_value=None
            ), mock.patch.object(
                archive_x, "exclusive_lock", side_effect=fake_lock
            ), mock.patch.object(
                archive_x, "archive_user", return_value=modern
            ), mock.patch.object(
                unified_x, "run_unified_followups", side_effect=interrupt_followups
            ):
                status = archive_x.main(
                    ["--user", "alice", "--output-root", str(root)]
                )

            self.assertEqual(status, 130)
            invocation = list((root / "runs").glob("*.json"))
            saved = json.loads(invocation[0].read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "interrupted")
            self.assertEqual(
                saved["results"][0]["phases"]["legacy"]["status"], "complete"
            )

    def test_next_start_finalizes_abandoned_root_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            abandoned = root / "runs" / "old-run.json"
            archive_x.atomic_write_json(
                abandoned,
                {
                    "invocation_id": "old-run",
                    "started_at": "2026-07-22T11:00:00Z",
                    "status": "running",
                    "results": [
                        {
                            "requested_handle": "alice",
                            "status": "running",
                            "phases": {"modern": {"status": "success"}},
                        }
                    ],
                },
            )
            newer = root / "runs" / "newer-waiter.json"
            archive_x.atomic_write_json(
                newer,
                {
                    "invocation_id": "newer-waiter",
                    "started_at": "2026-07-22T13:00:00Z",
                    "status": "running",
                    "results": [],
                },
            )

            finalized = archive_x.finalize_abandoned_invocations(
                root,
                current_invocation_id="new-run",
                current_started_at="2026-07-22T12:00:00Z",
                recovered_at="2026-07-22T12:00:00Z",
            )

            self.assertEqual(finalized, ["old-run"])
            saved = archive_x.load_json(abandoned, {})
            self.assertEqual(saved["status"], "interrupted")
            self.assertEqual(
                saved["failure_stage"],
                "process_ended_before_invocation_finalization",
            )
            self.assertTrue(saved["finalized_on_later_startup"])
            self.assertEqual(
                archive_x.load_json(newer, {})["status"], "running"
            )


if __name__ == "__main__":
    unittest.main()
