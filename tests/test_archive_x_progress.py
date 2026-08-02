import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sys

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import archive_x_progress as progress
import archive_x_dashboard as dashboard


def make_context(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE targets(
          post_id TEXT PRIMARY KEY, state TEXT, media_state TEXT,
          last_error_class TEXT,
          updated_at TEXT NOT NULL DEFAULT '2026-01-01T00:10:00Z');
        CREATE TABLE observations(
          post_id TEXT PRIMARY KEY, source_kind TEXT);
        CREATE TABLE reply_edges(
          child_id TEXT PRIMARY KEY, parent_id TEXT, conversation_id TEXT);
        """
    )
    targets = [
        ("1", "captured", "captured", None),
        ("2", "captured", "pending", None),
        ("3", "unavailable", "none", "deleted"),
        ("4", "unavailable", "none", "protected"),
        ("5", "unavailable", "none", "suspended"),
        ("6", "pending", "none", None),
        ("7", "retryable", "retryable", "transient"),
        ("8", "manual_review", "manual_review", "unknown"),
    ]
    connection.executemany(
        "INSERT INTO targets(post_id,state,media_state,last_error_class) "
        "VALUES (?,?,?,?)",
        targets,
    )
    connection.executemany(
        "INSERT INTO observations VALUES (?,?)",
        [("1", "x:focal"), ("2", "local:modern")],
    )
    connection.executemany(
        "INSERT INTO reply_edges VALUES (?,?,?)",
        [
            ("c1", "1", "a"), ("c2", "3", "b"), ("c3", "6", "c"),
            ("c4", "7", "d"), ("c5", "8", "e"),
        ],
    )
    connection.commit()
    connection.close()


class ProgressSignalsTests(unittest.TestCase):
    def test_read_only_context_metrics_keep_outcomes_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            make_context(path)
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            result = progress.collect_context_metrics(path)
            after = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(before, after)
        self.assertEqual(result["context_captured"], 2)
        self.assertEqual(result["context_parents_saved"], 1)
        self.assertEqual(result["context_unavailable"], 3)
        self.assertEqual(result["context_known_remaining"], 2)
        self.assertEqual(result["context_manual_review"], 1)
        self.assertEqual(result["context_media_actionable"], 2)
        self.assertEqual(result["context_media_captured"], 1)
        self.assertEqual(result["context_media_manual_review"], 1)
        self.assertEqual(result["context_media_remaining"], 3)
        self.assertEqual(result["conversations_closed"], 2)
        self.assertEqual(result["boundaries_deleted"], 1)
        self.assertEqual(result["boundaries_private"], 1)
        self.assertEqual(result["boundaries_suspended"], 1)

    def test_fast_context_metrics_skip_only_conversation_closure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            make_context(path)
            result = progress.collect_context_fast_metrics(path)

        self.assertEqual(result["context_captured"], 2)
        self.assertEqual(result["context_known_remaining"], 2)
        self.assertNotIn("conversations_closed", result)
        self.assertNotIn("conversations_total", result)

    def test_archive_metrics_use_manifest_not_terminal_or_file_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            user = Path(directory)
            run = user / "runs" / "one"
            run.mkdir(parents=True)
            (run / "manifest.json").write_text(json.dumps({
                "post_dataset": {"dataset_posts": 1234},
                "media_dataset": {"media_files": 56, "media_bytes": 7890},
            }))
            result = progress.collect_archive_metrics(user)
        self.assertEqual(result, {
            "archive_posts": 1234,
            "archive_media_files": 56,
            "archive_media_bytes": 7890,
        })

    def test_archive_metrics_follow_committed_legacy_window(self):
        with tempfile.TemporaryDirectory() as directory:
            user = Path(directory)
            modern = user / "runs" / "modern" / "manifest.json"
            modern.parent.mkdir(parents=True)
            modern.write_text(json.dumps({
                "post_dataset": {"dataset_posts": 100},
                "media_dataset": {"media_files": 5, "media_bytes": 900},
            }))
            legacy = user / "runs" / "legacy" / "manifest.json"
            legacy.parent.mkdir(parents=True)
            legacy.write_text(json.dumps({
                "mode": "legacy_backfill",
                "windows": [
                    {
                        "state_committed": True,
                        "dataset": {"dataset_posts": 112},
                    },
                    {"status": "running"},
                ],
            }))
            # A later-touched modern manifest may have the pre-legacy total;
            # dataset counts are monotonic, so the committed maximum wins.
            os.utime(legacy, (1, 1))
            os.utime(modern, (2, 2))

            result = progress.collect_archive_metrics(user)

        self.assertEqual(result["archive_posts"], 112)
        self.assertEqual(result["archive_media_files"], 5)
        self.assertEqual(result["archive_media_bytes"], 900)

    def test_export_generations_are_live_constant_size_dashboard_truth(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "context.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE archive_generation(
                  singleton INTEGER PRIMARY KEY,current_generation INTEGER);
                INSERT INTO archive_generation VALUES (1,12);
                CREATE TABLE current_pointers(
                  pointer_name TEXT PRIMARY KEY,generation INTEGER);
                INSERT INTO current_pointers VALUES ('portable_export',9);
                CREATE TABLE export_views(
                  view_name TEXT PRIMARY KEY,status TEXT,
                  durable_generation INTEGER,exported_generation INTEGER);
                INSERT INTO export_views VALUES ('posts','dirty',12,9);
                INSERT INTO export_views VALUES ('media','current',9,9);
                """
            )
            connection.commit()
            connection.close()

            result = progress.collect_export_metrics(path)

        self.assertEqual(result["archive_durable_generation"], 12)
        self.assertEqual(result["archive_exported_generation"], 9)
        self.assertEqual(result["archive_dirty_views"], 1)

    def test_estimator_uses_net_wall_clock_burn_and_hides_false_precision(self):
        estimate, rate = progress.estimate_known_queue([
            {"at": 0, "known_remaining": 1000, "resolved": 0},
            {"at": 3600, "known_remaining": 900, "resolved": 200},
        ], 900)
        self.assertEqual(estimate["seconds"], 32400)
        self.assertEqual(estimate["confidence"], "medium")
        self.assertEqual(rate["items_per_hour"], 200)
        growing, growing_rate = progress.estimate_known_queue([
            {"at": 0, "known_remaining": 1000, "resolved": 0},
            {"at": 3600, "known_remaining": 1100, "resolved": 200},
        ], 1100)
        self.assertIsNone(growing["seconds"])
        self.assertEqual(growing["qualifier"], "still discovering")
        self.assertEqual(growing_rate["items_per_hour"], 200)
        young, _ = progress.estimate_known_queue([
            {"at": 0, "known_remaining": 100, "resolved": 0},
            {"at": 60, "known_remaining": 90, "resolved": 10},
        ], 90)
        self.assertEqual(young["qualifier"], "collecting samples")
        preliminary, preliminary_rate = progress.estimate_known_queue([
            {"at": 0, "known_remaining": 100, "resolved": 0},
            {"at": 300, "known_remaining": 90, "resolved": 20},
        ], 90)
        self.assertEqual(preliminary["seconds"], 2700)
        self.assertEqual(preliminary["confidence"], "low")
        self.assertEqual(preliminary_rate["items_per_hour"], 240)
        blocked, _ = progress.estimate_known_queue([], 10, blocked=True)
        self.assertEqual(blocked["qualifier"], "phase blocked")
        blocked_with_rate, blocked_rate = progress.estimate_known_queue([
            {"at": 0, "known_remaining": 1000, "resolved": 0},
            {"at": 3600, "known_remaining": 900, "resolved": 200},
        ], 900, blocked=True)
        self.assertEqual(blocked_with_rate["qualifier"], "phase blocked")
        self.assertEqual(blocked_rate["items_per_hour"], 200)

    def test_context_samples_start_after_queue_seeding(self):
        samples = progress.phase_queue_samples([
            {"at": 0, "known_remaining": 0, "resolved": 0},
            {"at": 100, "known_remaining": 0, "resolved": 0},
            {"at": 200, "known_remaining": 8742, "resolved": 728},
            {"at": 500, "known_remaining": 8711, "resolved": 774},
        ], "context_metadata")

        self.assertEqual(
            [sample["known_remaining"] for sample in samples],
            [8742, 8711],
        )
        estimate, _rate = progress.estimate_known_queue(samples, 8711)
        self.assertIsNotNone(estimate["seconds"])
        self.assertEqual(estimate["confidence"], "low")

    def test_live_media_phase_uses_media_queue_for_eta(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_path = (
                root / "users" / "alice" / "_state" / "context.sqlite3"
            )
            make_context(db_path)
            totals = progress.empty_totals()
            totals.update({"context_captured": 100})
            snapshot = {
                "schema": progress.SCHEMA, "schema_version": 1,
                "invocation_id": "run",
                "started_at": "1970-01-01T00:00:00Z",
                "updated_at": "1970-01-01T00:10:00Z",
                "status": "running",
                "users": [{
                    "handle": "alice", "phase": "context_media",
                    "health": "healthy", "activity": "fetching 8",
                    "last_progress_at": "1970-01-01T00:19:59Z",
                    "wait_until": None,
                    "phases": {"context_media": "running"},
                    "totals": totals, "baseline": progress.empty_totals(),
                    "delta": totals,
                    # These are old-producer metadata samples: the zero marks
                    # the handoff into context media.
                    "samples": [
                        {"at": 0, "known_remaining": 100, "resolved": 0},
                        {"at": 600, "known_remaining": 0, "resolved": 100},
                    ],
                    "rate": None,
                    "estimate": {
                        "seconds": 0, "label": "~0s", "confidence": "low",
                        "qualifier": "known queue", "known_remaining": 0,
                    },
                    "action_required": 0,
                }],
            }

            result = progress.LiveProgressReader(root).enrich(
                snapshot, now=1200
            )

        user = result["users"][0]
        self.assertEqual(user["totals"]["context_media_actionable"], 2)
        self.assertEqual(user["totals"]["context_media_captured"], 1)
        self.assertEqual(user["totals"]["context_media_manual_review"], 1)
        self.assertEqual(user["estimate"]["seconds"], 600)
        self.assertEqual(user["rate"]["items_per_hour"], 12)
        output = dashboard.render(result, width=120, now=1200, unicode=False)
        self.assertIn("Media      1 saved", output)
        self.assertIn("1 review", output)
        self.assertIn("2 remaining", output)
        self.assertIn("~10m to finish media pass", output)

    def test_live_reader_derives_legacy_frontier_rate_and_eta(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "alice"
            state_path = user_dir / "_state" / "state.json"
            state_path.parent.mkdir(parents=True)
            state_path.write_text(json.dumps({
                "legacy_backfill": {
                    "status": "active",
                    "initial_until": "2010-10-25T00:00:00Z",
                    "next_until": "2010-10-19T00:00:00Z",
                    "floor_since": "2010-10-07T00:00:00Z",
                    "active_window": {
                        "owner_run_id": "legacy-run",
                        "since": "2010-10-16T00:00:00Z",
                        "until": "2010-10-19T00:00:00Z",
                    },
                },
            }))
            # During context seeding the SQLite file can exist before all
            # tables do. That must not suppress the valid timeline overlay.
            sqlite3.connect(state_path.parent / "context.sqlite3").close()
            manifest = user_dir / "runs" / "legacy-run" / "manifest.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(json.dumps({
                "mode": "legacy_backfill",
                "started_at": "2026-01-01T00:30:00Z",
                "windows": [
                    {
                        "since": "2010-10-22T00:00:00Z",
                        "until": "2010-10-25T00:00:00Z",
                        "state_committed": True,
                        "canonical_post_count": 5,
                        "dataset": {
                            "new_run_posts": 5, "dataset_posts": 105,
                        },
                    },
                    {
                        "since": "2010-10-19T00:00:00Z",
                        "until": "2010-10-22T00:00:00Z",
                        "state_committed": True,
                        "canonical_post_count": 7,
                        "dataset": {
                            "new_run_posts": 7, "dataset_posts": 112,
                        },
                    },
                ],
            }))
            totals = progress.empty_totals()
            totals["archive_posts"] = 100
            snapshot = {
                "schema": progress.SCHEMA, "schema_version": 1,
                "invocation_id": "run",
                "started_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "status": "running",
                "users": [{
                    "handle": "alice", "phase": "legacy",
                    "health": "stale", "activity": "checking legacy coverage",
                    "last_progress_at": "2026-01-01T00:00:00Z",
                    "wait_until": None, "phases": {"legacy": "running"},
                    "totals": totals, "baseline": dict(totals),
                    "delta": progress.empty_totals(), "samples": [],
                    "rate": None,
                    "estimate": {
                        "seconds": None, "label": None, "confidence": "none",
                        "qualifier": "collecting samples", "known_remaining": 0,
                    },
                    "action_required": 0,
                }],
            }
            now = progress.parse_time("2026-01-01T01:00:00Z")
            result = progress.LiveProgressReader(root).enrich(
                snapshot, now=now
            )

        user = result["users"][0]
        self.assertEqual(user["totals"]["archive_posts"], 112)
        self.assertEqual(user["delta"]["archive_posts"], 12)
        self.assertEqual(user["legacy"]["committed_windows"], 2)
        self.assertEqual(user["legacy"]["completed_seconds"], 6 * 86400)
        self.assertEqual(user["rate"]["coverage_days_per_hour"], 12.0)
        self.assertEqual(user["estimate"]["seconds"], 3600)
        output = dashboard.render(result, width=100, now=now, unicode=False)
        self.assertIn("6.0/18.0 days covered", output)
        self.assertIn("~1h 0m to account creation", output)
        self.assertIn("+12 posts", output)

    def test_nonactive_manual_review_does_not_hide_context_eta(self):
        totals = progress.empty_totals()
        totals.update({
            "context_captured": 100,
            "context_known_remaining": 1000,
        })
        later = dict(totals)
        later.update({
            "context_captured": 300,
            "context_known_remaining": 900,
        })
        current = [0.0]
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            progress,
            "collect_user_totals",
            side_effect=[totals, later],
        ):
            tracker = progress.ProgressTracker(
                Path(directory) / "progress.json",
                Path(directory),
                "run",
                ["alice"],
                "2026-01-01T00:00:00Z",
                clock=lambda: current[0],
            )
            tracker.event(
                "alice",
                phase="legacy",
                phase_status="manual_review",
                force=False,
            )
            tracker.event(
                "alice",
                phase="context_metadata",
                phase_status="running",
                force=False,
            )
            current[0] = 3600
            tracker.refresh(force=True)
            user = tracker.users["alice"]

        self.assertEqual(user["action_required"], 1)
        self.assertEqual(user["health"], "healthy")
        self.assertEqual(user["rate"]["items_per_hour"], 200)
        self.assertEqual(user["estimate"]["qualifier"], "known queue")

    def test_health_precedence(self):
        self.assertEqual(progress.derive_health(
            status="failed", action_required=3
        ), "failed")
        self.assertEqual(progress.derive_health(
            status="running", phase_status="manual_review", action_required=1
        ), "blocked")
        self.assertEqual(progress.derive_health(
            status="running", phase_status="running", action_required=1
        ), "healthy")
        self.assertEqual(progress.derive_health(
            status="running", phase_status="retrying"
        ), "retrying")
        self.assertEqual(progress.derive_health(
            status="running", wait_until="2026-01-01T01:00:00Z", now=0
        ), "waiting")
        self.assertEqual(progress.derive_health(
            status="running", last_progress_at="2026-01-01T00:00:00Z",
            now=1767226501,
        ), "stale")

    def test_schema_rejects_unknown_and_secret_fields(self):
        totals = progress.empty_totals()
        snapshot = {
            "schema": progress.SCHEMA, "schema_version": 1,
            "invocation_id": "run", "started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z", "status": "running",
            "users": [{
                "handle": "alice", "phase": "modern", "health": "healthy",
                "activity": "working", "last_progress_at": None,
                "wait_until": None, "phases": {"modern": "running"},
                "totals": totals, "baseline": totals, "delta": totals,
                "samples": [], "rate": None,
                "estimate": {
                    "seconds": None, "label": None, "confidence": "none",
                    "qualifier": "collecting samples", "known_remaining": 0,
                },
                "action_required": 0,
            }],
        }
        progress.validate_snapshot(snapshot)
        snapshot["cookie"] = "secret"
        with self.assertRaises(progress.ProgressError):
            progress.validate_snapshot(snapshot)
        snapshot.pop("cookie")
        snapshot["mystery"] = 1
        with self.assertRaises(progress.ProgressError):
            progress.validate_snapshot(snapshot)

    def test_tracker_writes_private_atomic_snapshot_and_preserves_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user = root / "users" / "alice"
            run = user / "runs" / "one"
            run.mkdir(parents=True)
            (run / "manifest.json").write_text(json.dumps({
                "post_dataset": {"dataset_posts": 2},
                "media_dataset": {"media_files": 1, "media_bytes": 9},
            }))
            make_context(user / "_state" / "context.sqlite3")
            path = root / "_state" / "progress" / "run.json"
            with mock.patch.object(progress, "utc_now", return_value="2026-01-01T00:00:00Z"):
                tracker = progress.ProgressTracker(
                    path, root, "run", ["alice"],
                    "2026-01-01T00:00:00Z", clock=lambda: 0,
                )
            saved = json.loads(path.read_text())
            progress.validate_snapshot(saved)
            self.assertEqual(saved["users"][0]["baseline"]["archive_posts"], 2)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_renderer_is_compact_width_bounded_and_plain(self):
        totals = progress.empty_totals()
        totals.update({
            "archive_posts": 258962, "archive_media_files": 50696,
            "archive_media_bytes": 27_100_000_000,
            "context_parents_saved": 6861, "context_unavailable": 3553,
            "context_known_remaining": 117936,
            "conversations_closed": 6561,
        })
        user = {
            "handle": "visakanv", "phase": "context_metadata",
            "health": "healthy", "activity": "fetching 434278834846715904",
            "last_progress_at": "2026-01-01T00:59:57Z", "wait_until": None,
            "phases": {"context_metadata": "running"}, "totals": totals,
            "baseline": progress.empty_totals(), "delta": totals,
            "samples": [], "rate": {"items_per_hour": 302.4, "window_seconds": 3600},
            "estimate": {
                "seconds": 16 * 86400, "label": "~16d", "confidence": "low",
                "qualifier": "known queue", "known_remaining": 117936,
            },
            "action_required": 0,
        }
        output = dashboard.render({
            "schema": progress.SCHEMA, "schema_version": 1,
            "invocation_id": "run", "started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T01:00:00Z", "status": "running",
            "users": [user],
        }, width=80, now=1767229200, unicode=False)
        self.assertEqual(len(output.splitlines()), 7)
        self.assertTrue(all(len(line) <= 80 for line in output.splitlines()))
        self.assertNotIn("\033", output)
        self.assertIn("117,936 known remaining", output)

        user["totals"].update(
            {
                "archive_durable_generation": 12,
                "archive_exported_generation": 9,
                "archive_dirty_views": 2,
            }
        )
        dirty_output = dashboard.render({
            "schema": progress.SCHEMA, "schema_version": 1,
            "invocation_id": "run", "started_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T01:00:00Z", "status": "running",
            "users": [user],
        }, width=80, now=1767229200, unicode=False)
        self.assertIn("Export     durable 12 | published 9 | 2 views pending", dirty_output)

    def test_tmux_adapter_is_optional_and_uses_argument_vectors(self):
        calls = []
        result = mock.Mock(stdout="%42\n")

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return result

        pane = progress.start_tmux_dashboard(
            Path("/tmp/run.json"), "run-1", Path("/repo"),
            environ={"TMUX": "/tmp/socket"}, isatty=True,
            terminal_size=__import__("os").terminal_size((100, 40)),
            runner=runner,
        )
        self.assertEqual(pane, "%42")
        self.assertEqual(calls[0][0][:3], ["tmux", "split-window", "-v"])
        self.assertIn("-d", calls[0][0])
        self.assertNotIn("-b", calls[0][0])
        self.assertEqual(
            calls[0][0][calls[0][0].index("-l") + 1],
            str(progress.DASHBOARD_PANE_LINES),
        )
        self.assertNotIsInstance(calls[0][0], str)
        self.assertIn("archive-x-dashboard:run-1", calls[1][0])
        self.assertIsNone(progress.start_tmux_dashboard(
            Path("/tmp/run.json"), "run-1", Path("/repo"),
            environ={"TMUX": "/tmp/socket", "ARCHIVE_X_DASHBOARD": "off"},
            isatty=True,
        ))

    def test_tmux_adapter_accepts_nominal_80_by_24_terminal_inside_tmux(self):
        calls = []

        def runner(command, **kwargs):
            calls.append(command)
            return mock.Mock(stdout="%42\n")

        pane = progress.start_tmux_dashboard(
            Path("/tmp/run.json"), "run-1", Path("/repo"),
            environ={"TMUX": "/tmp/socket"}, isatty=True,
            # tmux commonly consumes one row from a nominal 80x24 terminal.
            terminal_size=__import__("os").terminal_size((80, 23)),
            runner=runner,
        )

        self.assertEqual(pane, "%42")
        self.assertEqual(len(calls), 2)
        for size in ((71, 23), (80, 19)):
            with self.subTest(size=size):
                self.assertIsNone(progress.start_tmux_dashboard(
                    Path("/tmp/run.json"), "run-1", Path("/repo"),
                    environ={"TMUX": "/tmp/socket"}, isatty=True,
                    terminal_size=__import__("os").terminal_size(size),
                    runner=runner,
                ))

    def test_reporting_failure_disables_itself_without_escaping(self):
        tracker = mock.Mock(path=Path("/tmp/snapshot"))
        tracker.event.side_effect = OSError("disk unavailable")
        safe = progress.SafeProgressTracker(tracker)
        safe.event("alice")
        safe.event("alice")
        self.assertTrue(safe.failed)
        self.assertEqual(tracker.event.call_count, 1)


if __name__ == "__main__":
    unittest.main()
