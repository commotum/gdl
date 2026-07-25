import hashlib
import json
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
          last_error_class TEXT);
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
    connection.executemany("INSERT INTO targets VALUES (?,?,?,?)", targets)
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
        self.assertEqual(result["conversations_closed"], 2)
        self.assertEqual(result["boundaries_deleted"], 1)
        self.assertEqual(result["boundaries_private"], 1)
        self.assertEqual(result["boundaries_suspended"], 1)

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
        blocked, _ = progress.estimate_known_queue([], 10, blocked=True)
        self.assertEqual(blocked["qualifier"], "phase blocked")

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
        self.assertNotIsInstance(calls[0][0], str)
        self.assertIn("archive-x-dashboard:run-1", calls[1][0])
        self.assertIsNone(progress.start_tmux_dashboard(
            Path("/tmp/run.json"), "run-1", Path("/repo"),
            environ={"TMUX": "/tmp/socket", "ARCHIVE_X_DASHBOARD": "off"},
            isatty=True,
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
