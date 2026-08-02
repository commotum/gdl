import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location(
    "archive_x_measure", REPO / "scripts" / "archive_x_measure.py"
)
measure = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(measure)


def write_bytes(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


def make_large_fixture(root: Path, rows: int = 20_000) -> Path:
    user = root / "users" / "alice"
    sizes = {
        "posts.jsonl": 1_000,
        "authored-posts.jsonl": 800,
        "reposts.jsonl": 200,
        "media.jsonl": 300,
        "context-posts.jsonl": 600,
        "reply-edges.jsonl": 400,
    }
    for name, size in sizes.items():
        write_bytes(user / "dataset" / name, size)

    modern = user / "runs" / "modern" / "raw" / "timeline.jsonl"
    legacy_a = user / "runs" / "legacy" / "raw" / "a.jsonl"
    legacy_b = user / "runs" / "legacy" / "raw" / "b.jsonl"
    write_bytes(modern, 101)
    write_bytes(legacy_a, 103)
    write_bytes(legacy_b, 107)
    modern_manifest = {
        "mode": "modern",
        "endpoints": [
            {
                "endpoint": "info",
                "raw_path": "runs/modern/raw/info.jsonl",
            },
            {
                "endpoint": "timeline",
                "raw_path": str(modern.relative_to(user)),
            },
        ],
    }
    legacy_manifest = {
        "mode": "legacy_backfill",
        "windows": [
            {
                "state_committed": True,
                "canonical_raw_path": str(legacy_a.relative_to(user)),
                "canonical_post_count": 4,
                "walks": [
                    {"status": "valid", "search_requests": 3, "api_requests": 4},
                    {"status": "valid", "search_requests": 3, "api_requests": 4},
                ],
            },
            {
                "state_committed": True,
                "canonical_raw_path": str(legacy_b.relative_to(user)),
                "canonical_post_count": 6,
                "walks": [
                    {"status": "ambiguous", "search_requests": 1, "api_requests": 2},
                    {"status": "valid", "search_requests": 2, "api_requests": 3},
                ],
            },
        ],
    }
    for name, value in (("modern", modern_manifest), ("legacy", legacy_manifest)):
        path = user / "runs" / name / "manifest.json"
        path.write_text(json.dumps(value), encoding="utf-8")

    db = user / "_state" / "context.sqlite3"
    db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db)
    connection.executescript(
        """CREATE TABLE targets(
               post_id TEXT PRIMARY KEY,
               state TEXT NOT NULL,
               next_attempt_at REAL NOT NULL,
               depth_min INTEGER NOT NULL,
               media_state TEXT NOT NULL,
               media_next_attempt_at REAL NOT NULL
           );
           CREATE TABLE reply_edges(
               child_id TEXT PRIMARY KEY,
               parent_id TEXT NOT NULL
           );
           CREATE INDEX reply_edges_parent ON reply_edges(parent_id);
           CREATE TABLE observations(post_id TEXT PRIMARY KEY);"""
    )
    connection.executemany(
        "INSERT INTO targets VALUES(?,?,?,?,?,?)",
        (
            (
                str(index + 1),
                "pending" if index % 3 else "captured",
                0,
                index % 50,
                "pending" if index % 3 == 0 else "none",
                0,
            )
            for index in range(rows)
        ),
    )
    connection.executemany(
        "INSERT INTO reply_edges VALUES(?,?)",
        ((f"c{index}", str(index % rows + 1)) for index in range(rows // 2)),
    )
    connection.executemany(
        "INSERT INTO observations VALUES(?)",
        ((str(index * 3 + 1),) for index in range(rows // 3)),
    )
    connection.commit()
    connection.close()
    return user


class ReadOnlyMeasurementTests(unittest.TestCase):
    def test_reproduces_full_rewrite_and_full_scan_baselines_without_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user = make_large_fixture(root)
            before = {
                path.relative_to(root): (path.stat().st_size, path.stat().st_mtime_ns)
                for path in root.rglob("*")
                if path.is_file()
            }

            result = measure.inspect_user(root, "Alice")

            after = {
                path.relative_to(root): (path.stat().st_size, path.stat().st_mtime_ns)
                for path in root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(before, after)
            self.assertTrue(result["read_only"])
            self.assertEqual(result["raw_sources"], {"files": 3, "bytes": 311})
            self.assertGreaterEqual(result["manifests"]["scan_elapsed_us"], 0)
            self.assertEqual(result["legacy"]["committed_windows"], 2)
            self.assertEqual(result["legacy"]["walks"], 4)
            self.assertEqual(result["legacy"]["search_requests"], 9)
            self.assertEqual(result["legacy"]["api_requests"], 13)
            self.assertEqual(result["legacy"]["canonical_posts"], 10)
            self.assertEqual(
                result["runner_processes"],
                {
                    "endpoint_starts": 2,
                    "legacy_walk_starts": 4,
                    "total_known_starts": 6,
                },
            )
            io = result["baseline_logical_io"]
            self.assertEqual(io["one_post_merge_read_bytes"], 1_000)
            self.assertEqual(io["one_post_merge_write_bytes"], 2_000)
            self.assertEqual(io["legacy_window_merge_read_write_bytes"], 6_000)
            self.assertEqual(io["one_context_export_write_bytes"], 1_000)
            self.assertEqual(io["unchanged_seed_payload_bytes_revisited"], 311)
            metadata_plan = "\n".join(result["context"]["metadata_claim_plan"])
            self.assertIn("SCAN t", metadata_plan)
            self.assertIn("USE TEMP B-TREE FOR ORDER BY", metadata_plan)
            media_plan = "\n".join(result["context"]["media_claim_plan"])
            self.assertIn("SCAN targets", media_plan)
            self.assertIn("USE TEMP B-TREE FOR ORDER BY", media_plan)
            self.assertEqual(result["context"]["rows"]["targets"], 20_000)

    def test_rejects_unsafe_or_missing_user_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "users").mkdir()
            with self.assertRaisesRegex(measure.MeasurementError, "X handle"):
                measure.inspect_user(root, "../alice")
            with self.assertRaisesRegex(measure.MeasurementError, "does not exist"):
                measure.inspect_user(root, "alice")


if __name__ == "__main__":
    unittest.main()
