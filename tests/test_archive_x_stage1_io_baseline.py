import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x
import archive_x_context as context_x


def raw_post(post_id: int, *, author_id: int = 1) -> dict:
    return {
        "tweet_id": post_id,
        "retweet_id": 0,
        "quote_id": 0,
        "reply_id": 0,
        "conversation_id": post_id,
        "date": "2026-01-01 00:00:00",
        "content": f"post {post_id}",
        "author": {"id": author_id, "name": "alice", "nick": "Alice"},
        "user": {"id": 1, "name": "alice", "nick": "Alice"},
        "count": 0,
        "archived_at": "2026-01-02T00:00:00Z",
    }


def normalized_post(post_id: int) -> dict:
    return {
        "post_id": str(post_id),
        "posted_at": "2026-01-01T00:00:00Z",
        "text": f"post {post_id}",
        "relationship": "post",
        "is_authored_by_requested_user": True,
        "first_captured_at": "2026-01-02T00:00:00Z",
        "last_captured_at": "2026-01-02T00:00:00Z",
        "capture_count": 1,
        "source_endpoints": ["timeline"],
        "metrics": {},
        "gallery_dl": {"tweet_id": post_id},
    }


def write_jsonl(path: Path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


class HistoricalIoBaselineTests(unittest.TestCase):
    def test_small_post_deltas_rewrite_all_portable_views_each_time(self):
        with tempfile.TemporaryDirectory() as directory:
            user = Path(directory) / "users" / "alice"
            dataset = user / "dataset"
            existing = [normalized_post(index) for index in range(1, 5_001)]
            write_jsonl(dataset / "posts.jsonl", existing)
            write_jsonl(dataset / "authored-posts.jsonl", existing)
            write_jsonl(dataset / "reposts.jsonl", ())
            baseline_posts = (dataset / "posts.jsonl").stat().st_size

            logical_read = 0
            logical_write = 0
            for offset in range(3):
                raw = user / "runs" / f"delta-{offset}" / "raw.jsonl"
                write_jsonl(raw, [raw_post(10_000 + offset)])
                logical_read += (dataset / "posts.jsonl").stat().st_size
                archive_x.update_post_dataset(user, "alice", raw, "timeline")
                logical_write += sum(
                    (dataset / name).stat().st_size
                    for name in (
                        "posts.jsonl",
                        "authored-posts.jsonl",
                        "reposts.jsonl",
                    )
                )

            self.assertGreaterEqual(logical_read, baseline_posts * 3)
            self.assertGreaterEqual(logical_write, baseline_posts * 5)
            self.assertEqual(
                sum(1 for _ in archive_x.iter_jsonl(dataset / "posts.jsonl")),
                5_003,
            )

    def test_unchanged_seed_rehashes_and_reparses_committed_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user = root / "users" / "alice"
            raw = user / "runs" / "run-a" / "raw" / "timeline.jsonl"
            write_jsonl(raw, (raw_post(index) for index in range(1, 2_001)))
            state = user / "_state" / "state.json"
            state.parent.mkdir(parents=True, exist_ok=True)
            state.write_text(
                json.dumps(
                    {
                        "requested_user_id": "1",
                        "requested_handle": "alice",
                        "canonical_handle": "alice",
                    }
                ),
                encoding="utf-8",
            )
            manifest = raw.parent.parent / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "run_id": "run-a",
                        "status": "success",
                        "post_dataset": {"dataset_posts": 2_000},
                        "endpoints": [
                            {
                                "endpoint": "timeline",
                                "raw_path": str(raw.relative_to(user)),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            db = user / "_state" / "context.sqlite3"
            original_iter = archive_x.iter_jsonl
            original_hash = archive_x.sha256_file
            counters = {"records": 0, "hash_bytes": 0}

            def counted_iter(path):
                for record in original_iter(path):
                    if Path(path) == raw:
                        counters["records"] += 1
                    yield record

            def counted_hash(path):
                if Path(path) == raw:
                    counters["hash_bytes"] += raw.stat().st_size
                return original_hash(path)

            with mock.patch.object(
                archive_x, "iter_jsonl", counted_iter
            ), mock.patch.object(archive_x, "sha256_file", counted_hash):
                context_x.seed_context(user, db, dry_run=False, max_depth=10)
                self.assertEqual(counters["records"], 4_000)
                counters.update(records=0, hash_bytes=0)
                second = context_x.seed_context(
                    user, db, dry_run=False, max_depth=10
                )

            self.assertEqual(second["files_processed"], 0)
            self.assertEqual(second["files_skipped"], 1)
            self.assertEqual(counters["records"], 2_000)
            self.assertEqual(counters["hash_bytes"], raw.stat().st_size)

    def test_unchanged_media_index_and_context_export_are_fully_rebuilt(self):
        with tempfile.TemporaryDirectory() as directory:
            user = Path(directory) / "users" / "alice"
            media = user / "media" / "2026" / "01"
            for index in range(100):
                asset = media / f"2026-01-01T00-00-00_{index + 1}_1_alice.jpg"
                asset.parent.mkdir(parents=True, exist_ok=True)
                asset.write_bytes(b"asset")
                Path(str(asset) + ".json").write_text(
                    json.dumps(
                        {
                            "tweet_id": index + 1,
                            "num": 1,
                            "date": "2026-01-01 00:00:00",
                            "author": {"name": "alice"},
                            "sha256": "0" * 64,
                        }
                    ),
                    encoding="utf-8",
                )
            original_load = archive_x.load_json
            sidecar_reads = 0

            def counted_load(path, default=None):
                nonlocal sidecar_reads
                if str(path).endswith(".jpg.json"):
                    sidecar_reads += 1
                return original_load(path, default)

            with mock.patch.object(archive_x, "load_json", counted_load):
                archive_x.update_media_dataset(user, "alice")
                first_reads = sidecar_reads
                sidecar_reads = 0
                archive_x.update_media_dataset(user, "alice")
                second_reads = sidecar_reads
            self.assertEqual((first_reads, second_reads), (100, 100))
            media_view = user / "dataset" / "media.jsonl"
            self.assertGreater(media_view.stat().st_size, 0)

            state = user / "_state" / "state.json"
            state.parent.mkdir(parents=True, exist_ok=True)
            state.write_text(
                json.dumps(
                    {
                        "requested_user_id": "1",
                        "requested_handle": "alice",
                        "canonical_handle": "alice",
                    }
                ),
                encoding="utf-8",
            )
            db = user / "_state" / "context.sqlite3"
            with context_x.ContextDB(db) as context:
                context.bind_identity("1", "alice")
                with context_x.transaction(context.connection):
                    for index in range(1, 1_001):
                        post = raw_post(index, author_id=2)
                        raw_json = json.dumps(post, sort_keys=True)
                        context.connection.execute(
                            """INSERT INTO targets(
                                   post_id,state,discovered_at,updated_at
                               ) VALUES (?,'pending',?,?)""",
                            (str(index), "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                        )
                        context.connection.execute(
                            """INSERT INTO observations(
                                   post_id,captured_at,source_kind,raw_json,sha256
                               ) VALUES (?,?,?,?,?)""",
                            (
                                str(index),
                                "2026-01-01T00:00:00Z",
                                "fixture",
                                raw_json,
                                "0" * 64,
                            ),
                        )
                        context.connection.execute(
                            "UPDATE targets SET state='captured' WHERE post_id=?",
                            (str(index),),
                        )

            context_x.export_datasets(user, db)
            post_export = user / "dataset" / "context-posts.jsonl"
            first_inode = post_export.stat().st_ino
            first_size = post_export.stat().st_size
            context_x.export_datasets(user, db)
            self.assertNotEqual(post_export.stat().st_ino, first_inode)
            self.assertEqual(post_export.stat().st_size, first_size)
            self.assertGreater(first_size, 0)

    def test_context_completion_rehashes_large_asset_on_each_check(self):
        with tempfile.TemporaryDirectory() as directory:
            user = Path(directory) / "users" / "alice"
            asset = (
                user
                / "media"
                / "context"
                / "2026"
                / "01"
                / "2026-01-01T00-00-00_123_1_alice.mp4"
            )
            asset.parent.mkdir(parents=True, exist_ok=True)
            asset.write_bytes(b"v" * (2 * 1024 * 1024))
            digest = archive_x.sha256_file(asset)
            Path(str(asset) + ".json").write_text(
                json.dumps({"tweet_id": 123, "num": 1, "sha256": digest}),
                encoding="utf-8",
            )
            original_hash = archive_x.sha256_file
            bytes_hashed = 0

            def counted_hash(path):
                nonlocal bytes_hashed
                bytes_hashed += Path(path).stat().st_size
                return original_hash(path)

            with mock.patch.object(archive_x, "sha256_file", counted_hash):
                self.assertTrue(context_x.context_media_complete(user, "123"))
                self.assertTrue(context_x.context_media_complete(user, "123"))

            self.assertEqual(bytes_hashed, 4 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
