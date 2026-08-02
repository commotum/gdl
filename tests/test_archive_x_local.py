import hashlib
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x
import archive_x_context as context_x
import archive_x_local as local_x
import archive_x_progress as progress_x


def metadata(
    post_id: str,
    *,
    reply_id: str | None = None,
    author_id: str = "1",
    requested_user_id: str = "1",
) -> dict:
    return {
        "tweet_id": int(post_id),
        "retweet_id": 0,
        "quote_id": 0,
        "reply_id": int(reply_id) if reply_id else 0,
        "conversation_id": int(reply_id or post_id),
        "date": "2026-01-01 00:00:00",
        "date_original": None,
        "content": f"post {post_id}",
        "author": {"id": int(author_id), "name": "alice"},
        "user": {"id": int(requested_user_id), "name": "alice"},
        "archived_at": "2026-01-02T00:00:00Z",
        "subcategory": "timeline",
        "count": 0,
    }


class LocalStateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.user_dir = self.root / "users" / "alice"
        self.state_dir = self.user_dir / "_state"
        self.run_dir = self.user_dir / "runs" / "run-one"
        self.run_dir.mkdir(parents=True)
        self.state_dir.mkdir(parents=True)
        archive_x.atomic_write_json(
            self.state_dir / "state.json",
            {
                "requested_user_id": "1",
                "requested_handle": "alice",
                "canonical_handle": "alice",
            },
        )
        self.db_path = self.state_dir / "context.sqlite3"
        with context_x.ContextDB(self.db_path) as database:
            database.bind_identity("1", "alice")

    def tearDown(self):
        self.temporary.cleanup()

    def source(self, rows: list[dict], name: str = "timeline.posts.jsonl"):
        path = self.run_dir / name
        archive_x.atomic_write_jsonl(path, rows)
        return path

    def spec(self, path: Path, *, expected: str | None = None):
        return local_x.SourceSpec(
            path=path,
            source_kind="modern",
            run_id="run-one",
            operation_id="run-one:timeline",
            endpoint="timeline",
            expected_sha256=expected,
        )

    def ingest(self, path: Path, **kwargs):
        return local_x.ingest_source_once(
            self.user_dir,
            self.db_path,
            requested_handle="alice",
            target_user_id="1",
            spec=self.spec(path, **kwargs),
        )

    def test_source_is_streamed_once_then_exact_stat_hit_reads_zero_bytes(self):
        path = self.source(
            [metadata("100"), metadata("101", reply_id="100")]
        )
        first = self.ingest(path)

        self.assertEqual(first["bytes_read"], path.stat().st_size)
        self.assertEqual(first["new_posts"], 2)
        self.assertEqual(first["new_edges"], 1)
        self.assertEqual(first["local_parents"], 1)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_posts"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT state FROM targets WHERE post_id='100'"
                ).fetchone()[0],
                "captured",
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT value FROM progress_counters "
                    "WHERE counter_name='archive_posts_total'"
                ).fetchone()[0],
                2,
            )

        with mock.patch.object(
            local_x.archive_x,
            "sha256_file",
            side_effect=AssertionError("unchanged source was hashed"),
        ):
            second = self.ingest(path)
        self.assertEqual(second["status"], "unchanged")
        self.assertEqual(second["bytes_read"], 0)

    def test_stat_change_rehashes_without_reparse_and_content_change_blocks(self):
        path = self.source([metadata("100")])
        self.ingest(path)
        os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1))
        verified = self.ingest(path)
        self.assertEqual(verified["status"], "verified_stat_change")
        self.assertEqual(verified["raw_records"], 0)

        value = path.read_text(encoding="utf-8")
        path.write_text(value.replace("post 100", "xxxx 100"), encoding="utf-8")
        with self.assertRaisesRegex(local_x.LocalStateError, "content changed"):
            self.ingest(path)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT status FROM archive_sources"
                ).fetchone()[0],
                "changed",
            )

    def test_explicit_audit_ignores_stat_cache_and_detects_same_size_change(self):
        path = self.source([metadata("100")])
        self.ingest(path)
        stat = path.stat()
        value = path.read_text(encoding="utf-8")
        path.write_text(value.replace("post 100", "xxxx 100"), encoding="utf-8")
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

        self.assertEqual(self.ingest(path)["status"], "unchanged")
        with self.assertRaisesRegex(local_x.LocalStateError, "changed source"):
            local_x.audit_registered_sources(self.user_dir, self.db_path)

    def test_bad_identity_or_digest_never_exposes_archive_posts(self):
        bad = self.source([metadata("100", requested_user_id="2")], "bad.jsonl")
        with self.assertRaisesRegex(local_x.LocalStateError, "numeric archive scope"):
            self.ingest(bad)
        mismatch = self.source([metadata("101")], "mismatch.jsonl")
        with self.assertRaisesRegex(local_x.LocalStateError, "does not match"):
            self.ingest(mismatch, expected="f" * 64)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_posts"
                ).fetchone()[0],
                0,
            )

    def test_ctrl_c_discards_partial_staging_and_replays_whole_source(self):
        path = self.source([metadata("100"), metadata("101")])
        original = local_x.archive_x.normalize_post
        calls = 0

        def interrupted(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt
            return original(*args, **kwargs)

        with mock.patch.object(
            local_x.archive_x, "normalize_post", side_effect=interrupted
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.ingest(path)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_posts"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT status FROM archive_sources"
                ).fetchone()[0],
                "ingesting",
            )
        replayed = self.ingest(path)
        self.assertEqual(replayed["new_posts"], 2)

    def test_committed_source_rejects_provenance_alias(self):
        path = self.source([metadata("100")])
        self.ingest(path)
        with self.assertRaisesRegex(local_x.LocalStateError, "provenance changed"):
            local_x.ingest_source_once(
                self.user_dir,
                self.db_path,
                requested_handle="alice",
                target_user_id="1",
                spec=local_x.SourceSpec(
                    path=path,
                    source_kind="modern",
                    run_id="different-run",
                    operation_id="different-run:timeline",
                    endpoint="timeline",
                ),
            )

    def test_low_disk_stops_before_source_or_export_publication(self):
        path = self.source([metadata("100")])
        with self.assertRaisesRegex(local_x.LocalStateError, "free space"):
            local_x.ingest_source_once(
                self.user_dir,
                self.db_path,
                requested_handle="alice",
                target_user_id="1",
                spec=self.spec(path),
                disk_free=lambda _path: 0,
            )
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_sources"
                ).fetchone()[0],
                0,
            )

        self.ingest(path)
        with self.assertRaisesRegex(local_x.LocalStateError, "free space"):
            local_x.materialize_exports(
                self.user_dir, self.db_path, disk_free=lambda _path: 0
            )
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM export_batches"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT status FROM export_views WHERE view_name='posts'"
                ).fetchone()[0],
                "dirty",
            )

    def test_generation_export_is_reproducible_and_unchanged_is_zero_payload(self):
        path = self.source([metadata("100")])
        self.ingest(path)
        first = local_x.materialize_exports(self.user_dir, self.db_path)
        self.assertEqual(first["status"], "published")
        self.assertEqual(first["views_written"], len(local_x.EXPORT_VIEW_FILES))
        pointer = archive_x.load_json(
            self.user_dir / "dataset" / "current-export.json", {}
        )
        self.assertEqual(pointer["generation"], first["generation"])
        self.assertEqual(
            list(archive_x.iter_jsonl(self.user_dir / "dataset" / "posts.jsonl"))[0][
                "post_id"
            ],
            "100",
        )

        with mock.patch.object(
            local_x,
            "_write_export_view",
            side_effect=AssertionError("unchanged export rewrote a view"),
        ):
            second = local_x.materialize_exports(self.user_dir, self.db_path)
        self.assertEqual(second["status"], "unchanged")
        self.assertEqual(second["views_written"], 0)
        self.assertEqual(second["payload_bytes_read"], 0)

    def test_export_recovers_crash_after_generation_placement_without_x_work(self):
        path = self.source([metadata("100")])
        self.ingest(path)

        def fault(point: str):
            if point == "after_generation_placement":
                raise RuntimeError("injected placement crash")

        with self.assertRaisesRegex(RuntimeError, "placement crash"):
            local_x.materialize_exports(self.user_dir, self.db_path, fault=fault)
        recovered = local_x.materialize_exports(self.user_dir, self.db_path)
        self.assertEqual(recovered["status"], "unchanged")
        self.assertTrue((self.user_dir / "dataset" / "posts.jsonl").is_file())
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT state FROM export_batches"
                ).fetchone()[0],
                "published",
            )

    def test_export_recovers_every_publication_boundary_and_cleans_temps(self):
        base = self.source([metadata("100")])
        self.ingest(base)
        stages = (
            "after_database_prepare",
            "after_view_placement",
            "after_generation_placement",
            "after_pointer_publication",
            "after_database_finalization",
        )
        for index, stage in enumerate(stages):
            with self.subTest(stage=stage):
                if index:
                    delta = self.source(
                        [metadata(str(200 + index))], f"fault-{index}.jsonl"
                    )
                    local_x.ingest_source_once(
                        self.user_dir,
                        self.db_path,
                        requested_handle="alice",
                        target_user_id="1",
                        spec=local_x.SourceSpec(
                            path=delta,
                            source_kind="modern",
                            run_id="run-one",
                            operation_id=f"run-one:fault-{index}",
                            endpoint="timeline",
                        ),
                    )

                def fault(point: str, expected=stage):
                    if point == expected:
                        raise RuntimeError(f"injected {expected}")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    local_x.materialize_exports(
                        self.user_dir, self.db_path, fault=fault
                    )
                repaired = local_x.materialize_exports(
                    self.user_dir, self.db_path
                )
                self.assertIn(repaired["status"], {"published", "unchanged"})
                exports = self.user_dir / "dataset" / "exports"
                self.assertEqual(
                    list(exports.glob(".g*.tmp-*")) if exports.is_dir() else [],
                    [],
                )
                with context_x.ContextDB(self.db_path, create=False) as database:
                    self.assertFalse(
                        database.connection.execute(
                            "SELECT 1 FROM export_views "
                            "WHERE status<>'current' LIMIT 1"
                        ).fetchone()
                    )

    def test_compatibility_and_pointer_repairs_do_not_rewrite_views(self):
        path = self.source([metadata("100")])
        self.ingest(path)
        with mock.patch.object(
            local_x,
            "_repair_compatibility_links",
            side_effect=OSError("injected compatibility fault"),
        ):
            with self.assertRaisesRegex(OSError, "compatibility fault"):
                local_x.materialize_exports(self.user_dir, self.db_path)
        repaired = local_x.materialize_exports(self.user_dir, self.db_path)
        self.assertEqual(repaired["status"], "unchanged")
        self.assertEqual(
            repaired["compatibility_links"], len(local_x.EXPORT_VIEW_FILES)
        )

        pointer = self.user_dir / "dataset" / "current-export.json"
        pointer.unlink()
        with mock.patch.object(
            local_x,
            "_write_export_view",
            side_effect=AssertionError("pointer repair rewrote payload"),
        ):
            pointer_repair = local_x.materialize_exports(
                self.user_dir, self.db_path
            )
        self.assertEqual(pointer_repair["views_written"], 0)
        self.assertTrue(pointer.is_file())

    def test_manifest_and_source_history_are_one_time(self):
        raw = self.source([metadata("100")])
        archive_x.atomic_write_json(
            self.run_dir / "manifest.json",
            {
                "run_id": "run-one",
                "status": "success",
                "completed_at": "2026-01-02T00:00:00Z",
                "post_dataset": {"dataset_posts": 1},
                "endpoints": [
                    {
                        "endpoint": "timeline",
                        "raw_path": raw.relative_to(self.user_dir).as_posix(),
                        "descriptor_operation_id": "run-one:timeline",
                    }
                ],
            },
        )
        first = local_x.reconcile_source_history(self.user_dir, self.db_path)
        self.assertEqual((first["sources"], first["new_posts"]), (1, 1))
        with mock.patch.object(
            local_x,
            "_manifest_record_and_value",
            side_effect=AssertionError("history was rescanned"),
        ):
            second = local_x.reconcile_source_history(self.user_dir, self.db_path)
        self.assertEqual(second["sources"], 0)
        self.assertEqual(second["bytes_read"], 0)

    def test_indexed_recovery_candidates_replace_history_scan_after_reconciliation(self):
        manifest = self.run_dir / "manifest.json"
        archive_x.atomic_write_json(
            manifest,
            {
                "run_id": "run-one",
                "status": "failed",
                "completed_at": "2026-01-02T00:00:00Z",
            },
        )
        self.assertIsNone(
            local_x.indexed_recovery_manifest_candidates(
                self.user_dir, self.db_path
            )
        )
        local_x.reconcile_manifest_history(self.user_dir, self.db_path)
        self.assertEqual(
            local_x.indexed_recovery_manifest_candidates(
                self.user_dir, self.db_path
            ),
            [manifest.resolve()],
        )
        self.assertTrue(local_x.mark_manifest_processed(self.db_path, "run-one"))
        self.assertEqual(
            local_x.indexed_recovery_manifest_candidates(
                self.user_dir, self.db_path
            ),
            [],
        )

    def test_legacy_state_media_queue_moves_once_to_descriptor_refresh_jobs(self):
        state = archive_x.load_json(self.state_dir / "state.json", {})
        state["pending_media"] = [
            {
                "filename": "2020-01-01T00-00-00_1234567890123456789_1_alice.jpg",
                "post_id": "1234567890123456789",
                "media_number": 1,
                "attempts": 2,
                "next_retry_at": "2026-01-03T00:00:00Z",
            }
        ]
        first = local_x.reconcile_state_media_queue(
            self.user_dir, self.db_path, state
        )
        self.assertEqual(first["jobs"], 1)
        self.assertEqual(state["pending_media"], [])
        self.assertTrue((self.user_dir / first["backup_path"]).is_file())
        with context_x.ContextDB(self.db_path, create=False) as database:
            row = database.connection.execute(
                """SELECT state,compatibility_job,destination_scope,attempts
                     FROM asset_jobs WHERE owner_kind='post'
                       AND owner_id='1234567890123456789'
                       AND media_ordinal=1"""
            ).fetchone()
        self.assertEqual(
            tuple(row), ("needs_refresh", 1, "main", 2)
        )
        second = local_x.reconcile_state_media_queue(
            self.user_dir, self.db_path, state
        )
        self.assertEqual(second["status"], "unchanged")

    def test_historical_context_media_lane_migrates_once_and_drives_live_counters(self):
        captured = metadata("200", author_id="2")
        captured["count"] = 2
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.capture(
                "200",
                captured,
                source_kind="x:focal",
                target_user_id="1",
                max_depth=100,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM asset_jobs"
                ).fetchone()[0],
                0,
            )
        first = local_x.reconcile_context_media_jobs(self.db_path)
        self.assertEqual(first, {"targets": 1, "jobs": 2})
        with context_x.ContextDB(self.db_path, create=False) as database:
            jobs = list(database.connection.execute(
                """SELECT state,compatibility_job,destination_scope
                     FROM asset_jobs ORDER BY media_ordinal"""
            ))
            counters = local_x.counter_snapshot(database.connection)
        self.assertEqual(
            [tuple(row) for row in jobs],
            [("needs_refresh", 1, "context"), ("needs_refresh", 1, "context")],
        )
        self.assertEqual(counters["asset_jobs_total"], 2)
        self.assertEqual(counters["asset_jobs_state_needs_refresh"], 2)
        metrics = progress_x.collect_context_fast_metrics(self.db_path)
        self.assertEqual(metrics["context_media_actionable"], 2)
        self.assertEqual(local_x.reconcile_context_media_jobs(self.db_path), {
            "targets": 0,
            "jobs": 0,
        })

    def test_historical_captured_context_request_with_zero_assets_migrates(self):
        captured = metadata("200", author_id="2")
        self.assertEqual(captured["count"], 0)
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.capture(
                "200",
                captured,
                source_kind="x:focal",
                target_user_id="1",
                max_depth=100,
            )
            database.connection.execute(
                "UPDATE targets SET media_state='captured' WHERE post_id='200'"
            )

        self.assertEqual(
            local_x.reconcile_context_media_jobs(self.db_path),
            {"targets": 1, "jobs": 0},
        )
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM asset_jobs"
                ).fetchone()[0],
                0,
            )
            self.assertIsNotNone(
                database.connection.execute(
                    "SELECT 1 FROM current_pointers "
                    "WHERE pointer_name='context_media_jobs_reconciled'"
                ).fetchone()
            )
        self.assertEqual(
            local_x.reconcile_context_media_jobs(self.db_path),
            {"targets": 0, "jobs": 0},
        )

    def test_checkpoint_defers_tiny_delta_until_forced_without_view_rewrite(self):
        self.ingest(self.source([metadata("100")]))
        initial = local_x.checkpoint_exports(self.user_dir, self.db_path)
        self.assertEqual(initial["status"], "published")
        posts = self.user_dir / "dataset" / "posts.jsonl"
        inode = posts.stat().st_ino
        delta = self.source([metadata("101")], "delta.posts.jsonl")
        local_x.ingest_source_once(
            self.user_dir,
            self.db_path,
            requested_handle="alice",
            target_user_id="1",
            spec=local_x.SourceSpec(
                path=delta,
                source_kind="modern",
                run_id="run-one",
                operation_id="run-one:delta",
                endpoint="timeline",
            ),
        )
        deferred = local_x.checkpoint_exports(
            self.user_dir,
            self.db_path,
            generation_threshold=1_000,
            maximum_dirty_age=86_400,
        )
        self.assertEqual(deferred["status"], "deferred")
        self.assertGreater(
            deferred["durable_generation"], deferred["exported_generation"]
        )
        self.assertEqual(posts.stat().st_ino, inode)
        forced = local_x.checkpoint_exports(
            self.user_dir, self.db_path, force=True
        )
        self.assertEqual(forced["status"], "published")
        self.assertNotEqual(posts.stat().st_ino, inode)

    def test_preexisting_seed_ledger_is_upgraded_into_indexed_post_truth(self):
        raw = self.source([metadata("100")])
        digest = archive_x.sha256_file(raw)
        relative = raw.relative_to(self.user_dir).as_posix()
        archive_x.atomic_write_json(
            self.run_dir / "manifest.json",
            {
                "run_id": "run-one",
                "status": "success",
                "post_dataset": {"dataset_posts": 1},
                "endpoints": [
                    {"endpoint": "timeline", "raw_path": relative}
                ],
            },
        )
        with context_x.ContextDB(self.db_path, create=False) as database, (
            context_x.transaction(database.connection)
        ):
            database.connection.execute(
                """INSERT INTO seed_sources(
                       relative_path,sha256,source_kind,run_id,processed_at,
                       record_count,edge_count
                   ) VALUES (?,?,'modern','run-one','now',1,0)""",
                (relative, digest),
            )
            stat = raw.stat()
            database.connection.execute(
                """INSERT INTO archive_sources(
                       relative_path,source_kind,run_id,operation_id,
                       expected_sha256,stat_device,stat_inode,stat_size,
                       stat_mtime_ns,status,ingest_generation,registered_at,
                       processed_at,record_count,edge_count
                   ) VALUES (?,'modern','run-one','seed_context',?,?,?,?,?,
                             'committed',1,'now','now',1,0)""",
                (
                    relative,
                    digest,
                    stat.st_dev,
                    stat.st_ino,
                    stat.st_size,
                    stat.st_mtime_ns,
                ),
            )

        result = local_x.reconcile_source_history(self.user_dir, self.db_path)
        self.assertEqual(result["new_posts"], 1)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_posts"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM post_provenance"
                ).fetchone()[0],
                1,
            )

    def test_media_compatibility_view_migrates_once_and_explicit_audit_hashes(self):
        media = self.user_dir / "media" / "context" / "fixture.jpg"
        media.parent.mkdir(parents=True)
        media.write_bytes(b"media-bytes")
        digest = archive_x.sha256_file(media)
        sidecar = Path(str(media) + ".json")
        metadata_value = {
            "tweet_id": 100,
            "num": 1,
            "sha256": digest,
            "bytes": media.stat().st_size,
            "type": "photo",
            "date": "2026-01-01 00:00:00",
            "author": {"id": 2, "name": "other"},
            "user": {"id": 1, "name": "alice"},
        }
        archive_x.atomic_write_json(sidecar, metadata_value)
        record = local_x.portable_media_record(
            metadata_value,
            user_dir=self.user_dir,
            requested_handle="alice",
            asset_path=media,
            sidecar_path=sidecar,
        )
        archive_x.atomic_write_jsonl(
            self.user_dir / "dataset" / "media.jsonl", [record]
        )
        local_x.reconcile_source_history(self.user_dir, self.db_path)
        first = local_x.reconcile_media_index(
            self.user_dir, self.db_path, requested_handle="alice"
        )
        self.assertEqual(first["files"], 1)
        with context_x.ContextDB(self.db_path, create=False) as database:
            counters = local_x.counter_snapshot(database.connection)
            self.assertEqual(counters["archive_media_files"], 1)
            self.assertEqual(counters["archive_media_bytes"], len(b"media-bytes"))
            job = database.connection.execute(
                """SELECT asset_id,state,compatibility_job,destination_scope,
                          final_relative_path,final_sha256,final_bytes
                     FROM asset_jobs
                    WHERE owner_kind='post' AND owner_id='100'
                      AND media_ordinal=1"""
            ).fetchone()
            self.assertIsNotNone(job)
            self.assertEqual(job["state"], "captured")
            self.assertEqual(job["compatibility_job"], 1)
            self.assertEqual(job["destination_scope"], "context")
            self.assertEqual(job["final_relative_path"], record["asset_path"])
            self.assertEqual(job["final_sha256"], digest)
            self.assertEqual(job["final_bytes"], len(b"media-bytes"))
            self.assertEqual(
                database.connection.execute(
                    "SELECT asset_id FROM archive_media WHERE media_path=?",
                    (record["asset_path"],),
                ).fetchone()["asset_id"],
                job["asset_id"],
            )
            self.assertEqual(counters["asset_jobs_total"], 1)
            self.assertEqual(counters["asset_jobs_state_captured"], 1)
            self.assertIsNotNone(
                database.connection.execute(
                    "SELECT 1 FROM current_pointers "
                    "WHERE pointer_name='local_history_reconciled'"
                ).fetchone()
            )
        second = local_x.reconcile_media_index(
            self.user_dir, self.db_path, requested_handle="alice"
        )
        self.assertEqual((second["files"], second["bytes_read"]), (0, 0))
        audited = local_x.audit_registered_media(self.user_dir, self.db_path)
        self.assertEqual(audited["files_checked"], 1)
        stat = media.stat()
        media.write_bytes(b"MEDIA-bytes")
        os.utime(media, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        with self.assertRaisesRegex(local_x.LocalStateError, "changed content"):
            local_x.audit_registered_media(self.user_dir, self.db_path)

    def test_large_small_deltas_reduce_ordinary_payload_io_by_over_90_percent(self):
        base = self.source(
            [metadata(str(post_id)) for post_id in range(1, 5_001)],
            "base.jsonl",
        )
        self.ingest(base)
        local_x.materialize_exports(self.user_dir, self.db_path)
        dataset = self.user_dir / "dataset"
        base_posts = (dataset / "posts.jsonl").stat().st_size
        base_authored = (dataset / "authored-posts.jsonl").stat().st_size
        base_reposts = (dataset / "reposts.jsonl").stat().st_size
        baseline_lower_bound = 3 * (
            base_posts + base_posts + base_authored + base_reposts
        )

        ordinary_payload_io = 0
        for offset in range(3):
            delta = self.source(
                [metadata(str(10_000 + offset))],
                f"delta-{offset}.jsonl",
            )
            result = local_x.ingest_source_once(
                self.user_dir,
                self.db_path,
                requested_handle="alice",
                target_user_id="1",
                spec=local_x.SourceSpec(
                    path=delta,
                    source_kind="modern",
                    run_id="run-one",
                    operation_id=f"run-one:delta-{offset}",
                    endpoint="timeline",
                ),
            )
            ordinary_payload_io += int(result["bytes_read"])

        reduction = 1 - ordinary_payload_io / baseline_lower_bound
        self.assertGreaterEqual(reduction, 0.90)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(
                database.connection.execute(
                    "SELECT COUNT(*) FROM archive_posts"
                ).fetchone()[0],
                5_003,
            )
            self.assertEqual(
                database.connection.execute(
                    "SELECT status FROM export_views WHERE view_name='posts'"
                ).fetchone()[0],
                "dirty",
            )

    def test_five_thousand_manifest_history_scan_is_paid_once(self):
        for index in range(5_000):
            run = self.user_dir / "runs" / f"history-{index:05d}"
            run.mkdir(parents=True)
            archive_x.atomic_write_json(
                run / "manifest.json",
                {
                    "run_id": run.name,
                    "status": "failed",
                    "completed_at": "2026-01-01T00:00:00Z",
                },
            )
        first = local_x.reconcile_manifest_history(self.user_dir, self.db_path)
        self.assertEqual(first["manifests_loaded"], 5_000)
        with mock.patch.object(
            local_x,
            "_manifest_record_and_value",
            side_effect=AssertionError("manifest history was loaded twice"),
        ):
            started = time.monotonic()
            second = local_x.reconcile_manifest_history(
                self.user_dir, self.db_path
            )
            elapsed = time.monotonic() - started
        self.assertEqual(second["manifests_loaded"], 0)
        self.assertEqual(second["bytes_read"], 0)
        self.assertLess(elapsed, 0.25)

    def test_hundred_thousand_target_dashboard_uses_only_counters(self):
        with context_x.ContextDB(self.db_path, create=False) as database, (
            context_x.transaction(database.connection)
        ):
            database.connection.executemany(
                """INSERT INTO targets(
                       post_id,state,media_state,discovered_at,updated_at
                   ) VALUES (?,'pending','none','now','now')""",
                ((str(index),) for index in range(1, 100_001)),
            )
        connection = progress_x._open_context(self.db_path)
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        try:
            started = time.monotonic()
            result = progress_x._context_fast_metrics(connection)
            result.update(progress_x._context_closure_metrics(connection))
            elapsed = time.monotonic() - started
        finally:
            connection.close()
        historical_tables = ("targets", "observations", "reply_edges", "conversation_rollups")
        reads = [statement.lower() for statement in statements]
        self.assertFalse(
            any(
                f"from {table}" in statement
                for statement in reads
                for table in historical_tables
            ),
            reads,
        )
        self.assertEqual(result["context_known_remaining"], 100_000)
        self.assertLess(elapsed, 0.05)


if __name__ == "__main__":
    unittest.main()
