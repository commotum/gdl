import contextlib
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from gallery_dl.extractor.common import Extractor, Message
from gallery_dl.job import DownloadJob


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x_context as context_x
import archive_x_descriptors as descriptor_x
import archive_x_request_telemetry as request_telemetry
import archive_x


class FixtureExtractor(Extractor):
    category = "goal5fixture"
    subcategory = "descriptors"
    filename_fmt = "{tweet_id}_{num}.{extension}"
    directory_fmt = ()
    archive_fmt = "{tweet_id}_{num}"

    def __init__(self, records, options):
        super().__init__(re.match(r".*", "fixture://descriptors"))
        self._records = records
        self._options = options

    def config(self, key, default=None):
        return self._options.get(key, default)

    def config2(self, key, key2, default=None, sentinel=None):
        return self._options.get(key, self._options.get(key2, default))

    def config_accumulate(self, key):
        return self._options.get(key)

    def items(self):
        for post, files in self._records:
            yield Message.Directory, "", post.copy()
            for number, file in enumerate(files, 1):
                metadata = {**post, **file, "num": number}
                url = metadata.pop("url")
                yield Message.Url, url, metadata


def record(
    post_id: str,
    ordinal: int,
    *,
    operation: str = "op-1",
    run_id: str = "run-1",
    url: str | None = None,
    relative_path: str | None = None,
    owner_kind: str = "post",
    source_kind: str = "context",
    source_operation: str = "context",
) -> dict:
    url = url or f"https://pbs.twimg.com/media/{post_id}-{ordinal}.jpg?name=orig"
    owner_id = post_id if owner_kind == "post" else "account"
    filename = (
        Path(relative_path).name
        if relative_path
        else f"2026-01-01T00-00-00_{post_id}_{ordinal}_alice.jpg"
    )
    relative_path = relative_path or f"users/alice/media/2026/01/{filename}"
    row = {
        "schema": descriptor_x.SCHEMA,
        "schema_version": descriptor_x.SCHEMA_VERSION,
        "operation_id": operation,
        "run_id": run_id,
        "source_kind": source_kind,
        "source_operation": source_operation,
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "post_id": post_id if owner_kind == "post" else None,
        "media_ordinal": ordinal,
        "media_type": "photo",
        "extension": Path(filename).suffix.lstrip("."),
        "private_url": url,
        "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
        "url_host": "pbs.twimg.com",
        "filename": filename,
        "relative_directory": Path(relative_path).parent.as_posix(),
        "relative_path": relative_path,
        "width": 1200,
        "height": 800,
        "duration_seconds": None,
        "bitrate": None,
        "alt_text": "fixture",
        "variant": {"type": "photo", "width": 1200, "height": 800},
        "captured_at": "2026-01-01T00:00:00Z",
    }
    row["descriptor_sha256"] = hashlib.sha256(
        descriptor_x.canonical_json(descriptor_x.descriptor_payload(row)).encode()
    ).hexdigest()
    return descriptor_x.normalize_record(row)


def non_media_event(
    post_id: str,
    ordinal: int,
    *,
    operation: str = "op-1",
    run_id: str = "run-1",
    source_kind: str = "context",
    source_operation: str = "context",
) -> dict:
    row = {
        "schema": descriptor_x.NON_MEDIA_SCHEMA,
        "schema_version": descriptor_x.NON_MEDIA_SCHEMA_VERSION,
        "operation_id": operation,
        "run_id": run_id,
        "source_kind": source_kind,
        "source_operation": source_operation,
        "owner_kind": "post",
        "owner_id": post_id,
        "post_id": post_id,
        "media_ordinal": ordinal,
        "reason": "external_url",
        "captured_at": "2026-01-01T00:00:00Z",
    }
    row["event_sha256"] = hashlib.sha256(
        descriptor_x.canonical_json(
            descriptor_x.non_media_event_payload(row)
        ).encode()
    ).hexdigest()
    return descriptor_x.normalize_non_media_event(row)


def batch(*rows: dict, operation: str | None = None, digest: str | None = None):
    operation = operation or (rows[0]["operation_id"] if rows else "empty-op")
    digest = digest or hashlib.sha256(
        descriptor_x.canonical_json(rows).encode()
    ).hexdigest()
    return descriptor_x.DescriptorBatch(
        operation_id=operation,
        run_id=rows[0]["run_id"] if rows else "run-1",
        source_kind="context",
        source_operation="context",
        rows=tuple(rows),
        source_sha256=digest,
        ephemeral=True,
    )


def post(post_id: str, *, reply_id: str | None = None, count: int = 0) -> dict:
    return {
        "tweet_id": int(post_id),
        "conversation_id": 100,
        "reply_id": int(reply_id) if reply_id else 0,
        "count": count,
        "date": "2026-01-01 00:00:00",
        "archived_at": "2026-01-02T00:00:00Z",
        "author": {"id": 2, "name": "other"},
        "user": {"id": 1, "name": "alice"},
    }


class DescriptorPrepareCaptureTests(unittest.TestCase):
    def test_prepare_capture_is_private_exact_and_download_free(self):
        descriptor_x.install_postprocessor()
        records = [
            (
                {
                    "tweet_id": "100",
                    "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    "archived_at": "2026-01-02T00:00:00Z",
                    "author": {"name": "alice"},
                },
                [
                    {
                        "url": "https://pbs.twimg.com/media/a.jpg?name=orig",
                        "extension": "jpg",
                        "type": "photo",
                        "width": 1200,
                        "height": 800,
                    },
                    {
                        "url": "https://video.twimg.com/ext_tw_video/a.mp4?tag=1",
                        "extension": "mp4",
                        "type": "video",
                        "width": 1920,
                        "height": 1080,
                        "duration": 8.25,
                        "bitrate": 2_176_000,
                    },
                ],
            ),
            (
                {
                    "tweet_id": "101",
                    "date": datetime(2026, 1, 2, tzinfo=timezone.utc),
                    "archived_at": "2026-01-03T00:00:00Z",
                    "author": {"name": "bob"},
                },
                [
                    {
                        "url": "https://video.twimg.com/tweet_video/g.gif?tag=1",
                        "extension": "mp4",
                        "type": "animated_gif",
                        "width": 480,
                        "height": 270,
                        "duration": 2.0,
                        "bitrate": 0,
                    },
                    {
                        "url": "https://pbs.twimg.com/media/c.webp?name=orig",
                        "extension": "webp",
                        "type": "photo",
                        "width": 900,
                        "height": 900,
                    },
                ],
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "private" / "descriptors.jsonl"
            descriptor_x.prepare_artifact(artifact)
            options = {
                "download": False,
                "metadata-url": "media_url",
                "base-directory": str(root),
                "directory": [
                    "users",
                    "alice",
                    "media",
                    "{date:%Y}",
                    "{date:%m}",
                ],
                "filename": (
                    "{date:%Y-%m-%dT%H-%M-%S}_{tweet_id}_{num}_"
                    "{author[name]}.{extension}"
                ),
                "postprocessors": [
                    descriptor_x.postprocessor_config(
                        artifact_path=artifact,
                        archive_root=root,
                        operation_id="run-1:timeline",
                        run_id="run-1",
                        source_kind="modern",
                        source_operation="modern",
                    )
                ],
            }
            recorder = request_telemetry.RequestRecorder(
                root / "requests.json", "timeline"
            )
            with recorder.capture(), contextlib.redirect_stdout(io.StringIO()):
                status = DownloadJob(FixtureExtractor(records, options)).run()

            self.assertEqual(status, 0)
            self.assertEqual(recorder.value(0)["summary"]["actual_requests"], 0)
            loaded = descriptor_x.load_artifact(
                artifact,
                user_dir=root,
                operation_id="run-1:timeline",
                run_id="run-1",
                source_kind="modern",
                source_operation="modern",
            )
            self.assertEqual(len(loaded.rows), 4)
            self.assertEqual(
                [row["relative_path"] for row in loaded.rows],
                [
                    "users/alice/media/2026/01/2026-01-01T00-00-00_100_1_alice.jpg",
                    "users/alice/media/2026/01/2026-01-01T00-00-00_100_2_alice.mp4",
                    "users/alice/media/2026/01/2026-01-02T00-00-00_101_1_bob.mp4",
                    "users/alice/media/2026/01/2026-01-02T00-00-00_101_2_bob.webp",
                ],
            )
            self.assertEqual(
                (loaded.rows[1]["media_type"], loaded.rows[1]["bitrate"]),
                ("video", 2_176_000),
            )
            self.assertEqual(loaded.rows[2]["media_type"], "animated_gif")
            self.assertFalse(any((root / "users").rglob("*.jpg")))
            self.assertFalse(any((root / "users").rglob("*.mp4")))
            self.assertFalse(any((root / "users").rglob("*.webp")))
            if os.name == "posix":
                self.assertEqual(artifact.stat().st_mode & 0o777, 0o600)

    def test_external_card_is_recorded_as_non_media_without_a_warning(self):
        descriptor_x.install_postprocessor()
        records = [
            (
                {
                    "tweet_id": "100",
                    "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    "archived_at": "2026-01-02T00:00:00Z",
                    "author": {"name": "alice"},
                },
                [{"url": "https://example.com/article", "extension": ""}],
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "private" / "descriptors.jsonl"
            descriptor_x.prepare_artifact(artifact)
            options = {
                "download": False,
                "metadata-url": "media_url",
                "base-directory": str(root),
                "directory": ["users", "alice", "media"],
                "filename": "{tweet_id}_{num}.{extension}",
                "postprocessors": [
                    descriptor_x.postprocessor_config(
                        artifact_path=artifact,
                        archive_root=root,
                        operation_id="run-1:timeline",
                        run_id="run-1",
                        source_kind="modern",
                        source_operation="modern",
                    )
                ],
            }
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = DownloadJob(FixtureExtractor(records, options)).run()

            self.assertEqual(status, 0)
            self.assertNotIn("warning", output.getvalue().lower())
            loaded = descriptor_x.load_artifact(
                artifact,
                user_dir=root,
                operation_id="run-1:timeline",
                run_id="run-1",
                source_kind="modern",
                source_operation="modern",
            )
            self.assertEqual(loaded.rows, ())
            self.assertEqual(len(loaded.non_media_events), 1)
            event = loaded.non_media_events[0]
            self.assertEqual(
                (event["post_id"], event["media_ordinal"], event["reason"]),
                ("100", 1, "non_file_url"),
            )
            self.assertNotIn("example.com", json.dumps(loaded.safe_summary()))

    def test_artifact_validation_is_sanitized_and_keeps_valid_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "artifact.jsonl"
            good = record("100", 1)
            bad = dict(good)
            secret_url = "https://evil.example/private?token=do-not-log"
            bad["private_url"] = secret_url
            path.write_text(
                json.dumps(good) + "\n" + json.dumps(bad) + "\n{" + "\n",
                encoding="utf-8",
            )
            loaded = descriptor_x.load_artifact(
                path,
                user_dir=root,
                operation_id="op-1",
                run_id="run-1",
                source_kind="context",
                source_operation="context",
            )
            self.assertEqual(len(loaded.rows), 1)
            self.assertEqual(len(loaded.errors), 2)
            self.assertNotIn(secret_url, json.dumps(loaded.safe_summary()))
            self.assertNotIn("do-not-log", " ".join(loaded.errors))

    def test_invalid_utf8_and_changed_operation_are_rejected_per_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "artifact.jsonl"
            changed = record("101", 1, operation="other-op")
            path.write_bytes(
                json.dumps(record("100", 1)).encode()
                + b"\n\xff\xfe\n"
                + json.dumps(changed).encode()
                + b"\n"
            )
            loaded = descriptor_x.load_artifact(
                path,
                user_dir=root,
                operation_id="op-1",
                run_id="run-1",
                source_kind="context",
                source_operation="context",
            )
            self.assertEqual([row["post_id"] for row in loaded.rows], ["100"])
            self.assertEqual(len(loaded.errors), 2)
            self.assertNotIn("other-op", " ".join(loaded.errors))


class DescriptorEndpointIntegrationTests(unittest.TestCase):
    def test_modern_artifact_is_hash_bound_selected_and_committed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_dir = root / "users" / "alice"
            artifact = (
                user_dir
                / "runs"
                / "run-1"
                / "raw"
                / "timeline.descriptors.jsonl"
            )
            artifact.parent.mkdir(parents=True)
            rows = (
                record(
                    "100",
                    1,
                    operation="run-1:timeline",
                    run_id="run-1",
                    source_kind="modern",
                    source_operation="modern",
                ),
                record(
                    "999",
                    1,
                    operation="run-1:timeline",
                    run_id="run-1",
                    source_kind="modern",
                    source_operation="modern",
                ),
            )
            artifact.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            os.chmod(artifact, 0o600)
            endpoint = {
                "descriptor_artifact_path": str(artifact.relative_to(user_dir)),
                "descriptor_artifact_sha256": descriptor_x.sha256_file(artifact),
                "descriptor_operation_id": "run-1:timeline",
                "descriptor_source_kind": "modern",
                "descriptor_source_operation": "modern",
            }
            summary = archive_x.persist_descriptor_evidence(
                user_dir,
                target_user_id="1",
                canonical_handle="alice",
                accepted_records=(post("100", count=1),),
                endpoint_results=(endpoint,),
            )

            self.assertEqual(summary["rows_accepted"], 1)
            self.assertEqual(summary["rows_rejected"], 1)
            with context_x.ContextDB(
                user_dir / "_state" / "context.sqlite3", create=False
            ) as database:
                self.assertEqual(
                    database.connection.execute(
                        "SELECT COUNT(*) FROM descriptor_generations"
                    ).fetchone()[0],
                    1,
                )
                source = database.connection.execute(
                    "SELECT source_kind,operation_id FROM archive_sources"
                ).fetchone()
                self.assertEqual(tuple(source), ("modern", "run-1:timeline"))

            changed = dict(endpoint)
            changed["descriptor_artifact_sha256"] = "0" * 64
            degraded = archive_x.persist_descriptor_evidence(
                user_dir,
                target_user_id="1",
                canonical_handle="alice",
                accepted_records=(post("100", count=1),),
                endpoint_results=(changed,),
            )
            self.assertEqual(degraded["status"], "degraded")
            self.assertEqual(degraded["artifact_load_errors"], 1)


class DescriptorPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "context.sqlite3"
        self.database = context_x.ContextDB(self.path)
        self.database.bind_identity("1", "alice")

    def tearDown(self):
        self.database.close()
        self.directory.cleanup()

    def test_conversation_acceptance_filters_nearby_and_queues_missing_ordinal(self):
        self.database.add_edge(
            "200",
            "100",
            conversation_id="100",
            depth=0,
            run_id="seed",
            observed_at="now",
            max_depth=10,
        )
        rows = (
            record("100", 1),
            record("101", 1),
            record("999", 1),
        )
        evidence = batch(*rows)
        selected, _continuation = self.database.capture_conversation_response(
            "100",
            (
                post("100", reply_id="101", count=2),
                post("101", count=1),
                post("999", count=1),
            ),
            target_user_id="1",
            max_depth=10,
            descriptor_batches=(evidence,),
        )
        self.assertEqual(selected, ["100", "101"])
        owners = {
            tuple(row)
            for row in self.database.connection.execute(
                """SELECT owner_id,media_ordinal,state
                     FROM asset_jobs ORDER BY owner_id,media_ordinal"""
            )
        }
        self.assertEqual(
            owners,
            {
                ("100", 1, "pending"),
                ("100", 2, "needs_refresh"),
                ("101", 1, "pending"),
            },
        )
        self.assertIsNone(
            self.database.connection.execute(
                "SELECT 1 FROM descriptor_generations WHERE owner_id='999'"
            ).fetchone()
        )
        self.assertEqual(evidence.persistence["rows_rejected"], 1)
        self.assertEqual(evidence.persistence["needs_refresh_created"], 1)

    def test_external_card_marker_covers_and_repairs_false_missing_media(self):
        accepted = (post("100", count=1),)
        missing = batch(operation="missing-op", digest="a" * 64)
        self.database.persist_descriptor_batches((missing,), accepted)
        self.database.prepare_descriptor_refreshes()
        marker = descriptor_x.DescriptorBatch(
            operation_id="marker-op",
            run_id="run-1",
            source_kind="context",
            source_operation="context",
            rows=(),
            non_media_events=(
                non_media_event("100", 1, operation="marker-op"),
            ),
            source_sha256="b" * 64,
            ephemeral=True,
        )

        summary = self.database.persist_descriptor_batches((marker,), accepted)

        self.assertEqual(summary["non_media_events_accepted"], 1)
        self.assertEqual(summary["non_media_jobs_cleared"], 1)
        self.assertEqual(summary["needs_refresh_created"], 0)
        self.assertIsNone(
            self.database.connection.execute(
                "SELECT 1 FROM asset_jobs WHERE owner_id='100'"
            ).fetchone()
        )
        refresh = self.database.connection.execute(
            """SELECT state,last_error_class FROM descriptor_refresh_jobs
                 WHERE owner_id='100'"""
        ).fetchone()
        self.assertEqual(tuple(refresh), ("complete", "external_non_media"))

    def test_descriptor_failure_never_rolls_back_metadata(self):
        self.database.upsert_target(
            "100", conversation_id="100", depth=0, observed_at="now"
        )
        evidence = batch(record("100", 1))
        with mock.patch.object(
            self.database,
            "_persist_descriptor_batches",
            side_effect=RuntimeError("injected descriptor failure"),
        ):
            self.database.capture(
                "100",
                post("100", count=1),
                source_kind="x:focal",
                target_user_id="1",
                max_depth=10,
                descriptor_batches=(evidence,),
            )
        state = self.database.connection.execute(
            "SELECT state,media_state FROM targets WHERE post_id='100'"
        ).fetchone()
        self.assertEqual(tuple(state), ("captured", "pending"))
        job = self.database.connection.execute(
            "SELECT state FROM asset_jobs WHERE owner_id='100'"
        ).fetchone()
        self.assertEqual(job[0], "needs_refresh")
        self.assertEqual(evidence.persistence["status"], "degraded")
        self.assertNotIn("injected", json.dumps(evidence.persistence))

    def test_replay_deduplicates_and_new_url_creates_one_active_generation(self):
        accepted = (post("100", count=1),)
        first_row = record("100", 1, operation="op-1")
        first = batch(first_row, operation="op-1", digest="a" * 64)
        first_replay = batch(first_row, operation="op-1", digest="a" * 64)
        same_row = record("100", 1, operation="op-2")
        same = batch(same_row, operation="op-2", digest="b" * 64)
        changed_row = record(
            "100",
            1,
            operation="op-3",
            url="https://pbs.twimg.com/media/100-1.jpg?name=large",
        )
        changed = batch(changed_row, operation="op-3", digest="c" * 64)

        self.database.persist_descriptor_batches((first,), accepted)
        self.database.persist_descriptor_batches((first_replay,), accepted)
        self.database.persist_descriptor_batches((same,), accepted)
        self.database.persist_descriptor_batches((changed,), accepted)

        active_id = self.database.connection.execute(
            """SELECT descriptor_id FROM descriptor_generations
                 WHERE owner_id='100' AND state='active'"""
        ).fetchone()[0]
        self.database.connection.execute(
            """UPDATE asset_jobs SET state='captured',
                   final_relative_path=expected_relative_path,
                   final_sha256=?,final_bytes=4,completed_at='now'
                 WHERE owner_id='100'""",
            ("f" * 64,),
        )
        newest_row = record(
            "100",
            1,
            operation="op-4",
            url="https://pbs.twimg.com/media/100-1.jpg?name=4096x4096",
        )
        newest = batch(newest_row, operation="op-4", digest="d" * 64)
        self.database.persist_descriptor_batches((newest,), accepted)

        generations = list(
            self.database.connection.execute(
                """SELECT generation,state,url_sha256
                     FROM descriptor_generations ORDER BY generation"""
            )
        )
        self.assertEqual(
            [(row[0], row[1]) for row in generations],
            [(1, "superseded"), (2, "superseded"), (3, "active")],
        )
        observations = self.database.connection.execute(
            "SELECT COUNT(*) FROM descriptor_observations"
        ).fetchone()[0]
        self.assertEqual(observations, 4)
        captured = self.database.connection.execute(
            "SELECT state,descriptor_id FROM asset_jobs WHERE owner_id='100'"
        ).fetchone()
        self.assertEqual(captured[0], "captured")
        self.assertNotEqual(captured[1], active_id)
        self.assertNotIn(
            "https://pbs.twimg.com",
            json.dumps(changed.safe_summary()),
        )

    def test_two_confirmed_walks_choose_newest_url_after_post_agreement(self):
        accepted = (post("100", count=1),)
        first = batch(
            record(
                "100",
                1,
                operation="walk-1",
                url="https://pbs.twimg.com/media/100.jpg?name=small",
            ),
            operation="walk-1",
            digest="1" * 64,
        )
        second = batch(
            record(
                "100",
                1,
                operation="walk-2",
                url="https://pbs.twimg.com/media/100.jpg?name=orig",
            ),
            record("999", 1, operation="walk-2"),
            operation="walk-2",
            digest="2" * 64,
        )
        summary = self.database.persist_descriptor_batches(
            (first, second), accepted
        )
        active = self.database.connection.execute(
            """SELECT private_url,generation FROM descriptor_generations
                 WHERE owner_id='100' AND state='active'"""
        ).fetchone()
        self.assertTrue(active[0].endswith("name=orig"))
        self.assertEqual(active[1], 2)
        self.assertEqual(summary["rows_rejected"], 1)
        self.assertIsNone(
            self.database.connection.execute(
                "SELECT 1 FROM descriptor_generations WHERE owner_id='999'"
            ).fetchone()
        )

    def test_profile_info_produces_historical_exact_jobs_without_tweet_lookup(self):
        when = datetime(2024, 12, 28, 14, 28, 3, tzinfo=timezone.utc)
        avatar_id = int(
            ((when.timestamp() * 1000 - descriptor_x.SNOWFLAKE_EPOCH_MS))
        ) << 22
        info = {
            "id": 1,
            "name": "alice",
            "profile_image": (
                f"https://pbs.twimg.com/profile_images/{avatar_id}/pic_normal.jpg"
            ),
            "profile_banner": (
                "https://pbs.twimg.com/profile_banners/1/1723774748"
            ),
        }
        first = descriptor_x.profile_batch_from_info(
            info,
            user_dir=Path("/archive/users/alice"),
            operation_id="run-a:info-profile",
            run_id="run-a",
            captured_at="2026-01-01T00:00:00Z",
            source_relative_path="runs/run-a/raw/info.posts.jsonl",
            source_sha256="a" * 64,
        )
        summary = self.database.persist_descriptor_batches(
            (first,), (), allow_profile=True
        )
        paths = {
            row[0]
            for row in self.database.connection.execute(
                "SELECT expected_relative_path FROM asset_jobs"
            )
        }
        self.assertEqual(summary["jobs_created"], 2)
        self.assertIn(
            "users/alice/media/profile/profile-avatar_2024-12-28T14-28-03_alice.jpg",
            paths,
        )
        self.assertIn(
            "users/alice/media/profile/profile-background_2024-08-16T02-19-08_alice.jpg",
            paths,
        )

        changed_info = dict(info)
        changed_info["profile_banner"] = (
            "https://pbs.twimg.com/profile_banners/1/1767225600"
        )
        changed = descriptor_x.profile_batch_from_info(
            changed_info,
            user_dir=Path("/archive/users/alice"),
            operation_id="run-b:info-profile",
            run_id="run-b",
            captured_at="2026-01-02T00:00:00Z",
            source_relative_path="runs/run-b/raw/info.posts.jsonl",
            source_sha256="b" * 64,
        )
        self.database.persist_descriptor_batches(
            (changed,), (), allow_profile=True
        )
        banners = list(
            self.database.connection.execute(
                """SELECT generation,state FROM descriptor_generations
                     WHERE owner_kind='profile_background'
                     ORDER BY generation"""
            )
        )
        self.assertEqual(
            [tuple(row) for row in banners],
            [(1, "superseded"), (2, "active")],
        )
        banner_job = self.database.connection.execute(
            """SELECT state,expected_relative_path FROM asset_jobs
                 WHERE owner_kind='profile_background'"""
        ).fetchone()
        self.assertEqual(banner_job[0], "pending")
        self.assertIn("2026-01-01T00-00-00", banner_job[1])

    def test_context_worker_commits_descriptor_on_the_single_metadata_fetch(self):
        self.database.upsert_target(
            "100", conversation_id="100", depth=0, observed_at="now"
        )
        self.database.close()
        evidence = batch(record("100", 1))

        def fetcher(**_kwargs):
            metadata = post("100", count=1)
            return context_x.FetchResult(
                status=0,
                metadata=metadata,
                log="",
                interrupted=False,
                failed_downloads=[],
                rate_reset=None,
                records=(metadata,),
                descriptor_batches=(evidence,),
                request_telemetry={
                    "actual_requests": 1,
                    "by_category": {"x_api": 1},
                },
            )

        user_dir = Path(self.directory.name) / "users" / "alice"
        state_dir = user_dir / "_state"
        state_dir.mkdir(parents=True)
        (state_dir / "state.json").write_text(
            json.dumps(
                {
                    "requested_user_id": "1",
                    "requested_handle": "alice",
                    "canonical_handle": "alice",
                }
            ),
            encoding="utf-8",
        )
        counts = context_x.run_worker(
            repo_dir=REPO,
            archive_root=Path(self.directory.name),
            user_dir=user_dir,
            db_path=self.path,
            handle="alice",
            cookie_file=Path("/unused"),
            max_posts=1,
            request_delay="0",
            retry_delay=0,
            max_attempts=3,
            lease_seconds=60,
            fairness_quantum=5,
            max_depth=10,
            media=False,
            fetcher=fetcher,
        )
        self.database = context_x.ContextDB(self.path, create=False)
        self.assertEqual(counts["requests"], 1)
        self.assertEqual(counts["actual_requests"], 1)
        self.assertEqual(counts["descriptor_rows"], 1)
        self.assertEqual(
            self.database.connection.execute(
                "SELECT COUNT(*) FROM descriptor_generations"
            ).fetchone()[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
