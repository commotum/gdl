import hashlib
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

import archive_x
import archive_x_context as context_x
import archive_x_descriptors as descriptor_x
import archive_x_refresh as refresh_x


def metadata(post_id: str, count: int) -> dict:
    return {
        "tweet_id": int(post_id),
        "conversation_id": int(post_id),
        "reply_id": 0,
        "retweet_id": 0,
        "count": count,
        "date": "2026-01-01 00:00:00",
        "archived_at": "2026-01-02T00:00:00Z",
        "author": {"id": 2, "name": "other"},
        "user": {"id": 1, "name": "alice"},
    }


def descriptor(post_id: str, ordinal: int, *, scope: str = "context") -> dict:
    url = f"https://pbs.twimg.com/media/{post_id}-{ordinal}.jpg?name=orig"
    filename = f"2026-01-01T00-00-00_{post_id}_{ordinal}_other.jpg"
    directory = Path("users/alice/media")
    if scope == "context":
        directory /= "context"
    directory /= Path("2026/01")
    row = {
        "schema": descriptor_x.SCHEMA,
        "schema_version": descriptor_x.SCHEMA_VERSION,
        "operation_id": f"refresh-{post_id}",
        "run_id": f"refresh-{post_id}",
        "source_kind": "exact_refresh",
        "source_operation": "exact_refresh",
        "owner_kind": "post",
        "owner_id": post_id,
        "post_id": post_id,
        "media_ordinal": ordinal,
        "media_type": "photo",
        "extension": "jpg",
        "private_url": url,
        "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
        "url_host": "pbs.twimg.com",
        "filename": filename,
        "relative_directory": directory.as_posix(),
        "relative_path": (directory / filename).as_posix(),
        "width": 1200,
        "height": 800,
        "duration_seconds": None,
        "bitrate": None,
        "alt_text": "fixture",
        "variant": {"type": "photo"},
        "posted_at": "2026-01-01T00:00:00Z",
        "original_posted_at": None,
        "author_id": "2",
        "author_handle": "other",
        "conversation_id": post_id,
        "reply_id": None,
        "retweet_id": None,
        "captured_at": "2026-01-02T00:00:00Z",
    }
    row["descriptor_sha256"] = hashlib.sha256(
        descriptor_x.canonical_json(descriptor_x.descriptor_payload(row)).encode()
    ).hexdigest()
    return descriptor_x.normalize_record(row)


def non_media_event(post_id: str, ordinal: int = 1) -> dict:
    row = {
        "schema": descriptor_x.NON_MEDIA_SCHEMA,
        "schema_version": descriptor_x.NON_MEDIA_SCHEMA_VERSION,
        "operation_id": f"refresh-{post_id}",
        "run_id": f"refresh-{post_id}",
        "source_kind": "exact_refresh",
        "source_operation": "exact_refresh",
        "owner_kind": "post",
        "owner_id": post_id,
        "post_id": post_id,
        "media_ordinal": ordinal,
        "reason": "external_url",
        "captured_at": "2026-01-02T00:00:00Z",
    }
    row["event_sha256"] = hashlib.sha256(
        descriptor_x.canonical_json(
            descriptor_x.non_media_event_payload(row)
        ).encode()
    ).hexdigest()
    return descriptor_x.normalize_non_media_event(row)


def batch(
    post_id: str,
    rows=(),
    *,
    non_media_events=(),
    source_operation="exact_refresh",
):
    return descriptor_x.DescriptorBatch(
        operation_id=f"refresh-{post_id}",
        run_id=f"refresh-{post_id}",
        source_kind=(
            "exact_refresh" if source_operation == "exact_refresh" else source_operation
        ),
        source_operation=source_operation,
        rows=tuple(rows),
        non_media_events=tuple(non_media_events),
        source_sha256=hashlib.sha256(
            descriptor_x.canonical_json(tuple(rows)).encode()
        ).hexdigest(),
        ephemeral=True,
    )


def result(
    post_id: str,
    *,
    count: int | None = 1,
    rows=(),
    non_media_events=(),
    log: str = "",
    status: int = 0,
    actual_requests: int = 1,
    interrupted: bool = False,
) -> context_x.FetchResult:
    return context_x.FetchResult(
        status=status,
        metadata=metadata(post_id, count) if count is not None else None,
        log=log,
        interrupted=interrupted,
        failed_downloads=[],
        rate_reset=None,
        records=(),
        descriptor_batches=(
            batch(post_id, rows, non_media_events=non_media_events),
        ),
        request_telemetry={
            "actual_requests": actual_requests,
            "by_category": {"x_api": actual_requests},
        },
    )


class RefreshFallbackTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.user_dir = self.root / "users" / "alice"
        state_dir = self.user_dir / "_state"
        state_dir.mkdir(parents=True)
        archive_x.atomic_write_json(
            state_dir / "state.json",
            {
                "requested_user_id": "1",
                "requested_handle": "alice",
                "canonical_handle": "alice",
            },
        )
        self.db_path = state_dir / "context.sqlite3"
        with context_x.ContextDB(self.db_path) as database:
            database.bind_identity("1", "alice")

    def tearDown(self):
        self.temporary.cleanup()

    def add_missing(self, post_id="100", count=1, *, scope="context"):
        source_operation = "context" if scope == "context" else "modern"
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.persist_descriptor_batches(
                (batch(post_id, (), source_operation=source_operation),),
                (metadata(post_id, count),),
            )

    def add_usable(self, post_id="100", count=1, *, scope="context"):
        rows = tuple(descriptor(post_id, ordinal, scope=scope) for ordinal in range(1, count + 1))
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.persist_descriptor_batches(
                (batch(post_id, rows),), (metadata(post_id, count),)
            )

    def jobs(self, post_id="100"):
        with context_x.ContextDB(self.db_path, create=False) as database:
            return [
                dict(row)
                for row in database.connection.execute(
                    "SELECT * FROM asset_jobs WHERE owner_id=? ORDER BY media_ordinal",
                    (post_id,),
                )
            ]

    def refreshes(self, post_id="100"):
        with context_x.ContextDB(self.db_path, create=False) as database:
            return [
                dict(row)
                for row in database.connection.execute(
                    """SELECT * FROM descriptor_refresh_jobs
                         WHERE owner_id=? ORDER BY generation""",
                    (post_id,),
                )
            ]

    def run_worker(self, **overrides):
        options = {
            "repo_dir": REPO,
            "archive_root": self.root,
            "user_dir": self.user_dir,
            "db_path": self.db_path,
            "handle": "alice",
            "cookie_file": self.root / "cookies.txt",
            "request_delay": "0",
            "retry_delay": 0,
            "clock": lambda: 0,
            "sleep": lambda _seconds: None,
        }
        options.update(overrides)
        return refresh_x.run_descriptor_refresh_worker(**options)

    def test_usable_descriptor_never_enters_refresh(self):
        self.add_usable()
        calls = []

        def fetcher(**kwargs):
            calls.append(kwargs)
            raise AssertionError("usable descriptor reached fallback")

        observed = self.run_worker(fetcher=fetcher)

        self.assertEqual(observed["attempted"], 0)
        self.assertEqual(observed["refreshes_created"], 0)
        self.assertEqual(calls, [])
        self.assertEqual(self.refreshes(), [])

    def test_operator_commands_are_explicit_and_normal_run_needs_no_flag(self):
        parser = context_x.build_parser(REPO)
        repair = parser.parse_args(
            ["--user", "alice", "repair-descriptor", "100"]
        )
        clear = parser.parse_args(
            ["--user", "alice", "auth-stop", "--clear"]
        )
        ordinary = parser.parse_args(["--user", "alice", "run"])
        self.assertEqual(
            (repair.command, repair.post_id), ("repair-descriptor", "100")
        )
        self.assertTrue(clear.clear)
        self.assertEqual(ordinary.command, "run")

    def test_three_missing_assets_use_one_exact_refresh_and_keep_main_scope(self):
        self.add_missing(count=3, scope="main")
        calls = []

        def fetcher(**kwargs):
            calls.append(kwargs)
            self.assertEqual(kwargs["destination_scope"], "main")
            self.assertEqual(kwargs["request_operation"], "descriptor_refresh")
            self.assertEqual(kwargs["descriptor_source_kind"], "exact_refresh")
            rows = tuple(descriptor("100", ordinal, scope="main") for ordinal in range(1, 4))
            return result("100", count=3, rows=rows, actual_requests=2)

        observed = self.run_worker(fetcher=fetcher)

        self.assertEqual(len(calls), 1)
        self.assertEqual(observed["logical_requests"], 1)
        self.assertEqual(observed["actual_requests"], 2)
        self.assertEqual(observed["descriptor_rows"], 3)
        self.assertEqual([job["state"] for job in self.jobs()], ["pending"] * 3)
        self.assertTrue(
            all("/media/context/" not in job["expected_relative_path"] for job in self.jobs())
        )
        self.assertEqual(self.refreshes()[0]["state"], "complete")

    def test_external_card_refresh_completes_without_a_media_job(self):
        self.add_missing()

        observed = self.run_worker(
            fetcher=lambda **_kwargs: result(
                "100", non_media_events=(non_media_event("100"),)
            )
        )

        self.assertEqual(observed["complete"], 1)
        self.assertEqual(self.jobs(), [])
        refresh = self.refreshes()[0]
        self.assertEqual(refresh["state"], "complete")
        self.assertEqual(refresh["last_error_class"], "external_non_media")

    def test_refresh_uses_claim_token_and_persistent_actual_request_lane(self):
        self.add_missing()
        runner = object()
        calls = []

        def fetcher(**kwargs):
            calls.append(kwargs)
            return result("100", rows=(descriptor("100", 1),))

        with mock.patch.object(
            context_x,
            "reserve_request",
            side_effect=AssertionError("stacked logical pacing ran"),
        ):
            observed = self.run_worker(fetcher=fetcher, runner=runner)

        self.assertEqual(observed["complete"], 1)
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0]["runner"], runner)
        self.assertRegex(calls[0]["control_lease_token"], r"^[0-9a-f]{32}$")
        self.assertEqual(calls[0]["request_delay"], "0")

    def test_verified_compatibility_file_is_captured_without_request(self):
        relative = "users/alice/media/context/2026/01/date_100_1_other.jpg"
        asset = self.root / relative
        asset.parent.mkdir(parents=True)
        asset.write_bytes(b"existing")
        archive_x.atomic_write_json(
            Path(str(asset) + ".json"),
            {
                "tweet_id": 100,
                "num": "1",
                "sha256": archive_x.sha256_file(asset),
            },
        )
        with context_x.ContextDB(self.db_path, create=False) as database:
            stale_relative = (
                "users/alice/media/context/2026/01/"
                "date_100_1_old_handle.jpg"
            )
            database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,state,compatibility_job,
                       destination_scope,expected_relative_path,created_at,updated_at
                   ) VALUES ('post','100',1,'needs_refresh',1,'context',?,'now','now')""",
                (stale_relative,),
            )
        calls = []

        observed = self.run_worker(fetcher=lambda **kwargs: calls.append(kwargs))

        self.assertEqual(observed["local_existing"], 1)
        self.assertEqual(observed["attempted"], 0)
        self.assertEqual(calls, [])
        self.assertEqual(self.jobs()[0]["state"], "captured")
        self.assertEqual(self.jobs()[0]["final_relative_path"], relative)
        self.assertEqual(self.refreshes(), [])
        ledger = sqlite3.connect(
            self.user_dir / "_state" / "context-downloads.sqlite3"
        )
        try:
            self.assertEqual(
                ledger.execute("SELECT entry FROM media").fetchone()[0],
                "100_0_1",
            )
        finally:
            ledger.close()

    def test_refreshed_descriptor_rejection_never_creates_generation_two(self):
        self.add_usable()
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.connection.execute(
                """UPDATE asset_jobs SET state='needs_refresh',
                       last_error_class='descriptor_rejected',
                       last_error_detail='descriptor_rejected:status=403'"""
            )
        fresh = descriptor("100", 1)
        fresh["private_url"] += "&generation=2"
        fresh["url_sha256"] = hashlib.sha256(fresh["private_url"].encode()).hexdigest()
        fresh["descriptor_sha256"] = hashlib.sha256(
            descriptor_x.canonical_json(descriptor_x.descriptor_payload(fresh)).encode()
        ).hexdigest()
        fresh = descriptor_x.normalize_record(fresh)
        observed = self.run_worker(fetcher=lambda **_kwargs: result("100", rows=(fresh,)))
        self.assertEqual(observed["complete"], 1)
        self.assertEqual(self.jobs()[0]["state"], "pending")

        with context_x.ContextDB(self.db_path, create=False) as database:
            database.connection.execute(
                """UPDATE asset_jobs SET state='needs_refresh',
                       last_error_class='descriptor_rejected',
                       last_error_detail='descriptor_rejected:status=404'"""
            )
        calls = []
        observed = self.run_worker(fetcher=lambda **kwargs: calls.append(kwargs))

        self.assertEqual(observed["attempted"], 0)
        self.assertEqual(calls, [])
        self.assertEqual(self.jobs()[0]["state"], "unavailable")
        self.assertEqual(len(self.refreshes()), 1)

    def test_terminal_post_outcomes_remain_distinct_and_close_assets(self):
        cases = {
            "200": ("Tweet unavailable ('Deleted')", "deleted"),
            "201": ("AuthRequired: Protected Tweet", "private"),
            "202": ("Tweet unavailable ('Suspended')", "suspended"),
            "203": ("withheld in your country", "withheld"),
        }
        for post_id, (log, expected) in cases.items():
            self.add_missing(post_id)
            observed = self.run_worker(
                max_posts=1,
                fetcher=lambda post_id=post_id, log=log, **_kwargs: result(
                    post_id, count=None, rows=(), log=log, status=1
                ),
            )
            self.assertEqual(observed["unavailable"], 1)
            self.assertEqual(self.jobs(post_id)[0]["state"], "unavailable")
            self.assertEqual(self.refreshes(post_id)[0]["last_error_class"], expected)

    def test_transient_budget_is_one_generation_and_three_attempts(self):
        self.add_missing()
        calls = []

        def transient(**_kwargs):
            calls.append(1)
            return result(
                "100",
                count=None,
                rows=(),
                log="Dependency: Unspecified",
                status=1,
            )

        self.assertEqual(
            self.run_worker(fetcher=transient, max_posts=1)["retryable"], 1
        )
        self.assertEqual(
            self.run_worker(fetcher=transient, max_posts=1)["retryable"], 1
        )
        self.assertEqual(
            self.run_worker(fetcher=transient, max_posts=1)["manual_review"], 1
        )
        fourth = self.run_worker(fetcher=transient)

        self.assertEqual(fourth["attempted"], 0)
        self.assertEqual(len(calls), 3)
        refresh = self.refreshes()[0]
        self.assertEqual((refresh["generation"], refresh["attempts"]), (1, 3))
        self.assertEqual(refresh["state"], "manual_review")
        self.assertEqual(self.jobs()[0]["state"], "manual_review")

    def test_auth_stop_is_durable_shared_and_explicitly_cleared(self):
        self.add_missing()
        calls = []

        def authentication(**_kwargs):
            calls.append(1)
            return result(
                "100",
                count=None,
                rows=(),
                log="Could not authenticate you https://example.com/SECRET_SENTINEL",
                status=1,
            )

        with self.assertRaises(context_x.ContextAuthenticationError):
            self.run_worker(fetcher=authentication)
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertEqual(database.authentication_stop()["error_class"], "authentication")
        with self.assertRaises(context_x.ContextAuthenticationError):
            self.run_worker(fetcher=authentication)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.refreshes()[0]["attempts"], 0)
        self.assertNotIn(b"SECRET_SENTINEL", self.db_path.read_bytes())
        with context_x.ContextDB(self.db_path, create=False) as database:
            self.assertTrue(database.clear_authentication_stop())
            self.assertIsNone(database.authentication_stop())

    def test_auth_stop_does_not_block_zero_request_local_reconciliation(self):
        relative = "users/alice/media/context/2026/01/date_100_1_other.jpg"
        asset = self.root / relative
        asset.parent.mkdir(parents=True)
        asset.write_bytes(b"existing")
        archive_x.atomic_write_json(
            Path(str(asset) + ".json"),
            {
                "tweet_id": 100,
                "num": 1,
                "sha256": archive_x.sha256_file(asset),
            },
        )
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,state,compatibility_job,
                       destination_scope,expected_relative_path,created_at,updated_at
                   ) VALUES ('post','100',1,'needs_refresh',1,'context',?,'now','now')""",
                (relative,),
            )
            database.record_authentication_stop("authentication", now=1)
        calls = []
        observed = self.run_worker(fetcher=lambda **kwargs: calls.append(kwargs))
        self.assertEqual(observed["local_existing"], 1)
        self.assertEqual(calls, [])
        self.assertEqual(self.jobs()[0]["state"], "captured")

    def test_interrupt_releases_attempt_and_same_generation_resumes(self):
        self.add_missing()
        with self.assertRaises(KeyboardInterrupt):
            self.run_worker(fetcher=lambda **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))
        refresh = self.refreshes()[0]
        self.assertEqual((refresh["state"], refresh["attempts"]), ("retryable", 0))

        observed = self.run_worker(
            fetcher=lambda **_kwargs: result("100", rows=(descriptor("100", 1),))
        )
        self.assertEqual(observed["complete"], 1)
        self.assertEqual(len(self.refreshes()), 1)

    def test_stale_lease_is_reclaimed_without_new_generation(self):
        self.add_missing()
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.prepare_descriptor_refreshes()
            claimed = database.claim_descriptor_refresh(now=0, lease_seconds=10)
            self.assertEqual(claimed["attempts"], 1)

        observed = self.run_worker(
            clock=lambda: 100,
            lease_seconds=10,
            fetcher=lambda **_kwargs: result("100", rows=(descriptor("100", 1),)),
        )

        self.assertEqual(observed["complete"], 1)
        self.assertEqual(len(self.refreshes()), 1)
        self.assertEqual(self.refreshes()[0]["attempts"], 2)

    def test_operator_repair_is_the_only_generation_two_path(self):
        self.add_missing()
        self.run_worker(
            max_attempts=1,
            fetcher=lambda **_kwargs: result(
                "100", count=None, log="Dependency: Unspecified", status=1
            ),
        )
        self.assertEqual(self.refreshes()[0]["state"], "manual_review")
        with context_x.ContextDB(self.db_path, create=False) as database:
            refresh_id = database.enqueue_operator_refresh("100")
            self.assertGreater(refresh_id, 0)

        observed = self.run_worker(
            fetcher=lambda **_kwargs: result("100", rows=(descriptor("100", 1),))
        )

        self.assertEqual(observed["complete"], 1)
        refreshes = self.refreshes()
        self.assertEqual([row["generation"] for row in refreshes], [1, 2])
        self.assertEqual(refreshes[1]["reason"], "operator_repair")

    def test_success_with_missing_ordinal_is_manual_not_a_refresh_loop(self):
        self.add_missing(count=3)
        rows = (descriptor("100", 1), descriptor("100", 2))
        observed = self.run_worker(
            fetcher=lambda **_kwargs: result("100", count=3, rows=rows)
        )

        self.assertEqual(observed["manual_review"], 1)
        self.assertEqual([job["state"] for job in self.jobs()], [
            "pending",
            "pending",
            "manual_review",
        ])
        second = self.run_worker(fetcher=lambda **_kwargs: self.fail("no request"))
        self.assertEqual(second["attempted"], 0)
        self.assertEqual(len(self.refreshes()), 1)

    def test_invalid_refreshed_media_count_is_manual_review(self):
        self.add_missing()
        invalid = result("100", rows=(descriptor("100", 1),))
        invalid.metadata["count"] = "one"
        observed = self.run_worker(fetcher=lambda **_kwargs: invalid)
        self.assertEqual(observed["manual_review"], 1)
        self.assertEqual(self.jobs()[0]["state"], "manual_review")
        self.assertEqual(
            self.jobs()[0]["last_error_class"],
            "invalid_media_count_after_refresh",
        )

    def test_local_execution_errors_use_the_same_bounded_generation(self):
        self.add_missing()
        calls = []

        def failed_spawn(**_kwargs):
            calls.append(1)
            raise OSError("SECRET_PATH")

        self.assertEqual(
            self.run_worker(fetcher=failed_spawn, max_posts=1)["retryable"], 1
        )
        self.assertEqual(
            self.run_worker(fetcher=failed_spawn, max_posts=1)["retryable"], 1
        )
        self.assertEqual(
            self.run_worker(fetcher=failed_spawn, max_posts=1)["manual_review"],
            1,
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(self.refreshes()), 1)
        self.assertNotIn(b"SECRET_PATH", self.db_path.read_bytes())

    def test_runner_local_failure_restores_refresh_claim_and_stops_phase(self):
        self.add_missing()

        def failed_runner(**_kwargs):
            raise context_x.ContextLocalExecutionError("worker failed locally")

        with self.assertRaises(context_x.ContextLocalExecutionError):
            self.run_worker(fetcher=failed_runner)

        refresh = self.refreshes()[0]
        self.assertEqual(refresh["state"], "pending")
        self.assertEqual(refresh["attempts"], 0)
        self.assertIsNone(refresh["lease_token"])
        self.assertIsNone(refresh["last_error_class"])

    def test_unknown_legacy_destination_fails_without_request(self):
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.connection.execute(
                """INSERT INTO asset_jobs(
                       owner_kind,owner_id,media_ordinal,state,compatibility_job,
                       destination_scope,created_at,updated_at
                   ) VALUES ('post','100',1,'needs_refresh',1,'unknown','now','now')"""
            )
        calls = []
        observed = self.run_worker(fetcher=lambda **kwargs: calls.append(kwargs))
        self.assertEqual(observed["manual_review"], 1)
        self.assertEqual(calls, [])
        self.assertEqual(self.refreshes()[0]["last_error_class"], "refresh_destination_unknown")

    def test_quality_alert_uses_post_owner_ratio_after_minimum_sample(self):
        with context_x.ContextDB(self.db_path, create=False) as database:
            now = "now"
            for value in range(100, 200):
                database.connection.execute(
                    """INSERT INTO asset_jobs(
                           owner_kind,owner_id,media_ordinal,state,compatibility_job,
                           destination_scope,created_at,updated_at
                       ) VALUES ('post',?,1,'manual_review',1,'context',?,?)""",
                    (str(value), now, now),
                )
            for value in (100, 101, 102):
                database.connection.execute(
                    """INSERT INTO descriptor_refresh_jobs(
                           owner_kind,owner_id,generation,state,reason,
                           created_at,updated_at,completed_at
                       ) VALUES ('post',?,1,'manual_review','compatibility',?,?,?)""",
                    (str(value), now, now, now),
                )
            quality = database.descriptor_refresh_quality()
        self.assertEqual(quality["post_owners"], 100)
        self.assertEqual(quality["automatic_refresh_owners"], 3)
        self.assertAlmostEqual(quality["ratio"], 0.03)
        self.assertTrue(quality["alert"])

    def test_fetch_callback_runs_outside_sqlite_write_transaction(self):
        self.add_missing()

        def fetcher(**_kwargs):
            connection = sqlite3.connect(self.db_path, timeout=0)
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.rollback()
            finally:
                connection.close()
            return result("100", rows=(descriptor("100", 1),))

        observed = self.run_worker(fetcher=fetcher)
        self.assertEqual(observed["complete"], 1)

    def test_exact_refresh_config_is_focal_metadata_only_and_scope_aware(self):
        work = self.user_dir / "_state" / "work"
        work.mkdir()
        config, _raw, _descriptors = context_x.build_context_config(
            handle="alice",
            post_id="100",
            archive_root=self.root,
            user_dir=self.user_dir,
            cookie_file=self.root / "cookies.txt",
            work_dir=work,
            media=False,
            conversation=False,
            operation_id="refresh-op",
            descriptor_source_kind="exact_refresh",
            descriptor_source_operation="exact_refresh",
            destination_scope="main",
        )
        twitter = config["extractor"]["twitter"]
        self.assertEqual(
            twitter["directory"],
            ["users", "alice", "media", "{date:%Y}", "{date:%m}"],
        )
        self.assertEqual(twitter["tweet-endpoint"], "rest")
        self.assertFalse(twitter["conversations"])
        self.assertNotIn("archive", twitter)
        descriptor_pp = next(
            item
            for item in twitter["postprocessors"]
            if item["name"] == descriptor_x.POSTPROCESSOR_NAME
        )
        self.assertEqual(descriptor_pp["source-kind"], "exact_refresh")
        self.assertEqual(descriptor_pp["source-operation"], "exact_refresh")

    def test_profile_rejection_uses_no_extra_x_and_changed_info_reopens(self):
        info = {
            "id": 1,
            "name": "alice",
            "profile_image": (
                "https://pbs.twimg.com/profile_images/"
                "1873027274501783552/first_normal.jpg"
            ),
        }
        first = descriptor_x.profile_batch_from_info(
            info,
            user_dir=self.user_dir,
            operation_id="info-1",
            run_id="info-1",
            captured_at="2026-01-01T00:00:00Z",
            source_relative_path="runs/info-1.jsonl",
            source_sha256="a" * 64,
        )
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.persist_descriptor_batches((first,), (), allow_profile=True)
            database.connection.execute(
                """UPDATE asset_jobs SET state='needs_refresh',
                       last_error_class='descriptor_rejected',
                       last_error_detail='descriptor_rejected:status=404'
                     WHERE owner_kind='profile_avatar'"""
            )
        calls = []
        observed = self.run_worker(fetcher=lambda **kwargs: calls.append(kwargs))
        self.assertEqual(observed["attempted"], 0)
        self.assertEqual(calls, [])
        with context_x.ContextDB(self.db_path, create=False) as database:
            state = database.connection.execute(
                """SELECT state,expected_relative_path FROM asset_jobs
                     WHERE owner_kind='profile_avatar'"""
            ).fetchone()
        self.assertEqual(state["state"], "unavailable")
        first_path = state["expected_relative_path"]

        changed = dict(info)
        changed["profile_image"] = (
            "https://pbs.twimg.com/profile_images/"
            "1900000000000000000/second_normal.jpg"
        )
        second = descriptor_x.profile_batch_from_info(
            changed,
            user_dir=self.user_dir,
            operation_id="info-2",
            run_id="info-2",
            captured_at="2026-02-01T00:00:00Z",
            source_relative_path="runs/info-2.jsonl",
            source_sha256="b" * 64,
        )
        with context_x.ContextDB(self.db_path, create=False) as database:
            database.persist_descriptor_batches((second,), (), allow_profile=True)
            row = database.connection.execute(
                """SELECT state,expected_relative_path FROM asset_jobs
                     WHERE owner_kind='profile_avatar'"""
            ).fetchone()
        self.assertEqual(row["state"], "pending")
        self.assertNotEqual(row["expected_relative_path"], first_path)

    def test_completion_fault_leaves_lease_reclaimable_without_false_success(self):
        self.add_missing()
        with mock.patch.object(
            context_x.ContextDB,
            "descriptor_refresh_succeeded",
            side_effect=RuntimeError("injected completion fault"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.run_worker(
                    clock=lambda: 0,
                    lease_seconds=10,
                    fetcher=lambda **_kwargs: result(
                        "100", rows=(descriptor("100", 1),)
                    ),
                )
        self.assertEqual(self.refreshes()[0]["state"], "leased")
        self.assertEqual(self.jobs()[0]["state"], "needs_refresh")

        observed = self.run_worker(
            clock=lambda: 100,
            lease_seconds=10,
            fetcher=lambda **_kwargs: result(
                "100", rows=(descriptor("100", 1),)
            ),
        )
        self.assertEqual(observed["complete"], 1)
        self.assertEqual(len(self.refreshes()), 1)


if __name__ == "__main__":
    unittest.main()
