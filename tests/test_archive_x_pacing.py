import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x_context as context_x
import archive_x_pacing as pacing_x
import archive_x_request_telemetry as telemetry_x


class FakeClock:
    def __init__(self, value: float = 100.0):
        self.value = value
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class PacingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "context.sqlite3"
        with context_x.ContextDB(self.database) as database:
            database.bind_identity("12345", "fixture")

    def tearDown(self):
        self.temporary.cleanup()

    def options(
        self,
        *,
        low: float = 4.0,
        high: float = 4.0,
        lease: float = 30.0,
        backoff: float = 300.0,
    ) -> pacing_x.SchedulerOptions:
        return pacing_x.SchedulerOptions(
            database=self.database,
            scope_id="12345",
            delay_low=low,
            delay_high=high,
            lease_seconds=lease,
            backoff_429_seconds=backoff,
        )

    def scheduler(
        self,
        clock: FakeClock,
        operation: str = "context_metadata",
        **option_values,
    ) -> pacing_x.DurableRequestScheduler:
        return pacing_x.DurableRequestScheduler(
            self.options(**option_values),
            operation,
            clock=clock.now,
            sleep=clock.sleep,
            choose_gap=lambda low, _high: low,
        )

    def complete(
        self,
        scheduler,
        reservation,
        *,
        status=200,
        headers=None,
        rate_limit_threshold=None,
    ):
        scheduler.complete(
            reservation,
            status=status,
            headers=headers or {},
            error=None,
            rate_limit_threshold=rate_limit_threshold,
        )

    def pacing_row(self):
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        try:
            return dict(
                connection.execute(
                    "SELECT * FROM pacing WHERE singleton=1"
                ).fetchone()
            )
        finally:
            connection.close()

    def test_private_runner_options_are_all_or_none_and_round_trip(self):
        options = self.options(low=4, high=8, lease=120, backoff=240)
        argv = pacing_x.options_as_runner_args(options) + ["--version"]
        parsed, remaining = pacing_x.parse_runner_options(argv)
        self.assertEqual(parsed, options)
        self.assertEqual(remaining, ["--version"])
        with self.assertRaisesRegex(pacing_x.PacingError, "all scheduler"):
            pacing_x.parse_runner_options(
                [pacing_x.SCHEDULER_DB_OPTION, str(self.database)]
            )
        with self.assertRaisesRegex(pacing_x.PacingError, "numeric account"):
            invalid = list(argv)
            invalid[invalid.index(pacing_x.SCHEDULER_SCOPE_OPTION) + 1] = "alice"
            pacing_x.parse_runner_options(invalid)

    def test_actual_starts_keep_floor_and_long_idle_never_earns_a_burst(self):
        clock = FakeClock()
        with self.scheduler(clock) as scheduler:
            first = scheduler.reserve("x_api", "tweet_result")
            self.assertEqual(first.started_at, 100)
            self.complete(scheduler, first)
            second = scheduler.reserve("x_support", "client_bootstrap")
            self.assertEqual(second.started_at, 104)
            self.assertEqual(second.wait_source, "spacing")
            self.complete(scheduler, second)

            clock.value = 1_000
            third = scheduler.reserve("x_api", "tweet_detail")
            self.assertEqual(third.started_at, 1_000)
            self.complete(scheduler, third)
            fourth = scheduler.reserve("x_api", "tweet_detail")
            self.assertEqual(fourth.started_at, 1_004)
            self.complete(scheduler, fourth)

        self.assertEqual(clock.sleeps, [4.0, 4.0])
        row = self.pacing_row()
        self.assertEqual(row["request_sequence"], 4)
        self.assertIsNone(row["reservation_token"])
        self.assertEqual(row["next_request_at"], 1_008)

    def test_zero_outer_phase_sleeps_still_preserve_actual_request_floor(self):
        clock = FakeClock()
        starts = []
        for operation in sorted(telemetry_x.VALID_OPERATIONS):
            # Model Stage 9's removal of every logical phase/target sleep: a
            # new phase starts immediately, and only the actual-call lane waits.
            with self.scheduler(clock, operation=operation) as scheduler:
                reservation = scheduler.reserve("x_api", "x_api_other")
                starts.append(reservation.started_at)
                self.complete(scheduler, reservation)

        self.assertEqual(starts[0], 100)
        self.assertTrue(
            all(later - earlier >= 4 for earlier, later in zip(starts, starts[1:]))
        )
        self.assertEqual(self.pacing_row()["request_sequence"], len(starts))

    def test_restart_inherits_not_before_boundary(self):
        clock = FakeClock()
        first_scheduler = self.scheduler(clock, operation="timeline")
        first = first_scheduler.reserve("x_api", "user_tweets_replies")
        self.complete(first_scheduler, first)
        first_scheduler.close()

        with self.scheduler(clock, operation="legacy_walk") as restarted:
            second = restarted.reserve("x_api", "search_timeline")
            self.assertEqual(second.started_at, 104)
            self.assertEqual(second.waited_seconds, 4)
            self.complete(restarted, second)

    def test_active_lease_serializes_two_schedulers(self):
        release = threading.Event()
        first_started = threading.Event()
        starts: list[float] = []
        active = 0
        peak = 0
        guard = threading.Lock()

        def worker(hold: bool):
            nonlocal active, peak
            options = self.options(low=0.02, high=0.02, lease=2)
            with pacing_x.DurableRequestScheduler(
                options,
                "context_metadata",
                lease_poll_seconds=0.005,
            ) as scheduler:
                reservation = scheduler.reserve("x_api", "tweet_detail")
                with guard:
                    starts.append(reservation.started_at)
                    active += 1
                    peak = max(peak, active)
                if hold:
                    first_started.set()
                    release.wait(2)
                time.sleep(0.01)
                with guard:
                    active -= 1
                self.complete(scheduler, reservation)

        one = threading.Thread(target=worker, args=(True,))
        two = threading.Thread(target=worker, args=(False,))
        one.start()
        self.assertTrue(first_started.wait(2))
        two.start()
        time.sleep(0.03)
        self.assertEqual(len(starts), 1)
        release.set()
        one.join(2)
        two.join(2)
        self.assertFalse(one.is_alive())
        self.assertFalse(two.is_alive())
        self.assertEqual(peak, 1)
        self.assertGreaterEqual(starts[1] - starts[0], 0.019)

    def test_killed_request_reclaims_only_after_stale_horizon(self):
        clock = FakeClock()
        killed = self.scheduler(clock, lease=10)
        reservation = killed.reserve("x_api", "tweet_result")
        self.assertEqual(reservation.sequence, 1)
        killed.close()  # Simulate process loss before completion.
        clock.value = 111
        with self.scheduler(clock, operation="context_exact", lease=10) as restarted:
            replay = restarted.reserve("x_api", "tweet_result")
            self.assertEqual(replay.sequence, 2)
            self.assertEqual(replay.started_at, 111)
            self.complete(restarted, replay)
        row = self.pacing_row()
        self.assertEqual(row["reservation_recoveries"], 1)

    def test_interrupt_during_durable_wait_spends_no_request(self):
        clock = FakeClock()
        with self.scheduler(clock) as scheduler:
            first = scheduler.reserve("x_api", "tweet_result")
            self.complete(scheduler, first)

            def interrupt(_seconds):
                raise KeyboardInterrupt

            scheduler.sleep = interrupt
            with self.assertRaises(KeyboardInterrupt):
                scheduler.reserve("x_api", "tweet_detail")
        row = self.pacing_row()
        self.assertEqual(row["request_sequence"], 1)
        self.assertIsNone(row["reservation_token"])
        self.assertEqual(row["next_request_at"], 104)

        with self.scheduler(clock) as restarted:
            replay = restarted.reserve("x_api", "tweet_detail")
            self.assertEqual(replay.sequence, 2)
            self.assertEqual(replay.started_at, 104)
            self.complete(restarted, replay)

    def test_auth_stop_appearing_during_wait_prevents_the_next_claim(self):
        clock = FakeClock()
        with self.scheduler(clock) as scheduler:
            first = scheduler.reserve("x_api", "tweet_result")
            self.complete(scheduler, first)

            def stop_during_wait(seconds):
                with context_x.ContextDB(self.database) as database:
                    database.record_authentication_stop(
                        "authentication", now=clock.now()
                    )
                clock.sleep(seconds)

            scheduler.sleep = stop_during_wait
            with self.assertRaises(pacing_x.PacingAuthenticationError):
                scheduler.reserve("x_api", "tweet_detail")
        self.assertEqual(self.pacing_row()["request_sequence"], 1)

    def test_sqlite_busy_and_completion_fault_fail_closed_then_reclaim(self):
        clock = FakeClock()
        scheduler = self.scheduler(clock, lease=10)
        blocker = sqlite3.connect(self.database, isolation_level=None)
        blocker.execute("PRAGMA busy_timeout=1")
        scheduler.connection.execute("PRAGMA busy_timeout=1")
        blocker.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaisesRegex(pacing_x.PacingError, "database"):
                scheduler.reserve("x_api", "tweet_result")
        finally:
            blocker.rollback()
            blocker.close()
        self.assertEqual(self.pacing_row()["request_sequence"], 0)

        reservation = scheduler.reserve("x_api", "tweet_result")
        scheduler.connection.close()
        with self.assertRaisesRegex(pacing_x.PacingError, "database"):
            self.complete(scheduler, reservation)
        clock.value = 111
        with self.scheduler(clock, lease=10) as restarted:
            replay = restarted.reserve("x_api", "tweet_result")
            self.assertEqual(replay.sequence, 2)
            self.complete(restarted, replay)
        self.assertEqual(self.pacing_row()["reservation_recoveries"], 1)

    def test_low_quota_reset_is_durable_and_never_shortens_spacing(self):
        clock = FakeClock()
        with self.scheduler(clock) as scheduler:
            reservation = scheduler.reserve("x_api", "tweet_result")
            self.complete(
                scheduler,
                reservation,
                headers={
                    "x-rate-limit-remaining": "1",
                    "x-rate-limit-reset": "130",
                },
                rate_limit_threshold=2,
            )
        with self.scheduler(clock, operation="descriptor_refresh") as restarted:
            after_reset = restarted.reserve("x_api", "tweet_result")
            self.assertEqual(after_reset.started_at, 130)
            self.assertEqual(after_reset.wait_source, "rate_limit")
            self.complete(restarted, after_reset)

        clock.value = 200
        with self.scheduler(clock) as scheduler:
            reservation = scheduler.reserve("x_api", "tweet_result")
            self.complete(
                scheduler,
                reservation,
                headers={
                    "x-rate-limit-remaining": "1",
                    "x-rate-limit-reset": "201",
                },
                rate_limit_threshold=2,
            )
        self.assertEqual(self.pacing_row()["next_request_at"], 204)

    def test_429_without_reset_persists_backoff(self):
        clock = FakeClock()
        with self.scheduler(clock, backoff=60) as scheduler:
            reservation = scheduler.reserve("x_api", "tweet_result")
            self.complete(scheduler, reservation, status=429)
        with self.scheduler(clock, operation="retry_media", backoff=60) as restarted:
            next_reservation = restarted.reserve("x_api", "tweet_result")
            self.assertEqual(next_reservation.started_at, 160)
            self.assertEqual(next_reservation.wait_source, "http_429")
            self.complete(restarted, next_reservation)

    def test_cdn_and_external_calls_do_not_touch_x_lane(self):
        clock = FakeClock()
        with self.scheduler(clock) as scheduler:
            self.assertIsNone(scheduler.reserve("media_cdn", "media_asset"))
            self.assertIsNone(scheduler.reserve("external", "external_http"))
        row = self.pacing_row()
        self.assertEqual(row["request_sequence"], 0)
        self.assertIsNone(row["last_request_at"])

    def test_auth_stop_blocks_every_x_phase_before_reservation(self):
        with context_x.ContextDB(self.database) as database:
            database.record_authentication_stop("authentication", now=100)
        operations = (
            "info",
            "timeline",
            "retry_media",
            "profile_avatar",
            "profile_background",
            "context_metadata",
            "context_exact",
            "context_media",
            "descriptor_refresh",
            "legacy_walk",
        )
        for operation in operations:
            clock = FakeClock()
            with self.scheduler(clock, operation=operation) as scheduler:
                with self.assertRaises(pacing_x.PacingAuthenticationError):
                    scheduler.reserve("x_api", "x_api_other")
                self.assertEqual(clock.sleeps, [])
        self.assertEqual(self.pacing_row()["request_sequence"], 0)

    def test_scope_mismatch_and_stale_completion_fail_closed(self):
        clock = FakeClock()
        wrong = self.options()
        wrong = pacing_x.SchedulerOptions(
            database=wrong.database,
            scope_id="999",
            delay_low=wrong.delay_low,
            delay_high=wrong.delay_high,
            lease_seconds=wrong.lease_seconds,
            backoff_429_seconds=wrong.backoff_429_seconds,
        )
        with self.assertRaisesRegex(pacing_x.PacingError, "scope"):
            pacing_x.DurableRequestScheduler(wrong, "timeline")

        with self.scheduler(clock) as scheduler:
            reservation = scheduler.reserve("x_api", "tweet_result")
            fake = pacing_x.PacingReservation(
                token="0" * 32,
                sequence=reservation.sequence,
                category=reservation.category,
                endpoint=reservation.endpoint,
                operation=reservation.operation,
                started_at=reservation.started_at,
                waited_seconds=0,
                wait_source="none",
            )
            with self.assertRaisesRegex(pacing_x.PacingError, "stale"):
                self.complete(scheduler, fake)
            self.complete(scheduler, reservation)

    def test_schema_trigger_rejects_half_request_lease(self):
        connection = sqlite3.connect(self.database)
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "request lease"):
                connection.execute(
                    "UPDATE pacing SET reservation_token='bad' WHERE singleton=1"
                )
        finally:
            connection.close()

    def test_actual_boundary_recorder_uses_durable_gate_for_hidden_calls(self):
        class Response:
            status = 200
            headers = {"Content-Length": "0"}

        clock = FakeClock()
        with self.scheduler(clock, operation="context_exact") as scheduler:
            recorder = telemetry_x.RequestRecorder(
                self.root / "requests.json",
                "context_exact",
                clock=clock.now,
                wall_clock=clock.now,
                request_gate=scheduler,
            )
            for endpoint in ("TweetResultByRestId", "TweetDetail"):
                recorder.observe(
                    transport="fixture",
                    url=f"https://x.com/i/api/graphql/hash/{endpoint}",
                    method="GET",
                    send=Response,
                    status_getter=lambda response: response.status,
                    headers_getter=lambda response: response.headers,
                )
            recorder.observe(
                transport="fixture",
                url="https://pbs.twimg.com/media/file.jpg",
                method="GET",
                send=Response,
                status_getter=lambda response: response.status,
                headers_getter=lambda response: response.headers,
            )
            summary = recorder.value(0)["summary"]
        self.assertEqual(summary["actual_requests"], 3)
        self.assertEqual(summary["by_category"], {"media_cdn": 1, "x_api": 2})
        self.assertEqual(summary["pacing_wait_ms"], 4_000)
        self.assertEqual(summary["minimum_start_gap_ms"], 0)
        self.assertEqual(self.pacing_row()["request_sequence"], 2)


if __name__ == "__main__":
    unittest.main()
