import io
import json
import logging
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

import gallery_dl.config
import requests
from gallery_dl.extractor.common import Extractor


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_x_runner_control as control_x


class TwitterSessionFixture(Extractor):
    category = "twitter"
    subcategory = "goal5-control-fixture"

    def __init__(self):
        super().__init__(re.match(r".*", "fixture://control"))

    def config(self, _key, default=None):
        return default

    def config2(self, _key, _key2, default=None, sentinel=None):
        return default

    def config_accumulate(self, _key):
        return None


def run_command(scope, index, argv=None):
    return {
        "type": "run",
        "protocol": control_x.PROTOCOL_VERSION,
        "scope": scope,
        "item_id": f"{index:032x}",
        "lease_token": f"{index + 10_000:032x}",
        "argv": argv or ["--version"],
    }


def command_stream(commands):
    return io.StringIO(
        "".join(json.dumps(command) + "\n" for command in commands)
    )


def protocol_messages(stream):
    return [json.loads(line) for line in stream.getvalue().splitlines()]


class ControlProtocolTests(unittest.TestCase):
    def setUp(self):
        self.scope = control_x.account_scope_digest("12345")

    def test_worker_options_are_private_bounded_and_all_or_none(self):
        options = control_x.WorkerOptions(self.scope, 100, 900)
        parsed, remaining = control_x.parse_worker_options(
            control_x.worker_options_as_args(options)
        )
        self.assertEqual(parsed, options)
        self.assertEqual(remaining, [])
        self.assertEqual(
            control_x.parse_worker_options(["--version"]),
            (None, ["--version"]),
        )
        with self.assertRaises(control_x.ControlProtocolError):
            control_x.parse_worker_options([control_x.WORKER_OPTION])
        with self.assertRaisesRegex(control_x.ControlProtocolError, "1 to 100"):
            control_x.parse_worker_options(
                control_x.worker_options_as_args(
                    control_x.WorkerOptions(self.scope, 101, 900)
                )
            )

    def test_begin_and_result_are_token_matched_and_do_not_echo_arguments(self):
        secret = "SIGNED-PRIVATE-URL-SENTINEL"
        output = io.StringIO()
        gallery = io.StringIO()
        seen = []

        def execute(argv, *, runner_starts):
            seen.append((argv, runner_starts))
            return 0

        status = control_x.worker_loop(
            control_x.WorkerOptions(self.scope, 1, 60),
            execute,
            input_stream=command_stream(
                [run_command(self.scope, 1, [f"https://example.test/{secret}"])]
            ),
            protocol_output=output,
            gallery_output=gallery,
        )
        self.assertEqual(status, 0)
        messages = protocol_messages(output)
        self.assertEqual([item["type"] for item in messages], ["ready", "begin", "result"])
        self.assertEqual(messages[1]["item_id"], f"{1:032x}")
        self.assertEqual(messages[1]["lease_token"], f"{10001:032x}")
        self.assertEqual(messages[2]["item_id"], messages[1]["item_id"])
        self.assertEqual(messages[2]["lease_token"], messages[1]["lease_token"])
        self.assertTrue(messages[2]["retire"])
        self.assertEqual(messages[2]["retire_reason"], "item_cap")
        self.assertNotIn(secret, output.getvalue())
        self.assertEqual(seen, [([f"https://example.test/{secret}"], 1)])

    def test_wrong_scope_stops_before_execute(self):
        output = io.StringIO()
        executed = []
        command = run_command("f" * 64, 1)
        status = control_x.worker_loop(
            control_x.WorkerOptions(self.scope, 10, 60),
            lambda *_args, **_kwargs: executed.append(True),
            input_stream=command_stream([command]),
            protocol_output=output,
            gallery_output=io.StringIO(),
        )
        self.assertEqual(status, 32)
        self.assertEqual(executed, [])
        self.assertEqual(protocol_messages(output)[-1]["type"], "protocol_error")
        self.assertNotIn(self.scope, output.getvalue())

    def test_item_cleanup_prevents_config_and_handler_leak(self):
        output = io.StringIO()
        root = logging.getLogger()
        leaked_handler = logging.StreamHandler(io.StringIO())
        calls = 0

        def execute(_argv, *, runner_starts):
            nonlocal calls
            calls += 1
            if calls == 1:
                gallery_dl.config.set(("extractor",), "private-fixture", "leak")
                root.addHandler(leaked_handler)
                self.assertEqual(runner_starts, 1)
            else:
                self.assertIsNone(
                    gallery_dl.config.get(("extractor",), "private-fixture")
                )
                self.assertNotIn(leaked_handler, root.handlers)
                self.assertEqual(runner_starts, 0)
            return 0

        status = control_x.worker_loop(
            control_x.WorkerOptions(self.scope, 2, 60),
            execute,
            input_stream=command_stream(
                [run_command(self.scope, 1), run_command(self.scope, 2)]
            ),
            protocol_output=output,
            gallery_output=io.StringIO(),
        )
        self.assertEqual(status, 0)
        self.assertEqual(calls, 2)
        self.assertNotIn(leaked_handler, root.handlers)

    def test_keyboard_interrupt_returns_one_bounded_result_and_retires(self):
        output = io.StringIO()

        def interrupted(_argv, *, runner_starts):
            raise KeyboardInterrupt

        status = control_x.worker_loop(
            control_x.WorkerOptions(self.scope, 100, 60),
            interrupted,
            input_stream=command_stream([run_command(self.scope, 1)]),
            protocol_output=output,
            gallery_output=io.StringIO(),
        )
        self.assertEqual(status, 130)
        result = protocol_messages(output)[-1]
        self.assertEqual(result["status"], 130)
        self.assertEqual(result["retire_reason"], "interrupted")
        self.assertEqual(result["error_class"], "KeyboardInterrupt")

    def test_worker_age_cap_retires_only_after_current_result(self):
        values = iter((0.0, 2.0))
        output = io.StringIO()
        executed = []
        status = control_x.worker_loop(
            control_x.WorkerOptions(self.scope, 100, 1),
            lambda argv, **_kwargs: executed.append(argv) or 0,
            input_stream=command_stream(
                [run_command(self.scope, 1), run_command(self.scope, 2)]
            ),
            protocol_output=output,
            gallery_output=io.StringIO(),
            clock=lambda: next(values),
        )
        self.assertEqual(status, 0)
        self.assertEqual(executed, [["--version"]])
        result = protocol_messages(output)[-1]
        self.assertEqual(result["type"], "result")
        self.assertEqual(result["retire_reason"], "age_cap")
        self.assertEqual(result["session_age_ms"], 2_000)

    def test_production_worker_meets_thousand_item_start_and_session_metric(self):
        original_init = requests.Session.__init__
        sessions = 0

        def counted_init(session, *args, **kwargs):
            nonlocal sessions
            sessions += 1
            original_init(session, *args, **kwargs)

        starts = 0
        runner_start_sum = 0
        completed = 0
        with mock.patch.object(requests.Session, "__init__", counted_init):
            for batch in range(10):
                commands = [
                    run_command(self.scope, batch * 100 + offset)
                    for offset in range(1, 101)
                ]
                output = io.StringIO()

                def execute(_argv, *, runner_starts):
                    TwitterSessionFixture().initialize()
                    return 0

                status = control_x.worker_loop(
                    control_x.WorkerOptions(self.scope, 100, 3_600),
                    execute,
                    input_stream=command_stream(commands),
                    protocol_output=output,
                    gallery_output=io.StringIO(),
                )
                self.assertEqual(status, 0)
                results = [
                    item
                    for item in protocol_messages(output)
                    if item["type"] == "result"
                ]
                starts += 1
                runner_start_sum += sum(item["runner_starts"] for item in results)
                completed += len(results)

        self.assertEqual(completed, 1_000)
        self.assertEqual(starts, 10)
        self.assertEqual(runner_start_sum, 10)
        self.assertEqual(sessions, 10)
        self.assertGreaterEqual(1 - starts / 1_000, 0.98)


class ControlClientTests(unittest.TestCase):
    def setUp(self):
        self.scope = control_x.account_scope_digest("67890")

    def test_real_base_runner_worker_reuses_process_and_retires_at_cap(self):
        options = control_x.WorkerOptions(self.scope, 2, 60)
        output = []
        client = control_x.RunnerControlClient(
            [sys.executable, str(SCRIPTS / "gallery_dl_x_runner.py")],
            options,
        )
        try:
            first = client.run(
                item_id="1" * 32,
                lease_token="a" * 32,
                argv=["--version"],
                output=output.append,
            )
            second = client.run(
                item_id="2" * 32,
                lease_token="b" * 32,
                argv=["--version"],
                output=output.append,
            )
            self.assertFalse(first.retire)
            self.assertEqual(first.runner_starts, 1)
            self.assertTrue(second.retire)
            self.assertEqual(second.runner_starts, 0)
            self.assertEqual(client.starts, 1)

            third = client.run(
                item_id="3" * 32,
                lease_token="c" * 32,
                argv=["--version"],
                output=output.append,
            )
            self.assertEqual(third.runner_starts, 1)
            self.assertEqual(client.starts, 2)
        finally:
            client.close()
        self.assertGreaterEqual("".join(output).count("1.32.4"), 3)

    def test_real_worker_keeps_gallery_dl_off_control_stdin(self):
        """A real main() call used to fail before extraction in ~2 ms."""
        client = control_x.RunnerControlClient(
            [sys.executable, str(SCRIPTS / "gallery_dl_x_runner.py")],
            control_x.WorkerOptions(self.scope, 2, 60),
        )
        output = []
        arguments = ["--config-ignore", "--no-input", "--no-colors", "noop"]
        try:
            first = client.run(
                item_id="a" * 32,
                lease_token="b" * 32,
                argv=arguments,
                output=output.append,
            )
            second = client.run(
                item_id="c" * 32,
                lease_token="d" * 32,
                argv=arguments,
                output=output.append,
            )
        finally:
            client.close()

        self.assertEqual((first.status, first.error_class), (0, None))
        self.assertEqual((second.status, second.error_class), (0, None))
        self.assertEqual(output, [])

    def test_real_legacy_runner_supports_same_control_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = control_x.RunnerControlClient(
                [sys.executable, str(SCRIPTS / "gallery_dl_x_legacy_runner.py")],
                control_x.WorkerOptions(self.scope, 2, 60),
            )
            output = []
            try:
                ordinary = client.run(
                    item_id="8" * 32,
                    lease_token="d" * 32,
                    argv=["--version"],
                    output=output.append,
                )
                legacy = client.run(
                    item_id="9" * 32,
                    lease_token="e" * 32,
                    argv=[
                        "--archive-x-legacy-telemetry",
                        str(root / "legacy.json"),
                        "--archive-x-legacy-request-limit",
                        "5",
                        "--archive-x-legacy-empty-tail-pages",
                        "2",
                        "--archive-x-request-telemetry",
                        str(root / "requests.json"),
                        "--archive-x-operation",
                        "legacy_walk",
                        "--version",
                    ],
                    output=output.append,
                )
            finally:
                client.close()
        self.assertEqual(ordinary.status, 0)
        self.assertFalse(ordinary.retire)
        self.assertEqual(ordinary.runner_starts, 1)
        self.assertEqual(legacy.status, 0)
        self.assertTrue(legacy.retire)
        self.assertEqual(legacy.runner_starts, 0)
        self.assertEqual(client.starts, 1)
        self.assertGreaterEqual("".join(output).count("1.32.4"), 2)

    def test_output_callback_failure_does_not_break_archive_result(self):
        client = control_x.RunnerControlClient(
            [sys.executable, str(SCRIPTS / "gallery_dl_x_runner.py")],
            control_x.WorkerOptions(self.scope, 1, 60),
        )

        def broken_renderer(_line):
            raise RuntimeError("private-renderer-detail")

        try:
            result = client.run(
                item_id="7" * 32,
                lease_token="f" * 32,
                argv=["--version"],
                output=broken_renderer,
            )
        finally:
            client.close()
        self.assertEqual(result.status, 0)
        self.assertEqual(client.last_output_error_class, "RuntimeError")

    def test_unterminated_worker_output_cannot_hide_the_output_barrier(self):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "partial_output_worker.py"
            script.write_text(
                textwrap.dedent(
                    f"""
                    import sys
                    sys.path.insert(0, {str(SCRIPTS)!r})
                    import archive_x_runner_control as control

                    def execute(argv, *, runner_starts):
                        sys.stderr.write("unterminated-progress")
                        sys.stderr.flush()
                        return 0

                    options, remaining = control.parse_worker_options(sys.argv[1:])
                    if options is None or remaining:
                        raise SystemExit(32)
                    raise SystemExit(control.worker_loop(options, execute))
                    """
                ),
                encoding="utf-8",
            )
            output = []
            client = control_x.RunnerControlClient(
                [sys.executable, str(script)],
                control_x.WorkerOptions(self.scope, 1, 60),
            )
            try:
                result = client.run(
                    item_id="6" * 32,
                    lease_token="e" * 32,
                    argv=["--fixture"],
                    output=output.append,
                )
            finally:
                client.close()

        self.assertEqual(result.status, 0)
        self.assertIn("unterminated-progress", "".join(output))

    def test_worker_loss_after_begin_is_explicit_and_only_current_item_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "fixture_worker.py"
            script.write_text(
                textwrap.dedent(
                    f"""
                    import os
                    import sys
                    sys.path.insert(0, {str(SCRIPTS)!r})
                    import archive_x_runner_control as control

                    def execute(argv, *, runner_starts):
                        if argv == ["--crash"]:
                            os._exit(73)
                        return 0

                    options, remaining = control.parse_worker_options(sys.argv[1:])
                    if options is None or remaining:
                        raise SystemExit(32)
                    raise SystemExit(control.worker_loop(options, execute))
                    """
                ),
                encoding="utf-8",
            )
            client = control_x.RunnerControlClient(
                [sys.executable, str(script)],
                control_x.WorkerOptions(self.scope, 100, 60),
            )
            try:
                with self.assertRaises(control_x.RunnerWorkerLost) as caught:
                    client.run(
                        item_id="4" * 32,
                        lease_token="d" * 32,
                        argv=["--crash"],
                    )
                self.assertTrue(caught.exception.began)
                self.assertEqual(client.starts, 1)
                replay = client.run(
                    item_id="4" * 32,
                    lease_token="d" * 32,
                    argv=["--ok"],
                )
                self.assertEqual(replay.status, 0)
                self.assertEqual(client.starts, 2)
            finally:
                client.close()


if __name__ == "__main__":
    unittest.main()
