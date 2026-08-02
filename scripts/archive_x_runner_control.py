#!/usr/bin/env python3
"""Bounded same-account control protocol for pinned gallery-dl runners."""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import json
import logging
import re
import select
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Callable, TextIO

import gallery_dl.config
import gallery_dl.output
from gallery_dl.extractor.common import Extractor


PROTOCOL_VERSION = 1
WORKER_OPTION = "--archive-x-control-worker"
WORKER_SCOPE_OPTION = "--archive-x-worker-scope"
WORKER_MAX_ITEMS_OPTION = "--archive-x-worker-max-items"
WORKER_MAX_AGE_OPTION = "--archive-x-worker-max-age-seconds"
WORKER_VALUE_OPTIONS = frozenset(
    {WORKER_SCOPE_OPTION, WORKER_MAX_ITEMS_OPTION, WORKER_MAX_AGE_OPTION}
)
DEFAULT_MAX_ITEMS = 100
DEFAULT_MAX_AGE_SECONDS = 900.0
MAX_ARGUMENTS = 512
MAX_ARGUMENT_BYTES = 1_048_576
CONTROL_ID_RE = re.compile(r"[0-9a-f]{32}\Z")
SCOPE_RE = re.compile(r"[0-9a-f]{64}\Z")
SAFE_ERROR_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,79}\Z")
OUTPUT_BARRIER_PREFIX = "\x1egdl-x-control-output-end:"


class ControlProtocolError(RuntimeError):
    """A worker command or acknowledgement violated the private protocol."""


class RunnerWorkerLost(ControlProtocolError):
    """The bounded worker exited before a token-matched result."""

    def __init__(self, *, began: bool):
        super().__init__("runner worker exited before a complete result")
        self.began = began


@dataclasses.dataclass(frozen=True)
class WorkerOptions:
    scope: str
    max_items: int = DEFAULT_MAX_ITEMS
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS


@dataclasses.dataclass(frozen=True)
class WorkerResult:
    item_id: str
    lease_token: str
    status: int
    item_index: int
    runner_starts: int
    session_age_ms: int
    retire: bool
    retire_reason: str | None
    error_class: str | None


def account_scope_digest(account_id: str) -> str:
    if not account_id.isdecimal() or int(account_id) < 1:
        raise ControlProtocolError("worker account scope must be numeric")
    return hashlib.sha256(
        f"gdl-x-control-v1:{account_id}".encode("ascii")
    ).hexdigest()


def _positive_number(value: str, label: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise ControlProtocolError(f"{label} must be a number") from exc
    if number <= 0 or number == float("inf") or number != number:
        raise ControlProtocolError(f"{label} must be finite and positive")
    return number


def parse_worker_options(
    argv: list[str],
) -> tuple[WorkerOptions | None, list[str]]:
    enabled = False
    values: dict[str, str] = {}
    remaining: list[str] = []
    index = 0
    while index < len(argv):
        value = argv[index]
        if value == WORKER_OPTION:
            if enabled:
                raise ControlProtocolError("control-worker option was repeated")
            enabled = True
            index += 1
            continue
        if value in WORKER_VALUE_OPTIONS:
            if index + 1 >= len(argv):
                raise ControlProtocolError(f"{value} requires a value")
            if value in values:
                raise ControlProtocolError(f"{value} was provided more than once")
            values[value] = argv[index + 1]
            index += 2
            continue
        remaining.append(value)
        index += 1
    if not enabled and not values:
        return None, remaining
    if not enabled or set(values) != WORKER_VALUE_OPTIONS:
        raise ControlProtocolError(
            "control-worker mode and all worker limits are required together"
        )
    scope = values[WORKER_SCOPE_OPTION]
    if not SCOPE_RE.fullmatch(scope):
        raise ControlProtocolError("worker scope digest is invalid")
    max_items_number = _positive_number(
        values[WORKER_MAX_ITEMS_OPTION], "worker item cap"
    )
    if not max_items_number.is_integer() or max_items_number > 100:
        raise ControlProtocolError("worker item cap must be an integer from 1 to 100")
    max_age = _positive_number(values[WORKER_MAX_AGE_OPTION], "worker age cap")
    if max_age > 3_600:
        raise ControlProtocolError("worker age cap may not exceed one hour")
    return WorkerOptions(scope, int(max_items_number), max_age), remaining


def worker_options_as_args(options: WorkerOptions) -> list[str]:
    return [
        WORKER_OPTION,
        WORKER_SCOPE_OPTION,
        options.scope,
        WORKER_MAX_ITEMS_OPTION,
        str(options.max_items),
        WORKER_MAX_AGE_OPTION,
        str(options.max_age_seconds),
    ]


def _safe_error_name(exc: BaseException) -> str:
    name = exc.__class__.__name__
    return name if SAFE_ERROR_RE.fullmatch(name) else "WorkerError"


def _write_message(stream: TextIO, value: dict[str, Any]) -> None:
    stream.write(json.dumps(value, separators=(",", ":"), sort_keys=True))
    stream.write("\n")
    stream.flush()


def _validate_argv(value: Any) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or len(value) > MAX_ARGUMENTS
        or not all(isinstance(item, str) for item in value)
    ):
        raise ControlProtocolError("worker argument vector is invalid")
    total = sum(len(item.encode("utf-8")) for item in value)
    if total > MAX_ARGUMENT_BYTES or any("\x00" in item for item in value):
        raise ControlProtocolError("worker argument vector exceeds its bound")
    return list(value)


def _parse_command(line: str, options: WorkerOptions) -> dict[str, Any]:
    if len(line.encode("utf-8")) > MAX_ARGUMENT_BYTES + 8_192:
        raise ControlProtocolError("worker command exceeds its bound")
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ControlProtocolError("worker command is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ControlProtocolError("worker command must be an object")
    command_type = value.get("type")
    if command_type == "shutdown":
        if set(value) != {"type", "protocol", "scope"}:
            raise ControlProtocolError("worker shutdown fields are invalid")
        if value.get("protocol") != PROTOCOL_VERSION or value.get("scope") != options.scope:
            raise ControlProtocolError("worker shutdown scope is invalid")
        return value
    required = {"type", "protocol", "scope", "item_id", "lease_token", "argv"}
    if set(value) != required or command_type != "run":
        raise ControlProtocolError("worker run fields are invalid")
    if value.get("protocol") != PROTOCOL_VERSION or value.get("scope") != options.scope:
        raise ControlProtocolError("worker run scope is invalid")
    if not CONTROL_ID_RE.fullmatch(str(value.get("item_id") or "")):
        raise ControlProtocolError("worker item ID is invalid")
    if not CONTROL_ID_RE.fullmatch(str(value.get("lease_token") or "")):
        raise ControlProtocolError("worker lease token is invalid")
    value["argv"] = _validate_argv(value.get("argv"))
    return value


class AccountSessionPool:
    """Inject one Requests session into sequential Twitter extractors."""

    def __init__(self) -> None:
        self.session: Any = None
        self.extractors = 0
        self.reuses = 0
        self._original: Callable[..., Any] | None = None
        self._wrapper: Callable[..., Any] | None = None
        self._lock = threading.Lock()

    def __enter__(self) -> AccountSessionPool:
        original = Extractor.initialize
        pool = self

        def pooled_initialize(extractor: Extractor) -> Any:
            if getattr(extractor, "category", None) != "twitter":
                return original(extractor)
            with pool._lock:
                pool.extractors += 1
                if pool.session is not None:
                    extractor.session = pool.session
                    pool.reuses += 1
            result = original(extractor)
            with pool._lock:
                if pool.session is None:
                    pool.session = extractor.session
                elif extractor.session is not pool.session:
                    raise ControlProtocolError(
                        "Twitter extractor replaced the account session"
                    )
            return result

        self._original = original
        self._wrapper = pooled_initialize
        Extractor.initialize = pooled_initialize
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._original is not None and Extractor.initialize is self._wrapper:
            Extractor.initialize = self._original
        if self.session is not None:
            try:
                self.session.close()
            except Exception:
                pass


class GalleryItemState:
    """Restore process-global gallery-dl state after one control item."""

    def __init__(self) -> None:
        self.root = logging.getLogger()
        self.handlers = tuple(self.root.handlers)
        self.level = self.root.level
        self.ansi = gallery_dl.output.ANSI

    def reset(self) -> None:
        gallery_dl.config.clear()
        files = getattr(gallery_dl.config, "_files", None)
        if isinstance(files, list):
            files.clear()
        for handler in tuple(self.root.handlers):
            if handler not in self.handlers:
                self.root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        self.root.setLevel(self.level)
        gallery_dl.output.ANSI = self.ansi


def worker_loop(
    options: WorkerOptions,
    execute: Callable[..., int],
    *,
    input_stream: TextIO = sys.stdin,
    protocol_output: TextIO = sys.stdout,
    gallery_output: TextIO = sys.stderr,
    clock: Callable[[], float] = time.monotonic,
    session_pool_factory: Callable[[], AccountSessionPool] = AccountSessionPool,
) -> int:
    """Serve sequential commands; archive state remains parent-owned."""
    started = clock()
    completed = 0
    _write_message(
        protocol_output,
        {
            "type": "ready",
            "protocol": PROTOCOL_VERSION,
            "max_items": options.max_items,
            "max_age_ms": round(options.max_age_seconds * 1000),
        },
    )
    with session_pool_factory():
        for line in input_stream:
            try:
                command = _parse_command(line, options)
            except ControlProtocolError as exc:
                _write_message(
                    protocol_output,
                    {
                        "type": "protocol_error",
                        "protocol": PROTOCOL_VERSION,
                        "error_class": _safe_error_name(exc),
                    },
                )
                return 32
            if command["type"] == "shutdown":
                _write_message(
                    protocol_output,
                    {"type": "stopped", "protocol": PROTOCOL_VERSION},
                )
                return 0

            item_index = completed + 1
            runner_starts = 1 if item_index == 1 else 0
            _write_message(
                protocol_output,
                {
                    "type": "begin",
                    "protocol": PROTOCOL_VERSION,
                    "item_id": command["item_id"],
                    "lease_token": command["lease_token"],
                    "item_index": item_index,
                    "runner_starts": runner_starts,
                },
            )
            status = 1
            error_class = None
            forced_retire = None
            state = GalleryItemState()
            try:
                with contextlib.redirect_stdout(gallery_output), contextlib.redirect_stderr(
                    gallery_output
                ):
                    status = int(
                        execute(
                            command["argv"], runner_starts=runner_starts
                        )
                        or 0
                    )
            except KeyboardInterrupt:
                status = 130
                error_class = "KeyboardInterrupt"
                forced_retire = "interrupted"
            except SystemExit as exc:
                status = int(exc.code) if isinstance(exc.code, int) else 1
                error_class = "SystemExit"
                if status != 0:
                    forced_retire = "worker_error"
            except BaseException as exc:
                status = 1
                error_class = _safe_error_name(exc)
                forced_retire = "worker_error"
            finally:
                try:
                    gallery_output.flush()
                finally:
                    state.reset()
            # Start the barrier on a fresh line even if a renderer left a
            # carriage-return progress update or unterminated fragment.
            gallery_output.write(
                "\n"
                + OUTPUT_BARRIER_PREFIX
                + command["item_id"]
                + ":"
                + command["lease_token"]
                + "\n"
            )
            gallery_output.flush()

            completed += 1
            age_ms = max(0, round((clock() - started) * 1000))
            if forced_retire is not None:
                retire_reason = forced_retire
            elif completed >= options.max_items:
                retire_reason = "item_cap"
            elif age_ms >= round(options.max_age_seconds * 1000):
                retire_reason = "age_cap"
            else:
                retire_reason = None
            _write_message(
                protocol_output,
                {
                    "type": "result",
                    "protocol": PROTOCOL_VERSION,
                    "item_id": command["item_id"],
                    "lease_token": command["lease_token"],
                    "status": status,
                    "item_index": item_index,
                    "runner_starts": runner_starts,
                    "session_age_ms": age_ms,
                    "retire": retire_reason is not None,
                    "retire_reason": retire_reason,
                    "error_class": error_class,
                },
            )
            if retire_reason is not None:
                return status if retire_reason in {"interrupted", "worker_error"} else 0
    return 0


def _validate_ready(value: Any, options: WorkerOptions) -> None:
    if not isinstance(value, dict) or set(value) != {
        "type",
        "protocol",
        "max_items",
        "max_age_ms",
    }:
        raise ControlProtocolError("worker ready acknowledgement is invalid")
    if (
        value.get("type") != "ready"
        or value.get("protocol") != PROTOCOL_VERSION
        or value.get("max_items") != options.max_items
        or value.get("max_age_ms") != round(options.max_age_seconds * 1000)
    ):
        raise ControlProtocolError("worker ready limits do not match")


def _validate_begin(value: Any, item_id: str, lease_token: str) -> None:
    if not isinstance(value, dict) or set(value) != {
        "type",
        "protocol",
        "item_id",
        "lease_token",
        "item_index",
        "runner_starts",
    }:
        raise ControlProtocolError("worker begin acknowledgement is invalid")
    if (
        value.get("type") != "begin"
        or value.get("protocol") != PROTOCOL_VERSION
        or value.get("item_id") != item_id
        or value.get("lease_token") != lease_token
        or not isinstance(value.get("item_index"), int)
        or value["item_index"] < 1
        or value.get("runner_starts") not in {0, 1}
    ):
        raise ControlProtocolError("worker begin identity is invalid")


def _validate_result(value: Any, item_id: str, lease_token: str) -> WorkerResult:
    fields = {
        "type",
        "protocol",
        "item_id",
        "lease_token",
        "status",
        "item_index",
        "runner_starts",
        "session_age_ms",
        "retire",
        "retire_reason",
        "error_class",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ControlProtocolError("worker result acknowledgement is invalid")
    error_class = value.get("error_class")
    retire_reason = value.get("retire_reason")
    if (
        value.get("type") != "result"
        or value.get("protocol") != PROTOCOL_VERSION
        or value.get("item_id") != item_id
        or value.get("lease_token") != lease_token
        or not isinstance(value.get("status"), int)
        or not 0 <= value["status"] <= 255
        or not isinstance(value.get("item_index"), int)
        or value["item_index"] < 1
        or value.get("runner_starts") not in {0, 1}
        or not isinstance(value.get("session_age_ms"), int)
        or value["session_age_ms"] < 0
        or not isinstance(value.get("retire"), bool)
        or retire_reason not in {None, "item_cap", "age_cap", "interrupted", "worker_error"}
        or (value["retire"] != (retire_reason is not None))
        or (error_class is not None and not SAFE_ERROR_RE.fullmatch(str(error_class)))
    ):
        raise ControlProtocolError("worker result values are invalid")
    return WorkerResult(
        item_id=item_id,
        lease_token=lease_token,
        status=value["status"],
        item_index=value["item_index"],
        runner_starts=value["runner_starts"],
        session_age_ms=value["session_age_ms"],
        retire=value["retire"],
        retire_reason=retire_reason,
        error_class=error_class,
    )


class RunnerControlClient:
    """Parent-side owner of one bounded worker and one active item."""

    def __init__(
        self,
        command: list[str],
        options: WorkerOptions,
        *,
        startup_timeout: float = 15.0,
        shutdown_timeout: float = 5.0,
        popen: Callable[..., subprocess.Popen] = subprocess.Popen,
    ) -> None:
        if not command or not all(isinstance(value, str) and value for value in command):
            raise ControlProtocolError("worker launch command is invalid")
        self.command = list(command)
        self.options = options
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.popen = popen
        self.process: subprocess.Popen | None = None
        self._stderr_thread: threading.Thread | None = None
        self._output_callback: Callable[[str], None] | None = None
        self._callback_lock = threading.Lock()
        self._run_lock = threading.Lock()
        self._output_barrier = threading.Event()
        self._expected_output_barrier: str | None = None
        self.last_output_error_class: str | None = None
        self.starts = 0

    def _stderr_reader(self, process: subprocess.Popen) -> None:
        assert process.stderr is not None
        for line in process.stderr:
            with self._callback_lock:
                callback = self._output_callback
                expected = self._expected_output_barrier
            if expected is not None and line.rstrip("\n") == expected:
                self._output_barrier.set()
                continue
            if callback is not None:
                try:
                    callback(line)
                except BaseException as exc:
                    # Rendering or output capture is observability-only.  Keep
                    # draining stderr so the protocol barrier cannot deadlock.
                    with self._callback_lock:
                        self.last_output_error_class = _safe_error_name(exc)
                        if self._output_callback is callback:
                            self._output_callback = None

    @staticmethod
    def _read_json_line(stream: TextIO) -> dict[str, Any] | None:
        line = stream.readline()
        if not line:
            return None
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ControlProtocolError("worker emitted invalid JSON") from exc
        if not isinstance(value, dict):
            raise ControlProtocolError("worker acknowledgement is not an object")
        return value

    def _start(self) -> None:
        if self.process is not None:
            return
        process = self.popen(
            [*self.command, *worker_options_as_args(self.options)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self.process = process
        self.starts += 1
        assert process.stdout is not None
        ready, _, _ = select.select(
            [process.stdout], [], [], self.startup_timeout
        )
        if not ready:
            self._terminate()
            raise ControlProtocolError("runner worker did not become ready")
        value = self._read_json_line(process.stdout)
        try:
            _validate_ready(value, self.options)
        except BaseException:
            self._terminate()
            raise
        self._stderr_thread = threading.Thread(
            target=self._stderr_reader, args=(process,), daemon=True
        )
        self._stderr_thread.start()

    def run(
        self,
        *,
        item_id: str,
        lease_token: str,
        argv: list[str],
        output: Callable[[str], None] | None = None,
    ) -> WorkerResult:
        if not CONTROL_ID_RE.fullmatch(item_id) or not CONTROL_ID_RE.fullmatch(
            lease_token
        ):
            raise ControlProtocolError("worker item identity is invalid")
        arguments = _validate_argv(argv)
        with self._run_lock:
            self._start()
            process = self.process
            assert process is not None and process.stdin is not None
            assert process.stdout is not None
            with self._callback_lock:
                self._output_callback = output
                self.last_output_error_class = None
                self._expected_output_barrier = (
                    OUTPUT_BARRIER_PREFIX + item_id + ":" + lease_token
                )
                self._output_barrier.clear()
            began = False
            try:
                _write_message(
                    process.stdin,
                    {
                        "type": "run",
                        "protocol": PROTOCOL_VERSION,
                        "scope": self.options.scope,
                        "item_id": item_id,
                        "lease_token": lease_token,
                        "argv": arguments,
                    },
                )
                begin = self._read_json_line(process.stdout)
                if begin is None:
                    self._clear_dead_process()
                    raise RunnerWorkerLost(began=False)
                _validate_begin(begin, item_id, lease_token)
                began = True
                value = self._read_json_line(process.stdout)
                if value is None:
                    self._clear_dead_process()
                    raise RunnerWorkerLost(began=True)
                result = _validate_result(value, item_id, lease_token)
                if not self._output_barrier.wait(self.shutdown_timeout):
                    self._terminate()
                    raise ControlProtocolError(
                        "runner worker output boundary is incomplete"
                    )
            except KeyboardInterrupt:
                self.interrupt()
                raise
            except (BrokenPipeError, OSError):
                self._clear_dead_process()
                raise RunnerWorkerLost(began=began)
            finally:
                with self._callback_lock:
                    self._output_callback = None
                    self._expected_output_barrier = None
            if result.retire:
                self._wait_and_clear()
            return result

    def _wait_and_clear(self) -> None:
        process = self.process
        if process is None:
            return
        try:
            process.wait(timeout=self.shutdown_timeout)
        except subprocess.TimeoutExpired:
            self._terminate()
            raise ControlProtocolError("retiring runner worker did not exit")
        self._clear_dead_process()

    def _clear_dead_process(self) -> None:
        process = self.process
        self.process = None
        thread = self._stderr_thread
        self._stderr_thread = None
        if process is not None and process.poll() is None:
            try:
                process.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=self.shutdown_timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.2)
        if process is not None:
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass

    def _terminate(self) -> None:
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=self.shutdown_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        self._clear_dead_process()

    def interrupt(self) -> None:
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            try:
                process.send_signal(signal.SIGINT)
                process.wait(timeout=self.shutdown_timeout)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                self._terminate()
        self._clear_dead_process()

    def signal_interrupt(self) -> None:
        """Request current-item cancellation without consuming its result pipe."""
        process = self.process
        if process is None or process.poll() is not None:
            return
        try:
            process.send_signal(signal.SIGINT)
        except ProcessLookupError:
            pass

    def close(self) -> None:
        with self._run_lock:
            process = self.process
            if process is None:
                return
            if process.poll() is not None:
                self._clear_dead_process()
                return
            assert process.stdin is not None and process.stdout is not None
            try:
                _write_message(
                    process.stdin,
                    {
                        "type": "shutdown",
                        "protocol": PROTOCOL_VERSION,
                        "scope": self.options.scope,
                    },
                )
                ready, _, _ = select.select(
                    [process.stdout], [], [], self.shutdown_timeout
                )
                response = self._read_json_line(process.stdout) if ready else None
                if response != {"type": "stopped", "protocol": PROTOCOL_VERSION}:
                    raise ControlProtocolError(
                        "runner worker shutdown acknowledgement is invalid"
                    )
                self._wait_and_clear()
            except (BrokenPipeError, OSError, ControlProtocolError):
                self._terminate()
                raise

    def __enter__(self) -> RunnerControlClient:
        return self

    def __exit__(self, exc_type: object, *_exc: object) -> None:
        if exc_type is None:
            self.close()
            return
        try:
            self.close()
        except Exception:
            # Cleanup must not replace the exception raised by archive work.
            pass
