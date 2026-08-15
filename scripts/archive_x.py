#!/usr/bin/env python3
"""Conservative, resumable X archival wrapper around gallery-dl."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import importlib.metadata
import json
import mimetypes
import os
import random
import re
import secrets
import shlex
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import archive_x_descriptors as descriptor_x
import archive_x_request_telemetry as request_telemetry_x

try:
    import fcntl
except ImportError:  # pragma: no cover - this repo targets macOS/Linux
    fcntl = None


SCHEMA_NAME = "gdl-x-archive"
SCHEMA_VERSION = 1
MIN_GALLERY_DL = (1, 32, 0)
SNOWFLAKE_EPOCH = datetime(
    2010, 11, 4, 1, 42, 54, 657000, tzinfo=timezone.utc
)
HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}\Z")
DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?\Z")
CURSOR_RE = re.compile(r"Use '-o cursor=(.+)' to continue")
CHECKPOINT_CURSOR_RE = re.compile(r"Archive checkpoint cursor=(\S+)")
RATE_LIMIT_WAIT_RE = re.compile(
    r"\[twitter\]\[info\]\s+Waiting for .+\(rate limit\)\s*$"
)
DOWNLOAD_ERROR_RE = re.compile(
    r"\[download\]\[error\]\s+Failed to download\s+(.+?)\s*$"
)
HTTP_DOWNLOAD_WARNING_RE = re.compile(
    r"\[downloader\.http\]\[warning\]\s+'(\d{3})[^']*'\s+for\s+'[^']+'"
)
LOG_ERROR_RE = re.compile(r"\[[^\]]+\]\[error\]")
MEDIA_FILENAME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_(\d{5,25})_(\d+)_"
)
X_HOSTS = {
    "x.com",
    "www.x.com",
    "mobile.x.com",
    "twitter.com",
    "www.twitter.com",
    "mobile.twitter.com",
}
RESERVED_PATHS = {
    "compose",
    "explore",
    "hashtag",
    "home",
    "i",
    "intent",
    "messages",
    "notifications",
    "search",
    "settings",
    "share",
}
PROFILE_ENDPOINTS = (
    ("info", "info"),
    ("avatar", "photo"),
    ("background", "header_photo"),
)
EXIT_FLAGS = {
    1: "unexpected error",
    4: "extraction or download error",
    8: "challenge required",
    16: "authentication or authorization error",
    32: "input or configuration error",
    64: "unsupported URL",
    128: "operating-system error",
}
CHILD_INTERRUPT_GRACE_SECONDS = 15
CHILD_TERMINATE_GRACE_SECONDS = 10
MEDIA_UNAVAILABLE_HTTP_STATUSES = {404, 410}
MEDIA_UNAVAILABLE_MIN_ATTEMPTS = 2
MEDIA_UNAVAILABLE_MIN_AGE = timedelta(hours=24)
MEDIA_RETRY_MAX_ATTEMPTS = 3
MEDIA_RETRY_BASE_DELAY = timedelta(hours=6)
MEDIA_RETRY_MAX_DELAY = timedelta(days=7)


class ArchiveError(RuntimeError):
    """Expected user-facing archive failure."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def run_id(value: datetime | None = None) -> str:
    value = value or utc_now()
    token = os.urandom(3).hex()
    return value.strftime("%Y%m%dT%H%M%SZ") + "-" + token


def parse_datetime(value: str) -> datetime:
    raw = value.strip()
    if not raw:
        raise argparse.ArgumentTypeError("date cannot be empty")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid ISO-8601 date {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def effective_timeline_post_limit(args: argparse.Namespace) -> int | None:
    """Return either mutually exclusive modern acquisition bound."""
    return args.post_limit or getattr(args, "modern_max_posts", None)


def nonnegative_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def parse_duration(value: str) -> tuple[float, float]:
    match = DURATION_RE.fullmatch(value.strip())
    if not match:
        raise argparse.ArgumentTypeError(
            "duration must be SECONDS or MIN-MAX (for example 4-8)"
        )
    low = float(match.group(1))
    high = float(match.group(2) or match.group(1))
    if high < low:
        raise argparse.ArgumentTypeError("duration maximum is below minimum")
    return low, high


def duration_arg(value: str) -> str:
    parse_duration(value)
    return value


def sleep_random(duration: str, reason: str) -> float:
    low, high = parse_duration(duration)
    seconds = random.uniform(low, high)
    if seconds > 0:
        print(f"Waiting {seconds:.1f}s {reason}.")
        time.sleep(seconds)
    return seconds


def normalize_handle(spec: str) -> str:
    value = spec.strip()
    if not value:
        raise ValueError("empty user value")

    if value.startswith("@"):
        value = value[1:]

    if "://" not in value and re.match(
        r"(?:www\.|mobile\.)?(?:x|twitter)\.com/", value, re.I
    ):
        value = "https://" + value

    if "://" in value:
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower()
        if host not in X_HOSTS:
            raise ValueError(f"not an x.com/twitter.com URL: {spec!r}")
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError(f"X URL has no user handle: {spec!r}")
        value = parts[0]

    if value.lower() in RESERVED_PATHS:
        raise ValueError(f"X path is not a user profile: {spec!r}")
    if not HANDLE_RE.fullmatch(value):
        raise ValueError(f"invalid X user handle: {spec!r}")
    return value.lower()


def load_targets(users: list[str] | None, input_file: Path | None) -> list[str]:
    values: list[tuple[str, str]] = []
    if users:
        values.extend((value, "--user") for value in users)
    elif input_file:
        try:
            lines = input_file.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise ArchiveError(f"cannot read input file {input_file}: {exc}") from exc
        for number, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            values.append((stripped, f"{input_file}:{number}"))

    targets: list[str] = []
    seen: set[str] = set()
    errors: list[str] = []
    for value, source in values:
        try:
            handle = normalize_handle(value)
        except ValueError as exc:
            errors.append(f"{source}: {exc}")
            continue
        if handle not in seen:
            seen.add(handle)
            targets.append(handle)
    if errors:
        raise ArchiveError("invalid archive targets:\n  " + "\n  ".join(errors))
    if not targets:
        raise ArchiveError("no X users were found in the supplied input")
    return targets


def validate_cookie_file(path: Path) -> set[str]:
    try:
        file_stat = path.stat()
    except OSError as exc:
        raise ArchiveError(f"cannot read X cookie file {path}: {exc}") from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise ArchiveError(f"X cookie path is not a regular file: {path}")
    if os.name == "posix" and file_stat.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise ArchiveError(
            f"X cookie file permissions are too open ({stat.S_IMODE(file_stat.st_mode):03o}); "
            f"run: chmod 600 {shlex.quote(str(path))}"
        )

    present_names: set[str] = set()
    usable_names: set[str] = set()
    now = time.time()
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise ArchiveError(f"cannot read X cookie file {path}: {exc}") from exc
    for line in lines:
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_") :]
        elif not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        domain, _include, _cookie_path, _secure, expiry, name, value = fields[:7]
        domain = domain.lstrip(".").lower()
        # gallery-dl 1.32.x sends requests to x.com and looks up cookies for
        # its exact .x.com cookie domain.  A twitter.com-only export can look
        # plausible here while leaving the extractor unauthenticated.
        if domain != "x.com" or not value:
            continue
        present_names.add(name)
        expired = False
        try:
            expires_at = int(expiry)
            if expires_at > 10_000_000_000:
                expires_at //= 1000
            expired = bool(expires_at and expires_at < now)
        except ValueError:
            pass
        if not expired:
            usable_names.add(name)

    for required in ("auth_token", "ct0"):
        if required in usable_names:
            continue
        if required in present_names:
            raise ArchiveError(f"the X {required} cookie in {path} is expired")
        raise ArchiveError(
            f"{path} does not contain a usable .x.com {required} cookie"
        )
    return usable_names


def exact_mount_is_writable(path: Path) -> bool:
    if not path.is_dir() or not os.access(path, os.W_OK | os.X_OK):
        return False
    if sys.platform == "darwin":
        return os.path.ismount(path)
    try:
        proc = subprocess.run(
            ["findmnt", "-rn", "--target", str(path), "-o", "TARGET"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except FileNotFoundError:
        return os.path.ismount(path)
    targets = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    return str(path) in targets


def resolve_output_root(explicit: Path | None, *, plan_only: bool = False) -> Path:
    if explicit is not None:
        return explicit.expanduser().resolve()

    env_root = os.environ.get("GDL_X_ARCHIVE_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    candidates = (
        Path("/mnt/Bibliotheque"),
        Path("/tmp/Bibliotheque"),
        Path("/Volumes/Bibliotheque"),
    )
    for mount in candidates:
        if exact_mount_is_writable(mount):
            return mount / "gdl" / "x-archive"
    if plan_only:
        # A dry run promises no writes.  Show the intended stable destination
        # even when the disk is not presently mounted; the real run still
        # performs the fail-closed check above.
        return candidates[0] / "gdl" / "x-archive"
    raise ArchiveError(
        "Bibliotheque is not mounted read-write at /mnt/Bibliotheque, "
        "/tmp/Bibliotheque, or /Volumes/Bibliotheque. Mount it first or "
        "pass --output-root explicitly."
    )


def filesystem_is_read_only(path: Path) -> bool:
    """Inspect the containing filesystem without probing it with a write."""
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    return bool(os.statvfs(probe).f_flag & os.ST_RDONLY)


def gallery_dl_version() -> str:
    try:
        version = importlib.metadata.version("gallery-dl")
    except importlib.metadata.PackageNotFoundError as exc:
        raise ArchiveError(
            "gallery-dl is not installed; run this through scripts/archive-x"
        ) from exc
    numeric = tuple(int(part) for part in re.findall(r"\d+", version)[:3])
    if numeric < MIN_GALLERY_DL:
        minimum = ".".join(map(str, MIN_GALLERY_DL))
        raise ArchiveError(f"gallery-dl {minimum}+ is required; found {version}")
    return version


def verify_gallery_dl_x_runner(repo_dir: Path, version: str) -> None:
    """Fail before archive writes if the pinned X shim is incompatible."""
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_runner.py"),
        "--version",
    ]
    try:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ArchiveError(
            f"could not verify the gallery-dl X runner: {exc}"
        ) from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ArchiveError(
            "gallery-dl X runner compatibility check failed"
            + (f": {detail}" if detail else "")
        )
    reported = result.stdout.strip()
    if reported != version:
        raise ArchiveError(
            "gallery-dl X runner reported an unexpected version: "
            f"expected {version}, found {reported or 'no output'}"
        )


def x_scheduler_options(
    user_dir: Path,
    account_id: str,
    request_delay: str,
) -> Any:
    """Build the private actual-request lane only after numeric identity proof."""
    pacing = importlib.import_module("archive_x_pacing")
    if not account_id.isdecimal() or int(account_id) < 1:
        raise ArchiveError("X scheduler account identity is invalid")
    low, high = pacing.parse_delay(request_delay)
    low = max(pacing.DEFAULT_DELAY_LOW_SECONDS, low)
    high = max(low, high)
    return pacing.SchedulerOptions(
        database=user_dir / "_state" / "context.sqlite3",
        scope_id=account_id,
        delay_low=low,
        delay_high=high,
        lease_seconds=pacing.DEFAULT_REQUEST_LEASE_SECONDS,
        backoff_429_seconds=pacing.DEFAULT_429_BACKOFF_SECONDS,
    )


def existing_x_scheduler_options(
    user_dir: Path,
    state: dict[str, Any],
    request_delay: str,
) -> Any | None:
    """Read an existing v3 identity lane without migrating before proof."""
    database = user_dir / "_state" / "context.sqlite3"
    if not database.is_file():
        return None
    pacing = importlib.import_module("archive_x_pacing")
    expected = str(state.get("requested_user_id") or "")
    try:
        connection = sqlite3.connect(
            database.resolve().as_uri() + "?mode=ro", uri=True
        )
        try:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if not {"archive_account", "pacing"} <= tables:
                return None
            columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(pacing)")
            }
            if not pacing.DurableRequestScheduler.REQUIRED_COLUMNS <= columns:
                return None
            account = connection.execute(
                "SELECT user_id FROM archive_account WHERE singleton=1"
            ).fetchone()
            pacing_row = connection.execute(
                "SELECT COUNT(*) FROM pacing WHERE singleton=1"
            ).fetchone()
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        raise ArchiveError(
            "existing X scheduler state could not be validated"
        ) from exc
    if account is None or not str(account[0]).isdecimal():
        raise ArchiveError("existing X scheduler account identity is invalid")
    account_id = str(account[0])
    if not expected.isdecimal() or expected != account_id:
        raise ArchiveError(
            "existing X scheduler identity does not match archive state"
        )
    if pacing_row is None or int(pacing_row[0]) != 1:
        raise ArchiveError("existing X scheduler pacing row is invalid")
    return x_scheduler_options(user_dir, account_id, request_delay)


def bridge_identity_probe_boundary(
    options: Any,
    *,
    probe_completed_at: float,
) -> None:
    """Persist the one pre-scheduler probe gap after identity is proven."""
    if probe_completed_at < 0:
        raise ArchiveError("identity-probe pacing timestamp is invalid")
    boundary = probe_completed_at + random.uniform(
        float(options.delay_low), float(options.delay_high)
    )
    try:
        connection = sqlite3.connect(options.database, timeout=5.0)
        try:
            connection.execute("BEGIN IMMEDIATE")
            account = connection.execute(
                "SELECT user_id FROM archive_account WHERE singleton=1"
            ).fetchone()
            row = connection.execute(
                "SELECT next_request_at FROM pacing WHERE singleton=1"
            ).fetchone()
            if account is None or str(account[0]) != options.scope_id:
                raise ArchiveError(
                    "identity-probe pacing account does not match archive state"
                )
            if row is None:
                raise ArchiveError("identity-probe pacing row is missing")
            if boundary > float(row[0] or 0):
                connection.execute(
                    """UPDATE pacing SET next_request_at=?,
                              not_before_reason='spacing',updated_at=?
                         WHERE singleton=1""",
                    (boundary, iso_utc(utc_now())),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
    except ArchiveError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise ArchiveError(
            "identity-probe pacing boundary could not be persisted"
        ) from exc


def x_runner_control_client(
    repo_dir: Path,
    account_id: str,
    *,
    legacy: bool = False,
) -> Any:
    """Return one lazy bounded same-account runner; callers own its lifetime."""
    control = importlib.import_module("archive_x_runner_control")
    runner_name = (
        "gallery_dl_x_legacy_runner.py" if legacy else "gallery_dl_x_runner.py"
    )
    return control.RunnerControlClient(
        [sys.executable, str(repo_dir / "scripts" / runner_name)],
        control.WorkerOptions(control.account_scope_digest(account_id)),
    )


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def atomic_write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    count = 0
    with temporary.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            file.write("\n")
            count += 1
        file.flush()
        os.fsync(file.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    return count


def load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return default


def repair_resume_cursor(
    state_path: Path,
    *,
    expected_cursor: str,
    replacement_cursor: str,
    source_run_id: str,
    repaired_at: str,
) -> dict[str, Any]:
    """Atomically apply an operator-approved cursor repair with a stale guard."""
    state = load_json(state_path, {})
    resume = state.get("resume") if isinstance(state, dict) else None
    current = resume.get("cursor") if isinstance(resume, dict) else None
    if current != expected_cursor:
        raise ArchiveError(
            "resume cursor changed before repair: "
            f"expected {expected_cursor}, found {current or 'none'}"
        )
    repaired = dict(resume)
    repaired.update(
        {
            "cursor": replacement_cursor,
            "saved_at": repaired_at,
            "source_run_id": source_run_id,
            "operator_repaired_from_cursor": expected_cursor,
        }
    )
    state["resume"] = repaired
    atomic_write_json(state_path, state)
    return repaired


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    try:
        file = path.open("r", encoding="utf-8")
    except OSError:
        return
    with file:
        for line in file:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict):
                yield record


def jsonl_has_record(path: Path) -> bool:
    return next(iter_jsonl(path), None) is not None


def oldest_tweet_id(path: Path) -> str | None:
    oldest: int | None = None
    for record in iter_jsonl(path):
        value = id_string(record.get("tweet_id"))
        if not value:
            continue
        try:
            number = int(value)
        except ValueError:
            continue
        if number > 0 and (oldest is None or number < oldest):
            oldest = number
    return str(oldest) if oldest is not None else None


def synthetic_search_cursor(path: Path) -> str | None:
    tweet_id = oldest_tweet_id(path)
    return f"3_{tweet_id}/" if tweet_id else None


@contextmanager
def exclusive_lock(path: Path) -> Iterator[None]:
    if fcntl is None:
        raise ArchiveError("archive locking is unavailable on this platform")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as lock_file:
        os.chmod(path, 0o600)
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ArchiveError(
                f"another X archive process already holds {path}"
            ) from exc
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"pid={os.getpid()} started={iso_utc(utc_now())}\n")
        lock_file.flush()
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def id_string(value: Any) -> str | None:
    if value is None or value is False or value == 0 or value == "0":
        return None
    return str(value)


def same_user(
    author: dict[str, Any], user: dict[str, Any], requested_handle: str
) -> bool:
    """Compare stable IDs first, with handles only as a legacy fallback."""
    author_id = id_string(author.get("id"))
    user_id = id_string(user.get("id"))
    if author_id and user_id:
        return author_id == user_id
    author_handle = str(author.get("name") or "").lower()
    user_handle = str(user.get("name") or requested_handle).lower()
    return bool(author_handle) and author_handle == user_handle


def relation_for(metadata: dict[str, Any], requested_handle: str) -> str:
    subcategory = str(metadata.get("subcategory") or "")
    if subcategory == "avatar":
        return "profile_avatar"
    if subcategory == "background":
        return "profile_background"
    author = metadata.get("author") or {}
    user = metadata.get("user") or {}
    if id_string(metadata.get("retweet_id")):
        return "repost"
    if same_user(author, user, requested_handle):
        if id_string(metadata.get("reply_id")):
            return "reply"
        return "post"
    if id_string(metadata.get("quote_id")):
        return "quoted_source"
    return "context"


def normalize_post(
    metadata: dict[str, Any], requested_handle: str, endpoint: str
) -> dict[str, Any] | None:
    post_id = id_string(metadata.get("tweet_id"))
    if not post_id:
        return None
    author = metadata.get("author") or {}
    user = metadata.get("user") or {}
    author_handle = str(author.get("name") or "")
    relationship = relation_for(metadata, requested_handle)
    archived_at = str(metadata.get("archived_at") or iso_utc(utc_now()))
    repost_of_post_id = id_string(metadata.get("retweet_id"))
    requested_user_handle = str(user.get("name") or requested_handle)
    source_handle = (
        requested_user_handle if relationship == "repost" else author_handle
    )
    event_at = metadata.get("date")
    original_posted_at = (
        metadata.get("date_original")
        if relationship == "repost"
        else event_at
    )
    return {
        "schema": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "requested_handle": requested_handle,
        "requested_user_id": id_string(user.get("id")),
        "canonical_requested_handle": requested_user_handle or None,
        "post_id": post_id,
        "source_url": (
            f"https://x.com/{source_handle}/status/{post_id}"
            if source_handle
            else None
        ),
        "reposted_source_url": (
            f"https://x.com/{author_handle}/status/{repost_of_post_id}"
            if relationship == "repost" and author_handle and repost_of_post_id
            else None
        ),
        "relationship": relationship,
        "is_authored_by_requested_user": relationship in {"post", "reply"},
        "author_handle": author_handle or None,
        "author_id": id_string(author.get("id")),
        "author_display_name": author.get("nick"),
        # `posted_at` is the target account's timeline event time.  It equals
        # the post time normally and the repost action time for repost rows.
        "posted_at": event_at,
        "original_posted_at": original_posted_at,
        "reposted_at": event_at if relationship == "repost" else None,
        "first_captured_at": archived_at,
        "last_captured_at": archived_at,
        "capture_count": 1,
        "source_endpoints": [endpoint],
        "text": metadata.get("content"),
        "language": metadata.get("lang"),
        "reply_to_handle": metadata.get("reply_to"),
        "reply_to_post_id": id_string(metadata.get("reply_id")),
        "conversation_id": id_string(metadata.get("conversation_id")),
        "repost_of_post_id": repost_of_post_id,
        "hashtags": metadata.get("hashtags") or [],
        "mentions": metadata.get("mentions") or [],
        "sensitive": metadata.get("sensitive"),
        "metrics": {
            "likes": metadata.get("favorite_count"),
            "views": metadata.get("view_count"),
            "reposts": metadata.get("retweet_count"),
            "quotes": metadata.get("quote_count"),
            "replies": metadata.get("reply_count"),
            "bookmarks": metadata.get("bookmark_count"),
        },
        "gallery_dl": metadata,
    }


def record_richness(record: dict[str, Any]) -> tuple[int, int]:
    metadata = record.get("gallery_dl") or {}
    present = sum(value not in (None, "", [], {}) for value in metadata.values())
    return present, len(str(record.get("text") or ""))


def merge_post_records(
    existing: dict[str, Any] | None, new: dict[str, Any]
) -> dict[str, Any]:
    if not existing:
        return new

    # The newest crawl owns observation-time values (especially metrics), but
    # a temporarily sparse API response must not erase richer static metadata
    # captured earlier.  Merge nested raw dictionaries with new values taking
    # precedence instead of selecting one whole observation by "richness".
    def merge_dicts(old: dict[str, Any], latest: dict[str, Any]) -> dict[str, Any]:
        merged = old.copy()
        for key, value in latest.items():
            previous = merged.get(key)
            if isinstance(previous, dict) and isinstance(value, dict):
                merged[key] = merge_dicts(previous, value)
            else:
                merged[key] = value
        return merged

    chosen = merge_dicts(existing, new)
    chosen["metrics"] = new.get("metrics")
    chosen["first_captured_at"] = existing.get(
        "first_captured_at", new["first_captured_at"]
    )
    chosen["last_captured_at"] = new["last_captured_at"]
    chosen["capture_count"] = int(existing.get("capture_count") or 1) + 1
    chosen["source_endpoints"] = sorted(
        set(existing.get("source_endpoints") or ())
        | set(new.get("source_endpoints") or ())
    )
    return chosen


def post_sort_key(record: dict[str, Any]) -> tuple[str, int | str]:
    post_id = record.get("post_id") or ""
    try:
        numeric_id: int | str = int(post_id)
    except (TypeError, ValueError):
        numeric_id = str(post_id)
    return str(record.get("posted_at") or ""), numeric_id


def update_post_dataset(
    user_dir: Path, requested_handle: str, raw_path: Path, endpoint: str
) -> dict[str, int]:
    dataset_dir = user_dir / "dataset"
    posts_path = dataset_dir / "posts.jsonl"
    existing_by_id = {
        str(record["post_id"]): record
        for record in iter_jsonl(posts_path)
        if record.get("post_id")
    }

    run_by_id: dict[str, dict[str, Any]] = {}
    raw_count = 0
    for metadata in iter_jsonl(raw_path):
        raw_count += 1
        record = normalize_post(metadata, requested_handle, endpoint)
        if not record:
            continue
        post_id = record["post_id"]
        current = run_by_id.get(post_id)
        if current:
            endpoints = sorted(
                set(current["source_endpoints"]) | set(record["source_endpoints"])
            )
            if record_richness(record) > record_richness(current):
                current = record
            current["source_endpoints"] = endpoints
            run_by_id[post_id] = current
        else:
            run_by_id[post_id] = record

    for post_id, record in run_by_id.items():
        existing_by_id[post_id] = merge_post_records(
            existing_by_id.get(post_id), record
        )

    records = sorted(existing_by_id.values(), key=post_sort_key)
    all_count = atomic_write_jsonl(posts_path, records)
    authored_count = atomic_write_jsonl(
        dataset_dir / "authored-posts.jsonl",
        (
            record
            for record in records
            if record.get("is_authored_by_requested_user")
        ),
    )
    repost_count = atomic_write_jsonl(
        dataset_dir / "reposts.jsonl",
        (record for record in records if record.get("relationship") == "repost"),
    )
    return {
        "raw_records": raw_count,
        "new_run_posts": len(run_by_id),
        "dataset_posts": all_count,
        "authored_posts": authored_count,
        "reposts": repost_count,
    }


def update_profile_dataset(
    user_dir: Path, requested_handle: str, raw_path: Path, captured_at: str
) -> bool:
    profile = None
    for record in iter_jsonl(raw_path):
        profile = record
    if not profile:
        return False
    value = {
        "schema": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "requested_handle": requested_handle,
        "captured_at": captured_at,
        "profile": profile,
    }
    atomic_write_json(user_dir / "dataset" / "profile.json", value)
    return True


def profile_identity(raw_path: Path) -> tuple[str | None, str | None]:
    """Return the stable numeric ID and current handle from an info snapshot."""
    profile: dict[str, Any] | None = None
    for record in iter_jsonl(raw_path):
        profile = record
    if not profile:
        return None, None
    # The /info extractor emits the transformed user directly.  Accept a
    # nested `user` too so this remains usable with raw timeline fixtures.
    candidate = profile.get("user")
    if not isinstance(candidate, dict) or not candidate.get("id"):
        candidate = profile
    return id_string(candidate.get("id")), (
        str(candidate.get("name")) if candidate.get("name") else None
    )


def bind_profile_identity(
    state: dict[str, Any], requested_handle: str, observed_id: str, canonical_handle: str | None
) -> None:
    """Bind a handle archive to one X account, aborting on reassignment."""
    expected_id = id_string(state.get("requested_user_id"))
    if expected_id and expected_id != observed_id:
        raise ArchiveError(
            f"identity mismatch for @{requested_handle}: this archive is bound "
            f"to X user ID {expected_id}, but the handle now resolves to "
            f"{observed_id}; no timeline data was downloaded"
        )
    state.update(
        {
            "schema": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "requested_handle": requested_handle,
            "requested_user_id": observed_id,
            "canonical_handle": canonical_handle or requested_handle,
            "identity_checked_at": iso_utc(utc_now()),
        }
    )


def update_media_dataset(user_dir: Path, requested_handle: str) -> dict[str, int]:
    media_root = user_dir / "media"
    records: list[dict[str, Any]] = []
    total_bytes = 0
    if media_root.is_dir():
        for sidecar in sorted(media_root.rglob("*.json")):
            asset = Path(str(sidecar)[:-5])
            if not asset.is_file():
                continue
            metadata = load_json(sidecar, {})
            if not isinstance(metadata, dict):
                continue
            size = asset.stat().st_size
            total_bytes += size
            relation = relation_for(metadata, requested_handle)
            records.append(
                {
                    "schema": SCHEMA_NAME,
                    "schema_version": SCHEMA_VERSION,
                    "requested_handle": requested_handle,
                    "post_id": id_string(metadata.get("tweet_id")),
                    "relationship": relation,
                    "author_handle": (metadata.get("author") or {}).get("name"),
                    "posted_at": metadata.get("date"),
                    "original_posted_at": (
                        metadata.get("date_original")
                        if relation == "repost"
                        else metadata.get("date")
                    ),
                    "reposted_at": (
                        metadata.get("date") if relation == "repost" else None
                    ),
                    "media_number": metadata.get("num"),
                    "asset_path": str(asset.relative_to(user_dir)),
                    "sidecar_path": str(sidecar.relative_to(user_dir)),
                    "media_type": metadata.get("type"),
                    "mime_type": mimetypes.guess_type(asset.name)[0],
                    "bytes": size,
                    "sha256": metadata.get("sha256"),
                    "alt_text": metadata.get("description"),
                    "width": metadata.get("width"),
                    "height": metadata.get("height"),
                    "duration_seconds": metadata.get("duration"),
                    "source_url": metadata.get("media_url"),
                    "gallery_dl": metadata,
                }
            )
    records.sort(
        key=lambda record: (
            str(record.get("posted_at") or ""),
            str(record.get("post_id") or ""),
            int(record.get("media_number") or 0),
            record["asset_path"],
        )
    )
    count = atomic_write_jsonl(user_dir / "dataset" / "media.jsonl", records)
    return {"media_files": count, "media_bytes": total_bytes}


def write_dataset_readme(user_dir: Path) -> None:
    path = user_dir / "dataset" / "README.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    text = """# X archive dataset

This directory is a derived, portable view of immutable run snapshots.

- `posts.jsonl`: all retained posts and reposts, with explicit `relationship`.
- `authored-posts.jsonl`: only posts/replies authored by the requested user.
- `reposts.jsonl`: reposts, retaining the original author.
- `media.jsonl`: local asset paths, source metadata, and SHA-256 digests.
- `profile.json`: latest captured profile metadata.
- `context-posts.jsonl`: captured reply ancestors from the unified lifecycle.
- `reply-edges.jsonl`: child-to-parent graph and boundary states.
- `context-status.json`: queue, closure, pacing, and media summary.

`posted_at` is the target account's timeline-event timestamp. For a repost,
`reposted_at` is that event time and `original_posted_at` is the original
author's post time. `first_captured_at` and `last_captured_at` describe archive
observations. Engagement metrics are point-in-time values, not historical
totals. Raw per-run JSONL and logs live under `../runs/` and remain the source
of truth.

Reply context is automatically processed but remains ancestor-only. A bounded
first conversation response may settle several already-queued targets and their
verified parent paths, but unrelated siblings, descendants, further
conversation pagination, and quoted sources are excluded. Context state is
durable in `../_state/context.sqlite3`; the unified command rebuilds these
views, while `scripts/archive-x-context` remains available for advanced
maintenance.

Pre-Snowflake history is automatically initialized only after strict boundary
proof, then resumed through bounded internal UTC windows. Its frontier means
repeat-confirmed contiguous windows visible through X search; it is not proof
that deleted, private, withheld, or unindexed posts were recovered. Legacy
metadata may advance while its media remains in the shared pending-media queue.
Transient media receives a durable retry time. Repeated refreshed 404/410
responses, or three distinct failed archive-run attempts for another persistent
media error, become explicit unavailable evidence. An otherwise complete
archive reports `complete_with_unavailable_media` without retrying those assets
forever.
"""
    path.write_text(text, encoding="utf-8")
    os.chmod(path, 0o600)


def endpoint_url(handle: str, endpoint: str) -> str:
    if endpoint == "timeline":
        return f"https://x.com/{handle}/timeline"
    for name, path in PROFILE_ENDPOINTS:
        if endpoint == name:
            return f"https://x.com/{handle}/{path}"
    raise ValueError(endpoint)


def build_gallery_config(
    *,
    handle: str,
    endpoint: str,
    archive_root: Path,
    user_dir: Path,
    raw_partial: Path,
    cookie_file: Path,
    archive_run_id: str,
    archived_at: str,
    request_delay: str,
    download_delay: str,
    extractor_delay: str,
    include_reposts: bool,
    checksums: bool,
    cursor: str | None,
    download_media: bool = True,
    descriptor_artifact: Path | None = None,
    descriptor_operation_id: str | None = None,
    descriptor_source_kind: str | None = None,
    descriptor_source_operation: str | None = None,
    descriptor_owner_kind: str = "post",
) -> dict[str, Any]:
    postprocessors: list[dict[str, Any]] = []
    descriptor_values = (
        descriptor_artifact,
        descriptor_operation_id,
        descriptor_source_kind,
        descriptor_source_operation,
    )
    if any(value is not None for value in descriptor_values):
        if not all(value is not None for value in descriptor_values):
            raise ArchiveError("descriptor capture configuration is incomplete")
        postprocessors.append(
            descriptor_x.postprocessor_config(
                artifact_path=descriptor_artifact,
                archive_root=archive_root,
                operation_id=descriptor_operation_id,
                run_id=archive_run_id,
                source_kind=descriptor_source_kind,
                source_operation=descriptor_source_operation,
                owner_kind=descriptor_owner_kind,
            )
        )
    if download_media:
        if checksums:
            postprocessors.append(
                {"name": "hash", "mode": "sha256", "event": "file"}
            )
        postprocessors.append(
            {
                "name": "metadata",
                "event": "file",
                "mtime": True,
                "sort": True,
            }
        )
    postprocessors.append(
        {
            "name": "metadata",
            "mode": "jsonl",
            "event": "post",
            "base-directory": str(raw_partial.parent),
            "filename": raw_partial.name,
            "exclude": ["local_path", "media_url"],
            "sort": True,
        }
    )

    relation_filter = "author.get('id') == user.get('id')"
    if include_reposts:
        relation_filter += " or retweet_id"

    twitter: dict[str, Any] = {
        "cookies": str(cookie_file),
        "cookies-update": True,
        "archive": str(user_dir / "_state" / "downloads.sqlite3"),
        "archive-table": "media",
        "directory": [
            "users",
            handle,
            "media",
            "{date:%Y}",
            "{date:%m}",
        ],
        "filename": (
            "{date:%Y-%m-%dT%H-%M-%S}_{tweet_id}_{num}_"
            "{author[name]}.{extension}"
        ),
        "metadata-url": "media_url",
        "metadata-path": "local_path",
        "metadata-version": "gallery_dl",
        "keywords": {
            "archive_schema": SCHEMA_NAME,
            "archive_schema_version": SCHEMA_VERSION,
            "archive_run_id": archive_run_id,
            "archived_at": archived_at,
            "requested_handle": handle,
        },
        "text-tweets": True,
        "replies": True,
        "retweets": True if include_reposts else False,
        "quoted": False,
        # An old pinned post can appear first and make gallery-dl's generic
        # --date-after predicate stop an incremental crawl with exit code 0.
        "pinned": False,
        "expand": False,
        "showreplies": False,
        "cards": True,
        # Recovery delegates video selection to yt-dlp so it can choose among
        # the post's current variants instead of repeating gallery-dl's single
        # highest-bitrate CDN URL.
        "videos": "ytdl" if endpoint.startswith("retry-media-") else True,
        "previews": False,
        "articles": ["metadata", "html", "cover", "media"],
        "metadata-user": False,
        "unique": True,
        "transform": True,
        "ads": False,
        "ratelimit": "wait",
        "locked": "abort",
        "logout": False,
        "retries": 1,
        "retries-api": 1,
        "sleep-request": request_delay,
        "sleep": download_delay,
        "sleep-extractor": extractor_delay,
        "size": ["orig", "4096x4096", "large", "medium", "small"],
        "postprocessors": postprocessors,
        "avatar": {
            "directory": ["users", handle, "media", "profile"],
            "filename": (
                "profile-avatar_{date:%Y-%m-%dT%H-%M-%S}_"
                "{user[name]}.{extension}"
            ),
        },
        "background": {
            "directory": ["users", handle, "media", "profile"],
            "filename": (
                "profile-background_{date:%Y-%m-%dT%H-%M-%S}_"
                "{user[name]}.{extension}"
            ),
        },
        "timeline": {
            "strategy": "with_replies",
            "post-filter": relation_filter,
        },
    }
    if endpoint == "timeline" and cursor:
        twitter["timeline"]["cursor"] = cursor

    return {
        "extractor": {
            "base-directory": str(archive_root),
            "twitter": twitter,
        }
    }


def decode_exit_status(status: int) -> list[str]:
    if status == 0:
        return []
    if status < 0:
        return [f"terminated by signal {-status}"]
    descriptions = [text for bit, text in EXIT_FLAGS.items() if status & bit]
    return descriptions or [f"exit status {status}"]


def download_failure_from_line(line: str) -> dict[str, Any] | None:
    match = DOWNLOAD_ERROR_RE.search(line)
    if not match:
        return None
    filename = Path(match.group(1).strip()).name
    media_match = MEDIA_FILENAME_RE.match(filename)
    if not media_match:
        return {"filename": filename, "post_id": None, "media_number": None}
    return {
        "filename": filename,
        "post_id": media_match.group(1),
        "media_number": int(media_match.group(2)),
    }


def analyze_gallery_log(path: Path) -> tuple[list[dict[str, Any]], int]:
    failed_downloads: list[dict[str, Any]] = []
    other_error_count = 0
    pending_http_statuses: list[int] = []
    try:
        lines = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return failed_downloads, other_error_count
    with lines:
        for line in lines:
            if line.startswith("command: "):
                continue
            if match := HTTP_DOWNLOAD_WARNING_RE.search(line):
                pending_http_statuses.append(int(match.group(1)))
            failure = download_failure_from_line(line)
            if failure:
                if pending_http_statuses:
                    failure["http_statuses"] = sorted(set(pending_http_statuses))
                    failure["http_error_count"] = len(pending_http_statuses)
                failed_downloads.append(failure)
                pending_http_statuses = []
            elif LOG_ERROR_RE.search(line):
                other_error_count += 1
    return failed_downloads, other_error_count


def gallery_metadata_complete(
    status: int,
    resume_cursor: str | None,
    interrupted: bool,
    failed_downloads: list[dict[str, Any]],
    other_error_count: int,
) -> bool:
    """Whether extraction completed even if one or more assets failed."""
    if interrupted:
        return False
    if status == 0:
        return True
    return bool(
        status == 4
        and failed_downloads
        and all(
            id_string(failure.get("post_id"))
            and isinstance(failure.get("media_number"), int)
            and failure["media_number"] > 0
            for failure in failed_downloads
        )
        and not other_error_count
        and not resume_cursor
    )


class RateLimitProgressWatchdog:
    """Stop an endpoint after repeated quota windows without raw progress."""

    def __init__(self, progress_path: Path | None, limit: int):
        self.progress_path = progress_path
        self.limit = limit
        self.last_size = self._size()
        self.consecutive_stalls = 0

    def _size(self) -> int:
        if self.progress_path is None:
            return 0
        try:
            return self.progress_path.stat().st_size
        except OSError:
            return 0

    def observe(self, line: str) -> bool:
        if not self.limit or not RATE_LIMIT_WAIT_RE.search(line):
            return False
        size = self._size()
        if size > self.last_size:
            self.consecutive_stalls = 0
        else:
            self.consecutive_stalls += 1
        self.last_size = size
        return self.consecutive_stalls >= self.limit


def run_gallery_dl(
    command: list[str],
    log_path: Path,
    prefix: str,
    *,
    progress_path: Path | None = None,
    stalled_rate_limit_cycles: int = 0,
    runner: Any | None = None,
    control_lease_token: str | None = None,
    checkpoint_callback: Callable[[str], None] | None = None,
    planned_path_output: bool = False,
) -> tuple[
    int,
    str | None,
    float,
    bool,
    list[dict[str, Any]],
    int,
    bool,
    int,
]:
    started = time.monotonic()
    resume_cursor = None
    checkpoint_cursor = None
    interrupted = False
    failed_downloads: list[dict[str, Any]] = []
    other_error_count = 0
    stalled = False
    watchdog = RateLimitProgressWatchdog(
        progress_path, stalled_rate_limit_cycles
    )

    def display(line: str) -> None:
        stripped = line.lstrip()
        if planned_path_output and (
            stripped.startswith("/") or stripped.startswith("# /")
        ):
            indentation = line[: len(line) - len(stripped)]
            line = f"{indentation}planned output: {stripped}"
        print(f"[{prefix}] {line}", end="")

    def observe(line: str) -> None:
        nonlocal resume_cursor, checkpoint_cursor, other_error_count
        if match := CURSOR_RE.search(line):
            resume_cursor = match.group(1).strip()
        elif match := CHECKPOINT_CURSOR_RE.search(line):
            checkpoint_cursor = match.group(1).strip()
            if checkpoint_callback is not None:
                checkpoint_callback(checkpoint_cursor)
        failure = download_failure_from_line(line)
        if failure:
            failed_downloads.append(failure)
        elif LOG_ERROR_RE.search(line):
            other_error_count += 1

    def stop_child(*, interrupt_already_sent: bool) -> tuple[str, int]:
        """Drain a child after SIGINT, escalating so shutdown is bounded."""
        if not interrupt_already_sent:
            try:
                process.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass
        try:
            remainder, _ = process.communicate(
                timeout=CHILD_INTERRUPT_GRACE_SECONDS
            )
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                remainder, _ = process.communicate(
                    timeout=CHILD_TERMINATE_GRACE_SECONDS
                )
            except subprocess.TimeoutExpired:
                process.kill()
                remainder, _ = process.communicate()
        status = process.returncode if process.returncode is not None else 130
        return remainder, status

    def record_remainder(remainder: str) -> None:
        for line in remainder.splitlines(keepends=True):
            display(line)
            log.write(line)
            observe(line)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        os.chmod(log_path, 0o600)
        log.write("command: " + shlex.join(command) + "\n")
        log.flush()
        if runner is not None:
            control = importlib.import_module("archive_x_runner_control")
            if len(command) < 3 or command[0] != sys.executable:
                raise ArchiveError("controlled gallery runner command is invalid")

            def controlled_output(line: str) -> None:
                nonlocal stalled
                display(line)
                log.write(line)
                log.flush()
                observe(line)
                if not stalled and watchdog.observe(line):
                    stalled = True
                    message = (
                        "[archive-x][warning] No raw metadata progress across "
                        f"{watchdog.consecutive_stalls} consecutive X "
                        "rate-limit windows; stopping this endpoint with a "
                        "resumable checkpoint.\n"
                    )
                    print(f"[{prefix}] {message}", end="")
                    log.write(message)
                    log.flush()
                    runner.signal_interrupt()

            try:
                result = runner.run(
                    item_id=secrets.token_hex(16),
                    lease_token=control_lease_token or secrets.token_hex(16),
                    argv=command[2:],
                    output=controlled_output,
                )
                local_error = getattr(result, "error_class", None)
                if local_error is not None:
                    message = (
                        "[archive-x][error] Controlled gallery runner failed "
                        f"locally ({local_error}).\n"
                    )
                    controlled_output(message)
                    raise ArchiveError(
                        "controlled gallery runner failed locally "
                        f"({local_error})"
                    )
                status = int(result.status)
            except KeyboardInterrupt:
                interrupted = True
                status = 130
            except control.RunnerWorkerLost as exc:
                if stalled and exc.began:
                    status = 130
                else:
                    raise ArchiveError(
                        "controlled gallery runner exited before a result"
                    ) from exc
            except control.ControlProtocolError as exc:
                raise ArchiveError("controlled gallery runner failed closed") from exc
        else:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            assert process.stdout is not None
            try:
                for line in process.stdout:
                    display(line)
                    log.write(line)
                    log.flush()
                    observe(line)
                    if not stalled and watchdog.observe(line):
                        stalled = True
                        message = (
                            "[archive-x][warning] No raw metadata progress across "
                            f"{watchdog.consecutive_stalls} consecutive X "
                            "rate-limit windows; stopping this endpoint with a "
                            "resumable checkpoint.\n"
                        )
                        print(f"[{prefix}] {message}", end="")
                        log.write(message)
                        log.flush()
                        try:
                            process.send_signal(signal.SIGINT)
                        except ProcessLookupError:
                            pass
                        # Do not wait for EOF here: a child that ignores SIGINT
                        # could otherwise leave the watchdog blocked forever.
                        break
                if stalled:
                    remainder, status = stop_child(interrupt_already_sent=True)
                    record_remainder(remainder)
                else:
                    status = process.wait()
            except KeyboardInterrupt:
                interrupted = True
                remainder, status = stop_child(interrupt_already_sent=False)
                record_remainder(remainder)
            finally:
                log.flush()
                process.stdout.close()
    if stalled or interrupted or (status != 0 and other_error_count):
        # A checkpoint can be newer than gallery-dl's final cursor after a
        # SIGINT or extraction failure, but an earlier checkpoint can also
        # predate several successful pages before a sequence of real HTTP 429
        # responses.  Compare search progress and keep whichever boundary is
        # demonstrably farther along.  Do not promote a checkpoint merely for
        # a download-only failure: metadata enumeration may have completed.
        resume_cursor = prefer_advanced_search_cursor(
            resume_cursor, checkpoint_cursor
        )
    analyzed_failures, analyzed_other_errors = analyze_gallery_log(log_path)
    if analyzed_failures:
        failed_downloads = analyzed_failures
    other_error_count = analyzed_other_errors
    return (
        status,
        resume_cursor,
        time.monotonic() - started,
        interrupted,
        failed_downloads,
        other_error_count,
        stalled,
        watchdog.consecutive_stalls,
    )


def search_cursor_position(cursor: str | None) -> tuple[int, int] | None:
    """Return a comparable (stage, tweet ID) for search-stage cursors."""
    if not cursor:
        return None
    boundary = cursor.partition("/")[0]
    stage_text, separator, tweet_id_text = boundary.partition("_")
    if not separator:
        return None
    try:
        stage = int(stage_text)
        tweet_id = int(tweet_id_text)
    except ValueError:
        return None
    if stage not in {2, 3} or tweet_id < 1:
        return None
    return stage, tweet_id


def prefer_advanced_search_cursor(
    final_cursor: str | None, checkpoint_cursor: str | None
) -> str | None:
    """Prefer a checkpoint only when it demonstrably advanced pagination."""
    if not final_cursor:
        return checkpoint_cursor
    if not checkpoint_cursor:
        return final_cursor
    final_position = search_cursor_position(final_cursor)
    checkpoint_position = search_cursor_position(checkpoint_cursor)
    if final_position and checkpoint_position:
        final_stage, final_tweet_id = final_position
        checkpoint_stage, checkpoint_tweet_id = checkpoint_position
        if checkpoint_stage > final_stage or (
            checkpoint_stage == final_stage
            and checkpoint_tweet_id < final_tweet_id
        ):
            return checkpoint_cursor
    return final_cursor


def gallery_command(
    repo_dir: Path,
    config_path: Path,
    *,
    date_after: datetime | None,
    post_limit: int | None,
    retries: int,
    http_timeout: int,
    rate_limit: str,
    url: str,
    request_telemetry_path: Path | None = None,
    request_operation: str | None = None,
    download: bool = True,
    scheduler_options: Any | None = None,
) -> list[str]:
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_runner.py"),
    ]
    if (request_telemetry_path is None) != (request_operation is None):
        raise ArchiveError(
            "request telemetry path and operation must be provided together"
        )
    if request_telemetry_path is not None and request_operation is not None:
        command.extend(
            (
                "--archive-x-request-telemetry",
                str(request_telemetry_path),
                "--archive-x-operation",
                request_operation,
            )
        )
    if scheduler_options is not None:
        pacing = importlib.import_module("archive_x_pacing")
        command.extend(pacing.options_as_runner_args(scheduler_options))
    command.extend((
        "--config-ignore",
        "-c",
        str(repo_dir / "gallery-dl.conf"),
        "--config-json",
        str(config_path),
        "--no-input",
        "--no-colors",
        "--http-timeout",
        str(http_timeout),
        "--sleep-retries",
        "0" if scheduler_options is not None else "30-60",
        "--sleep-429",
        "0" if scheduler_options is not None else "300",
        "--limit-rate",
        rate_limit,
        "--retries",
        str(retries),
    ))
    if not download:
        command.append("--no-download")
    if date_after is not None:
        command.extend(("--date-after", iso_utc(date_after)))
    if post_limit is not None:
        command.extend(("--post-range", f"1-{post_limit}"))
    command.append(url)
    return command


def request_operation_for_endpoint(endpoint: str) -> str:
    if endpoint == "info":
        return "info"
    if endpoint == "timeline":
        return "timeline"
    if endpoint == "avatar":
        return "profile_avatar"
    if endpoint == "background":
        return "profile_background"
    if endpoint.startswith("retry-media-"):
        return "retry_media"
    raise ArchiveError(f"unsupported request telemetry endpoint: {endpoint}")


def descriptor_scope_for_endpoint(endpoint: str) -> tuple[str, str, str]:
    if endpoint == "timeline":
        return "modern", "modern", "post"
    if endpoint.startswith("retry-media-"):
        return "retry", "retry", "post"
    if endpoint == "avatar":
        return "profile", "info", "profile_avatar"
    if endpoint == "background":
        return "profile", "info", "profile_background"
    if endpoint == "info":
        return "info", "info", "post"
    raise ArchiveError(f"unsupported descriptor endpoint: {endpoint}")


def request_telemetry_summary(
    path: Path, expected_operation: str
) -> tuple[dict[str, Any] | None, str | None]:
    """Return a safe aggregate without making telemetry archive-authoritative."""
    try:
        summary = request_telemetry_x.read_summary(
            path, expected_operation=expected_operation
        )
    except request_telemetry_x.RequestTelemetryError:
        return None, "missing_invalid_or_private"
    return summary, None


def finalize_raw_file(partial: Path, success: bool) -> Path:
    partial.parent.mkdir(parents=True, exist_ok=True)
    if not partial.exists():
        partial.touch(mode=0o600)
    suffix = ".jsonl" if success else ".incomplete.jsonl"
    base = partial.name.removesuffix(".partial")
    if base.endswith(".jsonl"):
        base = base[:-6]
    destination = partial.with_name(base + suffix)
    os.replace(partial, destination)
    return destination


def archive_endpoint(
    *,
    args: argparse.Namespace,
    repo_dir: Path,
    archive_root: Path,
    user_dir: Path,
    handle: str,
    endpoint: str,
    run_dir: Path,
    archive_run_id: str,
    archived_at: str,
    date_after: datetime | None,
    cursor: str | None,
    target_url: str | None = None,
    retries: int | None = None,
    http_timeout: int | None = None,
    include_reposts: bool | None = None,
    stalled_rate_limit_cycles: int | None = None,
    download_media: bool | None = None,
    scheduler_options: Any | None = None,
    runner: Any | None = None,
    timeline_checkpoint_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    raw_partial = run_dir / "raw" / f"{endpoint}.posts.jsonl.partial"
    descriptor_partial = (
        run_dir / "raw" / f"{endpoint}.descriptors.jsonl.partial"
    )
    config_path = run_dir / f"{endpoint}.gallery-dl.json"
    request_path = run_dir / f"{endpoint}.requests.json"
    request_operation = request_operation_for_endpoint(endpoint)
    (
        descriptor_source_kind,
        descriptor_source_operation,
        descriptor_owner_kind,
    ) = descriptor_scope_for_endpoint(endpoint)
    descriptor_operation_id = f"{archive_run_id}:{endpoint}"
    descriptor_x.prepare_artifact(descriptor_partial)
    should_download_media = (
        endpoint != "timeline"
        if download_media is None
        else download_media
    )
    config = build_gallery_config(
        handle=handle,
        endpoint=endpoint,
        archive_root=archive_root,
        user_dir=user_dir,
        raw_partial=raw_partial,
        cookie_file=args.cookies,
        archive_run_id=archive_run_id,
        archived_at=archived_at,
        request_delay="0" if scheduler_options is not None else args.request_delay,
        download_delay=args.download_delay,
        extractor_delay="0" if scheduler_options is not None else args.extractor_delay,
        include_reposts=(
            not args.no_reposts
            if include_reposts is None
            else include_reposts
        ),
        checksums=not args.no_checksums,
        download_media=should_download_media,
        cursor=cursor,
        descriptor_artifact=descriptor_partial,
        descriptor_operation_id=descriptor_operation_id,
        descriptor_source_kind=descriptor_source_kind,
        descriptor_source_operation=descriptor_source_operation,
        descriptor_owner_kind=descriptor_owner_kind,
    )
    atomic_write_json(config_path, config)
    config_hash = sha256_file(config_path)
    url = target_url or endpoint_url(handle, endpoint)
    command = gallery_command(
        repo_dir,
        config_path,
        date_after=date_after if endpoint == "timeline" else None,
        post_limit=(
            effective_timeline_post_limit(args)
            if endpoint == "timeline"
            else None
        ),
        retries=args.retries if retries is None else retries,
        http_timeout=(
            args.http_timeout if http_timeout is None else http_timeout
        ),
        rate_limit=args.rate_limit,
        url=url,
        request_telemetry_path=request_path,
        request_operation=request_operation,
        download=should_download_media,
        scheduler_options=scheduler_options,
    )
    print(f"Archiving {handle}: {endpoint} ({url})")
    effective_stalled_cycles = (
        getattr(args, "stalled_rate_limit_cycles", 3)
        if stalled_rate_limit_cycles is None
        else stalled_rate_limit_cycles
    )
    (
        status,
        resume_cursor,
        duration,
        interrupted,
        failed_downloads,
        other_error_count,
        stalled,
        stalled_cycles,
    ) = run_gallery_dl(
        command,
        run_dir / f"{endpoint}.log",
        f"{handle}:{endpoint}",
        progress_path=raw_partial if endpoint == "timeline" else None,
        stalled_rate_limit_cycles=(
            effective_stalled_cycles
            if endpoint == "timeline"
            else 0
        ),
        runner=runner,
        checkpoint_callback=(
            timeline_checkpoint_callback if endpoint == "timeline" else None
        ),
        planned_path_output=(endpoint == "timeline" and not should_download_media),
    )
    synthetic_cursor = False
    if stalled:
        derived_cursor = synthetic_search_cursor(raw_partial)
        stage_three_boundary = bool(
            resume_cursor
            and resume_cursor.startswith("3_")
            and not resume_cursor.partition("/")[2]
        )
        if not resume_cursor:
            resume_cursor = derived_cursor or cursor
            synthetic_cursor = bool(derived_cursor)
        elif stage_three_boundary and derived_cursor:
            selected = prefer_advanced_search_cursor(
                resume_cursor, derived_cursor
            )
            synthetic_cursor = selected == derived_cursor and (
                selected != resume_cursor
            )
            resume_cursor = selected
    metadata_complete = gallery_metadata_complete(
        status,
        resume_cursor,
        interrupted,
        failed_downloads,
        other_error_count,
    )
    if stalled:
        metadata_complete = False
    raw_has_record = jsonl_has_record(raw_partial)
    if status != 0 and metadata_complete and not raw_has_record:
        metadata_complete = False
    raw_path = finalize_raw_file(raw_partial, metadata_complete)
    descriptor_path = descriptor_x.finalize_artifact(
        descriptor_partial, complete=metadata_complete
    )
    request_summary, request_error = request_telemetry_summary(
        request_path, request_operation
    )
    if interrupted:
        endpoint_status = "interrupted"
    elif stalled:
        endpoint_status = "stalled"
    elif status == 0:
        endpoint_status = "success"
    elif metadata_complete:
        endpoint_status = "media_partial"
    else:
        endpoint_status = "failed"
    return {
        "endpoint": endpoint,
        "url": url,
        "status": endpoint_status,
        "exit_code": status,
        "exit_reasons": decode_exit_status(status),
        "duration_seconds": round(duration, 3),
        "resume_cursor": resume_cursor,
        "interrupted": interrupted,
        "stalled": stalled,
        "stalled_rate_limit_cycles": stalled_cycles,
        "stalled_rate_limit_limit": (
            effective_stalled_cycles if endpoint == "timeline" else 0
        ),
        "synthetic_resume_cursor": synthetic_cursor,
        "metadata_complete": metadata_complete,
        "failed_downloads": failed_downloads,
        "other_error_count": other_error_count,
        "raw_has_record": raw_has_record,
        "raw_path": str(raw_path.relative_to(user_dir)),
        "descriptor_artifact_path": str(
            descriptor_path.relative_to(user_dir)
        ),
        "descriptor_artifact_sha256": descriptor_x.sha256_file(
            descriptor_path
        ),
        "descriptor_artifact_bytes": descriptor_path.stat().st_size,
        "descriptor_operation_id": descriptor_operation_id,
        "descriptor_source_kind": descriptor_source_kind,
        "descriptor_source_operation": descriptor_source_operation,
        "config_path": str(config_path.relative_to(user_dir)),
        "config_sha256": config_hash,
        "request_telemetry_path": (
            str(request_path.relative_to(user_dir))
            if request_path.is_file()
            else None
        ),
        "request_telemetry_sha256": (
            sha256_file(request_path) if request_path.is_file() else None
        ),
        "request_telemetry": request_summary,
        "request_telemetry_error": request_error,
        "command": command,
    }


def load_endpoint_descriptor_batch(
    user_dir: Path, endpoint_result: dict[str, Any]
) -> descriptor_x.DescriptorBatch:
    relative = endpoint_result.get("descriptor_artifact_path")
    operation_id = str(endpoint_result.get("descriptor_operation_id") or "")
    run_id_value = operation_id.partition(":")[0]
    path = user_dir / str(relative or "")
    if (
        not relative
        or not operation_id
        or not run_id_value
        or not path.is_file()
    ):
        raise descriptor_x.DescriptorError(
            "endpoint descriptor artifact evidence is incomplete"
        )
    batch = descriptor_x.load_artifact(
        path,
        user_dir=user_dir,
        operation_id=operation_id,
        run_id=run_id_value,
        source_kind=str(endpoint_result.get("descriptor_source_kind") or ""),
        source_operation=str(
            endpoint_result.get("descriptor_source_operation") or ""
        ),
    )
    expected_hash = str(endpoint_result.get("descriptor_artifact_sha256") or "")
    if batch.source_sha256 != expected_hash:
        raise descriptor_x.DescriptorError(
            "endpoint descriptor artifact digest changed"
        )
    return batch


def persist_descriptor_evidence(
    user_dir: Path,
    *,
    target_user_id: str,
    canonical_handle: str,
    accepted_records: Iterable[dict[str, Any]],
    endpoint_results: Iterable[dict[str, Any]] = (),
    batches: Iterable[descriptor_x.DescriptorBatch] = (),
    allow_profile: bool = False,
) -> dict[str, Any]:
    """Persist selected descriptor evidence without making metadata depend on it."""
    context_x = importlib.import_module("archive_x_context")
    records = tuple(accepted_records)
    loaded = list(batches)
    load_errors: list[str] = []
    for result in endpoint_results:
        try:
            loaded.append(load_endpoint_descriptor_batch(user_dir, result))
        except (descriptor_x.DescriptorError, OSError) as exc:
            load_errors.append(exc.__class__.__name__)
    with context_x.ContextDB(user_dir / "_state" / "context.sqlite3") as database:
        database.bind_identity(target_user_id, canonical_handle)
        summary = database.persist_descriptor_batches(
            loaded,
            records,
            allow_profile=allow_profile,
        )
    summary = dict(summary)
    summary["artifact_load_errors"] = len(load_errors)
    if load_errors:
        summary["artifact_load_error_classes"] = sorted(set(load_errors))
        if summary.get("status") == "complete":
            summary["status"] = "degraded"
    for batch in loaded:
        descriptor_x.discard_ephemeral_artifact(batch)
    return summary


def select_timeline_state(
    args: argparse.Namespace, state: dict[str, Any], started: datetime
) -> tuple[str | None, str, datetime | None]:
    """Select a saved cursor and preserve its original incremental cutoff."""
    modern_head = (
        state.get("modern_head")
        if isinstance(state.get("modern_head"), dict)
        and isinstance(state.get("legacy_backfill"), dict)
        else None
    )
    if modern_head and args.full_rescan:
        return None, iso_utc(started), SNOWFLAKE_EPOCH
    if modern_head and args.since is not None:
        return None, iso_utc(started), max(args.since, SNOWFLAKE_EPOCH)
    if modern_head and not args.post_limit:
        active = (
            modern_head.get("active")
            if isinstance(modern_head.get("active"), dict)
            else None
        )
        cursor = str(active.get("cursor")) if active and active.get("cursor") else None
        chain_started_at = (
            str(active.get("started_at")) if active else iso_utc(started)
        )
        cutoff_value = (
            active.get("date_after")
            if active
            else modern_head.get("last_successful_started_at")
        )
        try:
            cutoff = parse_datetime(str(cutoff_value)) - (
                timedelta(0) if active else timedelta(hours=args.overlap_hours)
            )
        except argparse.ArgumentTypeError:
            cutoff = None
        return cursor, chain_started_at, cutoff
    resume = state.get("resume") if isinstance(state.get("resume"), dict) else None
    if args.full_rescan or args.since is not None or args.post_limit:
        resume = None

    cursor = str(resume.get("cursor")) if resume and resume.get("cursor") else None
    chain_started_at = (
        str(resume.get("started_at")) if resume else iso_utc(started)
    )
    if cursor:
        saved_cutoff = resume.get("date_after") if resume else None
        if saved_cutoff:
            try:
                date_after = parse_datetime(str(saved_cutoff))
            except argparse.ArgumentTypeError:
                date_after = None
        else:
            # Resume states written by older versions did not retain this.
            # Re-crawling more is safer than inventing a cutoff and missing data.
            date_after = None
    elif args.since is not None:
        date_after = args.since
    elif args.full_rescan:
        date_after = None
    else:
        previous = state.get("last_successful_started_at")
        if previous:
            try:
                date_after = parse_datetime(str(previous)) - timedelta(
                    hours=args.overlap_hours
                )
            except argparse.ArgumentTypeError:
                date_after = None
        else:
            date_after = None
    return cursor, chain_started_at, date_after


def update_timeline_state(
    state: dict[str, Any],
    *,
    limited_run: bool,
    metadata_complete: bool,
    resume_cursor: str | None,
    handle: str,
    chain_started_at: str,
    date_after: datetime | None,
    observed_at: str,
    modern_head_mode: bool = False,
) -> None:
    """Commit crawl progress without discarding an older safe checkpoint."""
    if limited_run:
        # A smoke test must not advance or replace production crawl state.
        return
    if modern_head_mode:
        modern_head = state.get("modern_head")
        if not isinstance(modern_head, dict):
            raise ArchiveError("modern-head timeline state is missing")
        if metadata_complete:
            modern_head.update(
                {
                    "last_successful_started_at": chain_started_at,
                    "last_successful_completed_at": observed_at,
                    "active": None,
                }
            )
        elif resume_cursor:
            modern_head["active"] = {
                "cursor": resume_cursor,
                "started_at": chain_started_at,
                "date_after": iso_utc(date_after) if date_after else None,
                "saved_at": observed_at,
            }
        return
    if metadata_complete:
        state.update(
            {
                "schema": SCHEMA_NAME,
                "schema_version": SCHEMA_VERSION,
                "requested_handle": handle,
                "last_successful_started_at": chain_started_at,
                "last_successful_completed_at": observed_at,
                "resume": None,
            }
        )
    elif resume_cursor:
        state.update(
            {
                "schema": SCHEMA_NAME,
                "schema_version": SCHEMA_VERSION,
                "requested_handle": handle,
                "resume": {
                    "cursor": resume_cursor,
                    "started_at": chain_started_at,
                    "date_after": iso_utc(date_after) if date_after else None,
                    "saved_at": observed_at,
                },
            }
        )
    # A failure before the first new checkpoint is not evidence that an
    # existing cursor is invalid.  Preserve it rather than forcing a restart.


def merge_pending_media(
    state: dict[str, Any],
    failures: Iterable[dict[str, Any]],
    *,
    source_run_id: str,
    observed_at: str,
) -> None:
    def record_key(record: dict[str, Any]) -> str:
        if record.get("filename"):
            return "filename:" + Path(str(record["filename"])).name
        return "key:" + str(record.get("key") or "")

    def normalized_statuses(failure: dict[str, Any]) -> list[int]:
        statuses = failure.get("http_statuses")
        if not isinstance(statuses, list):
            return []
        return sorted(
            {
                status
                for status in statuses
                if isinstance(status, int) and 100 <= status <= 599
            }
        )

    def retry_at(attempts: int, *, unavailable_candidate: bool) -> str:
        if unavailable_candidate:
            delay = MEDIA_UNAVAILABLE_MIN_AGE
        else:
            multiplier = 2 ** max(0, min(attempts - 1, 8))
            delay = min(
                MEDIA_RETRY_BASE_DELAY * multiplier,
                MEDIA_RETRY_MAX_DELAY,
            )
        return iso_utc(parse_datetime(observed_at) + delay)

    def old_enough(record: dict[str, Any]) -> bool:
        try:
            first = parse_datetime(str(record.get("first_failed_at") or ""))
            latest = parse_datetime(observed_at)
        except argparse.ArgumentTypeError:
            return False
        return latest - first >= MEDIA_UNAVAILABLE_MIN_AGE

    current = state.get("pending_media")
    records = current if isinstance(current, list) else []
    by_key = {
        record_key(record): record.copy()
        for record in records
        if isinstance(record, dict) and (record.get("filename") or record.get("key"))
    }
    unavailable_value = state.get("unavailable_media")
    unavailable_records = (
        unavailable_value if isinstance(unavailable_value, list) else []
    )
    unavailable_by_key = {
        record_key(record): record.copy()
        for record in unavailable_records
        if isinstance(record, dict) and (record.get("filename") or record.get("key"))
    }
    for failure in failures:
        filename = Path(str(failure.get("filename") or "")).name
        if not filename:
            continue
        key = "filename:" + filename
        record = by_key.get(key, unavailable_by_key.get(key, {}))
        previous_source = record.get("last_source_run_id")
        attempts = int(record.get("attempts") or 0) + (
            0 if previous_source == source_run_id else 1
        )
        statuses = normalized_statuses(failure)
        unavailable_candidate = bool(statuses) and all(
            status in MEDIA_UNAVAILABLE_HTTP_STATUSES for status in statuses
        )
        record.update(
            {
                "filename": filename,
                "post_id": id_string(failure.get("post_id")),
                "media_number": failure.get("media_number"),
                "source_url": (
                    f"https://x.com/i/web/status/{failure.get('post_id')}"
                    if failure.get("post_id")
                    else None
                ),
                "first_failed_at": record.get("first_failed_at") or observed_at,
                "last_failed_at": observed_at,
                "last_source_run_id": source_run_id,
                "attempts": attempts,
                "last_http_statuses": statuses,
                "last_http_error_count": int(
                    failure.get("http_error_count") or len(statuses)
                ),
                "failure_class": (
                    "unavailable_candidate"
                    if unavailable_candidate
                    else "transient"
                ),
            }
        )
        terminal_http_failure = (
            unavailable_candidate
            and attempts >= MEDIA_UNAVAILABLE_MIN_ATTEMPTS
            and old_enough(record)
        )
        retry_budget_exhausted = attempts >= MEDIA_RETRY_MAX_ATTEMPTS
        if terminal_http_failure or retry_budget_exhausted:
            record.update(
                {
                    "status": "unavailable",
                    "unavailable_at": observed_at,
                    "unavailable_reason": (
                        "repeated_http_404_or_410"
                        if terminal_http_failure
                        else "media_retry_budget_exhausted"
                    ),
                    "next_retry_at": None,
                }
            )
            by_key.pop(key, None)
            unavailable_by_key[key] = record
        else:
            record.update(
                {
                    "status": "pending",
                    "unavailable_at": None,
                    "unavailable_reason": None,
                    "next_retry_at": retry_at(
                        attempts,
                        unavailable_candidate=unavailable_candidate,
                    ),
                }
            )
            unavailable_by_key.pop(key, None)
            by_key[key] = record

        post_id = id_string(failure.get("post_id"))
        post_key = "key:post:" + post_id if post_id else ""
        post_record = by_key.get(post_key)
        if post_record is not None:
            post_previous_source = post_record.get("last_source_run_id")
            post_attempts = int(post_record.get("attempts") or 0) + (
                0 if post_previous_source == source_run_id else 1
            )
            post_record.update(
                {
                    "last_failed_at": observed_at,
                    "last_source_run_id": source_run_id,
                    "attempts": post_attempts,
                    "last_http_statuses": statuses,
                    "failure_class": (
                        "unavailable_candidate"
                        if unavailable_candidate
                        else "transient"
                    ),
                    "next_retry_at": retry_at(
                        post_attempts,
                        unavailable_candidate=unavailable_candidate,
                    ),
                }
            )
            by_key[post_key] = post_record
    state["pending_media"] = sorted(
        by_key.values(),
        key=lambda record: str(record.get("filename") or record.get("key") or ""),
    )
    state["unavailable_media"] = sorted(
        unavailable_by_key.values(),
        key=lambda record: str(record.get("filename") or record.get("key") or ""),
    )


def pending_media_is_complete(
    user_dir: Path,
    record: dict[str, Any],
    unavailable_media: Iterable[dict[str, Any]] = (),
) -> bool:
    def asset_is_complete(path: Path) -> bool:
        if not path.is_file() or path.stat().st_size <= 0:
            return False
        sidecar = Path(str(path) + ".json")
        return (
            sidecar.is_file()
            and sidecar.stat().st_size > 0
            and isinstance(load_json(sidecar, None), dict)
        )

    unavailable = [
        item for item in unavailable_media if isinstance(item, dict)
    ]

    def asset_is_unavailable(post_id: str, media_number: int) -> bool:
        return any(
            id_string(item.get("post_id")) == post_id
            and item.get("media_number") == media_number
            and item.get("status") == "unavailable"
            for item in unavailable
        )

    media_root = user_dir / "media"
    if record.get("kind") == "post":
        post_id = id_string(record.get("post_id"))
        expected = record.get("expected_media_count")
        if not post_id or not isinstance(expected, int) or expected < 1:
            return False
        for media_number in range(1, expected + 1):
            pattern = f"*_{post_id}_{media_number}_*"
            if not (
                any(asset_is_complete(path) for path in media_root.rglob(pattern))
                or asset_is_unavailable(post_id, media_number)
            ):
                return False
        return True
    filename = Path(str(record.get("filename") or "")).name
    if filename and any(asset_is_complete(path) for path in media_root.rglob(filename)):
        return True
    post_id = id_string(record.get("post_id"))
    media_number = record.get("media_number")
    if not post_id or not media_number:
        return False
    pattern = f"*_{post_id}_{media_number}_*"
    return any(asset_is_complete(path) for path in media_root.rglob(pattern))


def prune_completed_pending_media(
    state: dict[str, Any], user_dir: Path
) -> list[dict[str, Any]]:
    unavailable_value = state.get("unavailable_media")
    unavailable = [
        record
        for record in (
            unavailable_value if isinstance(unavailable_value, list) else []
        )
        if isinstance(record, dict)
        and not pending_media_is_complete(user_dir, record)
    ]
    state["unavailable_media"] = unavailable
    current = state.get("pending_media")
    records = current if isinstance(current, list) else []
    remaining = [
        record
        for record in records
        if isinstance(record, dict)
        and not pending_media_is_complete(user_dir, record, unavailable)
    ]
    state["pending_media"] = remaining
    return remaining


def pending_media_due(
    state: dict[str, Any],
    user_dir: Path,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    current = prune_completed_pending_media(state, user_dir)
    observed = now or utc_now()
    due = []
    for record in current:
        value = record.get("next_retry_at")
        if not value:
            due.append(record)
            continue
        try:
            next_retry = parse_datetime(str(value))
        except argparse.ArgumentTypeError:
            due.append(record)
            continue
        if next_retry <= observed:
            due.append(record)
    return due


def media_queue_summary(
    state: dict[str, Any],
    user_dir: Path,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    pending = prune_completed_pending_media(state, user_dir)
    due = pending_media_due(state, user_dir, now=now)
    unavailable = state.get("unavailable_media")
    return {
        "pending": len(pending),
        "due": len(due),
        "deferred": len(pending) - len(due),
        "unavailable": len(unavailable) if isinstance(unavailable, list) else 0,
    }


def reclassify_pending_media_from_logs(
    state: dict[str, Any],
    user_dir: Path,
) -> int:
    """Enrich old pending records from their last immutable run log."""
    before = len(
        state.get("unavailable_media")
        if isinstance(state.get("unavailable_media"), list)
        else []
    )
    pending = list(
        state.get("pending_media")
        if isinstance(state.get("pending_media"), list)
        else []
    )
    for record in pending:
        if not isinstance(record, dict):
            continue
        source_run_id = str(record.get("last_source_run_id") or "")
        post_id = id_string(record.get("post_id"))
        if (
            not source_run_id
            or Path(source_run_id).name != source_run_id
            or not post_id
        ):
            continue
        run_dir = user_dir / "runs" / source_run_id
        candidates = [
            run_dir / f"retry-media-{post_id}.log",
            run_dir / "timeline.log",
        ]
        match = None
        for log_path in candidates:
            failures, _ = analyze_gallery_log(log_path)
            match = next(
                (
                    failure
                    for failure in failures
                    if (
                        failure.get("filename") == record.get("filename")
                        or (
                            id_string(failure.get("post_id")) == post_id
                            and failure.get("media_number")
                            == record.get("media_number")
                        )
                    )
                ),
                None,
            )
            if match is not None:
                break
        if match is None or not match.get("http_statuses"):
            continue
        merge_pending_media(
            state,
            [match],
            source_run_id=source_run_id,
            observed_at=str(record.get("last_failed_at") or iso_utc(utc_now())),
        )

    # Older state can already exceed today's bounded retry policy.  Migrate
    # those concrete asset records before due-work selection so a normal run
    # never spends another request merely to discover the budget was exhausted.
    pending = list(
        state.get("pending_media")
        if isinstance(state.get("pending_media"), list)
        else []
    )
    unavailable = list(
        state.get("unavailable_media")
        if isinstance(state.get("unavailable_media"), list)
        else []
    )
    unavailable_keys = {
        (
            Path(str(record.get("filename") or "")).name,
            id_string(record.get("post_id")),
            record.get("media_number"),
        )
        for record in unavailable
        if isinstance(record, dict)
    }
    remaining = []
    for record in pending:
        if not isinstance(record, dict):
            continue
        filename = Path(str(record.get("filename") or "")).name
        post_id = id_string(record.get("post_id"))
        media_number = record.get("media_number")
        concrete_asset = bool(filename and post_id and media_number)
        if (
            not concrete_asset
            or int(record.get("attempts") or 0) < MEDIA_RETRY_MAX_ATTEMPTS
        ):
            remaining.append(record)
            continue
        migrated = record.copy()
        migrated.update(
            {
                "status": "unavailable",
                "unavailable_at": (
                    record.get("last_failed_at") or iso_utc(utc_now())
                ),
                "unavailable_reason": "media_retry_budget_exhausted",
                "next_retry_at": None,
            }
        )
        key = (filename, post_id, media_number)
        if key not in unavailable_keys:
            unavailable.append(migrated)
            unavailable_keys.add(key)
    state["pending_media"] = remaining
    state["unavailable_media"] = unavailable
    prune_completed_pending_media(state, user_dir)
    after = len(
        state.get("unavailable_media")
        if isinstance(state.get("unavailable_media"), list)
        else []
    )
    return max(0, after - before)


def recovery_manifest_paths(
    user_dir: Path, manifest_paths: Iterable[Path] | None
) -> list[Path]:
    """Resolve either a one-time historical scan or indexed recovery candidates."""
    if manifest_paths is None:
        candidates = (user_dir / "runs").glob("*/manifest.json")
    else:
        candidates = manifest_paths
    runs_root = (user_dir / "runs").resolve()
    resolved: set[Path] = set()
    for candidate in candidates:
        path = Path(candidate).resolve()
        try:
            relative = path.relative_to(runs_root)
        except (OSError, ValueError) as exc:
            raise ArchiveError("recovery manifest escaped the user run root") from exc
        if (
            len(relative.parts) != 2
            or relative.name != "manifest.json"
            or ".." in relative.parts
            or not path.is_file()
        ):
            raise ArchiveError("recovery manifest path is invalid")
        resolved.add(path)
    return sorted(resolved)


def recover_download_only_runs(
    state: dict[str, Any],
    user_dir: Path,
    *,
    modern_head_mode: bool = False,
    manifest_paths: Iterable[Path] | None = None,
) -> list[str]:
    """Migrate older runs whose timeline ended but one asset failed."""
    recovered_value = state.get("recovered_download_only_runs")
    recovered = set(recovered_value if isinstance(recovered_value, list) else ())
    newly_recovered: list[str] = []
    for manifest_path in recovery_manifest_paths(user_dir, manifest_paths):
        manifest = load_json(manifest_path, {})
        if not isinstance(manifest, dict) or manifest.get("limited_run"):
            continue
        is_modern_head_run = manifest.get("timeline_mode") == "modern_head"
        if is_modern_head_run != modern_head_mode:
            continue
        completed_value = str(manifest.get("completed_at") or "")
        if manifest.get("status") not in {"failed", "partial"} or not completed_value:
            # Endpoint results are checkpointed into a still-running manifest
            # before derived datasets are rebuilt.  Such a provisional run is
            # not proof that the timeline can be advanced safely.
            continue
        try:
            parse_datetime(completed_value)
        except argparse.ArgumentTypeError:
            continue
        run_id_value = str(manifest.get("run_id") or manifest_path.parent.name)
        if run_id_value in recovered:
            continue
        timeline = next(
            (
                endpoint
                for endpoint in manifest.get("endpoints", ())
                if isinstance(endpoint, dict)
                and endpoint.get("endpoint") == "timeline"
            ),
            None,
        )
        if not timeline or timeline.get("interrupted"):
            continue
        if timeline.get("exit_code") != 4 or timeline.get("resume_cursor"):
            continue
        raw_relative = timeline.get("raw_path")
        raw_path = user_dir / str(raw_relative) if raw_relative else None
        if not raw_path or not jsonl_has_record(raw_path):
            continue
        failures, other_error_count = analyze_gallery_log(
            manifest_path.parent / "timeline.log"
        )
        if not gallery_metadata_complete(
            4, None, False, failures, other_error_count
        ):
            continue

        observed_at = completed_value
        merge_pending_media(
            state,
            failures,
            source_run_id=run_id_value,
            observed_at=observed_at,
        )
        started_at = str(manifest.get("started_at") or "")
        authority = state
        active_key = "resume"
        if modern_head_mode:
            modern_head = state.get("modern_head")
            if not isinstance(modern_head, dict):
                raise ArchiveError("modern-head timeline state is missing")
            authority = modern_head
            active_key = "active"
        previous = str(authority.get("last_successful_started_at") or "")
        active = (
            authority.get(active_key)
            if isinstance(authority.get(active_key), dict)
            else None
        )
        resume_started = str(active.get("started_at") or "") if active else ""
        if started_at and started_at >= previous and resume_started <= started_at:
            authority["last_successful_started_at"] = started_at
            authority["last_successful_completed_at"] = observed_at
            authority[active_key] = None
        recovered.add(run_id_value)
        newly_recovered.append(run_id_value)

    state["recovered_download_only_runs"] = sorted(recovered)
    prune_completed_pending_media(state, user_dir)
    return newly_recovered


def finalize_abandoned_manifests(
    user_dir: Path,
    *,
    recovered_at: str,
    manifest_paths: Iterable[Path] | None = None,
) -> list[str]:
    """Close stale running manifests after the global archive lock is held."""
    finalized: list[str] = []
    for manifest_path in recovery_manifest_paths(user_dir, manifest_paths):
        manifest = load_json(manifest_path, {})
        if not isinstance(manifest, dict) or manifest.get("status") != "running":
            continue
        run_id_value = str(manifest.get("run_id") or manifest_path.parent.name)
        manifest["status"] = "interrupted"
        manifest["failure_stage"] = "process_ended_before_manifest_finalization"
        manifest["completed_at"] = recovered_at
        manifest["finalized_on_later_startup"] = True
        atomic_write_json(manifest_path, manifest)
        finalized.append(run_id_value)
    return finalized


def finalize_abandoned_invocations(
    archive_root: Path,
    *,
    current_invocation_id: str,
    current_started_at: str,
    recovered_at: str,
) -> list[str]:
    """Close stale root readouts after the authoritative outer locks are held."""
    finalized: list[str] = []
    for path in sorted((archive_root / "runs").glob("*.json")):
        invocation = load_json(path, None)
        if not isinstance(invocation, dict):
            continue
        invocation_id = str(invocation.get("invocation_id") or path.stem)
        started_at = str(invocation.get("started_at") or "")
        if (
            invocation_id == current_invocation_id
            or (started_at and started_at >= current_started_at)
            or invocation.get("status") != "running"
        ):
            continue
        invocation["status"] = "interrupted"
        invocation["failure_stage"] = "process_ended_before_invocation_finalization"
        invocation["completed_at"] = recovered_at
        invocation["updated_at"] = recovered_at
        invocation["finalized_on_later_startup"] = True
        atomic_write_json(path, invocation)
        progress_path = (
            archive_root / "_state" / "progress" / f"{invocation_id}.json"
        )
        progress_snapshot = load_json(progress_path, None)
        if (
            isinstance(progress_snapshot, dict)
            and progress_snapshot.get("invocation_id") == invocation_id
            and progress_snapshot.get("status") == "running"
        ):
            progress_snapshot["status"] = "interrupted"
            progress_snapshot["updated_at"] = recovered_at
            atomic_write_json(progress_path, progress_snapshot)
        finalized.append(invocation_id)
    return finalized


def trailing_rate_limit_waits(path: Path) -> int:
    try:
        lines = path.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines()
    except OSError:
        return 0
    count = 0
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped or stripped == "KeyboardInterrupt":
            continue
        if CHECKPOINT_CURSOR_RE.search(line):
            continue
        if RATE_LIMIT_WAIT_RE.search(line):
            count += 1
            continue
        break
    return count


def recover_stalled_interrupted_runs(
    state: dict[str, Any],
    user_dir: Path,
    *,
    minimum_waits: int,
    modern_head_mode: bool = False,
    manifest_paths: Iterable[Path] | None = None,
) -> list[str]:
    """Recover a search-stage cursor when gallery-dl omitted one on SIGINT."""
    recovered_value = state.get("recovered_stalled_runs")
    recovered = set(recovered_value if isinstance(recovered_value, list) else ())
    newly_recovered: list[str] = []
    candidates: list[tuple[str, dict[str, Any]]] = []

    for manifest_path in recovery_manifest_paths(user_dir, manifest_paths):
        manifest = load_json(manifest_path, {})
        if not isinstance(manifest, dict) or manifest.get("limited_run"):
            continue
        is_modern_head_run = manifest.get("timeline_mode") == "modern_head"
        if is_modern_head_run != modern_head_mode:
            continue
        run_id_value = str(manifest.get("run_id") or manifest_path.parent.name)
        if run_id_value in recovered:
            continue
        timeline = next(
            (
                endpoint
                for endpoint in manifest.get("endpoints", ())
                if isinstance(endpoint, dict)
                and endpoint.get("endpoint") == "timeline"
            ),
            None,
        )
        if not timeline:
            continue
        stalled = bool(timeline.get("stalled"))
        interrupted = bool(timeline.get("interrupted"))
        if not stalled and not interrupted:
            continue

        raw_relative = timeline.get("raw_path")
        raw_path = user_dir / str(raw_relative) if raw_relative else None
        if not raw_path or not jsonl_has_record(raw_path):
            continue

        try:
            completed_at = parse_datetime(str(manifest.get("completed_at") or ""))
        except argparse.ArgumentTypeError:
            continue
        raw_modified_at = datetime.fromtimestamp(
            raw_path.stat().st_mtime, tz=timezone.utc
        )
        if completed_at - raw_modified_at < timedelta(
            minutes=10 * minimum_waits
        ):
            continue

        cursor = timeline.get("resume_cursor")
        wait_count = trailing_rate_limit_waits(
            manifest_path.parent / "timeline.log"
        )
        synthetic = False
        if not cursor:
            if wait_count < minimum_waits:
                continue
            cursor = synthetic_search_cursor(raw_path)
            synthetic = bool(cursor)
        if not cursor:
            continue

        observed_at = str(
            manifest.get("completed_at")
            or manifest.get("started_at")
            or iso_utc(utc_now())
        )
        failures, _ = analyze_gallery_log(
            manifest_path.parent / "timeline.log"
        )
        merge_pending_media(
            state,
            failures,
            source_run_id=run_id_value,
            observed_at=observed_at,
        )
        candidates.append(
            (
                str(manifest.get("started_at") or ""),
                {
                    "cursor": str(cursor),
                    "started_at": str(manifest.get("started_at") or observed_at),
                    "date_after": manifest.get("date_after"),
                    "saved_at": observed_at,
                    "source_run_id": run_id_value,
                    "synthetic": synthetic,
                    "stalled_rate_limit_cycles": wait_count,
                },
            )
        )
        recovered.add(run_id_value)
        newly_recovered.append(run_id_value)

    if candidates:
        candidate_started, candidate = max(candidates, key=lambda item: item[0])
        authority = state
        active_key = "resume"
        if modern_head_mode:
            modern_head = state.get("modern_head")
            if not isinstance(modern_head, dict):
                raise ArchiveError("modern-head timeline state is missing")
            authority = modern_head
            active_key = "active"
        successful_started = str(
            authority.get("last_successful_started_at") or ""
        )
        current = (
            authority.get(active_key)
            if isinstance(authority.get(active_key), dict)
            else None
        )
        current_started = str(current.get("started_at") or "") if current else ""
        if candidate_started >= successful_started and candidate_started >= current_started:
            authority[active_key] = candidate

    state["recovered_stalled_runs"] = sorted(recovered)
    prune_completed_pending_media(state, user_dir)
    return newly_recovered


def archive_user(
    args: argparse.Namespace,
    repo_dir: Path,
    archive_root: Path,
    handle: str,
    version: str,
) -> dict[str, Any]:
    started = utc_now()
    timeline_post_limit = effective_timeline_post_limit(args)
    current_run_id = run_id(started)
    user_dir = archive_root / "users" / handle
    run_dir = user_dir / "runs" / current_run_id
    state_path = user_dir / "_state" / "state.json"
    context_db_path = user_dir / "_state" / "context.sqlite3"
    local_module = importlib.import_module("archive_x_local")
    user_dir.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    state = load_json(state_path, {})
    if not isinstance(state, dict):
        state = {}
    run_dir.mkdir(parents=True, exist_ok=False)
    write_dataset_readme(user_dir)
    modern_head_mode = bool(
        isinstance(state.get("legacy_backfill"), dict)
        and isinstance(state.get("modern_head"), dict)
    )
    indexed_manifests = local_module.indexed_recovery_manifest_candidates(
        user_dir, context_db_path
    )
    recovery_manifests = (
        indexed_manifests
        if indexed_manifests is not None
        else recovery_manifest_paths(user_dir, None)
    )
    # Historical recovery is discovered read-only here, but no prior manifest,
    # state queue, or indexed archive truth may change until the live info
    # response proves this handle still belongs to the bound numeric account.
    finalized_abandoned_runs: list[str] = []
    recovered_streaming_runs: list[dict[str, Any]] = []
    recovered_stalled_runs: list[str] = []
    recovered_runs: list[str] = []
    newly_unavailable_media = 0
    media_reclassification_changed = False
    cursor: str | None = None
    chain_started_at = iso_utc(started)
    date_after: datetime | None = None
    manifest: dict[str, Any] = {
        "schema": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "run_id": current_run_id,
        "requested_handle": handle,
        "canonical_profile_url": f"https://x.com/{handle}",
        "started_at": iso_utc(started),
        "status": "running",
        "gallery_dl_version": version,
        "python_version": sys.version.split()[0],
        "archive_root": str(archive_root),
        "cookie_file": str(args.cookies),
        "cookie_values_logged": False,
        "reposts_included": not args.no_reposts,
        "quoted_source_media_included": False,
        "reply_context_policy": "target numeric author ID or repost-shaped entry",
        "repost_context_attribution_best_effort": bool(not args.no_reposts),
        "request_delay_seconds": args.request_delay,
        "download_delay_seconds": args.download_delay,
        "extractor_delay_seconds": args.extractor_delay,
        "date_after": iso_utc(date_after) if date_after else None,
        "resumed_from_cursor": cursor,
        "timeline_mode": "pending_identity",
        "limited_run": bool(timeline_post_limit),
        "retry_failed_only": bool(args.retry_failed_only),
        "finalized_abandoned_runs": finalized_abandoned_runs,
        "recovered_streaming_runs": [],
        "recovered_stalled_runs": recovered_stalled_runs,
        "recovered_download_only_runs": recovered_runs,
        "newly_unavailable_media": newly_unavailable_media,
        "media_reclassification_changed": media_reclassification_changed,
        "endpoints": [],
    }
    manifest_path = run_dir / "manifest.json"
    atomic_write_json(manifest_path, manifest)

    # Resolve and bind the stable numeric account ID before timeline media can
    # touch this handle's archive.  This fails closed if a handle is recycled.
    try:
        info_scheduler = existing_x_scheduler_options(
            user_dir, state, args.request_delay
        )
    except ArchiveError as exc:
        manifest["status"] = "failed"
        manifest["failure_stage"] = "preidentity_scheduler"
        manifest["error"] = str(exc)
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        return manifest
    info_result = archive_endpoint(
        args=args,
        repo_dir=repo_dir,
        archive_root=archive_root,
        user_dir=user_dir,
        handle=handle,
        endpoint="info",
        run_dir=run_dir,
        archive_run_id=current_run_id,
        archived_at=iso_utc(started),
        date_after=None,
        cursor=None,
        download_media=False,
        scheduler_options=info_scheduler,
    )
    info_probe_completed_at = time.time()
    manifest["endpoints"].append(info_result)
    atomic_write_json(manifest_path, manifest)
    info_raw = user_dir / info_result["raw_path"]
    if info_result.get("interrupted") or info_result["exit_code"] != 0:
        manifest["status"] = (
            "interrupted" if info_result.get("interrupted") else "failed"
        )
        manifest["failure_stage"] = "identity_probe"
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        if info_result.get("interrupted"):
            raise KeyboardInterrupt
        return manifest

    observed_user_id, canonical_handle = profile_identity(info_raw)
    if not observed_user_id:
        manifest["status"] = "failed"
        manifest["failure_stage"] = "identity_probe"
        manifest["error"] = "X profile metadata did not contain a numeric user ID"
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        return manifest
    try:
        bind_profile_identity(
            state, handle, observed_user_id, canonical_handle
        )
    except ArchiveError as exc:
        manifest["status"] = "failed"
        manifest["failure_stage"] = "identity_guard"
        manifest["error"] = str(exc)
        manifest["observed_user_id"] = observed_user_id
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        print(f"Identity guard stopped @{handle}: {exc}")
        return manifest

    manifest["requested_user_id"] = observed_user_id
    manifest["canonical_handle"] = canonical_handle or handle
    manifest["canonical_profile_url"] = (
        f"https://x.com/{canonical_handle or handle}"
    )
    atomic_write_json(state_path, state)

    # The identity guard now authorizes recovery and migration of the existing
    # archive.  Keep this entire block ahead of timeline selection so recovered
    # cursors and terminal media evidence affect this same invocation.
    try:
        if isinstance(state.get("legacy_backfill"), dict):
            legacy_module = importlib.import_module("archive_x_legacy")
            prepared = legacy_module.automatic_initialize_legacy(
                user_dir, initialized_at=iso_utc(started)
            )
            state = prepared["state"]
        modern_head_mode = bool(
            isinstance(state.get("legacy_backfill"), dict)
            and isinstance(state.get("modern_head"), dict)
        )
        finalized_abandoned_runs = finalize_abandoned_manifests(
            user_dir,
            recovered_at=iso_utc(started),
            manifest_paths=recovery_manifests,
        )
        recovered_streaming_runs = (
            local_module.recover_abandoned_streaming_sources(
                user_dir,
                context_db_path,
                requested_handle=canonical_handle or handle,
                target_user_id=observed_user_id,
                max_depth=1000,
            )
        )
        for recovered_stream in recovered_streaming_runs:
            checkpoint = str(
                recovered_stream.get("checkpoint_cursor") or ""
            )
            prior_run_id = str(recovered_stream.get("run_id") or "")
            prior_manifest = load_json(
                user_dir / "runs" / prior_run_id / "manifest.json", {}
            )
            if (
                not checkpoint
                or recovered_stream.get("status") != "recovered"
                or not isinstance(prior_manifest, dict)
                or prior_manifest.get("limited_run")
                or (
                    prior_manifest.get("timeline_mode") == "modern_head"
                ) != modern_head_mode
            ):
                continue
            try:
                prior_date_after = (
                    parse_datetime(str(prior_manifest["date_after"]))
                    if prior_manifest.get("date_after") else None
                )
            except argparse.ArgumentTypeError:
                prior_date_after = None
            update_timeline_state(
                state,
                limited_run=False,
                metadata_complete=False,
                resume_cursor=checkpoint,
                handle=handle,
                chain_started_at=str(
                    prior_manifest.get("started_at") or iso_utc(started)
                ),
                date_after=prior_date_after,
                observed_at=iso_utc(started),
                modern_head_mode=modern_head_mode,
            )
        recovered_stalled_runs = recover_stalled_interrupted_runs(
            state,
            user_dir,
            minimum_waits=getattr(args, "stalled_rate_limit_cycles", 3),
            modern_head_mode=modern_head_mode,
            manifest_paths=recovery_manifests,
        )
        recovered_runs = recover_download_only_runs(
            state,
            user_dir,
            modern_head_mode=modern_head_mode,
            manifest_paths=recovery_manifests,
        )
        media_state_before_reclassification = copy.deepcopy(state)
        newly_unavailable_media = reclassify_pending_media_from_logs(
            state, user_dir
        )
        media_reclassification_changed = (
            state != media_state_before_reclassification
        )
        cursor, chain_started_at, date_after = select_timeline_state(
            args, state, started
        )
        atomic_write_json(state_path, state)
    except ArchiveError as exc:
        manifest["status"] = "failed"
        manifest["failure_stage"] = "post_identity_recovery"
        manifest["error"] = str(exc)
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        return manifest

    manifest.update(
        {
            "finalized_abandoned_runs": finalized_abandoned_runs,
            "recovered_streaming_runs": [
                {
                    key: value
                    for key, value in item.items()
                    if key != "checkpoint_cursor"
                }
                for item in recovered_streaming_runs
            ],
            "recovered_stalled_runs": recovered_stalled_runs,
            "recovered_download_only_runs": recovered_runs,
            "newly_unavailable_media": newly_unavailable_media,
            "media_reclassification_changed": media_reclassification_changed,
            "resumed_from_cursor": cursor,
            "date_after": iso_utc(date_after) if date_after else None,
            "timeline_mode": "modern_head" if modern_head_mode else "history",
        }
    )
    atomic_write_json(manifest_path, manifest)
    if finalized_abandoned_runs:
        print(
            f"Finalized abandoned run manifest(s) for @{handle}: "
            f"{', '.join(finalized_abandoned_runs)}"
        )
    recovered_stream_ids = [
        str(item.get("run_id"))
        for item in recovered_streaming_runs
        if item.get("status") == "recovered"
    ]
    if recovered_stream_ids:
        print(
            f"Recovered incrementally indexed timeline source(s) for @{handle}: "
            f"{', '.join(recovered_stream_ids)}"
        )
    if recovered_stalled_runs:
        print(
            f"Recovered resumable search state for @{handle} from stalled "
            f"run(s): {', '.join(recovered_stalled_runs)}"
        )
    if recovered_runs:
        print(
            f"Recovered completed timeline state for @{handle} from "
            f"download-only run(s): {', '.join(recovered_runs)}"
        )
    if newly_unavailable_media:
        print(
            f"Classified {newly_unavailable_media} repeatedly missing media "
            f"item(s) for @{handle} as unavailable; normal runs will not retry them."
        )
    update_profile_dataset(
        user_dir, handle, info_raw, iso_utc(started)
    )

    # Only after the numeric identity guard may the one-time local migration
    # or any indexed archive truth be changed.  Recovery candidates discovered
    # read-only above are refreshed here before historical sources are marked
    # processed, so a crash-checkpointed manifest cannot lose its raw source.
    context_module = importlib.import_module("archive_x_context")
    try:
        with context_module.ContextDB(context_db_path) as database:
            database.bind_identity(
                observed_user_id, canonical_handle or handle
            )
        manifest_history = local_module.reconcile_manifest_history(
            user_dir, context_db_path
        )
        for candidate in recovery_manifests:
            if candidate.is_file():
                local_module.register_run_manifest(
                    user_dir, context_db_path, candidate, processed=True
                )
        source_history = local_module.reconcile_source_history(
            user_dir, context_db_path, max_depth=1000
        )
        media_history = local_module.reconcile_media_index(
            user_dir,
            context_db_path,
            requested_handle=canonical_handle or handle,
        )
        context_media_jobs = local_module.reconcile_context_media_jobs(
            context_db_path
        )
        legacy_media_queue = local_module.reconcile_state_media_queue(
            user_dir, context_db_path, state
        )
        atomic_write_json(state_path, state)
        manifest["local_reconciliation"] = {
            "manifest_history": manifest_history,
            "source_history": source_history,
            "media_history": media_history,
            "context_media_jobs": context_media_jobs,
            "legacy_media_queue": legacy_media_queue,
        }
        atomic_write_json(manifest_path, manifest)
        local_module.register_run_manifest(
            user_dir, context_db_path, manifest_path, processed=False
        )
    except (ArchiveError, OSError) as exc:
        manifest["status"] = "failed"
        manifest["failure_stage"] = "local_reconciliation"
        manifest["error"] = str(exc)
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        return manifest

    info_records = tuple(iter_jsonl(info_raw))
    profile_batches: tuple[descriptor_x.DescriptorBatch, ...] = ()
    profile_descriptor_error = None
    if info_records:
        try:
            profile_batches = (
                descriptor_x.profile_batch_from_info(
                    info_records[0],
                    user_dir=user_dir,
                    operation_id=f"{current_run_id}:info-profile",
                    run_id=current_run_id,
                    captured_at=iso_utc(started),
                    source_relative_path=str(info_raw.relative_to(user_dir)),
                    source_sha256=sha256_file(info_raw),
                ),
            )
        except descriptor_x.DescriptorError as exc:
            profile_descriptor_error = exc.__class__.__name__
    manifest["profile_descriptor_commit"] = persist_descriptor_evidence(
        user_dir,
        target_user_id=observed_user_id,
        canonical_handle=canonical_handle or handle,
        accepted_records=(),
        batches=profile_batches,
        allow_profile=True,
    )
    if profile_descriptor_error:
        manifest["profile_descriptor_commit"][
            "builder_error_class"
        ] = profile_descriptor_error
        manifest["profile_descriptor_commit"]["status"] = "degraded"
    atomic_write_json(manifest_path, manifest)
    local_module.register_run_manifest(
        user_dir, context_db_path, manifest_path, processed=False
    )

    normal_transition_run = bool(
        not args.retry_failed_only
        and not timeline_post_limit
        and args.since is None
        and not args.full_rescan
    )
    resume_state = state.get("resume")
    resume_cursor = (
        str(resume_state.get("cursor") or "")
        if isinstance(resume_state, dict)
        else ""
    )
    if (
        normal_transition_run
        and not isinstance(state.get("legacy_backfill"), dict)
        and resume_cursor.startswith("3_")
    ):
        legacy_module = importlib.import_module("archive_x_legacy")
        classification = legacy_module.classify_legacy_transition(user_dir)
        manifest["legacy_transition_preflight"] = classification
        if classification.get("decision") == "proven":
            prepared = legacy_module.automatic_initialize_legacy(
                user_dir, initialized_at=legacy_module.second_utc(started)
            )
            state = prepared["state"]
            modern_head_mode = True
            cursor, chain_started_at, date_after = select_timeline_state(
                args, state, started
            )
            manifest["resumed_from_cursor"] = cursor
            manifest["date_after"] = iso_utc(date_after) if date_after else None
            manifest["timeline_mode"] = "modern_head"
            manifest["legacy_transition_preflight"]["status"] = "initialized"
            print(
                f"Initialized legacy handoff for @{handle} from "
                f"{len(classification.get('confirmation_run_ids', ()))} "
                "verified prior no-progress run(s)."
            )
        atomic_write_json(manifest_path, manifest)

    # Historical state-JSON failures were migrated above.  Do not launch one
    # fresh X extractor per failed post: unified follow-up drains usable
    # descriptors directly from the CDN, then performs only bounded exceptional
    # descriptor refreshes.
    with context_module.ContextDB(context_db_path, create=False) as database:
        asset_availability = database.asset_availability(now=time.time())
    legacy_queue_migration = state.get("legacy_media_queue_migration") or {}
    manifest["media_recovery"] = {
        "mode": "descriptor_direct",
        "pending_before": int(asset_availability.get("total") or 0),
        "due_before": int(asset_availability.get("ready") or 0),
        "retried_post_ids": [],
        "pending_after": int(asset_availability.get("total") or 0),
        "due_after": int(asset_availability.get("ready") or 0),
        "deferred_after": max(
            0,
            int(asset_availability.get("total") or 0)
            - int(asset_availability.get("ready") or 0),
        ),
        "unavailable_after": 0,
        "legacy_manual_review": int(
            legacy_queue_migration.get("manual_review") or 0
        ),
    }

    if args.retry_failed_only:
        manifest["status"] = "success"
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        local_module.register_run_manifest(
            user_dir, context_db_path, manifest_path, processed=True
        )
        return manifest

    timeline_scheduler = x_scheduler_options(
        user_dir, observed_user_id, args.request_delay
    )
    if info_scheduler is None:
        try:
            # A new/pre-v3 archive has no account database before its first
            # probe. Persist that one gap after binding; local migration time
            # can naturally absorb it, and the actual timeline boundary is
            # still enforced by the durable lane without a stacked sleep.
            bridge_identity_probe_boundary(
                timeline_scheduler,
                probe_completed_at=info_probe_completed_at,
            )
        except ArchiveError as exc:
            manifest["status"] = "failed"
            manifest["failure_stage"] = "identity_probe_pacing"
            manifest["error"] = str(exc)
            manifest["completed_at"] = iso_utc(utc_now())
            atomic_write_json(manifest_path, manifest)
            return manifest

    legacy_module = importlib.import_module("archive_x_legacy")
    transition_watchdog = legacy_module.transition_watchdog_policy(
        user_dir,
        state,
        ambiguous_cycles=getattr(args, "stalled_rate_limit_cycles", 3),
    )
    manifest["timeline_stall_watchdog"] = transition_watchdog
    atomic_write_json(manifest_path, manifest)

    timeline_partial = run_dir / "raw" / "timeline.posts.jsonl.partial"
    timeline_ledger_relative = str(timeline_partial.relative_to(user_dir))
    timeline_stream_spec = local_module.SourceSpec(
        path=timeline_partial,
        source_kind="modern",
        run_id=current_run_id,
        operation_id=f"{current_run_id}:timeline",
        endpoint="timeline",
    )
    timeline_stream: dict[str, Any] = {
        "status": "waiting_for_checkpoint",
        "checkpoints": 0,
        "bytes_indexed": 0,
        "records_indexed": 0,
        "new_posts": 0,
        "updated_posts": 0,
        "cursor_publication_error": None,
        "error_class": None,
    }

    def commit_timeline_checkpoint(checkpoint: str) -> None:
        if timeline_stream["error_class"] is not None:
            return
        try:
            committed = local_module.ingest_streaming_source(
                user_dir,
                context_db_path,
                requested_handle=canonical_handle or handle,
                target_user_id=observed_user_id,
                spec=timeline_stream_spec,
                checkpoint_cursor=checkpoint,
                max_depth=1000,
            )
        except (ArchiveError, OSError, sqlite3.Error) as exc:
            timeline_stream["status"] = "degraded_to_final_ingest"
            timeline_stream["error_class"] = exc.__class__.__name__
            print(
                "Live timeline indexing paused; raw capture remains active "
                f"({exc.__class__.__name__})."
            )
            return
        timeline_stream["status"] = "streaming"
        timeline_stream["checkpoints"] += int(
            committed.get("checkpoint_committed", False)
        )
        timeline_stream["bytes_indexed"] += int(
            committed.get("bytes_read") or 0
        )
        timeline_stream["records_indexed"] += int(
            committed.get("raw_records") or 0
        )
        timeline_stream["new_posts"] += int(
            committed.get("new_posts") or 0
        )
        timeline_stream["updated_posts"] += int(
            committed.get("updated_posts") or 0
        )
        timeline_stream["committed_bytes"] = int(
            committed.get("committed_bytes") or 0
        )
        timeline_stream["frontier_post_id"] = committed.get(
            "frontier_post_id"
        )
        timeline_stream["frontier_posted_at"] = committed.get(
            "frontier_posted_at"
        )
        if not committed.get("checkpoint_committed"):
            return
        # SQLite is authoritative and commits first.  Publishing the cursor
        # second means a crash can only replay an older page, never skip rows.
        try:
            update_timeline_state(
                state,
                limited_run=bool(timeline_post_limit),
                metadata_complete=False,
                resume_cursor=checkpoint,
                handle=handle,
                chain_started_at=chain_started_at,
                date_after=date_after,
                observed_at=iso_utc(utc_now()),
                modern_head_mode=modern_head_mode,
            )
            atomic_write_json(state_path, state)
        except (ArchiveError, OSError) as exc:
            timeline_stream["cursor_publication_error"] = (
                exc.__class__.__name__
            )

    timeline_result = archive_endpoint(
        args=args,
        repo_dir=repo_dir,
        archive_root=archive_root,
        user_dir=user_dir,
        handle=handle,
        endpoint="timeline",
        run_dir=run_dir,
        archive_run_id=current_run_id,
        archived_at=iso_utc(started),
        date_after=date_after,
        cursor=cursor,
        stalled_rate_limit_cycles=transition_watchdog["cycles"],
        scheduler_options=timeline_scheduler,
        timeline_checkpoint_callback=commit_timeline_checkpoint,
    )
    timeline_result["incremental_indexing"] = dict(timeline_stream)
    manifest["endpoints"].append(timeline_result)
    atomic_write_json(manifest_path, manifest)
    timeline_raw = user_dir / timeline_result["raw_path"]
    timeline_complete = bool(
        timeline_result.get("metadata_complete")
        and not timeline_result.get("interrupted")
    )
    if timeline_result.get("failed_downloads"):
        merge_pending_media(
            state,
            timeline_result["failed_downloads"],
            source_run_id=current_run_id,
            observed_at=iso_utc(utc_now()),
        )
    if timeline_result.get("status") == "media_partial":
        processed = state.get("recovered_download_only_runs")
        processed_runs = set(processed if isinstance(processed, list) else ())
        processed_runs.add(current_run_id)
        state["recovered_download_only_runs"] = sorted(processed_runs)
    prune_completed_pending_media(state, user_dir)

    timeline_batches: tuple[descriptor_x.DescriptorBatch, ...] = ()
    timeline_descriptor_error: str | None = None
    try:
        timeline_batches = (
            load_endpoint_descriptor_batch(user_dir, timeline_result),
        )
    except (descriptor_x.DescriptorError, OSError) as exc:
        timeline_descriptor_error = exc.__class__.__name__
    try:
        manifest["post_dataset"] = local_module.finalize_streaming_source(
            user_dir,
            context_db_path,
            requested_handle=canonical_handle or handle,
            target_user_id=observed_user_id,
            spec=local_module.SourceSpec(
                path=timeline_raw,
                source_kind="modern",
                run_id=current_run_id,
                operation_id=f"{current_run_id}:timeline",
                endpoint="timeline",
            ),
            ledger_relative_path=timeline_ledger_relative,
            timeline_complete=timeline_complete,
            checkpoint_cursor=timeline_result.get("resume_cursor"),
            max_depth=1000,
            descriptor_batches=timeline_batches,
        )
    finally:
        for batch in timeline_batches:
            descriptor_x.discard_ephemeral_artifact(batch)
    timeline_result["descriptor_commit"] = dict(
        manifest["post_dataset"].get("descriptor_commit") or {}
    )
    timeline_result["incremental_indexing"].update(
        {
            "status": "committed",
            "timeline_complete": timeline_complete,
            "committed_bytes": int(timeline_raw.stat().st_size),
            "records_indexed": int(
                manifest["post_dataset"].get("raw_records") or 0
            ),
        }
    )
    if timeline_descriptor_error:
        timeline_result["descriptor_commit"]["artifact_load_errors"] = 1
        timeline_result["descriptor_commit"][
            "artifact_load_error_classes"
        ] = [timeline_descriptor_error]
        timeline_result["descriptor_commit"]["status"] = "degraded"
    manifest["media_dataset"] = {
        "status": "indexed_incrementally",
        "recursive_sidecar_scan": False,
    }

    # Advance crawl state only after raw records have been merged into the
    # derived datasets.  A crash before here retains the prior cursor and
    # safely replays this page instead of skipping records in posts.jsonl.
    update_timeline_state(
        state,
        limited_run=bool(timeline_post_limit),
        metadata_complete=timeline_complete,
        resume_cursor=timeline_result.get("resume_cursor"),
        handle=handle,
        chain_started_at=chain_started_at,
        date_after=date_after,
        observed_at=iso_utc(utc_now()),
        modern_head_mode=modern_head_mode,
    )
    atomic_write_json(state_path, state)

    if getattr(args, "seed_reply_context", False):
        manifest["reply_context_discovery"] = {
            "status": "deferred_to_unified_context_phase",
            "timeline_state_committed": True,
            "network_requests": 0,
        }
        atomic_write_json(manifest_path, manifest)

    if not timeline_complete:
        if timeline_result.get("interrupted"):
            manifest["status"] = "interrupted"
        elif timeline_result.get("stalled"):
            manifest["status"] = "stalled"
            manifest["failure_stage"] = "timeline_no_progress_watchdog"
        else:
            manifest["status"] = "failed"
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        local_module.register_run_manifest(
            user_dir, context_db_path, manifest_path, processed=False
        )
        if timeline_result.get("interrupted"):
            raise KeyboardInterrupt
        return manifest

    manifest["status"] = "limited" if timeline_post_limit else "success"
    manifest["profile_media"] = {
        "status": "queued_from_info_descriptors",
        "separate_x_extractors": 0,
    }
    atomic_write_json(state_path, state)
    manifest["completed_at"] = iso_utc(utc_now())
    atomic_write_json(manifest_path, manifest)
    local_module.register_run_manifest(
        user_dir, context_db_path, manifest_path, processed=True
    )
    return manifest


def dry_run_summary(
    args: argparse.Namespace,
    archive_root: Path,
    targets: list[str],
    version: str,
) -> None:
    print("Dry run: no X requests and no archive writes will be made.")
    print(f"gallery-dl: {version}")
    print(f"archive root: {archive_root}")
    read_only = filesystem_is_read_only(archive_root)
    print(
        "archive filesystem in this process: "
        + (
            "READ-ONLY; a real run will refuse to start"
            if read_only
            else "read-write"
        )
    )
    if args.output_root is None and not os.environ.get("GDL_X_ARCHIVE_ROOT"):
        print(
            "note: "
            + (
                "remount Bibliotheque read-write before a real run"
                if read_only
                else "a real run requires this Bibliotheque root to remain read-write"
            )
        )
    print(f"cookie file: {args.cookies} (values not displayed)")
    print(f"users ({len(targets)}): {', '.join(targets)}")
    print("identity/profile endpoint first: info (stable user-ID guard)")
    if args.retry_failed_only:
        print("mode: retry recorded incomplete media; no timeline crawl")
        print(
            "failed-media recovery: "
            f"{args.media_retries} direct attempt(s), "
            f"{args.media_timeout}s inactivity timeout, bounded exact refresh"
        )
    else:
        print("phase 1: modern timeline plus descriptor-based profile/media update")
        print("phase 2: guarded automatic legacy detection/resume to source-visible floor")
        print(
            "legacy protocol: 3-day roots with safe subdivision; "
            "2 matching walks and 2 distinct empty tail pages per walk"
        )
        print("phase 3: seed reply ancestors and drain their direct media jobs")
        print("profile media: reused from the mandatory info descriptor; no extra X endpoint")
    print(f"reposts: {'included and labeled' if not args.no_reposts else 'excluded'}")
    print("quoted-source media: excluded")
    print(
        "context scope: one bounded response; queued ancestors only; "
        "no unrelated siblings/descendants/quotes"
    )
    if not args.no_reposts:
        print("repost attribution: best effort where X omits wrapper-author identity")
    print(
        "actual X request lane: "
        f"{args.request_delay}s requested, minimum 4s, durable across restart"
    )
    print(f"direct media bandwidth limit: {args.rate_limit}")
    print(
        "no-progress watchdog: one unchanged window at a verified legacy-era "
        f"floor; {args.stalled_rate_limit_cycles} for ambiguous boundaries"
    )
    print(f"between users: {args.user_delay}s")
    if args.post_limit:
        print(
            f"modern diagnostic post limit: {args.post_limit}; automatic legacy and "
            "context network phases will be skipped"
        )
    if args.since is not None:
        print("explicit --since mode: modern acquisition only; backlog phases skipped")
    if args.legacy_max_windows is not None:
        print(f"advanced legacy bound: {args.legacy_max_windows} root window(s) per user")
    if args.context_max_posts is not None:
        print(f"advanced context metadata bound: {args.context_max_posts} attempt(s)")
    if args.context_media_max_posts is not None:
        print(
            "advanced context media bound: "
            f"{args.context_media_max_posts} attempt(s)"
        )
    if args.modern_max_posts is not None:
        print(
            f"advanced modern rollout bound: {args.modern_max_posts} post(s); "
            "downstream phases require existing initialized legacy state"
        )

    context_module = importlib.import_module("archive_x_context")
    legacy_module = importlib.import_module("archive_x_legacy")

    def integrity_label(summary: dict[str, Any]) -> str:
        value = summary.get("integrity_ok")
        return "ok" if value is True else "FAILED" if value is False else "not checked"

    for handle in targets:
        user_dir = archive_root / "users" / handle
        state = load_json(user_dir / "_state" / "state.json", None)
        print(f"plan @{handle}:")
        diagnostic_modern_only = bool(args.post_limit or args.since is not None)
        if not isinstance(state, dict):
            if args.retry_failed_only:
                print("  modern/legacy/context metadata: skipped (no existing state)")
            elif diagnostic_modern_only or args.modern_max_posts:
                print("  modern: bounded diagnostic acquisition")
                print("  legacy/context network: skipped by diagnostic mode")
            else:
                print("  modern: initial source-visible historical crawl")
                print("  legacy: evaluate only after an exact proven boundary")
            print("  shared media: no existing queue")
            if not args.retry_failed_only and not diagnostic_modern_only:
                print(
                    "  context: bootstrap from committed sources, then drain to closure"
                )
            continue
        # Preview legacy pending records using their immutable last-run logs,
        # but keep the dry run strictly in-memory.
        reclassify_pending_media_from_logs(state, user_dir)
        media_summary = media_queue_summary(state, user_dir)
        if args.retry_failed_only:
            print("  modern/legacy/context metadata: skipped by retry-only mode")
            print(
                "  shared media: "
                f"{media_summary['due']} due, "
                f"{media_summary['deferred']} deferred, "
                f"{media_summary['unavailable']} confirmed unavailable"
            )
            summary = context_module.readonly_context_summary(
                user_dir / "_state" / "context.sqlite3"
            )
            if summary["status"] == "absent":
                print("  context media: skipped; context database absent")
            else:
                print(
                    f"  context media: {summary['media_pending']} pending item(s); "
                    f"integrity={integrity_label(summary)}"
                )
            continue
        legacy = state.get("legacy_backfill")
        if isinstance(legacy, dict):
            validated = legacy_module.validate_legacy_state(
                legacy,
                expected_user_id=str(state.get("requested_user_id") or ""),
            )
            head = state.get("modern_head")
            head_active = isinstance(head, dict) and isinstance(
                head.get("active"), dict
            )
            print(
                "  modern: "
                + ("resume interrupted head" if head_active else "incremental head update")
            )
            print(
                "  legacy: "
                f"{validated['status']}; frontier={validated['next_until']}; "
                f"account_floor={validated['floor_since']}"
            )
        else:
            historical = (
                legacy_module.classify_legacy_transition(user_dir)
                if (
                    not diagnostic_modern_only
                    and not args.full_rescan
                    and args.modern_max_posts is None
                )
                else {"decision": "not_applicable"}
            )
            if historical.get("decision") == "proven":
                print("  modern: initialize legacy boundary, then update modern head")
                print(
                    "  legacy: initialize from "
                    f"{len(historical.get('confirmation_run_ids', ()))} "
                    "verified prior no-progress run(s), then backfill"
                )
            else:
                resume = state.get("resume")
                if isinstance(resume, dict):
                    print("  modern: resume historical crawl (cursor redacted)")
                elif state.get("last_successful_started_at"):
                    print("  modern: incremental update")
                else:
                    print("  modern: initial source-visible historical crawl")
                print(
                    "  legacy: not initialized; strict transition evidence required"
                )
            if args.modern_max_posts:
                print("  legacy/context network: skipped; legacy is not initialized")
        print(
            "  shared media: "
            f"{media_summary['due']} due, "
            f"{media_summary['deferred']} deferred, "
            f"{media_summary['unavailable']} confirmed unavailable"
        )
        summary = context_module.readonly_context_summary(
            user_dir / "_state" / "context.sqlite3"
        )
        if summary["status"] == "absent":
            print("  context: database absent; bootstrap all committed sources")
        else:
            print(
                "  context: "
                f"{summary['metadata_pending']} metadata pending, "
                f"{summary['manual_review']} manual review, "
                f"{summary['media_pending']} media pending, "
                f"integrity={integrity_label(summary)}"
            )
        if args.post_limit or args.since is not None:
            print("  note: legacy/context network work is shown but skipped this run")


def print_invocation_summary(results: list[dict[str, Any]]) -> None:
    """Print a compact non-authoritative readout from structured phase truth."""
    print("X archive phase summary:")
    phase_names = (
        "modern",
        "transition",
        "modern_head_after_transition",
        "legacy",
        "shared_media",
        "context_seed",
        "context_metadata",
        "context_media",
        "context_export",
    )
    for result in results:
        handle = result["requested_handle"]
        print(f"  @{handle}: {result['status']}")
        phases = result.get("phases") or {}
        for name in phase_names:
            phase = phases.get(name)
            if isinstance(phase, dict) and phase.get("status"):
                detail = ""
                if name == "context_export" and (
                    "durable_generation" in phase
                    or "exported_generation" in phase
                ):
                    detail = (
                        " (durable generation "
                        f"{int(phase.get('durable_generation') or 0):,}; "
                        "published generation "
                        f"{int(phase.get('exported_generation') or 0):,})"
                    )
                print(f"    {name}: {phase['status']}{detail}")


def build_parser(repo_dir: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scripts/archive-x",
        description=(
            "Conservatively archive an X account's modern timeline, any proven "
            "legacy-ID history, reply ancestors, media, and profile metadata."
        ),
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--user",
        action="append",
        help="X handle or profile URL; may be repeated",
    )
    target.add_argument(
        "--input-file",
        type=Path,
        help="text file containing one X handle or profile URL per line",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help=(
            "archive root; defaults to a mounted Bibliotheque/gdl/x-archive "
            "and never silently falls back to local storage"
        ),
    )
    parser.add_argument(
        "--cookies",
        type=Path,
        default=repo_dir / "state" / "cookies" / "x.cookies.txt",
        help="Netscape-format X cookie file",
    )
    parser.add_argument(
        "--no-reposts",
        action="store_true",
        help="exclude reposts; they are included and labeled by default",
    )
    parser.add_argument(
        "--full-rescan",
        action="store_true",
        help="rescan the full modern-ID domain without changing legacy state",
    )
    parser.add_argument(
        "--since",
        type=parse_datetime,
        help="archive posts on or after this ISO-8601 date",
    )
    parser.add_argument(
        "--overlap-hours",
        type=nonnegative_float,
        default=48.0,
        help="incremental recrawl overlap (default: 48)",
    )
    parser.add_argument(
        "--post-limit",
        type=positive_int,
        help="modern-only smoke limit; later network phases are skipped",
    )
    parser.add_argument(
        "--stalled-rate-limit-cycles",
        type=positive_int,
        default=3,
        help=(
            "stop an ambiguous timeline after this many unchanged X "
            "rate-limit windows; verified pre-Snowflake floors use one "
            "window (default for ambiguous boundaries: 3)"
        ),
    )
    parser.add_argument(
        "--request-delay",
        type=duration_arg,
        default="4-8",
        help=(
            "actual X request gap; values below 4s retain the 4s safety floor "
            "(default: 4-8)"
        ),
    )
    parser.add_argument(
        "--download-delay",
        type=duration_arg,
        default="1-3",
        help="delay before each asset download (default: 1-3)",
    )
    parser.add_argument(
        "--extractor-delay",
        type=duration_arg,
        default="2-5",
        help="delay before each endpoint starts (default: 2-5)",
    )
    parser.add_argument(
        "--endpoint-delay",
        type=duration_arg,
        default="10-20",
        help="delay between endpoint processes (default: 10-20)",
    )
    parser.add_argument(
        "--user-delay",
        type=duration_arg,
        default="60-120",
        help="delay between users in a batch (default: 60-120)",
    )
    parser.add_argument(
        "--retries",
        type=positive_int,
        default=1,
        help="general HTTP retries (default: 1)",
    )
    parser.add_argument(
        "--http-timeout",
        type=positive_int,
        default=60,
        help="normal HTTP inactivity timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--media-retries",
        type=positive_int,
        default=2,
        help="retries for previously failed media assets (default: 2)",
    )
    parser.add_argument(
        "--media-timeout",
        type=positive_int,
        default=300,
        help="inactivity timeout for failed-media recovery (default: 300)",
    )
    parser.add_argument(
        "--rate-limit",
        default="8M",
        help="asset download bandwidth limit (default: 8M)",
    )
    parser.add_argument(
        "--no-checksums",
        action="store_true",
        help="skip SHA-256 computation for newly downloaded assets",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="compatibility flag; independent users already continue safely",
    )
    parser.add_argument(
        "--retry-failed-only",
        action="store_true",
        help=(
            "retry recorded shared/context media without new timeline, legacy, "
            "or context-metadata acquisition"
        ),
    )
    parser.add_argument(
        "--seed-reply-context",
        action="store_true",
        help=(
            "deprecated compatibility flag; unified context processing is automatic"
        ),
    )
    parser.add_argument(
        "--modern-max-posts",
        type=positive_int,
        help=(
            "advanced rollout bound for modern posts; permits later bounded "
            "phases only when legacy is already initialized"
        ),
    )
    parser.add_argument(
        "--legacy-max-windows",
        type=positive_int,
        help="advanced rollout/diagnostic limit for committed legacy root windows",
    )
    parser.add_argument(
        "--context-max-posts",
        type=positive_int,
        help="advanced rollout/diagnostic limit for reply-parent attempts",
    )
    parser.add_argument(
        "--context-media-max-posts",
        type=positive_int,
        help="advanced rollout/diagnostic limit for context-media attempts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read-only preview of all phases and existing backlog state",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    repo_dir = Path(__file__).resolve().parent.parent
    parser = build_parser(repo_dir)
    args = parser.parse_args(argv)
    if args.seed_reply_context:
        print(
            "note: --seed-reply-context is deprecated and no longer changes "
            "behavior; context seeding and resolution are automatic"
        )
    if args.full_rescan and args.since is not None:
        parser.error("--full-rescan and --since cannot be used together")
    if args.modern_max_posts and (
        args.post_limit or args.since is not None or args.full_rescan
    ):
        parser.error(
            "--modern-max-posts cannot be combined with --post-limit, "
            "--since, or --full-rescan"
        )
    if args.retry_failed_only and (
        args.full_rescan
        or args.since is not None
        or args.post_limit
        or args.modern_max_posts
    ):
        parser.error(
            "--retry-failed-only cannot be combined with --full-rescan, "
            "--since, or --post-limit"
        )
    args.cookies = args.cookies.expanduser().resolve()
    if args.input_file is not None:
        args.input_file = args.input_file.expanduser().resolve()

    try:
        targets = load_targets(args.user, args.input_file)
        validate_cookie_file(args.cookies)
        archive_root = resolve_output_root(args.output_root, plan_only=args.dry_run)
        version = gallery_dl_version()
        verify_gallery_dl_x_runner(repo_dir, version)
        if args.dry_run:
            dry_run_summary(args, archive_root, targets, version)
            return 0

        if filesystem_is_read_only(archive_root):
            raise ArchiveError(
                f"archive root filesystem is mounted read-only: {archive_root}"
            )
        archive_root.mkdir(parents=True, exist_ok=True)
        if not os.access(archive_root, os.W_OK | os.X_OK):
            raise ArchiveError(f"archive root is not writable: {archive_root}")

        invocation_started = utc_now()
        invocation_id = run_id(invocation_started)
        invocation = {
            "schema": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "invocation_id": invocation_id,
            "started_at": iso_utc(invocation_started),
            "status": "running",
            "gallery_dl_version": version,
            "results": [],
        }
        invocation_path = archive_root / "runs" / f"{invocation_id}.json"
        progress_path = (
            archive_root / "_state" / "progress" / f"{invocation_id}.json"
        )
        progress_module = importlib.import_module("archive_x_progress")
        progress = progress_module.create_tracker(
            progress_path,
            archive_root,
            invocation_id,
            targets,
            invocation["started_at"],
        )
        progress_module.start_tmux_dashboard(
            progress.path, invocation_id, repo_dir
        )
        modern_results: dict[str, dict[str, Any]] = {}
        latest_combined: dict[str, dict[str, Any]] = {}

        def checkpoint_invocation(
            phase_results: dict[str, dict[str, Any]] | None = None,
            *,
            status: str = "running",
            error: str | None = None,
        ) -> None:
            nonlocal latest_combined
            if phase_results is not None:
                latest_combined = phase_results
            rows = []
            phases_by_handle = phase_results or {}
            for handle in targets:
                phases = phases_by_handle.get(handle)
                if phases is None and handle in modern_results:
                    phases = {"modern": modern_results[handle]}
                phases = phases or {}
                target_status = phases.get("status")
                if not target_status:
                    if status != "running":
                        target_status = status
                    else:
                        target_status = "running" if phases else "pending"
                rows.append(
                    {
                        "requested_handle": handle,
                        "status": target_status,
                        "phases": phases,
                    }
                )
            invocation["status"] = status
            invocation["results"] = rows
            invocation["updated_at"] = iso_utc(utc_now())
            if status != "running":
                invocation["completed_at"] = invocation["updated_at"]
            if error:
                invocation["error"] = error
            atomic_write_json(invocation_path, invocation)

        checkpoint_invocation()
        combined: dict[str, dict[str, Any]] = {}
        current_handle: str | None = None
        try:
            with exclusive_lock(repo_dir / "state" / "locks" / "archive-x.lock"), \
                 exclusive_lock(archive_root / "_state" / "archive-x.lock"):
                finalized_invocations = finalize_abandoned_invocations(
                    archive_root,
                    current_invocation_id=invocation_id,
                    current_started_at=invocation["started_at"],
                    recovered_at=iso_utc(utc_now()),
                )
                if finalized_invocations:
                    invocation["finalized_abandoned_invocations"] = (
                        finalized_invocations
                    )
                    checkpoint_invocation()
                unified = importlib.import_module("archive_x_unified")
                for index, handle in enumerate(targets):
                    if index:
                        sleep_random(args.user_delay, f"before user {handle}")
                    current_handle = handle
                    progress.event(
                        handle, phase="modern", phase_status="running",
                        activity="archiving authored timeline", force=True,
                        active=True,
                    )
                    try:
                        result = archive_user(
                            args, repo_dir, archive_root, handle, version
                        )
                    except ArchiveError as exc:
                        result = {
                            "status": "failed",
                            "failure_stage": "modern",
                            "error": str(exc),
                        }
                    modern_results[handle] = result
                    progress.event(
                        handle, phase="modern",
                        phase_status=str(result.get("status", "failed")),
                        activity="authored timeline checkpointed",
                        progress=True, force=True,
                    )
                    checkpoint_invocation(combined)
                    if result["status"] not in {
                        "success",
                        "complete_with_unavailable_media",
                        "partial",
                        "limited",
                        "stalled",
                    }:
                        print(
                            f"Modern archive phase for {handle} ended with status "
                            f"{result['status']}."
                        )

                    def checkpoint_account(
                        account_results: dict[str, dict[str, Any]],
                    ) -> None:
                        combined.update(account_results)
                        checkpoint_invocation(combined)

                    account_result = unified.run_unified_followups(
                        args,
                        repo_dir,
                        archive_root,
                        version,
                        {handle: result},
                        checkpoint=checkpoint_account,
                        progress=progress,
                    )
                    combined.update(account_result)
                    checkpoint_invocation(combined)
                    current_handle = None
        except KeyboardInterrupt:
            if current_handle and current_handle not in modern_results:
                modern_results[current_handle] = {
                    "status": "interrupted",
                    "failure_stage": "modern",
                }
            progress.finalize("interrupted")
            invocation["progress"] = progress.snapshot()
            checkpoint_invocation(latest_combined or None, status="interrupted")
            print("Interrupted; partial phase state, logs, and invocation were retained.")
            return 130
        except (ArchiveError, OSError) as exc:
            progress.finalize("failed")
            invocation["progress"] = progress.snapshot()
            checkpoint_invocation(
                latest_combined or None,
                status="failed",
                error=str(exc),
            )
            raise
        except Exception as exc:
            # Preserve the original traceback for unexpected programming or
            # dependency failures, but never leave the durable invocation and
            # its --exit-when-final dashboard claiming the worker is running.
            progress.finalize("failed")
            invocation["progress"] = progress.snapshot()
            checkpoint_invocation(
                latest_combined or None,
                status="failed",
                error=f"{exc.__class__.__name__}: {exc}",
            )
            raise

        results = []
        for handle in targets:
            target = combined.get(handle) or {
                "modern": modern_results[handle],
                "status": "failed",
            }
            results.append(
                {
                    "requested_handle": handle,
                    "status": target.get("status", "failed"),
                    "phases": target,
                }
            )
        unsuccessful = [
            result
            for result in results
            if result["status"]
            not in {"success", "limited", "complete_with_unavailable_media"}
        ]
        final_status = "failed" if unsuccessful else (
            "limited"
            if any(result["status"] == "limited" for result in results)
            else (
                "complete_with_unavailable_media"
                if any(
                    result["status"] == "complete_with_unavailable_media"
                    for result in results
                )
                else "success"
            )
        )
        progress.finalize(final_status)
        invocation["progress"] = progress.snapshot()
        checkpoint_invocation(combined, status=final_status)
        print_invocation_summary(results)
        return 1 if unsuccessful or len(results) < len(targets) else 0
    except ArchiveError as exc:
        parser.exit(2, f"archive-x: {exc}\n")
    except OSError as exc:
        parser.exit(2, f"archive-x: filesystem or process error: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
