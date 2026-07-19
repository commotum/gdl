#!/usr/bin/env python3
"""Conservative, resumable X archival wrapper around gallery-dl."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import mimetypes
import os
import random
import re
import shlex
import signal
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse

try:
    import fcntl
except ImportError:  # pragma: no cover - this repo targets macOS/Linux
    fcntl = None


SCHEMA_NAME = "gdl-x-archive"
SCHEMA_VERSION = 1
MIN_GALLERY_DL = (1, 32, 0)
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

`posted_at` is the target account's timeline-event timestamp. For a repost,
`reposted_at` is that event time and `original_posted_at` is the original
author's post time. `first_captured_at` and `last_captured_at` describe archive
observations. Engagement metrics are point-in-time values, not historical
totals. Raw per-run JSONL and logs live under `../runs/` and remain the source
of truth.
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
) -> dict[str, Any]:
    postprocessors: list[dict[str, Any]] = []
    if checksums:
        postprocessors.append(
            {"name": "hash", "mode": "sha256", "event": "file"}
        )
    postprocessors.extend(
        (
            {
                "name": "metadata",
                "event": "file",
                "mtime": True,
                "sort": True,
            },
            {
                "name": "metadata",
                "mode": "jsonl",
                "event": "post",
                "base-directory": str(raw_partial.parent),
                "filename": raw_partial.name,
                "exclude": ["local_path", "media_url"],
                "sort": True,
            },
        )
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
        "videos": True,
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
    try:
        lines = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return failed_downloads, other_error_count
    with lines:
        for line in lines:
            failure = download_failure_from_line(line)
            if failure:
                failed_downloads.append(failure)
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

    def observe(line: str) -> None:
        nonlocal resume_cursor, checkpoint_cursor, other_error_count
        if match := CURSOR_RE.search(line):
            resume_cursor = match.group(1).strip()
        elif match := CHECKPOINT_CURSOR_RE.search(line):
            checkpoint_cursor = match.group(1).strip()
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
            print(f"[{prefix}] {line}", end="")
            log.write(line)
            observe(line)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        os.chmod(log_path, 0o600)
        log.write("command: " + shlex.join(command) + "\n")
        log.flush()
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
                print(f"[{prefix}] {line}", end="")
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
    if stalled or interrupted:
        # A checkpoint can be newer than gallery-dl's SIGINT cursor, but an
        # earlier checkpoint can also predate several successful pages before
        # a sequence of real HTTP 429 responses.  Compare search progress and
        # keep whichever boundary is demonstrably farther along.
        resume_cursor = prefer_advanced_search_cursor(
            resume_cursor, checkpoint_cursor
        )
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
) -> list[str]:
    command = [
        sys.executable,
        str(repo_dir / "scripts" / "gallery_dl_x_runner.py"),
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
        "30-60",
        "--sleep-429",
        "300",
        "--limit-rate",
        rate_limit,
        "--retries",
        str(retries),
    ]
    if date_after is not None:
        command.extend(("--date-after", iso_utc(date_after)))
    if post_limit is not None:
        command.extend(("--post-range", f"1-{post_limit}"))
    command.append(url)
    return command


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
) -> dict[str, Any]:
    raw_partial = run_dir / "raw" / f"{endpoint}.posts.jsonl.partial"
    config_path = run_dir / f"{endpoint}.gallery-dl.json"
    config = build_gallery_config(
        handle=handle,
        endpoint=endpoint,
        archive_root=archive_root,
        user_dir=user_dir,
        raw_partial=raw_partial,
        cookie_file=args.cookies,
        archive_run_id=archive_run_id,
        archived_at=archived_at,
        request_delay=args.request_delay,
        download_delay=args.download_delay,
        extractor_delay=args.extractor_delay,
        include_reposts=(
            not args.no_reposts
            if include_reposts is None
            else include_reposts
        ),
        checksums=not args.no_checksums,
        cursor=cursor,
    )
    atomic_write_json(config_path, config)
    config_hash = sha256_file(config_path)
    url = target_url or endpoint_url(handle, endpoint)
    command = gallery_command(
        repo_dir,
        config_path,
        date_after=date_after if endpoint == "timeline" else None,
        post_limit=args.post_limit if endpoint == "timeline" else None,
        retries=args.retries if retries is None else retries,
        http_timeout=(
            args.http_timeout if http_timeout is None else http_timeout
        ),
        rate_limit=args.rate_limit,
        url=url,
    )
    print(f"Archiving {handle}: {endpoint} ({url})")
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
            getattr(args, "stalled_rate_limit_cycles", 3)
            if endpoint == "timeline"
            else 0
        ),
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
        "synthetic_resume_cursor": synthetic_cursor,
        "metadata_complete": metadata_complete,
        "failed_downloads": failed_downloads,
        "other_error_count": other_error_count,
        "raw_has_record": raw_has_record,
        "raw_path": str(raw_path.relative_to(user_dir)),
        "config_path": str(config_path.relative_to(user_dir)),
        "config_sha256": config_hash,
        "command": command,
    }


def select_timeline_state(
    args: argparse.Namespace, state: dict[str, Any], started: datetime
) -> tuple[str | None, str, datetime | None]:
    """Select a saved cursor and preserve its original incremental cutoff."""
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
) -> None:
    """Commit crawl progress without discarding an older safe checkpoint."""
    if limited_run:
        # A smoke test must not advance or replace production crawl state.
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
    current = state.get("pending_media")
    records = current if isinstance(current, list) else []
    by_filename = {
        str(record.get("filename")): record.copy()
        for record in records
        if isinstance(record, dict) and record.get("filename")
    }
    for failure in failures:
        filename = Path(str(failure.get("filename") or "")).name
        if not filename:
            continue
        record = by_filename.get(filename, {})
        previous_source = record.get("last_source_run_id")
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
                "attempts": int(record.get("attempts") or 0)
                + (0 if previous_source == source_run_id else 1),
            }
        )
        by_filename[filename] = record
    state["pending_media"] = sorted(
        by_filename.values(), key=lambda record: record["filename"]
    )


def pending_media_is_complete(user_dir: Path, record: dict[str, Any]) -> bool:
    def asset_is_complete(path: Path) -> bool:
        if not path.is_file() or path.stat().st_size <= 0:
            return False
        sidecar = Path(str(path) + ".json")
        return (
            sidecar.is_file()
            and sidecar.stat().st_size > 0
            and isinstance(load_json(sidecar, None), dict)
        )

    media_root = user_dir / "media"
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
    current = state.get("pending_media")
    records = current if isinstance(current, list) else []
    remaining = [
        record
        for record in records
        if isinstance(record, dict)
        and not pending_media_is_complete(user_dir, record)
    ]
    state["pending_media"] = remaining
    return remaining


def recover_download_only_runs(
    state: dict[str, Any], user_dir: Path
) -> list[str]:
    """Migrate older runs whose timeline ended but one asset failed."""
    recovered_value = state.get("recovered_download_only_runs")
    recovered = set(recovered_value if isinstance(recovered_value, list) else ())
    newly_recovered: list[str] = []
    for manifest_path in sorted((user_dir / "runs").glob("*/manifest.json")):
        manifest = load_json(manifest_path, {})
        if not isinstance(manifest, dict) or manifest.get("limited_run"):
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
        previous = str(state.get("last_successful_started_at") or "")
        resume = state.get("resume") if isinstance(state.get("resume"), dict) else None
        resume_started = str(resume.get("started_at") or "") if resume else ""
        if started_at and started_at >= previous and resume_started <= started_at:
            state["last_successful_started_at"] = started_at
            state["last_successful_completed_at"] = observed_at
            state["resume"] = None
        recovered.add(run_id_value)
        newly_recovered.append(run_id_value)

    state["recovered_download_only_runs"] = sorted(recovered)
    prune_completed_pending_media(state, user_dir)
    return newly_recovered


def finalize_abandoned_manifests(
    user_dir: Path, *, recovered_at: str
) -> list[str]:
    """Close stale running manifests after the global archive lock is held."""
    finalized: list[str] = []
    for manifest_path in sorted((user_dir / "runs").glob("*/manifest.json")):
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
) -> list[str]:
    """Recover a search-stage cursor when gallery-dl omitted one on SIGINT."""
    recovered_value = state.get("recovered_stalled_runs")
    recovered = set(recovered_value if isinstance(recovered_value, list) else ())
    newly_recovered: list[str] = []
    candidates: list[tuple[str, dict[str, Any]]] = []

    for manifest_path in sorted((user_dir / "runs").glob("*/manifest.json")):
        manifest = load_json(manifest_path, {})
        if not isinstance(manifest, dict) or manifest.get("limited_run"):
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
        successful_started = str(state.get("last_successful_started_at") or "")
        current = state.get("resume") if isinstance(state.get("resume"), dict) else None
        current_started = str(current.get("started_at") or "") if current else ""
        if candidate_started >= successful_started and candidate_started >= current_started:
            state["resume"] = candidate

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
    current_run_id = run_id(started)
    user_dir = archive_root / "users" / handle
    run_dir = user_dir / "runs" / current_run_id
    state_path = user_dir / "_state" / "state.json"
    user_dir.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=False)
    write_dataset_readme(user_dir)

    state = load_json(state_path, {})
    if not isinstance(state, dict):
        state = {}
    finalized_abandoned_runs = finalize_abandoned_manifests(
        user_dir, recovered_at=iso_utc(started)
    )
    if finalized_abandoned_runs:
        print(
            f"Finalized abandoned run manifest(s) for @{handle}: "
            f"{', '.join(finalized_abandoned_runs)}"
        )
    recovered_stalled_runs = recover_stalled_interrupted_runs(
        state,
        user_dir,
        minimum_waits=getattr(args, "stalled_rate_limit_cycles", 3),
    )
    recovered_runs = recover_download_only_runs(state, user_dir)
    if recovered_stalled_runs or recovered_runs:
        atomic_write_json(state_path, state)
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
    cursor, chain_started_at, date_after = select_timeline_state(
        args, state, started
    )

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
        "limited_run": bool(args.post_limit),
        "retry_failed_only": bool(args.retry_failed_only),
        "finalized_abandoned_runs": finalized_abandoned_runs,
        "recovered_stalled_runs": recovered_stalled_runs,
        "recovered_download_only_runs": recovered_runs,
        "endpoints": [],
    }
    manifest_path = run_dir / "manifest.json"
    atomic_write_json(manifest_path, manifest)

    # Resolve and bind the stable numeric account ID before timeline media can
    # touch this handle's archive.  This fails closed if a handle is recycled.
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
    )
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
    update_profile_dataset(
        user_dir, handle, info_raw, iso_utc(started)
    )

    pending_before = prune_completed_pending_media(state, user_dir)
    retried_post_ids: list[str] = []
    if pending_before:
        retry_post_ids = sorted(
            {
                str(record["post_id"])
                for record in pending_before
                if record.get("post_id")
            }
        )
        for post_id in retry_post_ids:
            try:
                sleep_random(
                    args.endpoint_delay,
                    f"before {handle}:retry-media-{post_id}",
                )
            except KeyboardInterrupt:
                manifest["status"] = "interrupted"
                manifest["completed_at"] = iso_utc(utc_now())
                atomic_write_json(manifest_path, manifest)
                raise
            result = archive_endpoint(
                args=args,
                repo_dir=repo_dir,
                archive_root=archive_root,
                user_dir=user_dir,
                handle=handle,
                endpoint=f"retry-media-{post_id}",
                run_dir=run_dir,
                archive_run_id=current_run_id,
                archived_at=iso_utc(started),
                date_after=None,
                cursor=None,
                target_url=f"https://x.com/{handle}/status/{post_id}",
                retries=max(args.retries, args.media_retries),
                http_timeout=max(args.http_timeout, args.media_timeout),
                # A queued failure may itself be a repost. Recovery must not
                # silently skip it merely because a later invocation chooses
                # --no-reposts for new timeline material.
                include_reposts=True,
            )
            manifest["endpoints"].append(result)
            atomic_write_json(manifest_path, manifest)
            retried_post_ids.append(post_id)
            if result.get("failed_downloads"):
                merge_pending_media(
                    state,
                    result["failed_downloads"],
                    source_run_id=current_run_id,
                    observed_at=iso_utc(utc_now()),
                )
            prune_completed_pending_media(state, user_dir)
            atomic_write_json(state_path, state)
            if result.get("interrupted"):
                manifest["status"] = "interrupted"
                manifest["media_dataset"] = update_media_dataset(
                    user_dir, handle
                )
                manifest["completed_at"] = iso_utc(utc_now())
                atomic_write_json(manifest_path, manifest)
                raise KeyboardInterrupt

    remaining_pending = prune_completed_pending_media(state, user_dir)
    manifest["media_recovery"] = {
        "pending_before": len(pending_before),
        "retried_post_ids": retried_post_ids,
        "pending_after": len(remaining_pending),
    }
    atomic_write_json(state_path, state)

    if args.retry_failed_only:
        manifest["media_dataset"] = update_media_dataset(user_dir, handle)
        manifest["pending_media"] = remaining_pending
        manifest["status"] = "success" if not remaining_pending else "partial"
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        return manifest

    try:
        sleep_random(args.endpoint_delay, f"before {handle}:timeline")
    except KeyboardInterrupt:
        manifest["status"] = "interrupted"
        manifest["completed_at"] = iso_utc(utc_now())
        atomic_write_json(manifest_path, manifest)
        raise

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
    )
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

    manifest["post_dataset"] = update_post_dataset(
        user_dir, handle, timeline_raw, "timeline"
    )
    manifest["media_dataset"] = update_media_dataset(user_dir, handle)

    # Advance crawl state only after raw records have been merged into the
    # derived datasets.  A crash before here retains the prior cursor and
    # safely replays this page instead of skipping records in posts.jsonl.
    update_timeline_state(
        state,
        limited_run=bool(args.post_limit),
        metadata_complete=timeline_complete,
        resume_cursor=timeline_result.get("resume_cursor"),
        handle=handle,
        chain_started_at=chain_started_at,
        date_after=date_after,
        observed_at=iso_utc(utc_now()),
    )
    atomic_write_json(state_path, state)

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
        if timeline_result.get("interrupted"):
            raise KeyboardInterrupt
        return manifest

    if args.post_limit:
        manifest["status"] = "limited"
    else:
        profile_partial = False
        for endpoint in ("avatar", "background"):
            try:
                sleep_random(args.endpoint_delay, f"before {handle}:{endpoint}")
            except KeyboardInterrupt:
                manifest["status"] = "interrupted"
                manifest["media_dataset"] = update_media_dataset(
                    user_dir, handle
                )
                manifest["completed_at"] = iso_utc(utc_now())
                atomic_write_json(manifest_path, manifest)
                raise
            result = archive_endpoint(
                args=args,
                repo_dir=repo_dir,
                archive_root=archive_root,
                user_dir=user_dir,
                handle=handle,
                endpoint=endpoint,
                run_dir=run_dir,
                archive_run_id=current_run_id,
                archived_at=iso_utc(started),
                date_after=None,
                cursor=None,
            )
            manifest["endpoints"].append(result)
            atomic_write_json(manifest_path, manifest)
            if result.get("interrupted"):
                manifest["status"] = "interrupted"
                manifest["media_dataset"] = update_media_dataset(
                    user_dir, handle
                )
                manifest["completed_at"] = iso_utc(utc_now())
                atomic_write_json(manifest_path, manifest)
                raise KeyboardInterrupt
            if result["exit_code"] != 0:
                profile_partial = True
                break
        remaining_pending = prune_completed_pending_media(state, user_dir)
        manifest["pending_media"] = remaining_pending
        manifest["status"] = (
            "partial" if profile_partial or remaining_pending else "success"
        )

    manifest["media_dataset"] = update_media_dataset(user_dir, handle)
    manifest["pending_media"] = prune_completed_pending_media(state, user_dir)
    atomic_write_json(state_path, state)
    manifest["completed_at"] = iso_utc(utc_now())
    atomic_write_json(manifest_path, manifest)
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
    if args.output_root is None and not os.environ.get("GDL_X_ARCHIVE_ROOT"):
        print("note: the real run will require Bibliotheque mounted read-write")
    print(f"cookie file: {args.cookies} (values not displayed)")
    print(f"users ({len(targets)}): {', '.join(targets)}")
    print("identity/profile endpoint first: info (stable user-ID guard)")
    if args.retry_failed_only:
        print("mode: retry recorded incomplete media; no timeline crawl")
        print(
            "failed-media recovery: "
            f"{max(args.retries, args.media_retries)} retries, "
            f"{max(args.http_timeout, args.media_timeout)}s inactivity timeout"
        )
    else:
        print("main endpoint: /USER/timeline (with replies + older search backfill)")
        print("profile media endpoints after timeline: avatar, background")
    print(f"reposts: {'included and labeled' if not args.no_reposts else 'excluded'}")
    print("quoted-source media: excluded")
    print("non-repost reply-thread context: excluded by numeric author ID")
    if not args.no_reposts:
        print("repost attribution: best effort where X omits wrapper-author identity")
    print(f"request delay: {args.request_delay}s")
    print(f"download delay: {args.download_delay}s")
    print(
        "no-progress watchdog: stop after "
        f"{args.stalled_rate_limit_cycles} unchanged rate-limit windows"
    )
    print(f"between users: {args.user_delay}s")
    if args.post_limit:
        print(f"post limit: {args.post_limit} (state will not mark a complete crawl)")


def build_parser(repo_dir: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scripts/archive-x",
        description=(
            "Conservatively archive X posts, replies, reposts, media, profile "
            "metadata, and point-in-time engagement metrics for later analysis."
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
        help="ignore incremental cutoff and any saved cursor",
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
        help="smoke-test limit; limited runs are never marked complete",
    )
    parser.add_argument(
        "--stalled-rate-limit-cycles",
        type=positive_int,
        default=3,
        help=(
            "stop and checkpoint a timeline after this many consecutive "
            "X rate-limit windows without new raw metadata (default: 3)"
        ),
    )
    parser.add_argument(
        "--request-delay",
        type=duration_arg,
        default="4-8",
        help="delay between X extraction requests (default: 4-8)",
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
        default=8,
        help="retries for previously failed media assets (default: 8)",
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
        help="continue to the next user after a failed/partial user run",
    )
    parser.add_argument(
        "--retry-failed-only",
        action="store_true",
        help=(
            "retry recorded incomplete media without crawling the timeline; "
            "completed download-only runs are recovered automatically"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and show the plan without network calls or writes",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    os.umask(0o077)
    repo_dir = Path(__file__).resolve().parent.parent
    parser = build_parser(repo_dir)
    args = parser.parse_args(argv)
    if args.full_rescan and args.since is not None:
        parser.error("--full-rescan and --since cannot be used together")
    if args.retry_failed_only and (
        args.full_rescan or args.since is not None or args.post_limit
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

        archive_root.mkdir(parents=True, exist_ok=True)
        if not os.access(archive_root, os.W_OK | os.X_OK):
            raise ArchiveError(f"archive root is not writable: {archive_root}")

        invocation_started = utc_now()
        invocation_id = run_id(invocation_started)
        results: list[dict[str, Any]] = []
        with exclusive_lock(repo_dir / "state" / "locks" / "archive-x.lock"), \
             exclusive_lock(archive_root / "_state" / "archive-x.lock"):
            for index, handle in enumerate(targets):
                if index:
                    sleep_random(args.user_delay, f"before user {handle}")
                try:
                    result = archive_user(
                        args, repo_dir, archive_root, handle, version
                    )
                except KeyboardInterrupt:
                    print("Interrupted; partial run data and logs were retained.")
                    return 130
                results.append(
                    {
                        "requested_handle": handle,
                        "run_id": result["run_id"],
                        "status": result["status"],
                    }
                )
                if result["status"] not in {"success", "limited"}:
                    print(
                        f"Archive for {handle} ended with status "
                        f"{result['status']}."
                    )
                    if not args.keep_going:
                        break

        invocation = {
            "schema": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "invocation_id": invocation_id,
            "started_at": iso_utc(invocation_started),
            "completed_at": iso_utc(utc_now()),
            "gallery_dl_version": version,
            "results": results,
        }
        atomic_write_json(archive_root / "runs" / f"{invocation_id}.json", invocation)
        unsuccessful = [
            result
            for result in results
            if result["status"] not in {"success", "limited"}
        ]
        return 1 if unsuccessful or len(results) < len(targets) else 0
    except ArchiveError as exc:
        parser.exit(2, f"archive-x: {exc}\n")
    except OSError as exc:
        parser.exit(2, f"archive-x: filesystem or process error: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
