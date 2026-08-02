#!/usr/bin/env python3
"""Direct, descriptor-backed media transfer for the unified X archive."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urljoin

import requests

import archive_x
import archive_x_context as context_x
import archive_x_descriptors as descriptor_x
import archive_x_local as local_x
import archive_x_request_telemetry as request_telemetry


PARTIAL_SCHEMA = "gdl-x-direct-media-partial"
PARTIAL_SCHEMA_VERSION = 1
DIRECT_SIDECAR_VERSION = 1
CHUNK_BYTES = 256 * 1024
MAX_REDIRECTS = 5
DEFAULT_MIN_FREE_BYTES = context_x.MIN_CONTEXT_MEDIA_FREE_BYTES
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 300.0
REDIRECT_STATUSES = {301, 302, 303, 307, 308}
REFRESH_STATUSES = {403, 404, 410}
RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}
CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+|\*)\Z")
RATE_RE = re.compile(r"(\d+(?:\.\d+)?)([KMG]?)\Z", re.IGNORECASE)


class DirectMediaError(archive_x.ArchiveError):
    """A sanitized transfer failure with one durable queue action."""

    def __init__(
        self,
        error_class: str,
        *,
        action: str = "retryable",
        status: int | None = None,
        count_attempt: bool = True,
    ) -> None:
        if action not in {"retryable", "needs_refresh", "manual_review"}:
            raise ValueError("invalid direct-media failure action")
        self.error_class = error_class
        self.action = action
        self.status = status
        self.count_attempt = count_attempt
        detail = error_class + (f":status={status}" if status is not None else "")
        super().__init__(detail)


@dataclass(frozen=True)
class TransferEvidence:
    relative_path: str
    sha256: str
    byte_count: int
    stat_result: os.stat_result
    network_used: bool
    resumed: bool = False
    portable_record: dict[str, Any] | None = None


def parse_rate_limit(value: str | int | float | None) -> float | None:
    if value in (None, "", 0, 0.0, "0"):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value < 0:
            raise ValueError("media rate limit must be nonnegative")
        return float(value) or None
    match = RATE_RE.fullmatch(str(value).strip())
    if not match:
        raise ValueError("media rate limit must be a number with K, M, or G")
    multiplier = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[
        match.group(2).upper()
    ]
    result = float(match.group(1)) * multiplier
    return result or None


def _destination(
    archive_root: Path, user_dir: Path, relative_path: str
) -> Path:
    relative = Path(str(relative_path or ""))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise DirectMediaError("unsafe_destination", action="manual_review")
    archive_root = archive_root.resolve()
    user_media = (user_dir / "media").resolve()
    parent = (archive_root / relative.parent).resolve()
    if user_media != parent and user_media not in parent.parents:
        raise DirectMediaError("unsafe_destination", action="manual_review")
    return parent / relative.name


def _partial_paths(work_dir: Path, job: dict[str, Any]) -> tuple[Path, Path]:
    token = str(job["descriptor_sha256"])[:16]
    stem = f"asset-{int(job['asset_id'])}-{token}"
    return work_dir / f"{stem}.part", work_dir / f"{stem}.json"


def _safe_unlink(paths: Iterable[Path]) -> None:
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _write_partial_state(
    state_path: Path,
    job: dict[str, Any],
    part_path: Path,
    digest: str,
    *,
    validator: str | None,
    complete: bool,
) -> None:
    archive_x.atomic_write_json(
        state_path,
        {
            "schema": PARTIAL_SCHEMA,
            "schema_version": PARTIAL_SCHEMA_VERSION,
            "asset_id": int(job["asset_id"]),
            "descriptor_id": int(job["descriptor_id"]),
            "descriptor_sha256": str(job["descriptor_sha256"]),
            "expected_relative_path": str(job["expected_relative_path"]),
            "bytes": part_path.stat().st_size,
            "sha256": digest,
            "validator": validator,
            "complete": bool(complete),
        },
    )


def _load_partial(
    state_path: Path, part_path: Path, job: dict[str, Any]
) -> dict[str, Any] | None:
    state = archive_x.load_json(state_path, None)
    if not isinstance(state, dict) or not part_path.is_file():
        return None
    expected = {
        "schema": PARTIAL_SCHEMA,
        "schema_version": PARTIAL_SCHEMA_VERSION,
        "asset_id": int(job["asset_id"]),
        "descriptor_id": int(job["descriptor_id"]),
        "descriptor_sha256": str(job["descriptor_sha256"]),
        "expected_relative_path": str(job["expected_relative_path"]),
    }
    if any(state.get(key) != value for key, value in expected.items()):
        return None
    size = state.get("bytes")
    digest = state.get("sha256")
    validator = state.get("validator")
    if (
        not isinstance(size, int)
        or size < 1
        or part_path.stat().st_size != size
        or not isinstance(digest, str)
        or not descriptor_x.SHA256_RE.fullmatch(digest)
        or (validator is not None and not isinstance(validator, str))
        or (isinstance(validator, str) and len(validator) > 1024)
        or archive_x.sha256_file(part_path) != digest
    ):
        return None
    return state


def _sidecar_matches_job(metadata: dict[str, Any], job: dict[str, Any]) -> bool:
    if job["owner_kind"] == "post":
        try:
            ordinal_matches = int(metadata.get("num")) == int(
                job["media_ordinal"]
            )
        except (TypeError, ValueError):
            ordinal_matches = False
        return (
            archive_x.id_string(metadata.get("tweet_id")) == job["owner_id"]
            and ordinal_matches
        )
    expected = (
        "avatar"
        if job["owner_kind"] == "profile_avatar"
        else "background"
    )
    return metadata.get("subcategory") == expected


def verify_existing(
    archive_root: Path,
    user_dir: Path,
    job: dict[str, Any],
    *,
    requested_handle: str | None = None,
) -> TransferEvidence | None:
    final_path = _destination(
        archive_root, user_dir, str(job["expected_relative_path"])
    )
    sidecar_path = Path(str(final_path) + ".json")
    if not final_path.is_file() or final_path.stat().st_size < 1:
        return None
    metadata = archive_x.load_json(sidecar_path, None)
    if not isinstance(metadata, dict) or not _sidecar_matches_job(metadata, job):
        return None
    digest = metadata.get("sha256")
    if (
        not isinstance(digest, str)
        or not descriptor_x.SHA256_RE.fullmatch(digest)
        or archive_x.sha256_file(final_path) != digest
    ):
        return None
    stat_result = final_path.stat()
    if requested_handle is None:
        _account_id, requested_handle = context_x.target_identity(user_dir)
    portable_record = local_x.portable_media_record(
        metadata,
        user_dir=user_dir,
        requested_handle=requested_handle,
        asset_path=final_path,
        sidecar_path=sidecar_path,
    )
    return TransferEvidence(
        relative_path=str(job["expected_relative_path"]),
        sha256=digest,
        byte_count=stat_result.st_size,
        stat_result=stat_result,
        network_used=False,
        portable_record=portable_record,
    )


def _integer_id(value: Any) -> int:
    normalized = archive_x.id_string(value)
    return int(normalized) if normalized and normalized.isdecimal() else 0


def build_sidecar(
    job: dict[str, Any],
    *,
    account_id: str,
    account_handle: str,
    final_path: Path,
    digest: str,
    byte_count: int,
) -> dict[str, Any]:
    profile = job["owner_kind"] != "post"
    subcategory = (
        "avatar"
        if job["owner_kind"] == "profile_avatar"
        else "background" if profile else str(job["source_operation"])
    )
    author_id = str(job.get("author_id") or account_id)
    author_handle = str(job.get("author_handle") or account_handle)
    return {
        "archive_schema": archive_x.SCHEMA_NAME,
        "archive_schema_version": archive_x.SCHEMA_VERSION,
        "archive_direct_media_version": DIRECT_SIDECAR_VERSION,
        "archive_descriptor_sha256": str(job["descriptor_sha256"]),
        "archived_at": str(job["captured_at"]),
        "subcategory": subcategory,
        "tweet_id": 0 if profile else _integer_id(job["owner_id"]),
        "conversation_id": _integer_id(job.get("conversation_id")),
        "reply_id": _integer_id(job.get("reply_id")),
        "retweet_id": _integer_id(job.get("retweet_id")),
        "num": int(job["media_ordinal"]),
        "type": str(job["media_type"]),
        "extension": str(job["extension"]),
        "date": job.get("posted_at"),
        "date_original": job.get("original_posted_at"),
        "author": {"id": _integer_id(author_id), "name": author_handle},
        "user": {"id": _integer_id(account_id), "name": account_handle},
        "width": job.get("width"),
        "height": job.get("height"),
        "duration": job.get("duration_seconds"),
        "bitrate": job.get("bitrate"),
        "description": job.get("alt_text"),
        "media_url": str(job["private_url"]),
        "local_path": str(final_path),
        "sha256": digest,
        "bytes": byte_count,
    }


def _response_validator(response: Any) -> str | None:
    etag = response.headers.get("ETag")
    if etag and len(etag) <= 1024:
        return str(etag)
    modified = response.headers.get("Last-Modified")
    return str(modified) if modified and len(modified) <= 1024 else None


def _request(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str],
    timeout: tuple[float, float],
) -> Any:
    current = descriptor_x.validate_media_url(url)
    for redirect_count in range(MAX_REDIRECTS + 1):
        try:
            response = session.get(
                current,
                headers=headers,
                stream=True,
                timeout=timeout,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise DirectMediaError(
                "cdn_network_error", action="retryable"
            ) from exc
        if response.status_code not in REDIRECT_STATUSES:
            return response
        location = response.headers.get("Location")
        response.close()
        if not location:
            raise DirectMediaError("cdn_redirect_missing", action="manual_review")
        try:
            current = descriptor_x.validate_media_url(urljoin(current, location))
        except descriptor_x.DescriptorError as exc:
            raise DirectMediaError(
                "cdn_redirect_origin", action="manual_review"
            ) from exc
        if redirect_count == MAX_REDIRECTS:
            raise DirectMediaError("cdn_redirect_loop", action="retryable")
    raise DirectMediaError("cdn_redirect_loop", action="retryable")


def _classify_response(response: Any, *, resumed: bool) -> None:
    status = int(response.status_code)
    if status in {200, 206}:
        if status == 206 and not resumed:
            raise DirectMediaError(
                "unexpected_partial_response", action="manual_review", status=status
            )
        return
    if status in REFRESH_STATUSES:
        raise DirectMediaError(
            "descriptor_rejected",
            action="needs_refresh",
            status=status,
        )
    if status in RETRYABLE_STATUSES:
        raise DirectMediaError("cdn_http_retryable", status=status)
    raise DirectMediaError(
        "cdn_http_unexpected", action="manual_review", status=status
    )


def _content_range(response: Any, resume_at: int) -> int | None:
    value = str(response.headers.get("Content-Range") or "")
    match = CONTENT_RANGE_RE.fullmatch(value)
    if not match or int(match.group(1)) != resume_at:
        raise DirectMediaError("invalid_content_range", action="manual_review")
    end = int(match.group(2))
    total = None if match.group(3) == "*" else int(match.group(3))
    if end < resume_at or (total is not None and end >= total):
        raise DirectMediaError("invalid_content_range", action="manual_review")
    return total


def _check_content_type(response: Any) -> None:
    encoding = str(response.headers.get("Content-Encoding") or "identity").lower()
    if encoding not in {"", "identity"}:
        raise DirectMediaError("encoded_media_response", action="manual_review")
    content_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0]
    if content_type and not (
        content_type.startswith("image/")
        or content_type.startswith("video/")
        or content_type in {"application/octet-stream", "binary/octet-stream"}
    ):
        raise DirectMediaError("invalid_media_content_type", action="manual_review")


def _persist_incomplete(
    part_path: Path,
    state_path: Path,
    job: dict[str, Any],
    digest: Any,
    validator: str | None,
) -> None:
    if not part_path.is_file() or part_path.stat().st_size < 1:
        return
    _write_partial_state(
        state_path,
        job,
        part_path,
        digest.hexdigest(),
        validator=validator,
        complete=False,
    )


def _download_to_partial(
    session: requests.Session,
    job: dict[str, Any],
    part_path: Path,
    state_path: Path,
    *,
    timeout: tuple[float, float],
    bytes_per_second: float | None,
    min_free_bytes: int,
    disk_free: Callable[[Path], int],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[str, int, bool, bool]:
    state = _load_partial(state_path, part_path, job)
    if state is None:
        _safe_unlink((part_path, state_path))
    elif state.get("complete") is True:
        return str(state["sha256"]), int(state["bytes"]), False, False

    resume_at = int(state["bytes"]) if state and state.get("validator") else 0
    validator = str(state["validator"]) if resume_at else None
    headers: dict[str, str] = {}
    if resume_at:
        headers["Range"] = f"bytes={resume_at}-"
        headers["If-Range"] = validator

    response = _request(
        session,
        str(job["private_url"]),
        headers=headers,
        timeout=timeout,
    )
    try:
        if response.status_code == 416 and resume_at:
            response.close()
            _safe_unlink((part_path, state_path))
            return _download_to_partial(
                session,
                job,
                part_path,
                state_path,
                timeout=timeout,
                bytes_per_second=bytes_per_second,
                min_free_bytes=min_free_bytes,
                disk_free=disk_free,
                monotonic=monotonic,
                sleep=sleep,
            )
        if response.status_code == 200 and resume_at:
            _safe_unlink((part_path, state_path))
            resume_at = 0
            validator = None
        resumed = bool(resume_at and response.status_code == 206)
        _classify_response(response, resumed=resumed)
        _check_content_type(response)
        total_size = _content_range(response, resume_at) if resumed else None
        length_value = response.headers.get("Content-Length")
        try:
            response_length = int(length_value) if length_value is not None else None
        except (TypeError, ValueError) as exc:
            raise DirectMediaError("invalid_content_length") from exc
        if response_length is not None and response_length < 0:
            raise DirectMediaError("invalid_content_length")
        required = response_length or 0
        if disk_free(part_path.parent) - required < min_free_bytes:
            raise DirectMediaError(
                "low_disk", count_attempt=False, action="retryable"
            )

        digest = hashlib.sha256()
        if resumed:
            with part_path.open("rb") as existing:
                for chunk in iter(lambda: existing.read(1024 * 1024), b""):
                    digest.update(chunk)
        mode = "ab" if resumed else "wb"
        response_bytes = 0
        started = monotonic()
        current_validator = _response_validator(response) or validator
        try:
            part_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with part_path.open(mode) as stream:
                os.chmod(part_path, 0o600)
                for chunk in response.iter_content(chunk_size=CHUNK_BYTES):
                    if not chunk:
                        continue
                    stream.write(chunk)
                    digest.update(chunk)
                    response_bytes += len(chunk)
                    if bytes_per_second:
                        expected = response_bytes / bytes_per_second
                        wait = expected - (monotonic() - started)
                        if wait > 0:
                            sleep(wait)
                stream.flush()
                os.fsync(stream.fileno())
        except KeyboardInterrupt:
            _persist_incomplete(
                part_path, state_path, job, digest, current_validator
            )
            raise
        except (OSError, requests.RequestException) as exc:
            _persist_incomplete(
                part_path, state_path, job, digest, current_validator
            )
            raise DirectMediaError("cdn_stream_error") from exc

        byte_count = part_path.stat().st_size
        if byte_count < 1:
            raise DirectMediaError("empty_media_response")
        if response_length is not None and response_bytes != response_length:
            _persist_incomplete(
                part_path, state_path, job, digest, current_validator
            )
            raise DirectMediaError("incomplete_media_response")
        if total_size is not None and byte_count != total_size:
            _persist_incomplete(
                part_path, state_path, job, digest, current_validator
            )
            raise DirectMediaError("incomplete_media_response")
        final_digest = digest.hexdigest()
        _write_partial_state(
            state_path,
            job,
            part_path,
            final_digest,
            validator=current_validator,
            complete=True,
        )
        return final_digest, byte_count, resumed, True
    finally:
        response.close()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def transfer_asset(
    session: requests.Session,
    archive_root: Path,
    user_dir: Path,
    work_dir: Path,
    job: dict[str, Any],
    *,
    account_id: str,
    account_handle: str,
    timeout: tuple[float, float] = (30.0, 300.0),
    bytes_per_second: float | None = 8 * 1024 * 1024,
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    disk_free: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> TransferEvidence:
    descriptor_x.validate_media_url(job.get("private_url"))
    final_path = _destination(
        archive_root, user_dir, str(job.get("expected_relative_path") or "")
    )
    work_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "posix":
        os.chmod(work_dir, 0o700)
    part_path, state_path = _partial_paths(work_dir, job)
    digest, byte_count, resumed, network_used = _download_to_partial(
        session,
        job,
        part_path,
        state_path,
        timeout=timeout,
        bytes_per_second=bytes_per_second,
        min_free_bytes=min_free_bytes,
        disk_free=disk_free,
        monotonic=monotonic,
        sleep=sleep,
    )
    final_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    sidecar = build_sidecar(
        job,
        account_id=account_id,
        account_handle=account_handle,
        final_path=final_path,
        digest=digest,
        byte_count=byte_count,
    )
    archive_x.atomic_write_json(Path(str(final_path) + ".json"), sidecar)
    os.replace(part_path, final_path)
    if os.name == "posix":
        os.chmod(final_path, 0o600)
    _fsync_directory(final_path.parent)
    placed_digest = archive_x.sha256_file(final_path)
    if placed_digest != digest or final_path.stat().st_size != byte_count:
        raise DirectMediaError("post_write_verification_failed")
    _safe_unlink((state_path,))
    stat_result = final_path.stat()
    portable_record = local_x.portable_media_record(
        sidecar,
        user_dir=user_dir,
        requested_handle=account_handle,
        asset_path=final_path,
        sidecar_path=Path(str(final_path) + ".json"),
    )
    return TransferEvidence(
        relative_path=str(job["expected_relative_path"]),
        sha256=digest,
        byte_count=byte_count,
        stat_result=stat_result,
        network_used=network_used,
        resumed=resumed,
        portable_record=portable_record,
    )


def _record_success(
    database: context_x.ContextDB,
    job: dict[str, Any],
    evidence: TransferEvidence,
) -> None:
    database.asset_succeeded(
        asset_id=int(job["asset_id"]),
        lease_token=str(job["lease_token"]),
        descriptor_id=int(job["descriptor_id"]),
        final_relative_path=evidence.relative_path,
        sha256=evidence.sha256,
        byte_count=evidence.byte_count,
        stat_result=evidence.stat_result,
        portable_record=evidence.portable_record,
    )


def _gallery_archive_entry(job: dict[str, Any], account_id: str) -> str:
    owner_kind = str(job["owner_kind"])
    if owner_kind == "post":
        return (
            f"{job['owner_id']}_{job.get('retweet_id') or 0}_"
            f"{int(job['media_ordinal'])}"
        )
    posted_at = str(job.get("posted_at") or "")
    try:
        date_value = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DirectMediaError(
            "profile_archive_date_invalid", action="manual_review"
        ) from exc
    prefix = "AV" if owner_kind == "profile_avatar" else "BG"
    return f"{prefix}_{account_id}_{date_value}"


def update_download_archive(
    user_dir: Path, job: dict[str, Any], *, account_id: str
) -> None:
    relative = Path(str(job["expected_relative_path"]))
    context_asset = "context" in relative.parts and "media" in relative.parts
    path = user_dir / "_state" / (
        "context-downloads.sqlite3" if context_asset else "downloads.sqlite3"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS media "
            "(entry TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        connection.execute(
            "INSERT OR IGNORE INTO media(entry) VALUES (?)",
            (_gallery_archive_entry(job, account_id),),
        )
    finally:
        connection.close()
    if os.name == "posix":
        os.chmod(path, 0o600)


def _require_download_archive(
    user_dir: Path, job: dict[str, Any], *, account_id: str
) -> None:
    try:
        update_download_archive(user_dir, job, account_id=account_id)
    except (OSError, sqlite3.Error, DirectMediaError) as exc:
        raise DirectMediaError(
            "download_ledger_write_failed",
            action="retryable",
            count_attempt=False,
        ) from exc


def run_direct_media_worker(
    *,
    archive_root: Path,
    user_dir: Path,
    db_path: Path,
    max_assets: int | None = None,
    lease_seconds: float = 1800.0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    timeout: tuple[float, float] = (30.0, 300.0),
    rate_limit: str | int | float | None = "8M",
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    telemetry_path: Path | None = None,
    session: requests.Session | None = None,
    clock: Callable[[], float] = time.time,
    disk_free: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if max_assets is not None and max_assets < 1:
        raise context_x.ContextError("direct media limit must be positive")
    if lease_seconds <= 0 or max_attempts < 1 or retry_delay < 0:
        raise context_x.ContextError("direct media retry settings are invalid")
    if (
        not isinstance(timeout, tuple)
        or len(timeout) != 2
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) <= 0
            for value in timeout
        )
    ):
        raise context_x.ContextError("direct media timeout must be two positive seconds")
    account_id, account_handle = context_x.target_identity(user_dir)
    bytes_per_second = parse_rate_limit(rate_limit)
    work_dir = user_dir / "_state" / "media-partials"
    telemetry_path = telemetry_path or (
        user_dir / "_state" / "direct-media.requests.json"
    )
    recorder = request_telemetry.RequestRecorder(
        telemetry_path, "direct_media"
    )
    owns_session = session is None
    telemetry_exit_code = 0
    counts: dict[str, Any] = {
        "attempted": 0,
        "captured": 0,
        "existing": 0,
        "downloaded": 0,
        "recovered_partial": 0,
        "resumed": 0,
        "retryable": 0,
        "needs_refresh": 0,
        "manual_review": 0,
        "bytes": 0,
        "ledger_updated": 0,
        "ledger_errors": 0,
    }
    try:
        with recorder.capture():
            if session is None:
                session = requests.Session()
                session.trust_env = False
            with context_x.ContextDB(db_path, create=False) as database:
                database.bind_identity(account_id, account_handle)
                while max_assets is None or counts["attempted"] < max_assets:
                    now = clock()
                    job = database.claim_asset(
                        now=now, lease_seconds=lease_seconds
                    )
                    if job is None:
                        break
                    counts["attempted"] += 1
                    try:
                        evidence = verify_existing(
                            archive_root,
                            user_dir,
                            job,
                            requested_handle=account_handle,
                        )
                        if evidence is not None:
                            _require_download_archive(
                                user_dir, job, account_id=account_id
                            )
                            counts["ledger_updated"] += 1
                            _record_success(database, job, evidence)
                            counts["captured"] += 1
                            counts["existing"] += 1
                            counts["bytes"] += evidence.byte_count
                            continue
                        if disk_free(user_dir) < min_free_bytes:
                            raise DirectMediaError(
                                "low_disk",
                                count_attempt=False,
                                action="retryable",
                            )
                        evidence = transfer_asset(
                            session,
                            archive_root,
                            user_dir,
                            work_dir,
                            job,
                            account_id=account_id,
                            account_handle=account_handle,
                            timeout=timeout,
                            bytes_per_second=bytes_per_second,
                            min_free_bytes=min_free_bytes,
                            disk_free=disk_free,
                            monotonic=monotonic,
                            sleep=sleep,
                        )
                        _require_download_archive(
                            user_dir, job, account_id=account_id
                        )
                        counts["ledger_updated"] += 1
                        _record_success(database, job, evidence)
                        counts["captured"] += 1
                        counts["downloaded"] += int(evidence.network_used)
                        counts["recovered_partial"] += int(
                            not evidence.network_used
                        )
                        counts["resumed"] += int(evidence.resumed)
                        counts["bytes"] += evidence.byte_count
                    except KeyboardInterrupt:
                        database.asset_failed(
                            asset_id=int(job["asset_id"]),
                            lease_token=str(job["lease_token"]),
                            descriptor_id=int(job["descriptor_id"]),
                            state="retryable",
                            error_class="interrupted",
                            detail="interrupted",
                            next_attempt_at=0,
                            count_attempt=False,
                        )
                        raise
                    except DirectMediaError as exc:
                        if exc.error_class == "download_ledger_write_failed":
                            counts["ledger_errors"] += 1
                        state = exc.action
                        if (
                            state == "retryable"
                            and exc.count_attempt
                            and int(job["attempts"]) >= max_attempts
                        ):
                            state = "manual_review"
                        next_at = 0.0
                        if state == "retryable" and exc.count_attempt:
                            exponent = max(0, int(job["attempts"]) - 1)
                            next_at = now + min(
                                retry_delay * (2**exponent), 86400.0
                            )
                        elif state == "retryable":
                            next_at = now + max(retry_delay, 60.0)
                        database.asset_failed(
                            asset_id=int(job["asset_id"]),
                            lease_token=str(job["lease_token"]),
                            descriptor_id=int(job["descriptor_id"]),
                            state=state,
                            error_class=exc.error_class,
                            detail=str(exc),
                            next_attempt_at=next_at,
                            count_attempt=exc.count_attempt,
                        )
                        counts[state] = int(counts.get(state, 0)) + 1
                        if exc.error_class == "low_disk":
                            break
    except BaseException as exc:
        telemetry_exit_code = 130 if isinstance(exc, KeyboardInterrupt) else 1
        raise
    finally:
        if owns_session and session is not None:
            session.close()
        telemetry_error = recorder.safe_write(telemetry_exit_code)
        counts["telemetry_error"] = telemetry_error
        counts["request_telemetry"] = recorder.value(telemetry_exit_code)["summary"]
    with context_x.ContextDB(db_path, create=False) as database:
        counts["remaining"] = database.asset_availability(now=clock())
    return counts
