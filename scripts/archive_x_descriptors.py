#!/usr/bin/env python3
"""Private, validated media descriptor capture for the X archiver.

The gallery-dl postprocessor installed here observes ``prepare`` events only.
It never downloads a file and never changes the outcome of an extraction.
Parent processes revalidate every row and apply authoritative post selection
before a descriptor or asset job can enter SQLite.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit


SCHEMA = "gdl-x-media-descriptor"
SCHEMA_VERSION = 1
POSTPROCESSOR_NAME = "archive_x_descriptor"
ALLOWED_HOSTS = {"pbs.twimg.com", "video.twimg.com", "ton.twimg.com"}
ALLOWED_OPERATIONS = {
    "modern",
    "legacy",
    "context",
    "retry",
    "info",
    "exact_refresh",
}
ALLOWED_SOURCE_KINDS = {
    "modern",
    "legacy",
    "context",
    "info",
    "retry",
    "profile",
    "exact_refresh",
}
ALLOWED_OWNER_KINDS = {"post", "profile_avatar", "profile_background"}
ALLOWED_MEDIA_TYPES = {
    "photo",
    "video",
    "animated_gif",
    "preview",
    "card",
    "article",
    "unknown",
}
EXTENSION_RE = re.compile(r"[A-Za-z0-9]{1,16}\Z")
HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
SNOWFLAKE_EPOCH_MS = 1_288_834_974_657


class DescriptorError(ValueError):
    """A sanitized descriptor artifact or ownership error."""


@dataclass
class DescriptorBatch:
    operation_id: str
    run_id: str
    source_kind: str
    source_operation: str
    rows: tuple[dict[str, Any], ...]
    errors: tuple[str, ...] = ()
    source_relative_path: str | None = None
    source_sha256: str | None = None
    source_record_count: int | None = None
    artifact_path: Path | None = None
    ephemeral: bool = False
    persistence: dict[str, Any] = field(default_factory=dict)

    def safe_summary(self) -> dict[str, Any]:
        return {
            "operation_id_sha256": hashlib.sha256(
                self.operation_id.encode("utf-8")
            ).hexdigest(),
            "rows": len(self.rows),
            "errors": len(self.errors),
            "source_registered": bool(self.source_relative_path),
            "ephemeral": self.ephemeral,
            "persistence": dict(self.persistence),
        }


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_id(value: Any, label: str) -> str:
    text = str(value or "")
    if not text.isdecimal() or int(text) < 1:
        raise DescriptorError(f"descriptor {label} is invalid")
    return text


def _optional_id(value: Any, label: str) -> str | None:
    if value in (None, "", 0, "0"):
        return None
    return _positive_id(value, label)


def _optional_handle(value: Any, label: str) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)
    if not HANDLE_RE.fullmatch(text):
        raise DescriptorError(f"descriptor {label} is invalid")
    return text


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise DescriptorError(f"descriptor {label} is invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise DescriptorError(f"descriptor {label} is invalid") from exc
    if result < 1:
        raise DescriptorError(f"descriptor {label} is invalid")
    return result


def _optional_nonnegative(
    value: Any, label: str, *, integral: bool
) -> int | float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise DescriptorError(f"descriptor {label} is invalid")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise DescriptorError(f"descriptor {label} is invalid") from exc
    if integral and not numeric.is_integer():
        raise DescriptorError(f"descriptor {label} is invalid")
    result: int | float = int(numeric) if integral else numeric
    if result < 0:
        raise DescriptorError(f"descriptor {label} is invalid")
    return result


def _private_url(value: Any) -> tuple[str, str, str]:
    if not isinstance(value, str) or not value:
        raise DescriptorError("descriptor URL is absent")
    parsed = urlsplit(value)
    try:
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise DescriptorError("descriptor URL origin is not allowed") from exc
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or host not in ALLOWED_HOSTS
        or port not in (None, 443)
        or not parsed.path
        or bool(parsed.fragment)
    ):
        raise DescriptorError("descriptor URL origin is not allowed")
    return value, host, sha256_bytes(value.encode("utf-8"))


def validate_media_url(value: Any) -> str:
    """Return an allowed HTTPS media URL without exposing it on failure."""
    return _private_url(value)[0]


def _relative_path(value: Any) -> tuple[str, str, str]:
    if not isinstance(value, str) or not value:
        raise DescriptorError("descriptor destination is absent")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.name in {"", ".", ".."}:
        raise DescriptorError("descriptor destination is unsafe")
    normalized = path.as_posix()
    return normalized, path.name, path.parent.as_posix()


def _media_type(value: Any, extension: str) -> str:
    text = str(value or "").lower()
    aliases = {"image": "photo", "gif": "animated_gif"}
    text = aliases.get(text, text)
    if not text:
        text = "photo" if extension.lower() in {
            "avif", "gif", "jpeg", "jpg", "png", "webp"
        } else "unknown"
    if text not in ALLOWED_MEDIA_TYPES:
        text = "unknown"
    return text


def _variant(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise DescriptorError("descriptor variant is invalid")
    allowed = {
        "type",
        "width",
        "height",
        "duration",
        "bitrate",
        "profile_kind",
    }
    if not set(value) <= allowed:
        raise DescriptorError("descriptor variant fields are invalid")
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if key in {"width", "height", "bitrate"}:
            normalized[key] = _optional_nonnegative(
                item, f"variant {key}", integral=True
            )
        elif key == "duration":
            normalized[key] = _optional_nonnegative(
                item, "variant duration", integral=False
            )
        elif key == "type":
            normalized[key] = _media_type(item, "")
        elif key == "profile_kind":
            text = str(item or "")
            if text not in {"profile_avatar", "profile_background"}:
                raise DescriptorError("descriptor profile variant is invalid")
            normalized[key] = text
    return normalized


def _capture_time(value: Any) -> str:
    if isinstance(value, datetime):
        observed = value
    elif isinstance(value, str) and value:
        try:
            observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DescriptorError("descriptor capture time is invalid") from exc
    else:
        raise DescriptorError("descriptor capture time is absent")
    if observed.tzinfo is None:
        raise DescriptorError("descriptor capture time lacks timezone")
    return observed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _optional_post_time(value: Any, label: str) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        observed = value
    elif isinstance(value, str):
        try:
            observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DescriptorError(f"descriptor {label} is invalid") from exc
    else:
        raise DescriptorError(f"descriptor {label} is invalid")
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return observed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def descriptor_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "owner_kind",
            "owner_id",
            "post_id",
            "media_ordinal",
            "media_type",
            "extension",
            "private_url",
            "url_sha256",
            "url_host",
            "filename",
            "relative_directory",
            "relative_path",
            "width",
            "height",
            "duration_seconds",
            "bitrate",
            "alt_text",
            "variant",
            "posted_at",
            "original_posted_at",
            "author_id",
            "author_handle",
            "conversation_id",
            "reply_id",
            "retweet_id",
        )
    }


def normalize_record(
    value: Any,
    *,
    expected_operation_id: str | None = None,
    expected_run_id: str | None = None,
    expected_source_operation: str | None = None,
    expected_source_kind: str | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DescriptorError("descriptor row is not an object")
    if value.get("schema") != SCHEMA or value.get("schema_version") != SCHEMA_VERSION:
        raise DescriptorError("descriptor schema is invalid")

    operation_id = str(value.get("operation_id") or "")
    run_id = str(value.get("run_id") or "")
    source_operation = str(value.get("source_operation") or "")
    source_kind = str(value.get("source_kind") or "")
    if not operation_id or len(operation_id) > 256 or not run_id or len(run_id) > 256:
        raise DescriptorError("descriptor operation binding is invalid")
    if source_operation not in ALLOWED_OPERATIONS:
        raise DescriptorError("descriptor source operation is invalid")
    if source_kind not in ALLOWED_SOURCE_KINDS:
        raise DescriptorError("descriptor source kind is invalid")
    for observed, expected, label in (
        (operation_id, expected_operation_id, "operation"),
        (run_id, expected_run_id, "run"),
        (source_operation, expected_source_operation, "source operation"),
        (source_kind, expected_source_kind, "source kind"),
    ):
        if expected is not None and observed != expected:
            raise DescriptorError(f"descriptor {label} binding changed")

    owner_kind = str(value.get("owner_kind") or "")
    if owner_kind not in ALLOWED_OWNER_KINDS:
        raise DescriptorError("descriptor owner kind is invalid")
    if owner_kind == "post":
        post_id = _positive_id(value.get("post_id"), "post ID")
        owner_id = post_id
    else:
        post_id = None
        owner_id = str(value.get("owner_id") or "")
        if owner_id != "account":
            raise DescriptorError("profile descriptor owner is invalid")

    ordinal = _positive_int(value.get("media_ordinal"), "media ordinal")
    extension = str(value.get("extension") or "").lower().lstrip(".")
    if not EXTENSION_RE.fullmatch(extension):
        raise DescriptorError("descriptor extension is invalid")
    private_url, host, url_sha256 = _private_url(value.get("private_url"))
    relative_path, filename, relative_directory = _relative_path(
        value.get("relative_path")
    )
    if str(value.get("filename") or "") != filename:
        raise DescriptorError("descriptor filename does not match destination")
    if str(value.get("relative_directory") or "") != relative_directory:
        raise DescriptorError("descriptor directory does not match destination")

    variant = _variant(value.get("variant"))
    alt_text = (
        str(value.get("alt_text"))
        if value.get("alt_text") is not None
        else None
    )
    if alt_text is not None and len(alt_text) > 16_384:
        raise DescriptorError("descriptor alt text is too large")
    row = {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "operation_id": operation_id,
        "run_id": run_id,
        "source_kind": source_kind,
        "source_operation": source_operation,
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "post_id": post_id,
        "media_ordinal": ordinal,
        "media_type": _media_type(value.get("media_type"), extension),
        "extension": extension,
        "private_url": private_url,
        "url_sha256": url_sha256,
        "url_host": host,
        "filename": filename,
        "relative_directory": relative_directory,
        "relative_path": relative_path,
        "width": _optional_nonnegative(value.get("width"), "width", integral=True),
        "height": _optional_nonnegative(value.get("height"), "height", integral=True),
        "duration_seconds": _optional_nonnegative(
            value.get("duration_seconds"), "duration", integral=False
        ),
        "bitrate": _optional_nonnegative(value.get("bitrate"), "bitrate", integral=True),
        "alt_text": alt_text,
        "variant": variant,
        "posted_at": _optional_post_time(value.get("posted_at"), "post time"),
        "original_posted_at": _optional_post_time(
            value.get("original_posted_at"), "original post time"
        ),
        "author_id": _optional_id(value.get("author_id"), "author ID"),
        "author_handle": _optional_handle(
            value.get("author_handle"), "author handle"
        ),
        "conversation_id": _optional_id(
            value.get("conversation_id"), "conversation ID"
        ),
        "reply_id": _optional_id(value.get("reply_id"), "reply ID"),
        "retweet_id": _optional_id(value.get("retweet_id"), "repost ID"),
        "captured_at": _capture_time(value.get("captured_at")),
    }
    observed_digest = str(value.get("descriptor_sha256") or "")
    expected_digest = sha256_bytes(canonical_json(descriptor_payload(row)).encode("utf-8"))
    if observed_digest != expected_digest:
        raise DescriptorError("descriptor digest is invalid")
    row["descriptor_sha256"] = expected_digest
    return row


def _safe_relative(path: Path, parent: Path) -> str:
    try:
        relative = path.resolve().relative_to(parent.resolve())
    except (OSError, ValueError) as exc:
        raise DescriptorError("descriptor artifact is outside its archive") from exc
    return relative.as_posix()


def prepare_artifact(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    if os.name == "posix":
        os.chmod(path, 0o600)


def finalize_artifact(partial: Path, *, complete: bool) -> Path:
    if not partial.exists():
        prepare_artifact(partial)
    base = partial.name.removesuffix(".partial")
    if base.endswith(".jsonl"):
        base = base[:-6]
    destination = partial.with_name(
        base + (".jsonl" if complete else ".incomplete.jsonl")
    )
    os.replace(partial, destination)
    if os.name == "posix":
        os.chmod(destination, 0o600)
    return destination


def load_artifact(
    path: Path,
    *,
    user_dir: Path,
    operation_id: str,
    run_id: str,
    source_kind: str,
    source_operation: str,
    ephemeral: bool = False,
) -> DescriptorBatch:
    relative = _safe_relative(path, user_dir)
    if os.name == "posix":
        os.chmod(path, 0o600)
    digest = sha256_file(path)
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    seen: set[tuple[str, str, int, str]] = set()
    with path.open("rb") as stream:
        for line_number, raw_line in enumerate(stream, 1):
            if not raw_line.strip():
                continue
            try:
                line = raw_line.decode("utf-8", errors="strict")
                value = json.loads(line)
                row = normalize_record(
                    value,
                    expected_operation_id=operation_id,
                    expected_run_id=run_id,
                    expected_source_operation=source_operation,
                    expected_source_kind=source_kind,
                )
                key = (
                    row["owner_kind"],
                    row["owner_id"],
                    row["media_ordinal"],
                    row["descriptor_sha256"],
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
            except (DescriptorError, json.JSONDecodeError, UnicodeError) as exc:
                errors.append(f"line {line_number}: {exc.__class__.__name__}")
    return DescriptorBatch(
        operation_id=operation_id,
        run_id=run_id,
        source_kind=source_kind,
        source_operation=source_operation,
        rows=tuple(rows),
        errors=tuple(errors),
        source_relative_path=None if ephemeral else relative,
        source_sha256=digest,
        source_record_count=len(rows) + len(errors),
        artifact_path=path,
        ephemeral=ephemeral,
    )


def postprocessor_config(
    *,
    artifact_path: Path,
    archive_root: Path,
    operation_id: str,
    run_id: str,
    source_kind: str,
    source_operation: str,
    owner_kind: str = "post",
) -> dict[str, Any]:
    if not artifact_path.is_absolute() or not archive_root.is_absolute():
        raise DescriptorError("descriptor capture paths must be absolute")
    if (
        not operation_id
        or len(operation_id) > 256
        or not run_id
        or len(run_id) > 256
    ):
        raise DescriptorError("descriptor operation binding is invalid")
    if source_kind not in ALLOWED_SOURCE_KINDS:
        raise DescriptorError("descriptor source kind is invalid")
    if source_operation not in ALLOWED_OPERATIONS:
        raise DescriptorError("descriptor source operation is invalid")
    if owner_kind not in ALLOWED_OWNER_KINDS:
        raise DescriptorError("descriptor owner kind is invalid")
    return {
        "name": POSTPROCESSOR_NAME,
        "event": "prepare",
        "artifact-path": str(artifact_path),
        "archive-root": str(archive_root),
        "operation-id": operation_id,
        "run-id": run_id,
        "source-kind": source_kind,
        "source-operation": source_operation,
        "owner-kind": owner_kind,
    }


def _event_record(pathfmt: Any, options: dict[str, Any]) -> dict[str, Any]:
    kwdict = pathfmt.kwdict
    author = kwdict.get("author")
    if not isinstance(author, dict):
        author = {}
    owner_kind = options["owner-kind"]
    if owner_kind == "post":
        post_id = _positive_id(kwdict.get("tweet_id"), "post ID")
        owner_id = post_id
        ordinal = _positive_int(kwdict.get("num"), "media ordinal")
    else:
        post_id = None
        owner_id = "account"
        ordinal = 1

    extension = str(kwdict.get("extension") or "").lower().lstrip(".")
    filename = pathfmt.build_filename(kwdict)
    archive_root = Path(options["archive-root"]).resolve()
    final_path = (Path(pathfmt.realdirectory) / filename).resolve()
    try:
        relative_path = final_path.relative_to(archive_root).as_posix()
    except ValueError as exc:
        raise DescriptorError("descriptor destination is outside archive root") from exc
    private_url, host, url_sha256 = _private_url(kwdict.get("media_url"))
    row = {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "operation_id": options["operation-id"],
        "run_id": options["run-id"],
        "source_kind": options["source-kind"],
        "source_operation": options["source-operation"],
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "post_id": post_id,
        "media_ordinal": ordinal,
        "media_type": _media_type(kwdict.get("type"), extension),
        "extension": extension,
        "private_url": private_url,
        "url_sha256": url_sha256,
        "url_host": host,
        "filename": filename,
        "relative_directory": str(Path(relative_path).parent.as_posix()),
        "relative_path": relative_path,
        "width": kwdict.get("width"),
        "height": kwdict.get("height"),
        "duration_seconds": kwdict.get("duration"),
        "bitrate": kwdict.get("bitrate"),
        "alt_text": kwdict.get("description"),
        "variant": {
            key: kwdict.get(key)
            for key in ("type", "width", "height", "duration", "bitrate")
            if kwdict.get(key) is not None
        },
        "posted_at": _optional_post_time(kwdict.get("date"), "post time"),
        "original_posted_at": _optional_post_time(
            kwdict.get("date_original"), "original post time"
        ),
        "author_id": _optional_id(author.get("id"), "author ID"),
        "author_handle": _optional_handle(
            author.get("name"), "author handle"
        ),
        "conversation_id": _optional_id(
            kwdict.get("conversation_id"), "conversation ID"
        ),
        "reply_id": _optional_id(kwdict.get("reply_id"), "reply ID"),
        "retweet_id": _optional_id(kwdict.get("retweet_id"), "repost ID"),
        "captured_at": str(
            kwdict.get("archived_at")
            or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        ),
    }
    row["descriptor_sha256"] = sha256_bytes(
        canonical_json(descriptor_payload(row)).encode("utf-8")
    )
    return normalize_record(row)


def install_postprocessor() -> None:
    import gallery_dl.postprocessor as postprocessor
    from gallery_dl.postprocessor.common import PostProcessor

    existing = postprocessor._cache.get(POSTPROCESSOR_NAME)  # noqa: SLF001
    if existing is not None:
        if getattr(existing, "_archive_x_descriptor", False):
            return
        raise DescriptorError("descriptor postprocessor name is already registered")

    class ArchiveXDescriptorPP(PostProcessor):
        _archive_x_descriptor = True

        def __init__(self, job: Any, options: dict[str, Any]):
            super().__init__(job)
            self.options = dict(options)
            self.path = Path(str(options.get("artifact-path") or ""))
            self.warned = False
            if not self.path.is_absolute():
                raise DescriptorError("descriptor artifact path must be absolute")
            # Revalidate non-secret binding options before gallery-dl starts.
            postprocessor_config(
                artifact_path=self.path,
                archive_root=Path(str(options.get("archive-root") or "")),
                operation_id=str(options.get("operation-id") or ""),
                run_id=str(options.get("run-id") or ""),
                source_kind=str(options.get("source-kind") or ""),
                source_operation=str(options.get("source-operation") or ""),
                owner_kind=str(options.get("owner-kind") or ""),
            )
            if not options.get("operation-id") or not options.get("run-id"):
                raise DescriptorError("descriptor operation binding is absent")
            job.register_hooks({"prepare": self.run}, options)

        def run(self, pathfmt: Any) -> None:
            try:
                row = _event_record(pathfmt, self.options)
                with self.path.open("a", encoding="utf-8") as stream:
                    stream.write(canonical_json(row) + "\n")
                if os.name == "posix":
                    os.chmod(self.path, 0o600)
            except Exception as exc:  # descriptor loss cannot fail metadata
                if not self.warned:
                    self.warned = True
                    self.log.warning(
                        "Archive descriptor capture skipped an event (%s)",
                        exc.__class__.__name__,
                    )

    postprocessor._cache[POSTPROCESSOR_NAME] = ArchiveXDescriptorPP  # noqa: SLF001


def _utc_filename(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


def _avatar_date(url: str, fallback: datetime) -> datetime:
    try:
        identifier = int(urlsplit(url).path.rsplit("/", 2)[-2])
        milliseconds = (identifier >> 22) + SNOWFLAKE_EPOCH_MS
        value = datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
        if 2006 <= value.year <= 2100:
            return value
    except (ValueError, OverflowError, OSError):
        pass
    return fallback


def _banner_date(url: str, fallback: datetime) -> datetime:
    try:
        seconds = int(urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1])
        value = datetime.fromtimestamp(seconds, tz=timezone.utc)
        if 2006 <= value.year <= 2100:
            return value
    except (ValueError, OverflowError, OSError):
        pass
    return fallback


def profile_batch_from_info(
    metadata: dict[str, Any],
    *,
    user_dir: Path,
    operation_id: str,
    run_id: str,
    captured_at: str,
    source_relative_path: str,
    source_sha256: str,
) -> DescriptorBatch:
    canonical_handle = str(metadata.get("name") or user_dir.name)
    if not HANDLE_RE.fullmatch(canonical_handle):
        raise DescriptorError("profile descriptor handle is invalid")
    captured_at = _capture_time(captured_at)
    fallback = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    relative_directory = (
        Path("users") / user_dir.name / "media" / "profile"
    ).as_posix()

    candidates: Iterable[tuple[str, Any, Any, str]] = (
        ("profile_avatar", metadata.get("profile_image"), _avatar_date, "jpg"),
        ("profile_background", metadata.get("profile_banner"), _banner_date, "jpg"),
    )
    for owner_kind, raw_url, date_parser, default_extension in candidates:
        if not raw_url:
            continue
        try:
            url = str(raw_url)
            if owner_kind == "profile_avatar":
                url = url.replace("_normal.", ".")
            private_url, host, url_sha256 = _private_url(url)
            extension = Path(urlsplit(url).path).suffix.lstrip(".").lower()
            if not EXTENSION_RE.fullmatch(extension):
                extension = default_extension
            date = date_parser(url, fallback)
            stem = (
                "profile-avatar"
                if owner_kind == "profile_avatar"
                else "profile-background"
            )
            filename = f"{stem}_{_utc_filename(date)}_{canonical_handle}.{extension}"
            row = {
                "schema": SCHEMA,
                "schema_version": SCHEMA_VERSION,
                "operation_id": operation_id,
                "run_id": run_id,
                "source_kind": "info",
                "source_operation": "info",
                "owner_kind": owner_kind,
                "owner_id": "account",
                "post_id": None,
                "media_ordinal": 1,
                "media_type": "photo",
                "extension": extension,
                "private_url": private_url,
                "url_sha256": url_sha256,
                "url_host": host,
                "filename": filename,
                "relative_directory": relative_directory,
                "relative_path": f"{relative_directory}/{filename}",
                "width": None,
                "height": None,
                "duration_seconds": None,
                "bitrate": None,
                "alt_text": None,
                "variant": {"profile_kind": owner_kind},
                "posted_at": _optional_post_time(date, "profile date"),
                "original_posted_at": None,
                "author_id": None,
                "author_handle": None,
                "conversation_id": None,
                "reply_id": None,
                "retweet_id": None,
                "captured_at": captured_at,
            }
            row["descriptor_sha256"] = sha256_bytes(
                canonical_json(descriptor_payload(row)).encode("utf-8")
            )
            rows.append(normalize_record(row))
        except DescriptorError as exc:
            errors.append(f"{owner_kind}: {exc}")

    if not SHA256_RE.fullmatch(source_sha256):
        raise DescriptorError("profile source digest is invalid")
    return DescriptorBatch(
        operation_id=operation_id,
        run_id=run_id,
        source_kind="info",
        source_operation="info",
        rows=tuple(rows),
        errors=tuple(errors),
        source_relative_path=source_relative_path,
        source_sha256=source_sha256,
        source_record_count=1,
    )


def discard_ephemeral_artifact(batch: DescriptorBatch) -> None:
    if not batch.ephemeral or batch.artifact_path is None:
        return
    try:
        batch.artifact_path.unlink()
    except OSError:
        pass
