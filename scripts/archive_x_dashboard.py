#!/usr/bin/env python3
"""Compact read-only live view of durable X archive progress."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import archive_x_progress as progress


def clip(text: str, width: int, unicode: bool) -> str:
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + ("…" if unicode else ".")


def age(value: str | None, now: float) -> str:
    timestamp = progress.parse_time(value)
    if timestamp is None:
        return "not yet"
    return f"{progress.human_duration(max(0, now - timestamp))} ago"


def short_date(value: str | None) -> str:
    timestamp = progress.parse_time(value)
    if timestamp is None:
        return "unknown"
    date = datetime.fromtimestamp(timestamp).astimezone()
    return f"{date.strftime('%b')} {date.day}, {date.year}"


def wait_label(value: str | None, now: float) -> str | None:
    timestamp = progress.parse_time(value)
    if timestamp is None or timestamp <= now:
        return None
    local = datetime.fromtimestamp(timestamp).astimezone()
    return f"waiting until {local.strftime('%I:%M %p').lstrip('0')}"


def render_user(
    user: dict[str, Any], *, started_at: str, width: int,
    now: float, unicode: bool,
) -> list[str]:
    separator = " · " if unicode else " | "
    totals, delta = user["totals"], user["delta"]
    started = progress.parse_time(started_at) or now
    active_phase = str(user["phase"])
    phase = active_phase.replace("_", " ").upper()
    rate = user.get("rate")
    if rate and rate.get("coverage_days_per_hour") is not None:
        rate_text = (
            f"{rate['coverage_days_per_hour']:,.1f} archive-days/hour"
        )
    elif rate and rate.get("items_per_hour") is not None:
        rate_text = (
            f"{progress.human_number(round(rate['items_per_hour']))} "
            f"{'media' if active_phase == 'context_media' else 'items'}/hour"
        )
    else:
        rate_text = "rate warming up"
    estimate = user["estimate"]
    if estimate.get("seconds") is not None:
        if active_phase == "legacy":
            destination = "to account creation"
        elif active_phase == "context_media":
            destination = "to finish media pass"
        else:
            destination = "for known queue"
        estimate_text = f"{estimate['label']} {destination}{separator}"
        estimate_text += f"{estimate['confidence']} confidence"
    else:
        estimate_text = (
            f"not shown{separator}{estimate.get('qualifier', 'unknown denominator')}"
        )
    actions = int(user.get("action_required") or 0)
    health = user["health"]
    if actions:
        blocked_phases = [
            name.replace("_", " ")
            for name, status in user["phases"].items()
            if status in {"manual_review", "blocked"}
        ]
        action_text = (
            f"{blocked_phases[0]} needs review"
            if actions == 1 and blocked_phases
            else f"{actions} need review"
        )
        health = f"{health}{separator}{action_text}"
    now_text = wait_label(user.get("wait_until"), now) or (
        f"progress {age(user.get('last_progress_at'), now)}"
    )
    lines = [
        f"@{user['handle']}  {phase}{separator}{health}"
        f"{separator}{progress.human_duration(now - started)}",
        f"Timeline   {progress.human_number(totals['archive_posts'])} posts"
        f"{separator}{progress.human_number(totals['archive_media_files'])} media"
        f"{separator}{progress.human_bytes(totals['archive_media_bytes'])}",
        f"Context    {progress.human_number(totals['context_parents_saved'])} parents saved"
        f"{separator}{progress.human_number(totals['context_unavailable'])} unavailable"
        f"{separator}{progress.human_number(totals['context_manual_review'])} review",
        f"Coverage   {progress.human_number(totals['conversations_closed'])} conversations closed"
        f"{separator}{progress.human_number(totals['context_known_remaining'])} known remaining",
        f"This run   +{progress.human_number(max(0, delta['context_parents_saved']))} parents"
        f"{separator}+{progress.human_number(max(0, delta['context_unavailable']))} boundaries"
        f"{separator}{rate_text}",
        f"Estimate   {estimate_text}",
        f"Now        {user['activity']}{separator}{now_text}",
    ]
    legacy = user.get("legacy")
    if active_phase == "legacy" and isinstance(legacy, dict):
        completed_days = legacy["completed_seconds"] / 86400
        total_days = legacy["total_seconds"] / 86400
        lines[2] = (
            f"Legacy     {completed_days:.1f}/{total_days:.1f} days covered"
            f"{separator}{legacy['percent']:.1f}%"
            f"{separator}frontier {short_date(legacy['next_until'])}"
        )
        lines[3] = (
            f"Context    {progress.human_number(totals['context_parents_saved'])} parents saved"
            f"{separator}{progress.human_number(totals['context_known_remaining'])} queued"
        )
        lines[4] = (
            f"This run   +{progress.human_number(max(0, delta['archive_posts']))} posts"
            f"{separator}{legacy['committed_windows']} windows"
            f"{separator}{rate_text}"
        )
    elif active_phase == "context_media":
        lines[3] = (
            f"Media      {progress.human_number(totals['context_media_captured'])} saved"
            f"{separator}{progress.human_number(totals['context_media_unavailable'])} unavailable"
            f"{separator}{progress.human_number(totals['context_media_manual_review'])} review"
            f"{separator}{progress.human_number(totals['context_media_actionable'])} remaining"
        )
        lines[4] = (
            f"This run   +{progress.human_number(max(0, delta['context_media_captured']))} media"
            f"{separator}+{progress.human_number(max(0, delta['context_media_manual_review']))} review"
            f"{separator}{rate_text}"
        )
    return [clip(line, width, unicode) for line in lines]


def render(
    snapshot: dict[str, Any], *, width: int = 80,
    now: float | None = None, unicode: bool = True,
) -> str:
    progress.validate_snapshot(snapshot)
    current = time.time() if now is None else now
    updated = progress.parse_time(snapshot.get("updated_at"))
    stale = (
        snapshot.get("status") == "running"
        and updated is not None
        and current - updated > 900
    )
    users = []
    for source in snapshot["users"]:
        user = dict(source)
        if stale and user.get("health") not in {"failed", "blocked"}:
            user["health"] = "stale"
            user["activity"] = "no telemetry update"
        users.append(user)
    blocks = [
        render_user(
            user, started_at=snapshot["started_at"], width=max(40, width),
            now=current, unicode=unicode,
        )
        for user in users
    ]
    return "\n\n".join("\n".join(block) for block in blocks)


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    progress.validate_snapshot(value)
    return value


def latest_snapshot(archive_root: Path) -> Path:
    paths = list((archive_root / "_state" / "progress").glob("*.json"))
    if not paths:
        raise progress.ProgressError("no progress snapshot exists yet")
    return max(paths, key=lambda path: path.stat().st_mtime_ns)


def snapshot_archive_root(path: Path) -> Path | None:
    if path.parent.name != "progress" or path.parent.parent.name != "_state":
        return None
    return path.parent.parent.parent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", nargs="?", type=Path)
    parser.add_argument("--archive-root", type=Path)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--exit-when-final", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.interval < 0.2:
        raise SystemExit("--interval must be at least 0.2 seconds")
    try:
        path = args.snapshot or latest_snapshot(
            args.archive_root
            or Path(os.environ.get("GDL_X_ARCHIVE_ROOT", "/mnt/Bibliotheque/gdl/x-archive"))
        )
        archive_root = args.archive_root or snapshot_archive_root(path)
        live = (
            progress.LiveProgressReader(archive_root)
            if archive_root is not None
            else None
        )
        while True:
            try:
                snapshot = load(path)
                if live is not None and snapshot["status"] == "running":
                    snapshot = live.enrich(snapshot)
                width = shutil.get_terminal_size((80, 24)).columns
                output = render(
                    snapshot, width=width,
                    unicode=(sys.stdout.encoding or "").lower().startswith("utf"),
                )
            except (OSError, json.JSONDecodeError, progress.ProgressError) as exc:
                output = f"Archive dashboard unavailable: {exc}"
                snapshot = None
            interactive = sys.stdout.isatty()
            if args.watch and interactive:
                print("\033[H\033[2J", end="")
            print(output, flush=True)
            if not args.watch:
                return 0 if snapshot is not None else 2
            if (
                args.exit_when_final and snapshot is not None
                and snapshot["status"] in progress.FINAL_STATUSES
            ):
                return 0
            time.sleep(args.interval if interactive else max(60.0, args.interval))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
