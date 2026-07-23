import importlib.util
import json
import os
import tempfile
import time
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location(
    "archive_x", REPO / "scripts" / "archive_x.py"
)
archive_x = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(archive_x)


def metadata(
    *,
    post_id="1940000000000000000",
    author="tszzl",
    user="tszzl",
    retweet_id=0,
    reply_id=0,
    date_original="2026-07-12 10:11:12",
    captured="2026-07-14T01:02:03Z",
    likes=12,
    author_id=None,
    user_id=1,
):
    return {
        "tweet_id": int(post_id),
        "retweet_id": retweet_id,
        "quote_id": 0,
        "reply_id": reply_id,
        "conversation_id": int(post_id),
        "date": "2026-07-13 18:02:03",
        "date_original": date_original if retweet_id else None,
        "content": "example post",
        "lang": "en",
        "author": {
            "id": author_id if author_id is not None else (1 if author == "tszzl" else 2),
            "name": author,
            "nick": author,
        },
        "user": {"id": user_id, "name": user, "nick": user},
        "favorite_count": likes,
        "view_count": 100,
        "retweet_count": 3,
        "quote_count": 2,
        "reply_count": 4,
        "bookmark_count": 5,
        "archived_at": captured,
        "subcategory": "timeline",
    }


class TargetParsingTests(unittest.TestCase):
    def test_normalizes_supported_forms(self):
        values = (
            "tszzl",
            "@TsZzL",
            "https://x.com/tszzl",
            "https://www.twitter.com/TSZZL/media",
            "x.com/tszzl/with_replies",
        )
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(archive_x.normalize_handle(value), "tszzl")

    def test_rejects_non_x_and_reserved_paths(self):
        for value in ("https://example.com/tszzl", "https://x.com/home", "-o proxy=x"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    archive_x.normalize_handle(value)

    def test_input_file_ignores_comments_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "users.txt"
            path.write_text(
                "# users\nhttps://x.com/tszzl\n\n@TSZZL\nhttps://twitter.com/gwern\n",
                encoding="utf-8",
            )
            self.assertEqual(archive_x.load_targets(None, path), ["tszzl", "gwern"])


class CookieTests(unittest.TestCase):
    def test_validates_without_returning_values(self):
        future = int(time.time()) + 3600
        content = (
            "# Netscape HTTP Cookie File\n"
            f"#HttpOnly_.x.com\tTRUE\t/\tTRUE\t{future}\tauth_token\tsecret-a\n"
            f".x.com\tTRUE\t/\tTRUE\t{future}\tct0\tsecret-b\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.cookies.txt"
            path.write_text(content, encoding="utf-8")
            os.chmod(path, 0o600)
            self.assertEqual(
                archive_x.validate_cookie_file(path), {"auth_token", "ct0"}
            )

    def test_rejects_twitter_only_cookies(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.cookies.txt"
            path.write_text(
                ".twitter.com\tTRUE\t/\tTRUE\t0\tauth_token\tsecret\n"
                ".twitter.com\tTRUE\t/\tTRUE\t0\tct0\tsecret\n",
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            with self.assertRaises(archive_x.ArchiveError):
                archive_x.validate_cookie_file(path)

    def test_accepts_any_unexpired_x_auth_cookie_regardless_of_order(self):
        past = int(time.time()) - 3600
        future = int(time.time()) + 3600
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.cookies.txt"
            path.write_text(
                f".x.com\tTRUE\t/\tTRUE\t{future}\tauth_token\tnew\n"
                f".x.com\tTRUE\t/\tTRUE\t{past}\tauth_token\told\n"
                f".x.com\tTRUE\t/\tTRUE\t{future}\tct0\tcsrf\n",
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            self.assertIn("auth_token", archive_x.validate_cookie_file(path))

    @unittest.skipUnless(os.name == "posix", "POSIX permissions only")
    def test_rejects_group_readable_cookie_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.cookies.txt"
            path.write_text(
                ".x.com\tTRUE\t/\tTRUE\t0\tauth_token\tsecret\n"
                ".x.com\tTRUE\t/\tTRUE\t0\tct0\tsecret\n",
                encoding="utf-8",
            )
            os.chmod(path, 0o640)
            with self.assertRaises(archive_x.ArchiveError):
                archive_x.validate_cookie_file(path)


class ConfigTests(unittest.TestCase):
    def build(self, include_reposts=True):
        return archive_x.build_gallery_config(
            handle="tszzl",
            endpoint="timeline",
            archive_root=Path("/archive"),
            user_dir=Path("/archive/users/tszzl"),
            raw_partial=Path("/archive/users/tszzl/runs/run/raw/timeline.posts.jsonl.partial"),
            cookie_file=Path("/cookies/x.txt"),
            archive_run_id="run",
            archived_at="2026-07-14T01:02:03Z",
            request_delay="4-8",
            download_delay="1-3",
            extractor_delay="2-5",
            include_reposts=include_reposts,
            checksums=True,
            cursor=None,
        )

    def test_safe_defaults_and_reposts(self):
        twitter = self.build()["extractor"]["twitter"]
        self.assertTrue(twitter["retweets"])
        self.assertIn("retweet_id", twitter["timeline"]["post-filter"])
        self.assertFalse(twitter["quoted"])
        self.assertFalse(twitter["pinned"])
        self.assertFalse(twitter["showreplies"])
        self.assertEqual(twitter["timeline"]["strategy"], "with_replies")
        self.assertEqual(twitter["ratelimit"], "wait")
        self.assertEqual(twitter["locked"], "abort")
        self.assertEqual(twitter["sleep-request"], "4-8")
        self.assertEqual(twitter["postprocessors"][0]["name"], "hash")

    def test_repost_opt_out(self):
        twitter = self.build(False)["extractor"]["twitter"]
        self.assertFalse(twitter["retweets"])
        self.assertNotIn("retweet_id", twitter["timeline"]["post-filter"])


class DatasetTests(unittest.TestCase):
    def test_normalizes_metrics_date_and_repost_authorship(self):
        value = metadata(author="other", retweet_id=1930000000000000000)
        record = archive_x.normalize_post(value, "tszzl", "timeline")
        self.assertEqual(record["relationship"], "repost")
        self.assertFalse(record["is_authored_by_requested_user"])
        self.assertEqual(record["author_handle"], "other")
        self.assertEqual(
            record["source_url"],
            "https://x.com/tszzl/status/1940000000000000000",
        )
        self.assertEqual(
            record["reposted_source_url"],
            "https://x.com/other/status/1930000000000000000",
        )
        self.assertEqual(record["posted_at"], "2026-07-13 18:02:03")
        self.assertEqual(record["reposted_at"], "2026-07-13 18:02:03")
        self.assertEqual(record["original_posted_at"], "2026-07-12 10:11:12")
        self.assertEqual(
            record["metrics"],
            {
                "likes": 12,
                "views": 100,
                "reposts": 3,
                "quotes": 2,
                "replies": 4,
                "bookmarks": 5,
            },
        )

    def test_builds_cumulative_and_authored_views(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "users" / "tszzl"
            raw1 = user_dir / "run1.jsonl"
            raw1.parent.mkdir(parents=True)
            rows = [
                metadata(),
                metadata(
                    post_id="1930000000000000000",
                    author="other",
                    retweet_id=1920000000000000000,
                ),
            ]
            raw1.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            result1 = archive_x.update_post_dataset(
                user_dir, "tszzl", raw1, "timeline"
            )
            self.assertEqual(result1["dataset_posts"], 2)
            self.assertEqual(result1["authored_posts"], 1)
            self.assertEqual(result1["reposts"], 1)

            raw2 = user_dir / "run2.jsonl"
            raw2.write_text(
                json.dumps(
                    metadata(captured="2026-07-15T01:02:03Z", likes=99)
                )
                + "\n",
                encoding="utf-8",
            )
            archive_x.update_post_dataset(user_dir, "tszzl", raw2, "timeline")
            records = list(
                archive_x.iter_jsonl(user_dir / "dataset" / "posts.jsonl")
            )
            updated = next(row for row in records if row["author_handle"] == "tszzl")
            self.assertEqual(updated["first_captured_at"], "2026-07-14T01:02:03Z")
            self.assertEqual(updated["last_captured_at"], "2026-07-15T01:02:03Z")
            self.assertEqual(updated["capture_count"], 2)
            self.assertEqual(updated["metrics"]["likes"], 99)

    def test_numeric_identity_survives_a_handle_rename(self):
        value = metadata(
            author="new_name", user="old_name", author_id=55, user_id=55
        )
        record = archive_x.normalize_post(value, "old_name", "timeline")
        self.assertEqual(record["relationship"], "post")
        self.assertTrue(record["is_authored_by_requested_user"])

    def test_new_metrics_win_when_old_metadata_is_richer(self):
        first = metadata(likes=12)
        first["article"] = {"title": "preserve me", "html": "<p>long</p>"}
        second = metadata(captured="2026-07-15T01:02:03Z", likes=99)
        old = archive_x.normalize_post(first, "tszzl", "timeline")
        new = archive_x.normalize_post(second, "tszzl", "timeline")
        merged = archive_x.merge_post_records(old, new)
        self.assertEqual(merged["metrics"]["likes"], 99)
        self.assertEqual(merged["gallery_dl"]["favorite_count"], 99)
        self.assertEqual(merged["gallery_dl"]["article"]["title"], "preserve me")


class StateAndIdentityTests(unittest.TestCase):
    def args(self, **overrides):
        values = {
            "full_rescan": False,
            "since": None,
            "post_limit": None,
            "overlap_hours": 48.0,
        }
        values.update(overrides)
        return Namespace(**values)

    def test_resume_preserves_original_date_cutoff(self):
        state = {
            "resume": {
                "cursor": "3_123/cursor-token",
                "started_at": "2026-07-10T00:00:00Z",
                "date_after": "2026-07-08T00:00:00Z",
            }
        }
        cursor, started, cutoff = archive_x.select_timeline_state(
            self.args(), state, datetime(2026, 7, 14, tzinfo=timezone.utc)
        )
        self.assertEqual(cursor, "3_123/cursor-token")
        self.assertEqual(started, "2026-07-10T00:00:00Z")
        self.assertEqual(archive_x.iso_utc(cutoff), "2026-07-08T00:00:00Z")

    def test_legacy_archive_uses_separate_modern_head_checkpoint(self):
        state = {
            "resume": {
                "cursor": "3_29116490825/",
                "started_at": "2026-07-20T00:00:00Z",
                "date_after": None,
            },
            "legacy_backfill": {"status": "pending"},
            "modern_head": {
                "last_successful_started_at": "2026-07-20T02:39:18Z",
                "active": None,
            },
        }
        cursor, started, cutoff = archive_x.select_timeline_state(
            self.args(overlap_hours=48),
            state,
            datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        )
        self.assertIsNone(cursor)
        self.assertEqual(started, "2026-07-22T12:00:00Z")
        self.assertEqual(archive_x.iso_utc(cutoff), "2026-07-18T02:39:18Z")

        boundary = json.loads(json.dumps(state["resume"]))
        archive_x.update_timeline_state(
            state,
            limited_run=False,
            metadata_complete=True,
            resume_cursor=None,
            handle="alice",
            chain_started_at=started,
            date_after=cutoff,
            observed_at="2026-07-22T12:05:00Z",
            modern_head_mode=True,
        )
        self.assertEqual(state["resume"], boundary)
        self.assertEqual(
            state["modern_head"]["last_successful_started_at"], started
        )
        self.assertIsNone(state["modern_head"]["active"])

    def test_existing_legacy_full_rescan_and_early_since_stay_in_modern_domain(self):
        state = {
            "resume": {"cursor": "3_29116490825/"},
            "legacy_backfill": {"status": "pending"},
            "modern_head": {
                "last_successful_started_at": "2026-07-20T02:39:18Z",
                "active": None,
            },
        }
        started_at = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
        cursor, _started, cutoff = archive_x.select_timeline_state(
            self.args(full_rescan=True), state, started_at
        )
        self.assertIsNone(cursor)
        self.assertEqual(cutoff, archive_x.SNOWFLAKE_EPOCH)

        cursor, _started, cutoff = archive_x.select_timeline_state(
            self.args(since=datetime(2009, 1, 1, tzinfo=timezone.utc)),
            state,
            started_at,
        )
        self.assertIsNone(cursor)
        self.assertEqual(cutoff, archive_x.SNOWFLAKE_EPOCH)

        requested = datetime(2020, 1, 1, tzinfo=timezone.utc)
        _cursor, _started, cutoff = archive_x.select_timeline_state(
            self.args(since=requested), state, started_at
        )
        self.assertEqual(cutoff, requested)

    def test_interrupted_modern_head_has_its_own_cursor(self):
        state = {
            "resume": {"cursor": "3_29116490825/"},
            "legacy_backfill": {"status": "pending"},
            "modern_head": {"active": None},
        }
        cutoff = datetime(2026, 7, 20, tzinfo=timezone.utc)
        archive_x.update_timeline_state(
            state,
            limited_run=False,
            metadata_complete=False,
            resume_cursor="3_2000000000000000000/head-token",
            handle="alice",
            chain_started_at="2026-07-22T12:00:00Z",
            date_after=cutoff,
            observed_at="2026-07-22T12:01:00Z",
            modern_head_mode=True,
        )
        self.assertEqual(state["resume"]["cursor"], "3_29116490825/")
        self.assertEqual(
            state["modern_head"]["active"]["cursor"],
            "3_2000000000000000000/head-token",
        )

    def test_identity_guard_rejects_a_recycled_handle(self):
        state = {"requested_user_id": "111"}
        with self.assertRaises(archive_x.ArchiveError):
            archive_x.bind_profile_identity(state, "name", "222", "name")
        self.assertEqual(state["requested_user_id"], "111")

    def test_profile_identity_reads_direct_info_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "info.jsonl"
            path.write_text(
                json.dumps({"id": 1460283925, "name": "tszzl"}) + "\n",
                encoding="utf-8",
            )
            self.assertEqual(
                archive_x.profile_identity(path), ("1460283925", "tszzl")
            )

    def test_dataset_readme_has_no_patch_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            user_dir = Path(directory) / "users" / "tszzl"
            archive_x.write_dataset_readme(user_dir)
            text = (user_dir / "dataset" / "README.md").read_text()
            self.assertNotIn("\n+", text)
            self.assertIn("original_posted_at", text)


if __name__ == "__main__":
    unittest.main()
