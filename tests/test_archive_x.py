import importlib.util
import json
import os
import tempfile
import time
import unittest
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
    captured="2026-07-14T01:02:03Z",
    likes=12,
):
    return {
        "tweet_id": int(post_id),
        "retweet_id": retweet_id,
        "quote_id": 0,
        "reply_id": reply_id,
        "conversation_id": int(post_id),
        "date": "2026-07-13 18:02:03",
        "content": "example post",
        "lang": "en",
        "author": {"id": 1 if author == "tszzl" else 2, "name": author, "nick": author},
        "user": {"id": 1, "name": user, "nick": user},
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


if __name__ == "__main__":
    unittest.main()
