import contextlib
import io
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

from gallery_dl.extractor.common import Extractor, Message
from gallery_dl.job import DownloadJob


REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
import archive_x_request_telemetry as request_telemetry


class DescriptorFixtureExtractor(Extractor):
    """Deterministic file-event fixture; it performs no HTTP requests."""

    category = "goal5fixture"
    subcategory = "descriptors"
    filename_fmt = "{tweet_id}_{num}.{extension}"
    directory_fmt = ()
    archive_fmt = "{tweet_id}_{num}"

    def __init__(self, records, options):
        super().__init__(re.match(r".*", "fixture://descriptors"))
        self._records = records
        self._options = options

    def config(self, key, default=None):
        return self._options.get(key, default)

    def config2(self, key, key2, default=None, sentinel=None):
        return self._options.get(key, self._options.get(key2, default))

    def config_accumulate(self, key):
        return self._options.get(key)

    def items(self):
        for post, files in self._records:
            yield Message.Directory, "", post.copy()
            for number, file in enumerate(files, 1):
                metadata = {**post, **file, "num": number}
                url = metadata.pop("url")
                yield Message.Url, url, metadata


class DescriptorCaptureMechanismTests(unittest.TestCase):
    def test_prepare_event_retains_all_file_fields_without_downloading(self):
        accepted = {"100", "101", "102"}
        records = [
            (
                {
                    "tweet_id": "100",
                    "date": "2026-01-01 00:00:00",
                    "author": {"name": "alice"},
                },
                [
                    {
                        "url": "https://pbs.twimg.com/media/a.jpg?name=orig",
                        "extension": "jpg",
                        "filename": "a",
                        "type": "photo",
                        "width": 1200,
                        "height": 800,
                        "description": "first",
                    },
                    {
                        "url": "https://pbs.twimg.com/media/b.png?name=orig",
                        "extension": "png",
                        "filename": "b",
                        "type": "photo",
                        "width": 640,
                        "height": 480,
                        "description": "second",
                    },
                ],
            ),
            (
                {
                    "tweet_id": "101",
                    "date": "2026-01-02 00:00:00",
                    "author": {"name": "bob"},
                },
                [
                    {
                        "url": "https://video.twimg.com/ext_tw_video/v.mp4?tag=1",
                        "extension": "mp4",
                        "filename": "v",
                        "type": "video",
                        "width": 1920,
                        "height": 1080,
                        "bitrate": 2_176_000,
                        "duration": 8.25,
                    }
                ],
            ),
            (
                {
                    "tweet_id": "102",
                    "date": "2026-01-03 00:00:00",
                    "author": {"name": "carol"},
                },
                [
                    {
                        "url": "https://video.twimg.com/tweet_video/a.gif?tag=1",
                        "extension": "mp4",
                        "filename": "a",
                        "type": "animated_gif",
                        "width": 480,
                        "height": 270,
                        "bitrate": 0,
                        "duration": 2.0,
                    },
                    {
                        "url": "https://pbs.twimg.com/media/c.webp?name=orig",
                        "extension": "webp",
                        "filename": "c",
                        "type": "photo",
                        "width": 900,
                        "height": 900,
                    },
                ],
            ),
            (
                {
                    "tweet_id": "999",
                    "date": "2026-01-04 00:00:00",
                    "author": {"name": "nearby"},
                },
                [
                    {
                        "url": "https://pbs.twimg.com/media/rejected.jpg?name=orig",
                        "extension": "jpg",
                        "filename": "rejected",
                        "type": "photo",
                        "width": 100,
                        "height": 100,
                    }
                ],
            ),
        ]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "descriptors.jsonl"
            options = {
                "download": False,
                "metadata-url": "media_url",
                "base-directory": str(root),
                "directory": [],
                "filename": "{tweet_id}_{num}.{extension}",
                "postprocessors": [
                    {
                        "name": "metadata",
                        "mode": "jsonl",
                        "event": "prepare",
                        "base-directory": str(root),
                        "directory": [],
                        "filename": artifact.name,
                    }
                ],
            }
            recorder = request_telemetry.RequestRecorder(
                root / "requests.json", "context_metadata"
            )
            with recorder.capture(), contextlib.redirect_stdout(io.StringIO()):
                status = DownloadJob(
                    DescriptorFixtureExtractor(records, options)
                ).run()

            self.assertEqual(status, 0)
            self.assertEqual(recorder.value(0)["summary"]["actual_requests"], 0)
            rows = [
                json.loads(line)
                for line in artifact.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(rows), 6)
            self.assertFalse(any(root.glob("*.jpg")))
            self.assertFalse(any(root.glob("*.png")))
            self.assertFalse(any(root.glob("*.mp4")))
            self.assertFalse(any(root.glob("*.webp")))

            selected = [
                row for row in rows if str(row["tweet_id"]) in accepted
            ]
            rejected = [
                row for row in rows if str(row["tweet_id"]) not in accepted
            ]
            self.assertEqual(len(selected), 5)
            self.assertEqual(
                [(str(row["tweet_id"]), row["num"]) for row in selected],
                [("100", 1), ("100", 2), ("101", 1), ("102", 1), ("102", 2)],
            )
            self.assertEqual([str(row["tweet_id"]) for row in rejected], ["999"])
            for row in rows:
                self.assertIn("media_url", row)
                self.assertIn("extension", row)
                self.assertIn("type", row)
                self.assertIn("author", row)
                self.assertIn("date", row)
                self.assertIsInstance(row["num"], int)
            video = next(row for row in rows if row.get("type") == "video")
            self.assertEqual(video["bitrate"], 2_176_000)
            self.assertEqual(video["duration"], 8.25)
            self.assertEqual((video["width"], video["height"]), (1920, 1080))


if __name__ == "__main__":
    unittest.main()
