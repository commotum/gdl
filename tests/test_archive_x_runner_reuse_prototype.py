import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests
from gallery_dl.extractor.common import Extractor


class SessionFixtureExtractor(Extractor):
    category = "goal5fixture"
    subcategory = "session"

    def __init__(self):
        super().__init__(re.match(r".*", "fixture://session"))

    def config(self, _key, default=None):
        return default

    def config2(self, _key, _key2, default=None, sentinel=None):
        return default

    def config_accumulate(self, _key):
        return None


def run_bounded_protocol(
    connection: sqlite3.Connection,
    *,
    batch_size: int,
    crash_before_result: int | None,
) -> tuple[int, list[tuple[str, int, str]]]:
    """Model begin/result boundaries around a durable one-item lease."""
    starts = 0
    events: list[tuple[str, int, str]] = []
    crashed = False
    while connection.execute(
        "SELECT 1 FROM jobs WHERE state != 'complete' LIMIT 1"
    ).fetchone():
        connection.execute("UPDATE jobs SET state='pending' WHERE state='leased'")
        starts += 1
        completed_in_process = 0
        while completed_in_process < batch_size:
            row = connection.execute(
                "SELECT id FROM jobs WHERE state='pending' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                break
            job_id = int(row[0])
            token = f"lease-{job_id}-{starts}"
            connection.execute(
                "UPDATE jobs SET state='leased',attempts=attempts+1,token=? WHERE id=?",
                (token, job_id),
            )
            connection.commit()
            events.append(("begin", job_id, token))
            if crash_before_result == job_id and not crashed:
                crashed = True
                break
            # The parent accepts a result only for the currently durable token.
            updated = connection.execute(
                "UPDATE jobs SET state='complete' WHERE id=? AND state='leased' AND token=?",
                (job_id, token),
            ).rowcount
            if updated != 1:
                raise AssertionError("result did not match its durable lease")
            connection.commit()
            events.append(("result", job_id, token))
            completed_in_process += 1
        if crash_before_result is not None and crashed and row is not None:
            crash_before_result = None
    return starts, events


class BoundedRunnerMechanismTests(unittest.TestCase):
    def test_gallery_extractors_can_share_one_account_session(self):
        original_init = requests.Session.__init__
        created = 0

        def counted_init(session, *args, **kwargs):
            nonlocal created
            created += 1
            original_init(session, *args, **kwargs)

        with mock.patch.object(requests.Session, "__init__", counted_init):
            for _ in range(1_000):
                SessionFixtureExtractor().initialize()
            independent_sessions = created

            created = 0
            shared = requests.Session()
            for _ in range(1_000):
                extractor = SessionFixtureExtractor()
                extractor.session = shared
                extractor.initialize()
            shared_sessions = created

        self.assertEqual(independent_sessions, 1_000)
        self.assertEqual(shared_sessions, 1)

    def test_bounded_control_protocol_retains_results_and_reclaims_one_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                "CREATE TABLE jobs(id INTEGER PRIMARY KEY,state TEXT,attempts INTEGER,token TEXT)"
            )
            connection.executemany(
                "INSERT INTO jobs VALUES(?,'pending',0,NULL)",
                ((index,) for index in range(1, 1_001)),
            )
            connection.commit()

            starts, events = run_bounded_protocol(
                connection,
                batch_size=100,
                crash_before_result=238,
            )

            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM jobs WHERE state='complete'"
                ).fetchone()[0],
                1_000,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT attempts FROM jobs WHERE id=238"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM jobs WHERE id != 238 AND attempts != 1"
                ).fetchone()[0],
                0,
            )
            connection.close()

        current_starts = 1_000
        self.assertEqual(starts, 11)
        self.assertGreaterEqual(1 - starts / current_starts, 0.98)
        begins = [event for event in events if event[0] == "begin"]
        results = [event for event in events if event[0] == "result"]
        self.assertEqual(len(begins), 1_001)
        self.assertEqual(len(results), 1_000)
        self.assertEqual(sum(event[1] == 238 for event in begins), 2)


if __name__ == "__main__":
    unittest.main()
