from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from apache_incubator_releases_mcp import releases
from tests.fixtures import release_dirs


class ReleaseParserTests(unittest.TestCase):
    def test_collects_dist_and_archive_files(self) -> None:
        with release_dirs() as (dist, archive):
            result = releases.collect_files("alpha", dist_base=str(dist), archive_base=str(archive))

        self.assertEqual(result["podling_slug"], "alpha")
        self.assertGreaterEqual(result["count"], 6)
        self.assertIn("dist", result["sources"])
        self.assertIn("archive", result["sources"])

    def test_podling_release_overview_pairs_sidecars_and_checks_disclaimer(self) -> None:
        with release_dirs() as (dist, archive):
            result = releases.release_overview("alpha", dist_base=str(dist), archive_base=str(archive))

        self.assertEqual(result["source_artifact_count"], 2)
        self.assertEqual(result["signature_count"], 2)
        self.assertEqual(result["checksum_count"], 2)
        latest = result["releases"][0]["source_artifacts"][0]
        self.assertEqual(len(latest["signatures"]), 1)
        self.assertEqual(len(latest["checksums"]), 1)
        self.assertTrue(result["incubating_hints"]["disclaimer_checks"][0]["has_disclaimer_file"])


if __name__ == "__main__":
    unittest.main()
