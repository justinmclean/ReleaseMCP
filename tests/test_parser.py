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
        self.assertEqual(len(result["source_statuses"]), 2)
        self.assertTrue(all(status["available"] for status in result["source_statuses"]))

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

    def test_collect_files_reports_missing_source_directory(self) -> None:
        with release_dirs() as (dist, archive):
            result = releases.collect_files(
                "alpha",
                dist_base=str(dist),
                archive_base=str(archive.parent / "missing-archive-root"),
            )

        self.assertGreater(result["count"], 0)
        statuses = {status["source"]: status for status in result["source_statuses"]}
        self.assertTrue(statuses["dist"]["available"])
        self.assertFalse(statuses["archive"]["available"])
        self.assertIn("Directory not found", statuses["archive"]["error"])

    def test_platform_distribution_checks_flag_guideline_hints(self) -> None:
        responses = {
            "https://api.github.test/repos/apache/alpha/releases": [
                {
                    "tag_name": "v1.1.0",
                    "name": "Alpha 1.1.0",
                    "body": "Apache Alpha release notes",
                    "prerelease": False,
                    "draft": False,
                    "html_url": "https://github.test/apache/alpha/releases/v1.1.0",
                    "published_at": "2026-01-01T00:00:00Z",
                },
                {
                    "tag_name": "v1.2.0-rc1",
                    "name": "Alpha 1.2.0 RC1",
                    "body": "Apache Alpha incubating disclaimer",
                    "prerelease": False,
                    "draft": False,
                    "html_url": "https://github.test/apache/alpha/releases/v1.2.0-rc1",
                    "published_at": "2026-02-01T00:00:00Z",
                },
            ],
            "https://docker.test/apache/alpha/": {
                "description": "Apache Alpha",
                "full_description": "Apache Alpha image",
            },
            "https://docker.test/apache/alpha/tags?page_size=100": {
                "results": [
                    {"name": "latest", "last_updated": "2026-01-01T00:00:00Z", "full_size": 123},
                    {"name": "1.2.0-rc1", "last_updated": "2026-02-01T00:00:00Z", "full_size": 123},
                ]
            },
            "https://pypi.test/apache-alpha/json": {
                "info": {
                    "name": "apache-alpha",
                    "version": "1.2.0rc1",
                    "summary": "Apache Alpha",
                    "description": "Apache Alpha Python package",
                    "license": "",
                    "classifiers": [],
                },
                "releases": {
                    "1.1.0": [
                        {
                            "filename": "apache_alpha-1.1.0-py3-none-any.whl",
                            "packagetype": "bdist_wheel",
                            "python_version": "py3",
                            "upload_time_iso_8601": "2026-01-01T00:00:00Z",
                            "digests": {"sha256": "abc"},
                            "has_sig": False,
                            "yanked": False,
                        }
                    ],
                    "1.2.0rc1": [
                        {
                            "filename": "apache_alpha-1.2.0rc1.tar.gz",
                            "packagetype": "sdist",
                            "python_version": "source",
                            "upload_time_iso_8601": "2026-02-01T00:00:00Z",
                            "digests": {"sha256": "def"},
                            "has_sig": False,
                            "yanked": False,
                        }
                    ],
                },
            },
        }
        original = releases._read_url_json
        releases._read_url_json = lambda url: responses[url]
        try:
            result = releases.platform_distribution_checks(
                "alpha",
                docker_images=["apache/alpha"],
                pypi_packages=["apache-alpha"],
                github_api_base="https://api.github.test/repos/apache",
                docker_api_base="https://docker.test",
                pypi_api_base="https://pypi.test",
            )
        finally:
            releases._read_url_json = original

        self.assertEqual(result["guidelines"], releases.DISTRIBUTION_GUIDELINES_URL)
        self.assertTrue(result["github"]["available"])
        self.assertIn("incubation disclaimer", " ".join(result["hints"]["github"]))
        self.assertIn("prereleases", " ".join(result["hints"]["github"]))
        self.assertTrue(result["docker_hub"][0]["latest_tag_present"])
        self.assertIn("latest tag", " ".join(result["hints"]["docker_hub"]))
        self.assertTrue(result["pypi"][0]["available"])
        self.assertIn("project description", " ".join(result["hints"]["pypi"]))
        self.assertIn("ALv2 license", " ".join(result["hints"]["pypi"]))
        self.assertIn("latest version", " ".join(result["hints"]["pypi"]))


if __name__ == "__main__":
    unittest.main()
