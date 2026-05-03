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

    def test_release_page_checks_accept_compliant_download_page(self) -> None:
        with release_dirs() as (dist, archive):
            page = dist.parent / "download.html"
            source = "apache-alpha-1.1.0-incubating-source-release.zip"
            page.write_text(
                f"""\
<html><body>
  <a href="https://www.apache.org/dyn/closer.lua/incubator/alpha/{source}">Source</a>
  <a href="https://downloads.apache.org/incubator/alpha/{source}.asc">PGP</a>
  <a href="https://downloads.apache.org/incubator/alpha/{source}.sha512">SHA512</a>
  <a href="https://downloads.apache.org/incubator/alpha/KEYS">KEYS</a>
  Please verify downloads with checksums and OpenPGP signatures.
</body></html>
""",
                encoding="utf-8",
            )

            result = releases.release_overview(
                "alpha",
                dist_base=str(dist),
                archive_base=str(archive),
                release_page_url=str(page),
            )

        checks = result["release_page_checks"]
        self.assertTrue(checks["available"])
        self.assertEqual(checks["guidelines"], releases.RELEASE_DOWNLOAD_PAGES_URL)
        self.assertFalse(checks["hints"])
        self.assertTrue(checks["facts"]["has_https_downloads_keys_link"])
        self.assertTrue(checks["facts"]["has_verification_instructions"])

    def test_release_page_checks_flag_download_page_hints(self) -> None:
        with release_dirs() as (dist, archive):
            page = dist.parent / "bad-download.html"
            source = "apache-alpha-1.1.0-incubating-source-release.zip"
            page.write_text(
                f"""\
<html><body>
  <a href="https://dist.apache.org/repos/dist/release/incubator/alpha/{source}">Source</a>
  <a href="https://www.apache.org/dyn/closer.lua/incubator/alpha">Downloads</a>
  <a href="http://downloads.apache.org/incubator/alpha/{source}.sha512">SHA512</a>
</body></html>
""",
                encoding="utf-8",
            )

            result = releases.release_overview(
                "alpha",
                dist_base=str(dist),
                archive_base=str(archive),
                release_page_url=str(page),
            )

        hints = " ".join(result["release_page_checks"]["hints"])
        self.assertIn("directly to dist.apache.org", hints)
        self.assertIn("top-level closer.lua", hints)
        self.assertIn("detached signature", hints)
        self.assertIn("KEYS", hints)
        self.assertIn("verifying downloads", hints)

    def test_discovers_release_page_from_common_project_url(self) -> None:
        with release_dirs() as (dist, archive):
            collected = releases.collect_files("alpha", dist_base=str(dist), archive_base=str(archive))
            files = [releases.ReleaseFile(**item) for item in collected["files"]]
        source = "apache-alpha-1.1.0-incubating-source-release.zip"
        original = releases._scan_url_page

        def fake_scan_url_page(url: str) -> tuple[list, str]:
            if url == "https://alpha.apache.org/download/":
                html_text = (
                    f"<html><body>"
                    f'<a href="https://www.apache.org/dyn/closer.lua/incubator/alpha/{source}">Source</a>'
                    f'<a href="https://downloads.apache.org/incubator/alpha/{source}.sha512">SHA512</a>'
                    f"</body></html>"
                )
            else:
                raise OSError("not found")
            s = releases._HtmlLinkScanner()
            s.feed(html_text)
            s.close()
            return s.links, s.visible_text

        releases._scan_url_page = fake_scan_url_page
        try:
            discovery = releases.discover_release_page_url("alpha", files)
        finally:
            releases._scan_url_page = original

        self.assertTrue(discovery["found"])
        self.assertEqual(discovery["location"], "https://alpha.apache.org/download/")

    def test_discovers_release_page_only_checks_standard_paths(self) -> None:
        with release_dirs() as (dist, archive):
            collected = releases.collect_files("alpha", dist_base=str(dist), archive_base=str(archive))
            files = [releases.ReleaseFile(**item) for item in collected["files"]]
        original = releases._scan_url_page
        attempted_urls: list[str] = []

        def fake_scan_url_page(url: str) -> tuple[list, str]:
            attempted_urls.append(url)
            raise OSError("not found")

        releases._scan_url_page = fake_scan_url_page
        try:
            discovery = releases.discover_release_page_url("alpha", files)
        finally:
            releases._scan_url_page = original

        self.assertFalse(discovery["found"])
        # Only standard path candidates — no homepage, no incubator subdomain
        self.assertEqual(attempted_urls, [f"https://alpha.apache.org/{p}" for p in releases.RELEASE_PAGE_PATHS])
        self.assertFalse(any("incubator.apache.org" in u for u in attempted_urls))

    def test_release_overview_checks_discovered_release_page(self) -> None:
        with release_dirs() as (dist, archive):
            collected = releases.collect_files("alpha", dist_base=str(dist), archive_base=str(archive))
        source = "apache-alpha-1.1.0-incubating-source-release.zip"
        original_collect_files = releases.collect_files
        original_scan_url_page = releases._scan_url_page

        def fake_scan_url_page(url: str) -> tuple[list, str]:
            if url == "https://alpha.apache.org/download/":
                html_text = (
                    f"<html><body>"
                    f'<a href="https://www.apache.org/dyn/closer.lua/incubator/alpha/{source}">Source</a>'
                    f'<a href="https://downloads.apache.org/incubator/alpha/{source}.asc">PGP</a>'
                    f'<a href="https://downloads.apache.org/incubator/alpha/{source}.sha512">SHA512</a>'
                    f'<a href="https://downloads.apache.org/incubator/alpha/KEYS">KEYS</a>'
                    f" Please verify downloads with checksums and OpenPGP signatures."
                    f"</body></html>"
                )
            else:
                raise OSError("not found")
            s = releases._HtmlLinkScanner()
            s.feed(html_text)
            s.close()
            return s.links, s.visible_text

        releases.collect_files = lambda *args, **kwargs: collected
        releases._scan_url_page = fake_scan_url_page
        try:
            result = releases.release_overview("alpha")
        finally:
            releases.collect_files = original_collect_files
            releases._scan_url_page = original_scan_url_page

        self.assertTrue(result["release_page_discovery"]["found"])
        self.assertEqual(
            result["release_page_checks"]["location"],
            "https://alpha.apache.org/download/",
        )
        self.assertFalse(result["release_page_checks"]["hints"])


    def test_release_overview_without_dist_base_does_not_fall_back_to_default_svn_dist(self) -> None:
        with release_dirs() as (dist, archive):
            original_collect_url = releases._collect_url
            calls: list[str] = []

            def fake_collect_url(url: str, source: str, max_depth: int, seen: set[str] | None = None):
                calls.append(url)
                return original_collect_url(url, source, max_depth, seen)

            releases._collect_url = fake_collect_url
            try:
                result = releases.release_overview(
                    "alpha",
                    archive_base=str(archive),
                    release_page_url=str(dist.parent / "missing-download.html"),
                )
            finally:
                releases._collect_url = original_collect_url

        self.assertEqual(result["sources"]["dist"], str(dist.parent / "missing-download.html"))
        self.assertFalse(any(url.startswith(releases.DEFAULT_DIST_BASE) for url in calls))

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
        maven_artifacts = {
            "response": {
                "docs": [
                    {
                        "g": "org.apache.alpha",
                        "a": "alpha-core",
                        "latestVersion": "1.2.0-RC1",
                        "versionCount": 2,
                        "p": "jar",
                        "timestamp": 1767225600000,
                    }
                ]
            }
        }
        maven_versions = {
            "response": {
                "docs": [
                    {"g": "org.apache.alpha", "a": "alpha-core", "v": "1.1.0", "timestamp": 1764547200000},
                    {"g": "org.apache.alpha", "a": "alpha-core", "v": "1.2.0-RC1", "timestamp": 1767225600000},
                ]
            }
        }
        maven_pom = """\
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <description>Apache Alpha component</description>
  <licenses>
    <license><name>MIT</name></license>
  </licenses>
  <developers>
    <developer><name>Alpha Team</name></developer>
  </developers>
</project>
"""
        original = releases._read_url_json
        original_text = releases._read_url_text

        def fake_json(url: str) -> object:
            if url.startswith("https://maven.test/search?"):
                if "core=gav" in url:
                    return maven_versions
                return maven_artifacts
            return responses[url]

        releases._read_url_json = fake_json
        releases._read_url_text = lambda url: maven_pom
        try:
            result = releases.platform_distribution_checks(
                "alpha",
                docker_images=["apache/alpha"],
                pypi_packages=["apache-alpha"],
                maven_group_ids=["org.apache.alpha"],
                github_api_base="https://api.github.test/repos/apache",
                docker_api_base="https://docker.test",
                pypi_api_base="https://pypi.test",
                maven_search_base="https://maven.test/search",
                maven_repository_base="https://repo.maven.test/maven2",
            )
        finally:
            releases._read_url_json = original
            releases._read_url_text = original_text

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
        self.assertEqual(result["maven"][0]["group_id"], "org.apache.alpha")
        self.assertEqual(result["maven"][0]["artifacts"][0]["artifact_id"], "alpha-core")
        self.assertTrue(result["maven"][0]["artifacts"][0]["latest_version_looks_unapproved"])
        self.assertFalse(result["maven"][0]["artifacts"][0]["latest_pom"]["license_is_alv2"])
        self.assertIn("POM description", " ".join(result["hints"]["maven"]))
        self.assertIn("ALv2 license", " ".join(result["hints"]["maven"]))
        self.assertIn("source control", " ".join(result["hints"]["maven"]))


if __name__ == "__main__":
    unittest.main()
