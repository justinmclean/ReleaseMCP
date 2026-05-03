from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from apache_incubator_releases_mcp import tools
from tests.fixtures import release_dirs


class ToolsTests(unittest.TestCase):
    def test_podling_releases_tool(self) -> None:
        with release_dirs() as (dist, archive):
            result = tools.podling_releases("alpha", dist_base=str(dist), archive_base=str(archive))

        self.assertEqual(result["podling_slug"], "alpha")
        self.assertEqual(result["source_artifact_count"], 2)
        self.assertEqual(len(result["source_statuses"]), 2)

    def test_tools_registered_with_schemas(self) -> None:
        self.assertIn("podling_releases", tools.TOOLS)
        self.assertEqual(
            tools.TOOLS["podling_releases"]["inputSchema"]["properties"]["podling"]["type"],
            "string",
        )
        self.assertEqual(
            tools.TOOLS["podling_releases"]["inputSchema"]["properties"]["include_platforms"]["type"],
            "boolean",
        )
        self.assertEqual(
            tools.TOOLS["podling_releases"]["inputSchema"]["properties"]["pypi_packages"]["type"],
            "array",
        )


if __name__ == "__main__":
    unittest.main()
