from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from apache_incubator_releases_mcp import protocol


class ProtocolTests(unittest.TestCase):
    def test_initialize(self) -> None:
        response = protocol.handle_payload({"jsonrpc": "2.0", "id": 1, "method": "initialize"})

        self.assertEqual(response["result"]["serverInfo"]["name"], "apache-incubator-releases-mcp")

    def test_tools_list_includes_podling_releases(self) -> None:
        response = protocol.handle_payload({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        names = {tool["name"] for tool in response["result"]["tools"]}

        self.assertIn("podling_releases", names)


if __name__ == "__main__":
    unittest.main()
