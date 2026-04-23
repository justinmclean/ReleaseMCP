from __future__ import annotations

from typing import Any

PODLING_PROPERTY = {"type": "string", "description": "Apache Incubator podling id or name"}
DIST_BASE_PROPERTY = {
    "type": "string",
    "description": "Optional dist.apache.org base URL or local release directory",
}
ARCHIVE_BASE_PROPERTY = {
    "type": "string",
    "description": "Optional archive.apache.org base URL or local archive directory",
}
MAX_DEPTH_PROPERTY = {
    "type": "integer",
    "description": "Maximum traversal depth under the podling directory; defaults to 1",
}
BOOLEAN_PROPERTY = {"type": "boolean", "description": "Optional boolean flag"}


def input_schema(
    properties: dict[str, Any], *, required: list[str] | None = None
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


def tool_definition(
    *,
    description: str,
    handler: Any,
    properties: dict[str, Any],
    required: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "description": description,
        "inputSchema": input_schema(properties, required=required),
        "handler": handler,
    }


def podling_release_properties() -> dict[str, Any]:
    return {
        "podling": PODLING_PROPERTY,
        "dist_base": DIST_BASE_PROPERTY,
        "archive_base": ARCHIVE_BASE_PROPERTY,
        "max_depth": MAX_DEPTH_PROPERTY,
    }
