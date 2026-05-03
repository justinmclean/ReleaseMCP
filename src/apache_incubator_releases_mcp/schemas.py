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
GITHUB_PROJECT_PROPERTY = {
    "type": "string",
    "description": "Optional apache/<project> GitHub repository name; defaults to the podling slug",
}
DOCKER_IMAGES_PROPERTY = {
    "type": "array",
    "items": {"type": "string"},
    "description": "Optional Docker Hub images in namespace/repository form",
}
PYPI_PACKAGES_PROPERTY = {
    "type": "array",
    "items": {"type": "string"},
    "description": "Optional PyPI package names; defaults to apache-<podling>",
}
MAVEN_GROUP_IDS_PROPERTY = {
    "type": "array",
    "items": {"type": "string"},
    "description": "Optional Maven groupIds; defaults to org.apache.<podling>",
}


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
        "include_platforms": {
            "type": "boolean",
            "description": (
                "Fetch optional GitHub, Docker Hub, and PyPI distribution evidence; "
                "defaults to false"
            ),
        },
        "github_project": GITHUB_PROJECT_PROPERTY,
        "docker_images": DOCKER_IMAGES_PROPERTY,
        "pypi_packages": PYPI_PACKAGES_PROPERTY,
        "maven_group_ids": MAVEN_GROUP_IDS_PROPERTY,
    }
