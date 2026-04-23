from __future__ import annotations

from typing import Any

from apache_incubator_releases_mcp import releases, schemas

_CONFIGURED_DIST_BASE: str | None = None
_CONFIGURED_ARCHIVE_BASE: str | None = None


def configure_defaults(
    dist_base: str | None = None,
    archive_base: str | None = None,
) -> None:
    global _CONFIGURED_DIST_BASE, _CONFIGURED_ARCHIVE_BASE
    if dist_base:
        _CONFIGURED_DIST_BASE = dist_base
    if archive_base:
        _CONFIGURED_ARCHIVE_BASE = archive_base


def require_non_empty_string(value: Any, key: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"'{key}' must be a string")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"'{key}' must be a non-empty string")
    return stripped


def optional_string(value: Any, key: str) -> str | None:
    if value is None:
        return None
    return require_non_empty_string(value, key)


def optional_depth(value: Any) -> int:
    if value is None:
        return 1
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("'max_depth' must be an integer")
    if value < 0:
        raise ValueError("'max_depth' must be greater than or equal to 0")
    if value > 1:
        raise ValueError("'max_depth' must be 0 or 1 to keep checks scoped to one podling")
    return value


def resolve_dist_base(value: str | None = None) -> str:
    return optional_string(value, "dist_base") or _CONFIGURED_DIST_BASE or releases.DEFAULT_DIST_BASE


def resolve_archive_base(value: str | None = None) -> str:
    return optional_string(value, "archive_base") or _CONFIGURED_ARCHIVE_BASE or releases.DEFAULT_ARCHIVE_BASE


def podling_releases(
    podling: str,
    dist_base: str | None = None,
    archive_base: str | None = None,
    max_depth: int | None = None,
) -> dict[str, Any]:
    """Return release evidence for one Apache Incubator podling."""
    return releases.release_overview(
        require_non_empty_string(podling, "podling"),
        dist_base=resolve_dist_base(dist_base),
        archive_base=resolve_archive_base(archive_base),
        max_depth=optional_depth(max_depth),
    )


TOOLS: dict[str, dict[str, Any]] = {
    "podling_releases": schemas.tool_definition(
        description=(
            "Return release artifact, signature, checksum, cadence, and Incubator naming evidence "
            "for one Apache Incubator podling."
        ),
        handler=podling_releases,
        properties=schemas.podling_release_properties(),
        required=["podling"],
    ),
}
