from __future__ import annotations

import html
import re
import tarfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

DEFAULT_DIST_BASE = "https://dist.apache.org/repos/dist/release/incubator"
DEFAULT_ARCHIVE_BASE = "https://archive.apache.org/dist/incubator"
ARCHIVE_SUFFIXES = (
    ".tar.gz",
    ".tar.bz2",
    ".tar.xz",
    ".tgz",
    ".zip",
)
CHECKSUM_SUFFIXES = (".sha512", ".sha256", ".sha1", ".md5")
SIGNATURE_SUFFIXES = (".asc", ".sig")
SIDE_SUFFIXES = SIGNATURE_SUFFIXES + CHECKSUM_SUFFIXES
VERSION_RE = re.compile(r"(?<!\d)v?(\d+(?:[._-]\d+)+(?:[-._][A-Za-z0-9]+)*)")
HTML_HREF_RE = re.compile(r'<a\s+[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>(.*)', re.I)
HTML_TAG_RE = re.compile(r"<[^>]+>")
DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}))?\b")


@dataclass(frozen=True)
class ReleaseFile:
    name: str
    location: str
    source: str
    path: str
    kind: str
    artifact_name: str
    version: str | None = None
    last_modified: str | None = None
    size: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _now_date() -> date:
    return datetime.now(UTC).date()


def podling_slug(podling: str) -> str:
    slug = re.sub(r"^apache[-_ ]+", "", podling.strip(), flags=re.I)
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", slug).strip("-").lower()
    if not slug:
        raise ValueError("'podling' must be a non-empty string")
    return slug


def _is_url(value: str) -> bool:
    return urllib.parse.urlparse(value).scheme in {"http", "https"}


def _join_source(base: str, podling: str) -> str:
    if _is_url(base):
        return urllib.parse.urljoin(base.rstrip("/") + "/", podling + "/")
    return str((Path(base).expanduser() / podling).resolve())


def _read_url_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "apache-incubator-releases-mcp/0.1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def _parse_listing_entries(text: str) -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    for line in text.splitlines():
        match = HTML_HREF_RE.search(line)
        if not match:
            continue
        href = html.unescape(match.group(1))
        label = html.unescape(HTML_TAG_RE.sub("", match.group(2))).strip()
        if href.startswith("?") or href.startswith("#") or href in {"../", "/"}:
            continue
        tail = html.unescape(HTML_TAG_RE.sub(" ", match.group(3)))
        date_match = DATE_RE.search(tail)
        last_modified = None
        if date_match:
            hour = int(date_match.group(4) or 0)
            minute = int(date_match.group(5) or 0)
            last_modified = (
                datetime(
                    int(date_match.group(1)),
                    int(date_match.group(2)),
                    int(date_match.group(3)),
                    hour,
                    minute,
                    tzinfo=UTC,
                )
                .isoformat()
                .replace("+00:00", "Z")
            )
        size = None
        size_match = re.search(r"\s(\d+(?:\.\d+)?[KMGTP]?|-)\s*$", tail.strip())
        if size_match:
            size = None if size_match.group(1) == "-" else size_match.group(1)
        entries.append({"href": href, "label": label or href, "last_modified": last_modified, "size": size})
    return entries


def _artifact_name(name: str) -> str:
    result = name
    for suffix in SIDE_SUFFIXES:
        if result.lower().endswith(suffix):
            result = result[: -len(suffix)]
            break
    return result


def _kind(name: str) -> str:
    lower = name.lower()
    if lower.endswith(SIGNATURE_SUFFIXES):
        return "signature"
    if lower.endswith(CHECKSUM_SUFFIXES):
        return "checksum"
    if lower.endswith(ARCHIVE_SUFFIXES):
        if any(token in lower for token in ("source", "src")) and "binary" not in lower and "-bin" not in lower:
            return "source_artifact"
        return "artifact"
    if lower in {"keys", "keys.txt"}:
        return "keys"
    return "other"


def _version(name: str) -> str | None:
    match = VERSION_RE.search(name)
    if not match:
        return None
    return match.group(1).replace("_", ".")


def _file_from_path(path: Path, root: Path, source: str) -> ReleaseFile:
    stat = path.stat()
    relative = path.relative_to(root).as_posix()
    modified = datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat().replace("+00:00", "Z")
    return ReleaseFile(
        name=path.name,
        location=str(path.resolve()),
        source=source,
        path=relative,
        kind=_kind(path.name),
        artifact_name=_artifact_name(path.name),
        version=_version(path.name),
        last_modified=modified,
        size=str(stat.st_size),
    )


def _collect_local(root: Path, source: str, max_depth: int) -> list[ReleaseFile]:
    if not root.exists():
        return []
    files: list[ReleaseFile] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        depth = len(path.relative_to(root).parts) - 1
        if depth <= max_depth:
            files.append(_file_from_path(path, root, source))
    return files


def _collect_url(url: str, source: str, max_depth: int, seen: set[str] | None = None) -> list[ReleaseFile]:
    seen = seen or set()
    if url in seen:
        return []
    seen.add(url)
    try:
        entries = _parse_listing_entries(_read_url_text(url))
    except Exception:
        return []
    files: list[ReleaseFile] = []
    for entry in entries:
        href = str(entry["href"])
        full = urllib.parse.urljoin(url, href)
        if href.endswith("/"):
            if max_depth > 0:
                files.extend(_collect_url(full, source, max_depth - 1, seen))
            continue
        name = urllib.parse.unquote(Path(urllib.parse.urlparse(full).path).name)
        files.append(
            ReleaseFile(
                name=name,
                location=full,
                source=source,
                path=urllib.parse.unquote(urllib.parse.urlparse(full).path),
                kind=_kind(name),
                artifact_name=_artifact_name(name),
                version=_version(name),
                last_modified=entry["last_modified"],
                size=entry["size"],
            )
        )
    return files


def collect_files(
    podling: str,
    *,
    dist_base: str = DEFAULT_DIST_BASE,
    archive_base: str = DEFAULT_ARCHIVE_BASE,
    max_depth: int = 1,
) -> dict[str, Any]:
    slug = podling_slug(podling)
    sources = {
        "dist": _join_source(dist_base, slug),
        "archive": _join_source(archive_base, slug),
    }
    files: list[ReleaseFile] = []
    for source, location in sources.items():
        if _is_url(location):
            files.extend(_collect_url(location, source, max_depth))
        else:
            files.extend(_collect_local(Path(location), source, max_depth))
    unique = {(item.source, item.location): item for item in files}
    return {
        "podling": podling,
        "podling_slug": slug,
        "sources": sources,
        "count": len(unique),
        "files": [item.to_dict() for item in sorted(unique.values(), key=lambda item: item.location)],
    }


def _iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _source_artifacts(files: list[ReleaseFile]) -> list[ReleaseFile]:
    source_files = [item for item in files if item.kind == "source_artifact"]
    if source_files:
        return source_files
    return [
        item
        for item in files
        if item.kind == "artifact" and "bin" not in item.name.lower() and "binary" not in item.name.lower()
    ]


def _sidecars_for(artifact: ReleaseFile, files: list[ReleaseFile]) -> dict[str, list[dict[str, Any]]]:
    signatures = [
        item.to_dict()
        for item in files
        if item.kind == "signature" and item.artifact_name == artifact.name
    ]
    checksums = [
        item.to_dict()
        for item in files
        if item.kind == "checksum" and item.artifact_name == artifact.name
    ]
    return {"signatures": signatures, "checksums": checksums}


def _release_groups(files: list[ReleaseFile]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for artifact in _source_artifacts(files):
        key = artifact.version or artifact.artifact_name
        group = groups.setdefault(
            key,
            {
                "version": artifact.version,
                "source_artifacts": [],
                "latest_date": None,
                "sources": sorted({artifact.source}),
            },
        )
        group["source_artifacts"].append({**artifact.to_dict(), **_sidecars_for(artifact, files)})
        group["sources"] = sorted(set(group["sources"]) | {artifact.source})
        artifact_date = _iso_date(artifact.last_modified)
        latest_date = _iso_date(group["latest_date"])
        if artifact_date and (latest_date is None or artifact_date > latest_date):
            group["latest_date"] = artifact_date.isoformat()
    return sorted(
        groups.values(),
        key=lambda group: (group["latest_date"] or "", group["version"] or ""),
        reverse=True,
    )


def release_cadence(files: list[ReleaseFile]) -> dict[str, Any]:
    groups = [group for group in _release_groups(files) if group.get("latest_date")]
    dates = sorted({_iso_date(group["latest_date"]) for group in groups if group["latest_date"]}, reverse=True)
    resolved_dates = [item for item in dates if item is not None]
    intervals = [
        (resolved_dates[index] - resolved_dates[index + 1]).days
        for index in range(len(resolved_dates) - 1)
    ]
    return {
        "last_release_date": resolved_dates[0].isoformat() if resolved_dates else None,
        "release_count_with_dates": len(resolved_dates),
        "days_since_last_release": (_now_date() - resolved_dates[0]).days if resolved_dates else None,
        "interval_days": intervals,
        "average_interval_days": round(sum(intervals) / len(intervals), 1) if intervals else None,
        "cadence": _cadence_label(intervals),
    }


def _cadence_label(intervals: list[int]) -> str:
    if not intervals:
        return "insufficient dated releases"
    average = sum(intervals) / len(intervals)
    if average <= 45:
        return "monthly or faster"
    if average <= 120:
        return "quarterly-ish"
    if average <= 240:
        return "semiannual-ish"
    return "infrequent"


def _inspect_local_disclaimer(location: str) -> dict[str, Any] | None:
    path = Path(location)
    if not path.exists() or not path.is_file():
        return None
    names: list[str] = []
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
        elif tarfile.is_tarfile(path):
            with tarfile.open(path) as archive:
                names = archive.getnames()
    except (OSError, tarfile.TarError, zipfile.BadZipFile):
        return None
    lower = [name.lower() for name in names]
    disclaimer_files = [names[index] for index, name in enumerate(lower) if Path(name).name == "disclaimer"]
    return {
        "checked": True,
        "disclaimer_files": disclaimer_files,
        "has_disclaimer_file": bool(disclaimer_files),
    }


def incubating_hints(files: list[ReleaseFile]) -> dict[str, Any]:
    source_artifacts = _source_artifacts(files)
    naming = [
        {
            "name": item.name,
            "location": item.location,
            "contains_incubating": "incubating" in item.name.lower(),
        }
        for item in source_artifacts
    ]
    local_disclaimers = [
        {"name": item.name, "location": item.location, **inspection}
        for item in source_artifacts
        if (inspection := _inspect_local_disclaimer(item.location)) is not None
    ]
    hints: list[str] = []
    if source_artifacts and any(not item["contains_incubating"] for item in naming):
        hints.append("Some source artifact names do not include 'incubating'.")
    if local_disclaimers and any(not item["has_disclaimer_file"] for item in local_disclaimers):
        hints.append("Some locally inspectable source artifacts do not contain a DISCLAIMER file.")
    if not local_disclaimers:
        hints.append("DISCLAIMER file presence was not inspected for remote source artifacts.")
    return {
        "artifact_naming": naming,
        "disclaimer_checks": local_disclaimers,
        "hints": hints,
    }


def release_overview(
    podling: str,
    *,
    dist_base: str = DEFAULT_DIST_BASE,
    archive_base: str = DEFAULT_ARCHIVE_BASE,
    max_depth: int = 1,
) -> dict[str, Any]:
    collected = collect_files(podling, dist_base=dist_base, archive_base=archive_base, max_depth=max_depth)
    files = [ReleaseFile(**item) for item in collected["files"]]
    groups = _release_groups(files)
    return {
        **collected,
        "release_count": len(groups),
        "releases": groups,
        "cadence": release_cadence(files),
        "source_artifact_count": len(_source_artifacts(files)),
        "signature_count": sum(1 for item in files if item.kind == "signature"),
        "checksum_count": sum(1 for item in files if item.kind == "checksum"),
        "incubating_hints": incubating_hints(files),
    }
