from __future__ import annotations

import html
import json
import re
import tarfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

DEFAULT_DIST_BASE = "https://dist.apache.org/repos/dist/release/incubator"
DEFAULT_ARCHIVE_BASE = "https://archive.apache.org/dist/incubator"
DEFAULT_GITHUB_API_BASE = "https://api.github.com/repos/apache"
DEFAULT_DOCKER_API_BASE = "https://hub.docker.com/v2/repositories"
DEFAULT_PYPI_API_BASE = "https://pypi.org/pypi"
DISTRIBUTION_GUIDELINES_URL = "https://incubator.apache.org/guides/distribution.html"
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
UNAPPROVED_TAG_RE = re.compile(
    r"(?:^|[-._\s])(?:rc\d*|candidate|nightly|snapshot|dev|master|main)(?:$|[-._\s])",
    re.I,
)
PYPI_PRERELEASE_RE = re.compile(r"(?:a|b|rc|dev)\d*$", re.I)
INCUBATOR_DISCLAIMER_RE = re.compile(
    r"\bincubat(?:ing|or)\b.*\bdisclaimer\b|\bdisclaimer\b.*\bincubat(?:ing|or)\b",
    re.I | re.S,
)


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


@dataclass(frozen=True)
class SourceStatus:
    source: str
    location: str
    available: bool
    transport: str
    file_count: int
    error: str | None = None

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


def _read_url_json(url: str) -> Any:
    return json.loads(_read_url_text(url))


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


def _collect_local(root: Path, source: str, max_depth: int) -> tuple[list[ReleaseFile], SourceStatus]:
    if not root.exists():
        return [], SourceStatus(
            source=source,
            location=str(root.resolve()),
            available=False,
            transport="filesystem",
            file_count=0,
            error=f"Directory not found: {root.resolve()}",
        )
    files: list[ReleaseFile] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        depth = len(path.relative_to(root).parts) - 1
        if depth <= max_depth:
            files.append(_file_from_path(path, root, source))
    return files, SourceStatus(
        source=source,
        location=str(root.resolve()),
        available=True,
        transport="filesystem",
        file_count=len(files),
    )


def _url_error_message(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        return f"HTTP {exc.code} for {exc.url}"
    if isinstance(exc, URLError):
        return f"URL error: {exc.reason}"
    return str(exc) or exc.__class__.__name__


def _collect_url(url: str, source: str, max_depth: int, seen: set[str] | None = None) -> tuple[list[ReleaseFile], SourceStatus]:
    seen = seen or set()
    if url in seen:
        return [], SourceStatus(
            source=source,
            location=url,
            available=True,
            transport="http",
            file_count=0,
        )
    seen.add(url)
    try:
        entries = _parse_listing_entries(_read_url_text(url))
    except Exception as exc:
        return [], SourceStatus(
            source=source,
            location=url,
            available=False,
            transport="http",
            file_count=0,
            error=_url_error_message(exc),
        )
    files: list[ReleaseFile] = []
    for entry in entries:
        href = str(entry["href"])
        full = urllib.parse.urljoin(url, href)
        if href.endswith("/"):
            if max_depth > 0:
                child_files, _ = _collect_url(full, source, max_depth - 1, seen)
                files.extend(child_files)
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
    return files, SourceStatus(
        source=source,
        location=url,
        available=True,
        transport="http",
        file_count=len(files),
    )


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
    source_statuses: list[SourceStatus] = []
    for source, location in sources.items():
        if _is_url(location):
            source_files, status = _collect_url(location, source, max_depth)
        else:
            source_files, status = _collect_local(Path(location), source, max_depth)
        files.extend(source_files)
        source_statuses.append(status)
    unique = {(item.source, item.location): item for item in files}
    return {
        "podling": podling,
        "podling_slug": slug,
        "sources": sources,
        "source_statuses": [status.to_dict() for status in source_statuses],
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


def _contains_incubator_disclaimer(value: str | None) -> bool:
    return bool(value and INCUBATOR_DISCLAIMER_RE.search(value))


def _is_unapproved_label(value: str | None) -> bool:
    return bool(value and UNAPPROVED_TAG_RE.search(value))


def _is_pypi_prerelease(version: str | None) -> bool:
    return bool(version and PYPI_PRERELEASE_RE.search(version.replace("-", "")))


def _is_alv2_license(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.lower()
    return "apache" in normalized and ("2.0" in normalized or "software license" in normalized)


def _github_release_facts(project: str, github_api_base: str) -> dict[str, Any]:
    url = f"{github_api_base.rstrip('/')}/{project}/releases"
    try:
        payload = _read_url_json(url)
    except Exception as exc:
        return {
            "source": "github",
            "location": url,
            "available": False,
            "error": _url_error_message(exc),
            "releases": [],
        }
    if not isinstance(payload, list):
        return {
            "source": "github",
            "location": url,
            "available": False,
            "error": "Unexpected GitHub releases response",
            "releases": [],
        }
    releases = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        tag_name = str(item.get("tag_name") or "")
        name = str(item.get("name") or "")
        body = str(item.get("body") or "")
        label = " ".join(part for part in (tag_name, name) if part)
        releases.append(
            {
                "tag_name": tag_name or None,
                "name": name or None,
                "html_url": item.get("html_url"),
                "draft": bool(item.get("draft")),
                "prerelease": bool(item.get("prerelease")),
                "published_at": item.get("published_at"),
                "contains_incubator_disclaimer": _contains_incubator_disclaimer(body),
                "looks_like_rc_nightly_snapshot_or_dev": _is_unapproved_label(label),
            }
        )
    return {
        "source": "github",
        "location": url,
        "available": True,
        "release_count": len(releases),
        "releases": releases,
    }


def _pypi_project_facts(package: str, pypi_api_base: str) -> dict[str, Any]:
    url = f"{pypi_api_base.rstrip('/')}/{urllib.parse.quote(package)}/json"
    try:
        payload = _read_url_json(url)
    except Exception as exc:
        return {
            "package": package,
            "location": url,
            "available": False,
            "error": _url_error_message(exc),
            "releases": [],
        }
    if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
        return {
            "package": package,
            "location": url,
            "available": False,
            "error": "Unexpected PyPI project response",
            "releases": [],
        }
    info = payload["info"]
    description = "\n".join(
        str(info.get(key) or "") for key in ("summary", "description")
    )
    classifiers = [
        str(item)
        for item in info.get("classifiers") or []
        if isinstance(item, str)
    ]
    releases_payload = payload.get("releases") or {}
    releases = []
    if isinstance(releases_payload, dict):
        for version, files in releases_payload.items():
            file_items = files if isinstance(files, list) else []
            release_files = [
                {
                    "filename": str(item.get("filename") or ""),
                    "packagetype": item.get("packagetype"),
                    "python_version": item.get("python_version"),
                    "upload_time_iso_8601": item.get("upload_time_iso_8601"),
                    "has_digests": bool(item.get("digests")),
                    "has_signature": bool(item.get("has_sig")),
                    "yanked": bool(item.get("yanked")),
                }
                for item in file_items
                if isinstance(item, dict)
            ]
            version_text = str(version)
            releases.append(
                {
                    "version": version_text,
                    "file_count": len(release_files),
                    "files": release_files,
                    "is_prerelease": _is_pypi_prerelease(version_text),
                    "looks_like_rc_nightly_snapshot_or_dev": _is_unapproved_label(version_text),
                    "all_files_have_digests": all(item["has_digests"] for item in release_files)
                    if release_files
                    else False,
                    "any_file_has_signature": any(item["has_signature"] for item in release_files),
                    "all_files_yanked": all(item["yanked"] for item in release_files)
                    if release_files
                    else False,
                }
            )
    return {
        "package": package,
        "location": f"https://pypi.org/project/{urllib.parse.quote(package)}/",
        "api_location": url,
        "available": True,
        "name": info.get("name"),
        "version": info.get("version"),
        "summary": info.get("summary"),
        "license": info.get("license"),
        "classifiers": classifiers,
        "contains_incubator_disclaimer": _contains_incubator_disclaimer(description),
        "license_is_alv2": _is_alv2_license(str(info.get("license") or ""))
        or any(_is_alv2_license(item) for item in classifiers),
        "latest_version_is_prerelease": _is_pypi_prerelease(str(info.get("version") or "")),
        "latest_version_looks_unapproved": _is_unapproved_label(str(info.get("version") or "")),
        "release_count": len(releases),
        "releases": sorted(releases, key=lambda item: item["version"], reverse=True),
    }


def _docker_json(path: str, docker_api_base: str) -> Any:
    return _read_url_json(f"{docker_api_base.rstrip('/')}/{path.lstrip('/')}")


def _docker_repository_facts(image: str, docker_api_base: str) -> dict[str, Any]:
    namespace, _, repository = image.partition("/")
    if not namespace or not repository:
        return {
            "image": image,
            "available": False,
            "error": "Docker image must use namespace/repository form",
            "tags": [],
        }
    repository_path = f"{namespace}/{repository}/"
    location = f"{docker_api_base.rstrip('/')}/{repository_path}"
    try:
        metadata = _docker_json(repository_path, docker_api_base)
        tag_payload = _docker_json(f"{repository_path}tags?page_size=100", docker_api_base)
    except Exception as exc:
        return {
            "image": image,
            "location": location,
            "available": False,
            "error": _url_error_message(exc),
            "tags": [],
        }
    if not isinstance(metadata, dict) or not isinstance(tag_payload, dict):
        return {
            "image": image,
            "location": location,
            "available": False,
            "error": "Unexpected Docker Hub response",
            "tags": [],
        }
    overview = "\n".join(
        str(metadata.get(key) or "") for key in ("description", "full_description")
    )
    tag_items = tag_payload.get("results") or []
    tags = [
        {
            "name": str(item.get("name") or ""),
            "last_updated": item.get("last_updated"),
            "full_size": item.get("full_size"),
            "looks_like_rc_nightly_snapshot_or_dev": _is_unapproved_label(
                str(item.get("name") or "")
            ),
        }
        for item in tag_items
        if isinstance(item, dict)
    ]
    return {
        "image": image,
        "location": location,
        "available": True,
        "description": metadata.get("description"),
        "contains_incubator_disclaimer": _contains_incubator_disclaimer(overview),
        "tag_count": len(tags),
        "tags": tags,
        "latest_tag_present": any(item["name"] == "latest" for item in tags),
    }


def platform_distribution_checks(
    podling: str,
    *,
    github_project: str | None = None,
    docker_images: list[str] | None = None,
    pypi_packages: list[str] | None = None,
    github_api_base: str = DEFAULT_GITHUB_API_BASE,
    docker_api_base: str = DEFAULT_DOCKER_API_BASE,
    pypi_api_base: str = DEFAULT_PYPI_API_BASE,
) -> dict[str, Any]:
    slug = podling_slug(podling)
    github_project = github_project or slug
    docker_images = docker_images or [f"apache/{slug}", f"apache{slug}/{slug}"]
    pypi_packages = pypi_packages or [f"apache-{slug}"]

    github = _github_release_facts(github_project, github_api_base)
    docker = [_docker_repository_facts(image, docker_api_base) for image in docker_images]
    pypi = [_pypi_project_facts(package, pypi_api_base) for package in pypi_packages]

    github_hints: list[str] = []
    if not github["available"]:
        github_hints.append(
            "GitHub releases could not be inspected; "
            "verify the apache/<project> release page manually."
        )
    elif github.get("releases"):
        releases = github["releases"]
        if any(not item["contains_incubator_disclaimer"] for item in releases):
            github_hints.append(
                "Some GitHub releases do not include visible incubation disclaimer text."
            )
        if any(
            item["looks_like_rc_nightly_snapshot_or_dev"] and not item["prerelease"]
            for item in releases
        ):
            github_hints.append(
                "Some GitHub release candidates, nightlies, snapshots, or dev builds "
                "are not marked as prereleases."
            )
        github_hints.append(
            "Confirm any releases from before incubation are clearly described and tagged as such."
        )
    else:
        github_hints.append(
            "No GitHub release entries were found; GitHub tags may still need manual review."
        )

    docker_hints: list[str] = []
    available_docker = [item for item in docker if item["available"]]
    if not available_docker:
        docker_hints.append(
            "Docker Hub repositories could not be inspected; "
            "verify apache/<project> or apache<project>/<project> manually."
        )
    for repository in available_docker:
        image = repository["image"]
        if not repository["contains_incubator_disclaimer"]:
            docker_hints.append(
                f"Docker Hub overview for {image} does not include visible "
                "incubation disclaimer text."
            )
        if repository["latest_tag_present"]:
            docker_hints.append(
                f"Verify Docker Hub latest tag for {image} points only to "
                "an IPMC-approved ASF release."
            )
        if any(item["looks_like_rc_nightly_snapshot_or_dev"] for item in repository["tags"]):
            docker_hints.append(
                f"Verify RC, nightly, snapshot, or dev Docker tags for {image} "
                "are clearly labeled and not promoted as releases."
            )
        docker_hints.append(
            f"Verify any Dockerfile for {image} includes an ASF header and "
            "incubation disclaimer."
        )

    pypi_hints: list[str] = []
    available_pypi = [item for item in pypi if item["available"]]
    if not available_pypi:
        pypi_hints.append(
            "PyPI projects could not be inspected; verify apache-<project> manually."
        )
    for project in available_pypi:
        package = project["package"]
        if not project["contains_incubator_disclaimer"]:
            pypi_hints.append(
                f"PyPI project description for {package} does not include visible "
                "incubation disclaimer text."
            )
        if not project["license_is_alv2"]:
            pypi_hints.append(
                f"PyPI metadata for {package} does not clearly display the ALv2 license."
            )
        if project["latest_version_is_prerelease"] or project["latest_version_looks_unapproved"]:
            pypi_hints.append(
                f"Verify PyPI latest version for {package} does not point to a release "
                "candidate, nightly, snapshot, or dev build."
            )
        if any(
            item["looks_like_rc_nightly_snapshot_or_dev"] and not item["is_prerelease"]
            for item in project["releases"]
        ):
            pypi_hints.append(
                f"Some PyPI RC, nightly, snapshot, or dev versions for {package} "
                "do not look like PyPI pre-releases."
            )
        if any(not item["all_files_have_digests"] for item in project["releases"]):
            pypi_hints.append(
                f"Some PyPI files for {package} do not expose digest metadata."
            )
        if not any(
            item["any_file_has_signature"]
            for item in project["releases"]
            if not item["all_files_yanked"]
        ):
            pypi_hints.append(
                f"No non-yanked PyPI files for {package} expose signature metadata; "
                "verify convenience artifacts are otherwise signed or verifiable."
            )
        pypi_hints.append(
            f"Verify pip install {package} installs only artifacts made from "
            "IPMC-approved ASF releases."
        )

    return {
        "guidelines": DISTRIBUTION_GUIDELINES_URL,
        "github": github,
        "docker_hub": docker,
        "pypi": pypi,
        "hints": {
            "github": github_hints,
            "docker_hub": docker_hints,
            "pypi": pypi_hints,
        },
    }


def release_overview(
    podling: str,
    *,
    dist_base: str = DEFAULT_DIST_BASE,
    archive_base: str = DEFAULT_ARCHIVE_BASE,
    max_depth: int = 1,
    include_platforms: bool = False,
    github_project: str | None = None,
    docker_images: list[str] | None = None,
    pypi_packages: list[str] | None = None,
    github_api_base: str = DEFAULT_GITHUB_API_BASE,
    docker_api_base: str = DEFAULT_DOCKER_API_BASE,
    pypi_api_base: str = DEFAULT_PYPI_API_BASE,
) -> dict[str, Any]:
    collected = collect_files(
        podling,
        dist_base=dist_base,
        archive_base=archive_base,
        max_depth=max_depth,
    )
    files = [ReleaseFile(**item) for item in collected["files"]]
    groups = _release_groups(files)
    result = {
        **collected,
        "release_count": len(groups),
        "releases": groups,
        "cadence": release_cadence(files),
        "source_artifact_count": len(_source_artifacts(files)),
        "signature_count": sum(1 for item in files if item.kind == "signature"),
        "checksum_count": sum(1 for item in files if item.kind == "checksum"),
        "incubating_hints": incubating_hints(files),
    }
    if include_platforms:
        result["platform_distribution_checks"] = platform_distribution_checks(
            podling,
            github_project=github_project,
            docker_images=docker_images,
            pypi_packages=pypi_packages,
            github_api_base=github_api_base,
            docker_api_base=docker_api_base,
            pypi_api_base=pypi_api_base,
        )
    return result
