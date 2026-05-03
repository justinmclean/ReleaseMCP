from __future__ import annotations

import contextvars
import html
import http.client
import json
import re
import ssl
import tarfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from xml.etree import ElementTree

DEFAULT_DIST_BASE = "https://dist.apache.org/repos/dist/release/incubator"
DEFAULT_ARCHIVE_BASE = "https://archive.apache.org/dist/incubator"
DEFAULT_GITHUB_API_BASE = "https://api.github.com/repos/apache"
DEFAULT_DOCKER_API_BASE = "https://hub.docker.com/v2/repositories"
DEFAULT_PYPI_API_BASE = "https://pypi.org/pypi"
DEFAULT_MAVEN_SEARCH_BASE = "https://search.maven.org/solrsearch/select"
DEFAULT_MAVEN_REPOSITORY_BASE = "https://repo1.maven.org/maven2"
MAVEN_ARTIFACT_LIMIT = 25
MAVEN_VERSION_LIMIT = 20
DISTRIBUTION_GUIDELINES_URL = "https://incubator.apache.org/guides/distribution.html"
RELEASE_DOWNLOAD_PAGES_URL = "https://infra.apache.org/release-download-pages.html"
RELEASE_PAGE_PATHS = (
    "downloads.html",
    "download.html",
    "downloads/",
    "download/",
    "releases.html",
    "releases/",
    "docs/download/",
    "docs/downloads/",
    "en/download/",
    "en/downloads/",
)
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
HTML_TAG_RE = re.compile(r"<[^>]+>")
DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}))?\b")
UNAPPROVED_TAG_RE = re.compile(
    r"(?:^|[-._\s])(?:rc\d*|candidate|nightly|snapshot|dev|master|main)(?:$|[-._\s])",
    re.I,
)
PYPI_PRERELEASE_RE = re.compile(r"(?:a|b|rc|dev)\d*$", re.I)
MAVEN_UNAPPROVED_SUFFIX_RE = re.compile(
    r"(?:^|[-.])(?:rc\d*|candidate|nightly|snapshot|dev|alpha|beta)(?:$|[-.])",
    re.I,
)
INCUBATOR_DISCLAIMER_RE = re.compile(
    r"\bincubat(?:ing|or|ion)\b.*\bdisclaimer\b"
    r"|\bdisclaimer\b.*\bincubat(?:ing|or|ion)\b"
    r"|\bundergoing incubation\b.*\bApache Software Foundation\b"
    r"|\bincubation is required\b.*\bnot yet been fully endorsed\b",
    re.I | re.S,
)


class _HtmlLinkScanner(HTMLParser):
    """Streaming HTML parser: accumulates (href, anchor_text, tail_text) tuples and visible text.

    Skips the bodies of <script>, <style>, and <noscript> tags entirely so that
    inline JavaScript or CSS (which can be megabytes) is never buffered.
    Visible text is also capped at _MAX_VISIBLE bytes so that text-heavy pages
    don't exhaust memory during keyword scanning.
    """

    convert_charrefs = True
    _SKIP_TAGS = frozenset({"script", "style", "noscript"})
    _MAX_VISIBLE = 256 * 1024  # 256 KB is more than enough for keyword detection
    _MAX_LINKS = 2000  # cap link list; no release directory or download page needs more

    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str, str]] = []
        self._visible: list[str] = []
        self._visible_len: int = 0
        self._href: str | None = None
        self._text: list[str] = []
        self._pending_href: str | None = None
        self._pending_text: str | None = None
        self._tail: list[str] = []
        self._tail_len: int = 0
        self._skip_depth: int = 0  # nesting depth inside script/style/noscript

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lower = tag.lower()
        if lower in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        self._flush_pending()
        if lower == "a" and len(self.links) < self._MAX_LINKS:
            self._href = dict(attrs).get("href") or ""
            self._text = []

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()
        if lower in self._SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth:
            return
        if lower == "a" and self._href is not None:
            self._pending_href = self._href
            self._pending_text = "".join(self._text)
            self._tail = []
            self._tail_len = 0
            self._href = None

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return  # discard script/style/noscript bodies immediately
        if self._href is not None:
            self._text.append(data)
        elif self._pending_href is not None:
            # Tail text is only used for date/size in directory listings; 1 KB is plenty.
            if self._tail_len < 1024:
                self._tail.append(data)
                self._tail_len += len(data)
        if self._visible_len < self._MAX_VISIBLE:
            self._visible.append(data)
            self._visible_len += len(data)

    def _flush_pending(self) -> None:
        if self._pending_href is not None:
            self.links.append((self._pending_href, self._pending_text or "", "".join(self._tail)))
            self._pending_href = None
            self._pending_text = None
            self._tail = []
            self._tail_len = 0

    def close(self) -> None:
        self._flush_pending()
        super().close()

    @property
    def visible_text(self) -> str:
        return "".join(self._visible)


class _HttpSession:
    """Per-operation HTTP connection pool with streaming HTML support.

    Maintains one persistent connection per (scheme, host, port) tuple so that
    the dist, archive, and project-website fetches that happen during a single
    podling check share connections instead of opening separate TCP handshakes.
    """

    _UA = "apache-incubator-releases-mcp/0.1.0"
    _CHUNK = 65536  # 64 KB read chunks for streaming

    def __init__(self) -> None:
        self._conns: dict[tuple[str, str, int], http.client.HTTPConnection] = {}

    def _open(self, scheme: str, host: str, port: int) -> http.client.HTTPConnection:
        key = (scheme, host, port)
        conn = self._conns.get(key)
        if conn is None:
            if scheme == "https":
                ctx = ssl.create_default_context()
                conn = http.client.HTTPSConnection(host, port, timeout=30, context=ctx)
            else:
                conn = http.client.HTTPConnection(host, port, timeout=30)
            self._conns[key] = conn
        return conn

    def _request(self, url: str, depth: int = 0) -> http.client.HTTPResponse:
        if depth > 5:
            raise URLError("too many redirects")
        parsed = urllib.parse.urlparse(url)
        scheme = parsed.scheme
        host = parsed.hostname or ""
        port = parsed.port or (443 if scheme == "https" else 80)
        path = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")
        conn = self._open(scheme, host, port)
        for attempt in range(2):
            try:
                conn.request("GET", path, headers={"User-Agent": self._UA, "Accept-Encoding": "identity"})
                resp = conn.getresponse()
                break
            except (http.client.HTTPException, OSError):
                if attempt:
                    raise
                conn.close()
                key = (scheme, host, port)
                if scheme == "https":
                    ctx = ssl.create_default_context()
                    conn = http.client.HTTPSConnection(host, port, timeout=30, context=ctx)
                else:
                    conn = http.client.HTTPConnection(host, port, timeout=30)
                self._conns[key] = conn
        if resp.status in (301, 302, 303, 307, 308):
            location = resp.getheader("Location") or ""
            resp.read()
            if location:
                return self._request(urllib.parse.urljoin(url, location), depth + 1)
        if resp.status >= 400:
            resp.read()
            raise HTTPError(url, resp.status, resp.reason, {}, None)
        return resp

    def get_text(self, url: str) -> str:
        return self._request(url).read().decode("utf-8", errors="replace")

    def scan_page(self, url: str) -> tuple[list[tuple[str, str, str]], str]:
        """Stream-parse an HTML page.

        Returns ``(links, visible_text)`` where ``links`` is a list of
        ``(href, anchor_text, tail_text)`` tuples and ``visible_text`` is the
        concatenated text content.  The response body is processed in
        ``_CHUNK``-byte increments and discarded immediately, so peak memory is
        proportional to the number of anchor tags and visible text — not the
        full HTML size.
        """
        resp = self._request(url)
        scanner = _HtmlLinkScanner()
        try:
            while True:
                chunk = resp.read(self._CHUNK)
                if not chunk:
                    break
                scanner.feed(chunk.decode("utf-8", errors="replace"))
        finally:
            scanner.close()
            resp.read()  # drain so the connection can be reused
        return scanner.links, scanner.visible_text

    def close(self) -> None:
        for conn in self._conns.values():
            try:
                conn.close()
            except Exception:
                pass
        self._conns.clear()

    def __enter__(self) -> "_HttpSession":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class _NullContext:
    """No-op context manager used when a session is already active."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_: object) -> None:
        pass


# Active session for the current logical operation.  Set by collect_files /
# release_overview so that all internal HTTP calls share connections.
_session_var: contextvars.ContextVar[_HttpSession | None] = contextvars.ContextVar(
    "_session_var", default=None
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
    session = _session_var.get()
    if session is not None:
        return session.get_text(url)
    request = urllib.request.Request(url, headers={"User-Agent": "apache-incubator-releases-mcp/0.1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def _scan_url_page(url: str) -> tuple[list[tuple[str, str, str]], str]:
    """Stream-parse an HTML page at *url*, returning (links, visible_text).

    This is a module-level function so tests can monkey-patch it to inject
    canned HTML without making real HTTP requests.
    """
    session = _session_var.get()
    if session is not None:
        return session.scan_page(url)
    with _HttpSession() as tmp:
        return tmp.scan_page(url)


def _scan_local_page(path: Path) -> tuple[list[tuple[str, str, str]], str]:
    text = path.expanduser().read_text(encoding="utf-8", errors="replace")
    scanner = _HtmlLinkScanner()
    scanner.feed(text)
    scanner.close()
    return scanner.links, scanner.visible_text


def _fetch_and_scan_page(location: str) -> tuple[list[tuple[str, str, str]], str]:
    """Scan an HTML page from a URL (streaming) or a local path (full read)."""
    if _is_url(location):
        return _scan_url_page(location)
    return _scan_local_page(Path(location))



def _read_url_json(url: str) -> Any:
    return json.loads(_read_url_text(url))


def _parse_listing_tail(tail: str) -> tuple[str | None, str | None]:
    """Extract (last_modified ISO string, size string) from a directory-listing tail."""
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
    return last_modified, size


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
        raw_links, _ = _scan_url_page(url)
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
    for href, label, tail in raw_links:
        if href.startswith("?") or href.startswith("#") or href in {"../", "/"}:
            continue
        full = urllib.parse.urljoin(url, href)
        if href.endswith("/"):
            if max_depth > 0:
                child_files, _ = _collect_url(full, source, max_depth - 1, seen)
                files.extend(child_files)
            continue
        name = urllib.parse.unquote(Path(urllib.parse.urlparse(full).path).name)
        last_modified, size = _parse_listing_tail(tail)
        files.append(
            ReleaseFile(
                name=name,
                location=full,
                source=source,
                path=urllib.parse.unquote(urllib.parse.urlparse(full).path),
                kind=_kind(name),
                artifact_name=_artifact_name(name),
                version=_version(name),
                last_modified=last_modified,
                size=size,
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
    dist_base: str | None = DEFAULT_DIST_BASE,
    archive_base: str = DEFAULT_ARCHIVE_BASE,
    max_depth: int = 1,
) -> dict[str, Any]:
    slug = podling_slug(podling)
    sources = {"archive": _join_source(archive_base, slug)}
    if dist_base is not None:
        sources = {"dist": _join_source(dist_base, slug), **sources}
    # Create a session if one isn't already active so dist + archive share connections.
    ctx: _HttpSession | _NullContext = _NullContext() if _session_var.get() else _HttpSession()
    with ctx as new_session:
        token = _session_var.set(new_session) if new_session is not None else None
        try:
            files: list[ReleaseFile] = []
            source_statuses: list[SourceStatus] = []
            for source, location in sources.items():
                if _is_url(location):
                    source_files, status = _collect_url(location, source, max_depth)
                else:
                    source_files, status = _collect_local(Path(location), source, max_depth)
                files.extend(source_files)
                source_statuses.append(status)
        finally:
            if token is not None:
                _session_var.reset(token)
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


def _build_page_links(raw_links: list[tuple[str, str, str]], page_url: str) -> list[dict[str, Any]]:
    """Convert raw scanner tuples into the structured link dicts used by release-page checks."""
    links = []
    for href, text, _tail in raw_links:
        href = href.strip()
        if not href or href.startswith("#"):
            continue
        resolved = href if href.startswith("[preferred]") else urllib.parse.urljoin(page_url, href)
        parsed = urllib.parse.urlparse(resolved)
        links.append(
            {
                "href": href,
                "resolved": resolved,
                "text": text,
                "scheme": parsed.scheme or None,
                "host": parsed.netloc or None,
                "path": urllib.parse.unquote(parsed.path),
                "uses_closer_lua": "/dyn/closer.lua/" in parsed.path,
                "uses_preferred_variable": href.startswith("[preferred]/"),
                "is_https_downloads_apache": parsed.scheme == "https"
                and parsed.netloc == "downloads.apache.org",
                "is_direct_dist_apache": parsed.netloc == "dist.apache.org",
            }
        )
    return links


def _release_file_from_link(link: dict[str, Any], source: str) -> ReleaseFile | None:
    name = _link_basename(link)
    kind = _kind(name)
    if kind == "other":
        return None
    return ReleaseFile(
        name=name,
        location=str(link["resolved"]),
        source=source,
        path=str(link["path"]).lstrip("/"),
        kind=kind,
        artifact_name=_artifact_name(name),
        version=_version(name),
    )


def _release_page_files(podling: str, release_page_url: str) -> tuple[list[ReleaseFile], SourceStatus]:
    try:
        raw_links, _ = _fetch_and_scan_page(release_page_url)
    except Exception as exc:
        return [], SourceStatus(
            source="dist",
            location=release_page_url,
            available=False,
            transport="http" if _is_url(release_page_url) else "local",
            file_count=0,
            error=_url_error_message(exc),
        )

    slug = podling_slug(podling)
    links = _build_page_links(raw_links, release_page_url)
    files: list[ReleaseFile] = []
    seen: set[str] = set()
    for link in links:
        if not (
            link["uses_closer_lua"]
            or link["uses_preferred_variable"]
            or link["is_https_downloads_apache"]
        ):
            continue
        if link["uses_closer_lua"] and not _is_top_level_closer_link(link, slug):
            pass
        elif link["uses_closer_lua"]:
            continue
        file = _release_file_from_link(link, "dist")
        if file is None or file.location in seen:
            continue
        seen.add(file.location)
        files.append(file)

    return files, SourceStatus(
        source="dist",
        location=release_page_url,
        available=True,
        transport="http" if _is_url(release_page_url) else "local",
        file_count=len(files),
    )


def _is_release_page_candidate(
    links: list[dict[str, Any]], visible_text: str, files: list[ReleaseFile]
) -> bool:
    text = visible_text.lower()
    current_artifact_names = {item.name for item in _source_artifacts(files) if item.source == "dist"}
    linked_names = {_link_basename(link) for link in links}
    has_known_artifact = bool(current_artifact_names & linked_names)
    has_release_link = any(
        link["uses_closer_lua"] or link["uses_preferred_variable"] or link["is_https_downloads_apache"]
        for link in links
    )
    has_download_text = "download" in text or "release" in text
    has_verification_hint = any(word in text for word in ("checksum", "signature", "pgp", "sha"))
    return has_known_artifact or (has_release_link and (has_download_text or has_verification_hint))



def discover_release_page_url(podling: str, files: list[ReleaseFile]) -> dict[str, Any]:
    slug = podling_slug(podling)
    base_url = f"https://{slug}.apache.org/"
    candidates = [urllib.parse.urljoin(base_url, path) for path in RELEASE_PAGE_PATHS]
    attempted: list[str] = []
    errors: dict[str, str] = {}

    for candidate in candidates:
        if candidate in attempted:
            continue
        attempted.append(candidate)
        try:
            raw_links, visible_text = _scan_url_page(candidate)
        except Exception as exc:
            errors[candidate] = _url_error_message(exc)
            continue
        page_links = _build_page_links(raw_links, candidate)
        if _is_release_page_candidate(page_links, visible_text, files):
            return {"found": True, "location": candidate, "attempted": attempted, "errors": errors}

    return {"found": False, "location": None, "attempted": attempted, "errors": errors}


def _link_basename(link: dict[str, Any]) -> str:
    path = str(link.get("path") or link.get("href") or "")
    if link.get("uses_preferred_variable"):
        path = str(link["href"])
    return Path(path).name


def _is_top_level_closer_link(link: dict[str, Any], podling: str) -> bool:
    if not link["uses_closer_lua"]:
        return False
    parsed = urllib.parse.urlparse(str(link["resolved"]))
    suffix = parsed.path.split("/dyn/closer.lua/", 1)[-1].strip("/")
    parts = [part for part in suffix.split("/") if part]
    return len(parts) <= 1 or (len(parts) == 2 and parts[0] == "incubator" and parts[1] == podling)


def _has_verification_instructions(visible_text: str, links: list[dict[str, Any]]) -> bool:
    text = visible_text.lower()
    link_targets = " ".join(str(link["resolved"]).lower() for link in links)
    verification_words = ("checksum", "sha", "signature", "pgp", "openpgp")
    return (
        ("verify" in text or "verification" in text)
        and any(word in text for word in verification_words)
    ) or "release-signing" in link_targets or "/dev/release" in link_targets


def release_page_checks(
    podling: str,
    release_page_url: str,
    files: list[ReleaseFile],
) -> dict[str, Any]:
    slug = podling_slug(podling)
    try:
        raw_links, visible_text = _fetch_and_scan_page(release_page_url)
    except Exception as exc:
        return {
            "guidelines": RELEASE_DOWNLOAD_PAGES_URL,
            "location": release_page_url,
            "available": False,
            "error": _url_error_message(exc),
            "facts": {},
            "hints": ["Release download page could not be inspected."],
        }

    links = _build_page_links(raw_links, release_page_url)
    current_sources = [item for item in _source_artifacts(files) if item.source == "dist"]
    current_artifact_names = {item.name for item in current_sources}
    current_signature_names = {
        item.name
        for item in files
        if item.source == "dist"
        and item.kind == "signature"
        and item.artifact_name in current_artifact_names
    }
    current_checksum_names = {
        item.name
        for item in files
        if item.source == "dist"
        and item.kind == "checksum"
        and item.artifact_name in current_artifact_names
    }
    linked_names = {_link_basename(link) for link in links}
    artifact_links = [
        link
        for link in links
        if _link_basename(link) in current_artifact_names
        or (
            _kind(_link_basename(link)) in {"source_artifact", "artifact"}
            and not link["is_https_downloads_apache"]
        )
    ]
    closer_artifact_links = [
        link
        for link in artifact_links
        if link["uses_closer_lua"] or link["uses_preferred_variable"]
    ]
    checksum_links = [link for link in links if _kind(_link_basename(link)) == "checksum"]
    signature_links = [link for link in links if _kind(_link_basename(link)) == "signature"]
    keys_links = [link for link in links if _link_basename(link).lower() in {"keys", "keys.txt"}]
    bad_dist_links = [link for link in links if link["is_direct_dist_apache"]]
    top_level_closer_links = [link for link in links if _is_top_level_closer_link(link, slug)]
    verification_instructions = _has_verification_instructions(visible_text, links)

    facts = {
        "link_count": len(links),
        "current_source_artifacts": sorted(current_artifact_names),
        "linked_current_source_artifacts": sorted(current_artifact_names & linked_names),
        "linked_current_signatures": sorted(current_signature_names & linked_names),
        "linked_current_checksums": sorted(current_checksum_names & linked_names),
        "artifact_link_count": len(artifact_links),
        "closer_artifact_link_count": len(closer_artifact_links),
        "checksum_link_count": len(checksum_links),
        "signature_link_count": len(signature_links),
        "keys_link_count": len(keys_links),
        "has_https_downloads_keys_link": any(
            link["is_https_downloads_apache"] for link in keys_links
        ),
        "all_checksum_links_use_https_downloads": all(
            link["is_https_downloads_apache"] for link in checksum_links
        )
        if checksum_links
        else False,
        "all_signature_links_use_https_downloads": all(
            link["is_https_downloads_apache"] for link in signature_links
        )
        if signature_links
        else False,
        "has_direct_dist_apache_links": bool(bad_dist_links),
        "has_top_level_closer_lua_links": bool(top_level_closer_links),
        "has_verification_instructions": verification_instructions,
    }

    hints: list[str] = []
    if current_artifact_names and not current_artifact_names & linked_names:
        hints.append(
            "No current source distribution from downloads.apache.org/dist evidence "
            "appears linked on the page."
        )
    if not closer_artifact_links:
        hints.append("No release artifact links using closer.lua or [preferred] were found.")
    if current_checksum_names and not current_checksum_names & linked_names:
        hints.append("No checksum links were found for the current source distributions.")
    if checksum_links and not facts["all_checksum_links_use_https_downloads"]:
        hints.append("Some checksum links do not use https://downloads.apache.org/.")
    if current_signature_names and not current_signature_names & linked_names:
        hints.append("No detached signature links were found for the current source distributions.")
    if signature_links and not facts["all_signature_links_use_https_downloads"]:
        hints.append("Some detached signature links do not use https://downloads.apache.org/.")
    if not facts["has_https_downloads_keys_link"]:
        hints.append("No HTTPS KEYS link to downloads.apache.org was found.")
    if bad_dist_links:
        hints.append(
            "The page links directly to dist.apache.org; download pages should use "
            "closer.lua for artifacts."
        )
    if top_level_closer_links:
        hints.append(
            "The page links to a top-level closer.lua project path instead of a "
            "release artifact path."
        )
    if not verification_instructions:
        hints.append(
            "No visible instructions or documentation link for verifying downloads was found."
        )

    return {
        "guidelines": RELEASE_DOWNLOAD_PAGES_URL,
        "location": release_page_url,
        "available": True,
        "facts": facts,
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
        label = " ".join(part for part in (tag_name, name) if part)
        releases.append(
            {
                "tag_name": tag_name or None,
                "name": name or None,
                "html_url": item.get("html_url"),
                "draft": bool(item.get("draft")),
                "prerelease": bool(item.get("prerelease")),
                "published_at": item.get("published_at"),
                "contains_incubator_disclaimer": _contains_incubator_disclaimer(str(item.get("body") or "")),
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
    del payload  # free the full JSON dict; we've extracted everything we need
    releases = []
    if isinstance(releases_payload, dict):
        for version, files in releases_payload.items():
            file_items = files if isinstance(files, list) else []
            version_text = str(version)
            all_have_digests = all(bool(item.get("digests")) for item in file_items if isinstance(item, dict))
            any_has_sig = any(bool(item.get("has_sig")) for item in file_items if isinstance(item, dict))
            all_yanked = all(bool(item.get("yanked")) for item in file_items if isinstance(item, dict))
            releases.append(
                {
                    "version": version_text,
                    "file_count": len(file_items),
                    "is_prerelease": _is_pypi_prerelease(version_text),
                    "looks_like_rc_nightly_snapshot_or_dev": _is_unapproved_label(version_text),
                    "all_files_have_digests": all_have_digests if file_items else False,
                    "any_file_has_signature": any_has_sig,
                    "all_files_yanked": all_yanked if file_items else False,
                }
            )
    del releases_payload
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


def _maven_search(query: str, maven_search_base: str, *, rows: int, core: str | None = None) -> Any:
    params = {"q": query, "rows": str(rows), "wt": "json"}
    if core:
        params["core"] = core
    return _read_url_json(f"{maven_search_base}?{urllib.parse.urlencode(params)}")


def _maven_docs(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    response = payload.get("response")
    if not isinstance(response, dict):
        return []
    docs = response.get("docs")
    if not isinstance(docs, list):
        return []
    return [item for item in docs if isinstance(item, dict)]


def _maven_pom_url(group_id: str, artifact_id: str, version: str, maven_repository_base: str) -> str:
    group_path = "/".join(group_id.split("."))
    filename = f"{artifact_id}-{version}.pom"
    return f"{maven_repository_base.rstrip('/')}/{group_path}/{artifact_id}/{version}/{filename}"


def _xml_text(element: ElementTree.Element, path: str) -> str | None:
    found = element.find(path)
    if found is None or found.text is None:
        return None
    text = found.text.strip()
    return text or None


def _maven_pom_facts(
    url: str,
    maven_repository_base: str,
    seen: set[str] | None = None,
) -> dict[str, Any]:
    seen = seen or set()
    if url in seen:
        return {
            "location": url,
            "available": False,
            "error": "Cyclic Maven parent POM reference",
        }
    seen.add(url)
    try:
        root = ElementTree.fromstring(_read_url_text(url))
    except Exception as exc:
        return {
            "location": url,
            "available": False,
            "error": _url_error_message(exc),
        }
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"
    path = f".//{namespace}"
    description = _xml_text(root, f"{path}description")
    name = _xml_text(root, f"{path}name")
    licenses = [
        {
            "name": _xml_text(license_item, f"{namespace}name"),
            "url": _xml_text(license_item, f"{namespace}url"),
        }
        for license_item in root.findall(f"{path}license")
    ]
    developers = [
        {
            "name": _xml_text(developer, f"{namespace}name"),
            "organization": _xml_text(developer, f"{namespace}organization"),
        }
        for developer in root.findall(f"{path}developer")
    ]
    scm = root.find(f"{path}scm")
    scm_facts = None
    if scm is not None:
        scm_facts = {
            "connection": _xml_text(scm, f"{namespace}connection"),
            "developer_connection": _xml_text(scm, f"{namespace}developerConnection"),
            "url": _xml_text(scm, f"{namespace}url"),
        }
    organization = root.find(f"{path}organization")
    organization_facts = None
    if organization is not None:
        organization_facts = {
            "name": _xml_text(organization, f"{namespace}name"),
            "url": _xml_text(organization, f"{namespace}url"),
        }
    parent_facts = None
    parent = root.find(f"{path}parent")
    if parent is not None:
        parent_group_id = _xml_text(parent, f"{namespace}groupId")
        parent_artifact_id = _xml_text(parent, f"{namespace}artifactId")
        parent_version = _xml_text(parent, f"{namespace}version")
        if parent_group_id and parent_artifact_id and parent_version:
            parent_url = _maven_pom_url(
                parent_group_id,
                parent_artifact_id,
                parent_version,
                maven_repository_base,
            )
            parent_facts = _maven_pom_facts(parent_url, maven_repository_base, seen)
    license_values = [
        value
        for license_item in licenses
        for value in (license_item["name"], license_item["url"])
    ]
    developer_values = [
        value
        for developer in developers
        for value in (developer["name"], developer["organization"])
    ]
    organization_values = list(organization_facts.values()) if organization_facts else []
    local_license_is_alv2 = any(_is_alv2_license(value) for value in license_values)
    local_developer_mentions_apache = any(
        "apache" in value.lower()
        for value in developer_values + organization_values + [name]
        if value
    )
    local_has_scm = bool(scm_facts and any(scm_facts.values()))
    parent_available = bool(parent_facts and parent_facts["available"])
    return {
        "location": url,
        "available": True,
        "name": name,
        "description_contains_incubator_disclaimer": _contains_incubator_disclaimer(description)
        or bool(parent_available and parent_facts["description_contains_incubator_disclaimer"]),
        "licenses": licenses,
        "license_is_alv2": local_license_is_alv2
        or bool(parent_available and parent_facts["license_is_alv2"]),
        "developers": developers,
        "organization": organization_facts,
        "developer_mentions_apache": local_developer_mentions_apache
        or bool(parent_available and parent_facts["developer_mentions_apache"]),
        "scm": scm_facts,
        "has_scm": local_has_scm or bool(parent_available and parent_facts["has_scm"]),
        "parent": parent_facts,
    }


def _maven_artifact_versions(group_id: str, artifact_id: str, maven_search_base: str) -> list[dict[str, Any]]:
    payload = _maven_search(
        f'g:"{group_id}" AND a:"{artifact_id}"',
        maven_search_base,
        rows=MAVEN_VERSION_LIMIT,
        core="gav",
    )
    versions = []
    for item in _maven_docs(payload):
        version = str(item.get("v") or item.get("latestVersion") or "")
        if not version:
            continue
        versions.append(
            {
                "version": version,
                "timestamp": item.get("timestamp"),
                "looks_like_rc_nightly_snapshot_or_dev": _is_unapproved_label(version)
                or bool(MAVEN_UNAPPROVED_SUFFIX_RE.search(version)),
                "uses_clear_unapproved_suffix": bool(MAVEN_UNAPPROVED_SUFFIX_RE.search(version)),
            }
        )
    return sorted(versions, key=lambda item: str(item["version"]), reverse=True)


def _maven_group_facts(
    group_id: str,
    maven_search_base: str,
    maven_repository_base: str,
) -> dict[str, Any]:
    location = f"https://central.sonatype.com/search?q={urllib.parse.quote(f'g:{group_id}')}"
    try:
        payload = _maven_search(f'g:"{group_id}"', maven_search_base, rows=MAVEN_ARTIFACT_LIMIT)
    except Exception as exc:
        return {
            "group_id": group_id,
            "location": location,
            "available": False,
            "error": _url_error_message(exc),
            "artifacts": [],
        }
    docs = _maven_docs(payload)
    artifacts = []
    for item in docs:
        artifact_id = str(item.get("a") or "")
        latest_version = str(item.get("latestVersion") or "")
        item_group_id = str(item.get("g") or group_id)
        if not artifact_id or not latest_version:
            continue
        artifact = {
            "group_id": item_group_id,
            "artifact_id": artifact_id,
            "latest_version": latest_version,
            "version_count": item.get("versionCount"),
            "packaging": item.get("p"),
            "timestamp": item.get("timestamp"),
            "under_expected_group_id": item_group_id == group_id,
            "latest_version_looks_unapproved": _is_unapproved_label(latest_version)
            or bool(MAVEN_UNAPPROVED_SUFFIX_RE.search(latest_version)),
            "versions": _maven_artifact_versions(item_group_id, artifact_id, maven_search_base),
            "latest_pom": _maven_pom_facts(
                _maven_pom_url(item_group_id, artifact_id, latest_version, maven_repository_base),
                maven_repository_base,
            ),
        }
        artifacts.append(artifact)
    return {
        "group_id": group_id,
        "location": location,
        "available": True,
        "artifact_count": len(artifacts),
        "artifacts": sorted(artifacts, key=lambda item: item["artifact_id"]),
    }


def platform_distribution_checks(
    podling: str,
    *,
    github_project: str | None = None,
    docker_images: list[str] | None = None,
    pypi_packages: list[str] | None = None,
    maven_group_ids: list[str] | None = None,
    github_api_base: str = DEFAULT_GITHUB_API_BASE,
    docker_api_base: str = DEFAULT_DOCKER_API_BASE,
    pypi_api_base: str = DEFAULT_PYPI_API_BASE,
    maven_search_base: str = DEFAULT_MAVEN_SEARCH_BASE,
    maven_repository_base: str = DEFAULT_MAVEN_REPOSITORY_BASE,
) -> dict[str, Any]:
    slug = podling_slug(podling)
    github_project = github_project or slug
    docker_images = docker_images or [f"apache/{slug}", f"apache{slug}/{slug}"]
    pypi_packages = pypi_packages or [f"apache-{slug}"]
    maven_group_ids = maven_group_ids or [f"org.apache.{slug}"]

    github = _github_release_facts(github_project, github_api_base)
    docker = [_docker_repository_facts(image, docker_api_base) for image in docker_images]
    pypi = [_pypi_project_facts(package, pypi_api_base) for package in pypi_packages]
    maven = [
        _maven_group_facts(group_id, maven_search_base, maven_repository_base)
        for group_id in maven_group_ids
    ]

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

    maven_hints: list[str] = []
    available_maven = [item for item in maven if item["available"]]
    if not available_maven:
        maven_hints.append(
            "Maven Central artifacts could not be inspected; verify org.apache.<project> manually."
        )
    for group in available_maven:
        group_id = group["group_id"]
        if not group["artifact_count"]:
            maven_hints.append(f"No Maven Central artifacts were found for groupId {group_id}.")
            continue
        if not group_id.startswith("org.apache."):
            maven_hints.append(
                f"Maven groupId {group_id} is not under the expected org.apache.<project> namespace."
            )
        for artifact in group["artifacts"]:
            coordinate = f"{artifact['group_id']}:{artifact['artifact_id']}"
            pom = artifact["latest_pom"]
            if artifact["latest_version_looks_unapproved"]:
                maven_hints.append(
                    f"Verify Maven latest version for {coordinate} does not point to a release "
                    "candidate, nightly, snapshot, or dev build."
                )
            if any(
                item["looks_like_rc_nightly_snapshot_or_dev"]
                and not item["uses_clear_unapproved_suffix"]
                for item in artifact["versions"]
            ):
                maven_hints.append(
                    f"Some Maven RC, nightly, snapshot, or dev versions for {coordinate} "
                    "are not clearly marked with a version suffix."
                )
            if not pom["available"]:
                maven_hints.append(f"Latest Maven POM for {coordinate} could not be inspected.")
                continue
            if not pom["description_contains_incubator_disclaimer"]:
                maven_hints.append(
                    f"Maven POM description for {coordinate} does not include visible "
                    "incubation disclaimer text."
                )
            if not pom["license_is_alv2"]:
                maven_hints.append(
                    f"Maven POM for {coordinate} does not clearly set the ALv2 license."
                )
            if not pom["developer_mentions_apache"]:
                maven_hints.append(
                    f"Maven POM for {coordinate} does not clearly name Apache as developer or organization."
                )
            if not pom["has_scm"]:
                maven_hints.append(
                    f"Maven POM for {coordinate} does not include visible source control information."
                )
            maven_hints.append(
                f"Verify Maven artifacts for {coordinate} are made from IPMC-approved ASF releases."
            )

    return {
        "guidelines": DISTRIBUTION_GUIDELINES_URL,
        "github": github,
        "docker_hub": docker,
        "pypi": pypi,
        "maven": maven,
        "hints": {
            "github": github_hints,
            "docker_hub": docker_hints,
            "pypi": pypi_hints,
            "maven": maven_hints,
        },
    }


def release_overview(
    podling: str,
    *,
    dist_base: str | None = None,
    archive_base: str = DEFAULT_ARCHIVE_BASE,
    max_depth: int = 1,
    release_page_url: str | None = None,
    include_platforms: bool = False,
    github_project: str | None = None,
    docker_images: list[str] | None = None,
    pypi_packages: list[str] | None = None,
    maven_group_ids: list[str] | None = None,
    github_api_base: str = DEFAULT_GITHUB_API_BASE,
    docker_api_base: str = DEFAULT_DOCKER_API_BASE,
    pypi_api_base: str = DEFAULT_PYPI_API_BASE,
    maven_search_base: str = DEFAULT_MAVEN_SEARCH_BASE,
    maven_repository_base: str = DEFAULT_MAVEN_REPOSITORY_BASE,
) -> dict[str, Any]:
    # One session covers dist, archive, project-website, and release-page fetches.
    ctx: _HttpSession | _NullContext = _NullContext() if _session_var.get() else _HttpSession()
    with ctx as new_session:
        token = _session_var.set(new_session) if new_session is not None else None
        try:
            return _release_overview_impl(
                podling,
                dist_base=dist_base,
                archive_base=archive_base,
                max_depth=max_depth,
                release_page_url=release_page_url,
                include_platforms=include_platforms,
                github_project=github_project,
                docker_images=docker_images,
                pypi_packages=pypi_packages,
                maven_group_ids=maven_group_ids,
                github_api_base=github_api_base,
                docker_api_base=docker_api_base,
                pypi_api_base=pypi_api_base,
                maven_search_base=maven_search_base,
                maven_repository_base=maven_repository_base,
            )
        finally:
            if token is not None:
                _session_var.reset(token)


def _release_overview_impl(
    podling: str,
    *,
    dist_base: str | None = None,
    archive_base: str = DEFAULT_ARCHIVE_BASE,
    max_depth: int = 1,
    release_page_url: str | None = None,
    include_platforms: bool = False,
    github_project: str | None = None,
    docker_images: list[str] | None = None,
    pypi_packages: list[str] | None = None,
    maven_group_ids: list[str] | None = None,
    github_api_base: str = DEFAULT_GITHUB_API_BASE,
    docker_api_base: str = DEFAULT_DOCKER_API_BASE,
    pypi_api_base: str = DEFAULT_PYPI_API_BASE,
    maven_search_base: str = DEFAULT_MAVEN_SEARCH_BASE,
    maven_repository_base: str = DEFAULT_MAVEN_REPOSITORY_BASE,
) -> dict[str, Any]:
    resolved_release_page_url = release_page_url
    release_page_discovery = None
    if dist_base is None and not resolved_release_page_url:
        release_page_discovery = discover_release_page_url(podling, [])
        if release_page_discovery["found"]:
            resolved_release_page_url = str(release_page_discovery["location"])

    collected = collect_files(
        podling,
        dist_base=dist_base,
        archive_base=archive_base,
        max_depth=max_depth,
    )
    if dist_base is None and resolved_release_page_url:
        page_files, page_status = _release_page_files(podling, resolved_release_page_url)
        collected["sources"] = {"dist": resolved_release_page_url, **collected["sources"]}
        collected["source_statuses"] = [page_status.to_dict(), *collected["source_statuses"]]
        existing_files = [ReleaseFile(**item) for item in collected["files"]]
        unique = {
            (item.source, item.location): item
            for item in [*existing_files, *page_files]
        }
        collected["files"] = [
            item.to_dict()
            for item in sorted(unique.values(), key=lambda item: item.location)
        ]
        collected["count"] = len(unique)
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
    if release_page_discovery is not None:
        result["release_page_discovery"] = release_page_discovery
    if not resolved_release_page_url and dist_base is not None and _is_url(dist_base):
        discovery = discover_release_page_url(podling, files)
        result["release_page_discovery"] = discovery
        if discovery["found"]:
            resolved_release_page_url = str(discovery["location"])
    if resolved_release_page_url:
        result["release_page_checks"] = release_page_checks(podling, resolved_release_page_url, files)
    if include_platforms:
        result["platform_distribution_checks"] = platform_distribution_checks(
            podling,
            github_project=github_project,
            docker_images=docker_images,
            pypi_packages=pypi_packages,
            maven_group_ids=maven_group_ids,
            github_api_base=github_api_base,
            docker_api_base=docker_api_base,
            pypi_api_base=pypi_api_base,
            maven_search_base=maven_search_base,
            maven_repository_base=maven_repository_base,
        )
    return result
