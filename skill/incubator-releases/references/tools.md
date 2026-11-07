# Tool Reference

The `incubator-releases` server exposes one tool.

## `podling_releases`

Returns release artifact, signature, checksum, cadence, and Incubator naming
evidence for one podling, plus optional release-page and platform distribution
checks.

### Arguments

| Name | Type | Notes |
| --- | --- | --- |
| `podling` | string | Required. Podling id or name. |
| `dist_base` | string | Optional current release base URL or local release directory. When omitted, the server discovers the podling download page and uses its release links as current distribution evidence. |
| `archive_base` | string | Optional `archive.apache.org` base URL or local archive directory. Defaults to `https://archive.apache.org/dist/incubator`. |
| `max_depth` | integer | Optional traversal depth under the podling directory, `0` to `2`. Defaults to `2`, which reaches `<version>/<subproject>/<file>` trees such as Toree's. Values above `2` are rejected so checks stay scoped to one podling. A directory that cannot be read sets that source's `error` in `source_statuses`. |
| `release_page_url` | string | Optional download page URL or local HTML file. When omitted and `dist_base` is not set, the server tries a bounded list of observed `https://<podling>.apache.org/` download and release page paths. |
| `include_platforms` | boolean | Optional. When true, fetches GitHub releases, Docker Hub, PyPI, and Maven Central metadata. Defaults to false. |
| `github_project` | string | Optional `apache/<project>` repository name. Defaults to the podling slug. |
| `docker_images` | string[] | Optional images in `namespace/repository` form. Defaults to the guideline patterns `apache/<podling>` and `apache<podling>/<podling>`. |
| `pypi_packages` | string[] | Optional package names. Defaults to the guideline pattern `apache-<podling>`. |
| `maven_group_ids` | string[] | Optional groupIds. Defaults to the guideline pattern `org.apache.<podling>`. |

`dist_base` and `archive_base` are overrides. The server can also be started
with `--dist-base` and `--archive-base` defaults, so omit these arguments on
routine calls.

### Response Keys

| Key | Meaning |
| --- | --- |
| `sources` | The dist and archive locations actually used. |
| `source_statuses` | Per source: whether it was read, and the error if not. Check this first. |
| `files`, `count` | Every release file found, with kind, version, and location. |
| `releases`, `release_count` | Files grouped by release version. |
| `cadence` | Last release date, observed intervals, and a cadence label. |
| `source_artifact_count` | Source artifacts found, as distinct from binaries and sidecars. |
| `signature_count`, `checksum_count` | Detached `.asc`/`.sig` and checksum sidecars found. |
| `incubating_hints` | Incubating naming and DISCLAIMER signals. |
| `release_page_discovery` | Present when the server looked for a download page. Includes `found` and `location`. |
| `release_page_checks` | Present when a download page was found or supplied. |
| `platform_distribution_checks` | Present when `include_platforms` was true. |

### Release Page Checks

Checks the download page against ASF Infra guidance: current source artifacts
linked through `closer.lua` or `[preferred]`, checksum, signature, and KEYS
links served from `https://downloads.apache.org/`, no direct
`dist.apache.org` artifact links, no top-level `closer.lua` links, and visible
verification instructions.

### Platform Distribution Checks

Checks GitHub, Docker Hub, PyPI, and Maven Central against the ASF Incubator
distribution guide, keeping observed facts separate from hints. Items that
metadata cannot settle, such as whether a Docker `latest` tag points only at an
IPMC-approved ASF release, are reported as needing human confirmation.
