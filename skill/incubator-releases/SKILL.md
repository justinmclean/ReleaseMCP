---
name: incubator-releases
description: >
  Use this skill to check release evidence for one Apache Incubator podling:
  current and archived release files, source artifacts, detached signatures,
  checksum sidecars, last release date and cadence, Incubating naming and
  DISCLAIMER hints, release download page checks against ASF Infra guidance,
  and optional GitHub, Docker Hub, PyPI, and Maven distribution hints against
  the ASF Incubator distribution guide.
---

# Incubator Releases Skill

This skill drives the `incubator-releases` MCP server, which answers one
question well: what release evidence exists for a single Apache Incubator
podling, and how does it compare with ASF release and distribution guidance.

It is evidence gathering, not adjudication. Keep observed facts separate from
hints, and phrase anything beyond the observed facts as follow-up for the
podling, its mentors, or the IPMC.

## Scope

- One podling per call. The server does not scan all podlings, and there is no
  bulk mode. Ask for the podling name first if the user has not given one.
- Release evidence only. Release votes, report narratives, and mentor coverage
  come from other Incubator tools, not this one.
- Network reads of `dist.apache.org`, `archive.apache.org`, project websites,
  and, when asked, GitHub, Docker Hub, PyPI, and Maven Central.

## Core Workflow

Start with the defaults and let the server discover the download page:

```text
incubator-releases:podling_releases(podling="<name>")
```

Add platform distribution evidence only when the user asks about GitHub
releases, Docker images, PyPI packages, or Maven artifacts, because it costs
extra network calls:

```text
incubator-releases:podling_releases(podling="<name>", include_platforms=true)
```

Point at a known download page when the user names one, or when discovery
found nothing:

```text
incubator-releases:podling_releases(
  podling="<name>",
  release_page_url="https://<podling>.apache.org/downloads.html",
)
```

Point at a local directory or a specific dist path when checking a release
candidate or a local mirror:

```text
incubator-releases:podling_releases(podling="<name>", dist_base="/path/to/release/dir")
```

See `references/tools.md` for the full argument list and response shape.

## Reading The Response

Read `source_statuses` before drawing any conclusion. A source that could not
be read is not the same as a source with no files, and reporting "no releases"
for a fetch failure is the main way this skill goes wrong.

Then work through:

- `releases` and `release_count` for the grouped release versions.
- `source_artifact_count`, `signature_count`, `checksum_count` for whether
  each source artifact carries a detached signature and a checksum sidecar.
- `cadence` for the last release date and the observed interval. Cadence is an
  observation from file dates, not a compliance verdict.
- `incubating_hints` for Incubating naming and DISCLAIMER signals.
- `release_page_discovery` and `release_page_checks` for the download page.
- `platform_distribution_checks` when `include_platforms` was set.

## Reporting

- Name the source of every fact: current dist, archive, the download page, or
  a platform API.
- Say plainly when a check could not run, and why.
- Some guideline items cannot be settled from metadata alone, such as whether
  a Docker `latest` tag, a PyPI latest version, or a Maven artifact points only
  at an IPMC-approved release. Report those as needing human confirmation
  rather than as a pass or a fail.
- Cite the guidance the server cites: the ASF Incubator distribution guide at
  https://incubator.apache.org/guides/distribution.html and the ASF Infra
  release download page guidance at
  https://infra.apache.org/release-download-pages.html.
