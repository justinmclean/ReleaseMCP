# Release MCP

A small MCP server for checking release evidence for one Apache Incubator podling at a time.

It checks:

- release files for a named podling under `dist.apache.org` and `archive.apache.org`
- last release date and observed cadence
- source artifacts, detached signatures, and checksum sidecars
- Incubating naming and disclaimer hints
- optional GitHub, Docker Hub, and PyPI distribution hints against the ASF Incubator distribution guide

## Requirements

- Python 3.11+

## Install

```bash
python3 -m pip install .
```

For development tools:

```bash
python3 -m pip install -e .[dev]
```

## Run

After installation, run the stdio MCP server with:

```bash
incubator-releases-mcp
```

For local development without installing first, run:

```bash
python3 server.py
```

The server uses `stdio`, so it is intended to be launched by an MCP client.

## Example MCP client config

```json
{
  "mcpServers": {
    "incubator-releases": {
      "command": "python3",
      "args": [
        "/Users/justinmclean/ReleaseMCP/server.py"
      ]
    }
  }
}
```

Configure startup defaults with command-line arguments:

- `--dist-base`: optional `dist.apache.org` base URL or local release directory
- `--archive-base`: optional `archive.apache.org` base URL or local archive directory

Tool calls can override those defaults with `dist_base` and `archive_base`. The server does not scan all podlings; every
tool call requires a single `podling`.

## Test

```bash
python3 -m unittest discover -s tests -v
```

## Coverage

```bash
python3 -m coverage run -m unittest discover -s tests
python3 -m coverage report -m
```

Coverage is scoped to the local `apache_incubator_releases_mcp` package.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the module layout, runtime flow, and parsing model.

## Usage Examples

These examples show prompts an IPMC member or mentor could type into an MCP client.

- "Check the latest releases for FooPodling."
- "Show the source artifacts, signatures, and checksums for FooPodling."
- "When was FooPodling's last Apache release, and what cadence does it appear to have?"
- "Does FooPodling's release evidence show incubating naming and disclaimer hints?"
- "Check FooPodling release evidence, including GitHub, Docker Hub, and PyPI distribution hints."

## Tools

### `podling_releases`

Return release artifact, signature, checksum, cadence, Incubator naming evidence, and optional GitHub/Docker Hub/PyPI
distribution evidence for one Apache Incubator podling.

The response also includes `source_statuses`, so callers can distinguish "no files found" from a source that could not
be read.

Arguments:

- `podling`: podling id or name
- `dist_base`: optional `dist.apache.org` base URL or local release directory
- `archive_base`: optional `archive.apache.org` base URL or local archive directory
- `max_depth`: optional traversal depth under the podling directory, either `0` or `1`; defaults to `1`
- `include_platforms`: optional boolean; when true, fetches GitHub releases, Docker Hub metadata, and PyPI metadata
- `github_project`: optional apache/<project> GitHub repository name; defaults to the podling slug
- `docker_images`: optional Docker Hub image names in `namespace/repository` form; defaults to both ASF Incubator
  guideline patterns, `apache/<podling>` and `apache<podling>/<podling>`
- `pypi_packages`: optional PyPI package names; defaults to the ASF Incubator guideline pattern, `apache-<podling>`

Platform checks cite the ASF Incubator distribution guide and keep observed facts separate from hints. Some guideline
items still require human confirmation, such as whether Docker `latest` or PyPI's latest version points only to an
IPMC-approved ASF release.
