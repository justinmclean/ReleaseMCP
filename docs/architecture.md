# Architecture

The server follows the same small-module shape as the other Incubator MCP projects.

## Runtime Flow

1. `server.py` places `src/` on `sys.path` for local checkout execution.
2. `apache_incubator_releases_mcp.protocol` handles stdio JSON-RPC and MCP tool calls.
3. `apache_incubator_releases_mcp.tools` validates arguments and resolves source defaults.
4. `apache_incubator_releases_mcp.releases` reads Apache release directory listings or local fixture directories for one
   podling and derives release evidence.

## Source Data

The default upstream locations are:

```text
https://dist.apache.org/repos/dist/release/incubator
https://archive.apache.org/dist/incubator
```

For a podling such as `foo`, the server checks the `foo/` child directory under both sources. Tool calls can also point
`dist_base` and `archive_base` at local directories with the same podling subdirectory layout.

The MCP surface is intentionally narrow: callers must provide a single `podling`, and the default traversal only checks
the podling directory plus one nested version directory level. It does not enumerate all Incubator podlings.

## Parsing Model

Release files are classified by filename:

- source artifacts and other archives
- detached signatures
- checksum sidecars
- `KEYS` files and other supporting files

Source artifacts are grouped by detected version or artifact filename. Each group includes matching signatures and
checksums. Cadence is derived from listing or filesystem modification dates when available.

Incubating hints are intentionally evidence-oriented. The server checks whether source artifact names include
`incubating`; when artifacts are local and inspectable, it also looks for a `DISCLAIMER` file inside source archives.

Optional platform distribution checks inspect GitHub releases, Docker Hub repository metadata, and PyPI project metadata
when callers pass `include_platforms`. These checks cite the ASF Incubator distribution guide and return observed
platform facts separately from review hints, because several guideline items require human confirmation.
