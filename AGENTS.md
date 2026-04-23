# Repository Guidelines

This project intentionally mirrors the lightweight Python layout used by the other Incubator MCP servers.

## Commands

- Run tests with `python3 -m unittest discover -s tests -v`.
- Run the local stdio server with `python3 server.py`.
- Install for development with `python3 -m pip install -e .[dev]`.

## Style

- Keep modules small and stdlib-first.
- Preserve the `protocol.py`, `tools.py`, `schemas.py`, `releases.py` separation.
- Avoid live network calls in unit tests; use local release directory fixtures.
- Release checks should keep source facts separate from human-facing hints.
