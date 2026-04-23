from __future__ import annotations

import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def release_dirs() -> Iterator[tuple[Path, Path]]:
    with tempfile.TemporaryDirectory() as temp_dir:
        base = Path(temp_dir)
        dist = base / "dist"
        archive = base / "archive"
        current = dist / "alpha"
        older = archive / "alpha" / "1.0.0"
        current.mkdir(parents=True)
        older.mkdir(parents=True)

        source = current / "apache-alpha-1.1.0-incubating-source-release.zip"
        with zipfile.ZipFile(source, "w") as archive_file:
            archive_file.writestr("apache-alpha-1.1.0-incubating/DISCLAIMER", "Apache Incubator disclaimer")
            archive_file.writestr("apache-alpha-1.1.0-incubating/README.md", "Alpha")
        (current / f"{source.name}.asc").write_text("signature", encoding="utf-8")
        (current / f"{source.name}.sha512").write_text("checksum", encoding="utf-8")
        (current / "KEYS").write_text("keys", encoding="utf-8")

        old_source = older / "apache-alpha-1.0.0-incubating-source-release.zip"
        with zipfile.ZipFile(old_source, "w") as archive_file:
            archive_file.writestr("apache-alpha-1.0.0-incubating/DISCLAIMER", "Apache Incubator disclaimer")
        (older / f"{old_source.name}.asc").write_text("signature", encoding="utf-8")
        (older / f"{old_source.name}.sha512").write_text("checksum", encoding="utf-8")

        yield dist, archive
