from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def sha256_file(path: str | Path) -> str:
    """Return SHA256 digest for a local model or metadata artifact."""
    artifact = Path(path)
    digest = sha256()
    with artifact.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_size(path: str | Path) -> int:
    """Return local artifact size in bytes."""
    return Path(path).stat().st_size
