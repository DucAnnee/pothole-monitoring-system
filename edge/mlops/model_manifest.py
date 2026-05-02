from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional

from .artifacts import file_size, sha256_file
from .manifest_signature import SIGNATURE_ALGORITHM, SIGNATURE_FIELD

ManifestModelType = Literal["yolo", "rfdetr"]
SCHEMA_VERSION = 1
SUPPORTED_MODEL_TYPES = {"yolo", "rfdetr"}


class ManifestError(Exception):
    """Raised when a hosted model manifest is incomplete or unsafe."""


def create_manifest(
    *,
    model_id: str,
    model_type: ManifestModelType,
    artifact_path: str | Path,
    confidence_threshold: float,
    input_size: Optional[int] = None,
    training_dataset: str = "",
    benchmark_summary_path: str | Path | None = None,
    min_edge_version: str = "0.1.0",
    artifact_uri: str = "",
    description: str = "",
    tags: Optional[Iterable[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a deployable model manifest from a local artifact."""
    artifact = Path(artifact_path)
    if not artifact.exists():
        raise ManifestError(f"Model artifact does not exist: {artifact}")

    manifest: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "model_id": model_id,
        "model_type": model_type,
        "artifact_name": artifact.name,
        "artifact_uri": artifact_uri,
        "artifact_sha256": sha256_file(artifact),
        "artifact_size_bytes": file_size(artifact),
        "confidence_threshold": float(confidence_threshold),
        "input_size": input_size,
        "training_dataset": training_dataset,
        "benchmark_summary_path": "",
        "benchmark_summary_sha256": "",
        "benchmark_summary_size_bytes": 0,
        "min_edge_version": min_edge_version,
        "description": description,
        "tags": list(tags or []),
        "metadata": metadata or {},
        "created_at_utc": _utc_now(),
    }

    if benchmark_summary_path:
        benchmark_summary = Path(benchmark_summary_path)
        if not benchmark_summary.exists():
            raise ManifestError(
                f"Benchmark summary does not exist: {benchmark_summary}"
            )
        manifest["benchmark_summary_path"] = benchmark_summary.name
        manifest["benchmark_summary_sha256"] = sha256_file(benchmark_summary)
        manifest["benchmark_summary_size_bytes"] = file_size(benchmark_summary)

    validate_manifest(manifest)
    return manifest


def load_manifest(path: str | Path) -> Dict[str, Any]:
    """Load and validate a manifest from disk."""
    manifest_path = Path(path)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestError(f"Invalid manifest JSON: {manifest_path}") from exc

    validate_manifest(manifest)
    return manifest


def write_manifest(manifest: Dict[str, Any], path: str | Path) -> None:
    """Write a manifest with stable formatting."""
    validate_manifest(manifest)
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def validate_manifest(manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the fields the edge updater must trust before deployment."""
    required = [
        "schema_version",
        "model_id",
        "model_type",
        "artifact_name",
        "artifact_sha256",
        "artifact_size_bytes",
        "confidence_threshold",
        "min_edge_version",
        "created_at_utc",
    ]
    missing = [field for field in required if field not in manifest]
    if missing:
        raise ManifestError(f"Manifest missing required fields: {', '.join(missing)}")

    if manifest["schema_version"] != SCHEMA_VERSION:
        raise ManifestError(
            f"Unsupported manifest schema_version: {manifest['schema_version']}"
        )

    if manifest["model_type"] not in SUPPORTED_MODEL_TYPES:
        raise ManifestError(f"Unsupported model_type: {manifest['model_type']}")

    if not _is_non_empty_string(manifest["model_id"]):
        raise ManifestError("model_id must be a non-empty string")
    if not _is_non_empty_string(manifest["artifact_name"]):
        raise ManifestError("artifact_name must be a non-empty string")

    digest = str(manifest["artifact_sha256"])
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest.lower()):
        raise ManifestError("artifact_sha256 must be a 64-character hex digest")

    if int(manifest["artifact_size_bytes"]) <= 0:
        raise ManifestError("artifact_size_bytes must be greater than 0")

    threshold = float(manifest["confidence_threshold"])
    if threshold < 0 or threshold > 1:
        raise ManifestError("confidence_threshold must be between 0 and 1")

    if SIGNATURE_FIELD in manifest:
        _validate_signature_metadata(manifest[SIGNATURE_FIELD])

    return manifest


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_signature_metadata(signature: Any) -> None:
    if not isinstance(signature, dict):
        raise ManifestError("signature must be an object")

    required = ["algorithm", "key_id", "value"]
    missing = [field for field in required if field not in signature]
    if missing:
        raise ManifestError(
            f"Manifest signature missing required fields: {', '.join(missing)}"
        )

    if signature["algorithm"] != SIGNATURE_ALGORITHM:
        raise ManifestError(
            f"Unsupported manifest signature algorithm: {signature['algorithm']}"
        )
    if not _is_non_empty_string(signature["key_id"]):
        raise ManifestError("signature.key_id must be a non-empty string")
    if not _is_non_empty_string(signature["value"]):
        raise ManifestError("signature.value must be a non-empty string")
