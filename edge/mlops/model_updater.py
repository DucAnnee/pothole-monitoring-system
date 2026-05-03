from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Callable, Dict
from urllib.parse import urljoin, urlparse
from urllib.request import url2pathname, urlopen

from .artifacts import file_size, sha256_file
from .manifest_signature import SignatureError, verify_manifest_signature
from .model_manifest import ManifestError, validate_manifest, write_manifest
from .model_registry import ModelRegistry, RegistryError
from pipeline_logger import get_pipeline_logger, log_event

LOGGER = get_pipeline_logger("mlops.model_updater")


class UpdateError(Exception):
    """Raised when a remote model package cannot be fetched or deployed."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and deploy hosted edge models")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="Validate a hosted manifest")
    check.add_argument("--manifest-uri", required=True)
    check.add_argument("--timeout-seconds", type=_positive_float, default=30.0)
    _add_signature_args(check)

    fetch = subparsers.add_parser("fetch", help="Fetch a manifest and model artifact")
    fetch.add_argument("--manifest-uri", required=True)
    fetch.add_argument("--staging-dir", default="models/staging")
    fetch.add_argument("--timeout-seconds", type=_positive_float, default=30.0)
    _add_signature_args(fetch)

    deploy = subparsers.add_parser(
        "deploy", help="Fetch, register, and deploy a hosted model"
    )
    deploy.add_argument("--manifest-uri", required=True)
    deploy.add_argument("--staging-dir", default="models/staging")
    deploy.add_argument("--artifacts-dir", default="models/artifacts")
    deploy.add_argument("--registry-path", default="mlops/model_registry.json")
    deploy.add_argument("--reason", default="")
    deploy.add_argument("--operator", default="")
    deploy.add_argument("--timeout-seconds", type=_positive_float, default=30.0)
    deploy.add_argument(
        "--config",
        default="config.yaml",
        help="Config path used for the candidate model load smoke test",
    )
    deploy.add_argument(
        "--skip-smoke-test",
        action="store_true",
        help="Register and activate without loading the candidate model first",
    )
    _add_signature_args(deploy)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        if args.command == "check":
            manifest = load_manifest_uri(
                args.manifest_uri,
                timeout_seconds=args.timeout_seconds,
                **_signature_kwargs(args),
            )
            print(json.dumps({"valid": True, "manifest": manifest}, indent=2))
            return

        if args.command == "fetch":
            package = fetch_model_package(
                args.manifest_uri,
                args.staging_dir,
                timeout_seconds=args.timeout_seconds,
                **_signature_kwargs(args),
            )
            print(json.dumps(package, indent=2, sort_keys=True))
            return

        if args.command == "deploy":
            result = deploy_from_manifest(
                manifest_uri=args.manifest_uri,
                staging_dir=args.staging_dir,
                artifacts_dir=args.artifacts_dir,
                registry_path=args.registry_path,
                reason=args.reason,
                operator=args.operator,
                timeout_seconds=args.timeout_seconds,
                candidate_validator=(
                    None
                    if args.skip_smoke_test
                    else _build_candidate_load_validator(args.config)
                ),
                **_signature_kwargs(args),
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return
    except (ManifestError, OSError, RegistryError, SignatureError, UpdateError) as exc:
        raise SystemExit(f"Update error: {exc}") from exc


def load_manifest_uri(
    manifest_uri: str,
    *,
    timeout_seconds: float = 30.0,
    signature_public_key_path: str | Path | None = None,
    expected_signature_key_id: str = "",
    require_signature: bool = False,
) -> Dict[str, Any]:
    """Load and validate a manifest from a local path or HTTP(S) URL."""
    log_event(LOGGER, "manifest_load_start", manifest_uri=manifest_uri)
    try:
        manifest = json.loads(
            _read_uri(manifest_uri, timeout_seconds=timeout_seconds).decode("utf-8")
        )
    except json.JSONDecodeError as exc:
        raise ManifestError(f"Invalid manifest JSON: {manifest_uri}") from exc
    validate_manifest(manifest)
    if require_signature or signature_public_key_path:
        verify_manifest_signature(
            manifest,
            signature_public_key_path,
            expected_key_id=expected_signature_key_id,
            require_signature=require_signature,
        )
    log_event(
        LOGGER,
        "manifest_load_complete",
        manifest_uri=manifest_uri,
        model_id=manifest.get("model_id", "unknown"),
    )
    return manifest


def fetch_model_package(
    manifest_uri: str,
    staging_dir: str | Path,
    *,
    timeout_seconds: float = 30.0,
    signature_public_key_path: str | Path | None = None,
    expected_signature_key_id: str = "",
    require_signature: bool = False,
) -> Dict[str, str]:
    """Download a manifest and artifact into a local staging directory."""
    log_event(
        LOGGER,
        "model_fetch_start",
        manifest_uri=manifest_uri,
        staging_dir=staging_dir,
    )
    manifest = load_manifest_uri(
        manifest_uri,
        timeout_seconds=timeout_seconds,
        signature_public_key_path=signature_public_key_path,
        expected_signature_key_id=expected_signature_key_id,
        require_signature=require_signature,
    )
    package_dir = Path(staging_dir) / manifest["model_id"]
    package_dir.mkdir(parents=True, exist_ok=True)

    artifact_uri = manifest.get("artifact_uri") or _resolve_related_uri(
        manifest_uri, manifest["artifact_name"]
    )
    artifact_path = package_dir / manifest["artifact_name"]
    log_event(
        LOGGER,
        "model_artifact_pull_start",
        model_id=manifest["model_id"],
        artifact_uri=artifact_uri,
        artifact_path=artifact_path,
    )
    _copy_or_download(artifact_uri, artifact_path, timeout_seconds=timeout_seconds)
    _verify_artifact(
        artifact_path,
        manifest["artifact_sha256"],
        int(manifest["artifact_size_bytes"]),
    )

    manifest_path = package_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    log_event(
        LOGGER,
        "model_fetch_complete",
        model_id=manifest["model_id"],
        package_dir=package_dir,
    )
    return {
        "model_id": str(manifest["model_id"]),
        "package_dir": str(package_dir),
        "manifest_path": str(manifest_path),
        "artifact_path": str(artifact_path),
    }


def deploy_from_manifest(
    *,
    manifest_uri: str,
    staging_dir: str | Path,
    artifacts_dir: str | Path,
    registry_path: str | Path,
    reason: str = "",
    operator: str = "",
    timeout_seconds: float = 30.0,
    signature_public_key_path: str | Path | None = None,
    expected_signature_key_id: str = "",
    require_signature: bool = False,
    candidate_validator: Callable[[Dict[str, Any], Path], None] | None = None,
) -> Dict[str, Any]:
    """Fetch, verify, register, smoke-check, and activate a hosted model."""
    log_event(
        LOGGER,
        "model_deploy_prepare",
        manifest_uri=manifest_uri,
        artifacts_dir=artifacts_dir,
        registry_path=registry_path,
    )
    package = fetch_model_package(
        manifest_uri,
        staging_dir,
        timeout_seconds=timeout_seconds,
        signature_public_key_path=signature_public_key_path,
        expected_signature_key_id=expected_signature_key_id,
        require_signature=require_signature,
    )
    manifest = load_manifest_uri(
        package["manifest_path"],
        timeout_seconds=timeout_seconds,
        signature_public_key_path=signature_public_key_path,
        expected_signature_key_id=expected_signature_key_id,
        require_signature=require_signature,
    )

    artifact_source = Path(package["artifact_path"])
    model_dir = Path(artifacts_dir) / manifest["model_id"]
    model_dir.mkdir(parents=True, exist_ok=True)
    artifact_target = model_dir / manifest["artifact_name"]

    if artifact_source.resolve() != artifact_target.resolve():
        log_event(
            LOGGER,
            "model_artifact_install",
            model_id=manifest["model_id"],
            source=artifact_source,
            target=artifact_target,
        )
        shutil.copy2(artifact_source, artifact_target)
    _verify_artifact(
        artifact_target,
        manifest["artifact_sha256"],
        int(manifest["artifact_size_bytes"]),
    )

    registry = ModelRegistry(registry_path)
    _ensure_registered(registry, manifest, artifact_target, manifest_uri)

    try:
        active = registry.get_active_model()
    except RegistryError:
        active = None

    if active and active.get("model_id") == manifest["model_id"]:
        registry.validate_model(active)
        log_event(
            LOGGER,
            "model_deploy_already_active",
            model_id=manifest["model_id"],
        )
        return {
            "action": "already_active",
            "model_id": manifest["model_id"],
            "artifact_path": str(artifact_target),
        }

    if candidate_validator:
        try:
            log_event(
                LOGGER,
                "model_candidate_validation_start",
                model_id=manifest["model_id"],
            )
            candidate_validator(manifest, artifact_target)
            log_event(
                LOGGER,
                "model_candidate_validation_complete",
                model_id=manifest["model_id"],
            )
        except Exception as exc:
            raise UpdateError(
                f"Candidate model validation failed for {manifest['model_id']}: {exc}"
            ) from exc

    deployment = registry.deploy_model(
        manifest["model_id"],
        reason=reason or f"Deploy from manifest {manifest_uri}",
        operator=operator,
        source="updater",
    )
    log_event(
        LOGGER,
        "model_deploy_complete",
        model_id=manifest["model_id"],
        deployment_id=deployment.get("deployment_id", ""),
    )
    return {"action": "deployed", "deployment": deployment}


def _add_signature_args(parser: argparse.ArgumentParser) -> None:
    """Add manifest signature verification arguments to a parser."""
    parser.add_argument(
        "--signature-public-key",
        help="Pinned Ed25519 public key used to verify signed manifests",
    )
    parser.add_argument(
        "--signature-key-id",
        default="",
        help="Expected manifest signature key id",
    )
    parser.add_argument(
        "--require-signature",
        action="store_true",
        help="Reject unsigned manifests",
    )


def _signature_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    if args.require_signature and not args.signature_public_key:
        raise UpdateError("--signature-public-key is required with --require-signature")
    return {
        "signature_public_key_path": args.signature_public_key,
        "expected_signature_key_id": args.signature_key_id,
        "require_signature": args.require_signature,
    }


def _build_candidate_load_validator(
    config_path: str,
) -> Callable[[Dict[str, Any], Path], None]:
    """Build a validator that smoke-loads through the edge segmenter path."""

    def validate_candidate(manifest: Dict[str, Any], artifact_path: Path) -> None:
        from config_loader import load_config
        from segmentation import PotholeSegmenter

        config = load_config(config_path)
        log_event(
            LOGGER,
            "model_smoke_load_start",
            model_id=manifest["model_id"],
            artifact_path=artifact_path,
        )
        segmenter = PotholeSegmenter.create(
            model_type=manifest["model_type"],
            model_path=str(artifact_path),
            trapezoid_coords=config.get_trapezoid_coords(),
            confidence_threshold=float(manifest["confidence_threshold"]),
            frame_interval=config.get_frame_interval(),
        )
        try:
            log_event(
                LOGGER,
                "model_smoke_load_complete",
                model_id=manifest["model_id"],
            )
        finally:
            segmenter.cleanup()

    return validate_candidate


def _ensure_registered(
    registry: ModelRegistry,
    manifest: Dict[str, Any],
    artifact_path: Path,
    manifest_uri: str,
) -> None:
    """Ensure the model described by the manifest is registered in the registry."""
    try:
        registered = registry.get_model(manifest["model_id"])
    except RegistryError:
        log_event(
            LOGGER,
            "model_register",
            model_id=manifest["model_id"],
            artifact_path=artifact_path,
        )
        registry.register_model(
            model_id=manifest["model_id"],
            model_type=manifest["model_type"],
            artifact_path=artifact_path,
            confidence_threshold=float(manifest["confidence_threshold"]),
            input_size=manifest.get("input_size"),
            training_dataset=manifest.get("training_dataset", ""),
            benchmark_summary_path=manifest.get("benchmark_summary_path", ""),
            description=manifest.get("description", ""),
            tags=manifest.get("tags", []),
            metadata={
                "manifest": manifest,
                "source_manifest_uri": manifest_uri,
            },
        )
        return

    if registered.get("artifact_sha256") != manifest["artifact_sha256"]:
        raise UpdateError(
            "Registered model_id has different artifact hash: "
            f"{manifest['model_id']}"
        )
    if registered.get("model_type") != manifest["model_type"]:
        raise UpdateError(
            f"Registered model_id has different model_type: {manifest['model_id']}"
        )


def _read_uri(uri: str, *, timeout_seconds: float) -> bytes:
    """Read bytes from a local path or HTTP(S) URL."""
    parsed = urlparse(uri)
    if parsed.scheme in {"http", "https", "file"}:
        with urlopen(uri, timeout=timeout_seconds) as response:
            return response.read()
    return Path(uri).read_bytes()


def _copy_or_download(uri: str, destination: Path, *, timeout_seconds: float) -> None:
    """Copy a local file or download an HTTP(S) URL to a destination path."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_path = _local_path_from_uri(uri)

    # The source is a local file
    if source_path is not None:
        if source_path.resolve() == destination.resolve():
            return
        log_event(LOGGER, "artifact_copy", source=source_path, destination=destination)
        shutil.copy2(source_path, destination)
        return

    # The source is remote
    # Download to a temporary file first and then move to the final destination
    temp_path = destination.with_suffix(destination.suffix + ".tmp")
    log_event(LOGGER, "artifact_download", uri=uri, destination=destination)
    try:
        with urlopen(uri, timeout=timeout_seconds) as response, temp_path.open(
            "wb"
        ) as f:
            shutil.copyfileobj(response, f)
        temp_path.replace(destination)
    except BaseException:
        if temp_path.exists():
            temp_path.unlink()
        raise


def _resolve_related_uri(base_uri: str, filename: str) -> str:
    """Resolve an artifact URI beside a manifest URI."""
    parsed = urlparse(base_uri)
    if parsed.scheme in {"http", "https", "file"}:
        return urljoin(base_uri, filename)
    return str(Path(base_uri).parent / filename)


def _local_path_from_uri(uri: str) -> Path | None:
    """Return a local path for local/file URIs; return None for remote URIs."""
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(url2pathname(parsed.path))
    if parsed.scheme in {"http", "https"}:
        return None
    return Path(uri)


def _verify_artifact(path: Path, expected_sha256: str, expected_size: int) -> None:
    """Verify artifact integrity after download or copy."""
    log_event(LOGGER, "artifact_verify_start", path=path)
    actual_sha256 = sha256_file(path)
    actual_size = file_size(path)
    if actual_sha256 != expected_sha256:
        raise UpdateError(
            f"SHA256 mismatch for {path}: expected {expected_sha256}, got {actual_sha256}"
        )
    if actual_size != expected_size:
        raise UpdateError(
            f"Size mismatch for {path}: expected {expected_size}, got {actual_size}"
        )
    log_event(LOGGER, "artifact_verify_complete", path=path, size=actual_size)


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return parsed


if __name__ == "__main__":
    main()
