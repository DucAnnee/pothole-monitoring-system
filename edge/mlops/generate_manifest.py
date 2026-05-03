from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict

from .manifest_signature import SignatureError, sign_manifest
from .model_manifest import (
    ManifestError,
    SUPPORTED_MODEL_TYPES,
    create_manifest,
    write_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an edge model manifest")
    parser.add_argument("--model-id", required=True)
    parser.add_argument(
        "--model-type",
        choices=sorted(SUPPORTED_MODEL_TYPES),
        required=True,
    )
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--confidence-threshold", type=float, required=True)
    parser.add_argument("--output")
    parser.add_argument("--package-dir")
    parser.add_argument("--input-size", type=int)
    parser.add_argument("--training-dataset", default="")
    parser.add_argument("--benchmark-summary")
    parser.add_argument("--min-edge-version", default="0.1.0")
    parser.add_argument("--artifact-uri", default="")
    parser.add_argument("--description", default="")
    parser.add_argument("--tags", default="", help="Comma-separated tags")
    parser.add_argument("--metadata-json", help="Path to extra manifest metadata JSON")
    parser.add_argument(
        "--signing-key",
        help="Ed25519 private key used to sign the generated manifest",
    )
    parser.add_argument(
        "--signature-key-id",
        default="",
        help="Stable key id recorded in the signature envelope",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        metadata = _load_metadata(args.metadata_json)
        artifact_path = Path(args.artifact_path)
        if not artifact_path.exists():
            raise ManifestError(f"Model artifact does not exist: {artifact_path}")
        output_path = _resolve_output_path(args)

        if args.package_dir:
            package_dir = Path(args.package_dir)
            package_dir.mkdir(parents=True, exist_ok=True)
            packaged_artifact = package_dir / artifact_path.name
            if artifact_path.resolve() != packaged_artifact.resolve():
                shutil.copy2(artifact_path, packaged_artifact)
            artifact_path = packaged_artifact

            if args.benchmark_summary:
                benchmark = Path(args.benchmark_summary)
                if not benchmark.exists():
                    raise ManifestError(
                        f"Benchmark summary does not exist: {benchmark}"
                    )
                packaged_summary = package_dir / benchmark.name
                if benchmark.resolve() != packaged_summary.resolve():
                    shutil.copy2(benchmark, packaged_summary)
                args.benchmark_summary = str(packaged_summary)

        manifest = create_manifest(
            model_id=args.model_id,
            model_type=args.model_type,
            artifact_path=artifact_path,
            confidence_threshold=args.confidence_threshold,
            input_size=args.input_size,
            training_dataset=args.training_dataset,
            benchmark_summary_path=args.benchmark_summary,
            min_edge_version=args.min_edge_version,
            artifact_uri=args.artifact_uri,
            description=args.description,
            tags=_parse_tags(args.tags),
            metadata=metadata,
        )

        if args.signing_key:
            manifest = sign_manifest(
                manifest,
                args.signing_key,
                key_id=args.signature_key_id,
            )

        write_manifest(manifest, output_path)
        print(f"Generated manifest for {manifest['model_id']}: {output_path}")
        if args.signing_key:
            print(f"Signed manifest with key_id={manifest['signature']['key_id']}")

    except (ManifestError, OSError, SignatureError) as exc:
        raise SystemExit(f"Manifest error: {exc}") from exc


def _resolve_output_path(args: argparse.Namespace) -> Path:
    if args.output:
        return Path(args.output)
    if args.package_dir:
        return Path(args.package_dir) / "manifest.json"
    raise SystemExit("Either --output or --package-dir is required")


def _load_metadata(path: str | None) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        metadata = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestError(f"Invalid metadata JSON: {path}") from exc
    if not isinstance(metadata, dict):
        raise ManifestError(f"Metadata JSON must contain an object: {path}")
    return metadata


def _parse_tags(value: str) -> list[str]:
    if not value:
        return []
    return [tag.strip() for tag in value.split(",") if tag.strip()]


if __name__ == "__main__":
    main()
