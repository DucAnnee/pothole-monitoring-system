"""Publish a generated edge model package locally or to GitHub Releases."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List

from .artifacts import file_size, sha256_file
from .model_manifest import ManifestError, load_manifest


class PublishError(Exception):
    """Raised when a model package cannot be published."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish an edge model package")
    parser.add_argument("--package-dir", required=True)
    parser.add_argument("--target", choices=["local", "github-release"], required=True)
    parser.add_argument("--output-dir", help="Destination root for --target local")
    parser.add_argument(
        "--repo",
        help="GitHub repository in owner/name form. Defaults to git remote origin.",
    )
    parser.add_argument(
        "--tag", help="GitHub Release tag. Defaults to model-<model_id>."
    )
    parser.add_argument(
        "--title", help="GitHub Release title. Defaults to Model <model_id>."
    )
    parser.add_argument("--notes", default="")
    parser.add_argument("--draft", action="store_true")
    parser.add_argument("--prerelease", action="store_true")
    parser.add_argument("--clobber", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        package = load_package(args.package_dir)

        if args.target == "local":
            result = publish_local(package, args.output_dir)
        else:
            result = publish_github_release(package, args)
        print(json.dumps(result, indent=2, sort_keys=True))
    except (ManifestError, PublishError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f"Publish error: {exc}") from exc


def load_package(package_dir: str | Path) -> Dict[str, object]:
    """Load and verify a generated model package directory."""
    root = Path(package_dir)
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise PublishError(f"Package is missing manifest.json: {root}")

    manifest = load_manifest(manifest_path)
    artifact_path = root / manifest["artifact_name"]
    if not artifact_path.exists():
        raise PublishError(f"Package is missing artifact: {artifact_path}")
    _verify_file(
        artifact_path,
        manifest["artifact_sha256"],
        int(manifest["artifact_size_bytes"]),
    )

    files: List[Path] = [manifest_path, artifact_path]
    benchmark_summary = manifest.get("benchmark_summary_path")
    if benchmark_summary:
        summary_path = root / str(benchmark_summary)
        if not summary_path.exists():
            raise PublishError(f"Package is missing benchmark summary: {summary_path}")
        if manifest.get("benchmark_summary_sha256"):
            _verify_file(
                summary_path,
                str(manifest["benchmark_summary_sha256"]),
                int(manifest.get("benchmark_summary_size_bytes", 0)),
            )
        files.append(summary_path)

    return {"root": root, "manifest": manifest, "files": files}


def publish_local(package: Dict[str, object], output_dir: str | None) -> Dict[str, str]:
    """Copy a package into a local hosted-directory layout for testing."""
    if not output_dir:
        raise PublishError("--output-dir is required for --target local")

    manifest = package["manifest"]
    assert isinstance(manifest, dict)
    destination = Path(output_dir) / str(manifest["model_id"])
    destination.mkdir(parents=True, exist_ok=True)

    for file_path in package["files"]:
        assert isinstance(file_path, Path)
        shutil.copy2(file_path, destination / file_path.name)

    return {
        "target": "local",
        "model_id": str(manifest["model_id"]),
        "manifest_uri": str(destination / "manifest.json"),
    }


def publish_github_release(
    package: Dict[str, object], args: argparse.Namespace
) -> Dict[str, str]:
    """Publish package files as GitHub Release assets through the gh CLI."""
    repo = args.repo or infer_current_github_repo()
    if not shutil.which("gh"):
        raise PublishError("GitHub CLI is not installed or not on PATH: gh")

    manifest = package["manifest"]
    assert isinstance(manifest, dict)
    tag = args.tag or f"model-{manifest['model_id']}"
    title = args.title or f"Model {manifest['model_id']}"
    notes = args.notes or f"Edge model release for {manifest['model_id']}"
    files = [str(path) for path in package["files"]]

    view = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo],
        capture_output=True,
        text=True,
        check=False,
    )

    if view.returncode == 0:
        command = ["gh", "release", "upload", tag, *files, "--repo", repo]
        if args.clobber:
            command.append("--clobber")
    else:
        command = [
            "gh",
            "release",
            "create",
            tag,
            *files,
            "--repo",
            repo,
            "--title",
            title,
            "--notes",
            notes,
        ]
        if args.draft:
            command.append("--draft")
        if args.prerelease:
            command.append("--prerelease")

    subprocess.run(command, check=True)
    return {
        "target": "github-release",
        "model_id": str(manifest["model_id"]),
        "repo": repo,
        "tag": tag,
    }


def infer_current_github_repo() -> str:
    """Return owner/name from the current git remote origin URL."""
    result = subprocess.run(
        ["git", "config", "--get", "remote.origin.url"],
        capture_output=True,
        text=True,
        check=False,
    )
    remote = result.stdout.strip()
    if result.returncode != 0 or not remote:
        raise PublishError(
            "--repo is required because git remote origin could not be inferred"
        )
    return _repo_from_remote_url(remote)


def _repo_from_remote_url(remote: str) -> str:
    if remote.startswith("git@github.com:"):
        repo = remote.removeprefix("git@github.com:")
    elif remote.startswith("https://github.com/"):
        repo = remote.removeprefix("https://github.com/")
    elif remote.startswith("ssh://git@github.com/"):
        repo = remote.removeprefix("ssh://git@github.com/")
    else:
        raise PublishError(f"Unsupported GitHub remote origin URL: {remote}")

    repo = repo.removesuffix(".git").strip("/")
    if repo.count("/") != 1:
        raise PublishError(f"Could not parse GitHub repo from remote origin: {remote}")
    return repo


def _verify_file(path: Path, expected_sha256: str, expected_size: int) -> None:
    actual_sha256 = sha256_file(path)
    actual_size = file_size(path)
    if actual_sha256 != expected_sha256:
        raise PublishError(
            f"SHA256 mismatch for {path}: expected {expected_sha256}, got {actual_sha256}"
        )
    if actual_size != expected_size:
        raise PublishError(
            f"Size mismatch for {path}: expected {expected_size}, got {actual_size}"
        )


if __name__ == "__main__":
    main()
