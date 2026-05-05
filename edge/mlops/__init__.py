"""MLOps helpers for edge model lifecycle management.

The package exposes lightweight model release, manifest, registry, and updater
helpers. Run `python -m mlops` to list the available command modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Dict

__version__ = "0.1.0"


@dataclass(frozen=True)
class CliModule:
    """One runnable `python -m mlops.<module>` entry point."""

    module: str
    description: str


CLI_MODULES: Dict[str, CliModule] = {
    "generate_manifest": CliModule(
        module="mlops.generate_manifest",
        description="Generate and optionally sign a deployable model manifest.",
    ),
    "manifest_keys": CliModule(
        module="mlops.manifest_keys",
        description="Generate Ed25519 manifest signing key pairs.",
    ),
    "model_registry": CliModule(
        module="mlops.model_registry",
        description="Register, validate, deploy, and roll back local model versions.",
    ),
    "model_updater": CliModule(
        module="mlops.model_updater",
        description="Check, fetch, smoke-load, and deploy hosted model packages.",
    ),
    "publish_release": CliModule(
        module="mlops.publish_release",
        description="Publish generated model packages locally or to GitHub Releases.",
    ),
}

_LAZY_EXPORTS = {
    "ManifestError": "mlops.model_manifest",
    "ModelRegistry": "mlops.model_registry",
    "PublishError": "mlops.publish_release",
    "RegistryError": "mlops.model_registry",
    "SIGNATURE_ALGORITHM": "mlops.manifest_signature",
    "SIGNATURE_FIELD": "mlops.manifest_signature",
    "SignatureError": "mlops.manifest_signature",
    "UpdateError": "mlops.model_updater",
    "canonical_manifest_bytes": "mlops.manifest_signature",
    "create_manifest": "mlops.model_manifest",
    "deploy_from_manifest": "mlops.model_updater",
    "fetch_model_package": "mlops.model_updater",
    "file_size": "mlops.artifacts",
    "load_manifest": "mlops.model_manifest",
    "load_package": "mlops.publish_release",
    "public_key_fingerprint": "mlops.manifest_signature",
    "sha256_file": "mlops.artifacts",
    "sign_manifest": "mlops.manifest_signature",
    "validate_manifest": "mlops.model_manifest",
    "verify_manifest_signature": "mlops.manifest_signature",
    "write_manifest": "mlops.model_manifest",
}


def available_modules() -> Dict[str, CliModule]:
    """Return runnable MLOps command modules keyed by short module name."""
    return dict(CLI_MODULES)


def format_available_modules() -> str:
    """Return a help message for package-level CLI discovery."""
    lines = [
        "MLOps command modules:",
        "",
    ]
    for name, command in sorted(CLI_MODULES.items()):
        lines.append(f"  python -m {command.module:<28} {command.description}")

    lines.extend(
        [
            "",
            "Use `--help` on any command module for command-specific options.",
        ]
    )
    return "\n".join(lines)


def __getattr__(name: str) -> object:
    """Load public helpers on demand without importing CLI modules eagerly."""
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(_LAZY_EXPORTS[name])
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return package globals plus lazy public exports for interactive use."""
    return sorted(set(globals()) | set(_LAZY_EXPORTS))


__all__ = [
    "CLI_MODULES",
    "CliModule",
    "ManifestError",
    "ModelRegistry",
    "PublishError",
    "RegistryError",
    "SIGNATURE_ALGORITHM",
    "SIGNATURE_FIELD",
    "SignatureError",
    "UpdateError",
    "__version__",
    "available_modules",
    "canonical_manifest_bytes",
    "create_manifest",
    "deploy_from_manifest",
    "fetch_model_package",
    "file_size",
    "format_available_modules",
    "load_manifest",
    "load_package",
    "public_key_fingerprint",
    "sha256_file",
    "sign_manifest",
    "validate_manifest",
    "verify_manifest_signature",
    "write_manifest",
]
