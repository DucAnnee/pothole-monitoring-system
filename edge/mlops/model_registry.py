from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional
from uuid import uuid4

from .artifacts import sha256_file

ModelType = Literal["yolo", "rfdetr"]
ModelStatus = Literal["registered", "active", "retired"]
DeploymentAction = Literal["bootstrap", "deploy", "rollback"]
DeploymentStatus = Literal["active", "superseded"]


class RegistryError(Exception):
    """Raised when model registry metadata is missing, invalid, or unsafe."""


@dataclass
class ModelVersion:
    """Single deployable edge segmentation model version."""

    model_id: str
    model_type: ModelType
    artifact_path: str
    artifact_sha256: str
    artifact_size_bytes: int
    confidence_threshold: float
    name: str = ""
    status: ModelStatus = "registered"
    input_size: Optional[int] = None
    training_dataset: str = ""
    benchmark_run_id: str = ""
    benchmark_summary_path: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at_utc: str = ""
    updated_at_utc: str = ""


@dataclass
class ModelDeployment:
    """Audit record for one edge model deployment decision."""

    deployment_id: str
    action: DeploymentAction
    model_id: str
    previous_model_id: Optional[str]
    status: DeploymentStatus
    created_at_utc: str
    model_type: ModelType
    artifact_path: str
    artifact_sha256: str
    artifact_size_bytes: int
    confidence_threshold: float
    reason: str = ""
    operator: str = ""
    source: str = "cli"
    rollback_of_deployment_id: str = ""


class ModelRegistry:
    """JSON-backed registry for edge segmentation model artifacts.

    The registry stores model metadata only. Weight files remain in `models/` or
    another artifact directory and are verified by SHA256 before activation/use.
    """

    SCHEMA_VERSION = 1

    def __init__(self, registry_path: str | Path = "mlops/model_registry.json"):
        self.registry_path = Path(registry_path)

    def list_models(self) -> list[Dict[str, Any]]:
        """Return all registered model versions sorted by model id."""
        payload = self._load_payload()
        return [
            payload["models"][model_id]
            for model_id in sorted(payload.get("models", {}).keys())
        ]

    def get_model(self, model_id: str) -> Dict[str, Any]:
        """Return one model version by id."""
        payload = self._load_payload()
        try:
            return payload["models"][model_id]
        except KeyError as exc:
            raise RegistryError(f"Unknown model_id: {model_id}") from exc

    def get_active_model(self) -> Dict[str, Any]:
        """Return the active model version."""
        payload = self._load_payload()
        active_model_id = payload.get("active_model_id")
        if not active_model_id:
            raise RegistryError("No active model is configured in the registry")
        return self.get_model(active_model_id)

    def list_deployments(self, limit: Optional[int] = None) -> list[Dict[str, Any]]:
        """Return deployment history with the most recent event first."""
        payload = self._load_payload()
        deployments = list(reversed(payload.get("deployment_history", [])))
        if limit is not None:
            return deployments[:limit]
        return deployments

    def register_model(
        self,
        model_id: str,
        model_type: ModelType,
        artifact_path: str | Path,
        confidence_threshold: float,
        *,
        name: str = "",
        input_size: Optional[int] = None,
        training_dataset: str = "",
        benchmark_run_id: str = "",
        benchmark_summary_path: str = "",
        description: str = "",
        tags: Optional[Iterable[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        activate: bool = False,
        replace: bool = False,
    ) -> Dict[str, Any]:
        """Register a model artifact and optionally make it active."""
        confidence_threshold = _validate_confidence_threshold(confidence_threshold)
        payload = self._load_payload(missing_ok=True)
        models = payload.setdefault("models", {})

        if model_id in models and not replace:
            raise RegistryError(
                f"model_id already exists: {model_id}. Use --replace to update it."
            )

        if model_id in models and payload.get("active_model_id") == model_id:
            raise RegistryError(
                "Cannot replace the active model in place. Register a new model_id "
                "and deploy it so rollback history stays valid."
            )

        artifact = Path(artifact_path)
        if not artifact.exists():
            raise RegistryError(f"Model artifact does not exist: {artifact}")

        now = _utc_now()
        previous = models.get(model_id, {})
        was_active = payload.get("active_model_id") == model_id

        model = ModelVersion(
            model_id=model_id,
            model_type=model_type,
            name=name or previous.get("name", model_id),
            artifact_path=str(artifact),
            artifact_sha256=sha256_file(artifact),
            artifact_size_bytes=artifact.stat().st_size,
            confidence_threshold=confidence_threshold,
            status="active" if was_active else "registered",
            input_size=input_size,
            training_dataset=training_dataset,
            benchmark_run_id=benchmark_run_id,
            benchmark_summary_path=benchmark_summary_path,
            description=description,
            tags=list(tags or []),
            metadata=metadata or {},
            created_at_utc=previous.get("created_at_utc") or now,
            updated_at_utc=now,
        )

        models[model_id] = asdict(model)
        if activate and not was_active:
            payload = self._deploy_in_payload(
                payload,
                model_id,
                action="deploy",
                reason="Registered and activated model",
                source="register",
            )
        elif activate or was_active:
            payload = self._activate_in_payload(payload, model_id)

        self._save_payload(payload)
        return models[model_id]

    def activate_model(self, model_id: str) -> Dict[str, Any]:
        """Deploy a registered model through the legacy activate API."""
        return self.deploy_model(
            model_id,
            reason="Manual activation",
            source="activate",
        )

    def deploy_model(
        self,
        model_id: str,
        *,
        reason: str = "",
        operator: str = "",
        source: str = "cli",
    ) -> Dict[str, Any]:
        """Validate and make one registered model active, recording history."""
        payload = self._load_payload()
        payload = self._deploy_in_payload(
            payload,
            model_id,
            action="deploy",
            reason=reason,
            operator=operator,
            source=source,
        )
        self._save_payload(payload)
        return payload["deployment_history"][-1]

    def rollback_model(
        self,
        deployment_id: Optional[str] = None,
        *,
        reason: str = "",
        operator: str = "",
        source: str = "cli",
    ) -> Dict[str, Any]:
        """Roll back to the model active before a prior deployment event."""
        payload = self._load_payload()
        target = self._find_rollback_target(payload, deployment_id)
        rollback_of = target["deployment_id"]
        previous_model_id = target.get("previous_model_id")
        if not previous_model_id:
            raise RegistryError(
                f"Deployment cannot be rolled back: {target['deployment_id']}"
            )

        payload = self._deploy_in_payload(
            payload,
            previous_model_id,
            action="rollback",
            reason=reason or f"Rollback of {rollback_of}",
            operator=operator,
            source=source,
            rollback_of_deployment_id=rollback_of,
        )
        self._save_payload(payload)
        return payload["deployment_history"][-1]

    def validate_model(self, model: str | Dict[str, Any]) -> Dict[str, Any]:
        """Verify that a registered artifact exists and matches its SHA256."""
        record = self.get_model(model) if isinstance(model, str) else model
        artifact = Path(record["artifact_path"])
        if not artifact.exists():
            raise RegistryError(f"Model artifact does not exist: {artifact}")

        actual_sha = sha256_file(artifact)
        expected_sha = record.get("artifact_sha256")
        if actual_sha != expected_sha:
            raise RegistryError(
                "Model artifact hash mismatch for "
                f"{record.get('model_id')}: expected {expected_sha}, got {actual_sha}"
            )

        expected_size = int(record.get("artifact_size_bytes", -1))
        actual_size = artifact.stat().st_size
        if expected_size != actual_size:
            raise RegistryError(
                "Model artifact size mismatch for "
                f"{record.get('model_id')}: expected {expected_size}, got {actual_size}"
            )

        return record

    def _deploy_in_payload(
        self,
        payload: Dict[str, Any],
        model_id: str,
        *,
        action: DeploymentAction,
        reason: str = "",
        operator: str = "",
        source: str = "cli",
        rollback_of_deployment_id: str = "",
    ) -> Dict[str, Any]:
        active_model_id = payload.get("active_model_id")
        if active_model_id == model_id:
            raise RegistryError(f"Model is already active: {model_id}")

        payload = self._activate_in_payload(payload, model_id)
        model = payload["models"][model_id]
        now = _utc_now()

        for deployment in payload.setdefault("deployment_history", []):
            deployment["status"] = "superseded"

        deployment = ModelDeployment(
            deployment_id=_new_deployment_id(action),
            action=action,
            model_id=model_id,
            previous_model_id=active_model_id,
            status="active",
            created_at_utc=now,
            model_type=model["model_type"],
            artifact_path=model["artifact_path"],
            artifact_sha256=model["artifact_sha256"],
            artifact_size_bytes=int(model["artifact_size_bytes"]),
            confidence_threshold=float(model["confidence_threshold"]),
            reason=reason,
            operator=operator,
            source=source,
            rollback_of_deployment_id=rollback_of_deployment_id,
        )

        payload["deployment_history"].append(asdict(deployment))
        payload["active_deployment_id"] = deployment.deployment_id

        return payload

    def _find_rollback_target(
        self, payload: Dict[str, Any], deployment_id: Optional[str]
    ) -> Dict[str, Any]:
        deployments = payload.get("deployment_history", [])

        if deployment_id:
            for deployment in deployments:
                if deployment.get("deployment_id") == deployment_id:
                    return deployment
            raise RegistryError(f"Unknown deployment_id: {deployment_id}")

        active_model_id = payload.get("active_model_id")
        for deployment in reversed(deployments):
            previous_model_id = deployment.get("previous_model_id")
            if previous_model_id and previous_model_id != active_model_id:
                return deployment

        raise RegistryError("No rollback target found in deployment history")

    def _activate_in_payload(
        self, payload: Dict[str, Any], model_id: str
    ) -> Dict[str, Any]:
        models = payload.get("models", {})

        if model_id not in models:
            raise RegistryError(f"Unknown model_id: {model_id}")

        self.validate_model(models[model_id])

        now = _utc_now()
        for current_id, model in models.items():
            model["status"] = "active" if current_id == model_id else "registered"
            model["updated_at_utc"] = now

        payload["active_model_id"] = model_id
        return payload

    def _load_payload(self, missing_ok: bool = False) -> Dict[str, Any]:
        if not self.registry_path.exists():
            if missing_ok:
                return self._empty_payload()
            raise RegistryError(f"Model registry not found: {self.registry_path}")

        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RegistryError(f"Invalid registry JSON: {self.registry_path}") from exc

        if payload.get("schema_version") != self.SCHEMA_VERSION:
            raise RegistryError(
                "Unsupported model registry schema_version: "
                f"{payload.get('schema_version')}"
            )

        payload.setdefault("active_model_id", None)
        payload.setdefault("models", {})
        payload.setdefault("active_deployment_id", None)
        payload.setdefault("deployment_history", [])
        return payload

    def _save_payload(self, payload: Dict[str, Any]) -> None:
        payload["schema_version"] = self.SCHEMA_VERSION
        payload["updated_at_utc"] = _utc_now()
        payload.setdefault("models", {})
        payload.setdefault("active_deployment_id", None)
        payload.setdefault("deployment_history", [])

        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.registry_path.with_suffix(self.registry_path.suffix + ".tmp")
        temp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(self.registry_path)

    def _empty_payload(self) -> Dict[str, Any]:
        now = _utc_now()
        return {
            "schema_version": self.SCHEMA_VERSION,
            "active_model_id": None,
            "active_deployment_id": None,
            "models": {},
            "deployment_history": [],
            "created_at_utc": now,
            "updated_at_utc": now,
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_deployment_id(action: DeploymentAction) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{action}-{timestamp}-{uuid4().hex[:8]}"


def _validate_confidence_threshold(value: float) -> float:
    threshold = float(value)
    if threshold < 0 or threshold > 1:
        raise RegistryError("confidence_threshold must be between 0 and 1")
    return threshold


def _parse_tags(value: str) -> list[str]:
    if not value:
        return []
    return [tag.strip() for tag in value.split(",") if tag.strip()]


def _print_model(model: Dict[str, Any]) -> None:
    print(json.dumps(model, indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage local edge model registry")
    parser.add_argument(
        "--registry-path",
        default="mlops/model_registry.json",
        help="Path to the model registry JSON file",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    register = subparsers.add_parser("register", help="Register a model artifact")
    register.add_argument("--model-id", required=True)
    register.add_argument("--model-type", choices=["yolo", "rfdetr"], required=True)
    register.add_argument("--artifact-path", required=True)
    register.add_argument("--confidence-threshold", type=float, required=True)
    register.add_argument("--name", default="")
    register.add_argument("--input-size", type=int)
    register.add_argument("--training-dataset", default="")
    register.add_argument("--benchmark-run-id", default="")
    register.add_argument("--benchmark-summary-path", default="")
    register.add_argument("--description", default="")
    register.add_argument("--tags", default="", help="Comma-separated tags")
    register.add_argument("--activate", action="store_true")
    register.add_argument("--replace", action="store_true")

    subparsers.add_parser("list", help="List registered models")

    show = subparsers.add_parser("show", help="Show one model record")
    show.add_argument("model_id")

    active = subparsers.add_parser("active", help="Show the active model")
    _ = active

    deploy = subparsers.add_parser("deploy", help="Deploy a registered model")
    deploy.add_argument("model_id")
    deploy.add_argument("--reason", default="")
    deploy.add_argument("--operator", default="")

    rollback = subparsers.add_parser(
        "rollback", help="Roll back to the previous active model"
    )
    rollback.add_argument("deployment_id", nargs="?")
    rollback.add_argument("--reason", default="")
    rollback.add_argument("--operator", default="")

    deployments = subparsers.add_parser("deployments", help="List deployment history")
    deployments.add_argument("--limit", type=int)

    activate = subparsers.add_parser("activate", help="Deploy a model")
    activate.add_argument("model_id")

    validate = subparsers.add_parser("validate", help="Validate model artifact hash")
    validate.add_argument("model_id", nargs="?")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    registry = ModelRegistry(args.registry_path)

    try:
        if args.command == "register":
            model = registry.register_model(
                model_id=args.model_id,
                model_type=args.model_type,
                artifact_path=args.artifact_path,
                confidence_threshold=args.confidence_threshold,
                name=args.name,
                input_size=args.input_size,
                training_dataset=args.training_dataset,
                benchmark_run_id=args.benchmark_run_id,
                benchmark_summary_path=args.benchmark_summary_path,
                description=args.description,
                tags=_parse_tags(args.tags),
                activate=args.activate,
                replace=args.replace,
            )
            _print_model(model)
            return

        if args.command == "list":
            for model in registry.list_models():
                marker = "*" if model.get("status") == "active" else " "
                print(
                    "{marker} {model_id} {model_type} {confidence} {path}".format(
                        marker=marker,
                        model_id=model.get("model_id", ""),
                        model_type=model.get("model_type", ""),
                        confidence=model.get("confidence_threshold", ""),
                        path=model.get("artifact_path", ""),
                    )
                )
            return

        if args.command == "show":
            _print_model(registry.get_model(args.model_id))
            return

        if args.command == "active":
            _print_model(registry.get_active_model())
            return

        if args.command == "deploy":
            _print_model(
                registry.deploy_model(
                    args.model_id,
                    reason=args.reason,
                    operator=args.operator,
                )
            )
            return

        if args.command == "rollback":
            _print_model(
                registry.rollback_model(
                    args.deployment_id,
                    reason=args.reason,
                    operator=args.operator,
                )
            )
            return

        if args.command == "deployments":
            for deployment in registry.list_deployments(args.limit):
                print(
                    "{status} {deployment_id} {action} {model_id} prev={previous}".format(
                        status=deployment.get("status", ""),
                        deployment_id=deployment.get("deployment_id", ""),
                        action=deployment.get("action", ""),
                        model_id=deployment.get("model_id", ""),
                        previous=deployment.get("previous_model_id", ""),
                    )
                )
            return

        if args.command == "activate":
            _print_model(registry.activate_model(args.model_id))
            return

        if args.command == "validate":
            model = (
                registry.get_model(args.model_id)
                if args.model_id
                else registry.get_active_model()
            )
            registry.validate_model(model)
            print(f"Validated {model['model_id']}: {model['artifact_path']}")
            return
    except RegistryError as exc:
        raise SystemExit(f"Registry error: {exc}") from exc


if __name__ == "__main__":
    main()
