# Edge Model Registry

The model registry is the first MLOps layer for edge segmentation deployments.
It stores deployable model metadata, not the model weights themselves.

## Registry Contents

Each entry records:

- `model_id`: Stable deployment identifier.
- `model_type`: Runtime implementation, currently `yolo` or `rfdetr`.
- `artifact_path`: Local path to the model weights.
- `artifact_sha256`: Hash used to detect accidental artifact drift.
- `confidence_threshold`: Threshold used by the edge pipeline.
- Optional benchmark, dataset, input-size, tags, and description metadata.

The registry also stores deployment history. Each deployment event snapshots the
artifact hash, model id, previous active model, reason, operator, and rollback
link so the edge can recover the last known working model without cloud access.

## Package Layout

Generated model packages should use this shape:

```text
mlops/dist/yolo11s-2026-05-01/
  manifest.json
  yolo11s-2026-05-01.pt
  summary.json
```

The manifest template lives at `mlops/templates/model_manifest.template.json`.
The validation contract lives at `mlops/schemas/model_manifest.schema.json`.
Generated package directories under `mlops/dist/` should not be committed.

## Signed Manifests

Model manifests can be signed with Ed25519. The release pipeline keeps the
private key in CI secrets, while each edge device is provisioned with the pinned
public key from `config.yaml`.

Generate a key pair for development:

```bash
python -m mlops.manifest_keys generate \
  --private-key .secrets/model_manifest_private_key.pem \
  --public-key .conf/model_manifest_public_key.pub \
  --key-id edge-model-release-v1
```

Do not commit the private key. The public key can be copied to edge devices and
referenced by `mlops.model_update.manifest_signature.public_key_path`.

## Commands

List registered models:

```bash
python -m mlops.model_registry list
```

Show the active model:

```bash
python -m mlops.model_registry active
```

Validate the active model artifact:

```bash
python -m mlops.model_registry validate
```

Register a new model and activate it:

```bash
python -m mlops.model_registry register \
  --model-id yolo11s-2026-05-01 \
  --model-type yolo \
  --artifact-path models/yolo11s.pt \
  --confidence-threshold 0.1 \
  --input-size 640 \
  --tags yolo,segmentation,candidate \
  --activate
```

Generate a model package after training or downloading a model:

```bash
python -m mlops.generate_manifest \
  --model-id yolo11s-2026-05-01 \
  --model-type yolo \
  --artifact-path models/staging/yolo11s-2026-05-01.pt \
  --confidence-threshold 0.1 \
  --input-size 640 \
  --training-dataset pothole-v3 \
  --signing-key .secrets/model_manifest_private_key.pem \
  --signature-key-id edge-model-release-v1 \
  --package-dir mlops/dist/yolo11s-2026-05-01
```

Publish a package to a local hosted directory for testing:

```bash
python -m mlops.publish_release \
  --package-dir mlops/dist/yolo11s-2026-05-01 \
  --target local \
  --output-dir /tmp/pothole-model-host
```

Publish a package to this repository's GitHub Releases:

```bash
python -m mlops.publish_release \
  --package-dir mlops/dist/yolo11s-2026-05-01 \
  --target github-release
```

By default, `publish_release` infers `DucAnnee/pothole-monitoring-system` from
`git remote origin` and uses a tag like `model-yolo11s-2026-05-01`.

Check a hosted manifest from the edge device:

```bash
python -m mlops.model_updater check \
  --manifest-uri /tmp/pothole-model-host/yolo11s-2026-05-01/manifest.json \
  --signature-public-key .conf/model_manifest_public_key.pub \
  --signature-key-id edge-model-release-v1 \
  --require-signature
```

Fetch, register, smoke-load, and deploy a hosted model from the edge device:

```bash
python -m mlops.model_updater deploy \
  --manifest-uri /tmp/pothole-model-host/yolo11s-2026-05-01/manifest.json \
  --signature-public-key .conf/model_manifest_public_key.pub \
  --signature-key-id edge-model-release-v1 \
  --require-signature \
  --reason "Promote benchmark winner" \
  --operator edge-admin
```

Deployment is two-phase: the updater first stages and registers the candidate,
then loads it with the configured segmenter before activating it. If the smoke
load fails, the previous active model stays active.

Enable startup deployment of the latest stable manifest:

```yaml
mlops:
  model_registry:
    enabled: true
    registry_path: "mlops/model_registry.json"
  model_update:
    enabled: true
    stable_manifest_uri: "https://github.com/DucAnnee/pothole-monitoring-system/releases/download/model-yolo11s-2026-05-01/manifest.json"
    manifest_signature:
      required: true
      public_key_path: ".conf/model_manifest_public_key.pub"
      key_id: "edge-model-release-v1"
    staging_dir: "models/staging"
    artifacts_dir: "models/artifacts"
    timeout_seconds: 30
    fail_on_error: false
    operator: "edge-startup"
    reason: "Startup stable model update"
```

`main.py` checks this manifest before initializing the segmenter. The updater
verifies the manifest signature, downloads the model, verifies SHA256 and size,
registers it as a candidate, smoke-loads it, and only then deploys it if it is
not already active.

Deploy an existing model:

```bash
python -m mlops.model_registry deploy yolo11s-local \
  --reason "Promote benchmark winner" \
  --operator edge-admin
```

List deployment history:

```bash
python -m mlops.model_registry deployments --limit 5
```

Rollback the latest deployment:

```bash
python -m mlops.model_registry rollback \
  --reason "Runtime health check failed" \
  --operator edge-admin
```

Rollback a specific deployment event:

```bash
python -m mlops.model_registry rollback deploy-20260501T120000Z-a1b2c3d4
```

## Runtime Behavior

When enabled in `config.yaml`, the edge pipeline resolves the active registry
entry and validates its artifact hash before loading the segmentation model. If
the registry is unavailable and `fallback_to_config` is true, the pipeline uses
the existing `models` section in `config.yaml`.
