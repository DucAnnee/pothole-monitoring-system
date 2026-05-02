from __future__ import annotations

import base64
import binascii
import hashlib
import json
from pathlib import Path
from typing import Any, Dict

SIGNATURE_FIELD = "signature"
SIGNATURE_ALGORITHM = "ed25519"


class SignatureError(Exception):
    """Raised when manifest signing or signature verification fails."""


def sign_manifest(
    manifest: Dict[str, Any],
    private_key_path: str | Path,
    *,
    key_id: str = "",
) -> Dict[str, Any]:
    """Return a manifest signed with an Ed25519 private key."""
    private_key = _load_private_key(private_key_path)
    signature = private_key.sign(canonical_manifest_bytes(manifest))
    signed_manifest = _unsigned_manifest(manifest)
    signed_manifest[SIGNATURE_FIELD] = {
        "algorithm": SIGNATURE_ALGORITHM,
        "key_id": key_id or public_key_fingerprint(private_key.public_key()),
        "value": base64.b64encode(signature).decode("ascii"),
    }
    return signed_manifest


def verify_manifest_signature(
    manifest: Dict[str, Any],
    public_key_path: str | Path | None,
    *,
    expected_key_id: str = "",
    require_signature: bool = True,
) -> None:
    """Verify a manifest signature with the pinned Ed25519 public key."""
    signature = manifest.get(SIGNATURE_FIELD)
    if signature is None:
        if require_signature:
            raise SignatureError("Manifest is unsigned")
        return

    if not public_key_path:
        raise SignatureError("A public key path is required to verify the manifest")

    if signature.get("algorithm") != SIGNATURE_ALGORITHM:
        raise SignatureError(
            f"Unsupported manifest signature algorithm: {signature.get('algorithm')}"
        )

    key_id = str(signature.get("key_id", ""))
    if expected_key_id and key_id != expected_key_id:
        raise SignatureError(
            f"Manifest signature key_id mismatch: expected {expected_key_id}, got {key_id}"
        )

    public_key = _load_public_key(public_key_path)
    try:
        signature_bytes = base64.b64decode(
            str(signature.get("value", "")),
            validate=True,
        )
    except binascii.Error as exc:
        raise SignatureError("Manifest signature value is not valid base64") from exc

    try:
        public_key.verify(signature_bytes, canonical_manifest_bytes(manifest))
    except Exception as exc:
        raise SignatureError("Manifest signature verification failed") from exc


def canonical_manifest_bytes(manifest: Dict[str, Any]) -> bytes:
    """Return stable bytes for signing, excluding the signature envelope."""
    return json.dumps(
        _unsigned_manifest(manifest),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def public_key_fingerprint(public_key: Any) -> str:
    """Return a short stable key id for an Ed25519 public key."""
    serialization = _crypto_serialization()
    key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return f"ed25519:{hashlib.sha256(key_bytes).hexdigest()[:16]}"


def _unsigned_manifest(manifest: Dict[str, Any]) -> Dict[str, Any]:
    unsigned = dict(manifest)
    unsigned.pop(SIGNATURE_FIELD, None)
    return unsigned


def _load_private_key(path: str | Path) -> Any:
    serialization = _crypto_serialization()
    try:
        key = serialization.load_pem_private_key(
            Path(path).read_bytes(),
            password=None,
        )
    except Exception as exc:
        raise SignatureError(f"Could not load manifest signing key: {path}") from exc

    ed25519 = _crypto_ed25519()
    if not isinstance(key, ed25519.Ed25519PrivateKey):
        raise SignatureError("Manifest signing key must be an Ed25519 private key")
    return key


def _load_public_key(path: str | Path) -> Any:
    serialization = _crypto_serialization()
    try:
        key = serialization.load_pem_public_key(Path(path).read_bytes())
    except Exception as exc:
        raise SignatureError(f"Could not load manifest public key: {path}") from exc

    ed25519 = _crypto_ed25519()
    if not isinstance(key, ed25519.Ed25519PublicKey):
        raise SignatureError("Manifest public key must be an Ed25519 public key")
    return key


def _crypto_serialization() -> Any:
    try:
        from cryptography.hazmat.primitives import serialization
    except ImportError as exc:
        raise SignatureError(
            "cryptography is required for signed manifests. "
            "Install dependencies from requirements.txt."
        ) from exc
    return serialization


def _crypto_ed25519() -> Any:
    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
    except ImportError as exc:
        raise SignatureError(
            "cryptography is required for signed manifests. "
            "Install dependencies from requirements.txt."
        ) from exc
    return ed25519
