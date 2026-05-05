from __future__ import annotations

import argparse
import os
from pathlib import Path

from .manifest_signature import public_key_fingerprint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage manifest signing keys")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser(
        "generate", help="Generate an Ed25519 manifest signing key pair"
    )
    generate.add_argument("--private-key", required=True)
    generate.add_argument("--public-key", required=True)
    generate.add_argument(
        "--key-id",
        default="",
        help="Optional stable key id. Defaults to a public-key fingerprint.",
    )
    generate.add_argument("--force", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "generate":
        key_id = generate_keypair(
            private_key_path=args.private_key,
            public_key_path=args.public_key,
            key_id=args.key_id,
            force=args.force,
        )
        print(f"Generated Ed25519 manifest key pair: key_id={key_id}")


def generate_keypair(
    *,
    private_key_path: str | Path,
    public_key_path: str | Path,
    key_id: str = "",
    force: bool = False,
) -> str:
    """Generate an Ed25519 key pair for manifest signing."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    private_path = Path(private_key_path)
    public_path = Path(public_key_path)

    if not force:
        for path in (private_path, public_path):
            if path.exists():
                raise SystemExit(f"Refusing to overwrite existing key: {path}")

    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    private_path.parent.mkdir(parents=True, exist_ok=True)
    public_path.parent.mkdir(parents=True, exist_ok=True)
    private_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    public_path.write_bytes(
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    os.chmod(private_path, 0o600)
    return key_id or public_key_fingerprint(public_key)


if __name__ == "__main__":
    main()
