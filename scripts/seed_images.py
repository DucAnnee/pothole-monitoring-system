#!/usr/bin/env python3
"""
Upload named seed images to MinIO by copying existing pipeline images.
Cycles through existing UUID raw_images to populate 53 named seed paths.

Run from project root:
    python scripts/seed_images.py

Requires MinIO container running (warehouse bucket, raw_images/ prefix).
"""
import subprocess
import sys


SEED_STEMS = [
    "d1_001", "d1_002", "d1_003", "d1_004", "d1_005", "d1_006", "d1_007",
    "d3_001", "d3_002", "d3_003", "d3_004", "d3_005", "d3_006",
    "d5_001", "d5_002", "d5_003", "d5_004", "d5_005",
    "d7_001", "d7_002", "d7_003", "d7_004", "d7_005",
    "bt_001", "bt_002", "bt_003", "bt_004", "bt_005", "bt_006", "bt_007",
    "gv_001", "gv_002", "gv_003", "gv_004", "gv_005", "gv_006",
    "pn_001", "pn_002", "pn_003", "pn_004", "pn_005",
    "tb_001", "tb_002", "tb_003", "tb_004", "tb_005", "tb_006",
    "td_001", "td_002", "td_003", "td_004", "td_005", "td_006",
]


def shell(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def setup_mc_alias() -> bool:
    r = shell("docker exec minio bash -c 'mc alias set local http://localhost:9000 minioadmin minioadmin --quiet'")
    if r.returncode != 0:
        print(f"mc alias setup failed: {r.stderr.strip()}", file=sys.stderr)
        return False
    return True


def list_source_images() -> list[str]:
    r = shell("docker exec minio bash -c 'mc ls --quiet local/warehouse/raw_images/'")
    if r.returncode != 0:
        return []
    images = []
    for line in r.stdout.strip().splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        name = parts[-1]
        # UUID-named images (pipeline output) contain hyphens; seed images do not
        if name.endswith(".jpg") and "-" in name:
            images.append(name)
    return images


def main() -> None:
    print("Setting up mc alias...", end=" ", flush=True)
    if not setup_mc_alias():
        sys.exit(1)
    print("ok")

    print("Listing existing pipeline images...", end=" ", flush=True)
    sources = list_source_images()
    if not sources:
        print("\nNo UUID-named images found in raw_images/. Run the edge pipeline first.", file=sys.stderr)
        sys.exit(1)
    print(f"{len(sources)} found")

    total = len(SEED_STEMS)
    ok = 0
    skipped = 0
    for i, stem in enumerate(SEED_STEMS):
        dst = f"{stem}.jpg"
        dst_key = f"local/warehouse/raw_images/{dst}"
        src_name = sources[i % len(sources)]
        src_key = f"local/warehouse/raw_images/{src_name}"

        # Check if seed image already exists
        check = shell(f"docker exec minio bash -c 'mc stat {dst_key} --quiet'")
        if check.returncode == 0:
            print(f"  [{i+1}/{total}] {dst} already exists, skipped")
            skipped += 1
            ok += 1
            continue

        r = shell(f"docker exec minio bash -c 'mc cp {src_key} {dst_key} --quiet'")
        if r.returncode == 0:
            ok += 1
            print(f"  [{i+1}/{total}] {dst} <- {src_name[:8]}...")
        else:
            print(f"  [{i+1}/{total}] FAILED {dst}: {r.stderr.strip()}", file=sys.stderr)

    print(f"\nDone: {ok}/{total} seed images uploaded ({skipped} already existed).")


if __name__ == "__main__":
    main()
