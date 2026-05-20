#!/usr/bin/env python3
"""
Refresh the lakehouse for a clean test run.

Clears:
  - PostGIS serving.current_road_defects (+ inbox, cascaded)
  - Iceberg iceberg.city.* tables via Trino
  - Redis cache (DB 0 and DB 1)

Does NOT reset Kafka consumer offsets (services stay running and are caught up).
Does NOT delete MinIO images.

Run from project root:
    python scripts/refresh_lakehouse.py
"""
import subprocess
import sys


def run(desc: str, cmd: str, check: bool = True) -> bool:
    print(f"  {desc}...", end=" ", flush=True)
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        print("FAIL")
        print(f"    {r.stderr.strip()}", file=sys.stderr)
        if check:
            return False
    else:
        print("ok")
    return True


def main() -> None:
    print("=== PostGIS ===")
    run(
        "Truncate road defects (cascade)",
        'docker exec postgis-serving psql -U serving -d postgis_serving -c '
        '"TRUNCATE serving.current_road_defects CASCADE"',
    )
    run(
        "Truncate projection inbox",
        'docker exec postgis-serving psql -U serving -d postgis_serving -c '
        '"TRUNCATE serving.current_road_defects_projection_inbox"',
    )

    print("\n=== Iceberg (city schema) ===")
    for table in [
        "iceberg.city.raw_events",
        "iceberg.city.surface_area_events",
        "iceberg.city.severity_scores",
        "iceberg.city.pothole_history",
        "iceberg.city.potholes",
    ]:
        desc = f"Delete {table}"
        print(f"  {desc}...", end=" ", flush=True)
        r = subprocess.run(
            ["docker", "exec", "trino", "trino", "--execute", f"DELETE FROM {table}"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            print("FAIL (non-fatal)")
        else:
            print("ok")

    print("\n=== Schema Registry ===")
    import urllib.request, json
    try:
        with urllib.request.urlopen("http://localhost:8082/subjects") as resp:
            subjects = json.loads(resp.read())
        for subject in subjects:
            for suffix in ["", "?permanent=true"]:
                req = urllib.request.Request(
                    f"http://localhost:8082/subjects/{subject}{suffix}",
                    method="DELETE",
                )
                with urllib.request.urlopen(req):
                    pass
            print(f"  Deleted subject {subject}... ok")
    except Exception as e:
        print(f"  Schema Registry cleanup failed (non-fatal): {e}")

    print("\n=== Redis ===")
    run("Flush DB 0 (API cache)", "docker exec redis redis-cli FLUSHDB")
    run("Flush DB 1 (session cache)", "docker exec redis redis-cli -n 1 FLUSHDB")

    print("\n=== Done ===")
    print("PostGIS and Iceberg cleared. Redis flushed.")
    print("Kafka consumer offsets unchanged — services will process new messages from current offset.")
    print("\nNext steps:")
    print("  1. Run edge:    cd edge && python main.py --video assets/test.mp4")
    print("  2. Seed data:   python scripts/seed_images.py && docker exec -i postgis-serving psql -U serving -d postgis_serving < scripts/seed_demo_data.sql")


if __name__ == "__main__":
    main()
