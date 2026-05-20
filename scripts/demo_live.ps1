param(
    [string]$Video = "assets/test.mp4",
    [int]$VerifyTimeoutSeconds = 200
)
# Default (Continue): benign native stderr from docker/flink CLIs must not abort
# the script. Terminating errors (Invoke-RestMethod failure, throws from called
# scripts) still propagate.
$ErrorActionPreference = "Continue"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")

$CoreTopics = @(
    "pothole.raw.events.v2", "pothole.surface.area.v2", "pothole.depth.v1",
    "pothole.severity.score.v1", "pothole.geo.enriched.v1"
)
$IcebergTables = @(
    "iceberg.bronze.raw_detection_events", "iceberg.bronze.surface_area_events",
    "iceberg.bronze.depth_estimation_events", "iceberg.bronze.severity_score_events",
    "iceberg.bronze.geo_enrichment_events", "iceberg.silver.detections",
    "iceberg.silver.observations", "iceberg.silver.defect_evidence",
    "iceberg.gold.current_road_defects", "iceberg.gold.defect_observation_history",
    "iceberg.city.raw_events", "iceberg.city.surface_area_events",
    "iceberg.city.severity_scores", "iceberg.city.potholes", "iceberg.city.pothole_history"
)

function Cancel-FlinkJobs {
    $jobs = (Invoke-RestMethod "http://localhost:8084/jobs").jobs | Where-Object { $_.status -eq "RUNNING" }
    foreach ($j in $jobs) { docker exec flink-jobmanager flink cancel $j.id 2>$null | Out-Null }
    Write-Host "Cancelled $($jobs.Count) Flink jobs"
}

function Reset-Topics {
    foreach ($t in $CoreTopics) {
        docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --delete --topic $t 2>$null | Out-Null
        docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --create --topic $t --partitions 12 --replication-factor 3 2>$null | Out-Null
    }
    Write-Host "Recreated $($CoreTopics.Count) topics"
}

function Truncate-Stores {
    foreach ($t in $IcebergTables) {
        docker exec trino trino --execute "DELETE FROM $t" 2>$null | Out-Null
    }
    docker exec postgis-serving psql -U serving -d postgis_serving -c "TRUNCATE serving.current_road_defects CASCADE; TRUNCATE serving.current_road_defects_projection_inbox;" 2>$null | Out-Null
    docker exec redis redis-cli FLUSHALL 2>$null | Out-Null
    Write-Host "Truncated Iceberg + PostGIS + Redis"
}

function Get-ServingCount {
    $out = (docker exec postgis-serving psql -U serving -d postgis_serving -t -c "SELECT count(*) FROM serving.current_road_defects;" 2>$null | Out-String)
    if ($out -match '\d+') { return [int]$Matches[0] } else { return 0 }
}

Write-Host "=== DEMO RESET ==="
Cancel-FlinkJobs
Reset-Topics
Truncate-Stores

Write-Host "=== RESTART SERVICES ==="
docker compose restart bev-surface-service depth-estimation-service severity-calculation-service final-enrichment-service etl-service geo-enrichment-service pipeline-observer | Out-Null

Write-Host "=== REBOOTSTRAP FLINK ==="
& (Join-Path $PSScriptRoot "start-lakehouse-jobs.ps1")

Write-Host "=== RUN EDGE ==="
Push-Location (Join-Path $RepoRoot "edge")
& "./.venv/Scripts/python.exe" main.py --video $Video
Pop-Location

Write-Host "=== VERIFY ==="
$deadline = (Get-Date).AddSeconds($VerifyTimeoutSeconds)
while ((Get-Date) -lt $deadline -and (Get-ServingCount) -lt 1) { Start-Sleep -Seconds 5 }
$count = Get-ServingCount
if ($count -ge 1) {
    Write-Host "PASS: $count defect(s) in serving.current_road_defects" -ForegroundColor Green
    docker exec postgis-serving psql -U serving -d postgis_serving -c "SELECT defect_id, severity_level, district, ward, road_segment_id, ST_AsText(geometry) FROM serving.current_road_defects ORDER BY last_seen_at DESC;"
    exit 0
}
else {
    Write-Host "FAIL: no defects projected within ${VerifyTimeoutSeconds}s" -ForegroundColor Red
    exit 1
}
