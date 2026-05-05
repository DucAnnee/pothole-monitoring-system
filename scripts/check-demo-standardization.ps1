$ErrorActionPreference = "Stop"

$topics = @(
  "pothole.raw.events.v2",
  "pothole.surface.area.v2",
  "pothole.depth.v1",
  "pothole.severity.score.v1"
)

foreach ($topic in $topics) {
  docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --describe --topic $topic | Out-Null
  Write-Host "[ok] topic $topic"
}

$jobs = Invoke-RestMethod -Uri "http://localhost:8084/jobs"
Write-Host "[ok] flink jobs endpoint returned $($jobs.jobs.Count) job(s)"

docker exec postgis-serving psql -U serving -d postgis_serving -c "SELECT count(*) FROM serving.current_road_defects;" | Out-Host
Write-Host "[ok] checked serving.current_road_defects"

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin | Out-Null
docker exec minio mc ls local/warehouse/raw_images/ | Out-Null
Write-Host "[ok] checked raw_images/"

docker exec minio mc ls local/warehouse/bev_images/ | Out-Null
Write-Host "[ok] checked bev_images/"

docker exec redis redis-cli ping | Out-Host
Write-Host "[ok] checked redis"
