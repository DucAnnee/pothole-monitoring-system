param(
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")

function Wait-ContainerRunning {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $running = docker inspect -f "{{.State.Running}}" $Name 2>$null
        if ($LASTEXITCODE -eq 0 -and $running -eq "true") {
            Write-Host "Container is running: $Name"
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out after $TimeoutSeconds seconds waiting for container '$Name' to run."
}

function Run-TrinoFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $resolvedPath = Resolve-Path $Path
    Write-Host "Applying DDL through Trino: $resolvedPath"
    Get-Content -Raw -LiteralPath $resolvedPath | docker exec -i trino trino --catalog iceberg --schema default
    if ($LASTEXITCODE -ne 0) {
        throw "Trino failed while applying '$resolvedPath'."
    }
}

function Submit-FlinkSql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FileName
    )

    Write-Host "Submitting Flink SQL job: $FileName"
    docker exec -d flink-jobmanager /bin/bash -lc "/opt/flink/bin/sql-client.sh -f /opt/pothole-lakehouse/sql/$FileName > /tmp/$FileName.log 2>&1"
    if ($LASTEXITCODE -ne 0) {
        throw "Flink SQL submission failed for '$FileName'."
    }
}

$containers = @(
    "flink-jobmanager",
    "flink-taskmanager",
    "trino",
    "postgis-serving",
    "polaris",
    "minio"
)

foreach ($container in $containers) {
    Wait-ContainerRunning -Name $container
}

$ddlFiles = @(
    "lakehouse/iceberg/001_medallion_namespaces.sql",
    "lakehouse/iceberg/010_bronze_tables.sql",
    "lakehouse/iceberg/020_silver_tables.sql",
    "lakehouse/iceberg/030_gold_tables.sql",
    "lakehouse/iceberg/040_ml_tables.sql"
)

foreach ($ddlFile in $ddlFiles) {
    Run-TrinoFile -Path (Join-Path $RepoRoot $ddlFile)
}

$flinkJobs = @(
    "010_kafka_to_bronze.sql",
    "020_silver_materialization.sql",
    "030_gold_materialization.sql",
    "040_gold_to_postgis_projection.sql"
)

foreach ($flinkJob in $flinkJobs) {
    Submit-FlinkSql -FileName $flinkJob
}

Write-Host "Lakehouse streaming jobs submitted. Open the Flink dashboard at http://localhost:8084."
