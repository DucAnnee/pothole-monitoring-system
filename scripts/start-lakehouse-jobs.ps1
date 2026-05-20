param(
    [int]$TimeoutSeconds = 180,
    [switch]$AllowDuplicateJobs
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $inspectJson = docker inspect $Name 2>$null
        if ($LASTEXITCODE -eq 0) {
            $state = @($inspectJson | ConvertFrom-Json)[0].State
            if ($state.Health) {
                if ($state.Health.Status -eq "healthy") {
                    Write-Host "Container is healthy: $Name"
                    return
                }
            } elseif ($state.Running -eq $true) {
                Write-Host "Container is running: $Name"
                return
            }
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out after $TimeoutSeconds seconds waiting for container '$Name' to become healthy or running."
}

function Wait-ContainerCompleted {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $inspectJson = docker inspect $Name 2>$null
        if ($LASTEXITCODE -eq 0) {
            $state = @($inspectJson | ConvertFrom-Json)[0].State
            if ($state.Status -eq "exited") {
                if ($state.ExitCode -eq 0) {
                    Write-Host "Container completed successfully: $Name"
                    return
                }

                throw "Container '$Name' exited with code $($state.ExitCode)."
            }

            if ($state.Status -in @("dead", "removing")) {
                throw "Container '$Name' reached unexpected status '$($state.Status)'."
            }
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out after $TimeoutSeconds seconds waiting for container '$Name' to complete."
}

function Wait-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri,

        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 5 | Out-Null
            Write-Host "HTTP endpoint is ready: $Name ($Uri)"
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "Timed out after $TimeoutSeconds seconds waiting for HTTP endpoint '$Name' at $Uri."
}

function Test-RunningFlinkJobs {
    try {
        $overview = Invoke-RestMethod -Uri "http://localhost:8084/jobs/overview" -TimeoutSec 5
    } catch {
        throw "Unable to query Flink jobs overview at http://localhost:8084/jobs/overview. $_"
    }

    $activeStates = @("RUNNING", "CREATED", "RESTARTING")
    return @($overview.jobs | Where-Object { $_.state -in $activeStates })
}

function Wait-FlinkJobCountIncrease {
    param(
        [Parameter(Mandatory = $true)]
        [int]$PreviousCount,

        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $activeJobs = Test-RunningFlinkJobs
        if ($activeJobs.Count -gt $PreviousCount) {
            Write-Host "Flink accepted '$Label': active jobs $PreviousCount -> $($activeJobs.Count)"
            return $activeJobs.Count
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out after $TimeoutSeconds seconds waiting for Flink to register a new active job for '$Label'. Check /tmp/$Label.log in flink-jobmanager."
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

    # Each SQL file is self-contained: it sets checkpointing, drops/recreates
    # the lakehouse catalog, and runs USE CATALOG before its INSERTs. No shared
    # session preamble is prepended.
    $sqlPath = "/opt/pothole-lakehouse/sql/$FileName"

    Write-Host "Submitting Flink SQL job: $FileName"
    docker exec -d flink-jobmanager /bin/bash -lc "/opt/flink/bin/sql-client.sh -f $sqlPath > /tmp/$FileName.log 2>&1"
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
    Wait-ContainerHealthy -Name $container
}

Wait-ContainerCompleted -Name "polaris-setup"
Wait-HttpReady -Name "Flink UI" -Uri "http://localhost:8084/overview"
Wait-HttpReady -Name "Trino" -Uri "http://localhost:8081/v1/info"

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

if (-not $AllowDuplicateJobs) {
    $activeJobs = Test-RunningFlinkJobs
    if ($activeJobs.Count -gt 0) {
        $jobSummary = ($activeJobs | ForEach-Object { "$($_.name) [$($_.state)]" }) -join ", "
        throw "Flink already has active jobs: $jobSummary. Stop existing jobs or rerun with -AllowDuplicateJobs for intentional parallel experiments."
    }
}

$activeJobCount = (Test-RunningFlinkJobs).Count
foreach ($flinkJob in $flinkJobs) {
    Submit-FlinkSql -FileName $flinkJob
    $activeJobCount = Wait-FlinkJobCountIncrease -PreviousCount $activeJobCount -Label $flinkJob
}

Write-Host "Lakehouse streaming jobs submitted. Open the Flink dashboard at http://localhost:8084."
