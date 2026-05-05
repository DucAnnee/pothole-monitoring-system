param(
    [switch]$KeepStack,
    [switch]$SkipBuild,
    [int]$TimeoutSeconds      = 600,
    [int]$EdgeDeviceCount     = 50,
    [int]$EventsPerDevice     = 20,
    [int]$Concurrency         = 10,
    [int]$StressDurationSeconds = 300,
    [int]$P95LatencyThresholdMs = 120000,
    [double]$MinThroughput    = 5.0,
    [int]$FramePoolSize       = 20
)

$ErrorActionPreference = "Stop"

$RepoRoot  = Split-Path -Parent $PSScriptRoot
$ComposeArgs = @(
    "compose",
    "-p", "pms-stress",
    "-f", "docker-compose.yml",
    "-f", "docker-compose.test.yml"
)

function Invoke-DockerCompose {
    param([string[]]$Cmd)
    & docker @($ComposeArgs + $Cmd)
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Cmd -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Wait-HttpReady {
    param(
        [string]$Name,
        [string]$Url,
        [int]$TimeoutSeconds
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
            Write-Host "[ready] $Name"
            return
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    throw "Timed out waiting for $Name at $Url"
}

Push-Location $RepoRoot
try {
    # ---- start infrastructure -----------------------------------------------
    $upArgs = @("up", "-d")
    if (-not $SkipBuild) { $upArgs += "--build" }

    Write-Host "[stress] Starting Docker Compose stress stack (project: pms-stress) ..."
    Invoke-DockerCompose $upArgs

    Write-Host "[stress] Waiting for infrastructure readiness ..."
    Wait-HttpReady "Schema Registry" "http://localhost:8082/subjects"      $TimeoutSeconds
    Wait-HttpReady "MinIO"           "http://localhost:9000/minio/health/live" $TimeoutSeconds
    Wait-HttpReady "Polaris"         "http://localhost:8182/q/health"      $TimeoutSeconds
    Wait-HttpReady "Trino"           "http://localhost:8081/v1/info"       $TimeoutSeconds

    # ---- export stress parameters as env vars --------------------------------
    $env:RUN_STRESS                   = "1"
    $env:EDGE_DEVICE_COUNT            = $EdgeDeviceCount
    $env:EVENTS_PER_DEVICE            = $EventsPerDevice
    $env:CONCURRENCY                  = $Concurrency
    $env:STRESS_DURATION_SECONDS      = $StressDurationSeconds
    $env:P95_LATENCY_THRESHOLD_MS     = $P95LatencyThresholdMs
    $env:MIN_THROUGHPUT_EVENTS_PER_SEC = $MinThroughput
    $env:FRAME_POOL_SIZE              = $FramePoolSize

    Write-Host ""
    Write-Host "[stress] Parameters:"
    Write-Host "  EDGE_DEVICE_COUNT             = $EdgeDeviceCount"
    Write-Host "  EVENTS_PER_DEVICE             = $EventsPerDevice"
    Write-Host "  CONCURRENCY                   = $Concurrency"
    Write-Host "  STRESS_DURATION_SECONDS       = $StressDurationSeconds"
    Write-Host "  P95_LATENCY_THRESHOLD_MS      = $P95LatencyThresholdMs"
    Write-Host "  MIN_THROUGHPUT_EVENTS_PER_SEC = $MinThroughput"
    Write-Host "  FRAME_POOL_SIZE               = $FramePoolSize"
    Write-Host "  Total events                  = $($EdgeDeviceCount * $EventsPerDevice)"
    Write-Host ""

    Write-Host "[stress] Running stress test ..."
    python -m pytest tests/stress/test_edge_fanout_pipeline.py -m stress -v -s
    if ($LASTEXITCODE -ne 0) {
        throw "Stress pytest failed with exit code $LASTEXITCODE"
    }

} finally {
    if (-not $KeepStack) {
        Write-Host "[stress] Tearing down stress stack and volumes ..."
        & docker @($ComposeArgs + @("down", "-v", "--remove-orphans"))
    } else {
        Write-Host "[stress] Keeping stack running (-KeepStack)."
    }
    Pop-Location
}
