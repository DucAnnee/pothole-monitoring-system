param(
    [switch]$KeepStack,
    [switch]$SkipBuild,
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$ComposeArgs = @(
    "compose",
    "-p", "pms-e2e",
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
    $upArgs = @("up", "-d")
    if (-not $SkipBuild) {
        $upArgs += "--build"
    }

    Write-Host "[e2e] Starting Docker Compose test stack..."
    Invoke-DockerCompose $upArgs

    Write-Host "[e2e] Waiting for infrastructure readiness..."
    Wait-HttpReady "Schema Registry" "http://localhost:8082/subjects" $TimeoutSeconds
    Wait-HttpReady "MinIO" "http://localhost:9000/minio/health/live" $TimeoutSeconds
    Wait-HttpReady "Polaris" "http://localhost:8182/q/health" $TimeoutSeconds
    Wait-HttpReady "Trino" "http://localhost:8081/v1/info" $TimeoutSeconds

    Write-Host "[e2e] Running real video edge-to-cloud test..."
    $env:RUN_REAL_E2E = "1"
    python -m pytest tests/e2e/test_real_video_pipeline.py -m e2e -v
    if ($LASTEXITCODE -ne 0) {
        throw "E2E pytest failed with exit code $LASTEXITCODE"
    }
} finally {
    if (-not $KeepStack) {
        Write-Host "[e2e] Tearing down Docker Compose test stack and volumes..."
        & docker @($ComposeArgs + @("down", "-v", "--remove-orphans"))
    } else {
        Write-Host "[e2e] Keeping stack running because -KeepStack was provided."
    }
    Pop-Location
}
