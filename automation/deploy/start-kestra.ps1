param(
    [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $scriptDir "docker-compose.kestra.yml"
$envPath = Join-Path $scriptDir $EnvFile

if (-not (Test-Path $composeFile)) {
    throw "Compose file not found: $composeFile"
}

if (-not (Test-Path $envPath)) {
    throw "Env file not found: $envPath`nCopy .env.example to .env first."
}

Write-Host "Starting Kestra stack with compose file: $composeFile"
docker compose --env-file $envPath -f $composeFile up -d

$port = "18080"
$line = Get-Content $envPath | Where-Object { $_ -like "KESTRA_HTTP_PORT=*" } | Select-Object -First 1
if ($line) {
    $parts = $line -split "=", 2
    if ($parts.Length -eq 2 -and $parts[1]) {
        $port = $parts[1].Trim()
    }
}

Write-Host "Kestra stack started."
Write-Host "Kestra UI/API: http://localhost:$port"
