param(
    [string]$EnvFile = ".env",
    [switch]$CheckExternalServices
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $scriptDir "docker-compose.kestra.yml"
$envPath = Join-Path $scriptDir $EnvFile

if (-not (Test-Path $composeFile)) {
    throw "Compose file not found: $composeFile"
}

if (-not (Test-Path $envPath)) {
    throw "Env file not found: $envPath"
}

function Get-EnvValue {
    param(
        [string]$Name,
        [string]$Default = ""
    )

    $line = Get-Content $envPath | Where-Object { $_ -like "$Name=*" } | Select-Object -First 1
    if (-not $line) {
        return $Default
    }
    $parts = $line -split "=", 2
    if ($parts.Length -ne 2) {
        return $Default
    }
    $value = $parts[1].Trim()
    if (-not $value) {
        return $Default
    }
    return $value
}

function Invoke-HealthCheck {
    param(
        [string]$Name,
        [string]$Url,
        [hashtable]$Headers = @{},
        [int]$TimeoutSec = 8
    )

    if (-not $Url) {
        throw "Health check URL is empty for $Name."
    }

    Write-Host "Checking $Name via endpoint: $Url"
    $resp = Invoke-RestMethod -Method GET -Uri $Url -Headers $Headers -TimeoutSec $TimeoutSec
    Write-Host "$Name check succeeded."
    return $resp
}

docker compose --env-file $envPath -f $composeFile ps

$port = Get-EnvValue -Name "KESTRA_HTTP_PORT" -Default "18080"
$username = Get-EnvValue -Name "KESTRA_BASIC_AUTH_USERNAME"
$password = Get-EnvValue -Name "KESTRA_BASIC_AUTH_PASSWORD"

$healthPaths = @("/api/v1/flows", "/api/v1/flows/search")
$headers = @{}
if ($username -and $password) {
    $raw = [System.Text.Encoding]::UTF8.GetBytes("$username`:$password")
    $headers["Authorization"] = "Basic " + [Convert]::ToBase64String($raw)
}

$success = $false
$lastError = $null

foreach ($path in $healthPaths) {
    $healthUrl = "http://localhost:$port$path"
    Write-Host "Checking Kestra readiness via endpoint: $healthUrl"
    try {
        $resp = Invoke-RestMethod -Method GET -Uri $healthUrl -Headers $headers -TimeoutSec 8
        Write-Host "Kestra readiness check succeeded via $path."
        $resp | ConvertTo-Json -Depth 4
        $success = $true
        break
    } catch [System.Net.WebException] {
        $statusCode = $null
        if ($_.Exception.Response) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        if ($statusCode -eq 404) {
            continue
        }
        $lastError = $_
        break
    } catch {
        $lastError = $_
        break
    }
}

if (-not $success) {
    if ($lastError) {
        Write-Warning "Kestra readiness check failed: $($lastError.Exception.Message)"
    } else {
        Write-Warning "Kestra readiness check failed: no supported readiness endpoint returned success."
    }
    exit 1
}

if (-not $CheckExternalServices) {
    return
}

$newApiBaseUrl = Get-EnvValue -Name "NEW_API_BASE_URL"
$newApiToken = Get-EnvValue -Name "NEW_API_TOKEN"
if ($newApiBaseUrl) {
    if (-not $newApiToken -or $newApiToken -eq "replace-me") {
        throw "NEW_API_TOKEN must be set before using -CheckExternalServices."
    }
    $newApiHeaders = @{
        Authorization = "Bearer $newApiToken"
    }
    Invoke-HealthCheck -Name "New API" -Url "$newApiBaseUrl/v1/models" -Headers $newApiHeaders | ConvertTo-Json -Depth 4
}

$appBaseUrl = Get-EnvValue -Name "APP_BASE_URL"
if ($appBaseUrl) {
    Invoke-HealthCheck -Name "Yanbao app" -Url "$appBaseUrl/health" | ConvertTo-Json -Depth 4
}

$meshRunnerBaseUrl = Get-EnvValue -Name "MESH_RUNNER_BASE_URL"
if ($meshRunnerBaseUrl) {
    Invoke-HealthCheck -Name "mesh_runner" -Url "$meshRunnerBaseUrl/health" | ConvertTo-Json -Depth 4
}

$promotePrepBaseUrl = Get-EnvValue -Name "PROMOTE_PREP_BASE_URL"
if ($promotePrepBaseUrl) {
    Invoke-HealthCheck -Name "promote_prep" -Url "$promotePrepBaseUrl/health" | ConvertTo-Json -Depth 4
}

$writebackABaseUrl = Get-EnvValue -Name "WRITEBACK_A_BASE_URL"
if ($writebackABaseUrl) {
    Invoke-HealthCheck -Name "writeback A" -Url "$writebackABaseUrl/health" | ConvertTo-Json -Depth 4
}

$writebackBBaseUrl = Get-EnvValue -Name "WRITEBACK_B_BASE_URL"
if ($writebackBBaseUrl) {
    Invoke-HealthCheck -Name "writeback B" -Url "$writebackBBaseUrl/health" | ConvertTo-Json -Depth 4
}

$loopControllerBaseUrl = Get-EnvValue -Name "LOOP_CONTROLLER_BASE_URL"
if ($loopControllerBaseUrl) {
    Invoke-HealthCheck -Name "loop_controller" -Url "$loopControllerBaseUrl/health" | ConvertTo-Json -Depth 4
}
