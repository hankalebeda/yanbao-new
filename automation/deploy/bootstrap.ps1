<#
.SYNOPSIS
  Cold-start the Windows-side Yanbao autonomy stack and optionally trigger Kestra.
.DESCRIPTION
  1. Initialize runtime directories.
  2. Start the full local stack on ports 38001 and 8092-8096.
  3. Verify health locally.
  4. Trigger the first Kestra master_loop execution unless -SkipKestra is set.
  5. Start watchdog in the background.
#>
param(
    [string]$RepoRoot = "",
    [string]$KestraUrl = "http://192.168.232.141:18080",
    [string]$KestraUser = "admin@kestra.io",
    [string]$KestraPassword = "Kestra20260327!",
    [switch]$SkipKestra
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
}

$deployDir = Join-Path $RepoRoot "automation\deploy"
$logDir = Join-Path $RepoRoot "runtime\services\logs"

Write-Host "============================================="
Write-Host " BOOTSTRAP: Kestra + New API + Writeback Stack"
Write-Host " Repo: $RepoRoot"
Write-Host " Kestra: $KestraUrl"
Write-Host "============================================="

Write-Host "`n[1/5] Initializing runtime directories..."
$initScript = Join-Path $deployDir "init-runtime.ps1"
& $initScript -RepoRoot $RepoRoot

Write-Host "`n[2/5] Starting Windows app + control-plane services..."
$startScript = Join-Path $deployDir "start-all-services.ps1"
& $startScript -RepoRoot $RepoRoot

Write-Host "`n[3/5] Verifying all services..."
$allOk = $true
$ports = @(
    @{ Name = "app"; Port = 38001 },
    @{ Name = "writeback_a"; Port = 8092 },
    @{ Name = "writeback_b"; Port = 8095 },
    @{ Name = "mesh_runner"; Port = 8093 },
    @{ Name = "promote_prep"; Port = 8094 },
    @{ Name = "loop_controller"; Port = 8096 }
)

foreach ($svc in $ports) {
    try {
        Invoke-RestMethod -Method GET -Uri "http://127.0.0.1:$($svc.Port)/health" -TimeoutSec 5 | Out-Null
        Write-Host "  [OK] $($svc.Name) on port $($svc.Port)"
    } catch {
        Write-Warning "  [FAIL] $($svc.Name) on port $($svc.Port): $($_.Exception.Message)"
        $allOk = $false
    }
}

if (-not $allOk) {
    Write-Warning "Some services are not healthy. Check logs in: $logDir"
    Write-Host "Continuing anyway - watchdog will monitor and restart."
}

if (-not $SkipKestra) {
    Write-Host "`n[4/5] Triggering first Kestra master_loop execution..."
    $kestraAuth = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes("${KestraUser}:${KestraPassword}"))
    $kestraHeaders = @{ Authorization = "Basic $kestraAuth" }
    try {
        $resp = Invoke-RestMethod `
            -Method POST `
            -Uri "$KestraUrl/api/v1/executions/yanbao.ops/yanbao_master_loop" `
            -Headers $kestraHeaders `
            -TimeoutSec 15
        Write-Host "  [OK] Kestra execution triggered: $($resp.id)"
    } catch {
        Write-Warning "  [FAIL] Could not trigger Kestra: $($_.Exception.Message)"
        Write-Host "  The master_loop schedule trigger will auto-start once Kestra is reachable."
    }
} else {
    Write-Host "`n[4/5] Skipping Kestra trigger (--SkipKestra)"
}

Write-Host "`n[5/5] Starting watchdog guardian..."
$watchdogScript = Join-Path $deployDir "watchdog.ps1"
$watchdogLog = Join-Path $RepoRoot "runtime\services\watchdog.log"
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$watchdogScript`" -RepoRoot `"$RepoRoot`"" -WindowStyle Hidden

Write-Host "`n============================================="
Write-Host " BOOTSTRAP COMPLETE"
Write-Host " Services: 6 FastAPI instances on ports 38001,8092-8096"
Write-Host " Watchdog: running in background"
if (-not $SkipKestra) {
    Write-Host " Kestra: master_loop triggered + schedule every 10min"
}
Write-Host " Logs: $logDir"
Write-Host " Watchdog log: $watchdogLog"
Write-Host "============================================="
