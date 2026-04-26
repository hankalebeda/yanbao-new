param(
    [string]$TaskName = "Yanbao-Git-Hourly-Sync",
    [int]$EveryMinutes = 60,
    [string]$Remote = "origin",
    [string]$Branch = "main",
    [ValidateSet("LIMITED", "HIGHEST")]
    [string]$RunLevel = "LIMITED",
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

if ($EveryMinutes -lt 60) {
    throw "EveryMinutes must be >= 60 for the hourly Git sync task"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $repoRoot "scripts\run_git_hourly_sync.ps1"
if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

function Invoke-Schtasks {
    param(
        [string[]]$Arguments
    )

    if ($WhatIf) {
        return ("schtasks " + ($Arguments -join " "))
    }

    $output = (& schtasks @Arguments 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        if ($output) {
            throw $output
        }
        throw "schtasks failed with exit code $LASTEXITCODE"
    }
    return $output
}

$start = (Get-Date).AddMinutes(1).ToString("HH:mm")
$taskAction = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runner`" -Remote `"$Remote`" -Branch `"$Branch`""

$createArgs = @(
    "/Create",
    "/F",
    "/SC",
    "MINUTE",
    "/MO",
    "$EveryMinutes",
    "/ST",
    $start,
    "/TN",
    $TaskName,
    "/TR",
    $taskAction,
    "/RL",
    $RunLevel
)

if ($WhatIf) {
    Invoke-Schtasks -Arguments $createArgs
    exit 0
}

try {
    Invoke-Schtasks -Arguments $createArgs | Out-Null
    Invoke-Schtasks -Arguments @("/Query", "/TN", $TaskName, "/FO", "LIST", "/V")
} catch {
    $message = $_.Exception.Message
    if ($message -match "Access is denied") {
        throw "Task registration was blocked by Windows permissions. Re-run from an elevated PowerShell window."
    }
    throw
}
