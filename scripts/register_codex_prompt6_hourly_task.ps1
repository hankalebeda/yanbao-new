param(
    [string]$TaskName = "Yanbao-Codex-Prompt6-Hourly",
    [int]$EveryMinutes = 60,
    [ValidateSet("LIMITED", "HIGHEST")]
    [string]$RunLevel = "LIMITED",
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

if ($EveryMinutes -lt 60) {
    throw "EveryMinutes must be >= 60 for the hourly Prompt 6 task"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $repoRoot "scripts\run_codex_prompt6_hourly.ps1"
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
$taskAction = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runner`""

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

Invoke-Schtasks -Arguments $createArgs | Out-Null
Invoke-Schtasks -Arguments @("/Query", "/TN", $TaskName, "/FO", "LIST", "/V")
