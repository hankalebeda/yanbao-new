param(
    [string]$TaskName = "Yanbao-Continuous-Audit",
    [int]$EveryMinutes = 30,
    [ValidateSet("LIMITED", "HIGHEST")]
    [string]$RunLevel = "LIMITED"
)

$ErrorActionPreference = "Stop"

if ($EveryMinutes -lt 5) {
    throw "EveryMinutes must be >= 5"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $repoRoot "scripts\run_continuous_repo_audit.ps1"
if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

function Invoke-Schtasks {
    param(
        [string[]]$Arguments
    )

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
$registerHint = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -TaskName `"$TaskName`" -EveryMinutes $EveryMinutes -RunLevel $RunLevel"

try {
    Invoke-Schtasks -Arguments @("/Create", "/F", "/SC", "MINUTE", "/MO", "$EveryMinutes", "/ST", $start, "/TN", $TaskName, "/TR", $taskAction, "/RL", $RunLevel) | Out-Null
    Invoke-Schtasks -Arguments @("/Query", "/TN", $TaskName, "/FO", "LIST", "/V")
} catch {
    $message = $_.Exception.Message
    if ($message -match "Access is denied") {
        throw "Task registration was blocked by Windows permissions. Re-run from an elevated PowerShell window. Command: $registerHint"
    }
    throw
}
