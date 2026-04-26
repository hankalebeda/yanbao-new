param(
    [string]$Remote = "origin",
    [string]$Branch = "main",
    [switch]$SkipPull,
    [switch]$NoPush,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $venvPython) { $venvPython } else { (Get-Command python).Source }
$runner = Join-Path $repoRoot "scripts\git_hourly_sync.py"

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

$logRoot = Join-Path $repoRoot "github\automation\_local\git_hourly_sync"
$null = New-Item -ItemType Directory -Force -Path $logRoot
$timestamp = Get-Date -Format "yyyyMMddTHHmmss"
$logPath = Join-Path $logRoot "$timestamp.log"

$argsList = @(
    $runner,
    "--remote", $Remote,
    "--branch", $Branch
)

if ($SkipPull) {
    $argsList += "--skip-pull"
}
if ($NoPush) {
    $argsList += "--no-push"
}
if ($DryRun) {
    $argsList += "--dry-run"
}

$env:PYTHONIOENCODING = "utf-8"
Set-Location $repoRoot
& $python @argsList *>&1 | Tee-Object -FilePath $logPath
exit $LASTEXITCODE
