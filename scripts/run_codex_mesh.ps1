param(
    [ValidateSet("run", "bench", "status", "replay")]
    [string]$Command = "status",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $venvPython) { $venvPython } else { (Get-Command python).Source }
$runner = Join-Path $repoRoot "scripts\codex_mesh.py"

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

$argsList = @(
    $runner,
    $Command
)

if ($RemainingArgs) {
    $argsList += $RemainingArgs
}

$env:PYTHONIOENCODING = "utf-8"
Set-Location $repoRoot
& $python @argsList
exit $LASTEXITCODE
