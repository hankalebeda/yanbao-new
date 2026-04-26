$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (Test-Path $venvPython) {
    $python = $venvPython
} else {
    $python = (Get-Command python).Source
}

$env:PYTHONIOENCODING = "utf-8"
$env:MOCK_LLM = "true"
$env:ENABLE_SCHEDULER = "false"
$env:STRICT_REAL_DATA = "false"

Set-Location $repoRoot
& $python (Join-Path $repoRoot "scripts\continuous_repo_audit.py")
exit $LASTEXITCODE
