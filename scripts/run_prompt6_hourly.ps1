param(
  [string]$BaseUrl = "http://127.0.0.1:8000",
  [int]$TimeoutMinutes = 50,
  [switch]$DryRun,
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$ExtraArgs
)

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptRoot
$pythonExe = if ($env:PYTHON_EXE) { $env:PYTHON_EXE } else { "python" }

Write-Host "[prompt6] launching manual run from $repoRoot"
Write-Host "[prompt6] base_url=$BaseUrl timeout_minutes=$TimeoutMinutes"

$argsList = @(
  (Join-Path $scriptRoot "prompt6_hourly_codex.py"),
  "run-once",
  "--repo-root", $repoRoot,
  "--base-url", $BaseUrl,
  "--timeout-minutes", "$TimeoutMinutes",
  "--provider", "sub.jlypx.de",
  "--provider", "infiniteai.cc",
  "--provider", "ai.qaq.al"
)

if ($DryRun) {
  $argsList += "--dry-run"
}

if ($ExtraArgs) {
  $argsList += $ExtraArgs
}

& $pythonExe @argsList
exit $LASTEXITCODE
