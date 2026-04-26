param(
  [string]$BaseUrl = "http://127.0.0.1:8000",
  [int]$TimeoutMinutes = 30,
  [switch]$DryRun,
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$ExtraArgs
)

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptRoot
$pythonExe = if ($env:PYTHON_EXE) { $env:PYTHON_EXE } else { "python" }

Write-Host "[issue22] launching manual analysis-only run from $repoRoot"
Write-Host "[issue22] base_url=$BaseUrl timeout_minutes=$TimeoutMinutes"

$argsList = @(
  (Join-Path $scriptRoot "issue_mining_22_codex.py"),
  "run-once",
  "--repo-root", $repoRoot,
  "--base-url", $BaseUrl,
  "--timeout-minutes", "$TimeoutMinutes",
  "--provider", "sub.jlypx.de",
  "--provider", "snew.145678.xyz",
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
