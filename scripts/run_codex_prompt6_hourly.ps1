param(
    [string[]]$Providers = @("sub.jlypx.de", "snew.145678.xyz", "ai.qaq.al", "infiniteai.cc"),
    [ValidateSet("legacy", "mesh")]
    [string]$DelegateMode = "legacy",
    [int]$MeshMaxWorkers = 4,
    [int]$MeshMaxDepth = 2,
    [string]$MeshBenchmarkLabel = "",
    [string[]]$MeshDisableProvider = @("api.925214.xyz"),
    [switch]$DryRun,
    [switch]$Ephemeral,
    [switch]$NoEnsureRuntime,
    [switch]$NoDangerouslyBypass,
    [ValidateSet("read-only", "workspace-write", "danger-full-access")]
    [string]$Sandbox = "danger-full-access",
    [string]$BaseUrl = "http://127.0.0.1:8000"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $venvPython) { $venvPython } else { (Get-Command python).Source }
$runner = Join-Path $repoRoot "scripts\codex_prompt6_hourly.py"

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

$argsList = @(
    $runner,
    "--delegate-mode", $DelegateMode,
    "--base-url", $BaseUrl,
    "--sandbox", $Sandbox,
    "--mesh-max-workers", $MeshMaxWorkers,
    "--mesh-max-depth", $MeshMaxDepth,
    "--providers"
)

$argsList += $Providers

if ($MeshBenchmarkLabel) {
    $argsList += @("--mesh-benchmark-label", $MeshBenchmarkLabel)
}
foreach ($disabledProvider in $MeshDisableProvider) {
    $argsList += @("--mesh-disable-provider", $disabledProvider)
}

if ($DryRun) {
    $argsList += "--dry-run"
}
if ($Ephemeral) {
    $argsList += "--ephemeral"
}
if ($NoEnsureRuntime) {
    $argsList += "--no-ensure-runtime"
}
if ($NoDangerouslyBypass) {
    $argsList += "--no-dangerously-bypass"
}

$env:PYTHONIOENCODING = "utf-8"
Set-Location $repoRoot
& $python @argsList
exit $LASTEXITCODE
