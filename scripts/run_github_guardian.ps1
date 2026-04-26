param(
    [ValidateSet("audit-only", "audit-and-fix")]
    [string]$Mode = "audit-and-fix",
    [string[]]$Providers = @("sub.jlypx.de", "snew.145678.xyz", "ai.qaq.al", "infiniteai.cc"),
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [string]$Remote = "origin",
    [string]$BranchPrefix = "auto-fix",
    [switch]$OpenPr,
    [switch]$NoPush,
    [switch]$DryRunFix,
    [switch]$NoEnsureRuntime,
    [switch]$NoDangerouslyBypass,
    [switch]$KeepWorktree,
    [ValidateSet("legacy", "mesh")]
    [string]$DelegateMode = "legacy",
    [int]$MeshMaxWorkers = 4,
    [int]$MeshMaxDepth = 2,
    [string]$MeshBenchmarkLabel = "",
    [string[]]$MeshDisableProvider = @("api.925214.xyz"),
    [ValidateSet("read-only", "workspace-write", "danger-full-access")]
    [string]$Sandbox = "danger-full-access"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $venvPython) { $venvPython } else { (Get-Command python).Source }
$runner = Join-Path $repoRoot "scripts\github_guardian.py"

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

$argsList = @(
    $runner,
    "--mode", $Mode,
    "--base-url", $BaseUrl,
    "--remote", $Remote,
    "--branch-prefix", $BranchPrefix,
    "--sandbox", $Sandbox,
    "--delegate-mode", $DelegateMode,
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

if (-not $NoPush) {
    $argsList += "--push"
}
if ($OpenPr) {
    $argsList += "--open-pr"
}
if ($DryRunFix) {
    $argsList += "--dry-run-fix"
}
if ($NoEnsureRuntime) {
    $argsList += "--no-ensure-runtime"
}
if ($NoDangerouslyBypass) {
    $argsList += "--no-dangerously-bypass"
}
if ($KeepWorktree) {
    $argsList += "--keep-worktree"
}

$env:PYTHONIOENCODING = "utf-8"
Set-Location $repoRoot
& $python @argsList
exit $LASTEXITCODE
