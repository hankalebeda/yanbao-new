<#
.SYNOPSIS
  Provision stable + readonly shard provider homes for the New API gateway.
.DESCRIPTION
  Wraps ai-api/codex/sync_newapi_channels.py so deployment can materialize
  lane-isolated provider homes and token/group mappings before starting
  mesh_runner or promote_prep. The script writes a JSON summary under output/
  and prints the canonical env values to wire into automation/deploy/.env.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Username,

    [Parameter(Mandatory = $true)]
    [string]$Password,

    [string]$RepoRoot = "",
    [string]$BaseUrl = "",
    [string]$ProvidersRoot = "",
    [string]$GatewayProviderBaseName = "newapi-192.168.232.141-3000",
    [string]$TokenName = "codex-relay-xhigh",
    [string]$GatewayProviderShards = "ro-a,ro-b,ro-c,ro-d",
    [string]$OutputPath = "",
    [switch]$AllowTokenFork
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
}

if (-not $BaseUrl) {
    $BaseUrl = [System.Environment]::GetEnvironmentVariable("NEW_API_BASE_URL")
    if (-not $BaseUrl) {
        $BaseUrl = "http://192.168.232.141:3000"
    }
}

if (-not $ProvidersRoot) {
    $ProvidersRoot = Join-Path $RepoRoot "ai-api\codex"
}

if (-not $OutputPath) {
    $OutputPath = Join-Path $RepoRoot "output\newapi_gateway_shards_latest.json"
}

$python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    $python = "python"
}

$syncScript = Join-Path $RepoRoot "ai-api\codex\sync_newapi_channels.py"
if (-not (Test-Path $syncScript)) {
    throw "sync_newapi_channels.py not found: $syncScript"
}

$outputDir = Split-Path -Parent $OutputPath
if ($outputDir -and -not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

$cmd = @(
    $syncScript,
    "--base-url", $BaseUrl,
    "--username", $Username,
    "--password", $Password,
    "--providers-root", $ProvidersRoot,
    "--token-name", $TokenName,
    "--provision-gateway-only",
    "--write-sharded-gateway-provider-dirs",
    "--gateway-provider-name", $GatewayProviderBaseName,
    "--gateway-provider-shards", $GatewayProviderShards,
    "--out", $OutputPath
)
if ($AllowTokenFork) {
    $cmd += "--allow-token-fork"
}

Write-Host "[provision] base_url=$BaseUrl"
Write-Host "[provision] providers_root=$ProvidersRoot"
Write-Host "[provision] gateway_provider_base=$GatewayProviderBaseName"
Write-Host "[provision] gateway_provider_shards=$GatewayProviderShards"
& $python @cmd
if ($LASTEXITCODE -ne 0) {
    throw "Provisioning New API shard provider homes failed with exit code $LASTEXITCODE."
}

$summary = Get-Content -Path $OutputPath -Raw -Encoding utf8 | ConvertFrom-Json
$stableProvider = "$GatewayProviderBaseName-stable"
$readonlyAllowlist = @()
foreach ($suffix in @($summary.gateway_provider_shards.suffixes)) {
    if ([string]$suffix -eq "stable") {
        continue
    }
    $readonlyAllowlist += "$GatewayProviderBaseName-$suffix"
}

Write-Host "[provision] summary=$OutputPath"
Write-Host "[provision] CODEX_CANONICAL_PROVIDER=$stableProvider"
Write-Host "[provision] CODEX_READONLY_PROVIDER_ALLOWLIST=$([string]::Join(',', $readonlyAllowlist))"
