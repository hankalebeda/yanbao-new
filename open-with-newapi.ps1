param(
  [ValidateSet("auto", "code", "cursor")]
  [string]$Ide = "code",

  [switch]$NoLaunch,

  [switch]$PrintEnv,

  [switch]$TestApi
)

$ErrorActionPreference = "Stop"

$base = Split-Path -Parent $MyInvocation.MyCommand.Path
$providerRoot = Join-Path $base "ai-api" "codex"
$keyPath = Join-Path $base "ai-api" "key.txt"

$primaryModel = "gpt-5.4"
$reviewModel = "gpt-5.3-codex"
$fallbackModels = @($primaryModel, $reviewModel, "gpt-5.2")

function Test-NewApiResponses {
  param(
    [string]$Endpoint,
    [string]$ApiKey
  )
  $headers = @{ "Authorization" = "Bearer $ApiKey" }
  try {
    $resp = Invoke-RestMethod -Uri "$Endpoint/models" -Headers $headers -TimeoutSec 10
    Write-Host "API_TEST: $($resp.data.Count) models available"
    return $true
  } catch {
    Write-Warning "API_TEST failed: $_"
    return $false
  }
}

$env:CODEX_PRIMARY_MODEL = $primaryModel
$env:CODEX_REVIEW_MODEL = $reviewModel
$env:CODEX_FALLBACK_MODELS = "FALLBACK_MODELS=$($fallbackModels -join ',')"

$configBlock = @"
model = "$primaryModel"
review_model = "$reviewModel"
fallback_models = ["$($fallbackModels -join '", "')"]
"@

if ($TestApi) {
  if (-not (Test-Path $keyPath)) {
    Write-Error "key.txt not found at $keyPath"
    exit 1
  }
  $lines = @(Get-Content $keyPath | ForEach-Object { $_.Trim() } | Where-Object { $_ })
  $endpoint = $lines[0].TrimEnd("/")
  $apiKey = $lines[1]
  $result = Test-NewApiResponses -Endpoint "$endpoint/v1" -ApiKey $apiKey
  if (-not $result) { exit 1 }
}

if ($PrintEnv) {
  Write-Host $configBlock
  exit 0
}

. (Join-Path $base "scripts" "Start-CodexWorkspace.ps1")

Start-CodexWorkspace -Ide $Ide -ProjectRoot $base -ProviderRoot $providerRoot -NoLaunch:$NoLaunch
