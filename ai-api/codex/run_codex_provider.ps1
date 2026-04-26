param(
  [string]$ProviderDir = "ai.qaq.al",
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$CodexArgs
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$providerPath = Join-Path $root $ProviderDir

if (-not (Test-Path $providerPath)) {
  throw "Provider directory not found: $providerPath"
}

$configPath = Join-Path $providerPath "config.toml"
$authPath = Join-Path $providerPath "auth.json"
$keyPath = Join-Path $providerPath "key.txt"

if (-not (Test-Path $configPath)) {
  throw "Missing config.toml: $configPath"
}

$apiKey = $null
if (Test-Path $keyPath) {
  $keyLines = @(
    Get-Content $keyPath | ForEach-Object { $_.Trim() } | Where-Object { $_ }
  )
  if ($keyLines.Count -ge 2 -and $keyLines[0] -match '^https?://') {
    $apiKey = $keyLines[1]
  } elseif ($keyLines.Count -ge 1) {
    $apiKey = $keyLines[0]
  }
}

if (-not $apiKey) {
  if (-not (Test-Path $authPath)) {
    throw "Missing auth.json: $authPath"
  }

  $authJson = Get-Content $authPath -Raw | ConvertFrom-Json
  $apiKey = $authJson.OPENAI_API_KEY
  if (-not $apiKey) {
    $apiKey = $authJson.api_key
  }
  if (-not $apiKey) {
    $apiKey = $authJson.apiKey
  }
}

if (-not $apiKey) {
  throw "Missing API key in key.txt/auth.json under: $providerPath"
}

$configText = Get-Content $configPath -Raw
$model = $null
$reviewModel = $null
if ($configText -match '(?m)^model = "([^"]+)"') {
  $model = $Matches[1]
}
if ($configText -match '(?m)^review_model = "([^"]+)"') {
  $reviewModel = $Matches[1]
}

$safeName = [regex]::Replace($ProviderDir, "[^A-Za-z0-9._-]", "_")
$portableHome = Join-Path $root ("portable_" + $safeName)
$portableCodex = Join-Path $portableHome ".codex"

New-Item -ItemType Directory -Force -Path $portableCodex | Out-Null
Copy-Item -Path $configPath -Destination (Join-Path $portableCodex "config.toml") -Force
Set-Content -Path (Join-Path $portableCodex "auth.json") -Value (@{ OPENAI_API_KEY = $apiKey } | ConvertTo-Json) -Encoding ASCII

$env:HOME = $portableHome
$env:USERPROFILE = $portableHome
$env:CODEX_HOME = $portableCodex

$launchArgs = @()
if ($model) {
  $launchArgs += @("-m", $model)
}
if ($reviewModel) {
  $launchArgs += @("-c", "review_model=""$reviewModel""")
}

& codex @launchArgs @CodexArgs
exit $LASTEXITCODE
