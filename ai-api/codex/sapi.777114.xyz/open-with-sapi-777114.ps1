param(
  [ValidateSet("auto", "code", "cursor")]
  [string]$Ide = "code",

  [switch]$NoLaunch,

  [switch]$PrintEnv
)

$ErrorActionPreference = "Stop"

function Get-ProviderSpecFromKeyFile {
  param(
    [Parameter(Mandatory = $true)]
    [string]$KeyPath
  )

  if (-not (Test-Path -LiteralPath $KeyPath)) {
    throw "Key file not found: $KeyPath"
  }

  $lines = @(
    Get-Content -LiteralPath $KeyPath |
      ForEach-Object { $_.Trim() } |
      Where-Object { $_ }
  )

  if ($lines.Count -lt 2) {
    throw "key.txt must contain homepage on line 1 and API key on line 2: $KeyPath"
  }

  $homepage = $lines[0].TrimEnd("/")
  $apiKey = $lines[1]

  if ($homepage -notmatch "^https?://") {
    throw "Homepage in key.txt must start with http:// or https://: $homepage"
  }

  if (-not $apiKey) {
    throw "API key is empty in $KeyPath"
  }

  $builder = [System.UriBuilder]$homepage
  $path = $builder.Path.TrimEnd("/")
  if ([string]::IsNullOrEmpty($path) -or $path -eq "/") {
    $path = "/v1"
  } elseif (-not $path.EndsWith("/v1")) {
    $path = "$path/v1"
  }

  $builder.Path = $path
  $builder.Query = ""
  $builder.Fragment = ""

  return [PSCustomObject]@{
    Name = ([System.Uri]$homepage).Host
    Homepage = $homepage
    Endpoint = $builder.Uri.AbsoluteUri.TrimEnd("/")
    ApiKey = $apiKey
  }
}

function Ensure-ProviderFiles {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ProviderRoot,

    [Parameter(Mandatory = $true)]
    [object]$ProviderSpec
  )

  $authPath = Join-Path $ProviderRoot "auth.json"
  $providerPath = Join-Path $ProviderRoot "provider.json"
  $configPath = Join-Path $ProviderRoot "config.toml"

  $authJson = [ordered]@{
    OPENAI_API_KEY = $ProviderSpec.ApiKey
  } | ConvertTo-Json
  Set-Content -LiteralPath $authPath -Value ($authJson + [Environment]::NewLine) -Encoding ASCII

  if (-not (Test-Path -LiteralPath $providerPath)) {
    $providerJson = [ordered]@{
      name = $ProviderSpec.Name
      endpoint = $ProviderSpec.Endpoint
      model = "gpt-5.4"
      homepage = $ProviderSpec.Homepage
      enabled = $true
      resource = "provider"
      app = "codex"
    } | ConvertTo-Json
    Set-Content -LiteralPath $providerPath -Value ($providerJson + [Environment]::NewLine) -Encoding ASCII
  }

  if (-not (Test-Path -LiteralPath $configPath)) {
    $configText = @(
      'model_provider = "OpenAI"'
      'model = "gpt-5.4"'
      'review_model = "gpt-5.4"'
      'model_reasoning_effort = "xhigh"'
      'disable_response_storage = true'
      'network_access = "enabled"'
      'windows_wsl_setup_acknowledged = true'
      'model_context_window = 1000000'
      'model_auto_compact_token_limit = 900000'
      'personality = "pragmatic"'
      ''
      '[model_providers.OpenAI]'
      'name = "OpenAI"'
      ('base_url = "{0}"' -f $ProviderSpec.Endpoint)
      'wire_api = "responses"'
      'supports_websockets = false'
      'requires_openai_auth = true'
      ''
      '[features]'
      'responses_websockets_v2 = false'
      'multi_agent = true'
      ''
      '[windows]'
      'sandbox = "elevated"'
      ''
    ) -join "`r`n"

    Set-Content -LiteralPath $configPath -Value $configText -Encoding ASCII
  }

  return [PSCustomObject]@{
    AuthPath = $authPath
    ProviderPath = $providerPath
    ConfigPath = $configPath
  }
}

$base = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $base "..\..\.."))
$launcherHelperPath = Join-Path $projectRoot "scripts\Start-CodexWorkspace.ps1"
$keyPath = Join-Path $base "key.txt"
$userDataDir = Join-Path $projectRoot ".vscode-sapi777114xyz-userdata"
$launchLogPath = Join-Path $projectRoot ".vscode-sapi777114xyz-last-launch.txt"

if (-not (Test-Path -LiteralPath $launcherHelperPath)) {
  throw "Launcher helper not found: $launcherHelperPath"
}

if (-not (Test-Path -LiteralPath $base)) {
  throw "Config directory not found: $base"
}

$providerSpec = Get-ProviderSpecFromKeyFile -KeyPath $keyPath
$providerFiles = Ensure-ProviderFiles -ProviderRoot $base -ProviderSpec $providerSpec

. $launcherHelperPath

$auth = Get-Content -LiteralPath $providerFiles.AuthPath -Raw | ConvertFrom-Json
$provider = Get-Content -LiteralPath $providerFiles.ProviderPath -Raw | ConvertFrom-Json

if (-not $auth.OPENAI_API_KEY) {
  throw "OPENAI_API_KEY is missing in $($providerFiles.AuthPath)"
}

if (-not $provider.endpoint) {
  throw "endpoint is missing in $($providerFiles.ProviderPath)"
}

Start-CodexWorkspace -ProjectRoot $projectRoot -CodexHome $base -UserDataDir $userDataDir -LaunchLogPath $launchLogPath -Ide $Ide -NoLaunch:$NoLaunch -PrintEnv:$PrintEnv
