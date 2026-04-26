param(
    [string]$KeyPath = "D:\yanbao\ai-api\claude\key0.txt",
    [string]$ProxyUrl = "http://127.0.0.1:10808"
)

$ErrorActionPreference = "Stop"

function Read-RelayPair {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Key file not found: $Path"
    }

    $lines = @(
        Get-Content -LiteralPath $Path |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ }
    )

    if ($lines.Count -lt 2) {
        throw "Expected endpoint and API key on separate non-empty lines: $Path"
    }

    return @{
        base_url = $lines[0].TrimEnd("/")
        api_key = $lines[1]
    }
}

function Ensure-Hashtable {
    param($Value)

    if ($null -eq $Value) {
        return @{}
    }

    if ($Value -is [System.Collections.IDictionary]) {
        $result = @{}
        foreach ($key in $Value.Keys) {
            $result[$key] = $Value[$key]
        }
        return $result
    }

    $result = @{}
    foreach ($prop in $Value.PSObject.Properties) {
        $result[$prop.Name] = $prop.Value
    }
    return $result
}

function Ensure-List {
    param($Value)

    $result = @()
    if ($null -eq $Value) {
        return $result
    }

    foreach ($item in $Value) {
        if ($null -ne $item) {
            $result += $item
        }
    }
    return $result
}

function Load-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return @{}
    }

    $raw = Get-Content -LiteralPath $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }

    try {
        return Ensure-Hashtable (ConvertFrom-Json -InputObject $raw)
    } catch {
        $sanitized = $raw `
            -replace '(?m)^\s*"http_proxy"\s*:\s*".*?"\s*,?\s*$', '' `
            -replace '(?m)^\s*"https_proxy"\s*:\s*".*?"\s*,?\s*$', '' `
            -replace ',\s*([}\]])', '$1'
        return Ensure-Hashtable (ConvertFrom-Json -InputObject $sanitized)
    }
}

function Save-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [hashtable]$Payload
    )

    $parent = Split-Path -Parent $Path
    if ($parent -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }

    $json = $Payload | ConvertTo-Json -Depth 20
    Set-Content -LiteralPath $Path -Value $json -Encoding UTF8
}

function Set-EnvVarEntries {
    param(
        [object[]]$CurrentEntries = @(),
        [Parameter(Mandatory = $true)]
        [hashtable]$DesiredMap
    )

    $byName = [ordered]@{}
    foreach ($entry in $CurrentEntries) {
        $hash = Ensure-Hashtable $entry
        $name = [string]$hash["name"]
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $byName[$name] = [ordered]@{
                name = $name
                value = [string]$hash["value"]
            }
        }
    }

    foreach ($name in $DesiredMap.Keys) {
        $byName[$name] = [ordered]@{
            name = $name
            value = [string]$DesiredMap[$name]
        }
    }

    return @($byName.Values)
}

$pair = Read-RelayPair -Path $KeyPath
$authMode = if ($pair.base_url -match '(^https?://)?([^/]+\.)?anyrouter\.top(?::\d+)?($|/)') { "auth_token" } else { "api_key" }
$preferredModel = if ($authMode -eq "auth_token") { "claude-sonnet-4-5-20250929" } else { "claude-opus-4-6" }
$envMap = [ordered]@{
    ANTHROPIC_BASE_URL = $pair.base_url
    ANTHROPIC_DEFAULT_OPUS_MODEL = "claude-opus-4-6"
    ANTHROPIC_DEFAULT_SONNET_MODEL = "claude-sonnet-4-5-20250929"
    ANTHROPIC_DEFAULT_HAIKU_MODEL = "claude-haiku-4-5-20251001"
    HTTP_PROXY = $ProxyUrl
    HTTPS_PROXY = $ProxyUrl
}
if ($authMode -eq "auth_token") {
    $envMap["ANTHROPIC_AUTH_TOKEN"] = $pair.api_key
} else {
    $envMap["ANTHROPIC_API_KEY"] = $pair.api_key
}

$claudeSettingsPath = "C:\Users\Administrator\.claude\settings.json"
$claudeSettings = Load-JsonFile -Path $claudeSettingsPath
if (-not $claudeSettings.ContainsKey('$schema')) {
    $claudeSettings['$schema'] = "https://json.schemastore.org/claude-code-settings.json"
}
$claudeEnv = Ensure-Hashtable $claudeSettings["env"]
foreach ($authVar in @("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")) {
    if ($claudeEnv.ContainsKey($authVar)) {
        [void]$claudeEnv.Remove($authVar)
    }
}
foreach ($key in $envMap.Keys) {
    $claudeEnv[$key] = $envMap[$key]
}
$claudeSettings["env"] = $claudeEnv
$claudeSettings["model"] = $preferredModel
$claudeSettings["forceLoginMethod"] = "console"
Save-JsonFile -Path $claudeSettingsPath -Payload $claudeSettings

$ideSettingsPaths = @(
    "C:\Users\Administrator\AppData\Roaming\Code\User\settings.json",
    "C:\Users\Administrator\AppData\Roaming\Cursor\User\settings.json"
)

foreach ($settingsPath in $ideSettingsPaths) {
    $settings = Load-JsonFile -Path $settingsPath
    $settings["http.proxy"] = $ProxyUrl
    $settings["http.proxyStrictSSL"] = $false
    $settings["claudeCode.disableLoginPrompt"] = $true
    if ($settings.ContainsKey("claudeCode.claudeProcessWrapper")) {
        [void]$settings.Remove("claudeCode.claudeProcessWrapper")
    }
    $settings["claudeCode.selectedModel"] = $preferredModel
    $currentEnvEntries = @(
        Ensure-List $settings["claudeCode.environmentVariables"] |
            Where-Object { [string]$_.name -notin @("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY") }
    )
    $settings["claudeCode.environmentVariables"] = Set-EnvVarEntries `
        -CurrentEntries $currentEnvEntries `
        -DesiredMap $envMap
    Save-JsonFile -Path $settingsPath -Payload $settings
}

Write-Output "SYNC_OK"
Write-Output "KEY_PATH=$KeyPath"
Write-Output "BASE_URL=$($pair.base_url)"
Write-Output "AUTH_MODE=$authMode"
Write-Output "PREFERRED_MODEL=$preferredModel"
if ($authMode -eq "auth_token") {
    Write-Output "ANTHROPIC_AUTH_TOKEN_SET=1"
    Write-Output "ANTHROPIC_API_KEY_SET=0"
} else {
    Write-Output "ANTHROPIC_AUTH_TOKEN_SET=0"
    Write-Output "ANTHROPIC_API_KEY_SET=1"
}
Write-Output "PROXY_URL=$ProxyUrl"
Write-Output "CLAUDE_SETTINGS=$claudeSettingsPath"
foreach ($settingsPath in $ideSettingsPaths) {
    Write-Output "IDE_SETTINGS=$settingsPath"
}
