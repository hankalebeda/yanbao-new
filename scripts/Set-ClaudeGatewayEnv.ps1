function Get-ClaudeKeyConfig {
    param(
        [Parameter(Mandatory = $true)]
        [string]$KeyPath
    )

    if (-not (Test-Path $KeyPath)) {
        throw "Key file not found: $KeyPath"
    }

    $keyLines = @(
        Get-Content $KeyPath | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    )

    if ($keyLines.Count -lt 2) {
        throw "key.txt must contain endpoint and auth token on separate non-empty lines: $KeyPath"
    }

    $endpoint = $keyLines[0].TrimEnd("/")
    $authToken = $keyLines[1]

    if ($endpoint -notmatch '^https?://') {
        throw "Endpoint must start with http:// or https:// in $KeyPath"
    }

    if (-not $authToken) {
        throw "Auth token is missing in $KeyPath"
    }

    return @{
        KeyPath = $KeyPath
        Endpoint = $endpoint
        AuthToken = $authToken
    }
}

function Resolve-ClaudeKeyPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BaseDir,

        [string]$PreferredPath = ""
    )

    if (-not [string]::IsNullOrWhiteSpace($PreferredPath)) {
        return $PreferredPath
    }

    $preferred = Join-Path $BaseDir "key0.txt"
    if (Test-Path $preferred) {
        return $preferred
    }

    $fallback = Join-Path $BaseDir "key1.txt"
    if (Test-Path $fallback) {
        return $fallback
    }

    return (Join-Path $BaseDir "key.txt")
}

function Set-ClaudeGatewayEnv {
    param(
        [Parameter(Mandatory = $true)]
        [string]$KeyPath,

        [ValidateSet("auto", "direct", "proxy")]
        [string]$ProxyMode = "auto",

        [string]$ProxyUrl = "http://127.0.0.1:10808"
    )

    $config = Get-ClaudeKeyConfig -KeyPath $KeyPath

    $env:ANTHROPIC_BASE_URL = $config.Endpoint
    $useAuthTokenOnly = $config.Endpoint -match '(^https?://)?([^/]+\.)?anyrouter\.top(?::\d+)?($|/)'
    if ($useAuthTokenOnly) {
        $env:ANTHROPIC_AUTH_TOKEN = $config.AuthToken
        if (Test-Path Env:ANTHROPIC_API_KEY) {
            Remove-Item Env:ANTHROPIC_API_KEY
        }
    } else {
        $env:ANTHROPIC_API_KEY = $config.AuthToken
        if (Test-Path Env:ANTHROPIC_AUTH_TOKEN) {
            Remove-Item Env:ANTHROPIC_AUTH_TOKEN
        }
    }

    foreach ($name in @(
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY"
    )) {
        if (Test-Path ("Env:" + $name)) {
            Remove-Item ("Env:" + $name)
        }
    }

    $resolvedProxyMode = $ProxyMode
    switch ($ProxyMode) {
        "proxy" {
            $env:HTTP_PROXY = $ProxyUrl
            $env:HTTPS_PROXY = $ProxyUrl
        }
        "direct" {
            foreach ($name in @("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")) {
                if (Test-Path ("Env:" + $name)) {
                    Remove-Item ("Env:" + $name)
                }
            }
        }
        default {
            $hasHttpProxy = -not [string]::IsNullOrEmpty($env:HTTP_PROXY)
            $hasHttpsProxy = -not [string]::IsNullOrEmpty($env:HTTPS_PROXY)
            if (-not $hasHttpProxy) {
                $env:HTTP_PROXY = $ProxyUrl
            }
            if (-not $hasHttpsProxy) {
                $env:HTTPS_PROXY = $ProxyUrl
            }
            if ($hasHttpProxy -or $hasHttpsProxy) {
                $resolvedProxyMode = "auto-existing"
            } else {
                $resolvedProxyMode = "auto-default"
            }
        }
    }

    if ([string]::IsNullOrEmpty($env:NO_PROXY)) {
        $env:NO_PROXY = "127.0.0.1,localhost,::1"
    }

    return @{
        KeyPath = $config.KeyPath
        Endpoint = $config.Endpoint
        AuthMode = if ($useAuthTokenOnly) { "auth_token" } else { "api_key" }
        ProxyMode = $resolvedProxyMode
        HttpProxy = $env:HTTP_PROXY
        HttpsProxy = $env:HTTPS_PROXY
    }
}
