<#
.SYNOPSIS
  Start the full Windows-side Yanbao autonomy stack.
.DESCRIPTION
  Loads automation/deploy/.env, then launches the Yanbao app (38001) plus
  writeback A/B, mesh_runner, promote_prep, and loop_controller (8092-8096).
  Each service is started through a generated detached launcher under
  runtime/services/start_*.cmd so the processes remain alive after this script
  exits and watchdog can restart them using the same canonical env.
#>
param(
    [string]$EnvFile = "",
    [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
}

if (-not $EnvFile) {
    $EnvFile = Join-Path $RepoRoot "automation\deploy\.env"
    if (-not (Test-Path $EnvFile)) {
        $EnvFile = Join-Path $RepoRoot "automation\deploy\.env.example"
    }
}

$envVars = @{}
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Length -eq 2) {
                $envVars[$parts[0].Trim()] = $parts[1].Trim()
            }
        }
    }
    Write-Host "[start] loaded env from: $EnvFile"
} else {
    Write-Warning "No deploy env file found at $EnvFile."
}

function Normalize-EnvValue {
    param([string]$Value)

    $text = [string]$Value
    if ($text.Length -ge 2) {
        $first = $text.Substring(0, 1)
        $last = $text.Substring($text.Length - 1, 1)
        if (($first -eq '"' -and $last -eq '"') -or ($first -eq "'" -and $last -eq "'")) {
            return $text.Substring(1, $text.Length - 2)
        }
    }
    return $text
}

function Get-Env {
    param(
        [string]$Name,
        [string]$Default = ""
    )

    if ($envVars.ContainsKey($Name)) {
        return (Normalize-EnvValue $envVars[$Name])
    }
    $value = [System.Environment]::GetEnvironmentVariable($Name)
    if ($value) {
        return $value
    }
    return $Default
}

$repoEnvVars = @{}
$repoEnvFile = Join-Path $RepoRoot ".env"
if (Test-Path $repoEnvFile) {
    Get-Content $repoEnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Length -eq 2) {
                $repoEnvVars[$parts[0].Trim()] = $parts[1].Trim()
            }
        }
    }
}

function Get-RepoEnv {
    param(
        [string]$Name,
        [string]$Default = ""
    )

    if ($repoEnvVars.ContainsKey($Name)) {
        return (Normalize-EnvValue $repoEnvVars[$Name])
    }
    return $Default
}

function Get-LocalNoProxy {
    $hosts = @(
        "127.0.0.1",
        "localhost",
        "::1",
        "192.168.232.141",
        "192.168.232.1"
    )
    foreach ($key in @("NO_PROXY", "no_proxy")) {
        $value = [System.Environment]::GetEnvironmentVariable($key)
        if ($value) {
            $hosts += ($value -split ",")
        }
    }

    $merged = New-Object System.Collections.Generic.List[string]
    foreach ($item in $hosts) {
        $clean = [string]$item
        if (-not $clean) { continue }
        $clean = $clean.Trim()
        if (-not $clean) { continue }
        if (-not $merged.Contains($clean)) {
            $merged.Add($clean)
        }
    }
    return [string]::Join(",", $merged)
}

$initScript = Join-Path $RepoRoot "automation\deploy\init-runtime.ps1"
if (Test-Path $initScript) {
    & $initScript -RepoRoot $RepoRoot
}
$compatScript = Join-Path $RepoRoot "automation\deploy\ensure-powershell-compat.ps1"
if (Test-Path $compatScript) {
    & $compatScript
}

foreach ($dir in @(
    "runtime\services",
    "runtime\services\logs",
    "runtime\loop_controller",
    "runtime\autonomous_fix_loop",
    "runtime\writeback_coordination",
    "runtime\issue_mesh\promote_prep\triage",
    "runtime\issue_mesh\promote_prep\code_fix",
    "automation\writeback_service\.audit\commits",
    "automation\writeback_service\.audit\idempotency",
    "automation\writeback_service\.audit_writeback_b\commits",
    "automation\writeback_service\.audit_writeback_b\idempotency"
)) {
    $fullPath = Join-Path $RepoRoot $dir
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
    }
}

$pidDir = Join-Path $RepoRoot "runtime\services"
$logDir = Join-Path $RepoRoot "runtime\services\logs"

$CONTROL_PLANE_TOKEN = Get-Env "INTERNAL_TOKEN" "kestra-internal-20260327"
$APP_INTERNAL_TOKEN = Get-RepoEnv "INTERNAL_CRON_TOKEN" (Get-RepoEnv "INTERNAL_API_KEY" $CONTROL_PLANE_TOKEN)
$WRITEBACK_A_TOKEN = Get-Env "WRITEBACK_A_TOKEN" $CONTROL_PLANE_TOKEN
$WRITEBACK_B_TOKEN = Get-Env "WRITEBACK_B_TOKEN" $CONTROL_PLANE_TOKEN
$MESH_RUNNER_TOKEN = Get-Env "MESH_RUNNER_TOKEN" $CONTROL_PLANE_TOKEN
$PROMOTE_PREP_TOKEN = Get-Env "PROMOTE_PREP_TOKEN" $CONTROL_PLANE_TOKEN
$LOOP_CONTROLLER_TOKEN = Get-Env "LOOP_CONTROLLER_TOKEN" $CONTROL_PLANE_TOKEN
$NEW_API_TOKEN = Get-Env "NEW_API_TOKEN" ""
$NEW_API_BASE_URL = Get-Env "NEW_API_BASE_URL" "http://192.168.232.141:3000"
$AUDIT_GATEWAY_ONLY = Get-Env "CODEX_AUDIT_GATEWAY_ONLY" "false"
$CANONICAL_PROVIDER = Get-Env "CODEX_CANONICAL_PROVIDER" "newapi-192.168.232.141-3000-stable"
$READONLY_LANE = Get-Env "CODEX_READONLY_LANE" "codex-readonly"
$STABLE_LANE = Get-Env "CODEX_STABLE_LANE" "codex-stable"
$READONLY_PROVIDER_ALLOWLIST = Get-Env "CODEX_READONLY_PROVIDER_ALLOWLIST" "newapi-192.168.232.141-3000-ro-a,newapi-192.168.232.141-3000-ro-b,newapi-192.168.232.141-3000-ro-c,newapi-192.168.232.141-3000-ro-d"
$MAX_WORKERS = [string]([Math]::Max([int](Get-Env "ISSUE_MESH_READONLY_MAX_WORKERS" "12"), 12))
$MAX_WORKERS_CAP = [string]([Math]::Max([int](Get-Env "ISSUE_MESH_MAX_WORKERS_CAP" $MAX_WORKERS), [int]$MAX_WORKERS))
$FIX_GOAL = Get-Env "FIX_GOAL_CONSECUTIVE" "10"
$AUDIT_INTERVAL = Get-Env "AUDIT_INTERVAL_SECONDS" "300"
$MONITOR_INTERVAL = Get-Env "MONITOR_INTERVAL_SECONDS" "1800"
$AUTONOMY_LOOP_ENABLED = Get-Env "AUTONOMY_LOOP_ENABLED" "true"
$AUTONOMY_LOOP_MODE = Get-Env "AUTONOMY_LOOP_MODE" "fix"
$AUTONOMY_LOOP_FIX_GOAL = Get-Env "AUTONOMY_LOOP_FIX_GOAL" $FIX_GOAL
$AUTONOMY_LOOP_AUDIT_INTERVAL = Get-Env "AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS" $AUDIT_INTERVAL
$AUTONOMY_LOOP_MONITOR_INTERVAL = Get-Env "AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS" $MONITOR_INTERVAL
$INTERNAL_TOKEN_ALIASES = Get-Env "INTERNAL_TOKEN_ALIASES" ""
$LLM_BASE_URL = Get-Env "PROMOTE_PREP_LLM_BASE_URL" ""
$LLM_API_KEY = Get-Env "PROMOTE_PREP_LLM_API_KEY" ""
$LOCAL_NO_PROXY = Get-LocalNoProxy
$env:NO_PROXY = $LOCAL_NO_PROXY
$env:no_proxy = $LOCAL_NO_PROXY
$NO_PROXY_VALUE = "127.0.0.1,localhost,::1,192.168.232.141,192.168.232.1"

function Assert-CodexProviderHomes {
    param(
        [string]$RepoRoot,
        [string]$CanonicalProvider,
        [string]$ReadonlyProviderAllowlist
    )

    $providerRoot = Join-Path $RepoRoot "ai-api\codex"
    $required = New-Object System.Collections.Generic.List[string]
    foreach ($name in @($CanonicalProvider) + ($ReadonlyProviderAllowlist -split ",")) {
        $clean = [string]$name
        if (-not $clean) { continue }
        $clean = $clean.Trim()
        if (-not $clean) { continue }
        if (-not $required.Contains($clean)) {
            $required.Add($clean)
        }
    }

    foreach ($providerName in $required) {
        $providerPath = Join-Path $providerRoot $providerName
        if (-not (Test-Path $providerPath)) {
            throw "MISSING_CODEX_PROVIDER_HOME: $providerName ($providerPath). Run automation/deploy/Provision-NewApiReadonlyShards.ps1 before starting services."
        }
    }
}

Assert-CodexProviderHomes -RepoRoot $RepoRoot -CanonicalProvider $CANONICAL_PROVIDER -ReadonlyProviderAllowlist $READONLY_PROVIDER_ALLOWLIST

function Resolve-Doc22Target {
    param([string]$RepoRoot)
    # Canonical doc22 prefix: docs/core/22_
    # Formal current-layer target stays under docs/core/22_*.md.
    # Encoding-safe doc22 path discovery: find the actual filename on disk
    # to avoid garbled CJK characters in PowerShell 5.1 ASCII/ANSI scripts.
    $match = Get-ChildItem (Join-Path $RepoRoot "docs\core") -Filter "22_*_v7_*.md" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($match) {
        return "docs/core/$($match.Name)"
    }
    # Fallback: build from Unicode code points (全量功能进度总表_v7_精审)
    $name = "22_" + [char]0x5168 + [char]0x91CF + [char]0x529F + [char]0x80FD + [char]0x8FDB + [char]0x5EA6 + [char]0x603B + [char]0x8868 + "_v7_" + [char]0x7CBE + [char]0x5BA1 + ".md"
    return "docs/core/$name"
}

function Get-WritebackBPolicy {
    param([string]$RepoRoot)

    $statePath = Join-Path $RepoRoot "automation\control_plane\current_state.json"
    $doc22Target = Resolve-Doc22Target -RepoRoot $RepoRoot
    $sharedArtifactDenyPaths = "output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json"
    $infraAllowPrefixes = "automation/control_plane/current_state.json,automation/control_plane/current_status.md"
    $baseDenyPrefixes = "app/,tests/,runtime/,LiteLLM/,docs/_temp/"

    $mode = "infra"
    $reason = $null
    if (Test-Path $statePath) {
        try {
            $state = Get-Content $statePath -Raw | ConvertFrom-Json -ErrorAction Stop
            $candidate = [string]$state.promote_target_mode
            if ($candidate -eq "doc22") {
                $mode = "doc22"
            } elseif ($candidate -ne "infra") {
                $reason = "CONTROL_PLANE_STATE_INVALID"
            }
        } catch {
            $reason = "CONTROL_PLANE_STATE_INVALID"
        }
    } else {
        $reason = "CONTROL_PLANE_STATE_MISSING"
    }

    $allowPrefixes = $doc22Target
    $denyPrefixes = $baseDenyPrefixes
    $denyPaths = $sharedArtifactDenyPaths
    if ($mode -ne "doc22") {
        $allowPrefixes = $infraAllowPrefixes
        $denyPrefixes = "$baseDenyPrefixes,docs/core/"
        $denyPaths = "$doc22Target,$sharedArtifactDenyPaths"
    }

    return @{
        Mode = $mode
        Reason = $reason
        StatePath = $statePath
        AllowPrefixes = $allowPrefixes
        DenyPrefixes = $denyPrefixes
        DenyPaths = $denyPaths
    }
}

$writebackBPolicy = Get-WritebackBPolicy -RepoRoot $RepoRoot
if ($writebackBPolicy.Reason) {
    Write-Warning "[start] writeback_b policy defaulted to infra because $($writebackBPolicy.Reason): $($writebackBPolicy.StatePath)"
}
Write-Host "[start] writeback_b promote_target_mode=$($writebackBPolicy.Mode); allow=$($writebackBPolicy.AllowPrefixes)"

$doc22Resolved = Resolve-Doc22Target -RepoRoot $RepoRoot
$WRITEBACK_A_DENY_PATHS = "$doc22Resolved,output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json"

$python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    $python = "python"
}

$services = @(
    @{
        Name = "app"; Port = 38001; Module = "app.main:app"; HealthWaitSeconds = 60
        Env = @{
            "INTERNAL_TOKEN" = $CONTROL_PLANE_TOKEN
            "INTERNAL_CRON_TOKEN" = $APP_INTERNAL_TOKEN
            "INTERNAL_TOKEN_ALIASES" = $INTERNAL_TOKEN_ALIASES
            "AUTONOMY_LOOP_ENABLED" = $AUTONOMY_LOOP_ENABLED
            "AUTONOMY_LOOP_MODE" = $AUTONOMY_LOOP_MODE
            "AUTONOMY_LOOP_FIX_GOAL" = $AUTONOMY_LOOP_FIX_GOAL
            "AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS" = $AUTONOMY_LOOP_AUDIT_INTERVAL
            "AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS" = $AUTONOMY_LOOP_MONITOR_INTERVAL
            "APP_BASE_URL" = "http://127.0.0.1:38001"
            "MESH_RUNNER_BASE_URL" = "http://127.0.0.1:8093"
            "MESH_RUNNER_AUTH_TOKEN" = $MESH_RUNNER_TOKEN
            "PROMOTE_PREP_BASE_URL" = "http://127.0.0.1:8094"
            "PROMOTE_PREP_AUTH_TOKEN" = $PROMOTE_PREP_TOKEN
            "WRITEBACK_A_BASE_URL" = "http://127.0.0.1:8092"
            "WRITEBACK_A_AUTH_TOKEN" = $WRITEBACK_A_TOKEN
            "WRITEBACK_B_BASE_URL" = "http://127.0.0.1:8095"
            "WRITEBACK_B_AUTH_TOKEN" = $WRITEBACK_B_TOKEN
            "NEW_API_BASE_URL" = $NEW_API_BASE_URL
            "NEW_API_TOKEN" = $NEW_API_TOKEN
            "LOOP_CONTROLLER_REPO_ROOT" = $RepoRoot
        }
    },
    @{
        Name = "writeback_a"; Port = 8092; Module = "automation.writeback_service.app:app"
        Env = @{
            "WRITEBACK_AUTH_TOKEN" = $WRITEBACK_A_TOKEN
            "WRITEBACK_REQUIRE_TRIAGE" = "true"
            "WRITEBACK_REQUIRE_FENCING" = "false"
            "WRITEBACK_ALLOW_PREFIXES" = "app/,tests/"
            "WRITEBACK_DENY_PREFIXES" = "runtime/"
            "WRITEBACK_DENY_PATHS" = $WRITEBACK_A_DENY_PATHS
            "WRITEBACK_REPO_ROOT" = $RepoRoot
        }
    },
    @{
        Name = "writeback_b"; Port = 8095; Module = "automation.writeback_service.app:app"
        Env = @{
            "WRITEBACK_AUTH_TOKEN" = $WRITEBACK_B_TOKEN
            "WRITEBACK_REQUIRE_TRIAGE" = "true"
            "WRITEBACK_REQUIRE_FENCING" = "true"
            "WRITEBACK_ALLOW_PREFIXES" = $writebackBPolicy.AllowPrefixes
            "WRITEBACK_DENY_PREFIXES" = $writebackBPolicy.DenyPrefixes
            "WRITEBACK_DENY_PATHS" = $writebackBPolicy.DenyPaths
            "WRITEBACK_AUDIT_DIR" = (Join-Path $RepoRoot "automation\writeback_service\.audit_writeback_b")
            "WRITEBACK_REPO_ROOT" = $RepoRoot
        }
    },
    @{
        Name = "mesh_runner"; Port = 8093; Module = "automation.mesh_runner.app:app"
        Env = @{
            "MESH_RUNNER_AUTH_TOKEN" = $MESH_RUNNER_TOKEN
            "ISSUE_MESH_READONLY_MAX_WORKERS" = $MAX_WORKERS
            "ISSUE_MESH_MAX_WORKERS_CAP" = $MAX_WORKERS_CAP
            "CODEX_AUDIT_GATEWAY_ONLY" = $AUDIT_GATEWAY_ONLY
            "CODEX_CANONICAL_PROVIDER" = $CANONICAL_PROVIDER
            "CODEX_READONLY_LANE" = $READONLY_LANE
            "CODEX_STABLE_LANE" = $STABLE_LANE
            "CODEX_READONLY_PROVIDER_ALLOWLIST" = $READONLY_PROVIDER_ALLOWLIST
            "NEW_API_BASE_URL" = $NEW_API_BASE_URL
            "NEW_API_TOKEN" = $NEW_API_TOKEN
        }
    },
    @{
        Name = "promote_prep"; Port = 8094; Module = "automation.promote_prep.app:app"
        Env = @{
            "PROMOTE_PREP_AUTH_TOKEN" = $PROMOTE_PREP_TOKEN
            "PROMOTE_PREP_NEW_API_BASE_URL" = $NEW_API_BASE_URL
            "PROMOTE_PREP_NEW_API_TOKEN" = $NEW_API_TOKEN
            "PROMOTE_PREP_LLM_BASE_URL" = $LLM_BASE_URL
            "PROMOTE_PREP_LLM_API_KEY" = $LLM_API_KEY
            "CODEX_CANONICAL_PROVIDER" = $CANONICAL_PROVIDER
        }
    },
    @{
        Name = "loop_controller"; Port = 8096; Module = "automation.loop_controller.app:app"
        Env = @{
            "LOOP_CONTROLLER_AUTH_TOKEN" = $LOOP_CONTROLLER_TOKEN
            "MESH_RUNNER_BASE_URL" = "http://127.0.0.1:8093"
            "MESH_RUNNER_AUTH_TOKEN" = $MESH_RUNNER_TOKEN
            "PROMOTE_PREP_BASE_URL" = "http://127.0.0.1:8094"
            "PROMOTE_PREP_AUTH_TOKEN" = $PROMOTE_PREP_TOKEN
            "WRITEBACK_A_BASE_URL" = "http://127.0.0.1:8092"
            "WRITEBACK_A_AUTH_TOKEN" = $WRITEBACK_A_TOKEN
            "WRITEBACK_B_BASE_URL" = "http://127.0.0.1:8095"
            "WRITEBACK_B_AUTH_TOKEN" = $WRITEBACK_B_TOKEN
            "APP_BASE_URL" = "http://127.0.0.1:38001"
            "INTERNAL_TOKEN" = $APP_INTERNAL_TOKEN
            "FIX_GOAL_CONSECUTIVE" = $FIX_GOAL
            "AUDIT_INTERVAL_SECONDS" = $AUDIT_INTERVAL
            "MONITOR_INTERVAL_SECONDS" = $MONITOR_INTERVAL
            "LOOP_CONTROLLER_REPO_ROOT" = $RepoRoot
            "NEW_API_BASE_URL" = $NEW_API_BASE_URL
            "NEW_API_TOKEN" = $NEW_API_TOKEN
        }
    }
)

function Stop-ServiceOnPort {
    param([int]$Port)

    $listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    foreach ($listener in $listeners) {
        $proc = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Host "[start] killing existing process $($proc.ProcessName) (PID $($proc.Id)) on port $Port"
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            Start-Sleep -Milliseconds 500
        }
    }
}

function Stop-ServiceProcesses {
    param(
        [string]$Name,
        [int]$Port,
        [string]$Module
    )

    Stop-ServiceOnPort -Port $Port

    $launcherName = "start_$Name.cmd"
    $matches = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $cmd = [string]$_.CommandLine
        $cmd -and (
            $cmd -like "*uvicorn $Module*--port $Port*" -or
            $cmd -like "*$launcherName*"
        )
    }
    foreach ($match in $matches) {
        try {
            Stop-Process -Id $match.ProcessId -Force -ErrorAction Stop
            Write-Host "[start] killed residual $Name process PID $($match.ProcessId)"
        } catch {
        }
    }

    if ($Name -eq "app") {
        Remove-Item (Join-Path $RepoRoot "runtime\loop_controller\runtime_lease.json") -Force -ErrorAction SilentlyContinue
    }
}

function New-ServiceLauncher {
    param(
        [string]$Name,
        [int]$Port,
        [string]$Module,
        [hashtable]$ServiceEnv
    )

    $launcherPath = Join-Path $pidDir "start_$Name.cmd"
    $lines = @(
        "@echo off",
        "set PYTHONPATH=$RepoRoot",
        "set NO_PROXY=$NO_PROXY_VALUE",
        "set no_proxy=$NO_PROXY_VALUE"
    )
    foreach ($entry in $ServiceEnv.GetEnumerator() | Sort-Object Name) {
        $escapedValue = [string]$entry.Value -replace '"', '""'
        $lines += "set $($entry.Key)=$escapedValue"
    }
    $lines += "cd /d $RepoRoot"
    $lines += """$python"" -m uvicorn $Module --host 0.0.0.0 --port $Port --log-level info"
    Set-Content -Path $launcherPath -Value $lines -Encoding ASCII -Force
    return $launcherPath
}

function Start-Service {
    param(
        [string]$Name,
        [int]$Port,
        [string]$Module,
        [hashtable]$ServiceEnv
    )

    Stop-ServiceProcesses -Name $Name -Port $Port -Module $Module

    $logFile = Join-Path $logDir "$Name.log"
    $errFile = "$logFile.err"
    $pidFile = Join-Path $pidDir "$Name.pid"
    $launcherPath = New-ServiceLauncher -Name $Name -Port $Port -Module $Module -ServiceEnv $ServiceEnv

    $proc = Start-Process `
        -FilePath "cmd.exe" `
        -ArgumentList "/c `"$launcherPath`"" `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $logFile `
        -RedirectStandardError $errFile `
        -PassThru

    $resolvedPid = $proc.Id
    $deadline = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $deadline) {
        $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($listener) {
            $resolvedPid = $listener.OwningProcess
            break
        }
        Start-Sleep -Milliseconds 250
    }

    Set-Content -Path $pidFile -Value $resolvedPid -Force
    Write-Host "[start] $Name started (launcher PID $($proc.Id), service PID $resolvedPid) on port $Port"
    return $resolvedPid
}

function Wait-ForHealth {
    param(
        [string]$Name,
        [int]$Port,
        [int]$MaxWaitSeconds = 30
    )

    $url = "http://127.0.0.1:$Port/health"
    $deadline = (Get-Date).AddSeconds($MaxWaitSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-RestMethod -Method GET -Uri $url -TimeoutSec 3 -ErrorAction Stop | Out-Null
            Write-Host "[start] $Name health OK on port $Port"
            return $true
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    Write-Warning "[start] $Name health check TIMEOUT on port $Port after ${MaxWaitSeconds}s"
    return $false
}

Write-Host "=========================================="
Write-Host " Starting Kestra Hybrid Services (dependency-ordered)"
Write-Host " Repo: $RepoRoot"
Write-Host " NO_PROXY: $LOCAL_NO_PROXY"
Write-Host "=========================================="

# Dependency-ordered startup stages:
#   Stage 1: app (38001) — all downstream depend on app
#   Stage 2: writeback_a (8092) + writeback_b (8095)
#   Stage 3: mesh_runner (8093) + promote_prep (8094)
#   Stage 4: loop_controller (8096) — depends on all above

$serviceMap = @{}
foreach ($svc in $services) {
    $serviceMap[$svc.Name] = $svc
}

$stages = @(
    @{ Label = "Stage 1: Core Application"; Names = @("app") },
    @{ Label = "Stage 2: Writeback Services"; Names = @("writeback_a", "writeback_b") },
    @{ Label = "Stage 3: Mesh Runner + Promote Prep"; Names = @("mesh_runner", "promote_prep") },
    @{ Label = "Stage 4: Loop Controller"; Names = @("loop_controller") }
)

$allHealthy = $true
$stageFailure = $false

foreach ($stage in $stages) {
    Write-Host "`n--- $($stage.Label) ---"
    if ($stageFailure) {
        Write-Warning "  Skipping $($stage.Label) due to prior stage failure."
        $allHealthy = $false
        continue
    }

    $stageHealthy = $true
    foreach ($name in $stage.Names) {
        $svc = $serviceMap[$name]
        Start-Service -Name $svc.Name -Port $svc.Port -Module $svc.Module -ServiceEnv $svc.Env | Out-Null
    }
    # Wait for all services in this stage to become healthy
    foreach ($name in $stage.Names) {
        $svc = $serviceMap[$name]
        $healthWaitSeconds = 30
        if ($svc.ContainsKey("HealthWaitSeconds")) {
            $healthWaitSeconds = [int]$svc.HealthWaitSeconds
        }
        $healthy = Wait-ForHealth -Name $svc.Name -Port $svc.Port -MaxWaitSeconds $healthWaitSeconds
        if (-not $healthy) {
            $stageHealthy = $false
            $allHealthy = $false
        }
    }
    if (-not $stageHealthy) {
        Write-Warning "  $($stage.Label) unhealthy — downstream stages will be skipped."
        $stageFailure = $true
    }
}

Write-Host "`n=========================================="
if ($allHealthy) {
    Write-Host " All 6 services started and healthy."
} elseif ($stageFailure) {
    Write-Warning " Stage failure detected. Check logs in: $logDir"
    Write-Warning " Fix the failing service and re-run this script."
} else {
    Write-Warning " Some services failed health checks. Check logs in: $logDir"
}
Write-Host "=========================================="
