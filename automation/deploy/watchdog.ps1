<#
.SYNOPSIS
  Watchdog for the Windows-hosted Yanbao autonomy stack.
.DESCRIPTION
  Monitors app(38001) plus writeback A/B, mesh_runner, promote_prep, and
  loop_controller. When a service fails health checks repeatedly, watchdog
  restarts it with the same canonical env contract as start-all-services.ps1.
#>
param(
    [string]$RepoRoot = "",
    [int]$PollIntervalSeconds = 30,
    [int]$MaxFailures = 3
)

$ErrorActionPreference = "Continue"

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
}

$pidDir = Join-Path $RepoRoot "runtime\services"
$logDir = Join-Path $RepoRoot "runtime\services\logs"
$watchdogLog = Join-Path $RepoRoot "runtime\services\watchdog.log"

$services = @(
    @{ Name = "app"; Port = 38001 },
    @{ Name = "writeback_a"; Port = 8092 },
    @{ Name = "writeback_b"; Port = 8095 },
    @{ Name = "mesh_runner"; Port = 8093 },
    @{ Name = "promote_prep"; Port = 8094 },
    @{ Name = "loop_controller"; Port = 8096 }
)

$failureCounts = @{}
foreach ($svc in $services) {
    $failureCounts[$svc.Name] = 0
}

function Write-Log {
    param([string]$Message)

    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Message"
    Add-Content -Path $watchdogLog -Value $line -ErrorAction SilentlyContinue
    Write-Host $line
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

$LOCAL_NO_PROXY = Get-LocalNoProxy
$env:NO_PROXY = $LOCAL_NO_PROXY
$env:no_proxy = $LOCAL_NO_PROXY

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
    # Fallback: build from Unicode code points
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

function Rotate-Logs {
    $cutoff = (Get-Date).AddDays(-7)
    Get-ChildItem -Path $logDir -File -ErrorAction SilentlyContinue | Where-Object {
        $_.LastWriteTime -lt $cutoff
    } | ForEach-Object {
        Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
    }
    if ((Test-Path $watchdogLog) -and (Get-Item $watchdogLog).Length -gt 10MB) {
        $tail = Get-Content $watchdogLog -Tail 2000
        Set-Content -Path $watchdogLog -Value $tail -Force
        Write-Log "watchdog.log truncated to last 2000 lines"
    }
}

function Test-ServiceHealth {
    param([string]$Name, [int]$Port)

    try {
        Invoke-RestMethod -Method GET -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 5 -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Get-ServicePid {
    param([string]$Name)

    $pidFile = Join-Path $pidDir "$Name.pid"
    if (Test-Path $pidFile) {
        $pid = Get-Content $pidFile -ErrorAction SilentlyContinue
        if ($pid) {
            return [int]$pid
        }
    }
    return $null
}

function New-ServiceLauncher {
    param(
        [string]$Name,
        [int]$Port,
        [string]$Module,
        [hashtable]$ServiceEnv,
        [string]$PythonPath
    )

    $launcherPath = Join-Path $pidDir "start_$Name.cmd"
    $lines = @("@echo off", "set PYTHONPATH=$RepoRoot")
    foreach ($entry in $ServiceEnv.GetEnumerator() | Sort-Object Name) {
        $escapedValue = [string]$entry.Value -replace '"', '""'
        $lines += "set $($entry.Key)=$escapedValue"
    }
    $lines += "cd /d $RepoRoot"
    $lines += """$PythonPath"" -m uvicorn $Module --host 0.0.0.0 --port $Port --log-level info"
    Set-Content -Path $launcherPath -Value $lines -Encoding ASCII -Force
    return $launcherPath
}

function Stop-ServiceProcesses {
    param(
        [string]$Name,
        [int]$Port,
        [string]$Module
    )

    $launcherName = "start_$Name.cmd"
    $matches = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $cmd = [string]$_.CommandLine
        $cmd -and (
            $cmd -like "*uvicorn $Module*--port $Port*" -or
            $cmd -like "*$launcherName*"
        )
    }
    foreach ($match in $matches) {
        Stop-Process -Id $match.ProcessId -Force -ErrorAction SilentlyContinue
    }

    if ($Name -eq "app") {
        Remove-Item (Join-Path $RepoRoot "runtime\loop_controller\runtime_lease.json") -Force -ErrorAction SilentlyContinue
    }
}

function Restart-FailedService {
    param([string]$Name, [int]$Port)

    Write-Log "RESTARTING $Name on port $Port"

    $pid = Get-ServicePid -Name $Name
    if ($pid) {
        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 500
    }
    Start-Sleep -Seconds 1

    $envFile = Join-Path $RepoRoot "automation\deploy\.env"
    if (-not (Test-Path $envFile)) {
        $envFile = Join-Path $RepoRoot "automation\deploy\.env.example"
    }

    $envVars = @{}
    if (Test-Path $envFile) {
        Get-Content $envFile | ForEach-Object {
            $line = $_.Trim()
            if ($line -and -not $line.StartsWith("#")) {
                $parts = $line -split "=", 2
                if ($parts.Length -eq 2) {
                    $envVars[$parts[0].Trim()] = $parts[1].Trim()
                }
            }
        }
    }

    function Get-EnvVal {
        param([string]$K, [string]$D = "")
        if ($envVars.ContainsKey($K)) {
            return (Normalize-EnvValue $envVars[$K])
        }
        $v = [System.Environment]::GetEnvironmentVariable($K)
        if ($v) {
            return $v
        }
        return $D
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

    function Get-RepoEnvVal {
        param([string]$K, [string]$D = "")
        if ($repoEnvVars.ContainsKey($K)) {
            return (Normalize-EnvValue $repoEnvVars[$K])
        }
        return $D
    }

    $CONTROL_PLANE_TOKEN = Get-EnvVal "INTERNAL_TOKEN" "kestra-internal-20260327"
    $APP_INTERNAL_TOKEN = Get-RepoEnvVal "INTERNAL_CRON_TOKEN" (Get-RepoEnvVal "INTERNAL_API_KEY" $CONTROL_PLANE_TOKEN)
    $WRITEBACK_A_TOKEN = Get-EnvVal "WRITEBACK_A_TOKEN" $CONTROL_PLANE_TOKEN
    $WRITEBACK_B_TOKEN = Get-EnvVal "WRITEBACK_B_TOKEN" $CONTROL_PLANE_TOKEN
    $MESH_RUNNER_TOKEN = Get-EnvVal "MESH_RUNNER_TOKEN" $CONTROL_PLANE_TOKEN
    $PROMOTE_PREP_TOKEN = Get-EnvVal "PROMOTE_PREP_TOKEN" $CONTROL_PLANE_TOKEN
    $LOOP_CONTROLLER_TOKEN = Get-EnvVal "LOOP_CONTROLLER_TOKEN" $CONTROL_PLANE_TOKEN
    $NEW_API_TOKEN = Get-EnvVal "NEW_API_TOKEN" ""
    $NEW_API_BASE_URL = Get-EnvVal "NEW_API_BASE_URL" "http://192.168.232.141:3000"
    $AUDIT_GATEWAY_ONLY = Get-EnvVal "CODEX_AUDIT_GATEWAY_ONLY" "false"
    $CANONICAL_PROVIDER = Get-EnvVal "CODEX_CANONICAL_PROVIDER" "newapi-192.168.232.141-3000-stable"
    $READONLY_LANE = Get-EnvVal "CODEX_READONLY_LANE" "codex-readonly"
    $STABLE_LANE = Get-EnvVal "CODEX_STABLE_LANE" "codex-stable"
    $READONLY_PROVIDER_ALLOWLIST = Get-EnvVal "CODEX_READONLY_PROVIDER_ALLOWLIST" "newapi-192.168.232.141-3000-ro-a,newapi-192.168.232.141-3000-ro-b,newapi-192.168.232.141-3000-ro-c,newapi-192.168.232.141-3000-ro-d"
    $MAX_WORKERS = [string]([Math]::Max([int](Get-EnvVal "ISSUE_MESH_READONLY_MAX_WORKERS" "12"), 12))
    $MAX_WORKERS_CAP = [string]([Math]::Max([int](Get-EnvVal "ISSUE_MESH_MAX_WORKERS_CAP" $MAX_WORKERS), [int]$MAX_WORKERS))
    $FIX_GOAL = Get-EnvVal "FIX_GOAL_CONSECUTIVE" "10"
    $AUDIT_INTERVAL = Get-EnvVal "AUDIT_INTERVAL_SECONDS" "300"
    $MONITOR_INTERVAL = Get-EnvVal "MONITOR_INTERVAL_SECONDS" "1800"
    $AUTONOMY_LOOP_ENABLED = Get-EnvVal "AUTONOMY_LOOP_ENABLED" "true"
    $AUTONOMY_LOOP_MODE = Get-EnvVal "AUTONOMY_LOOP_MODE" "fix"
    $AUTONOMY_LOOP_FIX_GOAL = Get-EnvVal "AUTONOMY_LOOP_FIX_GOAL" $FIX_GOAL
    $AUTONOMY_LOOP_AUDIT_INTERVAL = Get-EnvVal "AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS" $AUDIT_INTERVAL
    $AUTONOMY_LOOP_MONITOR_INTERVAL = Get-EnvVal "AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS" $MONITOR_INTERVAL
    $INTERNAL_TOKEN_ALIASES = Get-EnvVal "INTERNAL_TOKEN_ALIASES" ""
    $LLM_BASE_URL = Get-EnvVal "PROMOTE_PREP_LLM_BASE_URL" ""
    $LLM_API_KEY = Get-EnvVal "PROMOTE_PREP_LLM_API_KEY" ""

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

    $writebackBPolicy = Get-WritebackBPolicy -RepoRoot $RepoRoot
    if ($writebackBPolicy.Reason) {
        Write-Log "writeback_b policy defaulted to infra because $($writebackBPolicy.Reason): $($writebackBPolicy.StatePath)"
    }
    Write-Log "writeback_b restart policy: promote_target_mode=$($writebackBPolicy.Mode); allow=$($writebackBPolicy.AllowPrefixes)"
    Write-Log "local NO_PROXY=$LOCAL_NO_PROXY"

    $svcDefs = @{
        "app" = @{
            Module = "app.main:app"
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
        }
        "writeback_a" = @{
            Module = "automation.writeback_service.app:app"
            Env = @{
                "WRITEBACK_AUTH_TOKEN" = $WRITEBACK_A_TOKEN
                "WRITEBACK_REQUIRE_TRIAGE" = "true"
                "WRITEBACK_REQUIRE_FENCING" = "false"
                "WRITEBACK_ALLOW_PREFIXES" = "app/,tests/"
                "WRITEBACK_DENY_PREFIXES" = "runtime/"
                "WRITEBACK_DENY_PATHS" = "$((Resolve-Doc22Target -RepoRoot $RepoRoot)),output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json"
                "WRITEBACK_REPO_ROOT" = $RepoRoot
            }
        }
        "writeback_b" = @{
            Module = "automation.writeback_service.app:app"
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
        }
        "mesh_runner" = @{
            Module = "automation.mesh_runner.app:app"
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
        }
        "promote_prep" = @{
            Module = "automation.promote_prep.app:app"
            Env = @{
                "PROMOTE_PREP_AUTH_TOKEN" = $PROMOTE_PREP_TOKEN
                "PROMOTE_PREP_NEW_API_BASE_URL" = $NEW_API_BASE_URL
                "PROMOTE_PREP_NEW_API_TOKEN" = $NEW_API_TOKEN
                "PROMOTE_PREP_LLM_BASE_URL" = $LLM_BASE_URL
                "PROMOTE_PREP_LLM_API_KEY" = $LLM_API_KEY
                "CODEX_CANONICAL_PROVIDER" = $CANONICAL_PROVIDER
            }
        }
        "loop_controller" = @{
            Module = "automation.loop_controller.app:app"
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
    }

    if (-not $svcDefs.ContainsKey($Name)) {
        Write-Log "ERROR unknown service name: $Name"
        return
    }

    $python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path $python)) {
        $python = "python"
    }

    $logFile = Join-Path $logDir "$Name.log"
    $errFile = "$logFile.err"
    $pidFile = Join-Path $pidDir "$Name.pid"
    $def = $svcDefs[$Name]
    Stop-ServiceProcesses -Name $Name -Port $Port -Module $def.Module
    $launcherPath = New-ServiceLauncher -Name $Name -Port $Port -Module $def.Module -ServiceEnv $def.Env -PythonPath $python

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
    Write-Log "$Name restarted (launcher PID $($proc.Id), service PID $resolvedPid) on port $Port"
}

# Dependency graph: services that depend on a given service
$dependents = @{
    "app" = @("writeback_a", "writeback_b", "mesh_runner", "promote_prep", "loop_controller")
    "writeback_a" = @("loop_controller")
    "writeback_b" = @("loop_controller")
    "mesh_runner" = @("loop_controller")
    "promote_prep" = @("loop_controller")
    "loop_controller" = @()
}

# Track total restart attempts per service (reset on manual intervention)
$restartAttempts = @{}
foreach ($svc in $services) {
    $restartAttempts[$svc.Name] = 0
}
$maxRestartAttempts = 3
$degradedServices = New-Object System.Collections.Generic.List[string]

function Write-DegradedState {
    param([string]$ServiceName, [string]$Reason)
    $stateFile = Join-Path $RepoRoot "automation\control_plane\service_health.json"
    $health = @{}
    if (Test-Path $stateFile) {
        try { $health = Get-Content $stateFile -Raw | ConvertFrom-Json -ErrorAction Stop } catch { $health = @{} }
    }
    $health | Add-Member -NotePropertyName $ServiceName -NotePropertyValue @{
        status = "degraded"
        reason = $Reason
        timestamp = (Get-Date -Format "o")
    } -Force
    $health | ConvertTo-Json -Depth 5 | Set-Content $stateFile -Force -ErrorAction SilentlyContinue
}

Rotate-Logs
Write-Log "watchdog started - monitoring $($services.Count) services every ${PollIntervalSeconds}s; NO_PROXY=$LOCAL_NO_PROXY"

while ($true) {
    $restartedThisCycle = New-Object System.Collections.Generic.List[string]

    foreach ($svc in $services) {
        # Skip degraded services
        if ($degradedServices.Contains($svc.Name)) {
            continue
        }

        $healthy = Test-ServiceHealth -Name $svc.Name -Port $svc.Port
        if ($healthy) {
            if ($failureCounts[$svc.Name] -gt 0) {
                Write-Log "$($svc.Name) recovered on port $($svc.Port)"
            }
            $failureCounts[$svc.Name] = 0
            continue
        }

        $failureCounts[$svc.Name] += 1
        Write-Log "$($svc.Name) unhealthy on port $($svc.Port) (failure $($failureCounts[$svc.Name])/$MaxFailures)"
        if ($failureCounts[$svc.Name] -ge $MaxFailures) {
            $restartAttempts[$svc.Name] += 1

            if ($restartAttempts[$svc.Name] -gt $maxRestartAttempts) {
                Write-Log "ESCALATION: $($svc.Name) exceeded $maxRestartAttempts restart attempts — marking DEGRADED"
                if (-not $degradedServices.Contains($svc.Name)) {
                    $degradedServices.Add($svc.Name)
                }
                Write-DegradedState -ServiceName $svc.Name -Reason "exceeded_max_restart_attempts"
                $failureCounts[$svc.Name] = 0
                continue
            }

            Write-Log "Restarting $($svc.Name) (attempt $($restartAttempts[$svc.Name])/$maxRestartAttempts)"
            Restart-FailedService -Name $svc.Name -Port $svc.Port
            $failureCounts[$svc.Name] = 0
            $restartedThisCycle.Add($svc.Name)

            # Cascade: restart dependent services
            if ($dependents.ContainsKey($svc.Name)) {
                foreach ($depName in $dependents[$svc.Name]) {
                    if (-not $restartedThisCycle.Contains($depName) -and -not $degradedServices.Contains($depName)) {
                        Write-Log "CASCADE: restarting $depName because $($svc.Name) was restarted"
                        $depSvc = $services | Where-Object { $_.Name -eq $depName } | Select-Object -First 1
                        if ($depSvc) {
                            Restart-FailedService -Name $depSvc.Name -Port $depSvc.Port
                            $restartedThisCycle.Add($depName)
                            $failureCounts[$depName] = 0
                        }
                    }
                }
            }
        }
    }
    Rotate-Logs
    Start-Sleep -Seconds $PollIntervalSeconds
}
