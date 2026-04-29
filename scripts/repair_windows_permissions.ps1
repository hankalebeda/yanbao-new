param(
    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

if (-not $IsWindows -and $PSVersionTable.PSVersion.Major -ge 6) {
    if (-not $Quiet) {
        Write-Output 'repair_windows_permissions: skipped on non-Windows host'
    }
    exit 0
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

function Write-RepairLog {
    param([string]$Message)
    if (-not $Quiet) {
        Write-Output $Message
    }
}

function Remove-ExplicitDeny {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $acl = Get-Acl -LiteralPath $Path
    $denyRules = @($acl.Access | Where-Object { $_.AccessControlType -eq 'Deny' -and -not $_.IsInherited })
    if ($denyRules.Count -eq 0) {
        return
    }

    foreach ($rule in $denyRules) {
        [void]$acl.RemoveAccessRuleSpecific($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
    Write-RepairLog "removed explicit DENY rules: $Path ($($denyRules.Count))"
}

function Grant-CurrentUserFullControl {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $grant = "${identity}:(OI)(CI)F"
    $result = & icacls $Path /inheritance:e /grant:r $grant /T /C 2>&1
    $exit = $LASTEXITCODE
    if ($exit -ne 0) {
        throw "icacls grant failed for ${Path}: exit=${exit}; $($result -join "`n")"
    }
    Write-RepairLog "granted full control to ${identity}: $Path"
}

$targets = @(
    (Join-Path $repoRoot '.git'),
    (Join-Path $repoRoot '.pytest_cache'),
    (Join-Path $repoRoot '_archive')
) | Where-Object { Test-Path -LiteralPath $_ }

foreach ($target in $targets) {
    Remove-ExplicitDeny -Path $target
    Grant-CurrentUserFullControl -Path $target
}

$gitDir = Join-Path $repoRoot '.git'
if (Test-Path -LiteralPath $gitDir) {
    $probe = Join-Path $gitDir 'codex_acl_probe.tmp'
    'ok' | Set-Content -LiteralPath $probe -Encoding UTF8
    Remove-Item -LiteralPath $probe -Force
    Write-RepairLog 'verified .git write/delete probe'
}

$rootProbe = Join-Path $repoRoot 'codex_tmp_probe.txt'
if (Test-Path -LiteralPath $rootProbe) {
    Remove-Item -LiteralPath $rootProbe -Force
    Write-RepairLog "removed probe: $rootProbe"
}

Write-RepairLog 'repair_windows_permissions: complete'
