param(
    [ValidateSet('claude', 'amp')]
    [string]$Tool = 'claude',

    [ValidateRange(0, 2147483647)]
    [int]$MaxIterations = 0,

    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

$ralphRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent (Split-Path -Parent $ralphRoot)
$loopDir = Join-Path $ralphRoot 'loop'
$bashScript = Join-Path $loopDir 'ralph.sh'
$jqPath = Join-Path $ralphRoot 'bin\jq.exe'
$runtimePrd = Join-Path $loopDir 'prd.json'
$permissionRepairScript = Join-Path $repoRoot 'scripts\repair_windows_permissions.ps1'

if (Test-Path $permissionRepairScript) {
    try {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $permissionRepairScript -Quiet | Out-Null
    }
    catch {
        Write-Warning "Best-effort Windows permission repair failed: $($_.Exception.Message)"
    }
}

if (-not (Test-Path $bashScript)) {
    throw "Missing Ralph loop script: $bashScript"
}

if (-not (Test-Path $runtimePrd)) {
    throw "Missing runtime PRD JSON: $runtimePrd"
}

$gitPath = (Get-Command git -ErrorAction Stop).Source
$gitRoot = Split-Path -Parent (Split-Path -Parent $gitPath)
$bashCandidates = @(
    (Join-Path $gitRoot 'bin\bash.exe'),
    (Join-Path $gitRoot 'usr\bin\bash.exe')
)
$bashPath = $bashCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $bashPath) {
    throw "Git Bash not found under: $gitRoot"
}

if (-not (Test-Path $jqPath)) {
    throw "Missing local jq.exe at: $jqPath"
}

$env:PATH = "$(Split-Path -Parent $jqPath);$env:PATH"

if ($DryRun) {
    Write-Output "Tool=$Tool"
    if ($MaxIterations -eq 0) {
        Write-Output "MaxIterations=until-complete (0)"
    }
    else {
        Write-Output "MaxIterations=$MaxIterations"
    }
    Write-Output "GitPath=$gitPath"
    Write-Output "BashPath=$bashPath"
    Write-Output "JqPath=$jqPath"
    Write-Output "LoopDir=$loopDir"
    Write-Output "RuntimePrd=$runtimePrd"
    exit 0
}

Push-Location $loopDir
try {
    & $bashPath './ralph.sh' '--tool' $Tool $MaxIterations
}
finally {
    Pop-Location
}
