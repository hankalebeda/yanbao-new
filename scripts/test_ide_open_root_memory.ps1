param(
  [ValidateSet("cursor", "code", "antigravity")]
  [string]$Ide = "cursor",
  [string]$Folder = (Resolve-Path ".").Path,
  [int]$WarmupSeconds = 20,
  [int]$IntervalSeconds = 5,
  [int]$Samples = 24,
  [switch]$DisableExtensions,
  [switch]$UseSystemExtensions
)

$ErrorActionPreference = "Stop"

function Format-MB([long]$bytes) {
  if (-not $bytes) { return 0 }
  return [math]::Round(($bytes / 1MB), 1)
}

function Get-ExeCmd {
  param([string]$name)
  if ($name -eq "code") {
    $candidate = Join-Path $env:LocalAppData "Programs\\Microsoft VS Code\\Code.exe"
    if (Test-Path $candidate) { return $candidate }
  }

  if ($name -eq "antigravity") {
    $candidate = Join-Path $env:LocalAppData "Programs\\Antigravity\\bin\\antigravity.cmd"
    if (Test-Path $candidate) { return $candidate }
  }

  $cmd = Get-Command $name -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  throw "Cannot find '$name' on PATH."
}

function Get-DescendantPids {
  param([int]$RootPid)
  $all = Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId
  $childrenMap = @{}
  foreach ($p in $all) {
    if (-not $childrenMap.ContainsKey($p.ParentProcessId)) { $childrenMap[$p.ParentProcessId] = @() }
    $childrenMap[$p.ParentProcessId] += [int]$p.ProcessId
  }

  $result = New-Object System.Collections.Generic.List[int]
  $stack = New-Object System.Collections.Generic.Stack[int]
  $stack.Push($RootPid)
  while ($stack.Count -gt 0) {
    $procId = $stack.Pop()
    if ($result.Contains($procId)) { continue }
    $result.Add($procId) | Out-Null
    if ($childrenMap.ContainsKey($procId)) {
      foreach ($c in $childrenMap[$procId]) { $stack.Push($c) }
    }
  }
  return $result
}

function Measure-TreeMemory {
  param([int]$RootPid)
  $pids = Get-DescendantPids -RootPid $RootPid
  $procs = @()
  foreach ($procId in $pids) {
    $p = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if ($p) { $procs += $p }
  }
  $ws = ($procs | Measure-Object -Sum WorkingSet64).Sum
  $pm = ($procs | Measure-Object -Sum PrivateMemorySize64).Sum
  return [PSCustomObject]@{
    Pids = $pids.Count
    WorkingSetMB = Format-MB $ws
    PrivateMB = Format-MB $pm
  }
}

function Count-IncludedFiles {
  param([string]$root)
  # Avoid non-ASCII literals for Windows PowerShell 5.1 encoding quirks.
  $dir_tdx = ([char]0x901A) + ([char]0x8FBE) + ([char]0x4FE1)                 # 通达信
  $dir_history_plan = ([char]0x5386) + ([char]0x53F2) + ([char]0x65B9) + ([char]0x6848) # 历史方案
  $dir_history_data = ([char]0x5386) + ([char]0x53F2) + ([char]0x6570) + ([char]0x636E) # 历史数据

  $excludeDirNames = @(
    "data",
    "runtime",
    $dir_tdx,
    $dir_history_plan,
    $dir_history_data,
    ".venv",
    "venv",
    "env",
    "node_modules",
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".playwright-cli",
    "test-results"
  )
  $excluded = New-Object System.Collections.Generic.HashSet[string]
  foreach ($n in $excludeDirNames) { $excluded.Add($n) | Out-Null }

  $count = 0
  $stack = New-Object System.Collections.Generic.Stack[string]
  $stack.Push($root)
  while ($stack.Count -gt 0) {
    $dir = $stack.Pop()
    try {
      foreach ($sub in Get-ChildItem -LiteralPath $dir -Directory -Force -ErrorAction Stop) {
        if (-not $excluded.Contains($sub.Name)) { $stack.Push($sub.FullName) }
      }
      $count += (Get-ChildItem -LiteralPath $dir -File -Force -ErrorAction Stop).Count
    } catch {
      continue
    }
  }
  return $count
}

$ideCmd = Get-ExeCmd $Ide
$folderPath = (Resolve-Path $Folder).Path
$launchTime = Get-Date

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$tmpRoot = Join-Path $folderPath (Join-Path "runtime" (Join-Path "_ide_mem_test" $timestamp))
New-Item -ItemType Directory -Force -Path $tmpRoot | Out-Null
$userDataDir = Join-Path $tmpRoot "user-data"
$extDir = Join-Path $tmpRoot "extensions"
New-Item -ItemType Directory -Force -Path $userDataDir | Out-Null
New-Item -ItemType Directory -Force -Path $extDir | Out-Null

Write-Host "IDE: $Ide"
Write-Host "Folder: $folderPath"
Write-Host "Temp: $tmpRoot"
Write-Host "Included files (approx, excludes big dirs): $(Count-IncludedFiles -root $folderPath)"
Write-Host ""

$args = @("--new-window", "--user-data-dir", $userDataDir, "--extensions-dir", $extDir)
if ($DisableExtensions) { $args += "--disable-extensions" }
$args += $folderPath

if ($UseSystemExtensions) {
  if ($Ide -eq "cursor") {
    $sysExt = Join-Path $env:USERPROFILE ".cursor\\extensions"
    if (Test-Path $sysExt) { $args[($args.IndexOf("--extensions-dir") + 1)] = $sysExt }
  } else {
    $sysExt = Join-Path $env:USERPROFILE ".vscode\\extensions"
    if (Test-Path $sysExt) { $args[($args.IndexOf("--extensions-dir") + 1)] = $sysExt }
  }
}

Write-Host "Launching: $ideCmd $($args -join ' ')"
$p = Start-Process -FilePath $ideCmd -ArgumentList $args -PassThru
if ($p -is [array]) { $p = $p[0] }
$launcherPid = [int]$p.Id

Write-Host "Launcher PID: $launcherPid. Warmup ${WarmupSeconds}s..."
Start-Sleep -Seconds $WarmupSeconds

$exeName = if ($Ide -eq "cursor") { "Cursor.exe" } elseif ($Ide -eq "code") { "Code.exe" } else { "Antigravity.exe" }
$uniqueToken = $timestamp
$rootPid = $null

# If we launched the real .exe, it's already the app root.
if ($ideCmd.ToLower().EndsWith(".exe")) {
  $proc = Get-Process -Id $launcherPid -ErrorAction SilentlyContinue
  if ($proc -and ($proc.ProcessName + ".exe") -ieq $exeName) {
    $rootPid = $launcherPid
  }
}

# Otherwise (cmd wrapper), find the real process via unique token in CommandLine.
if (-not $rootPid) {
  for ($t = 0; $t -lt 20; $t++) {
    $candidates = Get-CimInstance Win32_Process | Where-Object {
      $_.Name -eq $exeName -and $_.CommandLine -and ($_.CommandLine -like "*$uniqueToken*")
    }
    if ($candidates) {
      $rootPid = [int]($candidates | Select-Object -First 1).ProcessId
      break
    }
    Start-Sleep -Seconds 1
  }
}

if (-not $rootPid) {
  # Fallback: pick a newly started process by StartTime.
  $nameNoExt = [IO.Path]::GetFileNameWithoutExtension($exeName)
  $new = Get-Process -Name $nameNoExt -ErrorAction SilentlyContinue | Where-Object { $_.StartTime -ge $launchTime.AddSeconds(-2) } | Sort-Object StartTime
  if ($new) { $rootPid = [int]$new[0].Id }
}

if (-not $rootPid) {
  throw "Failed to find app root for $Ide ($exeName)."
}
Write-Host "App Root PID: $rootPid"

$sampleRows = @()
for ($i = 1; $i -le $Samples; $i++) {
  $m = Measure-TreeMemory -RootPid $rootPid
  $row = [PSCustomObject]@{
    T = (Get-Date).ToString("HH:mm:ss")
    Sample = $i
    Pids = $m.Pids
    WorkingSetMB = $m.WorkingSetMB
    PrivateMB = $m.PrivateMB
  }
  $sampleRows += $row
  $row | Format-Table -AutoSize
  Start-Sleep -Seconds $IntervalSeconds
}

$maxWs = ($sampleRows | Measure-Object -Maximum WorkingSetMB).Maximum
$maxPm = ($sampleRows | Measure-Object -Maximum PrivateMB).Maximum
$last = $sampleRows[-1]

Write-Host ""
Write-Host "Summary: max WorkingSetMB=$maxWs, max PrivateMB=$maxPm; last WorkingSetMB=$($last.WorkingSetMB), last PrivateMB=$($last.PrivateMB)"

Write-Host "Stopping process tree..."
$pids = Get-DescendantPids -RootPid $rootPid | Sort-Object -Descending
foreach ($procId in $pids) {
  try { Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue } catch {}
}

Write-Host "Done."
