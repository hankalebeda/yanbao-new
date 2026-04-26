param(
  [int]$IntervalSeconds = 2,
  [int]$DurationSeconds = 120,
  [string]$OutCsv = ""
)

$ErrorActionPreference = "Stop"

if (-not $OutCsv) {
  $ts = Get-Date -Format "yyyyMMdd_HHmmss"
  $OutCsv = Join-Path (Resolve-Path ".").Path ("runtime\\perf_counters_{0}.csv" -f $ts)
}

New-Item -ItemType Directory -Force -Path (Split-Path $OutCsv) | Out-Null

$counters = @(
  "\\Processor(_Total)\\% Processor Time",
  "\\Memory\\% Committed Bytes In Use",
  "\\Memory\\Available MBytes",
  "\\Paging File(_Total)\\% Usage",
  "\\PhysicalDisk(_Total)\\% Disk Time",
  "\\PhysicalDisk(_Total)\\Disk Bytes/sec",
  "\\Network Interface(*)\\Bytes Total/sec"
)

$samples = [math]::Max(1, [int]([math]::Ceiling($DurationSeconds / $IntervalSeconds)))

Write-Host "Collecting perf counters..."
Write-Host "IntervalSeconds=$IntervalSeconds DurationSeconds=$DurationSeconds Samples=$samples"
Write-Host "OutCsv=$OutCsv"
Write-Host ""

# typeperf is extremely low-overhead and writes CSV directly.
typeperf $counters -si $IntervalSeconds -sc $samples -f CSV -o $OutCsv | Out-Null

Write-Host "Done."

