param(
  [int]$IntervalSeconds = 2,
  [int]$Samples = 60
)

$ErrorActionPreference = "SilentlyContinue"

function Get-ProcLike($name) {
  Get-Process | Where-Object { $_.ProcessName -like $name }
}

function Format-MB($bytes) {
  [math]::Round(($bytes / 1MB), 1)
}

Write-Host "Sampling Cursor/VSCode memory: interval=${IntervalSeconds}s samples=${Samples}"
Write-Host "Tip: open the repo root in Cursor first, then run this script."
Write-Host ""

$names = @("Cursor*", "Code*", "Electron*", "node*")

for ($i = 1; $i -le $Samples; $i++) {
  $procs = foreach ($n in $names) { Get-ProcLike $n } | Sort-Object -Property ProcessName,Id -Unique
  $cursor = $procs | Where-Object { $_.ProcessName -like "Cursor*" -or $_.Path -match "Cursor" }
  $code = $procs | Where-Object { $_.ProcessName -like "Code*" -or $_.Path -match "Code" }

  $target = @()
  if ($cursor) { $target += $cursor }
  elseif ($code) { $target += $code }

  $sumWorking = ($target | Measure-Object -Sum WorkingSet64).Sum
  $sumPrivate = ($target | Measure-Object -Sum PrivateMemorySize64).Sum

  $row = [PSCustomObject]@{
    Time = (Get-Date).ToString("HH:mm:ss")
    TargetCount = $target.Count
    WorkingSetMB = Format-MB $sumWorking
    PrivateMB = Format-MB $sumPrivate
  }

  $row | Format-Table -AutoSize
  Start-Sleep -Seconds $IntervalSeconds
}

