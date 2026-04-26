param(
  [switch]$IncludeHardwareSensors
)

$ErrorActionPreference = "Stop"

function Assert-Cmd($name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Missing required command: $name"
  }
}

Assert-Cmd winget

$packages = @(
  # Sysinternals: extremely useful, low overhead, and mostly runs only when you open it.
  @{ Id = "Microsoft.Sysinternals.ProcessExplorer"; Name = "Process Explorer" },
  @{ Id = "Microsoft.Sysinternals.ProcessMonitor"; Name = "Process Monitor" },
  @{ Id = "Microsoft.Sysinternals.Autoruns"; Name = "Autoruns" },
  @{ Id = "Microsoft.Sysinternals.RAMMap"; Name = "RAMMap" }
)

if ($IncludeHardwareSensors) {
  # Optional: lightweight hardware sensor reading (CPU/GPU temps, etc). Can run in tray.
  $packages += @{ Id = "LibreHardwareMonitor.LibreHardwareMonitor"; Name = "Libre Hardware Monitor" }
}

Write-Host "Installing packages via winget..."
foreach ($p in $packages) {
  Write-Host ("- {0} ({1})" -f $p.Name, $p.Id)
  winget install --id $p.Id --exact --silent --accept-package-agreements --accept-source-agreements
}

Write-Host ""
Write-Host "Done. Tip: Sysinternals tools are on-demand; they won't consume resources unless opened."

