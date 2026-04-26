param()

$ErrorActionPreference = "Stop"

$profilePath = $PROFILE.CurrentUserAllHosts
$profileDir = Split-Path -Parent $profilePath
$markerStart = "# >>> YANBAO_GET_DATE_ASUTC_COMPAT >>>"
$markerEnd = "# <<< YANBAO_GET_DATE_ASUTC_COMPAT <<<"
$shim = @'
# >>> YANBAO_GET_DATE_ASUTC_COMPAT >>>
if ($PSVersionTable.PSVersion.Major -lt 7) {
    function global:Get-Date {
        param([Parameter(ValueFromRemainingArguments = $true)] [object[]]$RemainingArgs)

        $items = @($RemainingArgs)
        $asUtc = $false
        $format = $null
        $passthrough = New-Object System.Collections.Generic.List[object]

        for ($i = 0; $i -lt $items.Count; $i++) {
            $itemText = [string]$items[$i]
            if ($itemText -eq "-AsUTC") {
                $asUtc = $true
                continue
            }
            if ($itemText -eq "-Format" -and ($i + 1) -lt $items.Count) {
                $format = [string]$items[$i + 1]
                $i++
                continue
            }
            $passthrough.Add($items[$i])
        }

        if (-not $asUtc) {
            $forwardArgs = $passthrough.ToArray()
            if ($null -ne $format) {
                $passthrough.Add("-Format")
                $passthrough.Add($format)
                $forwardArgs = $passthrough.ToArray()
            }
            if ($forwardArgs.Count -gt 0) {
                return & Microsoft.PowerShell.Utility\Get-Date @forwardArgs
            }
            return Microsoft.PowerShell.Utility\Get-Date
        }

        $forwardArgs = $passthrough.ToArray()
        if ($forwardArgs.Count -gt 0) {
            $date = & Microsoft.PowerShell.Utility\Get-Date @forwardArgs
        } else {
            $date = Microsoft.PowerShell.Utility\Get-Date
        }
        if ($date -isnot [datetime]) {
            $date = [datetime]::Parse([string]$date)
        }
        $utcDate = $date.ToUniversalTime()
        if ($null -ne $format) {
            return $utcDate.ToString($format)
        }
        return $utcDate
    }
}
# <<< YANBAO_GET_DATE_ASUTC_COMPAT <<<
'@

if (-not (Test-Path $profileDir)) {
    New-Item -ItemType Directory -Path $profileDir -Force | Out-Null
}

$existing = ""
if (Test-Path $profilePath) {
    $existing = Get-Content -Path $profilePath -Raw -Encoding utf8
}

$pattern = "(?s)" + [regex]::Escape($markerStart) + ".*?" + [regex]::Escape($markerEnd)
if ($existing -match $pattern) {
    $updated = [regex]::Replace($existing, $pattern, [System.Text.RegularExpressions.MatchEvaluator]{ param($m) $shim.TrimEnd("`r", "`n") })
    Set-Content -Path $profilePath -Value ($updated + "`r`n") -Encoding utf8
    Write-Host "[compat] PowerShell Get-Date -AsUTC shim updated: $profilePath"
    return
}

if ($existing -and -not $existing.EndsWith("`n")) {
    $existing += "`r`n"
}
$updated = $existing + $shim + "`r`n"
Set-Content -Path $profilePath -Value $updated -Encoding utf8
Write-Host "[compat] Installed PowerShell Get-Date -AsUTC shim: $profilePath"
