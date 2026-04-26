function Resolve-CodexIdeTarget {
    param(
        [ValidateSet("auto", "code", "cursor")]
        [string]$Ide = "code"
    )

    $codeExe = $null
    $codeCmdPath = $null
    $cursorCmd = $null
    if ($env:LOCALAPPDATA) {
        $codeExe = Join-Path $env:LOCALAPPDATA "Programs\Microsoft VS Code\Code.exe"
        $codeCmdPath = Join-Path $env:LOCALAPPDATA "Programs\Microsoft VS Code\bin\code.cmd"
        $cursorCmd = Join-Path $env:LOCALAPPDATA "Programs\cursor\resources\app\bin\cursor.cmd"
    }

    $codeCmd = Get-Command code -ErrorAction SilentlyContinue
    $codeCmdShell = Get-Command code.cmd -ErrorAction SilentlyContinue
    $cursorCommand = Get-Command cursor -ErrorAction SilentlyContinue
    $ideTarget = $null

    switch ($Ide) {
        "code" {
            if ($codeCmdPath -and (Test-Path $codeCmdPath)) {
                $ideTarget = $codeCmdPath
            } elseif ($codeCmdShell) {
                $ideTarget = $codeCmdShell.Source
            } elseif ($codeCmd -and $codeCmd.Source -like "*.cmd") {
                $ideTarget = $codeCmd.Source
            } elseif ($codeExe -and (Test-Path $codeExe)) {
                $ideTarget = $codeExe
            } elseif ($codeCmd) {
                $ideTarget = $codeCmd.Source
            }
        }
        "cursor" {
            if ($cursorCmd -and (Test-Path $cursorCmd)) {
                $ideTarget = $cursorCmd
            } elseif ($cursorCommand) {
                $ideTarget = $cursorCommand.Source
            }
        }
        default {
            if ($codeCmdPath -and (Test-Path $codeCmdPath)) {
                $ideTarget = $codeCmdPath
            } elseif ($codeCmdShell) {
                $ideTarget = $codeCmdShell.Source
            } elseif ($codeCmd -and $codeCmd.Source -like "*.cmd") {
                $ideTarget = $codeCmd.Source
            } elseif ($codeExe -and (Test-Path $codeExe)) {
                $ideTarget = $codeExe
            } elseif ($codeCmd) {
                $ideTarget = $codeCmd.Source
            } elseif ($cursorCmd -and (Test-Path $cursorCmd)) {
                $ideTarget = $cursorCmd
            } elseif ($cursorCommand) {
                $ideTarget = $cursorCommand.Source
            }
        }
    }

    return $ideTarget
}

function Start-CodexWorkspace {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProjectRoot,

        [Parameter(Mandatory = $true)]
        [string]$CodexHome,

        [Parameter(Mandatory = $true)]
        [string]$UserDataDir,

        [Parameter(Mandatory = $true)]
        [string]$LaunchLogPath,

        [ValidateSet("auto", "code", "cursor")]
        [string]$Ide = "code",

        [switch]$NoLaunch,

        [switch]$PrintEnv,

        [string[]]$ExtraLaunchInfo = @(),

        [string[]]$ExtraPrintLines = @()
    )

    $env:CODEX_HOME = $CodexHome

    if (Test-Path Env:OPENAI_API_KEY) {
        Remove-Item Env:OPENAI_API_KEY
    }

    if (Test-Path Env:OPENAI_BASE_URL) {
        Remove-Item Env:OPENAI_BASE_URL
    }

    New-Item -ItemType Directory -Force -Path $UserDataDir | Out-Null

    if ($PrintEnv) {
        Write-Host "CODEX_HOME=$env:CODEX_HOME"
        foreach ($line in $ExtraPrintLines) {
            Write-Host $line
        }
        Write-Host "OPENAI_API_KEY_SET=0"
        Write-Host "OPENAI_BASE_URL_SET=0"
        Write-Host "PROXY_STRATEGY=VSCode"
        Write-Host "USER_DATA_DIR=$UserDataDir"
    }

    if ($NoLaunch) {
        $launchInfo = @(
            "timestamp=$((Get-Date).ToString('s'))"
            "projectRoot=$ProjectRoot"
            "userDataDir=$UserDataDir"
            "CODEX_HOME=$env:CODEX_HOME"
        ) + $ExtraLaunchInfo + @(
            "OPENAI_API_KEY_SET=0"
            "OPENAI_BASE_URL_SET=0"
            "proxyStrategy=vscode"
            "ide=$Ide"
        )
        Set-Content -Path $LaunchLogPath -Value $launchInfo -Encoding ASCII
        return
    }

    $ideTarget = Resolve-CodexIdeTarget -Ide $Ide
    if (-not $ideTarget) {
        throw "VS Code or Cursor was not found. Install VS Code or ensure code is available in PATH."
    }

    Start-Process -FilePath $ideTarget -ArgumentList @(
        "--new-window",
        "--user-data-dir",
        $UserDataDir,
        $ProjectRoot
    ) -WorkingDirectory $ProjectRoot

    $launchInfo = @(
        "timestamp=$((Get-Date).ToString('s'))"
        "projectRoot=$ProjectRoot"
        "userDataDir=$UserDataDir"
        "CODEX_HOME=$env:CODEX_HOME"
    ) + $ExtraLaunchInfo + @(
        "OPENAI_API_KEY_SET=0"
        "OPENAI_BASE_URL_SET=0"
        "proxyStrategy=vscode"
        "ide=$Ide"
    )
    Set-Content -Path $LaunchLogPath -Value $launchInfo -Encoding ASCII
}
