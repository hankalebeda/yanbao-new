@echo off
setlocal

set "ADAPTER_STARTER=C:\Users\Administrator\Desktop\AI\claude\start-claude-local-adapter.ps1"
set "CLAUDE_EXE=C:\Users\Administrator\.local\bin\claude.exe"

if not exist "%CLAUDE_EXE%" (
  echo [claude-wrapper] Claude executable not found: %CLAUDE_EXE% 1>&2
  exit /b 1
)

for /f %%I in ('powershell -NoProfile -Command "(Test-NetConnection -ComputerName 127.0.0.1 -Port 9800 -WarningAction SilentlyContinue).TcpTestSucceeded"') do set "ADAPTER_UP=%%I"

if /I not "%ADAPTER_UP%"=="True" (
  if not exist "%ADAPTER_STARTER%" (
    echo [claude-wrapper] Adapter starter not found: %ADAPTER_STARTER% 1>&2
    exit /b 1
  )
  powershell -NoProfile -ExecutionPolicy Bypass -File "%ADAPTER_STARTER%"
)

"%CLAUDE_EXE%" %*
exit /b %ERRORLEVEL%
