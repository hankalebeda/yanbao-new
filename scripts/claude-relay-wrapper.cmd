@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "KEY_FILE=%PROJECT_ROOT%\ai-api\claude\key0.txt"
if not exist "%KEY_FILE%" set "KEY_FILE=%PROJECT_ROOT%\ai-api\claude\key1.txt"
if not exist "%KEY_FILE%" set "KEY_FILE=%PROJECT_ROOT%\ai-api\claude\key.txt"
set "PROXY_URL=http://127.0.0.1:10808"

if not exist "%KEY_FILE%" (
  echo [claude-relay-wrapper] key file not found: %KEY_FILE% 1>&2
  exit /b 1
)

set "ANTHROPIC_BASE_URL="
set "ANTHROPIC_API_KEY="
set "ANTHROPIC_AUTH_TOKEN="

for /f "usebackq delims=" %%L in ("%KEY_FILE%") do (
  if not defined ANTHROPIC_BASE_URL (
    set "ANTHROPIC_BASE_URL=%%L"
  ) else if not defined ANTHROPIC_API_KEY (
    set "ANTHROPIC_API_KEY=%%L"
  )
)

if not defined ANTHROPIC_BASE_URL (
  echo [claude-relay-wrapper] missing base URL in %KEY_FILE% 1>&2
  exit /b 1
)

if not defined ANTHROPIC_API_KEY (
  echo [claude-relay-wrapper] missing API key in %KEY_FILE% 1>&2
  exit /b 1
)

echo %ANTHROPIC_BASE_URL% | find /I "anyrouter.top" >nul
if %ERRORLEVEL%==0 (
  set "ANTHROPIC_AUTH_TOKEN=%ANTHROPIC_API_KEY%"
  set "ANTHROPIC_API_KEY="
) else (
  set "ANTHROPIC_AUTH_TOKEN="
)

set "HTTP_PROXY=%PROXY_URL%"
set "HTTPS_PROXY=%PROXY_URL%"
set "http_proxy=%PROXY_URL%"
set "https_proxy=%PROXY_URL%"

set "CLAUDE_TARGET=%~1"
if not defined CLAUDE_TARGET (
  set "CLAUDE_TARGET=claude"
) else (
  shift
)

"%CLAUDE_TARGET%" %*
exit /b %ERRORLEVEL%
