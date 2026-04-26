# Start uvicorn with full env for Phase 2-3 work
$ErrorActionPreference = "Stop"

# LLM providers come from ai-api/codex-active/ (CLIProxyAPI et al.)
# NewAPI relays are dead as of 2026-04-17; explicit env overrides removed so
# provider discovery path wins. If needed, set CODEX_API_* to force a fallback.

# Internal API token for cron/batch endpoints
$env:INTERNAL_TOKEN = "phase1-audit-token-20260417"
$env:INTERNAL_CRON_TOKEN = "phase1-audit-token-20260417"

# Ensure no proxy
$env:NO_PROXY = "*"
$env:no_proxy = "*"

# Make sure output dir exists
New-Item -ItemType Directory -Path "runtime" -Force | Out-Null

Write-Host "Starting uvicorn with INTERNAL_TOKEN + NewAPI..."
Start-Process -NoNewWindow -FilePath "D:\yanbao-new\.venv\Scripts\python.exe" `
  -ArgumentList "-m","uvicorn","app.main:app","--host","127.0.0.1","--port","8000","--log-level","warning" `
  -RedirectStandardOutput "runtime\uvicorn.out.log" `
  -RedirectStandardError "runtime\uvicorn.err.log"

Start-Sleep -Seconds 6
$listening = netstat -ano | findstr "LISTENING" | findstr ":8000"
if ($listening) {
    Write-Host "OK: $listening"
} else {
    Write-Host "FAIL to start. Check runtime\uvicorn.err.log:"
    Get-Content runtime\uvicorn.err.log -Tail 20
}
