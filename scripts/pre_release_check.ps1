# 发布前检查脚本（08 §8.1、核心文档一致性报告 P2）
# 串联：健康检查、回归测试、备份提示、检查清单输出
# 用法：.\scripts\pre_release_check.ps1 -BaseUrl "http://127.0.0.1:8010"

param(
    [string]$BaseUrl = "http://127.0.0.1:8010",
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$ROOT = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$LOG_DIR = Join-Path $ROOT "runtime" "logs"
$TIMESTAMP = Get-Date -Format "yyyyMMdd_HHmmss"

if (-not (Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null }

Write-Host "=== 发布前检查 $TIMESTAMP ===" -ForegroundColor Cyan
Write-Host "BaseUrl: $BaseUrl"
Write-Host ""

# 1. 健康检查
Write-Host "1. 健康检查..." -ForegroundColor Yellow
foreach ($path in @("/health", "/api/v1/internal/llm/health", "/api/v1/internal/metrics/summary")) {
    try {
        $r = Invoke-WebRequest -Uri "$BaseUrl$path" -UseBasicParsing -TimeoutSec 5
        if ($r.StatusCode -eq 200) { Write-Host "   [OK] $path 200" } else { Write-Host "   [WARN] $path $($r.StatusCode)" }
    } catch {
        if ($path -eq "/health") { Write-Host "   [FAIL] $path 不可达: $_"; exit 1 }
        Write-Host "   [WARN] $path 不可达: $_"
    }
}

# 2. 回归测试
if (-not $SkipTests) {
    Write-Host "2. 回归测试..." -ForegroundColor Yellow
    Push-Location $ROOT
    try {
        $out = python -m pytest tests/test_api.py tests/test_trade_calendar.py tests/test_e2e_sim.py -v 2>&1
        if ($LASTEXITCODE -ne 0) { Write-Host "   [FAIL] pytest 失败"; Write-Host $out; exit 1 }
        Write-Host "   [OK] pytest 通过"
    } finally { Pop-Location }
} else { Write-Host "2. 回归测试 跳过" }

# 3. 备份提示
Write-Host "3. 备份提示: 请执行 data/app.db 备份（见 08 §7.1）" -ForegroundColor Yellow

# 4. 检查清单输出
$checklistPath = Join-Path $LOG_DIR "deploy_checklist_$TIMESTAMP.txt"
@"
发布检查清单 $TIMESTAMP
- 健康检查: 通过
- 回归测试: $($SkipTests ? "跳过" : "通过")
- 备份: 请确认 data/app.db 已备份
- INTERNAL_API_KEY: 生产环境必须配置（07 §0.3）
"@ | Out-File -FilePath $checklistPath -Encoding UTF8
Write-Host "4. 检查清单已写入: $checklistPath" -ForegroundColor Green
Write-Host ""
Write-Host "=== 检查完成 ===" -ForegroundColor Cyan
