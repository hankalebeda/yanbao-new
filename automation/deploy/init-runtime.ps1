<#
.SYNOPSIS
  初始化运行时目录结构，确保首次启动不因目录缺失而失败。
.DESCRIPTION
  创建 Kestra + New API + 独立写回服务 体系所需的全部 runtime 目录。
  幂等操作：已存在的目录不会被覆盖。
#>
param(
    [string]$RepoRoot = (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)))
)

$ErrorActionPreference = "Stop"

$dirs = @(
    "runtime\loop_controller",
    "runtime\code_fix",
    "runtime\services",
    "runtime\issue_mesh",
    "runtime\codex_mesh\worktrees",
    "automation\writeback_service\.audit\commits",
    "automation\writeback_service\.audit\idempotency",
    "automation\writeback_service\.audit\preview",
    "automation\writeback_service\.audit\.locks",
    "automation\writeback_service\.audit_writeback_b\commits",
    "automation\writeback_service\.audit_writeback_b\idempotency",
    "automation\writeback_service\.audit_writeback_b\preview",
    "automation\writeback_service\.audit_writeback_b\.locks",
    "docs\_temp\issue_mesh_shadow"
)

foreach ($rel in $dirs) {
    $full = Join-Path $RepoRoot $rel
    if (-not (Test-Path $full)) {
        New-Item -ItemType Directory -Path $full -Force | Out-Null
        Write-Host "[init] created: $rel"
    }
}

Write-Host "[init] runtime directories ready."
