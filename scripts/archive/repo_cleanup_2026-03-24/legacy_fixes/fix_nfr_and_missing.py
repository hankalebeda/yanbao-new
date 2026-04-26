import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

# ===== 1. 扩展 NFR 表：补充 NFR-16~NFR-18 =====
old_nfr = """| NFR-15 | 研报质量与收益提升方法论；参数/Prompt/模型变更须通过 03 实验门禁 | 人工检查 + 01 §1.6、03 §10 |

---"""

new_nfr = """| NFR-15 | 研报质量与收益提升方法论；参数/Prompt/模型变更须通过 03 实验门禁 | 人工检查 + 01 §1.6、03 §10 |
| NFR-16 | 密码安全：用户密码存储使用 bcrypt（cost factor ≥ 12），禁止明文存储、禁止 MD5/SHA1 | pytest 断言密码字段不含原始密码；代码审查 |
| NFR-17 | API 速率限制：`POST /auth/login` ≤ 5 次/分钟/IP；`POST /auth/register` ≤ 3 次/分钟/IP；`POST /api/v1/reports/generate` ≤ 10 次/分钟/用户；其余 API ≤ 100 次/分钟/IP；超限返回 HTTP 429，body 含 `retry_after: int`（秒） | pytest 断言超限返回 429；人工验证 retry_after 字段存在 |
| NFR-18 | 数据保留策略：研报及模拟持仓/账户数据**永久保留**；热搜原始数据保留 90 天；调度执行日志保留 30 天；用户行为日志（反馈记录）永久保留 | 运维脚本定期清理；可查日志时间范围 |

---"""

if old_nfr in content:
    content = content.replace(old_nfr, new_nfr)
    print('OK: NFR-16~18 已添加')
else:
    print('ERROR NFR: 未找到原文')

# ===== 2. FR-11 补充 review_flag 完整枚举 =====
old_fr11_review = """| **复审** | 待复审由 GET /api/v1/admin/reports?review_flag=PENDING_REVIEW 获取；研报展示「待复审」标记；是否下架由 FR-12 PATCH 决定 |"""

new_fr11_review = """| **review_flag 枚举** | `NONE`（默认，无需复审）；`PENDING_REVIEW`（负反馈≥3次，待复审）；`REVIEWED_OK`（复审通过，维持发布）；`REVIEWED_REMOVED`（复审后下架：前台显示「该研报已下架」，不展示内容，记录仍保留，绩效统计中单独标注但不删除） |
| **复审** | 待复审由 GET /api/v1/admin/reports?review_flag=PENDING_REVIEW 获取；研报展示「待复审」标记；下架/通过由 FR-12 PATCH review_flag 决定 |"""

if old_fr11_review in content:
    content = content.replace(old_fr11_review, new_fr11_review)
    print('OK: review_flag 枚举已添加')
else:
    print('ERROR review_flag: 未找到原文')

# ===== 3. NFR-12 /health 接口补充响应结构 =====
old_nfr12 = """| NFR-12 | GET /health 返回服务状态 | pytest 断言 |"""

new_nfr12 = """| NFR-12 | `GET /health` 返回服务状态，响应体：`{ "status": "ok"\\|"degraded"\\|"down", "components": { "database": "ok"\\|"down", "scheduler": "ok"\\|"stopped", "llm_primary": "ok"\\|"down", "tdx_data": "ok"\\|"stale"\\|"missing", "cookie_session": "ok"\\|"expired" }, "last_report_generated_at": str?, "timestamp": str }` | pytest 断言 status ∈ {ok,degraded,down}；components 各字段存在 |"""

if old_nfr12 in content:
    content = content.replace(old_nfr12, new_nfr12)
    print('OK: /health 响应结构已更新')
else:
    print('ERROR /health: 未找到原文')

# ===== 4. FR-06 实操指令卡补充 position_ratio 与 sim 关系说明 =====
# 已在 FR-06 和 FR-08 中分别定义，在 FR-06 实操指令卡行加注说明
old_fr06_card = """| **实操指令卡** | `entry_price`=当日收盘价；`stop_loss`=entry×(1−ATR_pct×止损倍数)，ATR 不可用时固定止损率 8%（即×0.92）；`target_price`=entry×(1+止损幅度×1.5)；`max_hold_days`=策略类型对应 T+N |"""

new_fr06_card = """| **实操指令卡** | `entry_price`=当日收盘价；`stop_loss`=entry×(1−ATR_pct×止损倍数)，ATR 不可用时固定止损率 8%（即×0.92）；`target_price`=entry×(1+止损幅度×1.5)；`max_hold_days`=策略类型对应 T+N；`position_ratio: float`=FR-08 `sim_position_ratio × drawdown_state_factor`（即实际仓位比例，供 FR-08 计算开仓金额，公式见 FR-08 开仓金额公式） |"""

if old_fr06_card in content:
    content = content.replace(old_fr06_card, new_fr06_card)
    print('OK: position_ratio 说明已更新')
else:
    print('ERROR position_ratio: 未找到原文')

open(path, 'w', encoding='utf-8').write(content)
print('===== 全部写入完成 =====')
