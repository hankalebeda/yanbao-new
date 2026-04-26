"""
01_需求基线.md 一致性验证脚本
检查修改后是否存在交叉矛盾问题
"""
import sys
import re
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

errors = []
warnings = []
ok_items = []

# --- 检查 1：FR-01 是否有具体数值 ---
if '200 只' in content and '20 亿元' in content and '5000 万元' in content:
    ok_items.append('FR-01 核心池数量和门槛数值已定义')
else:
    errors.append('FR-01 缺少具体数值（200只/20亿/5000万）')

# --- 检查 2：strategy_type 判定机制 ---
if 'strategy_type 判定' in content and 'LLM 根据证据特征判定' in content:
    ok_items.append('FR-06 strategy_type 判定机制已明确')
else:
    errors.append('FR-06 strategy_type 判定机制未明确')

# --- 检查 3：BEAR 下 A 类和 B/C 类规则一致性 ---
if 'A 类**：仍然发布' in content and 'B 类、C 类：`published=False`' in content:
    ok_items.append('FR-06 BEAR 下 A/B/C 类规则已分别定义')
else:
    errors.append('FR-06 BEAR 下 A/B/C 类规则不完整')

# --- 检查 4：FR-08 drawdown_state 枚举完整性 ---
if 'NORMAL' in content and 'REDUCE' in content and 'HALT' in content and 'factor=1.0' in content:
    ok_items.append('FR-08 drawdown_state 枚举和系数完整')
else:
    errors.append('FR-08 drawdown_state 枚举或系数不完整')

# --- 检查 5：止盈/止损/超时触发条件 ---
if '收盘价 ≥ target_price' in content and '次日开盘价' in content and 'TIMEOUT' in content:
    ok_items.append('FR-08 止盈/止损/超时触发条件已定义')
else:
    errors.append('FR-08 止盈/止损/超时触发条件缺失')

# --- 检查 6：FR-05 MA 计算方式 ---
if 'MA20 向上' in content and '5 交易日前 MA20' in content and 'AND 关系' in content:
    ok_items.append('FR-05 MA 计算方式和 AND 关系已明确')
else:
    errors.append('FR-05 MA 计算方式或 AND 关系未明确')

# --- 检查 7：FR-07 SELL/HOLD 口径 ---
if 'SELL' in content and '方向命中率' in content and 'HOLD' in content and '不纳入任何绩效统计' in content:
    ok_items.append('FR-07 SELL/HOLD 结算口径已定义')
else:
    errors.append('FR-07 SELL/HOLD 结算口径缺失')

# --- 检查 8：FR-02 数据等待策略 ---
if '15:50' in content and 'quality_flag=degraded' in content and '数据等待策略' in content:
    ok_items.append('FR-02 数据等待策略已定义，与 NFR-04 不冲突')
else:
    errors.append('FR-02 数据等待策略未定义或与 NFR-04 冲突')

# --- 检查 9：FR-09 OAuth 接口 ---
if '/auth/qq/callback' in content and '/auth/wechat/callback' in content:
    ok_items.append('FR-09 OAuth 接口已定义')
else:
    errors.append('FR-09 OAuth 接口未定义')

# --- 检查 10：管理员初始化通过环境变量 ---
if 'ADMIN_USERNAME' in content and 'ADMIN_PASSWORD' in content:
    ok_items.append('FR-09 管理员初始化通过环境变量已定义')
else:
    errors.append('FR-09 管理员初始化未定义环境变量方式')

# --- 检查 11：FR-03 健康探测频率 ---
if '30 分钟' in content and '连续 3 次' in content and '401/403' in content:
    ok_items.append('FR-03 健康探测频率和失效判定已定义')
else:
    errors.append('FR-03 健康探测频率或失效判定未明确')

# --- 检查 12：NFR-16 密码安全 ---
if 'NFR-16' in content and 'bcrypt' in content:
    ok_items.append('NFR-16 密码安全已定义')
else:
    errors.append('NFR-16 密码安全未定义')

# --- 检查 13：NFR-17 速率限制 ---
if 'NFR-17' in content and '429' in content and '速率限制' in content:
    ok_items.append('NFR-17 速率限制已定义')
else:
    errors.append('NFR-17 速率限制未定义')

# --- 检查 14：NFR-18 数据保留 ---
if 'NFR-18' in content and '永久保留' in content:
    ok_items.append('NFR-18 数据保留策略已定义')
else:
    errors.append('NFR-18 数据保留策略未定义')

# --- 检查 15：review_flag 枚举完整性 ---
if 'REVIEWED_OK' in content and 'REVIEWED_REMOVED' in content and 'PENDING_REVIEW' in content:
    ok_items.append('review_flag 枚举完整（含 NONE/PENDING_REVIEW/REVIEWED_OK/REVIEWED_REMOVED）')
else:
    errors.append('review_flag 枚举不完整')

# --- 检查 16：/health 响应结构 ---
if '"database"' in content and '"scheduler"' in content and '"llm_primary"' in content:
    ok_items.append('/health 响应结构已定义')
else:
    errors.append('/health 响应结构未定义')

# --- 检查 17：FR-02 调度时间与 FR-08 结算时间一致性 ---
if '15:30 模拟实盘结算' in content:
    ok_items.append('FR-02 调度时间与 FR-08 结算时间（15:30）一致')
else:
    warnings.append('FR-02 中 FR-08 结算时间需确认是否为 15:30')

# --- 检查 18：FR-06 实操指令卡 position_ratio 说明 ---
if 'sim_position_ratio × drawdown_state_factor' in content:
    ok_items.append('FR-06 position_ratio 与 FR-08 sim_position_ratio 关系已说明')
else:
    errors.append('FR-06/FR-08 中 position_ratio 与 sim_position_ratio 关系未说明')

# --- 检查 19：FR-01 下游引用与 FR-06 上游引用一致性 ---
fr01_downstream = 'FR-04（采集）、FR-06（研报）、FR-10（列表）' in content
fr06_upstream = '上游：FR-04、FR-01、FR-05' in content
if fr01_downstream and fr06_upstream:
    ok_items.append('FR-01 下游 → FR-06 上游引用一致')
else:
    errors.append('FR-01 下游引用与 FR-06 上游引用不一致')

# --- 检查 20：AGENTS.md 中"方向命中率仅作监控"与 FR-07 一致性 ---
if '方向命中率仅作运营监控，不计入四维度绩效' in content:
    ok_items.append('FR-07 方向命中率口径与 AGENTS.md 约定一致')
else:
    warnings.append('FR-07 方向命中率定义需确认与 AGENTS.md 一致')

# ===== 输出结果 =====
print('\n' + '='*60)
print('✅ 通过检查项（{}条）'.format(len(ok_items)))
print('='*60)
for item in ok_items:
    print('  ✅ ' + item)

if warnings:
    print('\n' + '⚠️  警告项（{}条）'.format(len(warnings)))
    print('-'*60)
    for w in warnings:
        print('  ⚠️  ' + w)

if errors:
    print('\n' + '❌ 错误项（{}条）—— 需要修复'.format(len(errors)))
    print('='*60)
    for e in errors:
        print('  ❌ ' + e)
else:
    print('\n🎉 无冲突/遗漏，全部通过！')

print('\n文档总行数：', len(content.split('\n')))
