import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### FR-03 Cookie 与会话管理

| 维度 | 内容 |
|------|------|
| **功能域** | 热搜采集依赖（微博/抖音等需登录态） |
| **上下游** | 下游：FR-04（热搜采集依赖 `cookie_session`） |
| **触发** | 首次人工登录 / 健康探测定时任务发现失效 |
| **输入** | 人工登录凭证（首次）；健康探测无额外参数 |
| **输出** | `cookie_session`（可续期会话凭证）；健康状态 `ok` \\| `fail`；Schema 见 05_API §（待行动08） |
| **流程** | 访问 `/admin/cookie-setup` 或 `scripts/cookie_login.py` → 持久化 `cookie_session`；续期由健康探测发现失效后触发刷新 |
| **边界** | 会话失效 → 自动刷新；连续刷新失败 → 告警，需人工重新登录；失效恢复时长目标 ≤ 15 分钟 |
| **验收** | pytest 断言健康探测 `status in ("ok","fail")`；人工验证首次登录后健康探测返回 ok |
| **优先级** | P1 |
| **相关** | 05_API（内部）、04_数据治理 |"""

new = """### FR-03 Cookie 与会话管理

| 维度 | 内容 |
|------|------|
| **功能域** | 热搜采集依赖（微博/抖音/东财等需登录态） |
| **上下游** | 下游：FR-04（热搜采集依赖 `cookie_session`） |
| **触发** | 首次人工登录 / 健康探测定时任务发现失效 |
| **输入** | 人工登录凭证（首次）；平台标识 `platform` 枚举 weibo / douyin / eastmoney |
| **输出** | `cookie_session`（可续期会话凭证，按平台独立存储）；`health_status` 枚举 ok / fail；`last_checked_at: str`；`fail_reason: str?` |
| **健康探测规则** | **频率**：每 30 分钟执行一次（Cron: `*/30 * * * *`）；**失效判定**：HTTP 响应状态码为 401/403，或请求超时 > 30s，或响应体含特定登录失效标识 → 判定为 fail；**重试机制**：失效后立即尝试自动刷新 1 次；刷新失败则标记 fail |
| **告警阈值** | 同一平台连续 3 次探测结果为 fail（即连续 90 分钟失效）→ 触发告警（NFR-13），提示人工重新登录 |
| **流程** | 访问 `/admin/cookie-setup` 或运行 `scripts/cookie_login.py` → 持久化 `cookie_session` → 后续由健康探测自动维护 |
| **边界** | 会话失效 → 自动刷新 1 次；刷新失败 → 标记 fail，热搜降级至备用源（FR-04 降级逻辑）；连续 3 次失败 → 告警，需人工重新登录；失效恢复时长目标 ≤ 15 分钟 |
| **验收** | `test_fr03_health_check`：health_status ∈ {ok, fail}；`test_fr03_fail_reason_nonempty`：fail 时 fail_reason 非空；人工验证首次登录后探测返回 ok |
| **优先级** | P1 |
| **相关** | 05_API（内部）、04_数据治理 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: FR-03 已替换')
else:
    print('ERROR: 未找到原文')
