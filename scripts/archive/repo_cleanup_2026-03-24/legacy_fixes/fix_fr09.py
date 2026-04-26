import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### FR-09 商业化与权益（含登录/鉴权）

| 维度 | 内容 |
|------|------|
| **功能域** | 会员、权限、注册/登录/注销/鉴权 |
| **上下游** | 下游：FR-10、FR-11 |
| **触发** | 用户注册/登录/注销、Token 刷新、套餐订阅、支付回调、API 鉴权校验 |
| **接口** | POST /auth/register（username, password, email）→ Free；POST /auth/login → access_token（2h）+ refresh_token（7d）；POST /auth/refresh → 新 access_token；POST /auth/logout → 使 Token 失效 |
| **输入** | 注册/登录参数；套餐 ID；JWT Token |
| **输出** | 权益 Free / Pro / Enterprise；Token 对 |
| **边界** | 越权 → 401/403；支付回调失败 → 冻结权益，人工对账；用户名/邮箱已存在 → 409；Token 过期 → 401 |
| **验收** | pytest 断言越权 401/403；注册重复 409；登录返回 access_token；Token 续期逻辑 |
| **优先级** | P1 |
| **相关** | 05_API、04_数据治理 |"""

new = """### FR-09 商业化与权益（含登录/鉴权/OAuth）

| 维度 | 内容 |
|------|------|
| **功能域** | 会员、权限、注册/登录/注销/鉴权/第三方 OAuth |
| **上下游** | 下游：FR-10、FR-11 |
| **触发** | 用户注册/登录/注销、Token 刷新、OAuth 授权、套餐订阅、支付回调、API 鉴权校验 |
| **本地账号接口** | `POST /auth/register`（body: username, password, email）→ 创建 Free 账号，返回 user_id；`POST /auth/login`（body: username/email, password）→ access_token（TTL 2h）+ refresh_token（TTL 7d）；`POST /auth/refresh`（body: refresh_token）→ 新 access_token；`POST /auth/logout`（Header: Bearer token）→ 使 refresh_token 失效 |
| **OAuth 接口（QQ/微信首版实现）** | `GET /auth/qq`：重定向至 QQ 互联授权页（携带 client_id、redirect_uri、state）；`GET /auth/qq/callback`（query: code, state）→ 换取 access_token → 获取 QQ openid → 查找或创建关联账号 → 返回系统 access_token + refresh_token；`GET /auth/wechat`：重定向至微信开放平台授权页；`GET /auth/wechat/callback`（query: code, state）→ 换取 access_token → 获取 unionid/openid → 查找或创建关联账号 → 返回系统 Token；**账号合并规则**：OAuth 邮箱与已有本地账号邮箱相同 → 自动绑定；否则创建新 Free 账号 |
| **管理员初始化** | 系统首次启动时读取环境变量：`ADMIN_USERNAME`、`ADMIN_PASSWORD`、`ADMIN_EMAIL`；若数据库中不存在该用户名则创建 role=admin 账号；已存在则跳过（幂等）；环境变量缺失 → 启动日志告警，不阻塞 |
| **输入** | 注册/登录参数；OAuth code/state；套餐 ID；JWT Token |
| **输出** | `access_token: str`；`refresh_token: str`；`user_id: str`；`role` 枚举 free / pro / enterprise / admin；`expires_in: int`（秒） |
| **边界** | 越权访问 → 403；未登录访问需鉴权接口 → 401；用户名/邮箱已存在 → 409；Token 过期 → 401；支付回调失败 → 冻结权益，人工对账；OAuth state 校验失败 → 400 |
| **验收** | `test_fr09_unauthorized_403`：无 Token 访问受限接口 → 401；`test_fr09_login_returns_token`：登录返回 access_token；`test_fr09_register_duplicate_409`：重复注册 → 409；`test_fr09_token_refresh`：refresh_token 换新 access_token；`test_fr09_admin_init`：环境变量配置后启动 → admin 账号存在 |
| **优先级** | P1 |
| **相关** | 05_API、04_数据治理 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: FR-09 已替换')
else:
    print('ERROR: 未找到原文')
