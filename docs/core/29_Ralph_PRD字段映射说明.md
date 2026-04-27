# 29_Ralph_PRD字段映射说明

> **文档编号**：29  
> **文档名称**：Ralph PRD 字段映射说明  
> **项目名称**：A 股个股研报平台（`yanbao-new`）  
> **版本**：v1.1  
> **产出日期**：2026-04-27  
> **用途**：说明 `docs/core/27` 如何转换为 Ralph 最小 schema 的双份 `prd.json`。

---

## 1. 输入与输出

- Markdown 输入：`docs/core/27_PRD_研报平台增强与整体验收基线.md`。
- Runtime 输出：`.claude/ralph/loop/prd.json`。
- 命名副本：`.claude/ralph/prd/yanbao-platform-enhancement.json`。
- Step 2 只能把 runtime 输出作为直接任务入口；27/28/29 是 Step 1 的转换来源与规则解释，不是 Step 2 的平行任务清单。

---

## 2. 顶层字段映射

| `prd.json` 字段 | 来源 | 规则 |
| --- | --- | --- |
| `project` | 27 文档项目名称 | 固定表达为 A 股研报平台整体验收与增强。 |
| `branchName` | Ralph 配置 | 固定为 `ralph/ashare-research-platform`。 |
| `description` | 27 的目标、边界与执行说明 | 必须说明 story 数量、版本日期、规则内联、28/29 已补齐或可回溯，以及运行态闭环 phase 是否已写入。 |
| `userStories` | 27 的功能需求、技术规则、阶段拆分 | 按依赖顺序拆成单轮可完成 story。 |

---

## 3. Story 字段映射

| Story 字段 | 来源 | 规则 |
| --- | --- | --- |
| `id` | 依赖顺序 | 使用 `US-001` 起的连续编号。 |
| `title` | 原子能力名称 | 一条 story 只覆盖一个模型、一个 API、一个页面区块、一个采集适配器或一个治理点。 |
| `description` | 用户故事语义 | 写明角色、动作和目的，避免流程口号。 |
| `acceptanceCriteria` | 27/28 的验收门禁 | 必须可验证，包含入口、权限、状态码或错误码、幂等/唯一键、枚举/阈值、降级、focused pytest，以及当故事交付物是运行态事实时的 `check_state.py` / sqlite / endpoint / browser 证据。 |
| `priority` | 依赖顺序 | 越小越先执行；前置模型、契约、门禁先于服务、页面与聚合。 |
| `passes` | 真实验收状态 | 初始为 `false`；只有实现和验证通过后才能改为 `true`。 |
| `notes` | 执行元数据 | 必须是紧凑 JSON 字符串，承载依赖、入口、模型、权限、错误码、幂等、枚举、阈值、降级、断言和测试命令；运行态 story 的非零事实应至少折叠进 `exampleAssert`。 |

---

## 4. `notes` 必填键

`notes` 必须可被 JSON 解析，键集合至少包含：

- `group`
- `dependsOn`
- `endpoints`
- `models`
- `permissions`
- `errorCodes`
- `idempotency`
- `enums`
- `thresholds`
- `degradation`
- `exampleAssert`
- `pytest`

键值必须写成确定事实，不得使用“视情况”“适当处理”“保持一致”“正常返回”等模糊词。

### 4.1 运行态闭环 story 追加规则

- 当前仓库把 `US-101–US-108` 保留为运行态闭环 story，用来消除“测试全绿但 `check_state.py` 仍为 `Total published: 0`”的假完成状态。
- 这类 story 的 `acceptanceCriteria` 必须写明至少一种真实运行态证据：`python .\check_state.py`、sqlite `COUNT(*)`、真实 API 响应、或浏览器 / HTML 验证。
- `pytest` 字段继续只放 focused pytest 命令；额外运行态命令写在 `acceptanceCriteria`，最小非零或显式降级条件写在 `exampleAssert`。

---

## 5. 同步规则

- 修改 27 的目标、阶段、story 数量、验收口径或降级语义后，必须同步两份 `prd.json`；当前基线 story 总数为 108。
- 修改 runtime `prd.json` 的 story 边界、验收标准、依赖或 `notes` 后，必须回写 27 或本映射说明中的对应规则。
- 两份 `prd.json` 必须保持完全一致；任何分叉都视为 Step 2 启动阻塞。
- 禁止在 `prd.json` 中加入 Ralph 最小 schema 之外的 `tags`、`deps`、`owner`、`component` 等字段。
