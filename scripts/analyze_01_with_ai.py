#!/usr/bin/env python3
"""
调用 GPT Web、Gemini Web 对 docs/core/01_需求基线.md 进行可行性分析或问题分析。

前置条件：
- 主应用已启动：python -m uvicorn app.main:app --reload
- ai-api 对应 Web 端已登录（ChatGPT / Gemini 浏览器会话有效）

Sonnet 4.6（Claude）本项目无 API 集成；--feasibility 模式下会自动生成待粘贴文件。

用法：
  python scripts/analyze_01_with_ai.py              # 问题分析模式
  python scripts/analyze_01_with_ai.py --feasibility # 可行性分析（并行 GPT+Gemini + 生成 Sonnet 待粘贴）
"""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOC_PATH = ROOT / "docs" / "core" / "01_需求基线.md"
SONNET_PASTE_PATH = ROOT / "docs" / "_temp" / "01_需求基线_Sonnet_待粘贴.md"

FEASIBILITY_PROMPT = """你是一位资深技术架构师与项目管理专家。请对以下 A 股个股研报平台需求基线文档进行**可行性分析**。

**分析维度**（按优先级逐项评估）：

1. **技术可行性**：
   - 技术栈（FastAPI、SQLAlchemy、Jinja2、LLM 路由、通达信/东财数据源）能否支撑全部 FR？
   - 是否有技术上的高难度点或不确定点（如 OAuth/QQ 微信、北向资金/ETF 接入、LLM 降级链）？
   - 四维度绩效目标（胜率≥55%、盈亏比≥1.5、年化 Alpha≥10%）是否可通过回测/实验验证达成？

2. **资源与成本**：
   - 估算 P0/P1/P2 FR 的实现工作量（人天或相对复杂度）；
   - 外部依赖成本（LLM API、数据源、第三方登录）是否可控？
   - 单机部署（16GB 显存、Ollama 兜底）是否满足预期负载？

3. **时间与依赖**：
   - FR 实现顺序表是否合理？有无关键路径阻塞？
   - 哪些 FR 可并行，哪些必须串行？
   - 建议的首版可交付范围（MVP）与时间估算。

4. **风险与建议**：
   - 列出 3～5 项主要风险及缓解措施；
   - 是否存在「不可行」或「强烈建议调整」的需求？若有，说明理由；
   - 对文档中待定义/占位（如 05_API 待行动08、04 待行动10）的影响评估。

**输出格式**：
| 维度 | 结论 | 说明 |
|------|------|------|
| 技术可行性 | 高/中/低 | ... |
| 资源成本 | 可控/需关注/高风险 | ... |
| 时间估算 | X 周/月 | ... |
| 主要风险 | 1. ... 2. ... | ... |
| 建议调整 | 无 / 有（列出） | ... |

最后给出：**综合可行性结论**（可行 / 基本可行需调整 / 存在重大障碍）及**优先建议**（1～3 条）。

---

以下为待分析的需求基线文档内容：

"""

ANALYSIS_PROMPT = """你是一位挑剔的需求分析师与产品经理。请对以下需求基线文档进行结构化分析，判断是否存在问题。

**分析维度**（按优先级逐项检查）：

1. **5 问自检**（每条 FR 能否回答）：
   - 谁触发？
   - 输入是什么（字段名、类型、示例）？
   - 成功时输出什么（字段名、类型、枚举值）？
   - 失败时怎么处理（显式报错/降级/静默，分别说明）？
   - 如何验收（pytest 断言什么 / 人工检查什么）？

2. **常见遗漏检查**：
   - 登录/注销/鉴权是否完整？
   - 列表分页是否定义？
   - 幂等保护是否明确？
   - 并发安全是否考虑？
   - 事务一致性是否说明？
   - 权益能力矩阵（Free/Pro/Enterprise）是否清晰？
   - 市场状态 BEAR 下 B/C 信号过滤逻辑是否无歧义？

3. **SSOT 引用一致性**：
   - 文档中「见 05_API §（待行动08）」「见 04_数据治理 §（待行动10）」等占位是否正确？
   - 是否存在未定义枚举或未约定字段？

4. **可验收性**：
   - 每条 FR 的验收标准是否具体到「可写出 pytest 断言」？
   - 边界条件是否覆盖异常/降级场景？

5. **范围与依赖**：
   - 范围外说明是否完整？
   - FR 上下游依赖是否闭环（无悬空引用）？

**输出格式**：
| 问题类型 | FR/章节 | 描述 | 建议 |
|---------|---------|------|------|
| ... | ... | ... | ... |

最后给出：**是否存在阻塞性问题**（是/否）及**优先修复项**（1～3 条）。

---

以下为待分析的需求基线文档内容：

"""

BASE_URL = "http://127.0.0.1:8000"


def _call_provider(provider: str, full_prompt: str, timeout: int = 180) -> tuple[str, str]:
    """调用单个 provider，返回 (provider, response_text)。"""
    try:
        import httpx

        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                f"{BASE_URL}/api/v1/webai/analyze",
                json={"provider": provider, "prompt": full_prompt, "timeout_s": timeout},
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data.get("data"), dict):
                text = data["data"].get("response", str(data))
            else:
                text = str(data)
            return provider, text
    except httpx.ConnectError:
        return provider, f"[连接失败] 请确认主应用已启动 {BASE_URL}，且 {provider} Web 已登录"
    except Exception as e:
        return provider, f"[错误] {e}"


def _write_sonnet_paste(prompt: str, doc_content: str) -> None:
    """生成 Sonnet 待粘贴文件。"""
    SONNET_PASTE_PATH.parent.mkdir(parents=True, exist_ok=True)
    content = prompt + doc_content
    SONNET_PASTE_PATH.write_text(content, encoding="utf-8")
    print(f"Sonnet 待粘贴文件已生成：{SONNET_PASTE_PATH}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="调用 GPT/Gemini Web 对 01_需求基线.md 进行分析"
    )
    parser.add_argument(
        "--feasibility",
        action="store_true",
        help="可行性分析模式（默认：问题分析模式）",
    )
    parser.add_argument(
        "--providers",
        default="chatgpt,gemini",
        help="provider 列表，逗号分隔（默认：chatgpt,gemini）",
    )
    args = parser.parse_args()

    if not DOC_PATH.exists():
        print(f"错误：文档不存在 {DOC_PATH}")
        sys.exit(1)

    doc_content = DOC_PATH.read_text(encoding="utf-8")
    prompt = FEASIBILITY_PROMPT if args.feasibility else ANALYSIS_PROMPT
    full_prompt = prompt + doc_content
    mode = "可行性分析" if args.feasibility else "问题分析"
    providers = [p.strip() for p in args.providers.split(",") if p.strip()]

    try:
        import httpx
    except ImportError:
        print("请安装 httpx：pip install httpx")
        sys.exit(1)

    print(f"模式：{mode}")
    results: dict[str, str] = {}

    # 并行调用各 provider
    with ThreadPoolExecutor(max_workers=len(providers)) as ex:
        futures = {ex.submit(_call_provider, p, full_prompt): p for p in providers}
        for fut in as_completed(futures):
            provider, text = fut.result()
            results[provider] = text
            print(f"  {provider} 返回 {len(text)} 字符")

    # 可行性分析模式下自动生成 Sonnet 待粘贴文件
    if args.feasibility:
        _write_sonnet_paste(prompt, doc_content)

    out_dir = ROOT / "docs" / "_temp"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = (
        out_dir / "01_需求基线_可行性分析结果.json"
        if args.feasibility
        else out_dir / "01_需求基线_多AI分析结果.json"
    )
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存至 {out_file}")

    for provider, text in results.items():
        print(f"\n--- {provider} {mode}摘要 ---")
        print((text[:2000] + "..." if len(text) > 2000 else text))


if __name__ == "__main__":
    main()
