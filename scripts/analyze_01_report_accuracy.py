#!/usr/bin/env python3
"""
调用 GPT Web、Gemini Web 对 docs/core/01_需求基线.md 进行「研报准确率与后续优化」专项分析。

前置条件：
- 主应用已启动：python -m uvicorn app.main:app --reload
- ai-api 对应 Web 端已登录（ChatGPT / Gemini 浏览器会话有效）

Sonnet 4.6 需手动复制 docs/_temp/01_需求基线_研报准确率专项分析提示词.md 到 Claude 使用。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOC_01 = ROOT / "docs" / "core" / "01_需求基线.md"
DOC_03 = ROOT / "docs" / "core" / "03_详细设计.md"

ANALYSIS_PROMPT = """你是一位需求分析师与研报系统架构师。请针对以下问题，对需求基线文档进行专项分析：

**核心问题**：
1. 研报是系统的基础产出物，但文档中「如何提高研报准确率」的路径不清晰——读者读完后不知道具体该做什么才能提升准确率。
2. 研报作为基础数据，后续如何持续优化（Prompt/样本/参数/模型迭代）是重点，但需求基线中几乎没有体现。

**分析要求**：

1. **准确率提升路径缺失分析**
   - 01 中是否有显式的「研报准确率提升」需求或可追溯的 FR？
   - 01 对 03_详细设计 §10（研报质量与收益提升设计）的引用是否足够？读者能否在 01 中快速理解「发现问题→归因→改进→验证」闭环？
   - 四维度绩效（胜率≥55%、盈亏比≥1.5、年化Alpha≥10%）是结果目标，但「如何通过优化研报来达成」的输入路径是否在 01 中体现？

2. **研报优化链路缺口**
   - Few-shot 样本库、信号增强、错误归因、回测沙盒等机制在 03 §10 有设计，01 是否应增加对应的 FR 或 NFR 引用？
   - 研报生成（FR-06）与「研报优化」的关系是否应在 01 中显式表述？（例如：FR-06 产出研报 → 结算/反馈产生样本 → 样本注入改进下一轮生成）

3. **具体优化建议**
   - 请给出 3～5 条可落地的修改建议，使读者在阅读 01 时能够清楚：
     (a) 研报准确率提升的输入路径是什么；
     (b) 研报作为基础数据，后续有哪些可执行的优化动作；
     (c) 这些动作与哪些 FR/NFR 对应。

**输出格式**：

| 问题类型 | 位置 | 描述 | 具体修改建议 |
|---------|------|------|-------------|
| ... | ... | ... | ... |

**建议的 01 文档新增/修改内容**（可直接用于整合）：
- [列出建议新增的章节标题、段落或 FR/NFR 补充内容]

**结论**：是否存在「研报准确率与优化路径」的显著缺口？（是/否）及优先整合项（1～3 条）。

---

以下为待分析文档内容。

【01_需求基线.md】
"""

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    if not DOC_01.exists():
        print(f"错误：文档不存在 {DOC_01}")
        sys.exit(1)

    doc_01 = DOC_01.read_text(encoding="utf-8")
    doc_03_section = ""
    if DOC_03.exists():
        text = DOC_03.read_text(encoding="utf-8")
        if "## 10. 研报质量与收益提升设计" in text:
            start = text.index("## 10. 研报质量与收益提升设计")
            end = text.find("\n---\n", start + 1)
            if end == -1:
                end = len(text)
            doc_03_section = "\n\n【03_详细设计.md §10 研报质量与收益提升设计】\n" + text[start:end]

    full_prompt = ANALYSIS_PROMPT + doc_01 + doc_03_section

    try:
        import httpx
    except ImportError:
        print("请安装 httpx：pip install httpx")
        sys.exit(1)

    results: dict[str, str] = {}

    for provider in ["chatgpt", "gemini"]:
        print(f"\n正在调用 {provider}...")
        try:
            with httpx.Client(timeout=240) as client:
                resp = client.post(
                    f"{BASE_URL}/api/v1/webai/analyze",
                    json={
                        "provider": provider,
                        "prompt": full_prompt,
                        "timeout_s": 240,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data.get("data"), dict):
                    text = data["data"].get("response", str(data))
                else:
                    text = str(data)
                results[provider] = text
                print(f"  {provider} 返回 {len(text)} 字符")
        except httpx.ConnectError:
            print(f"  {provider} 连接失败：请确认主应用已启动 {BASE_URL}")
            results[provider] = f"[连接失败] 请确认主应用已启动，且 {provider} Web 已登录"
        except Exception as e:
            print(f"  {provider} 错误: {e}")
            results[provider] = f"[错误] {e}"

    out_dir = ROOT / "docs" / "_temp"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "01_需求基线_研报准确率_多AI分析结果.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存至 {out_file}")

    for provider, text in results.items():
        print(f"\n--- {provider} 分析摘要 ---")
        print((text[:2500] + "..." if len(text) > 2500 else text))


if __name__ == "__main__":
    main()
