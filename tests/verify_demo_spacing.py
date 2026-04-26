#!/usr/bin/env python3
"""
Demo 页面间距规则验证
验证 demo.css 与 HTML 中无冲突内联样式，确保间距规则正确。
运行: python tests/verify_demo_spacing.py
"""
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEMO_CSS = PROJECT_ROOT / "demo" / "demo.css"
INDEX_HTML = PROJECT_ROOT / "demo" / "index.html"
REPORTS_HTML = PROJECT_ROOT / "demo" / "reports_list.html"


def main():
    errors = []
    css = DEMO_CSS.read_text(encoding="utf-8")
    index = INDEX_HTML.read_text(encoding="utf-8")
    reports = REPORTS_HTML.read_text(encoding="utf-8")

    # 1. 市场横幅：应有 gap: 2px 6px（或更小）
    if "gap: 2px 6px" not in css or ".hero .summary-box .market-banner-grid" not in css:
        m = re.search(r"\.hero \.summary-box \.market-banner-grid \{ [^}]*gap:\s*([^;]+)", css, re.DOTALL)
        errors.append(f"market-banner-grid gap 应为 2px 6px，当前: {m.group(1) if m else '未找到'}")
    else:
        print("[OK] 市场横幅 grid gap: 2px 6px")

    # 2. 绩效丸：应有 gap >= 20px 32px
    m = re.search(r"\.hero \.summary-box \.hero-cred-metrics[^}]*gap:\s*([^;!]+)", css, re.DOTALL)
    if m:
        g = m.group(1).strip()
        parts = re.findall(r"(\d+)\s*px", g)
        if parts and len(parts) >= 2:
            row, col = int(parts[0]), int(parts[1])
            if row >= 18 and col >= 24:
                print(f"[OK] 绩效丸 metrics gap: {row}px {col}px")
            else:
                errors.append(f"绩效丸 gap 应 >= 18px 24px，当前: {row}px {col}px")
        else:
            print(f"[OK] 绩效丸 metrics gap: {g}")
    else:
        errors.append("未找到 .hero-cred-metrics gap 规则")

    # 3. report-row: gap >= 20, padding >= 18
    m = re.search(r"\.report-row \{[^}]*gap:\s*(\d+)px", css, re.DOTALL)
    if m and int(m.group(1)) >= 20:
        print(f"[OK] report-row gap: {m.group(1)}px")
    elif m:
        errors.append(f"report-row gap 应 >= 20px，当前: {m.group(1)}px")
    else:
        errors.append("未找到 report-row gap 规则")

    m = re.search(r"\.report-row-left[^}]*gap:\s*(\d+)px", css, re.DOTALL)
    if m and int(m.group(1)) >= 18:
        print(f"[OK] report-row-left gap: {m.group(1)}px")
    elif m:
        errors.append(f"report-row-left gap 应 >= 18px，当前: {m.group(1)}px")

    # 4. 检查 index.html 无冲突内联
    if 'market-banner-grid" style=' in index or 'market-banner-item" style=' in index:
        errors.append("index.html: 请移除 market-banner 上的冲突内联 style")
    if 'hero-cred-metrics" style=' in index:
        errors.append("index.html: 请移除 hero-cred-metrics 上的冲突内联 style")

    # 5. 检查 reports_list 无冲突内联
    if 'report-row" style=' in reports or 'report-row-left" style=' in reports:
        errors.append("reports_list.html: 请移除 report-row 相关冲突内联 style")

    if errors:
        print("\n--- 发现的问题 ---")
        for e in errors:
            print("[FAIL]", e)
        return 1
    print("\n全部间距规则验证通过。")
    return 0


if __name__ == "__main__":
    exit(main())
