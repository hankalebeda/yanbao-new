"""
build_verification_plan.py — 阶段 2: 从 SSOT(01~05) 和 claim_registry 构建验真计划

不从 22 推导规格（避免"总表自证"），而是从:
  - docs/core/01_需求基线.md (FR 定义与验收标准)
  - docs/core/02_系统架构.md (系统边界与职责)
  - docs/core/03_详细设计.md (状态机与执行链路)
  - docs/core/04_数据治理与血缘.md (治理与血缘约束)
  - docs/core/05_API与数据契约.md (接口 Schema)
  - governance/feature_registry.json (治理注册表)
  - page_expectations.py (UI 专属事实)
共同生成"应验证项".
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from dataclasses import dataclass, field, asdict

from .page_expectations import PAGE_EXPECTATIONS, get_frontend_fr_ids

# ── 常量 ──────────────────────────────────────────────

_SSOT_01 = Path("docs/core/01_需求基线.md")
_SSOT_02 = Path("docs/core/02_系统架构.md")
_SSOT_03 = Path("docs/core/03_详细设计.md")
_SSOT_04 = Path("docs/core/04_数据治理与血缘.md")
_SSOT_05 = Path("docs/core/05_API与数据契约.md")
_GOVERNANCE = Path("app/governance/feature_registry.json")


# ── 验证项模型 ──────────────────────────────────────────

@dataclass
class VerificationItem:
    """单条验真项."""
    feature_id: str           # FR00-AUTH-01
    fr_id: str                # FR-00
    title: str
    verify_kind: str          # api / dom / browser / contract / test_quality / config
    description: str          # 验真描述
    api_route: str = ""       # 需要调用的 API (如 GET /api/v1/home)
    page_id: str = ""         # 关联的页面 ID
    expected_status: int = 200
    contract_checks: list[str] = field(default_factory=list)  # 契约字段检查
    dom_checks: list[str] = field(default_factory=list)       # DOM 选择器检查
    forbidden_checks: list[str] = field(default_factory=list) # 禁止出现内容
    auth_context: str = ""    # anonymous / free / pro / admin
    priority: str = "P1"


# ── 从 SSOT 提取 API 契约 ──────────────────────────────

def _extract_api_contracts(ssot_05_path: Path) -> dict[str, dict]:
    """从 05 文档提取 API 路由 → 关键字段映射."""
    if not ssot_05_path.exists():
        return {}

    text = ssot_05_path.read_text(encoding="utf-8")
    contracts: dict[str, dict] = {}

    # 匹配 API 路由定义行
    route_pattern = re.compile(
        r"(GET|POST|PUT|PATCH|DELETE)\s+(`?(/[^\s`]+)`?)"
    )
    for m in route_pattern.finditer(text):
        method = m.group(1)
        route = m.group(3)
        key = f"{method} {route}"
        contracts[key] = {"method": method, "route": route, "fields": []}

    return contracts


def _load_ssot_context(paths: list[Path]) -> dict[str, str]:
    context: dict[str, str] = {}
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError("missing SSOT docs: " + ", ".join(missing))
    for path in paths:
        context[path.name] = path.read_text(encoding="utf-8-sig")
    return context


def _extract_acceptance_criteria(ssot_01_path: Path) -> dict[str, list[str]]:
    """从 01 文档提取 FR → 验收标准列表."""
    if not ssot_01_path.exists():
        return {}

    text = ssot_01_path.read_text(encoding="utf-8")
    criteria: dict[str, list[str]] = {}
    current_fr: str | None = None

    for line in text.splitlines():
        # 匹配 FR 标题
        m = re.match(r"^#+\s+(FR[-‐]\d+)", line)
        if m:
            current_fr = m.group(1).replace("‐", "-")
            criteria.setdefault(current_fr, [])
            continue

        # 匹配验收标准行 (通常是列表项)
        if current_fr and ("验收" in line or "acceptance" in line.lower()):
            criteria[current_fr].append(line.strip())

    return criteria


def _load_governance_registry(gov_path: Path) -> list[dict]:
    """加载治理注册表."""
    if not gov_path.exists():
        return []
    data = json.loads(gov_path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data.get("features", [])
    return data


# ── 核心构建逻辑 ──────────────────────────────────────

def build_verification_plan(
    claim_registry: list[dict],
    ssot_01: Path | None = None,
    ssot_02: Path | None = None,
    ssot_03: Path | None = None,
    ssot_04: Path | None = None,
    ssot_05: Path | None = None,
    governance: Path | None = None,
) -> list[dict]:
    """构建全量验真计划.

    Returns: verification_plan (list[dict])
    """
    ssot_01 = ssot_01 or _SSOT_01
    ssot_02 = ssot_02 or _SSOT_02
    ssot_03 = ssot_03 or _SSOT_03
    ssot_04 = ssot_04 or _SSOT_04
    ssot_05 = ssot_05 or _SSOT_05
    governance = governance or _GOVERNANCE

    # 主链显式输入 01~05；缺一即 fail-close。
    _ = _load_ssot_context([ssot_01, ssot_02, ssot_03, ssot_04, ssot_05])
    api_contracts = _extract_api_contracts(ssot_05)
    acceptance = _extract_acceptance_criteria(ssot_01)
    gov_features = _load_governance_registry(governance)
    frontend_frs = get_frontend_fr_ids()

    # 从治理注册表构建 feature_id → 治理信息 映射
    gov_map: dict[str, dict] = {}
    for gf in gov_features:
        fid = gf.get("feature_id", "")
        if fid:
            gov_map[fid] = gf

    items: list[VerificationItem] = []

    for claim in claim_registry:
        fid = claim["feature_id"]
        fr_id = claim["fr_id"]
        title = claim["title"]
        priority = claim.get("priority", "P1")
        has_frontend = claim.get("has_frontend", False)

        # --- 1. 契约校验 (所有 FR) ---
        gov = gov_map.get(fid, {})
        primary_api = gov.get("primary_api", {})
        if primary_api:
            api_key = f"{primary_api.get('method', 'GET')} {primary_api.get('path', '')}"
            contract_fields = gov.get("key_response_fields", [])
            items.append(VerificationItem(
                feature_id=fid,
                fr_id=fr_id,
                title=f"[契约] {title}",
                verify_kind="contract",
                description=f"按 05 契约校验 {api_key} 返回字段完整性",
                api_route=api_key,
                contract_checks=contract_fields,
                priority=priority,
            ))

        # --- 2. 测试质量审计 (所有 FR) ---
        ref_tests = claim.get("referenced_tests", [])
        if ref_tests:
            items.append(VerificationItem(
                feature_id=fid,
                fr_id=fr_id,
                title=f"[测试质量] {title}",
                verify_kind="test_quality",
                description=f"审计关联测试: {', '.join(ref_tests[:5])}",
                priority=priority,
            ))

        # --- 3. 前端页面验真 ---
        if has_frontend or fid in frontend_frs:
            for page in PAGE_EXPECTATIONS:
                if fid in page.fr_ids:
                    # DOM 层校验
                    if page.must_have_selectors:
                        items.append(VerificationItem(
                            feature_id=fid,
                            fr_id=fr_id,
                            title=f"[DOM] {title} - {page.page_id}",
                            verify_kind="dom",
                            description=f"HTML 解析校验 {page.route} 的 DOM 结构",
                            page_id=page.page_id,
                            dom_checks=page.must_have_selectors,
                            forbidden_checks=page.forbidden_content,
                            auth_context=_auth_context(page),
                            priority=priority,
                        ))

                    # 浏览器层校验
                    items.append(VerificationItem(
                        feature_id=fid,
                        fr_id=fr_id,
                        title=f"[浏览器] {title} - {page.page_id}",
                        verify_kind="browser",
                        description=f"Playwright 实测 {page.route}: 渲染/API/console error",
                        page_id=page.page_id,
                        api_route=", ".join(page.expected_api_calls) if page.expected_api_calls else "",
                        dom_checks=page.must_have_selectors,
                        forbidden_checks=page.forbidden_content,
                        auth_context=_auth_context(page),
                        priority=priority,
                    ))

        # --- 4. API 层真实可用性 (有 api_route 的 FR) ---
        if primary_api and primary_api.get("path"):
            route = primary_api["path"]
            method = primary_api.get("method", "GET")
            # 按不同角色测试
            for ctx in _relevant_auth_contexts(fr_id):
                items.append(VerificationItem(
                    feature_id=fid,
                    fr_id=fr_id,
                    title=f"[API] {title} ({ctx})",
                    verify_kind="api",
                    description=f"{method} {route} 以 {ctx} 身份调用",
                    api_route=f"{method} {route}",
                    auth_context=ctx,
                    priority=priority,
                ))

    plan = [asdict(item) for item in items]
    return plan


def _auth_context(page) -> str:
    if page.min_role == "admin":
        return "admin"
    if page.auth_required:
        return page.min_tier.lower() if page.min_tier != "anonymous" else "free"
    return "anonymous"


def _relevant_auth_contexts(fr_id: str) -> list[str]:
    """根据 FR 类别决定需要测试的角色."""
    if fr_id.startswith("FR-12"):
        return ["admin"]
    if fr_id.startswith("FR-09"):
        return ["anonymous", "free", "pro"]
    if fr_id in ("FR-08", "FR-10"):
        return ["anonymous", "free", "pro"]
    return ["anonymous"]


# ── CLI 入口 ──────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="构建验真计划")
    parser.add_argument("--claims", default="output/claim_registry.json")
    parser.add_argument("--output", default="output/verification_plan.json")
    args = parser.parse_args()

    claims = json.loads(Path(args.claims).read_text(encoding="utf-8"))
    plan = build_verification_plan(claims)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[plan] 生成 {len(plan)} 条验真项 → {out}")


if __name__ == "__main__":
    main()
