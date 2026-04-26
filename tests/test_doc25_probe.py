"""Tests for automation/agents/doc25_probe.py.

Verifies structural scanning of doc22 for alive markers and topic
package matching, using synthetic doc22 content.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from automation.agents.doc25_probe import Doc25AngleProbe, PRIORITY_TOPICS


@pytest.fixture
def repo_with_docs(tmp_path):
    """Create a minimal repo with doc22 and doc25 stubs."""
    core = tmp_path / "docs" / "core"
    core.mkdir(parents=True)

    doc22 = core / "22_全量功能进度总表_v7_精审.md"
    doc22.write_text(
        "# 22 全量功能进度总表\n\n"
        "## Section 2 — 问题清单\n\n"
        "### P1 Repo Mixed State — 仍存活\n"
        "🔴 真相层与血缘不一致，需要 lineage 修复\n"
        "建议角度: truth, lineage\n\n"
        "### P1 Live Runtime Recovery\n"
        "🟡 运行时恢复尚未完成\n"
        "坏批次与运行时问题 Still Alive\n\n"
        "### P2 ISSUE-REGISTRY\n"
        "Residual Risk: registry 完整性不足\n"
        "需要 payment 与 auth 联合治理\n\n"
        "### P2 FR-07 Writer Rebuild\n"
        "🟡 writer 重建进度落后\n\n"
        "## Section 3 — 已关闭\n"
        "### P0 全部关闭\n"
        "✅ 无存活问题\n",
        encoding="utf-8",
    )

    doc25 = core / "25_分析角度.md"
    doc25.write_text(
        "# 25 分析角度\n\n"
        "## Chapter 1\n角度1: 真相层\n角度5: 血缘\n\n"
        "## Chapter 2\n角度9: 坏批次\n角度10: 运行时\n",
        encoding="utf-8",
    )

    return tmp_path


class TestDoc25AngleProbe:
    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_structural_scan_finds_alive_markers(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()

        # Should find at least 4 alive markers (🔴, 🟡 ×2, Residual Risk, Still Alive)
        assert len(problems) >= 4

    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_problem_ids_are_unique(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()
        ids = [p.problem_id for p in problems]
        assert len(ids) == len(set(ids))

    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_severity_assignment(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()

        # At least one P1 problem should exist
        severities = [p.severity for p in problems]
        assert "P1" in severities

    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_source_probe_tag(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()
        for p in problems:
            assert p.source_probe == "doc25_angle"

    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_topic_matching(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()

        # Problems under "真相层与血缘" heading should match truth_lineage topic
        truth_problems = [p for p in problems if p.family == "truth_lineage"]
        assert len(truth_problems) >= 1

    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_topic_packages_apply_runtime_metadata(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()

        assert any(
            p.family == "truth_lineage"
            and p.task_family == "issue-registry"
            and p.lane_id == "gov_registry"
            for p in problems
        )
        assert any(
            p.family == "bad_batch_runtime"
            and p.current_status == "active"
            and p.suggested_approach == "execution_and_monitoring"
            for p in problems
        )

    @pytest.mark.asyncio
    async def test_no_doc22_returns_empty(self, tmp_path):
        probe = Doc25AngleProbe(tmp_path)
        problems = await probe.scan()
        assert problems == []

    @pytest.mark.asyncio
    @patch("automation.agents.codex_bridge.codex_available", new_callable=AsyncMock, return_value=False)
    async def test_closed_section_not_matched(self, _mock_avail, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        problems = await probe.scan()

        # The "✅ 无存活问题" line should NOT produce a problem
        for p in problems:
            assert "全部关闭" not in p.title or "无存活" not in p.description


class TestDoc25ParseCodexResult:
    def test_parses_json_array(self, tmp_path):
        probe = Doc25AngleProbe(tmp_path)
        result_text = """```json
[
  {
    "problem_id": "doc25_truth_01",
    "severity": "P1",
    "family": "truth_lineage",
    "title": "真相层不一致",
    "description": "lineage 断裂",
    "recommended_angles": [1, 5],
    "lane_id": "gov_registry"
  }
]
```"""
        problems = probe._parse_codex_result(result_text)
        assert len(problems) == 1
        assert problems[0].problem_id == "doc25_truth_01"
        assert problems[0].severity == "P1"
        assert problems[0].family == "truth_lineage"

    def test_handles_invalid_json(self, tmp_path):
        probe = Doc25AngleProbe(tmp_path)
        assert probe._parse_codex_result("not json at all") == []

    def test_handles_empty(self, tmp_path):
        probe = Doc25AngleProbe(tmp_path)
        assert probe._parse_codex_result("") == []


class TestDoc25CodexPrompt:
    def test_prompt_includes_structural_summary(self, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        doc22_text = probe._read_doc(probe._doc22_path)
        doc25_text = probe._read_doc(probe._doc25_path)
        structural = probe._structural_scan(doc22_text)

        prompt = probe._build_codex_prompt(doc22_text, doc25_text, structural)

        assert "结构化预扫描结果" in prompt
        assert "severity=" in prompt
        assert "最多返回 6 项" in prompt


class TestDoc25CodexExecution:
    @pytest.mark.asyncio
    async def test_codex_scan_uses_selected_provider_and_bounded_timeout(self, repo_with_docs):
        probe = Doc25AngleProbe(repo_with_docs)
        with patch("automation.agents.codex_bridge.detect_provider", return_value="bus.042999.xyz") as mock_detect:
            with patch("automation.agents.codex_bridge.codex_exec", new_callable=AsyncMock, return_value="[]") as mock_exec:
                result = await probe._codex_scan("doc22", "doc25", [])

        assert result == []
        mock_detect.assert_called_once_with(repo_with_docs)
        mock_exec.assert_awaited_once_with(
            mock_exec.await_args.args[0],
            repo_with_docs,
            timeout_s=120,
            provider="bus.042999.xyz",
        )
