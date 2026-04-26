"""
FR-06 LLM Provider 验收测试 — Gemini / ChatGPT / DeepSeek / Qwen Web 端点

覆盖功能：
  FR06-LLM-GEMINI-01  Gemini Provider 分析     /api/v1/gemini/analyze
  FR06-LLM-GEMINI-02  Gemini 批量分析           /api/v1/gemini/analyze/batch
  FR06-LLM-GEMINI-03  Gemini 会话管理           DELETE /api/v1/gemini/session
  FR06-LLM-GEMINI-04  Gemini 会话状态           GET /api/v1/gemini/session/status

  FR06-LLM-CHATGPT-01 ChatGPT Provider 分析    /api/v1/chatgpt/analyze
  FR06-LLM-CHATGPT-02 ChatGPT 批量分析          /api/v1/chatgpt/analyze/batch
  FR06-LLM-CHATGPT-03 ChatGPT 会话管理          DELETE /api/v1/chatgpt/session
  FR06-LLM-CHATGPT-04 ChatGPT 会话状态          GET /api/v1/chatgpt/session/status

  FR06-LLM-DEEPSEEK-01 DeepSeek Provider 分析  /api/v1/deepseek/analyze
  FR06-LLM-DEEPSEEK-02 DeepSeek 批量分析        /api/v1/deepseek/analyze/batch
  FR06-LLM-DEEPSEEK-03 DeepSeek 会话管理        DELETE /api/v1/deepseek/session
  FR06-LLM-DEEPSEEK-04 DeepSeek 会话状态        GET /api/v1/deepseek/session/status

  FR06-LLM-QWEN-01    Qwen Provider 分析       /api/v1/qwen/analyze
  FR06-LLM-QWEN-02    Qwen 批量分析             /api/v1/qwen/analyze/batch
  FR06-LLM-QWEN-03    Qwen 会话管理             DELETE /api/v1/qwen/session
  FR06-LLM-QWEN-04    Qwen 会话状态             GET /api/v1/qwen/session/status

测试策略：
  - 通过 monkeypatch 注入 mock client，屏蔽对真实浏览器/API 的依赖
  - 验证 HTTP 状态码、响应结构、错误处理（503/500）均符合规约
  - 会话状态端点覆盖 initialized=True/False 两种场景
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = [
    pytest.mark.feature("FR-06"),
    pytest.mark.feature("FR06-LLM-WEBAI-01"),
]

# 确保 ai-api 目录在 sys.path 中（main.py 已做相同处理）
_AI_API_PATH = str(Path(__file__).resolve().parents[1] / "ai-api")
if _AI_API_PATH not in sys.path:
    sys.path.insert(0, _AI_API_PATH)


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------

def _make_mock_client(response_text: str = "mock analysis result") -> MagicMock:
    """构造一个满足 *WebClient 接口的异步 mock。"""
    mock = MagicMock()
    mock.analyze = AsyncMock(return_value={"response": response_text, "elapsed_s": 0.1})
    mock.analyze_batch = AsyncMock(
        return_value=[
            {"code": "600519.SH", "name": "MOUTAI", "response": response_text, "elapsed_s": 0.1}
        ]
    )
    mock.close = AsyncMock()
    mock._ready = True
    return mock


# ===========================================================================
# GEMINI
# ===========================================================================

class TestGeminiAnalyze:
    """FR06-LLM-GEMINI-01：单条分析端点"""

    @pytest.mark.feature("FR06-LLM-GEMINI-01")
    def test_gemini_analyze_returns_200(self, client, monkeypatch):
        mock_client = _make_mock_client("gemini result")
        with patch("gemini_web.router.GeminiWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/gemini/analyze",
                json={"prompt": "分析 600519.SH 今日行情"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"]["response"] == "gemini result"

    @pytest.mark.feature("FR06-LLM-GEMINI-01")
    def test_gemini_analyze_503_on_runtime_error(self, client):
        with patch("gemini_web.router.GeminiWebClient.get", new=AsyncMock(side_effect=RuntimeError("Chrome not ready"))):
            resp = client.post(
                "/api/v1/gemini/analyze",
                json={"prompt": "analyze something"},
            )
        assert resp.status_code == 503
        body = resp.json()
        assert body.get("success") is False
        assert "Chrome not ready" in (body.get("error") or body.get("error_message") or body.get("detail", ""))

    @pytest.mark.feature("FR06-LLM-GEMINI-01")
    def test_gemini_analyze_500_on_generic_error(self, client):
        mock_client = _make_mock_client()
        mock_client.analyze = AsyncMock(side_effect=Exception("unexpected"))
        with patch("gemini_web.router.GeminiWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/gemini/analyze",
                json={"prompt": "analyze something"},
            )
        assert resp.status_code == 500


class TestGeminiAnalyzeBatch:
    """FR06-LLM-GEMINI-02：批量分析端点"""

    @pytest.mark.feature("FR06-LLM-GEMINI-02")
    def test_gemini_batch_returns_count(self, client):
        mock_client = _make_mock_client()
        with patch("gemini_web.router.GeminiWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/gemini/analyze/batch",
                json={
                    "stocks": [
                        {"code": "600519.SH", "name": "MOUTAI", "prompt": "分析该股"},
                        {"code": "000001.SZ", "name": "PINGAN", "prompt": "分析该股"},
                    ]
                },
            )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "count" in data
        assert "results" in data

    @pytest.mark.feature("FR06-LLM-GEMINI-02")
    def test_gemini_batch_503_on_runtime_error(self, client):
        with patch("gemini_web.router.GeminiWebClient.get", new=AsyncMock(side_effect=RuntimeError("no session"))):
            resp = client.post(
                "/api/v1/gemini/analyze/batch",
                json={"stocks": [{"code": "600519.SH", "name": "MT", "prompt": "p"}]},
            )
        assert resp.status_code == 503


class TestGeminiSession:
    """FR06-LLM-GEMINI-03/-04：会话管理与状态"""

    @pytest.mark.feature("FR06-LLM-GEMINI-03")
    def test_gemini_session_close_returns_closed_true(self, client):
        mock_inst = _make_mock_client()
        with patch("gemini_web.router.GeminiWebClient") as MockClass:
            MockClass._instance = mock_inst
            resp = client.delete("/api/v1/gemini/session")
        assert resp.status_code == 200
        assert resp.json()["data"]["closed"] is True

    @pytest.mark.feature("FR06-LLM-GEMINI-04")
    def test_gemini_session_status_initialized(self, client):
        mock_inst = MagicMock()
        mock_inst._ready = True
        with patch("gemini_web.router.GeminiWebClient") as MockClass:
            MockClass._instance = mock_inst
            resp = client.get("/api/v1/gemini/session/status")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["initialized"] is True
        assert data["ready"] is True

    @pytest.mark.feature("FR06-LLM-GEMINI-04")
    def test_gemini_session_status_not_initialized(self, client):
        with patch("gemini_web.router.GeminiWebClient") as MockClass:
            MockClass._instance = None
            resp = client.get("/api/v1/gemini/session/status")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["initialized"] is False
        assert data["ready"] is False


# ===========================================================================
# CHATGPT
# ===========================================================================

class TestChatGPTAnalyze:
    """FR06-LLM-CHATGPT-01：单条分析端点"""

    @pytest.mark.feature("FR06-LLM-CHATGPT-01")
    def test_chatgpt_analyze_returns_200(self, client):
        mock_client = _make_mock_client("chatgpt result")
        with patch("chatgpt_web.router.ChatGPTWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/chatgpt/analyze",
                json={"prompt": "分析 600519.SH"},
            )
        assert resp.status_code == 200
        assert resp.json()["data"]["response"] == "chatgpt result"

    @pytest.mark.feature("FR06-LLM-CHATGPT-01")
    def test_chatgpt_analyze_503_on_runtime_error(self, client):
        with patch("chatgpt_web.router.ChatGPTWebClient.get", new=AsyncMock(side_effect=RuntimeError("session lost"))):
            resp = client.post("/api/v1/chatgpt/analyze", json={"prompt": "test"})
        assert resp.status_code == 503

    @pytest.mark.feature("FR06-LLM-CHATGPT-01")
    def test_chatgpt_analyze_500_on_generic_error(self, client):
        mock_client = _make_mock_client()
        mock_client.analyze = AsyncMock(side_effect=Exception("oops"))
        with patch("chatgpt_web.router.ChatGPTWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post("/api/v1/chatgpt/analyze", json={"prompt": "test"})
        assert resp.status_code == 500


class TestChatGPTBatch:
    """FR06-LLM-CHATGPT-02：批量分析端点"""

    @pytest.mark.feature("FR06-LLM-CHATGPT-02")
    def test_chatgpt_batch_returns_count_and_results(self, client):
        mock_client = _make_mock_client()
        with patch("chatgpt_web.router.ChatGPTWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/chatgpt/analyze/batch",
                json={"stocks": [{"code": "600519.SH", "name": "MOUTAI", "prompt": "p"}]},
            )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["count"] >= 1
        assert isinstance(data["results"], list)


class TestChatGPTSession:
    """FR06-LLM-CHATGPT-03/-04：会话管理与状态"""

    @pytest.mark.feature("FR06-LLM-CHATGPT-03")
    def test_chatgpt_session_close(self, client):
        mock_inst = _make_mock_client()
        with patch("chatgpt_web.router.ChatGPTWebClient") as MockClass:
            MockClass._instance = mock_inst
            resp = client.delete("/api/v1/chatgpt/session")
        assert resp.status_code == 200
        assert resp.json()["data"]["closed"] is True

    @pytest.mark.feature("FR06-LLM-CHATGPT-04")
    def test_chatgpt_session_status_fields(self, client):
        with patch("chatgpt_web.router.ChatGPTWebClient") as MockClass:
            MockClass._instance = None
            resp = client.get("/api/v1/chatgpt/session/status")
        assert resp.status_code == 200
        assert "initialized" in resp.json()["data"]
        assert "ready" in resp.json()["data"]


# ===========================================================================
# DEEPSEEK
# ===========================================================================

class TestDeepSeekAnalyze:
    """FR06-LLM-DEEPSEEK-01：单条分析端点"""

    @pytest.mark.feature("FR06-LLM-DEEPSEEK-01")
    def test_deepseek_analyze_returns_200(self, client):
        mock_client = _make_mock_client("deepseek result")
        with patch("deepseek_web.router.DeepSeekWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/deepseek/analyze",
                json={"prompt": "分析 000001.SZ"},
            )
        assert resp.status_code == 200
        assert resp.json()["data"]["response"] == "deepseek result"

    @pytest.mark.feature("FR06-LLM-DEEPSEEK-01")
    def test_deepseek_analyze_503_on_runtime_error(self, client):
        with patch("deepseek_web.router.DeepSeekWebClient.get", new=AsyncMock(side_effect=RuntimeError("init failed"))):
            resp = client.post("/api/v1/deepseek/analyze", json={"prompt": "test"})
        assert resp.status_code == 503

    @pytest.mark.feature("FR06-LLM-DEEPSEEK-01")
    def test_deepseek_analyze_500_on_generic_error(self, client):
        mock_client = _make_mock_client()
        mock_client.analyze = AsyncMock(side_effect=Exception("network timeout"))
        with patch("deepseek_web.router.DeepSeekWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post("/api/v1/deepseek/analyze", json={"prompt": "test"})
        assert resp.status_code == 500


class TestDeepSeekBatch:
    """FR06-LLM-DEEPSEEK-02：批量分析端点"""

    @pytest.mark.feature("FR06-LLM-DEEPSEEK-02")
    def test_deepseek_batch_returns_count_and_results(self, client):
        mock_client = _make_mock_client()
        with patch("deepseek_web.router.DeepSeekWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/deepseek/analyze/batch",
                json={"stocks": [{"code": "000001.SZ", "name": "PINGAN", "prompt": "p"}]},
            )
        assert resp.status_code == 200
        assert "count" in resp.json()["data"]


class TestDeepSeekSession:
    """FR06-LLM-DEEPSEEK-03/-04：会话管理与状态"""

    @pytest.mark.feature("FR06-LLM-DEEPSEEK-03")
    def test_deepseek_session_close(self, client):
        mock_inst = _make_mock_client()
        with patch("deepseek_web.router.DeepSeekWebClient") as MockClass:
            MockClass._instance = mock_inst
            resp = client.delete("/api/v1/deepseek/session")
        assert resp.status_code == 200
        assert resp.json()["data"]["closed"] is True

    @pytest.mark.feature("FR06-LLM-DEEPSEEK-04")
    def test_deepseek_session_status_fields(self, client):
        with patch("deepseek_web.router.DeepSeekWebClient") as MockClass:
            MockClass._instance = None
            resp = client.get("/api/v1/deepseek/session/status")
        assert resp.status_code == 200
        assert "initialized" in resp.json()["data"]


# ===========================================================================
# QWEN
# ===========================================================================

class TestQwenAnalyze:
    """FR06-LLM-QWEN-01：单条分析端点"""

    @pytest.mark.feature("FR06-LLM-QWEN-01")
    def test_qwen_analyze_returns_200(self, client):
        mock_client = _make_mock_client("qwen result")
        with patch("qwen_web.router.QwenWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/qwen/analyze",
                json={"prompt": "分析 601318.SH"},
            )
        assert resp.status_code == 200
        assert resp.json()["data"]["response"] == "qwen result"

    @pytest.mark.feature("FR06-LLM-QWEN-01")
    def test_qwen_analyze_503_on_runtime_error(self, client):
        with patch("qwen_web.router.QwenWebClient.get", new=AsyncMock(side_effect=RuntimeError("Qwen not ready"))):
            resp = client.post("/api/v1/qwen/analyze", json={"prompt": "test"})
        assert resp.status_code == 503

    @pytest.mark.feature("FR06-LLM-QWEN-01")
    def test_qwen_analyze_500_on_generic_error(self, client):
        mock_client = _make_mock_client()
        mock_client.analyze = AsyncMock(side_effect=Exception("timeout"))
        with patch("qwen_web.router.QwenWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post("/api/v1/qwen/analyze", json={"prompt": "test"})
        assert resp.status_code == 500


class TestQwenBatch:
    """FR06-LLM-QWEN-02：批量分析端点"""

    @pytest.mark.feature("FR06-LLM-QWEN-02")
    def test_qwen_batch_returns_count_and_results(self, client):
        mock_client = _make_mock_client()
        with patch("qwen_web.router.QwenWebClient.get", new=AsyncMock(return_value=mock_client)):
            resp = client.post(
                "/api/v1/qwen/analyze/batch",
                json={"stocks": [{"code": "601318.SH", "name": "PINGAN", "prompt": "p"}]},
            )
        assert resp.status_code == 200
        assert "results" in resp.json()["data"]


class TestQwenSession:
    """FR06-LLM-QWEN-03/-04：会话管理与状态"""

    @pytest.mark.feature("FR06-LLM-QWEN-03")
    def test_qwen_session_close(self, client):
        mock_inst = _make_mock_client()
        with patch("qwen_web.router.QwenWebClient") as MockClass:
            MockClass._instance = mock_inst
            resp = client.delete("/api/v1/qwen/session")
        assert resp.status_code == 200
        assert resp.json()["data"]["closed"] is True

    @pytest.mark.feature("FR06-LLM-QWEN-04")
    def test_qwen_session_status_not_initialized(self, client):
        with patch("qwen_web.router.QwenWebClient") as MockClass:
            MockClass._instance = None
            resp = client.get("/api/v1/qwen/session/status")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["initialized"] is False

    @pytest.mark.feature("FR06-LLM-QWEN-04")
    def test_qwen_session_status_initialized(self, client):
        mock_inst = MagicMock()
        mock_inst._ready = True
        with patch("qwen_web.router.QwenWebClient") as MockClass:
            MockClass._instance = mock_inst
            resp = client.get("/api/v1/qwen/session/status")
        assert resp.status_code == 200
        assert resp.json()["data"]["initialized"] is True
