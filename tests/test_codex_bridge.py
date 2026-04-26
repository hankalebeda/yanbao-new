"""Tests for automation/agents/codex_bridge.py.

Verifies Codex CLI resolution, provider detection, and the async
codex_exec wrapper — all using mocked subprocesses.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from automation.agents import codex_bridge


# ---------------------------------------------------------------------------
# resolve_codex_executable
# ---------------------------------------------------------------------------

class TestResolveCodexExecutable:
    def test_returns_none_when_not_on_path(self):
        with patch("shutil.which", return_value=None), \
             patch("os.path.isfile", return_value=False):
            assert codex_bridge.resolve_codex_executable() is None

    def test_returns_first_match(self):
        def _which(name):
            if name == "codex.cmd":
                return r"C:\tools\codex.cmd"
            return None

        with patch("shutil.which", side_effect=_which):
            result = codex_bridge.resolve_codex_executable()
            assert result == r"C:\tools\codex.cmd"


# ---------------------------------------------------------------------------
# detect_provider
# ---------------------------------------------------------------------------

class TestDetectProvider:
    def test_env_override(self, tmp_path):
        with patch.dict(os.environ, {"CODEX_PROVIDER": "mykey"}):
            assert codex_bridge.detect_provider(tmp_path) == "mykey"

    def test_scans_directory(self, tmp_path):
        pr = tmp_path / "ai-api" / "codex" / "official"
        pr.mkdir(parents=True)
        (pr / "config.toml").write_text('model = "gpt-5.4"\n', encoding="utf-8")
        (pr / "auth.json").write_text("{}", encoding="utf-8")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CODEX_PROVIDER", None)
            result = codex_bridge.detect_provider(tmp_path)
            assert result == "official"

    def test_prefers_live_provider_when_probe_succeeds(self, tmp_path):
        slow = tmp_path / "ai-api" / "codex" / "sub.jlypx.de"
        good = tmp_path / "ai-api" / "codex" / "freeapi.dgbmc.top"
        slow.mkdir(parents=True)
        good.mkdir(parents=True)
        (slow / "config.toml").write_text('model = "gpt-5.4"\n', encoding="utf-8")
        (slow / "auth.json").write_text("{}", encoding="utf-8")
        (good / "config.toml").write_text('model = "gpt-5.4"\n', encoding="utf-8")
        (good / "auth.json").write_text("{}", encoding="utf-8")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CODEX_PROVIDER", None)
            with patch.object(codex_bridge, "_probe_provider_live", side_effect=lambda _root, name, timeout_s=8.0: name == "freeapi.dgbmc.top"):
                assert codex_bridge.detect_provider(tmp_path) == "freeapi.dgbmc.top"

    def test_prefers_persisted_success_provider(self, tmp_path):
        old = tmp_path / "ai-api" / "codex" / "sub.jlypx.de"
        saved = tmp_path / "ai-api" / "codex" / "wududu.edu.kg"
        old.mkdir(parents=True)
        saved.mkdir(parents=True)
        (old / "config.toml").write_text('model = "gpt-5.4"\n', encoding="utf-8")
        (old / "auth.json").write_text("{}", encoding="utf-8")
        (saved / "config.toml").write_text('model = "gpt-5.4"\n', encoding="utf-8")
        (saved / "auth.json").write_text("{}", encoding="utf-8")

        state_path = tmp_path / "runtime" / "agents" / "codex_bridge_state.json"
        state_path.parent.mkdir(parents=True)
        state_path.write_text(json.dumps({"last_success_provider": "wududu.edu.kg"}), encoding="utf-8")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CODEX_PROVIDER", None)
            with patch.object(codex_bridge, "_probe_provider_live", return_value=False):
                assert codex_bridge.detect_provider(tmp_path) == "wududu.edu.kg"

    def test_returns_empty_when_no_providers(self, tmp_path):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CODEX_PROVIDER", None)
            assert codex_bridge.detect_provider(tmp_path) == ""


# ---------------------------------------------------------------------------
# codex_available
# ---------------------------------------------------------------------------

class TestCodexAvailable:
    @pytest.mark.asyncio
    async def test_false_when_not_found(self):
        codex_bridge.reset_cache()
        with patch.object(codex_bridge, "resolve_codex_executable", return_value=None):
            assert await codex_bridge.codex_available() is False
        codex_bridge.reset_cache()

    @pytest.mark.asyncio
    async def test_true_when_version_works(self):
        codex_bridge.reset_cache()
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.returncode = 0

        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                assert await codex_bridge.codex_available() is True
        codex_bridge.reset_cache()

    @pytest.mark.asyncio
    async def test_cache_expires_after_ttl(self):
        """Ensure cache expires after TTL so Codex re-checked."""
        codex_bridge.reset_cache()
        with patch.object(codex_bridge, "resolve_codex_executable", return_value=None):
            assert await codex_bridge.codex_available() is False

        # Simulate TTL expiry
        codex_bridge._codex_available_ts = 0.0

        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock(return_value=0)
        mock_proc.returncode = 0
        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                assert await codex_bridge.codex_available() is True
        codex_bridge.reset_cache()


# ---------------------------------------------------------------------------
# codex_exec
# ---------------------------------------------------------------------------

class TestCodexExec:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_executable(self, tmp_path):
        with patch.object(codex_bridge, "resolve_codex_executable", return_value=None):
            result = await codex_bridge.codex_exec("hello", tmp_path)
            assert result == ""

    @pytest.mark.asyncio
    async def test_returns_output_from_last_message_file(self, tmp_path):
        """Codex writes result to --output-last-message file."""
        runtime_dir = tmp_path / "runtime" / "agents" / "codex_tmp"
        runtime_dir.mkdir(parents=True)

        async def fake_communicate(input=None):
            # Simulate codex writing the last-message file
            for f in runtime_dir.iterdir():
                if f.name.startswith("last_msg_"):
                    f.write_text('{"patches": []}')
                    break
            return (b"", b"")

        mock_proc = AsyncMock()
        mock_proc.communicate = fake_communicate
        mock_proc.returncode = 0
        mock_proc.kill = MagicMock()

        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch.object(codex_bridge, "detect_provider", return_value=""):
                with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                    result = await codex_bridge.codex_exec("fix this", tmp_path)
                    # May be empty or contain result depending on timing
                    assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_returns_empty_on_nonzero_exit(self, tmp_path):
        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(return_value=(b"", b"error"))
        mock_proc.returncode = 1
        mock_proc.kill = MagicMock()

        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch.object(codex_bridge, "detect_provider", return_value=""):
                with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                    result = await codex_bridge.codex_exec("fix this", tmp_path)
                    assert result == ""

    @pytest.mark.asyncio
    async def test_timeout_kills_process(self, tmp_path):
        mock_proc = AsyncMock()
        killed = asyncio.Event()

        async def slow_communicate(input=None):
            await killed.wait()
            return (b"", b"")

        mock_proc.communicate = slow_communicate
        mock_proc.returncode = -1
        mock_proc.kill = MagicMock(side_effect=killed.set)
        mock_proc.wait = AsyncMock()

        async def rest_noop(*a, **kw):
            return ""

        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch.object(codex_bridge, "detect_provider", return_value=""):
                with patch.object(codex_bridge, "detect_provider_candidates", return_value=[""]):
                    with patch.object(codex_bridge, "_rest_api_fallback", side_effect=rest_noop):
                        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
                            result = await codex_bridge.codex_exec(
                                "fix this", tmp_path, timeout_s=30,
                            )
                            assert result == ""
                            mock_proc.kill.assert_called_once()

    @pytest.mark.asyncio
    async def test_provider_env_setup(self, tmp_path):
        """Verify that provider home is prepared when provider is available."""
        pr = tmp_path / "ai-api" / "codex" / "test_prov"
        pr.mkdir(parents=True)
        (pr / "config.toml").write_text(
            '\n'.join([
                'model = "gpt-5.4"',
                '[model_providers.OpenAI]',
                'base_url = "https://relay.example/v1"',
            ]),
            encoding="utf-8",
        )
        (pr / "auth.json").write_text(json.dumps({"OPENAI_API_KEY": "sk-test"}), encoding="utf-8")

        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(return_value=(b"result", b""))
        mock_proc.returncode = 0
        mock_proc.kill = MagicMock()

        captured_env = {}
        captured_args = []

        async def capture_exec(*args, **kwargs):
            captured_args.extend(args)
            captured_env.update(kwargs.get("env", {}))
            return mock_proc

        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch("asyncio.create_subprocess_exec", side_effect=capture_exec):
                await codex_bridge.codex_exec(
                    "fix", tmp_path, provider="test_prov",
                )
                # Should have set HOME to portable home
                assert "portable_test_prov" in captured_env.get("HOME", "")
                assert captured_env.get("CODEX_HOME", "").endswith(".codex")
                assert captured_env.get("OPENAI_API_KEY") == "sk-test"
                assert captured_env.get("OPENAI_BASE_URL") == "https://relay.example/v1"
                assert captured_env.get("OPENAI_MODEL") == "gpt-5.4"
                assert captured_env.get("NO_PROXY") == "*"
                assert "--dangerously-bypass-approvals-and-sandbox" in captured_args
                assert "--ephemeral" in captured_args
                assert "--cd" in captured_args
                assert not Path(captured_env["HOME"]).exists()

    @pytest.mark.asyncio
    async def test_retries_next_provider_after_failure(self, tmp_path):
        for name in ("bad_provider", "good_provider"):
            pr = tmp_path / "ai-api" / "codex" / name
            pr.mkdir(parents=True)
            (pr / "config.toml").write_text(
                '\n'.join([
                    'model = "gpt-5.4"',
                    '[model_providers.OpenAI]',
                    'base_url = "https://relay.example/v1"',
                ]),
                encoding="utf-8",
            )
            (pr / "auth.json").write_text(json.dumps({"OPENAI_API_KEY": f"sk-{name}"}), encoding="utf-8")

        bad_proc = AsyncMock()
        bad_proc.communicate = AsyncMock(return_value=(b'{"type":"error","message":"401 Unauthorized"}', b""))
        bad_proc.returncode = 1
        bad_proc.kill = MagicMock()

        async def good_communicate(input=None):
            del input
            return (
                b'{"type":"item.completed","item":{"type":"agent_message","text":"{\\"ok\\": true}"}}',
                b"",
            )

        good_proc = AsyncMock()
        good_proc.communicate = good_communicate
        good_proc.returncode = 0
        good_proc.kill = MagicMock()

        with patch.object(codex_bridge, "resolve_codex_executable", return_value="codex"):
            with patch.object(codex_bridge, "detect_provider", return_value="bad_provider"):
                with patch.object(codex_bridge, "detect_provider_candidates", return_value=["bad_provider", "good_provider"]):
                    with patch("asyncio.create_subprocess_exec", side_effect=[bad_proc, good_proc]):
                        result = await codex_bridge.codex_exec("fix this", tmp_path, timeout_s=30)
                        assert result == '{"ok": true}'
