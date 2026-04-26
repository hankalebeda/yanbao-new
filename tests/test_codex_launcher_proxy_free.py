from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
OPEN_WITH_PS1 = sorted(REPO_ROOT.glob("open-with-*.ps1"))
OPEN_WITH_CMD = sorted(REPO_ROOT.glob("open-with-*.cmd"))
LAUNCHER_FILES = [
    p for p in OPEN_WITH_PS1 + [
        REPO_ROOT / "probe-with-925214.ps1",
        REPO_ROOT / "ai-api" / "codex" / "run_codex_provider.ps1",
        REPO_ROOT / "scripts" / "Start-CodexWorkspace.ps1",
    ]
    if p.exists()
]
BANNED_TOKENS = (
    "Set-CodexRelayProxy",
    "ProxyMode",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "10808",
    "proxy_policy",
)


def test_proxy_override_files_are_removed():
    assert not (REPO_ROOT / "scripts" / "Set-CodexRelayProxy.ps1").exists()
    assert not (REPO_ROOT / "ai-api" / "codex" / "proxy_policy.json").exists()


def test_launcher_scripts_do_not_manage_proxy_env():
    for path in LAUNCHER_FILES:
        text = path.read_text(encoding="utf-8")
        for token in BANNED_TOKENS:
            assert token not in text, f"{token} should not appear in {path}"


def test_open_with_wrappers_use_vscode_strategy_helper():
    for path in OPEN_WITH_PS1:
        text = path.read_text(encoding="utf-8")
        assert "Start-CodexWorkspace" in text
        assert "PROXY_STRATEGY=VSCode" not in text

    helper_text = (REPO_ROOT / "scripts" / "Start-CodexWorkspace.ps1").read_text(
        encoding="utf-8"
    )
    assert "PROXY_STRATEGY=VSCode" in helper_text
    assert "Start-Process" in helper_text


def test_cmd_wrappers_use_no_profile_powershell():
    expected_cmds = [p for p in OPEN_WITH_CMD + [REPO_ROOT / "probe-with-925214.cmd"] if p.exists()]
    for path in expected_cmds:
        text = path.read_text(encoding="utf-8").strip()
        assert text.startswith("@echo off\npowershell -NoProfile -ExecutionPolicy Bypass -File ")


def test_newapi_launcher_supports_optional_api_probe():
    text = (REPO_ROOT / "open-with-newapi.ps1").read_text(encoding="utf-8")
    assert "[switch]$TestApi" in text
    assert "function Test-NewApiResponses" in text
    assert "API_TEST" in text


def test_newapi_launcher_keeps_stable_first_model_chain():
    text = (REPO_ROOT / "open-with-newapi.ps1").read_text(encoding="utf-8")
    assert '$reviewModel = "gpt-5.3-codex"' in text
    assert '$fallbackModels = @($primaryModel, $reviewModel, "gpt-5.2")' in text
    assert "fallback_models = [\"$($fallbackModels -join '\", \"')\"]" in text
    assert "FALLBACK_MODELS=$($fallbackModels -join ',')" in text


def test_marybrown_launcher_supports_optional_api_probe():
    path = REPO_ROOT / "open-with-marybrown.ps1"
    if not path.exists():
        import pytest
        pytest.skip("open-with-marybrown.ps1 not present")
    text = path.read_text(encoding="utf-8")
    assert "[switch]$TestApi" in text
    assert "function Test-MarybrownResponses" in text
    assert 'name = "marybrown.dpdns.org"' in text


def test_vscode_helper_prefers_code_cmd_on_windows():
    text = (REPO_ROOT / "scripts" / "Start-CodexWorkspace.ps1").read_text(
        encoding="utf-8"
    )
    assert "bin\\code.cmd" in text
    assert "Get-Command code.cmd" in text
