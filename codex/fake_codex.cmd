@echo off
REM Fake Codex CLI wrapper - calls fake_codex.py directly
REM Used as CODEX_CLI_OVERRIDE to bypass auth issues with real codex.exe
REM Passes all arguments to Python script
d:\yanbao\.venv\Scripts\python.exe "%~dp0fake_codex.py" %*
