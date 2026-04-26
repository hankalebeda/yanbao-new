"""Codex orchestration package.

Provides a stable, thin orchestration layer for Codex sub-agent execution.

Architecture:
- mesh.py:  Core engine (provider discovery, health tracking, hedge execution)
- run.py:   Unified task runner (profile loading, structural context, delegation)
- profiles/: Task configs (YAML) defining goals and scopes, not AI instructions
- hourly/manual/mining.py: Backward-compatible thin wrappers

Design principle: pass AI structural boundaries (depth, scopes, topology),
never tactical instructions. Let the AI decide *how* to accomplish goals.
"""
__version__ = "2.0.0"
