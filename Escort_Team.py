#!/usr/bin/env python3
"""Thin entry point for the Multi-Agent Escort Team.

Delegates to automation.agents.run_team.main().
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from automation.agents.run_team import main  # noqa: E402

if __name__ == "__main__":
    main()
