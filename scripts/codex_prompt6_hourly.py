#!/usr/bin/env python3
"""Thin wrapper -- delegates to codex.hourly."""
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

if __name__ == "__main__":
    from codex.hourly import main

    raise SystemExit(main())

from codex import hourly  # noqa: E402

sys.modules[__name__] = hourly  # type: ignore[assignment]
