from __future__ import annotations

import json
import subprocess
from pathlib import Path



def test_walkforward_backtest_handles_empty_trade_range(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "walkforward_backtest.py"
    output_json = tmp_path / "walkforward-empty.json"

    result = subprocess.run(
        [
            "python",
            str(script),
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--stock-codes",
            "600000.SH",
            "--capital-tier",
            "10w",
            "--output-json",
            str(output_json),
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=90,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert output_json.exists()

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["records"] == []
    assert payload["stats"]["closed_count"] == 0
    assert payload["stats"]["win_rate"] == 0
    assert payload["stats"]["total_pnl_net"] == 0
    assert payload["stats"]["pnl_ratio"] is None
    assert payload["stats"]["annualized_pct"] == 0
