from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.core.db import SessionLocal, build_engine
from app.services.runtime_anchor_service import RuntimeAnchorService


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "app.db"
DEFAULT_CHECK_STATE_PATH = REPO_ROOT / "check_state.py"
DEFAULT_PROBE_PATHS = (
    "/health",
    "/api/v1/features",
    "/api/v1/home",
    "/api/v1/market/state",
    "/api/v1/reports?limit=3",
    "/api/v1/dashboard/stats?window_days=7",
)


@dataclass(slots=True)
class ProbeSummary:
    path: str
    status_code: int | None
    ok: bool
    payload: Any = None
    error: str | None = None


@dataclass(slots=True)
class RuntimeSentinelState:
    name: str
    ok: bool
    blocked_external: bool = False
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TruthSnapshot:
    generated_at: str
    check_state_stdout: str
    active_task_count: int | None
    published_report_count: int | None
    sqlite: dict[str, Any]
    probes: dict[str, ProbeSummary]
    anchors: dict[str, Any]
    sentinels: dict[str, RuntimeSentinelState]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "check_state_stdout": self.check_state_stdout,
            "active_task_count": self.active_task_count,
            "published_report_count": self.published_report_count,
            "sqlite": self.sqlite,
            "probes": {key: asdict(value) for key, value in self.probes.items()},
            "anchors": self.anchors,
            "sentinels": {key: asdict(value) for key, value in self.sentinels.items()},
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_count(label: str, text: str) -> int | None:
    match = re.search(rf"{re.escape(label)}:\s*(\d+)", text)
    return int(match.group(1)) if match else None


def run_check_state(
    *,
    repo_root: Path = REPO_ROOT,
    python_executable: str = sys.executable,
    script_path: Path = DEFAULT_CHECK_STATE_PATH,
) -> tuple[str, int | None, int | None]:
    result = subprocess.run(
        [python_executable, str(script_path)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    stdout = (result.stdout or "").strip()
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"check_state_failed:{result.returncode}:{stderr or stdout}")
    return stdout, _parse_count("Active tasks", stdout), _parse_count("Total published", stdout)


def _sqlite_scalar(cursor: sqlite3.Cursor, sql: str) -> Any:
    row = cursor.execute(sql).fetchone()
    return row[0] if row else None


def query_sqlite_truth(database_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    resolved_database_path = database_path.resolve()
    if not resolved_database_path.exists():
        raise RuntimeError(f"sqlite_database_missing:{resolved_database_path}")
    conn = sqlite3.connect(f"{resolved_database_path.as_uri()}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        return {
            "report_total": _sqlite_scalar(cur, "SELECT COUNT(*) FROM report"),
            "report_published": _sqlite_scalar(
                cur,
                "SELECT COUNT(*) FROM report WHERE published=1 AND COALESCE(is_deleted, 0)=0",
            ),
            "report_alive": _sqlite_scalar(
                cur,
                "SELECT COUNT(*) FROM report WHERE COALESCE(is_deleted, 0)=0",
            ),
            "published_buy": _sqlite_scalar(
                cur,
                "SELECT COUNT(*) FROM report WHERE published=1 AND COALESCE(is_deleted, 0)=0 AND recommendation='BUY'",
            ),
            "settlement_total": _sqlite_scalar(cur, "SELECT COUNT(*) FROM settlement_result"),
            "sim_position_open": _sqlite_scalar(
                cur,
                "SELECT COUNT(*) FROM sim_position WHERE position_status='OPEN'",
            ),
            "pool_latest_trade_date": _sqlite_scalar(cur, "SELECT MAX(trade_date) FROM stock_pool_snapshot"),
            "kline_latest_trade_date": _sqlite_scalar(cur, "SELECT MAX(trade_date) FROM kline_daily"),
            "market_state_latest_trade_date": _sqlite_scalar(cur, "SELECT MAX(trade_date) FROM market_state_cache"),
            "strategy_snapshot_latest_date": _sqlite_scalar(
                cur,
                "SELECT MAX(snapshot_date) FROM strategy_metric_snapshot",
            ),
            "baseline_snapshot_latest_date": _sqlite_scalar(
                cur,
                "SELECT MAX(snapshot_date) FROM baseline_metric_snapshot",
            ),
            "report_data_usage_count": _sqlite_scalar(cur, "SELECT COUNT(*) FROM report_data_usage"),
            "report_citation_count": _sqlite_scalar(cur, "SELECT COUNT(*) FROM report_citation"),
            "published_ok_nonterminal_task_count": _sqlite_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM report_generation_task AS task
                WHERE task.status IN ('Pending', 'Processing', 'Suspended')
                  AND EXISTS (
                      SELECT 1
                      FROM report
                      WHERE report.trade_date = task.trade_date
                        AND report.stock_code = task.stock_code
                        AND report.published = 1
                        AND COALESCE(report.is_deleted, 0) = 0
                        AND COALESCE(LOWER(report.quality_flag), 'ok') = 'ok'
                  )
                """,
            ),
        }
    finally:
        conn.close()


def _default_app():
    from app.main import app

    return app


def probe_application(
    *,
    app: Any | None = None,
    probe_paths: tuple[str, ...] = DEFAULT_PROBE_PATHS,
) -> dict[str, ProbeSummary]:
    target_app = app or _default_app()
    summaries: dict[str, ProbeSummary] = {}
    with TestClient(target_app, base_url="http://127.0.0.1") as client:
        for path in probe_paths:
            try:
                response = client.get(path, headers={"Host": "127.0.0.1"})
                payload: Any
                try:
                    payload = response.json()
                except Exception:
                    payload = response.text[:500]
                summaries[path] = ProbeSummary(
                    path=path,
                    status_code=response.status_code,
                    ok=200 <= response.status_code < 300,
                    payload=payload,
                    error=None,
                )
            except Exception as exc:
                summaries[path] = ProbeSummary(
                    path=path,
                    status_code=None,
                    ok=False,
                    payload=None,
                    error=str(exc),
                )
    return summaries


def _build_session_factory(database_url: str | None) -> sessionmaker:
    if not database_url:
        return SessionLocal
    engine = build_engine(database_url)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def collect_anchor_truth(
    *,
    session_factory: Callable[[], Session] | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    factory = session_factory or _build_session_factory(database_url)
    db = factory()
    try:
        service = RuntimeAnchorService(db)
        public_pool = service.public_pool_snapshot()
        runtime_market_state = service.runtime_market_state_row() or {}
        latest_public_market_state = service.latest_public_market_state_row() or {}
        return {
            "runtime_trade_date": service.runtime_trade_date(),
            "latest_published_report_trade_date": service.latest_published_report_trade_date(),
            "latest_complete_public_batch_trade_date": service.latest_complete_public_batch_trade_date(),
            "public_pool_trade_date": public_pool.get("public_pool_trade_date"),
            "public_pool_size": public_pool.get("pool_size"),
            "runtime_market_state": dict(runtime_market_state),
            "latest_public_market_state": dict(latest_public_market_state),
            "home_cache_key": list(
                service.home_cache_key(
                    viewer_tier="Free",
                    viewer_role="user",
                    window_days=7,
                )
            ),
        }
    finally:
        db.close()


def _probe_json_data(probes: dict[str, ProbeSummary], path: str) -> dict[str, Any]:
    probe = probes.get(path)
    if not probe or not isinstance(probe.payload, dict):
        return {}
    payload = probe.payload
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def derive_runtime_sentinels(
    *,
    sqlite_truth: dict[str, Any],
    probes: dict[str, ProbeSummary],
    anchors: dict[str, Any],
) -> dict[str, RuntimeSentinelState]:
    home_data = _probe_json_data(probes, "/api/v1/home")
    market_state_data = _probe_json_data(probes, "/api/v1/market/state")
    reports_data = _probe_json_data(probes, "/api/v1/reports?limit=3")
    dashboard_data = _probe_json_data(probes, "/api/v1/dashboard/stats?window_days=7")

    sentinels: dict[str, RuntimeSentinelState] = {}
    sentinels["public_pool_snapshot_available"] = RuntimeSentinelState(
        name="public_pool_snapshot_available",
        ok=bool(anchors.get("public_pool_trade_date")) and int(anchors.get("public_pool_size") or 0) > 0,
        details={
            "public_pool_trade_date": anchors.get("public_pool_trade_date"),
            "public_pool_size": anchors.get("public_pool_size"),
            "home_pool_size": home_data.get("pool_size"),
        },
    )
    sentinels["truth_layer_usage_nonzero"] = RuntimeSentinelState(
        name="truth_layer_usage_nonzero",
        ok=int(sqlite_truth.get("report_data_usage_count") or 0) > 0,
        details={"report_data_usage_count": sqlite_truth.get("report_data_usage_count")},
    )
    sentinels["runtime_market_state_available"] = RuntimeSentinelState(
        name="runtime_market_state_available",
        ok=bool(anchors.get("runtime_trade_date"))
        and bool(market_state_data.get("trade_date"))
        and bool(market_state_data.get("market_state")),
        details={
            "runtime_trade_date": anchors.get("runtime_trade_date"),
            "probe_trade_date": market_state_data.get("trade_date"),
            "market_state": market_state_data.get("market_state"),
        },
    )
    sentinels["published_reports_nonzero"] = RuntimeSentinelState(
        name="published_reports_nonzero",
        ok=int(sqlite_truth.get("report_published") or 0) > 0,
        details={"report_published": sqlite_truth.get("report_published")},
    )
    sentinels["settlement_rows_nonzero"] = RuntimeSentinelState(
        name="settlement_rows_nonzero",
        ok=int(sqlite_truth.get("settlement_total") or 0) > 0,
        details={"settlement_total": sqlite_truth.get("settlement_total")},
    )
    sentinels["sim_positions_nonzero"] = RuntimeSentinelState(
        name="sim_positions_nonzero",
        ok=int(sqlite_truth.get("sim_position_open") or 0) > 0,
        details={"sim_position_open": sqlite_truth.get("sim_position_open")},
    )
    sentinels["public_read_model_nonempty"] = RuntimeSentinelState(
        name="public_read_model_nonempty",
        ok=(
            probes.get("/api/v1/home", ProbeSummary("", None, False)).ok
            and probes.get("/api/v1/reports?limit=3", ProbeSummary("", None, False)).ok
            and int(reports_data.get("total") or 0) > 0
        ),
        details={
            "home_ok": probes.get("/api/v1/home", ProbeSummary("", None, False)).ok,
            "reports_ok": probes.get("/api/v1/reports?limit=3", ProbeSummary("", None, False)).ok,
            "reports_total": reports_data.get("total"),
            "dashboard_status": dashboard_data.get("data_status"),
        },
    )
    sentinels["admin_overview_consistent"] = RuntimeSentinelState(
        name="admin_overview_consistent",
        ok=bool(anchors.get("runtime_trade_date")) and int(sqlite_truth.get("report_published") or 0) > 0,
        details={
            "runtime_trade_date": anchors.get("runtime_trade_date"),
            "latest_published_report_trade_date": anchors.get("latest_published_report_trade_date"),
            "report_published": sqlite_truth.get("report_published"),
        },
    )
    missing_complete_public_batch_anchor = not bool(anchors.get("latest_complete_public_batch_trade_date"))
    runtime_history_warnings = ["missing_complete_public_batch_anchor"] if missing_complete_public_batch_anchor else []
    sentinels["runtime_history_repair_consistent"] = RuntimeSentinelState(
        name="runtime_history_repair_consistent",
        ok=int(sqlite_truth.get("report_published") or 0) > 0
        and int(sqlite_truth.get("published_ok_nonterminal_task_count") or 0) == 0,
        details={
            "latest_complete_public_batch_trade_date": anchors.get("latest_complete_public_batch_trade_date"),
            "complete_public_batch_anchor_policy": "warning_only",
            "missing_complete_public_batch_anchor": missing_complete_public_batch_anchor,
            "warnings": runtime_history_warnings,
            "report_published": sqlite_truth.get("report_published"),
            "published_ok_nonterminal_task_count": sqlite_truth.get("published_ok_nonterminal_task_count"),
        },
    )
    sentinels["walkforward_empty_range_cli_returns_empty_stats"] = _walkforward_empty_range_sentinel()
    return sentinels


def _walkforward_empty_range_sentinel() -> RuntimeSentinelState:
    """Verify the walkforward CLI handles a no-trade-day range without fabricating rows."""
    script_path = REPO_ROOT / "scripts" / "walkforward_backtest.py"
    with tempfile.TemporaryDirectory(prefix="ralph-walkforward-") as tmpdir:
        output_path = Path(tmpdir) / "empty-range.json"
        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--start-date",
                "2026-03-14",
                "--end-date",
                "2026-03-15",
                "--stock-codes",
                "600000.SH",
                "--capital-tier",
                "10w",
                "--output-json",
                str(output_path),
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=90,
        )
        details: dict[str, Any] = {
            "returncode": result.returncode,
            "stdout_tail": (result.stdout or "")[-500:],
            "stderr_tail": (result.stderr or "")[-500:],
            "output_exists": output_path.exists(),
        }
        payload: dict[str, Any] = {}
        if output_path.exists():
            try:
                payload = json.loads(output_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                details["json_error"] = str(exc)
        stats = payload.get("stats") if isinstance(payload, dict) else {}
        records = payload.get("records") if isinstance(payload, dict) else None
        ok = (
            result.returncode == 0
            and records == []
            and isinstance(stats, dict)
            and stats.get("closed_count") == 0
            and stats.get("win_rate") == 0
            and stats.get("total_pnl_net") == 0
            and stats.get("pnl_ratio") is None
            and stats.get("annualized_pct") == 0
        )
        details.update(
            {
                "records_count": len(records) if isinstance(records, list) else None,
                "stats": stats if isinstance(stats, dict) else None,
            }
        )
        return RuntimeSentinelState(
            name="walkforward_empty_range_cli_returns_empty_stats",
            ok=ok,
            details=details,
        )


def collect_truth_snapshot(
    *,
    repo_root: Path = REPO_ROOT,
    database_path: Path = DEFAULT_DB_PATH,
    database_url: str | None = None,
    app: Any | None = None,
    probe_paths: tuple[str, ...] = DEFAULT_PROBE_PATHS,
    session_factory: Callable[[], Session] | None = None,
) -> TruthSnapshot:
    stdout, active_task_count, published_report_count = run_check_state(repo_root=repo_root)
    sqlite_truth = query_sqlite_truth(database_path)
    probes = probe_application(app=app, probe_paths=probe_paths)
    anchors = collect_anchor_truth(session_factory=session_factory, database_url=database_url)
    sentinels = derive_runtime_sentinels(sqlite_truth=sqlite_truth, probes=probes, anchors=anchors)
    return TruthSnapshot(
        generated_at=_utc_now_iso(),
        check_state_stdout=stdout,
        active_task_count=active_task_count,
        published_report_count=published_report_count,
        sqlite=sqlite_truth,
        probes=probes,
        anchors=anchors,
        sentinels=sentinels,
    )


def truth_snapshot_json(snapshot: TruthSnapshot) -> str:
    return json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2)
