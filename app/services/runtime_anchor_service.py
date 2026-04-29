from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

from sqlalchemy.orm import Session

from app.models import StockPoolRefreshTask
from app.services import ssot_read_model as shared
from app.services.stock_pool import (
    evaluate_public_task_eligibility,
    get_public_pool_view,
    get_trade_date_kline_coverage_summary,
)


@dataclass(frozen=True)
class RuntimeAnchorVersions:
    public_runtime_version: str
    public_snapshot_version: str


class RuntimeAnchorService:
    def __init__(self, db: Session):
        self.db = db
        self._cache: dict[str, Any] = {}

    def _memoize(self, key: str, factory):
        if key not in self._cache:
            self._cache[key] = factory()
        return self._cache[key]

    def runtime_anchor_ceiling_trade_date(self) -> str:
        return shared._runtime_anchor_ceiling_trade_date()

    def filter_runtime_anchor_dates(self, values: set[str] | list[str]) -> list[str]:
        return shared._filter_runtime_anchor_dates(values)

    def latest_published_report_trade_date(self) -> str | None:
        return self._memoize("latest_published_report_trade_date", lambda: shared._latest_published_report_trade_date(self.db))

    def latest_public_market_state_row(self) -> Mapping[str, Any] | None:
        def factory():
            anchor_ceiling = self.runtime_anchor_ceiling_trade_date()
            return shared._execute_mappings(
                self.db,
                """
                SELECT trade_date, reference_date, market_state, cache_status, state_reason, market_state_degraded, computed_at
                FROM market_state_cache
                WHERE trade_date <= :anchor_ceiling
                ORDER BY trade_date DESC, computed_at DESC
                LIMIT 1
                """,
                {"anchor_ceiling": anchor_ceiling},
            ).first()

        return self._memoize("latest_public_market_state_row", factory)

    def runtime_market_state_row(self) -> Mapping[str, Any] | None:
        def factory():
            runtime_trade_date = self.runtime_trade_date()
            if not runtime_trade_date:
                return None
            return shared._execute_mappings(
                self.db,
                """
                SELECT trade_date, reference_date, market_state, cache_status, state_reason, market_state_degraded, computed_at
                FROM market_state_cache
                WHERE trade_date = :trade_date
                ORDER BY computed_at DESC
                LIMIT 1
                """,
                {"trade_date": runtime_trade_date},
            ).first()

        return self._memoize("runtime_market_state_row", factory)

    def _distinct_date_values(
        self,
        sql_text: str,
        params: dict[str, Any] | None = None,
    ) -> list[str]:
        rows = shared._execute_mappings(self.db, sql_text, params).all()
        values = {
            shared._iso_date(row.get("trade_date"))
            for row in rows
            if shared._iso_date(row.get("trade_date"))
        }
        return self.filter_runtime_anchor_dates(values)

    def latest_public_pool_task(self) -> StockPoolRefreshTask | None:
        def factory():
            anchor_ceiling = date.fromisoformat(self.runtime_anchor_ceiling_trade_date())
            return (
                self.db.query(StockPoolRefreshTask)
                .filter(
                    StockPoolRefreshTask.trade_date <= anchor_ceiling,
                    StockPoolRefreshTask.status.in_(("COMPLETED", "FALLBACK")),
                    StockPoolRefreshTask.pool_version.isnot(None),
                )
                .order_by(
                    StockPoolRefreshTask.trade_date.desc(),
                    StockPoolRefreshTask.updated_at.desc(),
                    StockPoolRefreshTask.finished_at.desc(),
                    StockPoolRefreshTask.created_at.desc(),
                )
                .first()
            )

        return self._memoize("latest_public_pool_task", factory)

    def public_pool_snapshot(self) -> dict[str, Any]:
        def factory():
            anchor_ceiling = self.runtime_anchor_ceiling_trade_date()
            pool_view = get_public_pool_view(self.db, max_trade_date=anchor_ceiling)
            if pool_view is None:
                return {"pool_view": None, "public_pool_trade_date": None, "pool_size": 0}
            public_pool_trade_date = shared._iso_date(pool_view.task.trade_date)
            filtered_trade_dates = self.filter_runtime_anchor_dates([public_pool_trade_date] if public_pool_trade_date else [])
            if not filtered_trade_dates:
                return {"pool_view": None, "public_pool_trade_date": None, "pool_size": 0}
            return {
                "pool_view": pool_view,
                "public_pool_trade_date": filtered_trade_dates[0],
                "pool_size": len(pool_view.core_rows),
            }

        return self._memoize("public_pool_snapshot", factory)

    def has_complete_public_batch_trace(self, *, trade_date: str) -> bool:
        def factory():
            return shared._has_complete_public_batch_trace(self.db, trade_date=trade_date)

        return self._memoize(f"has_complete_public_batch_trace:{trade_date}", factory)

    def latest_complete_public_batch_trade_date(self) -> str | None:
        def factory():
            candidate_dates = self._distinct_date_values(
                """
                SELECT trade_date
                FROM report
                WHERE published = 1
                  AND is_deleted = 0
                  AND COALESCE(LOWER(quality_flag), 'ok') = 'ok'
                UNION
                SELECT trade_date FROM report_generation_task
                UNION
                SELECT trade_date FROM stock_pool_refresh_task
                """
            )
            for trade_date in candidate_dates:
                if self.has_complete_public_batch_trace(trade_date=trade_date):
                    return trade_date
            return None

        return self._memoize("latest_complete_public_batch_trade_date", factory)

    def _has_complete_stats_snapshot_set(self, *, trade_date: str) -> bool:
        for current_window in shared._DASHBOARD_WINDOWS:
            window_start, window_end = shared._dashboard_window_bounds(
                date.fromisoformat(trade_date),
                current_window,
            )
            facts = shared._load_dashboard_source_facts(
                self.db,
                window_start=window_start,
                window_end=window_end,
                window_days=current_window,
            )
            strategy_rows = shared._load_dashboard_strategy_snapshot_rows(
                self.db,
                snapshot_date=trade_date,
                window_days=current_window,
            )
            baseline_rows = shared._load_dashboard_baseline_snapshot_rows(
                self.db,
                snapshot_date=trade_date,
                window_days=current_window,
            )
            strategy_keys = {str(row.get("strategy_type") or "") for row in strategy_rows}
            baseline_keys = {str(row.get("baseline_type") or "") for row in baseline_rows}
            if strategy_keys != set(shared._DASHBOARD_STRATEGIES) or baseline_keys != set(shared._DASHBOARD_BASELINES):
                return False
            by_strategy_type = shared._build_dashboard_strategy_metrics_from_snapshot_rows(strategy_rows)
            total_sample = sum(
                by_strategy_type[key]["sample_size"] for key in shared._DASHBOARD_STRATEGIES
            )
            if total_sample != facts["settled_count"]:
                return False
            if any(
                by_strategy_type[key]["sample_size"] != facts["settled_count_by_strategy"].get(key, 0)
                for key in shared._DASHBOARD_STRATEGIES
            ):
                return False
            if any(
                str(row.get("data_status") or "").upper() != "READY"
                for row in strategy_rows
                if (shared._to_int(row.get("sample_size")) or 0) > 0
            ):
                return False
        return True

    def latest_complete_stats_trade_date(self, *, max_trade_date: str | None = None) -> str | None:
        cache_key = f"latest_complete_stats_trade_date:{max_trade_date or ''}"
        def factory():
            candidate_dates = self._distinct_date_values(
                """
                SELECT snapshot_date AS trade_date FROM strategy_metric_snapshot
                UNION
                SELECT snapshot_date AS trade_date FROM baseline_metric_snapshot
                """
            )
            if max_trade_date:
                candidate_dates[:] = [
                    trade_date
                    for trade_date in candidate_dates
                    if trade_date <= max_trade_date
                ]
            for trade_date in candidate_dates:
                if not self.has_complete_public_batch_trace(trade_date=trade_date):
                    continue
                if self._has_complete_stats_snapshot_set(trade_date=trade_date):
                    return trade_date
            return None

        return self._memoize(cache_key, factory)

    def latest_stats_snapshot_trade_date(
        self,
        *,
        window_days: int,
        max_trade_date: str | None = None,
    ) -> str | None:
        cache_key = f"latest_stats_snapshot_trade_date:{window_days}:{max_trade_date or ''}"
        def factory():
            candidate_dates = self._distinct_date_values(
                """
                SELECT snapshot_date AS trade_date
                FROM strategy_metric_snapshot
                WHERE window_days = :window_days
                UNION
                SELECT snapshot_date AS trade_date
                FROM baseline_metric_snapshot
                WHERE window_days = :window_days
                """,
                {"window_days": window_days},
            )
            if max_trade_date:
                candidate_dates[:] = [
                    trade_date
                    for trade_date in candidate_dates
                    if trade_date <= max_trade_date
                ]
            return candidate_dates[0] if candidate_dates else None

        return self._memoize(cache_key, factory)

    def latest_sim_snapshot_trade_date(self) -> str | None:
        def factory():
            candidate_dates = self._distinct_date_values(
                """
                SELECT snapshot_date AS trade_date
                FROM sim_dashboard_snapshot
                """
            )
            return candidate_dates[0] if candidate_dates else None

        return self._memoize("latest_sim_snapshot_trade_date", factory)

    def runtime_trade_date(self) -> str | None:
        def factory():
            candidate_dates: set[str] = set()
            pool_snapshot = self.public_pool_snapshot()
            if pool_snapshot["public_pool_trade_date"]:
                candidate_dates.add(str(pool_snapshot["public_pool_trade_date"]))
            latest_complete_public_batch_trade_date = self.latest_complete_public_batch_trade_date()
            if latest_complete_public_batch_trade_date:
                candidate_dates.add(latest_complete_public_batch_trade_date)
            if not candidate_dates:
                market_state_trade_date = shared._scalar(
                    self.db,
                    """
                    SELECT MAX(trade_date)
                    FROM market_state_cache
                    """,
                )
                if market_state_trade_date:
                    candidate_dates.add(str(market_state_trade_date)[:10])
            filtered_dates = self.filter_runtime_anchor_dates(candidate_dates)
            return filtered_dates[0] if filtered_dates else None

        return self._memoize("runtime_trade_date", factory)

    def latest_runtime_trade_date(self) -> str | None:
        return self.runtime_trade_date()

    def home_reference_trade_date(
        self,
        *,
        public_runtime: Mapping[str, Any] | None = None,
        market_row: Mapping[str, Any] | None = None,
    ) -> str | None:
        runtime_status = public_runtime or self.public_runtime_status()
        resolved_market_row = market_row or self.runtime_market_state_row() or {}
        return (
            shared._iso_date((runtime_status or {}).get("trade_date"))
            or self.runtime_trade_date()
            or shared._iso_date((resolved_market_row or {}).get("trade_date"))
        )

    def _normalize_public_issue_reason(self, reason: str | None) -> str | None:
        normalized = str(reason or "").strip()
        if not normalized:
            return None
        if normalized in {
            "pool_version_missing",
            "pool_task_missing",
            "pool_task_not_ready",
            "pool_snapshot_missing",
            "underfilled_pool_snapshot",
            "core_pool_too_small",
            "core_pool_size_mismatch",
            "standby_pool_size_mismatch",
            "standby_pool_missing",
            "core_rank_invalid",
            "standby_rank_invalid",
            "pool_snapshot_duplicate_stock",
            "pool_snapshot_not_ready",
            "published_snapshot_missing",
            "fallback_task_not_runtime_anchor",
        }:
            return "home_snapshot_not_ready"
        return normalized

    def public_runtime_issue(self) -> dict[str, Any] | None:
        def factory():
            pool_snapshot = self.public_pool_snapshot()
            stable_trade_date = shared._iso_date(pool_snapshot.get("public_pool_trade_date"))
            latest_task = self.latest_public_pool_task()
            latest_task_trade_date = shared._iso_date(getattr(latest_task, "trade_date", None))
            latest_report_trade_date = self.latest_published_report_trade_date()

            if latest_task_trade_date and (stable_trade_date is None or latest_task_trade_date > stable_trade_date):
                eligibility = evaluate_public_task_eligibility(self.db, getattr(latest_task, "task_id", latest_task))
                if not eligibility.get("eligible"):
                    reason = self._normalize_public_issue_reason(eligibility.get("reason"))
                    if latest_report_trade_date and (
                        stable_trade_date is None or latest_report_trade_date > stable_trade_date
                    ) and reason in {None, "home_snapshot_not_ready"}:
                        reason = "home_source_inconsistent"
                    reason = reason or "home_snapshot_not_ready"
                    coverage = eligibility.get("kline_coverage")
                    issue = shared._build_public_runtime_issue(
                        self.db,
                        reason=reason,
                        data_status="DEGRADED" if stable_trade_date else "COMPUTING",
                        attempted_trade_date=latest_task_trade_date,
                        fallback_from=getattr(latest_task, "fallback_from", None),
                        task_status=getattr(latest_task, "status", None),
                    )
                    if coverage is not None and issue.get("kline_coverage") is None:
                        issue["kline_coverage"] = {
                            "trade_date": shared._iso_date(latest_task_trade_date),
                            "coverage_pct": coverage,
                        }
                    # Enrich display_hint with actual coverage counts
                    if reason == "KLINE_COVERAGE_INSUFFICIENT" and latest_task_trade_date:
                        cov_summary = get_trade_date_kline_coverage_summary(
                            self.db, trade_date=str(latest_task_trade_date)[:10]
                        )
                        if cov_summary:
                            avail = cov_summary.get("available_count", 0)
                            universe = cov_summary.get("universe_count", 0)
                            if universe > 0:
                                issue["display_hint"] = (
                                    f"当日行情覆盖不足（{avail}/{universe}），系统已回退到稳定批次。"
                                )
                    return issue

            if stable_trade_date and latest_report_trade_date and latest_report_trade_date > stable_trade_date:
                return shared._build_public_runtime_issue(
                    self.db,
                    reason="home_source_inconsistent",
                    data_status="DEGRADED",
                    attempted_trade_date=latest_report_trade_date,
                )

            runtime_trade_date = self.runtime_trade_date()
            if runtime_trade_date:
                from app.services.settlement_ssot import get_settlement_pipeline_status

                settlement_pipeline = get_settlement_pipeline_status(
                    self.db,
                    trade_date=runtime_trade_date,
                    target_scope="all",
                )
                pipeline_status = str(settlement_pipeline.get("pipeline_status") or "").upper()
                if pipeline_status in {"ACCEPTED", "RUNNING"}:
                    return shared._build_public_runtime_issue(
                        self.db,
                        reason=str(settlement_pipeline.get("status_reason") or "").strip() or "settlement_pipeline_not_completed",
                        data_status="COMPUTING",
                        attempted_trade_date=runtime_trade_date,
                        task_status=pipeline_status,
                    )
                if pipeline_status in {"FAILED", "DEGRADED"}:
                    return shared._build_public_runtime_issue(
                        self.db,
                        reason=str(settlement_pipeline.get("status_reason") or "").strip() or "settlement_pipeline_failed",
                        data_status="DEGRADED",
                        attempted_trade_date=runtime_trade_date,
                        task_status=pipeline_status,
                    )
            return None

        return self._memoize("public_runtime_issue", factory)

    def public_runtime_status(self) -> dict[str, Any]:
        def factory():
            pool_snapshot = self.public_pool_snapshot()
            pool_view = pool_snapshot["pool_view"]
            trade_date = shared._iso_date(pool_snapshot.get("public_pool_trade_date"))
            issue = self.public_runtime_issue()

            data_status = "READY"
            status_reason = None
            display_hint = None
            attempted_trade_date = trade_date
            fallback_from = shared._iso_date(getattr(pool_view.task, "fallback_from", None)) if pool_view is not None else None
            task_status = str(getattr(pool_view.task, "status", "") or "").upper() if pool_view is not None else None
            kline_coverage = None

            if pool_view is None:
                data_status = "COMPUTING"
                status_reason = "home_snapshot_not_ready"
                display_hint = shared.humanize_status_reason(status_reason)
            if issue is not None:
                data_status = str(issue.get("data_status") or data_status)
                status_reason = issue.get("status_reason") or status_reason
                display_hint = issue.get("display_hint") or display_hint
                attempted_trade_date = issue.get("attempted_trade_date") or attempted_trade_date
                fallback_from = issue.get("fallback_from") or fallback_from
                task_status = issue.get("task_status") or task_status
                kline_coverage = issue.get("kline_coverage")

            return {
                "trade_date": trade_date,
                "attempted_trade_date": attempted_trade_date,
                "pool_size": int(pool_snapshot.get("pool_size") or 0),
                "task_status": task_status,
                "fallback_from": fallback_from,
                "data_status": data_status,
                "status_reason": status_reason,
                "display_hint": display_hint,
                "kline_coverage": kline_coverage,
                "latest_published_report_trade_date": self.latest_published_report_trade_date(),
            }

        return self._memoize("public_runtime_status", factory)

    def public_performance_payload(self, *, window_days: int = 30) -> dict[str, Any]:
        from app.services.dashboard_query import get_public_performance_payload_ssot

        return get_public_performance_payload_ssot(
            self.db,
            window_days=window_days,
            runtime_anchor_service=self,
        )

    def merged_public_runtime_payload(self, *, window_days: int = 30) -> dict[str, Any]:
        payload = self.public_performance_payload(window_days=window_days)
        runtime_status = self.public_runtime_status()
        runtime_status_rank = {"READY": 0, "COMPUTING": 1, "DEGRADED": 2}
        payload_data_status = str(payload.get("data_status") or "READY").upper()
        runtime_data_status = str(runtime_status.get("data_status") or "READY").upper()
        if runtime_status_rank.get(runtime_data_status, 0) > runtime_status_rank.get(payload_data_status, 0):
            merged_data_status = runtime_data_status
            merged_status_reason = runtime_status.get("status_reason") or payload.get("status_reason")
            merged_display_hint = runtime_status.get("display_hint") or payload.get("display_hint")
        else:
            merged_data_status = payload.get("data_status")
            merged_status_reason = payload.get("status_reason")
            merged_display_hint = payload.get("display_hint")
        return {
            **payload,
            "trade_date": runtime_status.get("trade_date"),
            "attempted_trade_date": runtime_status.get("attempted_trade_date"),
            "pool_size": runtime_status.get("pool_size"),
            "task_status": runtime_status.get("task_status"),
            "fallback_from": runtime_status.get("fallback_from"),
            "data_status": merged_data_status,
            "status_reason": merged_status_reason,
            "display_hint": merged_display_hint,
            "kline_coverage": runtime_status.get("kline_coverage"),
        }

    def runtime_anchor_dates(self) -> dict[str, str | None]:
        def factory():
            pool_snapshot = self.public_pool_snapshot()
            runtime_trade_date = self.runtime_trade_date()
            return {
                "runtime_trade_date": runtime_trade_date,
                "latest_published_report_trade_date": self.latest_published_report_trade_date(),
                "public_pool_trade_date": pool_snapshot["public_pool_trade_date"],
                "latest_complete_public_batch_trade_date": self.latest_complete_public_batch_trade_date(),
                "stats_snapshot_date": self.latest_complete_stats_trade_date(max_trade_date=runtime_trade_date)
                or self.latest_stats_snapshot_trade_date(window_days=30, max_trade_date=runtime_trade_date),
                "sim_snapshot_date": self.latest_sim_snapshot_trade_date(),
            }

        return self._memoize("runtime_anchor_dates", factory)

    def runtime_history_anchor_trade_dates(
        self,
        *,
        trade_date_value: str,
    ) -> list[str]:
        cache_key = f"runtime_history_anchor_trade_dates:{trade_date_value}"

        def factory():
            candidate_dates = self._distinct_date_values(
                """
                SELECT trade_date
                FROM stock_pool_refresh_task
                WHERE trade_date <= :trade_date
                UNION
                SELECT trade_date
                FROM report_generation_task
                WHERE trade_date <= :trade_date
                UNION
                SELECT trade_date
                FROM report
                WHERE published = 1
                  AND is_deleted = 0
                  AND trade_date <= :trade_date
                """,
                {"trade_date": trade_date_value},
            )
            return [
                value
                for value in candidate_dates
                if self.has_complete_public_batch_trace(trade_date=value)
            ]

        return self._memoize(cache_key, factory)

    def _stats_snapshot_signature(
        self,
        *,
        window_days: int,
        snapshot_date: str | None,
    ) -> str:
        cache_key = f"stats_snapshot_signature:{window_days}:{snapshot_date or ''}"

        def factory():
            if not snapshot_date:
                return ""
            strategy_summary = shared._execute_mappings(
                self.db,
                """
                SELECT
                    MAX(created_at) AS created_at,
                    COUNT(*) AS row_count,
                    GROUP_CONCAT(fingerprint, '|') AS fingerprint
                FROM (
                    SELECT
                        strategy_type
                        || ':' || COALESCE(data_status, '')
                        || ':' || COALESCE(CAST(sample_size AS TEXT), '')
                        || ':' || COALESCE(CAST(coverage_pct AS TEXT), '')
                        || ':' || COALESCE(CAST(win_rate AS TEXT), '')
                        || ':' || COALESCE(CAST(profit_loss_ratio AS TEXT), '')
                        || ':' || COALESCE(CAST(alpha_annual AS TEXT), '')
                        || ':' || COALESCE(CAST(max_drawdown_pct AS TEXT), '')
                        || ':' || COALESCE(CAST(cumulative_return_pct AS TEXT), '')
                        || ':' || COALESCE(CAST(signal_validity_warning AS TEXT), '')
                        || ':' || COALESCE(display_hint, '') AS fingerprint,
                        created_at
                    FROM strategy_metric_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND window_days = :window_days
                    ORDER BY strategy_type ASC
                ) ordered_rows
                """,
                {"snapshot_date": snapshot_date, "window_days": window_days},
            ).first() or {}
            baseline_summary = shared._execute_mappings(
                self.db,
                """
                SELECT
                    MAX(created_at) AS created_at,
                    COUNT(*) AS row_count,
                    GROUP_CONCAT(fingerprint, '|') AS fingerprint
                FROM (
                    SELECT
                        baseline_type
                        || ':' || COALESCE(CAST(simulation_runs AS TEXT), '')
                        || ':' || COALESCE(CAST(sample_size AS TEXT), '')
                        || ':' || COALESCE(CAST(win_rate AS TEXT), '')
                        || ':' || COALESCE(CAST(profit_loss_ratio AS TEXT), '')
                        || ':' || COALESCE(CAST(alpha_annual AS TEXT), '')
                        || ':' || COALESCE(CAST(max_drawdown_pct AS TEXT), '')
                        || ':' || COALESCE(CAST(cumulative_return_pct AS TEXT), '')
                        || ':' || COALESCE(display_hint, '') AS fingerprint,
                        created_at
                    FROM baseline_metric_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND window_days = :window_days
                    ORDER BY baseline_type ASC
                ) ordered_rows
                """,
                {"snapshot_date": snapshot_date, "window_days": window_days},
            ).first() or {}
            return "|".join(
                str(value or "")
                for value in (
                    snapshot_date,
                    strategy_summary.get("created_at"),
                    strategy_summary.get("row_count"),
                    strategy_summary.get("fingerprint"),
                    baseline_summary.get("created_at"),
                    baseline_summary.get("row_count"),
                    baseline_summary.get("fingerprint"),
                )
            )

        return self._memoize(cache_key, factory)

    def _sim_snapshot_signature(self, *, snapshot_date: str | None) -> str:
        cache_key = f"sim_snapshot_signature:{snapshot_date or ''}"

        def factory():
            if not snapshot_date:
                return ""
            summary = shared._execute_mappings(
                self.db,
                """
                SELECT
                    MAX(created_at) AS created_at,
                    COUNT(*) AS row_count,
                    GROUP_CONCAT(fingerprint, '|') AS fingerprint
                FROM (
                    SELECT
                        capital_tier
                        || ':' || COALESCE(data_status, '')
                        || ':' || COALESCE(status_reason, '')
                        || ':' || COALESCE(CAST(total_return_pct AS TEXT), '')
                        || ':' || COALESCE(CAST(win_rate AS TEXT), '')
                        || ':' || COALESCE(CAST(profit_loss_ratio AS TEXT), '')
                        || ':' || COALESCE(CAST(alpha_annual AS TEXT), '')
                        || ':' || COALESCE(CAST(max_drawdown_pct AS TEXT), '')
                        || ':' || COALESCE(CAST(sample_size AS TEXT), '')
                        || ':' || COALESCE(display_hint, '')
                        || ':' || COALESCE(CAST(is_simulated_only AS TEXT), '') AS fingerprint,
                        created_at
                    FROM sim_dashboard_snapshot
                    WHERE snapshot_date = :snapshot_date
                    ORDER BY capital_tier ASC
                ) ordered_rows
                """,
                {"snapshot_date": snapshot_date},
            ).first() or {}
            return "|".join(
                str(value or "")
                for value in (
                    snapshot_date,
                    summary.get("created_at"),
                    summary.get("row_count"),
                    summary.get("fingerprint"),
                )
            )

        return self._memoize(cache_key, factory)

    def home_cache_key(
        self,
        *,
        viewer_tier: str | None = None,
        viewer_role: str | None = None,
        window_days: int = 30,
    ) -> tuple[Any, ...]:
        bind = self.db.get_bind()
        db_identity = None
        if bind is not None:
            db_identity = str(getattr(bind, "url", "") or "") or str(id(bind))
        runtime_status = self.public_runtime_status()
        runtime_versions = self.public_versions(window_days=window_days)
        anchor_dates = self.runtime_anchor_dates()
        stats_snapshot_date = self.latest_complete_stats_trade_date(
            max_trade_date=anchor_dates.get("runtime_trade_date"),
        ) or self.latest_stats_snapshot_trade_date(
            window_days=window_days,
            max_trade_date=anchor_dates.get("runtime_trade_date"),
        )
        stats_snapshot_signature = self._stats_snapshot_signature(
            window_days=window_days,
            snapshot_date=stats_snapshot_date,
        )
        public_market_row = self.latest_public_market_state_row() or {}
        pool_snapshot = self.public_pool_snapshot()
        pool_view = pool_snapshot["pool_view"]
        reference_trade_date = self.home_reference_trade_date(
            public_runtime=runtime_status,
            market_row=public_market_row,
        )
        report_summary = {}
        if reference_trade_date:
            report_summary = shared._execute_mappings(
                self.db,
                """
                SELECT
                    COUNT(*) AS row_count,
                    MAX(report_id) AS max_id,
                    MAX(updated_at) AS max_updated_at
                FROM report
                WHERE published = 1
                  AND is_deleted = 0
                  AND trade_date = :trade_date
                """,
                {"trade_date": reference_trade_date},
            ).first() or {}
        hotspot_summary = shared._execute_mappings(
            self.db,
            """
            SELECT
                COUNT(*) AS row_count,
                MAX(hotspot_item_id) AS max_id,
                MAX(fetch_time) AS max_fetch_time
            FROM market_hotspot_item
            WHERE fetch_time > datetime('now', '-24 hours')
            """,
        ).first() or {}
        pool_task_id = ""
        pool_version = ""
        pool_updated_at = ""
        core_pool_fingerprint: tuple[tuple[int, str], ...] = ()
        if pool_view is not None:
            pool_task = pool_view.task
            pool_task_id = str(getattr(pool_task, "task_id", "") or "")
            pool_version = str(getattr(pool_task, "pool_version", "") or "")
            pool_updated_at = str(
                getattr(pool_task, "updated_at", None)
                or getattr(pool_task, "finished_at", None)
                or ""
            )
            core_pool_fingerprint = tuple(
                (
                    int(getattr(row, "rank_no", 0) or 0),
                    str(getattr(row, "stock_code", "") or ""),
                )
                for row in sorted(
                    pool_view.core_rows,
                    key=lambda row: ((row.rank_no or 0), row.stock_code),
                )
            )
        return (
            db_identity,
            str(viewer_tier or "Free"),
            str(viewer_role or "").lower(),
            runtime_versions.public_runtime_version,
            stats_snapshot_date,
            stats_snapshot_signature,
            reference_trade_date,
            str(runtime_status.get("latest_published_report_trade_date") or ""),
            str(public_market_row.get("trade_date") or ""),
            str(public_market_row.get("market_state") or ""),
            str(public_market_row.get("cache_status") or ""),
            str(public_market_row.get("state_reason") or ""),
            str(public_market_row.get("computed_at") or ""),
            str(pool_snapshot.get("public_pool_trade_date") or ""),
            int(pool_snapshot.get("pool_size") or 0),
            pool_task_id,
            pool_version,
            pool_updated_at,
            core_pool_fingerprint,
            int(report_summary.get("row_count") or 0),
            str(report_summary.get("max_id") or ""),
            str(report_summary.get("max_updated_at") or ""),
            int(hotspot_summary.get("row_count") or 0),
            str(hotspot_summary.get("max_id") or ""),
            str(hotspot_summary.get("max_fetch_time") or ""),
        )

    def public_versions(self, *, window_days: int = 30) -> RuntimeAnchorVersions:
        runtime_status = self.public_runtime_status()
        anchor_dates = self.runtime_anchor_dates()
        stats_snapshot_date = self.latest_complete_stats_trade_date(max_trade_date=anchor_dates.get("runtime_trade_date")) or self.latest_stats_snapshot_trade_date(
            window_days=window_days,
            max_trade_date=anchor_dates.get("runtime_trade_date"),
        )
        stats_snapshot_signature = self._stats_snapshot_signature(
            window_days=window_days,
            snapshot_date=stats_snapshot_date,
        )
        sim_snapshot_signature = self._sim_snapshot_signature(
            snapshot_date=anchor_dates.get("sim_snapshot_date"),
        )
        public_runtime_version = "|".join(
            str(value or "")
            for value in (
                anchor_dates.get("runtime_trade_date"),
                anchor_dates.get("public_pool_trade_date"),
                anchor_dates.get("latest_complete_public_batch_trade_date"),
                runtime_status.get("attempted_trade_date"),
                runtime_status.get("task_status"),
                runtime_status.get("fallback_from"),
                runtime_status.get("data_status"),
                runtime_status.get("status_reason"),
                runtime_status.get("display_hint"),
                (runtime_status.get("kline_coverage") if isinstance(runtime_status.get("kline_coverage"), dict) else {}).get("available_count"),
                (runtime_status.get("kline_coverage") if isinstance(runtime_status.get("kline_coverage"), dict) else {}).get("universe_count"),
            )
        )
        public_snapshot_version = "|".join(
            str(value or "")
            for value in (
                window_days,
                anchor_dates.get("runtime_trade_date"),
                stats_snapshot_date,
                stats_snapshot_signature,
                anchor_dates.get("sim_snapshot_date"),
                sim_snapshot_signature,
            )
        )
        return RuntimeAnchorVersions(
            public_runtime_version=public_runtime_version,
            public_snapshot_version=public_snapshot_version,
        )
