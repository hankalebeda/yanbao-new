from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FR07EligibleReportFilter:
    """Single source of truth for FR-07 report eligibility."""

    def sql_clauses(self, *, alias: str = "r") -> tuple[str, ...]:
        return (
            f"{alias}.published = 1",
            f"{alias}.is_deleted = 0",
            f"{alias}.quality_flag = 'ok'",
        )

    def sql_condition(self, *, alias: str = "r") -> str:
        return " AND ".join(self.sql_clauses(alias=alias))

    def invalid_sql_condition(self, *, alias: str = "r") -> str:
        return f"NOT ({self.sql_condition(alias=alias)})"


FR07_ELIGIBLE_REPORTS = FR07EligibleReportFilter()
