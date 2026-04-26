from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Index,
    JSON,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
)


TABLE_TITLE_RE = re.compile(r"^(?:#### 表：|###\s+[\d.]+\s+.*?[：:])\s*`([^`]+)`")
COLUMN_ROW_RE = re.compile(r"^\| `([^`]+)` \| `([^`]+)` \| (是|否) \|")
BACKTICK_RE = re.compile(r"`([^`]+)`")
UNIQUE_RE = re.compile(r"`(uk_[^(]+)\(([^`]+)\)`")
DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
VARCHAR_RE = re.compile(r"^varchar\((\d+)\)$")

# Critical enum/status domains frozen by SSOT 03/04. These are enforced at runtime
# so tests and local scaffolds cannot silently drift back to legacy values.
ENUM_CHECKS: dict[tuple[str, str], tuple[str, ...]] = {
    # FR-01
    ("stock_pool_refresh_task", "status"): ("IDLE", "REFRESHING", "COMPLETED", "FALLBACK", "COLD_START_BLOCKED"),
    ("stock_pool_snapshot", "pool_role"): ("core", "standby", "evicted"),
    # FR-02
    ("scheduler_task_run", "status"): ("PENDING", "WAITING_UPSTREAM", "RUNNING", "SUCCESS", "FAILED", "SKIPPED"),
    ("scheduler_task_run", "trigger_source"): ("cron", "event"),
    # FR-03
    ("cookie_session", "status"): ("ACTIVE", "EXPIRING", "EXPIRED", "REFRESH_FAILED", "SKIPPED"),
    ("cookie_probe_log", "probe_outcome"): ("success", "failed", "skipped"),
    # FR-04
    ("data_batch", "batch_status"): ("RUNNING", "SUCCESS", "PARTIAL_SUCCESS", "FAILED"),
    ("data_batch", "quality_flag"): ("ok", "stale_ok", "missing", "degraded"),
    ("data_batch_lineage", "lineage_role"): ("MERGED_FROM", "FALLBACK_FROM", "DERIVED_FROM"),
    ("data_source_circuit_state", "circuit_state"): ("CLOSED", "OPEN", "HALF_OPEN"),
    ("market_hotspot_item", "quality_flag"): ("ok", "stale_ok", "missing", "degraded"),
    ("market_hotspot_item_source", "quality_flag"): ("ok", "stale_ok", "missing", "degraded"),
    ("market_hotspot_item_stock_link", "relation_role"): ("primary", "related"),
    ("report_data_usage", "status"): ("ok", "stale_ok", "missing", "degraded", "proxy_ok", "realtime_only"),
    # FR-05
    ("market_state_cache", "market_state"): ("BULL", "NEUTRAL", "BEAR"),
    ("market_state_cache", "cache_status"): ("FRESH", "CACHED", "DEGRADED_NEUTRAL"),
    # FR-06
    ("llm_circuit_state", "circuit_state"): ("CLOSED", "OPEN", "HALF_OPEN"),
    ("report_generation_task", "status"): ("Pending", "Processing", "Completed", "Failed", "Suspended", "Expired"),
    ("report_generation_task", "llm_fallback_level"): ("primary", "backup", "cli", "local", "failed"),
    ("report_generation_task", "risk_audit_status"): ("completed", "skipped", "not_triggered"),
    ("report", "publish_status"): ("DRAFT_GENERATED", "PUBLISHED", "UNPUBLISHED"),
    ("report", "quality_flag"): ("ok", "stale_ok", "degraded"),
    ("report", "llm_fallback_level"): ("primary", "backup", "cli", "local", "failed"),
    ("report", "risk_audit_status"): ("completed", "skipped", "not_triggered"),
    ("report", "review_flag"): ("NONE", "PENDING_REVIEW", "APPROVED", "REJECTED"),
    ("report", "recommendation"): ("BUY", "SELL", "HOLD"),
    ("report", "strategy_type"): ("A", "B", "C"),
    ("instruction_card", "stop_loss_calc_mode"): ("atr_multiplier", "fixed_92pct_fallback"),
    ("sim_trade_instruction", "status"): ("EXECUTE", "SKIPPED"),
    ("sim_trade_instruction", "capital_tier"): ("10k", "100k", "500k"),
    # FR-07
    ("pipeline_run", "pipeline_status"): ("ACCEPTED", "RUNNING", "COMPLETED", "FAILED", "DEGRADED"),
    ("settlement_task", "status"): ("QUEUED", "PROCESSING", "COMPLETED", "FAILED"),
    ("settlement_task", "target_scope"): ("all", "report_id", "stock_code"),
    ("settlement_result", "settlement_status"): ("pending", "settled", "skipped", "degraded"),
    ("settlement_result", "quality_flag"): ("ok", "stale_ok", "degraded"),
    ("settlement_result", "strategy_type"): ("A", "B", "C"),
    ("strategy_metric_snapshot", "data_status"): ("READY", "COMPUTING", "DEGRADED"),
    ("strategy_metric_snapshot", "strategy_type"): ("A", "B", "C"),
    ("baseline_task", "status"): ("BASELINE_QUEUED", "BASELINE_PROCESSING", "BASELINE_COMPLETED", "BASELINE_FAILED"),
    ("baseline_task", "baseline_type"): ("baseline_random", "baseline_ma_cross"),
    ("baseline_metric_snapshot", "baseline_type"): ("baseline_random", "baseline_ma_cross"),
    ("baseline_equity_curve_point", "baseline_type"): ("baseline_random", "baseline_ma_cross"),
    ("baseline_equity_curve_point", "capital_tier"): ("10k", "100k", "500k"),
    # FR-08
    ("sim_trade_batch_queue_item", "capital_tier"): ("10k", "100k", "500k"),
    ("sim_account", "drawdown_state"): ("NORMAL", "REDUCE", "HALT"),
    ("sim_account", "capital_tier"): ("10k", "100k", "500k"),
    ("sim_position", "position_status"): ("OPEN", "TAKE_PROFIT", "STOP_LOSS", "TIMEOUT", "DELISTED_LIQUIDATED", "SKIPPED"),
    ("sim_position", "capital_tier"): ("10k", "100k", "500k"),
    ("sim_equity_curve_point", "capital_tier"): ("10k", "100k", "500k"),
    ("sim_equity_curve_point", "drawdown_state"): ("NORMAL", "REDUCE", "HALT"),
    ("sim_dashboard_snapshot", "data_status"): ("READY", "COMPUTING", "DEGRADED"),
    ("sim_dashboard_snapshot", "capital_tier"): ("10k", "100k", "500k"),
    # FR-09
    ("user_session", "status"): ("ACTIVE", "EXPIRED", "REVOKED", "BLACKLISTED"),
    ("billing_order", "status"): ("CREATED", "PENDING", "PAID", "EXPIRED", "REFUNDED"),
    ("billing_order", "provider"): ("alipay", "wechat_pay"),
    # FR-09-b
    ("cleanup_task", "status"): ("SCHEDULED", "RUNNING", "COMPLETED", "FAILED"),
    ("cleanup_task_item", "result"): ("success", "failed", "skipped"),
    # FR-11
    ("report_feedback", "feedback_type"): ("positive", "negative"),
    # FR-12
    ("admin_operation", "status"): ("PENDING", "EXECUTING", "COMPLETED", "REJECTED", "FAILED"),
    ("admin_operation", "action_type"): ("PATCH_USER", "PATCH_REPORT", "FORCE_REGENERATE", "RECONCILE_ORDER", "RUN_SETTLEMENT", "UPSERT_COOKIE_SESSION"),
    # FR-13
    ("business_event", "event_status"): ("CREATED", "DEDUP_SKIPPED", "ENQUEUED"),
    ("business_event", "event_type"): ("POSITION_CLOSED", "BUY_SIGNAL_DAILY", "DRAWDOWN_ALERT", "REPORT_PENDING_REVIEW"),
    ("outbox_event", "dispatch_status"): ("PENDING", "DISPATCHING", "DISPATCHED", "DISPATCH_FAILED"),
    ("notification", "status"): ("sent", "failed", "skipped"),
    ("notification", "channel"): ("email", "webhook"),
    ("notification", "event_type"): ("POSITION_CLOSED", "BUY_SIGNAL_DAILY", "DRAWDOWN_ALERT", "REPORT_PENDING_REVIEW"),
    ("event_projection_cursor", "event_type"): ("POSITION_CLOSED", "BUY_SIGNAL_DAILY", "DRAWDOWN_ALERT", "REPORT_PENDING_REVIEW"),
}

# Targeted FK/index contracts promoted from SSOT 04 into executable schema.
# We only freeze the high-impact links that currently drive runtime lineage,
# event dispatch, and list/dashboard query performance.
FOREIGN_KEY_TARGETS: dict[tuple[str, str], str] = {
    ("report_generation_task", "stock_code"): "stock_master.stock_code",
    ("report_generation_task", "market_state_trade_date"): "market_state_cache.trade_date",
    ("report_generation_task", "refresh_task_id"): "stock_pool_refresh_task.task_id",
    ("report_generation_task", "trigger_task_run_id"): "scheduler_task_run.task_run_id",
    ("report_generation_task", "superseded_by_task_id"): "report_generation_task.task_id",
    ("report", "generation_task_id"): "report_generation_task.task_id",
    ("report", "stock_code"): "stock_master.stock_code",
    ("report", "market_state_trade_date"): "market_state_cache.trade_date",
    ("report", "reviewed_by"): "app_user.user_id",
    ("report", "superseded_by_report_id"): "report.report_id",
    ("sim_position", "report_id"): "report.report_id",
    ("sim_position", "capital_tier"): "sim_account.capital_tier",
    ("sim_position", "stock_code"): "stock_master.stock_code",
    ("billing_order", "user_id"): "app_user.user_id",
    ("outbox_event", "business_event_id"): "business_event.business_event_id",
    ("notification", "business_event_id"): "business_event.business_event_id",
    ("notification", "recipient_user_id"): "app_user.user_id",
    ("refresh_token", "user_id"): "app_user.user_id",
    ("refresh_token", "session_id"): "user_session.session_id",
    ("refresh_token", "rotated_from_token_id"): "refresh_token.refresh_token_id",
    ("jti_blacklist", "user_id"): "app_user.user_id",
    ("baseline_equity_curve_point", "capital_tier"): "sim_account.capital_tier",
    ("cleanup_task_item", "cleanup_id"): "cleanup_task.cleanup_id",
}

INDEX_SPECS: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "stock_master": (
        ("idx_stock_master_is_active", ("is_delisted", "is_st")),
        ("idx_stock_master_industry", ("industry",)),
    ),
    "stock_pool_refresh_task": (
        ("idx_stock_pool_refresh_task_status", ("status", "updated_at")),
    ),
    "report_generation_task": (
        ("idx_report_generation_task_status", ("status", "updated_at")),
        ("idx_report_generation_task_trade_date", ("trade_date", "stock_code")),
        ("idx_report_generation_task_batch", ("trigger_task_run_id", "status", "trade_date")),
        ("idx_report_generation_task_request", ("request_id", "created_at")),
    ),
    "report": (
        ("idx_report_trade_date", ("trade_date", "published", "is_deleted", "quality_flag")),
        ("idx_report_review", ("review_flag", "published", "is_deleted")),
        ("idx_report_stock_date", ("stock_code", "trade_date")),
        ("idx_report_list_filters", ("published", "is_deleted", "trade_date", "recommendation", "strategy_type", "market_state", "quality_flag")),
        ("idx_report_home", ("published", "is_deleted", "trade_date", "confidence", "report_id")),
    ),
    "sim_position": (
        ("idx_sim_position_status", ("capital_tier", "position_status", "signal_date")),
        ("idx_sim_position_stock", ("stock_code", "capital_tier")),
        ("idx_sim_position_list_filters", ("capital_tier", "position_status", "signal_date", "stock_code")),
    ),
    "billing_order": (
        ("idx_billing_order_status", ("status", "created_at")),
        ("idx_billing_order_user", ("user_id", "status")),
    ),
    "payment_webhook_event": (
        ("idx_payment_webhook_event_order", ("order_id", "received_at")),
        ("idx_payment_webhook_event_request", ("request_id", "received_at")),
    ),
    "settlement_task": (
        ("idx_settlement_task_request", ("request_id", "created_at")),
    ),
    "admin_operation": (
        ("idx_admin_operation_status", ("action_type", "status", "created_at")),
        ("idx_admin_operation_request", ("request_id", "action_type", "target_pk", "created_at")),
    ),
    "audit_log": (
        ("idx_audit_log_actor", ("actor_user_id", "created_at")),
        ("idx_audit_log_target", ("target_table", "target_pk")),
        ("idx_audit_log_request", ("request_id", "created_at")),
    ),
    "outbox_event": (
        ("idx_outbox_event_dispatch", ("dispatch_status", "next_retry_at")),
    ),
    "notification": (
        ("idx_notification_status", ("event_type", "status", "triggered_at")),
        ("idx_notification_recipient", ("recipient_scope", "recipient_key")),
    ),
    "refresh_token": (
        ("idx_refresh_token_user", ("user_id", "expires_at")),
        ("idx_refresh_token_session", ("session_id",)),
        ("idx_refresh_token_rotated_from", ("rotated_from_token_id",)),
    ),
    "jti_blacklist": (
        ("idx_jti_blacklist_expire", ("expires_at",)),
        ("idx_jti_blacklist_user", ("user_id",)),
    ),
    "cleanup_task": (
        ("idx_cleanup_task_status", ("status", "updated_at")),
        ("idx_cleanup_task_request", ("request_id",)),
    ),
    "cleanup_task_item": (
        ("idx_cleanup_task_item_cleanup", ("cleanup_id",)),
    ),
    "baseline_equity_curve_point": (
        ("idx_baseline_equity_curve_point_lookup", ("capital_tier", "baseline_type", "trade_date")),
    ),
    "pipeline_run": (
        ("idx_pipeline_run_trade_status", ("trade_date", "pipeline_status", "updated_at")),
    ),
}


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    raw_type: str
    required: bool


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_keys: tuple[str, ...]
    unique_constraints: tuple[tuple[str, tuple[str, ...]], ...]
    columns: tuple[ColumnSpec, ...]


def _supplemental_table_specs() -> tuple[TableSpec, ...]:
    return (
        TableSpec(
            name="pipeline_run",
            primary_keys=("pipeline_run_id",),
            unique_constraints=(("uk_pipeline_run_name_trade_date", ("pipeline_name", "trade_date")),),
            columns=(
                ColumnSpec("pipeline_run_id", "uuid", True),
                ColumnSpec("pipeline_name", "varchar(64)", True),
                ColumnSpec("trade_date", "date", True),
                ColumnSpec("pipeline_status", "varchar(32)", True),
                ColumnSpec("degraded", "bool", True),
                ColumnSpec("status_reason", "text", False),
                ColumnSpec("request_id", "varchar(64)", False),
                ColumnSpec("started_at", "datetime", False),
                ColumnSpec("finished_at", "datetime", False),
                ColumnSpec("updated_at", "datetime", True),
                ColumnSpec("created_at", "datetime", True),
            ),
        ),
    )


def _ensure_column(spec: TableSpec, column: ColumnSpec, *, before: str | None = None) -> TableSpec:
    if any(existing.name == column.name for existing in spec.columns):
        return spec

    columns = list(spec.columns)
    if before is None:
        columns.append(column)
    else:
        insert_at = next((idx for idx, existing in enumerate(columns) if existing.name == before), len(columns))
        columns.insert(insert_at, column)

    return TableSpec(
        name=spec.name,
        primary_keys=spec.primary_keys,
        unique_constraints=spec.unique_constraints,
        columns=tuple(columns),
    )


def _normalize_table_specs(specs: list[TableSpec]) -> list[TableSpec]:
    normalized: list[TableSpec] = []
    for spec in specs:
        if spec.name == "payment_webhook_event":
            spec = _ensure_column(
                spec,
                ColumnSpec("request_id", "varchar(64)", False),
                before="received_at",
            )
        normalized.append(spec)
    return normalized


def _default_doc_path() -> Path:
    core_dir = Path(__file__).resolve().parent.parent / "docs" / "core"
    return next(core_dir.glob("04_*.md"))


def _parse_table_specs(doc_path: Path) -> list[TableSpec]:
    lines = doc_path.read_text(encoding="utf-8").splitlines()
    specs: list[TableSpec] = []
    index = 0

    while index < len(lines):
        title_match = TABLE_TITLE_RE.match(lines[index])
        if not title_match:
            index += 1
            continue

        table_name = title_match.group(1)
        section_lines: list[str] = []
        index += 1
        while index < len(lines):
            if TABLE_TITLE_RE.match(lines[index]) or lines[index].startswith("### "):
                break
            section_lines.append(lines[index])
            index += 1

        primary_keys: tuple[str, ...] = ()
        unique_constraints: list[tuple[str, tuple[str, ...]]] = []
        columns: list[ColumnSpec] = []

        for line in section_lines:
            if "主键" in line:
                primary_keys = tuple(BACKTICK_RE.findall(line))
            for match in UNIQUE_RE.finditer(line):
                raw_columns = tuple(part.strip() for part in match.group(2).split(",") if part.strip())
                if raw_columns:
                    unique_constraints.append((match.group(1), raw_columns))
            if "唯一键" in line and not UNIQUE_RE.search(line):
                raw_tokens = BACKTICK_RE.findall(line)
                if raw_tokens:
                    if len(raw_tokens) == 1:
                        raw_columns = raw_tokens[0].strip().strip("()")
                        columns_tuple = tuple(part.strip() for part in raw_columns.split(",") if part.strip())
                    else:
                        columns_tuple = tuple(token.strip() for token in raw_tokens if token.strip())
                    if columns_tuple:
                        unique_constraints.append((f"uk_{table_name}_{'_'.join(columns_tuple)}", columns_tuple))

        in_column_table = False
        for line in section_lines:
            if line.startswith("| 字段 | 类型 | 必填 | 说明 |") or line.startswith("| 字段名 | 类型 | 必填 | 说明 |"):
                in_column_table = True
                continue
            if not in_column_table:
                continue
            if line.startswith("|---"):
                continue
            if not line.startswith("|"):
                if columns:
                    break
                continue
            row_match = COLUMN_ROW_RE.match(line)
            if not row_match:
                if columns:
                    break
                continue
            columns.append(
                ColumnSpec(
                    name=row_match.group(1),
                    raw_type=row_match.group(2),
                    required=row_match.group(3) == "是",
                )
            )

        if columns:
            specs.append(
                TableSpec(
                    name=table_name,
                    primary_keys=primary_keys,
                    unique_constraints=tuple(unique_constraints),
                    columns=tuple(columns),
                )
            )

    return specs


def load_table_specs(doc_path: str | Path | None = None) -> list[TableSpec]:
    path = Path(doc_path) if doc_path else _default_doc_path()
    specs = _normalize_table_specs(_parse_table_specs(path) + list(_supplemental_table_specs()))
    if len(specs) >= 55:
        return specs[:55]

    from app.models import Base as ModelBase

    existing_names = {spec.name for spec in specs}

    def _raw_type_from_column(column: Column) -> str:
        column_type = column.type
        if isinstance(column_type, String):
            return f"varchar({column_type.length or 255})"
        if isinstance(column_type, Text):
            return "text"
        if isinstance(column_type, DateTime):
            return "datetime"
        if isinstance(column_type, Date):
            return "date"
        if isinstance(column_type, Boolean):
            return "bool"
        if isinstance(column_type, Integer):
            return "int"
        if isinstance(column_type, JSON):
            return "json"
        if isinstance(column_type, Numeric):
            precision = getattr(column_type, "precision", None) or 18
            scale = getattr(column_type, "scale", None) or 6
            return f"decimal({precision},{scale})"
        return "text"

    fallback_specs: list[TableSpec] = []
    for table_name, table in ModelBase.metadata.tables.items():
        if table_name in existing_names:
            continue
        primary_keys = tuple(column.name for column in table.primary_key.columns)
        unique_constraints: list[tuple[str, tuple[str, ...]]] = []
        for constraint in table.constraints:
            if isinstance(constraint, UniqueConstraint):
                constraint_columns = tuple(column.name for column in constraint.columns)
                if constraint_columns:
                    unique_constraints.append((constraint.name or f"uk_{table_name}_{'_'.join(constraint_columns)}", constraint_columns))
        fallback_specs.append(
            TableSpec(
                name=table_name,
                primary_keys=primary_keys,
                unique_constraints=tuple(unique_constraints),
                columns=tuple(
                    ColumnSpec(column.name, _raw_type_from_column(column), not column.nullable)
                    for column in table.columns
                ),
            )
        )

    specs.extend(fallback_specs[: max(0, 55 - len(specs))])
    return specs[:55]


def _map_type(raw_type: str):
    normalized = raw_type.strip().lower()
    decimal_match = DECIMAL_RE.match(normalized)
    if decimal_match:
        return Numeric(int(decimal_match.group(1)), int(decimal_match.group(2)))

    varchar_match = VARCHAR_RE.match(normalized)
    if varchar_match:
        return String(int(varchar_match.group(1)))

    if normalized == "uuid":
        return String(36)
    if normalized == "text":
        return Text()
    if normalized == "json":
        return JSON()
    if normalized == "datetime":
        return DateTime()
    if normalized == "date":
        return Date()
    if normalized == "bool":
        return Boolean()
    if normalized == "int":
        return Integer()
    if normalized == "float":
        return Numeric(18, 6)

    return Text()


def _enum_check_constraint(table_name: str, column_name: str, allowed_values: tuple[str, ...]) -> CheckConstraint:
    quoted_values = ", ".join("'" + value.replace("'", "''") + "'" for value in allowed_values)
    return CheckConstraint(
        f"{column_name} IN ({quoted_values})",
        name=f"ck_{table_name}_{column_name}_enum",
    )


def build_metadata(doc_path: str | Path | None = None) -> tuple[MetaData, list[TableSpec]]:
    specs = load_table_specs(doc_path)
    metadata = MetaData()

    for spec in specs:
        columns: list[Column] = []
        for column_spec in spec.columns:
            column_args = [_map_type(column_spec.raw_type)]
            target = FOREIGN_KEY_TARGETS.get((spec.name, column_spec.name))
            if target:
                column_args.append(ForeignKey(target))
            columns.append(
                Column(
                    column_spec.name,
                    *column_args,
                    primary_key=column_spec.name in spec.primary_keys,
                    nullable=False if column_spec.name in spec.primary_keys else not column_spec.required,
                )
            )

        constraints = []
        partial_unique_indexes: list[tuple[str, tuple[str, ...]]] = []
        available_column_names = {column_spec.name for column_spec in spec.columns}
        for constraint_name, constraint_columns in spec.unique_constraints:
            if any(column_name not in available_column_names for column_name in constraint_columns):
                continue
            if spec.name == "report_feedback" and constraint_name == "uk_report_feedback_negative":
                partial_unique_indexes.append((constraint_name, constraint_columns))
                continue
            constraints.append(UniqueConstraint(*constraint_columns, name=constraint_name))
        for column_spec in spec.columns:
            enum_values = ENUM_CHECKS.get((spec.name, column_spec.name))
            if enum_values:
                constraints.append(_enum_check_constraint(spec.name, column_spec.name, enum_values))

        table = Table(spec.name, metadata, *columns, *constraints)
        for index_name, index_columns in partial_unique_indexes:
            if spec.name == "report_feedback" and index_columns == ("report_id", "user_id"):
                Index(
                    index_name,
                    table.c.report_id,
                    table.c.user_id,
                    unique=True,
                    sqlite_where=table.c.feedback_type == "negative",
                )

        # SSOT 04 requires "current effective" uniqueness for report generation
        # tasks and reports in addition to versioned uniqueness.
        if spec.name == "report_generation_task":
            if "idempotency_key" in table.c and "superseded_at" in table.c:
                Index(
                    "uk_report_generation_task_current",
                    table.c.idempotency_key,
                    unique=True,
                    sqlite_where=table.c.superseded_at.is_(None),
                )
        if spec.name == "report":
            if "idempotency_key" in table.c and "is_deleted" in table.c:
                Index(
                    "uk_report_current",
                    table.c.idempotency_key,
                    unique=True,
                    sqlite_where=table.c.is_deleted == False,  # noqa: E712
                )
        for index_name, index_columns in INDEX_SPECS.get(spec.name, ()):
            if all(column_name in table.c for column_name in index_columns):
                Index(index_name, *(table.c[column_name] for column_name in index_columns))

    return metadata, specs
