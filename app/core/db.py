from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import settings

def _configure_sqlite_engine(eng) -> None:
    if eng.dialect.name != "sqlite":
        return
    from sqlalchemy import event

    @event.listens_for(eng, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def build_engine(database_url: str):
    """Factory: create SQLAlchemy engine for an arbitrary DB URL."""
    ca = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    eng = create_engine(database_url, connect_args=ca)
    _configure_sqlite_engine(eng)
    return eng


engine = build_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def ensure_report_trade_date_column():
    """SQLite: 若 report 表缺少 trade_date 列则添加，便于 GET /api/v1/reports 筛选。"""
    if not settings.database_url.startswith("sqlite"):
        return
    with engine.connect() as conn:
        r = conn.execute(text("PRAGMA table_info(report)"))
        cols = [row[1] for row in r.fetchall()]
        if "trade_date" not in cols:
            conn.execute(text("ALTER TABLE report ADD COLUMN trade_date VARCHAR(10)"))
            conn.commit()


def ensure_report_source_column():
    """SQLite: 若 report 表缺少 source 列则添加，用于区分测试样本（source=test）与正式研报。"""
    if not settings.database_url.startswith("sqlite"):
        return
    with engine.connect() as conn:
        r = conn.execute(text("PRAGMA table_info(report)"))
        cols = [row[1] for row in r.fetchall()]
        if "source" not in cols:
            conn.execute(text("ALTER TABLE report ADD COLUMN source VARCHAR(16) DEFAULT 'real'"))
            conn.commit()


def ensure_user_phone_column():
    """SQLite: 若 app_user 表缺少 phone 列则添加，支持手机号注册/登录。"""
    if not settings.database_url.startswith("sqlite"):
        return
    with engine.connect() as conn:
        r = conn.execute(text("PRAGMA table_info(app_user)"))
        cols = [row[1] for row in r.fetchall()]
        if "phone" not in cols:
            conn.execute(text("ALTER TABLE app_user ADD COLUMN phone VARCHAR(32)"))
            conn.commit()


def ensure_report_llm_audit_columns():
    """v26 P0: report 表补 llm_actual_model / llm_provider_name / llm_endpoint 三列，
    用于回答 "实际命中哪个模型 / 哪个网关 / 哪个端点"，无证据则视为不合规。
    """
    if not settings.database_url.startswith("sqlite"):
        return
    with engine.connect() as conn:
        r = conn.execute(text("PRAGMA table_info(report)"))
        cols = [row[1] for row in r.fetchall()]
        if "llm_actual_model" not in cols:
            conn.execute(text("ALTER TABLE report ADD COLUMN llm_actual_model VARCHAR(64)"))
        if "llm_provider_name" not in cols:
            conn.execute(text("ALTER TABLE report ADD COLUMN llm_provider_name VARCHAR(64)"))
        if "llm_endpoint" not in cols:
            conn.execute(text("ALTER TABLE report ADD COLUMN llm_endpoint VARCHAR(255)"))
        conn.commit()


def ensure_app_user_admin_seed():
    """Ensure the documented super admin exists in app_user.

    Historical ad-hoc scripts wrote admin@yanbao.local into legacy table `user`,
    while real auth queries `app_user`. Copy/migrate the account here so web login
    works against the current auth model.
    """
    from app.core.security import hash_password
    from app.models import User

    db = SessionLocal()
    try:
        email = "admin@yanbao.local"
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            return

        legacy = None
        try:
            legacy = db.execute(
                text(
                    """
                    SELECT email, password_hash, nickname, role
                    FROM user
                    WHERE email = :email
                    LIMIT 1
                    """
                ),
                {"email": email},
            ).mappings().first()
        except Exception:
            legacy = None

        now = datetime.now(timezone.utc)
        db.add(
            User(
                user_id=str(uuid4()),
                email=email,
                password_hash=str((legacy or {}).get("password_hash") or hash_password("Yb!Admin#26-04-09")),
                nickname=str((legacy or {}).get("nickname") or "System Admin"),
                role="super_admin",
                tier="Enterprise",
                membership_level="annual",
                membership_expires_at=now + timedelta(days=3650),
                email_verified=True,
                failed_login_count=0,
                created_at=now,
                updated_at=now,
            )
        )
        db.commit()
    finally:
        db.close()

def ensure_sqlite_schema_alignment(engine) -> None:
    """Align SQLite schema with runtime enum changes for legacy databases."""
    if engine.dialect.name != "sqlite":
        return
    _ensure_report_data_usage_status_alignment(engine)


def _ensure_report_data_usage_status_alignment(target_engine) -> None:
    expected_tokens = ("proxy_ok", "realtime_only")
    with target_engine.connect() as conn:
        table_sql = conn.execute(
            text(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='report_data_usage'"
            )
        ).scalar()
        if not table_sql:
            return
        lowered = str(table_sql).lower()
        if all(token in lowered for token in expected_tokens):
            return

        indexes = conn.execute(
            text(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type='index'
                  AND tbl_name='report_data_usage'
                  AND sql IS NOT NULL
                ORDER BY name
                """
            )
        ).scalars().all()

        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
        conn.exec_driver_sql("ALTER TABLE report_data_usage RENAME TO report_data_usage__legacy")
        conn.exec_driver_sql(
            """
            CREATE TABLE report_data_usage (
                usage_id VARCHAR(36) PRIMARY KEY,
                trade_date DATE NOT NULL,
                stock_code VARCHAR(16) NOT NULL,
                dataset_name VARCHAR(32) NOT NULL,
                source_name VARCHAR(32) NOT NULL,
                batch_id VARCHAR(36) NOT NULL,
                fetch_time DATETIME NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'ok',
                status_reason TEXT,
                created_at DATETIME NOT NULL,
                CONSTRAINT ck_report_data_usage_status_enum CHECK (
                    status IN ('ok', 'stale_ok', 'missing', 'degraded', 'proxy_ok', 'realtime_only')
                )
            )
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO report_data_usage (
                usage_id,
                trade_date,
                stock_code,
                dataset_name,
                source_name,
                batch_id,
                fetch_time,
                status,
                status_reason,
                created_at
            )
            SELECT
                usage_id,
                trade_date,
                stock_code,
                dataset_name,
                source_name,
                batch_id,
                fetch_time,
                status,
                status_reason,
                created_at
            FROM report_data_usage__legacy
            """
        )
        conn.exec_driver_sql("DROP TABLE report_data_usage__legacy")
        for index_sql in indexes:
            conn.exec_driver_sql(index_sql)
        conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        conn.commit()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
