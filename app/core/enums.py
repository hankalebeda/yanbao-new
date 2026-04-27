from __future__ import annotations

from enum import Enum
from typing import Iterable


class FrozenStrEnum(str, Enum):
    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(member.value for member in cls)


class Recommendation(FrozenStrEnum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class QualityFlag(FrozenStrEnum):
    OK = "ok"
    STALE_OK = "stale_ok"
    MISSING = "missing"
    DEGRADED = "degraded"


class MarketState(FrozenStrEnum):
    BULL = "BULL"
    BEAR = "BEAR"
    NEUTRAL = "NEUTRAL"


class UserRole(FrozenStrEnum):
    USER = "user"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"


class UserTier(FrozenStrEnum):
    FREE = "Free"
    PRO = "Pro"
    ENTERPRISE = "Enterprise"


class PositionStatus(FrozenStrEnum):
    OPEN = "OPEN"
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    TIMEOUT = "TIMEOUT"
    CLOSED = "CLOSED"
    DELISTED_LIQUIDATED = "DELISTED_LIQUIDATED"
    SKIPPED = "SKIPPED"


class TaskStatus(FrozenStrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SUSPENDED = "SUSPENDED"
    EXPIRED = "EXPIRED"


class NotificationStatus(FrozenStrEnum):
    SENT = "sent"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"


class CapitalTier(FrozenStrEnum):
    TEN_K = "10k"
    HUNDRED_K = "100k"
    FIVE_HUNDRED_K = "500k"


class EnumContractConflict(ValueError):
    pass


SHARED_ENUMS: tuple[type[FrozenStrEnum], ...] = (
    Recommendation,
    QualityFlag,
    MarketState,
    UserRole,
    UserTier,
    PositionStatus,
    TaskStatus,
    NotificationStatus,
    CapitalTier,
)

CAPITAL_TIER_VALUES = CapitalTier.values()
DEFAULT_CAPITAL_TIERS = {
    "10k": {"label": "1 万档", "amount": 10_000},
    "100k": {"label": "10 万档", "amount": 100_000},
    "500k": {"label": "50 万档", "amount": 500_000},
}


def enum_values(enum_type: type[FrozenStrEnum]) -> tuple[str, ...]:
    return enum_type.values()


def build_enum_registry(enum_types: Iterable[type[FrozenStrEnum]] = SHARED_ENUMS) -> dict[str, tuple[str, ...]]:
    registry: dict[str, tuple[str, ...]] = {}
    for enum_type in enum_types:
        name = enum_type.__name__
        if name in registry:
            raise EnumContractConflict(f"duplicate enum registered: {name}")
        registry[name] = enum_values(enum_type)
    return registry


ENUM_REGISTRY = build_enum_registry()